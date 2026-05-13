import * as restate from "@restatedev/restate-sdk";

import {
  AppointmentState,
  CreateAppointmentWorkflowInput,
  ReminderDeliveryRequest,
  ReminderDeliveryResultInput,
  ReminderDeliveryStartResult,
  SendReminderInput,
  UpdateAppointmentWorkflowInput,
  type AppointmentEventType,
  type AppointmentState as AppointmentStateType,
  type ReminderType,
  type SendReminderInput as SendReminderInputType,
} from "../../models/appointment.model.js";
import {
  createAppointmentRecord,
  findAppointmentById,
  replaceAppointmentRecord,
} from "../../models/appointment.repository.js";
import { enqueueImmediateEmail } from "../queue/email-queue.service.js";
import { publishAppointmentEvent } from "../messaging/event-publisher.service.js";
import { reminderTargetMs, REMINDER_TYPES } from "../../utils/appointment.utils.js";

const RETRY = { maxRetryAttempts: 5 };
const STATE_KEY = "appointment-workflow";
const INVOCATIONS_KEY = "reminder-invocations";

type ReminderInvocations = Record<ReminderType, string>;

/*
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  Restate Virtual Object — "Appointment"                                ║
 * ║                                                                        ║
 * ║  Mỗi appointment ID là 1 key riêng trong Restate.                      ║
 * ║  Tất cả handler cùng key được serialize — không bao giờ chạy song song ║
 * ║  trên cùng 1 appointment, đảm bảo consistency.                         ║
 * ║                                                                        ║
 * ║  Flow tổng quan:                                                       ║
 * ║                                                                        ║
 * ║  1. Client gọi create/update/cancel qua REST API                       ║
 * ║     → Controller gọi Restate client → Restate gọi handler tương ứng    ║
 * ║                                                                        ║
 * ║  2. Handler xử lý:                                                     ║
 * ║     a) Load appointment từ MongoDB (ctx.run → findAppointmentById)     ║
 * ║     b) Validate trạng thái                                             ║
 * ║     c) Tạo state mới với version tăng                                  ║
 * ║     d) Cancel invocation cũ + đặt lịch nhắc mới (scheduleReminders)   ║
 * ║     e) Lưu MongoDB (ctx.run → createRecord/replaceRecord)             ║
 * ║     f) Publish event Kafka (ctx.run → publishAppointmentEvent)         ║
 * ║     g) Set tóm tắt vào Restate K/V (setState)                         ║
 * ║                                                                        ║
 * ║  3. Khi đến giờ nhắc, Restate tự gọi sendReminder                     ║
 * ║     → Validate version (skip nếu stale)                                ║
 * ║     → Đẩy job vào BullMQ (enqueueImmediateEmail)                       ║
 * ║                                                                        ║
 * ║  4. BullMQ Worker xử lý job:                                           ║
 * ║     a) Gọi startReminderDelivery → Restate validate lần cuối           ║
 * ║     b) Nếu shouldSend=true → gửi email qua SendGrid                   ║
 * ║     c) Gọi recordReminderResult → Restate ghi kết quả                 ║
 * ║                                                                        ║
 * ║  Explicit cancel:                                                      ║
 * ║  Khi update/cancel, invocation IDs cũ được đọc từ Restate K/V          ║
 * ║  → ctx.cancel() huỷ từng invocation → schedule mới (nếu update)        ║
 * ║  → Restate chỉ giữ đúng 3 invocation active tại mọi thời điểm.        ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
export const appointmentObject = restate.object({
  name: "Appointment",
  handlers: {

    /* ═══════════════════════════════════════════════════════════════════════
     *  CREATE — Tạo lịch hẹn mới
     *
     *  Flow: Load MongoDB → Idempotency check → Tạo state v1
     *        → Schedule 3 reminder (before/atTime/after) + lưu invocation IDs
     *        → Lưu MongoDB → Publish Kafka → Set Restate K/V
     * ═══════════════════════════════════════════════════════════════════════ */
    create: restate.createObjectHandler(
      {
        input: restate.serde.schema(CreateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        // B1: Load từ MongoDB — kiểm tra đã tồn tại chưa
        const existing = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (existing) {
          if (!input.idempotencyKey || existing.idempotencyKey === input.idempotencyKey) {
            setState(ctx, existing);
            return existing;
          }
          throw new restate.TerminalError(`Appointment ${ctx.key} already exists for a different idempotency key`);
        }

        // B2: Lấy timestamp từ Restate (deterministic, replay-safe)
        const now = await ctx.date.toJSON();

        // B3: Tạo state ban đầu — version=1, status="booked"
        const appointment: AppointmentStateType = {
          id: ctx.key,
          ...input,
          version: 1,
          status: "booked",
          createdAt: now,
          updatedAt: now,
          reminders: freshReminders(1),
          history: [{ type: "created", at: now, version: 1, details: { appointment: input } }],
        };

        // B4: Đặt 3 delayed call + lưu invocation IDs vào Restate K/V
        await scheduleReminders(ctx, appointment);

        // B5: Lưu vào MongoDB
        const saved = await ctx.run(
          "persist",
          () => createAppointmentRecord(appointment),
          RETRY,
        );

        // B6: Publish event ra Kafka
        await ctx.run("publish appointment.created", () => publishAppointmentEvent({
          eventId: `appointment.created:${saved.id}:${saved.version}`,
          type: "appointment.created",
          appointmentId: saved.id,
          version: saved.version,
          occurredAt: now,
          payload: { appointment: snapshot(saved) },
        }), RETRY);

        // B7: Lưu tóm tắt vào Restate K/V
        setState(ctx, saved);
        return saved;
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  GET — Lấy thông tin appointment
     * ═══════════════════════════════════════════════════════════════════════ */
    get: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!appointment) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }
        setState(ctx, appointment);
        return appointment;
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  UPDATE — Cập nhật lịch hẹn
     *
     *  Flow: Load → Validate → Cancel 3 invocation cũ → Tăng version
     *        → Schedule 3 invocation mới → Persist → Publish → setState
     *
     *  Explicit cancel: đọc invocation IDs từ Restate K/V → ctx.cancel()
     *  → Restate huỷ delayed call, không để tích lũy invocation rác.
     * ═══════════════════════════════════════════════════════════════════════ */
    update: restate.createObjectHandler(
      {
        input: restate.serde.schema(UpdateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const existing = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!existing) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }
        if (existing.status === "cancelled") {
          throw new restate.TerminalError(`Appointment ${ctx.key} is already cancelled`);
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        // Cancel 3 invocation cũ trên Restate (trước khi schedule mới)
        await cancelScheduledReminders(ctx);

        const appointment: AppointmentStateType = {
          ...existing,
          ...input,
          version: v,
          status: "booked",
          updatedAt: now,
          reminders: freshReminders(v),
          history: [...existing.history, {
            type: "updated", at: now, version: v,
            details: { before: snapshot(existing), after: { ...snapshot(existing), ...input, version: v, updatedAt: now } },
          }],
        };

        // Schedule 3 invocation mới với version mới + lưu invocation IDs
        await scheduleReminders(ctx, appointment);

        const saved = await ctx.run(
          "persist",
          () => replaceAppointmentRecord(appointment),
          RETRY,
        );

        await ctx.run("publish appointment.updated", () => publishAppointmentEvent({
          eventId: `appointment.updated:${saved.id}:${saved.version}`,
          type: "appointment.updated",
          appointmentId: saved.id,
          version: saved.version,
          occurredAt: now,
          payload: { appointment: snapshot(saved), before: snapshot(existing) },
        }), RETRY);

        setState(ctx, saved);
        return saved;
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  CANCEL — Huỷ lịch hẹn
     *
     *  Flow: Load → Validate → Cancel 3 invocation → status="cancelled"
     *        → Persist → Publish → setState
     *
     *  Idempotent: gọi cancel nhiều lần → trả bản cũ, không error.
     * ═══════════════════════════════════════════════════════════════════════ */
    cancel: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const existing = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!existing) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }
        if (existing.status === "cancelled") {
          setState(ctx, existing);
          return existing;
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        // Cancel 3 invocation đang chờ trên Restate
        await cancelScheduledReminders(ctx);

        const appointment: AppointmentStateType = {
          ...existing,
          version: v,
          status: "cancelled",
          cancelledAt: now,
          updatedAt: now,
          reminders: cancelledReminders(existing, v, now),
          history: [
            ...existing.history,
            ...cancelledReminderHistory(existing, v, now),
            { type: "cancelled", at: now, version: v, details: { before: snapshot(existing) } },
          ],
        };

        const saved = await ctx.run(
          "persist",
          () => replaceAppointmentRecord(appointment),
          RETRY,
        );

        await ctx.run("publish appointment.cancelled", () => publishAppointmentEvent({
          eventId: `appointment.cancelled:${saved.id}:${saved.version}`,
          type: "appointment.cancelled",
          appointmentId: saved.id,
          version: saved.version,
          occurredAt: now,
          payload: { appointment: snapshot(saved) },
        }), RETRY);

        setState(ctx, saved);
        return saved;
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  SEND REMINDER — Restate delayed call fire khi đến giờ nhắc
     *
     *  Được gọi tự động bởi Restate khi hết delay.
     *  Validate version + status → đẩy job vào BullMQ.
     *  Guard clauses vẫn giữ làm safety net phòng race condition.
     * ═══════════════════════════════════════════════════════════════════════ */
    sendReminder: restate.createObjectHandler(
      { input: restate.serde.schema(SendReminderInput) },
      async (ctx: restate.ObjectContext, input: SendReminderInputType) => {
        const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);

        // Guard clauses — safety net nếu cancel chưa kịp xử lý
        if (!appointment) return;
        if (appointment.version !== input.version) return;
        if (appointment.status === "cancelled") return;
        if (appointment.reminders[input.reminder].sent) return;

        const now = await ctx.date.toJSON();

        // Đẩy job vào BullMQ — deterministic jobId để dedupe khi replay
        const result = await ctx.run(
          `enqueue ${input.reminder} email`,
          () => enqueueImmediateEmail({
            reminder: input.reminder,
            scheduledFor: input.scheduledFor,
            appointment: emailPayload(appointment),
          }),
          RETRY,
        );

        appointment.reminders[input.reminder] = {
          ...appointment.reminders[input.reminder],
          scheduled: true,
          jobId: result.jobId,
          queuedAt: now,
        };
        appointment.updatedAt = now;
        appointment.history.push({
          type: "reminder_scheduled", at: now, version: appointment.version,
          reminder: input.reminder, jobId: result.jobId,
          details: { scheduledFor: input.scheduledFor },
        });

        await ctx.run("persist queued", () => replaceAppointmentRecord(appointment), RETRY);
        setState(ctx, appointment);
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  START REMINDER DELIVERY — BullMQ worker gọi TRƯỚC khi gửi email
     *
     *  "Cổng validation cuối cùng" — ngăn gửi email cho appointment
     *  đã bị update/cancel giữa lúc job nằm trong queue.
     * ═══════════════════════════════════════════════════════════════════════ */
    startReminderDelivery: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryRequest),
        output: restate.serde.schema(ReminderDeliveryStartResult),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!appointment) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }

        const now = await ctx.date.toJSON();

        const stale = validateReminder(appointment, input);
        if (stale) return { shouldSend: false, reason: stale };

        appointment.reminders[input.reminder] = {
          ...appointment.reminders[input.reminder],
          scheduled: false, jobId: input.jobId, startedAt: now, error: undefined,
        };
        appointment.updatedAt = now;
        appointment.history.push({
          type: "reminder_started", at: now, version: appointment.version,
          reminder: input.reminder, jobId: input.jobId,
        });

        await ctx.run("persist delivery start", () => replaceAppointmentRecord(appointment), RETRY);
        setState(ctx, appointment);
        return { shouldSend: true };
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  RECORD REMINDER RESULT — BullMQ worker gọi SAU khi gửi email
     *
     *  Bước cuối: schedule → queue → start → send email → record result
     * ═══════════════════════════════════════════════════════════════════════ */
    recordReminderResult: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryResultInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!appointment) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }

        const now = await ctx.date.toJSON();

        const stale = validateReminder(appointment, input);
        if (stale) return appointment;

        const r = appointment.reminders[input.reminder];

        if (input.result.status === "sent") {
          appointment.reminders[input.reminder] = { ...r, sent: true, scheduled: false, jobId: input.jobId, sentAt: now, error: undefined };
          appointment.history.push({ type: "reminder_sent", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId });
          appointment.updatedAt = now;

          await ctx.run("persist sent", () => replaceAppointmentRecord(appointment), RETRY);
          await ctx.run("publish reminder.sent", () => publishAppointmentEvent({
            eventId: `reminder.sent:${appointment.id}:${appointment.version}:${input.reminder}`,
            type: "reminder.sent",
            appointmentId: appointment.id,
            version: appointment.version,
            occurredAt: now,
            payload: { appointment: snapshot(appointment), reminder: input.reminder, jobId: input.jobId },
          }), RETRY);

        } else if (input.result.status === "skipped") {
          appointment.reminders[input.reminder] = { ...r, sent: false, scheduled: false, jobId: input.jobId, skippedAt: now, error: input.result.reason };
          appointment.history.push({ type: "reminder_skipped", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId, details: { reason: input.result.reason, statusCode: input.result.statusCode, responseBody: input.result.responseBody } });
          appointment.updatedAt = now;

          await ctx.run("persist skipped", () => replaceAppointmentRecord(appointment), RETRY);

        } else {
          appointment.reminders[input.reminder] = { ...r, sent: false, scheduled: false, jobId: input.jobId, failedAt: now, error: input.result.error };
          appointment.history.push({ type: "reminder_failed", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId, details: { error: input.result.error } });
          appointment.updatedAt = now;

          await ctx.run("persist failed", () => replaceAppointmentRecord(appointment), RETRY);
        }

        setState(ctx, appointment);
        return appointment;
      },
    ),
  },
});

/* ═══════════════════════════════════════════════════════════════════════════
 *  SCHEDULE REMINDERS — Đặt 3 delayed call + lưu invocation IDs
 *
 *  Với mỗi reminder (before/atTime/after):
 *    1. Tính targetMs từ startAt
 *    2. Nếu đã qua → skip
 *    3. Chưa qua → ctx.objectSendClient().sendReminder() với delay
 *    4. await invocationId → lưu vào Restate K/V (INVOCATIONS_KEY)
 *
 *  Invocation IDs được lưu để update/cancel có thể ctx.cancel() chúng,
 *  đảm bảo Restate chỉ giữ đúng 3 invocation active tại mọi thời điểm.
 * ═══════════════════════════════════════════════════════════════════════════ */
async function scheduleReminders(ctx: restate.ObjectContext, appointment: AppointmentStateType) {
  const nowMs = new Date(appointment.updatedAt).getTime();
  const invocations: ReminderInvocations = { before: "", atTime: "", after: "" };

  for (const reminder of REMINDER_TYPES) {
    const targetMs = reminderTargetMs(reminder, appointment.startAt);
    const scheduledFor = new Date(targetMs).toISOString();

    if (targetMs <= nowMs) {
      appointment.reminders[reminder] = {
        ...appointment.reminders[reminder],
        skippedAt: appointment.updatedAt,
        scheduledFor,
        error: "scheduled time already passed",
      };
      appointment.history.push({
        type: "reminder_skipped", at: appointment.updatedAt, version: appointment.version,
        reminder, details: { reason: "scheduled time already passed", scheduledFor },
      });
      continue;
    }

    // Đặt delayed call — lấy invocationId để có thể cancel sau
    const call = ctx
      .objectSendClient<AppointmentObject>({ name: "Appointment" }, ctx.key)
      .sendReminder(
        { reminder, version: appointment.version, scheduledFor },
        restate.rpc.sendOpts({ delay: targetMs - nowMs }),
      );

    const invocationId = await call.invocationId;
    invocations[reminder] = invocationId;

    appointment.reminders[reminder] = {
      ...appointment.reminders[reminder],
      scheduled: true,
      scheduledAt: appointment.updatedAt,
      scheduledFor,
    };
    appointment.history.push({
      type: "reminder_scheduled", at: appointment.updatedAt, version: appointment.version,
      reminder, details: { scheduledFor, invocationId },
    });
  }

  // Lưu invocation IDs vào Restate K/V — update/cancel sẽ đọc để ctx.cancel()
  ctx.set(INVOCATIONS_KEY, invocations);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  CANCEL SCHEDULED REMINDERS — Huỷ invocation cũ trước khi schedule mới
 *
 *  Đọc invocation IDs từ Restate K/V → ctx.cancel() từng cái
 *  → Xóa key để không cancel lại lần nữa
 *
 *  Gọi bởi: update (trước scheduleReminders) và cancel
 * ═══════════════════════════════════════════════════════════════════════════ */
async function cancelScheduledReminders(ctx: restate.ObjectContext) {
  const invocations = await ctx.get<ReminderInvocations>(INVOCATIONS_KEY);
  if (!invocations) return;

  for (const reminder of REMINDER_TYPES) {
    const invocationId = invocations[reminder];
    if (invocationId) {
      ctx.cancel(restate.InvocationIdParser.fromString(invocationId));
    }
  }

  ctx.clear(INVOCATIONS_KEY);
}

/* ─── Inline Helpers ────────────────────────────────────────────────────── */

function setState(ctx: restate.ObjectContext, a: AppointmentStateType) {
  ctx.set(STATE_KEY, { id: a.id, version: a.version, status: a.status, updatedAt: a.updatedAt });
}

function snapshot(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName, customerEmail: a.customerEmail,
    service: a.service, startAt: a.startAt, note: a.note, status: a.status,
    createdAt: a.createdAt, updatedAt: a.updatedAt, cancelledAt: a.cancelledAt,
  };
}

function emailPayload(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName,
    customerEmail: a.customerEmail, service: a.service, startAt: a.startAt, note: a.note,
  };
}

function freshReminders(version: number): AppointmentStateType["reminders"] {
  const empty = { version, sent: false, scheduled: false };
  return { before: { ...empty }, atTime: { ...empty }, after: { ...empty } };
}

function cancelledReminders(existing: AppointmentStateType, version: number, now: string) {
  const result = { ...existing.reminders };
  for (const r of REMINDER_TYPES) {
    if (result[r].scheduled && !result[r].sent) {
      result[r] = { ...result[r], version, scheduled: false, canceledAt: now, error: "appointment cancelled" };
    }
  }
  return result;
}

function cancelledReminderHistory(existing: AppointmentStateType, version: number, now: string) {
  return REMINDER_TYPES
    .filter((r) => existing.reminders[r].scheduled && !existing.reminders[r].sent)
    .map((r) => ({
      type: "reminder_cancelled" as const, at: now, version,
      reminder: r as ReminderType, jobId: existing.reminders[r].jobId,
      details: { reason: "appointment cancelled" },
    }));
}

function validateReminder(
  appointment: AppointmentStateType,
  input: { reminder: ReminderType; version: number; jobId: string },
) {
  if (appointment.version !== input.version) return `stale version ${input.version}`;
  if (appointment.status === "cancelled") return "appointment cancelled";
  if (appointment.reminders[input.reminder].sent) return "already sent";
  const r = appointment.reminders[input.reminder];
  if (r.jobId && r.jobId !== input.jobId) return `stale job ${input.jobId}`;
  return undefined;
}

export type AppointmentObject = typeof appointmentObject;
