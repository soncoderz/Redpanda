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
 * ║     d) Đặt lịch nhắc qua Restate delayed call (scheduleReminders)     ║
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
 * ║  Version-based staleness:                                              ║
 * ║  Khi appointment được update/cancel, version tăng lên.                 ║
 * ║  Các delayed call cũ mang version cũ → tự động bị skip                ║
 * ║  khi so sánh version không khớp, không cần cancel thủ công.            ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
export const appointmentObject = restate.object({
  name: "Appointment",
  handlers: {

    /* ═══════════════════════════════════════════════════════════════════════
     *  CREATE — Tạo lịch hẹn mới
     *
     *  Flow: Load MongoDB → Idempotency check → Tạo state v1
     *        → Schedule 3 reminder (before/atTime/after)
     *        → Lưu MongoDB → Publish Kafka → Set Restate K/V
     *
     *  Idempotency: Nếu appointment đã tồn tại với cùng idempotencyKey
     *  → trả về bản cũ thay vì throw error (safe retry).
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
          // Idempotent: cùng key hoặc không có key → trả về bản cũ
          if (!input.idempotencyKey || existing.idempotencyKey === input.idempotencyKey) {
            setState(ctx, existing);
            return existing;
          }
          // Khác idempotencyKey → conflict thật sự
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

        // B4: Đặt 3 delayed call cho reminder (before/atTime/after)
        //     Mỗi call mang version=1, khi appointment bị update → version tăng → call cũ tự skip
        scheduleReminders(ctx, appointment);

        // B5: Lưu vào MongoDB (ctx.run đảm bảo side-effect chỉ chạy 1 lần khi replay)
        const saved = await ctx.run(
          "persist",
          () => createAppointmentRecord(appointment),
          RETRY,
        );

        // B6: Publish event ra Kafka cho các consumer (analytics, telegram, SSE)
        await ctx.run("publish appointment.created", () => publishAppointmentEvent({
          eventId: `appointment.created:${saved.id}:${saved.version}`,
          type: "appointment.created",
          appointmentId: saved.id,
          version: saved.version,
          occurredAt: now,
          payload: { appointment: snapshot(saved) },
        }), RETRY);

        // B7: Lưu tóm tắt vào Restate K/V store (id, version, status, updatedAt)
        setState(ctx, saved);
        return saved;
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  GET — Lấy thông tin appointment
     *
     *  Flow: Load MongoDB → Trả về (đồng thời sync Restate K/V)
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
     *  Flow: Load MongoDB → Validate (tồn tại? chưa cancel?)
     *        → Tăng version → Reset reminders → Schedule mới
     *        → Lưu MongoDB → Publish Kafka → Set Restate K/V
     *
     *  Khi version tăng, các reminder cũ (mang version cũ) sẽ tự động
     *  bị skip trong sendReminder nhờ version mismatch check.
     * ═══════════════════════════════════════════════════════════════════════ */
    update: restate.createObjectHandler(
      {
        input: restate.serde.schema(UpdateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        // B1: Load + validate
        const existing = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!existing) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }
        if (existing.status === "cancelled") {
          throw new restate.TerminalError(`Appointment ${ctx.key} is already cancelled`);
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        // B2: Tạo state mới — version tăng, reminders reset về trạng thái trống
        //     Spread ...input ghi đè các trường được cập nhật (customerName, startAt, etc.)
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

        // B3: Schedule 3 reminder mới với version mới
        //     Reminder cũ (version cũ) sẽ tự skip khi fire → không cần cancel
        scheduleReminders(ctx, appointment);

        // B4: Persist → Publish → Set state
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
     *  Flow: Load MongoDB → Validate → Tăng version → status="cancelled"
     *        → Đánh dấu reminder đang chờ thành cancelled
     *        → Lưu MongoDB → Publish Kafka → Set Restate K/V
     *
     *  Idempotent: gọi cancel nhiều lần trên appointment đã cancel
     *  → trả về bản cũ, không throw error.
     * ═══════════════════════════════════════════════════════════════════════ */
    cancel: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const existing = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
        if (!existing) {
          throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
        }
        // Idempotent: đã cancel rồi → trả về luôn
        if (existing.status === "cancelled") {
          setState(ctx, existing);
          return existing;
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        // Đánh dấu tất cả reminder đang scheduled (chưa sent) thành cancelled
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
     *  Flow: Load MongoDB → Validate version + status + chưa gửi
     *        → Đẩy job vào BullMQ (enqueueImmediateEmail)
     *        → Cập nhật reminder status → Lưu MongoDB → Set Restate K/V
     *
     *  Được gọi tự động bởi Restate khi hết delay (trước/đúng/sau giờ hẹn).
     *  Nếu appointment đã bị update (version khác) hoặc cancel → return sớm,
     *  job không được tạo = reminder bị skip tự nhiên.
     *
     *  ctx.run wraps enqueueImmediateEmail → đảm bảo job chỉ được tạo 1 lần
     *  ngay cả khi Restate replay handler.
     * ═══════════════════════════════════════════════════════════════════════ */
    sendReminder: restate.createObjectHandler(
      { input: restate.serde.schema(SendReminderInput) },
      async (ctx: restate.ObjectContext, input: SendReminderInputType) => {
        const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);

        // Guard clauses — bất kỳ điều kiện nào fail → return sớm, không tạo job
        if (!appointment) return;                                    // appointment bị xóa
        if (appointment.version !== input.version) return;           // appointment đã update → version mới
        if (appointment.status === "cancelled") return;              // appointment đã huỷ
        if (appointment.reminders[input.reminder].sent) return;      // email đã gửi rồi

        const now = await ctx.date.toJSON();

        // Đẩy job vào BullMQ — job ID deterministic (appointment+version+reminder)
        // nên nếu Restate replay, BullMQ sẽ dedupe tự động
        const result = await ctx.run(
          `enqueue ${input.reminder} email`,
          () => enqueueImmediateEmail({
            reminder: input.reminder,
            scheduledFor: input.scheduledFor,
            appointment: emailPayload(appointment),
          }),
          RETRY,
        );

        // Cập nhật trạng thái reminder: đã queued vào BullMQ
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
     *  Flow: Load MongoDB → Validate (version, status, chưa gửi, đúng jobId)
     *        → Nếu hợp lệ: cập nhật startedAt, trả shouldSend=true
     *        → Nếu stale: trả shouldSend=false + reason
     *
     *  Đây là "cổng validation cuối cùng" trước khi email thật sự được gửi.
     *  Ngăn gửi email cho appointment đã bị update/cancel giữa lúc
     *  job nằm trong BullMQ queue chờ xử lý.
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

        // Validate: version khớp? chưa cancel? chưa gửi? đúng jobId?
        const stale = validateReminder(appointment, input);
        if (stale) return { shouldSend: false, reason: stale };

        // Đánh dấu reminder đang được xử lý (startedAt)
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

        // Worker nhận shouldSend=true → tiến hành gửi email
        return { shouldSend: true };
      },
    ),

    /* ═══════════════════════════════════════════════════════════════════════
     *  RECORD REMINDER RESULT — BullMQ worker gọi SAU khi gửi email
     *
     *  Flow: Load MongoDB → Validate → Cập nhật kết quả
     *        → sent:    đánh dấu sent=true + publish Kafka event
     *        → skipped: ghi reason (VD: email bounce, invalid address)
     *        → failed:  ghi error message để debug
     *
     *  Đây là bước cuối trong vòng đời reminder:
     *  schedule → queue → start → send email → record result
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

        // Nếu version/status không khớp → trả về state hiện tại, không cập nhật
        const stale = validateReminder(appointment, input);
        if (stale) return appointment;

        const r = appointment.reminders[input.reminder];

        if (input.result.status === "sent") {
          // Email gửi thành công → đánh dấu sent=true
          appointment.reminders[input.reminder] = { ...r, sent: true, scheduled: false, jobId: input.jobId, sentAt: now, error: undefined };
          appointment.history.push({ type: "reminder_sent", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId });
          appointment.updatedAt = now;

          await ctx.run("persist sent", () => replaceAppointmentRecord(appointment), RETRY);

          // Publish event "reminder.sent" → analytics, telegram, SSE
          await ctx.run("publish reminder.sent", () => publishAppointmentEvent({
            eventId: `reminder.sent:${appointment.id}:${appointment.version}:${input.reminder}`,
            type: "reminder.sent",
            appointmentId: appointment.id,
            version: appointment.version,
            occurredAt: now,
            payload: { appointment: snapshot(appointment), reminder: input.reminder, jobId: input.jobId },
          }), RETRY);

        } else if (input.result.status === "skipped") {
          // Email bị skip (VD: SendGrid trả lỗi nhẹ, email không hợp lệ)
          appointment.reminders[input.reminder] = { ...r, sent: false, scheduled: false, jobId: input.jobId, skippedAt: now, error: input.result.reason };
          appointment.history.push({ type: "reminder_skipped", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId, details: { reason: input.result.reason, statusCode: input.result.statusCode, responseBody: input.result.responseBody } });
          appointment.updatedAt = now;

          await ctx.run("persist skipped", () => replaceAppointmentRecord(appointment), RETRY);

        } else {
          // Email gửi thất bại (VD: SendGrid timeout, network error)
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
 *  SCHEDULE REMINDERS — Đặt 3 delayed call cho before/atTime/after
 *
 *  Tính thời gian target cho mỗi reminder dựa trên startAt:
 *    before  = startAt - REMINDER_BEFORE_MS
 *    atTime  = startAt
 *    after   = startAt + REMINDER_AFTER_MS
 *
 *  Nếu target đã qua (targetMs <= nowMs) → skip, ghi vào history.
 *  Nếu chưa qua → tạo Restate delayed call sendReminder với delay tương ứng.
 *
 *  Mỗi call mang version hiện tại → khi appointment bị update (version tăng),
 *  call cũ sẽ fire nhưng version mismatch → sendReminder return sớm.
 * ═══════════════════════════════════════════════════════════════════════════ */
function scheduleReminders(ctx: restate.ObjectContext, appointment: AppointmentStateType) {
  const nowMs = new Date(appointment.updatedAt).getTime();

  for (const reminder of REMINDER_TYPES) {
    const targetMs = reminderTargetMs(reminder, appointment.startAt);
    const scheduledFor = new Date(targetMs).toISOString();

    // Thời gian đã qua → skip reminder này
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

    // Đặt delayed call — Restate sẽ tự gọi sendReminder sau (targetMs - nowMs) ms
    ctx
      .objectSendClient<AppointmentObject>({ name: "Appointment" }, ctx.key)
      .sendReminder(
        { reminder, version: appointment.version, scheduledFor },
        restate.rpc.sendOpts({ delay: targetMs - nowMs }),
      );

    appointment.reminders[reminder] = {
      ...appointment.reminders[reminder],
      scheduled: true,
      scheduledAt: appointment.updatedAt,
      scheduledFor,
    };
    appointment.history.push({
      type: "reminder_scheduled", at: appointment.updatedAt, version: appointment.version,
      reminder, details: { scheduledFor },
    });
  }
}

/* ─── Inline Helpers ────────────────────────────────────────────────────── */

// Lưu tóm tắt vào Restate K/V — chỉ giữ 4 trường chính để query nhanh
function setState(ctx: restate.ObjectContext, a: AppointmentStateType) {
  ctx.set(STATE_KEY, { id: a.id, version: a.version, status: a.status, updatedAt: a.updatedAt });
}

// Trích các trường chính để lưu vào event payload / history detail
function snapshot(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName, customerEmail: a.customerEmail,
    service: a.service, startAt: a.startAt, note: a.note, status: a.status,
    createdAt: a.createdAt, updatedAt: a.updatedAt, cancelledAt: a.cancelledAt,
  };
}

// Trích thông tin tối thiểu cần thiết để gửi email
function emailPayload(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName,
    customerEmail: a.customerEmail, service: a.service, startAt: a.startAt, note: a.note,
  };
}

// Tạo reminder status trống cho version mới — before/atTime/after đều chưa gửi
function freshReminders(version: number): AppointmentStateType["reminders"] {
  const empty = { version, sent: false, scheduled: false };
  return { before: { ...empty }, atTime: { ...empty }, after: { ...empty } };
}

// Đánh dấu các reminder đang scheduled (chưa sent) thành cancelled
function cancelledReminders(existing: AppointmentStateType, version: number, now: string) {
  const result = { ...existing.reminders };
  for (const r of REMINDER_TYPES) {
    if (result[r].scheduled && !result[r].sent) {
      result[r] = { ...result[r], version, scheduled: false, canceledAt: now, error: "appointment cancelled" };
    }
  }
  return result;
}

// Tạo history entries cho mỗi reminder bị huỷ (dùng khi cancel appointment)
function cancelledReminderHistory(existing: AppointmentStateType, version: number, now: string) {
  return REMINDER_TYPES
    .filter((r) => existing.reminders[r].scheduled && !existing.reminders[r].sent)
    .map((r) => ({
      type: "reminder_cancelled" as const, at: now, version,
      reminder: r as ReminderType, jobId: existing.reminders[r].jobId,
      details: { reason: "appointment cancelled" },
    }));
}

// Validate reminder còn hợp lệ không — trả string lý do nếu stale, undefined nếu OK
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
