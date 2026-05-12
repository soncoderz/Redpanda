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

export const appointmentObject = restate.object({
  name: "Appointment",
  handlers: {
    /* ───────── Appointment Lifecycle (thêm / sửa / xóa) ───────── */

    /** Tạo lịch hẹn mới, lưu DB + publish event + đặt lịch nhắc qua Restate delay */
    create: restate.createObjectHandler(
      {
        input: restate.serde.schema(CreateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const existing = await load(ctx);
        if (existing) {
          if (!input.idempotencyKey || existing.idempotencyKey === input.idempotencyKey) {
            return existing;
          }
          throw new restate.TerminalError(`Appointment ${ctx.key} already exists for a different idempotency key`);
        }

        const now = await ctx.date.toJSON();

        return save(ctx, {
          id: ctx.key,
          ...input,
          version: 1,
          status: "booked",
          createdAt: now,
          updatedAt: now,
          reminders: freshReminders(1),
          history: [{ type: "created", at: now, version: 1, details: { appointment: input } }],
        }, "appointment.created");
      },
    ),

    /** Lấy thông tin lịch hẹn theo ID */
    get: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const appointment = await load(ctx, true);
        setState(ctx, appointment);
        return appointment;
      },
    ),

    /** Cập nhật lịch hẹn, tăng version, huỷ lịch nhắc cũ và đặt lại lịch nhắc mới */
    update: restate.createObjectHandler(
      {
        input: restate.serde.schema(UpdateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const existing = await load(ctx, true);
        if (existing.status === "cancelled") {
          throw new restate.TerminalError(`Appointment ${ctx.key} is already cancelled`);
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        return save(ctx, {
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
        }, "appointment.updated", { before: snapshot(existing) });
      },
    ),

    /** Huỷ lịch hẹn, đánh dấu reminder đang chờ thành cancelled */
    cancel: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const existing = await load(ctx, true);
        if (existing.status === "cancelled") {
          setState(ctx, existing);
          return existing;
        }

        const now = await ctx.date.toJSON();
        const v = existing.version + 1;

        return save(ctx, {
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
        }, "appointment.cancelled");
      },
    ),

    /* ───────── Reminder Delivery (Restate delay → BullMQ → gửi mail) ───────── */

    /** Restate gọi khi hết delay — validate rồi đẩy job immediate vào BullMQ */
    sendReminder: restate.createObjectHandler(
      { input: restate.serde.schema(SendReminderInput) },
      async (ctx: restate.ObjectContext, input: SendReminderInputType) => {
        const appointment = await load(ctx);
        if (!appointment) return;
        if (appointment.version !== input.version) return;
        if (appointment.status === "cancelled") return;
        if (appointment.reminders[input.reminder].sent) return;

        const now = await ctx.date.toJSON();

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

    /** BullMQ worker gọi trước khi gửi mail — validate lần cuối, trả shouldSend */
    startReminderDelivery: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryRequest),
        output: restate.serde.schema(ReminderDeliveryStartResult),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await load(ctx, true);
        const now = await ctx.date.toJSON();

        const stale = validateReminder(appointment, input);
        if (stale) return { shouldSend: false, reason: stale };

        appointment.reminders[input.reminder] = {
          ...appointment.reminders[input.reminder],
          scheduled: false, jobId: input.jobId, startedAt: now, error: undefined,
        };
        appointment.updatedAt = now;
        appointment.history.push({ type: "reminder_started", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId });

        await ctx.run("persist delivery start", () => replaceAppointmentRecord(appointment), RETRY);
        setState(ctx, appointment);
        return { shouldSend: true };
      },
    ),

    /** BullMQ worker gọi sau khi gửi mail — cập nhật kết quả sent/skipped/failed */
    recordReminderResult: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryResultInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await load(ctx, true);
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

/* ───────── Core helpers ───────── */

/** Load appointment từ MongoDB, throw nếu required=true mà không tìm thấy */
async function load(ctx: restate.ObjectContext, required?: false): Promise<AppointmentStateType | undefined>;
async function load(ctx: restate.ObjectContext, required: true): Promise<AppointmentStateType>;
async function load(ctx: restate.ObjectContext, required?: boolean) {
  const appointment = await ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
  if (!appointment && required) {
    throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
  }
  return appointment;
}

/** Lưu DB → publish event Kafka → đặt lịch nhắc (nếu booked) → set state */
async function save(
  ctx: restate.ObjectContext,
  appointment: AppointmentStateType,
  eventType: AppointmentEventType,
  eventPayload?: Record<string, unknown>,
) {
  if (appointment.status === "booked") {
    scheduleReminders(ctx, appointment);
  }

  const isNew = eventType === "appointment.created";
  appointment = await ctx.run(
    "persist",
    () => isNew ? createAppointmentRecord(appointment) : replaceAppointmentRecord(appointment),
    RETRY,
  );

  await ctx.run(`publish ${eventType}`, () => publishAppointmentEvent({
    eventId: `${eventType}:${appointment.id}:${appointment.version}`,
    type: eventType,
    appointmentId: appointment.id,
    version: appointment.version,
    occurredAt: appointment.updatedAt,
    payload: { appointment: snapshot(appointment), ...eventPayload },
  }), RETRY);

  setState(ctx, appointment);
  return appointment;
}

/* ───────── Reminder scheduling ───────── */

/** Đặt Restate delayed send cho mỗi reminder (before/atTime/after), skip nếu đã qua giờ */
function scheduleReminders(ctx: restate.ObjectContext, appointment: AppointmentStateType) {
  const nowMs = new Date(appointment.updatedAt).getTime();

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

/* ───────── Inline helpers ───────── */

/** Lưu workflow state vào Restate K/V store */
function setState(ctx: restate.ObjectContext, a: AppointmentStateType) {
  ctx.set(STATE_KEY, { id: a.id, version: a.version, status: a.status, updatedAt: a.updatedAt });
}

/** Trích các trường chính để lưu vào event payload / history */
function snapshot(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName, customerEmail: a.customerEmail,
    service: a.service, startAt: a.startAt, note: a.note, status: a.status,
    createdAt: a.createdAt, updatedAt: a.updatedAt, cancelledAt: a.cancelledAt,
  };
}

/** Trích thông tin cần thiết để gửi email */
function emailPayload(a: AppointmentStateType) {
  return {
    id: a.id, version: a.version, customerName: a.customerName,
    customerEmail: a.customerEmail, service: a.service, startAt: a.startAt, note: a.note,
  };
}

/** Tạo reminder status trống cho version mới (before/atTime/after đều chưa gửi) */
function freshReminders(version: number): AppointmentStateType["reminders"] {
  const empty = { version, sent: false, scheduled: false };
  return { before: { ...empty }, atTime: { ...empty }, after: { ...empty } };
}

/** Đánh dấu các reminder đang chờ thành cancelled (dùng khi huỷ lịch hẹn) */
function cancelledReminders(existing: AppointmentStateType, version: number, now: string) {
  const result = { ...existing.reminders };
  for (const r of REMINDER_TYPES) {
    if (result[r].scheduled && !result[r].sent) {
      result[r] = { ...result[r], version, scheduled: false, canceledAt: now, error: "appointment cancelled" };
    }
  }
  return result;
}

/** Tạo history entries cho các reminder bị huỷ */
function cancelledReminderHistory(existing: AppointmentStateType, version: number, now: string) {
  return REMINDER_TYPES
    .filter((r) => existing.reminders[r].scheduled && !existing.reminders[r].sent)
    .map((r) => ({
      type: "reminder_cancelled" as const, at: now, version,
      reminder: r as ReminderType, jobId: existing.reminders[r].jobId,
      details: { reason: "appointment cancelled" },
    }));
}

/** Kiểm tra reminder còn hợp lệ không (đúng version, chưa cancel, chưa gửi, đúng jobId) */
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

/** Chuyển error thành format { status: "failed", error: string } để record vào DB */
export function toRecordableFailure(error: unknown) {
  return { status: "failed" as const, error: error instanceof Error ? error.message : String(error) };
}

export type AppointmentObject = typeof appointmentObject;
