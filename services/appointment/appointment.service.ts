import * as restate from "@restatedev/restate-sdk";

import {
  AppointmentState,
  CreateAppointmentWorkflowInput,
  ReminderDeliveryRequest,
  ReminderDeliveryResultInput,
  ReminderDeliveryStartResult,
  SendReminderInput,
  UpdateAppointmentWorkflowInput,
  type AppointmentState as AppointmentStateType,
  type ReminderDeliveryRequest as ReminderDeliveryRequestType,
  type ReminderDeliveryResultInput as ReminderDeliveryResultInputType,
  type ReminderType,
  type SendReminderInput as SendReminderInputType,
} from "../../models/appointment.model.js";
import {
  applyAppointmentPatch,
  createAppointmentRecord,
  findAppointmentById,
  replaceAppointmentRecord,
} from "../../models/appointment.repository.js";
import {
  applyScheduleResult,
  appendHistory,
  appointmentSnapshot,
  buildAppointmentEvent,
  buildReminderSentEvent,
  emptyReminderStatus,
  errorMessage,
  markPendingRemindersCancelled,
  REMINDER_TYPES,
  reminderTargetMs,
  toEmailPayload,
  toWorkflowState,
  WORKFLOW_STATE_KEY,
  type ReminderScheduleResult,
} from "../../utils/appointment.utils.js";
import {
  enqueueImmediateEmail,
} from "../queue/email-queue.service.js";
import { publishAppointmentEvent } from "../messaging/event-publisher.service.js";

const RETRY = { maxRetryAttempts: 5 };

export const appointmentObject = restate.object({
  name: "Appointment",
  handlers: {
    create: restate.createObjectHandler(
      {
        input: restate.serde.schema(CreateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const existing = await loadAppointment(ctx);
        if (existing) {
          if (
            !input.idempotencyKey ||
            existing.idempotencyKey === input.idempotencyKey
          ) {
            ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(existing));
            return existing;
          }

          throw new restate.TerminalError(
            `Appointment ${ctx.key} already exists for a different idempotency key`,
          );
        }

        const now = await ctx.date.toJSON();
        let appointment: AppointmentStateType = appendHistory(
          {
            id: ctx.key,
            ...input,
            version: 1,
            status: "booked",
            createdAt: now,
            updatedAt: now,
            reminders: emptyReminderStatus(1),
            history: [],
          },
          { type: "created", at: now, details: { appointment: input } },
        );

        appointment = await ctx.run(
          "persist appointment",
          () => createAppointmentRecord(appointment),
          RETRY,
        );

        await publishEvent(ctx, buildAppointmentEvent({
          type: "appointment.created",
          appointment,
          occurredAt: now,
        }));

        appointment = await persistReminderSchedule(ctx, appointment, now);
        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
        return appointment;
      },
    ),

    get: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const appointment = await requireAppointment(ctx);
        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
        return appointment;
      },
    ),

    update: restate.createObjectHandler(
      {
        input: restate.serde.schema(UpdateAppointmentWorkflowInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const existing = await requireAppointment(ctx);
        if (existing.status === "cancelled") {
          throw new restate.TerminalError(
            `Appointment ${ctx.key} is already cancelled`,
          );
        }

        const now = await ctx.date.toJSON();

        let updated: AppointmentStateType = appendHistory(
          {
            ...applyAppointmentPatch(existing, input),
            version: existing.version + 1,
            status: "booked",
            updatedAt: now,
            reminders: emptyReminderStatus(existing.version + 1),
          },
          {
            type: "updated",
            at: now,
            details: {
              before: appointmentSnapshot(existing),
              after: {
                ...appointmentSnapshot(existing),
                ...input,
                version: existing.version + 1,
                updatedAt: now,
              },
            },
          },
        );

        updated = await ctx.run(
          "persist update",
          () => replaceAppointmentRecord(updated),
          RETRY,
        );

        await publishEvent(ctx, buildAppointmentEvent({
          type: "appointment.updated",
          appointment: updated,
          occurredAt: now,
          payload: { before: appointmentSnapshot(existing) },
        }));

        updated = await persistReminderSchedule(ctx, updated, now);
        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(updated));
        return updated;
      },
    ),

    cancel: restate.createObjectHandler(
      { output: restate.serde.schema(AppointmentState) },
      async (ctx: restate.ObjectContext) => {
        const existing = await requireAppointment(ctx);
        if (existing.status === "cancelled") {
          ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(existing));
          return existing;
        }

        const now = await ctx.date.toJSON();

        let cancelled = appendHistory(
          markPendingRemindersCancelled(
            {
              ...existing,
              version: existing.version + 1,
              status: "cancelled",
              cancelledAt: now,
              updatedAt: now,
            },
            now,
            "appointment cancelled",
          ),
          {
            type: "cancelled",
            at: now,
            details: { before: appointmentSnapshot(existing) },
          },
        );

        cancelled = await ctx.run(
          "persist cancellation",
          () => replaceAppointmentRecord(cancelled),
          RETRY,
        );

        await publishEvent(ctx, buildAppointmentEvent({
          type: "appointment.cancelled",
          appointment: cancelled,
          occurredAt: now,
        }));

        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(cancelled));
        return cancelled;
      },
    ),

    sendReminder: restate.createObjectHandler(
      {
        input: restate.serde.schema(SendReminderInput),
      },
      async (ctx: restate.ObjectContext, input: SendReminderInputType) => {
        const appointment = await loadAppointment(ctx);
        if (!appointment) return;
        if (appointment.version !== input.version) return;
        if (appointment.status === "cancelled") return;
        if (appointment.reminders[input.reminder].sent) return;

        const now = await ctx.date.toJSON();
        const payload = toEmailPayload(appointment);

        const result = await ctx.run(
          `enqueue ${input.reminder} email`,
          () => enqueueImmediateEmail({
            reminder: input.reminder,
            scheduledFor: input.scheduledFor,
            appointment: payload,
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
          type: "reminder_scheduled",
          at: now,
          version: appointment.version,
          reminder: input.reminder,
          jobId: result.jobId,
          details: { scheduledFor: input.scheduledFor },
        });

        await ctx.run(
          "persist queued",
          () => replaceAppointmentRecord(appointment),
          RETRY,
        );

        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
      },
    ),

    startReminderDelivery: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryRequest),
        output: restate.serde.schema(ReminderDeliveryStartResult),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await requireAppointment(ctx);
        const now = await ctx.date.toJSON();
        const stale = validateReminderJob(appointment, input);
        if (stale) return { shouldSend: false, reason: stale };

        appointment.reminders[input.reminder] = {
          ...appointment.reminders[input.reminder],
          scheduled: false,
          jobId: input.jobId,
          startedAt: now,
          error: undefined,
        };
        appointment.updatedAt = now;
        appointment.history.push({
          type: "reminder_started",
          at: now,
          version: appointment.version,
          reminder: input.reminder,
          jobId: input.jobId,
        });

        await ctx.run(
          "persist delivery start",
          () => replaceAppointmentRecord(appointment),
          RETRY,
        );

        ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
        return { shouldSend: true };
      },
    ),

    recordReminderResult: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryResultInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        const appointment = await requireAppointment(ctx);
        const now = await ctx.date.toJSON();
        const stale = validateReminderJob(appointment, input);
        if (stale) return appointment;

        const r = appointment.reminders[input.reminder];

        switch (input.result.status) {
          case "sent":
            appointment.reminders[input.reminder] = { ...r, sent: true, scheduled: false, jobId: input.jobId, sentAt: now, error: undefined };
            appointment.history.push({ type: "reminder_sent", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId });
            appointment.updatedAt = now;
            await ctx.run("persist sent", () => replaceAppointmentRecord(appointment), RETRY);
            await publishEvent(ctx, buildReminderSentEvent({ appointment, reminder: input.reminder, occurredAt: now, jobId: input.jobId }));
            ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
            return appointment;

          case "skipped":
            appointment.reminders[input.reminder] = { ...r, sent: false, scheduled: false, jobId: input.jobId, skippedAt: now, error: input.result.reason };
            appointment.history.push({ type: "reminder_skipped", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId, details: { reason: input.result.reason, statusCode: input.result.statusCode, responseBody: input.result.responseBody } });
            appointment.updatedAt = now;
            await ctx.run("persist skipped", () => replaceAppointmentRecord(appointment), RETRY);
            ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
            return appointment;

          case "failed":
            appointment.reminders[input.reminder] = { ...r, sent: false, scheduled: false, jobId: input.jobId, failedAt: now, error: input.result.error };
            appointment.history.push({ type: "reminder_failed", at: now, version: appointment.version, reminder: input.reminder, jobId: input.jobId, details: { error: input.result.error } });
            appointment.updatedAt = now;
            await ctx.run("persist failed", () => replaceAppointmentRecord(appointment), RETRY);
            ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
            return appointment;
        }
      },
    ),
  },
});

async function loadAppointment(ctx: restate.ObjectContext) {
  return ctx.run("load", () => findAppointmentById(ctx.key), RETRY);
}

async function requireAppointment(ctx: restate.ObjectContext) {
  const appointment = await loadAppointment(ctx);
  if (!appointment) {
    throw new restate.TerminalError(`Appointment ${ctx.key} does not exist`);
  }
  return appointment;
}

async function persistReminderSchedule(
  ctx: restate.ObjectContext,
  appointment: AppointmentStateType,
  now: string,
) {
  const result = scheduleRemindersViaRestate(ctx, appointment, now);

  const updated = applyScheduleResult(appointment, result);
  updated.updatedAt = now;

  return ctx.run("persist reminders", () => replaceAppointmentRecord(updated), RETRY);
}

function scheduleRemindersViaRestate(
  ctx: restate.ObjectContext,
  appointment: AppointmentStateType,
  now: string,
): ReminderScheduleResult {
  const reminders = emptyReminderStatus(appointment.version);
  const events: ReminderScheduleResult["events"] = [];
  const nowMs = new Date(now).getTime();

  for (const reminder of REMINDER_TYPES) {
    const targetMs = reminderTargetMs(reminder, appointment.startAt);
    const scheduledFor = new Date(targetMs).toISOString();

    if (targetMs <= nowMs) {
      reminders[reminder] = {
        version: appointment.version,
        sent: false,
        scheduled: false,
        skippedAt: now,
        scheduledFor,
        error: "scheduled time already passed",
      };
      events.push({
        type: "reminder_skipped",
        at: now,
        version: appointment.version,
        reminder,
        details: { reason: "scheduled time already passed", scheduledFor },
      });
      continue;
    }

    const delayMs = targetMs - nowMs;

    ctx
      .objectSendClient<AppointmentObject>({ name: "Appointment" }, ctx.key)
      .sendReminder(
        { reminder, version: appointment.version, scheduledFor },
        restate.rpc.sendOpts({ delay: delayMs }),
      );

    reminders[reminder] = {
      version: appointment.version,
      sent: false,
      scheduled: true,
      scheduledAt: now,
      scheduledFor,
    };
    events.push({
      type: "reminder_scheduled",
      at: now,
      version: appointment.version,
      reminder,
      details: { scheduledFor },
    });
  }

  return { reminders, events };
}

function validateReminderJob(
  appointment: AppointmentStateType,
  input: { reminder: ReminderType; version: number; jobId: string },
) {
  if (appointment.version !== input.version) return `stale version ${input.version}`;
  if (appointment.status === "cancelled") return "appointment cancelled";
  const r = appointment.reminders[input.reminder];
  if (r.sent) return "already sent";
  if (r.jobId && r.jobId !== input.jobId) return `stale job ${input.jobId}`;
  return undefined;
}

async function publishEvent(
  ctx: restate.ObjectContext,
  event: Parameters<typeof publishAppointmentEvent>[0],
) {
  await ctx.run(`publish ${event.type}`, () => publishAppointmentEvent(event), RETRY);
}

export function toRecordableFailure(error: unknown) {
  return { status: "failed" as const, error: errorMessage(error) };
}

export type AppointmentObject = typeof appointmentObject;
