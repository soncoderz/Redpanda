import * as restate from "@restatedev/restate-sdk";

import {
  AppointmentState,
  CreateAppointmentWorkflowInput,
  ReminderDeliveryRequest,
  ReminderDeliveryResultInput,
  ReminderDeliveryStartResult,
  UpdateAppointmentWorkflowInput,
  type AppointmentState as AppointmentStateType,
  type ReminderDeliveryRequest as ReminderDeliveryRequestType,
  type ReminderDeliveryResultInput as ReminderDeliveryResultInputType,
  type ReminderType,
} from "../models/appointment.model.js";
import {
  applyAppointmentPatch,
  createAppointmentRecord,
  findAppointmentById,
  replaceAppointmentRecord,
} from "../repositories/appointment.repository.js";
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
  toEmailPayload,
  toWorkflowState,
  WORKFLOW_STATE_KEY,
  type ReminderScheduleResult,
} from "../utils/appointment.utils.js";
import {
  removeAppointmentEmailJob,
  scheduleAppointmentEmail,
} from "./email-queue.service.js";
import { publishAppointmentEvent } from "./event-publisher.service.js";

const SIDE_EFFECT_RETRY = { maxRetryAttempts: 5 };

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
          {
            type: "created",
            at: now,
            details: { appointment: input },
          },
        );

        appointment = await ctx.run(
          "persist appointment",
          () => createAppointmentRecord(appointment),
          SIDE_EFFECT_RETRY,
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
        const appointment = await loadAppointment(ctx);
        if (!appointment) {
          throw new restate.TerminalError(
            `Appointment ${ctx.key} does not exist`,
          );
        }

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
        await cancelReminderJobs(ctx, existing);

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
          "persist appointment update",
          () => replaceAppointmentRecord(updated),
          SIDE_EFFECT_RETRY,
        );

        await publishEvent(ctx, buildAppointmentEvent({
          type: "appointment.updated",
          appointment: updated,
          occurredAt: now,
          payload: {
            before: appointmentSnapshot(existing),
          },
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
        await cancelReminderJobs(ctx, existing);

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
          "persist appointment cancellation",
          () => replaceAppointmentRecord(cancelled),
          SIDE_EFFECT_RETRY,
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

    startReminderDelivery: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryRequest),
        output: restate.serde.schema(ReminderDeliveryStartResult),
      },
      async (ctx: restate.ObjectContext, input) => {
        return startReminderDelivery(ctx, input);
      },
    ),

    recordReminderResult: restate.createObjectHandler(
      {
        input: restate.serde.schema(ReminderDeliveryResultInput),
        output: restate.serde.schema(AppointmentState),
      },
      async (ctx: restate.ObjectContext, input) => {
        return recordReminderResult(ctx, input);
      },
    ),
  },
});

async function loadAppointment(ctx: restate.ObjectContext) {
  return ctx.run(
    "load appointment",
    () => findAppointmentById(ctx.key),
    SIDE_EFFECT_RETRY,
  );
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
  const scheduleResult = await ctx.run(
    "schedule reminder jobs",
    () => scheduleReminderJobs(appointment, now),
    SIDE_EFFECT_RETRY,
  );

  const updated = applyScheduleResult(appointment, scheduleResult);
  updated.updatedAt = now;

  return ctx.run(
    "persist reminder metadata",
    () => replaceAppointmentRecord(updated),
    SIDE_EFFECT_RETRY,
  );
}

async function scheduleReminderJobs(
  appointment: AppointmentStateType,
  now: string,
): Promise<ReminderScheduleResult> {
  const reminders = emptyReminderStatus(appointment.version);
  const events: ReminderScheduleResult["events"] = [];
  const payload = toEmailPayload(appointment);

  for (const reminder of REMINDER_TYPES) {
    const result = await scheduleAppointmentEmail({
      reminder,
      appointment: payload,
      now,
    });

    if (result.scheduled) {
      reminders[reminder] = {
        version: appointment.version,
        sent: false,
        scheduled: true,
        jobId: result.jobId,
        scheduledAt: now,
        scheduledFor: result.scheduledFor,
      };
      events.push({
        type: "reminder_scheduled",
        at: now,
        version: appointment.version,
        reminder,
        jobId: result.jobId,
        details: { scheduledFor: result.scheduledFor },
      });
      continue;
    }

    reminders[reminder] = {
      version: appointment.version,
      sent: false,
      scheduled: false,
      skippedAt: now,
      scheduledFor: result.scheduledFor,
      error: result.reason,
    };
    events.push({
      type: "reminder_skipped",
      at: now,
      version: appointment.version,
      reminder,
      details: {
        reason: result.reason,
        scheduledFor: result.scheduledFor,
      },
    });
  }

  return { reminders, events };
}

async function cancelReminderJobs(
  ctx: restate.ObjectContext,
  appointment: AppointmentStateType,
) {
  const jobIds = REMINDER_TYPES.map((reminder) => appointment.reminders[reminder])
    .filter((status) => status.scheduled && !status.sent)
    .map((status) => status.jobId)
    .filter((jobId): jobId is string => Boolean(jobId));

  if (jobIds.length === 0) {
    return;
  }

  await ctx.run(
    "cancel reminder jobs",
    async () => {
      await Promise.all(jobIds.map((jobId) => removeAppointmentEmailJob(jobId)));
    },
    SIDE_EFFECT_RETRY,
  );
}

async function startReminderDelivery(
  ctx: restate.ObjectContext,
  input: ReminderDeliveryRequestType,
) {
  const appointment = await requireAppointment(ctx);
  const now = await ctx.date.toJSON();
  const stale = validateReminderJob(appointment, input);
  if (stale) {
    return { shouldSend: false, reason: stale };
  }

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
    "persist reminder delivery start",
    () => replaceAppointmentRecord(appointment),
    SIDE_EFFECT_RETRY,
  );

  ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
  return { shouldSend: true };
}

async function recordReminderResult(
  ctx: restate.ObjectContext,
  input: ReminderDeliveryResultInputType,
) {
  const appointment = await requireAppointment(ctx);
  const now = await ctx.date.toJSON();
  const stale = validateReminderJob(appointment, input);
  if (stale) {
    return appointment;
  }

  switch (input.result.status) {
    case "sent":
      appointment.reminders[input.reminder] = {
        ...appointment.reminders[input.reminder],
        sent: true,
        scheduled: false,
        jobId: input.jobId,
        sentAt: now,
        error: undefined,
      };
      appointment.history.push({
        type: "reminder_sent",
        at: now,
        version: appointment.version,
        reminder: input.reminder,
        jobId: input.jobId,
      });
      appointment.updatedAt = now;

      await ctx.run(
        "persist reminder sent",
        () => replaceAppointmentRecord(appointment),
        SIDE_EFFECT_RETRY,
      );

      await publishEvent(ctx, buildReminderSentEvent({
        appointment,
        reminder: input.reminder,
        occurredAt: now,
        jobId: input.jobId,
      }));
      ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
      return appointment;

    case "skipped":
      return saveReminderSkipped(
        ctx,
        appointment,
        input.reminder,
        input.jobId,
        now,
        input.result.reason,
        {
          statusCode: input.result.statusCode,
          responseBody: input.result.responseBody,
        },
      );

    case "failed":
      appointment.reminders[input.reminder] = {
        ...appointment.reminders[input.reminder],
        sent: false,
        scheduled: false,
        jobId: input.jobId,
        failedAt: now,
        error: input.result.error,
      };
      appointment.history.push({
        type: "reminder_failed",
        at: now,
        version: appointment.version,
        reminder: input.reminder,
        jobId: input.jobId,
        details: { error: input.result.error },
      });
      appointment.updatedAt = now;

      await ctx.run(
        "persist reminder failed",
        () => replaceAppointmentRecord(appointment),
        SIDE_EFFECT_RETRY,
      );
      ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(appointment));
      return appointment;
  }
}

async function saveReminderSkipped(
  ctx: restate.ObjectContext,
  appointment: AppointmentStateType,
  reminder: ReminderType,
  jobId: string,
  now: string,
  reason: string,
  details: Record<string, unknown> = {},
) {
  appointment.reminders[reminder] = {
    ...appointment.reminders[reminder],
    sent: false,
    scheduled: false,
    jobId,
    skippedAt: now,
    error: reason,
  };
  appointment.history.push({
    type: "reminder_skipped",
    at: now,
    version: appointment.version,
    reminder,
    jobId,
    details: { reason, ...details },
  });
  appointment.updatedAt = now;

  const updated = await ctx.run(
    "persist reminder skipped",
    () => replaceAppointmentRecord(appointment),
    SIDE_EFFECT_RETRY,
  );
  ctx.set(WORKFLOW_STATE_KEY, toWorkflowState(updated));

  return updated;
}

function validateReminderJob(
  appointment: AppointmentStateType,
  input: {
    reminder: ReminderType;
    version: number;
    jobId: string;
  },
) {
  if (appointment.version !== input.version) {
    return `stale reminder job version ${input.version}`;
  }

  if (appointment.status === "cancelled") {
    return "appointment cancelled";
  }

  const reminder = appointment.reminders[input.reminder];
  if (reminder.sent) {
    return "reminder already sent";
  }

  if (reminder.jobId && reminder.jobId !== input.jobId) {
    return `stale reminder job ${input.jobId}`;
  }

  return undefined;
}

async function publishEvent(
  ctx: restate.ObjectContext,
  event: Parameters<typeof publishAppointmentEvent>[0],
) {
  await ctx.run(
    `publish ${event.type}`,
    () => publishAppointmentEvent(event),
    SIDE_EFFECT_RETRY,
  );
}

export function toRecordableFailure(error: unknown) {
  return {
    status: "failed" as const,
    error: errorMessage(error),
  };
}

export type AppointmentObject = typeof appointmentObject;
