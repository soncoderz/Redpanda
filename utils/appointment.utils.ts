import { createHash } from "node:crypto";

import { env } from "../config/env.js";
import type {
  AppointmentEmailPayload,
  AppointmentEventEnvelope,
  AppointmentEventType,
  AppointmentHistoryEntry,
  AppointmentState,
  ReminderType,
} from "../models/appointment.model.js";

export const WORKFLOW_STATE_KEY = "appointment-workflow";
export const REMINDER_TYPES: ReminderType[] = ["before", "atTime", "after"];

export interface WorkflowState {
  id: string;
  version: number;
  status: AppointmentState["status"];
  updatedAt: string;
}

export interface ReminderScheduleResult {
  reminders: AppointmentState["reminders"];
  events: AppointmentHistoryEntry[];
}

export function appointmentIdFromIdempotencyKey(idempotencyKey: string) {
  const digest = createHash("sha256").update(idempotencyKey).digest("hex");
  return `appt_${digest.slice(0, 32)}`;
}

export function emptyReminderStatus(
  version: number,
): AppointmentState["reminders"] {
  return {
    before: emptyReminderDeliveryStatus(version),
    atTime: emptyReminderDeliveryStatus(version),
    after: emptyReminderDeliveryStatus(version),
  };
}

export function emptyReminderDeliveryStatus(version: number) {
  return {
    version,
    sent: false,
    scheduled: false,
  };
}

export function appendHistory(
  appointment: AppointmentState,
  event: Omit<AppointmentHistoryEntry, "version">,
) {
  appointment.history.push({
    ...event,
    version: appointment.version,
  });

  return appointment;
}

export function appointmentSnapshot(appointment: AppointmentState) {
  return {
    id: appointment.id,
    version: appointment.version,
    customerName: appointment.customerName,
    customerEmail: appointment.customerEmail,
    service: appointment.service,
    startAt: appointment.startAt,
    note: appointment.note,
    status: appointment.status,
    createdAt: appointment.createdAt,
    updatedAt: appointment.updatedAt,
    cancelledAt: appointment.cancelledAt,
  };
}

export function toEmailPayload(
  appointment: AppointmentState,
): AppointmentEmailPayload {
  return {
    id: appointment.id,
    version: appointment.version,
    customerName: appointment.customerName,
    customerEmail: appointment.customerEmail,
    service: appointment.service,
    startAt: appointment.startAt,
    note: appointment.note,
  };
}

export function toWorkflowState(appointment: AppointmentState): WorkflowState {
  return {
    id: appointment.id,
    version: appointment.version,
    status: appointment.status,
    updatedAt: appointment.updatedAt,
  };
}

export function applyScheduleResult(
  appointment: AppointmentState,
  scheduleResult: ReminderScheduleResult,
) {
  appointment.reminders = scheduleResult.reminders;
  appointment.history.push(...scheduleResult.events);
  return appointment;
}

export function markPendingRemindersCancelled(
  appointment: AppointmentState,
  now: string,
  reason: string,
) {
  for (const reminder of REMINDER_TYPES) {
    const status = appointment.reminders[reminder];
    if (!status.scheduled || status.sent) {
      continue;
    }

    appointment.reminders[reminder] = {
      ...status,
      version: appointment.version,
      scheduled: false,
      canceledAt: now,
      error: reason,
    };
    appointment.history.push({
      type: "reminder_cancelled",
      at: now,
      version: appointment.version,
      reminder,
      jobId: status.jobId,
      details: { reason },
    });
  }

  return appointment;
}

export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

export function buildAppointmentEvent(input: {
  type: AppointmentEventType;
  appointment: AppointmentState;
  occurredAt: string;
  payload?: Record<string, unknown>;
}): AppointmentEventEnvelope {
  return {
    eventId: `${input.type}:${input.appointment.id}:${input.appointment.version}`,
    type: input.type,
    appointmentId: input.appointment.id,
    version: input.appointment.version,
    occurredAt: input.occurredAt,
    payload: {
      appointment: appointmentSnapshot(input.appointment),
      ...input.payload,
    },
  };
}

export function buildReminderSentEvent(input: {
  appointment: AppointmentState;
  reminder: ReminderType;
  occurredAt: string;
  jobId: string;
}): AppointmentEventEnvelope {
  return {
    eventId: `reminder.sent:${input.appointment.id}:${input.appointment.version}:${input.reminder}`,
    type: "reminder.sent",
    appointmentId: input.appointment.id,
    version: input.appointment.version,
    occurredAt: input.occurredAt,
    payload: {
      appointment: appointmentSnapshot(input.appointment),
      reminder: input.reminder,
      jobId: input.jobId,
    },
  };
}

export function reminderTargetMs(reminder: ReminderType, startAt: string) {
  const startMs = new Date(startAt).getTime();
  switch (reminder) {
    case "before":
      return startMs - env.reminderBeforeMs;
    case "atTime":
      return startMs;
    case "after":
      return startMs + env.reminderAfterMs;
  }
}
