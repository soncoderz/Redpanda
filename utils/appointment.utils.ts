import type * as restate from "@restatedev/restate-sdk";
import type {
  AppointmentState,
  AppointmentEmailPayload,
} from "../models/appointment.model.js";

type ReminderType = "before" | "atTime" | "after";

export const STATE_KEY = "appointment";
export const ONE_MINUTE_MS = 60_000;
export const REMINDER_TYPES: ReminderType[] = ["before", "atTime", "after"];

export function emptyScheduleResult(version: number) {
  return {
    reminderInvocations: emptyReminderInvocations(),
    emailStatus: emptyEmailStatus(version),
    events: [] as AppointmentState["history"],
  };
}

export function emptyReminderInvocations() {
  return {
    before: "",
    atTime: "",
    after: "",
  };
}

export function emptyEmailStatus(version: number): AppointmentState["emailStatus"] {
  return {
    before: emptyEmailDeliveryStatus(version),
    atTime: emptyEmailDeliveryStatus(version),
    after: emptyEmailDeliveryStatus(version),
  };
}

export function emptyEmailDeliveryStatus(version: number) {
  return {
    version,
    sent: false,
    scheduled: false,
  };
}

export function appendHistory(
  appointment: AppointmentState,
  event: Omit<AppointmentState["history"][number], "version">,
) {
  appointment.history.push({
    ...event,
    version: appointment.version,
  });
  return appointment;
}

export function applyScheduleResult(
  appointment: AppointmentState,
  scheduleResult: ReturnType<typeof emptyScheduleResult>,
) {
  appointment.reminderInvocations = scheduleResult.reminderInvocations;
  appointment.emailStatus = scheduleResult.emailStatus;
  appointment.history.push(...scheduleResult.events);
}

export function normalizeAppointment(appointment: AppointmentState) {
  const version = appointment.version ?? 1;
  return {
    ...appointment,
    version,
    reminderInvocations: appointment.reminderInvocations ?? emptyReminderInvocations(),
    emailStatus: appointment.emailStatus ?? emptyEmailStatus(version),
    history: appointment.history ?? [],
  };
}

export function toEmailPayload(appointment: AppointmentState): AppointmentEmailPayload {
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
    arrivedAt: appointment.arrivedAt,
  };
}

export function saveStaleEmailSkipped(
  ctx: restate.ObjectContext,
  appointment: AppointmentState,
  reminder: ReminderType,
  now: string,
  reason: string,
) {
  appointment.history.push({
    type: "email_skipped",
    at: now,
    version: appointment.version,
    reminder,
    details: { reason },
  });
  appointment.updatedAt = now;

  ctx.set(STATE_KEY, appointment);
  return appointment;
}

export function saveEmailSkipped(
  ctx: restate.ObjectContext,
  appointment: AppointmentState,
  reminder: ReminderType,
  now: string,
  reason: string,
  details: Record<string, unknown> = {},
) {
  appointment.emailStatus[reminder] = {
    ...appointment.emailStatus[reminder],
    sent: false,
    scheduled: false,
    skippedAt: now,
    error: reason,
  };
  appointment.history.push({
    type: "email_skipped",
    at: now,
    version: appointment.version,
    reminder,
    invocationId: appointment.emailStatus[reminder].invocationId,
    details: { reason, ...details },
  });
  appointment.updatedAt = now;

  ctx.set(STATE_KEY, appointment);
  return appointment;
}

export function reminderTargetMs(reminder: ReminderType, startAt: string) {
  const startMs = new Date(startAt).getTime();
  switch (reminder) {
    case "before":
      return startMs - ONE_MINUTE_MS;
    case "atTime":
      return startMs;
    case "after":
      return startMs + ONE_MINUTE_MS;
  }
}

export function delayUntil(targetMs: number, nowMs: number) {
  return Math.max(0, targetMs - nowMs);
}

export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
