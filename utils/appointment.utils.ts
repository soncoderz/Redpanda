import { createHash } from "node:crypto";

import { env } from "../config/env.js";
import type { ReminderType } from "../models/appointment.model.js";

export const REMINDER_TYPES: ReminderType[] = ["before", "atTime", "after"];

export function appointmentIdFromIdempotencyKey(idempotencyKey: string) {
  const digest = createHash("sha256").update(idempotencyKey).digest("hex");
  return `appt_${digest.slice(0, 32)}`;
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
