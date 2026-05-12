import { z } from "zod";

import { env } from "../../config/env.js";
import {
  appointmentEmailQueue,
  EMAIL_QUEUE_NAME,
} from "../../config/queues.js";
import type {
  AppointmentEmailPayload,
  ReminderType,
} from "../../models/appointment.model.js";
import { logger } from "../../utils/logger.js";

export { EMAIL_QUEUE_NAME, appointmentEmailQueue };

export const AppointmentEmailJobData = z.object({
  reminder: z.enum(["before", "atTime", "after"]),
  scheduledFor: z.string().datetime(),
  appointment: z.object({
    id: z.string().min(1),
    version: z.number().int().min(1),
    customerName: z.string().min(1),
    customerEmail: z.string().email(),
    service: z.string().min(1),
    startAt: z.string().datetime(),
    note: z.string().optional(),
  }),
});

export type AppointmentEmailJobData = z.infer<typeof AppointmentEmailJobData>;
type EmailReminderType = AppointmentEmailJobData["reminder"];

export async function enqueueAppointmentEmail(input: {
  reminder: ReminderType;
  appointment: AppointmentEmailPayload;
  scheduledFor: string;
  jobId?: string;
}) {
  const data = AppointmentEmailJobData.parse({
    reminder: input.reminder,
    scheduledFor: input.scheduledFor,
    appointment: input.appointment,
  });
  const jobId =
    input.jobId ??
    appointmentEmailJobId(
      data.appointment.id,
      data.appointment.version,
      data.reminder,
    );
  const job = await appointmentEmailQueue.add("send", data, { jobId });

  return {
    jobId: job.id ?? jobId,
  };
}

export async function removeAppointmentEmailJob(jobId: string) {
  const job = await appointmentEmailQueue.getJob(jobId);
  if (!job) {
    return { removed: false, reason: "job not found" };
  }

  try {
    await job.remove();
    return { removed: true };
  } catch (error) {
    logger.warn(
      { jobId, error: error instanceof Error ? error.message : String(error) },
      "Unable to remove reminder job",
    );
    return { removed: false, reason: "job already active or completed" };
  }
}

export function appointmentEmailJobId(
  appointmentId: string,
  version: number,
  reminder: EmailReminderType,
) {
  const encodedAppointmentId = Buffer.from(appointmentId).toString("base64url");
  return `appointment-email-${encodedAppointmentId}-${version}-${reminder}`;
}

export async function closeAppointmentEmailQueue() {
  await appointmentEmailQueue.close();
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

export function delayUntil(targetMs: number, nowMs: number) {
  return Math.max(0, targetMs - nowMs);
}
