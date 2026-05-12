import { z } from "zod";

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

export async function enqueueImmediateEmail(input: {
  reminder: ReminderType;
  scheduledFor: string;
  appointment: AppointmentEmailPayload;
}) {
  const data = AppointmentEmailJobData.parse({
    reminder: input.reminder,
    scheduledFor: input.scheduledFor,
    appointment: input.appointment,
  });
  const jobId = appointmentEmailJobId(
    data.appointment.id,
    data.appointment.version,
    data.reminder,
  );
  const job = await appointmentEmailQueue.add("send", data, { jobId });

  return { jobId: job.id ?? jobId };
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
