import { Queue, type JobsOptions } from "bullmq";
import { z } from "zod";

import { env, readPositiveIntEnv } from "../config/env.js";
import type { AppointmentEmailPayload } from "../models/appointment.model.js";
import { createRedisConnection } from "./redis.service.js";

export const EMAIL_QUEUE_NAME = env.emailQueueName;

export const EmailReminderType = z.enum(["before", "atTime", "after"]);
export type EmailReminderType = z.infer<typeof EmailReminderType>;

export const AppointmentEmailJobData = z.object({
  reminder: EmailReminderType,
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

const defaultJobOptions: JobsOptions = {
  attempts: readPositiveIntEnv("EMAIL_JOB_ATTEMPTS", 3),
  backoff: {
    type: "exponential",
    delay: readPositiveIntEnv("EMAIL_JOB_BACKOFF_MS", 2_000),
  },
  removeOnComplete: {
    age: readPositiveIntEnv("EMAIL_JOB_REMOVE_COMPLETE_AGE_SECONDS", 86_400),
    count: readPositiveIntEnv("EMAIL_JOB_REMOVE_COMPLETE_COUNT", 10_000),
  },
  removeOnFail: {
    age: readPositiveIntEnv("EMAIL_JOB_REMOVE_FAIL_AGE_SECONDS", 604_800),
  },
};

export const appointmentEmailQueue = new Queue<AppointmentEmailJobData>(
  EMAIL_QUEUE_NAME,
  {
    connection: createRedisConnection(),
    defaultJobOptions,
  },
);

export async function enqueueAppointmentEmail(input: {
  reminder: EmailReminderType;
  appointment: AppointmentEmailPayload;
}) {
  const data = AppointmentEmailJobData.parse(input);
  const jobId = appointmentEmailJobId(
    data.appointment.id,
    data.appointment.version,
    data.reminder,
  );
  const job = await appointmentEmailQueue.add("send", data, { jobId });

  return {
    jobId: job.id ?? jobId,
  };
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
