import { Queue, type JobsOptions } from "bullmq";

import { env } from "./env.js";
import { createRedisConnection } from "./redis.js";
import type { AppointmentEmailJobData } from "../services/queue/email-queue.service.js";
import type { MaintenanceJobData } from "../services/queue/maintenance-queue.service.js";

export const EMAIL_QUEUE_NAME = env.emailQueueName;

const defaultEmailJobOptions: JobsOptions = {
  attempts: env.emailJobAttempts,
  backoff: {
    type: "exponential",
    delay: env.emailJobBackoffMs,
  },
  removeOnComplete: {
    age: env.emailJobRemoveCompleteAgeSeconds,
    count: env.emailJobRemoveCompleteCount,
  },
  removeOnFail: {
    age: env.emailJobRemoveFailAgeSeconds,
  },
};

export const appointmentEmailQueue = new Queue<AppointmentEmailJobData>(
  EMAIL_QUEUE_NAME,
  {
    connection: createRedisConnection(),
    defaultJobOptions: defaultEmailJobOptions,
  },
);

export const MAINTENANCE_QUEUE_NAME = env.maintenanceQueueName;

export const maintenanceQueue = new Queue<MaintenanceJobData>(
  MAINTENANCE_QUEUE_NAME,
  {
    connection: createRedisConnection(),
    defaultJobOptions: {
      attempts: 3,
      backoff: {
        type: "exponential",
        delay: 5_000,
      },
      removeOnComplete: {
        age: env.emailJobRemoveCompleteAgeSeconds,
        count: 1_000,
      },
      removeOnFail: {
        age: env.emailJobRemoveFailAgeSeconds,
      },
    },
  },
);
