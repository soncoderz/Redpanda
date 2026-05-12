import { Queue, type JobsOptions } from "bullmq";

import { env } from "./env.js";
import { createRedisConnection } from "./redis.js";
import type { AppointmentEmailJobData } from "../services/queue/email-queue.service.js";
import type { MaintenanceJobData } from "../services/queue/maintenance-queue.service.js";

/** Tên queue email — dùng để tạo queue và worker cùng tên */
export const EMAIL_QUEUE_NAME = env.emailQueueName;

/** Cấu hình mặc định cho email job: retry, backoff, tự xóa khi hoàn thành/thất bại */
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

/** BullMQ queue cho email nhắc lịch hẹn — kết nối Redis */
export const appointmentEmailQueue = new Queue<AppointmentEmailJobData>(
  EMAIL_QUEUE_NAME,
  {
    connection: createRedisConnection(),
    defaultJobOptions: defaultEmailJobOptions,
  },
);

/** Tên queue bảo trì — dọn dẹp job cũ định kỳ */
export const MAINTENANCE_QUEUE_NAME = env.maintenanceQueueName;

/** BullMQ queue cho job bảo trì — kết nối Redis */
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
