import { Queue } from "bullmq";
import { z } from "zod";

import { env } from "../../config/env.js";
import { createRedisConnection } from "../db/redis.service.js";

export const MAINTENANCE_QUEUE_NAME = env.maintenanceQueueName;

export const MaintenanceJobData = z.object({
  type: z.literal("queue-cleanup"),
});

export type MaintenanceJobData = z.infer<typeof MaintenanceJobData>;

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

export async function upsertCleanupScheduler() {
  return maintenanceQueue.upsertJobScheduler(
    "queue-cleanup",
    { every: env.cleanupEveryMs },
    {
      name: "cleanup",
      data: { type: "queue-cleanup" },
    },
  );
}

export async function closeMaintenanceQueue() {
  await maintenanceQueue.close();
}
