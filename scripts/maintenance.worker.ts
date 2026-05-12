import "dotenv/config";
import { Worker, type Job } from "bullmq";

import { env } from "../config/env.js";
import { appointmentEmailQueue } from "../services/queue/email-queue.service.js";
import {
  closeMaintenanceQueue,
  MAINTENANCE_QUEUE_NAME,
  MaintenanceJobData,
  upsertCleanupScheduler,
} from "../services/queue/maintenance-queue.service.js";
import { createRedisConnection } from "../config/redis.js";
import { logger } from "../utils/logger.js";

const connection = createRedisConnection();

await upsertCleanupScheduler();

const worker = new Worker<MaintenanceJobData>(
  MAINTENANCE_QUEUE_NAME,
  processMaintenanceJob,
  {
    connection,
    concurrency: 1,
  },
);

worker.on("completed", (job) => {
  logger.info({ jobId: job.id, name: job.name }, "Maintenance job completed");
});

worker.on("failed", (job, error) => {
  logger.error(
    { jobId: job?.id, name: job?.name, error: error.message },
    "Maintenance job failed",
  );
});

logger.info(
  { queue: MAINTENANCE_QUEUE_NAME, everyMs: env.cleanupEveryMs },
  "Maintenance worker started",
);

async function processMaintenanceJob(job: Job<MaintenanceJobData>) {
  const data = MaintenanceJobData.parse(job.data);

  switch (data.type) {
    case "queue-cleanup":
      return cleanupQueues();
  }
}

async function cleanupQueues() {
  const [completedEmailJobs, failedEmailJobs] = await Promise.all([
    appointmentEmailQueue.clean(
      env.cleanupGraceMs,
      env.cleanupLimit,
      "completed",
    ),
    appointmentEmailQueue.clean(env.cleanupGraceMs, env.cleanupLimit, "failed"),
  ]);

  return {
    completedEmailJobs: completedEmailJobs.length,
    failedEmailJobs: failedEmailJobs.length,
  };
}

async function shutdown(signal: NodeJS.Signals) {
  logger.info({ signal }, "Stopping maintenance worker");
  await worker.close();
  await closeMaintenanceQueue();
  await appointmentEmailQueue.close();
  await connection.quit();
}

process.once("SIGINT", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

process.once("SIGTERM", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});
