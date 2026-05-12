import "dotenv/config";
import { Worker, type Job } from "bullmq";

import { env } from "../config/env.js";
import type { AppointmentObject } from "../services/appointment/appointment.service.js";
import {
  AppointmentEmailJobData,
  closeAppointmentEmailQueue,
  EMAIL_QUEUE_NAME,
} from "../services/queue/email-queue.service.js";
import {
  sendAppointmentEmail,
  type EmailSendResult,
} from "../services/mail/mailer.service.js";
import { createRedisConnection } from "../services/db/redis.service.js";
import { restateClient } from "../services/restate/restate.service.js";
import { logger } from "../utils/logger.js";

const connection = createRedisConnection();

const worker = new Worker<AppointmentEmailJobData>(
  EMAIL_QUEUE_NAME,
  processEmailJob,
  {
    connection,
    concurrency: env.emailWorkerConcurrency,
    limiter: {
      max: env.emailRateMax,
      duration: env.emailRateDurationMs,
    },
  },
);

worker.on("completed", (job) => {
  logger.info(
    {
      jobId: job.id,
      reminder: job.data.reminder,
      appointmentId: job.data.appointment.id,
      version: job.data.appointment.version,
    },
    "Appointment email job completed",
  );
});

worker.on("failed", (job, error) => {
  logger.error(
    {
      jobId: job?.id,
      reminder: job?.data.reminder,
      appointmentId: job?.data.appointment.id,
      version: job?.data.appointment.version,
      attemptsMade: job?.attemptsMade,
      error: error.message,
    },
    "Appointment email job failed",
  );
});

logger.info(
  {
    queue: EMAIL_QUEUE_NAME,
    restateRuntimeUrl: env.restateRuntimeUrl,
    workerConcurrency: env.emailWorkerConcurrency,
    rateLimitMax: env.emailRateMax,
    rateLimitDurationMs: env.emailRateDurationMs,
  },
  "Appointment email worker started",
);

async function processEmailJob(job: Job<AppointmentEmailJobData>) {
  const data = AppointmentEmailJobData.parse(job.data);
  const appointmentClient = restateClient.objectClient<AppointmentObject>(
    { name: "Appointment" },
    data.appointment.id,
  );
  const jobId = job.id ?? "";

  const delivery = await appointmentClient.startReminderDelivery({
    reminder: data.reminder,
    version: data.appointment.version,
    jobId,
  });

  if (!delivery.shouldSend) {
    logger.info(
      {
        jobId,
        reminder: data.reminder,
        appointmentId: data.appointment.id,
        version: data.appointment.version,
        reason: delivery.reason,
      },
      "Appointment email job skipped before send",
    );
    return delivery;
  }

  try {
    const result = await sendAppointmentEmail(data.reminder, data.appointment);

    await appointmentClient.recordReminderResult({
      reminder: data.reminder,
      version: data.appointment.version,
      jobId,
      result: toRecordableResult(result),
    });

    return result;
  } catch (error) {
    if (isFinalAttempt(job)) {
      await appointmentClient.recordReminderResult({
        reminder: data.reminder,
        version: data.appointment.version,
        jobId,
        result: {
          status: "failed",
          error: errorMessage(error),
        },
      });
    }

    throw error;
  }
}

function toRecordableResult(result: EmailSendResult) {
  if (result.sent) {
    return {
      status: "sent" as const,
    };
  }

  return {
    status: "skipped" as const,
    reason: result.reason,
    statusCode: result.statusCode,
    responseBody: result.responseBody,
  };
}

function isFinalAttempt(job: Job) {
  const maxAttempts = job.opts.attempts ?? 1;
  return job.attemptsMade + 1 >= maxAttempts;
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

async function shutdown(signal: NodeJS.Signals) {
  logger.info({ signal }, "Stopping appointment email worker");
  await worker.close();
  await closeAppointmentEmailQueue();
  await connection.quit();
}

process.once("SIGINT", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

process.once("SIGTERM", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});
