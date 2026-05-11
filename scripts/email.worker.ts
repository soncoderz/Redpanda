import "dotenv/config";
import { Worker, type Job } from "bullmq";

import { env } from "../config/env.js";
import {
  sendAppointmentAfterEmail,
  sendAppointmentAtTimeEmail,
  sendAppointmentBeforeEmail,
  type EmailSendResult,
} from "../services/mailer.service.js";
import type { AppointmentObject } from "../services/appointment.service.js";
import {
  AppointmentEmailJobData,
  closeAppointmentEmailQueue,
  EMAIL_QUEUE_NAME,
  type EmailReminderType,
} from "../services/email-queue.service.js";
import { createRedisConnection } from "../services/redis.service.js";
import { restateClient } from "../services/restate-client.service.js";

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
  console.info("Appointment email job completed", {
    jobId: job.id,
    reminder: job.data.reminder,
    appointmentId: job.data.appointment.id,
    version: job.data.appointment.version,
  });
});

worker.on("failed", (job, error) => {
  console.error("Appointment email job failed", {
    jobId: job?.id,
    reminder: job?.data.reminder,
    appointmentId: job?.data.appointment.id,
    version: job?.data.appointment.version,
    attemptsMade: job?.attemptsMade,
    error: error.message,
  });
});

console.info("Appointment email worker started", {
  queue: EMAIL_QUEUE_NAME,
  restateRuntimeUrl: env.restateRuntimeUrl,
  workerConcurrency: env.emailWorkerConcurrency,
  rateLimitMax: env.emailRateMax,
  rateLimitDurationMs: env.emailRateDurationMs,
});

async function processEmailJob(job: Job<AppointmentEmailJobData>) {
  const data = AppointmentEmailJobData.parse(job.data);
  const appointmentClient = restateClient.objectClient<AppointmentObject>(
    { name: "Appointment" },
    data.appointment.id,
  );
  const jobId = job.id ?? "";

  const delivery = await appointmentClient.startEmailDelivery({
    reminder: data.reminder,
    version: data.appointment.version,
    jobId,
  });

  if (!delivery.shouldSend) {
    console.info("Appointment email job skipped before send", {
      jobId,
      reminder: data.reminder,
      appointmentId: data.appointment.id,
      version: data.appointment.version,
      reason: delivery.reason,
    });
    return delivery;
  }

  try {
    const result = await sendEmailByReminder(data.reminder, data.appointment);

    await appointmentClient.recordEmailResult({
      reminder: data.reminder,
      version: data.appointment.version,
      jobId,
      result: toRecordableResult(result),
    });

    return result;
  } catch (error) {
    if (isFinalAttempt(job)) {
      await appointmentClient.recordEmailResult({
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

function sendEmailByReminder(
  reminder: EmailReminderType,
  appointment: AppointmentEmailJobData["appointment"],
) {
  switch (reminder) {
    case "before":
      return sendAppointmentBeforeEmail(appointment);
    case "atTime":
      return sendAppointmentAtTimeEmail(appointment);
    case "after":
      return sendAppointmentAfterEmail(appointment);
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
  console.info(`Stopping appointment email worker after ${signal}`);
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
