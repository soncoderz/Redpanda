import "dotenv/config";
import { Worker, type Job } from "bullmq";

import { env } from "../config/env.js";
import type { AppointmentObject } from "../services/restate/appointment.handler.js";
import {
  AppointmentEmailJobData,
  closeAppointmentEmailQueue,
  EMAIL_QUEUE_NAME,
} from "../services/queue/email-queue.service.js";
import {
  sendAppointmentEmail,
  type EmailSendResult,
} from "../services/email/email.service.js";
import { createRedisConnection } from "../config/redis.js";
import { restateClient } from "../config/restate.js";
import { logger } from "../utils/logger.js";

// Kết nối Redis cho worker
const connection = createRedisConnection();

// Tạo BullMQ worker lắng nghe queue email (concurrency + rate limit)
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

/** Xử lý 1 job email: validate qua Restate → gửi mail → ghi kết quả về Restate */
async function processEmailJob(job: Job<AppointmentEmailJobData>) {
  const data = AppointmentEmailJobData.parse(job.data);
  const appointmentClient = restateClient.objectClient<AppointmentObject>(
    { name: "Appointment" },
    data.appointment.id,
  );
  const jobId = job.id ?? "";

  // Bước 1: Gọi Restate validate (đúng version? chưa cancel? chưa gửi?)
  const delivery = await appointmentClient.startReminderDelivery({
    reminder: data.reminder,
    version: data.appointment.version,
    jobId,
  });

  // Nếu không cần gửi → skip
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
    // Bước 2: Gửi email qua SendGrid (hoặc mock)
    const result = await sendAppointmentEmail(data.reminder, data.appointment);

    // Bước 3: Ghi kết quả (sent/skipped) về Restate → cập nhật MongoDB
    await appointmentClient.recordReminderResult({
      reminder: data.reminder,
      version: data.appointment.version,
      jobId,
      result: toRecordableResult(result),
    });

    return result;
  } catch (error) {
    // Nếu là lần thử cuối cùng → ghi lỗi về Restate
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

/** Chuyển kết quả gửi email thành format để ghi vào Restate */
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

/** Tắt worker, đóng queue và Redis */
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
