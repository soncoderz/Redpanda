export const env = {
  nodeEnv: process.env.NODE_ENV ?? "development",
  logLevel: process.env.LOG_LEVEL ?? "info",

  port: readPositiveIntEnv("PORT", 9080),
  publicRestateEndpoint: "",
  restateRuntimeUrl:
    process.env.RESTATE_RUNTIME_URL ?? "http://localhost:18080",
  restateAdminUrl: process.env.RESTATE_ADMIN_URL ?? "http://localhost:19070",
  restateAuthToken: emptyToUndefined(process.env.RESTATE_AUTH_TOKEN),
  restateIdentityKeys: readCsvEnv("RESTATE_IDENTITY_KEYS"),

  mongoUri:
    process.env.MONGODB_URI ?? "mongodb://localhost:27017/appointments",
  mongoDbName: emptyToUndefined(process.env.MONGODB_DB_NAME),

  redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
  queueDashboardPath: process.env.QUEUE_DASHBOARD_PATH ?? "/admin/queues",

  emailQueueName: process.env.EMAIL_QUEUE_NAME ?? "appointment-email",
  emailWorkerConcurrency: readPositiveIntEnv("EMAIL_WORKER_CONCURRENCY", 25),
  emailRateMax: readPositiveIntEnv("EMAIL_RATE_MAX", 50),
  emailRateDurationMs: readPositiveIntEnv("EMAIL_RATE_DURATION_MS", 1_000),
  emailJobAttempts: readPositiveIntEnv("EMAIL_JOB_ATTEMPTS", 5),
  emailJobBackoffMs: readPositiveIntEnv("EMAIL_JOB_BACKOFF_MS", 2_000),
  emailJobRemoveCompleteAgeSeconds: readPositiveIntEnv(
    "EMAIL_JOB_REMOVE_COMPLETE_AGE_SECONDS",
    86_400,
  ),
  emailJobRemoveCompleteCount: readPositiveIntEnv(
    "EMAIL_JOB_REMOVE_COMPLETE_COUNT",
    10_000,
  ),
  emailJobRemoveFailAgeSeconds: readPositiveIntEnv(
    "EMAIL_JOB_REMOVE_FAIL_AGE_SECONDS",
    604_800,
  ),
  reminderBeforeMs: readNonNegativeIntEnv("REMINDER_BEFORE_MS", 60_000),
  reminderAfterMs: readNonNegativeIntEnv("REMINDER_AFTER_MS", 60_000),

  maintenanceQueueName:
    process.env.MAINTENANCE_QUEUE_NAME ?? "appointment-maintenance",
  cleanupEveryMs: readPositiveIntEnv("CLEANUP_EVERY_MS", 3_600_000),
  cleanupGraceMs: readPositiveIntEnv("CLEANUP_GRACE_MS", 86_400_000),
  cleanupLimit: readPositiveIntEnv("CLEANUP_LIMIT", 10_000),

  sendgridApiKey: emptyToUndefined(process.env.SENDGRID_API_KEY),
  sendgridFromEmail: process.env.SENDGRID_FROM_EMAIL ?? "",
  sendgridFromName: process.env.SENDGRID_FROM_NAME ?? "Appointment Reminder",

  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "appointment-service",
  kafkaBrokers: readCsvEnv("KAFKA_BROKERS", ["localhost:19092"]),
  kafkaSsl: readBooleanEnv("KAFKA_SSL", false),
  kafkaUsername: emptyToUndefined(process.env.KAFKA_USERNAME),
  kafkaPassword: emptyToUndefined(process.env.KAFKA_PASSWORD),
  kafkaAppointmentTopic:
    process.env.KAFKA_APPOINTMENT_TOPIC ?? "appointment-events",
  kafkaAnalyticsGroupId:
    process.env.KAFKA_ANALYTICS_GROUP_ID ?? "appointment-analytics",
  kafkaTopicPartitions: readPositiveIntEnv("KAFKA_TOPIC_PARTITIONS", 3),
  kafkaTopicReplicationFactor: readPositiveIntEnv(
    "KAFKA_TOPIC_REPLICATION_FACTOR",
    1,
  ),
  kafkaTelegramGroupId:
    process.env.KAFKA_TELEGRAM_GROUP_ID ?? "appointment-telegram",
  kafkaChatTopic: process.env.KAFKA_CHAT_TOPIC ?? "chat-messages",

  telegramBotToken: emptyToUndefined(process.env.TELEGRAM_BOT_TOKEN),
  telegramChatId: emptyToUndefined(process.env.TELEGRAM_CHAT_ID),
};

env.publicRestateEndpoint =
  process.env.PUBLIC_RESTATE_ENDPOINT ?? `http://localhost:${env.port}/restate`;

export function readPositiveIntEnv(name: string, fallback: number) {
  const value = Number(process.env[name]);

  if (Number.isInteger(value) && value > 0) {
    return value;
  }

  return fallback;
}

export function readNonNegativeIntEnv(name: string, fallback: number) {
  const value = Number(process.env[name]);

  if (Number.isInteger(value) && value >= 0) {
    return value;
  }

  return fallback;
}

function readCsvEnv(name: string, fallback: string[] = []) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  return raw
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function readBooleanEnv(name: string, fallback: boolean) {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }

  return ["1", "true", "yes", "y"].includes(raw);
}

function emptyToUndefined(value: string | undefined) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
