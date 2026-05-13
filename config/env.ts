/** Biến môi trường toàn bộ ứng dụng — đọc từ process.env với giá trị mặc định */
export const env = {
  // --- Cấu hình chung ---
  nodeEnv: process.env.NODE_ENV ?? "development",
  logLevel: process.env.LOG_LEVEL ?? "info",

  // --- Server ---
  port: readPositiveIntEnv("PORT", 9080),
  publicRestateEndpoint: "",

  // --- Restate: durable execution framework ---
  restateRuntimeUrl:
    process.env.RESTATE_RUNTIME_URL ?? "http://localhost:18080",
  restateAdminUrl: process.env.RESTATE_ADMIN_URL ?? "http://localhost:19070",
  restateAuthToken: emptyToUndefined(process.env.RESTATE_AUTH_TOKEN),
  restateIdentityKeys: readCsvEnv("RESTATE_IDENTITY_KEYS"),

  // --- MongoDB: lưu appointment, event log, chat ---
  mongoUri:
    process.env.MONGODB_URI ?? "mongodb://localhost:27017/appointments",
  mongoDbName: emptyToUndefined(process.env.MONGODB_DB_NAME),

  // --- Redis: backend cho BullMQ queue ---
  redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
  queueDashboardPath: process.env.QUEUE_DASHBOARD_PATH ?? "/admin/queues",

  // --- Email Queue (BullMQ) ---
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

  // --- Reminder timing: trước/sau bao lâu so với startAt ---
  reminderBeforeMs: readNonNegativeIntEnv("REMINDER_BEFORE_MS", 60_000),
  reminderAfterMs: readNonNegativeIntEnv("REMINDER_AFTER_MS", 60_000),

  // --- Maintenance Queue: dọn dẹp job cũ ---
  maintenanceQueueName:
    process.env.MAINTENANCE_QUEUE_NAME ?? "appointment-maintenance",
  cleanupEveryMs: readPositiveIntEnv("CLEANUP_EVERY_MS", 3_600_000),
  cleanupGraceMs: readPositiveIntEnv("CLEANUP_GRACE_MS", 86_400_000),
  cleanupLimit: readPositiveIntEnv("CLEANUP_LIMIT", 10_000),

  // --- SendGrid: gửi email ---
  sendgridApiKey: emptyToUndefined(process.env.SENDGRID_API_KEY),
  sendgridFromEmail: process.env.SENDGRID_FROM_EMAIL ?? "",
  sendgridFromName: process.env.SENDGRID_FROM_NAME ?? "Appointment Reminder",

  // --- Schema Registry: validate event schema trước khi publish/consume ---
  schemaRegistryUrl:
    process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:18081",

  // --- Kafka Connect: sink connectors (MongoDB, etc.) ---
  kafkaConnectUrl:
    process.env.KAFKA_CONNECT_URL ?? "http://localhost:8083",

  // --- Kafka/Redpanda: event streaming ---
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
  kafkaChatGroupId: process.env.KAFKA_CHAT_GROUP_ID ?? "chat-storage",

  // --- Telegram Bot: gửi thông báo ---
  telegramBotToken: emptyToUndefined(process.env.TELEGRAM_BOT_TOKEN),
  telegramChatId: emptyToUndefined(process.env.TELEGRAM_CHAT_ID),
};

// Tạo URL endpoint Restate từ port server
env.publicRestateEndpoint =
  process.env.PUBLIC_RESTATE_ENDPOINT ?? `http://localhost:${env.port}/restate`;

/** Đọc biến môi trường dạng số nguyên dương, trả fallback nếu không hợp lệ */
function readPositiveIntEnv(name: string, fallback: number) {
  const value = Number(process.env[name]);

  if (Number.isInteger(value) && value > 0) {
    return value;
  }

  return fallback;
}

/** Đọc biến môi trường dạng số nguyên >= 0, trả fallback nếu không hợp lệ */
function readNonNegativeIntEnv(name: string, fallback: number) {
  const value = Number(process.env[name]);

  if (Number.isInteger(value) && value >= 0) {
    return value;
  }

  return fallback;
}

/** Đọc biến môi trường dạng CSV (phân tách bằng dấu phẩy) thành mảng string */
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

/** Đọc biến môi trường dạng boolean (1/true/yes/y = true) */
function readBooleanEnv(name: string, fallback: boolean) {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }

  return ["1", "true", "yes", "y"].includes(raw);
}

/** Chuyển chuỗi rỗng thành undefined (dùng cho optional env vars) */
function emptyToUndefined(value: string | undefined) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
