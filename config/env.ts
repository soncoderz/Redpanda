export const env = {
  port: readPositiveIntEnv("PORT", 9080),
  restateRuntimeUrl: process.env.RESTATE_RUNTIME_URL ?? "http://localhost:18080",
  restateAuthToken: process.env.RESTATE_AUTH_TOKEN,
  restateIdentityKeys: process.env.RESTATE_IDENTITY_KEYS?.split(",")
    .map((key) => key.trim())
    .filter(Boolean),
  publicRestateEndpoint: "",
  queueDashboardPath: process.env.QUEUE_DASHBOARD_PATH ?? "/admin/queues",
  appointmentsPerCreateRequest: readPositiveIntEnv(
    "APPOINTMENTS_PER_CREATE_REQUEST",
    100,
  ),
  redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
  emailQueueName: process.env.EMAIL_QUEUE_NAME ?? "appointment-email",
  emailWorkerConcurrency: readPositiveIntEnv("EMAIL_WORKER_CONCURRENCY", 100),
  emailRateMax: readPositiveIntEnv("EMAIL_RATE_MAX", 50),
  emailRateDurationMs: readPositiveIntEnv("EMAIL_RATE_DURATION_MS", 100),
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
