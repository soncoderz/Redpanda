import "dotenv/config";
import { serve } from "@hono/node-server";

import { createApp } from "./app.js";
import { env } from "./config/env.js";
import {
  connectKafkaProducer,
  disconnectKafkaProducer,
  ensureKafkaTopics,
} from "./services/messaging/kafka.service.js";
import {
  connectMongo,
  disconnectMongo,
} from "./services/db/mongodb.service.js";
import { closeAppointmentEmailQueue } from "./services/queue/email-queue.service.js";
import { closeMaintenanceQueue } from "./services/queue/maintenance-queue.service.js";
import { logger } from "./utils/logger.js";

await startupRetry("MongoDB", connectMongo);
await startupRetry("Kafka topics", ensureKafkaTopics);
await startupRetry("Kafka producer", connectKafkaProducer);

const app = createApp();

const server = serve({ fetch: app.fetch, port: env.port }, (info) => {
  logger.info(
    {
      url: `http://localhost:${info.port}`,
      restateEndpoint: env.publicRestateEndpoint,
      queueDashboard: `http://localhost:${info.port}${env.queueDashboardPath}`,
    },
    "Hono API listening",
  );
});

async function shutdown(signal: NodeJS.Signals) {
  logger.info({ signal }, "Stopping API server");
  server.close();
  await Promise.allSettled([
    disconnectKafkaProducer(),
    disconnectMongo(),
    closeAppointmentEmailQueue(),
    closeMaintenanceQueue(),
  ]);
}

process.once("SIGINT", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

process.once("SIGTERM", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

async function startupRetry(name: string, action: () => Promise<unknown>) {
  const attempts = 30;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await action();
      return;
    } catch (error) {
      logger.warn(
        {
          name,
          attempt,
          error: error instanceof Error ? error.message : String(error),
        },
        "Dependency is not ready",
      );
      await new Promise((resolve) => setTimeout(resolve, 2_000));
    }
  }

  throw new Error(`${name} was not ready after ${attempts} attempts`);
}
