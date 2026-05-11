import "dotenv/config";

import { env } from "../config/env.js";
import { AppointmentEventEnvelope } from "../models/appointment.model.js";
import { recordProcessedEvent } from "../models/event-log.repository.js";
import {
  createKafkaConsumer,
  subscribeToAppointmentEvents,
} from "../services/messaging/kafka.service.js";
import {
  connectMongo,
  disconnectMongo,
} from "../services/db/mongodb.service.js";
import { logger } from "../utils/logger.js";

await connectMongo();

const consumer = await createKafkaConsumer(env.kafkaAnalyticsGroupId);

logger.info(
  {
    topic: env.kafkaAppointmentTopic,
    groupId: env.kafkaAnalyticsGroupId,
  },
  "Analytics consumer started",
);

process.once("SIGINT", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

process.once("SIGTERM", (signal) => {
  void shutdown(signal).then(() => process.exit(0));
});

await subscribeToAppointmentEvents(consumer, async ({ message }) => {
  if (!message.value) {
    return;
  }

  const event = AppointmentEventEnvelope.parse(
    JSON.parse(message.value.toString("utf8")),
  );
  const result = await recordProcessedEvent(event);

  if (!result.inserted) {
    logger.info({ eventId: event.eventId }, "Duplicate event skipped");
    return;
  }

  logger.info(
    {
      eventId: event.eventId,
      type: event.type,
      appointmentId: event.appointmentId,
      version: event.version,
    },
    "Analytics event processed",
  );
});

async function shutdown(signal: NodeJS.Signals) {
  logger.info({ signal }, "Stopping analytics consumer");
  await consumer.disconnect();
  await disconnectMongo();
}
