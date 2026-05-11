import "dotenv/config";

import { env } from "../config/env.js";
import { AppointmentEventEnvelope } from "../models/appointment.model.js";
import {
  createKafkaConsumer,
  subscribeToAppointmentEvents,
} from "../services/messaging/kafka.service.js";
import {
  formatAppointmentEvent,
  isTelegramConfigured,
  sendTelegramMessage,
} from "../services/messaging/telegram.service.js";
import { logger } from "../utils/logger.js";

if (!isTelegramConfigured()) {
  logger.error(
    "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required. Set them in .env",
  );
  process.exit(1);
}

const consumer = await createKafkaConsumer(env.kafkaTelegramGroupId);

logger.info(
  {
    topic: env.kafkaAppointmentTopic,
    groupId: env.kafkaTelegramGroupId,
  },
  "Telegram consumer started",
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

  const text = formatAppointmentEvent(event);
  const result = await sendTelegramMessage(text);

  if (result.sent) {
    logger.info(
      {
        eventId: event.eventId,
        type: event.type,
        appointmentId: event.appointmentId,
      },
      "Telegram notification sent",
    );
  } else {
    logger.warn(
      {
        eventId: event.eventId,
        type: event.type,
        reason: result.reason,
      },
      "Telegram notification skipped",
    );
  }
});

async function shutdown(signal: NodeJS.Signals) {
  logger.info({ signal }, "Stopping Telegram consumer");
  await consumer.disconnect();
}
