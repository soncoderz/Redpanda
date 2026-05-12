import "dotenv/config";

import { env } from "../config/env.js";
import { runConsumer } from "../services/messaging/kafka.service.js";
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

await runConsumer({
  groupId: env.kafkaTelegramGroupId,
  async onEvent(event) {
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
  },
});
