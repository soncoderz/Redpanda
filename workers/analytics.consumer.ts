import "dotenv/config";

import { env } from "../config/env.js";
import { recordProcessedEvent } from "../models/event-log.repository.js";
import { runConsumer } from "../services/messaging/kafka.service.js";
import {
  connectMongo,
  disconnectMongo,
} from "../services/database/mongodb.service.js";
import { logger } from "../utils/logger.js";

// Kết nối MongoDB trước khi chạy consumer
await connectMongo();

// Chạy Kafka consumer đọc từ topic appointment-events → lưu event log vào MongoDB
await runConsumer({
  groupId: env.kafkaAnalyticsGroupId,
  async onEvent(event) {
    // Lưu event vào collection event_logs (idempotent qua unique eventId)
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
  },
  onShutdown: disconnectMongo,
});
