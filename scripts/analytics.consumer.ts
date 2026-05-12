import "dotenv/config";

import { env } from "../config/env.js";
import { recordProcessedEvent } from "../models/event-log.repository.js";
import { runConsumer } from "../services/messaging/kafka.service.js";
import {
  connectMongo,
  disconnectMongo,
} from "../services/db/mongodb.service.js";
import { logger } from "../utils/logger.js";

await connectMongo();

await runConsumer({
  groupId: env.kafkaAnalyticsGroupId,
  async onEvent(event) {
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
