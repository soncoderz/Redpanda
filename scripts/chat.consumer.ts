import "dotenv/config";

import { env } from "../config/env.js";
import { saveChatMessage } from "../models/chat.repository.js";
import { connectMongo, disconnectMongo } from "../services/db/mongodb.service.js";
import { runChatConsumer } from "../services/messaging/kafka.service.js";
import { logger } from "../utils/logger.js";

await connectMongo();

await runChatConsumer({
  groupId: env.kafkaChatGroupId,
  async onMessage(message) {
    const result = await saveChatMessage(message);
    if (result.inserted) {
      logger.info(
        { id: message.id, roomId: message.roomId, from: message.from },
        "Chat message saved",
      );
    } else {
      logger.debug(
        { id: message.id },
        "Chat message already exists, skipped",
      );
    }
  },
  async onShutdown() {
    await disconnectMongo();
  },
});
