import type { ChatMessage } from "./chat.model.js";
import { ChatMessageModel } from "./chat.schema.js";

export async function saveChatMessage(message: ChatMessage) {
  try {
    await ChatMessageModel.create(message);
    return { inserted: true };
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === 11000
    ) {
      return { inserted: false };
    }

    throw error;
  }
}
