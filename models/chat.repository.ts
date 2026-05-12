import type { ChatMessage } from "./chat.model.js";
import { ChatMessageModel } from "./chat.schema.js";

/** Lưu chat message vào MongoDB — idempotent qua unique id (duplicate = skip) */
export async function saveChatMessage(message: ChatMessage) {
  try {
    // Tạo document mới trong collection chat_messages
    await ChatMessageModel.create(message);
    return { inserted: true };
  } catch (error) {
    // Lỗi duplicate key (11000) = message đã lưu rồi → skip
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
