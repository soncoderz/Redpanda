import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import type { ChatMessage } from "../../models/chat.model.js";
import { env } from "../../config/env.js";
import {
  encodeWithSchema,
  getOrRegisterSchema,
} from "../../config/schema-registry.js";
import { getKafkaProducer } from "./kafka.service.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** JSON Schema cho chat message — dùng để register lên Schema Registry */
const chatMessageSchema = JSON.parse(
  readFileSync(
    resolve(__dirname, "..", "..", "schemas", "chat-message.schema.json"),
    "utf-8",
  ),
);

const SUBJECT = `${env.kafkaChatTopic}-value`;

/** EventEmitter để phát chat message real-time cho SSE stream */
const emitter = new EventEmitter();
// Tăng giới hạn listener vì mỗi SSE connection tạo 1 listener
emitter.setMaxListeners(200);

/** Publish chat message lên Kafka topic và phát qua EventEmitter cho SSE */
export async function publishChatMessage(message: ChatMessage) {
  // Register schema (hoặc lấy từ cache nếu đã register)
  const schemaId = await getOrRegisterSchema(SUBJECT, chatMessageSchema);

  // Encode payload theo Confluent wire format
  const encodedValue = await encodeWithSchema(schemaId, message as unknown as Record<string, unknown>);

  // Lấy Kafka producer đang kết nối
  const producer = await getKafkaProducer();

  // Gửi message lên Kafka topic chat-messages với key là roomId (đảm bảo ordering trong phòng)
  await producer.send({
    topic: env.kafkaChatTopic,
    acks: -1,
    messages: [
      {
        key: message.roomId,
        value: encodedValue,
        headers: { roomId: message.roomId, from: message.from },
      },
    ],
  });

  // Phát event local cho các SSE connection đang listen cùng phòng
  emitter.emit("chat", message);
}

/** Subscribe nhận tin nhắn chat theo phòng — trả về hàm unsubscribe */
export function subscribeChatRoom(
  roomId: string,
  listener: (message: ChatMessage) => void,
) {
  // Chỉ forward message thuộc đúng roomId đến listener
  const handler = (message: ChatMessage) => {
    if (message.roomId === roomId) {
      listener(message);
    }
  };

  emitter.on("chat", handler);

  // Trả về hàm unsubscribe để gỡ listener khi client ngắt kết nối
  return () => {
    emitter.off("chat", handler);
  };
}
