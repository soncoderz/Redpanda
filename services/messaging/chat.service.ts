import { EventEmitter } from "node:events";

import type { ChatMessage } from "../../models/chat.model.js";
import { env } from "../../config/env.js";
import { getKafkaProducer } from "./kafka.service.js";

const emitter = new EventEmitter();
emitter.setMaxListeners(200);

export async function publishChatMessage(message: ChatMessage) {
  const producer = await getKafkaProducer();

  await producer.send({
    topic: env.kafkaChatTopic,
    acks: -1,
    messages: [
      {
        key: message.roomId,
        value: JSON.stringify(message),
        headers: { roomId: message.roomId, from: message.from },
      },
    ],
  });

  emitter.emit("chat", message);
}

export function subscribeChatRoom(
  roomId: string,
  listener: (message: ChatMessage) => void,
) {
  const handler = (message: ChatMessage) => {
    if (message.roomId === roomId) {
      listener(message);
    }
  };

  emitter.on("chat", handler);

  return () => {
    emitter.off("chat", handler);
  };
}
