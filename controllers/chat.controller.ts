import type { Context } from "hono";

import { SendChatInput, type ChatMessage } from "../models/chat.model.js";
import {
  publishChatMessage,
  subscribeChatRoom,
} from "../services/messaging/chat.service.js";

/** TextEncoder dùng để encode SSE data thành bytes */
const encoder = new TextEncoder();

/** Handler gửi tin nhắn chat — validate input → tạo message → publish lên Kafka + SSE */
export async function sendMessage(c: Context) {
  // Parse và validate body request
  const body = await c.req.json().catch(() => undefined);
  const input = SendChatInput.parse(body);

  // Tạo chat message với UUID và timestamp
  const message: ChatMessage = {
    id: crypto.randomUUID(),
    roomId: input.roomId,
    from: input.from,
    text: input.text,
    sentAt: new Date().toISOString(),
  };

  // Publish message lên Kafka topic chat-messages và phát qua EventEmitter
  await publishChatMessage(message);

  return c.json(message, 201);
}

/** Handler SSE stream — client kết nối để nhận tin nhắn real-time theo phòng chat */
export function streamMessages(c: Context) {
  const roomId = c.req.param("roomId")!;

  // Tạo ReadableStream cho SSE connection
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      // Gửi event "connected" xác nhận kết nối thành công
      controller.enqueue(
        encoder.encode(
          `event: connected\ndata: ${JSON.stringify({ roomId })}\n\n`,
        ),
      );

      // Subscribe vào EventEmitter để nhận tin nhắn mới theo roomId
      const unsubscribe = subscribeChatRoom(roomId, (message) => {
        controller.enqueue(
          encoder.encode(
            `id: ${message.id}\nevent: message\ndata: ${JSON.stringify(message)}\n\n`,
          ),
        );
      });

      // Gửi keep-alive mỗi 25 giây để giữ kết nối SSE không bị timeout
      const keepAlive = setInterval(() => {
        controller.enqueue(encoder.encode(": keep-alive\n\n"));
      }, 25_000);

      // Khi client ngắt kết nối → dọn dẹp interval và unsubscribe
      c.req.raw.signal.addEventListener("abort", () => {
        clearInterval(keepAlive);
        unsubscribe();
        try {
          controller.close();
        } catch {
          // Stream may already be closed
        }
      });
    },
  });

  // Trả về Response với SSE headers
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
