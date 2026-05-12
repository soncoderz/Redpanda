import type { Context } from "hono";

import { SendChatInput, type ChatMessage } from "../models/chat.model.js";
import {
  publishChatMessage,
  subscribeChatRoom,
} from "../services/messaging/chat.service.js";

const encoder = new TextEncoder();

export async function sendMessage(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const input = SendChatInput.parse(body);

  const message: ChatMessage = {
    id: crypto.randomUUID(),
    roomId: input.roomId,
    from: input.from,
    text: input.text,
    sentAt: new Date().toISOString(),
  };

  await publishChatMessage(message);

  return c.json(message, 201);
}

export function streamMessages(c: Context) {
  const roomId = c.req.param("roomId")!;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(
        encoder.encode(
          `event: connected\ndata: ${JSON.stringify({ roomId })}\n\n`,
        ),
      );

      const unsubscribe = subscribeChatRoom(roomId, (message) => {
        controller.enqueue(
          encoder.encode(
            `id: ${message.id}\nevent: message\ndata: ${JSON.stringify(message)}\n\n`,
          ),
        );
      });

      const keepAlive = setInterval(() => {
        controller.enqueue(encoder.encode(": keep-alive\n\n"));
      }, 25_000);

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

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
