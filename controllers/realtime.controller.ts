import type { Context } from "hono";

import {
  subscribeRealtimeEvents,
} from "../services/messaging/realtime.service.js";

const encoder = new TextEncoder();

export function streamAppointmentEvents(c: Context) {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(
        encoder.encode("event: ready\ndata: {\"ok\":true}\n\n"),
      );

      const unsubscribe = subscribeRealtimeEvents((event) => {
        controller.enqueue(
          encoder.encode(
            [
              `id: ${event.eventId}`,
              `event: ${event.type}`,
              `data: ${JSON.stringify(event)}`,
              "",
              "",
            ].join("\n"),
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
          // The stream may already be closed by the client.
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
