import { serveStatic } from "@hono/node-server/serve-static";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { HonoAdapter } from "@bull-board/hono";
import { Hono } from "hono";
import { logger as honoLogger } from "hono/logger";

import { env } from "./config/env.js";
import { errorMiddleware } from "./middlewares/error.middleware.js";
import { createAppointmentRoutes } from "./routes/appointment.routes.js";
import { appointmentEmailQueue } from "./services/queue/email-queue.service.js";
import { maintenanceQueue } from "./services/queue/maintenance-queue.service.js";
import { restateEndpoint } from "./services/restate/restate.service.js";
import { kafkaProducerReady } from "./services/messaging/kafka.service.js";
import { subscribeRealtimeEvents } from "./services/messaging/realtime.service.js";
import { mongoReadyState } from "./services/db/mongodb.service.js";

const encoder = new TextEncoder();

export function createApp() {
  const app = new Hono();

  // BullMQ Dashboard
  const serverAdapter = new HonoAdapter(serveStatic);
  serverAdapter.setBasePath(env.queueDashboardPath);
  createBullBoard({
    queues: [
      new BullMQAdapter(appointmentEmailQueue, {
        description: "Appointment reminder email jobs",
      }),
      new BullMQAdapter(maintenanceQueue, {
        description: "Scheduled maintenance jobs",
      }),
    ],
    serverAdapter,
    options: {
      uiConfig: { boardTitle: "Appointment Email Queue" },
    },
  });

  app.use(honoLogger());
  app.route(env.queueDashboardPath, serverAdapter.registerPlugin());

  app.get("/", (c) =>
    c.json({
      name: "Appointment booking backend",
      health: "/health",
      restateEndpoint: "/restate",
      queueDashboard: env.queueDashboardPath,
      realtimeEvents: "/api/events/appointments",
      api: {
        createAppointment: "POST /api/appointments",
        listAppointments: "GET /api/appointments",
        getAppointment: "GET /api/appointments/:id",
        updateAppointment: "PATCH /api/appointments/:id",
        cancelAppointment: "DELETE /api/appointments/:id",
      },
    }),
  );

  app.get("/health", (c) =>
    c.json({
      ok: true,
      mongoReadyState: mongoReadyState(),
      kafkaProducerReady: kafkaProducerReady(),
      restateRuntimeUrl: env.restateRuntimeUrl,
      restateAdminUrl: env.restateAdminUrl,
      restateAuthConfigured: Boolean(env.restateAuthToken),
      publicRestateEndpoint: env.publicRestateEndpoint,
      queueDashboard: env.queueDashboardPath,
    }),
  );

  app.route("/api/appointments", createAppointmentRoutes());

  // SSE endpoint
  app.get("/api/events/appointments", (c) => {
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
  });

  app.onError(errorMiddleware);

  app.all("/restate", (c) => restateEndpoint(stripRestatePrefix(c.req.raw)));
  app.all("/restate/*", (c) => restateEndpoint(stripRestatePrefix(c.req.raw)));

  return app;
}

function stripRestatePrefix(request: Request) {
  const url = new URL(request.url);
  url.pathname = url.pathname.replace(/^\/restate(?=\/|$)/, "") || "/";
  return new Request(url, request);
}
