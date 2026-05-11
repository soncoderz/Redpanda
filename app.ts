import { Hono } from "hono";
import { logger as honoLogger } from "hono/logger";

import { env } from "./config/env.js";
import { createQueueDashboard } from "./controllers/queue-dashboard.controller.js";
import { errorMiddleware } from "./middlewares/error.middleware.js";
import { createAppointmentRoutes } from "./routes/appointment.routes.js";
import { createRealtimeRoutes } from "./routes/realtime.routes.js";
import { restateEndpoint } from "./services/restate-endpoint.service.js";
import { kafkaProducerReady } from "./services/kafka.service.js";
import { mongoReadyState } from "./services/mongodb.service.js";

export function createApp() {
  const app = new Hono();
  const queueDashboard = createQueueDashboard(env.queueDashboardPath);

  app.use(honoLogger());
  app.route(env.queueDashboardPath, queueDashboard);

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
  app.route("/api/events", createRealtimeRoutes());

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
