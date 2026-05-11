import { Hono } from "hono";
import { logger } from "hono/logger";

import { env } from "./config/env.js";
import { createQueueDashboard } from "./controllers/queue-dashboard.controller.js";
import { errorMiddleware } from "./middlewares/error.middleware.js";
import { createAppointmentRoutes } from "./routes/appointment.routes.js";
import { restateEndpoint } from "./services/restate-endpoint.service.js";

export function createApp() {
  const app = new Hono();
  const queueDashboard = createQueueDashboard(env.queueDashboardPath);

  app.use(logger());
  app.route(env.queueDashboardPath, queueDashboard);

  app.get("/", (c) =>
    c.json({
      name: "Restate + Hono appointment backend",
      health: "/health",
      restateEndpoint: "/restate",
      queueDashboard: env.queueDashboardPath,
      api: {
        createAppointment: "POST /api/appointments",
        getAppointment: "GET /api/appointments/:id",
        updateAppointment: "PUT /api/appointments/:id",
        markAppointmentArrived: "POST /api/appointments/:id/arrived",
      },
    }),
  );

  app.get("/health", (c) =>
    c.json({
      ok: true,
      restateRuntimeUrl: env.restateRuntimeUrl,
      restateAuthConfigured: Boolean(env.restateAuthToken),
      publicRestateEndpoint: env.publicRestateEndpoint,
      queueDashboard: env.queueDashboardPath,
    }),
  );

  app.route("/api/appointments", createAppointmentRoutes());

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
