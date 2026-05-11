import { Hono } from "hono";

import { streamAppointmentEvents } from "../controllers/realtime.controller.js";

export function createRealtimeRoutes() {
  const router = new Hono();

  router.get("/appointments", streamAppointmentEvents);

  return router;
}
