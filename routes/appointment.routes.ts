import { Hono } from "hono";

import {
  bulkCreateAppointments,
  cancelAppointment,
  createAppointment,
  getAppointment,
  listAppointments,
  updateAppointment,
} from "../controllers/appointment.controller.js";

export function createAppointmentRoutes() {
  const router = new Hono();

  router.post("/", createAppointment);
  router.post("/bulk", bulkCreateAppointments);
  router.get("/", listAppointments);
  router.get("/:id", getAppointment);
  router.patch("/:id", updateAppointment);
  router.delete("/:id", cancelAppointment);

  return router;
}
