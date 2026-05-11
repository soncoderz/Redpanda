import { Hono } from "hono";

import {
  createAppointment,
  getAppointment,
  markAppointmentArrived,
  updateAppointment,
} from "../controllers/appointment.controller.js";

export function createAppointmentRoutes() {
  const router = new Hono();

  router.post("/", createAppointment);
  router.get("/:id", getAppointment);
  router.put("/:id", updateAppointment);
  router.post("/:id/arrived", markAppointmentArrived);

  return router;
}
