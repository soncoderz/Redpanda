import { Hono } from "hono";

import {
  bulkCreateAppointments,
  cancelAppointment,
  createAppointment,
  getAppointment,
  listAppointments,
  updateAppointment,
} from "../controllers/appointment.controller.js";

/** Tạo router cho các endpoint quản lý lịch hẹn */
export function createAppointmentRoutes() {
  const router = new Hono();

  // POST /appointments — tạo lịch hẹn mới
  router.post("/", createAppointment);
  // POST /appointments/bulk — tạo nhiều lịch hẹn cùng lúc
  router.post("/bulk", bulkCreateAppointments);
  // GET /appointments — lấy danh sách lịch hẹn (filter theo status, email)
  router.get("/", listAppointments);
  // GET /appointments/:id — lấy chi tiết 1 lịch hẹn
  router.get("/:id", getAppointment);
  // PATCH /appointments/:id — cập nhật lịch hẹn
  router.patch("/:id", updateAppointment);
  // DELETE /appointments/:id — hủy lịch hẹn
  router.delete("/:id", cancelAppointment);

  return router;
}
