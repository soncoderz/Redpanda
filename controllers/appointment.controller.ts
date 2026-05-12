import type { Context } from "hono";

import { listAppointments as listAppointmentRecords } from "../models/appointment.repository.js";
import { restateClient } from "../config/restate.js";
import type { AppointmentObject } from "../services/appointment/appointment.service.js";
import { appointmentIdFromIdempotencyKey } from "../utils/appointment.utils.js";
import { logger } from "../utils/logger.js";
import {
  BulkCreateAppointmentInput,
  CreateAppointmentInput,
  ListAppointmentsQuery,
  UpdateAppointmentInput,
} from "../validation/appointment.validation.js";

/** POST /api/appointments — tạo lịch hẹn mới qua Restate */
export async function createAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = CreateAppointmentInput.parse(body);
  const headerIdempotencyKey = c.req.header("Idempotency-Key");
  const idempotencyKey = headerIdempotencyKey ?? payload.idempotencyKey;
  const { id, ...appointmentInput } = payload;

  // Tạo ID từ idempotency key (deterministic) hoặc random UUID
  const appointmentId =
    id ??
    (idempotencyKey
      ? appointmentIdFromIdempotencyKey(idempotencyKey)
      : crypto.randomUUID());

  // Gọi Restate virtual object "Appointment" → handler create
  const appointment = await appointmentClient(appointmentId).create({
    ...appointmentInput,
    idempotencyKey,
  });

  return c.json(appointment, 201);
}

/** POST /api/appointments/bulk — tạo nhiều lịch hẹn (chạy song song theo batch 10) */
export async function bulkCreateAppointments(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const input = BulkCreateAppointmentInput.parse(body);

  const baseTime = new Date(input.startAt).getTime();
  const results: { index: number; id: string; status: string }[] = [];
  const errors: { index: number; error: string }[] = [];

  // Chia thành batch 10 để không quá tải Restate
  const batchSize = 10;
  for (let batchStart = 0; batchStart < input.count; batchStart += batchSize) {
    const batchEnd = Math.min(batchStart + batchSize, input.count);
    const promises = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const num = String(i + 1).padStart(3, "0");
      const appointmentId = `apt-${Date.now().toString(36)}-${num}`;
      const startAt = new Date(
        baseTime + i * input.intervalMinutes * 60_000,
      ).toISOString();

      // Gọi Restate create cho mỗi appointment trong batch
      promises.push(
        appointmentClient(appointmentId)
          .create({
            customerName: `Customer ${num}`,
            customerEmail: `customer${num}@example.com`,
            service: input.service,
            startAt,
            note: `Bulk created #${i + 1}`,
          })
          .then(
            (apt: any) => {
              results.push({ index: i + 1, id: apt.id, status: "created" });
            },
            (err: any) => {
              const msg = err instanceof Error ? err.message : String(err);
              errors.push({ index: i + 1, error: msg });
              logger.warn({ index: i + 1, error: msg }, "Bulk create failed");
            }
          ),
      );
    }

    // Đợi hết batch trước khi chạy batch tiếp
    await Promise.all(promises);
  }

  return c.json(
    {
      total: input.count,
      created: results.length,
      failed: errors.length,
      results,
      errors,
    },
    201,
  );
}

/** GET /api/appointments — danh sách lịch hẹn (đọc trực tiếp từ MongoDB) */
export async function listAppointments(c: Context) {
  const query = ListAppointmentsQuery.parse({
    status: c.req.query("status"),
    customerEmail: c.req.query("customerEmail"),
    limit: c.req.query("limit"),
  });

  const appointments = await listAppointmentRecords(query);
  return c.json({ appointments });
}

/** GET /api/appointments/:id — chi tiết lịch hẹn (qua Restate để đảm bảo consistency) */
export async function getAppointment(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).get();
  return c.json(appointment);
}

/** PATCH /api/appointments/:id — cập nhật lịch hẹn qua Restate */
export async function updateAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = UpdateAppointmentInput.parse(body);
  const appointment = await appointmentClient(appointmentId(c)).update(payload);
  return c.json(appointment);
}

/** DELETE /api/appointments/:id — huỷ lịch hẹn qua Restate */
export async function cancelAppointment(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).cancel();
  return c.json(appointment);
}

/** Tạo Restate object client cho appointment ID */
function appointmentClient(id: string) {
  return restateClient.objectClient<AppointmentObject>(
    { name: "Appointment" },
    id,
  );
}

/** Lấy appointment ID từ URL param */
function appointmentId(c: Context) {
  const id = c.req.param("id");
  if (!id) {
    throw new Error("Appointment id is required");
  }

  return id;
}
