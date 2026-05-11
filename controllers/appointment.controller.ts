import type { Context } from "hono";

import { listAppointments as listAppointmentRecords } from "../repositories/appointment.repository.js";
import { restateClient } from "../services/restate-client.service.js";
import type { AppointmentObject } from "../services/appointment.service.js";
import { appointmentIdFromIdempotencyKey } from "../utils/appointment.utils.js";
import {
  CreateAppointmentInput,
  ListAppointmentsQuery,
  UpdateAppointmentInput,
} from "../validation/appointment.validation.js";

export async function createAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = CreateAppointmentInput.parse(body);
  const headerIdempotencyKey = c.req.header("Idempotency-Key");
  const idempotencyKey = headerIdempotencyKey ?? payload.idempotencyKey;
  const { id, ...appointmentInput } = payload;
  const appointmentId =
    id ??
    (idempotencyKey
      ? appointmentIdFromIdempotencyKey(idempotencyKey)
      : crypto.randomUUID());

  const appointment = await appointmentClient(appointmentId).create({
    ...appointmentInput,
    idempotencyKey,
  });

  return c.json(appointment, 201);
}

export async function listAppointments(c: Context) {
  const query = ListAppointmentsQuery.parse({
    status: c.req.query("status"),
    customerEmail: c.req.query("customerEmail"),
    limit: c.req.query("limit"),
  });

  const appointments = await listAppointmentRecords(query);
  return c.json({ appointments });
}

export async function getAppointment(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).get();

  return c.json(appointment);
}

export async function updateAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = UpdateAppointmentInput.parse(body);

  const appointment = await appointmentClient(appointmentId(c)).update(payload);

  return c.json(appointment);
}

export async function cancelAppointment(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).cancel();

  return c.json(appointment);
}

function appointmentClient(id: string) {
  return restateClient.objectClient<AppointmentObject>(
    { name: "Appointment" },
    id,
  );
}

function appointmentId(c: Context) {
  const id = c.req.param("id");
  if (!id) {
    throw new Error("Appointment id is required");
  }

  return id;
}
