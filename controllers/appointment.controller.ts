import type { Context } from "hono";

import { env } from "../config/env.js";
import {
  AppointmentInput,
  type AppointmentState,
} from "../models/appointment.model.js";
import { restateClient } from "../services/restate-client.service.js";
import {
  type AppointmentObject,
} from "../services/appointment.service.js";
import { CreateAppointmentInput } from "../validation/appointment.validation.js";

export async function createAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = CreateAppointmentInput.parse(body);

  const { id, ...appointmentInput } = payload;
  const baseAppointmentId = id ?? crypto.randomUUID();
  const appointments: AppointmentState[] = [];

  for (let index = 1; index <= env.appointmentsPerCreateRequest; index += 1) {
    const appointmentId = `${baseAppointmentId}-${String(index).padStart(3, "0")}`;
    const appointment = await appointmentClient(appointmentId).create(
      appointmentInput,
    );
    appointments.push(appointment);
  }

  return c.json(
    {
      count: appointments.length,
      appointments,
    },
    201,
  );
}

export async function getAppointment(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).get();

  return c.json(appointment);
}

export async function updateAppointment(c: Context) {
  const body = await c.req.json().catch(() => undefined);
  const payload = AppointmentInput.parse(body);

  const appointment = await appointmentClient(appointmentId(c)).update(payload);

  return c.json(appointment);
}

export async function markAppointmentArrived(c: Context) {
  const appointment = await appointmentClient(appointmentId(c)).markArrived();

  return c.json(appointment);
}

function appointmentClient(id: string) {
  return restateClient.objectClient<AppointmentObject>({ name: "Appointment" }, id);
}

function appointmentId(c: Context) {
  const id = c.req.param("id");
  if (!id) {
    throw new Error("Appointment id is required");
  }

  return id;
}
