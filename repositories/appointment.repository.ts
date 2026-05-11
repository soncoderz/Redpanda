import { AppointmentModel, type AppointmentRecord } from "../models/appointment.schema.js";
import {
  AppointmentState,
  type AppointmentPatchInput,
  type AppointmentState as AppointmentStateType,
} from "../models/appointment.model.js";

export class AppointmentRepositoryError extends Error {
  constructor(
    message: string,
    public readonly code: "not_found" | "duplicate",
  ) {
    super(message);
  }
}

export async function createAppointmentRecord(
  appointment: AppointmentStateType,
) {
  try {
    const doc = await AppointmentModel.create(toRecord(appointment));
    return toState(doc.toObject());
  } catch (error) {
    if (isMongoDuplicateKeyError(error)) {
      throw new AppointmentRepositoryError(
        `Appointment ${appointment.id} already exists`,
        "duplicate",
      );
    }

    throw error;
  }
}

export async function findAppointmentById(id: string) {
  const doc = await AppointmentModel.findOne({ appointmentId: id }).lean();
  return doc ? toState(doc) : undefined;
}

export async function requireAppointmentById(id: string) {
  const appointment = await findAppointmentById(id);
  if (!appointment) {
    throw new AppointmentRepositoryError(
      `Appointment ${id} does not exist`,
      "not_found",
    );
  }

  return appointment;
}

export async function replaceAppointmentRecord(
  appointment: AppointmentStateType,
) {
  const doc = await AppointmentModel.findOneAndUpdate(
    { appointmentId: appointment.id },
    { $set: toRecord(appointment) },
    { new: true, runValidators: true },
  ).lean();

  if (!doc) {
    throw new AppointmentRepositoryError(
      `Appointment ${appointment.id} does not exist`,
      "not_found",
    );
  }

  return toState(doc);
}

export async function listAppointments(input: {
  status?: AppointmentStateType["status"];
  customerEmail?: string;
  limit: number;
}) {
  const query: Record<string, unknown> = {};
  if (input.status) {
    query.status = input.status;
  }
  if (input.customerEmail) {
    query.customerEmail = input.customerEmail.toLowerCase();
  }

  const docs = await AppointmentModel.find(query)
    .sort({ startAt: 1 })
    .limit(input.limit)
    .lean();

  return docs.map(toState);
}

export function applyAppointmentPatch(
  appointment: AppointmentStateType,
  patch: AppointmentPatchInput,
) {
  return {
    ...appointment,
    ...patch,
  };
}

function toRecord(appointment: AppointmentStateType): AppointmentRecord {
  const { id, ...rest } = appointment;
  return {
    ...rest,
    appointmentId: id,
  };
}

function toState(record: AppointmentRecord | Record<string, unknown>) {
  const recordWithId = record as AppointmentRecord;
  return AppointmentState.parse({
    id: recordWithId.appointmentId,
    version: recordWithId.version,
    status: recordWithId.status,
    idempotencyKey: recordWithId.idempotencyKey,
    customerName: recordWithId.customerName,
    customerEmail: recordWithId.customerEmail,
    service: recordWithId.service,
    startAt: recordWithId.startAt,
    note: recordWithId.note,
    createdAt: recordWithId.createdAt,
    updatedAt: recordWithId.updatedAt,
    cancelledAt: recordWithId.cancelledAt,
    reminders: recordWithId.reminders,
    history: recordWithId.history ?? [],
  });
}

function isMongoDuplicateKeyError(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === 11000
  );
}
