import { AppointmentModel, type AppointmentRecord } from "./appointment.schema.js";
import {
  AppointmentState,
  type AppointmentPatchInput,
  type AppointmentState as AppointmentStateType,
} from "./appointment.model.js";

/** Lỗi repository: not_found hoặc duplicate */
export class AppointmentRepositoryError extends Error {
  constructor(
    message: string,
    public readonly code: "not_found" | "duplicate",
  ) {
    super(message);
  }
}

/** Tạo appointment mới trong MongoDB — throw duplicate nếu đã tồn tại */
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

/** Tìm appointment theo ID — trả undefined nếu không tìm thấy */
export async function findAppointmentById(id: string) {
  const doc = await AppointmentModel.findOne({ appointmentId: id }).lean();
  return doc ? toState(doc) : undefined;
}

/** Ghi đè toàn bộ dữ liệu appointment trong MongoDB (findOneAndUpdate) */
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

/** Lấy danh sách appointments — filter theo status, email, giới hạn số lượng */
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

/** Áp dụng patch input vào appointment (merge fields) */
export function applyAppointmentPatch(
  appointment: AppointmentStateType,
  patch: AppointmentPatchInput,
) {
  return {
    ...appointment,
    ...patch,
  };
}

/** Chuyển AppointmentState → MongoDB record (id → appointmentId) */
function toRecord(appointment: AppointmentStateType): AppointmentRecord {
  const { id, ...rest } = appointment;
  return {
    ...rest,
    appointmentId: id,
  };
}

/** Chuyển MongoDB record → AppointmentState (appointmentId → id) và validate schema */
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

/** Kiểm tra lỗi MongoDB duplicate key (code 11000) */
function isMongoDuplicateKeyError(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === 11000
  );
}
