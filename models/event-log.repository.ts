import mongoose, { Schema, type HydratedDocument } from "mongoose";

import type { AppointmentEventEnvelope, AppointmentEventType } from "./appointment.model.js";

/** Interface MongoDB document cho event log */
export interface EventLogRecord {
  eventId: string;
  type: AppointmentEventType;
  appointmentId: string;
  version: number;
  processedAt: string;
}

export type EventLogDocument = HydratedDocument<EventLogRecord>;

/** Schema Mongoose cho collection event_logs */
const EventLogSchema = new Schema<EventLogRecord>(
  {
    eventId: { type: String, required: true, unique: true },
    type: {
      type: String,
      required: true,
      enum: [
        "appointment.created",
        "appointment.updated",
        "appointment.cancelled",
        "reminder.sent",
      ],
    },
    appointmentId: { type: String, required: true, index: true },
    version: { type: Number, required: true, min: 1 },
    processedAt: { type: String, required: true },
  },
  {
    collection: "event_logs",
    versionKey: false,
  },
);

/** Mongoose model cho event log — tái sử dụng nếu đã tồn tại */
const EventLogModel =
  mongoose.models.EventLog ?? mongoose.model<EventLogRecord>("EventLog", EventLogSchema);

/** Lưu event đã xử lý vào MongoDB — idempotent qua unique eventId (duplicate = skip) */
export async function recordProcessedEvent(event: AppointmentEventEnvelope) {
  try {
    // Tạo document mới trong collection event_logs
    await EventLogModel.create({
      eventId: event.eventId,
      type: event.type,
      appointmentId: event.appointmentId,
      version: event.version,
      processedAt: new Date().toISOString(),
    });

    return { inserted: true };
  } catch (error) {
    // Lỗi duplicate key (11000) = event đã xử lý rồi → skip
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === 11000
    ) {
      return { inserted: false };
    }

    throw error;
  }
}
