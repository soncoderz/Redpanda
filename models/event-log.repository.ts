import mongoose, { Schema, type HydratedDocument } from "mongoose";

import type { AppointmentEventEnvelope, AppointmentEventType } from "./appointment.model.js";

export interface EventLogRecord {
  eventId: string;
  type: AppointmentEventType;
  appointmentId: string;
  version: number;
  processedAt: string;
}

export type EventLogDocument = HydratedDocument<EventLogRecord>;

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

const EventLogModel =
  mongoose.models.EventLog ?? mongoose.model<EventLogRecord>("EventLog", EventLogSchema);

export async function recordProcessedEvent(event: AppointmentEventEnvelope) {
  try {
    await EventLogModel.create({
      eventId: event.eventId,
      type: event.type,
      appointmentId: event.appointmentId,
      version: event.version,
      processedAt: new Date().toISOString(),
    });

    return { inserted: true };
  } catch (error) {
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
