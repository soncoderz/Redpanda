import mongoose, { Schema, type HydratedDocument } from "mongoose";

import type { AppointmentEventType } from "./appointment.model.js";

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

export const EventLogModel =
  mongoose.models.EventLog ?? mongoose.model<EventLogRecord>("EventLog", EventLogSchema);
