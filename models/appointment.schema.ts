import mongoose, { Schema, type HydratedDocument } from "mongoose";

import type {
  AppointmentHistoryEntry,
  AppointmentState,
  ReminderDeliveryStatus,
} from "./appointment.model.js";

export interface AppointmentRecord
  extends Omit<AppointmentState, "id"> {
  appointmentId: string;
}

export type AppointmentDocument = HydratedDocument<AppointmentRecord>;

const ReminderDeliveryStatusSchema = new Schema<ReminderDeliveryStatus>(
  {
    version: { type: Number, required: true, min: 1 },
    sent: { type: Boolean, required: true },
    scheduled: { type: Boolean, required: true },
    jobId: { type: String },
    scheduledAt: { type: String },
    scheduledFor: { type: String },
    queuedAt: { type: String },
    startedAt: { type: String },
    sentAt: { type: String },
    skippedAt: { type: String },
    failedAt: { type: String },
    canceledAt: { type: String },
    error: { type: String },
  },
  { _id: false },
);

const AppointmentHistoryEntrySchema = new Schema<AppointmentHistoryEntry>(
  {
    type: {
      type: String,
      required: true,
      enum: [
        "created",
        "updated",
        "cancelled",
        "reminder_scheduled",
        "reminder_cancelled",
        "reminder_started",
        "reminder_sent",
        "reminder_skipped",
        "reminder_failed",
      ],
    },
    at: { type: String, required: true },
    version: { type: Number, required: true, min: 1 },
    reminder: { type: String, enum: ["before", "atTime", "after"] },
    jobId: { type: String },
    details: { type: Schema.Types.Mixed },
  },
  { _id: false, minimize: false },
);

const AppointmentSchema = new Schema<AppointmentRecord>(
  {
    appointmentId: { type: String, required: true, unique: true },
    idempotencyKey: { type: String, unique: true, sparse: true },
    customerName: { type: String, required: true, trim: true },
    customerEmail: { type: String, required: true, trim: true, lowercase: true },
    service: { type: String, required: true, trim: true },
    startAt: { type: String, required: true },
    note: { type: String, trim: true },
    status: {
      type: String,
      required: true,
      enum: ["booked", "cancelled"],
      index: true,
    },
    version: { type: Number, required: true, min: 1 },
    createdAt: { type: String, required: true },
    updatedAt: { type: String, required: true },
    cancelledAt: { type: String },
    reminders: {
      before: { type: ReminderDeliveryStatusSchema, required: true },
      atTime: { type: ReminderDeliveryStatusSchema, required: true },
      after: { type: ReminderDeliveryStatusSchema, required: true },
    },
    history: {
      type: [AppointmentHistoryEntrySchema],
      required: true,
      default: [],
    },
  },
  {
    collection: "appointments",
    minimize: false,
    optimisticConcurrency: true,
  },
);

AppointmentSchema.index({ startAt: 1 });
AppointmentSchema.index({ customerEmail: 1, startAt: 1 });

export const AppointmentModel =
  mongoose.models.Appointment ??
  mongoose.model<AppointmentRecord>("Appointment", AppointmentSchema);
