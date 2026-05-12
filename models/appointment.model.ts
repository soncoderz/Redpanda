import { z } from "zod";

export const ReminderType = z.enum(["before", "atTime", "after"]);

export const AppointmentInput = z.object({
  customerName: z.string().trim().min(1),
  customerEmail: z.string().trim().email(),
  service: z.string().trim().min(1),
  startAt: z.string().datetime(),
  note: z.string().trim().max(2_000).optional(),
});

export const AppointmentPatchInput = AppointmentInput.partial().refine(
  (value) => Object.keys(value).length > 0,
  "At least one appointment field is required",
);

export const AppointmentStatus = z.enum(["booked", "cancelled"]);

export const ReminderDeliveryStatus = z.object({
  version: z.number().int().min(1),
  sent: z.boolean(),
  scheduled: z.boolean(),
  jobId: z.string().optional(),
  scheduledAt: z.string().optional(),
  scheduledFor: z.string().optional(),
  queuedAt: z.string().optional(),
  startedAt: z.string().optional(),
  sentAt: z.string().optional(),
  skippedAt: z.string().optional(),
  failedAt: z.string().optional(),
  canceledAt: z.string().optional(),
  error: z.string().optional(),
});

export const ReminderStatus = z.object({
  before: ReminderDeliveryStatus,
  atTime: ReminderDeliveryStatus,
  after: ReminderDeliveryStatus,
});

export const AppointmentHistoryEntry = z.object({
  type: z.enum([
    "created",
    "updated",
    "cancelled",
    "reminder_scheduled",
    "reminder_cancelled",
    "reminder_started",
    "reminder_sent",
    "reminder_skipped",
    "reminder_failed",
  ]),
  at: z.string(),
  version: z.number().int().min(1),
  reminder: ReminderType.optional(),
  jobId: z.string().optional(),
  details: z.record(z.string(), z.unknown()).optional(),
});

export const AppointmentEmailPayload = AppointmentInput.extend({
  id: z.string().min(1),
  version: z.number().int().min(1),
});

export const AppointmentState = AppointmentInput.extend({
  id: z.string().min(1),
  version: z.number().int().min(1),
  status: AppointmentStatus,
  idempotencyKey: z.string().min(1).optional(),
  createdAt: z.string(),
  updatedAt: z.string(),
  cancelledAt: z.string().optional(),
  reminders: ReminderStatus,
  history: z.array(AppointmentHistoryEntry),
});

export const CreateAppointmentWorkflowInput = AppointmentInput.extend({
  idempotencyKey: z.string().trim().min(1).optional(),
});

export const UpdateAppointmentWorkflowInput = AppointmentPatchInput;

export const ReminderDeliveryRequest = z.object({
  reminder: ReminderType,
  version: z.number().int().min(1),
  jobId: z.string().min(1),
});

export const ReminderDeliveryStartResult = z.object({
  shouldSend: z.boolean(),
  reason: z.string().optional(),
});

const ReminderDeliveryResult = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("sent"),
  }),
  z.object({
    status: z.literal("skipped"),
    reason: z.string(),
    statusCode: z.number().optional(),
    responseBody: z.unknown().optional(),
  }),
  z.object({
    status: z.literal("failed"),
    error: z.string(),
  }),
]);

export const ReminderDeliveryResultInput = ReminderDeliveryRequest.extend({
  result: ReminderDeliveryResult,
});

export const SendReminderInput = z.object({
  reminder: ReminderType,
  version: z.number().int().min(1),
  scheduledFor: z.string().datetime(),
});

export const AppointmentEventType = z.enum([
  "appointment.created",
  "appointment.updated",
  "appointment.cancelled",
  "reminder.sent",
]);

export const AppointmentEventEnvelope = z.object({
  eventId: z.string().min(1),
  type: AppointmentEventType,
  appointmentId: z.string().min(1),
  version: z.number().int().min(1),
  occurredAt: z.string(),
  payload: z.record(z.string(), z.unknown()),
});

export type AppointmentInput = z.infer<typeof AppointmentInput>;
export type AppointmentPatchInput = z.infer<typeof AppointmentPatchInput>;
export type AppointmentStatus = z.infer<typeof AppointmentStatus>;
export type ReminderType = z.infer<typeof ReminderType>;
export type ReminderDeliveryStatus = z.infer<
  typeof ReminderDeliveryStatus
>;
export type AppointmentHistoryEntry = z.infer<
  typeof AppointmentHistoryEntry
>;
export type AppointmentEmailPayload = z.infer<typeof AppointmentEmailPayload>;
export type AppointmentState = z.infer<typeof AppointmentState>;
export type CreateAppointmentWorkflowInput = z.infer<
  typeof CreateAppointmentWorkflowInput
>;
export type UpdateAppointmentWorkflowInput = z.infer<
  typeof UpdateAppointmentWorkflowInput
>;
export type ReminderDeliveryRequest = z.infer<
  typeof ReminderDeliveryRequest
>;
export type ReminderDeliveryResultInput = z.infer<
  typeof ReminderDeliveryResultInput
>;
export type SendReminderInput = z.infer<typeof SendReminderInput>;
export type AppointmentEventType = z.infer<typeof AppointmentEventType>;
export type AppointmentEventEnvelope = z.infer<
  typeof AppointmentEventEnvelope
>;
