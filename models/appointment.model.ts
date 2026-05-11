import { z } from "zod";

export const AppointmentInput = z.object({
  customerName: z.string().min(1),
  customerEmail: z.string().email(),
  service: z.string().min(1),
  startAt: z.string().datetime(),
  note: z.string().optional(),
});

export const ReminderType = z.enum(["before", "atTime", "after"]);

const ReminderInvocations = z.object({
  before: z.string(),
  atTime: z.string(),
  after: z.string(),
});

const EmailDeliveryStatus = z.object({
  version: z.number().int().min(1),
  sent: z.boolean(),
  scheduled: z.boolean(),
  invocationId: z.string().optional(),
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

const EmailStatus = z.object({
  before: EmailDeliveryStatus,
  atTime: EmailDeliveryStatus,
  after: EmailDeliveryStatus,
});

const AppointmentEvent = z.object({
  type: z.enum([
    "created",
    "updated",
    "marked_arrived",
    "email_scheduled",
    "email_cancelled",
    "email_queued",
    "email_started",
    "email_sent",
    "email_skipped",
    "email_failed",
  ]),
  at: z.string(),
  version: z.number().int().min(1),
  reminder: ReminderType.optional(),
  invocationId: z.string().optional(),
  details: z.record(z.string(), z.unknown()).optional(),
});

export const AppointmentEmailPayload = AppointmentInput.extend({
  id: z.string(),
  version: z.number().int().min(1),
});

export const EmailDeliveryRequest = z.object({
  reminder: ReminderType,
  version: z.number().int().min(1),
  jobId: z.string().min(1),
});

export const EmailDeliveryStartResult = z.object({
  shouldSend: z.boolean(),
  reason: z.string().optional(),
});

const EmailDeliveryResult = z.discriminatedUnion("status", [
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

export const EmailDeliveryResultInput = EmailDeliveryRequest.extend({
  result: EmailDeliveryResult,
});

export const AppointmentState = AppointmentInput.extend({
  id: z.string(),
  version: z.number().int().min(1),
  status: z.enum(["booked", "arrived"]),
  createdAt: z.string(),
  updatedAt: z.string(),
  arrivedAt: z.string().optional(),
  reminderInvocations: ReminderInvocations,
  emailStatus: EmailStatus,
  history: z.array(AppointmentEvent),
});

export type AppointmentInput = z.infer<typeof AppointmentInput>;
export type ReminderType = z.infer<typeof ReminderType>;
export type AppointmentEmailPayload = z.infer<typeof AppointmentEmailPayload>;
export type EmailDeliveryRequest = z.infer<typeof EmailDeliveryRequest>;
export type EmailDeliveryResultInput = z.infer<typeof EmailDeliveryResultInput>;
export type AppointmentState = z.infer<typeof AppointmentState>;
