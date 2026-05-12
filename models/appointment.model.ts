import { z } from "zod";

/** Loại reminder: trước giờ hẹn / đúng giờ / sau giờ hẹn */
export const ReminderType = z.enum(["before", "atTime", "after"]);

/** Schema validate dữ liệu đầu vào tạo appointment */
export const AppointmentInput = z.object({
  customerName: z.string().trim().min(1),
  customerEmail: z.string().trim().email(),
  service: z.string().trim().min(1),
  startAt: z.string().datetime(),
  note: z.string().trim().max(2_000).optional(),
});

/** Schema validate dữ liệu cập nhật appointment (partial — chỉ cần ít nhất 1 field) */
export const AppointmentPatchInput = AppointmentInput.partial().refine(
  (value) => Object.keys(value).length > 0,
  "At least one appointment field is required",
);

/** Trạng thái appointment: đang đặt hoặc đã hủy */
export const AppointmentStatus = z.enum(["booked", "cancelled"]);

/** Trạng thái gửi email reminder — theo dõi từ lúc schedule đến khi gửi xong */
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

/** Trạng thái 3 loại reminder (before, atTime, after) */
export const ReminderStatus = z.object({
  before: ReminderDeliveryStatus,
  atTime: ReminderDeliveryStatus,
  after: ReminderDeliveryStatus,
});

/** Một dòng lịch sử thay đổi của appointment */
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

/** Dữ liệu appointment gửi kèm email (subset của AppointmentState) */
export const AppointmentEmailPayload = AppointmentInput.extend({
  id: z.string().min(1),
  version: z.number().int().min(1),
});

/** Toàn bộ trạng thái appointment trong Restate Virtual Object */
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

/** Dữ liệu đầu vào workflow tạo appointment (từ API controller) */
export const CreateAppointmentWorkflowInput = AppointmentInput.extend({
  idempotencyKey: z.string().trim().min(1).optional(),
});

/** Dữ liệu đầu vào workflow cập nhật appointment */
export const UpdateAppointmentWorkflowInput = AppointmentPatchInput;

/** Yêu cầu bắt đầu gửi reminder — BullMQ worker gọi Restate để validate */
export const ReminderDeliveryRequest = z.object({
  reminder: ReminderType,
  version: z.number().int().min(1),
  jobId: z.string().min(1),
});

/** Kết quả validate: có nên gửi email hay không */
export const ReminderDeliveryStartResult = z.object({
  shouldSend: z.boolean(),
  reason: z.string().optional(),
});

/** Kết quả gửi email: sent / skipped / failed */
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

/** Dữ liệu BullMQ worker gửi về Restate sau khi xử lý email */
export const ReminderDeliveryResultInput = ReminderDeliveryRequest.extend({
  result: ReminderDeliveryResult,
});

/** Dữ liệu Restate delayed send gửi tới handler sendReminder */
export const SendReminderInput = z.object({
  reminder: ReminderType,
  version: z.number().int().min(1),
  scheduledFor: z.string().datetime(),
});

/** Loại event appointment publish lên Kafka */
export const AppointmentEventType = z.enum([
  "appointment.created",
  "appointment.updated",
  "appointment.cancelled",
  "reminder.sent",
]);

/** Envelope chứa event khi publish lên Kafka topic */
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
