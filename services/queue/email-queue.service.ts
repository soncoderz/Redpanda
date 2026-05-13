import { z } from "zod";

import { env } from "../../config/env.js";
import {
  appointmentEmailQueue,
  EMAIL_QUEUE_NAME,
} from "../../config/queues.js";
import type {
  AppointmentEmailPayload,
  ReminderType,
} from "../../models/appointment.model.js";

export { EMAIL_QUEUE_NAME, appointmentEmailQueue };

/** Schema validate dữ liệu job email trong BullMQ */
export const AppointmentEmailJobData = z.object({
  reminder: z.enum(["before", "atTime", "after"]),
  scheduledFor: z.string().datetime(),
  appointment: z.object({
    id: z.string().min(1),
    version: z.number().int().min(1),
    customerName: z.string().min(1),
    customerEmail: z.string().email(),
    service: z.string().min(1),
    startAt: z.string().datetime(),
    note: z.string().optional(),
  }),
});

export type AppointmentEmailJobData = z.infer<typeof AppointmentEmailJobData>;
type EmailReminderType = AppointmentEmailJobData["reminder"];

/** Đẩy job email ngay lập tức vào BullMQ (không delay — Restate đã xử lý delay) */
export async function enqueueImmediateEmail(input: {
  reminder: ReminderType;
  scheduledFor: string;
  appointment: AppointmentEmailPayload;
}) {
  const data = AppointmentEmailJobData.parse({
    reminder: input.reminder,
    scheduledFor: input.scheduledFor,
    appointment: input.appointment,
  });

  // Job ID deterministic → không tạo trùng job cho cùng appointment + version + reminder
  const jobId = appointmentEmailJobId(
    data.appointment.id,
    data.appointment.version,
    data.reminder,
  );
  const job = await appointmentEmailQueue.add("send", data, { jobId });

  return { jobId: job.id ?? jobId };
}

/** Tạo job ID deterministic: appointment-email-{id}-{version}-{reminder} */
function appointmentEmailJobId(
  appointmentId: string,
  version: number,
  reminder: EmailReminderType,
) {
  const encodedAppointmentId = Buffer.from(appointmentId).toString("base64url");
  return `appointment-email-${encodedAppointmentId}-${version}-${reminder}`;
}

/** Đóng queue connection */
export async function closeAppointmentEmailQueue() {
  await appointmentEmailQueue.close();
}
