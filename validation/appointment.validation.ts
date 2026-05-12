import { z } from "zod";

import {
  AppointmentInput,
  AppointmentPatchInput,
} from "../models/appointment.model.js";

/** Schema validate tạo appointment — mở rộng thêm id và idempotencyKey tùy chọn */
export const CreateAppointmentInput = AppointmentInput.extend({
  id: z.string().trim().min(1).optional(),
  idempotencyKey: z.string().trim().min(1).optional(),
});

/** Schema validate tạo hàng loạt appointment (stress test) */
export const BulkCreateAppointmentInput = z.object({
  count: z.number().int().min(1).max(500),
  startAt: z.string().datetime(),
  service: z.string().trim().min(1).default("General Checkup"),
  intervalMinutes: z.number().int().min(0).default(5),
});

/** Schema validate cập nhật appointment — dùng chung với model */
export const UpdateAppointmentInput = AppointmentPatchInput;

/** Schema validate query params khi lấy danh sách appointment */
export const ListAppointmentsQuery = z.object({
  status: z.enum(["booked", "cancelled"]).optional(),
  customerEmail: z.string().trim().email().optional(),
  limit: z.coerce.number().int().min(1).max(100).default(50),
});

export type CreateAppointmentInput = z.infer<typeof CreateAppointmentInput>;
export type BulkCreateAppointmentInput = z.infer<typeof BulkCreateAppointmentInput>;
export type UpdateAppointmentInput = z.infer<typeof UpdateAppointmentInput>;
export type ListAppointmentsQuery = z.infer<typeof ListAppointmentsQuery>;
