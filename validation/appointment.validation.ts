import { z } from "zod";

import {
  AppointmentInput,
  AppointmentPatchInput,
} from "../models/appointment.model.js";

export const CreateAppointmentInput = AppointmentInput.extend({
  id: z.string().trim().min(1).optional(),
  idempotencyKey: z.string().trim().min(1).optional(),
});

export const UpdateAppointmentInput = AppointmentPatchInput;

export const ListAppointmentsQuery = z.object({
  status: z.enum(["booked", "cancelled"]).optional(),
  customerEmail: z.string().trim().email().optional(),
  limit: z.coerce.number().int().min(1).max(100).default(50),
});

export type CreateAppointmentInput = z.infer<typeof CreateAppointmentInput>;
export type UpdateAppointmentInput = z.infer<typeof UpdateAppointmentInput>;
export type ListAppointmentsQuery = z.infer<typeof ListAppointmentsQuery>;
