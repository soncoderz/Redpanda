import { z } from "zod";

import { AppointmentInput } from "../models/appointment.model.js";

export const CreateAppointmentInput = AppointmentInput.extend({
  id: z.string().min(1).optional(),
});

export type CreateAppointmentInput = z.infer<typeof CreateAppointmentInput>;
