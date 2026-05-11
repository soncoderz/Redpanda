import { EventLogModel } from "../models/event-log.schema.js";
import type { AppointmentEventEnvelope } from "../models/appointment.model.js";

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
    if (isMongoDuplicateKeyError(error)) {
      return { inserted: false };
    }

    throw error;
  }
}

function isMongoDuplicateKeyError(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === 11000
  );
}
