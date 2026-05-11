import { env } from "../config/env.js";
import {
  AppointmentEventEnvelope,
  type AppointmentEventEnvelope as AppointmentEventEnvelopeType,
} from "../models/appointment.model.js";
import { getKafkaProducer } from "./kafka.service.js";
import { emitRealtimeEvent } from "./realtime.service.js";

export async function publishAppointmentEvent(
  event: AppointmentEventEnvelopeType,
) {
  const parsed = AppointmentEventEnvelope.parse(event);
  const producer = await getKafkaProducer();

  await producer.send({
    topic: env.kafkaAppointmentTopic,
    acks: -1,
    messages: [
      {
        key: parsed.appointmentId,
        value: JSON.stringify(parsed),
        headers: {
          eventId: parsed.eventId,
          eventType: parsed.type,
        },
      },
    ],
  });

  emitRealtimeEvent(parsed);
}
