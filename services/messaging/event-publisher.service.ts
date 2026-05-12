import { env } from "../../config/env.js";
import {
  AppointmentEventEnvelope,
  type AppointmentEventEnvelope as AppointmentEventEnvelopeType,
} from "../../models/appointment.model.js";
import { getKafkaProducer } from "./kafka.service.js";
import { emitRealtimeEvent } from "./realtime.service.js";

/** Gửi event tới Redpanda topic + đẩy SSE real-time cho browser */
export async function publishAppointmentEvent(
  event: AppointmentEventEnvelopeType,
) {
  const parsed = AppointmentEventEnvelope.parse(event);

  // Gửi message tới Redpanda (key = appointmentId để cùng partition)
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

  // Đẩy event tới SSE stream (browser) qua EventEmitter
  emitRealtimeEvent(parsed);
}
