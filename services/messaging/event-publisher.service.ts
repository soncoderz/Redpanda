import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { env } from "../../config/env.js";
import {
  encodeWithSchema,
  getOrRegisterSchema,
} from "../../config/schema-registry.js";
import {
  AppointmentEventEnvelope,
  type AppointmentEventEnvelope as AppointmentEventEnvelopeType,
} from "../../models/appointment.model.js";
import { getKafkaProducer } from "./kafka.service.js";
import { emitRealtimeEvent } from "./realtime.service.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** JSON Schema cho appointment event — dùng để register lên Schema Registry */
const appointmentEventSchema = JSON.parse(
  readFileSync(
    resolve(__dirname, "..", "..", "schemas", "appointment-event.schema.json"),
    "utf-8",
  ),
);

/** Subject name cho Schema Registry (convention: <topic>-value) */
const SUBJECT = `${env.kafkaAppointmentTopic}-value`;

/** Gửi event tới Redpanda topic + đẩy SSE real-time cho browser
 *
 *  Luồng: validate Zod → register/get schema ID → encode Confluent wire format → send
 *  Wire format: [0x00][4 byte schema ID][JSON payload]
 *  → Consumer đọc schema ID từ header → fetch schema từ registry → decode + validate
 */
export async function publishAppointmentEvent(
  event: AppointmentEventEnvelopeType,
) {
  const parsed = AppointmentEventEnvelope.parse(event);

  // Register schema (hoặc lấy từ cache nếu đã register)
  const schemaId = await getOrRegisterSchema(SUBJECT, appointmentEventSchema);

  // Encode payload theo Confluent wire format (magic byte + schema ID + JSON)
  const encodedValue = await encodeWithSchema(schemaId, parsed as unknown as Record<string, unknown>);

  // Gửi message tới Redpanda (key = appointmentId để cùng partition)
  const producer = await getKafkaProducer();
  await producer.send({
    topic: env.kafkaAppointmentTopic,
    acks: -1,
    messages: [
      {
        key: parsed.appointmentId,
        value: encodedValue,
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
