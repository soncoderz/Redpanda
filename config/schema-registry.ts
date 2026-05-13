import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";

import { env } from "./env.js";
import { logger } from "../utils/logger.js";

/** Schema Registry client kết nối tới Redpanda Schema Registry */
export const schemaRegistry = new SchemaRegistry({
  host: env.schemaRegistryUrl,
});

/** Cache schema ID sau khi register — tránh gọi registry mỗi lần publish */
const schemaIdCache = new Map<string, number>();

/** Lấy schema ID đã cache, register nếu chưa có */
export async function getOrRegisterSchema(
  subject: string,
  schema: Record<string, unknown>,
): Promise<number> {
  const cached = schemaIdCache.get(subject);
  if (cached) return cached;

  const { id } = await schemaRegistry.register(
    {
      type: SchemaType.JSON,
      schema: JSON.stringify(schema),
    },
    { subject },
  );

  schemaIdCache.set(subject, id);
  logger.info({ subject, schemaId: id }, "Schema registered");
  return id;
}

/** Encode payload với schema ID header (Confluent wire format: magic byte + 4 byte schema ID + JSON) */
export async function encodeWithSchema(
  schemaId: number,
  payload: Record<string, unknown>,
): Promise<Buffer> {
  return schemaRegistry.encode(schemaId, payload);
}

/** Decode message từ Confluent wire format → JSON object */
export async function decodeWithSchema<T = unknown>(
  buffer: Buffer,
): Promise<T> {
  return schemaRegistry.decode(buffer) as Promise<T>;
}
