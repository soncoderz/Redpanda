import "dotenv/config";

import { SchemaType } from "@kafkajs/confluent-schema-registry";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { env } from "../config/env.js";
import { schemaRegistry } from "../config/schema-registry.js";
import { logger } from "../utils/logger.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const schemasDir = resolve(__dirname, "..", "schemas");

/** Danh sách schema cần đăng ký: subject name + file path */
const schemas = [
  {
    subject: `${env.kafkaAppointmentTopic}-value`,
    file: "appointment-event.schema.json",
  },
  {
    subject: `${env.kafkaChatTopic}-value`,
    file: "chat-message.schema.json",
  },
];

async function registerSchemas() {
  logger.info(
    { registryUrl: env.schemaRegistryUrl },
    "Registering JSON schemas with Schema Registry",
  );

  for (const { subject, file } of schemas) {
    const schemaPath = resolve(schemasDir, file);
    const schemaJson = readFileSync(schemaPath, "utf-8");

    const { id } = await schemaRegistry.register(
      {
        type: SchemaType.JSON,
        schema: schemaJson,
      },
      { subject },
    );

    logger.info({ subject, schemaId: id, file }, "Schema registered");
  }

  logger.info("All schemas registered successfully");
}

registerSchemas().catch((error) => {
  logger.error({ err: error }, "Failed to register schemas");
  process.exit(1);
});
