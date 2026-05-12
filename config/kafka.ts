import {
  Kafka,
  logLevel,
  type Producer,
  type SASLOptions,
} from "kafkajs";

import { env } from "./env.js";
import { logger } from "../utils/logger.js";

const sasl: SASLOptions | undefined =
  env.kafkaUsername && env.kafkaPassword
    ? {
        mechanism: "plain",
        username: env.kafkaUsername,
        password: env.kafkaPassword,
      }
    : undefined;

export const kafka = new Kafka({
  clientId: env.kafkaClientId,
  brokers: env.kafkaBrokers,
  ssl: env.kafkaSsl,
  sasl,
  logLevel: logLevel.WARN,
});

let producer: Producer | undefined;
let producerConnected = false;

export async function connectKafkaProducer(): Promise<Producer> {
  if (producerConnected) {
    return getKafkaProducer();
  }

  producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
    retry: {
      retries: 8,
    },
  });

  await producer.connect();
  producerConnected = true;
  logger.info({ brokers: env.kafkaBrokers }, "Kafka producer connected");

  return producer;
}

export async function getKafkaProducer(): Promise<Producer> {
  if (!producer || !producerConnected) {
    return connectKafkaProducer();
  }

  return producer;
}

export async function disconnectKafkaProducer() {
  if (producer && producerConnected) {
    await producer.disconnect();
    producerConnected = false;
  }
}

export function kafkaProducerReady() {
  return producerConnected;
}
