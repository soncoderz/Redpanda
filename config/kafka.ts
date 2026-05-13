import {
  Kafka,
  logLevel,
  type Producer,
  type SASLOptions,
} from "kafkajs";

import { env } from "./env.js";
import { logger } from "../utils/logger.js";

///// Cấu hình SASL nếu có username/password — SCRAM-SHA-256 khớp với setup-acl.sh
const sasl: SASLOptions | undefined =
  env.kafkaUsername && env.kafkaPassword
    ? {
        mechanism: "scram-sha-256",
        username: env.kafkaUsername,
        password: env.kafkaPassword,
      }
    : undefined;

/** Kafka client kết nối tới Redpanda broker */
export const kafka = new Kafka({
  clientId: env.kafkaClientId,
  brokers: env.kafkaBrokers,
  ssl: env.kafkaSsl,
  sasl,
  logLevel: logLevel.WARN,
});

let producer: Producer | undefined;
let producerConnected = false;

/** Tạo và kết nối Kafka producer (idempotent — không gửi trùng message) */
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

/** Lấy producer đang kết nối, tự connect nếu chưa */
export async function getKafkaProducer(): Promise<Producer> {
  if (!producer || !producerConnected) {
    return connectKafkaProducer();
  }

  return producer;
}

/** Ngắt kết nối producer */
export async function disconnectKafkaProducer() {
  if (producer && producerConnected) {
    await producer.disconnect();
    producerConnected = false;
  }
}

/** Kiểm tra producer đã sẵn sàng chưa (dùng cho health check) */
export function kafkaProducerReady() {
  return producerConnected;
}
