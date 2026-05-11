import {
  Kafka,
  logLevel,
  type Consumer,
  type EachMessagePayload,
  type ITopicConfig,
  type Producer,
  type SASLOptions,
} from "kafkajs";

import { env } from "../../config/env.js";
import { logger } from "../../utils/logger.js";

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

export async function ensureKafkaTopics() {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const topics: ITopicConfig[] = [
      {
        topic: env.kafkaAppointmentTopic,
        numPartitions: env.kafkaTopicPartitions,
        replicationFactor: env.kafkaTopicReplicationFactor,
      },
    ];

    await admin.createTopics({
      waitForLeaders: true,
      topics,
    });

    logger.info(
      { topics: topics.map((topic) => topic.topic) },
      "Kafka topics are ready",
    );
  } finally {
    await admin.disconnect();
  }
}

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

export async function createKafkaConsumer(groupId: string) {
  const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
  });

  await consumer.connect();
  return consumer;
}

export async function subscribeToAppointmentEvents(
  consumer: Consumer,
  eachMessage: (payload: EachMessagePayload) => Promise<void>,
) {
  await consumer.subscribe({
    topic: env.kafkaAppointmentTopic,
    fromBeginning: true,
  });

  await consumer.run({ eachMessage });
}
