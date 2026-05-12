import {
  type Consumer,
  type EachMessagePayload,
  type ITopicConfig,
} from "kafkajs";

import { env } from "../../config/env.js";
import { kafka } from "../../config/kafka.js";
import {
  AppointmentEventEnvelope,
  type AppointmentEventEnvelope as AppointmentEventEnvelopeType,
} from "../../models/appointment.model.js";
import {
  ChatMessage,
  type ChatMessage as ChatMessageType,
} from "../../models/chat.model.js";
import { logger } from "../../utils/logger.js";

export {
  connectKafkaProducer,
  disconnectKafkaProducer,
  getKafkaProducer,
  kafkaProducerReady,
} from "../../config/kafka.js";

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
      {
        topic: env.kafkaChatTopic,
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

async function createKafkaConsumer(groupId: string) {
  const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
  });

  await consumer.connect();
  return consumer;
}

async function subscribeToAppointmentEvents(
  consumer: Consumer,
  eachMessage: (payload: EachMessagePayload) => Promise<void>,
) {
  await consumer.subscribe({
    topic: env.kafkaAppointmentTopic,
    fromBeginning: true,
  });

  await consumer.run({ eachMessage });
}

export async function runConsumer(options: {
  groupId: string;
  onEvent: (event: AppointmentEventEnvelopeType) => Promise<void>;
  onShutdown?: () => Promise<void>;
}) {
  const consumer = await createKafkaConsumer(options.groupId);

  logger.info(
    { topic: env.kafkaAppointmentTopic, groupId: options.groupId },
    "Consumer started",
  );

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info({ signal }, "Stopping consumer");
    await consumer.disconnect();
    await options.onShutdown?.();
  };

  process.once("SIGINT", (signal) => {
    void shutdown(signal).then(() => process.exit(0));
  });

  process.once("SIGTERM", (signal) => {
    void shutdown(signal).then(() => process.exit(0));
  });

  await subscribeToAppointmentEvents(consumer, async ({ message }) => {
    if (!message.value) return;
    const event = AppointmentEventEnvelope.parse(
      JSON.parse(message.value.toString("utf8")),
    );
    await options.onEvent(event);
  });
}

export async function runChatConsumer(options: {
  groupId: string;
  onMessage: (message: ChatMessageType) => Promise<void>;
  onShutdown?: () => Promise<void>;
}) {
  const consumer = await createKafkaConsumer(options.groupId);

  logger.info(
    { topic: env.kafkaChatTopic, groupId: options.groupId },
    "Chat consumer started",
  );

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info({ signal }, "Stopping chat consumer");
    await consumer.disconnect();
    await options.onShutdown?.();
  };

  process.once("SIGINT", (signal) => {
    void shutdown(signal).then(() => process.exit(0));
  });

  process.once("SIGTERM", (signal) => {
    void shutdown(signal).then(() => process.exit(0));
  });

  await consumer.subscribe({
    topic: env.kafkaChatTopic,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const chatMessage = ChatMessage.parse(
        JSON.parse(message.value.toString("utf8")),
      );
      await options.onMessage(chatMessage);
    },
  });
}
