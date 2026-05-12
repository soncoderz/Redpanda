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

/** Tạo các Kafka topic cần thiết (appointment-events, chat-messages) nếu chưa tồn tại */
export async function ensureKafkaTopics() {
  // Kết nối Kafka admin client để quản lý topic
  const admin = kafka.admin();
  await admin.connect();

  try {
    // Danh sách topic cần tạo: appointment-events và chat-messages
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

    // Tạo topic và chờ leader partition sẵn sàng
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

/** Tạo Kafka consumer với group ID chỉ định và kết nối tới broker */
async function createKafkaConsumer(groupId: string) {
  const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
  });

  await consumer.connect();
  return consumer;
}

/** Subscribe consumer vào topic appointment-events và chạy handler cho mỗi message */
async function subscribeToAppointmentEvents(
  consumer: Consumer,
  eachMessage: (payload: EachMessagePayload) => Promise<void>,
) {
  // Đọc từ đầu topic để không bỏ sót event nào
  await consumer.subscribe({
    topic: env.kafkaAppointmentTopic,
    fromBeginning: true,
  });

  await consumer.run({ eachMessage });
}

/** Chạy consumer đọc appointment events từ Kafka — dùng cho analytics và telegram consumer */
export async function runConsumer(options: {
  groupId: string;
  onEvent: (event: AppointmentEventEnvelopeType) => Promise<void>;
  onShutdown?: () => Promise<void>;
}) {
  // Tạo consumer với group ID riêng (mỗi consumer group nhận bản sao riêng của event)
  const consumer = await createKafkaConsumer(options.groupId);

  logger.info(
    { topic: env.kafkaAppointmentTopic, groupId: options.groupId },
    "Consumer started",
  );

  // Xử lý graceful shutdown: ngắt consumer khi nhận SIGINT/SIGTERM
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

  // Subscribe và xử lý từng message: parse JSON → validate schema → gọi callback
  await subscribeToAppointmentEvents(consumer, async ({ message }) => {
    if (!message.value) return;
    const event = AppointmentEventEnvelope.parse(
      JSON.parse(message.value.toString("utf8")),
    );
    await options.onEvent(event);
  });
}

/** Chạy consumer đọc chat messages từ Kafka — dùng cho chat consumer lưu vào MongoDB */
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

  // Xử lý graceful shutdown: ngắt consumer khi nhận SIGINT/SIGTERM
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

  // Subscribe vào topic chat-messages, đọc từ đầu
  await consumer.subscribe({
    topic: env.kafkaChatTopic,
    fromBeginning: true,
  });

  // Xử lý từng message: parse JSON → validate schema → gọi callback
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
