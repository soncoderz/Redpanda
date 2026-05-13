import {
  type Consumer,
  type EachMessagePayload,
  type ITopicConfig,
} from "kafkajs";

import { env } from "../../config/env.js";
import { kafka } from "../../config/kafka.js";
import { decodeWithSchema } from "../../config/schema-registry.js";
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

/** Chạy consumer đọc appointment events từ Kafka — dùng cho telegram consumer */
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

  // Subscribe và xử lý từng message: decode Schema Registry wire format → validate Zod → gọi callback
  // Fallback: nếu message không có magic byte (0x00) → parse JSON thuần (backward compatible)
  await subscribeToAppointmentEvents(consumer, async ({ message, topic, partition }) => {
    if (!message.value) return;

    let decoded: unknown;
    try {
      decoded = isConfluentWireFormat(message.value)
        ? await decodeWithSchema(message.value)
        : JSON.parse(message.value.toString("utf8"));
    } catch (error) {
      logger.warn(
        { topic, partition, offset: message.offset, err: error },
        "Failed to decode appointment event",
      );
      return;
    }

    const parsed = AppointmentEventEnvelope.safeParse(decoded);
    if (!parsed.success) {
      logger.warn(
        { topic, partition, offset: message.offset, errors: parsed.error.issues },
        "Skipping invalid appointment event",
      );
      return;
    }
    await options.onEvent(parsed.data);
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

  // Xử lý từng message: decode wire format → validate Zod → gọi callback (skip message lỗi)
  await consumer.run({
    eachMessage: async ({ message, topic, partition }) => {
      if (!message.value) return;

      let decoded: unknown;
      try {
        decoded = isConfluentWireFormat(message.value)
          ? await decodeWithSchema(message.value)
          : JSON.parse(message.value.toString("utf8"));
      } catch (error) {
        logger.warn(
          { topic, partition, offset: message.offset, err: error },
          "Failed to decode chat message",
        );
        return;
      }

      const parsed = ChatMessage.safeParse(decoded);
      if (!parsed.success) {
        logger.warn(
          { topic, partition, offset: message.offset, errors: parsed.error.issues },
          "Skipping invalid chat message",
        );
        return;
      }
      await options.onMessage(parsed.data);
    },
  });
}

/** Kiểm tra message có đúng Confluent wire format không (magic byte 0x00 ở đầu) */
function isConfluentWireFormat(buffer: Buffer): boolean {
  return buffer.length >= 5 && buffer[0] === 0;
}
