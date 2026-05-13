import "dotenv/config";

import { Kafka, logLevel } from "kafkajs";

const brokers = readCsvEnv("KAFKA_BROKERS", ["localhost:19092"]);
// const username = process.env.KAFKA_TEST_USERNAME ?? "chat-consumer";
// const password = process.env.KAFKA_TEST_PASSWORD ?? "chat-secret";
// const topic = process.env.KAFKA_TEST_TOPIC ?? "chat-messages";
// const groupId = process.env.KAFKA_TEST_GROUP_ID ?? "chat-storage";

const username = process.env.KAFKA_TEST_USERNAME ?? "api-service";
const password = process.env.KAFKA_TEST_PASSWORD ?? "api-service-secret";
const topic = process.env.KAFKA_TEST_TOPIC ?? "appointment-events";
const groupId = process.env.KAFKA_TEST_GROUP_ID ?? "appointment-analytics";


const kafka = new Kafka({
  clientId: "unauthorized-consumer-test",
  brokers,
  ssl: readBooleanEnv("KAFKA_SSL", false),
  sasl: {
    mechanism: "scram-sha-256",
    username,
    password,
  },
  logLevel: logLevel.WARN,
});

const consumer = kafka.consumer({
  groupId,
  allowAutoTopicCreation: false,
});

consumer.on(consumer.events.CRASH, (event) => {
  console.error("Consumer crashed:");
  console.error(errorSummary(event.payload.error));
});

try {
  console.log("Testing unauthorized consume:");
  console.log({ brokers, username, topic, groupId });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("Unexpectedly consumed a message:", {
        topic,
        partition,
        offset: message.offset,
      });
    },
  });

  await sleep(10_000);
  console.log("No authorization error after 10s. Check ACLs or test inputs.");
} catch (error) {
  console.error("Unauthorized consume failed as expected:");
  console.error(errorSummary(error));
} finally {
  await consumer.disconnect().catch(() => undefined);
}

function errorSummary(error: unknown) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack?.split("\n").slice(0, 3).join("\n"),
    };
  }

  return error;
}

function readCsvEnv(name: string, fallback: string[]) {
  const value = process.env[name];
  if (!value) return fallback;
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function readBooleanEnv(name: string, fallback: boolean) {
  const value = process.env[name];
  if (value === undefined) return fallback;
  return value === "true" || value === "1";
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
