import { Redis } from "ioredis";

import { env } from "./env.js";
import { logger } from "../utils/logger.js";

/** Tạo kết nối Redis (dùng cho BullMQ queue và worker) */
export function createRedisConnection() {
  // maxRetriesPerRequest: null — bắt buộc cho BullMQ
  const connection = new Redis(env.redisUrl, {
    maxRetriesPerRequest: null,
  });

  connection.on("error", (error) => {
    logger.error({ error: error.message }, "Redis connection error");
  });

  return connection;
}
