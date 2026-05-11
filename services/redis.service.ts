import { Redis } from "ioredis";

import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";

export function createRedisConnection() {
  const connection = new Redis(env.redisUrl, {
    maxRetriesPerRequest: null,
  });

  connection.on("error", (error) => {
    logger.error({ error: error.message }, "Redis connection error");
  });

  return connection;
}
