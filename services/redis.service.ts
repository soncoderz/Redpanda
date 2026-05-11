import { Redis } from "ioredis";

import { env } from "../config/env.js";

export function createRedisConnection() {
  const connection = new Redis(env.redisUrl, {
    maxRetriesPerRequest: null,
  });

  connection.on("error", (error) => {
    console.error("Redis connection error", {
      message: error.message,
    });
  });

  return connection;
}
