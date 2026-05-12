import mongoose from "mongoose";

import { env } from "../../config/env.js";
import { logger } from "../../utils/logger.js";

/** Kết nối MongoDB (skip nếu đã kết nối) */
export async function connectMongo() {
  if (mongoose.connection.readyState === 1) {
    return;
  }

  mongoose.set("strictQuery", true);

  await mongoose.connect(env.mongoUri, {
    dbName: env.mongoDbName,
    autoIndex: env.nodeEnv !== "production",
  });

  logger.info(
    {
      database: mongoose.connection.name,
      host: mongoose.connection.host,
    },
    "MongoDB connected",
  );
}

/** Ngắt kết nối MongoDB */
export async function disconnectMongo() {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
}

/** Trạng thái kết nối MongoDB (dùng cho health check) */
export function mongoReadyState() {
  return mongoose.connection.readyState;
}
