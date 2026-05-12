import type { ErrorHandler } from "hono";
import { z } from "zod";

import { logger } from "../utils/logger.js";

/** Middleware xử lý lỗi toàn cục — chuyển lỗi thành HTTP response phù hợp */
export const errorMiddleware: ErrorHandler = (err, c) => {
  // Lỗi validation Zod → 400 Bad Request
  if (err instanceof z.ZodError) {
    return c.json({ error: "Invalid request", issues: err.issues }, 400);
  }

  const message = err instanceof Error ? err.message : String(err);

  // Lỗi "does not exist" → 404 Not Found
  if (message.includes("does not exist")) {
    return c.json({ error: message }, 404);
  }

  // Lỗi "already exists" hoặc "already cancelled" → 409 Conflict
  if (
    message.includes("already exists") ||
    message.includes("already cancelled")
  ) {
    return c.json({ error: message }, 409);
  }

  // Lỗi không xác định → 500 Internal Server Error
  logger.error({ error: message, stack: err.stack }, "Unhandled request error");
  return c.json({ error: "Internal server error" }, 500);
};
