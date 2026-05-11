import type { ErrorHandler } from "hono";
import { z } from "zod";

import { logger } from "../utils/logger.js";

export const errorMiddleware: ErrorHandler = (err, c) => {
  if (err instanceof z.ZodError) {
    return c.json({ error: "Invalid request", issues: err.issues }, 400);
  }

  const message = err instanceof Error ? err.message : String(err);

  if (message.includes("does not exist")) {
    return c.json({ error: message }, 404);
  }

  if (
    message.includes("already exists") ||
    message.includes("already cancelled")
  ) {
    return c.json({ error: message }, 409);
  }

  logger.error({ error: message, stack: err.stack }, "Unhandled request error");
  return c.json({ error: "Internal server error" }, 500);
};
