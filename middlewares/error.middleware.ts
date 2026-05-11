import type { ErrorHandler } from "hono";
import { z } from "zod";

export const errorMiddleware: ErrorHandler = (err, c) => {
  if (err instanceof z.ZodError) {
    return c.json({ error: "Invalid request body", issues: err.issues }, 400);
  }

  console.error(err);
  return c.json({ error: err.message }, 500);
};
