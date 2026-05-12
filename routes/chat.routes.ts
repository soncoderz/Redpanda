import { Hono } from "hono";

import { sendMessage, streamMessages } from "../controllers/chat.controller.js";

export function createChatRoutes() {
  const router = new Hono();

  router.post("/send", sendMessage);
  router.get("/stream/:roomId", streamMessages);

  return router;
}
