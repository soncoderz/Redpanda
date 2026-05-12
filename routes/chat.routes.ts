import { Hono } from "hono";

import { sendMessage, streamMessages } from "../controllers/chat.controller.js";

/** Tạo router cho các endpoint chat */
export function createChatRoutes() {
  const router = new Hono();

  // POST /chat/send — gửi tin nhắn chat (publish lên Kafka + phát SSE)
  router.post("/send", sendMessage);
  // GET /chat/stream/:roomId — kết nối SSE nhận tin nhắn real-time theo phòng
  router.get("/stream/:roomId", streamMessages);

  return router;
}
