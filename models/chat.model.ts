import { z } from "zod";

/** Schema validate chat message — dữ liệu lưu vào MongoDB và publish lên Kafka */
export const ChatMessage = z.object({
  id: z.string().min(1),
  roomId: z.string().min(1),
  from: z.string().min(1),
  text: z.string().min(1).max(2_000),
  sentAt: z.string().datetime(),
});

export type ChatMessage = z.infer<typeof ChatMessage>;

/** Schema validate dữ liệu đầu vào gửi chat từ API */
export const SendChatInput = z.object({
  roomId: z.string().min(1),
  from: z.string().min(1),
  text: z.string().min(1).max(2_000),
});

export type SendChatInput = z.infer<typeof SendChatInput>;
