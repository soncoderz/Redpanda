import { z } from "zod";

export const ChatMessage = z.object({
  id: z.string().min(1),
  roomId: z.string().min(1),
  from: z.string().min(1),
  text: z.string().min(1).max(2_000),
  sentAt: z.string().datetime(),
});

export type ChatMessage = z.infer<typeof ChatMessage>;

export const SendChatInput = z.object({
  roomId: z.string().min(1),
  from: z.string().min(1),
  text: z.string().min(1).max(2_000),
});

export type SendChatInput = z.infer<typeof SendChatInput>;
