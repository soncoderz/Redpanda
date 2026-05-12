import mongoose, { Schema, type HydratedDocument } from "mongoose";

import type { ChatMessage } from "./chat.model.js";

export type ChatMessageDocument = HydratedDocument<ChatMessage>;

const ChatMessageSchema = new Schema<ChatMessage>(
  {
    id: { type: String, required: true, unique: true },
    roomId: { type: String, required: true, index: true },
    from: { type: String, required: true },
    text: { type: String, required: true, maxlength: 2_000 },
    sentAt: { type: String, required: true },
  },
  {
    collection: "chat_messages",
    versionKey: false,
  },
);

ChatMessageSchema.index({ roomId: 1, sentAt: 1 });

export const ChatMessageModel =
  mongoose.models.ChatMessage ??
  mongoose.model<ChatMessage>("ChatMessage", ChatMessageSchema);
