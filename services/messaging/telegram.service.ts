import { env } from "../../config/env.js";
import { logger } from "../../utils/logger.js";

/** URL gốc của Telegram Bot API */
const TELEGRAM_API = "https://api.telegram.org";

/** Kiểm tra token và chat ID đã được cấu hình chưa */
export function isTelegramConfigured() {
  return Boolean(env.telegramBotToken && env.telegramChatId);
}

/** Gửi message HTML tới Telegram chat qua Bot API */
export async function sendTelegramMessage(text: string) {
  // Kiểm tra cấu hình, bỏ qua nếu chưa thiết lập
  if (!env.telegramBotToken || !env.telegramChatId) {
    logger.warn("Telegram bot token or chat ID not configured, skipping");
    return { sent: false, reason: "not configured" };
  }

  // Tạo URL endpoint sendMessage của Telegram Bot API
  const url = `${TELEGRAM_API}/bot${env.telegramBotToken}/sendMessage`;

  // Gửi POST request với nội dung HTML tới chat ID đã cấu hình
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: env.telegramChatId,
      text,
      parse_mode: "HTML",
    }),
  });

  // Kiểm tra response — nếu lỗi thì log và trả về sent: false
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    logger.error(
      { status: response.status, body },
      "Failed to send Telegram message",
    );
    return { sent: false, reason: `HTTP ${response.status}` };
  }

  return { sent: true };
}

/** Format appointment event thành HTML message cho Telegram theo loại event */
export function formatAppointmentEvent(event: {
  type: string;
  appointmentId: string;
  version: number;
  occurredAt: string;
  payload: Record<string, unknown>;
}) {
  // Chuyển thời gian sang múi giờ Việt Nam
  const time = new Date(event.occurredAt).toLocaleString("vi-VN", {
    timeZone: "Asia/Ho_Chi_Minh",
  });

  // Lấy thông tin appointment từ payload
  const appointment = event.payload.appointment as
    | Record<string, unknown>
    | undefined;

  switch (event.type) {
    case "appointment.created":
      return [
        `<b>📅 Lịch hẹn mới</b>`,
        ``,
        `<b>ID:</b> <code>${event.appointmentId}</code>`,
        `<b>Khách:</b> ${appointment?.customerName ?? "N/A"}`,
        `<b>Email:</b> ${appointment?.customerEmail ?? "N/A"}`,
        `<b>Dịch vụ:</b> ${appointment?.service ?? "N/A"}`,
        `<b>Thời gian:</b> ${formatDateTime(appointment?.startAt)}`,
        appointment?.note ? `<b>Ghi chú:</b> ${appointment.note}` : "",
        ``,
        `<i>${time}</i>`,
      ]
        .filter(Boolean)
        .join("\n");

    case "appointment.updated":
      return [
        `<b>✏️ Cập nhật lịch hẹn</b>`,
        ``,
        `<b>ID:</b> <code>${event.appointmentId}</code>`,
        `<b>Khách:</b> ${appointment?.customerName ?? "N/A"}`,
        `<b>Dịch vụ:</b> ${appointment?.service ?? "N/A"}`,
        `<b>Thời gian:</b> ${formatDateTime(appointment?.startAt)}`,
        `<b>Version:</b> ${event.version}`,
        ``,
        `<i>${time}</i>`,
      ].join("\n");

    case "appointment.cancelled":
      return [
        `<b>❌ Hủy lịch hẹn</b>`,
        ``,
        `<b>ID:</b> <code>${event.appointmentId}</code>`,
        `<b>Khách:</b> ${appointment?.customerName ?? "N/A"}`,
        `<b>Dịch vụ:</b> ${appointment?.service ?? "N/A"}`,
        ``,
        `<i>${time}</i>`,
      ].join("\n");

    case "appointment.reminder.sent":
      return [
        `<b>📧 Đã gửi email nhắc hẹn</b>`,
        ``,
        `<b>ID:</b> <code>${event.appointmentId}</code>`,
        `<b>Loại:</b> ${event.payload.reminder ?? "N/A"}`,
        ``,
        `<i>${time}</i>`,
      ].join("\n");

    default:
      return [
        `<b>📌 Sự kiện: ${event.type}</b>`,
        ``,
        `<b>ID:</b> <code>${event.appointmentId}</code>`,
        `<b>Version:</b> ${event.version}`,
        ``,
        `<i>${time}</i>`,
      ].join("\n");
  }
}

/** Format datetime sang chuỗi tiếng Việt (múi giờ Asia/Ho_Chi_Minh) */
function formatDateTime(value: unknown) {
  if (typeof value !== "string") return "N/A";
  return new Date(value).toLocaleString("vi-VN", {
    timeZone: "Asia/Ho_Chi_Minh",
  });
}
