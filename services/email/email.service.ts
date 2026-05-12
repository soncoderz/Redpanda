import sgMail from "@sendgrid/mail";

import type { AppointmentEmailPayload, ReminderType } from "../../models/appointment.model.js";
import { env } from "../../config/env.js";
import { logger } from "../../utils/logger.js";

// Khởi tạo SendGrid API key nếu có — nếu không sẽ mock email
if (env.sendgridApiKey) {
  sgMail.setApiKey(env.sendgridApiKey);
  logger.info("SendGrid API key configured — emails will be sent");
} else {
  logger.warn("SENDGRID_API_KEY not set — emails will be mocked");
}

/** Kết quả gửi email: thành công hoặc thất bại kèm lý do */
export type EmailSendResult =
  | { sent: true }
  | {
      sent: false;
      reason: string;
      statusCode?: number;
      responseBody?: unknown;
    };

/** Tiêu đề email theo loại reminder */
const SUBJECT: Record<ReminderType, string> = {
  before: "Nhắc lịch hẹn",
  atTime: "Đến giờ hẹn",
  after: "Theo dõi sau hẹn",
};

/** Nội dung email theo loại reminder */
const BODY: Record<ReminderType, string> = {
  before: "Lịch hẹn của bạn sắp bắt đầu.",
  atTime: "Đã đến giờ hẹn của bạn.",
  after: "Lịch hẹn của bạn đã qua giờ.",
};

/** Gửi email nhắc lịch hẹn qua SendGrid (hoặc mock nếu không có API key) */
export async function sendAppointmentEmail(
  reminder: ReminderType,
  appointment: AppointmentEmailPayload,
): Promise<EmailSendResult> {
  // Tạo tiêu đề và nội dung email
  const subject = `${SUBJECT[reminder]}: ${appointment.service}`;
  const text = [
    `Xin chào ${appointment.customerName},`,
    "",
    BODY[reminder],
    `Mã lịch hẹn: ${appointment.id}`,
    `Dịch vụ: ${appointment.service}`,
    `Thời gian: ${appointment.startAt}`,
    appointment.note ? `Ghi chú: ${appointment.note}` : undefined,
  ]
    .filter(Boolean)
    .join("\n");

  // Nếu không có API key → mock email (chỉ log, không gửi thật)
  if (!env.sendgridApiKey) {
    logger.info(
      { to: appointment.customerEmail, subject },
      "Mock email sent (no SENDGRID_API_KEY)",
    );
    return { sent: true };
  }

  // Kiểm tra email người gửi đã cấu hình chưa
  if (!env.sendgridFromEmail) {
    return { sent: false, reason: "SENDGRID_FROM_EMAIL not configured" };
  }

  try {
    // Gọi SendGrid API gửi email
    const [response] = await sgMail.send({
      to: appointment.customerEmail,
      from: {
        email: env.sendgridFromEmail,
        name: env.sendgridFromName,
      },
      subject,
      text,
    });

    logger.info(
      { to: appointment.customerEmail, subject, statusCode: response.statusCode },
      "Email sent via SendGrid",
    );
    return { sent: true };
  } catch (error: any) {
    // Xử lý lỗi gửi email — lỗi 401/403 là lỗi xác thực không retry
    const statusCode = error?.code;
    const responseBody = error?.response?.body;

    logger.error(
      { to: appointment.customerEmail, statusCode, responseBody },
      "SendGrid email failed",
    );

    if (statusCode === 401 || statusCode === 403) {
      return {
        sent: false,
        reason: "SendGrid authentication failed",
        statusCode,
        responseBody,
      };
    }

    throw error;
  }
}
