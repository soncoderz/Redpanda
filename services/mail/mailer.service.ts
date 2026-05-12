import sgMail from "@sendgrid/mail";

import type { AppointmentEmailPayload, ReminderType } from "../../models/appointment.model.js";
import { env } from "../../config/env.js";
import { logger } from "../../utils/logger.js";

if (env.sendgridApiKey) {
  sgMail.setApiKey(env.sendgridApiKey);
  logger.info("SendGrid API key configured — emails will be sent");
} else {
  logger.warn("SENDGRID_API_KEY not set — emails will be mocked");
}

export type EmailSendResult =
  | { sent: true }
  | {
      sent: false;
      reason: string;
      statusCode?: number;
      responseBody?: unknown;
    };

const SUBJECT: Record<ReminderType, string> = {
  before: "Nhắc lịch hẹn",
  atTime: "Đến giờ hẹn",
  after: "Theo dõi sau hẹn",
};

const BODY: Record<ReminderType, string> = {
  before: "Lịch hẹn của bạn sắp bắt đầu.",
  atTime: "Đã đến giờ hẹn của bạn.",
  after: "Lịch hẹn của bạn đã qua giờ.",
};

export async function sendAppointmentEmail(
  reminder: ReminderType,
  appointment: AppointmentEmailPayload,
): Promise<EmailSendResult> {
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

  if (!env.sendgridApiKey) {
    logger.info(
      { to: appointment.customerEmail, subject },
      "Mock email sent (no SENDGRID_API_KEY)",
    );
    return { sent: true };
  }

  if (!env.sendgridFromEmail) {
    return { sent: false, reason: "SENDGRID_FROM_EMAIL not configured" };
  }

  try {
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
