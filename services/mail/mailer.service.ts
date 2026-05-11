import sgMail from "@sendgrid/mail";

import type { AppointmentEmailPayload } from "../../models/appointment.model.js";
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

export async function sendAppointmentBeforeEmail(
  appointment: AppointmentEmailPayload,
) {
  return sendAppointmentEmail({
    to: appointment.customerEmail,
    subject: `Nhắc lịch hẹn: ${appointment.service}`,
    text: [
      `Xin chào ${appointment.customerName},`,
      "",
      "Lịch hẹn của bạn sắp bắt đầu.",
      `Mã lịch hẹn: ${appointment.id}`,
      `Dịch vụ: ${appointment.service}`,
      `Thời gian: ${appointment.startAt}`,
      appointment.note ? `Ghi chú: ${appointment.note}` : undefined,
    ]
      .filter(Boolean)
      .join("\n"),
  });
}

export async function sendAppointmentAtTimeEmail(
  appointment: AppointmentEmailPayload,
) {
  return sendAppointmentEmail({
    to: appointment.customerEmail,
    subject: `Đến giờ hẹn: ${appointment.service}`,
    text: [
      `Xin chào ${appointment.customerName},`,
      "",
      "Đã đến giờ hẹn của bạn.",
      `Mã lịch hẹn: ${appointment.id}`,
      `Dịch vụ: ${appointment.service}`,
      `Thời gian: ${appointment.startAt}`,
      appointment.note ? `Ghi chú: ${appointment.note}` : undefined,
    ]
      .filter(Boolean)
      .join("\n"),
  });
}

export async function sendAppointmentAfterEmail(
  appointment: AppointmentEmailPayload,
) {
  return sendAppointmentEmail({
    to: appointment.customerEmail,
    subject: `Theo dõi sau hẹn: ${appointment.service}`,
    text: [
      `Xin chào ${appointment.customerName},`,
      "",
      "Lịch hẹn của bạn đã qua giờ.",
      `Mã lịch hẹn: ${appointment.id}`,
      `Dịch vụ: ${appointment.service}`,
      `Thời gian: ${appointment.startAt}`,
      appointment.note ? `Ghi chú: ${appointment.note}` : undefined,
    ]
      .filter(Boolean)
      .join("\n"),
  });
}

async function sendAppointmentEmail(message: {
  to: string;
  subject: string;
  text: string;
}): Promise<EmailSendResult> {
  if (!env.sendgridApiKey) {
    logger.info(
      { to: message.to, subject: message.subject },
      "Mock email sent (no SENDGRID_API_KEY)",
    );
    return { sent: true };
  }

  if (!env.sendgridFromEmail) {
    return { sent: false, reason: "SENDGRID_FROM_EMAIL not configured" };
  }

  try {
    const [response] = await sgMail.send({
      to: message.to,
      from: {
        email: env.sendgridFromEmail,
        name: env.sendgridFromName,
      },
      subject: message.subject,
      text: message.text,
    });

    logger.info(
      { to: message.to, subject: message.subject, statusCode: response.statusCode },
      "Email sent via SendGrid",
    );
    return { sent: true };
  } catch (error: any) {
    const statusCode = error?.code;
    const responseBody = error?.response?.body;

    logger.error(
      { to: message.to, statusCode, responseBody },
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
