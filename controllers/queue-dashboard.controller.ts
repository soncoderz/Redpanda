import { serveStatic } from "@hono/node-server/serve-static";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { HonoAdapter } from "@bull-board/hono";

import { appointmentEmailQueue } from "../services/email-queue.service.js";

export function createQueueDashboard(basePath: string) {
  const serverAdapter = new HonoAdapter(serveStatic);
  serverAdapter.setBasePath(basePath);

  createBullBoard({
    queues: [
      new BullMQAdapter(appointmentEmailQueue, {
        description: "Appointment reminder email jobs",
      }),
    ],
    serverAdapter,
    options: {
      uiConfig: {
        boardTitle: "Appointment Email Queue",
      },
    },
  });

  return serverAdapter.registerPlugin();
}
