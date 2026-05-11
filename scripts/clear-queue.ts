import "dotenv/config";

import {
  appointmentEmailQueue,
  closeAppointmentEmailQueue,
} from "../services/email-queue.service.js";
import {
  closeMaintenanceQueue,
  maintenanceQueue,
} from "../services/maintenance-queue.service.js";
import { logger } from "../utils/logger.js";

async function clearAllJobs() {
  logger.info("Clearing all BullMQ jobs");

  const [emailCounts, maintenanceCounts] = await Promise.all([
    appointmentEmailQueue.getJobCounts(),
    maintenanceQueue.getJobCounts(),
  ]);
  logger.info({ emailCounts, maintenanceCounts }, "Current queue counts");

  await Promise.all([
    appointmentEmailQueue.obliterate({ force: true }),
    maintenanceQueue.obliterate({ force: true }),
  ]);

  const [emailAfter, maintenanceAfter] = await Promise.all([
    appointmentEmailQueue.getJobCounts(),
    maintenanceQueue.getJobCounts(),
  ]);
  logger.info({ emailAfter, maintenanceAfter }, "Queues cleared");

  await closeAppointmentEmailQueue();
  await closeMaintenanceQueue();
  process.exit(0);
}

clearAllJobs().catch((error) => {
  logger.error({ error }, "Failed to clear jobs");
  process.exit(1);
});
