import "dotenv/config";

import {
  appointmentEmailQueue,
  closeAppointmentEmailQueue,
} from "../services/queue/email-queue.service.js";
import {
  closeMaintenanceQueue,
  maintenanceQueue,
} from "../services/queue/maintenance-queue.service.js";
import { logger } from "../utils/logger.js";

/** Xóa toàn bộ job trong tất cả BullMQ queue (email + maintenance) */
async function clearAllJobs() {
  logger.info("Clearing all BullMQ jobs");

  // Đếm số job hiện tại trong mỗi queue
  const [emailCounts, maintenanceCounts] = await Promise.all([
    appointmentEmailQueue.getJobCounts(),
    maintenanceQueue.getJobCounts(),
  ]);
  logger.info({ emailCounts, maintenanceCounts }, "Current queue counts");

  // Xóa sạch toàn bộ job trong cả 2 queue (bao gồm cả job đang chạy)
  await Promise.all([
    appointmentEmailQueue.obliterate({ force: true }),
    maintenanceQueue.obliterate({ force: true }),
  ]);

  // Kiểm tra lại sau khi xóa
  const [emailAfter, maintenanceAfter] = await Promise.all([
    appointmentEmailQueue.getJobCounts(),
    maintenanceQueue.getJobCounts(),
  ]);
  logger.info({ emailAfter, maintenanceAfter }, "Queues cleared");

  // Đóng kết nối queue và thoát
  await closeAppointmentEmailQueue();
  await closeMaintenanceQueue();
  process.exit(0);
}

// Chạy hàm clearAllJobs — nếu lỗi thì log và exit 1
clearAllJobs().catch((error) => {
  logger.error({ error }, "Failed to clear jobs");
  process.exit(1);
});
