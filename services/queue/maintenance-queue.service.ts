import { z } from "zod";

import { env } from "../../config/env.js";
import { maintenanceQueue, MAINTENANCE_QUEUE_NAME } from "../../config/queues.js";

export { MAINTENANCE_QUEUE_NAME, maintenanceQueue };

/** Schema validate dữ liệu job bảo trì */
export const MaintenanceJobData = z.object({
  type: z.literal("queue-cleanup"),
});

export type MaintenanceJobData = z.infer<typeof MaintenanceJobData>;

/** Tạo hoặc cập nhật lịch dọn dẹp queue định kỳ (BullMQ Job Scheduler) */
export async function upsertCleanupScheduler() {
  // Đăng ký job scheduler chạy định kỳ theo interval cleanupEveryMs
  return maintenanceQueue.upsertJobScheduler(
    "queue-cleanup",
    { every: env.cleanupEveryMs },
    {
      name: "cleanup",
      data: { type: "queue-cleanup" },
    },
  );
}

/** Đóng kết nối maintenance queue */
export async function closeMaintenanceQueue() {
  await maintenanceQueue.close();
}
