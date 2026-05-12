import { z } from "zod";

import { env } from "../../config/env.js";
import { maintenanceQueue, MAINTENANCE_QUEUE_NAME } from "../../config/queues.js";

export { MAINTENANCE_QUEUE_NAME, maintenanceQueue };

export const MaintenanceJobData = z.object({
  type: z.literal("queue-cleanup"),
});

export type MaintenanceJobData = z.infer<typeof MaintenanceJobData>;

export async function upsertCleanupScheduler() {
  return maintenanceQueue.upsertJobScheduler(
    "queue-cleanup",
    { every: env.cleanupEveryMs },
    {
      name: "cleanup",
      data: { type: "queue-cleanup" },
    },
  );
}

export async function closeMaintenanceQueue() {
  await maintenanceQueue.close();
}
