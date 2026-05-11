import "dotenv/config";
import { serve } from "@hono/node-server";

import { createApp } from "./app.js";
import { env } from "./config/env.js";

const app = createApp();

serve({ fetch: app.fetch, port: env.port }, (info) => {
  console.log(`Hono API listening on http://localhost:${info.port}`);
  console.log(`Register Restate endpoint: ${env.publicRestateEndpoint}`);
  console.log(
    `BullMQ dashboard: http://localhost:${info.port}${env.queueDashboardPath}`,
  );
});
