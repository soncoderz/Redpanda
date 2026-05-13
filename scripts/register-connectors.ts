import "dotenv/config";

import { readFileSync, readdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const connectorsDir = resolve(__dirname, "..", "connectors");

/** Chờ Kafka Connect REST API sẵn sàng — port TCP mở trước nhưng API cần thêm thời gian */
async function waitForConnectApi(url: string, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      // Connection refused hoặc socket closed — chờ thêm
    }
    await sleep(2_000);
  }
  throw new Error(`Kafka Connect API not ready at ${url} after ${timeoutMs / 1000}s`);
}

async function registerConnectors() {
  const connectUrl = env.kafkaConnectUrl;

  // Chờ Kafka Connect REST API sẵn sàng (port mở trước khi API ready)
  await waitForConnectApi(connectUrl);

  // Tìm tất cả file .json trong connectors/ (mỗi file = 1 connector config)
  const configFiles = readdirSync(connectorsDir).filter(
    (f) => f.endsWith(".json"),
  );

  if (configFiles.length === 0) {
    logger.warn("No connector config files found in connectors/");
    return;
  }

  for (const file of configFiles) {
    const configPath = resolve(connectorsDir, file);
    const config = JSON.parse(readFileSync(configPath, "utf-8"));
    const connectorName = config.name;

    // Kiểm tra connector đã tồn tại chưa
    const checkRes = await fetch(`${connectUrl}/connectors/${connectorName}`, {
      method: "GET",
    });

    if (checkRes.ok) {
      // Connector đã tồn tại → update config
      const updateRes = await fetch(
        `${connectUrl}/connectors/${connectorName}/config`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(config.config),
        },
      );

      if (!updateRes.ok) {
        const body = await updateRes.text();
        logger.error(
          { connector: connectorName, status: updateRes.status, body },
          "Failed to update connector",
        );
        continue;
      }

      logger.info({ connector: connectorName, file }, "Connector updated");
    } else {
      // Connector chưa tồn tại → tạo mới
      const createRes = await fetch(`${connectUrl}/connectors`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });

      if (!createRes.ok) {
        const body = await createRes.text();
        logger.error(
          { connector: connectorName, status: createRes.status, body },
          "Failed to create connector",
        );
        continue;
      }

      logger.info({ connector: connectorName, file }, "Connector created");
    }
  }

  // Liệt kê tất cả connector đang chạy
  const listRes = await fetch(`${connectUrl}/connectors?expand=status`);
  if (listRes.ok) {
    const connectors = await listRes.json();
    logger.info(
      { connectors: Object.keys(connectors as Record<string, unknown>) },
      "Active connectors",
    );
  }
}

registerConnectors().catch((error) => {
  logger.error({ err: error }, "Failed to register connectors");
  process.exit(1);
});
