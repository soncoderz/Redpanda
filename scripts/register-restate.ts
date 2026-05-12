import "dotenv/config";
import { setTimeout as sleep } from "node:timers/promises";

import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";

const maxAttempts = 30;

// Retry đăng ký endpoint với Restate Admin API (chờ Restate sẵn sàng)
for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
  try {
    // POST /deployments — đăng ký URL mà Restate sẽ gọi callback tới
    const response = await fetch(`${env.restateAdminUrl}/deployments`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(env.restateAuthToken
          ? { Authorization: `Bearer ${env.restateAuthToken}` }
          : {}),
      },
      body: JSON.stringify({
        uri: env.publicRestateEndpoint,
        use_http_11: true,
      }),
    });

    const body = await response.text();

    // 200 = đăng ký thành công, 409 = đã đăng ký rồi → cả hai đều OK
    if (response.ok || response.status === 409) {
      logger.info(
        {
          status: response.status,
          restateAdminUrl: env.restateAdminUrl,
          publicRestateEndpoint: env.publicRestateEndpoint,
        },
        "Restate deployment registered",
      );
      process.exit(0);
    }

    logger.warn(
      {
        attempt,
        status: response.status,
        body,
        restateAdminUrl: env.restateAdminUrl,
        publicRestateEndpoint: env.publicRestateEndpoint,
      },
      "Restate deployment registration attempt failed",
    );
  } catch (error) {
    logger.warn(
      {
        attempt,
        error: error instanceof Error ? error.message : String(error),
      },
      "Restate admin API is not ready",
    );
  }

  // Chờ 2s trước khi thử lại
  await sleep(2_000);
}

logger.error(
  {
    restateAdminUrl: env.restateAdminUrl,
    publicRestateEndpoint: env.publicRestateEndpoint,
  },
  "Failed to register Restate deployment",
);
process.exit(1);
