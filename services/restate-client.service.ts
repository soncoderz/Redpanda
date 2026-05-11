import * as clients from "@restatedev/restate-sdk-clients";

import { env } from "../config/env.js";

export const restateClient = clients.connect({
  url: env.restateRuntimeUrl,
  headers: env.restateAuthToken
    ? {
        Authorization: `Bearer ${env.restateAuthToken}`,
      }
    : undefined,
});
