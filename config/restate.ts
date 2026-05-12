import * as restate from "@restatedev/restate-sdk/fetch";
import * as clients from "@restatedev/restate-sdk-clients";

import { env } from "./env.js";
import { appointmentObject } from "../services/restate/appointment.handler.js";

/** Client gọi tới Restate runtime (dùng trong controller và email worker) */
export const restateClient = clients.connect({
  url: env.restateRuntimeUrl,
  headers: env.restateAuthToken
    ? {
        Authorization: `Bearer ${env.restateAuthToken}`,
      }
    : undefined,
});

/** HTTP endpoint nhận callback từ Restate runtime → xử lý bởi appointmentObject */
export const restateEndpoint = restate.createEndpointHandler({
  services: [appointmentObject],
  identityKeys: env.restateIdentityKeys?.length
    ? env.restateIdentityKeys
    : undefined,
});
