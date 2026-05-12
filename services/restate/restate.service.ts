import * as restate from "@restatedev/restate-sdk/fetch";
import * as clients from "@restatedev/restate-sdk-clients";

import { env } from "../../config/env.js";
import { appointmentObject } from "../appointment/appointment.service.js";

export const restateClient = clients.connect({
  url: env.restateRuntimeUrl,
  headers: env.restateAuthToken
    ? {
        Authorization: `Bearer ${env.restateAuthToken}`,
      }
    : undefined,
});

export const restateEndpoint = restate.createEndpointHandler({
  services: [appointmentObject],
  identityKeys: env.restateIdentityKeys?.length
    ? env.restateIdentityKeys
    : undefined,
});
