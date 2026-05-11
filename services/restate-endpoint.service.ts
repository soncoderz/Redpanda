import * as restateFetch from "@restatedev/restate-sdk/fetch";

import { env } from "../config/env.js";
import { appointmentObject } from "./appointment.service.js";

export const restateEndpoint = restateFetch.createEndpointHandler({
  services: [appointmentObject],
  identityKeys: env.restateIdentityKeys?.length
    ? env.restateIdentityKeys
    : undefined,
});
