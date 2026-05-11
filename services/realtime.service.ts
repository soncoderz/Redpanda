import { EventEmitter } from "node:events";

import type { AppointmentEventEnvelope } from "../models/appointment.model.js";

const emitter = new EventEmitter();
emitter.setMaxListeners(1_000);

export function emitRealtimeEvent(event: AppointmentEventEnvelope) {
  emitter.emit("appointment-event", event);
}

export function subscribeRealtimeEvents(
  listener: (event: AppointmentEventEnvelope) => void,
) {
  emitter.on("appointment-event", listener);

  return () => {
    emitter.off("appointment-event", listener);
  };
}
