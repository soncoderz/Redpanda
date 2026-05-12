import { EventEmitter } from "node:events";

import type { AppointmentEventEnvelope } from "../../models/appointment.model.js";

// EventEmitter nội bộ — chỉ hoạt động trong cùng process API server
const emitter = new EventEmitter();
emitter.setMaxListeners(1_000);

/** Phát event tới tất cả SSE client đang subscribe */
export function emitRealtimeEvent(event: AppointmentEventEnvelope) {
  emitter.emit("appointment-event", event);
}

/** Đăng ký nhận event real-time, trả về hàm unsubscribe */
export function subscribeRealtimeEvents(
  listener: (event: AppointmentEventEnvelope) => void,
) {
  emitter.on("appointment-event", listener);

  return () => {
    emitter.off("appointment-event", listener);
  };
}
