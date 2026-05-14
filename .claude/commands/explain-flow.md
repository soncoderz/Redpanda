# Explain Flow

Giải thích luồng chạy (data flow) của một feature trong hệ thống. Trace từ request đầu vào đến kết quả cuối cùng, qua tất cả các service liên quan.

## Cách thực hiện

1. **Hỏi user** muốn giải thích flow nào, hoặc suy từ context. Các flow chính:
   - `create` — Tạo appointment mới
   - `update` — Cập nhật appointment
   - `cancel` — Hủy appointment
   - `reminder` — Gửi email reminder (3 mốc: before, atTime, after)
   - `event` — Event flow từ Restate → Kafka → consumers
   - `email` — Email flow từ Restate → BullMQ → SendGrid → callback

2. **Đọc source code** thực tế, không dùng kiến thức cũ. Các file chính cần đọc:
   - `controllers/appointment.controller.ts` — API handler
   - `services/restate/appointment.handler.ts` — Restate Virtual Object
   - `services/queue/email-queue.service.ts` — BullMQ queue setup
   - `services/messaging/event-publisher.service.ts` — Kafka publisher
   - `services/messaging/kafka.service.ts` — Kafka consumer framework
   - `workers/email.worker.ts` — Email worker
   - `workers/telegram.consumer.ts` — Telegram consumer
   - `models/appointment.repository.ts` — MongoDB repository
   - `config/env.ts` — Environment config (timing, queue settings)

3. **Trình bày** theo format:

```
## Flow: <tên flow>

### Tổng quan
1 câu mô tả flow làm gì.

### Chi tiết từng bước

1. **[Component]** `file:line` — Mô tả action
   - Input: ...
   - Output: ...

2. **[Component]** `file:line` — Mô tả action
   ...

### Diagram
```
Component A → Component B → Component C
                ↓
            Component D
```

### Lưu ý quan trọng
- Điểm cần chú ý (version staleness, idempotency, replay safety, etc.)
```

4. **Chú ý** giải thích:
   - Tại sao dùng `ctx.run()` (replay safety)
   - Deterministic job ID tránh duplicate
   - Version-based staleness check
   - Explicit cancel invocations khi update/cancel
   - Consumer group isolation (mỗi consumer nhận bản sao riêng)
