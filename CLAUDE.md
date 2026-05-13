# CLAUDE.md — Project Rules

## Overview

Appointment booking backend with event-driven architecture:
**Restate** (durable execution) → **BullMQ** (email queue) → **Kafka/Redpanda** (event streaming) → **MongoDB** (persistence)

Tech stack: TypeScript, Hono, Restate SDK, BullMQ, KafkaJS, Mongoose, Zod, SendGrid, Pino logger.

---

## Coding Conventions

### TypeScript
- Strict mode enabled — không dùng `any`, không suppress lỗi bằng `@ts-ignore`
- ESM modules — tất cả import phải có đuôi `.js` (dù source là `.ts`)
- Dùng `type` import khi chỉ cần type: `import type { X } from "..."`
- Zod cho validation — định nghĩa schema trong `models/`, derive type bằng `z.infer<typeof Schema>`

### Naming
- Files: `kebab-case.ts` (vd: `email-queue.service.ts`, `appointment.controller.ts`)
- Variables/functions: `camelCase`
- Types/Interfaces: `PascalCase`
- Constants: `UPPER_SNAKE_CASE` cho config keys, `camelCase` cho computed values
- Zod schemas: `PascalCase` trùng tên với type (vd: `AppointmentState` schema → `AppointmentState` type)

### Comments
- Viết bằng tiếng Việt
- Comment giải thích WHY, không giải thích WHAT
- Mỗi file/function chính có 1 dòng JSDoc mô tả ngắn gọn

### Error Handling
- Dùng Zod `.parse()` ở boundary (controller) — throw ZodError nếu invalid
- Dùng `.safeParse()` khi cần handle error thủ công (vd: Kafka consumer skip invalid message)
- Log lỗi bằng `logger.error({ err, context }, "message")` — luôn kèm context object

---

## Architecture Patterns

### Restate Virtual Object (appointment lifecycle)
- File: `services/restate/appointment.handler.ts`
- Mỗi appointment là 1 virtual object, key = appointmentId
- State lưu trong Restate K/V (`ctx.get/set/clear`)
- Handlers: `create`, `update`, `cancel`, `sendReminder`, `markArrived`
- Side effects (MongoDB, Kafka, BullMQ) phải wrap trong `ctx.run()` để replay-safe
- Version-based staleness: mỗi lần update tăng version, delayed calls kiểm tra version trước khi xử lý
- Explicit cancel: lưu invocation IDs vào K/V, cancel invocations cũ trước khi schedule mới

### BullMQ Workers
- File pattern: `workers/<name>.worker.ts`
- Chạy process riêng biệt, kết nối Redis qua `createRedisConnection()`
- Job data validate bằng Zod schema
- Graceful shutdown: lắng nghe SIGINT/SIGTERM → `worker.close()`
- Deterministic job ID (từ appointmentId + version + reminder) để tránh duplicate trên replay

### Kafka Consumers
- File pattern: `workers/<name>.consumer.ts`
- Dùng `runConsumer()` hoặc `runChatConsumer()` wrapper từ `kafka.service.ts`
- Mỗi consumer có `groupId` riêng — nhận bản sao độc lập của events
- Decode Confluent wire format (Schema Registry) → validate Zod → skip invalid messages
- Backward compatible: fallback sang JSON thuần nếu message không có magic byte

### Schema Registry (Redpanda built-in)

- URL: `http://localhost:18081` (đã bật sẵn trong Redpanda)
- Dùng `@kafkajs/confluent-schema-registry` — Confluent-compatible
- JSON Schema files: `schemas/<name>.schema.json`
- Subject convention: `<topic-name>-value` (vd: `appointment-events-value`)
- Producer: register schema → encode Confluent wire format (`[0x00][schema ID 4 bytes][JSON]`)
- Consumer: decode wire format → validate Zod (double validation)
- Schema ID cache trong `config/schema-registry.ts` — tránh gọi registry mỗi lần publish
- Register schemas: `npm run schema:register`

### Kafka Connect (MongoDB Sink)

- Container riêng: `kafka-connect` (Confluent CP base + MongoDB connector plugin)
- Connector configs: `connectors/*.json`
- MongoDB Sink Connector: tự động ghi event từ `appointment-events` → MongoDB `event_logs`
- Dead letter queue: `appointment-events-dlq` cho messages lỗi
- Register connectors: `npm run connect:register`
- Quản lý qua Redpanda Console → tab Connect
- REST API: `http://localhost:8083/connectors`

### Repository Pattern (MongoDB)
- File pattern: `models/<name>.repository.ts`
- Hàm thuần (không class) — `upsertAppointment()`, `listAppointments()`
- Chỉ gọi từ bên trong `ctx.run()` (Restate) hoặc trực tiếp (controller read-only)

### Event Publishing (Kafka)
- Dùng `publishAppointmentEvent()` từ `event-publisher.service.ts`
- Event envelope: `{ eventId, type, appointmentId, version, occurredAt, payload }`
- Gọi bên trong `ctx.run()` để replay-safe

### Redpanda ACL (Access Control)

- Script setup: `scripts/setup-acl.sh` — tạo SASL users + ACL rules
- Chạy trong container: `docker exec -it appointment-backend-redpanda-0-1 bash < scripts/setup-acl.sh`
- Users: `admin` (superuser), `api-service` (producer), `telegram-consumer`, `chat-consumer`, `kafka-connect`
- Mỗi user chỉ truy cập được topic/group được phép (principle of least privilege)
- Bật bằng cách set `KAFKA_USERNAME`/`KAFKA_PASSWORD` trong `.env` cho từng service
- Code đã hỗ trợ SASL/SCRAM trong `config/kafka.ts` — tự bật khi có username/password

### Consumer Lag Monitoring

- Xem trực tiếp trên **Redpanda Console** → tab **Consumer Groups**
- Mỗi consumer group hiển thị: current offset, end offset, lag (số message chưa xử lý)
- Consumer groups trong dự án:
  - `appointment-telegram` — Telegram consumer
  - `chat-storage` — Chat consumer
  - `appointment-connect` — Kafka Connect MongoDB Sink

---

## Project Structure

```
config/          — env vars, Redis, Kafka, Restate, Schema Registry, BullMQ queue config
connectors/      — Kafka Connect: Dockerfile + connector JSON configs
controllers/     — Hono request handlers (parse input, gọi Restate, trả response)
middlewares/     — Hono middleware (error handling)
models/          — Zod schemas, Mongoose schemas, repository functions
routes/          — Hono router definitions
schemas/         — JSON Schema files cho Schema Registry (appointment-event, chat-message)
scripts/         — CLI scripts (dev, register Restate/schemas/connectors, clear queue)
services/
  database/      — MongoDB connection
  email/         — SendGrid integration
  messaging/     — Kafka producer/consumer, Telegram, SSE realtime
  queue/         — BullMQ queue setup
  restate/       — Restate Virtual Object handlers
utils/           — Logger, shared utilities
validation/      — Request validation schemas (extend base models)
workers/         — BullMQ workers + Kafka consumers (chạy process riêng)
```

### Quy tắc đặt file
- Service mới → tạo folder con trong `services/` nếu là domain mới, hoặc thêm file vào folder hiện có
- Worker/consumer mới → thêm vào `workers/`, thêm script vào `package.json`
- Validation schemas (cho API input) → `validation/`, model schemas (cho data) → `models/`
- Config mới → `config/`, utility functions → `utils/`

---

## Git & Workflow

### Commit Messages
- Format: `type: short description` (lowercase, tiếng Anh)
- Types: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`
- Ví dụ: `feat: add explicit cancel for Restate invocations on update`

### Trước khi commit
- Chạy `npx tsc --noEmit` — không commit nếu có type error
- Không commit file `.env` (chỉ commit `.env.example`)
- Không commit `node_modules/`, `dist/`, `logs/`

### Branch
- Main branch: `main`
- Feature branch: `feat/<tên-feature>` hoặc `fix/<tên-bug>`
- Không force push lên `main`

---

## Running the Project

```bash
# Start infrastructure (MongoDB, Redis, Redpanda, Restate, Kafka Connect)
docker compose up -d mongo redis redpanda-0 restate kafka-connect

# Dev mode (starts all services + workers)
npm run dev

# Individual services
npm run dev:app              # API server only
npm run worker:email:dev     # Email worker
npm run consumer:telegram:dev # Telegram consumer

# Schema Registry & Kafka Connect
npm run schema:register      # Register JSON schemas lên Redpanda
npm run connect:register     # Register MongoDB Sink connector

# Utils
npm run restate:register     # Register Restate endpoint
npm run queue:clear          # Clear BullMQ queues
npm run typecheck            # TypeScript check
```

## Key URLs (dev)

- API: http://localhost:9080
- BullMQ Dashboard: http://localhost:9080/admin/queues
- Restate Admin: http://localhost:19070
- Redpanda Console: http://localhost:8081
- Schema Registry: http://localhost:18081
- Kafka Connect REST: http://localhost:8083
