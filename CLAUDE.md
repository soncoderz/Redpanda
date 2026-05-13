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
- Parse JSON → validate Zod → skip invalid messages (không throw)

### Repository Pattern (MongoDB)
- File pattern: `models/<name>.repository.ts`
- Hàm thuần (không class) — `upsertAppointment()`, `listAppointments()`
- Chỉ gọi từ bên trong `ctx.run()` (Restate) hoặc trực tiếp (controller read-only)

### Event Publishing (Kafka)
- Dùng `publishAppointmentEvent()` từ `event-publisher.service.ts`
- Event envelope: `{ eventId, type, appointmentId, version, occurredAt, payload }`
- Gọi bên trong `ctx.run()` để replay-safe

---

## Project Structure

```
config/          — env vars, Redis, Kafka, Restate, BullMQ queue config
controllers/     — Hono request handlers (parse input, gọi Restate, trả response)
middlewares/     — Hono middleware (error handling)
models/          — Zod schemas, Mongoose schemas, repository functions
routes/          — Hono router definitions
scripts/         — CLI scripts (dev orchestrator, register Restate, clear queue)
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
# Start infrastructure (MongoDB, Redis, Redpanda, Restate)
docker compose up -d mongo redis redpanda-0 restate

# Dev mode (starts all services + workers)
npm run dev

# Individual services
npm run dev:app              # API server only
npm run worker:email:dev     # Email worker
npm run consumer:telegram:dev # Telegram consumer
npm run consumer:analytics:dev # Analytics consumer

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
