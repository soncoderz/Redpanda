# Appointment Booking Backend

Backend hệ thống đặt lịch hẹn xây dựng với Node.js/TypeScript, tích hợp nhiều công nghệ: Restate, BullMQ, Redis, Redpanda (Kafka), MongoDB, Hono, SendGrid, Telegram Bot.

## Tech Stack

| Công nghệ | Vai trò |
|---|---|
| **Hono** | REST API framework, health endpoint, queue dashboard, Server-Sent Events |
| **Restate** | Durable execution — đảm bảo mỗi bước (lưu DB, publish event, schedule job) không bị lặp lại khi crash/retry. Virtual Object lock per appointment |
| **MongoDB + Mongoose** | Lưu trữ dữ liệu lịch hẹn, reminder metadata, history, event log |
| **BullMQ + Redis** | Job queue gửi email nhắc lịch, delayed jobs, retry với exponential backoff, rate limiting |
| **Redpanda (Kafka)** | Event streaming — mỗi thay đổi appointment được publish 1 lần, nhiều consumer đọc độc lập |
| **SendGrid** | Gửi email nhắc lịch thật (fallback mock khi không có API key) |
| **Telegram Bot** | Gửi thông báo real-time khi có lịch hẹn mới/hủy/cập nhật |
| **Zod** | Validate dữ liệu đầu vào cho API, workflow, job, event |
| **Pino** | Structured logging (JSON format) |

## Cấu trúc thư mục

```
├── config/
│   └── env.ts                         Cấu hình environment
├── controllers/
│   ├── appointment.controller.ts      HTTP controllers (CRUD + bulk create)
│   ├── realtime.controller.ts         SSE streaming controller
│   └── queue-dashboard.controller.ts  BullMQ dashboard UI
├── middlewares/
│   └── error.middleware.ts            Global error handling
├── models/
│   ├── appointment.model.ts           Zod schemas (domain model)
│   ├── appointment.schema.ts          Mongoose schema + indexes
│   ├── appointment.repository.ts      MongoDB persistence
│   ├── event-log.schema.ts            Mongoose schema cho event log
│   └── event-log.repository.ts        Event log persistence (idempotent)
├── routes/
│   ├── appointment.routes.ts          REST API routes
│   └── realtime.routes.ts             SSE routes
├── scripts/
│   ├── email.worker.ts                BullMQ worker gửi email
│   ├── maintenance.worker.ts          Worker dọn dẹp queue
│   ├── analytics.consumer.ts          Kafka consumer → lưu event log
│   ├── telegram.consumer.ts           Kafka consumer → gửi Telegram
│   ├── register-restate.ts            Đăng ký endpoint với Restate
│   └── clear-queue.ts                 Xóa toàn bộ jobs trong queue
├── services/
│   ├── appointment/
│   │   └── appointment.service.ts     Restate virtual object (core logic)
│   ├── db/
│   │   ├── mongodb.service.ts         MongoDB connection
│   │   └── redis.service.ts           Redis connection (ioredis)
│   ├── mail/
│   │   └── mailer.service.ts          Gửi email qua SendGrid
│   ├── messaging/
│   │   ├── event-publisher.service.ts Publish event → Redpanda + SSE
│   │   ├── kafka.service.ts           KafkaJS producer/consumer
│   │   ├── realtime.service.ts        EventEmitter cho SSE
│   │   └── telegram.service.ts        Telegram Bot API
│   ├── queue/
│   │   ├── email-queue.service.ts     BullMQ queue cho email
│   │   └── maintenance-queue.service.ts BullMQ queue cho maintenance
│   └── restate/
│       ├── restate-client.service.ts  Restate ingress client
│       └── restate-endpoint.service.ts Restate HTTP endpoint
├── utils/
│   ├── appointment.utils.ts           Helper functions
│   └── logger.ts                      Pino logger
├── validation/
│   └── appointment.validation.ts      Request validation schemas
├── postman/                           Postman collection (import để test)
├── docker-compose.yml                 Docker Compose
├── Dockerfile                         Multi-stage build
├── app.ts                             Hono app setup
├── server.ts                          Main entry point
└── .env.example                       Template cấu hình
```

## Kiến trúc tổng quan

```
                                    ┌─────────────────────────────────────┐
                                    │           Redpanda (Kafka)          │
                                    │       topic: appointment-events     │
                                    └──────┬──────────────┬───────────────┘
                                           │              │
                                      ┌────▼────┐   ┌────▼──────┐
                                      │Analytics│   │ Telegram  │
                                      │Consumer │   │ Consumer  │
                                      └────┬────┘   └────┬──────┘
                                           │              │
                                      ┌────▼────┐   ┌────▼──────┐
                                      │MongoDB  │   │Telegram   │
                                      │EventLog │   │Bot API    │
                                      └─────────┘   └───────────┘

┌──────┐    ┌──────────┐    ┌─────────┐    ┌─────────┐    ┌──────────┐
│Client│───▶│API (Hono)│───▶│ Restate │───▶│MongoDB  │    │ BullMQ   │
└──────┘    └──────────┘    │ Virtual │    │Appoint- │    │ + Redis  │
                  │         │ Object  │───▶│ments    │    └────┬─────┘
                  │         └─────────┘    └─────────┘         │
                  │              │                        ┌────▼─────┐
                  │              └───────────────────────▶│Email     │
                  │                   publish event       │Worker    │
             ┌────▼────┐              + schedule jobs     └────┬─────┘
             │SSE      │                                       │
             │(Browser)│                                  ┌────▼─────┐
             └─────────┘                                  │SendGrid  │
                                                          └──────────┘
```

**Điểm mạnh của Redpanda trong kiến trúc này:**
- Producer (Restate) chỉ publish event **1 lần**
- Nhiều consumer đọc **cùng 1 stream**, chạy **độc lập**
- Thêm consumer mới (Telegram, Webhook, Statistics...) = thêm 1 file script, **không sửa code cũ**

## Luồng hoạt động

### 1. Tạo lịch hẹn

```
Client          API (Hono)         Restate            MongoDB         Redpanda          BullMQ
  │                │                  │                  │                │                │
  │ POST /appointments                │                  │                │                │
  │───────────────▶│                  │                  │                │                │
  │                │  create()        │                  │                │                │
  │                │─────────────────▶│                  │                │                │
  │                │                  │  save appointment │                │                │
  │                │                  │─────────────────▶│                │                │
  │                │                  │                  │                │                │
  │                │                  │  publish "appointment.created"    │                │
  │                │                  │─────────────────────────────────▶│                │
  │                │                  │                  │                │                │
  │                │                  │  schedule 3 delayed jobs (before/atTime/after)     │
  │                │                  │──────────────────────────────────────────────────▶│
  │                │                  │                  │                │                │
  │                │◀─────────────────│                  │                │                │
  │ 201 Created    │                  │                  │                │                │
  │◀───────────────│                  │                  │                │                │
  │                │                  │                  │                │                │
  │                │                  │       ┌──────────┴──────────┐                     │
  │                │                  │       │  Redpanda consumers │                     │
  │                │                  │       │  (chạy song song)   │                     │
  │                │                  │       ├─────────────────────┤                     │
  │                │                  │       │ Analytics → MongoDB │                     │
  │                │                  │       │ Telegram → Bot API  │                     │
  │                │                  │       └─────────────────────┘                     │
```

### 2. Gửi email nhắc lịch (khi đến giờ)

```
BullMQ Worker       Restate            MongoDB          SendGrid         Redpanda
     │                 │                  │                 │                │
     │  (delayed job fires — 1 phút trước giờ hẹn)         │                │
     │                 │                  │                 │                │
     │ startReminderDelivery()            │                 │                │
     │────────────────▶│                  │                 │                │
     │                 │  load appointment│                 │                │
     │                 │─────────────────▶│                 │                │
     │                 │  check: version, │                 │                │
     │                 │  status, sent?   │                 │                │
     │  shouldSend:true│                  │                 │                │
     │◀────────────────│                  │                 │                │
     │                 │                  │                 │                │
     │  sgMail.send() (gửi email nhắc lịch)                │                │
     │─────────────────────────────────────────────────────▶│                │
     │  200 OK         │                  │                 │                │
     │◀─────────────────────────────────────────────────────│                │
     │                 │                  │                 │                │
     │ recordReminderResult(status:"sent")│                 │                │
     │────────────────▶│                  │                 │                │
     │                 │  update reminder │                 │                │
     │                 │─────────────────▶│                 │                │
     │                 │                  │                 │                │
     │                 │  publish "reminder.sent"                            │
     │                 │───────────────────────────────────────────────────▶│
     │                 │                  │                 │                │
     │                 │                  │         ┌───────┴───────┐       │
     │                 │                  │         │   Consumers   │       │
     │                 │                  │         │ Analytics ✓   │       │
     │                 │                  │         │ Telegram  ✓   │       │
     │                 │                  │         └───────────────┘       │
```

### 3. Consumers từ Redpanda (chạy song song, độc lập)

```
                    Redpanda
                 (appointment-events)
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
   ┌─────────────┐ ┌──────────┐ ┌──────────────┐
   │  Analytics  │ │ Telegram │ │  (Consumer   │
   │  Consumer   │ │ Consumer │ │   mới...)    │
   │             │ │          │ │              │
   │ Group:      │ │ Group:   │ │ Group:       │
   │ appointment-│ │ appoint- │ │ appointment- │
   │ analytics   │ │ telegram │ │ xxx          │
   └──────┬──────┘ └────┬─────┘ └──────────────┘
          │              │
          ▼              ▼
   ┌─────────────┐ ┌──────────┐
   │  MongoDB    │ │ Telegram │
   │  event_logs │ │ Bot API  │
   └─────────────┘ └──────────┘

Mỗi consumer có group ID riêng → đọc CÙNG 1 stream event
Thêm consumer mới = thêm 1 file script + 1 group ID
KHÔNG cần sửa code appointment service
```

### 4. Real-time Events (SSE cho browser)

```
Browser                    API (Hono)         EventEmitter
   │                          │                    │
   │ GET /api/events/appointments                  │
   │─────────────────────────▶│                    │
   │  SSE stream opened       │                    │
   │◀─────────────────────────│                    │
   │                          │                    │
   │                          │  (khi có event)    │
   │                          │◀───────────────────│
   │  event: appointment.created                   │
   │◀─────────────────────────│                    │
   │                          │                    │
   │                          │◀───────────────────│
   │  event: appointment.cancelled                 │
   │◀─────────────────────────│                    │
   │                          │                    │

Lưu ý: SSE dùng EventEmitter trong process (không qua Redpanda)
→ chỉ nhận event từ cùng process API server
```

## Chi tiết từng bước

1. Client gọi `POST /api/appointments` với thông tin lịch hẹn (có thể kèm `Idempotency-Key`)
2. Controller gọi **Restate virtual object** `Appointment` (keyed by appointment ID)
3. Restate lưu appointment vào **MongoDB** trong `ctx.run()` (idempotent, crash-safe)
4. Restate publish event `appointment.created` tới **Redpanda** qua KafkaJS
5. Restate schedule 3 delayed jobs trong **BullMQ** (qua Redis):
   - `before` — 1 phút trước giờ hẹn
   - `atTime` — đúng giờ hẹn
   - `after` — 1 phút sau giờ hẹn
6. Khi đến giờ, **Email Worker** xử lý job:
   - Gọi `startReminderDelivery()` trên Restate để kiểm tra (version đúng? đã cancelled? đã gửi chưa?)
   - Nếu `shouldSend: true` → gửi email qua **SendGrid**
   - Gọi `recordReminderResult()` để ghi kết quả vào MongoDB và publish `reminder.sent` tới Redpanda
7. **Analytics consumer** đọc events từ Redpanda → lưu event log vào MongoDB
8. **Telegram consumer** đọc events từ Redpanda → gửi thông báo vào Telegram Bot
9. **SSE endpoint** push real-time events tới browser (qua EventEmitter, không qua Redpanda)

## Vai trò của từng công nghệ

### Restate — Durable Execution

```typescript
// Mỗi ctx.run() là 1 checkpoint
// Nếu crash giữa chừng → restart từ bước bị lỗi, không chạy lại bước đã xong
await ctx.run("save to MongoDB", () => createAppointmentRecord(appointment));
await ctx.run("publish event", () => publishAppointmentEvent(event));
await ctx.run("schedule jobs", () => scheduleReminderJobs(appointment));
```

- **Virtual Object lock**: chỉ 1 request xử lý cùng lúc trên 1 appointment ID → không race condition
- **Crash recovery**: server crash → restart từ bước bị lỗi
- **Validate trước khi gửi email**: Worker gọi Restate kiểm tra version, status trước khi gửi

### BullMQ + Redis — Delayed Job Queue

- Hẹn giờ gửi 3 email nhắc lịch (before / atTime / after)
- Dashboard xem trực quan job: `http://localhost:9080/admin/queues`
- Retry tự động với exponential backoff
- Rate limiting: tối đa N email/giây
- Hủy job dễ dàng khi update/cancel appointment

### Redpanda (Kafka) — Event Streaming

- Mỗi thay đổi appointment → 1 event trên topic `appointment-events`
- Nhiều consumer đọc cùng 1 topic, **mỗi consumer có group ID riêng**
- Thêm tính năng mới = thêm consumer mới, **không sửa code cũ**
- Hiện tại có 2 consumer: Analytics + Telegram

## Yêu cầu

- **Node.js** >= 20
- **Docker Desktop** (cho Restate, Redis, Redpanda)
- **MongoDB** (local hoặc Docker)

## Chạy Local (development)

### Bước 1: Cài đặt dependencies

```bash
npm install
```

### Bước 2: Cấu hình environment

```bash
copy .env.example .env
```

Chỉnh sửa `.env`:

```env
# Quan trọng: Restate chạy trong Docker, cần dùng host.docker.internal
PUBLIC_RESTATE_ENDPOINT=http://host.docker.internal:9080/restate

# Gửi email thật (bỏ trống = mock, chỉ log)
SENDGRID_API_KEY=SG.xxxxx
SENDGRID_FROM_EMAIL=your-email@example.com

# Telegram Bot (lấy từ @BotFather)
TELEGRAM_BOT_TOKEN=your-bot-token
TELEGRAM_CHAT_ID=your-chat-id
```

> **Lưu ý:** Vì Restate chạy trong Docker container, `PUBLIC_RESTATE_ENDPOINT` phải dùng `host.docker.internal` thay vì `localhost`.

### Bước 3: Khởi động infrastructure (Docker)

```bash
docker compose up -d redis redpanda-0 redpanda-console restate
```

> MongoDB có thể dùng local (nếu đã cài sẵn) hoặc thêm `mongo` vào lệnh trên.

### Bước 4: Chạy app và workers

Mở **5 terminal** riêng biệt:

```bash
# Terminal 1: API server
npm run dev

# Terminal 2: Đăng ký Restate endpoint (chạy 1 lần sau khi API sẵn sàng)
npm run restate:register

# Terminal 3: Email worker
npm run worker:email:dev

# Terminal 4: Analytics consumer (Redpanda → MongoDB)
npm run consumer:analytics:dev

# Terminal 5: Telegram consumer (Redpanda → Telegram Bot)
npm run consumer:telegram:dev
```

Optional:

```bash
# Terminal 6: Maintenance worker (dọn dẹp queue định kỳ)
npm run worker:maintenance:dev
```

### Tạo Telegram Bot

1. Mở Telegram → tìm **@BotFather** → gửi `/newbot`
2. Đặt tên + username → nhận **Token**
3. Gửi tin nhắn bất kỳ cho bot
4. Mở trình duyệt: `https://api.telegram.org/bot<TOKEN>/getUpdates`
5. Tìm `"chat":{"id": 123456}` → đó là **Chat ID**
6. Điền vào `.env`:
   ```env
   TELEGRAM_BOT_TOKEN=7123456789:AAH...
   TELEGRAM_CHAT_ID=123456789
   ```

## NPM Scripts

| Script | Mô tả |
|---|---|
| `npm run dev` | API server (hot-reload) |
| `npm run build` | Build TypeScript → `dist/` |
| `npm run start` | Production build |
| `npm run typecheck` | Kiểm tra type errors |
| `npm run worker:email:dev` | Email worker (dev) |
| `npm run worker:maintenance:dev` | Maintenance worker (dev) |
| `npm run consumer:analytics:dev` | Analytics consumer (dev) |
| `npm run consumer:telegram:dev` | Telegram consumer (dev) |
| `npm run restate:register` | Đăng ký endpoint với Restate |
| `npm run queue:clear` | Xóa tất cả jobs trong BullMQ |

## API Endpoints

### REST API

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/` | Thông tin API |
| `GET` | `/health` | Health check (MongoDB, Kafka, Restate) |
| `POST` | `/api/appointments` | Tạo lịch hẹn mới |
| `POST` | `/api/appointments/bulk` | Tạo nhiều lịch hẹn (batch) |
| `GET` | `/api/appointments` | Danh sách lịch hẹn |
| `GET` | `/api/appointments/:id` | Chi tiết lịch hẹn |
| `PATCH` | `/api/appointments/:id` | Cập nhật lịch hẹn (partial) |
| `DELETE` | `/api/appointments/:id` | Hủy lịch hẹn |

### Server-Sent Events

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/api/events/appointments` | Stream real-time events |

### Admin

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/admin/queues` | BullMQ dashboard (mở trên browser) |

## API Examples

### Tạo lịch hẹn

```bash
curl -X POST http://localhost:9080/api/appointments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  -d '{
    "customerName": "Nguyen Van A",
    "customerEmail": "nguyenvana@example.com",
    "service": "Cat toc",
    "startAt": "2026-05-12T10:30:00.000Z",
    "note": "Lan dau"
  }'
```

| Field | Type | Required | Mô tả |
|---|---|---|---|
| `customerName` | string | Yes | Tên khách hàng |
| `customerEmail` | string (email) | Yes | Email nhận reminder |
| `service` | string | Yes | Tên dịch vụ |
| `startAt` | string (ISO 8601) | Yes | Thời gian hẹn |
| `note` | string (max 2000) | No | Ghi chú |
| `id` | string | No | Custom appointment ID |
| `idempotencyKey` | string | No | Key chống trùng (hoặc header `Idempotency-Key`) |

### Tạo nhiều lịch hẹn (bulk)

```bash
curl -X POST http://localhost:9080/api/appointments/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "count": 10,
    "service": "Cat toc",
    "startAt": "2026-05-12T10:00:00.000Z",
    "intervalMinutes": 5
  }'
```

### Danh sách lịch hẹn

```bash
curl "http://localhost:9080/api/appointments?status=booked&limit=20"
```

### Cập nhật lịch hẹn

```bash
curl -X PATCH http://localhost:9080/api/appointments/{id} \
  -H "Content-Type: application/json" \
  -d '{ "startAt": "2026-05-12T11:00:00.000Z" }'
```

> Cập nhật sẽ tăng version, cancel reminder cũ, schedule reminder mới.

### Hủy lịch hẹn

```bash
curl -X DELETE http://localhost:9080/api/appointments/{id}
```

> Hủy sẽ cancel tất cả pending reminder emails.

### SSE real-time events

```bash
curl -N http://localhost:9080/api/events/appointments
```

## Postman Collection

Import file `postman/Redpanda-Appointment-API.postman_collection.json` vào Postman.

1. Mở Postman → **Import** (Ctrl+O)
2. Chọn file collection
3. Tự động lưu `appointmentId` khi tạo mới

## Event Contract (Redpanda)

Tất cả events trên topic `appointment-events`:

```json
{
  "eventId": "appointment.created:apt-001:1",
  "type": "appointment.created",
  "appointmentId": "apt-001",
  "version": 1,
  "occurredAt": "2026-05-11T03:00:00.000Z",
  "payload": {
    "appointment": { ... }
  }
}
```

| Event | Khi nào |
|---|---|
| `appointment.created` | Lịch hẹn được tạo |
| `appointment.updated` | Lịch hẹn được cập nhật |
| `appointment.cancelled` | Lịch hẹn bị hủy |
| `appointment.reminder.sent` | Email nhắc lịch đã gửi |

## Email Reminders

3 email nhắc lịch cho mỗi appointment:

| Loại | Thời điểm | Nội dung |
|---|---|---|
| `before` | 1 phút trước giờ hẹn | "Lịch hẹn của bạn sắp bắt đầu" |
| `atTime` | Đúng giờ hẹn | "Đã đến giờ hẹn của bạn" |
| `after` | 1 phút sau giờ hẹn | "Lịch hẹn của bạn đã qua giờ" |

**Cơ chế an toàn:**
- Job ID deterministic → không gửi trùng
- Worker kiểm tra với Restate trước khi gửi (version match, chưa cancelled, chưa gửi)
- Retry với exponential backoff khi SendGrid lỗi tạm thời
- Lỗi 401/403 (auth) → skip, không retry
- Update appointment → cancel jobs cũ, schedule jobs mới
- Cancel appointment → cancel tất cả pending jobs

## Telegram Notifications

Khi chạy Telegram consumer, bot sẽ gửi thông báo vào chat khi có:
- Lịch hẹn mới được tạo
- Lịch hẹn được cập nhật
- Lịch hẹn bị hủy
- Email nhắc lịch đã gửi

**Đây là ví dụ thực tế về sức mạnh của Redpanda**: thêm Telegram consumer mà không sửa bất kỳ dòng code nào trong appointment service.

## URLs khi chạy local

| Service | URL |
|---|---|
| API | http://localhost:9080 |
| Health Check | http://localhost:9080/health |
| BullMQ Dashboard | http://localhost:9080/admin/queues |
| SSE Events | http://localhost:9080/api/events/appointments |
| Restate Ingress | http://localhost:18080 |
| Restate Admin | http://localhost:19070 |
| Redpanda Console | http://localhost:8081 |

## Cấu hình quan trọng

| Biến | Mặc định | Mô tả |
|---|---|---|
| `PORT` | `9080` | Port API server |
| `PUBLIC_RESTATE_ENDPOINT` | `http://localhost:9080/restate` | URL Restate gọi tới app (**dùng `host.docker.internal`**) |
| `MONGODB_URI` | `mongodb://localhost:27017/appointments` | MongoDB |
| `REDIS_URL` | `redis://localhost:6379` | Redis |
| `KAFKA_BROKERS` | `localhost:19092` | Redpanda brokers |
| `SENDGRID_API_KEY` | *(trống = mock)* | SendGrid API key |
| `TELEGRAM_BOT_TOKEN` | *(trống)* | Telegram bot token |
| `TELEGRAM_CHAT_ID` | *(trống)* | Telegram chat ID |
| `REMINDER_BEFORE_MS` | `60000` | Nhắc trước giờ hẹn (ms) |
| `REMINDER_AFTER_MS` | `60000` | Nhắc sau giờ hẹn (ms) |

Xem `.env.example` để biết tất cả biến cấu hình.

## Docker Services

| Service | Image | Ports |
|---|---|---|
| `redis` | redis:7.4-alpine | 6379 |
| `redpanda-0` | redpanda:v26.1.6 | 19092, 18081, 18082 |
| `redpanda-console` | console:v3.7.2 | 8081 |
| `restate` | restate:latest | 18080, 19070 |
| `mongo` | mongo:8.0 | 27017 |
| `api` | (Dockerfile) | 9080 |
| `email-worker` | (Dockerfile) | - |
| `analytics-consumer` | (Dockerfile) | - |

## Troubleshooting

### Lỗi "service 'Appointment' not found"

Restate chưa đăng ký endpoint:

```bash
npm run restate:register
```

### Lỗi "handler 'xxx' was not found"

Restate có phiên bản cũ. Force re-register:

```bash
curl http://localhost:19070/deployments \
  -H "content-type: application/json" \
  -d '{"uri": "http://host.docker.internal:9080/restate", "force": true}'
```

### Lỗi "unable to reach the remote endpoint"

Restate trong Docker không thể gọi `localhost`. Đổi trong `.env`:

```env
PUBLIC_RESTATE_ENDPOINT=http://host.docker.internal:9080/restate
```

### Email không gửi (chỉ mock)

Kiểm tra `SENDGRID_API_KEY` trong `.env`. Bỏ trống = mock (chỉ log).

### Telegram không nhận thông báo

1. Kiểm tra `TELEGRAM_BOT_TOKEN` và `TELEGRAM_CHAT_ID` trong `.env`
2. Đảm bảo đã gửi ít nhất 1 tin nhắn cho bot trước
3. Đảm bảo `npm run consumer:telegram:dev` đang chạy

### Lỗi "Appointment xxx does not exist" từ BullMQ

Job cũ còn sót trong Redis. Xóa bằng:

```bash
npm run queue:clear
```
