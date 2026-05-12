# Appointment Booking Backend

Hệ thống đặt lịch hẹn với kiến trúc event-driven, sử dụng **Restate** (durable execution), **BullMQ** (job queue), **Redpanda/Kafka** (event streaming), **MongoDB**, **Hono**, **SendGrid**, **Telegram Bot**.

## Kiến trúc tổng quan

```
┌──────────┐     ┌──────────┐     ┌──────────────────────────────────┐
│  Client  │────▶│API (Hono)│────▶│  Restate Virtual Object          │
└──────────┘     └──────────┘     │  (keyed by appointment ID)       │
                      │           │                                  │
                      │           │  1. ctx.run("save") → MongoDB    │
                      │           │  2. ctx.run("publish") → Kafka   │
                      │           │  3. ctx.objectSendClient()       │
                      │           │     .sendReminder(delay: 1 min)  │
                      │           └──────────┬───────────────────────┘
                      │                      │
                 ┌────▼────┐                 │ (khi delay hết hạn)
                 │  SSE    │                 ▼
                 │(Browser)│     ┌──────────────────────┐
                 └─────────┘     │  sendReminder handler │
                                 │  - check version      │
                                 │  - check status       │
                                 │  - enqueue BullMQ     │
                                 └──────────┬────────────┘
                                            │
                                            ▼
                                 ┌──────────────────────┐
                                 │  BullMQ Email Worker  │
                                 │  1. startReminder     │
                                 │     Delivery()        │
                                 │  2. SendGrid gửi mail │
                                 │  3. recordReminder    │
                                 │     Result()          │
                                 └──────────────────────┘

┌────────────────────────── Redpanda (Kafka) ──────────────────────────┐
│                     topic: appointment-events                        │
│                     topic: chat-messages                              │
└──────┬──────────────────┬──────────────────┬─────────────────────────┘
       │                  │                  │
  ┌────▼─────┐      ┌────▼──────┐     ┌────▼──────┐
  │Analytics │      │ Telegram  │     │   Chat    │
  │Consumer  │      │ Consumer  │     │ Consumer  │
  │→ MongoDB │      │→ Bot API  │     │→ MongoDB  │
  └──────────┘      └───────────┘     └───────────┘
```

## Ba công nghệ chính làm gì?

### Restate — Durable Execution (điều phối trung tâm)

Restate là "bộ não" của hệ thống. Mỗi appointment là một **Virtual Object** có key riêng.

**Vai trò:**
- **Lock per appointment**: chỉ 1 request xử lý 1 appointment tại 1 thời điểm → không race condition
- **Checkpoint**: mỗi `ctx.run()` là 1 bước không lặp lại — crash rồi restart sẽ chạy tiếp từ bước lỗi
- **Delay scheduling**: `ctx.objectSendClient().sendReminder(input, { delay: ms })` — Restate tự nhớ và gọi lại handler sau N phút, không cần cron hay delayed queue
- **Version validation**: khi delay hết hạn, handler `sendReminder` kiểm tra version appointment — nếu đã bị update/cancel thì skip, không gửi email cũ

```
Tạo appointment → Restate delay 3 cuộc gọi (before / atTime / after)
                → 1 phút trước giờ hẹn: Restate gọi sendReminder
                → sendReminder check version OK → đẩy job vào BullMQ
                → nếu appointment đã bị cancel → skip, không làm gì
```

### BullMQ + Redis — Job Queue (gửi email)

BullMQ nhận job **ngay lập tức** (không delay) từ Restate và xử lý gửi email.

**Vai trò:**
- **Rate limiting**: tối đa N email/giây (tránh bị SendGrid block)
- **Retry + exponential backoff**: gửi lỗi → tự thử lại
- **Concurrency control**: worker xử lý song song nhiều job
- **Dashboard**: xem trạng thái job trực quan tại `/admin/queues`
- **3-step process**: validate qua Restate → gửi mail SendGrid → ghi kết quả về Restate

### Redpanda/Kafka — Event Streaming (fan-out)

Mỗi thay đổi appointment được publish **1 lần** lên Kafka topic. Nhiều consumer đọc **cùng 1 stream** độc lập.

**Vai trò:**
- **Decouple**: appointment service chỉ publish event, không biết ai đọc
- **Fan-out**: 1 event → nhiều consumer xử lý (analytics, telegram, webhook...)
- **Mở rộng**: thêm consumer mới = thêm 1 file + 1 group ID, **không sửa code cũ**

```
Restate publish event ──▶ Redpanda topic
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
              Analytics   Telegram   (Mới...)
              → MongoDB   → Bot     → Webhook
```

## Luồng hoạt động chi tiết

### 1. Tạo lịch hẹn

```
Client → POST /api/appointments → Controller → Restate create()

Restate (Virtual Object "Appointment"):
  ├── ctx.run("save")     → Lưu MongoDB (appointments collection)
  ├── ctx.run("publish")  → Publish event "appointment.created" lên Kafka
  ├── ctx.objectSendClient().sendReminder({ reminder: "before", delay: ... })
  ├── ctx.objectSendClient().sendReminder({ reminder: "atTime", delay: ... })
  ├── ctx.objectSendClient().sendReminder({ reminder: "after",  delay: ... })
  └── return appointment → Client nhận 201 Created

Song song (từ Kafka):
  ├── Analytics consumer → lưu event log vào MongoDB
  └── Telegram consumer  → gửi thông báo "Lịch hẹn mới" vào Telegram
```

### 2. Khi đến giờ gửi email (Restate delay hết hạn)

```
Restate tự gọi → sendReminder handler (cùng Virtual Object)

sendReminder:
  ├── Load appointment từ Restate state
  ├── Check: version khớp? status = "booked"? chưa gửi?
  │   ├── KHÔNG khớp → skip (appointment đã bị update/cancel)
  │   └── OK → ctx.run("enqueue email") → đẩy job vào BullMQ (KHÔNG delay)
  └── Cập nhật reminder status: queued

BullMQ Email Worker nhận job:
  ├── Bước 1: Gọi Restate startReminderDelivery() → validate lần nữa
  ├── Bước 2: SendGrid gửi email (hoặc mock nếu không có API key)
  └── Bước 3: Gọi Restate recordReminderResult() → ghi kết quả vào MongoDB
```

### 3. Cập nhật / Hủy lịch hẹn

```
Update: Client → PATCH /api/appointments/:id → Restate update()
  ├── Tăng version (v1 → v2)
  ├── Lưu MongoDB + publish event + schedule reminder MỚI (version v2)
  └── Reminder CŨ (version v1) khi đến giờ → sendReminder check version → SKIP

Cancel: Client → DELETE /api/appointments/:id → Restate cancel()
  ├── Tăng version, status = "cancelled"
  ├── Lưu MongoDB + publish event
  └── Tất cả reminder cũ khi đến giờ → sendReminder check status → SKIP

→ KHÔNG cần cancel job trong BullMQ — Restate version check tự loại bỏ job cũ
```

### 4. Chat real-time

```
Client → POST /api/chat/send → Kafka topic "chat-messages" + EventEmitter

Song song:
  ├── Chat consumer (Kafka → MongoDB): lưu message vào collection chat_messages
  └── SSE stream (EventEmitter → Browser): push message real-time tới client

Client → GET /api/chat/stream/:roomId → SSE connection nhận message theo phòng
```

## Cấu trúc thư mục

```
├── config/                          Cấu hình kết nối
│   ├── env.ts                       Biến môi trường (với giá trị mặc định)
│   ├── kafka.ts                     Kafka client + producer
│   ├── queues.ts                    BullMQ queue instances
│   ├── redis.ts                     Redis connection (cho BullMQ)
│   └── restate.ts                   Restate client + HTTP endpoint
│
├── controllers/                     HTTP request handlers
│   ├── appointment.controller.ts    CRUD lịch hẹn (gọi Restate)
│   └── chat.controller.ts          Gửi + stream chat messages
│
├── middlewares/
│   └── error.middleware.ts          Xử lý lỗi → HTTP status codes
│
├── models/                          Data models + database
│   ├── appointment.model.ts         Zod schemas (domain types)
│   ├── appointment.schema.ts        Mongoose schema + indexes
│   ├── appointment.repository.ts    MongoDB CRUD operations
│   ├── chat.model.ts                Zod schemas cho chat
│   ├── chat.schema.ts               Mongoose schema chat
│   ├── chat.repository.ts           MongoDB operations chat
│   └── event-log.repository.ts      Lưu event log (idempotent)
│
├── routes/                          Route definitions
│   ├── appointment.routes.ts        POST/GET/PATCH/DELETE /api/appointments
│   └── chat.routes.ts               POST /api/chat/send, GET /api/chat/stream
│
├── services/
│   ├── restate/                     Restate Virtual Object
│   │   └── appointment.handler.ts   Core logic: create/update/cancel/sendReminder
│   ├── database/
│   │   └── mongodb.service.ts       MongoDB connection + health check
│   ├── email/
│   │   └── email.service.ts         Gửi email qua SendGrid (hoặc mock)
│   ├── messaging/
│   │   ├── kafka.service.ts         Kafka producer/consumer + topic management
│   │   ├── event-publisher.service.ts  Publish appointment events lên Kafka
│   │   ├── chat.service.ts          Publish chat + EventEmitter fan-out
│   │   ├── realtime.service.ts      EventEmitter cho SSE stream
│   │   └── telegram.service.ts      Telegram Bot API + format messages
│   └── queue/
│       ├── email-queue.service.ts   BullMQ: enqueue email job (immediate)
│       └── maintenance-queue.service.ts  BullMQ: scheduled cleanup
│
├── workers/                         Background processes
│   ├── email.worker.ts              BullMQ worker: validate → gửi mail → ghi kết quả
│   ├── maintenance.worker.ts        BullMQ worker: dọn dẹp job cũ định kỳ
│   ├── analytics.consumer.ts       Kafka consumer: events → MongoDB event_logs
│   ├── telegram.consumer.ts        Kafka consumer: events → Telegram Bot
│   └── chat.consumer.ts            Kafka consumer: chat → MongoDB chat_messages
│
├── scripts/                         Tools & utilities
│   ├── dev.ts                       Orchestrator: 1 lệnh chạy tất cả
│   ├── register-restate.ts          Đăng ký endpoint với Restate Admin API
│   └── clear-queue.ts               Xóa toàn bộ BullMQ jobs
│
├── validation/
│   └── appointment.validation.ts    Validate request body/query
│
├── utils/
│   ├── appointment.utils.ts         Helpers: ID generation, reminder timing
│   └── logger.ts                    Pino structured logger
│
├── public/
│   └── chat.html                    Chat UI demo
│
├── postman/                         Postman collection (import để test API)
├── app.ts                           Hono app: routes + middleware + SSE + Restate
├── server.ts                        Entry point: connect DB → start HTTP server
├── docker-compose.yml               Infrastructure + production containers
└── Dockerfile                       Multi-stage build
```

## Chạy dự án

### Cách 1: Một lệnh duy nhất (khuyến nghị)

```bash
npm install
npm run dev
```

Lệnh `npm run dev` sẽ tự động:
1. Khởi động Docker containers (MongoDB, Redis, Redpanda, Restate)
2. Chờ tất cả services sẵn sàng
3. Chạy API server + tất cả workers/consumers
4. Đăng ký Restate endpoint
5. Khi nhấn `Ctrl+C` → dừng tất cả processes + Docker containers

Output hiển thị với màu riêng cho từng process:
```
[dev]          All services running! Press Ctrl+C to stop.
[api         ] Hono API listening
[email-worker] Appointment email worker started
[maintenance ] Maintenance worker started
[analytics   ] Consumer started
[telegram    ] Consumer started
[chat        ] Chat consumer started
```

### Cách 2: Chạy từng process riêng

```bash
# Terminal 1: Infrastructure
docker compose up -d mongo redis redpanda-0 redpanda-console restate

# Terminal 2: API server
npm run dev:app

# Terminal 3: Đăng ký Restate (chạy 1 lần)
npm run restate:register

# Terminal 4-8: Workers & Consumers
npm run worker:email:dev
npm run worker:maintenance:dev
npm run consumer:analytics:dev
npm run consumer:telegram:dev
npm run consumer:chat:dev
```

### Cấu hình .env

```bash
copy .env.example .env
```

Các biến quan trọng:

| Biến | Mặc định | Mô tả |
|---|---|---|
| `PORT` | `9080` | Port API server |
| `PUBLIC_RESTATE_ENDPOINT` | `http://localhost:9080/restate` | URL Restate gọi tới app (**dùng `host.docker.internal` khi Restate chạy trong Docker**) |
| `MONGODB_URI` | `mongodb://localhost:27017/appointments` | MongoDB connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis cho BullMQ |
| `KAFKA_BROKERS` | `localhost:19092` | Redpanda broker |
| `SENDGRID_API_KEY` | *(trống = mock)* | Để trống = chỉ log, không gửi mail thật |
| `TELEGRAM_BOT_TOKEN` | *(trống)* | Token từ @BotFather |
| `TELEGRAM_CHAT_ID` | *(trống)* | Chat ID nhận thông báo |
| `REMINDER_BEFORE_MS` | `60000` | Nhắc trước giờ hẹn (ms) |
| `REMINDER_AFTER_MS` | `60000` | Nhắc sau giờ hẹn (ms) |

## API Endpoints

### Appointments

| Method | Endpoint | Mô tả |
|---|---|---|
| `POST` | `/api/appointments` | Tạo lịch hẹn |
| `POST` | `/api/appointments/bulk` | Tạo nhiều lịch hẹn (stress test) |
| `GET` | `/api/appointments` | Danh sách (filter: `?status=booked&limit=20`) |
| `GET` | `/api/appointments/:id` | Chi tiết 1 lịch hẹn |
| `PATCH` | `/api/appointments/:id` | Cập nhật (tăng version, reschedule reminders) |
| `DELETE` | `/api/appointments/:id` | Hủy (cancel reminders) |

### Chat

| Method | Endpoint | Mô tả |
|---|---|---|
| `POST` | `/api/chat/send` | Gửi tin nhắn |
| `GET` | `/api/chat/stream/:roomId` | SSE stream nhận tin nhắn real-time |

### System

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/` | Thông tin API |
| `GET` | `/health` | Health check (MongoDB, Kafka, Restate) |
| `GET` | `/admin/queues` | BullMQ Dashboard UI |
| `GET` | `/api/events/appointments` | SSE stream appointment events |
| `GET` | `/chat` | Chat UI demo |

## API Examples

```bash
# Tạo lịch hẹn
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

# Danh sách lịch hẹn
curl "http://localhost:9080/api/appointments?status=booked&limit=20"

# Cập nhật (tăng version, schedule reminder mới)
curl -X PATCH http://localhost:9080/api/appointments/{id} \
  -H "Content-Type: application/json" \
  -d '{ "startAt": "2026-05-12T11:00:00.000Z" }'

# Hủy lịch hẹn
curl -X DELETE http://localhost:9080/api/appointments/{id}

# SSE real-time events
curl -N http://localhost:9080/api/events/appointments

# Gửi chat
curl -X POST http://localhost:9080/api/chat/send \
  -H "Content-Type: application/json" \
  -d '{ "roomId": "room-1", "from": "user1", "text": "Hello!" }'
```

## Event Contract (Kafka)

Topic `appointment-events`:

```json
{
  "eventId": "appointment.created:apt-001:1",
  "type": "appointment.created",
  "appointmentId": "apt-001",
  "version": 1,
  "occurredAt": "2026-05-12T10:00:00.000Z",
  "payload": { "appointment": { ... } }
}
```

| Event | Khi nào |
|---|---|
| `appointment.created` | Lịch hẹn được tạo |
| `appointment.updated` | Lịch hẹn được cập nhật |
| `appointment.cancelled` | Lịch hẹn bị hủy |
| `reminder.sent` | Email nhắc lịch đã gửi |

## Email Reminders

3 email cho mỗi appointment:

| Loại | Thời điểm | Nội dung |
|---|---|---|
| `before` | `REMINDER_BEFORE_MS` trước giờ hẹn | "Lịch hẹn của bạn sắp bắt đầu" |
| `atTime` | Đúng giờ hẹn | "Đã đến giờ hẹn của bạn" |
| `after` | `REMINDER_AFTER_MS` sau giờ hẹn | "Lịch hẹn của bạn đã qua giờ" |

**Cơ chế an toàn khi update/cancel:**
- Restate delay gọi `sendReminder` kèm version tại thời điểm tạo
- Khi đến giờ, `sendReminder` kiểm tra version hiện tại — nếu đã thay đổi → skip
- **Không cần cancel job** — version validation tự loại bỏ lời gọi cũ

## NPM Scripts

| Script | Mô tả |
|---|---|
| `npm run dev` | **Chạy tất cả** (Docker + API + workers + consumers) |
| `npm run dev:app` | Chỉ chạy API server (hot-reload) |
| `npm run build` | Build TypeScript → `dist/` |
| `npm run start` | Production mode |
| `npm run worker:email:dev` | Email worker riêng |
| `npm run worker:maintenance:dev` | Maintenance worker riêng |
| `npm run consumer:analytics:dev` | Analytics consumer riêng |
| `npm run consumer:telegram:dev` | Telegram consumer riêng |
| `npm run consumer:chat:dev` | Chat consumer riêng |
| `npm run restate:register` | Đăng ký endpoint với Restate |
| `npm run queue:clear` | Xóa tất cả BullMQ jobs |

## URLs khi chạy local

| Service | URL |
|---|---|
| API | http://localhost:9080 |
| Health Check | http://localhost:9080/health |
| BullMQ Dashboard | http://localhost:9080/admin/queues |
| SSE Events | http://localhost:9080/api/events/appointments |
| Chat UI | http://localhost:9080/chat |
| Restate Admin | http://localhost:19070 |
| Redpanda Console | http://localhost:8081 |

## Tạo Telegram Bot

1. Mở Telegram → tìm **@BotFather** → gửi `/newbot`
2. Đặt tên + username → nhận **Token**
3. Gửi tin nhắn bất kỳ cho bot
4. Mở trình duyệt: `https://api.telegram.org/bot<TOKEN>/getUpdates`
5. Tìm `"chat":{"id": 123456}` → đó là **Chat ID**
6. Điền vào `.env`

## Troubleshooting

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `service 'Appointment' not found` | Restate chưa đăng ký | `npm run restate:register` |
| `handler 'xxx' was not found` | Restate có phiên bản cũ | Force re-register (xem bên dưới) |
| `unable to reach the remote endpoint` | Restate Docker không gọi được localhost | Đổi `PUBLIC_RESTATE_ENDPOINT=http://host.docker.internal:9080/restate` |
| Email chỉ mock | `SENDGRID_API_KEY` trống | Điền API key vào `.env` |
| Telegram không nhận | Bot chưa cấu hình | Kiểm tra `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` |
| Job cũ lỗi trong BullMQ | Redis còn data cũ | `npm run queue:clear` |

Force re-register Restate:
```bash
curl http://localhost:19070/deployments \
  -H "content-type: application/json" \
  -d '{"uri": "http://host.docker.internal:9080/restate", "force": true}'
```

## Tech Stack

| Công nghệ | Vai trò |
|---|---|
| **Hono** | REST API framework + SSE + BullMQ dashboard |
| **Restate** | Durable execution — checkpoint, lock, delay scheduling |
| **MongoDB** | Lưu appointments, chat messages, event logs |
| **BullMQ + Redis** | Job queue gửi email — retry, rate limit, concurrency |
| **Redpanda (Kafka)** | Event streaming — fan-out tới nhiều consumers |
| **SendGrid** | Gửi email (mock khi không có API key) |
| **Telegram Bot** | Thông báo real-time qua Telegram |
| **Zod** | Schema validation toàn bộ dữ liệu |
| **Pino** | Structured logging (JSON) |

## Postman Collection

Import `postman/Redpanda-Appointment-API.postman_collection.json` vào Postman để test API.
