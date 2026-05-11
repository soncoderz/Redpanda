# Appointment Booking Backend

Backend hệ thống đặt lịch hẹn xây dựng với Node.js/TypeScript, tích hợp nhiều công nghệ: Restate, BullMQ, Redis, Redpanda (Kafka), MongoDB, Hono, SendGrid, Zod.

## Tech Stack

| Công nghệ | Vai trò |
|---|---|
| **Restate** | Orchestration lifecycle lịch hẹn, đảm bảo durable execution, serialized per-appointment, retry-safe side effects |
| **MongoDB + Mongoose** | Lưu trữ dữ liệu lịch hẹn, reminder metadata, history, idempotency log |
| **BullMQ + Redis** | Job queue gửi email nhắc lịch, retry với exponential backoff, rate limiting |
| **Redpanda + KafkaJS** | Event streaming cho các sự kiện appointment (created, updated, cancelled, reminder.sent) |
| **Hono** | REST API framework, health endpoint, queue dashboard, Server-Sent Events |
| **SendGrid** | Gửi email nhắc lịch thật (fallback mock khi không có API key) |
| **Zod** | Validate dữ liệu đầu vào cho API, workflow, job, event |
| **Pino** | Structured logging (JSON format) |

## Cấu trúc thư mục

```
├── config/              Cấu hình environment (env.ts)
├── controllers/         HTTP controllers (appointment, realtime, queue dashboard)
├── middlewares/          Error handling middleware
├── models/              Zod schemas + Mongoose schemas
├── repositories/        MongoDB persistence, idempotency storage
├── routes/              REST API routes + SSE routes
├── scripts/             Workers, consumers, scripts vận hành
│   ├── email.worker.ts          BullMQ worker gửi email
│   ├── maintenance.worker.ts    Worker dọn dẹp queue
│   ├── analytics.consumer.ts    Kafka consumer analytics
│   ├── register-restate.ts      Đăng ký endpoint với Restate
│   └── clear-queue.ts           Xóa toàn bộ jobs trong queue
├── services/            Business logic
│   ├── appointment.service.ts       Restate virtual object (appointment lifecycle)
│   ├── mailer.service.ts            Gửi email qua SendGrid
│   ├── email-queue.service.ts       BullMQ queue definition
│   ├── kafka.service.ts             Kafka producer
│   ├── mongodb.service.ts           MongoDB connection
│   ├── redis.service.ts             Redis connection
│   ├── restate-client.service.ts    Restate ingress client
│   ├── restate-endpoint.service.ts  Restate HTTP endpoint
│   └── realtime.service.ts          SSE event emitter
├── utils/               Helpers (logging, appointment utils, event builders)
├── validation/          Request validation schemas
├── postman/             Postman collection (import để test API)
├── docker-compose.yml   Docker Compose cho tất cả services
├── Dockerfile           Multi-stage build (node:22-alpine)
└── .env.example         Template cấu hình environment
```

## Luồng hoạt động

### 1. Tạo lịch hẹn

```
Client          API (Hono)         Restate            MongoDB         Redpanda          BullMQ
  │                │                  │                  │                │                │
  │ POST /appointments                │                  │                │                │
  │───────────────>│                  │                  │                │                │
  │                │  create()        │                  │                │                │
  │                │─────────────────>│                  │                │                │
  │                │                  │                  │                │                │
  │                │                  │  save appointment │                │                │
  │                │                  │─────────────────>│                │                │
  │                │                  │                  │                │                │
  │                │                  │  publish "appointment.created"    │                │
  │                │                  │─────────────────────────────────>│                │
  │                │                  │                  │                │                │
  │                │                  │  schedule 3 delayed jobs (before/atTime/after)     │
  │                │                  │──────────────────────────────────────────────────>│
  │                │                  │                  │                │                │
  │                │<─────────────────│                  │                │                │
  │ 201 Created    │                  │                  │                │                │
  │<───────────────│                  │                  │                │                │
```

### 2. Gửi email nhắc lịch (khi đến giờ)

```
BullMQ Worker       Restate            MongoDB          SendGrid         Redpanda
     │                 │                  │                 │                │
     │  (delayed job fires — 1 phút trước giờ hẹn)         │                │
     │                 │                  │                 │                │
     │ startReminderDelivery()            │                 │                │
     │────────────────>│                  │                 │                │
     │                 │  load appointment│                 │                │
     │                 │─────────────────>│                 │                │
     │                 │  check: version, │                 │                │
     │                 │  status, sent    │                 │                │
     │  shouldSend:true│                  │                 │                │
     │<────────────────│                  │                 │                │
     │                 │                  │                 │                │
     │  sgMail.send() (gửi email nhắc lịch)                │                │
     │─────────────────────────────────────────────────────>│                │
     │  200 OK         │                  │                 │                │
     │<─────────────────────────────────────────────────────│                │
     │                 │                  │                 │                │
     │ recordReminderResult(status:"sent")│                 │                │
     │────────────────>│                  │                 │                │
     │                 │  update reminder │                 │                │
     │                 │─────────────────>│                 │                │
     │                 │                  │                 │                │
     │                 │  publish "reminder.sent"                            │
     │                 │───────────────────────────────────────────────────>│
     │                 │                  │                 │                │
```

### 3. Analytics Consumer (chạy song song)

```
Redpanda              Analytics Consumer           MongoDB
   │                        │                        │
   │  appointment.created   │                        │
   │───────────────────────>│                        │
   │                        │  save event log        │
   │                        │───────────────────────>│
   │                        │                        │
   │  appointment.updated   │                        │
   │───────────────────────>│                        │
   │                        │  save event log        │
   │                        │───────────────────────>│
   │                        │                        │
   │  reminder.sent         │                        │
   │───────────────────────>│                        │
   │                        │  save event log        │
   │                        │───────────────────────>│
   │                        │                        │
```

### 4. Real-time Events (SSE cho browser)

```
Browser                    API (Hono)
   │                          │
   │ GET /api/events/appointments
   │─────────────────────────>│
   │  SSE stream opened       │
   │<─────────────────────────│
   │                          │
   │  event: appointment.created
   │<─────────────────────────│
   │                          │
   │  event: appointment.updated
   │<─────────────────────────│
   │                          │
   │  event: reminder.sent    │
   │<─────────────────────────│
   │                          │
```

**Chi tiết từng bước:**

1. Client gọi `POST /api/appointments` với thông tin lịch hẹn (có thể kèm `Idempotency-Key`)
2. API gọi Restate virtual object `Appointment` (keyed by appointment ID)
3. Restate lưu appointment vào **MongoDB** trong `ctx.run` (idempotent)
4. Restate publish event `appointment.created` tới **Redpanda** qua KafkaJS
5. Restate schedule 3 delayed jobs trong **BullMQ** (qua Redis):
   - `before` — 1 phút trước giờ hẹn
   - `atTime` — đúng giờ hẹn
   - `after` — 1 phút sau giờ hẹn
6. Khi đến giờ, BullMQ worker xử lý job:
   - Gọi `startReminderDelivery()` trên Restate để kiểm tra (version đúng? đã cancelled? đã gửi chưa?)
   - Nếu `shouldSend: true` → gửi email qua **SendGrid**
   - Gọi `recordReminderResult()` để ghi kết quả vào **MongoDB** và publish `reminder.sent` tới **Redpanda**
7. **Analytics consumer** đọc events từ **Redpanda** và lưu event log vào **MongoDB**
8. **SSE endpoint** push real-time events tới browser khi có thay đổi

## Yêu cầu

- **Node.js** >= 20
- **Docker Desktop** (cho MongoDB, Redis, Redpanda, Restate)

## Chạy với Docker (tất cả services)

```bash
docker compose up --build
```

Sau khi tất cả containers chạy, đăng ký Restate endpoint:

```bash
docker compose run --rm restate-register
```

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
# Bắt buộc cho gửi email thật
SENDGRID_API_KEY=SG.xxxxx
SENDGRID_FROM_EMAIL=your-email@example.com
SENDGRID_FROM_NAME=Appointment Reminder

# Quan trọng: Restate chạy trong Docker, cần dùng host.docker.internal
PUBLIC_RESTATE_ENDPOINT=http://host.docker.internal:9080/restate
```

> **Lưu ý:** Vì Restate chạy trong Docker container, `PUBLIC_RESTATE_ENDPOINT` phải dùng `host.docker.internal` thay vì `localhost` để container có thể gọi tới app trên máy host.

### Bước 3: Khởi động infrastructure (Docker)

```bash
docker compose up -d mongo redis redpanda-0 redpanda-console restate
```

### Bước 4: Chạy app và các workers

Mở 4 terminal riêng biệt:

```bash
# Terminal 1: API server
npm run dev

# Terminal 2: Đăng ký Restate endpoint (chạy 1 lần sau khi API sẵn sàng)
npm run restate:register

# Terminal 3: Email worker (xử lý jobs gửi email)
npm run worker:email:dev

# Terminal 4: Analytics consumer (xử lý Kafka events)
npm run consumer:analytics:dev
```

Optional:

```bash
# Terminal 5: Maintenance worker (dọn dẹp queue định kỳ)
npm run worker:maintenance:dev
```

## NPM Scripts

| Script | Mô tả |
|---|---|
| `npm run dev` | Chạy API server (hot-reload với tsx watch) |
| `npm run build` | Build TypeScript ra `dist/` |
| `npm run start` | Chạy production build |
| `npm run typecheck` | Kiểm tra type errors |
| `npm run worker:email:dev` | Email worker (dev, hot-reload) |
| `npm run worker:email` | Email worker (production) |
| `npm run worker:maintenance:dev` | Maintenance worker (dev, hot-reload) |
| `npm run worker:maintenance` | Maintenance worker (production) |
| `npm run consumer:analytics:dev` | Kafka analytics consumer (dev, hot-reload) |
| `npm run consumer:analytics` | Kafka analytics consumer (production) |
| `npm run restate:register` | Đăng ký endpoint với Restate runtime |
| `npm run queue:clear` | Xóa tất cả jobs trong BullMQ queue |

## API Endpoints

### REST API

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/` | Thông tin API |
| `GET` | `/health` | Health check (MongoDB, Kafka, Restate) |
| `POST` | `/api/appointments` | Tạo lịch hẹn mới |
| `GET` | `/api/appointments` | Danh sách lịch hẹn (từ MongoDB) |
| `GET` | `/api/appointments/:id` | Chi tiết lịch hẹn (từ Restate state) |
| `PATCH` | `/api/appointments/:id` | Cập nhật lịch hẹn (partial) |
| `PUT` | `/api/appointments/:id` | Cập nhật lịch hẹn (full) |
| `DELETE` | `/api/appointments/:id` | Hủy lịch hẹn |

### Server-Sent Events

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/api/events/appointments` | Stream real-time appointment events |

### Admin

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/admin/queues` | Bull Board dashboard (mở trên browser) |

## API Examples

### Tạo lịch hẹn

```bash
curl -X POST http://localhost:9080/api/appointments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-booking-001" \
  -d '{
    "customerName": "Nguyen Van A",
    "customerEmail": "nguyenvana@example.com",
    "service": "Cat toc",
    "startAt": "2026-05-12T10:30:00.000Z",
    "note": "Lan dau"
  }'
```

**Request body:**

| Field | Type | Required | Mô tả |
|---|---|---|---|
| `customerName` | string | Yes | Tên khách hàng |
| `customerEmail` | string (email) | Yes | Email khách hàng (nhận reminder) |
| `service` | string | Yes | Tên dịch vụ |
| `startAt` | string (ISO 8601) | Yes | Thời gian hẹn |
| `note` | string (max 2000) | No | Ghi chú |
| `id` | string | No | Custom appointment ID |
| `idempotencyKey` | string | No | Key chống trùng (có thể gửi qua header `Idempotency-Key`) |

### Danh sách lịch hẹn

```bash
# Tất cả
curl http://localhost:9080/api/appointments

# Lọc theo status
curl "http://localhost:9080/api/appointments?status=booked&limit=20"

# Lọc theo email
curl "http://localhost:9080/api/appointments?customerEmail=nguyenvana@example.com"
```

### Chi tiết lịch hẹn

```bash
curl http://localhost:9080/api/appointments/{id}
```

### Cập nhật lịch hẹn (partial)

```bash
curl -X PATCH http://localhost:9080/api/appointments/{id} \
  -H "Content-Type: application/json" \
  -d '{ "startAt": "2026-05-12T11:00:00.000Z" }'
```

> Cập nhật sẽ tăng version và reschedule lại tất cả reminder emails.

### Hủy lịch hẹn

```bash
curl -X DELETE http://localhost:9080/api/appointments/{id}
```

> Hủy sẽ cancel tất cả pending reminder emails.

### Lắng nghe real-time events (SSE)

```bash
curl -N http://localhost:9080/api/events/appointments
```

## Postman Collection

Import file `postman/Redpanda-Appointment-API.postman_collection.json` vào Postman để test nhanh tất cả endpoints.

**Cách import:**
1. Mở Postman → **Import** (Ctrl+O)
2. Chọn file `postman/Redpanda-Appointment-API.postman_collection.json`
3. Collection sẽ tự động lưu `appointmentId` khi tạo mới để dùng cho các request khác

## Cấu hình Environment

Xem `.env.example` để biết tất cả biến cấu hình. Các biến quan trọng:

| Biến | Mặc định | Mô tả |
|---|---|---|
| `PORT` | `9080` | Port API server |
| `PUBLIC_RESTATE_ENDPOINT` | `http://localhost:9080/restate` | URL để Restate gọi tới app (**dùng `host.docker.internal` khi chạy local**) |
| `RESTATE_RUNTIME_URL` | `http://localhost:18080` | Restate ingress URL |
| `RESTATE_ADMIN_URL` | `http://localhost:19070` | Restate admin API |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `MONGODB_URI` | `mongodb://localhost:27017/appointments` | MongoDB connection |
| `KAFKA_BROKERS` | `localhost:19092` | Kafka/Redpanda brokers |
| `SENDGRID_API_KEY` | *(empty)* | SendGrid API key (bỏ trống = mock email) |
| `SENDGRID_FROM_EMAIL` | *(empty)* | Email người gửi |
| `SENDGRID_FROM_NAME` | `Appointment Reminder` | Tên người gửi |
| `EMAIL_WORKER_CONCURRENCY` | `25` | Số jobs xử lý đồng thời |
| `EMAIL_RATE_MAX` | `50` | Rate limit: max jobs |
| `EMAIL_RATE_DURATION_MS` | `1000` | Rate limit: khoảng thời gian (ms) |
| `EMAIL_JOB_ATTEMPTS` | `5` | Số lần retry khi gửi email thất bại |
| `REMINDER_BEFORE_MS` | `60000` | Thời gian nhắc trước giờ hẹn (ms) |
| `REMINDER_AFTER_MS` | `60000` | Thời gian nhắc sau giờ hẹn (ms) |

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
| MongoDB | mongodb://localhost:27017 |
| Redis | redis://localhost:6379 |

## Event Contract (Kafka/Redpanda)

Tất cả events trên Redpanda dùng envelope format:

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

**Event types:**

| Event | Khi nào |
|---|---|
| `appointment.created` | Lịch hẹn được tạo |
| `appointment.updated` | Lịch hẹn được cập nhật |
| `appointment.cancelled` | Lịch hẹn bị hủy |
| `reminder.sent` | Email nhắc lịch đã gửi thành công |

## Email Reminders

Hệ thống gửi 3 email nhắc lịch cho mỗi appointment:

| Loại | Thời điểm | Nội dung |
|---|---|---|
| `before` | 1 phút trước giờ hẹn | "Lịch hẹn của bạn sắp bắt đầu" |
| `atTime` | Đúng giờ hẹn | "Đã đến giờ hẹn của bạn" |
| `after` | 1 phút sau giờ hẹn | "Lịch hẹn của bạn đã qua giờ" |

**Cơ chế an toàn:**
- Job ID deterministic (appointment ID + version + reminder type) → không gửi trùng
- Worker kiểm tra với Restate trước khi gửi (version match, chưa cancelled, chưa gửi)
- Retry với exponential backoff khi SendGrid lỗi tạm thời
- Lỗi 401/403 (auth) → skip, không retry
- Khi update appointment → cancel jobs cũ, schedule jobs mới
- Khi cancel appointment → cancel tất cả pending jobs

## Retry-Safe Patterns

- Restate wrap MongoDB writes, Kafka publishing, BullMQ scheduling trong `ctx.run` → side effects được journal, không lặp lại khi replay
- Appointment commands được serialize theo Restate virtual object key
- Create request hỗ trợ `Idempotency-Key` → API derive stable appointment ID từ key
- BullMQ reminder jobs dùng deterministic ID: `appointment-email-{base64(appointmentId)}-{version}-{reminder}`
- Kafka events dùng deterministic `eventId` → consumers có thể deduplicate
- Analytics consumer lưu processed event IDs vào MongoDB với unique index

## Docker Services

| Service | Image | Ports |
|---|---|---|
| `mongo` | mongo:8.0 | 27017 |
| `redis` | redis:7.4-alpine | 6379 |
| `redpanda-0` | redpanda:v26.1.6 | 19092, 18081, 18082 |
| `redpanda-console` | console:v3.7.2 | 8081 |
| `restate` | restate:latest | 18080, 19070 |
| `api` | (Dockerfile) | 9080 |
| `email-worker` | (Dockerfile) | - |
| `maintenance-worker` | (Dockerfile) | - |
| `analytics-consumer` | (Dockerfile) | - |
| `restate-register` | (Dockerfile) | - (one-shot) |

## Troubleshooting

### Lỗi "service 'Appointment' not found"

Restate chưa được đăng ký endpoint. Chạy:

```bash
npm run restate:register
```

### Lỗi "handler 'xxx' was not found"

Restate có phiên bản cũ của service. Force re-register:

```bash
curl http://localhost:19070/deployments \
  -H "content-type: application/json" \
  -d '{"uri": "http://host.docker.internal:9080/restate", "force": true}'
```

### Lỗi "unable to reach the remote endpoint" khi register

Restate trong Docker không thể gọi `localhost`. Đổi `PUBLIC_RESTATE_ENDPOINT` trong `.env`:

```env
PUBLIC_RESTATE_ENDPOINT=http://host.docker.internal:9080/restate
```

Restart app rồi register lại.

### Email không gửi (chỉ mock)

Kiểm tra `SENDGRID_API_KEY` trong `.env`. Nếu để trống, mailer sẽ mock (chỉ log, không gửi thật).

### Mongoose lỗi "models" export

Project dùng Mongoose v9 (ESM). Nếu gặp lỗi import, dùng `mongoose.models` và `mongoose.model()` từ default import thay vì named import.
