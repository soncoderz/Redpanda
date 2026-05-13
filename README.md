# Appointment Booking Backend

Hệ thống đặt lịch hẹn — khi khách đặt lịch, hệ thống tự động lưu vào database, gửi email nhắc lịch, thông báo Telegram, và chat real-time.

---

## Hệ thống này làm gì?

```
Khách đặt lịch cắt tóc lúc 10:00
    → Lưu vào MongoDB
    → Gửi thông báo Telegram cho chủ tiệm
    → Lúc 9:59 tự gửi email nhắc khách "Sắp đến giờ hẹn"
    → Lúc 10:00 gửi email "Đã đến giờ hẹn"
    → Lúc 10:01 gửi email "Lịch hẹn đã qua"

Khách đổi lịch sang 11:00
    → Hủy 3 email cũ (9:59, 10:00, 10:01)
    → Đặt lại 3 email mới (10:59, 11:00, 11:01)

Khách hủy lịch
    → Hủy tất cả email nhắc
    → Thông báo Telegram "Lịch hẹn đã bị hủy"
```

---

## Mỗi công nghệ làm gì?

Hãy tưởng tượng hệ thống như một **nhà hàng**:

```
┌─────────────────────────────────────────────────────────────────┐
│                        NHÀ HÀNG                                │
│                                                                 │
│  Hono        = Quầy lễ tân     (nhận order từ khách)           │
│  Restate     = Quản lý bếp     (điều phối ai làm gì, khi nào) │
│  MongoDB     = Sổ ghi chép     (lưu tất cả thông tin)         │
│  Redpanda    = Loa phát thanh  (thông báo cho mọi bộ phận)    │
│  BullMQ      = Hàng đợi giao   (xếp hàng gửi email lần lượt) │
│  Redis       = Bảng ghi nhớ    (nhớ hàng đợi đang có gì)      │
│  SendGrid    = Bưu điện        (gửi email thật)               │
│  Telegram    = Nhóm chat       (thông báo nhanh cho chủ tiệm) │
│  SSE         = Màn hình live   (khách xem update real-time)    │
└─────────────────────────────────────────────────────────────────┘
```

### Chi tiết từng công nghệ:

---

### 1. Hono — Nhận request từ client

```
Client gửi HTTP request → Hono nhận → chuyển cho bộ phận xử lý
```

Giống như **quầy lễ tân** — khách đến nói "tôi muốn đặt lịch", lễ tân ghi nhận và chuyển cho bếp xử lý.

**Các request Hono nhận:**

| Hành động | Request | Ví dụ |
|-----------|---------|-------|
| Đặt lịch | `POST /api/appointments` | Khách đặt cắt tóc 10:00 |
| Xem lịch | `GET /api/appointments` | Xem danh sách tất cả lịch hẹn |
| Sửa lịch | `PATCH /api/appointments/:id` | Đổi từ 10:00 sang 11:00 |
| Hủy lịch | `DELETE /api/appointments/:id` | Khách không đến nữa |
| Gửi chat | `POST /api/chat/send` | Nhắn tin trong phòng chat |
| Nhận chat live | `GET /api/chat/stream/:roomId` | Nhận tin nhắn real-time |

---

### 2. Restate — Điều phối xử lý (bộ não)

```
Khách A đặt lịch ──┐
                    ├── Restate xếp hàng, xử lý từng cái một
Khách B sửa lịch ──┘   (không bao giờ xử lý 2 request cùng 1 lịch hẹn)
```

Giống như **quản lý bếp** — nhận order, phân công ai làm gì, đảm bảo không bị trùng.

**3 việc quan trọng Restate làm:**

**a) Xử lý tuần tự (không xung đột)**
```
Khách A gọi sửa lịch 001  ──→  Restate xử lý xong
Khách B gọi hủy lịch 001  ──→  Restate mới xử lý tiếp
                                (không bao giờ chạy cùng lúc)
```

**b) Checkpoint (không mất dữ liệu nếu crash)**
```
Bước 1: Lưu MongoDB     ✅ xong (checkpoint)
Bước 2: Gửi Kafka       ❌ crash giữa chừng!
         → Restart
         → Restate biết bước 1 xong rồi, chỉ chạy lại bước 2
```

**c) Hẹn giờ gửi email (delayed call)**
```
Đặt lịch 10:00 → Restate hẹn:
   ├── 9:59  gọi sendReminder("before")
   ├── 10:00 gọi sendReminder("atTime")
   └── 10:01 gọi sendReminder("after")

Sửa lịch sang 11:00 → Restate:
   ├── Hủy 3 lịch hẹn cũ (9:59, 10:00, 10:01)
   └── Đặt 3 lịch hẹn mới (10:59, 11:00, 11:01)
```

---

### 3. MongoDB — Lưu dữ liệu

```
Database: Redpanda
├── appointments    ← Thông tin lịch hẹn (tên, email, giờ, trạng thái)
├── chat_messages   ← Tin nhắn chat
└── event_logs      ← Lịch sử sự kiện (ai đặt/sửa/hủy lúc nào)
```

Giống như **sổ ghi chép** — ghi lại mọi thứ, tắt máy bật lại vẫn còn.

---

### 4. Redpanda (Kafka) — Phát thông báo cho nhiều bên

```
API tạo lịch hẹn
    │
    └── Gửi 1 event vào Redpanda: "Lịch hẹn mới được tạo"
              │
              ├── Telegram Consumer nhận → gửi Telegram cho chủ tiệm
              ├── Kafka Connect nhận     → tự lưu vào MongoDB event_logs
              └── (Consumer mới)         → thêm bất kỳ lúc nào, không sửa code cũ
```

Giống như **loa phát thanh** — API chỉ nói 1 lần, ai muốn nghe thì tự đăng ký nghe.

**Tại sao không gọi trực tiếp?**
```
❌ Cách cũ:   API → gọi Telegram → gọi Analytics → gọi Webhook → ...
              (thêm 1 bên = sửa code API)

✅ Cách mới:  API → gửi event vào Redpanda
              Telegram tự nghe
              Analytics tự nghe
              Webhook tự nghe
              (thêm 1 bên = thêm 1 file consumer, không sửa API)
```

---

### 5. Schema Registry — Kiểm tra format message

```
Producer gửi message → Schema Registry kiểm tra: đúng format không?
                        ├── Đúng  → cho gửi
                        └── Sai   → báo lỗi

Consumer nhận message → Schema Registry giúp decode đúng cấu trúc
```

Giống như **bưu điện kiểm tra thư** — đảm bảo thư có đủ tên, địa chỉ, nội dung trước khi gửi.

---

### 6. Kafka Connect — Tự động lưu event vào MongoDB

```
Redpanda topic: appointment-events
    │
    └── Kafka Connect tự động đọc → ghi vào MongoDB collection event_logs
        (không cần viết code, chỉ cấu hình file JSON)
```

Giống như **máy photocopy tự động** — cứ có event mới là tự sao chép vào sổ lưu trữ.

Thay thế cho `analytics.consumer.ts` (file cũ đã tắt) — làm cùng 1 việc nhưng không cần code.

---

### 7. BullMQ + Redis — Xếp hàng gửi email

```
Restate: "Gửi email cho khách A" ──→ BullMQ xếp vào hàng đợi
Restate: "Gửi email cho khách B" ──→ BullMQ xếp vào hàng đợi
Restate: "Gửi email cho khách C" ──→ BullMQ xếp vào hàng đợi
                                          │
                                     Email Worker lấy ra gửi từng cái
                                     (tối đa 50 email/giây, gửi lỗi thì thử lại)
```

Giống như **hàng đợi giao hàng** — không gửi ồ ạt cùng lúc (sẽ bị SendGrid chặn), mà xếp hàng gửi lần lượt.

**Redis** nhớ hàng đợi đang có gì — tắt máy bật lại không mất job.

---

### 8. SendGrid — Gửi email thật

```
Có SENDGRID_API_KEY  → Gửi email thật qua SendGrid
Không có API key     → Chỉ log ra console (dev mode)
```

---

### 9. Telegram Bot — Thông báo cho chủ tiệm

```
Có lịch hẹn mới    → 📋 "Lịch hẹn mới: Nguyen Van A - Cắt tóc - 10:00"
Khách sửa lịch     → ✏️ "Lịch hẹn đã cập nhật: đổi sang 11:00"
Khách hủy          → ❌ "Lịch hẹn đã bị hủy"
Gửi email xong     → 📧 "Đã gửi email nhắc lịch"
```

---

### 10. SSE — Real-time cho browser

```
Browser mở kết nối SSE → Server push event xuống ngay khi có thay đổi
                         (không cần refresh, không cần polling)
```

Dùng cho:
- Chat real-time giữa các user
- Hiển thị lịch hẹn mới/sửa/hủy ngay trên giao diện

---

## Toàn bộ luồng đặt lịch (từ đầu đến cuối)

```
Bước 1: Khách gửi request
         POST /api/appointments {tên: "A", email: "a@mail.com", giờ: "10:00"}
              │
Bước 2: Hono nhận → validate dữ liệu → gửi cho Restate
              │
Bước 3: Restate xử lý (tuần tự, an toàn):
         ├── Lưu vào MongoDB (collection: appointments)
         ├── Gửi event vào Redpanda: "appointment.created"
         ├── Hẹn giờ: 9:59 gửi email "before"
         ├── Hẹn giờ: 10:00 gửi email "atTime"
         ├── Hẹn giờ: 10:01 gửi email "after"
         └── Trả kết quả cho khách (201 Created)
              │
Bước 4: Song song từ Redpanda:
         ├── Telegram Consumer → gửi thông báo Telegram
         ├── Kafka Connect     → lưu event vào MongoDB event_logs
         └── SSE               → push real-time cho browser
              │
Bước 5: Đến 9:59 (Restate tự gọi sendReminder):
         ├── Kiểm tra: lịch hẹn còn hiệu lực không?
         │   ├── Đã hủy/sửa → bỏ qua, không gửi
         │   └── Còn hiệu lực → đẩy job vào BullMQ
         │
         └── Email Worker nhận job:
              ├── Validate lần cuối qua Restate
              ├── Gửi email qua SendGrid
              └── Ghi kết quả vào MongoDB
```

---

## Cấu trúc thư mục (đơn giản)

```
📁 config/          Cấu hình kết nối (MongoDB, Redis, Kafka, Restate)
📁 controllers/     Nhận request từ client, validate, gọi Restate
📁 models/          Định nghĩa dữ liệu (Appointment, Chat, EventLog)
📁 routes/          Đăng ký URL endpoints
📁 services/
   📁 restate/      Bộ não xử lý: tạo/sửa/hủy lịch, gửi reminder
   📁 messaging/    Kafka producer/consumer, Telegram, SSE
   📁 queue/        BullMQ: xếp hàng gửi email
   📁 email/        Gửi email qua SendGrid
   📁 database/     Kết nối MongoDB
📁 workers/         Các process chạy riêng (email, telegram, chat)
📁 scripts/         Tools: khởi động dev, đăng ký services
📁 connectors/      Kafka Connect: config tự lưu event vào MongoDB
📁 schemas/         JSON Schema cho message validation
📁 validation/      Validate request body từ client
📁 middlewares/     Xử lý lỗi chung
📁 utils/           Logger, helper functions
📁 public/          Chat UI demo
```

---

## Chạy dự án

```bash
# Cài dependencies
npm install

# Chạy tất cả (1 lệnh duy nhất)
npm run dev
```

Lệnh `npm run dev` tự động:
1. Bật Docker containers (MongoDB, Redis, Redpanda, Restate, Kafka Connect)
2. Chờ tất cả sẵn sàng
3. Chạy API + tất cả workers
4. Đăng ký Restate, Schema Registry, Kafka Connect
5. `Ctrl+C` tắt tất cả

---

## Cấu hình (.env)

```bash
cp .env.example .env
```

| Biến | Giá trị | Ý nghĩa |
|------|---------|---------|
| `PORT` | `9080` | API chạy trên port nào |
| `MONGODB_URI` | `mongodb://localhost:27017` | Địa chỉ MongoDB |
| `MONGODB_DB_NAME` | `Redpanda` | Tên database |
| `REDIS_URL` | `redis://localhost:6379` | Địa chỉ Redis |
| `KAFKA_BROKERS` | `localhost:19092` | Địa chỉ Redpanda |
| `SENDGRID_API_KEY` | *(để trống)* | Trống = không gửi email thật |
| `TELEGRAM_BOT_TOKEN` | *(để trống)* | Token bot Telegram |
| `TELEGRAM_CHAT_ID` | *(để trống)* | Chat ID nhận thông báo |
| `KAFKA_USERNAME` | *(để trống)* | Trống = không dùng ACL |
| `KAFKA_PASSWORD` | *(để trống)* | Trống = không dùng ACL |

---

## ACL (bảo mật Kafka) — Tùy chọn

Mặc định **không bật**. Khi muốn bật:

```powershell
# 1. Tạo users + phân quyền
(Get-Content scripts/setup-acl.sh -Raw) -replace "`r`n", "`n" | docker exec -i appointment-backend-redpanda-0-1 bash

# 2. Điền vào .env
KAFKA_USERNAME=api-service
KAFKA_PASSWORD=api-service-secret

# 3. Chạy lại
npm run dev
```

**Phân quyền:**
```
admin           → toàn quyền (dùng cho Redpanda Console)
api-service     → đọc + ghi topics (API server + consumers dùng)
kafka-connect   → đọc events, quản lý internal topics
```

---

## URLs khi chạy local

| Trang | URL | Để làm gì |
|-------|-----|-----------|
| API | http://localhost:9080 | Gọi API |
| Health Check | http://localhost:9080/health | Kiểm tra hệ thống |
| BullMQ Dashboard | http://localhost:9080/admin/queues | Xem hàng đợi email |
| Chat UI | http://localhost:9080/chat | Demo chat real-time |
| Redpanda Console | http://localhost:8081 | Xem topics, consumer groups, connectors |
| Restate Admin | http://localhost:19070 | Quản lý Restate |

---

## Test nhanh

```bash
# Đặt lịch
curl -X POST http://localhost:9080/api/appointments \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Nguyen Van A","customerEmail":"a@mail.com","service":"Cat toc","startAt":"2026-05-14T10:00:00.000Z"}'

# Xem danh sách
curl http://localhost:9080/api/appointments

# Gửi chat
curl -X POST http://localhost:9080/api/chat/send \
  -H "Content-Type: application/json" \
  -d '{"roomId":"room-1","from":"user1","text":"Xin chao!"}'
```

---

## Lỗi thường gặp

| Lỗi | Tại sao | Cách sửa |
|-----|---------|----------|
| `ILLEGAL_SASL_STATE` | Có username/password trong .env nhưng Redpanda chưa bật ACL | Xóa `KAFKA_USERNAME` và `KAFKA_PASSWORD` trong .env |
| `service 'Appointment' not found` | Restate chưa đăng ký | Chạy `npm run restate:register` |
| API không start (port 9080) | MongoDB hoặc Kafka chưa sẵn sàng | Chờ Docker containers khởi động xong |
| Email chỉ log, không gửi thật | Chưa có SendGrid API key | Điền `SENDGRID_API_KEY` vào .env |
| Telegram không thông báo | Chưa cấu hình bot | Điền `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` |
| Connector task 0/1 | Thiếu quyền ACL | Chạy lại `setup-acl.sh` |

---

## Tóm tắt: Ai làm gì?

```
Client đặt lịch
    │
    ▼
  Hono          nhận request, validate dữ liệu
    │
    ▼
  Restate       xử lý logic, lưu DB, hẹn giờ email, gửi event
    │
    ├──▶ MongoDB        lưu lịch hẹn + chat + event logs
    │
    ├──▶ Redpanda       phát event cho nhiều bên nghe
    │       ├── Telegram Consumer  → thông báo Telegram
    │       ├── Kafka Connect      → tự lưu event vào MongoDB
    │       └── Chat Consumer      → lưu chat vào MongoDB
    │
    ├──▶ BullMQ + Redis    xếp hàng gửi email
    │       └── Email Worker → SendGrid gửi email thật
    │
    └──▶ SSE              push real-time cho browser
```
