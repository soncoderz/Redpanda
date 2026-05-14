# Giải Thích Toàn Bộ Dự Án

Tài liệu này mô tả vai trò của từng file chính trong dự án và các hàm/export quan trọng. Các thư mục sinh ra hoặc không nên đọc trực tiếp như `node_modules/`, `dist/`, `.git/`, `logs/` không được liệt kê chi tiết.

## Tổng Quan Kiến Trúc

Dự án là backend đặt lịch hẹn dùng:

- `Hono`: HTTP API.
- `Restate`: workflow bền vững cho create/update/cancel appointment và delayed reminder.
- `BullMQ + Redis`: queue gửi email và queue bảo trì.
- `Redpanda/Kafka`: event streaming cho appointment events và chat messages.
- `Schema Registry`: đăng ký/encode/decode JSON schema cho Kafka message.
- `Kafka Connect MongoDB Sink`: tự động ghi `appointment-events` vào MongoDB `event_logs`.
- `MongoDB`: lưu appointments, chat messages, event logs.
- `SendGrid`: gửi email thật, hoặc mock nếu thiếu API key.
- `Telegram Bot`: gửi thông báo từ Kafka event.
- `SSE`: đẩy realtime event/chat tới browser.

Luồng chính:

```txt
Client -> Hono API -> Restate Appointment object -> MongoDB
                                  |
                                  +-> Redpanda topic appointment-events
                                  |       +-> Telegram consumer
                                  |       +-> Kafka Connect -> MongoDB event_logs
                                  |
                                  +-> Restate delayed sendReminder
                                          -> BullMQ appointment-email
                                          -> email.worker
                                          -> SendGrid/mock
```

Chat:

```txt
POST /api/chat/send -> Redpanda topic chat-messages -> chat.consumer -> MongoDB chat_messages
                                    |
                                    +-> local EventEmitter -> SSE chat clients
```

## Root Files

### `.env`

File cấu hình môi trường thật khi chạy local. Chứa URL MongoDB, Redis, Restate, Kafka, Schema Registry, Kafka Connect, Telegram, SendGrid. Không nên copy secret từ file này vào tài liệu hoặc commit công khai.

### `.env.example`

Mẫu biến môi trường để người khác biết cần cấu hình gì. Không nên chứa token thật.

### `.dockerignore`

Khai báo file/thư mục không copy vào Docker build context, ví dụ `node_modules`, `dist`, log, file local.

### `.gitignore`

Khai báo file/thư mục Git không track, ví dụ dependency, build output, env local.

### `package.json`

Khai báo tên project, dependencies và npm scripts.

Scripts chính:

- `dev`: chạy `scripts/dev.ts`, start infra + API + workers/consumers.
- `dev:app`: chạy API server bằng `tsx watch server.ts`.
- `build`: compile TypeScript ra `dist`.
- `start`: chạy production entry `dist/server.js`.
- `typecheck`: kiểm tra TypeScript không emit file.
- `worker:email:dev` / `worker:email`: chạy email worker.
- `worker:maintenance:dev` / `worker:maintenance`: chạy maintenance worker.
- `consumer:telegram:dev` / `consumer:telegram`: chạy Telegram Kafka consumer.
- `consumer:chat:dev` / `consumer:chat`: chạy chat Kafka consumer.
- `restate:register`: đăng ký endpoint `/restate` với Restate Admin.
- `schema:register`: đăng ký JSON schemas lên Schema Registry.
- `connect:register`: đăng ký Kafka Connect connectors.
- `queue:clear`: xóa BullMQ jobs.

### `package-lock.json`

Lockfile của npm. Ghi chính xác phiên bản package đã cài để build/install tái lập được.

### `tsconfig.json`

Cấu hình TypeScript:

- `module`/`moduleResolution`: `NodeNext` cho ESM import `.js`.
- `strict`: bật strict type checking.
- `outDir`: build ra `dist`.
- `include`: source folders được compile.

### `Dockerfile`

Dockerfile multi-stage cho app Node.js:

- Stage `build`: cài dependency, copy source, chạy `npm run build`.
- Stage `runtime`: cài production dependencies và copy `dist`.
- Chạy mặc định `node dist/server.js`.

### `docker-compose.yml`

Khai báo toàn bộ hạ tầng local/container:

- `mongo`: MongoDB.
- `redis`: Redis cho BullMQ.
- `redpanda-0`: Kafka/Redpanda broker, bật SASL.
- `redpanda-console`: UI quản lý Redpanda, Schema Registry, Kafka Connect.
- `kafka-connect`: Kafka Connect worker, có MongoDB Sink connector plugin.
- `restate`: Restate runtime/admin.
- `api`, `email-worker`, `maintenance-worker`: service production container của app.

Kafka Connect trong file này có SASL cho consumer/producer để đọc topic và ghi DLQ.

### `README.md`

Tài liệu tổng quan dự án, flow, cách chạy, endpoint, event contract.

### `AGENTS.md`

Ghi chú/hướng dẫn dành cho coding agents khi làm việc trong repo.

### `CLAUDE.md`

Ghi chú/hướng dẫn tương tự dành cho Claude/Codex style workflow.

### `app.ts`

Tạo Hono app và mount toàn bộ route/middleware.

Hàm chính:

- `createApp()`: tạo app Hono, cấu hình BullMQ dashboard, logger, route root, health check, API routes, chat HTML, SSE appointment events, error middleware, và Restate endpoint.
- `stripRestatePrefix(request)`: xóa prefix `/restate` trước khi chuyển request cho Restate SDK.

Endpoint chính:

- `GET /`: thông tin service và endpoint.
- `GET /health`: health check Mongo/Kafka/Restate.
- `/api/appointments`: appointment API.
- `/api/chat`: chat API.
- `/chat`, `/chat/:roomId`: HTML chat demo.
- `/api/events/appointments`: SSE appointment events.
- `/restate`: endpoint callback Restate runtime.

### `server.ts`

Entry point chạy API server.

Hàm/chức năng:

- Kết nối MongoDB bằng `connectMongo`.
- Tạo Kafka topics bằng `ensureKafkaTopics`.
- Kết nối Kafka producer bằng `connectKafkaProducer`.
- Gọi `createApp()` và chạy HTTP server qua `serve`.
- `shutdown(signal)`: đóng server, Kafka producer, MongoDB, BullMQ queues.
- `startupRetry(name, action)`: retry dependency tối đa 30 lần, mỗi lần cách 2 giây.

## Config

### `config/env.ts`

Đọc và chuẩn hóa toàn bộ biến môi trường.

Export chính:

- `env`: object cấu hình dùng chung toàn app.

Helper:

- `readPositiveIntEnv(name, fallback)`: đọc số nguyên dương.
- `readNonNegativeIntEnv(name, fallback)`: đọc số nguyên >= 0.
- `readCsvEnv(name, fallback)`: đọc chuỗi CSV thành mảng string.
- `readBooleanEnv(name, fallback)`: đọc boolean từ `1/true/yes/y`.
- `emptyToUndefined(value)`: đổi string rỗng thành `undefined`.

### `config/kafka.ts`

Cấu hình KafkaJS client kết nối Redpanda.

Export/hàm chính:

- `kafka`: Kafka client dùng cho admin/producer/consumer.
- `connectKafkaProducer()`: tạo producer idempotent, connect và cache trạng thái.
- `getKafkaProducer()`: trả producer hiện tại hoặc tự connect nếu chưa có.
- `disconnectKafkaProducer()`: đóng producer khi shutdown.
- `kafkaProducerReady()`: trả trạng thái producer đã connect hay chưa.

### `config/schema-registry.ts`

Cấu hình Schema Registry client.

Export/hàm chính:

- `schemaRegistry`: client kết nối Redpanda Schema Registry.
- `getOrRegisterSchema(subject, schema)`: lấy schema ID từ cache hoặc register mới.
- `encodeWithSchema(schemaId, payload)`: encode payload theo Confluent wire format.
- `decodeWithSchema(buffer)`: decode message từ Confluent wire format.

### `config/restate.ts`

Cấu hình Restate client và HTTP endpoint handler.

Export chính:

- `restateClient`: client gọi Restate runtime từ controllers/workers.
- `restateEndpoint`: endpoint handler để Restate runtime gọi vào app.

### `config/redis.ts`

Tạo Redis connection cho BullMQ.

Hàm chính:

- `createRedisConnection()`: tạo `ioredis` connection với `maxRetriesPerRequest: null`, bắt buộc cho BullMQ.

### `config/queues.ts`

Tạo BullMQ queues dùng chung.

Export chính:

- `EMAIL_QUEUE_NAME`: tên queue email.
- `appointmentEmailQueue`: queue gửi email reminder.
- `MAINTENANCE_QUEUE_NAME`: tên queue bảo trì.
- `maintenanceQueue`: queue job bảo trì.

## Models Và Repositories

### `models/appointment.model.ts`

Định nghĩa Zod schemas và TypeScript types cho appointment domain.

Schema/export chính:

- `ReminderType`: `"before" | "atTime" | "after"`.
- `AppointmentInput`: dữ liệu tạo appointment.
- `AppointmentPatchInput`: dữ liệu update appointment, partial nhưng bắt buộc có ít nhất 1 field.
- `AppointmentStatus`: `"booked" | "cancelled"`.
- `ReminderDeliveryStatus`: trạng thái từng reminder.
- `ReminderStatus`: nhóm 3 reminder `before/atTime/after`.
- `AppointmentHistoryEntry`: một dòng lịch sử thay đổi appointment.
- `AppointmentEmailPayload`: dữ liệu cần để gửi email.
- `AppointmentState`: state đầy đủ của appointment.
- `CreateAppointmentWorkflowInput`: input Restate create.
- `UpdateAppointmentWorkflowInput`: input Restate update.
- `ReminderDeliveryRequest`: worker gọi Restate trước khi gửi email.
- `ReminderDeliveryStartResult`: Restate trả `shouldSend`.
- `ReminderDeliveryResultInput`: worker ghi kết quả gửi email về Restate.
- `SendReminderInput`: payload delayed call của Restate.
- `AppointmentEventType`: loại event Kafka.
- `AppointmentEventEnvelope`: envelope Kafka event.

### `models/appointment.schema.ts`

Mongoose schema cho collection `appointments`.

Export chính:

- `AppointmentRecord`: MongoDB record, dùng `appointmentId` thay cho `id`.
- `AppointmentDocument`: hydrated Mongoose document type.
- `AppointmentModel`: Mongoose model.

Schema nội bộ:

- `ReminderDeliveryStatusSchema`: schema trạng thái reminder.
- `AppointmentHistoryEntrySchema`: schema lịch sử.
- `AppointmentSchema`: schema chính.

Index chính:

- `startAt`.
- `customerEmail + startAt`.
- `appointmentId` unique.
- `idempotencyKey` unique sparse.

### `models/appointment.repository.ts`

Repository thao tác MongoDB cho appointment.

Hàm/export chính:

- `createAppointmentRecord(appointment)`: insert appointment mới, throw duplicate nếu trùng.
- `findAppointmentById(id)`: tìm appointment theo `appointmentId`, trả `undefined` nếu không có.
- `replaceAppointmentRecord(appointment)`: ghi đè toàn bộ appointment hiện có.
- `listAppointments(input)`: list appointments theo `status`, `customerEmail`, `limit`.

Helper:

- `AppointmentRepositoryError`: error có code `not_found` hoặc `duplicate`.
- `toRecord(appointment)`: đổi `AppointmentState.id` thành Mongo `appointmentId`.
- `toState(record)`: đổi Mongo record về `AppointmentState` và validate Zod.
- `isMongoDuplicateKeyError(error)`: nhận diện Mongo duplicate key code `11000`.

### `models/chat.model.ts`

Zod schemas cho chat.

Export chính:

- `ChatMessage`: message đầy đủ lưu DB/publish Kafka.
- `SendChatInput`: input API gửi chat.

### `models/chat.schema.ts`

Mongoose schema cho collection `chat_messages`.

Export chính:

- `ChatMessageDocument`.
- `ChatMessageModel`.

Index:

- `id` unique.
- `roomId`.
- `roomId + sentAt`.

### `models/chat.repository.ts`

Repository lưu chat.

Hàm chính:

- `saveChatMessage(message)`: insert message vào MongoDB, duplicate key thì trả `{ inserted: false }` để idempotent.

## Validation

### `validation/appointment.validation.ts`

Schema validate request API.

Export chính:

- `CreateAppointmentInput`: mở rộng `AppointmentInput` thêm `id` và `idempotencyKey`.
- `BulkCreateAppointmentInput`: input tạo hàng loạt appointment.
- `UpdateAppointmentInput`: alias của `AppointmentPatchInput`.
- `ListAppointmentsQuery`: validate query `status`, `customerEmail`, `limit`.

## Controllers

### `controllers/appointment.controller.ts`

HTTP handlers cho appointment routes.

Hàm chính:

- `createAppointment(c)`: parse body, tạo appointment ID, gọi Restate `create`, trả `201`.
- `bulkCreateAppointments(c)`: tạo nhiều appointment theo batch 10 để stress test.
- `listAppointments(c)`: đọc query, gọi Mongo repository list.
- `getAppointment(c)`: gọi Restate `get` theo ID.
- `updateAppointment(c)`: parse patch body, gọi Restate `update`.
- `cancelAppointment(c)`: gọi Restate `cancel`.

Helper:

- `appointmentClient(id)`: tạo Restate object client cho appointment ID.
- `appointmentId(c)`: lấy `id` từ URL param.

### `controllers/chat.controller.ts`

HTTP handlers cho chat.

Hàm chính:

- `sendMessage(c)`: parse input, tạo `ChatMessage`, publish Kafka/SSE qua `publishChatMessage`, trả `201`.
- `streamMessages(c)`: tạo SSE stream theo `roomId`, subscribe local EventEmitter, gửi keep-alive.

## Routes

### `routes/appointment.routes.ts`

Tạo router appointment.

Hàm chính:

- `createAppointmentRoutes()`: đăng ký routes:
  - `POST /`
  - `POST /bulk`
  - `GET /`
  - `GET /:id`
  - `PATCH /:id`
  - `DELETE /:id`

### `routes/chat.routes.ts`

Tạo router chat.

Hàm chính:

- `createChatRoutes()`: đăng ký:
  - `POST /send`
  - `GET /stream/:roomId`

## Middleware

### `middlewares/error.middleware.ts`

Middleware xử lý lỗi toàn cục cho Hono.

Export chính:

- `errorMiddleware`: đổi lỗi thành HTTP response:
  - Zod error -> `400`.
  - Message chứa `does not exist` -> `404`.
  - Message chứa `already exists` hoặc `already cancelled` -> `409`.
  - Lỗi khác -> log và trả `500`.

## Services

### `services/restate/appointment.handler.ts`

File quan trọng nhất của workflow appointment. Định nghĩa Restate Virtual Object `Appointment`.

Export chính:

- `appointmentObject`: Restate object có các handlers.
- `AppointmentObject`: type của Restate object.

Handlers:

- `create`: tạo appointment mới, idempotency check, schedule reminders, persist MongoDB, publish `appointment.created`.
- `get`: load appointment từ MongoDB và trả về state.
- `update`: load appointment, cancel delayed invocations cũ, tăng version, schedule reminders mới, persist, publish `appointment.updated`.
- `cancel`: cancel delayed invocations cũ, set status `cancelled`, persist, publish `appointment.cancelled`.
- `sendReminder`: Restate delayed call tự chạy khi tới giờ, validate state/version/status rồi enqueue BullMQ email job.
- `startReminderDelivery`: email worker gọi trước khi gửi mail để validate lần cuối; trả `shouldSend`.
- `recordReminderResult`: email worker gọi sau khi gửi mail để ghi `sent/skipped/failed`, persist, và publish `reminder.sent` nếu gửi thành công.

Helper:

- `scheduleReminders(ctx, appointment)`: tính 3 mốc reminder, tạo delayed call Restate, lưu invocation IDs.
- `cancelScheduledReminders(ctx)`: đọc invocation IDs và `ctx.cancel()` delayed calls cũ.
- `setState(ctx, a)`: lưu summary vào Restate K/V.
- `snapshot(a)`: lấy bản tóm tắt appointment để publish event.
- `emailPayload(a)`: lấy payload đủ để email worker gửi email.
- `freshReminders(version)`: tạo reminder status rỗng cho version mới.
- `cancelledReminders(existing, version, now)`: đánh dấu reminders đang chờ thành cancelled.
- `cancelledReminderHistory(existing, version, now)`: tạo history entries cho reminders bị cancel.
- `validateReminder(appointment, input)`: kiểm tra version cũ, appointment cancelled, already sent, stale job.

### `services/database/mongodb.service.ts`

Quản lý lifecycle MongoDB.

Hàm chính:

- `connectMongo()`: connect Mongoose, dùng `env.mongoDbName` nếu có.
- `disconnectMongo()`: disconnect nếu đang kết nối.
- `mongoReadyState()`: trả trạng thái Mongoose connection cho health check.

### `services/email/email.service.ts`

Gửi email reminder.

Export chính:

- `EmailSendResult`: union type kết quả gửi email.
- `sendAppointmentEmail(reminder, appointment)`: tạo subject/body, gửi qua SendGrid nếu có API key, hoặc mock log nếu không có API key.

Constant nội bộ:

- `SUBJECT`: tiêu đề theo reminder type.
- `BODY`: nội dung theo reminder type.

### `services/queue/email-queue.service.ts`

Quản lý queue email.

Export chính:

- `AppointmentEmailJobData`: Zod schema job email.
- `EMAIL_QUEUE_NAME`, `appointmentEmailQueue`: re-export từ config queue.
- `enqueueImmediateEmail(input)`: validate data, tạo deterministic job ID, add job vào BullMQ.
- `closeAppointmentEmailQueue()`: đóng queue.

Helper:

- `appointmentEmailJobId(appointmentId, version, reminder)`: tạo job ID dạng `appointment-email-{id}-{version}-{reminder}`.

### `services/queue/maintenance-queue.service.ts`

Quản lý queue bảo trì.

Export chính:

- `MaintenanceJobData`: schema job bảo trì.
- `MAINTENANCE_QUEUE_NAME`, `maintenanceQueue`: re-export.
- `upsertCleanupScheduler()`: tạo/cập nhật scheduler chạy cleanup định kỳ.
- `closeMaintenanceQueue()`: đóng queue.

### `services/messaging/kafka.service.ts`

Framework dùng chung cho Kafka topics/consumers.

Export chính:

- Re-export `connectKafkaProducer`, `disconnectKafkaProducer`, `getKafkaProducer`, `kafkaProducerReady`.
- `ensureKafkaTopics()`: tạo topic `appointment-events` và `chat-messages`.
- `runConsumer(options)`: chạy consumer đọc `appointment-events`, decode schema, validate `AppointmentEventEnvelope`, gọi `onEvent`.
- `runChatConsumer(options)`: chạy consumer đọc `chat-messages`, decode schema, validate `ChatMessage`, gọi `onMessage`.

Helper:

- `createKafkaConsumer(groupId)`: tạo KafkaJS consumer với group ID.
- `subscribeToAppointmentEvents(consumer, eachMessage)`: subscribe `appointment-events`.
- `isConfluentWireFormat(buffer)`: nhận diện message đã encode qua Schema Registry.

### `services/messaging/event-publisher.service.ts`

Publish appointment events.

Hàm chính:

- `publishAppointmentEvent(event)`: validate Zod, register/get schema ID, encode Confluent wire format, gửi vào topic `appointment-events`, đồng thời emit SSE realtime.

Constant nội bộ:

- `appointmentEventSchema`: JSON schema đọc từ `schemas/appointment-event.schema.json`.
- `SUBJECT`: subject Schema Registry dạng `<topic>-value`.

### `services/messaging/chat.service.ts`

Publish chat message và realtime chat local.

Hàm chính:

- `publishChatMessage(message)`: register/get chat schema, encode, gửi vào topic `chat-messages`, emit local EventEmitter.
- `subscribeChatRoom(roomId, listener)`: subscribe message theo phòng chat, trả unsubscribe function.

Constant nội bộ:

- `chatMessageSchema`: JSON schema đọc từ `schemas/chat-message.schema.json`.
- `SUBJECT`: subject Schema Registry của chat.
- `emitter`: EventEmitter nội bộ process API.

### `services/messaging/realtime.service.ts`

EventEmitter nội bộ cho appointment SSE.

Hàm chính:

- `emitRealtimeEvent(event)`: phát appointment event cho SSE clients.
- `subscribeRealtimeEvents(listener)`: đăng ký listener và trả unsubscribe function.

### `services/messaging/telegram.service.ts`

Gửi Telegram notification và format message.

Hàm chính:

- `isTelegramConfigured()`: kiểm tra token/chat ID.
- `sendTelegramMessage(text)`: POST HTML message tới Telegram Bot API.
- `formatAppointmentEvent(event)`: format appointment event thành HTML text.

Helper:

- `formatDateTime(value)`: format timestamp theo timezone `Asia/Ho_Chi_Minh`.

## Workers Và Consumers

### `workers/email.worker.ts`

BullMQ worker xử lý queue email reminder.

Luồng:

```txt
BullMQ job -> startReminderDelivery -> sendAppointmentEmail -> recordReminderResult
```

Hàm chính:

- `processEmailJob(job)`: validate job, gọi Restate kiểm tra, gửi email, ghi kết quả.
- `toRecordableResult(result)`: đổi `EmailSendResult` sang status `sent/skipped`.
- `isFinalAttempt(job)`: kiểm tra job đang ở lần retry cuối.
- `errorMessage(error)`: lấy message từ unknown error.
- `shutdown(signal)`: đóng worker, queue, Redis.

### `workers/maintenance.worker.ts`

BullMQ worker dọn dẹp job cũ.

Hàm chính:

- `processMaintenanceJob(job)`: switch theo `data.type`.
- `cleanupQueues()`: clean completed/failed jobs cũ trong email queue.
- `shutdown(signal)`: đóng worker, queues, Redis.

### `workers/telegram.consumer.ts`

Kafka consumer đọc `appointment-events`, gửi Telegram.

Chức năng:

- Kiểm tra Telegram config.
- Gọi `runConsumer` với group `appointment-telegram`.
- Trong `onEvent`: format event và gọi `sendTelegramMessage`.

### `workers/chat.consumer.ts`

Kafka consumer đọc `chat-messages`, lưu MongoDB.

Chức năng:

- Connect MongoDB.
- Gọi `runChatConsumer` với group `chat-storage`.
- Trong `onMessage`: gọi `saveChatMessage`.
- Shutdown thì disconnect MongoDB.

## Scripts

### `scripts/dev.ts`

Script dev tổng hợp.

Chức năng:

- Start Docker services.
- Chờ các port infra sẵn sàng.
- Spawn API, email worker, maintenance worker, telegram consumer, chat consumer.
- Register Restate endpoint.
- Register schemas.
- Register Kafka Connect connectors.
- Bắt Ctrl+C để kill child processes và stop Docker services.

Hàm chính:

- `dockerCompose(...args)`: chạy `docker compose`.
- `spawnProcess(name, args, color)`: spawn process con và gắn prefix log.
- `runOnce(cmd, args)`: chạy command một lần, resolve/reject theo exit code.
- `waitForPort(port, name, timeout)`: chờ TCP port mở.
- `tryConnect(port)`: thử connect TCP.
- `shutdown()`: dừng process và Docker services.
- `log(msg)`: log prefix `[dev]`.

### `scripts/register-restate.ts`

Đăng ký endpoint app với Restate Admin API.

Chức năng:

- Retry tối đa 30 lần.
- POST `${RESTATE_ADMIN_URL}/deployments` với `PUBLIC_RESTATE_ENDPOINT`.
- `200` hoặc `409` đều coi là OK.

### `scripts/register-schemas.ts`

Đăng ký JSON schemas lên Schema Registry.

Hàm chính:

- `registerSchemas()`: register `appointment-events-value` và `chat-messages-value`.

### `scripts/register-connectors.ts`

Đăng ký hoặc update Kafka Connect connectors từ folder `connectors/`.

Hàm chính:

- `waitForConnectApi(url, timeoutMs)`: chờ Kafka Connect REST API sẵn sàng.
- `registerConnectors()`: đọc mọi file `.json`, nếu connector tồn tại thì `PUT /config`, nếu chưa thì `POST /connectors`.

### `scripts/clear-queue.ts`

Xóa toàn bộ BullMQ jobs.

Hàm chính:

- `clearAllJobs()`: log counts, obliterate email queue và maintenance queue, kiểm tra lại counts, đóng queues.

### `scripts/setup-acl.sh`

Shell script tạo Redpanda SASL users và ACLs.

Tạo users:

- `admin`
- `api-service`
- `telegram-consumer`
- `chat-consumer`
- `kafka-connect`

Gán quyền:

- `api-service`: write/read/describe/create trên `appointment-events`, `chat-messages`; read group dev.
- `telegram-consumer`: read/describe `appointment-events`, read group `appointment-telegram`.
- `chat-consumer`: read/describe `chat-messages`, read group `chat-storage`.
- `kafka-connect`: read/describe `appointment-events`, all trên `_connect-*` và `appointment-events-dlq`, read các group Connect.

### `scripts/test-unauthorized-consumer.ts`

Script test ACL Kafka consumer.

Chức năng:

- Dùng username/password/topic/group từ env hoặc default.
- Connect KafkaJS consumer.
- Subscribe topic.
- Nếu thiếu quyền topic/group sẽ in lỗi authorization.

Helper:

- `errorSummary(error)`: rút gọn error.
- `readCsvEnv(name, fallback)`: đọc brokers CSV.
- `readBooleanEnv(name, fallback)`: đọc boolean.
- `sleep(ms)`: delay.

## Connectors

### `connectors/Dockerfile`

Build image Kafka Connect dựa trên `confluentinc/cp-kafka-connect-base:7.9.0`, cài plugin:

- `mongodb/kafka-connect-mongodb:1.14.1`

### `connectors/mongodb-sink-appointment-events.json`

Kafka Connect MongoDB Sink config.

Chức năng:

- Đọc topic `appointment-events`.
- Decode value bằng `io.confluent.connect.json.JsonSchemaConverter`.
- Ghi vào MongoDB:
  - database: `Redpanda`
  - collection: `event_logs`
- Dùng `eventId` làm document ID.
- Thêm field `processedAt`.
- Nếu lỗi thì đẩy sang topic `appointment-events-dlq`.

## Schemas

### `schemas/appointment-event.schema.json`

JSON Schema cho event envelope trong topic `appointment-events`.

Field bắt buộc:

- `eventId`
- `type`
- `appointmentId`
- `version`
- `occurredAt`
- `payload`

`type` hợp lệ:

- `appointment.created`
- `appointment.updated`
- `appointment.cancelled`
- `reminder.sent`

### `schemas/chat-message.schema.json`

JSON Schema cho message trong topic `chat-messages`.

Field bắt buộc:

- `id`
- `roomId`
- `from`
- `text`
- `sentAt`

## Utils

### `utils/appointment.utils.ts`

Helper appointment/reminder.

Export chính:

- `REMINDER_TYPES`: `["before", "atTime", "after"]`.
- `appointmentIdFromIdempotencyKey(idempotencyKey)`: hash SHA-256 idempotency key để tạo deterministic appointment ID.
- `reminderTargetMs(reminder, startAt)`: tính mốc gửi reminder theo `before/atTime/after`.

### `utils/logger.ts`

Cấu hình logger Pino.

Export chính:

- `logger`: logger chung có `service`, `environment`, timestamp ISO.

## Public Và API Client

### `public/chat.html`

Trang HTML demo chat. Dùng browser để gửi message và mở SSE stream theo room.

### `postman/Redpanda-Appointment-API.postman_collection.json`

Postman collection để test các endpoint API như health, appointment CRUD, chat.

## Claude/Codex Helper Files

### `.claude/settings.json`

Cấu hình quyền command cho Claude local environment. Không ảnh hưởng runtime app.

### `.claude/commands/add-endpoint.md`

Prompt template/hướng dẫn scaffold endpoint mới theo pattern project.

### `.claude/commands/add-worker.md`

Prompt template/hướng dẫn scaffold BullMQ worker hoặc Kafka consumer mới.

### `.claude/commands/debug-restate.md`

Hướng dẫn debug Restate Admin API: list invocations, xem state, cancel/purge invocation.

### `.claude/commands/explain-flow.md`

Hướng dẫn cách giải thích flow trong dự án theo từng component và file liên quan.

## Các Topic Và Consumer Group Hiện Tại

Topics app:

- `appointment-events`: event lịch hẹn.
- `appointment-events-dlq`: Dead Letter Queue cho MongoDB Sink connector.
- `chat-messages`: tin nhắn chat.

Consumer groups:

- `appointment-connect`: group nội bộ Kafka Connect worker.
- `appointment-telegram`: Telegram consumer đọc `appointment-events`.
- `chat-storage`: Chat consumer đọc `chat-messages`.
- `connect-mongodb-appointment-events-sink`: MongoDB Sink Connector đọc `appointment-events`.

## Luồng Reminder Tóm Tắt

```txt
create/update appointment
  -> scheduleReminders()
  -> Restate delayed sendReminder()
  -> enqueueImmediateEmail()
  -> BullMQ appointment-email
  -> email.worker processEmailJob()
  -> startReminderDelivery()
  -> sendAppointmentEmail()
  -> recordReminderResult()
  -> MongoDB + Kafka reminder.sent
```

Các guard quan trọng:

- `version` chống reminder cũ gửi sau khi appointment đã update.
- `status === "cancelled"` chống gửi mail cho lịch đã hủy.
- deterministic job ID chống enqueue trùng khi replay/retry.
- `startReminderDelivery` là lớp validate cuối trước khi gửi email thật.
