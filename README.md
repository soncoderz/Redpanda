# Redpanda Appointment Backend

Backend Node.js/TypeScript dùng Hono, Restate, BullMQ và Redis để quản lý lịch hẹn và email nhắc lịch.

## Cấu Trúc

```text
config/        # Cấu hình môi trường
controllers/   # HTTP controller và dashboard controller
logs/          # Thư mục log runtime
middlewares/   # Middleware xử lý lỗi
models/        # Schema và type dữ liệu
node_modules/  # Dependencies sau khi npm install
routes/        # Khai báo route
scripts/       # Worker và script vận hành
services/      # Business logic, queue, Redis, Restate, mailer
utils/         # Hàm tiện ích dùng chung
validation/    # Schema validate request
```

File chạy chính:

```text
app.ts
server.ts
```

## Chạy Dự Án

```bash
npm install
```

Tạo file `.env` từ mẫu:

```powershell
Copy-Item .env.example .env
```

Chạy API server:

```bash
npm run dev
```

Chạy email worker ở terminal khác:

```bash
npm run worker:dev
```

Build và chạy production:

```bash
npm run build
npm start
npm run worker
```

## Endpoint Chính

- `GET /health`
- `POST /api/appointments`
- `GET /api/appointments/:id`
- `PUT /api/appointments/:id`
- `POST /api/appointments/:id/arrived`
- `POST /restate`
- `GET /admin/queues`
