# Debug Restate

Kiểm tra trạng thái Restate runtime — invocations, state, pending tasks. Dùng Restate Admin API.

## Thực hiện

Đọc Restate Admin URL từ `config/env.ts` (mặc định: `http://localhost:19070`).

### 1. Liệt kê invocations

```bash
# Tất cả invocations đang pending/running
curl -s http://localhost:19070/invocations | jq '.invocations[] | {id: .id, target: .target, status: .status, created_at: .created_at}'

# Filter theo appointment ID (virtual object key)
curl -s "http://localhost:19070/invocations?target=Appointment/<appointment-id>" | jq '.'
```

### 2. Xem state của 1 appointment

```bash
# Lấy K/V state từ Restate
curl -s http://localhost:19070/services/Appointment/state | jq '.'

# State của 1 appointment cụ thể
curl -s "http://localhost:19070/services/Appointment/<appointment-id>/state" | jq '.'
```

### 3. Cancel invocation

```bash
# Cancel 1 invocation cụ thể
curl -X POST "http://localhost:19070/invocations/<invocation-id>/cancel"
```

### 4. Purge invocations

```bash
# Purge completed/cancelled invocations
curl -X POST "http://localhost:19070/invocations/<invocation-id>/purge"
```

## Quy trình debug

1. **Hỏi user** muốn kiểm tra gì:
   - Liệt kê tất cả invocations đang pending?
   - Xem state của 1 appointment cụ thể? (cần appointment ID)
   - Kiểm tra tại sao reminder không gửi?
   - Cancel invocations bị treo?

2. **Chạy lệnh** tương ứng qua curl/PowerShell.

3. **Phân tích kết quả**:
   - Nếu có nhiều invocations cho cùng appointment → có thể thiếu explicit cancel
   - Nếu state version khác invocation version → invocation sẽ bị skip (stale)
   - Nếu invocation status = "suspended" → đang chờ delay timeout

4. **Đề xuất fix** nếu phát hiện vấn đề.

## Lưu ý
- Restate Admin API chỉ khả dụng khi Restate runtime đang chạy (`docker compose up restate`)
- URL mặc định: `http://localhost:19070` (có thể thay đổi qua `RESTATE_ADMIN_URL` trong `.env`)
- API docs: https://docs.restate.dev/references/admin-api
