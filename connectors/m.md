{
  "name": "mongodb-appointment-events-sink",  // Tên connector (đặt gì cũng được)
  "config": {
    // ── Dùng plugin nào ──
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",  // Plugin MongoDB Sink
    "tasks.max": "1",              // Chạy 1 task (1 thread xử lý)

    // ── Đọc từ đâu, ghi vào đâu ──
    "topics": "appointment-events",           // Đọc từ topic nào
    "connection.uri": "mongodb://mongo:27017", // Kết nối MongoDB ở đâu
    "database": "Redpanda",                   // Ghi vào database nào
    "collection": "event_logs",               // Ghi vào collection nào

    // ── Decode message ──
    "key.converter": "...StringConverter",     // Key là string thường
    "value.converter": "...JsonSchemaConverter", // Value decode bằng Schema Registry
    "value.converter.schema.registry.url": "http://redpanda-0:8081", // Địa chỉ Schema Registry

    // ── Tạo _id cho document ──
    "document.id.strategy": "...ProvidedInValueStrategy",  // Lấy _id từ trong message
    "document.id.strategy.overwrite.existing": false,       // Không ghi đè nếu _id đã tồn tại
    // → field "eventId" trong message sẽ thành "_id" trong MongoDB

    // ── Transform message trước khi ghi ──
    "transforms": "extractId,addProcessedAt",  // 2 bước transform

    // Transform 1: lấy eventId làm key
    "transforms.extractId.type": "...ValueToKey",
    "transforms.extractId.fields": "eventId",

    // Transform 2: thêm field processedAt = thời gian Kafka Connect xử lý
    "transforms.addProcessedAt.type": "...InsertField$Value",
    "transforms.addProcessedAt.timestamp.field": "processedAt",

    // ── Ghi kiểu gì ──
    "writemodel.strategy": "...InsertOneDefaultStrategy",  // Insert mới, không update

    // ── Xử lý lỗi ──
    "mongo.errors.tolerance": "data",          // Lỗi data → bỏ qua, không crash
    "mongo.errors.log.enable": true,           // Log lỗi ra console
    "errors.tolerance": "all",                 // Kafka-level: chịu mọi loại lỗi
    "errors.deadletterqueue.topic.name": "appointment-events-dlq",  // Message lỗi → gửi vào topic DLQ
    "errors.deadletterqueue.topic.replication.factor": 1,
    "errors.deadletterqueue.context.headers.enable": true  // Kèm thông tin lỗi trong header
  }
}
