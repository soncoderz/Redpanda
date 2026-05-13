# Add Worker

Scaffold a new BullMQ worker or Kafka consumer for this project. The user will specify which type.

## Steps

1. **Ask** the user for:
   - Type: `bullmq-worker` or `kafka-consumer`
   - Name (e.g., "sms", "webhook", "notification")
   - What this worker/consumer processes (brief description)

---

## BullMQ Worker

### Queue Service (`services/queue/<name>-queue.service.ts`)
```typescript
// Follow pattern from services/queue/email-queue.service.ts
import { Queue } from "bullmq";
import { env } from "../../config/env.js";
import { createRedisConnection } from "../../config/redis.js";

export const QUEUE_NAME = env.<name>QueueName ?? "<name>-queue";

// Define Zod schema for job data
export const <Name>JobData = z.object({ ... });
export type <Name>JobData = z.infer<typeof <Name>JobData>;

export const <name>Queue = new Queue(QUEUE_NAME, {
  connection: createRedisConnection(),
  defaultJobOptions: { ... },
});
```

### Worker (`workers/<name>.worker.ts`)
```typescript
// Follow pattern from workers/email.worker.ts
import "dotenv/config";
import { Worker, type Job } from "bullmq";
// ... setup worker with connection, concurrency, rate limiter
// ... add completed/failed event listeners
// ... graceful shutdown (SIGINT/SIGTERM)
```

### Integration
- Add env vars to `config/env.ts` (queue name, concurrency, rate limit)
- Add to `.env.example`
- Add npm scripts to `package.json`:
  - `"worker:<name>:dev": "tsx watch workers/<name>.worker.ts"`
  - `"worker:<name>": "node dist/workers/<name>.worker.js"`
- Register queue in BullMQ Dashboard (`app.ts` → `createBullBoard`)

---

## Kafka Consumer

### Consumer (`workers/<name>.consumer.ts`)
```typescript
// Follow pattern from workers/telegram.consumer.ts
import "dotenv/config";
import { env } from "../config/env.js";
import { runConsumer } from "../services/messaging/kafka.service.js";
import { logger } from "../utils/logger.js";

await runConsumer({
  groupId: env.kafka<Name>GroupId,
  async onEvent(event) {
    // Process event
    logger.info({ eventId: event.eventId, type: event.type }, "<Name> processed");
  },
});
```

### Integration
- Add `kafka<Name>GroupId` to `config/env.ts`
- Add `KAFKA_<NAME>_GROUP_ID` to `.env.example`
- Add npm scripts to `package.json`:
  - `"consumer:<name>:dev": "tsx watch workers/<name>.consumer.ts"`
  - `"consumer:<name>": "node dist/workers/<name>.consumer.js"`

---

## Final Checks
- Run `npx tsc --noEmit` to verify no type errors
- Verify the worker/consumer can start without errors (if Docker services are running)
