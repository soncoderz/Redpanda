import "dotenv/config";
import { spawn, type ChildProcess } from "node:child_process";
import { createConnection } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

/* ───────── Cấu hình ───────── */

// Danh sách Docker container cần khởi động
const DOCKER_SERVICES = ["mongo", "redis", "redpanda-0", "redpanda-console", "restate", "kafka-connect"];

// Các port cần chờ sẵn sàng trước khi chạy ứng dụng
const INFRA_PORTS = [
  { port: 27017, name: "MongoDB" },
  { port: 6379, name: "Redis" },
  { port: 19092, name: "Redpanda" },
  { port: 18080, name: "Restate" },
  { port: 8083, name: "Kafka Connect" },
];

// Danh sách process ứng dụng cần chạy song song (mỗi process có màu riêng)
const APP_PROCESSES = [
  { name: "api", args: ["watch", "server.ts"], color: "\x1b[36m" },
  { name: "email-worker", args: ["workers/email.worker.ts"], color: "\x1b[33m" },
  { name: "maintenance", args: ["workers/maintenance.worker.ts"], color: "\x1b[32m" },
  // analytics consumer disabled — thay bằng Kafka Connect MongoDB Sink
  // { name: "analytics", args: ["workers/analytics.consumer.ts"], color: "\x1b[35m" },
  { name: "telegram", args: ["workers/telegram.consumer.ts"], color: "\x1b[34m" },
  { name: "chat", args: ["workers/chat.consumer.ts"], color: "\x1b[91m" },
];

/* ───────── State ───────── */

// Danh sách child process đang chạy (dùng để kill khi shutdown)
const children: ChildProcess[] = [];
// Flag tránh shutdown nhiều lần
let stopping = false;

// ANSI color codes cho output
const R = "\x1b[0m";
const DIM = "\x1b[2m";
const BOLD = "\x1b[1m";

/* ───────── Main: khởi động toàn bộ hệ thống ───────── */

try {
  // Bước 1: Khởi động Docker containers (MongoDB, Redis, Redpanda, Restate)
  log("Starting Docker containers...");
  await dockerCompose("up", "-d", ...DOCKER_SERVICES);

  // Bước 2: Chờ tất cả port infrastructure sẵn sàng
  log("Waiting for infrastructure...");
  await Promise.all(INFRA_PORTS.map((p) => waitForPort(p.port, p.name)));
  log("Infrastructure ready");

  // Bước 3: Khởi động các process ứng dụng (API, workers, consumers)
  log("Starting application processes...");
  for (const p of APP_PROCESSES) {
    spawnProcess(p.name, p.args, p.color);
  }

  // Bước 4: Chờ API server sẵn sàng
  await waitForPort(9080, "API");

  // Bước 5: Đăng ký endpoint với Restate Admin API
  log("Registering Restate endpoint...");
  await runOnce("tsx", ["scripts/register-restate.ts"]);

  // Bước 6: Đăng ký JSON schemas lên Schema Registry
  log("Registering schemas...");
  await runOnce("tsx", ["scripts/register-schemas.ts"]);

  // Bước 7: Đăng ký Kafka Connect connectors (non-fatal — Connect khởi động chậm)
  log("Registering connectors...");
  await runOnce("tsx", ["scripts/register-connectors.ts"]).catch((err) => {
    log(`⚠ Connector registration failed (run 'npm run connect:register' manually): ${err.message}`);
  });

  log(`${BOLD}All services running!${R} Press Ctrl+C to stop.`);
} catch (error: any) {
  console.error(`\n${DIM}[dev]${R} Fatal: ${error.message}`);
  await shutdown();
}

// Bắt SIGINT (Ctrl+C) và SIGTERM để graceful shutdown
process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());

/* ───────── Docker helpers ───────── */

/** Chạy lệnh docker compose với các tham số */
function dockerCompose(...args: string[]) {
  return runOnce("docker", ["compose", ...args]);
}

/* ───────── Process management ───────── */

/** Spawn process con với output có tag màu — stdout/stderr được pipe ra terminal */
function spawnProcess(name: string, args: string[], color: string) {
  const child = spawn("tsx", args, {
    shell: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, FORCE_COLOR: "1" },
  });

  // Tạo tag hiển thị tên process với màu
  const tag = `${color}[${name.padEnd(12)}]${R} `;

  // Pipe stdout/stderr ra terminal với tag prefix
  const pipe = (stream: NodeJS.ReadableStream | null, out: NodeJS.WriteStream) => {
    stream?.on("data", (data: Buffer) => {
      for (const line of data.toString().split("\n")) {
        if (line.trim()) out.write(`${tag}${line}\n`);
      }
    });
  };

  pipe(child.stdout, process.stdout);
  pipe(child.stderr, process.stderr);

  // Log khi process thoát (trừ khi đang shutdown)
  child.on("exit", (code) => {
    if (!stopping) log(`${color}${name}${R} exited (code ${code})`);
  });

  children.push(child);
}

/** Chạy command 1 lần và chờ kết thúc — dùng cho docker compose và register-restate */
function runOnce(cmd: string, args: string[]) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, { shell: true, stdio: "inherit" });
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} ${args[0]} failed (code ${code})`)),
    );
  });
}

/* ───────── Port check ───────── */

/** Chờ port sẵn sàng với timeout — poll mỗi 500ms */
function waitForPort(port: number, name: string, timeout = 30_000) {
  return new Promise<void>(async (resolve, reject) => {
    const deadline = Date.now() + timeout;

    while (Date.now() < deadline) {
      const ok = await tryConnect(port);
      if (ok) {
        log(`${name} ready (port ${port})`);
        return resolve();
      }
      await sleep(500);
    }

    reject(new Error(`${name} not ready on port ${port} after ${timeout / 1000}s`));
  });
}

/** Thử kết nối TCP tới port — trả true nếu thành công */
function tryConnect(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const conn = createConnection({ port, host: "localhost" });
    conn.on("connect", () => {
      conn.destroy();
      resolve(true);
    });
    conn.on("error", () => resolve(false));
    conn.setTimeout(1000, () => {
      conn.destroy();
      resolve(false);
    });
  });
}

/* ───────── Shutdown ───────── */

/** Dừng tất cả process và Docker containers — gọi khi nhận Ctrl+C */
async function shutdown() {
  if (stopping) return;
  stopping = true;

  log("Shutting down...");

  // Kill tất cả child process (Windows dùng taskkill, Unix dùng SIGTERM)
  for (const child of children) {
    if (process.platform === "win32" && child.pid) {
      spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { shell: true, stdio: "ignore" });
    } else {
      child.kill("SIGTERM");
    }
  }

  // Chờ 1 giây cho process thoát rồi dừng Docker containers
  await sleep(1_000);

  log("Stopping Docker containers...");
  await dockerCompose("stop", ...DOCKER_SERVICES).catch(() => {});

  log("All stopped");
  process.exit(0);
}

/* ───────── Util ───────── */

/** Log message với prefix [dev] */
function log(msg: string) {
  console.log(`${DIM}[dev]${R}          ${msg}`);
}
