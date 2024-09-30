import cluster from "node:cluster";
import os from "node:os";
import { log } from "./log";
import { Command } from "@commander-js/extra-typings";
import { readFileSync } from "fs";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  POSTGRES_URL,
  POSTGRES_SCHEMA,
  REDIS_URL,
  SHARD_INDEX,
  TOTAL_SHARDS,
} from "./env";
import * as process from "node:process";
import url from "node:url";
import { getQueue, getWorker } from "./worker";
import { App } from "./app";

const numCPUs = os.cpus().length;

const runTask = (task: () => Promise<void>) => {
  return async () => {
    try {
      await task();
    } catch (err) {
      log.error(`Error in running the task: ${err}`);
      process.exit(1);
    }
  };
};

const createTaskWorker = (task: () => Promise<void>) => {
  cluster.fork();
  if (cluster.isWorker) {
    // Worker process
    runTask(task)().then(() => process.exit(0));
  } else {
    // Master process
    log.info(`Worker ${process.pid} started`);
  }
};

//If the module is being run directly, start the shuttle
if (import.meta.url.endsWith(url.pathToFileURL(process.argv[1] || "").toString())) {
  async function start() {
    log.info(`Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    log.info("Starting shuttle");
    await app.start();
  }

  async function backfill() {
    log.info(`Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    app.ensureMigrations();
    const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid)) : [];
    log.info(`Backfilling fids: ${fids}`);
    const backfillQueue = getQueue(app.redis.client);
    await app.backfillFids(fids, backfillQueue);
    log.info(`Backfill complete: ${backfillQueue}`);
    // Start the worker after initiating a backfill
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
    return;
  }

  async function worker() {
    log.info(`Starting worker connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  async function runInParallel() {
    if (cluster.isPrimary) {
      // Master process: Fork two workers
      log.info("Starting master process...");

      // Fork for the shuttle task
      const shuttleWorker = cluster.fork();
      shuttleWorker.on("message", (msg) => {
        if (msg === "shuttle-started") {
          log.info("Shuttle process started.");
        }
      });

      // Fork for the worker task
      const workerProcess = cluster.fork();
      workerProcess.on("message", (msg) => {
        if (msg === "worker-started") {
          log.info("Worker process started.");
        }
      });

      cluster.on("exit", (worker, code, signal) => {
        log.info(`Worker ${worker.process.pid} exited with code ${code} and signal ${signal}`);
      });
    } else {
      // Worker processes: Check which one should run which function
      if (cluster.worker?.id === 1) {
        // First worker runs the shuttle
        (async () => {
          await start();
          process.send?.("shuttle-started");
        })();
      } else if (cluster.worker?.id === 2) {
        // Second worker runs the worker task
        (async () => {
          await worker();
          process.send?.("worker-started");
        })();
      }
    }
  }

  // for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  //   process.on(signal, async () => {
  //     log.info(`Received ${signal}. Shutting down...`);
  //     (async () => {
  //       await sleep(10_000);
  //       log.info(`Shutdown took longer than 10s to complete. Forcibly terminating.`);
  //       process.exit(1);
  //     })();
  //     await app?.stop();
  //     process.exit(1);
  //   });
  // }

  const program = new Command()
    .name("shuttle")
    .description("Synchronizes a Farcaster Hub with a Postgres database")
    .version(JSON.parse(readFileSync("./package.json").toString()).version);

  program.command("start").description("Starts the shuttle").action(start);
  program.command("backfill").description("Queue up backfill for the worker").action(backfill);
  program.command("worker").description("Starts the backfill worker").action(worker);
  program.command("both").description("Starts the forward and the backfill worker").action(runInParallel);

  program.parse(process.argv);
}
