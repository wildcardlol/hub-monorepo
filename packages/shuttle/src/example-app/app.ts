import {
  DB,
  getDbClient,
  getHubClient,
  MessageHandler,
  StoreMessageOperation,
  MessageReconciliation,
  RedisClient,
  HubEventProcessor,
  EventStreamHubSubscriber,
  EventStreamConnection,
  HubEventStreamConsumer,
  HubSubscriber,
  MessageState,
} from "../index"; // If you want to use this as a standalone app, replace this import with "@farcaster/shuttle"
import { AppDb, MessageData, migrateToLatest, Tables } from "./db";
import {
  bytesToHexString,
  getStorageUnitExpiry,
  getStorageUnitType,
  HubEvent,
  isCastAddMessage,
  isCastRemoveMessage,
  isIdRegisterOnChainEvent,
  isLinkAddMessage,
  isLinkRemoveMessage,
  isMergeOnChainHubEvent,
  isReactionAddMessage,
  isReactionRemoveMessage,
  isSignerOnChainEvent,
  isStorageRentOnChainEvent,
  isUserDataAddMessage,
  isVerificationAddAddressMessage,
  isVerificationRemoveMessage,
  Message,
} from "@farcaster/hub-nodejs";
import { log } from "./log";
import { MAX_FID } from "./env";
import { ok } from "neverthrow";
import { Queue } from "bullmq";
import { bytesToHex, farcasterTimeToDate } from "../utils";
import { createCast, deleteCast } from "./utils/cast";
import { createReaction, deleteReaction } from "./utils/reaction";
import { createLink, deleteLink } from "./utils/link";
import { createVerification, deleteVerification } from "./utils/verification";
import { createUserData } from "./utils/userData";

const hubId = "shuttle";

export class App implements MessageHandler {
  private readonly db: DB;
  private readonly dbSchema: string;
  private hubSubscriber: HubSubscriber;
  private streamConsumer: HubEventStreamConsumer;
  public redis: RedisClient;
  private readonly hubId;

  constructor(
    db: DB,
    dbSchema: string,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer,
  ) {
    this.db = db;
    this.dbSchema = dbSchema;
    this.redis = redis;
    this.hubSubscriber = hubSubscriber;
    this.hubId = hubId;
    this.streamConsumer = streamConsumer;
  }

  static create(
    dbUrl: string,
    dbSchema: string,
    redisUrl: string,
    hubUrl: string,
    totalShards: number,
    shardIndex: number,
    hubSSL = false,
  ) {
    const db = getDbClient(dbUrl, dbSchema);
    const hub = getHubClient(hubUrl, { ssl: hubSSL });
    const redis = RedisClient.create(redisUrl);
    const eventStreamForWrite = new EventStreamConnection(redis.client);
    const eventStreamForRead = new EventStreamConnection(redis.client);
    const shardKey = totalShards === 0 ? "all" : `${shardIndex}`;
    const hubSubscriber = new EventStreamHubSubscriber(
      hubId,
      hub,
      eventStreamForWrite,
      redis,
      shardKey,
      log,
      null,
      totalShards,
      shardIndex,
    );
    const streamConsumer = new HubEventStreamConsumer(hub, eventStreamForRead, shardKey);

    return new App(db, dbSchema, redis, hubSubscriber, streamConsumer);
  }

  async onHubEvent(event: HubEvent, txn: DB): Promise<boolean> {
    if (isMergeOnChainHubEvent(event)) {
      const onChainEvent = event.mergeOnChainEventBody.onChainEvent;
      let body = {};
      if (isIdRegisterOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.idRegisterEventBody.eventType,
          from: bytesToHex(onChainEvent.idRegisterEventBody.from),
          to: bytesToHex(onChainEvent.idRegisterEventBody.to),
          recoveryAddress: bytesToHex(onChainEvent.idRegisterEventBody.recoveryAddress),
        };
      } else if (isSignerOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.signerEventBody.eventType,
          key: bytesToHex(onChainEvent.signerEventBody.key),
          keyType: onChainEvent.signerEventBody.keyType,
          metadata: bytesToHex(onChainEvent.signerEventBody.metadata),
          metadataType: onChainEvent.signerEventBody.metadataType,
        };
      } else if (isStorageRentOnChainEvent(onChainEvent)) {
        body = {
          eventType: getStorageUnitType(onChainEvent),
          expiry: getStorageUnitExpiry(onChainEvent),
          units: onChainEvent.storageRentEventBody.units,
          payer: bytesToHex(onChainEvent.storageRentEventBody.payer),
        };
      }
      try {
        await (txn as AppDb)
          .insertInto("onchain_events")
          .values({
            fid: onChainEvent.fid,
            timestamp: new Date(onChainEvent.blockTimestamp * 1000),
            blockNumber: onChainEvent.blockNumber,
            logIndex: onChainEvent.logIndex,
            txHash: onChainEvent.transactionHash,
            type: onChainEvent.type,
            body: body,
          })
          .execute();
        log.info(`Recorded OnchainEvent ${onChainEvent.type} for fid  ${onChainEvent.fid}`);
      } catch (e) {
        log.error("Failed to insert onchain event", e);
      }
    }
    return false;
  }

  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean,
  ): Promise<void> {
    if (!isNew) {
      // Message was already in the db, no-op
      return;
    }

    const appDB = txn as unknown as AppDb; // Need this to make typescript happy, not clean way to "inherit" table types

    // Example of how to materialize casts into a separate table. Insert casts into a separate table, and mark them as deleted when removed
    // Note that since we're relying on "state", this can sometimes be invoked twice. e.g. when a CastRemove is merged, this call will be invoked 2 twice:
    // castAdd, operation=delete, state=deleted (the cast that the remove is removing)
    // castRemove, operation=merge, state=deleted (the actual remove message)
    const messageDesc = wasMissed ? `missed message (${operation})` : `message (${operation})`;
    const messageData: MessageData = {
      messageType: message.data?.type,
      fid: message.data?.fid,
      timestamp: (message.data && farcasterTimeToDate(message.data.timestamp)) || new Date(),
      network: message.data?.network,
      hash: message.hash,
      hashScheme: message.hashScheme,
      signature: message.signature,
      signatureScheme: message.signatureScheme,
      signer: message.signer,
    };

    const isCastMessage = isCastAddMessage(message) || isCastRemoveMessage(message);
    if (isCastMessage) {
      if (state === "created") {
        createCast(appDB, message, messageData, wasMissed, messageDesc, state, log);
      } else if (state === "deleted") {
        deleteCast(appDB, message, wasMissed, state, messageDesc, log);
      }
      log.info(
        `proc cast: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
          message.data?.type
        })`,
      );
    }

    const isReactionMessage = isReactionAddMessage(message) || isReactionRemoveMessage(message);
    if (isReactionMessage) {
      if (state === "created") {
        createReaction(appDB, message, messageData, wasMissed, messageDesc, state, log);
      } else if (state === "deleted") {
        deleteReaction(appDB, message, wasMissed, state, messageDesc, log);
      }
      log.info(
        `proc reaction: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
          message.data?.type
        })`,
      );
    }

    const isLinkMessage = isLinkAddMessage(message) || isLinkRemoveMessage(message);
    if (isLinkMessage) {
      if (state === "created") {
        createLink(appDB, message, messageData, messageDesc, state, wasMissed, log);
      } else if (state === "deleted") {
        deleteLink(appDB, message, state, wasMissed, messageDesc, log);
      }
      log.info(
        `proc link: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
          message.data?.type
        })`,
      );
    }

    const isVerificationMessage = isVerificationAddAddressMessage(message) || isVerificationRemoveMessage(message);
    if (isVerificationMessage) {
      if (state === "created") {
        createVerification(appDB, message, messageData, messageDesc, state, log);
      } else if (state === "deleted") {
        deleteVerification(appDB, message, state, messageDesc, log);
      }
      log.info(
        `proc verification: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
          message.data?.type
        })`,
      );
    }

    const isUserDataMessage = isUserDataAddMessage(message);
    if (isUserDataMessage) {
      if (state === "created") {
        createUserData(appDB, message, messageData, messageDesc, state, log);
      }
      log.info(
        `proc user_data: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
          message.data?.type
        })`,
      );
    }
  }

  async start() {
    await this.ensureMigrations();
    // Hub subscriber listens to events from the hub and writes them to a redis stream. This allows for scaling by
    // splitting events to multiple streams
    await this.hubSubscriber.start();

    // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("Starting stream consumer");
    // Stream consumer reads from the redis stream and inserts them into postgres
    await this.streamConsumer.start(async (event) => {
      await this.processHubEvent(event);
      return ok({ skipped: false });
    });
  }

  async reconcileFids(fids: number[]) {
    // biome-ignore lint/style/noNonNullAssertion: client is always initialized
    const reconciler = new MessageReconciliation(this.hubSubscriber.hubClient!, this.db, log);
    for (const fid of fids) {
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          if (missingInDb) {
            await HubEventProcessor.handleMissingMessage(this.db, message, this);
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb ? "pruned" : revokedInDb ? "revoked" : "existing";
            log.info(`Reconciled ${messageDesc} message ${bytesToHexString(message.hash)._unsafeUnwrap()}`);
          }
        },
        async (message, missingInHub) => {
          if (missingInHub) {
            log.info(`Message ${bytesToHexString(message.hash)._unsafeUnwrap()} is missing in the hub`);
          }
        },
      );
    }
  }

  async backfillFids(fids: number[], backfillQueue: Queue) {
    const startedAt = Date.now();
    if (fids.length === 0) {
      const maxFidResult = await this.hubSubscriber.hubClient.getFids({ pageSize: 1, reverse: true });
      if (maxFidResult.isErr()) {
        log.error("Failed to get max fid", maxFidResult.error);
        throw maxFidResult.error;
      }
      const maxFid = MAX_FID ? parseInt(MAX_FID) : maxFidResult.value.fids[0];
      if (!maxFid) {
        log.error("Max fid was undefined");
        throw new Error("Max fid was undefined");
      }
      log.info(`Queuing up fids upto: ${maxFid}`);
      // create an array of arrays in batches of 100 upto maxFid
      const batchSize = 10;
      const fids = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize).map((fid) => fid + 1);
      for (const start of fids) {
        const subset = Array.from({ length: batchSize }, (_, i) => start + i);
        await backfillQueue.add("reconcile", { fids: subset });
      }
    } else {
      await backfillQueue.add("reconcile", { fids });
    }
    await backfillQueue.add("completionMarker", { startedAt });
    log.info("Backfill jobs queued");
  }

  private async processHubEvent(hubEvent: HubEvent) {
    await HubEventProcessor.processHubEvent(this.db, hubEvent, this);
  }

  async ensureMigrations() {
    const result = await migrateToLatest(this.db, this.dbSchema, log);
    if (result.isErr()) {
      log.error("Failed to migrate database", result.error);
      throw result.error;
    }
  }

  async stop() {
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`Stopped at eventId: ${lastEventId}`);
  }
}
