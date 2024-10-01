import { ColumnType, FileMigrationProvider, Generated, GeneratedAlways, Kysely, MigrationInfo, Migrator } from "kysely";
import { Logger } from "./log";
import { err, ok, Result } from "neverthrow";
import path from "path";
import { promises as fs } from "fs";
import { fileURLToPath } from "node:url";
import { HubTables } from "@farcaster/hub-shuttle";
import { CastIdJson, Fid, VerificationAddEthAddressBodyJson } from "../shuttle";
import { MessageType, ReactionType, UserDataType } from "@farcaster/hub-nodejs";

export const TIMESTAMP_48_HOURS = 172800;

const createMigrator = async (db: Kysely<HubTables>, dbSchema: string, log: Logger) => {
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  const migrator = new Migrator({
    db,
    migrationTableSchema: dbSchema,
    provider: new FileMigrationProvider({
      fs,
      path,
      migrationFolder: path.join(currentDir, "migrations"),
    }),
  });

  return migrator;
};

export const migrateToLatest = async (
  db: Kysely<HubTables>,
  dbSchema: string,
  log: Logger,
): Promise<Result<void, unknown>> => {
  const migrator = await createMigrator(db, dbSchema, log);

  const { error, results } = await migrator.migrateToLatest();

  results?.forEach((it) => {
    if (it.status === "Success") {
      log.info(`Migration "${it.migrationName}" was executed successfully`);
    } else if (it.status === "Error") {
      log.error(`failed to execute migration "${it.migrationName}"`);
    }
  });

  if (error) {
    log.error("Failed to apply all database migrations");
    log.error(error);
    return err(error);
  }

  log.info("Migrations up to date");
  return ok(undefined);
};

export type MessageData = {
  messageType: MessageType;
  fid: Fid;
  timestamp: Date;
  network: Number;
  hash: Uint8Array;
  hashScheme: Number;
  signature: Uint8Array;
  signatureScheme: Number;
  signer: Uint8Array;
};

export type MessageRow = MessageData & {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  deletedAt: Date | null;
};

export type CastRow = MessageRow & {
  text: string;
  embedsDeprecated: string[];
  embeds: string[];
  mentions: number[];
  mentionsPositions: number[];
  parentUrl: string | null;
  parentCastId: CastIdJson;
};

export type ReactionsRow = MessageRow & {
  type: ReactionType;
  targetCastId: CastIdJson;
  targetUrl: string;
};

export type LinkRow = MessageRow & {
  type: string;
  targetFid: Number;
};

export type VerificationRow = MessageRow & {
  claim: VerificationAddEthAddressBodyJson;
};

export type UserDataRow = MessageRow & {
  type: UserDataType;
  value: string;
};

export type OnChainEventRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  timestamp: Date;
  fid: Fid;
  blockNumber: number;
  logIndex: number;
  type: number;
  txHash: Uint8Array;
  body: Record<string, string | number>;
};

type NotificationExtraData = {
  likedBy: Fid[];
  followedBy: Fid[];
  targetCastHash: string | null;
  newCastHash: string | null;
};

export type Notifications = {
  id: number;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  notificationType: EngagementType;
  recipient: Fid;
  actor: Fid;
  timestamp: Date;
  isRead: boolean;
  timestampGroup: Date;
  unitIdentifier: string | null;
  extraData: NotificationExtraData;
};

export enum EngagementType {
  LIKE = 1,
  FOLLOW = 2,
  RECAST = 3,
  REPLY = 4,
  MENTION = 5,
  QUOTE = 6,
  TRADE = 7,
}

export type FarcasterUser = {
  id: number;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  fid: Fid;
  username: string;
  displayName: string;
  custodyAddress: string | null;
  profileBio: string;
  pfpUrl: string;
  followerCount: number;
  followingCount: number;
  activeStatus: string;
  powerBadge: boolean;
  cdnUrl: string;
  triedSavingCdnUrl: boolean;
  banned: boolean;
  score: number | null;
  madePointsZero: boolean;
  lastCometchatSync: Date | null;
};

export interface Tables extends HubTables {
  casts: CastRow;
  reactions: ReactionsRow;
  onchain_events: OnChainEventRow;
  links: LinkRow;
  verifications: VerificationRow;
  user_data: UserDataRow;
  notifications: Notifications;
  farcaster_user: FarcasterUser;
}

export type AppDb = Kysely<Tables>;
