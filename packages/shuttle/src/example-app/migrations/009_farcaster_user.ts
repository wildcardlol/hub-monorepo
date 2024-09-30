import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: legacy code, avoid using ignore for new code
export const up = async (db: Kysely<any>) => {
  // Casts -------------------------------------------------------------------------------------
  await db.schema
    .createTable("farcasterUser")
    .addColumn("fid", "bigint", (col) => col.primaryKey())
    .addColumn("username", "varchar(256)", (col) => col.notNull().defaultTo(""))
    .addColumn("displayName", "varchar(256)", (col) => col.notNull().defaultTo(""))
    .addColumn("custodyAddress", "varchar(100)")
    .addColumn("profileBio", "text", (col) => col.notNull().defaultTo(""))
    .addColumn("pfpUrl", "varchar(1024)", (col) => col.notNull().defaultTo(""))
    .addColumn("followerCount", "bigint", (col) => col.defaultTo(0))
    .addColumn("followingCount", "bigint", (col) => col.defaultTo(0))
    .addColumn("activeStatus", "varchar(100)")
    .addColumn("powerBadge", "boolean", (col) => col.defaultTo(false))
    .addColumn("cdnUrl", "varchar(1024)", (col) => col.defaultTo(""))
    .addColumn("triedSavingCdnUrl", "boolean", (col) => col.defaultTo(false))
    .addColumn("banned", "boolean", (col) => col.defaultTo(false))
    .addColumn("score", "decimal(20, 18)")
    .addColumn("madePointsZero", "boolean", (col) => col.defaultTo(false))
    .addColumn("lastCometchatSync", "timestamp")
    .execute();

  await db.schema.createIndex("farcasterUser_username_index").on("farcasterUser").column("username").execute();

  await db.schema
    .createIndex("farcasterUser_followerCount_index")
    .on("farcasterUser")
    .column("followerCount")
    .execute();

  await db.schema
    .createIndex("farcasterUser_followingCount_index")
    .on("farcasterUser")
    .column("followingCount")
    .execute();
};
