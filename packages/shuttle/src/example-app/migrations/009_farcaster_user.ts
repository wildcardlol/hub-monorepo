import { Kysely } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: legacy code, avoid using ignore for new code
export const up = async (db: Kysely<any>) => {
  // Casts -------------------------------------------------------------------------------------
  await db.schema
    .createTable("farcasterUser")
    .addColumn("fid", "bigint", (col) => col.primaryKey())
    .addColumn("username", "varchar(256)", (col) => col.notNull().defaultTo(""))
    .addColumn("displayName", "varchar(256)", (col) => col.notNull().defaultTo(""))
    .addColumn("profileBio", "text", (col) => col.notNull().defaultTo(""))
    .addColumn("pfpUrl", "varchar(1024)", (col) => col.notNull().defaultTo(""))
    .addColumn("followerCount", "bigint", (col) => col.defaultTo(0))
    .addColumn("followingCount", "bigint", (col) => col.defaultTo(0))
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
