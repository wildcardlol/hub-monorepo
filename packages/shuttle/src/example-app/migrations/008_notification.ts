import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: legacy code, avoid using ignore for new code
export const up = async (db: Kysely<any>) => {
  // Casts -------------------------------------------------------------------------------------
  await db.schema
    .createTable("notifications")
    .addColumn("id", "serial", (col) => col.primaryKey())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    // message data
    .addColumn("notificationType", sql`smallint`, (col) => col.notNull())
    .addColumn("recipient", "bigint", (col) => col.notNull())
    .addColumn("actor", "bigint")
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("isRead", "boolean", (col) => col.notNull().defaultTo(false))
    .addColumn("timestampGroup", "timestamptz")
    .addColumn("unitIdentifier", "text")
    .addColumn("extraData", "json")
    .addUniqueConstraint("notification_unique_constraint", [
      "recipient",
      "notificationType",
      "timestampGroup",
      "unitIdentifier",
    ])
    .execute();
};
