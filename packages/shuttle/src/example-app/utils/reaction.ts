import { sql } from "kysely";
import { bytesToHexString, Message } from "@farcaster/hub-nodejs";
import { AppDb, EngagementType, MessageData, TIMESTAMP_48_HOURS } from "../db";
import { Logger } from "../log";
import { MessageState } from "../../index";
import { farcasterTimeToDate } from "../../utils";

export const createReaction = async (
  appDB: AppDb,
  message: Message,
  messageData: MessageData,
  missedMessage: boolean,
  messageDesc: string,
  state: MessageState,
  log: Logger,
) => {
  try {
    await appDB
      .insertInto("reactions")
      .values({
        ...messageData,
        // reactions data
        type: message.data?.reactionBody?.type,
        targetCastId: message.data?.reactionBody?.targetCastId,
        targetUrl: message.data?.reactionBody?.targetUrl || "",
      })
      .execute();

    if (!missedMessage) {
      if (message.data?.reactionBody?.type === 1) {
        await appDB
          .insertInto("notifications")
          .values({
            notificationType: EngagementType.LIKE,
            recipient: message.data?.reactionBody?.targetCastId?.fid,
            actor: message.data.fid,
            timestamp: messageData.timestamp,
            timestampGroup: farcasterTimeToDate(Math.floor(message.data.timestamp / TIMESTAMP_48_HOURS)),
            unitIdentifier: bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap(),
            extraData: {
              liked_by: [message.data?.fid],
            },
          })
          .onConflict((oc) =>
            oc.constraint("notification_unique_constraint").doUpdateSet({
              updatedAt: sql`current_timestamp`,
              actor: message.data?.fid,
              timestamp: messageData.timestamp,
              extraData: sql`jsonb_set(
                notifications.extra_data::jsonb,
                '{liked_by}',
                COALESCE((notifications.extra_data::jsonb->'liked_by')::jsonb, '[]') || ${sql`${message.data?.fid}`}::jsonb
              )`,
            }),
          )
          .execute();
      } else if (message.data?.reactionBody?.type === 2) {
        await appDB
          .insertInto("notifications")
          .values({
            notificationType: EngagementType.RECAST,
            recipient: message.data?.reactionBody?.targetCastId?.fid,
            actor: message.data.fid,
            timestamp: messageData.timestamp,
            timestampGroup: farcasterTimeToDate(Math.floor(message.data.timestamp / TIMESTAMP_48_HOURS)),
            unitIdentifier: bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap(),
            extraData: {
              recasted_by: [message.data?.fid],
            },
          })
          .onConflict((oc) =>
            oc.constraint("notification_unique_constraint").doUpdateSet({
              updatedAt: sql`current_timestamp`,
              actor: message.data?.fid,
              timestamp: messageData.timestamp,
              extraData: sql`jsonb_set(
                notifications.extra_data::jsonb,
                '{recasted_by}',
                COALESCE((notifications.extra_data::jsonb->'recasted_by')::jsonb, '[]') || ${sql`${message.data?.fid}`}::jsonb
              )`,
            }),
          )
          .execute();
      }
    }
  } catch (e) {
    log.error(
      `Failed to insert reaction: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};

export const deleteReaction = async (
  appDB: AppDb,
  message: Message,
  missedMessage: boolean,
  state: MessageState,
  messageDesc: string,
  log: Logger,
) => {
  try {
    await appDB
      .updateTable("reactions")
      .set({
        updatedAt: sql`current_timestamp`,
        deletedAt: (message.data && farcasterTimeToDate(message.data.timestamp)) || new Date(),
      })
      .where("hash", "=", message.hash)
      .execute();

    if (!missedMessage) {
      if (message.data?.reactionBody?.type === 1) {
        await appDB
          .updateTable("notifications")
          .set({
            updatedAt: sql`current_timestamp`,
            extraData: sql`jsonb_set(
              notifications.extra_data::jsonb,
              '{liked_by}', 
              (
                SELECT jsonb_agg(elem)
                FROM jsonb_array_elements(notifications.extra_data::jsonb->'liked_by') AS elem
                WHERE elem NOT IN (${sql`${message.data.fid}`})
              )
            )`,
          })
          .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
          .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
          .where("notificationType", "=", EngagementType.LIKE)
          .where("unitIdentifier", "=", bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap())
          .execute();

        await appDB
          .deleteFrom("notifications")
          .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
          .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
          .where("notificationType", "=", EngagementType.LIKE)
          .where("unitIdentifier", "=", bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap())
          .where(sql`(notifications.extra_data::jsonb->'liked_by')::jsonb`, "=", sql`'[]'::jsonb`)
          .execute();
      } else if (message.data?.reactionBody?.type === 2) {
        await appDB
          .updateTable("notifications")
          .set({
            updatedAt: sql`current_timestamp`,
            extraData: sql`jsonb_set(
              notifications.extra_data::jsonb,
              '{recasted_by}', 
              (
                SELECT jsonb_agg(elem)
                FROM jsonb_array_elements(notifications.extra_data::jsonb->'recasted_by') AS elem
                WHERE elem NOT IN (${sql`${message.data.fid}`})
              )
            )`,
          })
          .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
          .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
          .where("notificationType", "=", EngagementType.RECAST)
          .where("unitIdentifier", "=", bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap())
          .execute();

        await appDB
          .deleteFrom("notifications")
          .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
          .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
          .where("notificationType", "=", EngagementType.RECAST)
          .where("unitIdentifier", "=", bytesToHexString(message.data.reactionBody.targetCastId?.hash)._unsafeUnwrap())
          .where(sql`(notifications.extra_data::jsonb->'recasted_by')::jsonb`, "=", sql`'[]'::jsonb`)
          .execute();
      }
    }
  } catch (e) {
    log.error(
      `Failed to delete reaction: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};
