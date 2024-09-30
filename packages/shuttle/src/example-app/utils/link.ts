import { sql } from "kysely";
import { bytesToHexString, Message } from "@farcaster/hub-nodejs";
import { AppDb, EngagementType, MessageData, TIMESTAMP_48_HOURS } from "../db";
import { Logger } from "../log";
import { MessageState } from "../../index";
import { farcasterTimeToDate } from "../../utils";

export const createLink = async (
  appDB: AppDb,
  message: Message,
  messageData: MessageData,
  messageDesc: string,
  state: MessageState,
  missedMessage: boolean,
  log: Logger,
) => {
  try {
    await appDB
      .insertInto("links")
      .values({
        // message data
        ...messageData,
        // link data
        type: message.data?.linkBody?.type,
        targetFid: message.data?.linkBody?.targetFid,
      })
      .execute();

    if (!missedMessage) {
      await appDB
        .insertInto("notifications")
        .values({
          notificationType: EngagementType.FOLLOW,
          recipient: message.data?.linkBody?.targetFid,
          actor: message.data.fid,
          timestamp: messageData.timestamp,
          timestampGroup: farcasterTimeToDate(Math.floor(message.data.timestamp / TIMESTAMP_48_HOURS)),
          unitIdentifier: message.data?.linkBody?.targetFid,
          extraData: {
            followed_by: [message.data?.fid],
          },
        })
        .onConflict((oc) =>
          oc.constraint("notification_unique_constraint").doUpdateSet({
            updatedAt: sql`current_timestamp`,
            actor: message.data?.fid,
            timestamp: messageData.timestamp,
            extraData: sql`jsonb_set(
              notifications.extra_data::jsonb,
              '{followed_by}',
              COALESCE((notifications.extra_data::jsonb->'followed_by')::jsonb, '[]') || ${sql`${message.data?.fid}`}::jsonb
            )`,
          }),
        )
        .execute();
    }

    await appDB
      .insertInto("farcaster_user")
      .values({
        fid: message.data?.fid,
        followingCount: 1,
      })
      .onConflict((oc) =>
        oc.column("fid").doUpdateSet({
          followingCount: sql`farcaster_user.following_count + 1`,
        }),
      )
      .execute();

    await appDB
      .insertInto("farcaster_user")
      .values({
        fid: message.data?.linkBody?.targetFid,
        followerCount: 1,
      })
      .onConflict((oc) =>
        oc.column("fid").doUpdateSet({
          followerCount: sql`farcaster_user.follower_count + 1`,
        }),
      )
      .execute();
  } catch (e) {
    log.error(
      `Failed to insert link: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};

export const deleteLink = async (
  appDB: AppDb,
  message: Message,
  state: MessageState,
  missedMessage: boolean,
  messageDesc: string,
  log: Logger,
) => {
  try {
    await appDB
      .updateTable("links")
      .set({
        updatedAt: sql`current_timestamp`,
        deletedAt: (message.data && farcasterTimeToDate(message.data.timestamp)) || new Date(),
      })
      .where("hash", "=", message.hash)
      .execute();

    if (!missedMessage) {
      await appDB
        .updateTable("notifications")
        .set({
          updatedAt: sql`current_timestamp`,
          extraData: sql`jsonb_set(
            notifications.extra_data::jsonb,
            '{followed_by}',
            (
              SELECT jsonb_agg(elem)
              FROM jsonb_array_elements(notifications.extra_data::jsonb->'followed_by') AS elem
              WHERE elem NOT IN (${sql`${message.data.fid}`})
            )
          )`,
        })
        .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
        .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
        .where("notificationType", "=", EngagementType.FOLLOW)
        .where("unitIdentifier", "=", null)
        .execute();

      await appDB
        .deleteFrom("notifications")
        .where("recipient", "=", message.data?.reactionBody?.targetCastId?.fid)
        .where("timestampGroup", "=", farcasterTimeToDate(Math.floor(message.data?.timestamp / TIMESTAMP_48_HOURS)))
        .where("notificationType", "=", EngagementType.FOLLOW)
        .where("unitIdentifier", "=", null)
        .where(sql`(notifications.extra_data::jsonb->'followed_by')::jsonb`, "=", sql`'[]'::jsonb`)
        .execute();
    }

    await appDB
      .updateTable("farcaster_user")
      .set({
        followingCount: sql`farcaster_user.following_count - 1`,
      })
      .where("fid", "=", message.data?.fid)
      .execute();

    await appDB
      .updateTable("farcaster_user")
      .set({
        followerCount: sql`farcaster_user.follower_count - 1`,
      })
      .where("fid", "=", message.data?.linkBody?.targetFid)
      .execute();
  } catch (e) {
    log.error(
      `Failed to delete link: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};
