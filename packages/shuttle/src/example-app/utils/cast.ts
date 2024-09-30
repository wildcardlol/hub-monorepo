import { sql } from "kysely";
import { bytesToHexString, Message } from "@farcaster/hub-nodejs";
import { AppDb, EngagementType, MessageData } from "../db";
import { Logger } from "../log";
import { MessageState } from "../../index";
import { farcasterTimeToDate } from "../../utils";

export const createCast = async (
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
      .insertInto("casts")
      .values({
        ...messageData,
        // cast data
        text: message.data?.castAddBody?.text || "",
        embedsDeprecated: message.data?.castAddBody?.embedsDeprecated,
        embeds: message.data?.castAddBody?.embeds,
        mentions: message.data?.castAddBody?.mentions,
        mentionsPositions: message.data?.castAddBody?.mentionsPositions,
        parentUrl: message.data?.castAddBody?.parentUrl || "",
        parentCastId: message.data?.castAddBody?.parentCastId,
      })
      .execute();

    if (!missedMessage) {
      if (message.data?.castAddBody?.mentions) {
        message.data?.castAddBody?.mentions.forEach(async (mention) => {
          await appDB
            .insertInto("notifications")
            .values({
              notificationType: EngagementType.MENTION,
              recipient: mention,
              actor: message.data?.fid,
              timestamp: messageData.timestamp,
              timestampGroup: null,
              unitIdentifier: bytesToHexString(messageData.hash)._unsafeUnwrap(),
              extraData: {
                new_cast_hash: bytesToHexString(messageData.hash)._unsafeUnwrap(),
              },
            })
            .execute();
        });
      }

      if (message.data?.castAddBody?.parentCastId?.hash) {
        if (message.data?.fid !== 786858 || !message.data?.castAddBody?.text.includes("✅")) {
          await appDB
            .insertInto("notifications")
            .values({
              notificationType: EngagementType.REPLY,
              recipient: message.data.castAddBody.parentCastId.fid,
              actor: message.data?.fid,
              timestamp: messageData.timestamp,
              timestampGroup: null,
              unitIdentifier: bytesToHexString(messageData.hash)._unsafeUnwrap(),
              extraData: {
                new_cast_hash: bytesToHexString(messageData.hash)._unsafeUnwrap(),
                target_cast_hash: bytesToHexString(message.data?.castAddBody?.parentCastId?.hash)._unsafeUnwrap(),
              },
            })
            .execute();
        }
      }

      if (message.data?.castAddBody?.embeds) {
        message.data?.castAddBody?.embeds.forEach(async (embed) => {
          if (embed.castId) {
            await appDB
              .insertInto("notifications")
              .values({
                notificationType: EngagementType.QUOTE,
                recipient: embed.castId.fid,
                actor: messageData.fid,
                timestamp: messageData.timestamp,
                timestampGroup: null,
                unitIdentifier: bytesToHexString(messageData.hash)._unsafeUnwrap(),
                extraData: {
                  new_cast_hash: bytesToHexString(messageData.hash)._unsafeUnwrap(),
                  target_cast_hash: bytesToHexString(embed.castId.hash)._unsafeUnwrap(),
                },
              })
              .execute();
          }
        });
      }
    }
  } catch (e) {
    log.error(
      `Failed to insert cast: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};

export const deleteCast = async (
  appDB: AppDb,
  message: Message,
  missedMessage: boolean,
  state: MessageState,
  messageDesc: string,
  log: Logger,
) => {
  try {
    await appDB
      .updateTable("casts")
      .set({
        updatedAt: sql`current_timestamp`,
        deletedAt: (message.data && farcasterTimeToDate(message.data.timestamp)) || new Date(),
      })
      .where("hash", "=", message.hash)
      .execute();

    if (!missedMessage) {
      if (message.data?.castAddBody?.mentions) {
        message.data?.castAddBody?.mentions.forEach(async (mention) => {
          await appDB
            .deleteFrom("notifications")
            .where("recipient", "=", mention)
            .where("timestampGroup", "=", null)
            .where("notificationType", "=", EngagementType.MENTION)
            .where("unitIdentifier", "=", bytesToHexString(message.hash)._unsafeUnwrap())
            .execute();
        });
      }

      if (message.data?.castAddBody?.parentCastId?.hash) {
        await appDB
          .deleteFrom("notifications")
          .where("recipient", "=", message.data?.castAddBody.parentCastId?.fid)
          .where("notificationType", "=", EngagementType.REPLY)
          .where("timestampGroup", "=", null)
          .where("unitIdentifier", "=", bytesToHexString(message.hash)._unsafeUnwrap())
          .execute();
      }

      if (message.data?.castAddBody?.embeds) {
        message.data?.castAddBody?.embeds.forEach(async (embed) => {
          if (embed.castId) {
            await appDB
              .deleteFrom("notifications")
              .where("recipient", "=", embed.castId.fid)
              .where("timestampGroup", "=", null)
              .where("notificationType", "=", EngagementType.QUOTE)
              .where("unitIdentifier", "=", bytesToHexString(message.hash)._unsafeUnwrap())
              .execute();
          }
        });
      }
    }
  } catch (e) {
    log.error(
      `Failed to delete cast: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};
