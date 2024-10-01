import { bytesToHexString, Message } from "@farcaster/hub-nodejs";
import { AppDb, MessageData } from "../db";
import { Logger } from "../log";
import { MessageState } from "../../index";
import { farcasterTimeToDate } from "../../utils";

export const createVerification = async (
  appDB: AppDb,
  message: Message,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  messageData: MessageData,
  messageDesc: string,
  state: MessageState,
  log: Logger,
) => {
  try {
    await appDB
      .insertInto("verifications")
      .values({
        // message data
        ...messageData,
        // link data
        claim: message.data?.verificationAddAddressBody,
      })
      .execute();
  } catch (e) {
    log.error(
      `Failed to insert verification: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};

export const deleteVerification = async (
  appDB: AppDb,
  message: Message,
  state: MessageState,
  messageDesc: string,
  log: Logger,
) => {
  try {
    await appDB
      .updateTable("verifications")
      .set({ deletedAt: (message.data && farcasterTimeToDate(message.data.timestamp)) || new Date() })
      .where("hash", "=", message.hash)
      .execute();
  } catch (e) {
    log.error(
      `Failed to delete verification: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};
