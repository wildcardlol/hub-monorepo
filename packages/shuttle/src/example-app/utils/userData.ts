import { bytesToHexString, Message } from "@farcaster/hub-nodejs";
import { AppDb, MessageData } from "../db";
import { MessageState } from "../../index";
import { Logger } from "../log";

export const createUserData = async (
  appDB: AppDb,
  message: Message,
  messageData: MessageData,
  messageDesc: string,
  state: MessageState,
  log: Logger,
) => {
  try {
    await appDB
      .insertInto("user_data")
      .values({
        // message data
        ...messageData,
        // link data
        type: message.data?.userDataBody?.type,
        value: message.data?.userDataBody?.value,
      })
      .execute();

    if (message.data?.userDataBody?.type === 1) {
      await appDB
        .insertInto("farcaster_user")
        .values({
          fid: message.data.fid,
          pfpUrl: message.data?.userDataBody?.value,
        })
        .onConflict((oc) =>
          oc.column("fid").doUpdateSet({
            pfpUrl: message.data?.userDataBody?.value,
          }),
        )
        .execute();
    } else if (message.data?.userDataBody?.type === 2) {
      await appDB
        .insertInto("farcaster_user")
        .values({
          fid: message.data.fid,
          displayName: message.data?.userDataBody?.value,
        })
        .onConflict((oc) =>
          oc.column("fid").doUpdateSet({
            displayName: message.data?.userDataBody?.value,
          }),
        )
        .execute();
    } else if (message.data?.userDataBody?.type === 3) {
      await appDB
        .insertInto("farcaster_user")
        .values({
          fid: message.data.fid,
          profileBio: message.data?.userDataBody?.value,
        })
        .onConflict((oc) =>
          oc.column("fid").doUpdateSet({
            profileBio: message.data?.userDataBody?.value,
          }),
        )
        .execute();
    } else if (message.data?.userDataBody?.type === 6) {
      await appDB
        .insertInto("farcaster_user")
        .values({
          fid: message.data.fid,
          username: message.data?.userDataBody?.value,
        })
        .onConflict((oc) =>
          oc.column("fid").doUpdateSet({
            username: message.data?.userDataBody?.value,
          }),
        )
        .execute();
    }
  } catch (e) {
    log.error(
      `Failed to insert user data: ${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${
        message.data?.type
      })`,
    );
    log.error(e);
  }
};
