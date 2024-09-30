# Shuttle Example App
This app demonstrates the usage of the Shuttle package to store data from a hub to a PostgreSQL database and create notifications for event streams. It subscribes to the hub for incoming messages, processes them, and stores them in the database while generating corresponding notifications.

### Features
- Uses the `MessageHandler` to store the event messages:
  - Cast
  - Link
  - Reaction
  - User data
  - Verification

- Creates Notifications for events: 
  - Reaction on casts
  - New user followed
  - Mentions, Quote, Replies on Casts
