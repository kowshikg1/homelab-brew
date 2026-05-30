# Telegram Webhook Template

This file documents the webhook template configuration for Jellyfin to send notifications to Telegram.

## Plugin Details

- **Plugin**: Webhook
- **Repository**: https://github.com/jellyfin/jellyfin-plugin-webhook

## Setup Instructions

### 1. Add Generic Destination

Navigate to **Settings > Add Generic Destination** in the Jellyfin Webhook plugin.

### 2. Configuration

Fill out the form with the following details:

- **Webhook Name**: Give a recognizable name for your webhook (e.g., "Telegram Notification")
- **Webhook URL**: `https://api.telegram.org/<bot_id>/sendMessage`
  - Replace `<bot_id>` with your Telegram bot token

After adding the **Webhook URL**, Turn on **Status: Enable**



### 3. Enable Events

Enable the following sessions/events:

- **Playback Start**: Triggers notification when playback begins
- **Session Start**: Triggers notification when a session is initiated

These events will send messages to your Telegram bot when the specified triggers occur.

- In **User Filter:** opt for users we want to get notifications from and opt necessary **Item Type** - all
- **Do not send when message body is empty**: Enable this option    


### 3. Request Body Template

Use the following JSON in the request body/**Template:**

```json
{
  "chat_id": "<telegram chat id to get alert on>",
  "parse_mode": "Markdown",
  "text": "
{{#if_equals NotificationType 'PlaybackStart'}}
 🎬 *Jellyfin Playing*

👤 *User:* {{NotificationUsername}}
🖥️ *Device:* {{DeviceName}}
📱 *Client:* {{ClientName}}

🎥 *{{Name}}*
{{#if_equals ItemType 'Episode'}}
📺 *{{SeriesName}}*  
Season {{SeasonNumber}} · Episode {{EpisodeNumber}}
{{else}}
📅 {{Year}}
{{/if_equals}}

⏱️ Position: {{PlaybackPosition}} / {{RunTime}}
{{else}}
  {{#if_equals NotificationType 'SessionStart'}}
🎥 *Jellyfin Session Started*

👤 *User:* {{NotificationUsername}}
🖥️ *Device:* {{DeviceName}}
  {{/if_equals}}
{{/if_equals}}"
}
```

### 4. Add Request Header

Add this header in **Add Request Header**:

- **Key**: `Content-Type`
- **Value**: `application/json`