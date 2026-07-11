# Unified Chat — Twitch + Kick + TikTok + YouTube

A single page that merges Twitch, Kick, TikTok, and YouTube chats into one
feed, where each message is tagged with its platform icon (TW / KK / TT / YT).
Just enter a username — no login or password required for reading.

## Run

```bash
npm install
npm start
```

Then open: `http://localhost:3000`

## Configuration via `.env`

All secrets and settings live in a `.env` file at the project root (ignored by
git). This is safer than passing them on the command line, where they would
show up in the process list (`ps` / Task Manager).

```bash
cp .env.example .env      # then edit .env and fill in the values
npm start
```

The `.env.example` template documents every variable. All values are optional;
leave them empty and the site runs open (for local development).

```env
SITE_PASSWORD=              # site password (empty = open site)
TWITCH_CLIENT_ID=           # OAuth for chatting under your own name
TWITCH_CLIENT_SECRET=
TWITCH_REDIRECT_URI=        # auto-built if left empty
TIKTOK_SIGN_API_KEY=        # Euler Stream key for TikTok
PORT=3000
```

> You can still pass any variable on the command line
> (`SITE_PASSWORD=x npm start`) and it will override the `.env` value. But
> `.env` is simpler and safer for production.

### Protecting the site with a password (recommended for public deployments)

Set `SITE_PASSWORD` in `.env`.

- The password is requested on the first entry screen before access to anything.
- It is verified at both the **HTTP** (`/api/auth`) and **WebSocket** (`/ws`)
  layers: no client can join the chat or send a message before passing it.
- After the first successful entry it is persisted in the browser
  (`localStorage`) so it is not requested again.
- **Everyone is forced through the lock screen** — both first-time visitors and
  returning ones. A returning visitor with a valid saved password is let in
  automatically (the saved password is re-verified against the server); anyone
  without one — including people who entered before the password feature was
  added — is sent to the lock screen first.
- If left empty (`SITE_PASSWORD` unset), the site is fully open.

### Twitch video player (optional)

Works out of the box with no setup: after picking a Twitch channel, a **Show
Video** button appears below the chat; it builds the player at the top (same
layout as the Twitch mobile app), and **Hide Video** tears it down entirely to
save bandwidth and resources.

> **Hosting:** the Twitch player requires your domain to be registered as an
> **Embed domain** on Twitch. For custom domains, register them at
> https://dashboard.twitch.tv/settings → Embed. `localhost` works automatically
> for development.

#### How ads work in the embed (important, please read)

The official Twitch embed player (`player.twitch.tv`) does **not** accept an
OAuth token via query parameters or any API. This is a hard platform limitation,
not a bug. As a result:

- **Ad suppression in the embed depends solely on the VIEWER being logged into
  `twitch.tv` in their own browser** (their session cookies), as a subscriber of
  the channel or a Twitch Turbo user. It is entirely independent of any token
  this site holds.
- Our Twitch OAuth token (scopes `chat:read chat:edit user:read:chat`) only
  authenticates IRC for **sending chat messages** under your name. It **cannot**
  make the embed ad-free. There is no honest way to pass it to the player.

So the two modes behave exactly as you described:

- **Authenticated (connected) user:** the embed plays normally. If that viewer
  is *also* logged into `twitch.tv` as a subscriber / Turbo, Twitch serves them
  the ad-free stream variant automatically — because of *their* browser session,
  not because of our token.
- **Guest (not connected):** the embed plays normally too, but Twitch shows the
  ordinary ads it imposes on non-subscribers.

The UI reflects this honestly: the composer status shows
`Chatting as @<login>` for connected users and
`Anonymous guest (read only) — embed may show ads` for guests. We do not pretend
the token removes ads, because it does not.

### Connecting your Twitch account to chat (optional)

Reading works anonymously with no setup. Chatting under your own account name,
however, requires registering an application on Twitch:

1. Go to https://dev.twitch.tv/console and create an application.
2. Copy the **Client ID** and create a **Client Secret**.
3. In the application settings, add an **OAuth Redirect URL**:
   - Locally: `http://localhost:3000/auth/twitch/callback`
   - For deployment: `https://your-domain.com/auth/twitch/callback`
4. Put the values in `.env`:

```env
TWITCH_CLIENT_ID=your_id
TWITCH_CLIENT_SECRET=your_secret
TWITCH_REDIRECT_URI=http://localhost:3000/auth/twitch/callback
```

> `TWITCH_REDIRECT_URI` is optional; if left empty it is built automatically from
> the request host. But behind a proxy / HTTPS it is best to set it manually so
> it matches what you registered in the Twitch Console.

A **Connect Twitch to chat** button then appears below the chat. Clicking it
opens a one-time Twitch authorization page, after which the token is persisted
in the browser; on later visits you re-enter with your account automatically
without re-authorizing. Anyone who does not connect their account stays
anonymous and read-only.

## Architecture (how the connection survives on mobile)

Mobile browsers freeze and drop their connections (WebSockets) when you switch
to other apps — this is OS behavior and cannot be prevented from inside the
browser. So the logic is split like this:

- **`server.js` (Node server):** this is what connects to Twitch, Kick, TikTok,
  and YouTube and keeps the connections open 24/7 (Node on a computer/server
  does not freeze like a phone). It stores the last **200 messages** per session
  and sends them to every new/reconnecting client (a snapshot) so no message is
  lost while you are away. It also reconnects to each platform automatically on
  any drop. When the last client leaves, the session is not destroyed
  immediately: it gets a grace period (30 minutes) during which it stays alive
  and keeps capturing messages; if any client returns the period is cancelled,
  otherwise it is destroyed along with its platform connections.
- **`public/app.js` (browser):** connects only to your server over a single
  WebSocket at `/ws`. On returning to the page from the background
  (`visibilitychange`) or on a drop, it reconnects automatically (exponential
  backoff up to 10 seconds), then receives the snapshot and recovers the
  messages it missed.

Each session (a Twitch + Kick + TikTok + YouTube combination) is independent
with its own clients and feed, and stays alive for 30 minutes after the last
client leaves before being destroyed automatically.

## How each platform works

- **Twitch:** the server connects to the public chat server over WebSocket
  (`wss://irc-ws.chat.twitch.tv`) as an anonymous viewer for read-only access
  (no login or API key). This is the standard method used by external chat
  viewers.
- **Kick:** there is no official read API without login, so the server connects
  to the same (Pusher) WebSocket the browser uses on kick.com. Unofficial, and
  may break if Kick changes something on their end.
- **TikTok:** there is no official read API. The server uses the
  `tiktok-live-connector` library, which connects to TikTok's internal Webcast
  service over WebSocket. Because TikTok requires a cryptographic signature for
  each connection, the library relies on the **Euler Stream** service as a Sign
  Server (the free Community plan is enough for personal use). Unofficial and
  depends on a third party (Euler Stream) for signing.
- **YouTube:** there is no practical read API for continuous reading (the
  official API costs 5 units/read and blows the free quota within an hour). So
  the server uses `youtubei.js` (LuanRT), which talks to **InnerTube** — the
  same internal API the YouTube browser uses. No key, no quota, no limits.
  Unofficial and depends on InnerTube's interface stability.

### If the Kick chat stops

Most likely Kick changed their Pusher "app key". To fix it:

1. Open kick.com on any live channel.
2. Open DevTools → Network tab → filter by `pusher`.
3. You will see a URL like `wss://ws-us2.pusher.com/app/XXXXXXX?...`.
4. Copy the value after `/app/` and replace `KICK_PUSHER_APP_KEY` in
   `server.js`.

## Enabling TikTok

TikTok needs an **Euler Stream** API key to sign connections (it will not work
without it):

1. Create a free account on [eulerstream.com](https://www.eulerstream.com)
   (Community plan: 2,500 requests/day, enough for personal use).
2. Copy the API key from the dashboard.
3. Start the server with the environment variable:

   ```bash
   TIKTOK_SIGN_API_KEY="your_key_here" npm start
   ```

Without the key, TikTok shows an `error` status with a message explaining the
key is required, while Twitch, Kick, and YouTube keep working normally.

> **Note:** TikTok only shows chat while a stream is live. If the account is not
> live right now, you get a "not live" message instead of a silent failure.

## YouTube

YouTube needs no setup — it works out of the box with no key. The only
difference is that the input is a **live stream URL** or **video id** (not a
username):

- `https://www.youtube.com/watch?v=XXXXXXXXXXX` ✅
- `https://youtu.be/XXXXXXXXXXX` ✅
- `https://www.youtube.com/live/XXXXXXXXXXX` ✅
- `XXXXXXXXXXX` (video id only, 11 chars) ✅

> **Notes:**
> - YouTube ties chat to a **specific stream**, not an account; so the stream
>   must be **live right now**. If it has ended, you get a "YouTube stream
>   ended" message.
> - The library retries automatically on transient errors (up to 10 times).
>   When a stream truly ends it stops and does not retry pointlessly.

## Notes

- Entering just one username (Twitch, Kick, TikTok, or YouTube) is enough and
  the page runs with a single feed.
- If an account has never streamed or does not exist, a clear error message
  appears in the feed instead of a silent failure.
- `server.js` is both the proxy layer and the actual bridge to the platforms —
  responsible for keeping connections alive, storing messages, and reconnecting.
