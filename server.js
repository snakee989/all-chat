// Load environment variables from .env (if present) before anything else.
// This keeps secrets (site password, Twitch/TikTok keys) in the .env file
// instead of passing them on the command line (where they show up in the
// process list: ps/Task Manager).
require("dotenv").config();

const express = require("express");
const path = require("path");
const crypto = require("crypto");
const pino = require("pino");
const rateLimit = require("express-rate-limit");
const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");
const {
  TikTokLiveConnection,
  WebcastEvent,
  ControlEvent,
  InvalidUniqueIdError,
  UserOfflineError,
} = require("tiktok-live-connector");
const { Innertube, UniversalCache, YTNodes } = require("youtubei.js");

const app = express();
const PORT = process.env.PORT || 3000;
// Trust proxy headers (Cloudflare/nginx) so X-Forwarded-Proto/Host are
// forwarded correctly and the server builds HTTPS URLs matching what the
// browser sees.
app.set("trust proxy", true);

// Structured logger (pino). In production the logs are JSON lines to stdout;
// in development (NODE_ENV !== production) pino.pretty() colorizes them for
// readability in the terminal. LOG_LEVEL (default "info") controls verbosity.
const log = pino({
  level: process.env.LOG_LEVEL || "info",
  ...(process.env.NODE_ENV !== "production"
    ? { transport: { target: "pino-pretty", options: { colorize: true, translateTime: "SYS:HH:MM:ss" } } }
    : {}),
});

// HTTP + WebSocket server on the same port
const server = http.createServer(app);
// maxPayload caps inbound WS frames so a malicious client cannot exhaust
// memory by sending huge messages. 64 KiB is generous for our tiny JSON
// payloads (auth / join / send).
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 65536 });

// Basic security headers (defense-in-depth). Registered BEFORE express.static
// so they apply to static file responses too. The CSP is deliberately
// permissive enough to allow the embedded Twitch player and Google Fonts, but
// forbids inline scripts/styles and `eval`, which is the main win: any future
// XSS cannot run inline code or exfiltrate the stored Twitch token / site
// password from localStorage. If a new external resource breaks under CSP,
// relax the relevant directive here.
const CSP = [
  "default-src 'self'",
  // No inline scripts: app.js is same-origin and there are no inline handlers.
  "script-src 'self'",
  // style-src allows Google Fonts; 'unsafe-inline' is needed because the page
  // sets a few element.style.* properties and the Google Fonts stylesheet pulls
  // in @font-face (CSS is not script, so this is low risk).
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src 'self' https://fonts.gstatic.com",
  // Emotes come from Twitch's static-cdn plus unknown Kick/YouTube CDNs, so
  // allow https: for images broadly. data: covers any inlined SVG fallback.
  "img-src 'self' data: https:",
  // WebSocket back to our own origin (wss/wss). ws: is included so local dev
  // over plain http still works.
  "connect-src 'self' wss: ws:",
  // The Twitch player iframe.
  "frame-src https://player.twitch.tv",
  "media-src 'self' https:",
  // Mitigate clickjacking; reinforces X-Frame-Options below.
  "frame-ancestors 'self'",
  "base-uri 'self'",
  "form-action 'self'",
].join("; ");
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "SAMEORIGIN");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Content-Security-Policy", CSP);
  next();
});

app.use(express.static(path.join(__dirname, "public")));

// How many messages we keep per session (to send to the client on reconnect)
const BACKLOG_LIMIT = 200;
// Wait time before reconnecting to a platform after a drop
const PLATFORM_RECONNECT_MS = 3000;
// Maximum reconnect backoff delay. Reconnect attempts use exponential backoff
// (PLATFORM_RECONNECT_MS * 2^attempt) capped here, so a persistently-failing
// platform backs off gradually instead of hammering every 3s forever.
const PLATFORM_RECONNECT_MAX_MS = 60 * 1000;
// Computes an exponential-backoff delay for a reconnect attempt number.
function reconnectBackoff(attempt) {
  const d = PLATFORM_RECONNECT_MS * Math.pow(2, attempt);
  return Math.min(d, PLATFORM_RECONNECT_MAX_MS);
}
// Grace period after the last client leaves before actually destroying the
// session (cancelled if a client returns). Acts as the FALLBACK when we cannot
// determine the stream's status (no Twitch/Kick configured, transient API
// failure, or the stream is still live). When the stream is confirmed ended
// (see STREAM_ENDED_CONFIRM_MS), the shorter STREAM_ENDED_GRACE_MS is used
// instead. Kept generous deliberately: the aggregate stats (subs/gifts/bits)
// are tied to the session's lifetime, and a stream runs for hours — a viewer
// who closes the browser for a meeting or a nap should not lose the stream's
// running totals. Platform connections stay open (and keep capturing events)
// throughout this window.
const SESSION_GRACE_MS = 60 * 60 * 1000; // 1 hour (fallback)
// How long BOTH Twitch and Kick must read "offline" (definitively ended) in a
// row before we conclude the stream has ended. A debounce against transient
// API hiccups and reconnect flicker: "unknown" never counts as offline, only
// an explicit offline response from Helix /streams or Kick's channel API does.
const STREAM_ENDED_CONFIRM_MS = 3 * 60 * 1000; // 3 minutes
// Shorter grace window applied once the stream is confirmed ended. Combined
// with the confirm window above, a clientless session whose stream ended is
// destroyed in ~5 minutes rather than the full SESSION_GRACE_MS.
const STREAM_ENDED_GRACE_MS = 2 * 60 * 1000; // 2 minutes
// Precautionary periodic sweep: closes any stuck session that the grace timer
// did not run on for any reason. Must exceed SESSION_GRACE_MS so it only ever
// catches sessions already past their grace window.
const SWEEP_INTERVAL_MS = 8 * 60 * 60 * 1000; // every 8 hours
// How long to remember a community-gift batch so the per-recipient `subgift`
// USERNOTICEs that follow it are not double-counted. Twitch sends those
// follow-ups within seconds, so 5 minutes is a generous safety margin.
const COMMUNITY_GIFT_TRACKER_TTL_MS = 5 * 60 * 1000; // 5 minutes
// How often to refresh the live viewer count for Twitch (Helix /streams) and
// Kick (channel API). Neither platform pushes this over its chat socket, so we
// poll. 60s stays well under Twitch Helix's per-token rate limit and Kick's
// tolerance, while keeping the number responsive enough to track a stream.
const VIEWER_POLL_MS = 60 * 1000; // 1 minute
// Maximum number of concurrent sessions. Each session opens platform
// connections (Twitch/Kick/TikTok/YouTube WebSocket + IRC), so an unbounded
// count lets a malicious client exhaust server resources by creating unlimited
// sessions with random channel names. Tunable via MAX_SESSIONS env var.
const MAX_SESSIONS = Number(process.env.MAX_SESSIONS) > 0 ? Number(process.env.MAX_SESSIONS) : 50;
// Maximum concurrent WebSocket connections per client IP. Each open socket
// holds memory and a file descriptor; without a per-IP cap, an attacker can
// bypass the per-connection WS rate limiter simply by opening many sockets
// (slow-loris-style). Tunable via MAX_WS_PER_IP env var.
const MAX_WS_PER_IP = Number(process.env.MAX_WS_PER_IP) > 0 ? Number(process.env.MAX_WS_PER_IP) : 30;
// How long an unauthenticated client WebSocket may stay open before we close
// it. Prevents idle auth-pending sockets from piling up when the site is
// password-protected. The legitimate client sends `auth` within milliseconds
// of connecting, so 10s is a generous upper bound.
const WS_AUTH_TIMEOUT_MS = 10 * 1000;
const KICK_PUSHER_APP_KEY = "32cbd69e4b950bf97679";
// Kick Pusher event names. Kick emits these as PHP-style event strings
// (App\Events\...) over the chatrooms.{id}.v2 channel. We surface the ones that
// matter for an "alerts" feed: subscriptions, gifted subs, follows, and raids.
const KICK_EVENT = {
  SUBSCRIPTION: "App\\Events\\SubscriptionEvent",
  GIFTED_SUBS: "App\\Events\\GiftedSubscriptionsEvent",
  // Kick has shipped two different follow event names; we accept both.
  FOLLOW: "App\\Events\\FollowEvent",
  FOLLOW_LEGACY: "App\\Events\\FollowersUpdatedEvent",
  RAID: "App\\Events\\HostEvent",
};
// Euler Stream API key for signing TikTok connections. Put it in the
// TIKTOK_SIGN_API_KEY env variable (the free Community plan is enough:
// https://www.eulerstream.com). Required for TikTok to work.
const TIKTOK_SIGN_API_KEY = process.env.TIKTOK_SIGN_API_KEY || "";

// Site password (gate). Required from every visitor before they can connect to
// the chat/WS. Put it in the SITE_PASSWORD env variable. If left empty, the
// site stays open. Comparison is constant-time to avoid timing leaks, and we
// compare after trimming whitespace.
const SITE_PASSWORD = (process.env.SITE_PASSWORD || "").trim();

// Twitch OAuth settings (optional, for chatting under the user's own name).
// Register an app at https://dev.twitch.tv/console and put the Client ID /
// Secret here.
const TWITCH_CLIENT_ID = (process.env.TWITCH_CLIENT_ID || "").trim();
const TWITCH_CLIENT_SECRET = (process.env.TWITCH_CLIENT_SECRET || "").trim();
// Redirect URI must match exactly what you registered in the Twitch Console.
// Default: /auth/twitch/callback on the same host. For a server behind https,
// set TWITCH_REDIRECT_URI manually (e.g. https://my-domain.com/auth/twitch/callback).
const TWITCH_REDIRECT_URI =
  (process.env.TWITCH_REDIRECT_URI || "").trim();

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ----------------------------------------------------------------------
// Rate limiting (express-rate-limit). We define per-endpoint limiters so that
// abuse of one route (e.g. password guessing on /api/auth) does not starve
// unrelated traffic. Behind a proxy, `trust proxy` is already enabled above so
// the real client IP is read from X-Forwarded-For.
// ----------------------------------------------------------------------

// Shared handler: log rate-limit hits so operators can see abuse patterns.
function rateLimitHandler(req, res) {
  log.warn({ ip: req.ip, path: req.path }, "rate limit exceeded");
  res.status(429).json({ error: "Too many requests. Please slow down." });
}

// Password-guessing protection on /api/auth: 10 attempts per 15 minutes per IP.
const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  handler: rateLimitHandler,
});

// Kick chatroom lookup: 30 requests per minute (user typing names one at a time).
const kickLookupLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  handler: rateLimitHandler,
});

// OAuth start + callback: 10 per minute (the flow involves redirects, so this
// is generous for a single user but blocks automated scanning).
const oauthLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  handler: rateLimitHandler,
});

// General API limiter for the remaining /api/* and /auth/* routes (token
// pickup, oauth-error, auth-status): 60 per minute.
const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  handler: rateLimitHandler,
});

// Constant-time string comparison to reduce timing leaks when comparing
// passwords/tokens. Uses crypto.timingSafeEqual over UTF-8 bytes; when the two
// inputs differ in length the comparison is guaranteed false, and we still
// perform a dummy timingSafeEqual so the timing does not leak the length
// mismatch.
function timingSafeEqualStr(a, b) {
  const ab = Buffer.from(String(a), "utf8");
  const bb = Buffer.from(String(b), "utf8");
  if (ab.length !== bb.length) {
    // Compare bb against itself to keep the timing constant regardless of which
    // branch we take, then return false.
    if (bb.length > 0) crypto.timingSafeEqual(bb, bb);
    return false;
  }
  if (ab.length === 0) return true;
  return crypto.timingSafeEqual(ab, bb);
}

// Is site protection enabled? (Site password)
function siteProtected() {
  return SITE_PASSWORD.length > 0;
}

// Strips IRC line-delimiter and NUL characters from a value before it is
// interpolated into an IRC command string (JOIN / PRIVMSG / NICK / PASS).
// Without this, a malicious client can embed \r\n in a username or message to
// inject arbitrary IRC commands (QUIT, PRIVMSG to other channels, etc.).
function sanitizeIrcParam(value) {
  return String(value).replace(/[\r\n\0]/g, "");
}

// Wraps fetch() with an AbortController timeout so a slow/unresponsive remote
// API can never hang the server indefinitely. Default 10 s.
const FETCH_TIMEOUT_MS = 10000;
function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...options, signal: controller.signal }).finally(() =>
    clearTimeout(timer)
  );
}

// Sends data to a client WebSocket only if it is still open. In async callbacks
// (e.g. after an awaited fetch) the client may have disconnected by the time we
// respond — calling ws.send() on a closed socket throws and would crash the
// handler with an unhandled exception.
function safeSend(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(data);
    } catch {
      /* socket closed between the check and the send — ignore */
    }
  }
}

// A single shared Innertube client for all YouTube connections (not created per
// session). We initialize it once; anyone who needs it awaits this promise.
let ytClientPromise = null;
function getYoutubeClient() {
  if (!ytClientPromise) {
    ytClientPromise = Innertube.create({
      // Simple cache to reduce repeated requests for Innertube keys
      cache: new UniversalCache(false),
    }).catch((err) => {
      ytClientPromise = null; // allow retrying later
      throw err;
    });
  }
  return ytClientPromise;
}

// ----------------------------------------------------------------------
// SessionManager: each session (twitch + kick together) has its own state.
// A session is not destroyed the moment the last client leaves; it gets a
// grace period (SESSION_GRACE_MS) during which it stays alive. If any client
// returns during that window, the teardown is cancelled and work resumes.
// Otherwise it is destroyed.
// ----------------------------------------------------------------------

const sessions = new Map(); // key "tw=xxx|kk=yyy|tt=zzz|yt=www" -> Session

function sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput) {
  return `tw=${twitchUser || ""}|kk=${kickUser || ""}|tt=${tiktokUser || ""}|yt=${youtubeInput || ""}`;
}

class Session {
  constructor(twitchUser, kickUser, tiktokUser, youtubeInput) {
    this.twitchUser = twitchUser || null;
    this.kickUser = kickUser || null; // comma-joined string for keys/snapshot (kept for compatibility)
    // Kick supports up to 3 channels per session. The FIRST is "primary" (its
    // messages have no shared badge and DO count in stats); the rest are
    // "mirrors" (badged "↪ name", excluded from stats). Order is significant.
    this.kickUsers = kickUser ? String(kickUser).split(",").map((s) => s.trim()).filter(Boolean) : [];
    this.tiktokUser = tiktokUser || null;
    this.youtubeInput = youtubeInput || null; // YouTube URL/video id (parsed later)
    this.key = sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput);
    this.clients = new Set(); // browsers connected to this session
    this.backlog = []; // recent messages (sent to every new/reconnecting client)
    this.seq = 0; // monotonic counter per message (key for resume on reconnect)
    // Aggregate stats for the stat panels in the header. Each entry carries the
    // platform it came from ("tw"/"kk") so the UI can badge every name. These
    // are rebuilt only from events seen during this session's lifetime — they
    // reset when the session is destroyed (after the grace period).
    this.stats = {
      subs: new Map(), // key: name|platform -> { name, platform, months, count }
      gifts: new Map(), // key: name|platform -> { name, platform, count }
      bits: new Map(), // key: name|platform -> { name, platform, total }
    };
    this.statsTimer = null; // debounce timer for stats broadcasts
    // Tracks recent community-gift batches so the individual `subgift`
    // USERNOTICEs Twitch sends right after a `submysterygift` don't get
    // double-counted on top of the batch total. Key: giver|platform ->
    // { remaining, ts }. `remaining` is consumed one per follow-up subgift.
    this.recentCommunityGifts = new Map();
    this.platforms = {
      twitch: {
        socket: null, status: "connecting", reconnecting: false, timer: null, error: null,
        viewers: null, viewerTimer: null, attempt: 0,
        // Stream-ended detection (server.js docs above). `offlineState` is the
        // most recent definitive poll result; `offlineSince` is when it became
        // "offline" (nulled on any other state). "unknown" is treated as not
        // offline so a transient API failure can never trigger a teardown.
        offlineState: "unknown", offlineSince: null,
      },
      kick: {
        status: "connecting", // aggregate status derived from per-channel conns
        error: null,
        conns: new Map(), // channelName -> conn (see connectKickChannel for the full field list)
      },
      tiktok: { connection: null, status: "connecting", reconnecting: false, timer: null, error: null, attempt: 0 },
      youtube: { livechat: null, status: "connecting", reconnecting: false, timer: null, error: null, videoId: null, attempt: 0 },
    };
    this.graceTimer = null; // grace-period timer after the last client leaves
    this.emptySince = null; // when the session became clientless (used by the safety net)
    this.sendConnection = null; // authenticated IRC connection for writing to Twitch (created on demand)
    // True once Twitch+Kick are both confirmed offline for STREAM_ENDED_CONFIRM_MS.
    // Drives scheduleExpiry() to use the shorter STREAM_ENDED_GRACE_MS so a
    // clientless session whose stream ended is reaped in minutes, not the full
    // SESSION_GRACE_MS. Updated by evaluateStreamEnded(), called from each poll.
    this.streamEnded = false;
    this.closed = false;
  }

  // Add a client (browser) to the session.
  // lastId: the last id the client previously received (if any) so we only
  // send what they missed.
  addClient(ws, lastId) {
    this.clients.add(ws);
    // If the session was in its grace period, cancel the deferred teardown and
    // return to normal state
    this.cancelExpiry();
    this.emptySince = null;

    let backlog;
    if (typeof lastId === "number" && lastId > 0) {
      // Resume: is the gap still within the buffer?
      const oldest = this.backlog.length ? this.backlog[0].id : 0;
      if (lastId >= oldest) {
        // Gap fits inside the backlog — send only the messages after lastId
        const idx = this.backlog.findIndex((m) => m.id > lastId);
        backlog = idx === -1 ? [] : this.backlog.slice(idx);
      } else {
        // Gap is larger than the buffer — send the latest 200 available; we do
        // not attempt precise catch-up
        backlog = this.backlog;
      }
    } else {
      // Brand-new connection: send the whole available backlog
      backlog = this.backlog;
    }

    // Send the last id we reached so the client stores it as the next resume point
    ws.send(
      JSON.stringify({
        type: "snapshot",
        twitchUser: this.twitchUser,
        kickUser: this.kickUser,
        tiktokUser: this.tiktokUser,
        youtubeInput: this.youtubeInput,
        twitchStatus: this.platforms.twitch.status,
        kickStatus: this.platforms.kick.status,
        tiktokStatus: this.platforms.tiktok.status,
        youtubeStatus: this.platforms.youtube.status,
        twitchError: this.platforms.twitch.error,
        kickError: this.platforms.kick.error,
        tiktokError: this.platforms.tiktok.error,
        youtubeError: this.platforms.youtube.error,
        // Live viewer counts (null when unknown / platform offline). For Kick
        // we send the sum across all channels so the header shows one number.
        twitchViewers: this.platforms.twitch.viewers,
        kickViewers: this.aggregateKickViewers(),
        backlog,
        lastId: this.seq,
        stats: this.serializeStats(),
      })
    );
  }

  removeClient(ws) {
    this.clients.delete(ws);
    // If nobody is left, start a grace period instead of immediate teardown:
    // if a client returns the session stays alive and keeps capturing messages
    // during it; otherwise it is destroyed after the period ends.
    if (this.clients.size === 0) {
      this.emptySince = Date.now();
      this.scheduleExpiry();
    }
  }

  // Starts the grace timer to destroy the session if nobody returns. The
  // window is dynamic: the full SESSION_GRACE_MS while the stream is still live
  // (or its status is unknown), but the shorter STREAM_ENDED_GRACE_MS once the
  // stream is confirmed ended via evaluateStreamEnded(). This reuses the same
  // single destroy path — we only vary the delay.
  scheduleExpiry() {
    if (this.graceTimer || this.closed) return; // do not schedule twice
    const ms = this.streamEnded ? STREAM_ENDED_GRACE_MS : SESSION_GRACE_MS;
    this.graceTimer = setTimeout(() => {
      this.graceTimer = null;
      // Final check: if a client returned before the timer fired, keep the session
      if (this.clients.size > 0 || this.closed) return;
      this.destroy();
      sessions.delete(this.key);
    }, ms);
  }

  // Cancels the grace period when a client returns (called from addClient).
  cancelExpiry() {
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }
  }

  // Decides whether the session's stream has ended based on the latest offline
  // state of each relevant platform, and re-arms the grace timer if the
  // decision flips while the session is clientless. Called after every Twitch
  // and Kick viewer poll (pollTwitchViewers / pollKickViewers).
  //
  // "Ended" requires that EVERY relevant platform has read a definitive
  // "offline" (not "unknown") for at least STREAM_ENDED_CONFIRM_MS without
  // interruption. Relevant platforms are Twitch (if a channel is configured)
  // and Kick (if any channels are configured); a platform that isn't in this
  // session is ignored rather than counted as offline. Sessions with neither
  // configured (e.g. TikTok/YouTube only) never set streamEnded and fall back
  // to the plain grace window.
  evaluateStreamEnded() {
    if (this.closed) return;
    const now = Date.now();
    const grace = STREAM_ENDED_CONFIRM_MS;

    // Collect the relevant platform offline states. If none are relevant this
    // session can never declare "ended" via this mechanism — bail out early.
    const states = [];
    if (this.twitchUser) {
      const st = this.platforms.twitch;
      states.push(st);
    }
    if (this.platforms.kick.conns.size > 0) {
      for (const conn of this.platforms.kick.conns.values()) states.push(conn);
    }
    if (states.length === 0) return;

    // All relevant platforms must be confirmed offline past the debounce window.
    const ended = states.every((s) =>
      s.offlineState === "offline" && s.offlineSince && now - s.offlineSince >= grace
    );

    if (ended !== this.streamEnded) {
      this.streamEnded = ended;
      log.info({ session: this.key, streamEnded: ended }, "stream-ended state changed");
      // If clientless with a grace timer armed, re-arm it with the now-correct
      // window so a just-ended stream is reaped promptly (or a revived stream
      // gets the full window again). A live session is never destroyed here —
      // the grace timer only runs when clients.size === 0 (see removeClient).
      if (this.clients.size === 0 && this.graceTimer) {
        this.cancelExpiry();
        this.scheduleExpiry();
      }
    }
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const ws of this.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    }
  }

  // New chat message from one of the platforms
  // emotes: optional array of {start,end,id} to replace parts of the content
  // with emote images
  // bits: optional number — Twitch cheer amount (flags the message as a bits message)
  // sharedFrom: optional string — origin channel for a mirrored message. This
  //   covers BOTH Twitch Shared Chat (a message duplicated from a linked room)
  //   and additional Kick channels in a multi-channel session (everything past
  //   the primary). Such messages get a "↪ name" badge and are EXCLUDED from
  //   stats — only the primary channel contributes to header totals.
  pushChat(platform, username, content, color, emotes, bits, sharedFrom) {
    const msg = {
      id: ++this.seq, // monotonic key for resume and de-duplication
      type: "chat",
      platform,
      username,
      content,
      color,
      ts: Date.now(),
    };
    if (Array.isArray(emotes) && emotes.length) msg.emotes = emotes;
    if (Number.isFinite(bits) && bits > 0) msg.bits = bits;
    if (sharedFrom) msg.sharedFrom = sharedFrom;
    this.backlog.push(msg);
    while (this.backlog.length > BACKLOG_LIMIT) this.backlog.shift();
    this.broadcast(msg);
    if (Number.isFinite(bits) && bits > 0) {
      // Mirrored messages don't count toward stats — only the primary channel does.
      if (!sharedFrom) this.recordBits(platform, username, bits);
      this.scheduleStatsBroadcast();
    }
  }

  // New platform event (Twitch sub/resub/gift/raid). Events live in the same
  // backlog as chat messages so they survive a client reconnect/resume, but
  // carry type:"event" so the client renders them as a distinct styled row.
  // `info` is the structured event payload from parseUsernotice().
  pushEvent(platform, kind, who, text, info) {
    const msg = {
      id: ++this.seq,
      type: "event",
      platform,
      kind, // sub | resub | gift | communitygift | raid
      who, // display name of the actor
      text, // short human-readable summary (already built by the caller)
      ts: Date.now(),
    };
    if (info && info.message) {
      msg.message = info.message; // optional attached user message (resub/gift)
    }
    if (info && info.sharedFrom) {
      msg.sharedFrom = info.sharedFrom; // mirrored event origin (Twitch shared / extra Kick channel)
    }
    this.backlog.push(msg);
    while (this.backlog.length > BACKLOG_LIMIT) this.backlog.shift();
    this.broadcast(msg);
    // Mirrored events don't count toward stats — only the primary channel does.
    if (!(info && info.sharedFrom)) this.recordEvent(platform, kind, who, info);
    this.scheduleStatsBroadcast();
  }

  // Updates the aggregate stats for the header panels. Mirrors the event kinds
  // we surface in the feed: sub/resub -> subs, gift/communitygift -> gifts
  // (count of subs gifted). Follows and raids are intentionally not counted
  // (they have no persistent "actor total" the way the others do).
  recordEvent(platform, kind, who, info) {
    if (!who || !kind) return;
    const key = `${who}|${platform}`;
    switch (kind) {
      case "sub":
      case "resub": {
        const months = info && info.months ? info.months : null;
        const prev = this.stats.subs.get(key);
        // Keep the highest month count seen (cumulative) so a resub doesn't
        // overwrite a higher prior value with a lower one.
        const bestMonths = prev && prev.months != null && (months == null || prev.months >= months)
          ? prev.months
          : (months != null ? months : (prev ? prev.months : null));
        this.stats.subs.set(key, { name: who, platform, months: bestMonths });
        break;
      }
      case "gift": {
        // A single gifted sub (Twitch `subgift`). Twitch fires one
        // `submysterygift` (batch) followed by one `subgift` per recipient —
        // so the batch was already counted in `communitygift` below. Consume
        // one slot from the recent batch to avoid double-counting; only count
        // it as 1 if no matching batch is found (a standalone direct gift).
        const recent = this.recentCommunityGifts.get(key);
        if (recent && recent.remaining > 0) {
          recent.remaining -= 1;
          if (recent.remaining <= 0) this.recentCommunityGifts.delete(key);
          break; // already counted via the communitygift batch
        }
        const prevG = this.stats.gifts.get(key) || { name: who, platform, count: 0 };
        prevG.count += 1;
        this.stats.gifts.set(key, prevG);
        break;
      }
      case "communitygift": {
        // A community gift batch (Twitch `submysterygift` / Kick gift event).
        // Twitch follows this with one `subgift` per recipient; we track the
        // batch here so those follow-ups are not double-counted as case "gift".
        const n = info && Number.isFinite(info.count) && info.count > 0 ? info.count : 1;
        const prev = this.stats.gifts.get(key) || { name: who, platform, count: 0 };
        prev.count += n;
        this.stats.gifts.set(key, prev);
        this.recentCommunityGifts.set(key, { remaining: n, ts: Date.now() });
        break;
      }
      default:
        break; // follow / raid etc. — not tracked in stats
    }
  }

  // Accumulates bits per user across multiple cheers.
  recordBits(platform, username, bits) {
    if (!username || !Number.isFinite(bits) || bits <= 0) return;
    const key = `${username}|${platform}`;
    const prev = this.stats.bits.get(key) || { name: username, platform, total: 0 };
    prev.total += bits;
    this.stats.bits.set(key, prev);
  }

  // Clears all aggregate stats (subs/gifts/bits) and broadcasts the empty
  // state immediately. Triggered by an explicit client "reset_stats" command
  // (the header reset button). Broadcasts right away rather than going through
  // the debounced `scheduleStatsBroadcast` because this is a deliberate user
  // action, not part of an event storm. `recentCommunityGifts` is also cleared
  // so an in-flight community batch isn't mis-counted against the fresh totals.
  resetStats() {
    if (this.closed) return;
    this.stats.subs.clear();
    this.stats.gifts.clear();
    this.stats.bits.clear();
    this.recentCommunityGifts.clear();
    this.broadcast({ type: "stats", stats: this.serializeStats() });
  }

  // Debounced broadcast: if many events arrive in quick succession (e.g. a
  // sub bomb), we coalesce them into a single stats update so we don't flood
  // the clients with one message per gift.
  scheduleStatsBroadcast() {
    if (this.statsTimer || this.closed) return;
    this.statsTimer = setTimeout(() => {
      this.statsTimer = null;
      if (this.closed) return;
      this.broadcast({ type: "stats", stats: this.serializeStats() });
    }, 500);
    // Prune stale community-gift trackers: if the individual `subgift`
    // follow-ups never arrived (e.g. some recipients were already subbed),
    // drop the entry after a grace window so the Map can't grow unbounded.
    const now = Date.now();
    for (const [k, v] of this.recentCommunityGifts) {
      if (now - v.ts > COMMUNITY_GIFT_TRACKER_TTL_MS) this.recentCommunityGifts.delete(k);
    }
  }

  // Serializes the stats Maps into plain arrays for JSON transport. Each list
  // is sorted descending by its metric (gifts by count, bits by total) so the
  // client can render directly without re-sorting.
  serializeStats() {
    const subs = Array.from(this.stats.subs.values()).sort((a, b) => (b.months || 0) - (a.months || 0));
    const gifts = Array.from(this.stats.gifts.values()).sort((a, b) => b.count - a.count);
    const bits = Array.from(this.stats.bits.values()).sort((a, b) => b.total - a.total);
    return { subs, gifts, bits };
  }

  setPlatformStatus(platform, status, error) {
    const st = this.platforms[platform];
    st.status = status;
    if (error !== undefined) st.error = error || null;
    // When a platform leaves the "live" state we drop its viewer count so the
    // header doesn't show a stale number next to an "error"/"reconnecting" pill.
    // It gets repopulated by the next poll once the connection is healthy again.
    if (status !== "live") {
      if (platform === "twitch") {
        st.viewers = null;
      } else if (platform === "kick") {
        for (const conn of this.platforms.kick.conns.values()) conn.viewers = null;
      }
    }
    this.broadcast({ type: "status", platform, status, error: st.error });
    if (status !== "live") this.broadcastViewers(platform);
  }

  // Sets the live viewer count for a platform (Twitch) or a single Kick channel
  // and broadcasts the updated number. `count` is null to clear. The broadcast is
  // a dedicated `viewers` message (kept separate from `status` so the client can
  // update the number without touching the status text, and to avoid spamming
  // status broadcasts every poll cycle).
  setViewers(platform, count, kickChannelName) {
    if (platform === "twitch") {
      this.platforms.twitch.viewers = Number.isFinite(count) && count >= 0 ? count : null;
    } else if (platform === "kick") {
      const conn = this.platforms.kick.conns.get(kickChannelName);
      if (!conn) return;
      conn.viewers = Number.isFinite(count) && count >= 0 ? count : null;
    } else {
      return;
    }
    this.broadcastViewers(platform);
  }

  // Broadcasts the current viewer count for a platform. For Kick we send the
  // sum across all channels (the header shows one aggregate number per pill).
  broadcastViewers(platform) {
    if (platform === "twitch") {
      this.broadcast({ type: "viewers", platform: "twitch", count: this.platforms.twitch.viewers });
    } else if (platform === "kick") {
      this.broadcast({ type: "viewers", platform: "kick", count: this.aggregateKickViewers() });
    }
  }

  // Sum of viewer counts across all Kick channels in this session. Channels
  // whose count is unknown (null) contribute 0.
  aggregateKickViewers() {
    let sum = 0;
    let any = false;
    for (const conn of this.platforms.kick.conns.values()) {
      if (conn.viewers != null) {
        sum += conn.viewers;
        any = true;
      }
    }
    return any ? sum : null;
  }

  destroy() {
    if (this.closed) return;
    this.closed = true;
    this.cancelExpiry();
    this.emptySince = null;
    if (this.statsTimer) {
      clearTimeout(this.statsTimer);
      this.statsTimer = null;
    }
    this.recentCommunityGifts.clear();
    teardownTwitch(this);
    teardownKick(this);
    teardownTikTok(this);
    teardownYouTube(this);
    // Close the IRC write connection if any
    if (this.sendConnection && this.sendConnection.socket) {
      try { this.sendConnection.socket.close(); } catch {}
      this.sendConnection = null;
    }
    log.info({ session: this.key, clients: this.clients.size }, "session destroyed");
  }
}

// Returns the existing session for the given channel combination, or creates a
// new one. Returns null if the session cap (MAX_SESSIONS) has been reached and
// no matching session exists — the caller must relay an error to the client.
function getOrCreateSession(twitchUser, kickUser, tiktokUser, youtubeInput) {
  const key = sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput);
  let s = sessions.get(key);
  if (!s) {
    // Enforce the concurrent-session cap. An existing session is always returned
    // (a returning client to an active session does not count against the cap);
    // only truly new sessions are blocked when the limit is full.
    if (sessions.size >= MAX_SESSIONS) {
      log.warn({ active: sessions.size, max: MAX_SESSIONS }, "session cap reached, rejecting new session");
      return null;
    }
    s = new Session(twitchUser, kickUser, tiktokUser, youtubeInput);
    sessions.set(key, s);
    log.info({ session: key, twitch: !!twitchUser, kick: !!kickUser, tiktok: !!tiktokUser, youtube: !!youtubeInput, active: sessions.size }, "session created");
    if (twitchUser) connectTwitch(s);
    if (kickUser) connectKick(s);
    if (tiktokUser) connectTikTok(s);
    if (youtubeInput) connectYouTube(s);
  }
  return s;
}

// ----------------------------------------------------------------------
// Kick username -> chatroom_id (resolved server-side, because the browser is
// blocked by CORS/Cloudflare)
// ----------------------------------------------------------------------
async function lookupKickChatroom(username) {
  const r = await fetchWithTimeout(`https://kick.com/api/v2/channels/${encodeURIComponent(username)}`, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      Accept: "application/json",
    },
  });
  if (r.status === 404) throw new Error("This user was not found on Kick");
  if (!r.ok) throw new Error(`Kick rejected the request (${r.status})`);
  const data = await r.json();
  const chatroomId = data?.chatroom?.id;
  if (!chatroomId) throw new Error("This user has no chat (they may never have streamed)");
  return chatroomId;
}

// Fetches the current viewer count for a Kick channel via the same v2 channel
// endpoint used for chatroom lookup. Kick exposes the count at two locations in
// the response (`livestream.viewer_count` when live, top-level `viewerCount`
// otherwise); we prefer the livestream value since that is the true live count.
// Returns null when the request fails or the channel is offline.
// Returns a tri-state result so the caller can distinguish a definitively-ended
// stream from a transient API failure:
//   { viewers: number, state: "live"    } — channel is broadcasting now
//   { viewers: null,   state: "offline" } — channel data resolved, but no live
//                                           broadcast (data.livestream absent)
//   { viewers: null,   state: "unknown" } — request failed/credentials missing;
//                                           must NOT be treated as offline
async function fetchKickViewerCount(username) {
  try {
    const r = await fetchWithTimeout(`https://kick.com/api/v2/channels/${encodeURIComponent(username)}`, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        Accept: "application/json",
      },
    });
    if (!r.ok) return { viewers: null, state: "unknown" };
    const data = await r.json();
    // A channel is live only when `livestream` is present and carries a real
    // viewer_count. Read the count via the field's own presence to avoid
    // Number(null)===0 falsely reporting "live with 0 viewers".
    const liveStream = data?.livestream;
    if (liveStream && liveStream.viewer_count != null) {
      const live = Number(liveStream.viewer_count);
      if (Number.isFinite(live) && live >= 0) return { viewers: live, state: "live" };
    }
    // data resolved but livestream is absent/null -> the channel is definitively
    // offline. (Kick only populates `livestream` while broadcasting.)
    return { viewers: null, state: "offline" };
  } catch (err) {
    log.warn({ err: String(err?.message || err), username }, "kick viewer count fetch failed");
    return { viewers: null, state: "unknown" };
  }
}

app.get("/api/kick-lookup/:username", kickLookupLimiter, async (req, res) => {
  const username = req.params.username.trim().toLowerCase();
  if (!username) return res.status(400).json({ error: "Empty username" });
  try {
    const chatroomId = await lookupKickChatroom(username);
    res.json({ username, chatroomId });
  } catch (err) {
    const msg = String(err.message || err);
    const status = msg.includes("not found") || msg.includes("no chat") ? 404 : 502;
    if (status === 502) log.warn({ err, username }, "kick lookup failed");
    res.status(status).json({ error: msg });
  }
});

// ----------------------------------------------------------------------
// Site password (Gate): verify the password
// ----------------------------------------------------------------------
// We return no details about why it failed, just ok/false, to prevent targeted
// guessing.
app.post("/api/auth", authLimiter, (req, res) => {
  // Never cache the auth response so a stale ok:true is never served.
  res.setHeader("Cache-Control", "no-store");
  if (!siteProtected()) {
    // Protection is disabled on the server — allow access directly
    return res.json({ ok: true, protected: false });
  }
  const password = req.body && typeof req.body.password === "string" ? req.body.password : "";
  if (timingSafeEqualStr(password.trim(), SITE_PASSWORD)) {
    return res.json({ ok: true, protected: true });
  }
  return res.status(401).json({ ok: false, protected: true, error: "Incorrect password" });
});

// Tells the client whether the site is protected (so it shows the password
// screen) before attempting.
app.get("/api/auth-status", apiLimiter, (req, res) => {
  res.json({ protected: siteProtected() });
});

// ----------------------------------------------------------------------
// Twitch OAuth (optional): lets the user chat under their own name.
// Flow:
//   1) The browser opens /auth/twitch?state=... (with a redirect URL back to us)
//   2) Twitch returns to /auth/twitch/callback?code=...
//   3) The server exchanges code for an access_token (server-to-server, secret)
//   4) We validate the token and fetch the username, then store it in a
//      short-lived cookie (one-time, httpOnly) and redirect to the home page.
//      The client picks up the token via /api/twitch-token and persists it to
//      localStorage.
// ----------------------------------------------------------------------
function buildRedirectUri(req) {
  if (TWITCH_REDIRECT_URI) return TWITCH_REDIRECT_URI;
  // Build it from the request: same host/protocol
  const proto = req.headers["x-forwarded-proto"] || (req.socket && req.socket.encrypted ? "https" : "http");
  const host = req.headers["x-forwarded-host"] || req.headers.host || `localhost:${PORT}`;
  return `${proto}://${host}/auth/twitch/callback`;
}

// OAuth start: redirect the user to Twitch to authorize the app.
app.get("/auth/twitch", oauthLimiter, (req, res) => {
  if (!TWITCH_CLIENT_ID) {
    return res.status(400).send("Twitch OAuth is not configured on the server (TWITCH_CLIENT_ID missing).");
  }
  const redirectUri = buildRedirectUri(req);
  // Generate and store the state in a short-lived HttpOnly cookie so we can
  // verify it on the callback (prevents OAuth CSRF). The client no longer owns
  // or passes the state.
  const state = cryptoRandom();
  setAuthCookie(res, "tw_oauth_state", state, 300); // 5 minutes
  const params = new URLSearchParams({
    client_id: TWITCH_CLIENT_ID,
    redirect_uri: redirectUri,
    response_type: "code",
    scope: "chat:read chat:edit user:read:chat openid",
    state,
    force_verify: "true",
  });
  res.redirect(302, `https://id.twitch.tv/oauth2/authorize?${params.toString()}`);
});

// Twitch callback: exchange code for a token, then store it in a short-lived
// cookie and redirect to the page.
app.get("/auth/twitch/callback", oauthLimiter, async (req, res) => {
  const code = req.query.code;
  if (!code) return oauthFailRedirect(res, "nocode");
  // Verify the OAuth state against the cookie set in /auth/twitch to prevent
  // CSRF (an attacker tricking a victim into completing the attacker's OAuth).
  const expectedState = readAuthCookie(req, "tw_oauth_state");
  clearAuthCookie(res, "tw_oauth_state");
  const receivedState = typeof req.query.state === "string" ? req.query.state : "";
  if (!expectedState || !receivedState || !timingSafeEqualStr(expectedState, receivedState)) {
    log.warn({ hasCookie: !!expectedState, hasQuery: !!receivedState }, "oauth state mismatch");
    return oauthFailRedirect(res, "state");
  }
  if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
    return oauthFailRedirect(res, "notconfigured");
  }
  const redirectUri = buildRedirectUri(req);

  let tokenResp;
  try {
    tokenResp = await fetchWithTimeout("https://id.twitch.tv/oauth2/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: TWITCH_CLIENT_ID,
        client_secret: TWITCH_CLIENT_SECRET,
        code,
        grant_type: "authorization_code",
        redirect_uri: redirectUri,
      }),
    });
  } catch (err) {
    log.warn({ err }, "oauth token exchange network error");
    return oauthFailRedirect(res, "network");
  }
  if (!tokenResp.ok) {
    log.warn({ status: tokenResp.status }, "oauth token exchange rejected");
    return oauthFailRedirect(res, "token");
  }
  const tokenJson = await tokenResp.json();
  const accessToken = tokenJson.access_token;
  if (!accessToken) return oauthFailRedirect(res, "noaccess");

  // Validate the token and fetch the username (login)
  let login = "";
  try {
    const verify = await fetchWithTimeout("https://id.twitch.tv/oauth2/validate", {
      headers: { Authorization: `OAuth ${accessToken}` },
    });
    if (verify.ok) {
      const v = await verify.json();
      login = v.login || "";
    }
  } catch (err) {
    // Even if validation fails, the token may still be valid; return it without login
    log.warn({ err }, "oauth token validate failed (continuing without login)");
  }

  // Store the result in an in-memory map under a random key, and set it in a
  // short-lived (30s) httpOnly cookie. This is more reliable than passing the
  // token in the URL fragment, which may be stripped by proxies/HTTPS and never
  // reach the browser.
  const ticket = cryptoRandom();
  oauthTickets.set(ticket, { token: accessToken, login, exp: Date.now() + 30000 });
  // Self-cleanup: delete the ticket after its 30s lifetime so an unpicked
  // ticket never lingers. (The periodic 8h sweep is only a backstop.)
  const cleanupTimer = setTimeout(() => oauthTickets.delete(ticket), 30000);
  cleanupTimer.unref();
  setAuthCookie(res, "tw_oauth_ticket", ticket, 30);
  log.info({ login: login || "(unknown)" }, "oauth success");
  res.redirect(302, "/");
});

// When the page opens after returning from Twitch: the client asks this
// endpoint, which reads the cookie and returns the token (then the ticket is
// deleted immediately = one-time use).
app.get("/api/twitch-token", apiLimiter, (req, res) => {
  // This response may carry the user's OAuth access_token, so never allow it
  // to be stored by a shared/proxy cache or the browser HTTP cache.
  res.setHeader("Cache-Control", "no-store");
  const ticket = readAuthCookie(req, "tw_oauth_ticket");
  if (!ticket) return res.json({ ok: false });
  const data = oauthTickets.get(ticket);
  oauthTickets.delete(ticket); // one-time use
  clearAuthCookie(res, "tw_oauth_ticket");
  if (!data || data.exp < Date.now()) return res.json({ ok: false });
  res.json({ ok: true, access_token: data.token, login: data.login || "" });
});

// In-memory OAuth ticket store — random key -> {token, login, exp}
const oauthTickets = new Map();
function cryptoRandom() {
  return crypto.randomBytes(18).toString("base64url");
}

// ----------------------------------------------------------------------
// Twitch Shared Chat support: resolve a foreign channel id (from the IRC
// `source-room-id` tag) to a login name so the client can label mirrored
// messages with "↪ channelName". Uses an app access token (client
// credentials grant) to call Helix; the token and resolved logins are cached
// for the process lifetime. If TWITCH_CLIENT_ID/SECRET are unset, resolution
// silently no-ops (the client then shows a generic "↪ shared" badge).
// ----------------------------------------------------------------------
let twitchAppToken = { token: null, exp: 0 };            // cached app access token + its expiry
const channelIdToLogin = new Map();                       // source room id -> channel login (permanent cache)
const pendingChannelLookups = new Map();                  // source room id -> in-flight Promise (dedup)

// Returns a valid app access token, fetching/refreshing it as needed.
// Returns null if Twitch credentials aren't configured or a fetch fails —
// callers treat null as "resolution unavailable" and fall back gracefully.
async function getTwitchAppAccessToken() {
  if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) return null;
  // Reuse the cached token until 60s before it expires.
  if (twitchAppToken.token && twitchAppToken.exp - 60000 > Date.now()) {
    return twitchAppToken.token;
  }
  try {
    const r = await fetchWithTimeout("https://id.twitch.tv/oauth2/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: TWITCH_CLIENT_ID,
        client_secret: TWITCH_CLIENT_SECRET,
        grant_type: "client_credentials",
      }),
    });
    if (!r.ok) {
      log.warn({ status: r.status }, "twitch app token request rejected");
      return twitchAppToken.token; // keep the previous token (if any) rather than clearing
    }
    const j = await r.json();
    if (!j.access_token || !j.expires_in) {
      log.warn("twitch app token response missing fields");
      return twitchAppToken.token;
    }
    twitchAppToken = { token: j.access_token, exp: Date.now() + j.expires_in * 1000 };
    return twitchAppToken.token;
  } catch (err) {
    log.warn({ err: String(err?.message || err) }, "twitch app token request failed");
    return twitchAppToken.token;
  }
}

// Resolves a Twitch channel id to its login (lowercase username) via Helix.
// Cached per-id for the process lifetime; concurrent calls for the same id
// share a single in-flight request. Returns null on any failure.
async function resolveChannelLogin(channelId) {
  if (!channelId) return null;
  const cached = channelIdToLogin.get(channelId);
  if (cached !== undefined) return cached;
  // Dedup concurrent lookups so a burst of shared messages issues one request.
  const pending = pendingChannelLookups.get(channelId);
  if (pending) return pending;
  const p = (async () => {
    const token = await getTwitchAppAccessToken();
    if (!token) return null;
    try {
      const r = await fetchWithTimeout(
        `https://api.twitch.tv/helix/channels?broadcaster_id=${encodeURIComponent(channelId)}`,
        { headers: { "Client-ID": TWITCH_CLIENT_ID, Authorization: `Bearer ${token}` } }
      );
      if (!r.ok) {
        log.warn({ status: r.status, channelId }, "twitch helix channel lookup rejected");
        return null;
      }
      const j = await r.json();
      const login = j?.data?.[0]?.broadcaster_login || null;
      if (login) channelIdToLogin.set(channelId, login);
      return login;
    } catch (err) {
      log.warn({ err: String(err?.message || err), channelId }, "twitch helix channel lookup failed");
      return null;
    }
  })();
  pendingChannelLookups.set(channelId, p);
  try {
    return await p;
  } finally {
    pendingChannelLookups.delete(channelId);
  }
}
// Builds a cookie with the chosen SameSite attribute (lax by default; for
// cross-subdomain deployment you may need none, but none requires Secure).
function authCookieAttributes(maxAgeSec) {
  const parts = ["Path=/", `Max-Age=${maxAgeSec}`, "SameSite=Lax", "HttpOnly"];
  // Behind HTTPS we add Secure so the cookie is only sent over an encrypted connection
  if (process.env.HTTPS === "1" || TWITCH_REDIRECT_URI.startsWith("https://")) {
    parts.push("Secure");
  }
  return parts.join("; ");
}
function setAuthCookie(res, name, value, maxAgeSec) {
  res.setHeader("Set-Cookie", `${name}=${encodeURIComponent(value)}; ${authCookieAttributes(maxAgeSec)}`);
}
function clearAuthCookie(res, name) {
  res.setHeader("Set-Cookie", `${name}=; ${authCookieAttributes(0)}`);
}
function readAuthCookie(req, name) {
  const header = req.headers.cookie;
  if (!header) return null;
  for (const part of header.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k === name) return decodeURIComponent((v.join("=") || ""));
  }
  return null;
}
// On OAuth failure we redirect to the page with an error flag in a short-lived
// cookie the client reads.
function oauthFailRedirect(res, reason) {
  setAuthCookie(res, "tw_oauth_error", reason, 30);
  res.redirect(302, "/");
}
app.get("/api/twitch-oauth-error", apiLimiter, (req, res) => {
  const reason = readAuthCookie(req, "tw_oauth_error");
  if (reason) clearAuthCookie(res, "tw_oauth_error");
  res.json({ error: reason || null });
});

// ----------------------------------------------------------------------
// Twitch: anonymous IRC over WebSocket (read-only)
// ----------------------------------------------------------------------
function connectTwitch(session) {
  if (session.closed) return;
  const username = session.twitchUser;
  const st = session.platforms.twitch;
  st.reconnecting = false;
  session.setPlatformStatus("twitch", "connecting");

  const socket = new WebSocket("wss://irc-ws.chat.twitch.tv:443");
  st.socket = socket;
  const anonNick = `justinfan${Math.floor(10000 + Math.random() * 89999)}`;

  socket.addEventListener("open", () => {
    socket.send("CAP REQ :twitch.tv/tags twitch.tv/commands");
    socket.send("PASS SCHMOOPIIE");
    socket.send(`NICK ${sanitizeIrcParam(anonNick)}`);
    socket.send(`JOIN #${sanitizeIrcParam(username)}`);
  });

  socket.addEventListener("message", async (event) => {
    const lines = String(event.data).split("\r\n").filter(Boolean);
    for (const line of lines) {
      const parsed = parseTwitchLine(line);
      if (!parsed) continue;
      if (parsed.ping) {
        socket.send("PONG :tmi.twitch.tv");
        continue;
      }
      if (parsed.joined) {
        st.attempt = 0; // connection succeeded — reset backoff
        session.setPlatformStatus("twitch", "live");
        // Begin polling Helix for the live viewer count now that we're in the
        // room. Torn down on disconnect/teardown.
        startTwitchViewerPoll(session);
        continue;
      }
      if (parsed.message) {
        // Emit immediately for non-shared messages. For Shared Chat messages
        // (carrying a source-room-id) we resolve the origin channel login in
        // the BACKGROUND (non-blocking) so a slow Helix lookup can never stall
        // the rest of the feed. Message ordering across the boundary is not
        // critical because the client renders by monotonic `id` and the origin
        // only affects the "↪ name" badge text.
        const m = parsed.message;
        if (m.sharedRoomId) {
          resolveChannelLogin(m.sharedRoomId)
            .then((login) => {
              if (session.closed) return;
              session.pushChat("tw", m.username, m.content, m.color, m.emotes, m.bits, login || "shared");
            })
            .catch(() => {
              if (session.closed) return;
              session.pushChat("tw", m.username, m.content, m.color, m.emotes, m.bits, "shared");
            });
        } else {
          session.pushChat("tw", m.username, m.content, m.color, m.emotes, m.bits);
        }
        continue;
      }
      if (parsed.event) {
        const text = formatTwitchEvent(parsed.event);
        if (!text) continue;
        const ev = parsed.event;
        if (ev.sharedRoomId) {
          resolveChannelLogin(ev.sharedRoomId)
            .then((login) => {
              if (session.closed) return;
              ev.sharedFrom = login || "shared";
              session.pushEvent("tw", ev.kind, ev.who, text, ev);
            })
            .catch(() => {
              if (session.closed) return;
              ev.sharedFrom = "shared";
              session.pushEvent("tw", ev.kind, ev.who, text, ev);
            });
        } else {
          session.pushEvent("tw", ev.kind, ev.who, text, ev);
        }
        continue;
      }
    }
  });

  socket.addEventListener("close", () => {
    st.socket = null;
    // Stop polling viewers while disconnected; startTwitchViewerPoll restarts
    // it once the next IRC connection reports "live" again.
    if (st.viewerTimer) {
      clearInterval(st.viewerTimer);
      st.viewerTimer = null;
    }
    if (session.closed) return;
    log.info({ session: session.key, channel: username }, "twitch read socket closed, reconnecting");
    session.setPlatformStatus("twitch", "reconnecting"); // clears viewers -> null
    scheduleTwitchReconnect(session);
  });

  socket.addEventListener("error", () => {
    // An error is usually followed by close; we just update the status
    log.warn({ session: session.key, channel: username }, "twitch read socket error");
    session.setPlatformStatus("twitch", "error");
  });
}

function scheduleTwitchReconnect(session) {
  const st = session.platforms.twitch;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  const delay = reconnectBackoff(st.attempt);
  st.attempt += 1;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectTwitch(session);
  }, delay);
}

// Fetches the live viewer count for a Twitch channel via Helix /streams. Uses
// the cached app access token (server-to-server; no user OAuth needed). Returns
// null when credentials aren't configured, the channel is offline, or any
// request fails — callers treat null as "unknown" and the header stays blank.
// Returns a tri-state result so the caller can distinguish a definitively-ended
// stream from a transient API failure:
//   { viewers: number, state: "live"    } — an active stream was returned
//   { viewers: null,   state: "offline" } — 200 OK but `data` is empty, i.e. the
//                                           channel is not broadcasting right now
//   { viewers: null,   state: "unknown" } — request failed/no credentials;
//                                           must NOT be treated as offline
async function fetchTwitchViewerCount(login) {
  if (!TWITCH_CLIENT_ID || !login) return { viewers: null, state: "unknown" };
  try {
    const token = await getTwitchAppAccessToken();
    if (!token) return { viewers: null, state: "unknown" };
    const r = await fetchWithTimeout(
      `https://api.twitch.tv/helix/streams?user_login=${encodeURIComponent(login)}`,
      { headers: { "Client-ID": TWITCH_CLIENT_ID, Authorization: `Bearer ${token}` } }
    );
    if (!r.ok) {
      log.warn({ status: r.status, login }, "twitch helix streams request rejected");
      return { viewers: null, state: "unknown" };
    }
    const j = await r.json();
    // The stream array is empty when the channel is offline; `viewer_count` is
    // only present for an active live stream.
    const stream = j?.data?.[0];
    if (stream) {
      const count = Number(stream.viewer_count);
      return Number.isFinite(count) && count >= 0
        ? { viewers: count, state: "live" }
        : { viewers: null, state: "live" };
    }
    // 200 OK with an empty data array -> channel is definitively offline.
    return { viewers: null, state: "offline" };
  } catch (err) {
    log.warn({ err: String(err?.message || err), login }, "twitch viewer count fetch failed");
    return { viewers: null, state: "unknown" };
  }
}

// Starts/stops the periodic viewer-count poll for the Twitch channel of this
// session. Polled because Twitch does not push the live viewer count over IRC.
// `startTwitchViewerPoll` is called when the IRC read connection goes live and
// clears the count (null) on teardown/error so the header never shows a stale
// number next to an offline pill.
function startTwitchViewerPoll(session) {
  const st = session.platforms.twitch;
  if (st.viewerTimer || session.closed) return;
  // Fire once immediately so the count appears without waiting for the first
  // tick, then on the interval. setInterval schedules the first run only after
  // one period, which would leave the header blank for up to a minute on join.
  pollTwitchViewers(session);
  st.viewerTimer = setInterval(() => pollTwitchViewers(session), VIEWER_POLL_MS);
}

async function pollTwitchViewers(session) {
  if (session.closed) return;
  const st = session.platforms.twitch;
  // Only poll while we believe we're live; otherwise clear (defensive — the
  // poll is torn down on teardown, but the connection may have dropped between
  // ticks without the timer being cleared yet).
  if (st.status !== "live") {
    if (st.viewers !== null) session.setViewers("twitch", null);
    // While the read socket is reconnecting we have no definitive signal, so
    // mark unknown rather than offline to avoid a false stream-ended trigger.
    st.offlineState = "unknown";
    st.offlineSince = null;
    return;
  }
  const { viewers: count, state } = await fetchTwitchViewerCount(session.twitchUser);
  if (session.closed) return;
  // Record the offline state for stream-ended detection before updating the
  // header number. A transition INTO "offline" stamps offlineSince; any other
  // state clears it.
  if (state === "offline") {
    if (st.offlineState !== "offline") st.offlineSince = Date.now();
  } else {
    st.offlineSince = null;
  }
  st.offlineState = state;
  session.setViewers("twitch", count);
  session.evaluateStreamEnded();
}

function teardownTwitch(session) {
  const st = session.platforms.twitch;
  if (st.timer) {
    clearTimeout(st.timer);
    st.timer = null;
  }
  if (st.viewerTimer) {
    clearInterval(st.viewerTimer);
    st.viewerTimer = null;
  }
  st.reconnecting = false;
  if (st.socket) {
    try {
      st.socket.close();
    } catch {}
    st.socket = null;
  }
}

function parseTwitchLine(line) {
  let tags = {};
  let rest = line;

  if (line.startsWith("@")) {
    const spaceIdx = line.indexOf(" ");
    const rawTags = line.slice(1, spaceIdx);
    rest = line.slice(spaceIdx + 1);
    rawTags.split(";").forEach((kv) => {
      // Split on the FIRST '=' only — values may legally contain '='. Splitting
      // on every '=' (the old behavior) dropped the tail of such values.
      const eq = kv.indexOf("=");
      const k = eq === -1 ? kv : kv.slice(0, eq);
      const v = eq === -1 ? "" : unescapeTagValue(kv.slice(eq + 1));
      tags[k] = v;
    });
  }

  if (rest.startsWith("PING")) return { ping: true };
  // JOIN command: anchored so a chat message whose text happens to contain
  // "JOIN #" isn't misread as a join signal. A real JOIN line looks like
  // ":nick!user@host JOIN #channel" (optionally with a trailing account tag).
  if (/\sJOIN\s+#\S+\s*$/.test(rest)) return { joined: true };

  // Shared Chat: when a room is in Stream Together mode, messages typed in a
  // linked channel are mirrored into this one. Twitch marks them with
  // `source-room-id` (the origin channel) which differs from `room-id` (the
  // channel we joined). We forward the raw origin id; the caller resolves it
  // to a login asynchronously so this parser stays synchronous and pure.
  const sharedRoomId =
    tags["source-room-id"] && tags["source-room-id"] !== tags["room-id"]
      ? tags["source-room-id"]
      : null;

  const privmsgMatch = rest.match(/^:(\S+) PRIVMSG (#\S+) :(.*)$/);
  if (privmsgMatch) {
    const [, prefix, , content] = privmsgMatch;
    const username = tags["display-name"] || prefix.split("!")[0];
    const color = tags["color"] && tags["color"] !== "" ? tags["color"] : "#9147ff";
    const emotes = parseTwitchEmotes(tags["emotes"], content);
    const out = { message: { username, content, color, emotes } };
    // Bits (cheers) ride inside a normal PRIVMSG, flagged by the `bits` tag.
    const bits = Number(tags["bits"]);
    if (Number.isFinite(bits) && bits > 0) out.message.bits = bits;
    if (sharedRoomId) out.message.sharedRoomId = sharedRoomId;
    return out;
  }

  // USERNOTICE: subscription / resub / gift sub / raid events. The kind is in
  // the `msg-id` tag (sub, resub, subgift, submysterygift, raid, ...). The
  // human-readable text comes from `system-msg`, and for resubs/gifts the user
  // may attach a custom message which arrives as the trailing PRIVMSG-style
  // payload (": their message").
  const usernoticeMatch = rest.match(/^:(\S+) USERNOTICE (#\S+)(?: :(.*))?$/);
  if (usernoticeMatch) {
    const [, prefix, , customMsg] = usernoticeMatch;
    const event = parseUsernotice(tags, prefix, customMsg, sharedRoomId);
    if (event) return { event };
  }

  return null;
}

// Unescapes a Twitch IRC tag value. Twitch escapes a few characters in tag
// values so the key=value pairs stay on a single line: \s -> space, \: -> ;,
// \\ -> \, \r, \n. We reverse this so messages like system-msg read naturally.
function unescapeTagValue(v) {
  if (!v) return v;
  return v.replace(/\\(.)/g, (_, ch) => {
    switch (ch) {
      case "s": return " ";
      case ":": return ";";
      case "\\": return "\\";
      case "r": return "\r";
      case "n": return "\n";
      default: return ch;
    }
  });
}

// Converts the emote tag (e.g. 25:0-4,12-16/1902:6-10)
// into an array of {id,start,end} (half-open) over the content characters.
function parseTwitchEmotes(rawEmotes, content) {
  if (!rawEmotes) return [];
  const out = [];
  for (const part of String(rawEmotes).split("/")) {
    if (!part) continue;
    const [idStr, ranges] = part.split(":");
    const id = Number(idStr);
    if (!Number.isFinite(id) || !ranges) continue;
    for (const range of ranges.split(",")) {
      const [s, e] = range.split("-");
      const start = Number(s);
      const end = Number(e);
      if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
      if (start < 0 || end >= content.length || start > end) continue;
      out.push({ id, start, end });
    }
  }
  return out;
}

// Parses the tags of a USERNOTICE into a structured event object we can render
// distinctly in the feed. Returns null for msg-ids we don't surface (so the
// line is silently dropped rather than rendered as a confusing empty event).
//
// Handled kinds (msg-id):
//   sub            — first-time subscription
//   resub          — subscription renewal (cumulative months in msg-param-cumulative-months)
//   subgift        — a single gifted sub (recipient in msg-param-recipient-display-name)
//   submysterygift — a community gift batch (count in msg-param-mass-gift-count)
//   raid           — a raid (viewers in msg-param-viewerCount, target in msg-param-displayName)
function parseUsernotice(tags, prefix, customMsg, sharedRoomId) {
  const kind = tags["msg-id"];
  const login = prefix ? prefix.split("!")[0] : "";
  const who = tags["display-name"] || tags["login"] || login || "";
  const systemMsg = tags["system-msg"] || "";
  const months = Number(tags["msg-param-cumulative-months"]);
  const recipient = tags["msg-param-recipient-display-name"] || "";
  const giftCount = Number(tags["msg-param-mass-gift-count"] || tags["msg-param-sender-count"]);
  const raidTarget = tags["msg-param-displayName"] || "";
  const raidViewers = Number(tags["msg-param-viewerCount"]);
  const tier = { 1000: "1", 2000: "2", 3000: "3" }[tags["msg-param-sub-plan"]] || "";

  let event;
  switch (kind) {
    case "sub":
      event = { kind: "sub", who, months: 1, tier, systemMsg };
      break;
    case "resub":
      event = { kind: "resub", who, months: Number.isFinite(months) && months > 0 ? months : null, tier, systemMsg };
      break;
    case "subgift":
      event = { kind: "gift", who, recipient, tier, systemMsg };
      break;
    case "submysterygift":
      event = { kind: "communitygift", who, count: Number.isFinite(giftCount) && giftCount > 0 ? giftCount : null, tier, systemMsg };
      break;
    case "raid":
      event = { kind: "raid", who, target: raidTarget, viewers: Number.isFinite(raidViewers) && raidViewers >= 0 ? raidViewers : null, systemMsg };
      break;
    default:
      return null; // unhandled msg-id — drop silently
  }
  // A resub or gift can carry an optional user-typed message
  if (customMsg && String(customMsg).trim()) event.message = String(customMsg);
  // Shared Chat: origin channel id, when this event was mirrored from a linked room.
  if (sharedRoomId) event.sharedRoomId = sharedRoomId;
  return event;
}

// Builds the short human-readable summary text for a Twitch event, shown as
// the main label of the event row. Returns "" for events we can't summarize
// (so the caller can skip emitting them).
function formatTwitchEvent(ev) {
  if (!ev || !ev.kind) return "";
  const name = ev.who || "Someone";
  switch (ev.kind) {
    case "sub":
      return `${name} subscribed!`;
    case "resub":
      return ev.months
        ? `${name} resubscribed — ${ev.months} month${ev.months === 1 ? "" : "s"}!`
        : `${name} resubscribed!`;
    case "gift":
      return ev.recipient
        ? `${name} gifted a sub to ${ev.recipient}!`
        : `${name} gifted a sub!`;
    case "communitygift":
      return ev.count
        ? `${name} gifted ${ev.count} subs to the community!`
        : `${name} gifted subs to the community!`;
    case "raid":
      return ev.target
        ? `${name} is raiding with ${ev.viewers ?? 0} viewer${(ev.viewers || 0) === 1 ? "" : "s"}!`
        : `${name} is raiding!`;
    default:
      return ev.systemMsg || "";
  }
}

// ----------------------------------------------------------------------
// Kick: public Pusher WebSocket (read-only)
// ----------------------------------------------------------------------
// Updates the status of a single Kick channel connection, then recomputes and
// broadcasts the aggregate Kick status. This replaces direct setPlatformStatus
// calls for Kick now that one session may hold several channels.
function setKickConnStatus(session, channelName, status, error) {
  const conn = session.platforms.kick.conns.get(channelName);
  if (!conn) return;
  conn.status = status;
  if (error !== undefined) {
    conn.error = error || null;
  } else if (status === "live") {
    // Clear any stale error when the channel comes back live, so a later drop
    // doesn't resurface an outdated error string via recomputeKickStatus.
    conn.error = null;
  }
  recomputeKickStatus(session);
}

// Derives a single aggregate status/error from the per-channel Kick conns and
// broadcasts it. Rules: all live -> "live"; all down -> first error; mixed ->
// "N/total live" (e.g. "2/3 live"). The client displays the string verbatim.
function recomputeKickStatus(session) {
  const conns = [...session.platforms.kick.conns.values()];
  if (!conns.length) return; // nothing connected yet; initial "connecting" stays
  const live = conns.filter((c) => c.status === "live").length;
  let status, error;
  if (live === conns.length) {
    status = "live";
    error = null;
  } else if (live === 0) {
    const failed = conns.find((c) => c.status === "error") || conns[0];
    status = failed.status;
    error = failed.error || null;
  } else {
    status = `${live}/${conns.length} live`;
    error = null;
  }
  session.platforms.kick.status = status;
  session.platforms.kick.error = error;
  session.broadcast({ type: "status", platform: "kick", status, error });
}

// Coordinator: opens one Pusher connection per Kick channel in the session.
// The first channel is "primary" (no shared badge, counted in stats); the rest
// are mirrors (badged, excluded from stats — see the sharedFrom guard in
// pushChat/pushEvent).
async function connectKick(session) {
  if (session.closed || !session.kickUsers.length) return;
  session.platforms.kick.status = "connecting";
  session.broadcast({ type: "status", platform: "kick", status: "connecting", error: null });
  for (const channelName of session.kickUsers) {
    if (!session.platforms.kick.conns.has(channelName)) {
      session.platforms.kick.conns.set(channelName, {
        socket: null,
        chatroomId: null,
        pingTimer: null,
        reconnectTimer: null,
        reconnecting: false,
        status: "connecting",
        error: null,
        attempt: 0,
        // Stream-ended detection (see Session.evaluateStreamEnded). Mirrors the
        // twitch platform-state fields above. Unknown by default; set to
        // "offline" only when Kick's channel API resolves with no livestream.
        offlineState: "unknown",
        offlineSince: null,
      });
    }
    connectKickChannel(session, channelName);
  }
}

// Connects (or reconnects) a single Kick channel's Pusher socket. The closure
// captures the channel's sharedFrom so every parsed message is stamped with the
// right origin before it reaches the shared pushChat/pushEvent pipeline.
async function connectKickChannel(session, channelName) {
  if (session.closed) return;
  const st = session.platforms.kick;
  const conn = st.conns.get(channelName);
  if (!conn) return;
  conn.reconnecting = false;
  setKickConnStatus(session, channelName, "connecting");

  // Resolve chatroom_id once and reuse it across reconnects.
  if (!conn.chatroomId) {
    try {
      conn.chatroomId = await lookupKickChatroom(channelName);
    } catch (err) {
      conn.chatroomId = null;
      const errMsg = String(err.message || err);
      const fatal = classifyKickError(errMsg).fatal;
      log.warn({ session: session.key, channel: channelName, err: errMsg, fatal }, "kick chatroom lookup failed");
      setKickConnStatus(session, channelName, "error", errMsg);
      // Fatal errors (user not found, no chatroom) should not trigger an
      // infinite reconnect loop — they won't resolve until the input changes.
      if (!fatal) scheduleKickReconnect(session, channelName);
      return;
    }
  }
  if (session.closed) return;

  const socket = new WebSocket(
    `wss://ws-us2.pusher.com/app/${KICK_PUSHER_APP_KEY}?protocol=7&client=js&version=7.6.0&flash=false`
  );
  conn.socket = socket;

  // The first Kick channel is primary (no badge, counts in stats). Additional
  // channels are mirrors: their messages carry this name as `sharedFrom`, which
  // the feed renders as a "↪ name" badge and the stats layer excludes.
  const isPrimary = session.kickUsers[0] === channelName;
  const sharedFrom = isPrimary ? null : channelName;

  socket.addEventListener("open", () => {
    if (session.closed) return;
    // Subscribe to the v2 chatroom channel only. Subscribing to BOTH v2 and
    // the legacy v1 channel previously caused every message/event to be
    // delivered twice (and the monotonic `seq` id cannot detect content
    // duplicates), which doubled chat rows and stats. v2 is the current API.
    subscribeKickChannel(socket, `chatrooms.${conn.chatroomId}.v2`);
    conn.attempt = 0; // connection succeeded — reset backoff
    setKickConnStatus(session, channelName, "live");

    conn.pingTimer = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ event: "pusher:ping", data: {} }));
      }
    }, 25000);

    // Begin polling the channel API for the live viewer count. Kick does not
    // publish a viewer-count event over the chatrooms Pusher channel, so we
    // poll the same v2 channel endpoint we use for chatroom lookup. Cleared on
    // close/teardown.
    startKickViewerPoll(session, channelName);
  });

  socket.addEventListener("message", (event) => {
    const parsed = parseKickFrame(event.data);
    if (!parsed) return;
    if (parsed.chat) {
      const c = parsed.chat;
      session.pushChat("kk", c.username, c.content, c.color, c.emotes, undefined, sharedFrom);
    } else if (parsed.event) {
      const text = formatKickEvent(parsed.event);
      if (!text) return;
      parsed.event.sharedFrom = sharedFrom; // stamps the event for the feed + stats guard
      session.pushEvent("kk", parsed.event.kind, parsed.event.who, text, parsed.event);
    }
  });

  socket.addEventListener("close", () => {
    conn.socket = null;
    if (conn.pingTimer) {
      clearInterval(conn.pingTimer);
      conn.pingTimer = null;
    }
    if (conn.viewerTimer) {
      clearInterval(conn.viewerTimer);
      conn.viewerTimer = null;
    }
    if (session.closed) return;
    log.info({ session: session.key, channel: channelName }, "kick socket closed, reconnecting");
    // Clear this channel's count (and broadcast the recomputed aggregate) so
    // the header doesn't show a stale number next to a "reconnecting" pill.
    session.setViewers("kick", null, channelName);
    setKickConnStatus(session, channelName, "reconnecting");
    scheduleKickReconnect(session, channelName);
  });

  socket.addEventListener("error", () => {
    log.warn({ session: session.key, channel: channelName }, "kick socket error");
    setKickConnStatus(session, channelName, "error");
  });
}

function scheduleKickReconnect(session, channelName) {
  const conn = session.platforms.kick.conns.get(channelName);
  if (!conn || conn.reconnecting || session.closed) return;
  conn.reconnecting = true;
  const delay = reconnectBackoff(conn.attempt);
  conn.attempt += 1;
  conn.reconnectTimer = setTimeout(() => {
    conn.reconnectTimer = null;
    if (!session.closed) connectKickChannel(session, channelName);
  }, delay);
}

// Periodic viewer-count poll for a single Kick channel. One poll per channel
// (not one per session) so a multi-channel session shows the correct aggregate
// even when channels join at different times. Each result is written via
// setViewers("kick", ..., channelName), which recomputes and broadcasts the
// session-wide sum.
function startKickViewerPoll(session, channelName) {
  const conn = session.platforms.kick.conns.get(channelName);
  if (!conn || conn.viewerTimer || session.closed) return;
  pollKickViewers(session, channelName); // immediate first run
  conn.viewerTimer = setInterval(() => pollKickViewers(session, channelName), VIEWER_POLL_MS);
}

async function pollKickViewers(session, channelName) {
  if (session.closed) return;
  const conn = session.platforms.kick.conns.get(channelName);
  if (!conn || conn.status !== "live") {
    // Defensive: the poll is cleared on close, but the connection may have
    // dropped between ticks. Clear any stale count for this channel.
    if (conn && conn.viewers !== null) session.setViewers("kick", null, channelName);
    // While reconnecting we have no definitive signal, so mark unknown rather
    // than offline to avoid a false stream-ended trigger.
    if (conn) {
      conn.offlineState = "unknown";
      conn.offlineSince = null;
    }
    return;
  }
  const { viewers: count, state } = await fetchKickViewerCount(channelName);
  if (session.closed) return;
  // Record offline state for stream-ended detection. Same transition rule as
  // pollTwitchViewers: stamp offlineSince on the offline entry-edge only.
  if (state === "offline") {
    if (conn.offlineState !== "offline") conn.offlineSince = Date.now();
  } else {
    conn.offlineSince = null;
  }
  conn.offlineState = state;
  session.setViewers("kick", count, channelName);
  session.evaluateStreamEnded();
}

function teardownKick(session) {
  const st = session.platforms.kick;
  for (const conn of st.conns.values()) {
    if (conn.reconnectTimer) {
      clearTimeout(conn.reconnectTimer);
      conn.reconnectTimer = null;
    }
    if (conn.pingTimer) {
      clearInterval(conn.pingTimer);
      conn.pingTimer = null;
    }
    if (conn.viewerTimer) {
      clearInterval(conn.viewerTimer);
      conn.viewerTimer = null;
    }
    conn.reconnecting = false;
    if (conn.socket) {
      try {
        conn.socket.close();
      } catch {}
      conn.socket = null;
    }
  }
  st.conns.clear();
}

function subscribeKickChannel(socket, channel) {
  socket.send(JSON.stringify({ event: "pusher:subscribe", data: { channel, auth: "" } }));
}

// Parses a Pusher frame from Kick into either { chat: {...} } for a normal
// message or { event: {...} } for a subscription/gift/follow/raid alert.
// Returns null for anything we don't surface (Pusher control frames, unknown
// event names, or malformed payloads).
function parseKickFrame(raw) {
  let outer;
  try {
    outer = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!outer.event) return null;
  const eventName = String(outer.event);

  let payload = outer.data;
  if (typeof payload === "string") {
    try {
      payload = JSON.parse(payload);
    } catch {
      return null;
    }
  }
  if (!payload) return null;

  // Chat message. Match the canonical Kick event name exactly rather than as a
  // substring, so a future event like DeletedChatMessage isn't mis-parsed as a
  // normal chat line. We accept the known name plus an endsWith fallback in
  // case Kick adjusts the namespace prefix.
  if (eventName === "App\\Events\\ChatMessageEvent" || eventName.endsWith("\\ChatMessageEvent")) {
    const chat = parseKickChat(payload);
    return chat ? { chat } : null;
  }

  // Alert events
  switch (eventName) {
    case KICK_EVENT.SUBSCRIPTION:
      return { event: parseKickSubscription(payload) };
    case KICK_EVENT.GIFTED_SUBS:
      return { event: parseKickGiftedSubs(payload) };
    case KICK_EVENT.FOLLOW:
    case KICK_EVENT.FOLLOW_LEGACY:
      return { event: parseKickFollow(payload) };
    case KICK_EVENT.RAID:
      return { event: parseKickRaid(payload) };
    default:
      return null; // unhandled event — drop silently
  }
}

// Extracts a normal chat message from a ChatMessageEvent payload.
function parseKickChat(payload) {
  const msgObj = payload.message ?? payload;
  const content = msgObj.content ?? msgObj.message ?? "";
  const sender = payload.sender ?? payload.user ?? {};
  const username = sender.username ?? sender.slug ?? "user";
  const color = sender.identity?.color || "#53fc18";

  // Kick embeds emotes inline in the text as [emote:id:name], so we extract them.
  const emotes = parseKickEmotes(content);

  if (!content) return null;
  return { username, content, color, emotes };
}

// SubscriptionEvent: a user subscribed (new or renewing). The months/tier
// fields are optional and vary by Kick payload revision, so we read them
// defensively.
function parseKickSubscription(payload) {
  const username =
    payload.username || payload.user?.username || payload.channel?.username || "Someone";
  const months = Number(payload.months || payload.cumulative_months);
  const tier = payload.tier || payload.sub_tier || "";
  return {
    kind: "sub",
    who: username,
    months: Number.isFinite(months) && months > 0 ? months : null,
    tier,
  };
}

// GiftedSubscriptionsEvent: someone gifted subs. Kick's payload shape has
// shifted over time and isn't officially documented, so we read every known
// variant:
//   - gifter name: gifter_username (snake) / gifterUsername (camel) / username
//   - count: gifted_usernames (an array — its length is the count) /
//     giftedCount / giftee_quantity / quantity / amount
function parseKickGiftedSubs(payload) {
  const username =
    payload.gifter_username || payload.gifterUsername ||
    payload.username || payload.user?.username || "Someone";
  let count = null;
  if (Array.isArray(payload.gifted_usernames)) {
    count = payload.gifted_usernames.length;
  } else {
    const n = Number(payload.giftedCount || payload.giftee_quantity || payload.quantity || payload.amount);
    count = Number.isFinite(n) && n > 0 ? n : null;
  }
  return {
    kind: "communitygift",
    who: username,
    count,
  };
}

// Follow event (App\Events\FollowEvent, formerly App\Events\FollowersUpdatedEvent).
// Payload shape varies by Kick revision: the older FollowersUpdatedEvent carried
// the follower's `username`, while the newer FollowEvent sometimes carries only
// a `followersCount` total (no per-follower name). We surface a name when we can
// find one; otherwise we return null so the caller drops the event silently —
// a count-only update has no "actor" to show in the feed or the follows panel.
function parseKickFollow(payload) {
  const username =
    payload.username || payload.user?.username ||
    payload.follower_username || payload.followerUsername ||
    payload.follower?.username || "";
  if (!username) return null; // count-only update — nothing to display
  return { kind: "follow", who: username };
}

// HostEvent: a raid (host). The raider's channel and the number of viewers
// they brought along.
function parseKickRaid(payload) {
  const who = payload.username || payload.host_username || payload.channel?.username || "Someone";
  const viewers = Number(payload.viewers || payload.viewer_count);
  return {
    kind: "raid",
    who,
    viewers: Number.isFinite(viewers) && viewers >= 0 ? viewers : null,
  };
}

// Builds the short human-readable summary for a Kick event. Returns "" for
// events we can't summarize (so the caller can skip emitting them).
function formatKickEvent(ev) {
  if (!ev || !ev.kind) return "";
  const name = ev.who || "Someone";
  switch (ev.kind) {
    case "sub":
      return ev.months
        ? `${name} resubscribed — ${ev.months} month${ev.months === 1 ? "" : "s"}!`
        : `${name} subscribed!`;
    case "communitygift":
      return ev.count
        ? `${name} gifted ${ev.count} sub${ev.count === 1 ? "" : "s"} to the community!`
        : `${name} gifted subs to the community!`;
    case "follow":
      return `${name} followed!`;
    case "raid":
      return ev.viewers != null
        ? `${name} is raiding with ${ev.viewers} viewer${ev.viewers === 1 ? "" : "s"}!`
        : `${name} is raiding!`;
    default:
      return "";
  }
}

// Classifies a Kick chatroom-lookup error: fatal = no point retrying (the user
// does not exist or has no chatroom). Mirrors the fatal/retryable split the
// Twitch and TikTok connectors already have.
function classifyKickError(text) {
  const t = String(text || "");
  if (/not found|no chat/i.test(t)) return { fatal: true, text: t };
  return { fatal: false, text: t };
}

// Extracts Kick emotes from the text ([emote:id:name]) into {id,start,end}
// where start/end are inclusive UTF-16 indices (matching parseTwitchEmotes).
function parseKickEmotes(content) {
  if (!content) return [];
  const out = [];
  const re = /\[emote:(\d+):[^\]]+\]/g;
  let m;
  while ((m = re.exec(content)) !== null) {
    const id = Number(m[1]);
    if (!Number.isFinite(id) || id <= 0) continue;
    const start = m.index;
    const end = m.index + m[0].length - 1; // inclusive end
    if (start < 0 || end >= content.length || start > end) continue;
    out.push({ id, start, end });
  }
  return out;
}

// ----------------------------------------------------------------------
// TikTok: live chat via tiktok-live-connector (Euler Stream Sign Server)
// Read-only, no login. Requires TIKTOK_SIGN_API_KEY to work.
// ----------------------------------------------------------------------
async function connectTikTok(session) {
  if (session.closed) return;
  const username = session.tiktokUser;
  const st = session.platforms.tiktok;
  st.reconnecting = false;
  session.setPlatformStatus("tiktok", "connecting");

  if (!TIKTOK_SIGN_API_KEY) {
    session.setPlatformStatus(
      "tiktok",
      "error",
      "TikTok requires an Euler Stream API key (TIKTOK_SIGN_API_KEY) on the server."
    );
    return;
  }

  const connection = new TikTokLiveConnection(username, {
    signApiKey: TIKTOK_SIGN_API_KEY,
    // Do not process the initial data batch (recent history) to avoid duplicates/flooding
    processInitialData: false,
    // We do not need extended gift data in read-only mode
    enableExtendedGiftInfo: false,
  });
  st.connection = connection;

  connection.on(WebcastEvent.CHAT, (data) => {
    if (session.closed) return;
    const user = data?.user || {};
    // nickname = display name, uniqueId = @handle; prefer nickname, fall back to uniqueId
    const displayName = user.nickname || user.uniqueId || "user";
    const content = data?.comment ?? data?.content ?? "";
    if (!content) return;
    // TikTok does not provide a per-user color — use TikTok's default color
    session.pushChat("tt", displayName, String(content), "#fe2c55");
  });

  connection.on(ControlEvent.DISCONNECTED, () => {
    st.connection = null;
    if (session.closed) return;
    log.info({ session: session.key, channel: username }, "tiktok disconnected, reconnecting");
    session.setPlatformStatus("tiktok", "reconnecting");
    scheduleTikTokReconnect(session);
  });

  try {
    await connection.connect();
    if (session.closed) {
      try { connection.disconnect(); } catch {}
      return;
    }
    st.attempt = 0; // connection succeeded — reset backoff
    session.setPlatformStatus("tiktok", "live");
  } catch (err) {
    st.connection = null;
    if (session.closed) return;
    const msg = classifyTikTokError(err);
    log.warn({ session: session.key, channel: username, err: msg.text, fatal: msg.fatal }, "tiktok connect failed");
    if (msg.fatal) {
      // Non-retryable error (invalid handle / not live) — report and do not retry
      session.setPlatformStatus("tiktok", "error", msg.text);
      return;
    }
    session.setPlatformStatus("tiktok", "error", msg.text);
    scheduleTikTokReconnect(session);
  }
}

function scheduleTikTokReconnect(session) {
  const st = session.platforms.tiktok;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  const delay = reconnectBackoff(st.attempt);
  st.attempt += 1;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectTikTok(session);
  }, delay);
}

function teardownTikTok(session) {
  const st = session.platforms.tiktok;
  if (st.timer) {
    clearTimeout(st.timer);
    st.timer = null;
  }
  st.reconnecting = false;
  if (st.connection) {
    try {
      st.connection.disconnect();
    } catch {}
    st.connection = null;
  }
}

// Classifies a TikTok connection error: fatal = no point retrying.
function classifyTikTokError(err) {
  const name = err?.constructor?.name || "";
  const text = String(err?.message || err || "");
  if (err instanceof InvalidUniqueIdError || name === "InvalidUniqueIdError") {
    return { fatal: true, text: "Invalid TikTok username" };
  }
  if (err instanceof UserOfflineError || name === "UserOfflineError") {
    return { fatal: true, text: "This account is not live on TikTok right now" };
  }
  // Narrow text match for offline signals. The previous broad /offline|not live/i
  // could classify a transient message like "Euler service is offline" as fatal,
  // permanently abandoning a channel that would recover.
  if (/user offline|not currently live|currently offline/i.test(text)) {
    return { fatal: true, text: "This account is not live on TikTok right now" };
  }
  if (/rate limit|quota|429/i.test(text)) {
    return { fatal: false, text: "Euler Stream request limit exceeded; will retry later" };
  }
  return { fatal: false, text: "Could not connect to TikTok" };
}

// ----------------------------------------------------------------------
// YouTube: live chat via youtubei.js (InnerTube — no key, no quota)
// Read-only. Accepts a stream URL (watch?v= / youtu.be) or a raw videoId.
// ----------------------------------------------------------------------
// Extracts the videoId from a YouTube URL, or returns it as-is if already
// valid. Returns null if no valid id is found.
function extractYouTubeVideoId(input) {
  if (!input) return null;
  const s = String(input).trim();

  // 1) watch?v= URL (with/without youtu.be, with/without playlist)
  let m = s.match(/[?&]v=([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 2) Short youtu.be/VIDEOID URL
  m = s.match(/youtu\.be\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 3) /live/ or /embed/ URL
  m = s.match(/(?:live|embed|shorts)\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 4) Bare videoId (exactly 11 chars from the allowed alphabet)
  if (/^[A-Za-z0-9_-]{11}$/.test(s)) return s;

  return null;
}

async function connectYouTube(session) {
  if (session.closed) return;
  const st = session.platforms.youtube;
  st.reconnecting = false;
  session.setPlatformStatus("youtube", "connecting");

  // Resolve the videoId from the input
  let videoId = extractYouTubeVideoId(session.youtubeInput);
  if (!videoId) {
    session.setPlatformStatus(
      "youtube",
      "error",
      "Enter a valid YouTube live URL (youtube.com/watch?v=... or youtu.be/...) or a video id"
    );
    return;
  }
  st.videoId = videoId;
  if (session.closed) return;

  let yt;
  try {
    yt = await getYoutubeClient();
  } catch (err) {
    if (session.closed) return;
    log.warn({ session: session.key, videoId, err: String(err?.message || err) }, "youtube client init failed");
    session.setPlatformStatus("youtube", "error", "Could not initialize the YouTube client: " + String(err?.message || err));
    scheduleYouTubeReconnect(session);
    return;
  }
  if (session.closed) return;

  // Fetch video info (needed to create the LiveChat)
  let info;
  try {
    info = await yt.getInfo(videoId);
  } catch (err) {
    if (session.closed) return;
    const msg = String(err?.message || err);
    // Video not found / private / unavailable. Use specific phrases rather than
    // a bare "404" substring, which could otherwise match an incidental "404" in
    // a transient error message and wrongly mark a recoverable stream as dead.
    if (/video (unavailable|not found)|does not exist|is private|VideoUnavailable|This video is (private|unavailable)/i.test(msg)) {
      log.info({ session: session.key, videoId }, "youtube video not found or private (fatal)");
      session.setPlatformStatus("youtube", "error", "This video does not exist or is private");
      return; // fatal
    }
    log.warn({ session: session.key, videoId, err: msg }, "youtube getInfo failed");
    session.setPlatformStatus("youtube", "error", "Could not fetch stream data: " + msg);
    scheduleYouTubeReconnect(session);
    return;
  }
  if (session.closed) return;

  // Verify it is actually a live stream
  if (!info.basic_info?.is_live) {
    session.setPlatformStatus("youtube", "error", "This stream is not live right now (or has ended)");
    return; // fatal — no point retrying until the input changes
  }

  // Create the LiveChat and start streaming
  let livechat;
  try {
    livechat = info.getLiveChat();
  } catch (err) {
    if (session.closed) return;
    log.warn({ session: session.key, videoId, err: String(err?.message || err) }, "youtube getLiveChat failed");
    session.setPlatformStatus("youtube", "error", "This stream has no chat: " + String(err?.message || err));
    return;
  }
  st.livechat = livechat;
  if (session.closed) {
    try { livechat.stop(); } catch {}
    return;
  }

  // Flag to avoid double-emitting the "stream ended" message
  let endedHandled = false;
  const markEnded = (reason) => {
    if (endedHandled || session.closed) return;
    endedHandled = true;
    st.livechat = null;
    // Use a distinct "ended" status (rather than "error") so the client can
    // show a neutral "stream ended" message instead of a red error pill.
    session.setPlatformStatus("youtube", "ended", reason);
  };

  // Only normal chat messages (ignore paid/pinned/ads)
  livechat.on("chat-update", (action) => {
    if (session.closed) return;
    try {
      if (!action.is(YTNodes.AddChatItemAction)) return;
      const item = action.as(YTNodes.AddChatItemAction).item;
      if (!item || item.type !== "LiveChatTextMessage") return;

      const chatMsg = item.as(YTNodes.LiveChatTextMessage);
      const username = chatMsg.author?.name?.toString() || "user";
      const content = chatMsg.message?.toString() || "";
      if (!content) return;
      // YouTube does not provide a per-user color — use YouTube's default color
      session.pushChat("yt", username, content, "#ff0000");
    } catch {
      // Silently ignore items we cannot parse
    }
  });

  // Stream ended permanently — no point reconnecting
  livechat.on("end", () => markEnded("YouTube stream ended"));

  // Error: the library retries internally up to 10 times, then emits end.
  // Here we just record the transient status without tearing down. The error
  // detail (err) is intentionally not surfaced to the client — the library
  // handles recovery, and surfacing transient internal errors would be noisy.
  livechat.on("error", (err) => {
    if (session.closed) return;
    // Only update status, do not reschedule (the library handles it). The error
    // detail (err) is intentionally not surfaced to the client — the library
    // handles recovery, and surfacing transient internal errors would be noisy.
    log.debug({ session: session.key, videoId, err: String(err?.message || err) }, "youtube livechat transient error (library retries)");
    if (st.status !== "error") session.setPlatformStatus("youtube", "reconnecting");
  });

  // First successful response = the stream is live and chat works
  livechat.on("start", () => {
    if (!session.closed) {
      st.attempt = 0; // connection succeeded — reset backoff
      session.setPlatformStatus("youtube", "live");
    }
  });

  // Start streaming
  try {
    livechat.start();
  } catch (err) {
    if (session.closed) return;
    st.livechat = null;
    log.warn({ session: session.key, videoId, err: String(err?.message || err) }, "youtube livechat start failed");
    session.setPlatformStatus("youtube", "error", "Failed to start chat streaming: " + String(err?.message || err));
    scheduleYouTubeReconnect(session);
  }
}

function scheduleYouTubeReconnect(session) {
  const st = session.platforms.youtube;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  const delay = reconnectBackoff(st.attempt);
  st.attempt += 1;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectYouTube(session);
  }, delay);
}

function teardownYouTube(session) {
  const st = session.platforms.youtube;
  if (st.timer) {
    clearTimeout(st.timer);
    st.timer = null;
  }
  st.reconnecting = false;
  if (st.livechat) {
    try {
      st.livechat.stop();
    } catch {}
    st.livechat = null;
  }
}

// ----------------------------------------------------------------------
// Twitch SEND: an authenticated IRC connection under the user's name for
// writing to chat. Writing to Twitch requires a registered connection
// (NICK + PASS token) from a real account, so the anonymous read connection
// cannot be reused. We create one connection per session on the first send
// request and keep it for the lifetime of the session.
// ----------------------------------------------------------------------
// Validates a Twitch token and returns the login (username), or null.
async function validateTwitchToken(accessToken) {
  try {
    const r = await fetchWithTimeout("https://id.twitch.tv/oauth2/validate", {
      headers: { Authorization: `OAuth ${accessToken}` },
    });
    if (!r.ok) return null;
    const j = await r.json();
    if (!j.login || j.expires_in <= 0) return null;
    // Make sure the scope allows writing
    const scopes = Array.isArray(j.scopes) ? j.scopes : [];
    if (!scopes.includes("chat:edit")) return null;
    return j.login;
  } catch (err) {
    log.warn({ err: String(err?.message || err) }, "twitch token validate request failed");
    return null;
  }
}

// Ensures an authenticated IRC write connection exists for the session, and
// resolves with the username when ready. Returns a promise that resolves with
// the username on readiness, or rejects with an error message.
function ensureTwitchSendConnection(session, accessToken) {
  return new Promise((resolve, reject) => {
    if (session.closed) return reject(new Error("Session is closed"));

    validateTwitchToken(accessToken).then((login) => {
      if (!login) return reject(new Error("Twitch token is invalid or expired — reconnect your account"));
      if (!session.twitchUser) return reject(new Error("No Twitch channel in this session"));

      // Reuse an existing connection for the same login
      const existing = session.sendConnection;
      if (existing && existing.login === login && existing.socket && existing.socket.readyState === WebSocket.OPEN) {
        return resolve(login);
      }
      // Close any previous, different connection
      if (existing && existing.socket) {
        try { existing.socket.close(); } catch {}
      }

      const socket = new WebSocket("wss://irc-ws.chat.twitch.tv:443");
      const conn = { socket, login, ready: false };
      session.sendConnection = conn;

      socket.addEventListener("open", () => {
        // The session may have been destroyed between token validation and
        // socket open. Bail out and close the socket so we don't leak an
        // authenticated IRC connection for a closed session.
        if (session.closed) {
          try { socket.close(); } catch {}
          session.sendConnection = null;
          reject(new Error("Session is closed"));
          return;
        }
        socket.send("CAP REQ :twitch.tv/tags twitch.tv/commands");
        socket.send(`PASS oauth:${sanitizeIrcParam(accessToken)}`);
        socket.send(`NICK ${sanitizeIrcParam(login)}`);
        // We wait for 001 (WELCOME) to confirm auth succeeded
      });

      socket.addEventListener("message", (event) => {
        const lines = String(event.data).split("\r\n").filter(Boolean);
        for (const line of lines) {
          // 001 = welcome = auth succeeded
          if (/\s001\s/.test(line)) {
            socket.send(`JOIN #${sanitizeIrcParam(session.twitchUser)}`);
            conn.ready = true;
            resolve(login);
            continue;
          }
          // NOTICE usually follows an auth failure
          if (/NOTICE\s+\*\s+:Login authentication failed/i.test(line) ||
              /NOTICE.*:Login authentication failed/i.test(line)) {
            try { socket.close(); } catch {}
            session.sendConnection = null;
            reject(new Error("Twitch login failed — invalid token"));
            return;
          }
          if (line.startsWith("PING")) {
            socket.send("PONG :tmi.twitch.tv");
          }
        }
      });

      socket.addEventListener("close", () => {
        if (session.sendConnection === conn) session.sendConnection = null;
        // If the socket closed before we received 001 (WELCOME), the promise
        // would otherwise hang forever — reject so the client gets a result.
        if (!conn.ready) reject(new Error("Twitch chat connection closed before authentication completed"));
      });
      socket.addEventListener("error", () => {
        if (!conn.ready) reject(new Error("Could not reach Twitch's chat server for writing"));
      });
    }).catch(() => reject(new Error("Could not validate the Twitch token")));
  });
}

// Handles a send request from the client: requires accessToken + content.
function handleTwitchSend(session, ws, msg) {
  if (!session || !session.twitchUser) {
    safeSend(ws, JSON.stringify({ type: "send_result", ok: false, error: "No Twitch channel linked" }));
    return;
  }
  const accessToken = typeof msg.access_token === "string" ? msg.access_token.trim() : "";
  const content = typeof msg.content === "string" ? msg.content.slice(0, 500) : "";
  if (!accessToken) {
    safeSend(ws, JSON.stringify({ type: "send_result", ok: false, error: "Connect your Twitch account first" }));
    return;
  }
  if (!content) {
    safeSend(ws, JSON.stringify({ type: "send_result", ok: false, error: "Message is empty" }));
    return;
  }

  ensureTwitchSendConnection(session, accessToken)
    .then((login) => {
      const conn = session.sendConnection;
      if (!conn || !conn.socket || conn.socket.readyState !== WebSocket.OPEN) {
        throw new Error("Connection is not ready");
      }
      // PRIVMSG: standard IRC form for sending a chat message. Sanitize both
      // the channel and the content to prevent IRC command injection via \r\n.
      conn.socket.send(`PRIVMSG #${sanitizeIrcParam(session.twitchUser)} :${sanitizeIrcParam(content)}`);
      safeSend(ws, JSON.stringify({ type: "send_result", ok: true }));
    })
    .catch((err) => {
      safeSend(ws, JSON.stringify({ type: "send_result", ok: false, error: String(err.message || err) }));
    });
}

// ----------------------------------------------------------------------
// WebSocket for clients (browsers)
// ----------------------------------------------------------------------

// Per-connection WS rate limiter. express-rate-limit covers HTTP, but WS
// messages bypass Express entirely, so a malicious client could flood auth /
// join / send messages after a single HTTP upgrade. This sliding-window counter
// caps each message type per connection. Limits are generous for a real user
// but block automated flooding.
const WS_RATE_WINDOW_MS = 10 * 1000;
const WS_RATE_LIMITS = {
  auth: 5,         // reconnect storms are fine; brute-force password is not
  join: 10,        // user switching channels
  send: 20,        // Twitch IRC is slow; 20/10s is well above human typing speed
  reset_stats: 5,  // deliberate action; no reason to spam more than a few times
};
const WS_RATE_TOTAL = 60; // catch-all for any message type combined

class WsRateLimiter {
  constructor() {
    this.buckets = {}; // type -> [timestamps...]
    this.total = [];
  }
  // Returns true if the message is allowed, false if rate-limited.
  allow(type) {
    const now = Date.now();
    const cutoff = now - WS_RATE_WINDOW_MS;
    // Prune the total bucket
    this.total = this.total.filter((t) => t > cutoff);
    if (this.total.length >= WS_RATE_TOTAL) return false;
    // Per-type bucket
    const limit = WS_RATE_LIMITS[type];
    if (limit) {
      // Re-read and prune in place so the same array reference is mutated.
      let bucket = this.buckets[type];
      if (!bucket) {
        bucket = [];
        this.buckets[type] = bucket;
      }
      // Remove expired entries from the bucket (mutate in place)
      for (let i = bucket.length - 1; i >= 0; i--) {
        if (bucket[i] <= cutoff) bucket.splice(i, 1);
      }
      if (bucket.length >= limit) return false;
      bucket.push(now);
    }
    this.total.push(now);
    return true;
  }
}

// Per-IP concurrent connection counter, used to enforce MAX_WS_PER_IP.
const ipConnCount = new Map(); // ip -> count

// Verifies the Origin header on an incoming WS upgrade against the request's
// own host, blocking cross-site WebSocket hijacking (CSWSH). A page on another
// origin that opens a WS to us would otherwise be able to attempt auth/brute
// and spam despite per-connection rate limits. Non-browser clients (curl,
// server-side tests) send no Origin — those are allowed only when the site is
// NOT password-protected, so an open dev server stays easy to test, while a
// protected deployment requires a browser Origin.
function isOriginAllowed(req) {
  const origin = req.headers.origin;
  // No Origin header = non-browser client. Allow only on open sites.
  if (!origin) return !siteProtected();
  // Build the allowed origin from the request host (trust proxy is on, so
  // X-Forwarded-Host is respected by Express; for the raw upgrade we read the
  // forwarded host directly if present).
  const host =
    (typeof req.headers["x-forwarded-host"] === "string" && req.headers["x-forwarded-host"]) ||
    req.headers.host;
  if (!host) return false;
  const proto =
    (typeof req.headers["x-forwarded-proto"] === "string" && req.headers["x-forwarded-proto"]) ||
    (req.socket && req.socket.encrypted ? "https" : "http");
  try {
    const allowed = new URL(`${proto}://${host}`).origin;
    return origin === allowed;
  } catch {
    return false;
  }
}

wss.on("connection", (ws, req) => {
  let session = null;
  // Auth is required before anything else when protection is enabled.
  let authed = !siteProtected();
  const limiter = new WsRateLimiter();
  const clientIp = req.socket.remoteAddress;

  // Reject cross-origin WebSocket upgrades (CSWSH defense).
  if (!isOriginAllowed(req)) {
    log.warn({ ip: clientIp, origin: req.headers.origin }, "ws connection rejected: bad origin");
    try { ws.close(4003, "origin not allowed"); } catch {}
    return;
  }

  // Enforce a per-IP concurrent connection cap.
  if (clientIp) {
    const cur = (ipConnCount.get(clientIp) || 0) + 1;
    if (cur > MAX_WS_PER_IP) {
      log.warn({ ip: clientIp, count: cur - 1, max: MAX_WS_PER_IP }, "ws connection rejected: per-IP cap");
      try { ws.close(4029, "too many connections"); } catch {}
      return;
    }
    ipConnCount.set(clientIp, cur);
  }

  // If the site is protected, require auth within a short window or close the
  // socket so idle auth-pending connections cannot accumulate.
  let authTimer = null;
  if (siteProtected()) {
    authTimer = setTimeout(() => {
      if (!authed) {
        try { ws.close(4002, "auth timeout"); } catch {}
      }
    }, WS_AUTH_TIMEOUT_MS);
    authTimer.unref();
  }

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    // Guard against JSON primitives that parse successfully but are not objects:
    // JSON.parse("null") -> null, JSON.parse("true") -> true, JSON.parse("123")
    // -> 123. Accessing `.type` on `null` throws TypeError, so reject early.
    if (msg === null || typeof msg !== "object") return;

    // Rate limit: check before processing any message type.
    if (!limiter.allow(msg.type)) {
      log.warn({ ip: clientIp, type: msg.type }, "ws rate limit exceeded");
      safeSend(ws, JSON.stringify({ type: "error", error: "You are sending messages too fast. Please slow down." }));
      return;
    }

    // WebSocket auth: must precede any other command.
    if (msg.type === "auth") {
      if (!siteProtected()) {
        authed = true;
        safeSend(ws, JSON.stringify({ type: "auth_ok" }));
        return;
      }
      const password = typeof msg.password === "string" ? msg.password : "";
      if (timingSafeEqualStr(password.trim(), SITE_PASSWORD)) {
        authed = true;
        if (authTimer) { clearTimeout(authTimer); authTimer = null; }
        safeSend(ws, JSON.stringify({ type: "auth_ok" }));
      } else {
        safeSend(ws, JSON.stringify({ type: "auth_fail", error: "Incorrect password" }));
        try { ws.close(4001, "unauthorized"); } catch {}
      }
      return;
    }

    // Everything below requires successful auth first.
    if (!authed) {
      safeSend(ws, JSON.stringify({ type: "need_auth", error: "Password is required first" }));
      try { ws.close(4001, "unauthorized"); } catch {}
      return;
    }

    if (msg.type === "join") {
      if (session) session.removeClient(ws); // switch session if previously joined
      const tw = (msg.twitch || "").trim().toLowerCase() || null;
      // Kick: accept comma-separated channels (up to 3). Normalize each,
      // dedup while preserving order — the FIRST is the "primary" channel.
      const kkChannels = [...new Set(
        String(msg.kick || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean)
      )].slice(0, 3);
      const kk = kkChannels.length ? kkChannels.join(",") : null;
      // TikTok: strip @ or a full URL (keep the part after the last /)
      let tt = (msg.tiktok || "").trim();
      tt = tt.replace(/^@/, "");
      if (/\/|@/.test(tt)) {
        const m = tt.split(/[/@]/).filter(Boolean).pop();
        tt = m || "";
      }
      tt = tt.toLowerCase() || null;
      // YouTube: accepts a full URL or videoId — we pass it through as-is
      const yt = (msg.youtube || "").trim() || null;
      if (!tw && !kk && !tt && !yt) {
        safeSend(ws, JSON.stringify({ type: "error", error: "Enter at least one username" }));
        return;
      }
      const existing = getOrCreateSession(tw, kk, tt, yt);
      if (!existing) {
        safeSend(ws, JSON.stringify({ type: "error", error: "Server is at capacity. Please try again later." }));
        return;
      }
      session = existing;
      session.addClient(ws, msg.lastId);
      return;
    }

    // Send a chat message (requires a valid Twitch token from the client; we
    // pass it to the server because writing over IRC needs an authenticated
    // connection under the user's name).
    if (msg.type === "send") {
      handleTwitchSend(session, ws, msg);
      return;
    }

    // Reset the session's aggregate stats (subs/gifts/bits). The server clears
    // the maps and broadcasts the empty state to every client of the session;
    // no per-client ack is sent. Ignored before a session is joined. Note:
    // any client that knows the channel names may join a session and reset its
    // stats — sessions are shared by design, keyed only by the channel tuple.
    if (msg.type === "reset_stats") {
      if (session) {
        log.info({ ip: clientIp, session: session.key }, "stats reset by client");
        session.resetStats();
      }
      return;
    }
  });

  ws.on("close", () => {
    if (authTimer) { clearTimeout(authTimer); authTimer = null; }
    if (session) session.removeClient(ws);
    // Release the per-IP connection slot.
    if (clientIp) {
      const cur = ipConnCount.get(clientIp);
      if (cur != null) {
        if (cur <= 1) ipConnCount.delete(clientIp);
        else ipConnCount.set(clientIp, cur - 1);
      }
    }
  });

  // A socket 'error' with no listener re-throws and can crash the process.
  ws.on("error", (err) => {
    log.debug({ ip: clientIp, err: String(err && err.message ? err.message : err) }, "client ws error");
  });
});

// ----------------------------------------------------------------------
// Safety net: a periodic sweep that tallies stuck/idle sessions and closes
// them by force. This is just a precaution in case the grace timer fails for
// some reason (a bug / a dangling reference) — it is not relied upon as the
// normal expiry mechanism, since delaying teardown up to 4 hours would defeat
// its purpose.
// ----------------------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  let sweptSessions = 0;
  for (const session of sessions.values()) {
    // A clientless session that has exceeded the grace period and was not
    // destroyed yet (the grace timer failed for some reason).
    if (session.clients.size === 0 && session.emptySince && now - session.emptySince >= SESSION_GRACE_MS) {
      log.info({ session: session.key }, "sweep: destroying stuck session");
      session.destroy();
      sessions.delete(session.key);
      sweptSessions++;
    }
  }
  // Clean up any OAuth tickets that were never picked up (the client navigated
  // away before calling /api/twitch-token). Each ticket also self-deletes via
  // a 30s setTimeout on creation, so this sweep is a backstop only.
  let sweptTickets = 0;
  for (const [key, ticket] of oauthTickets) {
    if (ticket.exp < now) {
      oauthTickets.delete(key);
      sweptTickets++;
    }
  }
  if (sweptSessions || sweptTickets) {
    log.info({ sweptSessions, sweptTickets, activeSessions: sessions.size }, "sweep complete");
  }
}, SWEEP_INTERVAL_MS).unref();

// Log listen errors (e.g. EADDRINUSE) instead of crashing. Must be attached
// BEFORE server.listen so the error event has a listener when it fires.
server.on("error", (err) => {
  log.error({ err: String(err && err.message ? err.message : err) }, "server error");
});

try {
  server.listen(PORT, () => {
    log.info({ port: PORT, siteProtected: siteProtected(), tiktokKey: !!TIKTOK_SIGN_API_KEY, twitchOAuth: !!TWITCH_CLIENT_ID }, "server started");
  });
} catch (err) {
  // On some platforms EADDRINUSE is thrown synchronously from listen() rather
  // than emitted as an 'error' event; catch it so we log a clear message and
  // exit cleanly instead of bubbling to uncaughtException.
  log.error({ err: String(err && err.message ? err.message : err) }, "server listen error");
  process.exit(1);
}

// ----------------------------------------------------------------------
// Process stability: catch otherwise-fatal errors so a single bad async
// path (a missed `await`, an unhandled rejection) cannot take the whole
// service down. We log and continue rather than exiting — appropriate for
// a long-running server with many independent platform connections.
// ----------------------------------------------------------------------
process.on("unhandledRejection", (reason) => {
  log.error({ err: String(reason && reason.stack ? reason.stack : reason) }, "unhandledRejection (continuing)");
});
process.on("uncaughtException", (err) => {
  log.error({ err: String(err && err.stack ? err.stack : err) }, "uncaughtException (continuing)");
});

// Graceful shutdown on SIGINT/SIGTERM: close the WS server and HTTP server
// so platform sockets and timers tear down cleanly, then exit. A hard 5s
// timeout forces exit if anything keeps the event loop alive.
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, "shutting down");
  // Destroy every session so platform sockets/timers are torn down now.
  for (const s of sessions.values()) {
    try { s.destroy(); } catch {}
  }
  sessions.clear();
  try { wss.close(); } catch {}
  try { server.close(); } catch {}
  const forceExit = setTimeout(() => {
    log.warn("shutdown timed out, forcing exit");
    process.exit(0);
  }, 5000);
  forceExit.unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
