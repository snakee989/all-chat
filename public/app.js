// =====================================================================
// Unified Chat — client
// Responsibilities: site password gate, channel selection, unified WS
// connection, message rendering with emoji/emote support, a toggleable
// Twitch video player, and Twitch OAuth for chatting under the user's
// own name.
// =====================================================================

// ---------- Elements ----------
const siteGate = document.getElementById("site-gate");
const siteGateForm = document.getElementById("site-gate-form");
const sitePasswordInput = document.getElementById("site-password-input");
const siteGateError = document.getElementById("site-gate-error");

const gate = document.getElementById("gate");
const gateForm = document.getElementById("gate-form");
const gateError = document.getElementById("gate-error");
const gateNotice = document.getElementById("gate-notice");
const twitchInput = document.getElementById("twitch-input");
const kickInput = document.getElementById("kick-input");
const tiktokInput = document.getElementById("tiktok-input");
const youtubeInput = document.getElementById("youtube-input");

// Kick multi-channel: the primary input (#kick-input) is always present; up to
// KICK_MAX_CHANNELS-1 extra rows are appended into #kick-fields on demand.
const KICK_MAX_CHANNELS = 3;
const kickFields = document.getElementById("kick-fields");
const kickAddBtn = document.getElementById("kick-add-btn");

// Returns all Kick channel input elements (primary + any extras), in order.
function kickChannelInputs() {
  return [...kickFields.querySelectorAll(".kick-channel-input")];
}

// Updates the "+" button's disabled state based on how many rows exist.
function refreshKickAddBtn() {
  kickAddBtn.disabled = kickChannelInputs().length >= KICK_MAX_CHANNELS;
}

// Adds an extra Kick channel row with a "×" remove button (not shown on the
// primary row, which is always kept). Called when "+" is clicked.
function addKickChannelRow(value = "") {
  if (kickChannelInputs().length >= KICK_MAX_CHANNELS) return;
  const row = document.createElement("div");
  row.className = "kick-input-row";

  const input = document.createElement("input");
  input.type = "text";
  input.className = "kick-channel-input";
  input.placeholder = "extra channel (mirrored)";
  input.value = value;
  input.autocomplete = "off";

  const removeBtn = document.createElement("button");
  removeBtn.type = "button";
  removeBtn.className = "kick-remove-btn";
  removeBtn.textContent = "×";
  removeBtn.title = "Remove channel";
  removeBtn.addEventListener("click", () => {
    row.remove();
    refreshKickAddBtn();
  });

  row.appendChild(input);
  row.appendChild(removeBtn);
  kickFields.appendChild(row);
  refreshKickAddBtn();
}

// Collects, normalizes, dedups (order-preserving) all Kick channel values and
// returns the comma-joined string (or "" if none). Used on submit.
function collectKickChannels() {
  const names = kickChannelInputs()
    .map((i) => i.value.trim().toLowerCase())
    .filter(Boolean);
  const deduped = [...new Set(names)]; // preserve order, drop repeats
  return deduped.slice(0, KICK_MAX_CHANNELS).join(",");
}

// Restores Kick inputs from a saved comma-joined string: first channel goes
// into the primary input, the rest into freshly added extra rows.
function restoreKickChannels(commaJoined) {
  const channels = String(commaJoined || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, KICK_MAX_CHANNELS);
  // Remove any extra rows added previously (keep only the primary).
  for (const row of [...kickFields.querySelectorAll(".kick-input-row")].slice(1)) {
    row.remove();
  }
  kickInput.value = channels[0] || "";
  for (let i = 1; i < channels.length; i++) addKickChannelRow(channels[i]);
  refreshKickAddBtn();
}

kickAddBtn.addEventListener("click", () => addKickChannelRow());

const stage = document.getElementById("stage");
const resetBtn = document.getElementById("reset-btn");
const feed = document.getElementById("feed");

// Stat icons + panel
const statIcons = document.getElementById("stat-icons");
const statSubsBtn = document.getElementById("stat-subs-btn");
const statBitsBtn = document.getElementById("stat-bits-btn");
const statSubsCount = document.getElementById("stat-subs-count");
const statBitsCount = document.getElementById("stat-bits-count");
const statPanel = document.getElementById("stat-panel");
const statPanelTitle = document.getElementById("stat-panel-title");
const statPanelBody = document.getElementById("stat-panel-body");
const statPanelClose = document.getElementById("stat-panel-close");
const statPanelReset = document.getElementById("stat-panel-reset");

const twStatus = document.getElementById("tw-status");
const kkStatus = document.getElementById("kk-status");
const ttStatus = document.getElementById("tt-status");
const ytStatus = document.getElementById("yt-status");
// Live viewer-count spans inside the TW/KK status pills. Hidden when null.
const twViewers = document.getElementById("tw-viewers");
const kkViewers = document.getElementById("kk-viewers");

// Video + composer
const videoArea = document.getElementById("video-area");
const videoHost = document.getElementById("video-host");
const videoToggleBtn = document.getElementById("video-toggle-btn");

const composer = document.getElementById("composer");
const composerStatus = document.getElementById("composer-status");
const composerForm = document.getElementById("composer-form");
const composerText = document.getElementById("composer-text");
const composerSend = document.querySelector(".composer-send");
const connectTwitchBtn = document.getElementById("connect-twitch-btn");

// ---------- State ----------
let socket = null;
let joinedTwitch = null;
let joinedKick = null;
let joinedTikTok = null;
let joinedYouTube = null;
let reconnectAttempts = 0;
let reconnectTimer = null;
// Last id received from the server — key for resuming and de-duplication
let lastSeenId = 0;

// Aggregate stats received from the server (subs / gifts / bits).
// Each is an array of {name, platform, ...metric}. The server sends the full
// snapshot on connect and debounced deltas on every change.
let stats = { subs: [], gifts: [], bits: [] };
// Which stat panel is currently open (null = none). Only one open at a time.
let statsPanelOpen = null;

// Site password (stored locally after the first correct entry)
let sitePassword = "";
// Whether the site gate has been unlocked this session. The chat is never
// shown until this flips to true, so returning visitors (who have a saved
// channel but no saved password) still hit the lock screen first.
let siteUnlocked = false;

// Twitch OAuth: token + login (stored locally; read-only otherwise)
let twitchAccessToken = "";
let twitchLogin = "";

// Deferred OAuth result message to show once the feed is ready.
// We capture it at boot but can only display it after openStage() clears the
// feed, so it renders in both the auto-resume and manual-entry paths.
let pendingOAuthMessage = null;

// Video: handle/id for the player to guarantee full teardown
let videoEmbed = null; // reference to the Twitch embed object
let videoShown = false;

// ---------- localStorage keys ----------
const STORAGE_KEY = "dualchat:join";
const STORAGE_PASSWORD = "dualchat:sitepw";
const STORAGE_TWITCH = "dualchat:twitch";

// =====================================================================
// localStorage helpers
// =====================================================================
function readStoredJoin() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const tw = typeof parsed.twitch === "string" ? parsed.twitch.trim().toLowerCase() : "";
    const kk = typeof parsed.kick === "string" ? parsed.kick.trim().toLowerCase() : "";
    const tt = typeof parsed.tiktok === "string" ? parsed.tiktok.trim().toLowerCase() : "";
    // YouTube is stored as-is (URL/video id) without normalization
    const yt = typeof parsed.youtube === "string" ? parsed.youtube.trim() : "";
    if (!tw && !kk && !tt && !yt) return null;
    return { twitch: tw, kick: kk, tiktok: tt, youtube: yt };
  } catch {
    return null;
  }
}

function writeStoredJoin(twitchUser, kickUser, tiktokUser, youtubeInput) {
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        twitch: twitchUser || "",
        kick: kickUser || "",
        tiktok: tiktokUser || "",
        youtube: youtubeInput || "",
      })
    );
  } catch {
    /* localStorage unavailable (e.g. private mode) — ignore silently */
  }
}

function clearStoredJoin() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

// Shows a banner on the setup screen explaining why saved channels were cleared
// (e.g. "the previous stream has ended"). Cleared on any new join attempt.
function showGateNotice(text) {
  if (!gateNotice) return;
  gateNotice.textContent = text;
  gateNotice.hidden = false;
}

function clearGateNotice() {
  if (!gateNotice) return;
  gateNotice.textContent = "";
  gateNotice.hidden = true;
}

function readStoredPassword() {
  try {
    return localStorage.getItem(STORAGE_PASSWORD) || "";
  } catch {
    return "";
  }
}

function writeStoredPassword(pw) {
  try {
    if (pw) localStorage.setItem(STORAGE_PASSWORD, pw);
    else localStorage.removeItem(STORAGE_PASSWORD);
  } catch {
    /* ignore */
  }
}

function readStoredTwitch() {
  try {
    const raw = localStorage.getItem(STORAGE_TWITCH);
    if (!raw) return null;
    const p = JSON.parse(raw);
    if (!p || typeof p !== "object") return null;
    return {
      access_token: typeof p.access_token === "string" ? p.access_token : "",
      login: typeof p.login === "string" ? p.login : "",
    };
  } catch {
    return null;
  }
}

function writeStoredTwitch(accessToken, login) {
  try {
    if (accessToken) {
      localStorage.setItem(STORAGE_TWITCH, JSON.stringify({ access_token: accessToken, login }));
    } else {
      localStorage.removeItem(STORAGE_TWITCH);
    }
  } catch {
    /* ignore */
  }
}

// =====================================================================
// Phase 1 — Site password gate
// =====================================================================
// The chat is unreachable until the site gate is unlocked. A returning
// visitor with a saved password is auto-unlocked (verified against the
// server); anyone without one is forced to the lock screen — even if they
// already had a saved channel from before the password feature existed.
async function initSiteGate() {
  let protectedFlag = true;
  try {
    const r = await fetch("/api/auth-status", { cache: "no-store" });
    if (r.ok) {
      const j = await r.json();
      protectedFlag = !!j.protected;
    }
  } catch {
    // If the request fails, assume protection is on (fail safe)
  }

  if (!protectedFlag) {
    // Site is not protected — go straight to channel selection
    sitePassword = "";
    enterApp();
    return;
  }

  // Site is protected: is there a saved password? Validate it with the server.
  const saved = readStoredPassword();
  if (saved) {
    const ok = await verifySitePassword(saved);
    if (ok) {
      sitePassword = saved;
      enterApp();
      return;
    }
    // Saved password is no longer valid — clear it
    writeStoredPassword("");
  }

  // No valid password yet: show the lock screen and DO NOT enter the app.
  siteGate.hidden = false;
}

async function verifySitePassword(pw) {
  try {
    const r = await fetch("/api/auth", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password: pw }),
    });
    if (!r.ok) return false;
    const j = await r.json();
    return !!j.ok;
  } catch {
    return false;
  }
}

siteGateForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const pw = sitePasswordInput.value;
  if (!pw) {
    siteGateError.textContent = "Please enter the password.";
    siteGateError.hidden = false;
    return;
  }
  const ok = await verifySitePassword(pw);
  if (!ok) {
    siteGateError.textContent = "Incorrect password.";
    siteGateError.hidden = false;
    return;
  }
  siteGateError.hidden = true;
  // Persist in the browser so it is recognized automatically next time
  writeStoredPassword(pw);
  sitePassword = pw;
  siteGate.hidden = true;
  enterApp();
});

// Lock the app again: hide the chat and username screens, force the lock.
// Called when the server rejects our password over WS (e.g. it changed on
// the server). Nobody can bypass the gate from a stale session.
function lockApp() {
  siteUnlocked = false;
  sitePassword = "";
  writeStoredPassword("");
  // Hide the chat + username screens first so only the lock remains
  stage.hidden = true;
  gate.hidden = true;
  destroyVideo();
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  reconnectAttempts = 0;
  if (socket) {
    try {
      socket.close();
    } catch {}
    socket = null;
  }
  joinedTwitch = null;
  joinedKick = null;
  joinedTikTok = null;
  joinedYouTube = null;
  lastSeenId = 0;
  feed.innerHTML = "";
  twitchInput.value = "";
  kickInput.value = "";
  tiktokInput.value = "";
  youtubeInput.value = "";
  composer.hidden = true;
  videoToggleBtn.hidden = true;
  videoArea.hidden = true;
  clearStoredJoin();
  siteGate.hidden = false;
}

// =====================================================================
// Phase 2 — Channel selection (username gate) + resume
// =====================================================================
// enterApp() is the single entry point into the app body. It is only ever
// reached after the site gate is unlocked (or if the site is unprotected).
// It shows the username screen, prefills a saved channel, and — if a
// channel exists — auto-resumes directly into the chat.
function enterApp() {
  siteUnlocked = true;
  gate.hidden = false;

  // Prefill + auto-resume if a channel was previously saved
  const saved = readStoredJoin();
  if (saved) {
    twitchInput.value = saved.twitch;
    restoreKickChannels(saved.kick);
    tiktokInput.value = saved.tiktok;
    youtubeInput.value = saved.youtube;
    openStage(saved.twitch, saved.kick, saved.tiktok, saved.youtube);
  }
}

gateForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const twitchUser = twitchInput.value.trim().toLowerCase();
  const kickUser = collectKickChannels();
  const tiktokUser = tiktokInput.value.trim().toLowerCase();
  const youtubeUser = youtubeInput.value.trim();

  if (!twitchUser && !kickUser && !tiktokUser && !youtubeUser) {
    gateError.textContent = "Enter at least one username (Twitch, Kick, TikTok, or YouTube).";
    gateError.hidden = false;
    return;
  }
  gateError.hidden = true;
  clearGateNotice();
  writeStoredJoin(twitchUser, kickUser, tiktokUser, youtubeUser);
  openStage(twitchUser, kickUser, tiktokUser, youtubeUser);
});

function openStage(twitchUser, kickUser, tiktokUser, youtubeUser) {
  gate.hidden = true;
  stage.hidden = false;
  feed.innerHTML = "";
  lastSeenId = 0;

  joinedTwitch = twitchUser || null;
  joinedKick = kickUser || null;
  joinedTikTok = tiktokUser || null;
  joinedYouTube = youtubeUser || null;

  // Reset stats on a new stage; show the icons only for platforms that emit
  // events we track (Twitch + Kick). YouTube/TikTok-only sessions hide them.
  stats = { subs: [], gifts: [], bits: [] };
  statsPanelOpen = null;
  statPanel.hidden = true;
  refreshStatsUI();
  statIcons.hidden = !(joinedTwitch || joinedKick);

  if (!twitchUser) twStatus.textContent = "not set";
  if (!kickUser) kkStatus.textContent = "not set";
  if (!tiktokUser) ttStatus.textContent = "not set";
  if (!youtubeUser) ytStatus.textContent = "not set";

  // Composer + video button appear only for Twitch sessions
  if (twitchUser) {
    composer.hidden = false;
    videoToggleBtn.hidden = false;
    initTwitchAuthUI();
  } else {
    composer.hidden = true;
    videoToggleBtn.hidden = true;
    videoArea.hidden = true;
  }

  connectServer();

  // Surface any OAuth result (from the redirect back from Twitch) now that
  // the feed is mounted. openStage() cleared the feed above, so the message
  // is visible in both the auto-resume and manual-entry paths.
  if (pendingOAuthMessage) {
    addSystemMessage(pendingOAuthMessage);
    pendingOAuthMessage = null;
  }
}

resetBtn.addEventListener("click", () => {
  // Tear everything down and return to the username screen; clear saved channel
  destroyVideo();
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  reconnectAttempts = 0;
  if (socket) {
    try {
      socket.close();
    } catch {}
    socket = null;
  }
  joinedTwitch = null;
  joinedKick = null;
  joinedTikTok = null;
  joinedYouTube = null;
  lastSeenId = 0;
  feed.innerHTML = "";
  twitchInput.value = "";
  kickInput.value = "";
  tiktokInput.value = "";
  youtubeInput.value = "";
  composer.hidden = true;
  videoToggleBtn.hidden = true;
  videoArea.hidden = true;
  statIcons.hidden = true;
  statPanel.hidden = true;
  statsPanelOpen = null;
  clearStoredJoin();
  clearGateNotice();
  gate.hidden = false;
  stage.hidden = true;
});

// =====================================================================
// Server connection (single WS) — with password authentication
// =====================================================================
function wsUrl() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}/ws`;
}

function connectServer() {
  // Clean up any previous connection
  if (socket) {
    try {
      socket.close();
    } catch {}
    socket = null;
  }

  socket = new WebSocket(wsUrl());

  socket.addEventListener("open", () => {
    reconnectAttempts = 0;
    // Authenticate with the password first (the server rejects any other
    // command before auth when protection is enabled)
    socket.send(JSON.stringify({ type: "auth", password: sitePassword }));
  });

  socket.addEventListener("message", (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    // Auth responses
    if (msg.type === "auth_ok") {
      if (joinedTwitch) twStatus.textContent = "connecting...";
      if (joinedKick) kkStatus.textContent = "connecting...";
      if (joinedTikTok) ttStatus.textContent = "connecting...";
      if (joinedYouTube) ytStatus.textContent = "connecting...";
      // Send the last id so we only receive what we missed (if still buffered)
      socket.send(
        JSON.stringify({
          type: "join",
          twitch: joinedTwitch,
          kick: joinedKick,
          tiktok: joinedTikTok,
          youtube: joinedYouTube,
          lastId: lastSeenId,
        })
      );
      return;
    }
    if (msg.type === "auth_fail" || msg.type === "need_auth") {
      // Password is no longer valid (e.g. changed on the server) — re-lock
      addSystemMessage("Your saved password is no longer valid. Please re-enter it.");
      lockApp();
      return;
    }

    if (msg.type === "snapshot") {
      // Determine whether this is a resume (we had a lastId) or a fresh connect
      const isResume = lastSeenId > 0;
      if (!isResume) feed.innerHTML = "";

      for (const m of msg.backlog) {
        if (m.id > lastSeenId) {
          if (m.type === "event") {
            addEventMessage(m.platform, m.kind, m.who, m.text, m.message, false, m.sharedFrom);
          } else {
            addChatMessage(m.platform, m.username, m.content, m.color, m.emotes, false, m.bits, m.sharedFrom);
          }
          lastSeenId = m.id;
        }
      }

      // Advance the cursor to whatever the server reached (even if nothing new)
      if (typeof msg.lastId === "number" && msg.lastId > lastSeenId) {
        lastSeenId = msg.lastId;
      }

      if (joinedTwitch) twStatus.textContent = msg.twitchStatus;
      if (joinedKick) kkStatus.textContent = msg.kickStatus;
      if (joinedTikTok) ttStatus.textContent = msg.tiktokStatus;
      if (joinedYouTube) ytStatus.textContent = msg.youtubeStatus;

      // Adopt the server's live viewer counts (null = unknown/offline -> hidden)
      setViewerCount(twViewers, joinedTwitch ? msg.twitchViewers : null);
      setViewerCount(kkViewers, joinedKick ? msg.kickViewers : null);

      if (msg.tiktokError) addSystemMessage("TikTok: " + msg.tiktokError);
      if (msg.kickError) addSystemMessage("Kick: " + msg.kickError);
      if (msg.twitchError) addSystemMessage("Twitch: " + msg.twitchError);
      if (msg.youtubeError) addSystemMessage("YouTube: " + msg.youtubeError);

      // Adopt the server's stats snapshot (full state on connect/reconnect)
      if (msg.stats) applyStats(msg.stats);
      return;
    }

    if (msg.type === "stats") {
      applyStats(msg.stats);
      return;
    }

    if (msg.type === "status") {
      if (msg.platform === "twitch" && joinedTwitch) twStatus.textContent = msg.status;
      if (msg.platform === "kick" && joinedKick) kkStatus.textContent = msg.status;
      if (msg.platform === "tiktok" && joinedTikTok) ttStatus.textContent = msg.status;
      if (msg.platform === "youtube" && joinedYouTube) ytStatus.textContent = msg.status;
      // Surface the reason for error/ended states as a system message so the
      // viewer understands why a platform went quiet (e.g. "YouTube: stream ended").
      if (msg.error && (msg.status === "error" || msg.status === "ended")) {
        addSystemMessage(platformLabel(msg.platform) + ": " + msg.error);
      }
      return;
    }

    // Live viewer-count update for Twitch/Kick. The server polls each platform
    // and broadcasts the current count (or null to clear it). We only show it
    // when the user is joined to that platform.
    if (msg.type === "viewers") {
      if (msg.platform === "twitch" && joinedTwitch) setViewerCount(twViewers, msg.count);
      else if (msg.platform === "kick" && joinedKick) setViewerCount(kkViewers, msg.count);
      return;
    }

    if (msg.type === "chat") {
      // The monotonic id is the reference: ignore anything older/duplicate
      if (typeof msg.id === "number" && msg.id <= lastSeenId) return;
      if (typeof msg.id === "number") lastSeenId = msg.id;
      addChatMessage(msg.platform, msg.username, msg.content, msg.color, msg.emotes, true, msg.bits, msg.sharedFrom);
      return;
    }

    if (msg.type === "event") {
      if (typeof msg.id === "number" && msg.id <= lastSeenId) return;
      if (typeof msg.id === "number") lastSeenId = msg.id;
      addEventMessage(msg.platform, msg.kind, msg.who, msg.text, msg.message, true, msg.sharedFrom);
      return;
    }

    if (msg.type === "send_result") {
      handleSendResult(msg);
      return;
    }

    if (msg.type === "session_ended") {
      // The server destroyed our previous session because the stream ended, and
      // is refusing to silently resume. Tear down like the "Change" button, but
      // also surface why on the setup screen so the viewer knows to re-enter the
      // channels they want to follow now (the previous merged setup is gone).
      destroyVideo();
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      reconnectAttempts = 0;
      if (socket) {
        try {
          socket.close();
        } catch {}
        socket = null;
      }
      joinedTwitch = null;
      joinedKick = null;
      joinedTikTok = null;
      joinedYouTube = null;
      lastSeenId = 0;
      feed.innerHTML = "";
      twitchInput.value = "";
      kickInput.value = "";
      tiktokInput.value = "";
      youtubeInput.value = "";
      composer.hidden = true;
      videoToggleBtn.hidden = true;
      videoArea.hidden = true;
      statIcons.hidden = true;
      statPanel.hidden = true;
      statsPanelOpen = null;
      clearStoredJoin();
      gate.hidden = false;
      stage.hidden = true;
      showGateNotice("The previous stream has ended. Enter the channels you want to follow.");
      return;
    }

    if (msg.type === "error") {
      addSystemMessage(msg.error || "An unknown error occurred.");
    }
  });

  socket.addEventListener("close", () => {
    if (joinedTwitch) twStatus.textContent = "reconnecting";
    if (joinedKick) kkStatus.textContent = "reconnecting";
    if (joinedTikTok) ttStatus.textContent = "reconnecting";
    if (joinedYouTube) ytStatus.textContent = "reconnecting";
    // Hide the viewer counts until the connection is back — the server is the
    // source of truth and we don't want a frozen number next to "reconnecting".
    setViewerCount(twViewers, null);
    setViewerCount(kkViewers, null);
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    // An error is usually followed by close; we just surface the reconnect state
    if (joinedTwitch) twStatus.textContent = "error";
    if (joinedKick) kkStatus.textContent = "error";
    if (joinedTikTok) ttStatus.textContent = "error";
    if (joinedYouTube) ytStatus.textContent = "error";
  });
}

function scheduleReconnect() {
  // Exponential backoff, capped at 10 seconds. Give up after a bounded number
  // of attempts so a permanently-unreachable server doesn't loop forever; the
  // user can retry by interacting with the page (which resets the counter).
  const MAX_RECONNECT_ATTEMPTS = 20;
  if (reconnectTimer) return;
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    addSystemMessage("Connection lost. Reconnect manually to retry.");
    return;
  }
  reconnectAttempts++;
  const delay = Math.min(10000, 1000 * Math.pow(2, reconnectAttempts - 1));
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (joinedTwitch || joinedKick || joinedTikTok || joinedYouTube) connectServer();
  }, delay);
}

// Returning to the page after backgrounding: reconnect immediately if dropped
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (!joinedTwitch && !joinedKick && !joinedTikTok && !joinedYouTube) return;
  // If the socket is closed or closing, reconnect now
  if (!socket || socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    reconnectAttempts = 0;
    connectServer();
  }
});

// =====================================================================
// Feed rendering
// =====================================================================
function addSystemMessage(text) {
  const row = document.createElement("div");
  row.className = "system-row";
  row.textContent = text;
  feed.appendChild(row);
  scrollFeedIfNeeded();
}

// Formats and shows the live viewer count inside a status pill. `count` is null
// when the platform is offline or the count is unknown — in that case the span
// is hidden so the pill just shows its status text. Large counts are grouped
// with thousands separators (e.g. 12,345) for readability.
function setViewerCount(span, count) {
  if (!span) return;
  if (typeof count === "number" && count >= 0) {
    span.textContent = count.toLocaleString();
    span.hidden = false;
  } else {
    span.textContent = "";
    span.hidden = true;
  }
}

// Renders a Twitch platform event (sub / resub / gift / communitygift / raid)
// as a distinct, highlighted row so it stands out from regular chat. The kind
// picks the icon + accent; an optional attached user message (resub/gift) is
// shown as a smaller line beneath the summary.
function addEventMessage(platform, kind, who, text, message, autoscroll, sharedFrom) {
  const wasAtBottom = autoscroll
    ? feed.scrollTop + feed.clientHeight >= feed.scrollHeight - 40
    : false;

  const row = document.createElement("div");
  row.className = `event-row event-${kind || "sub"}`;

  const iconWrap = document.createElement("span");
  iconWrap.className = "event-icon";
  iconWrap.textContent = eventIconFor(kind);

  // Shared Chat badge: shown when this event was mirrored from a linked room.
  // Placed between the icon and the body so it reads "★ ↪ninja  User subbed!".
  let sharedBadge = null;
  if (sharedFrom) {
    sharedBadge = document.createElement("span");
    sharedBadge.className = "shared-badge";
    sharedBadge.textContent = `↪ ${sharedFrom}`;
  }

  const body = document.createElement("span");
  body.className = "event-body";

  const label = document.createElement("span");
  label.className = "event-label";
  label.textContent = text || `${who || "Someone"} ${kind || ""}`;

  body.appendChild(label);

  // Optional attached user message (e.g. a resub message the viewer typed)
  if (message) {
    const msgLine = document.createElement("span");
    msgLine.className = "event-message";
    msgLine.textContent = message;
    body.appendChild(msgLine);
  }

  row.appendChild(iconWrap);
  if (sharedBadge) row.appendChild(sharedBadge);
  row.appendChild(body);
  feed.appendChild(row);

  if (autoscroll && wasAtBottom) feed.scrollTop = feed.scrollHeight;

  while (feed.children.length > 500) feed.removeChild(feed.firstChild);
}

// Picks a glyph for the event kind. Keeps it text-based so no extra assets are
// needed; the CSS color-codes each kind so the shape is enough to tell apart.
function eventIconFor(kind) {
  switch (kind) {
    case "sub":
    case "resub":
      return "★"; // star — subscription
    case "gift":
    case "communitygift":
      return "🎁"; // gift
    case "follow":
      return "❤"; // heart — follow (Kick)
    case "raid":
      return "⚡"; // raid
    default:
      return "★";
  }
}

// =====================================================================
// Stats panels (subs / bits)
// =====================================================================
// The server tracks per-name aggregates with the platform each came from. We
// store them locally and re-render the count badges + the open panel whenever
// a new snapshot/delta arrives.

function applyStats(next) {
  if (!next || typeof next !== "object") return;
  stats = {
    subs: Array.isArray(next.subs) ? next.subs : [],
    gifts: Array.isArray(next.gifts) ? next.gifts : [],
    bits: Array.isArray(next.bits) ? next.bits : [],
  };
  refreshStatsUI();
  // If a panel is open, refresh its contents too
  if (statsPanelOpen) renderStatsPanel(statsPanelOpen);
}

// Updates the count badges on the header icons. The subs count combines
// subscribers + gifters (both are "subscription" activity); bits shows its
// own total.
function refreshStatsUI() {
  statSubsCount.textContent = String(stats.subs.length + stats.gifts.length);
  statBitsCount.textContent = String(stats.bits.length);
}

// Opens the panel for the given kind, or closes it if already open. Only one
// panel is ever open at a time; clicking a different icon switches over.
function openStatsPanel(kind) {
  if (statsPanelOpen === kind) {
    // Same icon clicked again — close
    closeStatsPanel();
    return;
  }
  statsPanelOpen = kind;
  // Highlight the active icon
  [statSubsBtn, statBitsBtn].forEach((b) =>
    b.classList.toggle("is-active", b === kindButton(kind))
  );
  renderStatsPanel(kind);
  statPanel.hidden = false;
}

function closeStatsPanel() {
  statsPanelOpen = null;
  statPanel.hidden = true;
  [statSubsBtn, statBitsBtn].forEach((b) => b.classList.remove("is-active"));
}

function kindButton(kind) {
  return kind === "subs" ? statSubsBtn : statBitsBtn;
}

// Builds the inner content of the panel for the given kind.
function renderStatsPanel(kind) {
  // Title + body differ per kind
  if (kind === "subs") {
    statPanelTitle.textContent = "Subscriptions & Gifts";
    statPanelBody.innerHTML = "";
    statPanelBody.appendChild(buildStatsSection("Subscribers", stats.subs, (e) =>
      e.months ? `${e.months} mo` : ""
    ));
    statPanelBody.appendChild(buildStatsSection("Gifters", stats.gifts, (e) =>
      `${e.count} gifted`
    ));
    return;
  }
  if (kind === "bits") {
    statPanelTitle.textContent = "Bits";
    statPanelBody.innerHTML = "";
    statPanelBody.appendChild(buildStatsSection("Cheerers", stats.bits, (e) =>
      `${e.total} bits`
    ));
    return;
  }
}

// Builds one titled section (e.g. "Subscribers") as a list of name rows. Each
// row shows the platform badge + name + a metric string. `metric` returns the
// small trailing text (or "" for none).
function buildStatsSection(title, entries, metric) {
  const wrap = document.createElement("div");
  wrap.className = "stat-section";

  const head = document.createElement("div");
  head.className = "stat-section-title";
  head.textContent = `${title} (${entries.length})`;
  wrap.appendChild(head);

  if (entries.length === 0) {
    const empty = document.createElement("div");
    empty.className = "stat-empty";
    empty.textContent = "None yet";
    wrap.appendChild(empty);
    return wrap;
  }

  const list = document.createElement("div");
  list.className = "stat-list";
  for (const e of entries) {
    const row = document.createElement("div");
    row.className = "stat-entry";

    const badge = document.createElement("span");
    badge.className = `stat-entry-badge ${e.platform || ""}`;
    badge.textContent = e.platform === "tw" ? "TW" : e.platform === "kk" ? "KK" : "?";

    const name = document.createElement("span");
    name.className = "stat-entry-name";
    name.textContent = e.name || "Unknown";

    row.appendChild(badge);
    row.appendChild(name);

    const m = metric(e);
    if (m) {
      const val = document.createElement("span");
      val.className = "stat-entry-value";
      val.textContent = m;
      row.appendChild(val);
    }
    list.appendChild(row);
  }
  wrap.appendChild(list);
  return wrap;
}

// Icon click handlers + close button
statSubsBtn.addEventListener("click", () => openStatsPanel("subs"));
statBitsBtn.addEventListener("click", () => openStatsPanel("bits"));
statPanelClose.addEventListener("click", closeStatsPanel);

// Reset the session's aggregate stats (subs/gifts/bits). The server clears its
// maps and broadcasts the empty state back to every client of the session, so
// no ack is expected — the next "stats" message will refresh this UI. Mirrors
// the guarded socket.send pattern used by the Twitch chat composer.
statPanelReset.addEventListener("click", () => {
  if (!confirm("Reset all stats (subscriptions, gifts, bits) to zero? This affects everyone viewing this session.")) return;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    addSystemMessage("Connection lost — could not reset stats.");
    return;
  }
  socket.send(JSON.stringify({ type: "reset_stats" }));
});

// Shared row builder for live messages and backlog.
// autoscroll only moves to the bottom if the user was already near it (new msgs).
// bits (optional number): Twitch cheer amount — renders a bits badge on the row.
// sharedFrom (optional string): Twitch Shared Chat origin channel login — renders
//   a "↪ name" badge signalling the message was mirrored from a linked room.
function addChatMessage(platform, username, content, color, emotes, autoscroll, bits, sharedFrom) {
  const wasAtBottom = autoscroll
    ? feed.scrollTop + feed.clientHeight >= feed.scrollHeight - 40
    : false;

  const row = document.createElement("div");
  row.className = "msg-row";
  if (Number.isFinite(bits) && bits > 0) row.classList.add("has-bits");

  const icon = document.createElement("span");
  icon.className = `msg-icon ${platform}`;
  icon.textContent = platform === "tw" ? "TW" : platform === "kk" ? "KK" : platform === "tt" ? "TT" : "YT";

  // Bits badge: shown only for Twitch cheers. Placed right after the platform
  // icon so the user sees "TW ◆500 username: message" at a glance.
  let bitsBadge = null;
  if (Number.isFinite(bits) && bits > 0) {
    bitsBadge = document.createElement("span");
    bitsBadge.className = "bits-badge";
    bitsBadge.textContent = String(bits);
  }

  // Shared Chat badge: shown when this message was mirrored from a linked room.
  // Reads "↪ channelName" so viewers can tell foreign messages apart at a glance.
  let sharedBadge = null;
  if (sharedFrom) {
    sharedBadge = document.createElement("span");
    sharedBadge.className = "shared-badge";
    sharedBadge.textContent = `↪ ${sharedFrom}`;
  }

  const text = document.createElement("span");
  text.className = "msg-text";

  const user = document.createElement("span");
  user.className = "msg-user";
  // Setting style.color as a DOM property is safe from XSS — the browser
  // simply ignores invalid values. No HTML-escaping is needed (or correct)
  // here; escapeAttr was a no-op misuse.
  user.style.color = color;
  user.textContent = username;

  const contentWrap = document.createElement("span");
  contentWrap.className = "msg-content";
  renderContentInto(contentWrap, content, emotes, platform);

  text.appendChild(user);
  text.appendChild(document.createTextNode(" "));
  text.appendChild(contentWrap);

  // Order: platform icon, shared origin, bits, then the text. So a shared
  // cheered message reads "TW  ↪ninja  ◆500  user: message".
  row.appendChild(icon);
  if (sharedBadge) row.appendChild(sharedBadge);
  if (bitsBadge) row.appendChild(bitsBadge);
  row.appendChild(text);
  feed.appendChild(row);

  if (autoscroll && wasAtBottom) feed.scrollTop = feed.scrollHeight;

  // keep the DOM light during long sessions
  while (feed.children.length > 500) feed.removeChild(feed.firstChild);
}

// Fills an element with the message body: plain text after escaping, and
// emotes as images. We build via DOM (not innerHTML) for emotes so onerror
// handlers and safe escaping both work reliably.
function renderContentInto(el, content, emotes, platform) {
  content = String(content ?? "");
  if (!Array.isArray(emotes) || emotes.length === 0) {
    el.textContent = content;
    return;
  }

  // Twitch indexes emote positions by codepoints (Unicode characters), while
  // JavaScript indexes strings by UTF-16 units. Any emoji before the emote
  // (a surrogate pair = one codepoint but two UTF-16 units) shifts the index,
  // so parts of the text disappear or the emote overlaps adjacent text. We
  // translate the ranges to UTF-16 first. Kick gives positions in UTF-16
  // already (from a regex), so no translation is needed there.
  const needMap = platform === "tw";
  const cpMap = needMap ? buildCodepointMap(content) : null;

  // Convert each emote entry to a UTF-16 half-open range, skipping invalid ones
  const sorted = emotes
    .map((e) => {
      if (!e || !Number.isFinite(e.start) || !Number.isFinite(e.end)) return null;
      if (e.start < 0 || e.end < e.start) return null;
      if (needMap) {
        // cpMap.length - 1 = number of codepoints (= end sentinel position)
        if (e.start >= cpMap.length - 1 || e.end >= cpMap.length - 1) return null;
        return { start: cpMap[e.start], end: cpMap[e.end + 1] - 1, id: e.id };
      }
      if (e.end >= content.length) return null;
      return e;
    })
    .filter(Boolean)
    .slice()
    .sort((a, b) => a.start - b.start);

  let cursor = 0;
  let applied = false;
  for (const e of sorted) {
    if (e.start < cursor) continue; // overlap — skip
    // Text before the emote
    if (e.start > cursor) {
      el.appendChild(document.createTextNode(content.slice(cursor, e.start)));
    }
    const rawText = content.slice(e.start, e.end + 1);
    el.appendChild(buildEmoteImg(e.id, rawText, platform));
    cursor = e.end + 1;
    applied = true;
  }
  // Whatever remains after the last emote
  if (cursor < content.length) {
    el.appendChild(document.createTextNode(content.slice(cursor)));
  }
  if (!applied) {
    // No valid emote applied — fall back to plain text
    el.textContent = content;
  }
}

// Builds a map translating codepoint positions to UTF-16 unit positions.
// map[i] = UTF-16 start offset of codepoint i.
// map[numberOfCodepoints] = content.length (end sentinel).
// This is required because Twitch indexes emotes by codepoints, not UTF-16.
function buildCodepointMap(str) {
  const map = [];
  let cp = 0;
  for (let i = 0; i < str.length; ) {
    map[cp] = i;
    const code = str.charCodeAt(i);
    // High surrogate (emoji / chars outside BMP) = two UTF-16 units, one codepoint
    i += code >= 0xd800 && code <= 0xdbff ? 2 : 1;
    cp++;
  }
  map[cp] = str.length;
  return map;
}

// Builds an <img> for an emote with a safe fallback: on image error we show
// the raw text. For Twitch emotes we try several URL forms: emotesv2
// (subscriber/normal) then emotesv1 (legacy) then the legacy static-cdn —
// some subscriber emotes have an id that only exists on v1. If all fail we
// show the raw text.
function buildEmoteImg(id, rawText, platform) {
  // For Kick, the in-text form is [emote:id:name] — show only the name, not the form
  let displayText = rawText;
  if (platform === "kk") {
    const m = rawText.match(/^\[emote:\d+:([^\]]+)\]$/);
    if (m) displayText = m[1];
  }

  const numericId = Number(id);
  const valid = Number.isFinite(numericId) && numericId > 0;

  // If id is not a valid number, an <img> is pointless — show text directly
  if (!valid) {
    const span = document.createElement("span");
    span.textContent = displayText;
    return span;
  }

  const img = document.createElement("img");
  img.className = "emote";
  img.alt = displayText;
  img.title = displayText;
  img.loading = "lazy";
  img.decoding = "async";

  // Candidate emote image URLs; we try them in order until one works.
  let urls;
  if (platform === "tw") {
    urls = [
      `https://static-cdn.jtvnw.net/emoticons/v2/${numericId}/default/dark/1.0`,
      `https://static-cdn.jtvnw.net/emoticons/v2/${numericId}/default/light/1.0`,
      `https://static-cdn.jtvnw.net/emoticons/v1/${numericId}/1.0`,
      `https://static-cdn.jtvnw.net/emoticons/v1/${numericId}/2.0`,
      `https://static-cdn.jtvnw.net/emoticons/v1/${numericId}/3.0`,
    ];
  } else {
    urls = [`https://files.kick.com/emotes/${numericId}/fullsize`];
  }
  let urlIdx = 0;
  img.src = urls[0];

  // Two-stage fallback:
  //  (1) try the next URL in the list if one exists.
  //  (2) all URLs exhausted — replace the image with the raw text.
  img.addEventListener("error", () => {
    urlIdx++;
    if (urlIdx < urls.length) {
      img.src = urls[urlIdx];
      return;
    }
    const textNode = document.createTextNode(displayText);
    img.replaceWith(textNode);
  });
  return img;
}

function scrollFeedIfNeeded() {
  feed.scrollTop = feed.scrollHeight;
}

// =====================================================================
// Twitch video player (embed) — toggleable with full teardown
// =====================================================================
// "Show Video" builds a Twitch player iframe at the top.
// "Hide Video" destroys it entirely (removes the iframe + stops loading) to
// save bandwidth and device resources on mobile.
//
// IMPORTANT (ads / authenticated viewing):
// The official Twitch embed (player.twitch.tv) does NOT accept an OAuth
// token via query params or any API. Ad suppression in the embed depends
// solely on the VIEWER being logged into twitch.tv in their own browser
// (their session cookies) — as a subscriber or Turbo user. Our chat OAuth
// token (chat:read/chat:edit) only authenticates IRC for sending messages;
// it cannot make the embed ad-free. So:
//   - Authenticated (connected) user: the embed plays normally. If they are
//     also logged into twitch.tv as a subscriber/Turbo, Twitch serves the
//     ad-free variant automatically (independent of us).
//   - Guest: the embed plays normally too, but Twitch shows the normal
//     ads it imposes on non-subscribers.
// We reflect the user's connection status in the composer honestly; we do
// not pretend the token removes ads, because it does not.
videoToggleBtn.addEventListener("click", () => {
  if (videoShown) {
    destroyVideo();
  } else {
    showVideo();
  }
});

function showVideo() {
  if (!joinedTwitch) return;
  videoShown = true;
  videoArea.hidden = false;
  videoToggleBtn.textContent = "Hide Video";
  videoToggleBtn.classList.add("is-active");

  // We build the iframe directly (simpler and lighter than loading the full
  // Twitch Embed JS library, and it needs no parent domain registered with
  // the Twitch embed settings). We pass parent=current host.
  const parent = location.hostname || "localhost";
  const channel = encodeURIComponent(joinedTwitch);
  const src = `https://player.twitch.tv/?channel=${channel}&parent=${encodeURIComponent(parent)}&muted=false&autoplay=true`;
  const iframe = document.createElement("iframe");
  iframe.src = src;
  iframe.allow = "autoplay; fullscreen; encrypted-media";
  iframe.allowFullscreen = true;
  iframe.title = "Twitch stream";
  // Use a class so it is easy to remove later
  iframe.className = "video-player";
  videoHost.appendChild(iframe);
  videoEmbed = { iframe };
}

function destroyVideo() {
  videoShown = false;
  videoArea.hidden = true;
  videoToggleBtn.textContent = "Show Video";
  videoToggleBtn.classList.remove("is-active");
  // Removing the iframe stops the video load and destroys the player fully
  if (videoEmbed && videoEmbed.iframe) {
    videoEmbed.iframe.src = "about:blank"; // halts all requests immediately
    try { videoEmbed.iframe.remove(); } catch {}
    videoEmbed = null;
  }
  videoHost.innerHTML = "";
}

// =====================================================================
// Twitch OAuth — connect your account to chat under your own name
// =====================================================================
function initTwitchAuthUI() {
  // If a token is saved, use it directly
  const stored = readStoredTwitch();
  if (stored && stored.access_token) {
    twitchAccessToken = stored.access_token;
    twitchLogin = stored.login || "";
    setConnectedUI();
    return;
  }
  setDisconnectedUI();
}

function setConnectedUI() {
  connectTwitchBtn.textContent = twitchLogin
    ? `Connected as @${twitchLogin} (disconnect)`
    : "Disconnect Twitch";
  connectTwitchBtn.classList.add("is-connected");
  composerForm.hidden = false;
  // Honest status: being connected lets you CHAT. It does NOT remove embed
  // ads — those depend on the viewer's own twitch.tv login, not this token.
  composerStatus.textContent = twitchLogin
    ? `Chatting as @${twitchLogin}`
    : "Connected";
}

function setDisconnectedUI() {
  twitchAccessToken = "";
  twitchLogin = "";
  connectTwitchBtn.textContent = "Connect Twitch to chat";
  connectTwitchBtn.classList.remove("is-connected");
  composerForm.hidden = true;
  // Honest status: anonymous = read-only chat, and the embed shows ads
  composerStatus.textContent = "Anonymous guest (read only) — embed may show ads";
}

connectTwitchBtn.addEventListener("click", () => {
  if (twitchAccessToken) {
    // Disconnect: clear stored data and reset the UI
    writeStoredTwitch("", "");
    setDisconnectedUI();
    return;
  }
  // Start OAuth: the server generates and verifies the state parameter (stored
  // in a short-lived HttpOnly cookie) to prevent CSRF. The client just kicks
  // off the flow.
  location.href = "/auth/twitch";
});

// After OAuth returns from Twitch: the server stored the token in a short-lived
// cookie (one-time use). We ask this endpoint at boot, and if a token exists we
// pick it up and persist it locally. This is more reliable than reading the URL
// fragment, which may not survive proxies/HTTPS.
async function pickupTwitchOAuth() {
  try {
    const r = await fetch("/api/twitch-token", { cache: "no-store" });
    if (!r.ok) return false;
    const j = await r.json();
    if (j.ok && j.access_token) {
      twitchAccessToken = j.access_token;
      twitchLogin = j.login || "";
      writeStoredTwitch(j.access_token, twitchLogin);
      return true;
    }
  } catch {
    /* ignore */
  }
  return false;
}

// Checks for a deferred OAuth error (from a cookie) to show the user.
async function pickupTwitchOAuthError() {
  try {
    const r = await fetch("/api/twitch-oauth-error", { cache: "no-store" });
    if (!r.ok) return null;
    const j = await r.json();
    return j.error || null;
  } catch {
    return null;
  }
}

// Send a chat message
composerForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const content = composerText.value;
  if (!content.trim()) return;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    addSystemMessage("Connection lost — could not send.");
    return;
  }
  if (!twitchAccessToken) {
    addSystemMessage("Connect your Twitch account first to chat.");
    return;
  }
  composerSend.disabled = true;
  socket.send(
    JSON.stringify({
      type: "send",
      access_token: twitchAccessToken,
      content,
    })
  );
  composerText.value = "";
});

function handleSendResult(msg) {
  composerSend.disabled = false;
  if (!msg.ok) {
    addSystemMessage("Could not send: " + (msg.error || "unknown error"));
    // If the token is no longer valid, reset the UI to "not connected"
    if (/not valid|invalid|unauthorized|token/i.test(String(msg.error || ""))) {
      writeStoredTwitch("", "");
      setDisconnectedUI();
    }
  }
}

// =====================================================================
// Helpers
// =====================================================================
function platformLabel(platform) {
  return platform === "twitch"
    ? "Twitch"
    : platform === "kick"
    ? "Kick"
    : platform === "tiktok"
    ? "TikTok"
    : platform === "youtube"
    ? "YouTube"
    : platform;
}

// =====================================================================
// Boot
// =====================================================================
(async function boot() {
  // Pick up the OAuth result if we are returning from Twitch (server put it
  // in a short-lived cookie). We capture it here but only DISPLAY it after
  // the feed is ready (inside openStage), so it shows in both the auto-resume
  // and manual-entry paths.
  const justConnected = await pickupTwitchOAuth();
  const oauthError = await pickupTwitchOAuthError();

  if (justConnected) {
    pendingOAuthMessage = twitchLogin
      ? `Twitch account connected: @${twitchLogin}`
      : "Twitch account connected.";
  } else if (oauthError) {
    const reasons = {
      nocode: "No authorization code arrived from Twitch",
      notconfigured: "OAuth is not configured on the server (TWITCH_CLIENT_ID/SECRET)",
      network: "Could not reach Twitch's servers",
      token: "Twitch rejected the code-for-token exchange (check TWITCH_REDIRECT_URI)",
      noaccess: "Twitch did not return an access_token",
      state: "Security check failed (state mismatch) — please try again",
    };
    pendingOAuthMessage = "Twitch connection failed: " + (reasons[oauthError] || oauthError);
  }

  // Start with the site password gate (if protection is enabled). enterApp()
  // is only called once the gate is satisfied — so nobody (old or new visitor)
  // reaches the chat without a valid password.
  await initSiteGate();
})();
