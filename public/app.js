// ---------- Elements ----------
const gate = document.getElementById("gate");
const gateForm = document.getElementById("gate-form");
const gateError = document.getElementById("gate-error");
const twitchInput = document.getElementById("twitch-input");
const kickInput = document.getElementById("kick-input");

const stage = document.getElementById("stage");
const resetBtn = document.getElementById("reset-btn");
const feed = document.getElementById("feed");

const twStatus = document.getElementById("tw-status");
const kkStatus = document.getElementById("kk-status");

// ---------- State ----------
let socket = null;
let joinedTwitch = null;
let joinedKick = null;
let reconnectAttempts = 0;
let reconnectTimer = null;
// تتبع آخر رسالة استلمناها لتجاهل المكرر عند إعادة استقبال الـ backlog
let lastChatTs = 0;
// إشارة: أول snapshot بعد الاتصال (نعيد بناء الخلاصة من الـ backlog)
let pendingSnapshot = null;

// ---------- Entry screen ----------
gateForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const twitchUser = twitchInput.value.trim().toLowerCase();
  const kickUser = kickInput.value.trim().toLowerCase();

  if (!twitchUser && !kickUser) {
    gateError.textContent = "Enter at least one username (Twitch or Kick).";
    gateError.hidden = false;
    return;
  }
  gateError.hidden = true;
  openStage(twitchUser, kickUser);
});

function openStage(twitchUser, kickUser) {
  gate.hidden = true;
  stage.hidden = false;
  feed.innerHTML = "";
  lastChatTs = 0;

  joinedTwitch = twitchUser || null;
  joinedKick = kickUser || null;

  if (!twitchUser) twStatus.textContent = "not set";
  if (!kickUser) kkStatus.textContent = "not set";

  connectServer();
}

resetBtn.addEventListener("click", () => {
  // اقطع كل شي وارجع للشاشة الأولى
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
  feed.innerHTML = "";
  gate.hidden = false;
  stage.hidden = true;
});

// ---------- Server connection (single WS) ----------
function wsUrl() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}/ws`;
}

function connectServer() {
  // نظف أي اتصال سابق
  if (socket) {
    try {
      socket.close();
    } catch {}
    socket = null;
  }

  socket = new WebSocket(wsUrl());

  socket.addEventListener("open", () => {
    reconnectAttempts = 0;
    if (joinedTwitch) twStatus.textContent = "connecting...";
    if (joinedKick) kkStatus.textContent = "connecting...";
    socket.send(JSON.stringify({ type: "join", twitch: joinedTwitch, kick: joinedKick }));
  });

  socket.addEventListener("message", (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    if (msg.type === "snapshot") {
      // الحالة الأولية + الرسائل التي فاتت (نبني الخلاصة من جديد)
      pendingSnapshot = msg;
      feed.innerHTML = "";
      lastChatTs = 0;

      if (joinedTwitch) twStatus.textContent = msg.twitchStatus;
      if (joinedKick) kkStatus.textContent = msg.kickStatus;

      for (const m of msg.backlog) {
        renderBacklogItem(m);
        if (m.ts > lastChatTs) lastChatTs = m.ts;
      }
      pendingSnapshot = null;
      return;
    }

    if (msg.type === "status") {
      if (msg.platform === "twitch" && joinedTwitch) twStatus.textContent = msg.status;
      if (msg.platform === "kick" && joinedKick) kkStatus.textContent = msg.status;
      return;
    }

    if (msg.type === "chat") {
      // تجاهل المكرر من الـ backlog وقت إعادة الاتصال السريع
      if (msg.ts <= lastChatTs && pendingSnapshot === null) {
        // قد تكون رسالة لحقت بالـ snapshot، تجاهلها فقط لو كانت أقدم
        // (نجعلها تساوي تماماً للسماح بالرسائل الجديدة)
      }
      if (msg.ts < lastChatTs) return;
      lastChatTs = msg.ts;
      addChatMessage(msg.platform, msg.username, msg.content, msg.color);
      return;
    }

    if (msg.type === "error") {
      addSystemMessage(msg.error || "حدث خطأ غير معروف.");
    }
  });

  socket.addEventListener("close", () => {
    if (joinedTwitch) twStatus.textContent = "reconnecting";
    if (joinedKick) kkStatus.textContent = "reconnecting";
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    // الخطأ يتبعه close عادةً؛ نكتفي بإظهار حالة إعادة الاتصال
    if (joinedTwitch) twStatus.textContent = "error";
    if (joinedKick) kkStatus.textContent = "error";
  });
}

function scheduleReconnect() {
  // تراجع أسي (exponential backoff) بحد أقصى 10 ثوانٍ
  if (reconnectTimer) return;
  reconnectAttempts++;
  const delay = Math.min(10000, 1000 * Math.pow(2, reconnectAttempts - 1));
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (joinedTwitch || joinedKick) connectServer();
  }, delay);
}

// الرجوع للصفحة بعد الذهاب للخلفية: أعد الاتصال فوراً إذا كان مقطوعاً
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (!joinedTwitch && !joinedKick) return;
  // إذا كان السوكet مغلق أو في طريقه للإغلاق، أعد الاتصال الآن
  if (!socket || socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    reconnectAttempts = 0;
    connectServer();
  }
});

// ---------- Feed rendering ----------
function addSystemMessage(text) {
  const row = document.createElement("div");
  row.className = "system-row";
  row.textContent = text;
  feed.appendChild(row);
  scrollFeedIfNeeded();
}

function addChatMessage(platform, username, content, color) {
  const wasAtBottom = feed.scrollTop + feed.clientHeight >= feed.scrollHeight - 40;

  const row = document.createElement("div");
  row.className = "msg-row";

  const icon = document.createElement("span");
  icon.className = `msg-icon ${platform}`;
  icon.textContent = platform === "tw" ? "TW" : "KK";

  const text = document.createElement("span");
  text.className = "msg-text";
  text.innerHTML = `<span class="msg-user" style="color:${escapeAttr(color)}">${escapeHtml(
    username
  )}</span> <span class="msg-content">${escapeHtml(content)}</span>`;

  row.appendChild(icon);
  row.appendChild(text);
  feed.appendChild(row);

  if (wasAtBottom) feed.scrollTop = feed.scrollHeight;

  // keep the DOM light during long sessions
  while (feed.children.length > 500) feed.removeChild(feed.firstChild);
}

// نفس addChatMessage لكن بدون تتبع آخر ts (للرسائل من الـ backlog)
function renderBacklogItem(m) {
  const row = document.createElement("div");
  row.className = "msg-row";

  const icon = document.createElement("span");
  icon.className = `msg-icon ${m.platform}`;
  icon.textContent = m.platform === "tw" ? "TW" : "KK";

  const text = document.createElement("span");
  text.className = "msg-text";
  text.innerHTML = `<span class="msg-user" style="color:${escapeAttr(m.color)}">${escapeHtml(
    m.username
  )}</span> <span class="msg-content">${escapeHtml(m.content)}</span>`;

  row.appendChild(icon);
  row.appendChild(text);
  feed.appendChild(row);

  while (feed.children.length > 500) feed.removeChild(feed.firstChild);
}

function scrollFeedIfNeeded() {
  feed.scrollTop = feed.scrollHeight;
}

// ---------- Helpers ----------
function escapeHtml(str) {
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(str) {
  return String(str).replace(/"/g, "&quot;");
}
