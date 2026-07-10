// ---------- Elements ----------
const gate = document.getElementById("gate");
const gateForm = document.getElementById("gate-form");
const gateError = document.getElementById("gate-error");
const twitchInput = document.getElementById("twitch-input");
const kickInput = document.getElementById("kick-input");
const tiktokInput = document.getElementById("tiktok-input");
const youtubeInput = document.getElementById("youtube-input");

const stage = document.getElementById("stage");
const resetBtn = document.getElementById("reset-btn");
const feed = document.getElementById("feed");

const twStatus = document.getElementById("tw-status");
const kkStatus = document.getElementById("kk-status");
const ttStatus = document.getElementById("tt-status");
const ytStatus = document.getElementById("yt-status");

// ---------- State ----------
let socket = null;
let joinedTwitch = null;
let joinedKick = null;
let joinedTikTok = null;
let joinedYouTube = null;
let reconnectAttempts = 0;
let reconnectTimer = null;
// آخر id استلمناه من السيرفر — مفتاح الاستئناف وإزالة التكرار
let lastSeenId = 0;

// ---------- localStorage (تذكر آخر قناة) ----------
const STORAGE_KEY = "dualchat:join";

function readStoredJoin() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const tw = typeof parsed.twitch === "string" ? parsed.twitch.trim().toLowerCase() : "";
    const kk = typeof parsed.kick === "string" ? parsed.kick.trim().toLowerCase() : "";
    const tt = typeof parsed.tiktok === "string" ? parsed.tiktok.trim().toLowerCase() : "";
    // يوتيوب نُخزّنه كما هو (رابط/فيديو آي دي) دون تطبيع
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
    /* تخزين محلي غير متاح (وضع خاص مثلاً) — نتجاهل بهدوء */
  }
}

function clearStoredJoin() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    /* تجاهل */
  }
}

// ---------- Entry screen ----------
gateForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const twitchUser = twitchInput.value.trim().toLowerCase();
  const kickUser = kickInput.value.trim().toLowerCase();
  const tiktokUser = tiktokInput.value.trim().toLowerCase();
  const youtubeUser = youtubeInput.value.trim();

  if (!twitchUser && !kickUser && !tiktokUser && !youtubeUser) {
    gateError.textContent = "Enter at least one username (Twitch, Kick, TikTok, or YouTube).";
    gateError.hidden = false;
    return;
  }
  gateError.hidden = true;
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

  if (!twitchUser) twStatus.textContent = "not set";
  if (!kickUser) kkStatus.textContent = "not set";
  if (!tiktokUser) ttStatus.textContent = "not set";
  if (!youtubeUser) ytStatus.textContent = "not set";

  connectServer();
}

resetBtn.addEventListener("click", () => {
  // اقطع كل شي وارجع للشاشة الأولى، وامسح القناة المحفوظة
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
  clearStoredJoin();
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
    if (joinedTikTok) ttStatus.textContent = "connecting...";
    if (joinedYouTube) ytStatus.textContent = "connecting...";
    // نرسل آخر id لنستلم فقط ما فاتنا (إن كان لا يزال ضمن الـ buffer)
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
  });

  socket.addEventListener("message", (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    if (msg.type === "snapshot") {
      // نحدد إن كان استئنافاً (كان عندنا lastId سابق) أم اتصالاً جديداً
      const isResume = lastSeenId > 0;
      if (!isResume) feed.innerHTML = "";

      for (const m of msg.backlog) {
        if (m.id > lastSeenId) {
          addChatMessage(m.platform, m.username, m.content, m.color, m.emotes, false);
          lastSeenId = m.id;
        }
      }

      // حدّث النقطة المرجعية بما وصل إليه السيرفر (حتى لو لم يُرسل شيئاً جديداً)
      if (typeof msg.lastId === "number" && msg.lastId > lastSeenId) {
        lastSeenId = msg.lastId;
      }

      if (joinedTwitch) twStatus.textContent = msg.twitchStatus;
      if (joinedKick) kkStatus.textContent = msg.kickStatus;
      if (joinedTikTok) ttStatus.textContent = msg.tiktokStatus;
      if (joinedYouTube) ytStatus.textContent = msg.youtubeStatus;

      if (msg.tiktokError) addSystemMessage("TikTok: " + msg.tiktokError);
      if (msg.kickError) addSystemMessage("Kick: " + msg.kickError);
      if (msg.twitchError) addSystemMessage("Twitch: " + msg.twitchError);
      if (msg.youtubeError) addSystemMessage("YouTube: " + msg.youtubeError);
      return;
    }

    if (msg.type === "status") {
      if (msg.platform === "twitch" && joinedTwitch) twStatus.textContent = msg.status;
      if (msg.platform === "kick" && joinedKick) kkStatus.textContent = msg.status;
      if (msg.platform === "tiktok" && joinedTikTok) ttStatus.textContent = msg.status;
      if (msg.platform === "youtube" && joinedYouTube) ytStatus.textContent = msg.status;
      if (msg.error && msg.status === "error") addSystemMessage(platformLabel(msg.platform) + ": " + msg.error);
      return;
    }

    if (msg.type === "chat") {
      // المعرّف التصاعدي هو المرجع: أهمل أي شيء قديم/مكرر
      if (typeof msg.id === "number" && msg.id <= lastSeenId) return;
      if (typeof msg.id === "number") lastSeenId = msg.id;
      addChatMessage(msg.platform, msg.username, msg.content, msg.color, msg.emotes, true);
      return;
    }

    if (msg.type === "error") {
      addSystemMessage(msg.error || "حدث خطأ غير معروف.");
    }
  });

  socket.addEventListener("close", () => {
    if (joinedTwitch) twStatus.textContent = "reconnecting";
    if (joinedKick) kkStatus.textContent = "reconnecting";
    if (joinedTikTok) ttStatus.textContent = "reconnecting";
    if (joinedYouTube) ytStatus.textContent = "reconnecting";
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    // الخطأ يتبعه close عادةً؛ نكتفي بإظهار حالة إعادة الاتصال
    if (joinedTwitch) twStatus.textContent = "error";
    if (joinedKick) kkStatus.textContent = "error";
    if (joinedTikTok) ttStatus.textContent = "error";
    if (joinedYouTube) ytStatus.textContent = "error";
  });
}

function scheduleReconnect() {
  // تراجع أسي (exponential backoff) بحد أقصى 10 ثوانٍ
  if (reconnectTimer) return;
  reconnectAttempts++;
  const delay = Math.min(10000, 1000 * Math.pow(2, reconnectAttempts - 1));
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (joinedTwitch || joinedKick || joinedTikTok || joinedYouTube) connectServer();
  }, delay);
}

// الرجوع للصفحة بعد الذهاب للخلفية: أعد الاتصال فوراً إذا كان مقطوعاً
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (!joinedTwitch && !joinedKick && !joinedTikTok && !joinedYouTube) return;
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

// مُنشئ صف واحد مشترك بين الرسائل الحية ومن الـ backlog.
// autoscroll يحرك القاع فقط لو كان المستخدم أصلاً في الأسفل (للرسائل الجديدة).
function addChatMessage(platform, username, content, color, emotes, autoscroll) {
  const wasAtBottom = autoscroll
    ? feed.scrollTop + feed.clientHeight >= feed.scrollHeight - 40
    : false;

  const row = document.createElement("div");
  row.className = "msg-row";

  const icon = document.createElement("span");
  icon.className = `msg-icon ${platform}`;
  icon.textContent = platform === "tw" ? "TW" : platform === "kk" ? "KK" : platform === "tt" ? "TT" : "YT";

  const text = document.createElement("span");
  text.className = "msg-text";

  const user = document.createElement("span");
  user.className = "msg-user";
  user.style.color = escapeAttr(color);
  user.textContent = username;

  const contentWrap = document.createElement("span");
  contentWrap.className = "msg-content";
  renderContentInto(contentWrap, content, emotes, platform);

  text.appendChild(user);
  text.appendChild(document.createTextNode(" "));
  text.appendChild(contentWrap);

  row.appendChild(icon);
  row.appendChild(text);
  feed.appendChild(row);

  if (autoscroll && wasAtBottom) feed.scrollTop = feed.scrollHeight;

  // keep the DOM light during long sessions
  while (feed.children.length > 500) feed.removeChild(feed.firstChild);
}

// يملأ عنصراً بمحتوى الرسالة: نص عادي بعد التهريب، وإيموتات كصور.
// نبني عبر DOM (وليس innerHTML) لإيموتات لضمان عمل onerror والتهريب الآمن.
function renderContentInto(el, content, emotes, platform) {
  content = String(content ?? "");
  if (!Array.isArray(emotes) || emotes.length === 0) {
    el.textContent = content;
    return;
  }

  // فلترة ورتّب حسب البداية تصاعدياً، متجاهلاً المداخل المتداخلة/غير الصالحة
  const sorted = emotes
    .filter((e) => e && Number.isFinite(e.start) && Number.isFinite(e.end) && e.start >= 0 && e.end < content.length && e.start <= e.end)
    .slice()
    .sort((a, b) => a.start - b.start);

  let cursor = 0;
  for (const e of sorted) {
    if (e.start < cursor) continue; // تداخل — تخطّاه
    // النص قبل الإيموت
    if (e.start > cursor) {
      el.appendChild(document.createTextNode(content.slice(cursor, e.start)));
    }
    const rawText = content.slice(e.start, e.end + 1);
    el.appendChild(buildEmoteImg(e.id, rawText, platform));
    cursor = e.end + 1;
  }
  // ما تبقّى بعد آخر إيموت
  if (cursor < content.length) {
    el.appendChild(document.createTextNode(content.slice(cursor)));
  }
  if (cursor === 0) {
    // لم يُطبَّق أي إيموت صالح — اكتفِ بالنص
    el.textContent = content;
  }
}

// يبني عنصر <img> للإيموت مع تراجع آمن: عند فشل الصورة نُظهر النص الخام.
function buildEmoteImg(id, rawText, platform) {
  // لـ Kick، الصيغة داخل النص هي [emote:id:name] — نعرض الاسم فقط لا الصيغة كاملة
  let displayText = rawText;
  if (platform === "kk") {
    const m = rawText.match(/^\[emote:\d+:([^\]]+)\]$/);
    if (m) displayText = m[1];
  }

  const img = document.createElement("img");
  img.className = "emote";
  img.alt = displayText;
  img.title = displayText;
  img.loading = "lazy";

  // نُجبر id على رقم لمنع حقن أي شيء في الرابط
  const numericId = Number(id);
  if (Number.isFinite(numericId) && numericId > 0) {
    img.src = platform === "tw"
      ? `https://static-cdn.jtvnw.net/emoticons/v2/${numericId}/default/dark/1.0`
      : `https://files.kick.com/emotes/${numericId}/fullsize`;
  }

  // تراجع: عند فشل التحميل استبدل الصورة بالنص الخام
  img.addEventListener("error", () => {
    const textNode = document.createTextNode(displayText);
    img.replaceWith(textNode);
  });
  return img;
}

function scrollFeedIfNeeded() {
  feed.scrollTop = feed.scrollHeight;
}

// ---------- Helpers ----------
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

function escapeHtml(str) {
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(str) {
  return String(str).replace(/"/g, "&quot;");
}

// ---------- Boot: استئناف تلقائي إن كانت هناك قناة محفوظة ----------
(function boot() {
  const saved = readStoredJoin();
  if (saved) {
    twitchInput.value = saved.twitch;
    kickInput.value = saved.kick;
    tiktokInput.value = saved.tiktok;
    youtubeInput.value = saved.youtube;
    openStage(saved.twitch, saved.kick, saved.tiktok, saved.youtube);
  }
})();
