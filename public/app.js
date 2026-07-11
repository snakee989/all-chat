// =====================================================================
// Unified Chat — client
// المسؤوليات: شاشة كلمة مرور الموقع، اختيار القنوات، اتصال WS موحّد،
// عرض الرسائل مع دعم الإيموجي/الإيموتات، مشغل فيديو تويتش قابل للإخفاء،
// وربط حساب تويتش (OAuth) للكتابة باسم المستخدم.
// =====================================================================

// ---------- Elements ----------
const siteGate = document.getElementById("site-gate");
const siteGateForm = document.getElementById("site-gate-form");
const sitePasswordInput = document.getElementById("site-password-input");
const siteGateError = document.getElementById("site-gate-error");

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
// آخر id استلمناه من السيرفر — مفتاح الاستئناف وإزالة التكرار
let lastSeenId = 0;

// كلمة مرور الموقع (محفوظة محلياً بعد أول إدخال)
let sitePassword = "";

// Twitch OAuth: token + login (محفوظة محلياً للقراءة فقط)
let twitchAccessToken = "";
let twitchLogin = "";

// Video: نسخة/معرّف المشغل لضمان التدمير الكامل
let videoEmbed = null; // مرجع كائن Twitch.Embed
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
    /* تجاهل */
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
    /* تجاهل */
  }
}

// =====================================================================
// Phase 1 — Site password gate
// =====================================================================
// نسأل السيرفر إن كان محمياً؛ فإن لم يكن، نتخطّى شاشة كلمة المرور تماماً.
async function initSiteGate() {
  let protectedFlag = true;
  try {
    const r = await fetch("/api/auth-status", { cache: "no-store" });
    if (r.ok) {
      const j = await r.json();
      protectedFlag = !!j.protected;
    }
  } catch {
    // لو فشل الطلب، نفترض الحماية مفعّلة (آمن افتراضياً)
  }

  if (!protectedFlag) {
    // الموقع غير محمي — اذهب مباشرة لاختيار القنوات
    sitePassword = "";
    showGate();
    return;
  }

  // الموقع محمي: هل توجد كلمة مرور محفوظة؟ جرّبها مباشرة مع السيرفر.
  const saved = readStoredPassword();
  if (saved) {
    const ok = await verifySitePassword(saved);
    if (ok) {
      sitePassword = saved;
      showGate();
      return;
    }
    // كلمة المرور المحفوظة لم تعد صالحة — امسحها
    writeStoredPassword("");
  }

  // اعرض شاشة كلمة المرور
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
    siteGateError.textContent = "أدخل كلمة المرور.";
    siteGateError.hidden = false;
    return;
  }
  const ok = await verifySitePassword(pw);
  if (!ok) {
    siteGateError.textContent = "كلمة المرور غير صحيحة.";
    siteGateError.hidden = false;
    return;
  }
  siteGateError.hidden = true;
  // حفظ دائم في المتصفح حتى لا يُعاد إدخالها
  writeStoredPassword(pw);
  sitePassword = pw;
  siteGate.hidden = true;
  showGate();
});

// =====================================================================
// Phase 2 — Username selection (gate)
// =====================================================================
function showGate() {
  gate.hidden = false;
}

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

  // الـ composer وزر الفيديو يظهران فقط لجلسات تويتش
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
}

resetBtn.addEventListener("click", () => {
  // اقطع كل شي وارجع للشاشة الأولى، وامسح القناة المحفوظة
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
  gate.hidden = false;
  stage.hidden = true;
});

// =====================================================================
// Server connection (single WS) — مع مصادقة كلمة المرور
// =====================================================================
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
    // مصادقة كلمة المرور أولاً (السيرفر يرفض أي أمر آخر قبلها إن كانت الحماية مفعّلة)
    socket.send(JSON.stringify({ type: "auth", password: sitePassword }));
  });

  socket.addEventListener("message", (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    // ردود المصادقة
    if (msg.type === "auth_ok") {
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
      return;
    }
    if (msg.type === "auth_fail" || msg.type === "need_auth") {
      // كلمة المرور غير صالحة (ربما تغيّرت على السيرفر) — ارجع لشاشة الدخول
      addSystemMessage("انتهت صلاحية كلمة المرور، يُرجى إعادة الإدخال.");
      writeStoredPassword("");
      sitePassword = "";
      siteGate.hidden = false;
      try { socket.close(); } catch {}
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

    if (msg.type === "send_result") {
      handleSendResult(msg);
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

  // تويتش تُفهرس مواضع الإيموت على أساس codepoints (محارف Unicode)، بينما
  // JavaScript تُفهرس النص بوحدات UTF-16. أي إيموجي قبل الإيموت (زوج بديل =
  // codepoint واحد لكن وحدتا UTF-16) يُزاح المؤشر فتختفي أجزاء من النص أو
  // يتداخل الإيموت مع نص مجاور. نترجم النطاقات إلى UTF-16 أولاً.
  // كيك تعطي المواضع بـ UTF-16 أصلاً (من regex) فلا نترجمها.
  const needMap = platform === "tw";
  const cpMap = needMap ? buildCodepointMap(content) : null;

  // حوّل كل مدخل إيموت إلى نطاق UTF-16 ضمني النهاية، متجاهلاً غير الصالح
  const sorted = emotes
    .map((e) => {
      if (!e || !Number.isFinite(e.start) || !Number.isFinite(e.end)) return null;
      if (e.start < 0 || e.end < e.start) return null;
      if (needMap) {
        // cpMap.length - 1 = عدد الـ codepoints (= موضع حارس النهاية)
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
    if (e.start < cursor) continue; // تداخل — تخطّاه
    // النص قبل الإيموت
    if (e.start > cursor) {
      el.appendChild(document.createTextNode(content.slice(cursor, e.start)));
    }
    const rawText = content.slice(e.start, e.end + 1);
    el.appendChild(buildEmoteImg(e.id, rawText, platform));
    cursor = e.end + 1;
    applied = true;
  }
  // ما تبقّى بعد آخر إيموت
  if (cursor < content.length) {
    el.appendChild(document.createTextNode(content.slice(cursor)));
  }
  if (!applied) {
    // لم يُطبَّق أي إيموت صالح — اكتفِ بالنص
    el.textContent = content;
  }
}

// يبني خريطة تحوّل مواضع codepoints إلى مواضع وحدات UTF-16.
// map[i] = موضع بداية (codepoint رقم i) ضمن وحدات UTF-16.
// map[numberOfCodepoints] = content.length (حارس النهاية).
// هذا ضروري لأن تويتش تُفهرس الإيموتات بـ codepoints لا بـ UTF-16.
function buildCodepointMap(str) {
  const map = [];
  let cp = 0;
  for (let i = 0; i < str.length; ) {
    map[cp] = i;
    const code = str.charCodeAt(i);
    // زوج بديل عالٍ (إيموجي/محارف خارج BMP) = وحدتان في UTF-16، codepoint واحد
    i += code >= 0xd800 && code <= 0xdbff ? 2 : 1;
    cp++;
  }
  map[cp] = str.length;
  return map;
}

// يبني عنصر <img> للإيموت مع تراجع آمن: عند فشل الصورة نُظهر النص الخام.
// لإيموتات تويتش نجرّب صيغاً متعددة: emotesv2 (المشترك/العادي) ثم emotesv1
// (القديم) ثم static-cdn القديم — بعض إيموتات المشتركين لها id لا يوجد إلا
// على v1. عند فشل كل الصيغ نُظهر النص الخام.
function buildEmoteImg(id, rawText, platform) {
  // لـ Kick، الصيغة داخل النص هي [emote:id:name] — نعرض الاسم فقط لا الصيغة كاملة
  let displayText = rawText;
  if (platform === "kk") {
    const m = rawText.match(/^\[emote:\d+:([^\]]+)\]$/);
    if (m) displayText = m[1];
  }

  const numericId = Number(id);
  const valid = Number.isFinite(numericId) && numericId > 0;

  // إن لم يكن id رقماً صالحاً، لا فائدة من <img> — اعرض النص مباشرة
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

  // قائمة الصيغ الممكنة لصورة الإيموت؛ نجرّبها بالترتيب حتى تنجح واحدة.
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

  // تراجع على مرحلتين:
  //  (1) جرّب الصيغة التالية من القائمة لو وُجدت.
  //  (2) نفدت كل الصيغ — استبدل الصورة بالنص الخام.
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
// Twitch video player (embed) — قابل للإخفاء مع تدمير كامل
// =====================================================================
// زر "Show Video": يبني iframe مشغل تويتش في الأعلى.
// زر "Hide Video": يدمّر المشغل تماماً (إزالة iframe + إيقاف التحميل) لتوفير
// الإنترنت وموارد الجهاز على الجوال.
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

  // نبني iframe مباشرة (أبسط وأخفّ من تحميل Twitch Embed JS library بالكامل،
  // ولا يتطلّب parent domain مسجّلاً على خادم تويتش). نمرّر parent=current host.
  const parent = location.hostname || "localhost";
  const channel = encodeURIComponent(joinedTwitch);
  const src = `https://player.twitch.tv/?channel=${channel}&parent=${encodeURIComponent(parent)}&muted=false&autoplay=true`;
  const iframe = document.createElement("iframe");
  iframe.src = src;
  iframe.allow = "autoplay; fullscreen; encrypted-media";
  iframe.allowFullscreen = true;
  iframe.title = "Twitch stream";
  // نستخدم class لإزالته لاحقاً بسهولة
  iframe.className = "video-player";
  videoHost.appendChild(iframe);
  videoEmbed = { iframe };
}

function destroyVideo() {
  videoShown = false;
  videoArea.hidden = true;
  videoToggleBtn.textContent = "Show Video";
  videoToggleBtn.classList.remove("is-active");
  // إزالة iframe تقطع تحميل الفيديو وتُدمّر المشغل كاملاً (لا نترك موارد)
  if (videoEmbed && videoEmbed.iframe) {
    videoEmbed.iframe.src = "about:blank"; // يوقف كل الطلبات فوراً
    try { videoEmbed.iframe.remove(); } catch {}
    videoEmbed = null;
  }
  videoHost.innerHTML = "";
}

// =====================================================================
// Twitch OAuth — ربط الحساب للكتابة باسم المستخدم
// =====================================================================
function initTwitchAuthUI() {
  // إن وُجد token محفوظ، استخدمه مباشرة
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
  composerStatus.textContent = twitchLogin ? `Chatting as @${twitchLogin}` : "Connected";
}

function setDisconnectedUI() {
  twitchAccessToken = "";
  twitchLogin = "";
  connectTwitchBtn.textContent = "Connect Twitch to chat";
  connectTwitchBtn.classList.remove("is-connected");
  composerForm.hidden = true;
  composerStatus.textContent = "Anonymous (read only)";
}

connectTwitchBtn.addEventListener("click", () => {
  if (twitchAccessToken) {
    // قطع الاتصال: امسح المحفوظات وأعد الواجهة
    writeStoredTwitch("", "");
    setDisconnectedUI();
    return;
  }
  // ابدأ OAuth: وجّه المستخدم لـ /auth/twitch. state عشوائي بسيط لمكافحة CSRF.
  const state = Math.random().toString(36).slice(2);
  sessionStorage.setItem("twitch_oauth_state", state);
  // نمرّر state كـ query param؛ السيرفر يُعيد توجيه تويتش إلى callback.
  location.href = `/auth/twitch?state=${encodeURIComponent(state)}`;
});

// بعد عودة OAuth: نتائج تويتش تعود في fragment (#auth=ok&access_token=...).
function handleOAuthRedirect() {
  if (!location.hash || !location.hash.startsWith("#auth=")) return;
  const params = new URLSearchParams(location.hash.slice(1));
  // امسح الـ hash فوراً حتى لا يُعاد معالجته عند التحديث
  history.replaceState(null, "", location.pathname + location.search);

  if (params.get("auth") === "ok") {
    const token = params.get("access_token") || "";
    const login = params.get("login") || "";
    if (token) {
      twitchAccessToken = token;
      twitchLogin = login;
      writeStoredTwitch(token, login);
      addSystemMessage(login ? `تم ربط حساب تويتش: @${login}` : "تم ربط حساب تويتش.");
    }
  } else {
    const reason = params.get("reason") || "unknown";
    addSystemMessage("فشل ربط حساب تويتش (" + reason + "). تأكد أن TWITCH_REDIRECT_URI مضبوط على السيرفر ومطابق لما سجّلته في Twitch Console.");
  }
}

// إرسال رسالة كشات
composerForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const content = composerText.value;
  if (!content.trim()) return;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    addSystemMessage("الاتصال مقطوع — تعذّر الإرسال.");
    return;
  }
  if (!twitchAccessToken) {
    addSystemMessage("اربط حساب تويتش أولاً للكتابة.");
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
    addSystemMessage("تعذّر الإرسال: " + (msg.error || "خطأ غير معروف"));
    // لو الـ token لم يعد صالحاً، أعِد الواجهة إلى وضع "غير مربوط"
    if (/not valid|invalid|unauthorized|توكن/i.test(String(msg.error || ""))) {
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

function escapeHtml(str) {
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(str) {
  return String(str).replace(/"/g, "&quot;");
}

// =====================================================================
// Boot
// =====================================================================
(function boot() {
  // عالج نتيجة OAuth إن كنا عائدين من تويتش
  handleOAuthRedirect();

  // ابدأ بشاشة كلمة المرور (إن كانت الحماية مفعّلة)
  initSiteGate().then(() => {
    // بعد تجاوز البوابة، استئناف تلقائي إن كانت هناك قناة محفوظة
    const saved = readStoredJoin();
    if (saved) {
      twitchInput.value = saved.twitch;
      kickInput.value = saved.kick;
      tiktokInput.value = saved.tiktok;
      youtubeInput.value = saved.youtube;
      openStage(saved.twitch, saved.kick, saved.tiktok, saved.youtube);
    }
  });
})();
