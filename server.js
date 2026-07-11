// حمّل متغيرات البيئة من ملف .env (إن وُجد) قبل أي شيء آخر.
// هكذا تبقى الأسرار (كلمة مرور الموقع، مفاتيح تويتش/تيكتوك) في ملف .env
// بدل تمريرها في سطر الأوامر (حيث تظهر في قائمة العمليات ps/Task Manager).
require("dotenv").config();

const express = require("express");
const path = require("path");
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

// الخادم HTTP + WebSocket على نفس المنفذ
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

app.use(express.static(path.join(__dirname, "public")));

// كم رسالة نحتفظ بها لكل جلسة (لإرسالها للعميل عند إعادة الاتصال)
const BACKLOG_LIMIT = 200;
// ثواني الانتظار قبل إعادة الاتصال بمنصة بعد قطع
const PLATFORM_RECONNECT_MS = 3000;
// فترة السماح بعد انقطاع آخر عميل قبل تدمير الجلسة فعلياً (تُلغى لو رجع عميل)
const SESSION_GRACE_MS = 30 * 60 * 1000; // 30 دقيقة
// فحص دوري احتياطي: يغلق أي جلسة معلّقة لم يُطبّق عليها تايمر السماح لأي سبب
const SWEEP_INTERVAL_MS = 4 * 60 * 60 * 1000; // كل 4 ساعات
const KICK_PUSHER_APP_KEY = "32cbd69e4b950bf97679";
// مفتاح Euler Stream لتوقيع اتصالات تيكتوك. ضعه في متغير البيئة TIKTOK_SIGN_API_KEY
// (خطة Community المجانية تكفي: https://www.eulerstream.com). مطلوب لتشغيل تيكتوك.
const TIKTOK_SIGN_API_KEY = process.env.TIKTOK_SIGN_API_KEY || "";

// كلمة مرور الموقع (Gate). تُطلب من كل زائر قبل السماح بالاتصال بالشات/الـ WS.
// ضعها في متغير البيئة SITE_PASSWORD. لو تركتها فارغة، يبقى الموقع مفتوحاً.
// التحقق بأسلوب constant-time لمنع تسرّب التوقيت، ومقارنة بعد تهذيب المسافات.
const SITE_PASSWORD = (process.env.SITE_PASSWORD || "").trim();

// إعدادات Twitch OAuth (اختياري، للكتابة باسم المستخدم في الشات).
// سجّل تطبيقاً في https://dev.twitch.tv/console وضع Client ID / Secret هنا.
const TWITCH_CLIENT_ID = (process.env.TWITCH_CLIENT_ID || "").trim();
const TWITCH_CLIENT_SECRET = (process.env.TWITCH_CLIENT_SECRET || "").trim();
// Redirect URI يجب أن تطابق ما سجّلته في Twitch Console تماماً. افتراضياً /auth/twitch/callback
// على نفس المضيف. لخادم خلف https، اضبط TWITCH_REDIRECT_URI يدوياً (مثال:
// https://my-domain.com/auth/twitch/callback).
const TWITCH_REDIRECT_URI =
  (process.env.TWITCH_REDIRECT_URI || "").trim();

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// مقارنة نص بأسلوب constant-time لتقليل تسرّب التوقيت في مقارنة كلمات المرور/الـ tokens.
function timingSafeEqualStr(a, b) {
  a = String(a);
  b = String(b);
  // نحدّ طول a لتقليل تسريب طول كلمة المرور بقدر الإمكان، لكن Node لا يوفّر
  // timingSafeEqual لطولين مختلفين، فنطيل b لمطابقة a ثم نقارن.
  const max = Math.max(a.length, b.length, 32);
  const ab = Buffer.from(a.padEnd(max, "\u0000"), "utf8");
  const bb = Buffer.from(b.padEnd(max, "\u0000"), "utf8");
  let diff = ab.length ^ bb.length;
  for (let i = 0; i < ab.length && i < bb.length; i++) diff |= ab[i] ^ bb[i];
  return diff === 0 && a === b;
}

// هل حماية الموقع مفعّلة؟ (Site password)
function siteProtected() {
  return SITE_PASSWORD.length > 0;
}

// عميل Innertube واحد مشترك لكل اتصالات يوتيوب (لا يُنشأ لكل جلسة).
// نُهيّئه مرّة واحدة؛ كل من يحتاجه ينتظر هذا الوعد.
let ytClientPromise = null;
function getYoutubeClient() {
  if (!ytClientPromise) {
    ytClientPromise = Innertube.create({
      // تخزين مؤقت بسيط لتقليل الطلبات المتكررة على مفاتيح Innertube
      cache: new UniversalCache(false),
    }).catch((err) => {
      ytClientPromise = null; // السماح بإعادة المحاولة لاحقاً
      throw err;
    });
  }
  return ytClientPromise;
}

// ----------------------------------------------------------------------
// SessionManager: كل جلسة (twitch + kick معاً) لها حالتها الخاصة.
// الجلسة لا تُدمَّر لحظة مغادرة آخر عميل، بل تُمنح فترة سماح (SESSION_GRACE_MS)
// تبقى خلالها حية؛ لو رجع أي عميل خلالها تُلغى ويُستأنف العمل. وإلا تُدمَّر.
// ----------------------------------------------------------------------

const sessions = new Map(); // key "tw=xxx|kk=yyy|tt=zzz|yt=www" -> Session

function sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput) {
  return `tw=${twitchUser || ""}|kk=${kickUser || ""}|tt=${tiktokUser || ""}|yt=${youtubeInput || ""}`;
}

class Session {
  constructor(twitchUser, kickUser, tiktokUser, youtubeInput) {
    this.twitchUser = twitchUser || null;
    this.kickUser = kickUser || null;
    this.tiktokUser = tiktokUser || null;
    this.youtubeInput = youtubeInput || null; // رابط/فيديو آي دي يوتيوب (يُحلّل لاحقاً)
    this.key = sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput);
    this.clients = new Set(); // المتصفحات المتصلة بهذه الجلسة
    this.backlog = []; // آخر الرسائل (نرسلها لكل عميل جديد/معيد اتصال)
    this.seq = 0; // عدّاد تصاعدي لكل رسالة (مفتاح الاستئناف عند إعادة الاتصال)
    this.platforms = {
      twitch: { socket: null, status: "connecting", reconnecting: false, timer: null, error: null },
      kick: { socket: null, status: "connecting", reconnecting: false, timer: null, pingTimer: null, chatroomId: null, pendingSubs: [], error: null },
      tiktok: { connection: null, status: "connecting", reconnecting: false, timer: null, error: null },
      youtube: { livechat: null, status: "connecting", reconnecting: false, timer: null, error: null, videoId: null },
    };
    this.graceTimer = null; // تايمر فترة السماح بعد انقطاع آخر عميل
    this.emptySince = null; // متى أصبحت الجلسة بلا عملاء (تستخدمه شبكة الأمان)
    this.sendConnection = null; // اتصال IRC مُصادَق للكتابة في تويتش (يُنشأ عند الطلب)
    this.closed = false;
  }

  // إضافة عميل (متصفح) للجلسة.
  // lastId: آخر id استلمه العميل سابقاً (إن وُجد) ليُرسل له فقط ما فاته.
  addClient(ws, lastId) {
    this.clients.add(ws);
    // لو كانت الجلسة في فترة السماح، ألغِ التدمير المؤجَّل وعُد لحالتها الطبيعية
    this.cancelExpiry();
    this.emptySince = null;

    let backlog;
    if (typeof lastId === "number" && lastId > 0) {
      // استئناف: هل الفجوة لا تزال ضمن الـ buffer؟
      const oldest = this.backlog.length ? this.backlog[0].id : 0;
      if (lastId >= oldest) {
        // الفجوة محصورة داخل الـ backlog — أرسل فقط الجديد بعد lastId
        const idx = this.backlog.findIndex((m) => m.id > lastId);
        backlog = idx === -1 ? [] : this.backlog.slice(idx);
      } else {
        // الفجوة أكبر من الـ buffer — أرسل آخر 200 المتوفرة ولا نحاول لقط الدقيق
        backlog = this.backlog;
      }
    } else {
      // اتصال جديد تماماً: أرسل كل الـ backlog المتوفر
      backlog = this.backlog;
    }

    // نرسل آخر id وصلناه ليحفظه العميل كنقطة استئناف تالية
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
        backlog,
        lastId: this.seq,
      })
    );
  }

  removeClient(ws) {
    this.clients.delete(ws);
    // إذا ما بقي أحد، شغّل فترة سماح بدل التدمير الفوري: لو رجع عميل تُلغى
    // الجلسة تبقى حية خلالها وتلتقط الرسائل، وإلا تُدمَّر بعد انتهاء الفترة.
    if (this.clients.size === 0) {
      this.emptySince = Date.now();
      this.scheduleExpiry();
    }
  }

  // يبدأ عدّ فترة السماح لتدمير الجلسة بعد SESSION_GRACE_MS لو لم يرجع أحد.
  scheduleExpiry() {
    if (this.graceTimer || this.closed) return; // لا تكرّر التأجيل
    this.graceTimer = setTimeout(() => {
      this.graceTimer = null;
      // تأكّد نهائي: لو رجع عميل قبل إطلاق التايمر أبقِ الجلسة
      if (this.clients.size > 0 || this.closed) return;
      this.destroy();
      sessions.delete(this.key);
    }, SESSION_GRACE_MS);
  }

  // يُلغي فترة السماح عند عودة عميل (تُستدعى من addClient).
  cancelExpiry() {
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const ws of this.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    }
  }

  // رسالة شات جديدة من إحدى المنصتين
  // emotes: مصفوفة اختيارية {start,end,id} لاستبدال أجزاء المحتوى بصور الإيموت
  pushChat(platform, username, content, color, emotes) {
    const msg = {
      id: ++this.seq, // مفتاح تصاعدي للاستئناف وإزالة التكرار
      type: "chat",
      platform,
      username,
      content,
      color,
      ts: Date.now(),
    };
    if (Array.isArray(emotes) && emotes.length) msg.emotes = emotes;
    this.backlog.push(msg);
    while (this.backlog.length > BACKLOG_LIMIT) this.backlog.shift();
    this.broadcast(msg);
  }

  setPlatformStatus(platform, status, error) {
    const st = this.platforms[platform];
    st.status = status;
    if (error !== undefined) st.error = error || null;
    this.broadcast({ type: "status", platform, status, error: st.error });
  }

  destroy() {
    this.closed = true;
    this.cancelExpiry();
    this.emptySince = null;
    teardownTwitch(this);
    teardownKick(this);
    teardownTikTok(this);
    teardownYouTube(this);
    // أغلق اتصال الكتابة بـ IRC إن وُجد
    if (this.sendConnection && this.sendConnection.socket) {
      try { this.sendConnection.socket.close(); } catch {}
      this.sendConnection = null;
    }
  }
}

function getOrCreateSession(twitchUser, kickUser, tiktokUser, youtubeInput) {
  const key = sessionKey(twitchUser, kickUser, tiktokUser, youtubeInput);
  let s = sessions.get(key);
  if (!s) {
    s = new Session(twitchUser, kickUser, tiktokUser, youtubeInput);
    sessions.set(key, s);
    if (twitchUser) connectTwitch(s);
    if (kickUser) connectKick(s);
    if (tiktokUser) connectTikTok(s);
    if (youtubeInput) connectYouTube(s);
  }
  return s;
}

// ----------------------------------------------------------------------
// Kick username -> chatroom_id (من السيرفر، لأن المتصفح يُحجَب بسبب CORS/Cloudflare)
// ----------------------------------------------------------------------
async function lookupKickChatroom(username) {
  const r = await fetch(`https://kick.com/api/v2/channels/${encodeURIComponent(username)}`, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      Accept: "application/json",
    },
  });
  if (r.status === 404) throw new Error("ما لقينا هذا اليوزر على كيك");
  if (!r.ok) throw new Error(`كيك رفض الطلب (${r.status})`);
  const data = await r.json();
  const chatroomId = data?.chatroom?.id;
  if (!chatroomId) throw new Error("هذا اليوزر ما عنده شات (يمكن ما بث قبل)");
  return chatroomId;
}

app.get("/api/kick-lookup/:username", async (req, res) => {
  const username = req.params.username.trim().toLowerCase();
  if (!username) return res.status(400).json({ error: "اسم مستخدم فاضي" });
  try {
    const chatroomId = await lookupKickChatroom(username);
    res.json({ username, chatroomId });
  } catch (err) {
    const msg = String(err.message || err);
    const status = msg.includes("ما لقينا") || msg.includes("ما عنده شات") ? 404 : 502;
    res.status(status).json({ error: msg });
  }
});

// ----------------------------------------------------------------------
// Site password (Gate): تحقق من كلمة المرور
// ----------------------------------------------------------------------
// لا نُرجع أي تفاصيل عن سبب الفشل، فقط ok/false، لمنع التخمين الموجَّه.
app.post("/api/auth", (req, res) => {
  if (!siteProtected()) {
    // الحماية معطّلة على السيرفر — نسمح بالدخول مباشرة
    return res.json({ ok: true, protected: false });
  }
  const password = req.body && typeof req.body.password === "string" ? req.body.password : "";
  if (timingSafeEqualStr(password.trim(), SITE_PASSWORD)) {
    return res.json({ ok: true, protected: true });
  }
  return res.status(401).json({ ok: false, protected: true, error: "كلمة المرور غير صحيحة" });
});

// يخبر العميل إن كان الموقع محمياً (ليُظهر شاشة كلمة المرور) قبل المحاولة.
app.get("/api/auth-status", (req, res) => {
  res.json({ protected: siteProtected() });
});

// ----------------------------------------------------------------------
// Twitch OAuth (اختياري): السماح للمستخدم بالكتابة في الشات باسمه.
// Flow:
//   1) المتصفح يفتح /auth/twitch?state=... (مع redirect URL يعود لموقعنا)
//   2) تويتش تعود إلى /auth/twitch/callback?code=...
//   3) السيرفر يتبادل code مقابل access_token (server-to-server، سرّي)
//   4) نتحقق من الـ token ونجلب اسم المستخدم، ثم نُعيد التوجيه للصفحة
//      الرئيسية مع وضع token/login في fragment (#) حتى لا يصل للسيرفر في logs.
// ----------------------------------------------------------------------
function buildRedirectUri(req) {
  if (TWITCH_REDIRECT_URI) return TWITCH_REDIRECT_URI;
  // نبنيها من الطلب: نفس المضيف/البروتوكول
  const proto = req.headers["x-forwarded-proto"] || (req.connection && req.connection.encrypted ? "https" : "http");
  const host = req.headers["x-forwarded-host"] || req.headers.host || `localhost:${PORT}`;
  return `${proto}://${host}/auth/twitch/callback`;
}

// بداية الـ OAuth: نُوجّه المستخدم لتويتش ليُفوّض التطبيق.
app.get("/auth/twitch", (req, res) => {
  if (!TWITCH_CLIENT_ID) {
    return res.status(400).send("Twitch OAuth غير مُهيّأ على السيرفر (TWITCH_CLIENT_ID مفقود).");
  }
  const redirectUri = buildRedirectUri(req);
  const state = req.query.state || Math.random().toString(36).slice(2);
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

// رد تويتش: نتبادل code بـ token، ثم نُعيد التوجيه للصفحة بالنتيجة في fragment.
app.get("/auth/twitch/callback", async (req, res) => {
  const code = req.query.code;
  const state = req.query.state;
  if (!code) return res.redirect(302, "/#auth=error&reason=nocode");
  if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
    return res.redirect(302, "/#auth=error&reason=notconfigured");
  }
  const redirectUri = buildRedirectUri(req);

  let tokenResp;
  try {
    tokenResp = await fetch("https://id.twitch.tv/oauth2/token", {
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
  } catch {
    return res.redirect(302, "/#auth=error&reason=network");
  }
  if (!tokenResp.ok) {
    return res.redirect(302, "/#auth=error&reason=token");
  }
  const tokenJson = await tokenResp.json();
  const accessToken = tokenJson.access_token;
  if (!accessToken) return res.redirect(302, "/#auth=error&reason=noaccess");

  // تحقق من الـ token واجلب اسم المستخدم (Login)
  let login = "";
  try {
    const verify = await fetch("https://id.twitch.tv/oauth2/validate", {
      headers: { Authorization: `OAuth ${accessToken}` },
    });
    if (verify.ok) {
      const v = await verify.json();
      login = v.login || "";
    }
  } catch {
    // حتى لو فشل التحقق، الـ token قد يكون صالحاً؛ نُرجعه بلا login
  }

  // نضع النتيجة في fragment (#) كي لا تُ logged في سيرفرات/بروكسي
  const frag = new URLSearchParams({
    auth: "ok",
    access_token: accessToken,
    login,
    state: state || "",
  });
  res.redirect(302, `/#${frag.toString()}`);
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
    socket.send(`NICK ${anonNick}`);
    socket.send(`JOIN #${username}`);
  });

  socket.addEventListener("message", (event) => {
    const lines = String(event.data).split("\r\n").filter(Boolean);
    for (const line of lines) {
      const parsed = parseTwitchLine(line);
      if (!parsed) continue;
      if (parsed.ping) {
        socket.send("PONG :tmi.twitch.tv");
        continue;
      }
      if (parsed.joined) {
        session.setPlatformStatus("twitch", "live");
        continue;
      }
      if (parsed.message) {
        session.pushChat(
          "tw",
          parsed.message.username,
          parsed.message.content,
          parsed.message.color,
          parsed.message.emotes
        );
      }
    }
  });

  socket.addEventListener("close", () => {
    st.socket = null;
    if (session.closed) return;
    session.setPlatformStatus("twitch", "reconnecting");
    scheduleTwitchReconnect(session);
  });

  socket.addEventListener("error", () => {
    // الخطأ يتبعه close عادةً، نكتفي بتحديث الحالة
    session.setPlatformStatus("twitch", "error");
  });
}

function scheduleTwitchReconnect(session) {
  const st = session.platforms.twitch;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectTwitch(session);
  }, PLATFORM_RECONNECT_MS);
}

function teardownTwitch(session) {
  const st = session.platforms.twitch;
  if (st.timer) {
    clearTimeout(st.timer);
    st.timer = null;
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
      const [k, v] = kv.split("=");
      tags[k] = v;
    });
  }

  if (rest.startsWith("PING")) return { ping: true };
  if (rest.includes(" JOIN #")) return { joined: true };

  const privmsgMatch = rest.match(/^:(\S+) PRIVMSG (#\S+) :(.*)$/);
  if (!privmsgMatch) return null;

  const [, prefix, , content] = privmsgMatch;
  const username = tags["display-name"] || prefix.split("!")[0];
  const color = tags["color"] && tags["color"] !== "" ? tags["color"] : "#9147ff";
  const emotes = parseTwitchEmotes(tags["emotes"], content);

  return { message: { username, content, color, emotes } };
}

// يحوّل tag الخاص بالإيموت (مثل: 25:0-4,12-16/1902:6-10)
// إلى مصفوفة {id,start,end} ضمنية النهاية على محارف content.
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

// ----------------------------------------------------------------------
// Kick: public Pusher WebSocket (read-only)
// ----------------------------------------------------------------------
async function connectKick(session) {
  if (session.closed) return;
  const username = session.kickUser;
  const st = session.platforms.kick;
  st.reconnecting = false;
  session.setPlatformStatus("kick", "connecting");

  // حل chatroom_id إن لم يكن معروفاً
  if (!st.chatroomId) {
    try {
      st.chatroomId = await lookupKickChatroom(username);
    } catch (err) {
      st.chatroomId = null;
      session.setPlatformStatus("kick", "error", String(err.message || err));
      scheduleKickReconnect(session);
      return;
    }
  }
  if (session.closed) return;

  const socket = new WebSocket(
    `wss://ws-us2.pusher.com/app/${KICK_PUSHER_APP_KEY}?protocol=7&client=js&version=7.6.0&flash=false`
  );
  st.socket = socket;

  socket.addEventListener("open", () => {
    if (session.closed) return;
    subscribeKickChannel(socket, `chatrooms.${st.chatroomId}.v2`);
    subscribeKickChannel(socket, `chatrooms.${st.chatroomId}`);
    session.setPlatformStatus("kick", "live");

    st.pingTimer = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ event: "pusher:ping", data: {} }));
      }
    }, 25000);
  });

  socket.addEventListener("message", (event) => {
    const parsed = parseKickFrame(event.data);
    if (parsed) session.pushChat("kk", parsed.username, parsed.content, parsed.color, parsed.emotes);
  });

  socket.addEventListener("close", () => {
    st.socket = null;
    if (st.pingTimer) {
      clearInterval(st.pingTimer);
      st.pingTimer = null;
    }
    if (session.closed) return;
    session.setPlatformStatus("kick", "reconnecting");
    scheduleKickReconnect(session);
  });

  socket.addEventListener("error", () => {
    session.setPlatformStatus("kick", "error");
  });
}

function scheduleKickReconnect(session) {
  const st = session.platforms.kick;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectKick(session);
  }, PLATFORM_RECONNECT_MS);
}

function teardownKick(session) {
  const st = session.platforms.kick;
  if (st.timer) {
    clearTimeout(st.timer);
    st.timer = null;
  }
  if (st.pingTimer) {
    clearInterval(st.pingTimer);
    st.pingTimer = null;
  }
  st.reconnecting = false;
  if (st.socket) {
    try {
      st.socket.close();
    } catch {}
    st.socket = null;
  }
}

function subscribeKickChannel(socket, channel) {
  socket.send(JSON.stringify({ event: "pusher:subscribe", data: { channel, auth: "" } }));
}

function parseKickFrame(raw) {
  let outer;
  try {
    outer = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!outer.event || !String(outer.event).includes("ChatMessage")) return null;

  let payload = outer.data;
  if (typeof payload === "string") {
    try {
      payload = JSON.parse(payload);
    } catch {
      return null;
    }
  }
  if (!payload) return null;

  const msgObj = payload.message ?? payload;
  const content = msgObj.content ?? msgObj.message ?? "";
  const sender = payload.sender ?? payload.user ?? {};
  const username = sender.username ?? sender.slug ?? "user";
  const color = sender.identity?.color || "#53fc18";
  const id = msgObj.id || `${username}-${content}-${Date.now()}`;

  // كيك يدمج الإيموت داخل النص نفسه بصيغة [emote:id:name]، فنستخرجه منه.
  const emotes = parseKickEmotes(content);

  if (!content) return null;
  return { id, username, content, color, emotes };
}

// يستخرج إيموتات كيك من النص ([emote:id:name]) إلى {id,start,end}.
function parseKickEmotes(content) {
  if (!content) return [];
  const out = [];
  const re = /\[emote:(\d+):[^\]]+\]/g;
  let m;
  while ((m = re.exec(content)) !== null) {
    const id = Number(m[1]);
    if (!Number.isFinite(id) || id <= 0) continue;
    const start = m.index;
    const end = m.index + m[0].length - 1; // ضمنية النهاية
    if (start < 0 || end >= content.length || start > end) continue;
    out.push({ id, start, end });
  }
  return out;
}

// ----------------------------------------------------------------------
// TikTok: live chat via tiktok-live-connector (Euler Stream Sign Server)
// للقراءة فقط، بدون دخول. تحتاج TIKTOK_SIGN_API_KEY للتشغيل.
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
      "تيكتوك يحتاج مفتاح Euler Stream (TIKTOK_SIGN_API_KEY) على السيرفر."
    );
    return;
  }

  const connection = new TikTokLiveConnection(username, {
    signApiKey: TIKTOK_SIGN_API_KEY,
    // لا نعالج دفعة الرسائل الأولية (تاريخ حديث) لتجنّب التكرار/الإغراق
    processInitialData: false,
    // لا نحتاج بيانات الهدايا الموسّعة في وضع القراءة فقط
    enableExtendedGiftInfo: false,
  });
  st.connection = connection;

  connection.on(WebcastEvent.CHAT, (data) => {
    if (session.closed) return;
    const user = data?.user || {};
    // nickname = الاسم المعروض، uniqueId = @اليوزر؛ نفضّل nickname ونرجع لـ uniqueId
    const displayName = user.nickname || user.uniqueId || "user";
    const content = data?.comment ?? data?.content ?? "";
    if (!content) return;
    // تيكتوك لا يوفّر لوناً لكل مستخدم — نستخدم لون تيكتوك الافتراضي
    session.pushChat("tt", displayName, String(content), "#fe2c55");
  });

  connection.on(ControlEvent.DISCONNECTED, () => {
    st.connection = null;
    if (session.closed) return;
    session.setPlatformStatus("tiktok", "reconnecting");
    scheduleTikTokReconnect(session);
  });

  try {
    await connection.connect();
    if (session.closed) {
      try { connection.disconnect(); } catch {}
      return;
    }
    session.setPlatformStatus("tiktok", "live");
  } catch (err) {
    st.connection = null;
    if (session.closed) return;
    const msg = classifyTikTokError(err);
    if (msg.fatal) {
      // خطأ غير قابل لإعادة المحاولة (يوزر غير صالح/غير مباشر) — نُبلغ ولا نعيد
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
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectTikTok(session);
  }, PLATFORM_RECONNECT_MS);
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

// يصنّف خطأ الاتصال بتيكتوك: fatal = لا فائدة من إعادة المحاولة.
function classifyTikTokError(err) {
  const name = err?.constructor?.name || "";
  const text = String(err?.message || err || "");
  if (err instanceof InvalidUniqueIdError || name === "InvalidUniqueIdError") {
    return { fatal: true, text: "اسم مستخدم تيكتوك غير صالح" };
  }
  if (err instanceof UserOfflineError || name === "UserOfflineError" || /offline|not live/i.test(text)) {
    return { fatal: true, text: "هذا الحساب غير مباشر الآن على تيكتوك" };
  }
  if (/rate limit|quota|429/i.test(text)) {
    return { fatal: false, text: "تم تجاوز حد طلبات Euler Stream، سنحاول لاحقاً" };
  }
  return { fatal: false, text: "تعذّر الاتصال بتيكتوك" };
}

// ----------------------------------------------------------------------
// YouTube: live chat via youtubei.js (InnerTube — بدون مفتاح، بدون quota)
// للقراءة فقط. يقبل رابط بث (watch?v= / youtu.be) أو videoId مكشوف.
// ----------------------------------------------------------------------
// يستخرج videoId من رابط يوتيوب أو يُرجعه كما هو إن كان صالحاً.
// يُرجع null لو لم يُعثر على معرّف صالح.
function extractYouTubeVideoId(input) {
  if (!input) return null;
  const s = String(input).trim();

  // 1) رابط watch?v= (مع/بدون youtu.be، مع/بدون قائمة تشغيل)
  let m = s.match(/[?&]v=([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 2) رابط مختصر youtu.be/VIDEOID
  m = s.match(/youtu\.be\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 3) رابط /live/ أو /embed/
  m = s.match(/(?:live|embed|shorts)\/([A-Za-z0-9_-]{11})/);
  if (m) return m[1];

  // 4) videoId مكشوف (11 حرفاً بالضبط من المحارف المسموحة)
  if (/^[A-Za-z0-9_-]{11}$/.test(s)) return s;

  return null;
}

async function connectYouTube(session) {
  if (session.closed) return;
  const st = session.platforms.youtube;
  st.reconnecting = false;
  session.setPlatformStatus("youtube", "connecting");

  // حلّ videoId من الإدخال
  let videoId = extractYouTubeVideoId(session.youtubeInput);
  if (!videoId) {
    session.setPlatformStatus(
      "youtube",
      "error",
      "أدخل رابط بث يوتيوب صحيح (youtube.com/watch?v=... أو youtu.be/...) أو معرّف الفيديو"
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
    session.setPlatformStatus("youtube", "error", "تعذّر تهيئة عميل يوتيوب: " + String(err?.message || err));
    scheduleYouTubeReconnect(session);
    return;
  }
  if (session.closed) return;

  // نجلب معلومات الفيديو (نحتاجها لإنشاء LiveChat)
  let info;
  try {
    info = await yt.getInfo(videoId);
  } catch (err) {
    if (session.closed) return;
    const msg = String(err?.message || err);
    // فيديو غير موجود/خاص
    if (/not found|private|404|does not exist|VideoUnavailable/i.test(msg)) {
      session.setPlatformStatus("youtube", "error", "هذا الفيديو غير موجود أو خاص");
      return; // fatal
    }
    session.setPlatformStatus("youtube", "error", "تعذّر جلب بيانات البث: " + msg);
    scheduleYouTubeReconnect(session);
    return;
  }
  if (session.closed) return;

  // تحقق أنه بث مباشر فعلاً
  if (!info.basic_info?.is_live) {
    session.setPlatformStatus("youtube", "error", "هذا البث غير مباشر الآن (أو انتهى)");
    return; // fatal — لا فائدة من إعادة المحاولة حتى يتغيّر الإدخال
  }

  // أنشئ LiveChat وابدأ التدفّق
  let livechat;
  try {
    livechat = info.getLiveChat();
  } catch (err) {
    if (session.closed) return;
    session.setPlatformStatus("youtube", "error", "لا يوجد شات لهذا البث: " + String(err?.message || err));
    return;
  }
  st.livechat = livechat;
  if (session.closed) {
    try { livechat.stop(); } catch {}
    return;
  }

  // علامة لتجنّب إعادة الإرسال المزدوج لرسالة «انتهى البث»
  let endedHandled = false;
  const markEnded = (reason) => {
    if (endedHandled || session.closed) return;
    endedHandled = true;
    st.livechat = null;
    session.setPlatformStatus("youtube", "error", reason);
  };

  // رسائل الشات العادية فقط (نتجاهل المدفوعة/الملصقات/الإعلانات)
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
      // يوتيوب لا يوفّر لوناً لكل مستخدم — نستخدم لون يوتيوب الافتراضي
      session.pushChat("yt", username, content, "#ff0000");
    } catch {
      // نتجاهل العناصر التي لا نستطيع تحليلها بصمت
    }
  });

  // البث انتهى نهائياً — لا فائدة من إعادة الاتصال
  livechat.on("end", () => markEnded("انتهى بث يوتيوب"));

  // خطأ: المكتبة تُعيد المحاولة داخلياً حتى 10 مرات، ثم تُطلق end.
  // هنا نكتفي بتسجيل الحالة العابرة دون قطع الاتصال.
  livechat.on("error", (err) => {
    if (session.closed) return;
    // نحدّث الحالة فقط دون إعادة جدولة (المكتبة تتعامل معها)
    if (st.status !== "error") session.setPlatformStatus("youtube", "reconnecting");
  });

  // أول استجابة ناجحة = البث حيّ والشات يعمل
  livechat.on("start", () => {
    if (!session.closed) session.setPlatformStatus("youtube", "live");
  });

  // ابدأ التدفّق
  try {
    livechat.start();
  } catch (err) {
    if (session.closed) return;
    st.livechat = null;
    session.setPlatformStatus("youtube", "error", "فشل بدء تدفّق الشات: " + String(err?.message || err));
    scheduleYouTubeReconnect(session);
  }
}

function scheduleYouTubeReconnect(session) {
  const st = session.platforms.youtube;
  if (st.reconnecting || session.closed) return;
  st.reconnecting = true;
  st.timer = setTimeout(() => {
    st.timer = null;
    if (!session.closed) connectYouTube(session);
  }, PLATFORM_RECONNECT_MS);
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
// Twitch SEND: اتصال IRC مُصادَق باسم المستخدم للكتابة في الشات.
// الكتابة في تويتش تتطلب اتصالاً مُسجَّلاً (NICK + PASS token) من حساب حقيقي،
// فلا يمكن إعادة استخدام اتصال القراءة المجهول. نُنشئ اتصالاً لكل جلسة عند
// أول طلب إرسال، ونحتفظ به طوال عمر الجلسة.
// ----------------------------------------------------------------------
// يتحقق من صحة توكن تويتش ويرجع login (اسم المستخدم) أو null.
async function validateTwitchToken(accessToken) {
  try {
    const r = await fetch("https://id.twitch.tv/oauth2/validate", {
      headers: { Authorization: `OAuth ${accessToken}` },
    });
    if (!r.ok) return null;
    const j = await r.json();
    if (!j.login || j.expires_in <= 0) return null;
    // نتأكد أن الـ scope يسمح بالكتابة
    const scopes = Array.isArray(j.scopes) ? j.scopes : [];
    if (!scopes.includes("chat:edit")) return null;
    return j.login;
  } catch {
    return null;
  }
}

// يضمن وجود اتصال IRC مُصادَق للكتابة للجلسة، ويستدعي onReady(username) متى صار جاهزاً.
// نعيد وعداً يحلّ باسم المستخدم عند الجاهزية، أو يُرفض برسالة خطأ.
function ensureTwitchSendConnection(session, accessToken) {
  return new Promise((resolve, reject) => {
    if (session.closed) return reject(new Error("الجلسة مغلقة"));

    validateTwitchToken(accessToken).then((login) => {
      if (!login) return reject(new Error("توكن تويتش غير صالح أو منتهي — أعِد الربط"));
      if (!session.twitchUser) return reject(new Error("لا توجد قناة تويتش في هذه الجلسة"));

      // إن وُجد اتصال سابق لنفس الـ login، أعد استخدامه
      const existing = session.sendConnection;
      if (existing && existing.login === login && existing.socket && existing.socket.readyState === WebSocket.OPEN) {
        return resolve(login);
      }
      // أغلق أي اتصال سابق مختلف
      if (existing && existing.socket) {
        try { existing.socket.close(); } catch {}
      }

      const socket = new WebSocket("wss://irc-ws.chat.twitch.tv:443");
      const conn = { socket, login, ready: false, pending: [] };
      session.sendConnection = conn;

      socket.addEventListener("open", () => {
        socket.send("CAP REQ :twitch.tv/tags twitch.tv/commands");
        socket.send(`PASS oauth:${accessToken}`);
        socket.send(`NICK ${login}`);
        // ننتظر 001 (WELCOME) للتأكد من نجاح المصادقة
      });

      socket.addEventListener("message", (event) => {
        const lines = String(event.data).split("\r\n").filter(Boolean);
        for (const line of lines) {
          // 001 = ترحيب = المصادقة نجحت
          if (/\s001\s/.test(line)) {
            socket.send(`JOIN #${session.twitchUser}`);
            conn.ready = true;
            // أرسل أي رسائل معلّقة
            for (const p of conn.pending) p();
            conn.pending = [];
            resolve(login);
            continue;
          }
          // NOTICE يعقب فشل المصادقة عادةً
          if (/NOTICE\s+\*\s+:Login authentication failed/i.test(line) ||
              /NOTICE.*:Login authentication failed/i.test(line)) {
            try { socket.close(); } catch {}
            session.sendConnection = null;
            reject(new Error("فشل تسجيل الدخول لتويتش — توكن غير صالح"));
            return;
          }
          if (line.startsWith("PING")) {
            socket.send("PONG :tmi.twitch.tv");
          }
        }
      });

      socket.addEventListener("close", () => {
        if (session.sendConnection === conn) session.sendConnection = null;
      });
      socket.addEventListener("error", () => {
        if (!conn.ready) reject(new Error("تعذّر الاتصال بخادم تويتش للكتابة"));
      });
    }).catch(() => reject(new Error("تعذّر التحقق من توكن تويتش")));
  });
}

// يعالج طلب الإرسال من العميل: يتطلب accessToken + content.
function handleTwitchSend(session, ws, msg) {
  if (!session || !session.twitchUser) {
    ws.send(JSON.stringify({ type: "send_result", ok: false, error: "لا توجد قناة تويتش مرتبطة" }));
    return;
  }
  const accessToken = typeof msg.access_token === "string" ? msg.access_token.trim() : "";
  const content = typeof msg.content === "string" ? msg.content.slice(0, 500) : "";
  if (!accessToken) {
    ws.send(JSON.stringify({ type: "send_result", ok: false, error: "يجب ربط حساب تويتش أولاً" }));
    return;
  }
  if (!content) {
    ws.send(JSON.stringify({ type: "send_result", ok: false, error: "الرسالة فارغة" }));
    return;
  }

  ensureTwitchSendConnection(session, accessToken)
    .then((login) => {
      const conn = session.sendConnection;
      if (!conn || !conn.socket || conn.socket.readyState !== WebSocket.OPEN) {
        throw new Error("الاتصال غير جاهز");
      }
      // PRIVMSG: صيغة IRC القياسية لإرسال رسالة شات
      conn.socket.send(`PRIVMSG #${session.twitchUser} :${content}`);
      ws.send(JSON.stringify({ type: "send_result", ok: true }));
    })
    .catch((err) => {
      ws.send(JSON.stringify({ type: "send_result", ok: false, error: String(err.message || err) }));
    });
}

// ----------------------------------------------------------------------
// WebSocket للعملاء (المتصفحات)
// ----------------------------------------------------------------------
wss.on("connection", (ws) => {
  let session = null;
  // المصادقة مطلوبة قبل أي شيء آخر إن كانت الحماية مفعّلة.
  let authed = !siteProtected();

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // المصادقة على الـ WebSocket: يجب أن تسبق أي أمر آخر.
    if (msg.type === "auth") {
      if (!siteProtected()) {
        authed = true;
        ws.send(JSON.stringify({ type: "auth_ok" }));
        return;
      }
      const password = typeof msg.password === "string" ? msg.password : "";
      if (timingSafeEqualStr(password.trim(), SITE_PASSWORD)) {
        authed = true;
        ws.send(JSON.stringify({ type: "auth_ok" }));
      } else {
        ws.send(JSON.stringify({ type: "auth_fail", error: "كلمة المرور غير صحيحة" }));
        try { ws.close(4001, "unauthorized"); } catch {}
      }
      return;
    }

    // كل ما يلي يتطلب مصادقة ناجحة أولاً.
    if (!authed) {
      ws.send(JSON.stringify({ type: "need_auth", error: "مطلوب كلمة المرور أولاً" }));
      try { ws.close(4001, "unauthorized"); } catch {}
      return;
    }

    if (msg.type === "join") {
      if (session) session.removeClient(ws); // بدل الجلسة لو سبق وانضم
      const tw = (msg.twitch || "").trim().toLowerCase() || null;
      const kk = (msg.kick || "").trim().toLowerCase() || null;
      // تيكتوك: ننظّف الاسم من @ أو رابط كامل (نبقي الجزء بعد آخر /)
      let tt = (msg.tiktok || "").trim();
      tt = tt.replace(/^@/, "");
      if (/\/|@/.test(tt)) {
        const m = tt.split(/[/@]/).filter(Boolean).pop();
        tt = m || "";
      }
      tt = tt.toLowerCase() || null;
      // يوتيوب: يقبل رابطاً كاملاً أو videoId — لا نُطعّم الحالة بل نُمرّره كما هو
      const yt = (msg.youtube || "").trim() || null;
      if (!tw && !kk && !tt && !yt) {
        ws.send(JSON.stringify({ type: "error", error: "أدخل اسم مستخدم واحد على الأقل" }));
        return;
      }
      session = getOrCreateSession(tw, kk, tt, yt);
      session.addClient(ws, msg.lastId);
      return;
    }

    // إرسال رسالة كشات (يتطلب توكن تويتش صالح من العميل، ونمرّره للسيرفر لأن
    // الكتابة في IRC تتطلب اتصالاً مُصادَقاً باسم المستخدم).
    if (msg.type === "send") {
      handleTwitchSend(session, ws, msg);
      return;
    }
  });

  ws.on("close", () => {
    if (session) session.removeClient(ws);
  });
});

// ----------------------------------------------------------------------
// شبكة أمان: فحص دوري يحصي الجلسات المعطّلة/المعلّقة ويغلقها قسرياً.
// هذا مجرد احتياط لو فشل تايمر فترة السماح لسبب ما (bug/مرجع معلّق) —
// لا يُعتمد عليه كآلية انتهاء عادية لأن تأخّر التدمير حتى 4 ساعات يهدر الغرض.
// ----------------------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const session of sessions.values()) {
    // جلسة بلا عملاء تجاوزت فترة السماح ولم تُدمَّر بعد (تايمر السماح فشل لأي سبب).
    if (session.clients.size === 0 && session.emptySince && now - session.emptySince >= SESSION_GRACE_MS) {
      session.destroy();
      sessions.delete(session.key);
    }
  }
}, SWEEP_INTERVAL_MS).unref();

server.listen(PORT, () => {
  console.log(`Server running: http://localhost:${PORT}`);
});
