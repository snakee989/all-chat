const express = require("express");
const path = require("path");
const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");

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
const KICK_PUSHER_APP_KEY = "32cbd69e4b950bf97679";

// ----------------------------------------------------------------------
// SessionManager: كل جلسة (twitch + kick معاً) لها حالتها الخاصة.
// الجلسة تموت تلقائياً عندما يصبح عدد عملائها صفراً.
// ----------------------------------------------------------------------
const sessions = new Map(); // key "tw=xxx|kk=yyy" -> Session

function sessionKey(twitchUser, kickUser) {
  return `tw=${twitchUser || ""}|kk=${kickUser || ""}`;
}

class Session {
  constructor(twitchUser, kickUser) {
    this.twitchUser = twitchUser || null;
    this.kickUser = kickUser || null;
    this.key = sessionKey(twitchUser, kickUser);
    this.clients = new Set(); // المتصفحات المتصلة بهذه الجلسة
    this.backlog = []; // آخر الرسائل (نرسلها لكل عميل جديد/معيد اتصال)
    this.seq = 0; // عدّاد تصاعدي لكل رسالة (مفتاح الاستئناف عند إعادة الاتصال)
    this.platforms = {
      twitch: { socket: null, status: "connecting", reconnecting: false, timer: null },
      kick: { socket: null, status: "connecting", reconnecting: false, timer: null, pingTimer: null, chatroomId: null, pendingSubs: [] },
    };
    this.closed = false;
  }

  // إضافة عميل (متصفح) للجلسة.
  // lastId: آخر id استلمه العميل سابقاً (إن وُجد) ليُرسل له فقط ما فاته.
  addClient(ws, lastId) {
    this.clients.add(ws);

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
        twitchStatus: this.platforms.twitch.status,
        kickStatus: this.platforms.kick.status,
        backlog,
        lastId: this.seq,
      })
    );
  }

  removeClient(ws) {
    this.clients.delete(ws);
    // إذا ما بقي أحد، نموذج الجلسة (نغلِق الاتصالات ونحذفها)
    if (this.clients.size === 0) {
      this.destroy();
      sessions.delete(this.key);
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

  setPlatformStatus(platform, status) {
    this.platforms[platform].status = status;
    this.broadcast({ type: "status", platform, status });
  }

  destroy() {
    this.closed = true;
    teardownTwitch(this);
    teardownKick(this);
  }
}

function getOrCreateSession(twitchUser, kickUser) {
  const key = sessionKey(twitchUser, kickUser);
  let s = sessions.get(key);
  if (!s) {
    s = new Session(twitchUser, kickUser);
    sessions.set(key, s);
    if (twitchUser) connectTwitch(s);
    if (kickUser) connectKick(s);
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
      session.setPlatformStatus("kick", "error");
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
// WebSocket للعملاء (المتصفحات)
// ----------------------------------------------------------------------
wss.on("connection", (ws) => {
  let session = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.type === "join") {
      if (session) session.removeClient(ws); // بدل الجلسة لو سبق وانضم
      const tw = (msg.twitch || "").trim().toLowerCase() || null;
      const kk = (msg.kick || "").trim().toLowerCase() || null;
      if (!tw && !kk) {
        ws.send(JSON.stringify({ type: "error", error: "أدخل اسم مستخدم واحد على الأقل" }));
        return;
      }
      session = getOrCreateSession(tw, kk);
      session.addClient(ws, msg.lastId);
    }
  });

  ws.on("close", () => {
    if (session) session.removeClient(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Server running: http://localhost:${PORT}`);
});
