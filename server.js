// ══════════════════════════════════════════════════════════════
// BOT PANEL — সম্পূর্ণ নতুন করে বানানো, সরল ও শক্ত ভিত্তি
// ══════════════════════════════════════════════════════════════
const express   = require("express");
const fs        = require("fs");
const path      = require("path");
const { spawn, fork } = require("child_process");
const multer    = require("multer");
const WebSocket = require("ws");
const archiver  = require("archiver");
const unzipper  = require("unzipper");
const crypto    = require("crypto");
const http      = require("http");

process.on("uncaughtException", (err) => {
  console.log("⚠️ uncaughtException (সার্ভার বাঁচানো হলো):", err && err.message);
});
process.on("unhandledRejection", (err) => {
  console.log("⚠️ unhandledRejection (সার্ভার বাঁচানো হলো):", err && (err.message || err));
});

const app  = express();
const PORT = process.env.PORT || 10000;
const BDIR = path.join(__dirname, "bot");
if (!fs.existsSync(BDIR)) fs.mkdirSync(BDIR, { recursive: true });

const CFG_FILE = path.join(__dirname, "panel.config.json");
function loadJ(f, def) { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch (e) { return def; } }
function saveJ(f, d) { try { fs.writeFileSync(f, JSON.stringify(d, null, 2)); } catch (e) {} }

let cfg = loadJ(CFG_FILE, {});
if (!cfg.authToken) { cfg.authToken = crypto.randomBytes(24).toString("hex"); saveJ(CFG_FILE, cfg); }
if (!cfg.password) { cfg.password = process.env.PANEL_PASSWORD || "admin123"; saveJ(CFG_FILE, cfg); }

// ── stats (lifetime) — MongoDB-তে ব্যাকআপ থাকবে ──
let stats = loadJ(path.join(__dirname, "stats.json"), { starts: 0, crashes: 0, totalUptime: 0, history: [] });
function saveStats() { saveJ(path.join(__dirname, "stats.json"), stats); saveStatsToMongo(); }

// ── কুকি পার্স করার সহজ হেল্পার (কোনো npm প্যাকেজ লাগে না) ──
function getCookies(req) {
  const out = {};
  const h = req.headers.cookie;
  if (h) {
    h.split(";").forEach(function (p) {
      const i = p.indexOf("=");
      if (i > 0) out[p.slice(0, i).trim()] = decodeURIComponent(p.slice(i + 1).trim());
    });
  }
  return out;
}

// ── AUTH — শুধু একটা persistent কুকি টোকেন, কোনো session store লাগে না ──
function auth(req, res, next) {
  const token = getCookies(req).authToken;
  if (token && token === cfg.authToken) return next();
  if (req.path.indexOf("/api/") === 0) return res.status(401).json({ error: "unauthorized" });
  res.redirect("/login");
}

app.use(express.json({ limit: "200mb" }));
app.use(express.urlencoded({ extended: true, limit: "200mb" }));

// ══════════════════════════════════════════════════════════════
// MONGODB — ফাইল ও স্ট্যাটস ব্যাকআপের জন্য
// ══════════════════════════════════════════════════════════════
let mongoose = null, FileModel = null, dbConnected = false;

async function connectMongo() {
  const uri = process.env.MONGO_URI || cfg.mongoUri;
  if (!uri) { console.log("ℹ️ MONGO_URI নেই — MongoDB ছাড়াই চলবে"); return; }
  try {
    mongoose = require("mongoose");
    await mongoose.connect(uri, { serverSelectionTimeoutMS: 8000 });
    dbConnected = true;
    console.log("✅ MongoDB connected");
    const schema = new mongoose.Schema({
      path: { type: String, unique: true, index: true },
      content: Buffer,
      isDir: Boolean,
      mtime: Date,
      size: Number,
    });
    FileModel = mongoose.models.BotFile || mongoose.model("BotFile", schema);
    await restoreFilesFromMongo();
    await restoreStatsFromMongo();
  } catch (e) {
    console.log("⚠️ MongoDB connect error:", e.message);
    dbConnected = false;
  }
}

async function saveFileToMongo(relPath, content, isDir) {
  if (!dbConnected || !FileModel) return;
  try {
    await FileModel.updateOne(
      { path: relPath },
      { path: relPath, content: isDir ? null : Buffer.from(content), isDir: !!isDir, mtime: new Date(), size: content ? content.length : 0 },
      { upsert: true }
    );
  } catch (e) { console.log("⚠️ mongo save error:", e.message); }
}

async function deleteFileFromMongo(relPath) {
  if (!dbConnected || !FileModel) return;
  try {
    const esc = relPath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    await FileModel.deleteMany({ path: { $regex: "^" + esc + "(/|$)" } });
  } catch (e) { console.log("⚠️ mongo delete error:", e.message); }
}

async function restoreFilesFromMongo() {
  if (!dbConnected || !FileModel) return;
  try {
    const all = await FileModel.find({ path: { $not: /^__/ } }).lean();
    for (const f of all) {
      const full = path.join(BDIR, f.path);
      try {
        if (f.isDir) {
          fs.mkdirSync(full, { recursive: true });
        } else {
          fs.mkdirSync(path.dirname(full), { recursive: true });
          fs.writeFileSync(full, f.content || Buffer.alloc(0));
        }
      } catch (e) {}
    }
    console.log("✅ MongoDB থেকে " + all.length + "টা ফাইল restore হয়েছে");
  } catch (e) { console.log("⚠️ restore error:", e.message); }
}

async function saveStatsToMongo() {
  if (!dbConnected || !FileModel) return;
  try {
    await FileModel.updateOne(
      { path: "__panel_stats__" },
      { path: "__panel_stats__", content: Buffer.from(JSON.stringify(stats)), isDir: false, mtime: new Date() },
      { upsert: true }
    );
  } catch (e) {}
}

async function restoreStatsFromMongo() {
  if (!dbConnected || !FileModel) return;
  try {
    const doc = await FileModel.findOne({ path: "__panel_stats__" });
    if (doc && doc.content) {
      try { stats = Object.assign(stats, JSON.parse(doc.content.toString())); } catch (e) {}
    }
  } catch (e) {}
}

// ══════════════════════════════════════════════════════════════
// বট প্রসেস কন্ট্রোল
// ══════════════════════════════════════════════════════════════
let botProc = null, botStart = null, botReady = false;
let botLogs = [];
let autoRestart = true;
let installInProgress = false;
let consecutiveCrashes = 0;
let restartTimer = null;

const wss = new WebSocket.Server({ noServer: true });
function broadcast(data) {
  wss.clients.forEach(function (c) {
    if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify(data));
  });
}
function addLog(text, type) {
  const entry = { time: new Date().toLocaleTimeString("en-GB"), text: text, type: type || "info" };
  botLogs.push(entry);
  if (botLogs.length > 1500) botLogs.shift();
  broadcast({ type: "log", data: entry });
}

function stripAnsi(s) { return s.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, ""); }
function classifyLine(s) {
  if (/error|err!|❌|exception/i.test(s)) return "error";
  if (/warn|⚠️/i.test(s)) return "warn";
  if (/✅|success|সফল/i.test(s)) return "success";
  return "info";
}

function startBot(reason) {
  if (botProc) return { ok: false, msg: "বট ইতিমধ্যে চলছে" };
  if (installInProgress) return { ok: false, msg: "npm install চলছে, একটু অপেক্ষা করো" };

  const idxCandidates = ["index.js", "app.js", "bot.js"];
  let idx = null;
  for (const f of idxCandidates) { if (fs.existsSync(path.join(BDIR, f))) { idx = f; break; } }
  if (!idx) return { ok: false, msg: "index.js পাওয়া যায়নি — বট আপলোড করো" };

  const nmDir = path.join(BDIR, "node_modules");

  function reallySpawn() {
    botProc = fork(idx, [], { cwd: BDIR, env: Object.assign({}, process.env), stdio: ["ignore", "pipe", "pipe", "ipc"] });
    botStart = Date.now();
    botReady = false;
    stats.starts++;
    saveStats();
    addLog("🟡 বট চালু হচ্ছে (" + (reason || "manual") + ")", "warn");
    broadcast({ type: "status", running: false, starting: true });

    botProc.stdout.on("data", function (d) {
      const s = stripAnsi(d.toString()).trim();
      if (s) addLog(s, classifyLine(s));
    });
    botProc.stderr.on("data", function (d) {
      const s = stripAnsi(d.toString()).trim();
      if (s) addLog(s, "error");
    });
    botProc.on("message", function (msg) {
      if (msg && msg.type === "bot_ready") {
        botReady = true;
        addLog("✅ বট সম্পূর্ণ প্রস্তুত — " + (msg.commands || 0) + " কমান্ড লোড হয়েছে", "success");
        broadcast({ type: "status", running: true, starting: false, ready: true });
      }
    });
    botProc.on("exit", function (code, sig) {
      const up = botStart ? Math.floor((Date.now() - botStart) / 1000) : 0;
      stats.totalUptime += up;
      stats.history.push({ date: new Date().toISOString(), uptime: up, code: code || sig });
      if (stats.history.length > 100) stats.history.shift();
      if (code !== 0 && code !== null) stats.crashes++;
      saveStats();
      addLog("🔴 বট বন্ধ (code:" + (code || sig) + ", uptime:" + up + "s)", "error");
      botProc = null; botStart = null; botReady = false;
      broadcast({ type: "status", running: false, starting: false, ready: false });

      if (autoRestart && code !== 0 && code !== null) {
        if (up >= 120) consecutiveCrashes = 0; else consecutiveCrashes++;
        const waitSec = Math.min(10 * Math.pow(2, consecutiveCrashes), 300);
        addLog("🔄 Auto-restart " + waitSec + " সেকেন্ড পরে...", "warn");
        restartTimer = setTimeout(function () { startBot("auto-restart"); }, waitSec * 1000);
      }
    });
  }

  if (!fs.existsSync(nmDir)) {
    installInProgress = true;
    addLog("📦 npm install শুরু (ব্যাকগ্রাউন্ডে — সাইট ফ্রিজ হবে না)", "warn");
    const npmProc = spawn("npm", ["install", "--omit=dev", "--no-audit", "--no-fund"], { cwd: BDIR });
    let errBuf = "";
    npmProc.stdout.on("data", function (d) { addLog("📦 " + d.toString().trim(), "info"); });
    npmProc.stderr.on("data", function (d) { errBuf += d.toString(); });
    const hangGuard = setTimeout(function () {
      addLog("⚠️ npm install ৫ মিনিটেও শেষ হয়নি, বন্ধ করা হলো", "error");
      try { npmProc.kill("SIGKILL"); } catch (e) {}
    }, 5 * 60 * 1000);
    npmProc.on("exit", function (code) {
      clearTimeout(hangGuard);
      installInProgress = false;
      if (code === 0) { addLog("✅ npm install সম্পন্ন", "success"); reallySpawn(); }
      else { addLog("❌ npm install ব্যর্থ: " + errBuf.slice(-300), "error"); }
    });
    return { ok: true, msg: "npm install শুরু হয়েছে ব্যাকগ্রাউন্ডে" };
  }

  reallySpawn();
  return { ok: true, msg: "বট চালু হয়েছে" };
}

function stopBot() {
  if (restartTimer) { clearTimeout(restartTimer); restartTimer = null; }
  if (!botProc) return { ok: false, msg: "বট এমনিতেই বন্ধ" };
  try { botProc.kill("SIGTERM"); } catch (e) {}
  return { ok: true, msg: "বট বন্ধ করা হচ্ছে" };
}

function notifyBotFile(action, relPath) {
  if (!botProc || !botProc.connected) return;
  try { botProc.send({ type: "panel_file_change", action: action, relPath: relPath }); } catch (e) {}
}

// ══════════════════════════════════════════════════════════════
// ফাইল ম্যানেজার হেল্পার
// ══════════════════════════════════════════════════════════════
function safePath(base, rel) {
  const p = path.normalize(path.join(base, rel || ""));
  if (p.indexOf(path.normalize(base)) !== 0) throw new Error("invalid path");
  return p;
}

function walkCount(dir) {
  let c = 0;
  try {
    fs.readdirSync(dir).forEach(function (f) {
      if (f === "node_modules" || f === ".git") return;
      const full = path.join(dir, f);
      const s = fs.statSync(full);
      c += s.isDirectory() ? walkCount(full) : 1;
    });
  } catch (e) {}
  return c;
}
let fileCountCache = { value: 0, at: 0 };
function countBotFiles() {
  if (Date.now() - fileCountCache.at < 60000) return fileCountCache.value;
  fileCountCache = { value: walkCount(BDIR), at: Date.now() };
  return fileCountCache.value;
}

// ══════════════════════════════════════════════════════════════
// ROUTES — AUTH
// ══════════════════════════════════════════════════════════════
app.get("/login", function (req, res) {
  res.set("Cache-Control", "no-store");
  if (getCookies(req).authToken === cfg.authToken) return res.redirect("/");
  res.send(loginHTML());
});
app.post("/login", function (req, res) {
  if (req.body.password === cfg.password) {
    res.append("Set-Cookie", "authToken=" + cfg.authToken + "; Max-Age=" + (30 * 24 * 60 * 60) + "; Path=/; HttpOnly; SameSite=Lax");
    res.json({ ok: true });
  } else {
    res.json({ ok: false, msg: "ভুল পাসওয়ার্ড" });
  }
});
app.get("/logout", function (req, res) {
  res.append("Set-Cookie", "authToken=; Max-Age=0; Path=/");
  res.redirect("/login");
});

app.get("/", auth, function (req, res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate");
  res.send(mainHTML());
});

app.get("/ping", function (req, res) { res.json({ ok: true }); });

// ══════════════════════════════════════════════════════════════
// ROUTES — বট কন্ট্রোল / স্ট্যাটস
// ══════════════════════════════════════════════════════════════
app.get("/api/stats", auth, function (req, res) {
  res.json({
    running: !!botProc,
    ready: botReady,
    starting: !!botProc && !botReady,
    currentUptime: botStart ? Math.floor((Date.now() - botStart) / 1000) : 0,
    memMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    serverUptime: Math.floor(process.uptime()),
    node: process.version,
    botFiles: countBotFiles(),
    starts: stats.starts,
    crashes: stats.crashes,
    totalUptime: stats.totalUptime,
    history: stats.history.slice().reverse().slice(0, 10),
    autoRestart: autoRestart,
    mongoConnected: dbConnected
  });
});

app.post("/api/bot/start", auth, function (req, res) { res.json(startBot("manual")); });
app.post("/api/bot/stop", auth, function (req, res) { res.json(stopBot()); });
app.post("/api/bot/restart", auth, function (req, res) {
  stopBot();
  setTimeout(function () { res.json(startBot("restart")); }, 2500);
});
app.post("/api/bot/install", auth, function (req, res) {
  if (installInProgress) return res.json({ ok: false, msg: "npm install ইতিমধ্যে চলছে" });
  installInProgress = true;
  addLog("📦 npm install শুরু (ম্যানুয়াল)", "warn");
  const npmProc = spawn("npm", ["install", "--omit=dev", "--no-audit", "--no-fund"], { cwd: BDIR });
  let errBuf = "";
  npmProc.stdout.on("data", function (d) { addLog("📦 " + d.toString().trim(), "info"); });
  npmProc.stderr.on("data", function (d) { errBuf += d.toString(); });
  npmProc.on("exit", function (code) {
    installInProgress = false;
    if (code === 0) addLog("✅ npm install সম্পন্ন", "success");
    else addLog("❌ npm install ব্যর্থ: " + errBuf.slice(-300), "error");
  });
  res.json({ ok: true, msg: "npm install শুরু হয়েছে" });
});
app.get("/api/bot/logs", auth, function (req, res) { res.json({ logs: botLogs }); });
app.post("/api/bot/clearlogs", auth, function (req, res) { botLogs = []; broadcast({ type: "clearLogs" }); res.json({ ok: true }); });

app.post("/api/cookie", auth, function (req, res) {
  try {
    fs.writeFileSync(path.join(BDIR, "appstate.json"), req.body.cookie || "");
    saveFileToMongo("appstate.json", req.body.cookie || "", false);
    const r = startBot("cookie-save");
    res.json(r);
  } catch (e) { res.status(500).json({ ok: false, msg: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// ROUTES — ফাইল ম্যানেজার
// ══════════════════════════════════════════════════════════════
app.get("/api/files", auth, function (req, res) {
  try {
    const dir = safePath(BDIR, req.query.path || "");
    if (!fs.existsSync(dir)) return res.json({ items: [], current: req.query.path || "" });
    const items = fs.readdirSync(dir)
      .filter(function (n) { return n.indexOf(".") !== 0; })
      .map(function (name) {
        const full = path.join(dir, name);
        const s = fs.statSync(full);
        return { name: name, isDir: s.isDirectory(), size: s.size, mtime: s.mtime };
      })
      .sort(function (a, b) { return (b.isDir - a.isDir) || a.name.localeCompare(b.name); });
    res.json({ items: items, current: req.query.path || "" });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/file/read", auth, function (req, res) {
  try {
    const f = safePath(BDIR, req.query.path);
    const s = fs.statSync(f);
    if (s.size > 5 * 1024 * 1024) return res.json({ error: "ফাইল অনেক বড়" });
    res.json({ content: fs.readFileSync(f, "utf8") });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/file/save", auth, function (req, res) {
  try {
    const f = safePath(BDIR, req.body.path);
    fs.mkdirSync(path.dirname(f), { recursive: true });
    fs.writeFileSync(f, req.body.content || "");
    const rel = path.relative(BDIR, f);
    saveFileToMongo(rel, req.body.content || "", false);
    notifyBotFile("save", rel);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/file/delete", auth, function (req, res) {
  try {
    const f = safePath(BDIR, req.body.path);
    const rel = path.relative(BDIR, f);
    fs.rmSync(f, { recursive: true, force: true });
    deleteFileFromMongo(rel);
    notifyBotFile("delete", rel);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/file/mkdir", auth, function (req, res) {
  try {
    const f = safePath(BDIR, req.body.path);
    fs.mkdirSync(f, { recursive: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/file/download", auth, function (req, res) {
  try {
    const f = safePath(BDIR, req.query.path);
    if (fs.statSync(f).isDirectory()) {
      res.setHeader("Content-Disposition", "attachment; filename=\"" + path.basename(f) + ".zip\"");
      const a = archiver("zip", { zlib: { level: 9 } });
      a.pipe(res); a.directory(f, false); a.finalize();
    } else { res.download(f); }
  } catch (e) { res.status(500).send(e.message); }
});

const upload = multer({ dest: path.join(require("os").tmpdir(), "panel-uploads") });
app.post("/api/upload", auth, upload.single("file"), function (req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: "no file" });
    const destRel = req.body.path || "";
    const destDir = safePath(BDIR, destRel);
    fs.mkdirSync(destDir, { recursive: true });
    const original = req.file.originalname;
    if (/\.zip$/i.test(original)) {
      fs.createReadStream(req.file.path).pipe(unzipper.Extract({ path: destDir })).on("close", function () {
        fs.unlinkSync(req.file.path);
        res.json({ ok: true, extracted: true });
      }).on("error", function (e) { res.status(500).json({ error: e.message }); });
    } else {
      const dest = path.join(destDir, original);
      fs.renameSync(req.file.path, dest);
      const rel = path.relative(BDIR, dest);
      saveFileToMongo(rel, fs.readFileSync(dest), false);
      notifyBotFile("save", rel);
      res.json({ ok: true });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════
// HTML — লগইন পেজ
// ══════════════════════════════════════════════════════════════
function loginHTML() {
  return '<!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">' +
    '<title>Bot Panel Login</title><style>' +
    'body{margin:0;background:#07070e;color:#dde0f0;font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh}' +
    '.box{background:#141424;border:1px solid #232338;border-radius:16px;padding:28px;width:min(340px,90vw)}' +
    'h1{font-size:20px;margin:0 0 20px;text-align:center}' +
    'input{width:100%;box-sizing:border-box;padding:12px;border-radius:10px;border:1px solid #232338;background:#0d0d18;color:#dde0f0;font-size:15px;margin-bottom:12px}' +
    'button{width:100%;padding:12px;border:none;border-radius:10px;background:#6c63ff;color:#fff;font-size:15px;font-weight:700;cursor:pointer}' +
    '#msg{color:#f05252;font-size:13px;text-align:center;min-height:18px;margin-bottom:8px}' +
    '</style></head><body><div class="box"><h1>🤖 Bot Panel</h1>' +
    '<input type="password" id="pw" placeholder="পাসওয়ার্ড">' +
    '<div id="msg"></div>' +
    '<button onclick="doLogin()">লগইন</button>' +
    '<script>' +
    'function doLogin(){' +
    'fetch("/login",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"password="+encodeURIComponent(document.getElementById("pw").value)})' +
    '.then(function(r){return r.json();}).then(function(d){' +
    'if(d.ok){location.href="/";}else{document.getElementById("msg").textContent=d.msg||"ব্যর্থ";}' +
    '}).catch(function(e){document.getElementById("msg").textContent="নেটওয়ার্ক এরর";});' +
    '}' +
    'document.getElementById("pw").addEventListener("keydown",function(e){if(e.key==="Enter")doLogin();});' +
    '</script></div></body></html>';
}

// ══════════════════════════════════════════════════════════════
// HTML — মূল প্যানেল (স্ট্রিং concatenation ব্যবহার করা হয়েছে ইচ্ছাকৃতভাবে —
// ব্যাকটিক নেস্টিং-এর ঝুঁকি এড়াতে, যেটা আগের ভার্সনে সমস্যা তৈরি করেছিল)
// ══════════════════════════════════════════════════════════════
function mainHTML() {
  var css = ''
    + 'body{margin:0;background:#07070e;color:#dde0f0;font-family:system-ui,sans-serif;padding-bottom:70px}'
    + '.top{position:sticky;top:0;background:#0d0d18;padding:12px 14px;display:flex;align-items:center;gap:10px;border-bottom:1px solid #232338;z-index:100}'
    + '.top h1{font-size:16px;margin:0;flex:1}'
    + '.dot{width:8px;height:8px;border-radius:50%;background:#f05252}'
    + '.dot.on{background:#3ecf8e}'
    + '.dot.starting{background:#f0b429}'
    + '.pill{display:flex;align-items:center;gap:6px;background:#1a1a2e;padding:5px 10px;border-radius:20px;font-size:11px}'
    + '.btn{padding:6px 10px;border-radius:8px;border:1px solid #232338;background:transparent;color:#f05252;font-size:11px}'
    + '.tabs{position:fixed;bottom:0;left:0;right:0;background:#0d0d18;border-top:1px solid #232338;display:flex}'
    + '.tab{flex:1;text-align:center;padding:10px 0;color:#5a5a80;font-size:10px;background:none;border:none}'
    + '.tab.active{color:#6c63ff}'
    + '.page{display:none;padding:14px}'
    + '.page.active{display:block}'
    + '.card{background:#141424;border:1px solid #232338;border-radius:14px;padding:16px;margin-bottom:12px}'
    + '.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}'
    + '.stat{background:#1a1a2e;border-radius:10px;padding:12px;text-align:center;margin-bottom:8px}'
    + '.stat b{display:block;font-size:20px}'
    + '.stat span{font-size:11px;color:#5a5a80}'
    + 'button.act{padding:12px;border:none;border-radius:10px;color:#fff;font-weight:700;font-size:13px}'
    + '.b-go{background:#3ecf8e}.b-stop{background:#f05252}.b-mid{background:#38bdf8}'
    + 'textarea,input.f{width:100%;box-sizing:border-box;background:#0d0d18;border:1px solid #232338;color:#dde0f0;border-radius:10px;padding:10px;font-size:13px}'
    + '.logline{padding:6px 8px;margin-bottom:4px;border-radius:6px;background:#1a1a2e;border-left:3px solid #5a5a80;font-family:monospace;font-size:11px}'
    + '.logline.error{border-left-color:#f05252;color:#ff9b9b}'
    + '.logline.warn{border-left-color:#f0b429;color:#ffd980}'
    + '.logline.success{border-left-color:#3ecf8e;color:#7fe8ba}'
    + '.frow{display:flex;align-items:center;gap:10px;padding:10px;border-radius:10px;background:#141424;margin-bottom:6px}'
    + '.frow span.n{flex:1;font-size:13px}'
    + '#diagBox{display:none}';

  var body = ''
    + '<div class="top">'
    + '<h1>🤖 Bot Panel</h1>'
    + '<div class="pill"><div class="dot" id="dot"></div><span id="statusTxt">লোড...</span></div>'
    + '<button class="btn" onclick="location.href=\'/logout\'">বের</button>'
    + '</div>'

    + '<div id="pg-home" class="page active">'
    + '<div class="card"><h3>বট কন্ট্রোল</h3>'
    + '<div class="grid2" style="margin-bottom:10px">'
    + '<button class="act b-go" onclick="botAct(\'start\')">▶ চালু</button>'
    + '<button class="act b-stop" onclick="botAct(\'stop\')">⏹ বন্ধ</button>'
    + '</div>'
    + '<button class="act b-mid" style="width:100%;margin-bottom:10px" onclick="botAct(\'restart\')">🔄 রিস্টার্ট</button>'
    + '<button class="act" style="width:100%;background:#1a1a2e;color:#dde0f0" onclick="botAct(\'install\')">📦 npm install</button>'
    + '</div>'
    + '<div class="card"><h3>স্ট্যাটস</h3><div class="grid2">'
    + '<div class="stat"><b id="sMem">--</b><span>Memory MB</span></div>'
    + '<div class="stat"><b id="sUp">--</b><span>Server Uptime</span></div>'
    + '<div class="stat"><b id="sFiles">--</b><span>বট ফাইল</span></div>'
    + '<div class="stat"><b id="sStarts">--</b><span>মোট Start</span></div>'
    + '<div class="stat"><b id="sCrash">--</b><span>মোট Crash</span></div>'
    + '<div class="stat"><b id="sNode">--</b><span>Node.js</span></div>'
    + '</div></div>'
    + '<div class="card"><h3>🍪 Facebook Cookie</h3>'
    + '<textarea id="cookieBox" rows="4" placeholder="cookie বা appstate.json পেস্ট করো"></textarea>'
    + '<button class="act b-go" style="width:100%;margin-top:10px" onclick="saveCookie()">✅ সেভ ও বট চালু করো</button>'
    + '</div>'
    + '</div>'

    + '<div id="pg-files" class="page">'
    + '<div class="card">'
    + '<div id="pathBar" style="font-size:12px;color:#5a5a80;margin-bottom:10px">📁 root</div>'
    + '<div id="flist"></div>'
    + '</div></div>'

    + '<div id="pg-logs" class="page">'
    + '<div class="card"><div id="lbox" style="max-height:70vh;overflow-y:auto"></div></div>'
    + '</div>'

    + '<div id="pg-settings" class="page">'
    + '<div class="card"><h3>পাসওয়ার্ড বদলাও</h3>'
    + '<input class="f" type="password" id="newPw" placeholder="নতুন পাসওয়ার্ড" style="margin-bottom:10px">'
    + '<button class="act b-go" style="width:100%" onclick="changePw()">সেভ করো</button>'
    + '</div></div>'

    + '<div class="tabs">'
    + '<button class="tab active" onclick="goTab(\'home\',this)">🏠<br>হোম</button>'
    + '<button class="tab" onclick="goTab(\'files\',this)">📁<br>ফাইল</button>'
    + '<button class="tab" onclick="goTab(\'logs\',this)">📋<br>লগ</button>'
    + '<button class="tab" onclick="goTab(\'settings\',this)">⚙️<br>সেটিংস</button>'
    + '</div>';

  return '<!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8">'
    + '<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">'
    + '<title>Bot Panel</title><style>' + css + '</style></head><body>'
    + body
    + '<script>' + clientJS() + '</script>'
    + '</body></html>';
}
function clientJS() {
  return ''
  + 'window.addEventListener("error",function(e){showErr((e.message||"unknown")+" @ "+(e.lineno||""));});'
  + 'window.addEventListener("unhandledrejection",function(e){showErr("Promise: "+(e.reason&&e.reason.message?e.reason.message:String(e.reason)));});'
  + 'function showErr(msg){'
  + 'try{'
  + 'var d=document.getElementById("diagBox");'
  + 'if(!d){d=document.createElement("div");d.id="diagBox";'
  + 'd.style.cssText="display:block;position:fixed;bottom:60px;left:8px;right:8px;background:#2a0a0a;border:2px solid #f05252;border-radius:10px;padding:10px;z-index:9999;font-family:monospace;font-size:11px;color:#ffaaaa;max-height:160px;overflow-y:auto";'
  + 'document.body.appendChild(d);}'
  + 'd.innerHTML="ERROR: "+String(msg).replace(/</g,"&lt;")+"<hr>"+d.innerHTML;'
  + '}catch(e){}'
  + '}'
  + 'var curDir="";'
  + 'function goTab(id,btn){'
  + 'var tabs=document.querySelectorAll(".tab");for(var i=0;i<tabs.length;i++)tabs[i].className="tab";'
  + 'btn.className="tab active";'
  + 'var pages=document.querySelectorAll(".page");for(var j=0;j<pages.length;j++)pages[j].className="page";'
  + 'document.getElementById("pg-"+id).className="page active";'
  + 'if(id==="files")loadFiles(curDir);'
  + 'if(id==="logs")loadLogs();'
  + '}'
  + 'function esc(t){return String(t).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}'
  + 'function fmtT(s){s=s||0;var h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h>0?(h+"h "+m+"m"):(m+"m");}'
  + 'function updateStatus(running,starting){'
  + 'var dot=document.getElementById("dot");'
  + 'dot.className="dot"+(running?" on":(starting?" starting":""));'
  + 'var t=document.getElementById("statusTxt");'
  + 't.textContent=running?"চলছে":(starting?"চালু হচ্ছে...":"বন্ধ");'
  + '}'
  + 'function refresh(){'
  + 'var ctrl=new AbortController();'
  + 'var toId=setTimeout(function(){ctrl.abort();},8000);'
  + 'fetch("/api/stats",{signal:ctrl.signal}).then(function(r){'
  + 'clearTimeout(toId);'
  + 'if(r.status===401){location.href="/login";return null;}'
  + 'return r.json();'
  + '}).then(function(d){'
  + 'if(!d)return;'
  + 'document.getElementById("sMem").textContent=d.memMB||"--";'
  + 'document.getElementById("sUp").textContent=fmtT(d.serverUptime);'
  + 'document.getElementById("sFiles").textContent=d.botFiles||0;'
  + 'document.getElementById("sStarts").textContent=d.starts||0;'
  + 'document.getElementById("sCrash").textContent=d.crashes||0;'
  + 'document.getElementById("sNode").textContent=d.node||"--";'
  + 'updateStatus(d.ready,d.starting);'
  + '}).catch(function(e){showErr("refresh failed: "+(e&&e.message?e.message:e));});'
  + '}'
  + 'function botAct(a){'
  + 'fetch("/api/bot/"+a,{method:"POST"}).then(function(r){return r.json();}).then(function(d){'
  + 'toast(d.msg||(d.ok?"হয়েছে":"ব্যর্থ"));'
  + '}).catch(function(e){showErr("botAct failed: "+e.message);});'
  + '}'
  + 'function saveCookie(){'
  + 'var v=document.getElementById("cookieBox").value;'
  + 'fetch("/api/cookie",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({cookie:v})})'
  + '.then(function(r){return r.json();}).then(function(d){toast(d.msg||"হয়েছে");}).catch(function(e){showErr(e.message);});'
  + '}'
  + 'function changePw(){toast("এই ফিচার শীঘ্রই আসবে");}'
  + 'function toast(msg){'
  + 'var t=document.createElement("div");'
  + 't.style.cssText="position:fixed;top:60px;left:8px;right:8px;background:#1a1a2e;border:1px solid #232338;padding:10px;border-radius:10px;z-index:500;text-align:center;font-size:13px";'
  + 't.textContent=msg;document.body.appendChild(t);'
  + 'setTimeout(function(){t.remove();},3000);'
  + '}'
  + 'function loadFiles(dir){'
  + 'curDir=dir||"";'
  + 'fetch("/api/files?path="+encodeURIComponent(curDir)).then(function(r){return r.json();}).then(function(d){'
  + 'var list=document.getElementById("flist");list.innerHTML="";'
  + 'document.getElementById("pathBar").textContent="📁 "+(curDir||"root");'
  + 'if(curDir){'
  + 'var up=document.createElement("div");up.className="frow";'
  + 'up.innerHTML="<span class=\\"n\\">⬆️ উপরে যাও</span>";'
  + 'up.onclick=function(){loadFiles(curDir.split("/").slice(0,-1).join("/"));};'
  + 'list.appendChild(up);'
  + '}'
  + '(d.items||[]).forEach(function(item){'
  + 'var row=document.createElement("div");row.className="frow";'
  + 'var icon=item.isDir?"📁":"📄";'
  + 'row.innerHTML="<span>"+icon+"</span><span class=\\"n\\">"+esc(item.name)+"</span>";'
  + 'row.onclick=function(){'
  + 'if(item.isDir)loadFiles((curDir?curDir+"/":"")+item.name);'
  + 'else window.open("/api/file/download?path="+encodeURIComponent((curDir?curDir+"/":"")+item.name));'
  + '};'
  + 'list.appendChild(row);'
  + '});'
  + '}).catch(function(e){showErr("loadFiles failed: "+e.message);});'
  + '}'
  + 'var logFilter="all";'
  + 'function appendLog(e){'
  + 'var box=document.getElementById("lbox");'
  + 'var d=document.createElement("div");'
  + 'd.className="logline "+(e.type||"info");'
  + 'd.textContent="["+e.time+"] "+e.text;'
  + 'box.appendChild(d);'
  + 'box.scrollTop=box.scrollHeight;'
  + '}'
  + 'function loadLogs(){'
  + 'fetch("/api/bot/logs").then(function(r){return r.json();}).then(function(d){'
  + 'var box=document.getElementById("lbox");box.innerHTML="";'
  + '(d.logs||[]).forEach(appendLog);'
  + '}).catch(function(e){showErr("loadLogs failed: "+e.message);});'
  + '}'
  + 'var ws;'
  + 'function connectWS(){'
  + 'try{'
  + 'var proto=location.protocol==="https:"?"wss:":"ws:";'
  + 'ws=new WebSocket(proto+"//"+location.host);'
  + 'ws.onmessage=function(ev){'
  + 'try{'
  + 'var m=JSON.parse(ev.data);'
  + 'if(m.type==="log")appendLog(m.data);'
  + 'if(m.type==="status")updateStatus(m.ready,m.starting);'
  + 'if(m.type==="clearLogs"){document.getElementById("lbox").innerHTML="";}'
  + '}catch(e){}'
  + '};'
  + 'ws.onclose=function(){setTimeout(connectWS,3000);};'
  + 'ws.onerror=function(){};'
  + '}catch(e){showErr("WS connect failed: "+e.message);}'
  + '}'
  + 'connectWS();'
  + 'refresh();'
  + 'setInterval(refresh,10000);';
}

// ══════════════════════════════════════════════════════════════
// সার্ভার বুটস্ট্র্যাপ
// ══════════════════════════════════════════════════════════════
const server = http.createServer(app);
server.on("upgrade", function (req, socket, head) {
  wss.handleUpgrade(req, socket, head, function (ws) {
    wss.emit("connection", ws, req);
  });
});

server.listen(PORT, function () {
  console.log("✅ Panel running on port " + PORT);
  connectMongo().then(function () {
    console.log("বুট সম্পূর্ণ");
  });
});
