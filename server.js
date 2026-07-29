"use strict";
const express   = require("express");
const session   = require("express-session");
const multer    = require("multer");
const http      = require("http");
const WebSocket = require("ws");
const fs        = require("fs");
const path      = require("path");
const https     = require("https");
const http2     = require("http");
const { spawn, execSync } = require("child_process");
const archiver  = require("archiver");
const unzipper  = require("unzipper");

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ── CRASH GUARD ── একটা রিকোয়েস্টে সমস্যা হলে যেন পুরো সার্ভার ক্র্যাশ/রিস্টার্ট না হয়ে যায়
// (রিস্টার্ট হলে ephemeral ডিস্কের সব ফাইল + লগ হারিয়ে যায়)
process.on("uncaughtException", (err) => {
  console.log("⚠️ uncaughtException (সার্ভার বাঁচানো হলো):", err && err.message);
});
process.on("unhandledRejection", (err) => {
  console.log("⚠️ unhandledRejection (সার্ভার বাঁচানো হলো):", err && (err.message || err));
});

// ── CONFIG ──
const CFG   = path.join(__dirname, "panel.config.json");
const BDIR  = path.join(__dirname, "bot");
const LFILE = path.join(__dirname, "panel.log");
const SFILE = path.join(__dirname, "stats.json");
const LTFILE = path.join(__dirname, "lifetime.json"); // panel/bot RAM + mongo ব্যবহারের সর্বকালীন উচ্চতম (lifetime peak) — MongoDB-তেও ব্যাকআপ থাকে, তাই panel restart হলেও হারায় না
const PORT  = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGODB_URI || "mongodb+srv://belal:belal123456@cluster0.i1wofni.mongodb.net/botpanel?appName=Cluster0";

function loadJ(f,def={}){try{return JSON.parse(fs.readFileSync(f,"utf8"));}catch{return def;}}
function saveJ(f,d){try{fs.writeFileSync(f,JSON.stringify(d,null,2));}catch{}}

// panel.config.json শুধু লোকাল ডিস্কে (ephemeral) থাকলে container restart এ হারিয়ে
// যায় (Auto Restart টগল, পাসওয়ার্ড ইত্যাদি সব রিসেট হয়ে যায়) — তাই MongoDB তেও
// একটা কপি রাখা হয়, boot এর সময় সেখান থেকে ফিরিয়ে আনা হয়
function saveCfg(){
  saveJ(CFG,cfg);
  if(db_connected && FileModel){
    FileModel.findOneAndUpdate(
      {path:"__panel_config__"},
      {path:"__panel_config__", content:Buffer.from(JSON.stringify(cfg)), isDir:false, mtime:new Date(), size:JSON.stringify(cfg).length},
      {upsert:true}
    ).catch(e=>console.log("⚠️ saveCfg mongo error:",e.message));
  }
}
async function loadCfgFromMongo(){
  try{
    if(!db_connected||!FileModel) return;
    const doc=await FileModel.findOne({path:"__panel_config__"});
    if(doc && doc.content){
      const saved=JSON.parse(doc.content.toString());
      Object.assign(cfg, saved);
      if(cfg.autoRestart!==undefined) autoRestart=!!cfg.autoRestart;
      saveJ(CFG,cfg);
      console.log("✅ প্যানেল সেটিংস MongoDB থেকে ফিরিয়ে আনা হয়েছে (Auto Restart:", autoRestart, ")");
    }
  }catch(e){console.log("⚠️ loadCfgFromMongo error:",e.message);}
}

let cfg   = loadJ(CFG);
let stats = loadJ(SFILE,{starts:0,crashes:0,totalUptime:0,history:[],loginAttempts:{}});
let lifetime = loadJ(LTFILE,{peakPanelMB:0,peakBotMB:0,peakMongoMB:0,firstSeen:new Date().toISOString()});
const PASS = process.env.PANEL_PASSWORD || cfg.password || "admin123";
if(!fs.existsSync(BDIR)) fs.mkdirSync(BDIR,{recursive:true});

// ── MONGODB ──
let mongoose, FileModel, db_connected = false;

async function connectMongo(){
  try {
    mongoose = require("mongoose");
    await mongoose.connect(MONGO_URI, {serverSelectionTimeoutMS:5000});
    db_connected = true;
    console.log("✅ MongoDB connected");

    const fileSchema = new mongoose.Schema({
      path:    {type:String, required:true, unique:true},
      content: {type:Buffer, default:Buffer.alloc(0)},
      isDir:   {type:Boolean, default:false},
      mtime:   {type:Date, default:Date.now},
      size:    {type:Number, default:0}
    });
    FileModel = mongoose.models.BotFile || mongoose.model("BotFile", fileSchema);

    // প্যানেল সেটিংস (Auto Restart টগল ইত্যাদি) MongoDB থেকে ফিরিয়ে আনা
    await loadCfgFromMongo();

    // restore files from MongoDB on startup
    await restorePanelPersistent();
    await restoreFromMongo();
    await importRepoZipIfPresent();

    // ── আগে বট চালু ছিল কিনা চেক করে, থাকলে নিজে থেকেই আবার চালু করা ──
    // (পুরো container restart হয়ে গেলে এটাই একমাত্র উপায় যেটা বট
    // নিজে থেকে আবার চালু করতে পারে, "Auto Restart" টগল শুধু in-process
    // ক্র্যাশের জন্য কাজ করে, পুরো restart-এর জন্য না)
    setTimeout(async()=>{
      try{
        const shouldRun=await getShouldRun();
        const asPath=path.join(BDIR,"appstate.json");
        let hasValidCookie=false;
        if(fs.existsSync(asPath)){
          try{const arr=JSON.parse(fs.readFileSync(asPath,"utf8"));hasValidCookie=Array.isArray(arr)&&arr.length>0;}catch{}
        }
        if(shouldRun && hasValidCookie){
          log("🔄 আগে বট চালু ছিল — নিজে থেকেই আবার চালু করা হচ্ছে...","warn");
          startBot("auto-boot");
        }
      }catch(e){console.log("⚠️ auto-boot check error:",e.message);}
    },8000); // ফাইল restore + npm install এর জন্য একটু সময় দেওয়া হলো
  } catch(e) {
    console.log("⚠️ MongoDB connect failed:", e.message);
    db_connected = false;
    setTimeout(connectMongo, 30000);
  }
}

// GitHub রিপোতে সরাসরি রাখা ZIP ফাইল (server.js এর পাশে) থাকলে সেটা অটো extract + MongoDB সেভ করে —
// ফোনের ধীর নেটওয়ার্কে প্যানেল দিয়ে আপলোডের ঝামেলা এড়াতে। GitHub এর নিজস্ব আপলোড অনেক বেশি নির্ভরযোগ্য।
// একবার import হয়ে গেলে MongoDB তে marker রাখা হয়, তাই একই zip বারবার import হবে না।
async function importRepoZipIfPresent(){
  try{
    const files = fs.readdirSync(__dirname).filter(f=>f.toLowerCase().endsWith(".zip"));
    if(files.length===0) return;
    const zipName = files[0];
    const zipPath = path.join(__dirname, zipName);
    const stat = fs.statSync(zipPath);
    const signature = zipName+":"+stat.size;

    let markerDoc=null;
    if(db_connected && FileModel){
      markerDoc = await FileModel.findOne({path:"__zip_import_marker__"});
    }
    if(markerDoc && markerDoc.content && markerDoc.content.toString()===signature){
      console.log("ℹ️ repo zip ("+zipName+") আগেই import করা হয়েছে, স্কিপ করা হলো");
      return;
    }

    log("📦 রিপোতে নতুন ZIP পাওয়া গেছে ("+zipName+") — অটো-ইম্পোর্ট শুরু হচ্ছে...","info");
    const result = await processUploadedFile(zipPath, zipName, "");
    log("📦 অটো-ইম্পোর্ট ফলাফল: "+(result.body && result.body.msg),"success");

    if(db_connected && FileModel){
      await FileModel.findOneAndUpdate(
        {path:"__zip_import_marker__"},
        {path:"__zip_import_marker__", content:Buffer.from(signature), isDir:false, mtime:new Date(), size:signature.length},
        {upsert:true}
      );
    }
  }catch(e){
    console.log("⚠️ auto-import zip error:", e.message);
  }
}

// Restore all files from MongoDB to disk
async function restoreFromMongo(){
  if(!db_connected || !FileModel) return;
  try {
    const files = await FileModel.find({});
    let restored = 0;
    for(const f of files){
      const full = path.join(BDIR, f.path);
      if(f.isDir){
        if(!fs.existsSync(full)) fs.mkdirSync(full, {recursive:true});
      } else {
        fs.mkdirSync(path.dirname(full), {recursive:true});
        if(!fs.existsSync(full)){
          fs.writeFileSync(full, f.content);
          restored++;
        }
      }
    }
    if(restored > 0) console.log(`✅ MongoDB থেকে ${restored}টা ফাইল restore হয়েছে`);
  } catch(e){ console.log("⚠️ restore error:", e.message); }
}

// Save a file to MongoDB
async function saveToMongo(relPath, content, isDir=false){
  if(!db_connected || !FileModel) return;
  try {
    const size = Buffer.isBuffer(content) ? content.length : Buffer.byteLength(content||"");
    await FileModel.findOneAndUpdate(
      {path: relPath},
      {path:relPath, content: Buffer.isBuffer(content)?content:Buffer.from(content||""), isDir, mtime:new Date(), size},
      {upsert:true, new:true}
    );
  } catch(e){ console.log("⚠️ mongo save error:", e.message); }
}

// Delete from MongoDB
async function deleteFromMongo(relPath){
  if(!db_connected || !FileModel) return;
  try {
    // delete the file and any files under this path (if folder)
    await FileModel.deleteMany({path: {$regex: `^${relPath.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}(/|$)`}});
    await FileModel.deleteOne({path: relPath});
  } catch(e){ console.log("⚠️ mongo delete error:", e.message); }
}

// ── প্যানেলের নিজস্ব stats.json ও lifetime.json MongoDB-তে ব্যাকআপ/রিস্টোর ──
// (Render-এর ডিস্ক ephemeral, তাই এগুলো শুধু ডিস্কে রাখলে প্যানেল restart হলেই "লাইফটাইম" হিসাব শূন্য হয়ে যেত)
async function savePanelStatsToMongo(){ await saveToMongo("__panel_stats__", JSON.stringify(stats), false); }
async function saveLifetimeToMongo(){ await saveToMongo("__panel_lifetime__", JSON.stringify(lifetime), false); }
async function restorePanelPersistent(){
  if(!db_connected || !FileModel) return;
  try{
    const s = await FileModel.findOne({path:"__panel_stats__"});
    if(s && s.content){ try{ stats = {...stats, ...JSON.parse(s.content.toString())}; saveJ(SFILE,stats); }catch{} }
    const l = await FileModel.findOne({path:"__panel_lifetime__"});
    if(l && l.content){ try{ lifetime = {...lifetime, ...JSON.parse(l.content.toString())}; saveJ(LTFILE,lifetime); }catch{} }
    console.log("✅ প্যানেলের লাইফটাইম স্ট্যাটস MongoDB থেকে restore হয়েছে");
  }catch(e){ console.log("⚠️ panel stats restore error:", e.message); }
}
function bumpLifetimePeak(key, valueMB){
  if(valueMB==null) return;
  if(valueMB > (lifetime[key]||0)){
    lifetime[key] = valueMB;
    saveJ(LTFILE, lifetime);
    saveLifetimeToMongo(); // fire-and-forget — নতুন রেকর্ড হলেই সাথে সাথে ব্যাকআপ
  }
}

// Sync a directory to MongoDB recursively
// stats: {ok, fail, skipped, failedFiles} — passed by reference across recursive calls
async function syncDirToMongo(dirPath, relBase, stats){
  if(!db_connected || !FileModel) return stats||{ok:0,fail:0,skipped:0,failedFiles:[]};
  if(!stats) stats={ok:0,fail:0,skipped:0,failedFiles:[]};
  let items=[];
  try{ items = fs.readdirSync(dirPath); }
  catch(e){ console.log("⚠️ readdir error:", e.message); return stats; }

  for(const name of items){
    const full = path.join(dirPath, name);
    const rel  = relBase ? relBase+"/"+name : name;
    let stat;
    try{ stat = fs.statSync(full); }
    catch(e){ stats.fail++; stats.failedFiles.push(rel); continue; }

    if(stat.isDirectory()){
      try{ await saveToMongo(rel, Buffer.alloc(0), true); stats.ok++; }
      catch(e){ stats.fail++; stats.failedFiles.push(rel); }
      // recurse regardless — one bad folder shouldn't skip its siblings
      await syncDirToMongo(full, rel, stats);
    } else {
      if(stat.size < 10*1024*1024){ // 10MB limit
        try{
          const content = fs.readFileSync(full);
          await saveToMongo(rel, content, false);
          stats.ok++;
        }catch(e){
          stats.fail++; stats.failedFiles.push(rel);
          console.log("⚠️ sync error on "+rel+":", e.message);
        }
      } else {
        stats.skipped++; stats.failedFiles.push(rel+" (10MB+, skipped)");
      }
    }
  }
  return stats;
}

// ── MIDDLEWARE ──
app.use(express.json({limit:"500mb"}));
app.use(express.urlencoded({extended:true,limit:"500mb"}));
app.use(session({secret:process.env.SESSION_SECRET||"belal_bot_panel_2024",resave:false,saveUninitialized:false,cookie:{maxAge:7*24*60*60*1000}}));
const upload = multer({storage:multer.diskStorage({destination:(r,f,cb)=>cb(null,"/tmp/"),filename:(r,f,cb)=>cb(null,Date.now()+"_"+f.originalname)}),limits:{fileSize:500*1024*1024}});

const auth = (req,res,next) => req.session.ok ? next() : res.redirect("/login");
const safe = (base,rel) => { const f=path.resolve(base,rel||""); if(!f.startsWith(path.resolve(base))) throw new Error("Access denied"); return f; };

// ── BOT ──
let botProc=null, botLogs=[], botStart=null, autoRestart=cfg.autoRestart||false, rsTimer=null, _consecutiveCrashes=0;

function bc(d){wss.clients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(JSON.stringify(d));});}

function log(text,type="info"){
  const e={time:new Date().toLocaleTimeString("bn-BD"),text,type,ts:Date.now()};
  botLogs.push(e); if(botLogs.length>2000) botLogs.shift();
  bc({type:"log",data:e});
  try{fs.appendFileSync(LFILE,`[${e.time}][${type}] ${text}\n`);}catch{}
}

function fmtS(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h>0?h+"h "+m+"m":m>0?m+"m "+sc+"s":sc+"s";}

// ── বট "চালু থাকার কথা ছিল কিনা" MongoDB তে মনে রাখা — যাতে পুরো container
// restart হয়ে গেলেও বট নিজে থেকেই আবার চালু হতে পারে, শুধু ক্র্যাশ না
async function setShouldRun(val){
  try{
    if(!db_connected||!FileModel) return;
    await FileModel.findOneAndUpdate(
      {path:"__bot_should_run__"},
      {path:"__bot_should_run__", content:Buffer.from(val?"1":"0"), isDir:false, mtime:new Date(), size:1},
      {upsert:true}
    );
  }catch(e){console.log("⚠️ setShouldRun error:",e.message);}
}
async function getShouldRun(){
  try{
    if(!db_connected||!FileModel) return false;
    const doc=await FileModel.findOne({path:"__bot_should_run__"});
    return !!(doc && doc.content && doc.content.toString()==="1");
  }catch{return false;}
}

function startBot(by="manual"){
  if(botProc) return {ok:false,msg:"বট ইতিমধ্যে চলছে"};
  const idx=["index.js","app.js","main.js","bot.js","start.js"].find(f=>fs.existsSync(path.join(BDIR,f)));
  if(!idx) return {ok:false,msg:"index.js পাওয়া যায়নি — বট আপলোড করুন"};
  const pkgFile=path.join(BDIR,"package.json");
  const nmDir=path.join(BDIR,"node_modules");
  if(!fs.existsSync(nmDir)){
    try{log("📦 npm install চলছে...","warn");execSync("npm install",{cwd:BDIR,timeout:180000});log("✅ npm install সম্পন্ন","success");}
    catch(e){log("⚠️ npm install সমস্যা: "+e.message,"error");}
  }
  botProc=spawn("node",[idx],{cwd:BDIR,env:{...process.env,FORCE_COLOR:"1"}});
  botStart=Date.now(); stats.starts++; saveJ(SFILE,stats); savePanelStatsToMongo();
  setShouldRun(true);
  log(`🟢 বট চালু (${by}) — ${idx}`,"success"); bc({type:"status",running:true});
  const NOISY=[/Warning: Accessing non-existent property/i,/circular dependency/i,/--trace-warnings/i,/\[DEP\d+\]/i,/is deprecated\. Please use/i];
  const isNoisy=s=>NOISY.some(rx=>rx.test(s));
  // eslint-disable-next-line no-control-regex
  const stripAnsi=s=>s.replace(/\x1B\[[0-9;]*[a-zA-Z]/g,"");
  botProc.stdout.on("data",d=>{const s=stripAnsi(d.toString()).trim();if(s&&!isNoisy(s))log(s,"info");});
  botProc.stderr.on("data",d=>{const s=stripAnsi(d.toString()).trim();if(s&&!isNoisy(s))log(s,"error");});
  botProc.on("exit",(code,sig)=>{
    const up=botStart?Math.floor((Date.now()-botStart)/1000):0;
    stats.totalUptime+=up; stats.history.push({date:new Date().toISOString(),uptime:up,code:code||sig});
    if(stats.history.length>100) stats.history.shift();
    if(code!==0&&code!==null){
      stats.crashes++;
      try{
        fs.writeFileSync(path.join(BDIR,".crash_flag.json"), JSON.stringify({
          time: new Date().toISOString(), code: code||sig, uptimeSec: up
        }));
      }catch{}
    }
    saveJ(SFILE,stats); savePanelStatsToMongo();
    log(`🔴 বট বন্ধ (code:${code||sig}, uptime:${fmtS(up)})`,"error");
    botProc=null; botStart=null; bc({type:"status",running:false});
    if(autoRestart&&code!==0&&code!==null){
      // ── উঠতি-ধাপে অপেক্ষা (exponential backoff) ──
      // দ্রুত/বারবার ক্র্যাশ হলে (বিশেষত ফেসবুকের 429 rate-limit) প্রতিবার
      // অপেক্ষার সময় বাড়বে, যাতে ফেসবুককে বারবার বিরক্ত করে ব্লক আরও
      // দীর্ঘায়িত না করি। বট মোটামুটি স্থিতিশীলভাবে (২ মিনিট+) চললে
      // কাউন্টার রিসেট হয়ে যাবে।
      if(up>=120) _consecutiveCrashes=0; else _consecutiveCrashes++;
      const waitSec=Math.min(10*Math.pow(2,_consecutiveCrashes),300); // ১০সে থেকে সর্বোচ্চ ৫মিনিট
      log(`🔄 Auto-restart ${waitSec} সেকেন্ড পরে... (পরপর ${_consecutiveCrashes} বার ক্র্যাশ)`,"warn");
      rsTimer=setTimeout(()=>startBot("auto-restart"),waitSec*1000);
    }
  });
  return {ok:true,msg:"বট চালু হয়েছে"};
}

function stopBot(){
  if(rsTimer){clearTimeout(rsTimer);rsTimer=null;}
  if(!botProc) return {ok:false,msg:"বট চলছে না"};
  try{botProc.kill("SIGTERM");setTimeout(()=>{try{if(botProc)botProc.kill("SIGKILL");}catch{}},5000);}catch{}
  botProc=null; botStart=null;
  setShouldRun(false);
  log("🔴 বট বন্ধ করা হয়েছে","warn"); bc({type:"status",running:false});
  return {ok:true,msg:"বট বন্ধ হয়েছে"};
}

// ── SELF PING ──
function selfPing(){
  try{
    const url=process.env.RENDER_EXTERNAL_URL||cfg.siteUrl||"";
    if(!url) return;
    const mod=url.startsWith("https")?https:http2;
    mod.get(url+"/ping",()=>{}).on("error",()=>{});
  }catch{}
}
setInterval(selfPing,4*60*1000);
setTimeout(selfPing,30*1000);

// ── SCHEDULE ──
setInterval(()=>{
  if(!cfg.scheduleRestart||!cfg.scheduleTime)return;
  const[h,m]=cfg.scheduleTime.split(":").map(Number),now=new Date();
  if(now.getHours()===h&&now.getMinutes()===m&&now.getSeconds()<10&&botProc){
    stopBot();setTimeout(()=>startBot("schedule"),3000);log("⏰ Scheduled restart","warn");
  }
},10000);

// ── ROUTES: AUTH ──
app.get("/ping",(req,res)=>res.json({ok:true,running:!!botProc,mongo:db_connected,time:new Date().toISOString()}));
app.get("/health",(req,res)=>res.json({ok:true}));
app.get("/login",(req,res)=>{if(req.session.ok)return res.redirect("/");res.send(loginHTML());});
// ── Login brute-force সুরক্ষা ── বারবার ভুল পাসওয়ার্ড দিলে সাময়িক ব্লক
const loginAttempts = new Map(); // ip -> {count, lockUntil}
app.post("/login",(req,res)=>{
  const ip = req.ip || req.connection.remoteAddress || "unknown";
  const rec = loginAttempts.get(ip) || {count:0, lockUntil:0};
  if(Date.now() < rec.lockUntil){
    const waitSec = Math.ceil((rec.lockUntil-Date.now())/1000);
    return res.json({ok:false,msg:`⛔ অনেকবার ভুল পাসওয়ার্ড দেওয়া হয়েছে — ${waitSec} সেকেন্ড পর আবার চেষ্টা করো`});
  }
  if(req.body.password===PASS){
    req.session.ok=true;
    loginAttempts.delete(ip);
    res.json({ok:true});
  } else {
    rec.count++;
    if(rec.count>=5){ rec.lockUntil = Date.now()+5*60*1000; rec.count=0; } // ৫ বার ভুল হলে ৫ মিনিট লক
    loginAttempts.set(ip, rec);
    res.json({ok:false,msg:"❌ ভুল পাসওয়ার্ড"});
  }
});
app.get("/logout",(req,res)=>{req.session.destroy();res.redirect("/login");});
app.get("/"   ,auth,(req,res)=>res.send(mainHTML()));

// ── BOT API ──
app.post("/api/bot/start",   auth,(req,res)=>res.json(startBot()));
app.post("/api/bot/stop",    auth,(req,res)=>res.json(stopBot()));
app.post("/api/bot/restart", auth,(req,res)=>{stopBot();setTimeout(()=>res.json(startBot("restart")),2000);});
app.get("/api/bot/status",   auth,(req,res)=>res.json({running:!!botProc,uptime:botStart?Math.floor((Date.now()-botStart)/1000):0}));
app.get("/api/bot/logs",     auth,(req,res)=>res.json({logs:botLogs}));
app.post("/api/bot/clearlogs",auth,(req,res)=>{botLogs=[];bc({type:"clearLogs"});res.json({ok:true});});
app.post("/api/bot/install", auth,(req,res)=>{
  if(!fs.existsSync(path.join(BDIR,"package.json"))) return res.json({ok:false,msg:"package.json নেই"});
  try{log("📦 npm install চলছে...","warn");execSync("npm install",{cwd:BDIR,timeout:180000});log("✅ সম্পন্ন","success");res.json({ok:true,msg:"npm install সম্পন্ন"});}
  catch(e){log("❌ npm install ব্যর্থ: "+e.message,"error");res.json({ok:false,msg:e.message});}
});
app.post("/api/bot/autorestart",auth,(req,res)=>{autoRestart=!!req.body.enabled;cfg.autoRestart=autoRestart;saveCfg();res.json({ok:true,enabled:autoRestart});});
app.get("/api/bot/downloadlog",auth,(req,res)=>{if(fs.existsSync(LFILE))res.download(LFILE,"bot.log");else res.status(404).send("No log");});
app.post("/api/bot/clearlogfile",auth,(req,res)=>{try{fs.writeFileSync(LFILE,"");res.json({ok:true});}catch(e){res.json({ok:false,msg:e.message});}});

app.get("/api/stats",auth,(req,res)=>{
  function countF(d){let c=0;try{fs.readdirSync(d).forEach(f=>{const s=fs.statSync(path.join(d,f));c+=s.isDirectory()?countF(path.join(d,f)):1;});}catch{}return c;}
  res.json({...stats,running:!!botProc,currentUptime:botStart?Math.floor((Date.now()-botStart)/1000):0,
    autoRestart,memMB:Math.round(process.memoryUsage().rss/1024/1024),
    serverUptime:Math.floor(process.uptime()),node:process.version,
    botFiles:countF(BDIR),mongoConnected:db_connected});
});

// ── লাইভ সিস্টেম মনিটর — RAM (panel+bot) + MongoDB storage + Render bandwidth (optional API key) ──
function getBotMemMB(){
  if(!botProc || !botProc.pid) return null;
  try{
    const status = fs.readFileSync("/proc/"+botProc.pid+"/status","utf8");
    const m = status.match(/VmRSS:\s+(\d+)\s+kB/);
    return m ? Math.round(parseInt(m[1],10)/1024) : null;
  }catch{ return null; } // /proc না থাকলে (নন-লিনাক্স) বা প্রসেস শেষ হয়ে গেলে
}
let _renderCache = {data:null, at:0};
async function getRenderBandwidth(){
  const key = process.env.RENDER_API_KEY, svc = process.env.RENDER_SERVICE_ID;
  if(!key || !svc) return {configured:false};
  if(Date.now()-_renderCache.at < 5*60*1000 && _renderCache.data) return _renderCache.data; // ৫ মিনিট cache — বারবার কল করে নিজেই bandwidth না খায়
  return new Promise((resolve)=>{
    const now=Date.now(), start=now-24*3600*1000;
    const url = `https://api.render.com/v1/metrics/bandwidth?resource=${svc}&startTime=${new Date(start).toISOString()}&endTime=${new Date(now).toISOString()}`;
    https.get(url,{headers:{Authorization:"Bearer "+key,Accept:"application/json"}},(r)=>{
      let body="";r.on("data",c=>body+=c);
      r.on("end",()=>{
        try{
          const j = JSON.parse(body);
          const result = {configured:true, ok:r.statusCode===200, raw:j};
          _renderCache={data:result, at:Date.now()};
          resolve(result);
        }catch(e){ resolve({configured:true, ok:false, error:"parse ব্যর্থ"}); }
      });
    }).on("error",e=>resolve({configured:true, ok:false, error:e.message}));
  });
}
function getHeavyStatus(){
  try{
    const p = path.join(BDIR, ".heavy_status.json");
    const raw = JSON.parse(fs.readFileSync(p,"utf8"));
    if(Date.now()-raw.t > 15000) return null; // ১৫ সেকেন্ডের পুরনো হলে বাসি ধরে নেওয়া হচ্ছে (বট বন্ধ থাকতে পারে)
    return {active:raw.active, max:raw.max};
  }catch{ return null; }
}
app.get("/api/system/live",auth,async(req,res)=>{
  let mongoStats = null;
  if(db_connected && mongoose && mongoose.connection && mongoose.connection.db){
    try{
      const s = await mongoose.connection.db.stats();
      mongoStats = {
        dataSizeMB: +(s.dataSize/1024/1024).toFixed(2),
        storageSizeMB: +(s.storageSize/1024/1024).toFixed(2),
        indexSizeMB: +(s.indexSize/1024/1024).toFixed(2),
        totalMB: +((s.dataSize+s.indexSize)/1024/1024).toFixed(2),
        objects: s.objects
      };
    }catch(e){ mongoStats = {error: e.message}; }
  }
  const render = await getRenderBandwidth();
  const panelMB = Math.round(process.memoryUsage().rss/1024/1024);
  const botMB = getBotMemMB();

  // ── লাইফটাইম সর্বোচ্চ রেকর্ড আপডেট (MongoDB-তে ব্যাকআপসহ, তাই restart হলেও হারায় না) ──
  bumpLifetimePeak("peakPanelMB", panelMB);
  bumpLifetimePeak("peakBotMB", botMB);
  if(mongoStats && mongoStats.totalMB!=null) bumpLifetimePeak("peakMongoMB", mongoStats.totalMB);

  res.json({
    ok:true,
    time: Date.now(),
    ram: { panelMB, botMB, capMB: 512 }, // Render ফ্রি ইনস্ট্যান্সের হার্ড সীমা — প্রতিষ্ঠিত সত্য, API কল লাগে না
    mongo: mongoStats,
    render,
    heavy: getHeavyStatus(),
    lifetime: {
      ...lifetime,
      totalStarts: stats.starts||0,
      totalCrashes: stats.crashes||0,
      totalUptimeSec: stats.totalUptime||0
    }
  });
});



app.get("/api/backup",auth,(req,res)=>{
  res.setHeader("Content-Disposition",`attachment; filename="bot-backup-${Date.now()}.zip"`);
  const a=archiver("zip",{zlib:{level:9}});a.pipe(res);a.directory(BDIR,false);a.finalize();
});

// ── FILE API ──
app.get("/api/files",auth,(req,res)=>{
  try{
    const dir=safe(BDIR,req.query.path||"");
    if(!fs.existsSync(dir))return res.json({items:[],current:req.query.path||""});
    const items=fs.readdirSync(dir).map(name=>{
      const f=path.join(dir,name),s=fs.statSync(f);
      return{name,isDir:s.isDirectory(),size:s.size,mtime:s.mtime,ext:path.extname(name).toLowerCase()};
    }).sort((a,b)=>(b.isDir-a.isDir)||a.name.localeCompare(b.name));
    res.json({items,current:req.query.path||""});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get("/api/file/read",auth,(req,res)=>{
  try{
    const f=safe(BDIR,req.query.path),s=fs.statSync(f);
    if(s.size>5*1024*1024) return res.json({error:"ফাইল অনেক বড় (5MB+)"});
    res.json({content:fs.readFileSync(f,"utf8"),size:s.size});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/save",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    fs.mkdirSync(path.dirname(f),{recursive:true});
    fs.writeFileSync(f,req.body.content||"");
    // MongoDB তে সেভ
    const relPath = path.relative(BDIR,f);
    await saveToMongo(relPath, req.body.content||"");
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/delete",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    const relPath = path.relative(BDIR,f);
    fs.rmSync(f,{recursive:true,force:true});
    await deleteFromMongo(relPath);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/mkdir",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    fs.mkdirSync(f,{recursive:true});
    const relPath = path.relative(BDIR,f);
    await saveToMongo(relPath, Buffer.alloc(0), true);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/rename",auth,async(req,res)=>{
  try{
    const from=safe(BDIR,req.body.from),to=safe(BDIR,req.body.to);
    function cpR(s,d){fs.mkdirSync(d,{recursive:true});fs.readdirSync(s).forEach(n=>{const ss=path.join(s,n),dd=path.join(d,n);fs.statSync(ss).isDirectory()?cpR(ss,dd):fs.copyFileSync(ss,dd);});}
    const stat=fs.statSync(from);
    if(stat.isDirectory()){cpR(from,to);fs.rmSync(from,{recursive:true,force:true});}
    else{fs.copyFileSync(from,to);fs.unlinkSync(from);}
    // MongoDB আপডেট
    const fromRel=path.relative(BDIR,from), toRel=path.relative(BDIR,to);
    await deleteFromMongo(fromRel);
    if(stat.isDirectory()) await syncDirToMongo(to,toRel);
    else await saveToMongo(toRel,fs.readFileSync(to));
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/newfile",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    if(fs.existsSync(f)) return res.json({ok:false,msg:"ফাইল আছে"});
    fs.mkdirSync(path.dirname(f),{recursive:true});
    const content=req.body.content||"";
    fs.writeFileSync(f,content);
    const relPath=path.relative(BDIR,f);
    await saveToMongo(relPath,content);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/copy",auth,async(req,res)=>{
  try{
    const from=safe(BDIR,req.body.from),to=safe(BDIR,req.body.to);
    function cpR(s,d){fs.mkdirSync(d,{recursive:true});fs.readdirSync(s).forEach(n=>{const ss=path.join(s,n),dd=path.join(d,n);fs.statSync(ss).isDirectory()?cpR(ss,dd):fs.copyFileSync(ss,dd);});}
    const stat=fs.statSync(from);
    if(stat.isDirectory()) cpR(from,to);
    else fs.copyFileSync(from,to);
    const toRel=path.relative(BDIR,to);
    if(stat.isDirectory()) await syncDirToMongo(to,toRel);
    else await saveToMongo(toRel,fs.readFileSync(to));
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get("/api/file/download",auth,(req,res)=>{
  try{
    const f=safe(BDIR,req.query.path);
    if(fs.statSync(f).isDirectory()){
      res.setHeader("Content-Disposition",`attachment; filename="${path.basename(f)}.zip"`);
      const a=archiver("zip",{zlib:{level:9}});a.pipe(res);a.directory(f,false);a.finalize();
    }else res.download(f);
  }catch(e){res.status(500).send(e.message);}
});

// ── Shared processing for an uploaded file already sitting on disk ──
// filePath: temp file location, originalName: user's filename, reqPath: target folder (relative)
// Returns {httpStatus, body} — caller just forwards this as the JSON response.
async function processUploadedFile(filePath, originalName, reqPath){
  const t=safe(BDIR,reqPath||"");
  fs.mkdirSync(t,{recursive:true});

  if(originalName.endsWith(".zip")){
    // ZIP extract — validate first so a truncated/incomplete upload fails loudly
    // instead of silently extracting only whatever partial bytes arrived.
    const tmpX=path.join("/tmp","xtr_"+Date.now());
    fs.mkdirSync(tmpX,{recursive:true});
    let zipDir;
    try{
      zipDir = await unzipper.Open.file(filePath); // reads central directory — throws if truncated/corrupt
    }catch(e){
      try{fs.unlinkSync(filePath);}catch{}
      try{fs.rmSync(tmpX,{recursive:true,force:true});}catch{}
      log("❌ ZIP ফাইল অসম্পূর্ণ/করাপ্ট — আপলোড সম্পূর্ণ হয়নি: "+e.message,"error");
      return {httpStatus:400, body:{ok:false,msg:"❌ ZIP ফাইল অসম্পূর্ণ বা করাপ্ট, আপলোড ঠিকমতো শেষ হয়নি। আবার চেষ্টা করো।"}};
    }
    let expectedCount = zipDir.files.filter(f=>!f.path.startsWith("__MACOSX")).length;
    await zipDir.extract({path:tmpX, concurrency:5});
    try{fs.unlinkSync(filePath);}catch{}
    // cleanup junk
    ["__MACOSX",".DS_Store"].forEach(j=>{const jj=path.join(tmpX,j);if(fs.existsSync(jj))fs.rmSync(jj,{recursive:true,force:true});});
    // auto-flatten
    const entries=fs.readdirSync(tmpX);
    const nonDot=entries.filter(f=>!f.startsWith("."));
    let src=tmpX;
    if(nonDot.length===1){
      const s=path.join(tmpX,nonDot[0]);
      if(fs.statSync(s).isDirectory()){
        src=s;
        // ফ্ল্যাটেন হলে ওই wrapper ফোল্ডারটার নিজের এন্ট্রি বাদ দিয়ে গণনা করো (তার ভেতরের জিনিস আলাদাভাবে গোনা হবে)
        if(zipDir.files.some(f=>f.path.replace(/\/$/,"")===nonDot[0])) expectedCount--;
      }
    }
    // cross-device safe copy
    function cpR(s,d){
      fs.mkdirSync(d,{recursive:true});
      fs.readdirSync(s).forEach(n=>{
        const ss=path.join(s,n),dd=path.join(d,n);
        if(fs.statSync(ss).isDirectory()) cpR(ss,dd);
        else fs.copyFileSync(ss,dd);
      });
    }
    cpR(src,t);
    try{fs.rmSync(tmpX,{recursive:true,force:true});}catch{}
    // MongoDB তে sync
    const relT=path.relative(BDIR,t)||"";
    const syncStats=await syncDirToMongo(t,relT);
    if(syncStats.fail>0 || syncStats.skipped>0){
      log("⚠️ ZIP extract হয়েছে কিন্তু "+syncStats.fail+" টা ফাইল MongoDB তে সেভ ব্যর্থ, "+syncStats.skipped+" টা স্কিপ (10MB+): "+syncStats.failedFiles.join(", "),"error");
      return {httpStatus:200, body:{ok:true,msg:"⚠️ ZIP extract হয়েছে, কিন্তু "+(syncStats.fail+syncStats.skipped)+" টা ফাইল MongoDB তে সেভ হয়নি (দেখুন লগ) — restart হলে এগুলো হারিয়ে যাবে!",failedFiles:syncStats.failedFiles}};
    } else if(syncStats.ok < expectedCount){
      log("⚠️ ZIP এ ছিল "+expectedCount+" টা এন্ট্রি কিন্তু extract/sync হয়েছে মাত্র "+syncStats.ok+" টা — আপলোড সম্ভবত অসম্পূর্ণ ছিল","error");
      return {httpStatus:200, body:{ok:true,msg:"⚠️ ZIP এ ছিল "+expectedCount+" টা ফাইল/ফোল্ডার, কিন্তু মাত্র "+syncStats.ok+" টা পাওয়া গেছে। আপলোড সম্ভবত মাঝপথে কেটে গিয়েছিল — আবার আপলোড করো।"}};
    } else {
      log("📦 ZIP extract সম্পন্ন → "+(reqPath||"/")+" ("+syncStats.ok+" ফাইল)","success");
      return {httpStatus:200, body:{ok:true,msg:"ZIP extract সম্পন্ন ✅ সব "+syncStats.ok+" টা ফাইল MongoDB তে সেভ হয়েছে"}};
    }
  } else {
    // সাধারণ ফাইল
    const dst=path.join(t,originalName);
    fs.copyFileSync(filePath,dst);
    try{fs.unlinkSync(filePath);}catch{}
    const relPath=path.relative(BDIR,dst);
    await saveToMongo(relPath,fs.readFileSync(dst));
    return {httpStatus:200, body:{ok:true,msg:`✅ ${originalName} আপলোড সম্পন্ন`}};
  }
}

// ── UPLOAD: ZIP + যেকোনো ফাইল (একবারে, ছোট ফাইল/ভালো নেটওয়ার্কের জন্য) ──
app.post("/api/file/upload",auth,upload.single("file"),async(req,res)=>{
  try{
    const result = await processUploadedFile(req.file.path, req.file.originalname, req.body.path||"");
    res.status(result.httpStatus).json(result.body);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── UPLOAD: CHUNKED (দুর্বল/অস্থির নেটওয়ার্কের জন্য — প্রতিটা ছোট টুকরা আলাদাভাবে পাঠায়, fail হলে শুধু সেই টুকরাই আবার পাঠানো যায়) ──
const CHUNK_DIR = "/tmp/upload_chunks";
const chunkUpload = multer({storage:multer.diskStorage({
  destination:(r,f,cb)=>{
    const dir=path.join(CHUNK_DIR, String(r.body.uploadId||"unknown"));
    fs.mkdirSync(dir,{recursive:true});
    cb(null,dir);
  },
  filename:(r,f,cb)=>cb(null, String(r.body.chunkIndex).padStart(6,"0"))
}),limits:{fileSize:5*1024*1024}}); // each chunk max 5MB

// আপলোড আগে থেকে কতটুকু হয়ে আছে চেক করার endpoint — resume করার জন্য
app.get("/api/file/upload-status",auth,(req,res)=>{
  try{
    const uploadId=String(req.query.uploadId||"");
    const dir=path.join(CHUNK_DIR,uploadId);
    if(!uploadId || !fs.existsSync(dir)) return res.json({have:[]});
    const have=fs.readdirSync(dir).map(n=>parseInt(n,10)).filter(n=>!isNaN(n));
    res.json({have});
  }catch(e){res.json({have:[]});}
});

// ব্যাকগ্রাউন্ড প্রসেসিং এর ফলাফল রাখার জন্য (মেমরিতে, প্রতিটা uploadId এর জন্য)
const uploadResults = new Map();

app.post("/api/file/upload-chunk",auth,chunkUpload.single("chunk"),async(req,res)=>{
  try{
    const {uploadId,chunkIndex,totalChunks,fileName,path:reqPath}=req.body;
    if(!uploadId||chunkIndex===undefined||!totalChunks||!fileName){
      return res.status(400).json({ok:false,msg:"❌ চাংক তথ্য অসম্পূর্ণ"});
    }
    const idx=parseInt(chunkIndex,10), total=parseInt(totalChunks,10);
    const dir=path.join(CHUNK_DIR,String(uploadId));
    const present=fs.existsSync(dir)?fs.readdirSync(dir):[];
    if(present.length < total){
      // আরও চাংক বাকি আছে
      return res.json({ok:true,chunkIndex:idx,done:false,have:present.length,need:total});
    }
    // সব চাংক পৌঁছে গেছে — একত্র (reassemble) করো, তারপর সাথে সাথেই রেসপন্স পাঠিয়ে দাও
    const sortedPresent=present.slice().sort();
    const finalPath=path.join("/tmp","reassembled_"+Date.now()+"_"+fileName);
    const ws=fs.createWriteStream(finalPath);
    for(const chunkFile of sortedPresent){
      const buf=fs.readFileSync(path.join(dir,chunkFile));
      ws.write(buf);
    }
    await new Promise((ok,fail)=>ws.end(err=>err?fail(err):ok()));
    try{fs.rmSync(dir,{recursive:true,force:true});}catch{}

    uploadResults.set(uploadId,{status:"processing"});
    res.json({ok:true,chunkIndex:idx,done:true,processing:true,uploadId,msg:"📦 ফাইল পৌঁছেছে, এখন extract + সেভ হচ্ছে (ব্যাকগ্রাউন্ডে)..."});

    // ভারী কাজ (extract + MongoDB sync) request থেকে আলাদা করে ব্যাকগ্রাউন্ডে — যাতে সময় বেশি লাগলেও
    // HTTP request/connection timeout হয়ে সার্ভার ক্র্যাশ না করে
    processUploadedFile(finalPath, fileName, reqPath||"")
      .then(result=>{ uploadResults.set(uploadId,{status:"done",...result.body}); })
      .catch(e=>{ uploadResults.set(uploadId,{status:"done",ok:false,msg:"❌ "+e.message}); });
  }catch(e){res.status(500).json({ok:false,msg:"❌ "+e.message});}
});

// ব্যাকগ্রাউন্ড প্রসেসিং শেষ হয়েছে কিনা চেক করার জন্য — ক্লায়েন্ট এটা পোল করবে
app.get("/api/file/upload-result",auth,(req,res)=>{
  const uploadId=String(req.query.uploadId||"");
  const r=uploadResults.get(uploadId);
  if(!r) return res.json({status:"unknown"});
  res.json(r);
  if(r.status==="done") uploadResults.delete(uploadId); // একবার দেখানোর পর মুছে ফেলা
});


// Multiple files upload
app.post("/api/file/upload-multi",auth,upload.array("files",50),async(req,res)=>{
  try{
    const t=safe(BDIR,req.body.path||"");
    fs.mkdirSync(t,{recursive:true});
    const results=[];
    for(const file of req.files){
      const dst=path.join(t,file.originalname);
      fs.copyFileSync(file.path,dst);
      try{fs.unlinkSync(file.path);}catch{}
      const relPath=path.relative(BDIR,dst);
      await saveToMongo(relPath,fs.readFileSync(dst));
      results.push(file.originalname);
    }
    res.json({ok:true,msg:`✅ ${results.length}টা ফাইল আপলোড হয়েছে`});
  }catch(e){res.status(500).json({error:e.message});}
});

// Search
app.get("/api/file/search",auth,(req,res)=>{
  const q=(req.query.q||"").toLowerCase();if(!q)return res.json({results:[]});
  const results=[];
  function walk(dir,rel){try{fs.readdirSync(dir).forEach(name=>{const f=path.join(dir,name),rp=rel?rel+"/"+name:name,s=fs.statSync(f);if(name.toLowerCase().includes(q))results.push({name,path:rp,isDir:s.isDirectory(),size:s.size});if(s.isDirectory()&&results.length<100)walk(f,rp);});}catch{}}
  walk(BDIR,"");res.json({results:results.slice(0,50)});
});

// MongoDB sync manually
app.post("/api/mongo/sync",auth,async(req,res)=>{
  try{
    await syncDirToMongo(BDIR,"");
    res.json({ok:true,msg:"সব ফাইল MongoDB তে sync হয়েছে ✅"});
  }catch(e){res.json({ok:false,msg:e.message});}
});

app.post("/api/mongo/restore",auth,async(req,res)=>{
  try{
    await restoreFromMongo();
    res.json({ok:true,msg:"MongoDB থেকে সব ফাইল restore হয়েছে ✅"});
  }catch(e){res.json({ok:false,msg:e.message});}
});

app.get("/api/mongo/status",auth,(req,res)=>res.json({connected:db_connected}));

// ── ENV ──
app.get("/api/env",auth,(req,res)=>{const f=path.join(BDIR,".env");res.json({content:fs.existsSync(f)?fs.readFileSync(f,"utf8"):"",exists:fs.existsSync(f)});});
app.post("/api/env/save",auth,async(req,res)=>{
  try{
    fs.writeFileSync(path.join(BDIR,".env"),req.body.content||"");
    await saveToMongo(".env",req.body.content||"");
    res.json({ok:true});
  }catch(e){res.json({ok:false,msg:e.message});}
});

// ── COOKIE ──
app.get("/api/cookie/status",auth,(req,res)=>{
  try{
    const p=path.join(BDIR,"appstate.json");
    if(!fs.existsSync(p)) return res.json({saved:false});
    const content=fs.readFileSync(p,"utf8").trim();
    let arr; try{arr=JSON.parse(content);}catch{arr=null;}
    const saved = Array.isArray(arr) && arr.length>0;
    res.json({saved});
  }catch(e){res.json({saved:false});}
});

app.post("/api/cookie/save",auth,async(req,res)=>{
  try{
    const cookie=req.body.cookie||"";
    let appstate;
    try{appstate=JSON.parse(cookie);}catch{appstate=null;}
    if(appstate&&Array.isArray(appstate)){
      const content=JSON.stringify(appstate,null,2);
      fs.writeFileSync(path.join(BDIR,"appstate.json"),content);
      await saveToMongo("appstate.json",content);
      res.json({ok:true,msg:"Appstate সেভ হয়েছে ✅"});
    } else {
      const envFile=path.join(BDIR,".env");
      let env=fs.existsSync(envFile)?fs.readFileSync(envFile,"utf8"):"";
      if(env.includes("COOKIE=")) env=env.replace(/COOKIE=.*/,"COOKIE="+cookie);
      else env+="\nCOOKIE="+cookie;
      env=env.trim();
      fs.writeFileSync(envFile,env);
      await saveToMongo(".env",env);
      res.json({ok:true,msg:"Cookie .env এ সেভ হয়েছে ✅"});
    }
  }catch(e){res.json({ok:false,msg:e.message});}
});

// ── SETTINGS ──
app.get("/api/settings",auth,(req,res)=>res.json({...cfg,mongoConnected:db_connected}));
app.post("/api/settings/save",auth,(req,res)=>{
  Object.assign(cfg,req.body);
  if(req.body.autoRestart!==undefined) autoRestart=!!req.body.autoRestart;
  if(req.body.siteUrl) cfg.siteUrl=req.body.siteUrl.trim();
  saveCfg();
  res.json({ok:true});
});
app.post("/api/settings/password",auth,(req,res)=>{
  const{current,newPass}=req.body;
  if(current!==PASS&&current!==cfg.password) return res.json({ok:false,msg:"বর্তমান পাসওয়ার্ড ভুল"});
  if(!newPass||newPass.length<4) return res.json({ok:false,msg:"কমপক্ষে ৪ অক্ষর"});
  cfg.password=newPass;saveCfg();res.json({ok:true,msg:"পাসওয়ার্ড পরিবর্তন হয়েছে"});
});

// ── WS ──
wss.on("connection",ws=>{
  ws.send(JSON.stringify({type:"status",running:!!botProc}));
  ws.send(JSON.stringify({type:"logs",data:botLogs}));
  ws.send(JSON.stringify({type:"mongo",connected:db_connected}));
});

// ── START ──
server.listen(PORT,()=>{
  console.log("Panel: http://localhost:"+PORT);
  if(process.env.RENDER_EXTERNAL_URL&&!cfg.siteUrl){
    cfg.siteUrl=process.env.RENDER_EXTERNAL_URL;
    saveJ(CFG,cfg);
  }
  // MongoDB connect
  try{
    require.resolve("mongoose");
    connectMongo();
  }catch{
    console.log("⚠️ mongoose not installed — running without MongoDB");
    console.log("📦 Installing mongoose...");
    try{
      execSync("npm install mongoose",{timeout:60000});
      connectMongo();
    }catch(e){console.log("mongoose install failed:",e.message);}
  }
});

// ════════════════ HTML ════════════════
function loginHTML(){return `<!DOCTYPE html><html lang="bn"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bot Panel</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#07070e;font-family:'Segoe UI',sans-serif;overflow:hidden}
.bg{position:fixed;inset:0}
.orb{position:absolute;border-radius:50%;filter:blur(90px);opacity:.18;animation:fl 8s ease-in-out infinite}
.o1{width:500px;height:500px;background:#6c63ff;top:-150px;left:-150px}
.o2{width:350px;height:350px;background:#ff6584;bottom:-100px;right:-100px;animation-delay:4s}
.o3{width:200px;height:200px;background:#43e97b;top:40%;left:45%;animation-delay:2s}
@keyframes fl{0%,100%{transform:scale(1)}50%{transform:scale(1.2)}}
.card{position:relative;z-index:1;background:rgba(255,255,255,.04);backdrop-filter:blur(40px);border:1px solid rgba(255,255,255,.08);border-radius:28px;padding:52px 40px;width:90%;max-width:400px;text-align:center;box-shadow:0 30px 80px rgba(0,0,0,.6)}
.logo{width:90px;height:90px;margin:0 auto 22px;background:linear-gradient(135deg,#6c63ff,#ff6584);border-radius:26px;display:flex;align-items:center;justify-content:center;font-size:40px;box-shadow:0 0 60px rgba(108,99,255,.5);animation:pulse 3s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 40px rgba(108,99,255,.4)}50%{box-shadow:0 0 90px rgba(108,99,255,.9)}}
h1{color:#fff;font-size:24px;font-weight:900;margin-bottom:4px}
.sub{color:rgba(255,255,255,.3);font-size:13px;margin-bottom:36px}
input{width:100%;padding:15px 18px;border-radius:14px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.06);color:#fff;font-size:15px;outline:none;margin-bottom:14px;transition:.3s}
input:focus{border-color:#6c63ff;background:rgba(108,99,255,.1)}
.btn{width:100%;padding:15px;border-radius:14px;border:none;background:linear-gradient(135deg,#6c63ff,#ff6584);color:#fff;font-size:16px;font-weight:800;cursor:pointer;transition:.3s}
.btn:hover{transform:translateY(-2px);box-shadow:0 12px 40px rgba(108,99,255,.5)}
.err{background:rgba(255,85,85,.1);border:1px solid rgba(255,85,85,.2);color:#ff8080;padding:11px;border-radius:10px;font-size:13px;margin-bottom:14px;display:none}
.err.show{display:block}
</style></head><body>
<div class="bg"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
<div class="card">
  <div class="logo"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="width:28px;height:28px"><rect x="4" y="8" width="16" height="12" rx="3" fill="#fff" fill-opacity=".95"/><circle cx="9" cy="14" r="1.6" fill="#6C63FF"/><circle cx="15" cy="14" r="1.6" fill="#6C63FF"/><rect x="10.2" y="17" width="3.6" height="1.3" rx=".65" fill="#6C63FF"/><line x1="12" y1="4" x2="12" y2="8" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/><circle cx="12" cy="3.2" r="1.5" fill="#fff"/><rect x="1.8" y="12" width="2" height="4" rx="1" fill="#fff" fill-opacity=".8"/><rect x="20.2" y="12" width="2" height="4" rx="1" fill="#fff" fill-opacity=".8"/></svg></div>
  <h1>Bot Panel</h1>
  <p class="sub">তোমার বট কন্ট্রোল সেন্টার</p>
  <div class="err" id="err"></div>
  <input type="password" id="pw" placeholder="🔐 পাসওয়ার্ড লিখুন" autofocus>
  <button class="btn" onclick="login()">প্রবেশ করুন →</button>
</div>
<script>
async function login(){
  const r=await fetch("/login",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"password="+encodeURIComponent(document.getElementById("pw").value)});
  const d=await r.json();
  if(d.ok)location.href="/";
  else{const e=document.getElementById("err");e.textContent=d.msg;e.classList.add("show");}
}
document.getElementById("pw").addEventListener("keydown",e=>e.key==="Enter"&&login());
</script></body></html>`;}

function mainHTML(){
const pname=cfg.panelName||"Bot Panel";
return `<!DOCTYPE html><html lang="bn"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#07070e">
<title>${pname}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--bg:#07070e;--s1:#0d0d18;--s2:#141424;--s3:#1a1a2e;--bd:#232338;--tx:#dde0f0;--mu:#5a5a80;--ac:#6c63ff;--gr:#3ecf8e;--rd:#f05252;--yw:#f0b429;--bl:#38bdf8;--or:#fb923c}
body{background:var(--bg);color:var(--tx);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;overflow-x:hidden}
.top{position:fixed;top:0;left:0;right:0;height:54px;background:rgba(13,13,24,.97);backdrop-filter:blur(20px);border-bottom:1px solid var(--bd);display:flex;align-items:center;padding:0 14px;z-index:200;gap:10px}
.top-logo{width:34px;height:34px;background:linear-gradient(135deg,var(--ac),#ff6584);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;box-shadow:0 0 20px rgba(108,99,255,.4);transition:box-shadow .4s}
.top-logo svg{width:20px;height:20px;filter:drop-shadow(0 0 2px rgba(255,255,255,.4))}
.top-logo.live{animation:logoPulse 1.8s ease-in-out infinite}
@keyframes logoPulse{
  0%,100%{box-shadow:0 0 20px rgba(108,99,255,.4),0 0 0 0 rgba(46,213,115,.5)}
  50%{box-shadow:0 0 28px rgba(108,99,255,.7),0 0 0 8px rgba(46,213,115,0)}
}
.top-name{font-size:15px;font-weight:800;color:#fff;flex:1}
.top-pills{display:flex;align-items:center;gap:6px}
.top-pill{display:flex;align-items:center;gap:5px;background:var(--s2);border:1px solid var(--bd);border-radius:99px;padding:4px 10px;font-size:11px}
.dot{width:7px;height:7px;border-radius:50%;background:var(--rd);flex-shrink:0;transition:.3s}
.dot.on{background:var(--gr);box-shadow:0 0 8px var(--gr);animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.top-out{padding:6px 10px;border-radius:8px;border:1px solid rgba(240,82,82,.3);background:transparent;color:var(--rd);font-size:11px;cursor:pointer}
.tabs{position:fixed;bottom:0;left:0;right:0;background:rgba(13,13,24,.97);backdrop-filter:blur(20px);border-top:1px solid var(--bd);display:grid;grid-template-columns:repeat(5,1fr);height:60px;z-index:200;padding-bottom:env(safe-area-inset-bottom)}
.tab{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;cursor:pointer;border:none;background:transparent;color:var(--mu);transition:.15s;position:relative}
.tab.active{color:var(--ac)}
.tab .ti{font-size:22px;line-height:1}
.tab .tl{font-size:9px;font-weight:700}
.tab::after{content:"";position:absolute;top:0;left:50%;transform:translateX(-50%);width:0;height:2px;background:var(--ac);border-radius:0 0 3px 3px;transition:.2s}
.tab.active::after{width:40px}
.main{padding:66px 12px 72px;min-height:100vh}
.page{display:none}.page.active{display:block}
.pg-title{font-size:15px;font-weight:800;color:#fff;margin-bottom:12px}
.sg{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.sg3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px}
.sc{background:linear-gradient(135deg,var(--s2),var(--s3));border:1px solid var(--bd);border-radius:14px;padding:14px;transition:.2s}
.sc:hover{border-color:var(--ac)}
.sc-i{font-size:24px;margin-bottom:6px}
.sc-v{font-size:20px;font-weight:900;color:#fff}
.sc-l{font-size:10px;color:var(--mu);margin-top:2px}
.bc{background:var(--s2);border:1px solid var(--bd);border-radius:16px;padding:14px;margin-bottom:12px}
.bst{display:flex;align-items:center;gap:10px;margin-bottom:12px;padding:10px 12px;background:var(--s3);border-radius:12px}
.bst-txt{font-size:14px;font-weight:700}
.bst-up{font-size:11px;color:var(--mu);margin-top:2px}
.bg2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.bg3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:8px}
.mon-card{background:linear-gradient(135deg,var(--s2),var(--s3));border:1px solid var(--bd);border-radius:16px;padding:16px;margin-bottom:14px}
.mon-head{display:flex;align-items:center;gap:8px;font-size:14px;font-weight:800;color:#fff;margin-bottom:12px}
.mon-row{margin-bottom:14px}
.mon-row:last-child{margin-bottom:0}
.mon-row-top{display:flex;justify-content:space-between;align-items:baseline;font-size:12px;margin-bottom:6px}
.mon-row-label{color:var(--tx);font-weight:600}
.mon-row-val{color:var(--mu);font-variant-numeric:tabular-nums}
.mbar{height:10px;border-radius:6px;background:var(--s1);border:1px solid var(--bd);overflow:hidden}
.mbar-fill{height:100%;border-radius:6px;transition:width .5s ease,background .5s ease}
.mon-note{font-size:10px;color:var(--mu);margin-top:10px;line-height:1.5}
.mon-peak{font-size:10px;color:var(--mu);margin-top:5px;display:flex;justify-content:flex-end;gap:4px}
.mon-peak b{color:var(--bl);font-weight:700}
.mon-badge{display:inline-block;font-size:10px;padding:2px 8px;border-radius:20px;margin-left:6px;font-weight:700}
.mon-badge.ok{background:rgba(62,207,142,.15);color:var(--gr)}
.mon-badge.warn{background:rgba(240,180,41,.15);color:var(--yw)}
.mon-badge.err{background:rgba(240,82,82,.15);color:var(--rd)}
.mon-badge.off{background:rgba(90,90,128,.2);color:var(--mu)}
.btn{width:100%;padding:11px 8px;border-radius:12px;border:none;font-size:12px;font-weight:700;cursor:pointer;transition:.15s;display:flex;align-items:center;justify-content:center;gap:5px}
.btn:active{transform:scale(.96)}
.b-start{background:linear-gradient(135deg,#3ecf8e,#22d3ee);color:#000}
.b-stop{background:linear-gradient(135deg,#f05252,#fb7185);color:#fff}
.b-restart{background:linear-gradient(135deg,#f0b429,#fb923c);color:#000}
.b-npm{background:linear-gradient(135deg,#38bdf8,#6c63ff);color:#fff}
.b-backup{background:linear-gradient(135deg,#a78bfa,#ec4899);color:#fff}
.b-ghost{background:transparent;border:1px solid var(--bd);color:var(--tx)}
.b-green{background:linear-gradient(135deg,#3ecf8e,#10b981);color:#000}
.tog-row{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-top:1px solid var(--bd);margin-top:8px}
.tog{position:relative;width:44px;height:24px;flex-shrink:0}
.tog input{display:none}
.tog-bg{position:absolute;inset:0;background:var(--bd);border-radius:99px;cursor:pointer;transition:.3s}
.tog input:checked+.tog-bg{background:var(--gr)}
.tog-dot{position:absolute;top:3px;left:3px;width:18px;height:18px;background:#fff;border-radius:50%;transition:.3s;pointer-events:none}
.tog input:checked~.tog-dot{transform:translateX(20px)}
.mongo-badge{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:99px;font-size:11px;font-weight:700;border:1px solid var(--bd);background:var(--s3)}
.mongo-badge.ok{border-color:rgba(62,207,142,.3);color:var(--gr)}
.mongo-badge.err{border-color:rgba(240,82,82,.3);color:var(--rd)}
.cookie-box{background:var(--s2);border:1px solid var(--bd);border-radius:16px;padding:14px;margin-bottom:12px}
textarea.ci{width:100%;height:80px;background:var(--s3);border:1px solid var(--bd);border-radius:10px;padding:10px;color:var(--tx);font-family:'Courier New',monospace;font-size:11px;resize:none;outline:none;transition:.2s;line-height:1.5;margin:10px 0}
textarea.ci:focus{border-color:var(--ac)}
.hist{display:flex;flex-direction:column;gap:5px;max-height:180px;overflow-y:auto}
.hi{display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--s2);border-radius:8px;border:1px solid var(--bd);font-size:10px}
.hi-date{color:var(--mu);flex:1}
.hi-up{color:var(--gr);font-weight:700}
.hi-code{color:var(--yw)}
.log-bar{display:flex;gap:5px;margin-bottom:10px;overflow-x:auto;padding-bottom:2px}
.log-bar::-webkit-scrollbar{display:none}
.lf{padding:5px 10px;border-radius:7px;border:1px solid var(--bd);background:transparent;color:var(--mu);font-size:11px;cursor:pointer;white-space:nowrap;transition:.15s}
.lf.on{background:var(--ac);color:#fff;border-color:var(--ac)}
.lbox{background:#020209;border:1px solid var(--bd);border-radius:12px;padding:10px;height:calc(100vh - 210px);overflow-y:auto;font-family:'Courier New',monospace;font-size:11px}
.lbox::-webkit-scrollbar{width:3px}
.lbox::-webkit-scrollbar-thumb{background:var(--bd);border-radius:2px}
.le{display:flex;gap:5px;padding:2px 0;line-height:1.6}
.lt{color:var(--mu);white-space:nowrap;font-size:10px;flex-shrink:0}
.lx{word-break:normal;overflow-wrap:anywhere;line-height:1.55}
.li .lx{color:#9ca3af}.ls .lx{color:var(--gr)}.lr .lx{color:var(--rd)}.lw .lx{color:var(--yw)}
.pathbar{background:var(--s2);border:1px solid var(--bd);border-radius:10px;padding:8px 12px;font-size:12px;color:var(--mu);margin-bottom:10px;overflow-x:auto;white-space:nowrap;display:flex;align-items:center;gap:4px}
.pathbar::-webkit-scrollbar{display:none}
.pp{color:var(--ac);cursor:pointer;font-weight:600}.pp:hover{text-decoration:underline}
.fm-acts{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.tbtn{padding:7px 12px;border-radius:9px;border:1px solid var(--bd);background:var(--s2);color:var(--tx);font-size:12px;cursor:pointer;white-space:nowrap;transition:.15s;display:inline-flex;align-items:center;gap:4px}
.tbtn:hover{background:var(--s3)}
.tbtn.p{background:var(--ac);border-color:var(--ac);color:#fff}
.tbtn.d{border-color:rgba(240,82,82,.3);color:var(--rd)}
.sinput{width:100%;padding:10px 14px;border-radius:10px;border:1px solid var(--bd);background:var(--s2);color:var(--tx);font-size:13px;outline:none;margin-bottom:10px;transition:.2s}
.sinput:focus{border-color:var(--ac)}
.flist{background:var(--s2);border:1px solid var(--bd);border-radius:14px;overflow:hidden}
.frow{display:flex;align-items:center;gap:10px;padding:11px 12px;border-bottom:1px solid rgba(255,255,255,.03);cursor:pointer;transition:.12s}
.frow:last-child{border-bottom:none}
.frow:active{background:rgba(108,99,255,.07)}
.fi{font-size:19px;flex-shrink:0;width:24px;text-align:center}
.fn{flex:1;overflow:hidden}
.fn-name{font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:500}
.fn-meta{font-size:10px;color:var(--mu);margin-top:2px}
.fa{display:flex;gap:3px;flex-shrink:0}
.fab{padding:5px 7px;border-radius:6px;border:none;background:var(--s3);color:var(--mu);font-size:11px;cursor:pointer;transition:.12s}
.fab:hover{background:var(--bd);color:var(--tx)}
.fab.del:hover{background:rgba(240,82,82,.15);color:var(--rd)}
.empty-fm{padding:40px;text-align:center;color:var(--mu)}
.ed-top{background:var(--s2);border:1px solid var(--bd);border-radius:12px 12px 0 0;padding:10px 12px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.ed-fn{flex:1;font-size:12px;color:var(--ac);font-weight:700;overflow:hidden;text-overflow:ellipsis}
.ed-lang{font-size:10px;color:var(--mu);background:var(--s3);padding:2px 7px;border-radius:5px}
#ced{width:100%;height:calc(100vh - 240px);background:#010108;border:1px solid var(--bd);border-top:none;border-radius:0 0 12px 12px;padding:14px;color:#e6edf3;font-family:'Courier New',monospace;font-size:13px;line-height:1.8;resize:none;outline:none;tab-size:2}
.upzone{border:2px dashed var(--bd);border-radius:16px;padding:40px 16px;text-align:center;cursor:pointer;background:var(--s2);transition:.3s;margin-bottom:12px}
.upzone:active,.upzone.drag{border-color:var(--ac);background:rgba(108,99,255,.05)}
.uz-i{font-size:48px;margin-bottom:12px;animation:bounce 2s ease-in-out infinite}
@keyframes bounce{0%,100%{transform:translateY(0)}50%{transform:translateY(-6px)}}
.prog-wrap{background:var(--s2);border:1px solid var(--bd);border-radius:12px;padding:14px;display:none;margin-bottom:12px}
.prog-top{display:flex;justify-content:space-between;font-size:12px;margin-bottom:7px}
.prog-bg{background:var(--bd);border-radius:99px;height:7px;overflow:hidden}
.prog{height:100%;background:linear-gradient(90deg,var(--ac),var(--gr));border-radius:99px;transition:width .2s;width:0}
#envEd{width:100%;height:250px;background:#010108;border:1px solid var(--bd);border-radius:10px;padding:12px;color:#e6edf3;font-family:'Courier New',monospace;font-size:13px;line-height:1.8;resize:vertical;outline:none;margin-bottom:10px;transition:.2s}
#envEd:focus{border-color:var(--ac)}
.set-card{background:var(--s2);border:1px solid var(--bd);border-radius:14px;padding:14px;margin-bottom:10px}
.set-title{font-size:13px;font-weight:700;color:#fff;margin-bottom:12px}
.set-row{display:flex;align-items:center;justify-content:space-between;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.04);gap:8px}
.set-row:last-child{border-bottom:none;padding-bottom:0}
.sr-l{font-size:13px;flex:1}.sr-s{font-size:10px;color:var(--mu);margin-top:2px}
.sinp{padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--s1);color:var(--tx);font-size:12px;outline:none;max-width:170px;transition:.2s}
.sinp:focus{border-color:var(--ac)}
.mbg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:500;align-items:flex-end;justify-content:center;backdrop-filter:blur(6px)}
.mbg.open{display:flex}
.modal{background:var(--s2);border:1px solid var(--bd);border-radius:20px 20px 0 0;padding:22px;width:100%;max-width:520px;animation:mIn .25s ease;padding-bottom:max(22px,env(safe-area-inset-bottom))}
@keyframes mIn{from{transform:translateY(100%)}to{transform:translateY(0)}}
.modal h3{font-size:15px;font-weight:800;margin-bottom:14px;color:#fff;text-align:center}
.modal input{width:100%;padding:11px 14px;border-radius:10px;border:1px solid var(--bd);background:var(--s1);color:var(--tx);font-size:14px;outline:none;margin-bottom:12px;transition:.2s}
.modal input:focus{border-color:var(--ac)}
.modal-btns{display:flex;gap:8px}
.tw{position:fixed;top:62px;right:12px;display:flex;flex-direction:column;gap:6px;z-index:999;pointer-events:none;max-width:260px}
.toast{background:var(--s3);border-radius:10px;padding:10px 14px;font-size:12px;animation:tIn .3s ease;box-shadow:0 8px 24px rgba(0,0,0,.5);pointer-events:auto;border-left:3px solid var(--bd);position:relative;overflow:hidden;display:flex;align-items:center;gap:8px}
@keyframes tIn{from{transform:translateX(120%);opacity:0}to{transform:translateX(0);opacity:1}}
.toast.success{border-left-color:var(--gr);color:var(--gr);box-shadow:0 8px 24px rgba(46,213,115,.15)}
.toast.error{border-left-color:var(--rd);color:var(--rd);box-shadow:0 8px 24px rgba(240,82,82,.15)}
.toast.warn{border-left-color:var(--yw);color:var(--yw);box-shadow:0 8px 24px rgba(255,193,7,.15)}
.toast-bar{position:absolute;bottom:0;left:0;height:2px;background:currentColor;opacity:.5;animation:tShrink 4s linear forwards}
@keyframes tShrink{from{width:100%}to{width:0%}}
.srow{padding:10px 12px;background:var(--s2);border:1px solid var(--bd);border-radius:9px;margin-bottom:6px;cursor:pointer;transition:.12s}
.srow:active{background:var(--s3)}
.srow-p{font-size:11px;color:var(--ac);margin-bottom:2px;font-weight:600}
.srow-m{font-size:11px;color:var(--mu)}
.multi-upload-area{border:1px dashed var(--bd);border-radius:12px;padding:16px;background:var(--s2);margin-bottom:12px}
</style></head><body>

<div class="top">
  <div class="top-logo" id="topLogo"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="4" y="8" width="16" height="12" rx="3" fill="#fff" fill-opacity=".95"/><circle cx="9" cy="14" r="1.6" fill="#6C63FF"/><circle cx="15" cy="14" r="1.6" fill="#6C63FF"/><rect x="10.2" y="17" width="3.6" height="1.3" rx=".65" fill="#6C63FF"/><line x1="12" y1="4" x2="12" y2="8" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/><circle cx="12" cy="3.2" r="1.5" fill="#fff"/><rect x="1.8" y="12" width="2" height="4" rx="1" fill="#fff" fill-opacity=".8"/><rect x="20.2" y="12" width="2" height="4" rx="1" fill="#fff" fill-opacity=".8"/></svg></div>
  <div class="top-name">${pname}</div>
  <div class="top-pills">
    <div class="top-pill"><div class="dot" id="tDot"></div><span id="tStatus">লোড...</span></div>
    <div class="top-pill" id="mongoPill"><div class="dot" id="mongoDot"></div><span id="mongoStatus">DB</span></div>
  </div>
  <button class="top-out" onclick="location.href='/logout'">বের</button>
</div>

<div class="main">

<!-- HOME -->
<div id="pg-home" class="page active">
  <div class="sg">
    <div class="sc"><div class="sc-i">💾</div><div class="sc-v" id="cMem">--</div><div class="sc-l">Memory MB</div></div>
    <div class="sc"><div class="sc-i">⏱️</div><div class="sc-v" id="cSup">--</div><div class="sc-l">Server Uptime</div></div>
    <div class="sc"><div class="sc-i">📦</div><div class="sc-v" id="cFiles">--</div><div class="sc-l">বট ফাইল</div></div>
    <div class="sc"><div class="sc-i">🚀</div><div class="sc-v" id="cStarts">--</div><div class="sc-l">মোট Start</div></div>
  </div>
  <div class="sg3">
    <div class="sc"><div class="sc-i">💥</div><div class="sc-v" id="cCrash">--</div><div class="sc-l">Crash</div></div>
    <div class="sc"><div class="sc-i">🕐</div><div class="sc-v" id="cTup">--</div><div class="sc-l">মোট Uptime</div></div>
    <div class="sc"><div class="sc-i">🖥️</div><div class="sc-v" id="cNode">--</div><div class="sc-l">Node.js</div></div>
  </div>

  <!-- COOKIE -->
  <div class="cookie-box">
    <div style="font-size:13px;font-weight:700;margin-bottom:4px">🍪 Facebook Cookie / Appstate</div>
    <div style="font-size:11px;color:var(--mu)">Cookie বা appstate.json paste করুন → বট চালু</div>
    <div id="cookieStatus" style="font-size:12px;margin:6px 0;padding:6px 10px;border-radius:8px;display:none"></div>
    <textarea class="ci" id="cookieInput" placeholder='[{"key":"c_user","value":"..."}] অথবা plain cookie string'></textarea>
    <button class="btn b-start" onclick="saveCookie()">✅ Cookie সেভ ও বট চালু করুন</button>
  </div>

  <!-- BOT CONTROL -->
  <div class="bc">
    <div class="bst">
      <div class="dot" id="sDot"></div>
      <div><div class="bst-txt" id="sTxt">চেক করছে...</div><div class="bst-up" id="sUp"></div></div>
    </div>
    <div class="bg2">
      <button class="btn b-start"   onclick="botAct('start')">▶ চালু</button>
      <button class="btn b-stop"    onclick="botAct('stop')">⏹ বন্ধ</button>
    </div>
    <div class="bg3">
      <button class="btn b-restart" onclick="botAct('restart')">🔄 রিস্টার্ট</button>
      <button class="btn b-npm"     onclick="npmInst()">📦 npm</button>
      <button class="btn b-backup"  onclick="doBackup()">💾 Backup</button>
    </div>
    <div class="bg2" style="margin-top:8px">
      <button class="btn b-green" onclick="mongoSync()">☁️ MongoDB Sync</button>
      <button class="btn b-ghost" onclick="mongoRestore()">🔄 Restore</button>
    </div>
    <div class="tog-row">
      <div><div style="font-size:13px;font-weight:600">Auto Restart</div><div style="font-size:10px;color:var(--mu);margin-top:2px">Crash হলে অটো চালু</div></div>
      <label class="tog"><input type="checkbox" id="arTog" onchange="toggleAR(this.checked)"><div class="tog-bg"></div><div class="tog-dot"></div></label>
    </div>
  </div>

  <!-- HISTORY -->
  <div class="bc">
    <div class="pg-title">📈 Restart ইতিহাস</div>
    <div class="hist" id="histList"><div style="font-size:12px;color:var(--mu);text-align:center;padding:12px">লোড হচ্ছে...</div></div>
  </div>
</div>

<!-- LIVE MONITOR -->
<div id="pg-monitor" class="page">
  <div class="pg-title">📊 লাইভ সিস্টেম মনিটর</div>

  <!-- CARD ১: RAM / Render রিসোর্স -->
  <div class="mon-card">
    <div class="mon-head">🖥️ RAM ব্যবহার <span class="mon-badge off">সীমা ৫১২MB</span></div>

    <div class="mon-row" id="mHeavyRow" style="display:none">
      <div class="mon-row-top"><span class="mon-row-label">⬇️ লাইভ ডাউনলোড</span><span class="mon-row-val" id="mHeavyTxt">-- / --</span></div>
      <div class="mbar"><div class="mbar-fill" id="mHeavyBar" style="width:0%;background:var(--bl)"></div></div>
    </div>

    <div class="mon-row">
      <div class="mon-row-top"><span class="mon-row-label">🧩 প্যানেল</span><span class="mon-row-val" id="mPanelTxt">-- MB / 512 MB</span></div>
      <div class="mbar"><div class="mbar-fill" id="mPanelBar" style="width:0%;background:var(--gr)"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ (লাইফটাইম): <b id="mPanelPeak">-- MB</b></div>
    </div>

    <div class="mon-row">
      <div class="mon-row-top"><span class="mon-row-label">🤖 বট</span><span class="mon-row-val" id="mBotTxt">-- MB / 512 MB</span></div>
      <div class="mbar"><div class="mbar-fill" id="mBotBar" style="width:0%;background:var(--gr)"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ (লাইফটাইম): <b id="mBotPeak">-- MB</b></div>
    </div>

    <div class="mon-row">
      <div class="mon-row-top"><span class="mon-row-label">⚡ মোট (প্যানেল+বট)</span><span class="mon-row-val" id="mTotalTxt">-- MB / 512 MB</span></div>
      <div class="mbar"><div class="mbar-fill" id="mTotalBar" style="width:0%;background:var(--gr)"></div></div>
    </div>

    <div class="mon-row" id="mRenderRow" style="display:none">
      <div class="mon-row-top"><span class="mon-row-label">🌐 Render ব্যান্ডউইথ (২৪ ঘণ্টা)<span class="mon-badge" id="mRenderBadge"></span></span><span class="mon-row-val" id="mRenderTxt"></span></div>
    </div>
    <div class="mon-note" id="mRenderNote">ℹ️ Render bandwidth সরাসরি দেখতে Render Dashboard → Account Settings-এ একটা API Key বানিয়ে <b>RENDER_API_KEY</b> আর <b>RENDER_SERVICE_ID</b> নামে Environment Variable হিসেবে বসিয়ে দাও — তাহলে এখানেও লাইভ দেখাবে। এই মুহূর্তে সঠিক মোট ব্যান্ডউইথ/মাসিক সীমার জন্য Render Dashboard-ই সবচেয়ে নির্ভরযোগ্য জায়গা।</div>
  </div>

  <!-- CARD ২: MongoDB স্টোরেজ -->
  <div class="mon-card">
    <div class="mon-head">🗄️ MongoDB Atlas স্টোরেজ <span class="mon-badge off">M0 সীমা ৫১২MB</span></div>

    <div class="mon-row">
      <div class="mon-row-top"><span class="mon-row-label">💽 ব্যবহৃত (ডেটা + ইনডেক্স)</span><span class="mon-row-val" id="mMongoTxt">-- MB / 512 MB</span></div>
      <div class="mbar"><div class="mbar-fill" id="mMongoBar" style="width:0%;background:var(--gr)"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ (লাইফটাইম): <b id="mMongoPeak">-- MB</b></div>
    </div>

    <div class="sg3" style="margin-top:12px">
      <div class="sc"><div class="sc-i">📄</div><div class="sc-v" id="mMongoObjs" style="font-size:16px">--</div><div class="sc-l">মোট এন্ট্রি</div></div>
      <div class="sc"><div class="sc-i">💾</div><div class="sc-v" id="mMongoData" style="font-size:16px">--</div><div class="sc-l">Data (MB)</div></div>
      <div class="sc"><div class="sc-i">🔎</div><div class="sc-v" id="mMongoIdx" style="font-size:16px">--</div><div class="sc-l">Index (MB)</div></div>
    </div>
    <div class="mon-note">ℹ️ এই হিসাব MongoDB Atlas-এর ফ্রি (M0) টায়ারের ৫১২MB সীমা ধরে দেখানো হচ্ছে। অন্য টায়ার ব্যবহার করলে সীমা ভিন্ন হবে, তখন এই শতাংশ সরাসরি প্রযোজ্য না।</div>
  </div>

  <!-- CARD ৩: লাইফটাইম সামারি -->
  <div class="mon-card">
    <div class="mon-head">⏳ লাইফটাইম সামারি <span class="mon-badge ok">সব সময়ই সেভ হয়</span></div>
    <div class="sg3">
      <div class="sc"><div class="sc-i">🚀</div><div class="sc-v" id="mLtStarts" style="font-size:16px">--</div><div class="sc-l">মোট চালু হয়েছে</div></div>
      <div class="sc"><div class="sc-i">💥</div><div class="sc-v" id="mLtCrashes" style="font-size:16px">--</div><div class="sc-l">মোট ক্র্যাশ</div></div>
      <div class="sc"><div class="sc-i">🕒</div><div class="sc-v" id="mLtUptime" style="font-size:14px">--</div><div class="sc-l">মোট সচল সময়</div></div>
    </div>
    <div class="mon-note" style="margin-top:12px">📅 ট্র্যাক করা হচ্ছে যবে থেকে: <b id="mLtSince">--</b><br>ℹ️ এই সংখ্যাগুলো প্যানেল বা বট যতবারই restart হোক না কেন কখনো শূন্য হয়ে যায় না — MongoDB-তে স্থায়ীভাবে জমা থাকে।</div>
  </div>
</div>

<!-- LOGS -->
<div id="pg-logs" class="page">
  <div class="log-bar">
    <button class="lf on" onclick="setLF('all',this)">📋 সব</button>
    <button class="lf" onclick="setLF('success',this)">✅</button>
    <button class="lf" onclick="setLF('error',this)">❌</button>
    <button class="lf" onclick="setLF('warn',this)">⚠️</button>
    <button class="lf" onclick="clearLogs()">🗑</button>
    <button class="lf" onclick="window.open('/api/bot/downloadlog')">⬇️</button>
    <button class="lf" onclick="autoScroll=!autoScroll;this.textContent=autoScroll?'↓ Auto':'↕ Man'">↓ Auto</button>
  </div>
  <div class="lbox" id="lbox"></div>
</div>

<!-- FILES -->
<div id="pg-files" class="page">
  <div id="edView" style="display:none">
    <div class="ed-top">
      <button class="tbtn" onclick="closeEd()">← ফিরে</button>
      <span class="ed-fn" id="edFn"></span>
      <span class="ed-lang" id="edLang"></span>
      <button class="tbtn p" onclick="saveFile()">💾 সেভ</button>
      <button class="tbtn" onclick="dlF(curEdit)">⬇️</button>
    </div>
    <textarea id="ced" spellcheck="false"></textarea>
  </div>
  <div id="fmView">
    <div class="pathbar" id="pathBar">📁 root</div>
    <div class="fm-acts">
      <button class="tbtn p" onclick="showM('mkdir')">📁+</button>
      <button class="tbtn p" onclick="showM('newfile')">📄+</button>
      <button class="tbtn" onclick="loadFiles(curDir)">🔄</button>
      <button class="tbtn" onclick="editF('package.json')">📋 pkg</button>
      <button class="tbtn" onclick="editF('index.js')">📜 index</button>
      <button class="tbtn" onclick="editF('.env')">🔐 env</button>
    </div>
    <input class="sinput" type="text" id="fq" placeholder="🔍 ফাইল খোঁজুন..." oninput="doFS()">
    <div id="fsRes" style="display:none;margin-bottom:10px"></div>
    <div class="flist" id="flist"></div>
  </div>
</div>

<!-- UPLOAD -->
<div id="pg-upload" class="page">
  <div class="pg-title">⬆️ আপলোড</div>

  <!-- ZIP UPLOAD -->
  <div class="upzone" id="upZone" onclick="document.getElementById('fInp').click()">
    <div class="uz-i">📦</div>
    <div style="font-size:14px;font-weight:700;margin-bottom:5px">ZIP আপলোড করুন</div>
    <div style="color:var(--mu);font-size:12px">অটো extract + MongoDB সেভ হবে</div>
    <div style="color:var(--bl);font-size:11px;margin-top:6px;font-weight:600">সর্বোচ্চ ৫০০MB</div>
  </div>
  <input type="file" id="fInp" accept=".zip" style="display:none" onchange="uploadF(this.files[0])">

  <div class="prog-wrap" id="progWrap">
    <div class="prog-top"><span id="upFN">আপলোড হচ্ছে...</span><span id="upPct">0%</span></div>
    <div class="prog-bg"><div class="prog" id="progBar"></div></div>
    <div id="upSt" style="font-size:11px;color:var(--mu);margin-top:5px"></div>
  </div>

  <!-- SINGLE FILE -->
  <div class="multi-upload-area">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">📄 একক ফাইল আপলোড</div>
    <input type="file" id="singleInp" style="display:none" onchange="uploadSingle(this.files[0])">
    <button class="tbtn p" onclick="document.getElementById('singleInp').click()">📄 ফাইল বেছে নিন</button>
    <div id="singleStatus" style="font-size:12px;color:var(--mu);margin-top:8px"></div>
  </div>

  <!-- MULTI FILE -->
  <div class="multi-upload-area">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">📂 একসাথে অনেক ফাইল</div>
    <input type="file" id="multiInp" multiple style="display:none" onchange="uploadMulti(this.files)">
    <button class="tbtn p" onclick="document.getElementById('multiInp').click()">📂 ফাইলগুলো বেছে নিন</button>
    <div id="multiStatus" style="font-size:12px;color:var(--mu);margin-top:8px"></div>
  </div>

  <!-- CURRENT FOLDER -->
  <div style="background:var(--s2);border:1px solid var(--bd);border-radius:12px;padding:12px;font-size:12px">
    <div style="color:var(--mu);margin-bottom:6px">📁 আপলোড হবে:</div>
    <div style="color:var(--ac);font-weight:700">/bot/<span id="uploadDir">root</span></div>
    <div style="color:var(--mu);font-size:11px;margin-top:4px">ফাইল ম্যানেজারে ফোল্ডারে ঢুকে আপলোড করলে সেখানে যাবে</div>
  </div>
</div>

<!-- MORE -->
<div id="pg-more" class="page">
  <!-- ENV -->
  <div class="set-card">
    <div class="set-title">⚙️ Environment (.env)</div>
    <textarea id="envEd" spellcheck="false" placeholder="TOKEN=xxx&#10;COOKIE=xxx&#10;PREFIX=!&#10;ADMIN_ID=123456&#10;MONGO_URL=xxx"></textarea>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="tbtn p" onclick="saveEnv()">💾 সেভ</button>
      <button class="tbtn" onclick="loadEnv()">🔄 রিলোড</button>
    </div>
  </div>

  <!-- MONGODB STATUS -->
  <div class="set-card">
    <div class="set-title">☁️ MongoDB স্ট্যাটাস</div>
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
      <div class="mongo-badge" id="mongoBadge">চেক করছে...</div>
      <div style="font-size:11px;color:var(--mu)" id="mongoInfo"></div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="tbtn p" onclick="mongoSync()">☁️ এখনই Sync</button>
      <button class="tbtn" onclick="mongoRestore()">🔄 Restore</button>
    </div>
    <div style="font-size:11px;color:var(--mu);margin-top:10px">💡 Sync করলে সব ফাইল MongoDB তে সেভ হবে। Render restart হলে অটো restore হবে।</div>
  </div>

  <!-- SETTINGS -->
  <div class="set-card">
    <div class="set-title">🔧 Settings</div>
    <div class="set-row"><div><div class="sr-l">Panel নাম</div></div><input class="sinp" type="text" id="sName" placeholder="${pname}"></div>
    <div class="set-row"><div><div class="sr-l">Site URL</div><div class="sr-s">ঘুম বন্ধের জন্য</div></div><input class="sinp" type="text" id="sSiteUrl" placeholder="https://xxx.onrender.com"></div>
    <div class="set-row"><div><div class="sr-l">Auto Restart</div><div class="sr-s">Crash হলে অটো</div></div><label class="tog"><input type="checkbox" id="sAR" onchange="toggleAR(this.checked)"><div class="tog-bg"></div><div class="tog-dot"></div></label></div>
    <div class="set-row"><div><div class="sr-l">Schedule Restart</div><div class="sr-s">প্রতিদিন নির্দিষ্ট সময়ে</div></div><label class="tog"><input type="checkbox" id="sSched"><div class="tog-bg"></div><div class="tog-dot"></div></label></div>
    <div class="set-row"><div><div class="sr-l">Restart সময়</div></div><input class="sinp" type="time" id="sTime" value="03:00"></div>
    <div style="margin-top:12px"><button class="tbtn p" onclick="saveSettings()">💾 সেভ</button></div>
  </div>

  <!-- PASSWORD -->
  <div class="set-card">
    <div class="set-title">🔐 পাসওয়ার্ড পরিবর্তন</div>
    <div class="set-row"><div class="sr-l">বর্তমান</div><input class="sinp" type="password" id="sCur" placeholder="বর্তমান"></div>
    <div class="set-row"><div class="sr-l">নতুন</div><input class="sinp" type="password" id="sNew" placeholder="নতুন (৪+ অক্ষর)"></div>
    <div style="margin-top:12px"><button class="tbtn p" onclick="changePw()">🔐 পরিবর্তন</button></div>
  </div>

  <!-- MAINTENANCE -->
  <div class="set-card">
    <div class="set-title">🛠️ Maintenance</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="tbtn" onclick="doBackup()">💾 Full Backup</button>
      <button class="tbtn d" onclick="clearLogFile()">🗑 Log মুছুন</button>
      <button class="tbtn" onclick="location.href='/logout'">🚪 লগআউট</button>
    </div>
  </div>
</div>

</div>

<!-- TABS -->
<div class="tabs">
  <button class="tab active" onclick="goTab('home',this)"><span class="ti">🏠</span><span class="tl">হোম</span></button>
  <button class="tab" onclick="goTab('monitor',this)"><span class="ti">📊</span><span class="tl">মনিটর</span></button>
  <button class="tab" onclick="goTab('logs',this)"><span class="ti">📋</span><span class="tl">লগ</span></button>
  <button class="tab" onclick="goTab('files',this)"><span class="ti">📁</span><span class="tl">ফাইল</span></button>
  <button class="tab" onclick="goTab('upload',this)"><span class="ti">⬆️</span><span class="tl">আপলোড</span></button>
  <button class="tab" onclick="goTab('more',this)"><span class="ti">⚙️</span><span class="tl">আরো</span></button>
</div>

<!-- MODALS -->
<div class="mbg" id="mod-mkdir"><div class="modal"><h3>📁 নতুন ফোল্ডার</h3><input type="text" id="mkN" placeholder="ফোল্ডারের নাম"><div class="modal-btns"><button class="tbtn" onclick="closeM('mkdir')">বাতিল</button><button class="tbtn p" onclick="doMkdir()">তৈরি</button></div></div></div>
<div class="mbg" id="mod-newfile"><div class="modal"><h3>📄 নতুন ফাইল</h3><input type="text" id="nfN" placeholder="test.js বা commands/mycommand.js"><div class="modal-btns"><button class="tbtn" onclick="closeM('newfile')">বাতিল</button><button class="tbtn p" onclick="doNewFile()">তৈরি</button></div></div></div>
<div class="mbg" id="mod-rename"><div class="modal"><h3>✏️ নাম পরিবর্তন</h3><input type="text" id="rnV" placeholder="নতুন নাম"><div class="modal-btns"><button class="tbtn" onclick="closeM('rename')">বাতিল</button><button class="tbtn p" onclick="doRename()">পরিবর্তন</button></div></div></div>
<div class="mbg" id="mod-copy"><div class="modal"><h3>📋 Copy করুন</h3><input type="text" id="cpTo" placeholder="destination path"><div class="modal-btns"><button class="tbtn" onclick="closeM('copy')">বাতিল</button><button class="tbtn p" onclick="doCopy()">Copy</button></div></div></div>

<div class="tw" id="tw"></div>

<script>
let curDir="",curEdit="",renameFrom="",copyFrom="",logFilter="all",autoScroll=true;
let ws,_botUpSec=0,_botRunning=false;

// TABS
function goTab(id,btn){
  document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));
  btn.classList.add("active");
  document.querySelectorAll(".page").forEach(p=>p.classList.remove("active"));
  document.getElementById("pg-"+id).classList.add("active");
  if(id==="files") loadFiles(curDir);
  if(id==="more"){loadEnv();loadSettings();}
  if(id==="logs") document.getElementById("lbox").scrollTop=document.getElementById("lbox").scrollHeight;
  if(id==="upload"){document.getElementById("uploadDir").textContent=curDir||"root";}
  if(id==="monitor") loadMonitor();
}

// WS
function connectWS(){
  const proto=location.protocol==="https:"?"wss":"ws";
  ws=new WebSocket(proto+"://"+location.host);
  ws.onmessage=e=>{
    const m=JSON.parse(e.data);
    if(m.type==="log") appendLog(m.data);
    if(m.type==="logs"){document.getElementById("lbox").innerHTML="";m.data.forEach(appendLog);}
    if(m.type==="status") updateStatus(m.running);
    if(m.type==="clearLogs") document.getElementById("lbox").innerHTML="";
    if(m.type==="mongo") updateMongo(m.connected);
  };
  ws.onclose=()=>setTimeout(connectWS,3000);
}

function appendLog(e){
  if(logFilter!=="all"&&e.type!==logFilter)return;
  const box=document.getElementById("lbox");
  const d=document.createElement("div");
  const cls={info:"li",success:"ls",error:"lr",warn:"lw"}[e.type||"info"]||"li";
  d.className="le "+cls;d.dataset.t=e.type||"info";
  d.innerHTML='<span class="lt">'+e.time+'</span><span class="lx">'+esc(e.text)+'</span>';
  box.appendChild(d);
  if(autoScroll) box.scrollTop=box.scrollHeight;
}

function esc(t){return String(t).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;")}
function setLF(f,btn){logFilter=f;document.querySelectorAll(".lf").forEach(b=>b.classList.remove("on"));btn.classList.add("on");document.querySelectorAll(".le").forEach(el=>el.style.display=(f==="all"||el.dataset.t===f)?"flex":"none");}
function clearLogs(){fetch("/api/bot/clearlogs",{method:"POST"});}
function clearLogFile(){if(!confirm("Log file মুছবেন?"))return;fetch("/api/bot/clearlogfile",{method:"POST"}).then(r=>r.json()).then(d=>toast(d.ok?"✅ মুছা হয়েছে":"❌ ব্যর্থ",d.ok?"success":"error"));}

function updateStatus(running){
  _botRunning=running;
  if(!running) _botUpSec=0;
  [document.getElementById("sDot"),document.getElementById("tDot")].forEach(d=>{if(d)d.className="dot"+(running?" on":"");});
  const tLogo=document.getElementById("topLogo");if(tLogo)tLogo.className="top-logo"+(running?" live":"");
  const st=document.getElementById("sTxt");if(st)st.textContent=running?"✅ বট চলছে":"🔴 বট বন্ধ";
  const ts=document.getElementById("tStatus");if(ts)ts.textContent=running?"✅ চলছে":"🔴 বন্ধ";
}

function updateMongo(connected){
  const dot=document.getElementById("mongoDot");
  const status=document.getElementById("mongoStatus");
  const badge=document.getElementById("mongoBadge");
  if(dot) dot.className="dot"+(connected?" on":"");
  if(status) status.textContent=connected?"DB ✅":"DB ❌";
  if(badge){badge.textContent=connected?"✅ MongoDB সংযুক্ত":"❌ MongoDB বিচ্ছিন্ন";badge.className="mongo-badge"+(connected?" ok":" err");}
}

function fmtT(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h>0?h+"h "+m+"m":m>0?m+"m "+sc+"s":sc+"s";}
function fsz(b){if(!b||b===0)return"—";if(b<1024)return b+"B";if(b<1048576)return(b/1024).toFixed(1)+"KB";return(b/1048576).toFixed(1)+"MB";}
function fdt(d){
  try{
    const then=new Date(d).getTime(), now=Date.now();
    const diffSec=Math.floor((now-then)/1000);
    if(diffSec<0||isNaN(diffSec)) return new Date(d).toLocaleDateString("bn-BD",{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"});
    if(diffSec<10) return "এইমাত্র";
    if(diffSec<60) return diffSec+" সেকেন্ড আগে";
    const diffMin=Math.floor(diffSec/60);
    if(diffMin<60) return diffMin+" মিনিট আগে";
    const diffHr=Math.floor(diffMin/60);
    if(diffHr<24) return diffHr+" ঘণ্টা আগে";
    const diffDay=Math.floor(diffHr/24);
    if(diffDay===1) return "গতকাল";
    if(diffDay<7) return diffDay+" দিন আগে";
    if(diffDay<30) return Math.floor(diffDay/7)+" সপ্তাহ আগে";
    // পুরনো হলে সরাসরি তারিখ দেখাও
    return new Date(d).toLocaleDateString("bn-BD",{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"});
  }catch{return"";}
}
// প্রতি ৩০ সেকেন্ডে ফাইল লিস্টের সময়গুলো লাইভ রিফ্রেশ করো (GitHub-এর মতো)
// — data-mtime attribute থেকে আবার হিসাব করে টেক্সট আপডেট করে, পুরো লিস্ট আবার লোড করে না
setInterval(()=>{
  document.querySelectorAll("[data-mtime]").forEach(el=>{
    const m=el.getAttribute("data-mtime");
    if(m) el.textContent=fdt(m);
  });
},30000);

// Live uptime counter
setInterval(()=>{if(!_botRunning)return;_botUpSec++;const el=document.getElementById("sUp");if(el)el.textContent="⏱ চলছে: "+fmtT(_botUpSec);},1000);

async function refresh(){
  try{
    const[st,bs]=await Promise.all([fetch("/api/stats").then(r=>r.json()),fetch("/api/bot/status").then(r=>r.json())]);
    document.getElementById("cMem").textContent=st.memMB||"--";
    document.getElementById("cSup").textContent=fmtT(st.serverUptime||0);
    document.getElementById("cFiles").textContent=st.botFiles||0;
    document.getElementById("cStarts").textContent=st.starts||0;
    const cc=document.getElementById("cCrash");if(cc)cc.textContent=st.crashes||0;
    const ct=document.getElementById("cTup");if(ct)ct.textContent=fmtT((st.totalUptime||0)+(bs.uptime||0));
    const cn=document.getElementById("cNode");if(cn)cn.textContent=(st.node||"").replace("v","");
    updateStatus(bs.running);
    updateMongo(st.mongoConnected||false);
    fetch("/api/cookie/status").then(r=>r.json()).then(cs=>{
      const el=document.getElementById("cookieStatus");if(!el)return;
      if(cs.saved){el.style.display="block";el.style.background="rgba(46,213,115,.12)";el.style.color="var(--gr)";el.textContent="✅ Cookie ইতিমধ্যে সেভ করা আছে — নতুন করে না বসালেও চলবে";}
      else{el.style.display="block";el.style.background="rgba(240,82,82,.12)";el.style.color="var(--rd)";el.textContent="⚠️ এখনো কোনো Cookie সেভ নেই";}
    }).catch(()=>{});
    if(bs.running&&bs.uptime>0&&_botUpSec===0) _botUpSec=bs.uptime;
    [document.getElementById("arTog"),document.getElementById("sAR")].forEach(el=>{if(el)el.checked=st.autoRestart||false;});
    const hist=(st.history||[]).slice().reverse().slice(0,8);
    const hl=document.getElementById("histList");
    if(hl)hl.innerHTML=hist.length?hist.map(h=>'<div class="hi"><span class="hi-date">'+new Date(h.date).toLocaleString("bn-BD").substring(0,16)+'</span><span class="hi-up">'+fmtT(h.uptime)+'</span><span class="hi-code">'+h.code+'</span></div>').join(""):'<div style="font-size:12px;color:var(--mu);text-align:center;padding:10px">ইতিহাস নেই</div>';
    const pgMon=document.getElementById("pg-monitor");
    if(pgMon&&pgMon.classList.contains("active")) loadMonitor();
  }catch{}
}

// LIVE MONITOR
function _mColor(pct){ return pct<60?"var(--gr)":pct<85?"var(--yw)":"var(--rd)"; }
async function loadMonitor(){
  try{
    const d=await fetch("/api/system/live").then(r=>r.json());
    const cap=(d.ram&&d.ram.capMB)||512;

    const pMB=d.ram&&d.ram.panelMB!=null?d.ram.panelMB:null;
    const bMB=d.ram&&d.ram.botMB!=null?d.ram.botMB:null;
    const tMB=(pMB||0)+(bMB||0);

    if(pMB!=null){const p=Math.min(100,Math.round(pMB/cap*100));document.getElementById("mPanelTxt").textContent=pMB+" MB / "+cap+" MB";document.getElementById("mPanelBar").style.width=p+"%";document.getElementById("mPanelBar").style.background=_mColor(p);}
    if(bMB!=null){const p=Math.min(100,Math.round(bMB/cap*100));document.getElementById("mBotTxt").textContent=bMB+" MB / "+cap+" MB";document.getElementById("mBotBar").style.width=p+"%";document.getElementById("mBotBar").style.background=_mColor(p);}
    else {document.getElementById("mBotTxt").textContent="বট বন্ধ আছে";document.getElementById("mBotBar").style.width="0%";}
    {const p=Math.min(100,Math.round(tMB/cap*100));document.getElementById("mTotalTxt").textContent=tMB+" MB / "+cap+" MB";document.getElementById("mTotalBar").style.width=p+"%";document.getElementById("mTotalBar").style.background=_mColor(p);}

    const hr=document.getElementById("mHeavyRow");
    if(d.heavy){
      hr.style.display="block";
      const p=Math.min(100,Math.round((d.heavy.active/d.heavy.max)*100));
      document.getElementById("mHeavyTxt").textContent=d.heavy.active+" / "+d.heavy.max+" চলছে";
      document.getElementById("mHeavyBar").style.width=p+"%";
    } else { hr.style.display="none"; }

    const rr=document.getElementById("mRenderRow"),rb=document.getElementById("mRenderBadge"),rt=document.getElementById("mRenderTxt"),rn=document.getElementById("mRenderNote");
    if(d.render&&d.render.configured){
      rr.style.display="block";
      if(d.render.ok){rb.textContent="লাইভ";rb.className="mon-badge ok";rt.textContent="Render API থেকে ডেটা এসেছে (নিচে raw দেখুন প্রয়োজনে)";rn.style.display="none";}
      else{rb.textContent="ব্যর্থ";rb.className="mon-badge err";rt.textContent=d.render.error||"API কল ব্যর্থ হয়েছে";}
    } else { rr.style.display="none"; }

    if(d.mongo&&!d.mongo.error){
      const mCap=512; // Atlas M0 ফ্রি টায়ারের সীমা
      const used=d.mongo.totalMB||0;
      const p=Math.min(100,Math.round(used/mCap*100));
      document.getElementById("mMongoTxt").textContent=used+" MB / "+mCap+" MB";
      document.getElementById("mMongoBar").style.width=p+"%";
      document.getElementById("mMongoBar").style.background=_mColor(p);
      document.getElementById("mMongoObjs").textContent=d.mongo.objects!=null?d.mongo.objects:"--";
      document.getElementById("mMongoData").textContent=d.mongo.dataSizeMB!=null?d.mongo.dataSizeMB:"--";
      document.getElementById("mMongoIdx").textContent=d.mongo.indexSizeMB!=null?d.mongo.indexSizeMB:"--";
    } else {
      document.getElementById("mMongoTxt").textContent="সংযুক্ত নেই";
      document.getElementById("mMongoBar").style.width="0%";
    }

    // লাইফটাইম পিক + সামারি (MongoDB থেকে, প্যানেল/বট যতবার restart হোক না কেন হারায় না)
    if(d.lifetime){
      const lt=d.lifetime;
      document.getElementById("mPanelPeak").textContent=(lt.peakPanelMB||0)+" MB";
      document.getElementById("mBotPeak").textContent=(lt.peakBotMB||0)+" MB";
      document.getElementById("mMongoPeak").textContent=(lt.peakMongoMB||0)+" MB";
      document.getElementById("mLtStarts").textContent=(lt.totalStarts||0)+" বার";
      document.getElementById("mLtCrashes").textContent=(lt.totalCrashes||0)+" বার";
      document.getElementById("mLtUptime").textContent=_fmtLifeUp(lt.totalUptimeSec||0);
      document.getElementById("mLtSince").textContent=lt.firstSeen?new Date(lt.firstSeen).toLocaleDateString("bn-BD",{year:"numeric",month:"long",day:"numeric"}):"--";
    }
  }catch(e){}
}
function _fmtLifeUp(sec){
  const d=Math.floor(sec/86400),h=Math.floor((sec%86400)/3600),m=Math.floor((sec%3600)/60);
  if(d>0) return d+"দিন "+h+"ঘণ্টা";
  if(h>0) return h+"ঘণ্টা "+m+"মিনিট";
  return m+"মিনিট";
}

// BOT
async function botAct(a){
  toast("⏳ "+{start:"চালু",stop:"বন্ধ",restart:"রিস্টার্ট"}[a]+"...","warn");
  const d=await fetch("/api/bot/"+a,{method:"POST"}).then(r=>r.json());
  toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");
  setTimeout(refresh,2500);
}
async function npmInst(){toast("📦 npm install শুরু...","warn");const d=await fetch("/api/bot/install",{method:"POST"}).then(r=>r.json());toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");}
function doBackup(){window.open("/api/backup");}
async function toggleAR(v){
  await fetch("/api/bot/autorestart",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enabled:v})});
  toast(v?"✅ Auto Restart চালু":"⚠️ বন্ধ",v?"success":"warn");
  [document.getElementById("arTog"),document.getElementById("sAR")].forEach(el=>{if(el)el.checked=v;});
}

// COOKIE
async function saveCookie(){
  const c=document.getElementById("cookieInput").value.trim();
  if(!c)return toast("❌ Cookie লিখুন","error");
  const d=await fetch("/api/cookie/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({cookie:c})}).then(r=>r.json());
  toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");
  if(d.ok){document.getElementById("cookieInput").value="";toast("🔄 বট চালু হচ্ছে...","warn");setTimeout(()=>botAct("start"),1500);}
}

// MONGODB
async function mongoSync(){toast("☁️ Sync শুরু হচ্ছে...","warn");const d=await fetch("/api/mongo/sync",{method:"POST"}).then(r=>r.json());toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");}
async function mongoRestore(){toast("🔄 Restore শুরু...","warn");const d=await fetch("/api/mongo/restore",{method:"POST"}).then(r=>r.json());toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");if(d.ok)setTimeout(()=>loadFiles(curDir),2000);}

// FILE ICONS
function ficon(name,isDir){
  if(isDir)return"📁";
  const e=name.split(".").pop().toLowerCase();
  return{js:"📜",mjs:"📜",cjs:"📜",json:"📋",md:"📝",txt:"📄",env:"🔐",log:"📋",jpg:"🖼",jpeg:"🖼",png:"🖼",gif:"🖼",webp:"🖼",mp3:"🎵",mp4:"🎬",zip:"📦",tar:"📦",gz:"📦",html:"🌐",css:"🎨",ts:"📘",py:"🐍",sh:"⚡",bat:"⚡",yml:"⚙️",yaml:"⚙️",xml:"📋",lock:"🔒",gitignore:"👁️",npmrc:"⚙️",babelrc:"⚙️"}[e]||"📄";
}
function langExt(n){const e=n.split(".").pop().toLowerCase();return{js:"JavaScript",json:"JSON",md:"Markdown",html:"HTML",css:"CSS",py:"Python",ts:"TypeScript",sh:"Shell",env:"ENV",txt:"Text",yml:"YAML",xml:"XML"}[e]||e.toUpperCase();}

function buildPath(dir){
  const bar=document.getElementById("pathBar");
  const parts=dir?dir.split("/"):[];
  let html='<span class="pp" onclick="loadFiles(\\'\\')">📁 root</span>';
  let acc="";
  parts.forEach(p=>{acc+=(acc?"/":"")+p;const c=acc;html+='<span style="color:var(--mu)"> / </span><span class="pp" onclick="loadFiles(\\''+c+'\\')">'+p+'</span>';});
  bar.innerHTML=html;
}

async function loadFiles(dir){
  curDir=dir||"";buildPath(curDir);
  document.getElementById("fmView").style.display="block";
  document.getElementById("edView").style.display="none";
  document.getElementById("fsRes").style.display="none";
  document.getElementById("fq").value="";
  document.getElementById("uploadDir").textContent=curDir||"root";
  const data=await fetch("/api/files?path="+encodeURIComponent(curDir)).then(r=>r.json());
  const list=document.getElementById("flist");list.innerHTML="";
  if(curDir){
    const up=document.createElement("div");up.className="frow";
    up.innerHTML='<span class="fi">⬆️</span><div class="fn"><div class="fn-name">.. উপরে যান</div></div>';
    up.onclick=()=>loadFiles(curDir.split("/").slice(0,-1).join("/"));
    list.appendChild(up);
  }
  if(!data.items?.length){list.innerHTML='<div class="empty-fm"><div style="font-size:40px;margin-bottom:8px">📭</div><div>ফোল্ডার খালি</div></div>';return;}
  data.items.forEach(item=>{
    const fp=curDir?curDir+"/"+item.name:item.name;
    const row=document.createElement("div");row.className="frow";
    row.innerHTML='<span class="fi">'+ficon(item.name,item.isDir)+'</span>'
      +'<div class="fn"><div class="fn-name">'+item.name+'</div><div class="fn-meta">'+fsz(item.size)+(item.mtime?' · <span data-mtime="'+item.mtime+'">'+fdt(item.mtime)+'</span>':"")+'</div></div>'
      +'<div class="fa">'
      +(item.isDir?'':'<button class="fab" onclick="event.stopPropagation();editF(\\''+fp+'\\')">✏️</button>')
      +'<button class="fab" onclick="event.stopPropagation();dlF(\\''+fp+'\\')">⬇️</button>'
      +'<button class="fab" onclick="event.stopPropagation();showRename(\\''+fp+'\\',\\''+item.name+'\\')">🔤</button>'
      +'<button class="fab" onclick="event.stopPropagation();showCopy(\\''+fp+'\\')">📋</button>'
      +'<button class="fab del" onclick="event.stopPropagation();delItem(\\''+fp+'\\',\\''+item.name+'\\')">🗑</button>'
      +'</div>';
    if(item.isDir) row.onclick=()=>loadFiles(fp);
    else row.onclick=()=>editF(fp);
    list.appendChild(row);
  });
}

async function editF(p){
  const d=await fetch("/api/file/read?path="+encodeURIComponent(p)).then(r=>r.json());
  if(d.error)return toast("❌ "+d.error,"error");
  curEdit=p;
  document.getElementById("edFn").textContent=p.split("/").pop();
  document.getElementById("edLang").textContent=langExt(p);
  document.getElementById("ced").value=d.content;
  document.getElementById("fmView").style.display="none";
  document.getElementById("edView").style.display="block";
}
function closeEd(){document.getElementById("edView").style.display="none";document.getElementById("fmView").style.display="block";}
async function saveFile(){
  const d=await fetch("/api/file/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:curEdit,content:document.getElementById("ced").value})}).then(r=>r.json());
  toast(d.ok?"✅ সেভ + MongoDB আপডেট":"❌ "+d.error,d.ok?"success":"error");
}
function dlF(p){window.open("/api/file/download?path="+encodeURIComponent(p));}
async function delItem(p,name){
  if(!confirm('"'+name+'" ডিলিট করবেন? MongoDB থেকেও মুছবে।'))return;
  const d=await fetch("/api/file/delete",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:p})}).then(r=>r.json());
  toast(d.ok?"🗑 ডিলিট হয়েছে":"❌ "+d.error,d.ok?"success":"error");if(d.ok)loadFiles(curDir);
}

// MODALS
document.querySelectorAll(".mbg").forEach(bg=>bg.addEventListener("click",e=>{if(e.target===bg)bg.classList.remove("open");}));
function showM(id){document.getElementById("mod-"+id).classList.add("open");setTimeout(()=>document.querySelector("#mod-"+id+" input")?.focus(),100);}
function closeM(id){document.getElementById("mod-"+id).classList.remove("open");}

async function doMkdir(){
  const n=document.getElementById("mkN").value.trim();if(!n)return;
  const fp=curDir?curDir+"/"+n:n;
  const d=await fetch("/api/file/mkdir",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:fp})}).then(r=>r.json());
  closeM("mkdir");toast(d.ok?"📁 তৈরি হয়েছে":"❌ "+d.error,d.ok?"success":"error");if(d.ok)loadFiles(curDir);
}

async function doNewFile(){
  const n=document.getElementById("nfN").value.trim();if(!n)return;
  const fp=curDir?curDir+"/"+n:n;
  const d=await fetch("/api/file/newfile",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:fp,content:""})}).then(r=>r.json());
  closeM("newfile");if(d.ok){toast("📄 তৈরি + MongoDB সেভ","success");editF(fp);}else toast("❌ "+d.error,"error");
}

function showRename(p,name){renameFrom=p;document.getElementById("rnV").value=name;showM("rename");}
async function doRename(){
  const n=document.getElementById("rnV").value.trim();if(!n)return;
  const dir=renameFrom.includes("/")?renameFrom.split("/").slice(0,-1).join("/"):"";
  const to=dir?dir+"/"+n:n;
  const d=await fetch("/api/file/rename",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({from:renameFrom,to})}).then(r=>r.json());
  closeM("rename");toast(d.ok?"✅ নাম পরিবর্তন":"❌ "+d.error,d.ok?"success":"error");if(d.ok)loadFiles(curDir);
}

function showCopy(p){copyFrom=p;document.getElementById("cpTo").value=p+"_copy";showM("copy");}
async function doCopy(){
  const to=document.getElementById("cpTo").value.trim();if(!to)return;
  const d=await fetch("/api/file/copy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({from:copyFrom,to})}).then(r=>r.json());
  closeM("copy");toast(d.ok?"📋 Copy হয়েছে":"❌ "+d.error,d.ok?"success":"error");if(d.ok)loadFiles(curDir);
}

// FILE SEARCH
let fst;
function doFS(){
  const q=document.getElementById("fq").value.trim();
  const res=document.getElementById("fsRes");
  if(!q){res.style.display="none";return;}
  clearTimeout(fst);fst=setTimeout(async()=>{
    const d=await fetch("/api/file/search?q="+encodeURIComponent(q)).then(r=>r.json());
    if(!d.results?.length){res.style.display="block";res.innerHTML='<div style="font-size:12px;color:var(--mu);padding:10px;text-align:center">📭 পাওয়া যায়নি</div>';return;}
    res.style.display="block";
    res.innerHTML=d.results.map(r=>'<div class="srow" onclick="'+(r.isDir?"loadFiles('"+r.path+"')":"editF('"+r.path+"')")+'"><div class="srow-p">'+ficon(r.name,r.isDir)+" "+r.path+'</div><div class="srow-m">'+fsz(r.size)+'</div></div>').join("");
  },300);
}

// UPLOAD — চাংক করে পাঠানো হয় (ধীর/অস্থির নেটওয়ার্কে নির্ভরযোগ্য), প্রতিটা চাংক fail করলে শুধু সেটাই রিট্রাই হয়
async function uploadF(file){
  if(!file)return;
  const pw=document.getElementById("progWrap"),pb=document.getElementById("progBar"),pp=document.getElementById("upPct"),ps=document.getElementById("upSt"),fn=document.getElementById("upFN");
  pw.style.display="block";fn.textContent=file.name;pb.style.width="0%";pp.textContent="0%";ps.textContent="চেক করা হচ্ছে (আগের অসম্পূর্ণ আপলোড আছে কিনা)...";
  const CHUNK_SIZE=50*1024; // 50KB — ধীর নেটওয়ার্কের জন্য ছোট রাখা হয়েছে
  const totalChunks=Math.max(1,Math.ceil(file.size/CHUNK_SIZE));
  // ফাইলের নাম+সাইজ+lastModified থেকে স্থায়ী ID — একই ফাইল আবার সিলেক্ট করলে আগের progress থেকে resume হবে, শুরু থেকে না
  const uploadId="f"+file.size+"_"+(file.lastModified||0)+"_"+file.name.replace(/[^a-zA-Z0-9]/g,"").slice(0,40);
  const already=new Set();
  try{
    const st=await fetch("/api/file/upload-status?uploadId="+encodeURIComponent(uploadId)).then(r=>r.json());
    (st.have||[]).forEach(i=>already.add(i));
  }catch(e){}
  if(already.size>0) ps.textContent="আগের আপলোড থেকে resume হচ্ছে ("+already.size+"/"+totalChunks+" আগে থেকেই আছে)...";

  async function sendChunk(i){
    const start=i*CHUNK_SIZE,end=Math.min(file.size,start+CHUNK_SIZE);
    const blob=file.slice(start,end);
    let attempt=0;
    while(attempt<8){
      try{
        const fd=new FormData();
        fd.append("chunk",blob,"chunk");
        fd.append("uploadId",uploadId);
        fd.append("chunkIndex",i);
        fd.append("totalChunks",totalChunks);
        fd.append("fileName",file.name);
        fd.append("path",curDir||"");
        const d=await fetch("/api/file/upload-chunk",{method:"POST",body:fd}).then(r=>r.json());
        if(d && d.ok!==false)return d;
        throw new Error(d.msg||"চাংক ব্যর্থ");
      }catch(e){
        attempt++;
        ps.innerHTML='<span style="color:#f5a623">⚠️ চাংক '+(i+1)+'/'+totalChunks+' রিট্রাই ('+attempt+'/8)...</span>';
        await new Promise(r=>setTimeout(r,Math.min(1000*attempt,8000)));
      }
    }
    return null;
  }

  const toSend=[];
  for(let i=0;i<totalChunks;i++) if(!already.has(i)) toSend.push(i);
  let doneCount=already.size,lastResult=null,failed=false;
  function upd(){const p=Math.round((doneCount/totalChunks)*100);pb.style.width=p+"%";pp.textContent=p+"%";}
  upd();

  for(const i of toSend){
    const d=await sendChunk(i);
    if(!d){
      failed=true;
      ps.innerHTML='<span style="color:var(--rd)">❌ চাংক '+(i+1)+' বারবার ব্যর্থ — নেটওয়ার্ক ঠিক হলে একই ফাইল আবার সিলেক্ট করলে যেখানে থেমেছিল সেখান থেকে চালিয়ে যাবে</span>';
      toast("❌ আপলোড থেমে গেছে — নেটওয়ার্ক ঠিক হলে আবার চেষ্টা করো","error");
      break;
    }
    lastResult=d;doneCount++;upd();
    ps.textContent="চাংক "+doneCount+"/"+totalChunks+" পাঠানো হয়েছে...";
  }
  if(failed){document.getElementById("fInp").value="";return;}

  // সব চাংক আগে থেকেই ছিল (নতুন কিছু পাঠানো লাগেনি) হলে ফাইনালাইজ করার জন্য শেষ চাংকটা আবার পাঠাও
  if(!lastResult||!lastResult.done) lastResult=await sendChunk(totalChunks-1);

  if(lastResult&&lastResult.done&&lastResult.processing){
    // extract+সেভ ব্যাকগ্রাউন্ডে চলছে — শেষ না হওয়া পর্যন্ত পোল করো (রিকোয়েস্ট টাইমআউট এড়াতে)
    ps.innerHTML='<span style="color:#f5a623">⏳ '+(lastResult.msg||"প্রসেস হচ্ছে...")+'</span>';
    let final=null;
    for(let tries=0;tries<150;tries++){ // সর্বোচ্চ ~৭.৫ মিনিট (150 x 3s)
      await new Promise(r=>setTimeout(r,3000));
      try{
        const st=await fetch("/api/file/upload-result?uploadId="+encodeURIComponent(uploadId)).then(r=>r.json());
        if(st.status==="done"){final=st;break;}
      }catch(e){}
    }
    if(final){
      if(final.ok){ps.innerHTML='<span style="color:var(--gr)">✅ '+(final.msg||"সম্পন্ন")+'</span>';toast("✅ "+(final.msg||"সম্পন্ন"),"success");}
      else{ps.innerHTML='<span style="color:var(--rd)">❌ '+(final.msg||"ব্যর্থ")+'</span>';toast("❌ "+(final.msg||"ব্যর্থ"),"error");}
    }else{
      ps.innerHTML='<span style="color:var(--rd)">⚠️ অনেকক্ষণ হয়ে গেছে, ফলাফল জানা যায়নি — "লগ" বা "ফাইল" ট্যাবে গিয়ে সরাসরি চেক করো</span>';
    }
  } else if(lastResult&&lastResult.done){
    if(lastResult.ok){ps.innerHTML='<span style="color:var(--gr)">✅ '+(lastResult.msg||"সম্পন্ন")+'</span>';toast("✅ "+(lastResult.msg||"সম্পন্ন"),"success");}
    else{ps.innerHTML='<span style="color:var(--rd)">❌ '+(lastResult.msg||"ব্যর্থ")+'</span>';toast("❌ "+(lastResult.msg||"ব্যর্থ"),"error");}
  }
  document.getElementById("fInp").value="";
}

async function uploadSingle(file){
  if(!file)return;
  const st=document.getElementById("singleStatus");
  st.textContent="⏳ আপলোড হচ্ছে...";
  const fd=new FormData();fd.append("file",file);fd.append("path",curDir||"");
  const d=await fetch("/api/file/upload",{method:"POST",body:fd}).then(r=>r.json());
  st.innerHTML=d.ok?'<span style="color:var(--gr)">✅ '+d.msg+'</span>':'<span style="color:var(--rd)">❌ '+d.error+'</span>';
  toast(d.ok?"✅ "+d.msg:"❌ "+d.error,d.ok?"success":"error");
  document.getElementById("singleInp").value="";
}

async function uploadMulti(files){
  if(!files||!files.length)return;
  const st=document.getElementById("multiStatus");
  st.textContent="⏳ "+files.length+"টা ফাইল আপলোড হচ্ছে...";
  const fd=new FormData();
  for(const f of files) fd.append("files",f);
  fd.append("path",curDir||"");
  const d=await fetch("/api/file/upload-multi",{method:"POST",body:fd}).then(r=>r.json());
  st.innerHTML=d.ok?'<span style="color:var(--gr)">✅ '+d.msg+'</span>':'<span style="color:var(--rd)">❌ '+d.error+'</span>';
  toast(d.ok?"✅ "+d.msg:"❌ "+d.error,d.ok?"success":"error");
  document.getElementById("multiInp").value="";
}

// Drag & Drop on upload zone
const uz=document.getElementById("upZone");
uz.addEventListener("dragover",e=>{e.preventDefault();uz.classList.add("drag");});
uz.addEventListener("dragleave",()=>uz.classList.remove("drag"));
uz.addEventListener("drop",e=>{e.preventDefault();uz.classList.remove("drag");uploadF(e.dataTransfer.files[0]);});

// ENV
async function loadEnv(){const d=await fetch("/api/env").then(r=>r.json());document.getElementById("envEd").value=d.content||"";}
async function saveEnv(){const d=await fetch("/api/env/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({content:document.getElementById("envEd").value})}).then(r=>r.json());toast(d.ok?"✅ .env সেভ + MongoDB":"❌ "+d.msg,d.ok?"success":"error");}

// SETTINGS
async function loadSettings(){
  const d=await fetch("/api/settings").then(r=>r.json());
  document.getElementById("sName").value=d.panelName||"";
  document.getElementById("sSiteUrl").value=d.siteUrl||location.origin;
  document.getElementById("sAR").checked=d.autoRestart||false;
  document.getElementById("sSched").checked=d.scheduleRestart||false;
  document.getElementById("sTime").value=d.scheduleTime||"03:00";
  const mi=document.getElementById("mongoInfo");
  if(mi) mi.textContent=d.mongoConnected?"MongoDB সংযুক্ত ✅":"MongoDB বিচ্ছিন্ন ❌";
}
async function saveSettings(){
  const body={panelName:document.getElementById("sName").value,siteUrl:document.getElementById("sSiteUrl").value,autoRestart:document.getElementById("sAR").checked,scheduleRestart:document.getElementById("sSched").checked,scheduleTime:document.getElementById("sTime").value};
  const d=await fetch("/api/settings/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}).then(r=>r.json());
  toast(d.ok?"✅ সেভ হয়েছে":"❌ ব্যর্থ",d.ok?"success":"error");
}
async function changePw(){
  const d=await fetch("/api/settings/password",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({current:document.getElementById("sCur").value,newPass:document.getElementById("sNew").value})}).then(r=>r.json());
  toast(d.ok?"✅ "+d.msg:"❌ "+d.msg,d.ok?"success":"error");
  if(d.ok){document.getElementById("sCur").value="";document.getElementById("sNew").value="";}
}

// TOAST
function toast(msg,type="success"){
  const w=document.getElementById("tw"),el=document.createElement("div");
  const icons={success:"✅",error:"❌",warn:"⚠️"};
  el.className="toast "+type;
  el.innerHTML='<span>'+(icons[type]||"ℹ️")+'</span><span>'+msg+'</span><div class="toast-bar"></div>';
  w.appendChild(el);
  setTimeout(()=>{el.style.opacity="0";el.style.transition=".3s";setTimeout(()=>el.remove(),300);},4000);
}

// KEYBOARD
document.addEventListener("keydown",e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==="s"&&curEdit){e.preventDefault();saveFile();}
  if(e.key==="Escape") document.querySelectorAll(".mbg.open").forEach(m=>m.classList.remove("open"));
});

// INIT
connectWS();
refresh();
setInterval(refresh,10000);
</script>
</body></html>`;}
