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
const { spawn, fork, execSync, spawnSync } = require("child_process");
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
const AFILE = path.join(__dirname, "alerts.json"); // ইন-প্যানেল লাইফটাইম অ্যালার্ট/নোটিফিকেশন হিস্ট্রি — MongoDB-তেও ব্যাকআপ থাকে
const PORT  = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGODB_URI || "mongodb+srv://belal:belal123456@cluster0.i1wofni.mongodb.net/botpanel?appName=Cluster0";

function loadJ(f,def={}){try{return JSON.parse(fs.readFileSync(f,"utf8"));}catch{return def;}}
function saveJ(f,d){try{fs.writeFileSync(f,JSON.stringify(d,null,2));}catch{}}

let cfg   = loadJ(CFG);
if(!cfg.authToken){ cfg.authToken = require("crypto").randomBytes(24).toString("hex"); saveJ(CFG,cfg); }
function getCookies(req){
  const out={}; const h=req.headers.cookie;
  if(h) h.split(";").forEach(p=>{ const i=p.indexOf("="); if(i>0) out[p.slice(0,i).trim()]=decodeURIComponent(p.slice(i+1).trim()); });
  return out;
}
let stats = loadJ(SFILE,{starts:0,crashes:0,totalUptime:0,history:[],loginAttempts:{}});
let lifetime = loadJ(LTFILE,{peakPanelMB:0,peakBotMB:0,peakMongoMB:0,firstSeen:new Date().toISOString()});
const PASS = process.env.PANEL_PASSWORD || cfg.password || "admin123";
if(!fs.existsSync(BDIR)) fs.mkdirSync(BDIR,{recursive:true});

// ── ইন-প্যানেল লাইফটাইম অ্যালার্ট সিস্টেম (ফোনে পুশ নোটিফিকেশনের বদলে — সবকিছু ওয়েবসাইটের ভিতরেই) ──
// প্রতিটা গুরুত্বপূর্ণ ঘটনা (ক্র্যাশ, প্রতিরোধমূলক রিস্টার্ট, স্টোরেজ সতর্কতা ইত্যাদি) এখানে জমা থাকে,
// MongoDB-তে ব্যাকআপসহ — তাই প্যানেল যতবারই restart হোক, অ্যালার্ট হিস্ট্রি হারায় না
let alerts = loadJ(AFILE, []);
let _alertCooldowns = {};
function notify(level, title, message, {cooldownKey=null, cooldownMs=0} = {}){
  if(cooldownKey){
    const last=_alertCooldowns[cooldownKey]||0;
    if(Date.now()-last < cooldownMs) return; // এখনো কুলডাউনে, স্কিপ
    _alertCooldowns[cooldownKey]=Date.now();
  }
  const entry = { id: Date.now()+"-"+Math.random().toString(36).slice(2,7), time: new Date().toISOString(), level, title, message, read:false };
  alerts.push(entry);
  if(alerts.length>500) alerts.shift(); // সাম্প্রতিক ৫০০টা যথেষ্ট, তার বেশি দরকার নেই
  saveJ(AFILE, alerts);
  saveAlertsToMongo(); // fire-and-forget ব্যাকআপ
  bc({type:"alert", data: entry});
}
async function saveAlertsToMongo(){ await saveToMongo("__panel_alerts__", JSON.stringify(alerts), false); }
async function restoreAlertsFromMongo(){
  if(!db_connected || !FileModel) return;
  try{
    const a = await FileModel.findOne({path:"__panel_alerts__"});
    if(a && a.content){ try{ alerts = JSON.parse(a.content.toString()); saveJ(AFILE, alerts); }catch{} }
  }catch(e){ console.log("⚠️ alerts restore error:", e.message); }
}

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

    // restore files from MongoDB on startup
    await restorePanelPersistent();
    await restoreAlertsFromMongo();
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

// প্যানেল থেকে ফাইল অ্যাড/এডিট/ডিলিট হলে সাথে সাথে (fs.watch-এর অপেক্ষা না করে) বটকে সরাসরি IPC দিয়ে জানানো —
// fs.watch কিছু কন্টেইনার ফাইলসিস্টেমে অনির্ভরযোগ্য/দেরিতে ফায়ার করতে পারে, IPC সবসময় তাৎক্ষণিক ও নির্ভরযোগ্য
function notifyBotFile(action, relPath){
  if(!botProc || !botProc.connected) return;
  try{ botProc.send({ type: "panel_file_change", action, relPath }); }catch{}
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

const auth = (req,res,next) => {
  const okSession = req.session && req.session.ok;
  const okToken = getCookies(req).authToken === cfg.authToken;
  if(okSession || okToken) return next();
  if(req.path.startsWith("/api/")) return res.status(401).json({error:"session expired — আবার লগইন করো"});
  res.redirect("/login");
};
const safe = (base,rel) => { const f=path.resolve(base,rel||""); if(!f.startsWith(path.resolve(base))) throw new Error("Access denied"); return f; };

// ── BOT ──
let botProc=null, botLogs=[], botStart=null, botReady=false, autoRestart=true, rsTimer=null, _consecutiveCrashes=0, _installInProgress=false; // ── Auto-Restart সবসময় ON থাকবে (ইউজারের সিদ্ধান্ত অনুযায়ী) — বন্ধ করার অপশন সরিয়ে দেওয়া হয়েছে

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
  if(_installInProgress) return {ok:false,msg:"📦 npm install ইতিমধ্যে ব্যাকগ্রাউন্ডে চলছে — শেষ হওয়া পর্যন্ত অপেক্ষা করো"};
  const idx=["index.js","app.js","main.js","bot.js","start.js"].find(f=>fs.existsSync(path.join(BDIR,f)));
  if(!idx) return {ok:false,msg:"index.js পাওয়া যায়নি — বট আপলোড করুন"};
  const nmDir=path.join(BDIR,"node_modules");

  function actuallySpawnBot(){
    botProc=fork(idx,[],{cwd:BDIR,env:{...process.env,FORCE_COLOR:"1"},stdio:["ignore","pipe","pipe","ipc"]});
    botStart=Date.now(); botReady=false; stats.starts++; saveJ(SFILE,stats); savePanelStatsToMongo();
    setShouldRun(true);
    log(`🟡 বট চালু হচ্ছে (${by}) — ${idx}`,"warn"); bc({type:"status",running:false,starting:true});
    const NOISY=[
      /Warning: Accessing non-existent property/i,/circular dependency/i,/--trace-warnings/i,/\[DEP\d+\]/i,/is deprecated\. Please use/i,
      /node_modules[\\/](mqtt|fca-unofficial|bluebird)[\\/]/i,     // fca-unofficial/mqtt-এর নিজস্ব internal stack trace লাইন
      /at (MqttClient|Writable|Duplexify|Socket|TLSSocket|writeOrBuffer|doWrite|addChunk)/i, // mqtt লাইব্রেরির internal কল-স্ট্যাক
      /not part of the conversation \d+/i,                          // bot যে গ্রুপে নেই, সেখানে পুরনো মেসেজ পাঠানোর normal ব্যর্থতা
      /Cannot get MQTT region/i,                                    // fca-unofficial-এর পরিচিত, প্রভাবহীন warning
      /ScreenTime and Badge telemetry/i,                            // fca-unofficial-এর normal telemetry নোটিশ
      /Unrecognized option given to setOptions/i,                   // fca-unofficial-এর পুরনো config warning, ক্ষতিকর না
      /unsendMessage.*(isNotCritical|rid:|payload:|lid:)/i          // পুরনো মেসেজ unsend করতে ব্যর্থ হওয়ার normal detail, গুরুত্বপূর্ণ না
    ];
    const isNoisy=s=>NOISY.some(rx=>rx.test(s));
    // eslint-disable-next-line no-control-regex
    const stripAnsi=s=>s.replace(/\x1B\[[0-9;]*[a-zA-Z]/g,"");
    const classify=(s)=>{
      if(/\[ক্রটি\]|ERR!|❌|Unhandled Rejection|uncaughtException/i.test(s)) return "error";
      if(/\[সতর্ক\]|⚠️|WARN\b/i.test(s)) return "warn";
      if(/\[সফল\]|✅|সফলভাবে/i.test(s)) return "success";
      return "info";
    };
    const cleanPrefix=(s)=> s.replace(/^\s*[⚠️❌✅]*\s*\[(সতর্ক|ক্রটি|সফল|তথ্য)\]\s*»\s*/u,"").replace(/^\s*[⚠️❌✅ℹ️]+\s*/u,"").trim();
    botProc.stdout.on("data",d=>{const s=stripAnsi(d.toString()).trim();if(s&&!isNoisy(s))log(cleanPrefix(s),classify(s));});
    botProc.stderr.on("data",d=>{const s=stripAnsi(d.toString()).trim();if(s&&!isNoisy(s))log(cleanPrefix(s),"error");});
    botProc.on("message",(msg)=>{
      if(msg?.type==="bot_ready"){
        botReady=true;
        log(`✅ বট সম্পূর্ণ প্রস্তুত — ${msg.commands||0} কমান্ড লোড হয়েছে (${msg.failed||0} ব্যর্থ)`,"success");
        bc({type:"status",running:true,starting:false,ready:true});
      }
    });
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
        notify("error", "🔴 বট ক্র্যাশ করেছে!", `কোড: ${code||sig} | আগের সেশন সচল ছিল: ${fmtS(up)} | Auto-restart চেষ্টা চলছে...`);
      }
      saveJ(SFILE,stats); savePanelStatsToMongo();
      log(`🔴 বট বন্ধ (code:${code||sig}, uptime:${fmtS(up)})`,"error");
      botProc=null; botStart=null; botReady=false; bc({type:"status",running:false,starting:false,ready:false});
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
  }

  if(!fs.existsSync(nmDir)){
    // ⚠️ আগে এখানে execSync ব্যবহার হতো, যেটা npm install শেষ না হওয়া পর্যন্ত
    // পুরো ওয়েবসাইটকেই (Express সার্ভার) ফ্রিজ করে রাখত — এখন async spawn,
    // তাই ওয়েবসাইট npm install চলাকালীনও স্বাভাবিকভাবে খোলা/ব্যবহার করা যাবে
    _installInProgress = true;
    const ramBefore = Math.round(process.memoryUsage().rss/1024/1024);
    log(`📦 npm install শুরু — ব্যাকগ্রাউন্ডে (এই মুহূর্তে RAM: ${ramBefore}MB)`,"warn");
    bc({type:"status",running:false,installing:true});
    const npmProc = spawn(process.platform==="win32"?"npm.cmd":"npm", ["install","--omit=dev","--no-audit","--no-fund","--prefer-offline"], {cwd:BDIR});
    let npmErr="", npmBuf="";
    const flushLine=(s)=>{
      npmBuf += s;
      let i;
      while((i=npmBuf.indexOf("\n"))>=0){
        const line=npmBuf.slice(0,i).trim(); npmBuf=npmBuf.slice(i+1);
        if(line) log("📦 "+line,"info");
      }
    };
    npmProc.stdout.on("data",d=>flushLine(d.toString()));
    npmProc.stderr.on("data",d=>{const s=d.toString();npmErr+=s;flushLine(s);});
    // ── নিরাপত্তা: npm install যদি কোনো কারণে ৫ মিনিটেও শেষ না হয় (ঝুলে যায়),
    // জোর করে বন্ধ করে lock ছেড়ে দেওয়া হবে — যাতে সিস্টেম চিরস্থায়ীভাবে আটকে না থাকে
    const hangGuard = setTimeout(()=>{
      log("⚠️ npm install ৫ মিনিটেও শেষ হয়নি — জোর করে বন্ধ করা হলো","error");
      try{ npmProc.kill("SIGKILL"); }catch{}
    }, 5*60*1000);
    npmProc.on("exit",(code)=>{
      clearTimeout(hangGuard);
      _installInProgress = false;
      const ramAfter = Math.round(process.memoryUsage().rss/1024/1024);
      if(code===0){
        log(`✅ npm install সম্পন্ন — সব প্যাকেজ ইনস্টল হয়েছে (RAM এখন: ${ramAfter}MB)`,"success");
        actuallySpawnBot();
      } else {
        log(`⚠️ npm install ব্যর্থ (code ${code}, RAM এখন: ${ramAfter}MB): `+npmErr.slice(-300),"error");
        notify("error", "⚠️ npm install ব্যর্থ", "বট চালু করা যায়নি — dependency install fail করেছে। প্যানেলের লগ দেখো।");
        bc({type:"status",running:false});
      }
    });
    npmProc.on("error",(e)=>{ clearTimeout(hangGuard); _installInProgress=false; log("⚠️ npm install চালু করতে ব্যর্থ: "+e.message,"error"); });
    return {ok:true,msg:"📦 npm install ব্যাকগ্রাউন্ডে শুরু হয়েছে — একটু পর বট নিজে থেকেই চালু হয়ে যাবে, ওয়েবসাইট এখনই স্বাভাবিকভাবে ব্যবহার করা যাবে"};
  }

  actuallySpawnBot();
  return {ok:true,msg:"বট চালু হয়েছে"};
}

function stopBot(){
  if(rsTimer){clearTimeout(rsTimer);rsTimer=null;}
  if(!botProc) return {ok:false,msg:"বট চলছে না"};
  try{botProc.kill("SIGTERM");setTimeout(()=>{try{if(botProc)botProc.kill("SIGKILL");}catch{}},5000);}catch{}
  botProc=null; botStart=null; botReady=false;
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

// ── কানেকশন-রিফ্রেশ (প্রতি ৬ ঘণ্টায়) ──
// fca-unofficial (unofficial FB API লাইব্রেরি)-র একটা পরিচিত সমস্যা আছে: বট প্রসেস বেঁচে থাকে
// (প্যানেলে "✅ চলছে" দেখায়) কিন্তু Facebook-এর real-time message listener (MQTT) মাঝে মাঝে
// নিঃশব্দে বন্ধ হয়ে যায় — কোনো ক্র্যাশ ছাড়াই, তাই প্যানেল এটা নিজে থেকে ধরতে পারে না।
// এটা কোড দিয়ে ১০০% বন্ধ করা যায় না (unofficial API-র নিজস্ব সীমাবদ্ধতা), কিন্তু নিয়মিত
// বিরতিতে connection রিফ্রেশ করলে ঝুঁকি অনেকটাই কমে — তাই প্রতি ৬ ঘণ্টায় একবার প্রতিরোধমূলক restart।
setInterval(()=>{
  if(!botProc || !botReady || !botStart) return;
  const upHours = (Date.now()-botStart)/(3600*1000);
  if(upHours >= 6){
    log("🔄 কানেকশন-রিফ্রেশ — Facebook-এর real-time listener নিঃশব্দে বন্ধ হওয়া ঠেকাতে প্রতিরোধমূলক রিস্টার্ট","warn");
    notify("info", "🔄 কানেকশন-রিফ্রেশ", "প্রতি ৬ ঘণ্টায় প্রতিরোধমূলক রিস্টার্ট হয়েছে (Facebook listener সতেজ রাখতে)।", {cooldownKey:"conn-refresh", cooldownMs:5*60*60*1000});
    stopBot(); setTimeout(()=>startBot("connection-refresh"),3000);
  }
},10*60*1000); // প্রতি ১০ মিনিটে চেক করা হয়, কিন্তু আসল রিস্টার্ট ৬ ঘণ্টা পার হলেই একবার

// ── প্রতিরোধমূলক RAM গার্ড ── প্যানেল খোলা থাকুক বা না থাকুক, প্রতি ৩০ সেকেন্ডে ব্যাকগ্রাউন্ডে
// নিজে থেকেই চেক করে — RAM যদি ক্রমাগত ৪৭০MB+ থাকে (Render-এর ৫১২MB হার্ড সীমার কাছাকাছি),
// তাহলে OOM crash হওয়ার আগেই বট নিজে থেকে গ্রেসফুলি রিস্টার্ট করে দেয় — আর ফোনে সাথে সাথে জানিয়ে দেয়
let _highRamStreak = 0;
setInterval(()=>{
  if(!botProc) { _highRamStreak=0; return; }
  const botMB = getBotMemMB();
  if(botMB==null) return;
  if(botMB >= 470){
    _highRamStreak++;
    if(_highRamStreak>=3){ // পরপর ৩ বার (≈১.৫ মিনিট) উচ্চ থাকলেই তবে রিস্টার্ট — এক-দুইবারের স্পাইকে না
      log(`🛡️ প্রতিরোধমূলক রিস্টার্ট — বট RAM ${botMB}MB (৫১২MB সীমার কাছাকাছি)`,"warn");
      notify("warn", "🛡️ প্রতিরোধমূলক রিস্টার্ট", `বট RAM ${botMB}MB ছুঁয়ে ফেলেছিল (সীমা ৫১২MB) — ক্র্যাশ হওয়ার আগেই নিজে থেকে নিরাপদে রিস্টার্ট করা হলো।`, {cooldownKey:"preventive-restart", cooldownMs:10*60*1000});
      _highRamStreak=0;
      stopBot(); setTimeout(()=>startBot("preventive-ram-guard"),3000);
    }
  } else {
    _highRamStreak=0;
  }
},30*1000);

// ── MongoDB স্টোরেজ প্রায় শেষ হয়ে গেলে দিনে একবার সতর্ক করা ──
setInterval(async()=>{
  if(!db_connected || !mongoose?.connection?.db) return;
  try{
    const s = await mongoose.connection.db.stats();
    const usedMB = (s.dataSize+s.indexSize)/1024/1024;
    if(usedMB > 512*0.85){
      notify("warn", "⚠️ MongoDB স্টোরেজ প্রায় শেষ", `${Math.round(usedMB)}MB / 512MB ব্যবহার হয়ে গেছে (Atlas M0 ফ্রি সীমা)। পুরনো/অপ্রয়োজনীয় ডেটা সরানো দরকার হতে পারে।`, {cooldownKey:"mongo-storage-warn", cooldownMs:24*60*60*1000});
    }
  }catch{}
},30*60*1000); // প্রতি ৩০ মিনিটে চেক, কিন্তু নোটিফিকেশন দিনে একবারের বেশি না (cooldown দিয়ে)

// ── ROUTES: AUTH ──
app.get("/ping",(req,res)=>res.json({ok:true,running:!!botProc,mongo:db_connected,time:new Date().toISOString()}));
app.get("/health",(req,res)=>res.json({ok:true}));
app.get("/login",(req,res)=>{
  res.set("Cache-Control","no-store, no-cache, must-revalidate, proxy-revalidate");res.set("Pragma","no-cache");res.set("Expires","0");
  if(req.session.ok||getCookies(req).authToken===cfg.authToken)return res.redirect("/");
  res.send(loginHTML());
});
app.post("/login",(req,res)=>{
  if(req.body.password===PASS){
    req.session.ok=true;
    res.append("Set-Cookie", `authToken=${cfg.authToken}; Max-Age=${30*24*60*60}; Path=/; HttpOnly; SameSite=Lax`);
    res.json({ok:true});
  }
  else res.json({ok:false,msg:"❌ ভুল পাসওয়ার্ড"});
});
app.get("/logout",(req,res)=>{req.session.destroy(()=>{}); res.append("Set-Cookie","authToken=; Max-Age=0; Path=/"); res.redirect("/login");});
app.get("/"   ,auth,(req,res)=>{res.set("Cache-Control","no-store, no-cache, must-revalidate, proxy-revalidate");res.set("Pragma","no-cache");res.set("Expires","0");res.send(mainHTML());});

// ── BOT API ──
app.post("/api/bot/start",   auth,(req,res)=>res.json(startBot()));
app.post("/api/bot/stop",    auth,(req,res)=>res.json(stopBot()));
app.post("/api/bot/restart", auth,(req,res)=>{stopBot();setTimeout(()=>res.json(startBot("restart")),2000);});
app.get("/api/bot/status",   auth,(req,res)=>res.json({running:!!botProc,ready:botReady,uptime:botStart?Math.floor((Date.now()-botStart)/1000):0}));
app.get("/api/bot/logs",     auth,(req,res)=>res.json({logs:botLogs}));
app.post("/api/bot/clearlogs",auth,(req,res)=>{botLogs=[];bc({type:"clearLogs"});res.json({ok:true});});
app.post("/api/bot/install", auth,(req,res)=>{
  if(!fs.existsSync(path.join(BDIR,"package.json"))) return res.json({ok:false,msg:"package.json নেই"});
  if(_installInProgress) return res.json({ok:false,msg:"📦 npm install ইতিমধ্যে চলছে"});
  _installInProgress = true;
  log("📦 npm install শুরু (ম্যানুয়াল, ব্যাকগ্রাউন্ডে)...","warn");
  const npmProc = spawn(process.platform==="win32"?"npm.cmd":"npm", ["install","--omit=dev","--no-audit","--no-fund","--prefer-offline"], {cwd:BDIR});
  let npmErr="", npmBuf="";
  const flushLine=(s)=>{npmBuf+=s;let i;while((i=npmBuf.indexOf("\n"))>=0){const line=npmBuf.slice(0,i).trim();npmBuf=npmBuf.slice(i+1);if(line)log("📦 "+line,"info");}};
  npmProc.stdout.on("data",d=>flushLine(d.toString()));
  npmProc.stderr.on("data",d=>{const s=d.toString();npmErr+=s;flushLine(s);});
  const hangGuard=setTimeout(()=>{log("⚠️ npm install ৫ মিনিটেও শেষ হয়নি — বন্ধ করা হলো","error");try{npmProc.kill("SIGKILL");}catch{}},5*60*1000);
  npmProc.on("exit",(code)=>{
    clearTimeout(hangGuard); _installInProgress=false;
    if(code===0) log("✅ npm install সম্পন্ন","success");
    else log("❌ npm install ব্যর্থ: "+npmErr.slice(-300),"error");
  });
  npmProc.on("error",(e)=>{clearTimeout(hangGuard);_installInProgress=false;log("⚠️ npm install চালু করতে ব্যর্থ: "+e.message,"error");});
  res.json({ok:true,msg:"📦 npm install ব্যাকগ্রাউন্ডে শুরু হয়েছে — লগে অগ্রগতি দেখতে পারবে"});
});
app.post("/api/bot/autorestart",auth,(req,res)=>{autoRestart=true;cfg.autoRestart=true;saveJ(CFG,cfg);res.json({ok:true,enabled:true,note:"Auto-Restart সবসময় ON থাকে, বন্ধ করা যায় না"});});
app.get("/api/bot/downloadlog",auth,(req,res)=>{if(fs.existsSync(LFILE))res.download(LFILE,"bot.log");else res.status(404).send("No log");});
app.post("/api/bot/clearlogfile",auth,(req,res)=>{try{fs.writeFileSync(LFILE,"");res.json({ok:true});}catch(e){res.json({ok:false,msg:e.message});}});

let _fileCountCache = {value:0, at:0};
function countF(d){
  if(Date.now()-_fileCountCache.at < 60000) return _fileCountCache.value; // ৬০ সেকেন্ড ক্যাশ — বারবার ভারী ডিস্ক-স্ক্যান এড়ানো
  function walk(dir){
    let c=0;
    try{
      fs.readdirSync(dir).forEach(f=>{
        if(f==="node_modules"||f===".git") return; // এগুলো বট ফাইল না, গুনে লাভ নেই — শুধু সময় নষ্ট
        const s=fs.statSync(path.join(dir,f));
        c+=s.isDirectory()?walk(path.join(dir,f)):1;
      });
    }catch{}
    return c;
  }
  _fileCountCache = {value: walk(d), at: Date.now()};
  return _fileCountCache.value;
}
app.get("/api/stats",auth,(req,res)=>{
  // botFiles: cached value সাথে সাথে return — disk scan background এ করা হবে, block করবে না
  const cached = _fileCountCache.value;
  if(Date.now()-_fileCountCache.at > 60000){
    // background এ scan, এই request block করবে না
    setImmediate(()=>countF(BDIR));
  }
  res.json({...stats,running:!!botProc,ready:botReady,currentUptime:botStart?Math.floor((Date.now()-botStart)/1000):0,
    autoRestart,memMB:Math.round(process.memoryUsage().rss/1024/1024),
    serverUptime:Math.floor(process.uptime()),node:process.version,
    botFiles:cached,mongoConnected:db_connected});
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

// ── নেটওয়ার্ক থ্রুপুট (রিয়েল, /proc/net/dev থেকে) — হ্যাকিং-স্টাইল টার্মিনাল ট্যাবের জন্য ──
let _netPrev = null;
function readNetBytes(){
  try{
    const raw = fs.readFileSync("/proc/net/dev","utf8");
    let rx=0, tx=0;
    raw.split("\n").slice(2).forEach(line=>{
      const m = line.trim().match(/^([\w.]+):\s*(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)/);
      if(m && m[1]!=="lo"){ rx += parseInt(m[2],10); tx += parseInt(m[3],10); }
    });
    return {rx, tx, t: Date.now()};
  }catch{ return null; }
}
function getNetSpeed(){
  const now = readNetBytes();
  if(!now) return {rxKBs:null, txKBs:null};
  if(!_netPrev){ _netPrev = now; return {rxKBs:0, txKBs:0}; }
  const dt = (now.t - _netPrev.t)/1000;
  const rxKBs = dt>0 ? Math.max(0, +((now.rx-_netPrev.rx)/1024/dt).toFixed(1)) : 0;
  const txKBs = dt>0 ? Math.max(0, +((now.tx-_netPrev.tx)/1024/dt).toFixed(1)) : 0;
  _netPrev = now;
  return {rxKBs, txKBs};
}
let _cpuPrev = null;
function getCpuPercent(){
  const usage = process.cpuUsage(); // মাইক্রোসেকেন্ডে, প্যানেল প্রসেসের নিজের CPU সময়
  const now = Date.now();
  if(!_cpuPrev){ _cpuPrev = {usage, t:now}; return 0; }
  const dtMs = now - _cpuPrev.t;
  const cpuMs = (usage.user - _cpuPrev.usage.user + usage.system - _cpuPrev.usage.system)/1000;
  _cpuPrev = {usage, t:now};
  if(dtMs<=0) return 0;
  return Math.min(100, Math.round((cpuMs/dtMs)*100));
}
app.get("/api/system/terminal",auth,(req,res)=>{
  const net = getNetSpeed();
  const botMB = getBotMemMB(), panelMB = Math.round(process.memoryUsage().rss/1024/1024);
  res.json({
    ok:true, time:Date.now(),
    net,
    cpuPercent: getCpuPercent(),
    ramPercent: Math.min(100, Math.round(((botMB||0)+panelMB)/512*100)),
    botRunning: !!botProc, botReady, heavy: getHeavyStatus(),
    uptimeSec: botStart?Math.floor((Date.now()-botStart)/1000):0,
    tail: botLogs.slice(-6).map(l=>({time:l.time,text:l.text,type:l.type}))
  });
});

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
    const showHidden = req.query.showHidden==="1";
    const items=fs.readdirSync(dir)
      .filter(name=> showHidden || !name.startsWith(".")) // অভ্যন্তরীণ মার্কার ফাইল (.crash_flag.json ইত্যাদি) ডিফল্টে লুকানো — এলোমেলো লাগে
      .map(name=>{
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

// ── 🧪 কমান্ড টেস্টার — সিনট্যাক্স, প্রয়োজনীয় স্ট্রাকচার, dependency, আর ফাইলের ভিতরের API লিংক লাইভ চেক ──
function testUrl(url){
  return new Promise(resolve=>{
    try{
      const mod = url.startsWith("https") ? https : require("http");
      const req = mod.request(url, {method:"HEAD", timeout:6000}, r=>{
        resolve({url, ok: r.statusCode<400, status:r.statusCode});
        r.resume();
      });
      req.on("timeout", ()=>{ req.destroy(); resolve({url, ok:false, status:"timeout"}); });
      req.on("error", e=>resolve({url, ok:false, status:"error: "+e.message}));
      req.end();
    }catch(e){ resolve({url, ok:false, status:"error: "+e.message}); }
  });
}
app.post("/api/file/test",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    if(!fs.existsSync(f)) return res.json({ok:false,msg:"ফাইল খুঁজে পাওয়া যায়নি"});
    const content = fs.readFileSync(f,"utf8");
    const result = { syntax:null, structure:null, dependencies:[], apis:[] };

    // ১. সিনট্যাক্স চেক
    const chk = spawnSync(process.execPath, ["--check", f], {timeout:10000});
    result.syntax = chk.status===0
      ? {ok:true, msg:"সিনট্যাক্স ঠিক আছে ✅"}
      : {ok:false, msg:(chk.stderr||"").toString().split("\n").slice(0,4).join(" ")||"সিনট্যাক্স এরর"};

    if(result.syntax.ok){
      // ২. স্ট্রাকচার চেক — আলাদা isolated প্রসেসে require করা হচ্ছে, যাতে ভাঙা ফাইল মূল প্যানেলকে ক্র্যাশ না করে
      const probe = spawnSync(process.execPath, ["-e", `
        try{
          const cmd = require(${JSON.stringify(f)});
          const out = {
            hasConfig: !!(cmd && cmd.config),
            name: cmd?.config?.name || null,
            aliases: cmd?.config?.aliases || [],
            hasRunFn: !!(cmd && (cmd.run || cmd.onStart || cmd.onCall)),
            dependencies: cmd?.config?.dependencies || {}
          };
          console.log("###RESULT###"+JSON.stringify(out));
        }catch(e){ console.log("###ERROR###"+e.message); }
      `], {cwd: path.dirname(f), timeout:8000});
      const out = (probe.stdout||"").toString();
      if(out.includes("###ERROR###")){
        result.structure = {ok:false, msg:"ফাইল লোড করতে ব্যর্থ: "+out.split("###ERROR###")[1].trim().slice(0,200)};
      } else if(out.includes("###RESULT###")){
        try{
          const parsed = JSON.parse(out.split("###RESULT###")[1].trim());
          const problems=[];
          if(!parsed.hasConfig) problems.push("module.exports.config নেই");
          if(!parsed.name) problems.push("config.name নেই");
          if(!parsed.hasRunFn) problems.push("run/onStart/onCall ফাংশন নেই");
          result.structure = problems.length
            ? {ok:false, msg:"সমস্যা: "+problems.join(", ")}
            : {ok:true, msg:`ঠিক আছে ✅ — নাম: "${parsed.name}"${parsed.aliases.length?", alias: "+parsed.aliases.join(", "):""}`};
          // ৩. Dependency চেক
          for(const [pkg] of Object.entries(parsed.dependencies||{})){
            try{ require.resolve(pkg, {paths:[path.join(BDIR,"node_modules")]}); result.dependencies.push({pkg, ok:true}); }
            catch{ result.dependencies.push({pkg, ok:false}); }
          }
        }catch{ result.structure = {ok:false, msg:"ফলাফল পার্স করতে ব্যর্থ"}; }
      } else {
        result.structure = {ok:false, msg:"টাইমআউট বা কোনো আউটপুট পাওয়া যায়নি (৮ সেকেন্ডের বেশি সময় নিয়েছে)"};
      }

      // ৪. ফাইলের ভিতরের API URL বের করে লাইভ টেস্ট (সর্বোচ্চ ৫টা, ডুপ্লিকেট বাদে)
      const urls = [...new Set((content.match(/https?:\/\/[^\s"'`)]+/g)||[]))].slice(0,5);
      result.apis = await Promise.all(urls.map(testUrl));
    }

    res.json({ok:true, result});
  }catch(e){ res.status(500).json({ok:false, error:e.message}); }
});

app.post("/api/file/save",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    fs.mkdirSync(path.dirname(f),{recursive:true});
    fs.writeFileSync(f,req.body.content||"");
    // MongoDB তে সেভ
    const relPath = path.relative(BDIR,f);
    await saveToMongo(relPath, req.body.content||"");
    notifyBotFile("save", relPath);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post("/api/file/delete",auth,async(req,res)=>{
  try{
    const f=safe(BDIR,req.body.path);
    const relPath = path.relative(BDIR,f);
    fs.rmSync(f,{recursive:true,force:true});
    await deleteFromMongo(relPath);
    notifyBotFile("delete", relPath);
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
    notifyBotFile("delete", fromRel);
    if(!stat.isDirectory()) notifyBotFile("save", toRel);
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
    notifyBotFile("save", relPath);
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
app.get("/api/alerts",auth,(req,res)=>res.json({alerts: alerts.slice().reverse()}));
app.post("/api/alerts/clear",auth,(req,res)=>{alerts=[];saveJ(AFILE,alerts);saveAlertsToMongo();res.json({ok:true});});
app.post("/api/settings/save",auth,(req,res)=>{
  Object.assign(cfg,req.body);
  autoRestart=true; cfg.autoRestart=true; // ── সবসময় ON, সেটিংস থেকে বন্ধ করা যাবে না
  if(req.body.siteUrl) cfg.siteUrl=req.body.siteUrl.trim();
  saveJ(CFG,cfg);
  res.json({ok:true});
});
app.post("/api/settings/password",auth,(req,res)=>{
  const{current,newPass}=req.body;
  if(current!==PASS&&current!==cfg.password) return res.json({ok:false,msg:"বর্তমান পাসওয়ার্ড ভুল"});
  if(!newPass||newPass.length<4) return res.json({ok:false,msg:"কমপক্ষে ৪ অক্ষর"});
  cfg.password=newPass;saveJ(CFG,cfg);res.json({ok:true,msg:"পাসওয়ার্ড পরিবর্তন হয়েছে"});
});

// ── WS ──

// ════════════════ LOGIN HTML ════════════════
function loginHTML(){return `<!DOCTYPE html>
<html lang="bn"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta name="theme-color" content="#050510">
<title>Bot Panel — লগইন</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#050510;--ac:#7c6eff;--pk:#ff6b9d;--gn:#00f5a0;
  --tx:#e8eaf6;--mu:#6b6b9a;--bd:rgba(255,255,255,.07);
  --glass:rgba(255,255,255,.04);--blur:blur(40px);
}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--tx);font-family:'Segoe UI',system-ui,sans-serif;display:flex;align-items:center;justify-content:center}

/* Ambient orbs */
.orbs{position:fixed;inset:0;pointer-events:none;overflow:hidden}
.orb{position:absolute;border-radius:50%;filter:blur(80px);opacity:.15;animation:drift 12s ease-in-out infinite}
.orb1{width:600px;height:600px;background:radial-gradient(circle,#7c6eff,transparent);top:-200px;left:-200px}
.orb2{width:500px;height:500px;background:radial-gradient(circle,#ff6b9d,transparent);bottom:-150px;right:-150px;animation-delay:6s}
.orb3{width:300px;height:300px;background:radial-gradient(circle,#00f5a0,transparent);top:40%;left:50%;animation-delay:3s}
@keyframes drift{0%,100%{transform:scale(1) translate(0,0)}50%{transform:scale(1.15) translate(20px,-20px)}}

/* Grid lines */
.grid{position:fixed;inset:0;background-image:linear-gradient(rgba(124,110,255,.04) 1px,transparent 1px),linear-gradient(90deg,rgba(124,110,255,.04) 1px,transparent 1px);background-size:40px 40px;pointer-events:none}

.card{
  position:relative;z-index:2;
  background:var(--glass);backdrop-filter:var(--blur);
  border:1px solid var(--bd);
  border-radius:28px;padding:48px 36px;width:90%;max-width:400px;
  box-shadow:0 40px 80px rgba(0,0,0,.6),inset 0 1px 0 rgba(255,255,255,.08);
  animation:cardIn .6s cubic-bezier(.16,1,.3,1);
}
@keyframes cardIn{from{opacity:0;transform:translateY(40px) scale(.96)}to{opacity:1;transform:none}}

.logo-ring{
  width:88px;height:88px;margin:0 auto 24px;position:relative;
  display:flex;align-items:center;justify-content:center;
}
.logo-ring::before{
  content:"";position:absolute;inset:0;border-radius:50%;
  background:conic-gradient(from 0deg,#7c6eff,#ff6b9d,#00f5a0,#7c6eff);
  animation:spin 4s linear infinite;
  padding:2px;
  -webkit-mask:linear-gradient(#fff 0 0) content-box,linear-gradient(#fff 0 0);
  -webkit-mask-composite:xor;mask-composite:exclude;
}
.logo-inner{
  width:76px;height:76px;border-radius:50%;
  background:linear-gradient(135deg,#1a1540,#0d0d1f);
  display:flex;align-items:center;justify-content:center;
  font-size:32px;position:relative;z-index:1;
  box-shadow:0 0 40px rgba(124,110,255,.3);
}
@keyframes spin{to{transform:rotate(360deg)}}

h1{text-align:center;font-size:22px;font-weight:900;color:#fff;margin-bottom:6px;letter-spacing:-.3px}
.sub{text-align:center;color:var(--mu);font-size:13px;margin-bottom:32px}

.field{position:relative;margin-bottom:14px}
.field input{
  width:100%;padding:14px 46px 14px 16px;
  border-radius:14px;border:1px solid var(--bd);
  background:rgba(255,255,255,.05);
  color:var(--tx);font-size:15px;outline:none;transition:.25s;
}
.field input:focus{border-color:var(--ac);background:rgba(124,110,255,.07);box-shadow:0 0 0 3px rgba(124,110,255,.12)}
.field .eye{position:absolute;right:14px;top:50%;transform:translateY(-50%);cursor:pointer;font-size:18px;opacity:.5;transition:.2s;border:none;background:transparent;color:var(--tx);padding:4px}
.field .eye:hover{opacity:1}

.err-box{
  background:rgba(255,107,107,.08);border:1px solid rgba(255,107,107,.25);
  color:#ff9b9b;border-radius:10px;padding:10px 14px;font-size:12.5px;
  margin-bottom:14px;display:none;animation:shake .3s ease
}
.err-box.show{display:block}
@keyframes shake{0%,100%{transform:translateX(0)}25%{transform:translateX(-6px)}75%{transform:translateX(6px)}}

.btn-login{
  width:100%;padding:15px;border-radius:14px;border:none;
  background:linear-gradient(135deg,var(--ac),var(--pk));
  color:#fff;font-size:15px;font-weight:800;cursor:pointer;
  transition:.2s;position:relative;overflow:hidden;letter-spacing:.3px;
}
.btn-login::after{content:"";position:absolute;inset:0;background:rgba(255,255,255,0);transition:.2s}
.btn-login:hover{transform:translateY(-2px);box-shadow:0 12px 40px rgba(124,110,255,.4)}
.btn-login:hover::after{background:rgba(255,255,255,.07)}
.btn-login:active{transform:scale(.97)}
.btn-login.loading{pointer-events:none}
.btn-login.loading::before{content:"";position:absolute;left:0;top:0;bottom:0;width:40%;background:rgba(255,255,255,.15);animation:shimmer 1s infinite}
@keyframes shimmer{from{transform:skewX(-20deg) translateX(-150%)}to{transform:skewX(-20deg) translateX(400%)}}

.ver{text-align:center;font-size:10px;color:var(--mu);margin-top:20px;opacity:.5}
</style>
</head><body>
<div class="orbs"><div class="orb orb1"></div><div class="orb orb2"></div><div class="orb orb3"></div></div>
<div class="grid"></div>
<div class="card">
  <div class="logo-ring"><div class="logo-inner">🤖</div></div>
  <h1>${cfg.panelName||'Bot Panel'}</h1>
  <p class="sub">আপনার বট কন্ট্রোল সেন্টার</p>
  <div class="err-box" id="err"></div>
  <div class="field">
    <input type="password" id="pw" placeholder="পাসওয়ার্ড লিখুন" autofocus>
    <button class="eye" id="eyeBtn" type="button" onclick="toggleEye()">👁</button>
  </div>
  <button class="btn-login" id="loginBtn" onclick="login()">প্রবেশ করুন →</button>
  <p class="ver">v2.0 — Render Free Optimized</p>
</div>
<script>
function toggleEye(){const i=document.getElementById('pw');i.type=i.type==='password'?'text':'password';}
async function login(){
  const btn=document.getElementById('loginBtn');
  btn.classList.add('loading');btn.textContent='যাচাই হচ্ছে...';
  const pw=document.getElementById('pw').value;
  try{
    const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'password='+encodeURIComponent(pw)});
    const d=await r.json();
    if(d.ok){btn.textContent='✅ সফল!';setTimeout(()=>location.href='/',300);}
    else{
      const e=document.getElementById('err');e.textContent=d.msg||'❌ ভুল পাসওয়ার্ড';e.classList.add('show');
      btn.classList.remove('loading');btn.textContent='প্রবেশ করুন →';
      setTimeout(()=>e.classList.remove('show'),4000);
    }
  }catch(e){btn.classList.remove('loading');btn.textContent='প্রবেশ করুন →';}
}
document.getElementById('pw').addEventListener('keydown',e=>{if(e.key==='Enter')login();});
</script>
</body></html>`;
}

// ════════════════ MAIN HTML v2 ════════════════
function mainHTML(){
const pname=cfg.panelName||'Bot Panel';
const BUILD_VER='v2.0-2026';
return `<!DOCTYPE html>
<html lang="bn"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#050510">
<title>${pname}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#050510;--s1:#0b0b1a;--s2:#111127;--s3:#181830;--s4:#1e1e38;
  --bd:rgba(255,255,255,.07);--bd2:rgba(255,255,255,.12);
  --tx:#e8eaf6;--mu:#6b6b9a;--dim:#3a3a5c;
  --ac:#7c6eff;--ac2:#5a50cc;
  --pk:#ff6b9d;--gn:#00f5a0;--yw:#ffd460;--rd:#ff5572;--bl:#38d9f5;--or:#ff9f43;
  --glass:rgba(255,255,255,.04);
}
html{overflow-x:hidden}
body{background:var(--bg);color:var(--tx);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;overflow-x:hidden}

/* AMBIENT */
.ambient{position:fixed;inset:0;pointer-events:none;overflow:hidden;z-index:0}
.amb-orb{position:absolute;border-radius:50%;filter:blur(100px);opacity:.07;animation:ambDrift 15s ease-in-out infinite}
.amb1{width:800px;height:800px;background:var(--ac);top:-300px;left:-300px}
.amb2{width:600px;height:600px;background:var(--pk);bottom:-200px;right:-200px;animation-delay:7s}
@keyframes ambDrift{0%,100%{transform:scale(1)}50%{transform:scale(1.1)}}

/* TOP BAR */
.topbar{
  position:fixed;top:0;left:0;right:0;height:56px;z-index:200;
  background:rgba(5,5,16,.92);backdrop-filter:blur(24px);
  border-bottom:1px solid var(--bd);
  display:flex;align-items:center;padding:0 12px;gap:10px;
}
.top-logo{
  width:36px;height:36px;border-radius:11px;flex-shrink:0;
  background:linear-gradient(135deg,var(--ac),var(--pk));
  display:flex;align-items:center;justify-content:center;font-size:18px;
  box-shadow:0 0 20px rgba(124,110,255,.3);
  transition:box-shadow .4s;
}
.top-logo.live{animation:logoPulse 2s ease-in-out infinite}
.top-logo.starting{animation:logoPulse .8s ease-in-out infinite;filter:hue-rotate(80deg)}
@keyframes logoPulse{
  0%,100%{box-shadow:0 0 20px rgba(124,110,255,.3)}
  50%{box-shadow:0 0 40px rgba(0,245,160,.6),0 0 0 6px rgba(0,245,160,.08)}
}
.top-name{font-size:15px;font-weight:800;color:#fff;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.top-right{display:flex;align-items:center;gap:6px;flex-shrink:0}

/* Status Pill */
.stat-pill{
  display:flex;align-items:center;gap:6px;
  background:var(--s2);border:1px solid var(--bd);border-radius:99px;
  padding:5px 10px;font-size:11.5px;font-weight:600;
  cursor:default;transition:.3s;
}
.stat-dot{width:7px;height:7px;border-radius:50%;background:var(--rd);flex-shrink:0;transition:.3s}
.stat-dot.on{background:var(--gn);box-shadow:0 0 8px var(--gn);animation:blink 2s infinite}
.stat-dot.starting{background:var(--yw);box-shadow:0 0 8px var(--yw);animation:blink .8s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}

.icon-btn{
  width:34px;height:34px;border-radius:9px;border:1px solid var(--bd);
  background:transparent;color:var(--tx);font-size:16px;
  display:flex;align-items:center;justify-content:center;cursor:pointer;
  transition:.15s;position:relative;flex-shrink:0;
}
.icon-btn:hover{background:var(--s3);border-color:var(--bd2)}
.badge{
  position:absolute;top:-5px;right:-5px;background:var(--rd);
  color:#fff;font-size:9px;font-weight:800;min-width:16px;height:16px;
  border-radius:8px;display:none;align-items:center;justify-content:center;padding:0 3px;
}

/* TOAST */
.toast-wrap{position:fixed;top:64px;right:10px;z-index:999;display:flex;flex-direction:column;gap:6px;pointer-events:none;max-width:280px}
.toast{
  background:var(--s3);border-radius:12px;padding:11px 15px;font-size:12.5px;
  box-shadow:0 12px 32px rgba(0,0,0,.5);pointer-events:auto;
  border-left:3px solid var(--bd2);
  animation:toastIn .3s cubic-bezier(.16,1,.3,1);
}
@keyframes toastIn{from{transform:translateX(110%);opacity:0}to{transform:none;opacity:1}}
.toast.success{border-left-color:var(--gn);color:var(--gn)}
.toast.error{border-left-color:var(--rd);color:var(--rd)}
.toast.warn{border-left-color:var(--yw);color:var(--yw)}
.toast.info{border-left-color:var(--bl);color:var(--bl)}

/* MAIN CONTENT */
.main{padding:64px 12px 72px;min-height:100vh;position:relative;z-index:1}
.page{display:none}.page.active{display:block;animation:pgIn .2s ease}
@keyframes pgIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}

/* SECTION TITLE */
.sec-title{font-size:13px;font-weight:800;color:var(--mu);text-transform:uppercase;letter-spacing:.8px;margin-bottom:10px;display:flex;align-items:center;gap:6px}
.sec-title::before{content:"";flex:1;height:1px;background:var(--bd)}

/* CARDS */
.card{
  background:linear-gradient(135deg,var(--s2),var(--s1));
  border:1px solid var(--bd);border-radius:18px;padding:16px;
  margin-bottom:12px;transition:.2s;
}
.card:hover{border-color:rgba(124,110,255,.2)}
.card-glow{box-shadow:0 0 30px rgba(124,110,255,.08),inset 0 1px 0 rgba(255,255,255,.05)}

/* STAT GRID */
.stat-grid{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:12px}
.stat-grid.s3{grid-template-columns:repeat(3,1fr)}
.stat-cell{
  background:linear-gradient(135deg,var(--s2),var(--s3));
  border:1px solid var(--bd);border-radius:14px;padding:14px 12px;
  transition:.2s;cursor:default;
}
.stat-cell:hover{border-color:rgba(124,110,255,.25);transform:translateY(-1px)}
.stat-icon{font-size:20px;margin-bottom:6px}
.stat-val{font-size:22px;font-weight:900;color:#fff;line-height:1;margin-bottom:3px}
.stat-val.sm{font-size:16px}
.stat-lbl{font-size:10px;color:var(--mu);font-weight:600;text-transform:uppercase;letter-spacing:.5px}

/* BOT STATUS CARD */
.bot-state{
  display:flex;align-items:center;gap:12px;padding:14px 16px;
  border-radius:14px;margin-bottom:14px;transition:.4s;
  border:1px solid var(--bd);position:relative;overflow:hidden;
}
.bot-state::before{content:"";position:absolute;inset:0;opacity:.06;transition:.4s}
.bot-state.running{border-color:rgba(0,245,160,.25)}
.bot-state.running::before{background:var(--gn)}
.bot-state.starting{border-color:rgba(255,212,96,.25)}
.bot-state.starting::before{background:var(--yw)}
.bot-state.stopped{border-color:rgba(255,85,114,.2)}
.bot-state.stopped::before{background:var(--rd)}
.bot-state-icon{font-size:30px;flex-shrink:0}
.bot-state-info{flex:1}
.bot-state-txt{font-size:15px;font-weight:800;color:#fff}
.bot-state-sub{font-size:11px;color:var(--mu);margin-top:2px}

/* BUTTONS */
.btn{
  width:100%;padding:12px 10px;border-radius:13px;border:none;
  font-size:13px;font-weight:700;cursor:pointer;transition:.15s;
  display:flex;align-items:center;justify-content:center;gap:6px;
  letter-spacing:.2px;
}
.btn:active{transform:scale(.95)}
.btn:disabled{opacity:.4;pointer-events:none}
.btn-start{background:linear-gradient(135deg,var(--gn),#00c48c);color:#000}
.btn-stop{background:linear-gradient(135deg,var(--rd),#ff3357);color:#fff}
.btn-restart{background:linear-gradient(135deg,var(--yw),var(--or));color:#000}
.btn-npm{background:linear-gradient(135deg,var(--bl),var(--ac));color:#fff}
.btn-backup{background:linear-gradient(135deg,#a78bfa,var(--pk));color:#fff}
.btn-ghost{background:transparent;border:1px solid var(--bd);color:var(--tx)}
.btn-ghost:hover{background:var(--s3)}
.btn-primary{background:linear-gradient(135deg,var(--ac),var(--ac2));color:#fff}
.btn-danger{background:transparent;border:1px solid rgba(255,85,114,.3);color:var(--rd)}
.btn-grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
.btn-grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}

/* PROGRESS BAR */
.prog-wrap{background:var(--s2);border:1px solid var(--bd);border-radius:12px;padding:14px;display:none;margin-bottom:12px}
.prog-label{display:flex;justify-content:space-between;font-size:12px;margin-bottom:8px;color:var(--tx)}
.prog-track{height:8px;background:var(--s1);border-radius:99px;overflow:hidden;border:1px solid var(--bd)}
.prog-fill{height:100%;border-radius:99px;background:linear-gradient(90deg,var(--ac),var(--gn));transition:width .2s;width:0}
.prog-status{font-size:11px;color:var(--mu);margin-top:7px;min-height:16px}

/* RAM / MONITOR BARS */
.mon-bar-wrap{margin-bottom:14px}
.mon-bar-header{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.mon-bar-label{font-size:12px;font-weight:600;color:var(--tx)}
.mon-bar-val{font-size:12px;color:var(--mu);font-variant-numeric:tabular-nums}
.mon-track{height:10px;border-radius:6px;background:var(--s1);border:1px solid var(--bd);overflow:hidden}
.mon-fill{height:100%;border-radius:6px;transition:width .5s ease,background .5s ease}
.mon-peak{font-size:10px;color:var(--mu);margin-top:5px;display:flex;justify-content:flex-end;gap:4px}
.mon-peak b{color:var(--bl)}
.mon-badge{
  display:inline-flex;align-items:center;gap:4px;
  padding:2px 9px;border-radius:99px;font-size:10px;font-weight:700;margin-left:6px;
}
.mon-badge.ok{background:rgba(0,245,160,.12);color:var(--gn);border:1px solid rgba(0,245,160,.2)}
.mon-badge.warn{background:rgba(255,212,96,.12);color:var(--yw);border:1px solid rgba(255,212,96,.2)}
.mon-badge.err{background:rgba(255,85,114,.12);color:var(--rd);border:1px solid rgba(255,85,114,.2)}
.mon-badge.info{background:rgba(56,217,245,.12);color:var(--bl);border:1px solid rgba(56,217,245,.2)}

/* TERMINAL */
.terminal{
  background:#000;border-radius:14px;overflow:hidden;
  border:1px solid #1a3a1a;
  box-shadow:0 0 40px rgba(0,245,160,.06),inset 0 0 80px rgba(0,245,160,.02);
}
.term-topbar{
  background:#080e08;padding:9px 14px;
  display:flex;align-items:center;gap:6px;border-bottom:1px solid #1a3a1a;
}
.term-dot{width:10px;height:10px;border-radius:50%}
.term-dot.r{background:#ff5f56}.term-dot.y{background:#ffbd2e}.term-dot.g{background:#27c93f}
.term-title{margin-left:8px;font-family:'Courier New',monospace;font-size:11px;color:#4a7a4a}
.term-body{
  padding:16px;font-family:'Courier New',monospace;
  font-size:12.5px;line-height:1.9;color:#33ff66;
  text-shadow:0 0 5px rgba(51,255,102,.4);
  min-height:300px;
}
.t-line{white-space:normal;word-break:break-word}
.t-dim{color:#2a5a2a;font-size:11px}
.t-bold{color:#7fffb0;text-shadow:0 0 8px rgba(127,255,176,.6)}
.t-cursor{display:inline-block;animation:tcBlink 1s step-end infinite}
@keyframes tcBlink{0%,50%{opacity:1}51%,100%{opacity:0}}
.t-sep{border-top:1px dashed #1a3a1a;margin:10px 0}
.t-bar-row{margin:3px 0 12px}
.t-bar{height:8px;background:#060e06;border:1px solid #1a3a1a;border-radius:2px;overflow:hidden}
.t-bar-fill{height:100%;transition:width .6s ease;box-shadow:0 0 10px currentColor}
.t-net{background:#00e5ff;color:#00e5ff}
.t-cpu{background:#ffd700;color:#ffd700}
.t-ram{background:#ff4466;color:#ff4466}
.t-glitch{
  font-size:18px;font-weight:900;letter-spacing:2px;color:#33ff66;
  text-shadow:0 0 10px rgba(51,255,102,.6);margin-bottom:10px;
  animation:tGlitch 4s infinite;position:relative;
}
.t-glitch::before,.t-glitch::after{content:attr(data-t);position:absolute;left:0;top:0}
.t-glitch::before{color:#ff33aa;animation:tG1 2.5s infinite;clip-path:inset(0 0 60% 0)}
.t-glitch::after{color:#33ccff;animation:tG2 3s infinite;clip-path:inset(60% 0 0 0)}
@keyframes tGlitch{0%,90%,100%{transform:none}92%{transform:translate(-2px,1px)}95%{transform:translate(2px,-1px)}}
@keyframes tG1{0%,90%,100%{transform:none}92%{transform:translate(2px,-1px)}}
@keyframes tG2{0%,90%,100%{transform:none}92%{transform:translate(-3px,0)}}
.t-tail-info{color:#5fae7a}.t-tail-success{color:#7fffb0}.t-tail-error{color:#ff8080}.t-tail-warn{color:#ffd633}

/* LOGS */
.log-controls{display:flex;gap:6px;margin-bottom:10px;overflow-x:auto;padding-bottom:2px;flex-wrap:nowrap}
.log-controls::-webkit-scrollbar{display:none}
.lf-btn{
  padding:6px 12px;border-radius:8px;border:1px solid var(--bd);
  background:transparent;color:var(--mu);font-size:11.5px;cursor:pointer;
  white-space:nowrap;transition:.15s;flex-shrink:0;
}
.lf-btn.on{background:var(--ac);color:#fff;border-color:var(--ac)}
.lf-btn:hover:not(.on){background:var(--s3)}
.logbox{
  background:#06060f;border:1px solid var(--bd);border-radius:14px;
  padding:10px;height:calc(100vh - 210px);overflow-y:auto;
  font-family:'Courier New',monospace;font-size:11.5px;
}
.logbox::-webkit-scrollbar{width:4px}
.logbox::-webkit-scrollbar-thumb{background:var(--s3);border-radius:2px}
.log-entry{
  display:flex;gap:8px;align-items:flex-start;
  padding:7px 10px;margin-bottom:4px;border-radius:9px;
  background:var(--s1);border-left:3px solid var(--bd);
  transition:.12s;
}
.le-icon{flex-shrink:0;font-size:12px;line-height:1.5}
.le-time{color:var(--mu);white-space:nowrap;font-size:9.5px;flex-shrink:0;padding-top:2px;opacity:.7}
.le-txt{word-break:normal;overflow-wrap:anywhere;line-height:1.55}
.le-info{border-left-color:#3a4a7a}.le-info .le-txt{color:#9ca3c0}
.le-success{border-left-color:var(--gn);background:rgba(0,245,160,.05)}.le-success .le-txt{color:#7fe8ba}
.le-error{border-left-color:var(--rd);background:rgba(255,85,114,.06)}.le-error .le-txt{color:#ff9bb0}
.le-warn{border-left-color:var(--yw);background:rgba(255,212,96,.06)}.le-warn .le-txt{color:#ffd980}

body.log-full .topbar,.body.log-full .navtabs,.body.log-full .toast-wrap{display:none!important}
body.log-full .logbox{position:fixed;inset:0;height:100vh;border-radius:0;z-index:300;background:#000;padding:8px 8px 56px}
body.log-full .log-controls{position:fixed;bottom:0;left:0;right:0;z-index:301;background:rgba(5,5,16,.97);backdrop-filter:blur(12px);padding:8px;border-top:1px solid var(--bd);margin:0}

/* FILE MANAGER */
.pathbar{
  background:var(--s2);border:1px solid var(--bd);border-radius:10px;
  padding:9px 13px;font-size:12px;color:var(--mu);
  margin-bottom:10px;overflow-x:auto;white-space:nowrap;
  display:flex;align-items:center;gap:4px;
}
.pathbar::-webkit-scrollbar{display:none}
.pp{color:var(--ac);cursor:pointer;font-weight:700;transition:.15s}
.pp:hover{opacity:.8;text-decoration:underline}
.fm-toolbar{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.tbtn{
  padding:7px 12px;border-radius:9px;border:1px solid var(--bd);
  background:var(--s2);color:var(--tx);font-size:12px;cursor:pointer;
  white-space:nowrap;transition:.15s;display:inline-flex;align-items:center;gap:5px;
}
.tbtn:hover{background:var(--s3);border-color:var(--bd2)}
.tbtn.primary{background:var(--ac);border-color:var(--ac);color:#fff}
.tbtn.danger{border-color:rgba(255,85,114,.3);color:var(--rd)}
.tbtn.danger:hover{background:rgba(255,85,114,.1)}
.search-input{
  width:100%;padding:10px 14px;border-radius:10px;border:1px solid var(--bd);
  background:var(--s2);color:var(--tx);font-size:13px;outline:none;
  margin-bottom:10px;transition:.2s;
}
.search-input:focus{border-color:var(--ac);box-shadow:0 0 0 3px rgba(124,110,255,.1)}

/* File list */
.file-list{display:flex;flex-direction:column;gap:6px}
.file-row{
  display:flex;align-items:center;gap:12px;padding:11px 13px;
  border-radius:13px;cursor:pointer;transition:.15s;
  background:linear-gradient(135deg,var(--s2),var(--s1));
  border:1px solid var(--bd);
}
.file-row:hover{border-color:rgba(124,110,255,.25);transform:translateX(2px)}
.file-row:active{transform:scale(.98)}
.file-icon{
  width:38px;height:38px;border-radius:10px;
  display:flex;align-items:center;justify-content:center;font-size:18px;
  flex-shrink:0;background:var(--s3);
}
.fi-code{background:rgba(255,107,107,.12)}.fi-data{background:rgba(56,217,245,.12)}
.fi-media{background:rgba(160,100,255,.12)}.fi-dir{background:rgba(124,110,255,.15)}
.fi-archive{background:rgba(255,212,96,.12)}.fi-doc{background:rgba(0,245,160,.08)}
.file-info{flex:1;overflow:hidden}
.file-name{font-size:13px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.file-meta{font-size:10px;color:var(--mu);margin-top:2px}
.file-actions{display:flex;gap:2px;flex-shrink:0}
.fab{
  padding:6px 8px;border-radius:8px;border:none;
  background:transparent;color:var(--mu);font-size:12px;cursor:pointer;transition:.12s;
}
.fab:hover{background:var(--s3);color:var(--tx)}
.fab.del:hover{background:rgba(255,85,114,.12);color:var(--rd)}
.fm-empty{padding:48px;text-align:center;color:var(--mu)}
.fm-summary{font-size:10.5px;color:var(--mu);padding:2px 4px 10px;display:flex;justify-content:space-between}

/* CODE EDITOR */
.editor-header{
  background:var(--s2);border:1px solid var(--bd);border-radius:12px 12px 0 0;
  padding:10px 12px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;
}
.ed-filename{flex:1;font-size:12px;color:var(--ac);font-weight:700;overflow:hidden;text-overflow:ellipsis}
.ed-lang-badge{font-size:10px;color:var(--mu);background:var(--s3);padding:2px 8px;border-radius:5px}
.editor-wrap{position:relative}
#ced{
  width:100%;height:calc(100vh - 230px);
  background:#03030e;border:1px solid var(--bd);border-top:none;border-radius:0 0 12px 12px;
  padding:14px;color:#e6edf3;font-family:'Courier New',monospace;font-size:13px;
  line-height:1.8;resize:none;outline:none;tab-size:2;
}
#cedHl{
  position:absolute;top:0;left:0;width:100%;height:calc(100vh - 230px);
  margin:0;padding:14px;background:#03030e;border:1px solid var(--bd);
  border-top:none;border-radius:0 0 12px 12px;
  color:#e6edf3;font-family:'Courier New',monospace;font-size:13px;
  line-height:1.8;white-space:pre-wrap;word-break:break-word;
  overflow:hidden;pointer-events:none;z-index:0;display:none;tab-size:2;
}
.editor-wrap.hl-active #cedHl{display:block}
.editor-wrap.hl-active #ced{color:transparent;caret-color:#e6edf3;background:transparent;position:relative;z-index:1}
.tok-kw{color:#ff79c6}.tok-str{color:#f1fa8c}.tok-num{color:#bd93f9}.tok-com{color:#6272a4;font-style:italic}.tok-fn{color:#50fa7b}.tok-prop{color:#8be9fd}

/* UPLOAD */
.upload-zone{
  border:2px dashed var(--bd);border-radius:18px;padding:48px 20px;
  text-align:center;cursor:pointer;background:var(--s2);
  transition:.3s;margin-bottom:14px;
}
.upload-zone:hover,.upload-zone.drag{border-color:var(--ac);background:rgba(124,110,255,.06)}
.uz-icon{font-size:52px;margin-bottom:14px;animation:uFloat 3s ease-in-out infinite}
@keyframes uFloat{0%,100%{transform:translateY(0)}50%{transform:translateY(-8px)}}

/* MODALS */
.overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:500;backdrop-filter:blur(8px);align-items:flex-end;justify-content:center}
.overlay.open{display:flex}
.modal{
  background:var(--s2);border:1px solid var(--bd);border-radius:22px 22px 0 0;
  padding:24px;width:100%;max-width:520px;
  animation:mSlide .28s cubic-bezier(.16,1,.3,1);
  padding-bottom:max(24px,env(safe-area-inset-bottom));
}
@keyframes mSlide{from{transform:translateY(100%)}to{transform:none}}
.modal h3{font-size:16px;font-weight:900;margin-bottom:16px;color:#fff;text-align:center}
.modal-input{
  width:100%;padding:12px 14px;border-radius:10px;border:1px solid var(--bd);
  background:var(--s1);color:var(--tx);font-size:14px;outline:none;
  margin-bottom:12px;transition:.2s;
}
.modal-input:focus{border-color:var(--ac)}
.modal-btns{display:flex;gap:8px}

/* ALERT DRAWER */
.alert-banner{position:fixed;top:60px;left:8px;right:8px;z-index:600;display:flex;flex-direction:column;gap:6px;pointer-events:none}
.alert-item-banner{
  pointer-events:auto;background:var(--s2);border:1px solid var(--bd);
  border-left:4px solid var(--bl);border-radius:12px;padding:11px 14px;
  font-size:12px;box-shadow:0 10px 30px rgba(0,0,0,.5);
  animation:aSlide .3s ease;display:flex;gap:8px;align-items:flex-start;
}
.alert-item-banner.error{border-left-color:var(--rd)}
.alert-item-banner.warn{border-left-color:var(--yw)}
@keyframes aSlide{from{transform:translateY(-16px);opacity:0}to{transform:none;opacity:1}}
.aib-x{margin-left:auto;cursor:pointer;color:var(--mu);border:none;background:transparent;font-size:16px;padding:0 2px}

.drawer-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:700}
.drawer-overlay.show{display:block}
.drawer{
  position:fixed;top:0;right:-100%;width:min(420px,94vw);height:100%;
  background:var(--s1);z-index:701;
  transition:right .28s cubic-bezier(.16,1,.3,1);
  box-shadow:-12px 0 50px rgba(0,0,0,.5);display:flex;flex-direction:column;
}
.drawer.show{right:0}
.drawer-head{display:flex;justify-content:space-between;align-items:center;padding:18px;border-bottom:1px solid var(--bd)}
.drawer-body{flex:1;overflow-y:auto;padding:12px}
.alert-list-item{
  background:var(--s2);border-left:3px solid var(--bd);border-radius:10px;
  padding:12px;margin-bottom:8px;font-size:12px;
}
.alert-list-item.error{border-left-color:var(--rd)}
.alert-list-item.warn{border-left-color:var(--yw)}
.alert-list-item.info{border-left-color:var(--bl)}
.ali-title{font-weight:700;margin-bottom:3px}
.ali-time{font-size:10px;color:var(--mu);margin-top:4px}

/* FILE TEST PANEL */
.test-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:800;backdrop-filter:blur(6px)}
.test-overlay.show{display:block}
.test-panel{
  display:none;position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);
  width:min(500px,94vw);max-height:82vh;
  background:var(--s1);border:1px solid var(--bd);border-radius:18px;
  z-index:801;flex-direction:column;box-shadow:0 30px 80px rgba(0,0,0,.7);
}
.test-panel.show{display:flex}
.test-body{flex:1;overflow-y:auto;padding:14px}
.test-sec{background:var(--s2);border-radius:10px;padding:12px;margin-bottom:9px;border-left:3px solid var(--bd)}
.test-sec.ok{border-left-color:var(--gn)}.test-sec.bad{border-left-color:var(--rd)}

/* SETTINGS */
.set-card{background:var(--s2);border:1px solid var(--bd);border-radius:14px;padding:14px;margin-bottom:10px}
.set-title{font-size:13px;font-weight:800;color:#fff;margin-bottom:12px;display:flex;align-items:center;gap:6px}
.set-row{
  display:flex;align-items:center;justify-content:space-between;
  padding:10px 0;border-bottom:1px solid rgba(255,255,255,.04);gap:8px;
}
.set-row:last-child{border-bottom:none;padding-bottom:0}
.set-row-label{font-size:13px;flex:1}
.set-row-sub{font-size:10px;color:var(--mu);margin-top:2px}
.set-input{
  padding:7px 10px;border-radius:8px;border:1px solid var(--bd);
  background:var(--s1);color:var(--tx);font-size:12px;outline:none;
  max-width:160px;transition:.2s;width:100%;
}
.set-input:focus{border-color:var(--ac)}
.toggle{position:relative;width:44px;height:24px;flex-shrink:0}
.toggle input{display:none}
.toggle-track{position:absolute;inset:0;background:var(--dim);border-radius:99px;cursor:pointer;transition:.3s}
.toggle input:checked+.toggle-track{background:var(--gn)}
.toggle-thumb{position:absolute;top:3px;left:3px;width:18px;height:18px;background:#fff;border-radius:50%;transition:.3s;pointer-events:none}
.toggle input:checked~.toggle-thumb{transform:translateX(20px)}
.mongo-status-badge{
  display:inline-flex;align-items:center;gap:6px;padding:6px 12px;
  border-radius:99px;font-size:12px;font-weight:700;border:1px solid var(--bd);
}
.mongo-status-badge.ok{border-color:rgba(0,245,160,.3);color:var(--gn);background:rgba(0,245,160,.07)}
.mongo-status-badge.err{border-color:rgba(255,85,114,.3);color:var(--rd);background:rgba(255,85,114,.07)}

/* Cookie textarea */
.cookie-area{
  width:100%;height:80px;background:var(--s1);border:1px solid var(--bd);
  border-radius:10px;padding:10px;color:var(--tx);font-family:'Courier New',monospace;
  font-size:11px;resize:none;outline:none;transition:.2s;line-height:1.5;margin:10px 0;
}
.cookie-area:focus{border-color:var(--ac)}
#envEd{
  width:100%;height:240px;background:#03030e;border:1px solid var(--bd);
  border-radius:10px;padding:12px;color:#e6edf3;font-family:'Courier New',monospace;
  font-size:13px;line-height:1.8;resize:vertical;outline:none;margin-bottom:12px;transition:.2s;
}
#envEd:focus{border-color:var(--ac)}

/* History */
.hist-list{display:flex;flex-direction:column;gap:5px;max-height:200px;overflow-y:auto}
.hist-row{display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--s2);border-radius:8px;border:1px solid var(--bd);font-size:10.5px}
.hist-date{color:var(--mu);flex:1}
.hist-up{color:var(--gn);font-weight:700}
.hist-code{color:var(--yw)}

/* OWNER CARD */
.owner-card{
  background:linear-gradient(135deg,#120f28,#1e1640);
  border:1px solid rgba(160,100,255,.2);border-radius:18px;padding:18px;margin-bottom:12px;
  box-shadow:0 0 40px rgba(124,110,255,.1);
}
.owner-crown{text-align:center;font-size:13px;font-weight:900;color:#e0b840;text-shadow:0 0 12px rgba(224,184,64,.4);margin-bottom:14px;letter-spacing:.5px}
.owner-row{display:flex;justify-content:space-between;padding:7px 2px;font-size:12.5px;border-bottom:1px solid rgba(255,255,255,.04)}
.owner-row:last-child{border-bottom:none}
.owner-k{color:var(--mu)}.owner-v{color:var(--tx);font-weight:600;text-align:right}
.owner-sep{text-align:center;font-size:10px;color:#8870c0;margin:12px 0 8px;letter-spacing:1.5px;text-transform:uppercase}

/* SEARCH RESULTS */
.search-result{padding:9px 12px;background:var(--s2);border:1px solid var(--bd);border-radius:9px;margin-bottom:5px;cursor:pointer;transition:.12s}
.search-result:hover{background:var(--s3);border-color:rgba(124,110,255,.2)}
.sr-path{font-size:11px;color:var(--ac);margin-bottom:2px;font-weight:600}
.sr-meta{font-size:10.5px;color:var(--mu)}

/* MULTI-UPLOAD AREA */
.multi-up{border:1px dashed var(--bd);border-radius:12px;padding:16px;background:var(--s2);margin-bottom:12px}

/* BOTTOM NAVIGATION */
.navtabs{
  position:fixed;bottom:0;left:0;right:0;z-index:200;
  background:rgba(5,5,16,.95);backdrop-filter:blur(24px);
  border-top:1px solid var(--bd);
  display:grid;grid-template-columns:repeat(7,1fr);
  height:60px;padding-bottom:env(safe-area-inset-bottom);
}
.nav-tab{
  display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:2px;cursor:pointer;border:none;background:transparent;
  color:var(--mu);transition:.15s;position:relative;padding:0;
}
.nav-tab.active{color:var(--ac)}
.nav-tab.active::before{
  content:"";position:absolute;top:0;left:50%;transform:translateX(-50%);
  width:32px;height:2px;background:var(--ac);border-radius:0 0 3px 3px;
}
.nav-icon{font-size:18px;line-height:1}
.nav-label{font-size:8.5px;font-weight:700;letter-spacing:.2px}

/* Responsive scrollbars */
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-thumb{background:var(--s4);border-radius:2px}
::-webkit-scrollbar-track{background:transparent}
</style>
</head>
<body>

<!-- Ambient background -->
<div class="ambient"><div class="amb-orb amb1"></div><div class="amb-orb amb2"></div></div>

<!-- TOP BAR -->
<div class="topbar">
  <div class="top-logo" id="topLogo">🤖</div>
  <div class="top-name">${pname}</div>
  <div class="top-right">
    <div class="stat-pill" id="botPill">
      <div class="stat-dot" id="tDot"></div>
      <span id="tStatus">সংযোগ...</span>
    </div>
    <div class="stat-pill" id="mongoPill" style="display:none">
      <div class="stat-dot" id="mDot"></div>
      <span id="mStatus">DB</span>
    </div>
    <button class="icon-btn" onclick="openAlerts()" title="নোটিফিকেশন">
      🔔<span class="badge" id="bellBadge">0</span>
    </button>
    <button class="icon-btn" onclick="location.href='/logout'" title="লগআউট">🚪</button>
  </div>
</div>

<!-- TOAST CONTAINER -->
<div class="toast-wrap" id="toastWrap"></div>

<!-- ALERT BANNER -->
<div class="alert-banner" id="alertBanner"></div>

<!-- ALERT DRAWER -->
<div class="drawer-overlay" id="drawerOverlay" onclick="closeAlerts()"></div>
<div class="drawer" id="alertDrawer">
  <div class="drawer-head">
    <span style="font-weight:800;font-size:15px">🔔 নোটিফিকেশন</span>
    <div style="display:flex;gap:6px">
      <button class="tbtn" onclick="clearAlerts()">🗑 মুছো</button>
      <button class="tbtn" onclick="closeAlerts()">✕</button>
    </div>
  </div>
  <div class="drawer-body" id="alertList"></div>
</div>

<!-- FILE TEST OVERLAY -->
<div class="test-overlay" id="testOverlay" onclick="closeTest()"></div>
<div class="test-panel" id="testPanel">
  <div class="drawer-head">
    <span style="font-weight:800;font-size:15px">🧪 ফাইল টেস্ট রিপোর্ট</span>
    <button class="tbtn" onclick="closeTest()">✕ বন্ধ</button>
  </div>
  <div class="test-body" id="testBody"></div>
</div>

<!-- MAIN CONTENT -->
<div class="main">

<!-- ── HOME ── -->
<div class="page active" id="pg-home">

  <!-- Bot Status -->
  <div class="bot-state stopped" id="botStateCard">
    <div class="bot-state-icon" id="botStateIcon">⏳</div>
    <div class="bot-state-info">
      <div class="bot-state-txt" id="sTxt">সংযোগ হচ্ছে...</div>
      <div class="bot-state-sub" id="sUp"></div>
    </div>
    <div class="stat-dot starting" id="sDot"></div>
  </div>

  <!-- Cookie Box -->
  <div class="card card-glow" style="margin-bottom:12px">
    <div style="font-size:13px;font-weight:800;margin-bottom:4px">🍪 Facebook Cookie / Appstate</div>
    <div style="font-size:11px;color:var(--mu);margin-bottom:6px">Cookie অথবা appstate.json paste করুন</div>
    <div id="cookieStatus" style="display:none;font-size:12px;padding:7px 10px;border-radius:8px;margin-bottom:8px"></div>
    <textarea class="cookie-area" id="cookieInput" placeholder='[{"key":"c_user","value":"..."}] অথবা plain cookie string'></textarea>
    <button class="btn btn-start" onclick="saveCookie()">✅ Cookie সেভ ও বট চালু করুন</button>
  </div>

  <!-- Control Buttons -->
  <div class="card">
    <div class="btn-grid2">
      <button class="btn btn-start" onclick="botAct('start')">▶ চালু করো</button>
      <button class="btn btn-stop" onclick="botAct('stop')">⏹ বন্ধ করো</button>
    </div>
    <div class="btn-grid3">
      <button class="btn btn-restart" onclick="botAct('restart')">🔄 রিস্টার্ট</button>
      <button class="btn btn-npm" onclick="npmInst()">📦 npm</button>
      <button class="btn btn-backup" onclick="doBackup()">💾 Backup</button>
    </div>
    <div style="display:flex;gap:8px;margin-top:8px">
      <button class="btn btn-ghost" onclick="mongoSync()" style="flex:1">☁️ Sync MongoDB</button>
      <button class="btn btn-ghost" onclick="mongoRestore()" style="flex:1">🔄 Restore</button>
    </div>
  </div>

  <!-- Stats Grid -->
  <div class="stat-grid">
    <div class="stat-cell"><div class="stat-icon">💾</div><div class="stat-val" id="cMem">--</div><div class="stat-lbl">Panel RAM MB</div></div>
    <div class="stat-cell"><div class="stat-icon">⏱️</div><div class="stat-val sm" id="cSup">--</div><div class="stat-lbl">Server Uptime</div></div>
    <div class="stat-cell"><div class="stat-icon">📦</div><div class="stat-val" id="cFiles">--</div><div class="stat-lbl">বট ফাইল</div></div>
    <div class="stat-cell"><div class="stat-icon">🚀</div><div class="stat-val" id="cStarts">--</div><div class="stat-lbl">মোট Start</div></div>
  </div>
  <div class="stat-grid s3">
    <div class="stat-cell"><div class="stat-icon">💥</div><div class="stat-val" id="cCrash">--</div><div class="stat-lbl">Crash</div></div>
    <div class="stat-cell"><div class="stat-icon">🕐</div><div class="stat-val sm" id="cTup">--</div><div class="stat-lbl">মোট Uptime</div></div>
    <div class="stat-cell"><div class="stat-icon">🖥️</div><div class="stat-val sm" id="cNode">--</div><div class="stat-lbl">Node.js</div></div>
  </div>

  <!-- Restart History -->
  <div class="card">
    <div class="sec-title">📈 Restart ইতিহাস</div>
    <div class="hist-list" id="histList"><div style="font-size:12px;color:var(--mu);text-align:center;padding:12px">লোড হচ্ছে...</div></div>
  </div>
</div>

<!-- ── MONITOR ── -->
<div class="page" id="pg-monitor">
  <div class="card card-glow">
    <div class="sec-title" style="margin-bottom:14px">🖥️ RAM ব্যবহার <span class="mon-badge info">সীমা ৫১২MB</span></div>
    <div id="mHeavyRow" style="display:none">
      <div class="mon-bar-wrap">
        <div class="mon-bar-header"><span class="mon-bar-label">⬇️ সক্রিয় ডাউনলোড</span><span class="mon-bar-val" id="mHeavyTxt">--</span></div>
        <div class="mon-track"><div class="mon-fill" id="mHeavyBar" style="width:0%;background:var(--bl)"></div></div>
      </div>
    </div>
    <div class="mon-bar-wrap">
      <div class="mon-bar-header"><span class="mon-bar-label">🧩 প্যানেল</span><span class="mon-bar-val" id="mPanelTxt">-- MB</span></div>
      <div class="mon-track"><div class="mon-fill" id="mPanelBar" style="width:0%"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ: <b id="mPanelPeak">--</b></div>
    </div>
    <div class="mon-bar-wrap">
      <div class="mon-bar-header"><span class="mon-bar-label">🤖 বট</span><span class="mon-bar-val" id="mBotTxt">-- MB</span></div>
      <div class="mon-track"><div class="mon-fill" id="mBotBar" style="width:0%"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ: <b id="mBotPeak">--</b></div>
    </div>
    <div class="mon-bar-wrap" style="margin-bottom:0">
      <div class="mon-bar-header"><span class="mon-bar-label">⚡ মোট</span><span class="mon-bar-val" id="mTotalTxt">-- MB</span></div>
      <div class="mon-track"><div class="mon-fill" id="mTotalBar" style="width:0%"></div></div>
    </div>
  </div>

  <div class="card card-glow">
    <div class="sec-title" style="margin-bottom:14px">🗄️ MongoDB Atlas <span class="mon-badge info">M0 সীমা ৫১২MB</span></div>
    <div class="mon-bar-wrap">
      <div class="mon-bar-header"><span class="mon-bar-label">💽 ব্যবহৃত</span><span class="mon-bar-val" id="mMongoTxt">-- MB</span></div>
      <div class="mon-track"><div class="mon-fill" id="mMongoBar" style="width:0%"></div></div>
      <div class="mon-peak">📈 সর্বোচ্চ: <b id="mMongoPeak">--</b></div>
    </div>
    <div class="stat-grid s3" style="margin-top:12px;margin-bottom:0">
      <div class="stat-cell"><div class="stat-icon">📄</div><div class="stat-val sm" id="mMongoObjs">--</div><div class="stat-lbl">এন্ট্রি</div></div>
      <div class="stat-cell"><div class="stat-icon">💾</div><div class="stat-val sm" id="mMongoData">--</div><div class="stat-lbl">Data MB</div></div>
      <div class="stat-cell"><div class="stat-icon">🔎</div><div class="stat-val sm" id="mMongoIdx">--</div><div class="stat-lbl">Index MB</div></div>
    </div>
  </div>

  <div class="card card-glow">
    <div class="sec-title" style="margin-bottom:14px">⏳ লাইফটাইম সামারি <span class="mon-badge ok">MongoDB-তে স্থায়ী</span></div>
    <div class="stat-grid s3" style="margin-bottom:0">
      <div class="stat-cell"><div class="stat-icon">🚀</div><div class="stat-val sm" id="mLtStarts">--</div><div class="stat-lbl">মোট Start</div></div>
      <div class="stat-cell"><div class="stat-icon">💥</div><div class="stat-val sm" id="mLtCrashes">--</div><div class="stat-lbl">Crash</div></div>
      <div class="stat-cell"><div class="stat-icon">🕒</div><div class="stat-val sm" id="mLtUptime">--</div><div class="stat-lbl">Uptime</div></div>
    </div>
    <div style="font-size:10px;color:var(--mu);margin-top:10px">📅 যবে থেকে: <b id="mLtSince" style="color:var(--bl)">--</b></div>
  </div>
</div>

<!-- ── TERMINAL ── -->
<div class="page" id="pg-term">
  <div class="terminal">
    <div class="term-topbar">
      <span class="term-dot r"></span><span class="term-dot y"></span><span class="term-dot g"></span>
      <span class="term-title">root@bot-panel:~# monitor.sh</span>
    </div>
    <div class="term-body">
      <div class="t-glitch" data-t="BOT PANEL v2.0">BOT PANEL v2.0</div>
      <div class="t-line t-dim">[<span id="hkTime">--:--:--</span>] secure link <span style="color:#33ff66">✓</span></div>
      <div class="t-line t-dim">[sys] mongo ......... <span style="color:#33ff66">OK</span></div>
      <div class="t-line t-dim">[sys] ipc ........... <span style="color:#33ff66">OK</span></div>
      <div class="t-sep"></div>
      <div class="t-line">🌐 NET ⬇ <b id="hkRx">0.0</b> KB/s &nbsp; ⬆ <b id="hkTx">0.0</b> KB/s</div>
      <div class="t-bar-row"><div class="t-bar"><div class="t-bar-fill t-net" id="hkNetBar" style="width:0%"></div></div></div>
      <div class="t-line">🧠 CPU <b id="hkCpu">0</b>%</div>
      <div class="t-bar-row"><div class="t-bar"><div class="t-bar-fill t-cpu" id="hkCpuBar" style="width:0%"></div></div></div>
      <div class="t-line">💾 RAM <b id="hkRam">0</b>% <span class="t-dim">(512MB)</span></div>
      <div class="t-bar-row"><div class="t-bar"><div class="t-bar-fill t-ram" id="hkRamBar" style="width:0%"></div></div></div>
      <div class="t-sep"></div>
      <div class="t-line">🤖 BOT: <b id="hkBotSt" style="color:#ff5566">OFFLINE</b></div>
      <div class="t-line">⏱️ UPTIME: <b id="hkUptime">00:00:00</b></div>
      <div class="t-sep"></div>
      <div class="t-line t-dim">$ tail -f live.log</div>
      <div id="hkTail"></div>
      <div class="t-line t-dim">&gt; <span class="t-cursor">▋</span></div>
    </div>
  </div>
</div>

<!-- ── LOGS ── -->
<div class="page" id="pg-logs">
  <div class="log-controls">
    <button class="lf-btn on" onclick="setLF('all',this)">📋 সব</button>
    <button class="lf-btn" onclick="setLF('success',this)">✅ সফল</button>
    <button class="lf-btn" onclick="setLF('error',this)">❌ ত্রুটি</button>
    <button class="lf-btn" onclick="setLF('warn',this)">⚠️ সতর্ক</button>
    <button class="lf-btn" onclick="clearLogs()">🗑 মুছো</button>
    <button class="lf-btn" onclick="window.open('/api/bot/downloadlog')">⬇️ ডাউনলোড</button>
    <button class="lf-btn" onclick="autoScroll=!autoScroll;this.textContent=autoScroll?'↓ Auto':'↕ Manual'">↓ Auto</button>
    <button class="lf-btn" onclick="toggleLogFull()">⛶ Full</button>
  </div>
  <div class="logbox" id="lbox"></div>
</div>

<!-- ── FILES ── -->
<div class="page" id="pg-files">
  <!-- Editor view -->
  <div id="edView" style="display:none">
    <div class="editor-header">
      <button class="tbtn" onclick="closeEd()">← ফিরে</button>
      <span class="ed-filename" id="edFn"></span>
      <span class="ed-lang-badge" id="edLang"></span>
      <button class="tbtn" onclick="testFile()">🧪</button>
      <button class="tbtn primary" onclick="saveFile()">💾 সেভ</button>
      <button class="tbtn" onclick="dlF(curEdit)">⬇️</button>
    </div>
    <div class="editor-wrap" id="edWrap">
      <pre id="cedHl" aria-hidden="true"><code></code></pre>
      <textarea id="ced" spellcheck="false" oninput="onEdInput()" onscroll="syncEdScroll()"></textarea>
    </div>
  </div>

  <!-- File manager view -->
  <div id="fmView">
    <div class="pathbar" id="pathBar">📁 root</div>
    <div class="fm-summary" id="fmSummary"></div>
    <div class="fm-toolbar">
      <button class="tbtn primary" onclick="showM('mkdir')">📁 +</button>
      <button class="tbtn primary" onclick="showM('newfile')">📄 +</button>
      <button class="tbtn" onclick="loadFiles(curDir)">🔄</button>
      <button class="tbtn" onclick="editF('package.json')">📋 pkg</button>
      <button class="tbtn" onclick="editF('index.js')">📜 index</button>
      <button class="tbtn" onclick="editF('.env')">🔐 .env</button>
    </div>
    <input class="search-input" type="text" id="fq" placeholder="🔍 ফাইল খোঁজুন..." oninput="doFS()">
    <div id="fsRes" style="display:none;margin-bottom:10px"></div>
    <div class="file-list" id="flist"></div>
  </div>
</div>

<!-- ── UPLOAD ── -->
<div class="page" id="pg-upload">
  <div class="upload-zone" id="upZone" onclick="document.getElementById('fInp').click()">
    <div class="uz-icon">📦</div>
    <div style="font-size:15px;font-weight:800;margin-bottom:6px">ZIP আপলোড</div>
    <div style="color:var(--mu);font-size:12px">ক্লিক বা ড্র্যাগ করুন — অটো extract + MongoDB</div>
    <div style="color:var(--bl);font-size:11px;margin-top:8px;font-weight:600">সর্বোচ্চ ৫০০MB</div>
  </div>
  <input type="file" id="fInp" accept=".zip" style="display:none" onchange="uploadF(this.files[0])">

  <div class="prog-wrap" id="progWrap">
    <div class="prog-label"><span id="upFN">আপলোড হচ্ছে...</span><span id="upPct">0%</span></div>
    <div class="prog-track"><div class="prog-fill" id="progBar"></div></div>
    <div class="prog-status" id="upSt"></div>
  </div>

  <div class="multi-up">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">📄 একক ফাইল</div>
    <input type="file" id="singleInp" style="display:none" onchange="uploadSingle(this.files[0])">
    <button class="tbtn primary" onclick="document.getElementById('singleInp').click()">📄 ফাইল বেছে নিন</button>
    <div id="singleStatus" style="font-size:12px;color:var(--mu);margin-top:8px"></div>
  </div>

  <div class="multi-up">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">📂 একাধিক ফাইল</div>
    <input type="file" id="multiInp" multiple style="display:none" onchange="uploadMulti(this.files)">
    <button class="tbtn primary" onclick="document.getElementById('multiInp').click()">📂 ফাইল বেছে নিন</button>
    <div id="multiStatus" style="font-size:12px;color:var(--mu);margin-top:8px"></div>
  </div>

  <div class="card" style="font-size:12px">
    <div style="color:var(--mu);margin-bottom:5px">📁 আপলোড হবে:</div>
    <div style="color:var(--ac);font-weight:700">/bot/<span id="uploadDir">root</span></div>
  </div>
</div>

<!-- ── MORE ── -->
<div class="page" id="pg-more">
  <!-- Owner -->
  <div class="owner-card">
    <div class="owner-crown">👑 MASTER BELAL NETWORK 👑</div>
    <div class="owner-row"><span class="owner-k">👤 Name</span><span class="owner-v">BELAL YT ✅</span></div>
    <div class="owner-row"><span class="owner-k">🎭 Nick</span><span class="owner-v">চাঁদের পাহাড়</span></div>
    <div class="owner-row"><span class="owner-k">🏡 Address</span><span class="owner-v">Kurigram, BD 🇧🇩</span></div>
    <div class="owner-row"><span class="owner-k">💼 Job</span><span class="owner-v">Bot Developer</span></div>
    <div class="owner-sep">🔗 যোগাযোগ</div>
    <div class="owner-row"><span class="owner-k">📞 WhatsApp</span><span class="owner-v">01913246554</span></div>
    <div class="owner-sep">⚡ এই প্যানেল</div>
    <div class="owner-row"><span class="owner-k">🏗️ Version</span><span class="owner-v" style="font-family:monospace;font-size:11px">${BUILD_VER}</span></div>
  </div>

  <!-- ENV -->
  <div class="set-card">
    <div class="set-title">⚙️ Environment (.env)</div>
    <textarea id="envEd" spellcheck="false" placeholder="TOKEN=xxx&#10;COOKIE=xxx&#10;PREFIX=!&#10;ADMIN_ID=123&#10;MONGO_URL=xxx"></textarea>
    <div style="display:flex;gap:8px">
      <button class="tbtn primary" onclick="saveEnv()">💾 সেভ</button>
      <button class="tbtn" onclick="loadEnv()">🔄 রিলোড</button>
    </div>
  </div>

  <!-- MongoDB -->
  <div class="set-card">
    <div class="set-title">☁️ MongoDB</div>
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
      <div class="mongo-status-badge" id="mongoBadge">চেক...</div>
      <div style="font-size:11px;color:var(--mu)" id="mongoInfo"></div>
    </div>
    <div style="display:flex;gap:8px">
      <button class="tbtn primary" onclick="mongoSync()">☁️ Sync</button>
      <button class="tbtn" onclick="mongoRestore()">🔄 Restore</button>
    </div>
  </div>

  <!-- Settings -->
  <div class="set-card">
    <div class="set-title">🔧 সেটিংস</div>
    <div class="set-row">
      <div><div class="set-row-label">প্যানেলের নাম</div></div>
      <input class="set-input" type="text" id="sName" placeholder="${pname}">
    </div>
    <div class="set-row">
      <div><div class="set-row-label">Site URL</div><div class="set-row-sub">ঘুম এড়ানোর জন্য</div></div>
      <input class="set-input" type="text" id="sSiteUrl" placeholder="https://xxx.onrender.com">
    </div>
    <div class="set-row">
      <div><div class="set-row-label">Auto Restart</div><div class="set-row-sub">সবসময় চালু</div></div>
      <label class="toggle"><input type="checkbox" id="sAR" checked disabled><div class="toggle-track"></div><div class="toggle-thumb"></div></label>
    </div>
    <div class="set-row">
      <div><div class="set-row-label">Schedule Restart</div></div>
      <label class="toggle"><input type="checkbox" id="sSched"><div class="toggle-track"></div><div class="toggle-thumb"></div></label>
    </div>
    <div class="set-row">
      <div><div class="set-row-label">Restart সময়</div></div>
      <input class="set-input" type="time" id="sTime" value="03:00">
    </div>
    <div style="margin-top:12px"><button class="tbtn primary" onclick="saveSettings()">💾 সেভ</button></div>
  </div>

  <!-- Password -->
  <div class="set-card">
    <div class="set-title">🔐 পাসওয়ার্ড বদলান</div>
    <div class="set-row"><span class="set-row-label">বর্তমান</span><input class="set-input" type="password" id="sCur" placeholder="বর্তমান পাসওয়ার্ড"></div>
    <div class="set-row"><span class="set-row-label">নতুন</span><input class="set-input" type="password" id="sNew" placeholder="নতুন (৪+ অক্ষর)"></div>
    <div style="margin-top:12px"><button class="tbtn primary" onclick="changePw()">🔐 পরিবর্তন করুন</button></div>
  </div>

  <!-- Maintenance -->
  <div class="set-card">
    <div class="set-title">🛠️ Maintenance</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="tbtn" onclick="doBackup()">💾 Backup</button>
      <button class="tbtn danger" onclick="clearLogFile()">🗑 Log মুছুন</button>
      <button class="tbtn danger" onclick="doReset()">🔄 Reset</button>
      <button class="tbtn" onclick="location.href='/logout'">🚪 লগআউট</button>
    </div>
  </div>
</div>

</div><!-- /main -->

<!-- BOTTOM NAVIGATION -->
<div class="navtabs">
  <button class="nav-tab active" onclick="goTab('home',this)"><span class="nav-icon">🏠</span><span class="nav-label">হোম</span></button>
  <button class="nav-tab" onclick="goTab('monitor',this)"><span class="nav-icon">📊</span><span class="nav-label">মনিটর</span></button>
  <button class="nav-tab" onclick="goTab('term',this)"><span class="nav-icon">⚡</span><span class="nav-label">Terminal</span></button>
  <button class="nav-tab" onclick="goTab('logs',this)"><span class="nav-icon">📋</span><span class="nav-label">লগ</span></button>
  <button class="nav-tab" onclick="goTab('files',this)"><span class="nav-icon">📁</span><span class="nav-label">ফাইল</span></button>
  <button class="nav-tab" onclick="goTab('upload',this)"><span class="nav-icon">⬆️</span><span class="nav-label">আপলোড</span></button>
  <button class="nav-tab" onclick="goTab('more',this)"><span class="nav-icon">⚙️</span><span class="nav-label">আরো</span></button>
</div>

<!-- MODALS -->
<div class="overlay" id="mod-mkdir" onclick="if(event.target===this)closeM('mkdir')">
  <div class="modal"><h3>📁 নতুন ফোল্ডার</h3><input class="modal-input" type="text" id="mkN" placeholder="ফোল্ডারের নাম"><div class="modal-btns"><button class="tbtn" onclick="closeM('mkdir')" style="flex:1">বাতিল</button><button class="tbtn primary" onclick="doMkdir()" style="flex:1">তৈরি করুন</button></div></div>
</div>
<div class="overlay" id="mod-newfile" onclick="if(event.target===this)closeM('newfile')">
  <div class="modal"><h3>📄 নতুন ফাইল</h3><input class="modal-input" type="text" id="nfN" placeholder="test.js বা commands/mycommand.js"><div class="modal-btns"><button class="tbtn" onclick="closeM('newfile')" style="flex:1">বাতিল</button><button class="tbtn primary" onclick="doNewFile()" style="flex:1">তৈরি করুন</button></div></div>
</div>
<div class="overlay" id="mod-rename" onclick="if(event.target===this)closeM('rename')">
  <div class="modal"><h3>✏️ নাম পরিবর্তন</h3><input class="modal-input" type="text" id="rnV" placeholder="নতুন নাম"><div class="modal-btns"><button class="tbtn" onclick="closeM('rename')" style="flex:1">বাতিল</button><button class="tbtn primary" onclick="doRename()" style="flex:1">পরিবর্তন</button></div></div>
</div>
<div class="overlay" id="mod-copy" onclick="if(event.target===this)closeM('copy')">
  <div class="modal"><h3>📋 Copy করুন</h3><input class="modal-input" type="text" id="cpTo" placeholder="destination path"><div class="modal-btns"><button class="tbtn" onclick="closeM('copy')" style="flex:1">বাতিল</button><button class="tbtn primary" onclick="doCopy()" style="flex:1">Copy</button></div></div>
</div>

<!-- ═══════════════ JAVASCRIPT ═══════════════ -->
<script>
// ── GLOBAL SAFETY ──
function _escSafe(t){return String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
window.addEventListener('error',function(e){
  const m=(e.message||'').toLowerCase();
  if(m.includes('network')||m.includes('fetch')||m.includes('failed to fetch')||m.includes('websocket')||m.includes('load'))return;
  console.error('[panel error]',e.message,e.filename,e.lineno);
});
window.addEventListener('unhandledrejection',function(e){
  const r=(e.reason&&(e.reason.message||String(e.reason)))||'';
  if(r.toLowerCase().includes('abort')||r.toLowerCase().includes('fetch')||r.toLowerCase().includes('websocket'))return;
  console.error('[unhandled]',r);
});

// ── STATE ──
let curDir='', curEdit='', renameFrom='', copyFrom='', logFilter='all', autoScroll=true;
let ws, _wsConnected=false, _botRunning=false, _botUpSec=0;
let _alertsCache=[], _unreadAlerts=0, _refreshFails=0;
let _hackTimer=null;

// ── TOAST ──
function toast(msg,type='success'){
  const w=document.getElementById('toastWrap');
  const el=document.createElement('div');
  el.className='toast '+type;el.textContent=msg;w.appendChild(el);
  setTimeout(()=>{el.style.opacity='0';el.style.transition='.3s';setTimeout(()=>el.remove(),300);},4000);
}

// ── ESC ──
function esc(t){return String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

// ── FORMAT HELPERS ──
function fmtT(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h>0?h+'h '+m+'m':m>0?m+'m '+sc+'s':sc+'s';}
function fsz(b){if(!b||b===0)return'—';if(b<1024)return b+'B';if(b<1048576)return(b/1024).toFixed(1)+'KB';return(b/1048576).toFixed(1)+'MB';}
function fdt(d){
  try{
    const diff=Math.floor((Date.now()-new Date(d).getTime())/1000);
    if(diff<0||isNaN(diff))return new Date(d).toLocaleDateString('bn-BD');
    if(diff<10)return'এইমাত্র';if(diff<60)return diff+'সে আগে';
    const m=Math.floor(diff/60);if(m<60)return m+'মি আগে';
    const h=Math.floor(m/60);if(h<24)return h+'ঘ আগে';
    const dy=Math.floor(h/24);if(dy===1)return'গতকাল';if(dy<7)return dy+'দিন আগে';
    return new Date(d).toLocaleDateString('bn-BD',{month:'short',day:'numeric'});
  }catch{return'';}
}
function _fmtLt(s){const d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);if(d>0)return d+'দ '+h+'ঘ';if(h>0)return h+'ঘ '+m+'মি';return m+'মি';}
setInterval(()=>{document.querySelectorAll('[data-mtime]').forEach(el=>{const m=el.getAttribute('data-mtime');if(m)el.textContent=fdt(m);});},30000);
setInterval(()=>{if(!_botRunning)return;_botUpSec++;const el=document.getElementById('sUp');if(el)el.textContent='⏱ চলছে: '+fmtT(_botUpSec);},1000);

// ── TABS ──
function goTab(id,btn){
  document.querySelectorAll('.nav-tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active');
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('pg-'+id).classList.add('active');
  if(id!=='term'){clearInterval(_hackTimer);_hackTimer=null;}
  if(id==='files')loadFiles(curDir);
  if(id==='more'){loadEnv();loadSettings();}
  if(id==='logs')document.getElementById('lbox').scrollTop=document.getElementById('lbox').scrollHeight;
  if(id==='upload')document.getElementById('uploadDir').textContent=curDir||'root';
  if(id==='monitor')loadMonitor();
  if(id==='term'){loadTerminal();_hackTimer=setInterval(loadTerminal,1500);}
}

// ── WEBSOCKET ──
function connectWS(){
  const proto=location.protocol==='https:'?'wss':'ws';
  const ts=document.getElementById('tStatus');if(ts&&!_wsConnected)ts.textContent='সংযোগ...';
  ws=new WebSocket(proto+'://'+location.host);
  ws.onopen=()=>{
    _wsConnected=true;
    // WS connect হলেই status pill update করো — refresh() শেষ হওয়ার আগেই
    const ts=document.getElementById('tStatus');
    if(ts&&ts.textContent==='সংযোগ...')ts.textContent='সংযুক্ত';
    refresh(); // background এ full data আনো
  };
  ws.onmessage=e=>{
    try{
      const m=JSON.parse(e.data);
      if(m.type==='log')appendLog(m.data);
      if(m.type==='logs'){document.getElementById('lbox').innerHTML='';m.data.forEach(appendLog);}
      if(m.type==='status')updateBotStatus(m.ready||(m.running&&!m.starting),m.running&&!m.ready&&!m.starting);
      if(m.type==='clearLogs')document.getElementById('lbox').innerHTML='';
      if(m.type==='alert'){showAlertBanner(m.data);_alertsCache.unshift(m.data);_unreadAlerts++;updateBell();}
      if(m.type==='mongo')updateMongo(m.connected);
    }catch(e){}
  };
  ws.onerror=()=>{_wsConnected=false;};
  ws.onclose=()=>{_wsConnected=false;setTimeout(connectWS,3000);};
}

// ── STATUS UPDATE ──
function updateBotStatus(running,starting){
  _botRunning=running;if(!running)_botUpSec=0;
  const card=document.getElementById('botStateCard');
  const icon=document.getElementById('botStateIcon');
  const logo=document.getElementById('topLogo');
  const sDot=document.getElementById('sDot');
  const tDot=document.getElementById('tDot');
  const sTxt=document.getElementById('sTxt');
  const tStatus=document.getElementById('tStatus');
  if(running){
    card&&(card.className='bot-state running');
    icon&&(icon.textContent='✅');
    logo&&(logo.className='top-logo live');
    [sDot,tDot].forEach(d=>{if(d){d.className='stat-dot on';}});
    sTxt&&(sTxt.textContent='✅ বট চলছে');
    tStatus&&(tStatus.textContent='✅ চলছে');
  }else if(starting){
    card&&(card.className='bot-state starting');
    icon&&(icon.textContent='🟡');
    logo&&(logo.className='top-logo starting');
    [sDot,tDot].forEach(d=>{if(d){d.className='stat-dot starting';}});
    sTxt&&(sTxt.textContent='🟡 বট চালু হচ্ছে...');
    tStatus&&(tStatus.textContent='🟡 চালু হচ্ছে');
  }else{
    card&&(card.className='bot-state stopped');
    icon&&(icon.textContent='🔴');
    logo&&(logo.className='top-logo');
    [sDot,tDot].forEach(d=>{if(d){d.className='stat-dot';}});
    sTxt&&(sTxt.textContent='🔴 বট বন্ধ');
    tStatus&&(tStatus.textContent='🔴 বন্ধ');
  }
}
function updateMongo(connected){
  const mDot=document.getElementById('mDot');
  const mStatus=document.getElementById('mStatus');
  const mongoPill=document.getElementById('mongoPill');
  const mongoBadge=document.getElementById('mongoBadge');
  const mongoInfo=document.getElementById('mongoInfo');
  if(mongoPill)mongoPill.style.display='flex';
  if(mDot)mDot.className='stat-dot'+(connected?' on':'');
  if(mStatus)mStatus.textContent=connected?'DB ✅':'DB ❌';
  if(mongoBadge){mongoBadge.textContent=connected?'✅ MongoDB সংযুক্ত':'❌ MongoDB বিচ্ছিন্ন';mongoBadge.className='mongo-status-badge'+(connected?' ok':' err');}
  if(mongoInfo)mongoInfo.textContent=connected?'সংযুক্ত ✅':'বিচ্ছিন্ন ❌';
}

// ── REFRESH ──
async function refresh(){
  try{
    const ac=new AbortController();
    const tid=setTimeout(()=>ac.abort(),30000);
    const [rSt,rBs]=await Promise.all([fetch('/api/stats',{signal:ac.signal}),fetch('/api/bot/status',{signal:ac.signal})]);
    clearTimeout(tid);
    if(rSt.status===401||rBs.status===401){location.href='/login';return;}
    const st=await rSt.json(),bs=await rBs.json();
    document.getElementById('cMem').textContent=st.memMB||'--';
    document.getElementById('cSup').textContent=fmtT(st.serverUptime||0);
    document.getElementById('cFiles').textContent=st.botFiles||0;
    document.getElementById('cStarts').textContent=st.starts||0;
    const cc=document.getElementById('cCrash');if(cc)cc.textContent=st.crashes||0;
    const ct=document.getElementById('cTup');if(ct)ct.textContent=fmtT((st.totalUptime||0)+(bs.uptime||0));
    const cn=document.getElementById('cNode');if(cn)cn.textContent=(st.node||'').replace('v','');
    updateBotStatus(!!bs.ready,bs.running&&!bs.ready);
    updateMongo(st.mongoConnected||false);
    fetch('/api/cookie/status').then(r=>r.json()).then(cs=>{
      const el=document.getElementById('cookieStatus');if(!el)return;
      el.style.display='block';
      if(cs.saved){el.style.cssText='display:block;background:rgba(0,245,160,.1);color:var(--gn);border-radius:8px;padding:7px 10px;font-size:12px';el.textContent='✅ Cookie সেভ করা আছে';}
      else{el.style.cssText='display:block;background:rgba(255,85,114,.1);color:var(--rd);border-radius:8px;padding:7px 10px;font-size:12px';el.textContent='⚠️ কোনো Cookie সেভ নেই';}
    }).catch(()=>{});
    if(bs.running&&bs.uptime>0&&_botUpSec===0)_botUpSec=bs.uptime;
    const hist=(st.history||[]).slice().reverse().slice(0,8);
    const hl=document.getElementById('histList');
    if(hl)hl.innerHTML=hist.length?hist.map(h=>'<div class="hist-row"><span class="hist-date">'+new Date(h.date).toLocaleString('bn-BD').substring(0,16)+'</span><span class="hist-up">'+fmtT(h.uptime)+'</span><span class="hist-code">'+h.code+'</span></div>').join(''):'<div style="font-size:12px;color:var(--mu);text-align:center;padding:10px">ইতিহাস নেই</div>';
    const pgMon=document.getElementById('pg-monitor');
    if(pgMon&&pgMon.classList.contains('active'))loadMonitor();
    _refreshFails=0;
  }catch(e){
    _refreshFails++;
    const ts=document.getElementById('tStatus');
    if(ts)ts.textContent=_refreshFails<=3?'সংযোগ হচ্ছে...':'⚠️ retry #'+_refreshFails;
    console.warn('[refresh #'+_refreshFails+']',e&&e.message);
  }
}

// ── MONITOR ──
function _mc(p){return p<60?'var(--gn)':p<85?'var(--yw)':'var(--rd)';}
async function loadMonitor(){
  try{
    const d=await fetch('/api/system/live').then(r=>r.json());
    const cap=(d.ram&&d.ram.capMB)||512;
    const pMB=d.ram&&d.ram.panelMB!=null?d.ram.panelMB:null;
    const bMB=d.ram&&d.ram.botMB!=null?d.ram.botMB:null;
    const tMB=(pMB||0)+(bMB||0);
    if(pMB!=null){const p=Math.min(100,Math.round(pMB/cap*100));document.getElementById('mPanelTxt').textContent=pMB+' MB / '+cap+' MB';document.getElementById('mPanelBar').style.cssText='width:'+p+'%;background:'+_mc(p);}
    if(bMB!=null){const p=Math.min(100,Math.round(bMB/cap*100));document.getElementById('mBotTxt').textContent=bMB+' MB / '+cap+' MB';document.getElementById('mBotBar').style.cssText='width:'+p+'%;background:'+_mc(p);}
    else{document.getElementById('mBotTxt').textContent='বট বন্ধ';document.getElementById('mBotBar').style.width='0%';}
    {const p=Math.min(100,Math.round(tMB/cap*100));document.getElementById('mTotalTxt').textContent=tMB+' MB / '+cap+' MB';document.getElementById('mTotalBar').style.cssText='width:'+p+'%;background:'+_mc(p);}
    const hr=document.getElementById('mHeavyRow');
    if(d.heavy){hr.style.display='block';const p=Math.min(100,Math.round((d.heavy.active/d.heavy.max)*100));document.getElementById('mHeavyTxt').textContent=d.heavy.active+' / '+d.heavy.max;document.getElementById('mHeavyBar').style.width=p+'%';}
    else hr.style.display='none';
    if(d.mongo&&!d.mongo.error){
      const mCap=512,used=d.mongo.totalMB||0,p=Math.min(100,Math.round(used/mCap*100));
      document.getElementById('mMongoTxt').textContent=used+' MB / '+mCap+' MB';
      document.getElementById('mMongoBar').style.cssText='width:'+p+'%;background:'+_mc(p);
      document.getElementById('mMongoObjs').textContent=d.mongo.objects!=null?d.mongo.objects:'--';
      document.getElementById('mMongoData').textContent=d.mongo.dataSizeMB!=null?d.mongo.dataSizeMB:'--';
      document.getElementById('mMongoIdx').textContent=d.mongo.indexSizeMB!=null?d.mongo.indexSizeMB:'--';
    }else{document.getElementById('mMongoTxt').textContent='সংযুক্ত নেই';document.getElementById('mMongoBar').style.width='0%';}
    if(d.lifetime){
      const lt=d.lifetime;
      document.getElementById('mPanelPeak').textContent=(lt.peakPanelMB||0)+' MB';
      document.getElementById('mBotPeak').textContent=(lt.peakBotMB||0)+' MB';
      document.getElementById('mMongoPeak').textContent=(lt.peakMongoMB||0)+' MB';
      document.getElementById('mLtStarts').textContent=(lt.totalStarts||0)+'x';
      document.getElementById('mLtCrashes').textContent=(lt.totalCrashes||0)+'x';
      document.getElementById('mLtUptime').textContent=_fmtLt(lt.totalUptimeSec||0);
      document.getElementById('mLtSince').textContent=lt.firstSeen?new Date(lt.firstSeen).toLocaleDateString('bn-BD',{year:'numeric',month:'long',day:'numeric'}):'--';
    }
  }catch(e){}
}

// ── TERMINAL ──
async function loadTerminal(){
  try{
    const d=await fetch('/api/system/terminal').then(r=>r.json());
    document.getElementById('hkTime').textContent=new Date().toLocaleTimeString('en-GB');
    if(d.net){
      document.getElementById('hkRx').textContent=(d.net.rxKBs||0).toFixed(1);
      document.getElementById('hkTx').textContent=(d.net.txKBs||0).toFixed(1);
      document.getElementById('hkNetBar').style.width=Math.min(100,Math.round(((d.net.rxKBs||0)+(d.net.txKBs||0))/2))+'%';
    }
    document.getElementById('hkCpu').textContent=d.cpuPercent||0;
    document.getElementById('hkCpuBar').style.width=(d.cpuPercent||0)+'%';
    document.getElementById('hkRam').textContent=d.ramPercent||0;
    document.getElementById('hkRamBar').style.width=(d.ramPercent||0)+'%';
    if(d.uptimeSec!=null)document.getElementById('hkUptime').textContent=fmtT(d.uptimeSec);
    const bs=document.getElementById('hkBotSt');
    if(bs){
      if(d.botReady){bs.textContent='ONLINE ✓';bs.style.color='#33ff66';}
      else if(d.botRunning){bs.textContent='BOOTING...';bs.style.color='#ffd633';}
      else{bs.textContent='OFFLINE ✕';bs.style.color='#ff4466';}
    }
    const tail=document.getElementById('hkTail');
    if(tail&&d.tail)tail.innerHTML=d.tail.map(l=>'<div class="t-line t-tail-'+(l.type||'info')+'">['+l.time+'] '+esc(l.text)+'</div>').join('');
  }catch(e){}
}

// ── LOGS ──
function appendLog(e){
  if(logFilter!=='all'&&e.type!==logFilter)return;
  const box=document.getElementById('lbox');
  const d=document.createElement('div');
  const cls={info:'le-info',success:'le-success',error:'le-error',warn:'le-warn'}[e.type||'info']||'le-info';
  const icon={info:'ℹ️',success:'✅',error:'❌',warn:'⚠️'}[e.type||'info']||'ℹ️';
  d.className='log-entry '+cls;d.dataset.t=e.type||'info';
  d.innerHTML='<span class="le-icon">'+icon+'</span><span class="le-time">'+e.time+'</span><span class="le-txt">'+esc(e.text)+'</span>';
  box.appendChild(d);
  if(autoScroll)box.scrollTop=box.scrollHeight;
}
function setLF(f,btn){logFilter=f;document.querySelectorAll('.lf-btn').forEach(b=>b.classList.remove('on'));btn.classList.add('on');document.querySelectorAll('.log-entry').forEach(el=>el.style.display=(f==='all'||el.dataset.t===f)?'flex':'none');}
function clearLogs(){fetch('/api/bot/clearlogs',{method:'POST'});}
function toggleLogFull(){document.body.classList.toggle('log-full');}

// ── ALERTS ──
const _lvlIcon={info:'ℹ️',warn:'⚠️',error:'🔴'};
function showAlertBanner(a){
  const box=document.getElementById('alertBanner');
  const d=document.createElement('div');
  d.className='alert-item-banner '+(a.level||'info');
  d.innerHTML='<span>'+(_lvlIcon[a.level]||'ℹ️')+'</span><div><b>'+esc(a.title)+'</b><div style="color:var(--mu);font-size:11px;margin-top:2px">'+esc(a.message)+'</div></div><button class="aib-x" onclick="this.parentElement.remove()">✕</button>';
  box.appendChild(d);
  setTimeout(()=>{if(d.parentElement)d.remove();},9000);
}
function updateBell(){
  const b=document.getElementById('bellBadge');
  if(_unreadAlerts>0){b.style.display='flex';b.textContent=_unreadAlerts>99?'99+':_unreadAlerts;}
  else b.style.display='none';
}
function renderAlertList(){
  const list=document.getElementById('alertList');
  if(!_alertsCache.length){list.innerHTML='<div style="text-align:center;color:var(--mu);padding:40px;font-size:13px">🔕 কোনো নোটিফিকেশন নেই</div>';return;}
  list.innerHTML=_alertsCache.map(a=>'<div class="alert-list-item '+(a.level||'info')+'"><div class="ali-title">'+(_lvlIcon[a.level]||'ℹ️')+' '+esc(a.title)+'</div><div style="font-size:12px;color:var(--mu)">'+esc(a.message)+'</div><div class="ali-time">'+new Date(a.time).toLocaleString('bn-BD')+'</div></div>').join('');
}
async function openAlerts(){
  document.getElementById('alertDrawer').classList.add('show');
  document.getElementById('drawerOverlay').classList.add('show');
  _unreadAlerts=0;updateBell();
  try{const d=await fetch('/api/alerts').then(r=>r.json());_alertsCache=d.alerts||[];}catch{}
  renderAlertList();
}
function closeAlerts(){document.getElementById('alertDrawer').classList.remove('show');document.getElementById('drawerOverlay').classList.remove('show');}
async function clearAlerts(){
  if(!confirm('সব নোটিফিকেশন মুছবে?'))return;
  await fetch('/api/alerts/clear',{method:'POST'});
  _alertsCache=[];renderAlertList();
}
(async()=>{try{const d=await fetch('/api/alerts').then(r=>r.json());_alertsCache=d.alerts||[];}catch{}})();

// ── BOT CONTROL ──
async function botAct(a){
  toast('⏳ '+{start:'চালু',stop:'বন্ধ',restart:'রিস্টার্ট'}[a]+'...','warn');
  const d=await fetch('/api/bot/'+a,{method:'POST'}).then(r=>r.json());
  toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');
  setTimeout(refresh,2500);
}
async function npmInst(){toast('📦 npm install শুরু...','warn');const d=await fetch('/api/bot/install',{method:'POST'}).then(r=>r.json());toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');}
function doBackup(){window.open('/api/backup');}
async function mongoSync(){toast('☁️ Sync শুরু...','warn');const d=await fetch('/api/mongo/sync',{method:'POST'}).then(r=>r.json());toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');}
async function mongoRestore(){toast('🔄 Restore শুরু...','warn');const d=await fetch('/api/mongo/restore',{method:'POST'}).then(r=>r.json());toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');if(d.ok)setTimeout(()=>loadFiles(curDir),2000);}
async function saveCookie(){
  const c=document.getElementById('cookieInput').value.trim();
  if(!c)return toast('❌ Cookie লিখুন','error');
  const d=await fetch('/api/cookie/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({cookie:c})}).then(r=>r.json());
  toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');
  if(d.ok){document.getElementById('cookieInput').value='';toast('🔄 বট চালু হচ্ছে...','warn');setTimeout(()=>botAct('start'),1500);}
}
async function doReset(){
  if(!confirm('সব লগ ও নোটিফিকেশন মুছে ফ্রেশ করবে?'))return;
  await fetch('/api/bot/clearlogs',{method:'POST'});
  await fetch('/api/alerts/clear',{method:'POST'});
  document.getElementById('lbox').innerHTML='';
  document.getElementById('alertBanner').innerHTML='';
  _alertsCache=[];_unreadAlerts=0;updateBell();renderAlertList();
  toast('🔄 রিসেট হয়েছে','success');
}
function clearLogFile(){if(!confirm('Log file মুছবে?'))return;fetch('/api/bot/clearlogfile',{method:'POST'}).then(r=>r.json()).then(d=>toast(d.ok?'✅ মুছা হয়েছে':'❌ ব্যর্থ',d.ok?'success':'error'));}

// ── FILE ICONS ──
function ficon(n,isDir){
  if(isDir)return'📁';
  const e=n.split('.').pop().toLowerCase();
  return{js:'📜',mjs:'📜',cjs:'📜',json:'📋',md:'📝',txt:'📄',env:'🔐',log:'📋',jpg:'🖼️',jpeg:'🖼️',png:'🖼️',gif:'🖼️',webp:'🖼️',mp3:'🎵',mp4:'🎬',zip:'📦',tar:'📦',gz:'📦',html:'🌐',css:'🎨',ts:'📘',py:'🐍',sh:'⚡',bat:'⚡',yml:'⚙️',yaml:'⚙️',xml:'📋',lock:'🔒'}[e]||'📄';
}
function fclass(n,isDir){
  if(isDir)return'fi-dir';
  const e=n.split('.').pop().toLowerCase();
  if(['js','mjs','cjs','ts','py','sh','bat'].includes(e))return'fi-code';
  if(['json','yml','yaml','xml','env','lock'].includes(e))return'fi-data';
  if(['jpg','jpeg','png','gif','webp','mp3','mp4'].includes(e))return'fi-media';
  if(['zip','tar','gz'].includes(e))return'fi-archive';
  return'fi-doc';
}
function langExt(n){const e=n.split('.').pop().toLowerCase();return{js:'JavaScript',json:'JSON',md:'Markdown',html:'HTML',css:'CSS',py:'Python',ts:'TypeScript',sh:'Shell',env:'ENV',txt:'Text',yml:'YAML',xml:'XML'}[e]||e.toUpperCase();}

// ── FILE MANAGER ──
function buildPath(dir){
  const bar=document.getElementById('pathBar');
  const parts=dir?dir.split('/'):[],pb=['<span class="pp" onclick="loadFiles(\\'\\')">📁 root</span>'];
  let acc='';
  parts.forEach(p=>{acc+=(acc?'/':'')+p;const c=acc;pb.push('<span style="color:var(--mu)"> / </span><span class="pp" onclick="loadFiles(\\''+c+'\\')">'+p+'</span>');});
  bar.innerHTML=pb.join('');
}
async function loadFiles(dir){
  curDir=dir||'';buildPath(curDir);
  document.getElementById('fmView').style.display='block';
  document.getElementById('edView').style.display='none';
  document.getElementById('fsRes').style.display='none';
  document.getElementById('fq').value='';
  document.getElementById('uploadDir').textContent=curDir||'root';
  const data=await fetch('/api/files?path='+encodeURIComponent(curDir)).then(r=>r.json());
  const nF=(data.items||[]).filter(i=>!i.isDir).length,nD=(data.items||[]).filter(i=>i.isDir).length;
  document.getElementById('fmSummary').innerHTML='<span>📁 '+nD+' ফোল্ডার · 📄 '+nF+' ফাইল</span>';
  const list=document.getElementById('flist');list.innerHTML='';
  if(curDir){
    const up=document.createElement('div');up.className='file-row';
    up.innerHTML='<div class="file-icon fi-dir">⬆️</div><div class="file-info"><div class="file-name">.. উপরে যাও</div></div>';
    up.onclick=()=>loadFiles(curDir.split('/').slice(0,-1).join('/'));
    list.appendChild(up);
  }
  if(!(data.items&&data.items.length)){list.innerHTML='<div class="fm-empty"><div style="font-size:44px;margin-bottom:10px">📭</div><div>ফোল্ডার খালি</div></div>';return;}
  data.items.forEach(item=>{
    const fp=curDir?curDir+'/'+item.name:item.name;
    const row=document.createElement('div');row.className='file-row';
    row.innerHTML='<div class="file-icon '+fclass(item.name,item.isDir)+'">'+ficon(item.name,item.isDir)+'</div>'
      +'<div class="file-info"><div class="file-name">'+item.name+'</div><div class="file-meta">'+fsz(item.size)+(item.mtime?' · <span data-mtime="'+item.mtime+'">'+fdt(item.mtime)+'</span>':'')+'</div></div>'
      +'<div class="file-actions">'
      +(!item.isDir?'<button class="fab" onclick="event.stopPropagation();editF(\\''+fp+'\\')">✏️</button>':'')
      +(!item.isDir&&/\\.js$/i.test(item.name)?'<button class="fab" onclick="event.stopPropagation();testFile(\\''+fp+'\\')">🧪</button>':'')
      +'<button class="fab" onclick="event.stopPropagation();dlF(\\''+fp+'\\')">⬇️</button>'
      +'<button class="fab" onclick="event.stopPropagation();showRename(\\''+fp+'\\',\\''+item.name+'\\')">🔤</button>'
      +'<button class="fab" onclick="event.stopPropagation();showCopy(\\''+fp+'\\')">📋</button>'
      +'<button class="fab del" onclick="event.stopPropagation();delItem(\\''+fp+'\\',\\''+item.name+'\\')">🗑</button>'
      +'</div>';
    if(item.isDir)row.onclick=()=>loadFiles(fp);else row.onclick=()=>editF(fp);
    list.appendChild(row);
  });
}

// ── EDITOR ──
async function editF(p){
  const d=await fetch('/api/file/read?path='+encodeURIComponent(p)).then(r=>r.json());
  if(d.error)return toast('❌ '+d.error,'error');
  curEdit=p;
  document.getElementById('edFn').textContent=p.split('/').pop();
  document.getElementById('edLang').textContent=langExt(p);
  document.getElementById('ced').value=d.content;
  document.getElementById('fmView').style.display='none';
  document.getElementById('edView').style.display='block';
  const isHl=/\.(js|json|mjs|cjs)$/i.test(p);
  document.getElementById('edWrap').classList.toggle('hl-active',isHl);
  document.getElementById('ced').classList.toggle('hl-on',isHl);
  if(isHl)renderHL();
}
function onEdInput(){if(document.getElementById('edWrap').classList.contains('hl-active'))renderHL();}
function syncEdScroll(){const hl=document.getElementById('cedHl'),ta=document.getElementById('ced');hl.scrollTop=ta.scrollTop;hl.scrollLeft=ta.scrollLeft;}
function renderHL(){
  const code=document.getElementById('ced').value;
  document.querySelector('#cedHl code').innerHTML=hlJS(code);
  syncEdScroll();
}
function hlJS(src){
  let s=esc(src);const tokens=[];
  s=s.replace(/(\/\*[\s\S]*?\*\/|\/\/[^\n]*)/g,m=>{tokens.push('<span class="tok-com">'+m+'</span>');return '\u0000'+(tokens.length-1)+'\u0000';});
  s=s.replace(/(&quot;(?:[^&]|&(?!quot;))*?&quot;|'(?:[^'\\]|\\.)*')/g,m=>{tokens.push('<span class="tok-str">'+m+'</span>');return '\u0000'+(tokens.length-1)+'\u0000';});
  s=s.replace(/\b(const|let|var|function|async|await|return|if|else|for|while|do|switch|case|break|continue|try|catch|finally|throw|new|class|extends|super|this|typeof|instanceof|in|of|null|undefined|true|false|import|export|default|require|module|exports|delete|void|yield|static|get|set)\b/g,'<span class="tok-kw">$1</span>');
  s=s.replace(/\b(\d+\.?\d*)\b/g,'<span class="tok-num">$1</span>');
  s=s.replace(/\b([a-zA-Z_$][\w$]*)(?=\s*\()/g,'<span class="tok-fn">$1</span>');
  s=s.replace(/\u0000(\d+)\u0000/g,(_,i)=>tokens[+i]);
  return s;
}
function closeEd(){document.getElementById('edView').style.display='none';document.getElementById('fmView').style.display='block';}
async function saveFile(){
  const d=await fetch('/api/file/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:curEdit,content:document.getElementById('ced').value})}).then(r=>r.json());
  toast(d.ok?'✅ সেভ হয়েছে':'❌ '+d.error,d.ok?'success':'error');
}
function dlF(p){window.open('/api/file/download?path='+encodeURIComponent(p));}
async function delItem(p,name){
  if(!confirm('"'+name+'" ডিলিট করবে? MongoDB থেকেও মুছবে।'))return;
  const d=await fetch('/api/file/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:p})}).then(r=>r.json());
  toast(d.ok?'🗑 ডিলিট হয়েছে':'❌ '+d.error,d.ok?'success':'error');if(d.ok)loadFiles(curDir);
}

// ── FILE TEST ──
async function testFile(pathOverride){
  const target=pathOverride||curEdit;if(!target)return;
  document.getElementById('testOverlay').classList.add('show');
  document.getElementById('testPanel').classList.add('show');
  document.getElementById('testBody').innerHTML='<div style="text-align:center;padding:32px;color:var(--mu)">⏳ টেস্ট চলছে...</div>';
  try{
    const d=await fetch('/api/file/test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:target})}).then(r=>r.json());
    if(!d.ok){document.getElementById('testBody').innerHTML='<div class="test-sec bad"><b>❌ ব্যর্থ</b><br>'+esc(d.msg||d.error||'অজানা এরর')+'</div>';return;}
    renderTestResult(d.result);
  }catch(e){document.getElementById('testBody').innerHTML='<div class="test-sec bad">❌ নেটওয়ার্ক এরর</div>';}
}
function closeTest(){document.getElementById('testOverlay').classList.remove('show');document.getElementById('testPanel').classList.remove('show');}
function renderTestResult(r){
  const structOk=!r.structure||r.structure.ok;
  const depsOk=!r.dependencies||r.dependencies.every(d=>d.ok);
  const apisOk=!r.apis||r.apis.every(a=>a.ok);
  const allGood=r.syntax.ok&&structOk&&depsOk&&apisOk;
  let html=allGood?'<div class="test-sec ok" style="text-align:center;font-size:14px;font-weight:800;margin-bottom:10px">✅ ফাইল সম্পূর্ণ ঠিক আছে</div>':'<div class="test-sec bad" style="text-align:center;font-size:14px;font-weight:800;margin-bottom:10px">⚠️ সমস্যা আছে</div>';
  html+='<div class="test-sec '+(r.syntax.ok?'ok':'bad')+'"><b>'+(r.syntax.ok?'✅':'❌')+' সিনট্যাক্স</b><div style="font-size:12px;color:var(--mu);margin-top:4px">'+esc(r.syntax.msg)+'</div></div>';
  if(r.structure)html+='<div class="test-sec '+(r.structure.ok?'ok':'bad')+'"><b>'+(r.structure.ok?'✅':'❌')+' কমান্ড স্ট্রাকচার</b><div style="font-size:12px;color:var(--mu);margin-top:4px">'+esc(r.structure.msg)+'</div></div>';
  if(r.dependencies&&r.dependencies.length){
    html+='<div class="test-sec '+(r.dependencies.every(d=>d.ok)?'ok':'bad')+'"><b>Dependencies</b>';
    r.dependencies.forEach(d=>{html+='<div style="display:flex;justify-content:space-between;font-size:11px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,.05)"><span>'+esc(d.pkg)+'</span><span style="color:'+(d.ok?'var(--gn)':'var(--rd)')+'">'+(d.ok?'✅ আছে':'❌ নেই')+'</span></div>';});
    html+='</div>';
  }
  if(r.apis&&r.apis.length){
    html+='<div class="test-sec '+(r.apis.every(a=>a.ok)?'ok':'bad')+'"><b>API লিংক</b>';
    r.apis.forEach(a=>{html+='<div style="display:flex;justify-content:space-between;font-size:11px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,.05)"><span style="word-break:break-all;flex:1;color:var(--mu)">'+esc(a.url)+'</span><span style="flex-shrink:0;margin-left:8px;color:'+(a.ok?'var(--gn)':'var(--rd)')+'">'+esc(String(a.status))+'</span></div>';});
    html+='</div>';
  }
  document.getElementById('testBody').innerHTML=html;
}

// ── MODALS ──
function showM(id){document.getElementById('mod-'+id).classList.add('open');setTimeout(()=>{const f=document.querySelector('#mod-'+id+' .modal-input');if(f)f.focus();},120);}
function closeM(id){document.getElementById('mod-'+id).classList.remove('open');}
async function doMkdir(){
  const n=document.getElementById('mkN').value.trim();if(!n)return;
  const fp=curDir?curDir+'/'+n:n;
  const d=await fetch('/api/file/mkdir',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:fp})}).then(r=>r.json());
  closeM('mkdir');toast(d.ok?'📁 তৈরি হয়েছে':'❌ '+d.error,d.ok?'success':'error');if(d.ok)loadFiles(curDir);
}
async function doNewFile(){
  const n=document.getElementById('nfN').value.trim();if(!n)return;
  const fp=curDir?curDir+'/'+n:n;
  const d=await fetch('/api/file/newfile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:fp,content:''})}).then(r=>r.json());
  closeM('newfile');if(d.ok){toast('📄 তৈরি হয়েছে','success');editF(fp);}else toast('❌ '+d.error,'error');
}
function showRename(p,name){renameFrom=p;document.getElementById('rnV').value=name;showM('rename');}
async function doRename(){
  const n=document.getElementById('rnV').value.trim();if(!n)return;
  const dir=renameFrom.includes('/')?renameFrom.split('/').slice(0,-1).join('/'):'';
  const to=dir?dir+'/'+n:n;
  const d=await fetch('/api/file/rename',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({from:renameFrom,to})}).then(r=>r.json());
  closeM('rename');toast(d.ok?'✅ নাম পরিবর্তন':'❌ '+d.error,d.ok?'success':'error');if(d.ok)loadFiles(curDir);
}
function showCopy(p){copyFrom=p;document.getElementById('cpTo').value=p+'_copy';showM('copy');}
async function doCopy(){
  const to=document.getElementById('cpTo').value.trim();if(!to)return;
  const d=await fetch('/api/file/copy',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({from:copyFrom,to})}).then(r=>r.json());
  closeM('copy');toast(d.ok?'📋 Copy হয়েছে':'❌ '+d.error,d.ok?'success':'error');if(d.ok)loadFiles(curDir);
}

// ── FILE SEARCH ──
let _fst;
function doFS(){
  const q=document.getElementById('fq').value.trim();
  const res=document.getElementById('fsRes');
  if(!q){res.style.display='none';return;}
  clearTimeout(_fst);_fst=setTimeout(async()=>{
    const d=await fetch('/api/file/search?q='+encodeURIComponent(q)).then(r=>r.json());
    if(!(d.results&&d.results.length)){res.style.display='block';res.innerHTML='<div style="font-size:12px;color:var(--mu);padding:10px;text-align:center">📭 পাওয়া যায়নি</div>';return;}
    res.style.display='block';
    res.innerHTML=d.results.map(r=>'<div class="search-result" onclick="'+(r.isDir?"loadFiles('"+r.path+"')":"editF('"+r.path+"')")+'"><div class="sr-path">'+ficon(r.name,r.isDir)+' '+r.path+'</div><div class="sr-meta">'+fsz(r.size)+'</div></div>').join('');
  },300);
}

// ── UPLOAD ──
async function uploadF(file){
  if(!file)return;
  const pw=document.getElementById('progWrap'),pb=document.getElementById('progBar');
  const pp=document.getElementById('upPct'),ps=document.getElementById('upSt'),fn=document.getElementById('upFN');
  pw.style.display='block';fn.textContent=file.name;pb.style.width='0%';pp.textContent='0%';ps.textContent='চেক করছি...';
  const CHUNK=50*1024,total=Math.max(1,Math.ceil(file.size/CHUNK));
  const uid='f'+file.size+'_'+(file.lastModified||0)+'_'+file.name.replace(/[^a-zA-Z0-9]/g,'').slice(0,40);
  const already=new Set();
  try{const st=await fetch('/api/file/upload-status?uploadId='+encodeURIComponent(uid)).then(r=>r.json());(st.have||[]).forEach(i=>already.add(i));}catch{}
  if(already.size>0)ps.textContent='Resume হচ্ছে ('+already.size+'/'+total+' আগে থেকেই আছে)...';
  async function sendChunk(i){
    const start=i*CHUNK,end=Math.min(file.size,start+CHUNK),blob=file.slice(start,end);
    let attempt=0;
    while(attempt<8){
      try{
        const fd=new FormData();fd.append('chunk',blob,'chunk');fd.append('uploadId',uid);fd.append('chunkIndex',i);fd.append('totalChunks',total);fd.append('fileName',file.name);fd.append('path',curDir||'');
        const d=await fetch('/api/file/upload-chunk',{method:'POST',body:fd}).then(r=>r.json());
        if(d&&d.ok!==false)return d;throw new Error(d.msg||'fail');
      }catch(e){attempt++;ps.textContent='⚠️ চাংক '+(i+1)+'/'+total+' retry ('+attempt+'/8)...';await new Promise(r=>setTimeout(r,Math.min(1000*attempt,8000)));}
    }
    return null;
  }
  const toSend=[],done={count:already.size};
  for(let i=0;i<total;i++)if(!already.has(i))toSend.push(i);
  let lastResult=null,failed=false;
  function upd(){const p=Math.round((done.count/total)*100);pb.style.width=p+'%';pp.textContent=p+'%';}
  upd();
  for(const i of toSend){
    const d=await sendChunk(i);
    if(!d){failed=true;ps.innerHTML='<span style="color:var(--rd)">❌ আপলোড থেমে গেছে — ফাইল আবার সিলেক্ট করলে resume হবে</span>';toast('❌ আপলোড ব্যর্থ','error');break;}
    lastResult=d;done.count++;upd();ps.textContent='চাংক '+done.count+'/'+total+' পাঠানো...';
  }
  if(failed){document.getElementById('fInp').value='';return;}
  if(!lastResult||!lastResult.done)lastResult=await sendChunk(total-1);
  if(lastResult&&lastResult.done&&lastResult.processing){
    ps.innerHTML='<span style="color:var(--yw)">⏳ '+(lastResult.msg||'প্রসেস হচ্ছে...')+'</span>';
    let final=null;
    for(let tries=0;tries<150;tries++){
      await new Promise(r=>setTimeout(r,3000));
      try{const st=await fetch('/api/file/upload-result?uploadId='+encodeURIComponent(uid)).then(r=>r.json());if(st.status==='done'){final=st;break;}}catch{}
    }
    if(final){
      if(final.ok){ps.innerHTML='<span style="color:var(--gn)">✅ '+(final.msg||'সম্পন্ন')+'</span>';toast('✅ '+(final.msg||'সম্পন্ন'),'success');}
      else{ps.innerHTML='<span style="color:var(--rd)">❌ '+(final.msg||'ব্যর্থ')+'</span>';toast('❌ '+(final.msg||'ব্যর্থ'),'error');}
    }else{ps.innerHTML='<span style="color:var(--rd)">⚠️ ফলাফল জানা যায়নি — লগ/ফাইল ট্যাবে চেক করো</span>';}
  }else if(lastResult&&lastResult.done){
    if(lastResult.ok){ps.innerHTML='<span style="color:var(--gn)">✅ '+(lastResult.msg||'সম্পন্ন')+'</span>';toast('✅ '+(lastResult.msg||'সম্পন্ন'),'success');}
    else{ps.innerHTML='<span style="color:var(--rd)">❌ '+(lastResult.msg||'ব্যর্থ')+'</span>';toast('❌ '+(lastResult.msg||'ব্যর্থ'),'error');}
  }
  document.getElementById('fInp').value='';
}
async function uploadSingle(file){
  if(!file)return;const st=document.getElementById('singleStatus');st.textContent='⏳ আপলোড হচ্ছে...';
  const fd=new FormData();fd.append('file',file);fd.append('path',curDir||'');
  const d=await fetch('/api/file/upload',{method:'POST',body:fd}).then(r=>r.json());
  st.innerHTML=d.ok?'<span style="color:var(--gn)">✅ '+d.msg+'</span>':'<span style="color:var(--rd)">❌ '+d.error+'</span>';
  toast(d.ok?'✅ '+d.msg:'❌ '+d.error,d.ok?'success':'error');document.getElementById('singleInp').value='';
}
async function uploadMulti(files){
  if(!files||!files.length)return;const st=document.getElementById('multiStatus');st.textContent='⏳ '+files.length+'টা আপলোড হচ্ছে...';
  const fd=new FormData();for(const f of files)fd.append('files',f);fd.append('path',curDir||'');
  const d=await fetch('/api/file/upload-multi',{method:'POST',body:fd}).then(r=>r.json());
  st.innerHTML=d.ok?'<span style="color:var(--gn)">✅ '+d.msg+'</span>':'<span style="color:var(--rd)">❌ '+d.error+'</span>';
  toast(d.ok?'✅ '+d.msg:'❌ '+d.error,d.ok?'success':'error');document.getElementById('multiInp').value='';
}
const uz=document.getElementById('upZone');
uz.addEventListener('dragover',e=>{e.preventDefault();uz.classList.add('drag');});
uz.addEventListener('dragleave',()=>uz.classList.remove('drag'));
uz.addEventListener('drop',e=>{e.preventDefault();uz.classList.remove('drag');uploadF(e.dataTransfer.files[0]);});

// ── ENV ──
async function loadEnv(){const d=await fetch('/api/env').then(r=>r.json());document.getElementById('envEd').value=d.content||'';}
async function saveEnv(){const d=await fetch('/api/env/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:document.getElementById('envEd').value})}).then(r=>r.json());toast(d.ok?'✅ .env সেভ + MongoDB':'❌ '+d.msg,d.ok?'success':'error');}

// ── SETTINGS ──
async function loadSettings(){
  const d=await fetch('/api/settings').then(r=>r.json());
  document.getElementById('sName').value=d.panelName||'';
  document.getElementById('sSiteUrl').value=d.siteUrl||location.origin;
  document.getElementById('sAR').checked=d.autoRestart||false;
  document.getElementById('sSched').checked=d.scheduleRestart||false;
  document.getElementById('sTime').value=d.scheduleTime||'03:00';
}
async function saveSettings(){
  const body={panelName:document.getElementById('sName').value,siteUrl:document.getElementById('sSiteUrl').value,autoRestart:document.getElementById('sAR').checked,scheduleRestart:document.getElementById('sSched').checked,scheduleTime:document.getElementById('sTime').value};
  const d=await fetch('/api/settings/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(r=>r.json());
  toast(d.ok?'✅ সেভ হয়েছে':'❌ ব্যর্থ',d.ok?'success':'error');
}
async function changePw(){
  const d=await fetch('/api/settings/password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current:document.getElementById('sCur').value,newPass:document.getElementById('sNew').value})}).then(r=>r.json());
  toast(d.ok?'✅ '+d.msg:'❌ '+d.msg,d.ok?'success':'error');
  if(d.ok){document.getElementById('sCur').value='';document.getElementById('sNew').value='';}
}

// ── KEYBOARD ──
document.addEventListener('keydown',e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==='s'&&curEdit){e.preventDefault();saveFile();}
  if(e.key==='Escape'){document.querySelectorAll('.overlay.open').forEach(m=>m.classList.remove('open'));closeAlerts();closeTest();}
});

// ── INIT ──
// প্রথমে /ping দিয়ে Render server wake করো (cold start এ 50s লাগে)
// WS এবং refresh() আলাদাভাবে চলবে — একটা fail করলে অন্যটা কাজ করবে
fetch('/ping').catch(()=>{}); // fire-and-forget pre-warm
connectWS();
setTimeout(()=>{if(_refreshFails>0||!_wsConnected)refresh();},10000);
setInterval(refresh,15000);
</script>
</body></html>`;
}

// ── WebSocket handler ──
wss.on("connection",ws=>{
  // connect হওয়ার সাথে সাথেই সব current state পাঠিয়ে দাও
  // client কে refresh() এর জন্য অপেক্ষা করতে হবে না
  ws.send(JSON.stringify({type:"status",running:!!botProc,starting:botProc&&!botReady,ready:botReady}));
  ws.send(JSON.stringify({type:"logs",data:botLogs.slice(-200)})); // শেষ ২০০ লগ, সব না
  ws.send(JSON.stringify({type:"mongo",connected:db_connected}));
  ws.on("error",()=>{}); // WS error চুপচাপ handle
});

// ── START ──
server.listen(PORT,()=>{
  console.log("Panel v2: http://localhost:"+PORT);
  if(process.env.RENDER_EXTERNAL_URL&&!cfg.siteUrl){
    cfg.siteUrl=process.env.RENDER_EXTERNAL_URL;
    saveJ(CFG,cfg);
  }
  try{
    require.resolve("mongoose");
    connectMongo();
  }catch{
    console.log("⚠️ mongoose not installed — installing...");
    try{
      const {execSync}=require("child_process");
      execSync("npm install mongoose",{timeout:60000});
      connectMongo();
    }catch(e){console.log("mongoose install failed:",e.message);}
  }
});
