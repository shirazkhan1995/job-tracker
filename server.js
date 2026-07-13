#!/usr/bin/env node
/**
 * Job tracker web app — dependency-free Node server.
 *
 *   node server.js            → http://localhost:8787
 *   PORT=3000 node server.js  → custom port
 *
 * API:
 *   GET  /api/jobs            all jobs (tracker db merged with seen/applied state)
 *   POST /api/jobs/seen       {key}                mark clicked/seen
 *   POST /api/jobs/applied    {key, applied}       toggle applied
 *   POST /api/refresh         run tracker.js now (one at a time)
 *   GET  /api/status          {running, lastRunAt, counts}
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const JOBS_FILE = path.join(DATA_DIR, "jobs.json");
const USER_FILE = path.join(DATA_DIR, "userstate.json");
const STATE_FILE = path.join(DATA_DIR, "state.json");
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";

function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return fallback; }
}
function saveJSON(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}

let refreshing = null; // child process while tracker runs

function jobList() {
  const jobs = loadJSON(JOBS_FILE, {});
  const user = loadJSON(USER_FILE, {});
  return Object.values(jobs).map(j => ({
    ...j,
    seen: !!user[j.key]?.seenAt,
    seenAt: user[j.key]?.seenAt || null,
    applied: !!user[j.key]?.appliedAt,
    appliedAt: user[j.key]?.appliedAt || null,
    dismissed: !!user[j.key]?.dismissedAt,
    dismissedAt: user[j.key]?.dismissedAt || null,
  }));
}

function send(res, code, body, type = "application/json") {
  const data = type === "application/json" ? JSON.stringify(body) : body;
  res.writeHead(code, { "Content-Type": type, "Cache-Control": "no-store" });
  res.end(data);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let d = "";
    req.on("data", c => { d += c; if (d.length > 1e6) req.destroy(); });
    req.on("end", () => { try { resolve(d ? JSON.parse(d) : {}); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  try {
    if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
      return send(res, 200, fs.readFileSync(path.join(ROOT, "index.html")), "text/html; charset=utf-8");
    }
    if (req.method === "GET" && url.pathname === "/api/jobs") {
      return send(res, 200, { jobs: jobList() });
    }
    if (req.method === "GET" && url.pathname === "/api/status") {
      const state = loadJSON(STATE_FILE, {});
      const jobs = jobList();
      return send(res, 200, {
        running: !!refreshing,
        canRefresh: true,
        lastRunAt: state.lastRunAt || null,
        counts: {
          active: jobs.filter(j => j.active).length,
          new: jobs.filter(j => j.active && !j.seen && !j.applied && !j.dismissed).length,
          applied: jobs.filter(j => j.applied).length,
          expired: jobs.filter(j => !j.active).length,
        },
      });
    }
    if (req.method === "POST" && url.pathname === "/api/jobs/seen") {
      const { key } = await readBody(req);
      if (!key) return send(res, 400, { error: "key required" });
      const user = loadJSON(USER_FILE, {});
      user[key] = { ...user[key], seenAt: user[key]?.seenAt || new Date().toISOString() };
      saveJSON(USER_FILE, user);
      return send(res, 200, { ok: true });
    }
    if (req.method === "POST" && url.pathname === "/api/jobs/applied") {
      const { key, applied } = await readBody(req);
      if (!key) return send(res, 400, { error: "key required" });
      const user = loadJSON(USER_FILE, {});
      user[key] = { ...user[key], seenAt: user[key]?.seenAt || new Date().toISOString() };
      user[key].appliedAt = applied ? (user[key].appliedAt || new Date().toISOString()) : null;
      saveJSON(USER_FILE, user);
      return send(res, 200, { ok: true });
    }
    if (req.method === "POST" && url.pathname === "/api/jobs/dismissed") {
      const { key, dismissed } = await readBody(req);
      if (!key) return send(res, 400, { error: "key required" });
      const user = loadJSON(USER_FILE, {});
      user[key] = { ...user[key] };
      user[key].dismissedAt = dismissed ? (user[key].dismissedAt || new Date().toISOString()) : null;
      saveJSON(USER_FILE, user);
      return send(res, 200, { ok: true });
    }
    if (req.method === "POST" && url.pathname === "/api/refresh") {
      if (refreshing) return send(res, 409, { error: "refresh already running" });
      refreshing = spawn(process.execPath, [path.join(ROOT, "tracker.js")], {
        cwd: ROOT, stdio: ["ignore", "inherit", "inherit"],
      });
      refreshing.on("exit", () => { refreshing = null; });
      return send(res, 202, { ok: true });
    }
    send(res, 404, { error: "not found" });
  } catch (e) {
    send(res, 500, { error: e.message });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`Job tracker UI → http://localhost:${PORT} (bound to ${HOST})`);
});
