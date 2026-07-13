#!/usr/bin/env node
/**
 * Job tracker: finds remote jobs matching
 *   ("playwright" OR "puppeteer" OR "cypress") AND ("javascript" OR "typescript")
 * across global remote-job APIs (Remotive, Jobicy, Himalayas, RemoteOK), and
 * auto-discovers Greenhouse / Lever / Ashby boards from matched jobs, which it
 * then polls directly on every subsequent run.
 *
 * Matching is case-INsensitive with word boundaries.
 *
 * Usage: node tracker.js
 * State: data/seen.json (jobs already reported), data/boards.json (discovered ATS boards)
 * Output: reports/YYYY-MM-DD.md and reports/latest.md
 */

const fs = require("fs");
const path = require("path");

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const REPORT_DIR = path.join(ROOT, "reports");
const CONFIG = JSON.parse(fs.readFileSync(path.join(ROOT, "config.json"), "utf8"));

const NOW = Date.now();
const MAX_AGE_MS = CONFIG.maxAgeDays * 24 * 60 * 60 * 1000;
const CUTOFF = NOW - MAX_AGE_MS;
const UA = { "User-Agent": "Mozilla/5.0 (job-tracker; personal use)" };

// ---------- small utils ----------

function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return fallback; }
}
function saveJSON(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}

async function getJSON(url, opts = {}) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), CONFIG.requestTimeoutMs);
  try {
    const res = await fetch(url, { headers: UA, signal: ctl.signal, ...opts });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

async function pool(items, worker, size = CONFIG.concurrency) {
  const results = [];
  let i = 0;
  const lanes = Array.from({ length: Math.min(size, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await worker(items[idx]);
    }
  });
  await Promise.all(lanes);
  return results;
}

function decodeEntities(s) {
  return s
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'").replace(/&nbsp;/g, " ")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n))
    .replace(/&amp;/g, "&");
}
function stripHtml(s) {
  return decodeEntities(String(s || ""))
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// ---------- keyword matching (case-insensitive, word-boundary) ----------

function termRegex(term) {
  const esc = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(?<![A-Za-z0-9])${esc}(?![A-Za-z0-9])`, "i");
}
const ANY_OF = CONFIG.keywords.anyOf.map(t => ({ t, re: termRegex(t) }));
const ALL_OF = CONFIG.keywords.allOf.map(group => group.map(t => ({ t, re: termRegex(t) })));

function matchKeywords(text) {
  const matched = [];
  let anyHit = false;
  for (const { t, re } of ANY_OF) if (re.test(text)) { anyHit = true; matched.push(t); }
  if (!anyHit) return null;
  for (const group of ALL_OF) {
    let groupHit = false;
    for (const { t, re } of group) if (re.test(text)) { groupHit = true; matched.push(t); }
    if (!groupHit) return null;
  }
  return matched;
}

// ---------- normalized job shape ----------
// { source, company, title, url, location, postedAt(ms|null), matched[], salary }

function normKey(job) {
  return `${job.company}|${job.title}`.toLowerCase().replace(/\s+/g, " ").trim();
}

// ---------- global aggregator sources ----------

async function fetchRemotive(errors) {
  const jobs = [];
  for (const kw of CONFIG.keywords.anyOf) {
    try {
      const j = await getJSON(`https://remotive.com/api/remote-jobs?search=${encodeURIComponent(kw)}`);
      for (const x of j.jobs || []) {
        jobs.push({
          source: "remotive",
          company: x.company_name,
          title: x.title,
          url: x.url,
          location: x.candidate_required_location || "Remote",
          postedAt: x.publication_date ? Date.parse(x.publication_date) : null,
          salary: x.salary || "",
          rawHtml: x.description || "",
          text: `${x.title} ${stripHtml(x.description)}`,
        });
      }
    } catch (e) { errors.push(`remotive(${kw}): ${e.message}`); }
  }
  return jobs;
}

async function fetchJobicy(errors) {
  const jobs = [];
  for (const kw of CONFIG.keywords.anyOf) {
    try {
      const j = await getJSON(`https://jobicy.com/api/v2/remote-jobs?count=100&tag=${encodeURIComponent(kw)}`);
      for (const x of j.jobs || []) {
        jobs.push({
          source: "jobicy",
          company: x.companyName,
          title: x.jobTitle,
          url: x.url,
          location: x.jobGeo || "Remote",
          postedAt: x.pubDate ? Date.parse(x.pubDate) : null,
          salary: "",
          rawHtml: x.jobDescription || "",
          text: `${x.jobTitle} ${stripHtml(x.jobDescription)}`,
        });
      }
    } catch (e) { errors.push(`jobicy(${kw}): ${e.message}`); }
  }
  return jobs;
}

async function fetchHimalayas(errors, stopAt) {
  // API returns 20 jobs/page newest-first; page back until `stopAt` (last run
  // time, or the age cutoff on first run), hard-capped to avoid runaway.
  const PAGE = 20;
  const jobs = [];
  let done = false;
  try {
    for (let p = 0; p < CONFIG.himalayas.maxPages && !done; p += CONFIG.concurrency) {
      const offsets = [];
      for (let k = p; k < Math.min(p + CONFIG.concurrency, CONFIG.himalayas.maxPages); k++) {
        offsets.push(k * PAGE);
      }
      const batches = await Promise.all(offsets.map(off =>
        getJSON(`https://himalayas.app/jobs/api?limit=${PAGE}&offset=${off}`)
          .then(j => j.jobs || [])
          .catch(() => null)
      ));
      for (const batch of batches) {
        if (!batch || !batch.length) { done = true; continue; }
        for (const x of batch) {
          const postedAt = x.pubDate ? x.pubDate * 1000 : null;
          const restr = Array.isArray(x.locationRestrictions) ? x.locationRestrictions : [];
          jobs.push({
            source: "himalayas",
            company: x.companyName,
            title: x.title,
            url: x.applicationLink || x.guid,
            location: restr.length ? restr.join(", ") : "Worldwide (no restriction)",
            postedAt,
            salary: x.minSalary ? `${x.minSalary}-${x.maxSalary || ""} ${x.currency || ""}` : "",
            rawHtml: x.description || "",
            text: `${x.title} ${stripHtml(x.description)}`,
          });
        }
        const oldest = batch[batch.length - 1].pubDate * 1000;
        if (oldest && oldest < stopAt) done = true;
      }
    }
  } catch (e) { errors.push(`himalayas: ${e.message}`); }
  return jobs;
}

async function fetchRemoteOK(errors) {
  const jobs = [];
  try {
    const j = await getJSON("https://remoteok.com/api");
    for (const x of j) {
      if (!x || !x.id) continue; // first element is a legal notice
      jobs.push({
        source: "remoteok",
        company: x.company,
        title: x.position,
        url: x.url || x.apply_url,
        location: x.location || "Remote",
        postedAt: x.date ? Date.parse(x.date) : null,
        salary: x.salary_min ? `$${x.salary_min}-$${x.salary_max}` : "",
        rawHtml: x.description || "",
        text: `${x.position} ${stripHtml(x.description)} ${(x.tags || []).join(" ")}`,
      });
    }
  } catch (e) { errors.push(`remoteok: ${e.message}`); }
  return jobs;
}

const WWR_FEEDS = [
  "https://weworkremotely.com/remote-jobs.rss",
  "https://weworkremotely.com/categories/remote-programming-jobs.rss",
  "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
  "https://weworkremotely.com/categories/remote-front-end-programming-jobs.rss",
  "https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss",
  "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
];

function rssField(item, tag) {
  const m = item.match(new RegExp(`<${tag}>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</${tag}>`));
  return m ? m[1].trim() : "";
}

async function fetchWWR(errors) {
  const jobs = [];
  const seenGuids = new Set();
  for (const feed of WWR_FEEDS) {
    try {
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), CONFIG.requestTimeoutMs);
      const res = await fetch(feed, { headers: UA, signal: ctl.signal });
      clearTimeout(t);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const xml = await res.text();
      for (const item of xml.match(/<item>[\s\S]*?<\/item>/g) || []) {
        const guid = rssField(item, "guid") || rssField(item, "link");
        if (seenGuids.has(guid)) continue;
        seenGuids.add(guid);
        const rawTitle = decodeEntities(rssField(item, "title"));
        const sep = rawTitle.indexOf(": ");
        const company = sep > 0 ? rawTitle.slice(0, sep) : "?";
        const title = sep > 0 ? rawTitle.slice(sep + 2) : rawTitle;
        const desc = rssField(item, "description");
        const pub = rssField(item, "pubDate");
        jobs.push({
          source: "weworkremotely",
          company,
          title,
          url: rssField(item, "link") || guid,
          location: decodeEntities(rssField(item, "region")) || "Remote",
          postedAt: pub ? Date.parse(pub) : null,
          salary: "",
          rawHtml: desc,
          text: `${title} ${stripHtml(desc)}`,
        });
      }
    } catch (e) { errors.push(`weworkremotely(${feed.split("/").pop()}): ${e.message}`); }
  }
  return jobs;
}

// ---------- ATS board sources (auto-discovered) ----------

// Verified-live boards used to seed data/boards.json on first run; the list
// grows automatically as discovery finds more.
const DEFAULT_BOARDS = {
  greenhouse: [
    "airbnb", "anthropic", "brex", "canonical", "cloudflare", "coinbase",
    "databricks", "duolingo", "elastic", "figma", "gitlab", "grafanalabs",
    "instacart", "mongodb", "okta", "postman", "reddit", "remotecom",
    "saucelabs", "smartbear", "stripe", "twilio", "vercel",
  ],
  lever: ["aircall", "binance", "nium", "spotify", "veeva", "voodoo"],
  ashby: [
    "clerk", "cursor", "deel", "docker", "elevenlabs", "linear", "notion",
    "openai", "posthog", "qawolf", "ramp", "replit", "sierra", "supabase", "vanta",
  ],
  invalid: [],
};

async function fetchGreenhouseBoard(slug, errors) {
  try {
    const j = await getJSON(`https://boards-api.greenhouse.io/v1/boards/${slug}/jobs?content=true`);
    return (j.jobs || []).map(x => {
      const locs = [x.location?.name, ...(x.offices || []).map(o => o?.name)].filter(Boolean).join("; ");
      return {
        source: `greenhouse:${slug}`,
        company: x.company_name || slug,
        title: x.title,
        url: x.absolute_url,
        location: locs || "?",
        postedAt: Date.parse(x.first_published || x.updated_at) || null,
        salary: "",
        rawHtml: x.content || "",
        text: `${x.title} ${stripHtml(x.content)}`,
        remoteHint: /remote/i.test(locs),
      };
    });
  } catch (e) { errors.push(`greenhouse/${slug}: ${e.message}`); return []; }
}

async function fetchLeverBoard(slug, errors) {
  try {
    const j = await getJSON(`https://api.lever.co/v0/postings/${slug}?mode=json`);
    return (j || []).map(x => {
      const locs = [x.categories?.location, ...(x.categories?.allLocations || []), x.country]
        .filter(Boolean).join("; ");
      return {
        source: `lever:${slug}`,
        company: slug,
        title: x.text,
        url: x.hostedUrl,
        location: locs || "?",
        postedAt: x.createdAt || null,
        salary: "",
        rawHtml: x.description || "",
        text: `${x.text} ${x.descriptionPlain || ""} ${(x.lists || []).map(l => `${l.text} ${stripHtml(l.content)}`).join(" ")} ${x.additionalPlain || ""}`,
        remoteHint: x.workplaceType === "remote" || /remote/i.test(locs),
      };
    });
  } catch (e) { errors.push(`lever/${slug}: ${e.message}`); return []; }
}

async function fetchAshbyBoard(slug, errors) {
  try {
    const j = await getJSON(`https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(slug)}?includeCompensation=true`);
    return (j.jobs || []).map(x => {
      const locs = [x.location, ...(x.secondaryLocations || []).map(l => l?.location)].filter(Boolean).join("; ");
      return {
        source: `ashby:${slug}`,
        company: slug,
        title: x.title,
        url: x.jobUrl,
        location: locs || "?",
        postedAt: x.publishedAt ? Date.parse(x.publishedAt) : null,
        salary: x.compensation?.compensationTierSummary || "",
        rawHtml: x.descriptionHtml || "",
        text: `${x.title} ${x.descriptionPlain || stripHtml(x.descriptionHtml)}`,
        remoteHint: x.isRemote === true || /remote/i.test(locs),
      };
    });
  } catch (e) { errors.push(`ashby/${slug}: ${e.message}`); return []; }
}

async function fetchRecruiteeBoard(slug, errors) {
  try {
    const j = await getJSON(`https://${slug}.recruitee.com/api/offers/`);
    return (j.offers || []).map(x => {
      const locs = [x.location, x.city, x.state_name, x.country].filter(Boolean).join("; ");
      return {
        source: `recruitee:${slug}`,
        company: x.company_name || slug,
        title: x.title,
        url: x.careers_url || `https://${slug}.recruitee.com/o/${x.slug}`,
        location: locs || "?",
        postedAt: Date.parse(x.published_at || x.created_at) || null,
        salary: x.salary?.min ? `${x.salary.min}-${x.salary.max || ""} ${x.salary.currency || ""}` : "",
        rawHtml: `${x.description || ""} ${x.requirements || ""}`,
        text: `${x.title} ${stripHtml(x.description)} ${stripHtml(x.requirements)}`,
        remoteHint: x.remote === true || /remote/i.test(locs),
      };
    });
  } catch (e) { errors.push(`recruitee/${slug}: ${e.message}`); return []; }
}

// ---------- board discovery ----------

const BOARD_PATTERNS = [
  { ats: "greenhouse", re: /(?:boards|job-boards)\.greenhouse\.io\/(?:embed\/job_board\?(?:[^"'\s]*&)?for=)?([A-Za-z0-9_-]{2,})/g },
  { ats: "greenhouse", re: /greenhouse\.io\/embed\/job_board\/js\?for=([A-Za-z0-9_-]{2,})/g },
  { ats: "lever", re: /jobs\.(?:eu\.)?lever\.co\/([A-Za-z0-9_-]{2,})/g },
  { ats: "ashby", re: /jobs\.ashbyhq\.com\/(?!api)([A-Za-z0-9%_.-]{2,})/g },
  { ats: "ashby", re: /api\.ashbyhq\.com\/posting-api\/job-board\/([A-Za-z0-9%_.-]{2,})/g },
  { ats: "recruitee", re: /([a-z0-9-]{2,})\.recruitee\.com/g },
];
const SLUG_BLOCKLIST = new Set(["embed", "job_board", "js", "jobs", "api", "wp-content", "careers", "www"]);

function scanForBoards(hay, boards) {
  let found = 0;
  for (const { ats, re } of BOARD_PATTERNS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(hay))) {
      const slug = decodeURIComponent(m[1]).toLowerCase().replace(/[/?#.].*$/, "");
      if (!slug || SLUG_BLOCKLIST.has(slug)) continue;
      boards[ats] = boards[ats] || [];
      if (!boards[ats].includes(slug) && !(boards.invalid || []).includes(`${ats}/${slug}`)) {
        boards[ats].push(slug);
        found++;
      }
    }
  }
  return found;
}

// Daily light-weight discovery vectors (deep discovery lives in discover.js):
// harvest the current HN "Who is hiring" thread, and probe ATS slugs derived
// from company names appearing in today's aggregator feeds.

async function probeStatus(url) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 15000);
  try {
    const res = await fetch(url, { headers: UA, signal: ctl.signal });
    res.body?.cancel?.();
    return res.status;
  } catch { return 0; }
  finally { clearTimeout(t); }
}

const PROBE_URLS = {
  ashby: s => `https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(s)}`,
  greenhouse: s => `https://boards-api.greenhouse.io/v1/boards/${s}/jobs`,
  lever: s => `https://api.lever.co/v0/postings/${s}?mode=json&limit=1`,
  recruitee: s => `https://${s}.recruitee.com/api/offers/`,
};
const PROBE_ATSES = Object.keys(PROBE_URLS);

async function harvestHNThread(boards, errors) {
  let found = 0;
  try {
    const threads = await getJSON(
      "https://hn.algolia.com/api/v1/search_by_date?query=%22who%20is%20hiring%22&tags=story,author_whoishiring&hitsPerPage=1"
    );
    const story = threads.hits[0];
    if (!story) return 0;
    const j = await getJSON(
      `https://hn.algolia.com/api/v1/search_by_date?tags=comment,story_${story.objectID}&hitsPerPage=1000`
    );
    const dec = s => (s || "").replace(/&#x2F;/g, "/").replace(/&amp;/g, "&");
    for (const h of j.hits) found += scanForBoards(dec(h.comment_text), boards);
  } catch (e) { errors.push(`hn-harvest: ${e.message}`); }
  return found;
}

function companySlugCandidates(name) {
  const base = String(name || "").toLowerCase()
    .replace(/\b(inc|llc|ltd|limited|gmbh|corp|co|ab|sl|srl|bv|pte|plc|sas|sa)\b\.?/g, " ")
    .trim();
  const out = new Set();
  const solid = base.replace(/[^a-z0-9]+/g, "");
  const hyphen = base.replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  if (solid.length >= 3) out.add(solid);
  if (hyphen.length >= 3 && hyphen !== solid) out.add(hyphen);
  return [...out];
}

async function probeCompanyNames(jobs, boards, probed) {
  const known = new Set();
  for (const ats of PROBE_ATSES) {
    for (const s of boards[ats] || []) known.add(`${ats}/${s}`);
  }
  const companies = [...new Set(jobs.map(j => j.company).filter(Boolean))];
  const tasks = [];
  for (const name of companies) {
    const cands = companySlugCandidates(name).filter(slug =>
      PROBE_ATSES.some(ats => !probed[`${ats}/${slug}`] && !known.has(`${ats}/${slug}`))
    );
    if (cands.length) tasks.push(cands);
    if (tasks.length >= CONFIG.probeCapPerRun) break;
  }
  let found = 0;
  await pool(tasks, async cands => {
    for (const slug of cands) {
      for (const ats of PROBE_ATSES) {
        const key = `${ats}/${slug}`;
        if (known.has(key) || probed[key]) continue;
        const code = await probeStatus(PROBE_URLS[ats](slug));
        if (code === 200) {
          boards[ats] = boards[ats] || [];
          if (!boards[ats].includes(slug)) { boards[ats].push(slug); found++; }
          known.add(key);
          return;
        }
        probed[key] = 1;
      }
    }
  });
  return found;
}

// Aggregator descriptions rarely embed the real apply link, so for new matches
// we fetch the listing page itself and scan its HTML for ATS board URLs.
async function discoverBoards(jobs, boards, errors) {
  let found = 0;
  for (const job of jobs) found += scanForBoards(`${job.url || ""} ${job.rawHtml || ""}`, boards);
  const toFetch = jobs.filter(j => j.url && j.url.startsWith("http")).slice(0, CONFIG.discoveryFetchCap);
  await pool(toFetch, async job => {
    try {
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), 20000);
      const res = await fetch(job.url, { headers: UA, signal: ctl.signal, redirect: "follow" });
      clearTimeout(t);
      if (!res.ok) return;
      const html = (await res.text()).slice(0, 2_000_000);
      found += scanForBoards(`${res.url} ${html}`, boards);
    } catch { /* discovery is best-effort */ }
  });
  return found;
}

// ---------- filtering ----------

function classify(job) {
  const matched = matchKeywords(job.text);
  if (!matched) return null;
  // no age filter: every job still listed on a board is open — the report
  // shows the posting date so freshness is visible
  const isAggregator = !job.source.includes(":"); // ATS board sources are "ats:slug"
  const isRemote = isAggregator || job.remoteHint || /remote|anywhere/i.test(job.location);
  if (!isRemote) return null;
  return { ...job, matched: [...new Set(matched)] };
}

// ---------- report ----------

function fmtDate(ms) {
  return ms ? new Date(ms).toISOString().slice(0, 10) : "unknown";
}

function renderJob(j) {
  const loc = j.location.length > 120 ? j.location.slice(0, 117) + "…" : j.location;
  const lines = [
    `### ${j.title} — ${j.company}`,
    `- **Posted:** ${fmtDate(j.postedAt)} · **Location:** ${loc} · **Source:** ${j.source}`,
    `- **Matched:** ${j.matched.join(", ")}${j.salary ? ` · **Salary:** ${j.salary}` : ""}`,
    `- **Link:** ${j.url}`,
    "",
  ];
  return lines.join("\n");
}

function renderReport(dateStr, newJobs, stats, errors, firstRun) {
  const jobs = [...newJobs].sort((a, b) => (b.postedAt || 0) - (a.postedAt || 0));
  const filterText =
    `(${CONFIG.keywords.anyOf.join(" OR ")}) AND ` +
    CONFIG.keywords.allOf.map(g => `(${g.join(" OR ")})`).join(" AND ");
  const out = [];
  out.push(`# Job Tracker Report — ${dateStr}`);
  out.push("");
  out.push(`> Filter: ${filterText} · remote only · all open postings`);
  out.push(`> Scanned: ${stats.scanned} jobs from ${stats.sources} sources (${stats.boards} ATS boards polled, ${stats.newBoards} newly discovered)`);
  if (firstRun) out.push(`> First run — backfilling last ${CONFIG.maxAgeDays} days. Future runs report only newly seen jobs.`);
  out.push("");
  out.push(`## 💼 New remote matches (${jobs.length})`);
  out.push("");
  out.push(jobs.length ? jobs.map(renderJob).join("\n") : "_No new matches today._\n");
  if (errors.length) {
    out.push(`## ⚠️ Source errors (${errors.length})`);
    out.push("");
    for (const e of errors) out.push(`- ${e}`);
    out.push("");
  }
  return out.join("\n");
}

// ---------- main ----------

async function main() {
  const seenFile = path.join(DATA_DIR, "seen.json");
  const boardsFile = path.join(DATA_DIR, "boards.json");
  const stateFile = path.join(DATA_DIR, "state.json");
  const probedFile = path.join(DATA_DIR, "probed.json");
  const seen = loadJSON(seenFile, {});
  const boards = loadJSON(boardsFile, DEFAULT_BOARDS);
  for (const ats of PROBE_ATSES) boards[ats] = boards[ats] || [];
  boards.invalid = boards.invalid || [];
  const state = loadJSON(stateFile, {});
  const firstRun = !state.lastRunAt;
  // page Himalayas back to the last run (with 1-day overlap), or the full
  // age window on first run
  const himalayasStopAt = firstRun ? CUTOFF : Math.max(CUTOFF, state.lastRunAt - 86400000);
  const errors = [];

  console.log(`[1/4] Fetching global sources (Remotive, Jobicy, Himalayas back to ${fmtDate(himalayasStopAt)}, RemoteOK, WeWorkRemotely)...`);
  const [remotive, jobicy, himalayas, remoteok, wwr] = await Promise.all([
    fetchRemotive(errors),
    fetchJobicy(errors),
    fetchHimalayas(errors, himalayasStopAt),
    fetchRemoteOK(errors),
    fetchWWR(errors),
  ]);
  const aggregatorJobs = [...remotive, ...jobicy, ...himalayas, ...remoteok, ...wwr];
  console.log(`      ${aggregatorJobs.length} jobs fetched.`);

  // every fetched job's payload is scanned for ATS board links; unseen
  // keyword matches additionally get their listing page fetched
  const aggregatorMatches = aggregatorJobs.map(classify).filter(Boolean);
  const unseenMatches = aggregatorMatches.filter(j => !seen[normKey(j)]);
  let newBoards = 0;
  for (const job of aggregatorJobs) newBoards += scanForBoards(job.rawHtml || "", boards);
  newBoards += await discoverBoards(unseenMatches, boards, errors);
  newBoards += await harvestHNThread(boards, errors);
  const probed = loadJSON(probedFile, {});
  newBoards += await probeCompanyNames(aggregatorJobs, boards, probed);
  saveJSON(probedFile, probed);
  console.log(`[2/4] ${aggregatorMatches.length} aggregator matches (${unseenMatches.length} unseen); ${newBoards} new ATS boards discovered.`);

  const boardTasks = [
    ...boards.greenhouse.map(s => ({ ats: "greenhouse", s })),
    ...boards.lever.map(s => ({ ats: "lever", s })),
    ...boards.ashby.map(s => ({ ats: "ashby", s })),
    ...boards.recruitee.map(s => ({ ats: "recruitee", s })),
  ];
  console.log(`[3/4] Polling ${boardTasks.length} ATS boards directly...`);
  const boardErrors = [];
  const boardJobs = (await pool(boardTasks, ({ ats, s }) =>
    ats === "greenhouse" ? fetchGreenhouseBoard(s, boardErrors)
    : ats === "lever" ? fetchLeverBoard(s, boardErrors)
    : ats === "ashby" ? fetchAshbyBoard(s, boardErrors)
    : fetchRecruiteeBoard(s, boardErrors)
  )).flat();

  // boards that 404 are dead slugs — remember so we don't retry forever
  for (const err of boardErrors) {
    if (/HTTP 404/.test(err)) {
      const [board] = err.split(":");
      boards.invalid = boards.invalid || [];
      if (!boards.invalid.includes(board)) boards.invalid.push(board);
      for (const ats of PROBE_ATSES) {
        boards[ats] = (boards[ats] || []).filter(s => `${ats}/${s}` !== board);
      }
    } else {
      errors.push(err);
    }
  }

  const boardMatches = boardJobs.map(classify).filter(Boolean);
  console.log(`      ${boardJobs.length} board jobs fetched, ${boardMatches.length} match.`);

  // dedupe (prefer direct ATS entries over aggregator copies), then keep only unseen
  const all = new Map();
  for (const j of [...aggregatorMatches, ...boardMatches]) {
    const k = normKey(j);
    const prev = all.get(k);
    const jDirect = j.source.includes(":");
    const prevDirect = prev?.source.includes(":");
    if (!prev || (jDirect && !prevDirect)) all.set(k, j);
  }

  const newJobs = [];
  for (const [k, j] of all) {
    if (seen[k]) continue;
    seen[k] = { firstSeen: new Date().toISOString(), title: j.title, company: j.company, url: j.url };
    newJobs.push(j);
  }

  // maintain the web app's job database (data/jobs.json): upsert everything
  // matched this run, expire what disappeared
  const jobsDbFile = path.join(DATA_DIR, "jobs.json");
  const jobsDb = loadJSON(jobsDbFile, {});
  const nowIso = new Date().toISOString();
  // boards that errored this run (non-404) can't prove a job is gone
  const failedSources = new Set();
  for (const err of boardErrors) {
    if (/HTTP 404/.test(err)) continue;
    const m = err.match(/^([a-z]+)\/([^:]+):/);
    if (m) failedSources.add(`${m[1]}:${m[2]}`);
  }
  for (const [k, j] of all) {
    const prev = jobsDb[k] || {};
    jobsDb[k] = {
      key: k,
      title: j.title, company: j.company, url: j.url,
      location: j.location, source: j.source,
      matched: j.matched, salary: j.salary || "",
      postedAt: j.postedAt || prev.postedAt || null,
      firstSeenAt: prev.firstSeenAt || nowIso,
      lastSeenAt: nowIso,
      active: true, expiredAt: null,
    };
  }
  const AGGREGATOR_TTL_MS = 30 * 86400000;
  for (const e of Object.values(jobsDb)) {
    if (!e.active || e.lastSeenAt === nowIso) continue;
    const fromBoard = (e.source || "").includes(":");
    if (fromBoard ? !failedSources.has(e.source)
                  : NOW - Date.parse(e.lastSeenAt) > AGGREGATOR_TTL_MS) {
      e.active = false;
      e.expiredAt = nowIso;
    }
  }
  saveJSON(jobsDbFile, jobsDb);

  const dateStr = new Date().toISOString().slice(0, 10);
  const stats = {
    scanned: aggregatorJobs.length + boardJobs.length,
    sources: 5 + boardTasks.length,
    boards: boardTasks.length,
    newBoards,
  };
  const report = renderReport(dateStr, newJobs, stats, errors, firstRun);

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  fs.writeFileSync(path.join(REPORT_DIR, `${dateStr}.md`), report);
  fs.writeFileSync(path.join(REPORT_DIR, "latest.md"), report);
  saveJSON(seenFile, seen);
  saveJSON(boardsFile, boards);
  saveJSON(stateFile, { ...state, lastRunAt: NOW });

  console.log(`[4/4] Done. ${newJobs.length} new matching jobs. Report: reports/${dateStr}.md`);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
