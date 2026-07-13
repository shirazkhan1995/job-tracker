#!/usr/bin/env node
/**
 * Deep ATS-board discovery. Run occasionally (weekly cron) or by hand:
 *   node discover.js
 *
 * Vectors:
 *  1. Hacker News "Ask HN: Who is hiring?" threads (last 6 months) — comments
 *     link Greenhouse/Lever/Ashby boards directly.
 *  2. Y Combinator public company dataset — probe slug candidates derived from
 *     company name/slug/website against all three ATS APIs.
 *
 * Positives land in data/boards.json (picked up by tracker.js on its next
 * run); failed probes are cached in data/probed.json so they never repeat.
 */

const fs = require("fs");
const path = require("path");

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const BOARDS_FILE = path.join(DATA_DIR, "boards.json");
const PROBED_FILE = path.join(DATA_DIR, "probed.json");
const CONCURRENCY = 16;
const UA = { "User-Agent": "Mozilla/5.0 (job-tracker discovery; personal use)" };

function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return fallback; }
}
function saveJSON(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}

async function getJSON(url) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 30000);
  try {
    const res = await fetch(url, { headers: UA, signal: ctl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally { clearTimeout(t); }
}

async function status(url) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 15000);
  try {
    const res = await fetch(url, { headers: UA, signal: ctl.signal });
    // drain minimal body so sockets are reusable
    res.body?.cancel?.();
    return res.status;
  } catch { return 0; }
  finally { clearTimeout(t); }
}

async function pool(items, worker, size = CONCURRENCY) {
  let i = 0;
  await Promise.all(Array.from({ length: Math.min(size, items.length) }, async () => {
    while (i < items.length) await worker(items[i++]);
  }));
}

const ATS_LINK_RE = /(?:boards|job-boards)\.greenhouse\.io\/([A-Za-z0-9_-]{2,})|jobs\.(?:eu\.)?lever\.co\/([A-Za-z0-9_-]{2,})|jobs\.ashbyhq\.com\/([A-Za-z0-9%_.-]{2,})/g;
const SLUG_BLOCKLIST = new Set(["embed", "job_board", "js", "jobs", "api", "wp-content", "careers", "www"]);

function addBoard(boards, ats, rawSlug, added) {
  const slug = decodeURIComponent(rawSlug).toLowerCase().replace(/[/?#.].*$/, "");
  if (!slug || SLUG_BLOCKLIST.has(slug)) return;
  boards[ats] = boards[ats] || [];
  if (!boards[ats].includes(slug) && !(boards.invalid || []).includes(`${ats}/${slug}`)) {
    boards[ats].push(slug);
    added.add(`${ats}/${slug}`);
  }
}

function decodeHN(s) {
  return (s || "")
    .replace(/&#x2F;/g, "/").replace(/&#x27;/g, "'")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&amp;/g, "&");
}

// ---- vector 1: HN Who is hiring ----

async function harvestHN(boards, added) {
  const threads = await getJSON(
    "https://hn.algolia.com/api/v1/search_by_date?query=%22who%20is%20hiring%22&tags=story,author_whoishiring&hitsPerPage=6"
  );
  for (const story of threads.hits) {
    for (let page = 0; page < 3; page++) {
      const j = await getJSON(
        `https://hn.algolia.com/api/v1/search_by_date?tags=comment,story_${story.objectID}&hitsPerPage=1000&page=${page}`
      );
      for (const h of j.hits) {
        const text = decodeHN(h.comment_text);
        let m;
        ATS_LINK_RE.lastIndex = 0;
        while ((m = ATS_LINK_RE.exec(text))) {
          if (m[1]) addBoard(boards, "greenhouse", m[1], added);
          else if (m[2]) addBoard(boards, "lever", m[2], added);
          else if (m[3]) addBoard(boards, "ashby", m[3], added);
        }
      }
      if (j.hits.length < 1000) break;
    }
    console.log(`  HN "${story.title}" scanned; boards so far: ${added.size}`);
  }
}

// ---- vector 2: YC companies ----

function slugCandidates(company) {
  const out = new Set();
  const norm = s => s.toLowerCase().trim();
  if (company.slug) out.add(norm(company.slug));
  if (company.name) {
    const n = norm(company.name).replace(/[^a-z0-9]+/g, "");
    const h = norm(company.name).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    if (n.length >= 3) out.add(n);
    if (h.length >= 3) out.add(h);
  }
  try {
    const host = new URL(company.website).hostname.replace(/^www\./, "");
    const label = host.split(".")[0];
    if (label.length >= 3) out.add(label.toLowerCase());
  } catch { /* no/bad website */ }
  return [...out];
}

const PROBES = {
  ashby: s => `https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(s)}`,
  greenhouse: s => `https://boards-api.greenhouse.io/v1/boards/${s}/jobs`,
  lever: s => `https://api.lever.co/v0/postings/${s}?mode=json&limit=1`,
  recruitee: s => `https://${s}.recruitee.com/api/offers/`,
};

// ---- vector 3: Common Crawl subdomain enumeration (full Recruitee directory) ----

async function harvestCommonCrawl(boards, added) {
  const info = await getJSON("https://index.commoncrawl.org/collinfo.json");
  const index = info[0].id;
  console.log(`  using index ${index}`);
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 300000);
  try {
    const res = await fetch(
      `https://index.commoncrawl.org/${index}-index?url=*.recruitee.com&output=json&fl=url&collapse=urlkey:45`,
      { headers: UA, signal: ctl.signal }
    );
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.text();
    for (const line of body.trim().split("\n")) {
      try {
        const h = new URL(JSON.parse(line).url).hostname;
        if (!h.endsWith(".recruitee.com")) continue;
        const slug = h.replace(".recruitee.com", "").toLowerCase();
        if (/^(www|api|app|assets|blog|help|support|status|docs|careers)$/.test(slug)) continue;
        if (/^[a-z0-9][a-z0-9-]{1,62}$/.test(slug)) addBoard(boards, "recruitee", slug, added);
      } catch { /* skip malformed lines */ }
    }
  } finally { clearTimeout(t); }
}

async function probeCompanies(companies, boards, probed, added) {
  const atses = Object.keys(PROBES);
  const known = new Set();
  for (const ats of atses) {
    for (const s of boards[ats] || []) known.add(`${ats}/${s}`);
  }
  let done = 0;
  await pool(companies, async company => {
    if (++done % 200 === 0) console.log(`  probed ${done}/${companies.length} companies; found ${added.size}`);
    for (const slug of slugCandidates(company)) {
      for (const ats of atses) {
        const key = `${ats}/${slug}`;
        if (known.has(key) || probed[key]) continue;
        const code = await status(PROBES[ats](slug));
        if (code === 200) {
          addBoard(boards, ats, slug, added);
          known.add(key);
          return; // one live board per company is enough
        }
        probed[key] = 1;
      }
    }
  });
}

async function main() {
  const boards = loadJSON(BOARDS_FILE, { greenhouse: [], lever: [], ashby: [], invalid: [] });
  const probed = loadJSON(PROBED_FILE, {});
  const added = new Set();
  const before = { ...Object.fromEntries(["greenhouse", "lever", "ashby", "recruitee"].map(a => [a, (boards[a] || []).length])) };

  console.log("[1/3] Harvesting HN 'Who is hiring' threads (last 6 months)...");
  try { await harvestHN(boards, added); } catch (e) { console.error("  HN harvest failed:", e.message); }
  saveJSON(BOARDS_FILE, boards);

  console.log("[2/3] Enumerating *.recruitee.com from Common Crawl...");
  try { await harvestCommonCrawl(boards, added); } catch (e) { console.error("  Common Crawl failed:", e.message); }
  saveJSON(BOARDS_FILE, boards);

  console.log("[3/3] Probing YC companies (isHiring) against Ashby/Greenhouse/Lever/Recruitee...");
  try {
    const yc = await getJSON("https://yc-oss.github.io/api/companies/all.json");
    const hiring = yc.filter(c => c.isHiring && c.status !== "Inactive");
    console.log(`  ${hiring.length} hiring YC companies to probe.`);
    await probeCompanies(hiring, boards, probed, added);
  } catch (e) { console.error("  YC probe failed:", e.message); }

  saveJSON(BOARDS_FILE, boards);
  saveJSON(PROBED_FILE, probed);
  const after = Object.fromEntries(["greenhouse", "lever", "ashby", "recruitee"].map(a => [a, (boards[a] || []).length]));
  console.log(`Done. +${added.size} boards. greenhouse ${before.greenhouse}->${after.greenhouse}, lever ${before.lever}->${after.lever}, ashby ${before.ashby}->${after.ashby}, recruitee ${before.recruitee}->${after.recruitee}`);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
