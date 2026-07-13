# Job Tracker

Daily tracker for remote jobs matching:

> **("playwright" OR "puppeteer" OR "webdriverio") AND ("javascript" OR "typescript")**

Matching is **case-insensitive** with word boundaries, applied to the job title + full description. Remote jobs only; all open postings qualify regardless of posting age (the date is shown). Location judgment is left to you — the report/UI shows each job's location string.

The web UI (Vercel) supports per-job and **bulk** actions: mark seen (automatic on click), applied, or "not gonna apply" (dismissed). State lives in Neon Postgres, synced across devices.

## Run

```bash
node tracker.js
```

- **First run** backfills jobs posted in the last 60 days (`maxAgeDays` in `config.json`).
- **Subsequent runs** report only jobs not seen before — that's your daily "new jobs" report.

Output: `reports/YYYY-MM-DD.md` (and a `reports/latest.md` copy).

## How it gets "all companies"

Greenhouse / Lever / Ashby have **no global search API** — they only expose per-company endpoints. So the tracker works in two layers:

1. **Global aggregators** (cover any company, no API key needed):
   - Himalayas (`himalayas.app/jobs/api`, paged incrementally back to the last run)
   - WeWorkRemotely (main + programming category RSS feeds; best source of "Anywhere in the World" roles)
   - Jobicy, Remotive, RemoteOK
2. **Direct ATS boards** (`data/boards.json`, ~1,450 across Greenhouse/Lever/Ashby/**Recruitee** and growing): polled directly on every run (full postings, publish dates, salary where available). Dead slugs (404) are auto-retired to `invalid`. The list grows via:
   - payload/link scanning of every fetched aggregator job;
   - the current HN "Who is hiring" thread (scanned daily);
   - probing ATS slugs derived from company names seen in the day's feeds (capped by `probeCapPerRun`);
   - `node discover.js` — deep discovery: last 6 months of HN hiring threads, **Common Crawl enumeration of every `*.recruitee.com` career site** (~700 companies — a near-complete Recruitee directory), and probing ~1,500 actively-hiring YC companies against all four ATS APIs. Run it weekly-ish; failed probes are cached in `data/probed.json` and never repeated.

## Files

| Path | Purpose |
|---|---|
| `config.json` | keywords, age window, worldwide terms, paging caps |
| `data/seen.json` | jobs already reported (keyed by company+title) |
| `data/boards.json` | auto-growing ATS board list — add slugs by hand any time |
| `data/state.json` | last run timestamp |
| `reports/` | daily markdown reports |

## Daily schedule

Installed via `crontab` (09:00 every day):

```
0 9 * * * cd "/home/shriaz/Documents/job scraping" && /usr/bin/node tracker.js >> data/cron.log 2>&1
```

- View: `crontab -l` · Edit/remove: `crontab -e`
- Note: cron only fires if the machine is on at 09:00; running `node tracker.js` by hand any time is always safe.

## Known limitations

- Aggregator job pages (Himalayas/Jobicy/WWR) are Cloudflare-protected, so some report links go to the aggregator listing rather than the company's apply page.
- Remotive's public API currently returns only ~40 jobs (they restricted it), so it contributes little.
- Duplicate detection is by normalized company+title, so a company reposting the identical title later won't reappear.
- A "worldwide" label depends on the job board's own location field; some genuinely worldwide roles are labelled just "Remote" — those land in the second section rather than being lost.
