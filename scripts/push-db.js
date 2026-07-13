#!/usr/bin/env node
/**
 * Mirror data/jobs.json into Neon Postgres. Runs in GitHub Actions after
 * tracker.js (needs DATABASE_URL). Creates tables on first run.
 *
 *   node scripts/push-db.js [--seed-user-state]
 *
 * --seed-user-state additionally imports data/userstate.json (one-time
 * migration of local seen/applied state; never overwrites existing rows).
 */

const fs = require("fs");
const path = require("path");
const { neon } = require("@neondatabase/serverless");

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL not set");
  process.exit(1);
}
const sql = neon(process.env.DATABASE_URL);
const ROOT = path.join(__dirname, "..");

async function main() {
  await sql`
    CREATE TABLE IF NOT EXISTS jobs (
      key TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      company TEXT NOT NULL,
      url TEXT NOT NULL,
      location TEXT,
      source TEXT,
      matched TEXT,
      salary TEXT,
      posted_at TIMESTAMPTZ,
      worldwide BOOLEAN NOT NULL DEFAULT FALSE,
      relocation BOOLEAN NOT NULL DEFAULT FALSE,
      first_seen_at TIMESTAMPTZ NOT NULL,
      last_seen_at TIMESTAMPTZ NOT NULL,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      expired_at TIMESTAMPTZ
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS user_state (
      key TEXT PRIMARY KEY,
      seen_at TIMESTAMPTZ,
      applied_at TIMESTAMPTZ
    )`;

  const jobs = Object.values(JSON.parse(fs.readFileSync(path.join(ROOT, "data", "jobs.json"), "utf8")));
  const CHUNK = 20;
  for (let i = 0; i < jobs.length; i += CHUNK) {
    await Promise.all(jobs.slice(i, i + CHUNK).map(j => sql`
      INSERT INTO jobs (key, title, company, url, location, source, matched, salary,
                        posted_at, worldwide, relocation, first_seen_at, last_seen_at, active, expired_at)
      VALUES (${j.key}, ${j.title}, ${j.company}, ${j.url}, ${j.location}, ${j.source},
              ${(j.matched || []).join(",")}, ${j.salary || ""},
              ${j.postedAt ? new Date(j.postedAt) : null}, ${!!j.worldwide}, ${!!j.relocation},
              ${j.firstSeenAt}, ${j.lastSeenAt}, ${!!j.active}, ${j.expiredAt})
      ON CONFLICT (key) DO UPDATE SET
        title = EXCLUDED.title, company = EXCLUDED.company, url = EXCLUDED.url,
        location = EXCLUDED.location, source = EXCLUDED.source, matched = EXCLUDED.matched,
        salary = EXCLUDED.salary, posted_at = COALESCE(EXCLUDED.posted_at, jobs.posted_at),
        worldwide = EXCLUDED.worldwide, relocation = EXCLUDED.relocation,
        last_seen_at = EXCLUDED.last_seen_at, active = EXCLUDED.active,
        expired_at = EXCLUDED.expired_at
    `));
  }
  console.log(`pushed ${jobs.length} jobs to Postgres`);

  if (process.argv.includes("--seed-user-state")) {
    let user = {};
    try { user = JSON.parse(fs.readFileSync(path.join(ROOT, "data", "userstate.json"), "utf8")); } catch {}
    for (const [key, s] of Object.entries(user)) {
      await sql`
        INSERT INTO user_state (key, seen_at, applied_at)
        VALUES (${key}, ${s.seenAt || null}, ${s.appliedAt || null})
        ON CONFLICT (key) DO NOTHING
      `;
    }
    console.log(`seeded ${Object.keys(user).length} user-state rows`);
  }
}

main().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
