const { sql, authorized, deny } = require("../lib/db");

module.exports = async (req, res) => {
  if (!authorized(req)) return deny(res);
  const rows = await sql`
    SELECT j.*, s.seen_at, s.applied_at
    FROM jobs j LEFT JOIN user_state s USING (key)
  `;
  const jobs = rows.map(r => ({
    key: r.key,
    title: r.title,
    company: r.company,
    url: r.url,
    location: r.location || "?",
    source: r.source,
    matched: (r.matched || "").split(",").filter(Boolean),
    salary: r.salary || "",
    postedAt: r.posted_at ? new Date(r.posted_at).getTime() : null,
    worldwide: !!r.worldwide,
    relocation: !!r.relocation,
    firstSeenAt: r.first_seen_at,
    lastSeenAt: r.last_seen_at,
    active: !!r.active,
    expiredAt: r.expired_at,
    seen: !!r.seen_at,
    seenAt: r.seen_at,
    applied: !!r.applied_at,
    appliedAt: r.applied_at,
    dismissed: !!r.dismissed_at,
    dismissedAt: r.dismissed_at,
  }));
  res.setHeader("Cache-Control", "no-store");
  res.status(200).json({ jobs });
};
