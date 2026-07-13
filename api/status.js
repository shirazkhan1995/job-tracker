const { sql, authorized, deny } = require("../lib/db");

module.exports = async (req, res) => {
  if (!authorized(req)) return deny(res);
  const [row] = await sql`
    SELECT
      (SELECT max(last_seen_at) FROM jobs) AS last_run,
      count(*) FILTER (WHERE j.active) AS active,
      count(*) FILTER (WHERE j.active AND s.seen_at IS NULL AND s.applied_at IS NULL) AS new,
      count(*) FILTER (WHERE s.applied_at IS NOT NULL) AS applied,
      count(*) FILTER (WHERE NOT j.active) AS expired
    FROM jobs j LEFT JOIN user_state s USING (key)
  `;
  res.setHeader("Cache-Control", "no-store");
  res.status(200).json({
    running: false,
    canRefresh: false, // scraping is done by GitHub Actions on schedule
    lastRunAt: row.last_run ? new Date(row.last_run).getTime() : null,
    counts: {
      active: +row.active, new: +row.new, applied: +row.applied, expired: +row.expired,
    },
  });
};
