const { sql, authorized, deny } = require("../../lib/db");

module.exports = async (req, res) => {
  if (!authorized(req)) return deny(res);
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });
  const { key, applied } = req.body || {};
  if (!key) return res.status(400).json({ error: "key required" });
  if (applied) {
    await sql`
      INSERT INTO user_state (key, seen_at, applied_at) VALUES (${key}, now(), now())
      ON CONFLICT (key) DO UPDATE SET
        seen_at = COALESCE(user_state.seen_at, now()),
        applied_at = COALESCE(user_state.applied_at, now())
    `;
  } else {
    await sql`
      INSERT INTO user_state (key, seen_at, applied_at) VALUES (${key}, now(), NULL)
      ON CONFLICT (key) DO UPDATE SET applied_at = NULL
    `;
  }
  res.status(200).json({ ok: true });
};
