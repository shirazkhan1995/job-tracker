const { sql, authorized, deny } = require("../../lib/db");

module.exports = async (req, res) => {
  if (!authorized(req)) return deny(res);
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });
  const { key } = req.body || {};
  if (!key) return res.status(400).json({ error: "key required" });
  await sql`
    INSERT INTO user_state (key, seen_at) VALUES (${key}, now())
    ON CONFLICT (key) DO UPDATE SET seen_at = COALESCE(user_state.seen_at, now())
  `;
  res.status(200).json({ ok: true });
};
