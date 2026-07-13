const { sql, authorized, deny } = require("../../lib/db");

module.exports = async (req, res) => {
  if (!authorized(req)) return deny(res);
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });
  const { key, dismissed } = req.body || {};
  if (!key) return res.status(400).json({ error: "key required" });
  if (dismissed) {
    await sql`
      INSERT INTO user_state (key, dismissed_at) VALUES (${key}, now())
      ON CONFLICT (key) DO UPDATE SET dismissed_at = COALESCE(user_state.dismissed_at, now())
    `;
  } else {
    await sql`
      INSERT INTO user_state (key, dismissed_at) VALUES (${key}, NULL)
      ON CONFLICT (key) DO UPDATE SET dismissed_at = NULL
    `;
  }
  res.status(200).json({ ok: true });
};
