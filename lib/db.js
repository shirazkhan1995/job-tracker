const { neon } = require("@neondatabase/serverless");

const sql = neon(process.env.DATABASE_URL);

// Single-user auth: ACCESS_CODE env var on Vercel; the frontend sends it as a
// header (asked for once per device, kept in localStorage).
function authorized(req) {
  const want = process.env.ACCESS_CODE || "";
  if (!want) return true;
  return (req.headers["x-access-code"] || "") === want;
}

function deny(res) {
  res.status(401).json({ error: "unauthorized" });
}

module.exports = { sql, authorized, deny };
