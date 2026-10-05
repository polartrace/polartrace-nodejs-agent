// Minimal Express app monitored by Polartrace.
//
// Run it with the agent preloaded (no code changes needed):
//
//   POLARTRACE_APP_NAME=example-service \
//   POLARTRACE_LICENSE_KEY=<your-api-key> \
//   node -r polartrace server.js
//
const express = require("express");

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.json({ hello: "world" });
});

app.get("/slow", async (req, res) => {
  await new Promise((resolve) => setTimeout(resolve, 250));
  res.json({ took: "250ms" });
});

app.get("/error", (req, res) => {
  res.status(500).json({ error: "synthetic failure" });
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`example app listening on :${port}`);
});
