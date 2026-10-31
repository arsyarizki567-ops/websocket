const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 3000;
const MXS_KEY = process.env.MXS_KEY || "MXS-DEFAULT-KEY-CHANGE-THIS";

// Dedupe cache: key = "jobId:name" → timestamp last broadcast
// Kalau jobId+name sama dalam DEDUPE_TTL detik, skip broadcast
const DEDUPE_TTL = 60 * 1000; // 60 detik
const dedupeCache = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k, t] of dedupeCache) {
    if (now - t > DEDUPE_TTL) dedupeCache.delete(k);
  }
}, 30000);

const server = http.createServer((req, res) => {
  // Health check
  if (req.method === "GET" && req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("MXS WS Server OK");
    return;
  }

  // Scanner POST endpoint: POST /find?key=MXS-...
  if (req.method === "POST" && req.url.startsWith("/find")) {
    const urlKey = new URL("http://x" + req.url).searchParams.get("key");
    if (urlKey !== MXS_KEY) {
      res.writeHead(403); res.end("Invalid key"); return;
    }
    let body = "";
    req.on("data", d => body += d);
    req.on("end", () => {
      let payload;
      try { payload = JSON.parse(body); } catch { res.writeHead(400); res.end("Bad JSON"); return; }

      // Dedupe: skip kalau jobId sama dalam 60 detik (1 server = 1 broadcast per TTL)
      const dedupeKey = payload.jobId || `noJob:${Date.now()}`;
      const now = Date.now();
      if (payload.jobId && dedupeCache.has(dedupeKey) && now - dedupeCache.get(dedupeKey) < DEDUPE_TTL) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, broadcast: 0, deduped: true }));
        return;
      }
      if (payload.jobId) dedupeCache.set(dedupeKey, now);

      // Broadcast ke semua WS client yang pake key sama
      const msg = JSON.stringify(payload);
      let sent = 0;
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.authed) {
          client.send(msg);
          sent++;
        }
      });

      const topBR = Array.isArray(payload.brainrots) && payload.brainrots[0] ? payload.brainrots[0].name : (payload.name || "?");
      console.log(`[FIND] ${topBR} owner=${payload.owner || "?"} job=${(payload.jobId||"").slice(0,8)}... brs=${Array.isArray(payload.brainrots)?payload.brainrots.length:1} broadcast=${sent}`);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, broadcast: sent }));
    });
    return;
  }

  res.writeHead(404); res.end("Not found");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  const urlKey = new URL("http://x" + req.url).searchParams.get("key");
  if (urlKey !== MXS_KEY) {
    ws.close(4001, "Invalid key");
    return;
  }
  ws.authed = true;
  console.log(`[WS] Client connected. Total: ${[...wss.clients].filter(c=>c.authed).length}`);

  ws.on("message", (raw) => {
    // Relay presence / mxs_user messages ke semua client lain
    let data;
    try { data = JSON.parse(raw.toString()); } catch { return; }
    if (data.type === "mxs_user" || data.type === "hello") {
      wss.clients.forEach(client => {
        if (client !== ws && client.readyState === WebSocket.OPEN && client.authed) {
          client.send(raw.toString());
        }
      });
    }
  });

  ws.on("close", () => {
    console.log(`[WS] Client disconnected. Total: ${[...wss.clients].filter(c=>c.authed && c.readyState===WebSocket.OPEN).length}`);
  });

  ws.on("error", () => {});
});

server.listen(PORT, () => {
  console.log(`MXS WS Server running on port ${PORT}`);
  console.log(`Key: ${MXS_KEY}`);
});
