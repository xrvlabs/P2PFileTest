var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// server.ts
var import_express = __toESM(require("express"), 1);
var import_path = __toESM(require("path"), 1);
var import_vite = require("vite");
var sessions = /* @__PURE__ */ new Map();
setInterval(() => {
  const now = Date.now();
  for (const [code, sess] of sessions.entries()) {
    if (now - sess.lastActive > 24 * 60 * 60 * 1e3) {
      sessions.delete(code);
    }
  }
}, 10 * 60 * 1e3);
async function startServer() {
  const app = (0, import_express.default)();
  const PORT = 3e3;
  app.use(import_express.default.json({ limit: "50mb" }));
  app.use(import_express.default.urlencoded({ extended: true, limit: "50mb" }));
  app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") {
      return res.sendStatus(200);
    }
    next();
  });
  app.get("/api/health", (req, res) => {
    res.json({ status: "ok", activeSessions: sessions.size, timestamp: Date.now() });
  });
  app.post("/api/session/create", (req, res) => {
    const { code, metadata } = req.body;
    if (!code || !metadata) {
      return res.status(400).json({ error: "Code and metadata are required" });
    }
    const cleanCode = code.toUpperCase().trim();
    const session = {
      code: cleanCode,
      createdAt: Date.now(),
      lastActive: Date.now(),
      metadata,
      signals: [],
      signalSeq: 0,
      status: "waiting",
      relayChunks: /* @__PURE__ */ new Map(),
      relayTotalChunks: 0
    };
    sessions.set(cleanCode, session);
    res.json({ ok: true, code: cleanCode });
  });
  app.get("/api/session/:code", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found or expired" });
    }
    session.lastActive = Date.now();
    res.json({
      ok: true,
      session: {
        code: session.code,
        metadata: session.metadata,
        status: session.status,
        createdAt: session.createdAt
      }
    });
  });
  app.post("/api/session/:code/join", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }
    session.status = "connecting";
    session.lastActive = Date.now();
    session.signalSeq++;
    session.signals.push({
      id: session.signalSeq,
      from: "receiver",
      payload: { type: "receiver_joined" },
      timestamp: Date.now()
    });
    res.json({ ok: true });
  });
  app.post("/api/session/:code/signal", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const { from, payload } = req.body;
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }
    session.lastActive = Date.now();
    session.signalSeq++;
    session.signals.push({
      id: session.signalSeq,
      from,
      payload,
      timestamp: Date.now()
    });
    if (session.signals.length > 200) {
      session.signals.splice(0, session.signals.length - 200);
    }
    res.json({ ok: true, seq: session.signalSeq });
  });
  app.get("/api/session/:code/signals", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const afterSeq = parseInt(req.query.afterSeq, 10) || 0;
    const forRole = req.query.forRole;
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }
    session.lastActive = Date.now();
    const newSignals = session.signals.filter((s) => {
      const isNew = s.id > afterSeq;
      const isForMe = forRole ? s.from !== forRole : true;
      return isNew && isForMe;
    });
    res.json({
      ok: true,
      currentSeq: session.signalSeq,
      signals: newSignals,
      status: session.status
    });
  });
  app.post("/api/session/:code/relay/chunk", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const { chunkIndex, totalChunks, data, fileIndex } = req.body;
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }
    session.lastActive = Date.now();
    session.relayTotalChunks = totalChunks;
    session.relayChunks.set(chunkIndex, { data, fileIndex, totalChunks });
    res.json({ ok: true, received: chunkIndex });
  });
  app.get("/api/session/:code/relay/chunk", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const chunkIndex = parseInt(req.query.chunkIndex, 10);
    const session = sessions.get(code);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }
    session.lastActive = Date.now();
    const chunk = session.relayChunks.get(chunkIndex);
    if (!chunk) {
      return res.status(404).json({ ready: false });
    }
    res.json({
      ready: true,
      chunkIndex,
      data: chunk.data,
      fileIndex: chunk.fileIndex,
      totalChunks: chunk.totalChunks
    });
  });
  app.post("/api/session/:code/relay/clear", (req, res) => {
    const code = req.params.code.toUpperCase().trim();
    const session = sessions.get(code);
    if (session) {
      session.relayChunks.clear();
      session.status = "completed";
    }
    res.json({ ok: true });
  });
  if (process.env.NODE_ENV !== "production") {
    const vite = await (0, import_vite.createServer)({
      server: { middlewareMode: true },
      appType: "spa"
    });
    app.use(vite.middlewares);
  } else {
    const distPath = import_path.default.join(process.cwd(), "dist");
    app.use(import_express.default.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(import_path.default.join(distPath, "index.html"));
    });
  }
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Live P2P Signaling & Relay Server running on port ${PORT}`);
  });
}
startServer();
//# sourceMappingURL=server.cjs.map
