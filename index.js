// server.js — Market Stream Server (Finnhub + Twelve Data) + WebSocket bridge
// - Keeps ALL your pairs/symbols
// - Stores only latest tick per symbol (no OOM)
// - Prevents Finnhub reconnect storms (reconnect ONLY on close)
// - Treats close 1006 like a "cooldown" (min 60s) to avoid 429
// - Only resets backoff after Finnhub stays alive 30s
//
// .env required:
// FINNHUB_API_KEY=...
// TWELVE_DATA_API_KEY=...
// PORT=8080 (optional)

import http from "http";
import dotenv from "dotenv";
import WebSocket from "ws";
import { WebSocketServer } from "ws";
import { LISTED_CFD_INSTRUMENTS, LISTED_CFD_SUBSCRIPTIONS, ListedCfdFeed } from './listed-cfds.js';

dotenv.config();

console.log("🚀 Starting Market Stream Server (Finnhub + Twelve Data)");

const PORT = Number(process.env.PORT || 8080);

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || "";

const FINNHUB_URL = FINNHUB_API_KEY ? `wss://ws.finnhub.io?token=${FINNHUB_API_KEY}` : null;
const TWELVE_URL = TWELVE_DATA_API_KEY
  ? `${process.env.TWELVE_DATA_WS_URL || 'wss://ws.twelvedata.com/v1/quotes/price'}?apikey=${encodeURIComponent(TWELVE_DATA_API_KEY)}`
  : null;

console.log("FINNHUB enabled:", Boolean(FINNHUB_URL));
console.log("TWELVE enabled:", Boolean(TWELVE_URL));

const BROADCAST_INTERVAL_MS = 250;
const MAX_WS_CLIENTS = 5000;

// ================== YOUR SYMBOLS ==================
const finnhubSymbols = [
  // Commodities
  "OANDA:XAG_USD","OANDA:NATGAS_USD","OANDA:BCO_USD","OANDA:WTICO_USD",
  "OANDA:XPT_USD","OANDA:XAU_USD","OANDA:XPD_USD","OANDA:CORN_USD",
  "OANDA:WHEAT_USD","OANDA:SOYBN_USD","OANDA:SUGAR_USD","OANDA:COFFEE_USD",
  "OANDA:COTTON_USD","OANDA:COCOA_USD",

  // Forex - Majors
  "OANDA:EUR_USD","OANDA:GBP_USD","OANDA:AUD_USD","OANDA:NZD_USD",
  "OANDA:USD_JPY","OANDA:USD_CHF","OANDA:USD_CAD",

  // Minors / Crosses
  "OANDA:EUR_GBP","OANDA:EUR_JPY","OANDA:EUR_AUD","OANDA:EUR_CHF","OANDA:EUR_CAD","OANDA:EUR_NZD",
  "OANDA:GBP_JPY","OANDA:GBP_CHF","OANDA:GBP_AUD","OANDA:GBP_CAD","OANDA:GBP_NZD",
  "OANDA:AUD_JPY","OANDA:AUD_CHF","OANDA:AUD_CAD","OANDA:AUD_NZD",
  "OANDA:NZD_JPY","OANDA:NZD_CHF","OANDA:NZD_CAD",
  "OANDA:CAD_JPY","OANDA:CAD_CHF",
  "OANDA:CHF_JPY",

  // Popular Exotics
  "OANDA:USD_CNH","OANDA:USD_HKD","OANDA:USD_SGD","OANDA:USD_THB",
  "OANDA:USD_ZAR","OANDA:USD_MXN","OANDA:USD_TRY","OANDA:USD_PLN",
  "OANDA:USD_HUF","OANDA:USD_CZK","OANDA:USD_DKK","OANDA:USD_NOK",
  "OANDA:USD_SEK","OANDA:USD_RUB",

  // Asia & Pacific
  "OANDA:SGD_JPY","OANDA:CNH_JPY","OANDA:HKD_JPY",
  "OANDA:AUD_SGD","OANDA:AUD_HKD","OANDA:AUD_CNH",
  "OANDA:NZD_SGD","OANDA:USD_TWD","OANDA:USD_KRW","OANDA:USD_INR",

  // Scandinavia & Europe
  "OANDA:EUR_SEK","OANDA:EUR_NOK","OANDA:EUR_DKK",
  "OANDA:GBP_NOK","OANDA:GBP_SEK",
  "OANDA:NOK_SEK",
  "OANDA:EUR_PLN","OANDA:EUR_CZK","OANDA:EUR_HUF",

  // Middle East
  "OANDA:USD_SAR","OANDA:USD_AED","OANDA:USD_QAR",
  "OANDA:USD_ILS","OANDA:EUR_ILS",

  // Latin America
  "OANDA:EUR_MXN","OANDA:GBP_MXN","OANDA:AUD_MXN",
  "OANDA:USD_CLP","OANDA:USD_COP","OANDA:USD_PEN",

  // Africa
  "OANDA:EUR_ZAR","OANDA:GBP_ZAR","OANDA:AUD_ZAR"
];

const twelveSymbols = [
  // Major US Stocks
  "AAPL","MSFT","GOOGL","AMZN","TSLA","META","NFLX","NVDA","BA","JPM","GS","BAC","WMT","DIS","CVX",
  // More US Stocks
  "AMD","INTC","ORCL","IBM","ADBE","PYPL","UBER","LYFT","PFE","MRNA","KO","PEP","XOM","T","V","MA",
  // New Additions
  "CSCO","CRM","COST","AVGO","TXN","QCOM","AMAT","INTU","SBUX","MCD",
  "LOW","HD","CAT","HON","AMGN","LLY","UNH","ABT","CVS","WFC",
  "MS","AXP","BLK","C","BK","TGT","NKE","SHOP","SNOW","PLTR",
  // International Stocks
  "NESN.SW","SIE.DE","BMW.DE","DAI.DE","RY.TO","SHOP.TO","SONY.T","7203.T","9984.T","BP.L","HSBA.L","VOD.L",
  // Global Indices (some may fail; kept anyway)
  "SPX","DJI","NDX","RUT","DAX","FTSE","CAC","N225","HSI","AS51","STOXX50E","SMI",
  // Popular ETFs
  "SPY","QQQ","DIA","IWM","VTI","ARKK",
  // New Additions
  "XLK","XLF","XLE","XLY","XLP","XLV","XLI","XLB","XLU","IYR",
  "SMH","SOXX","TLT","HYG","GLD","SLV","BITO","SPYD","JEPI","VOO"
];

// ================== UTILS ==================
function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
function normalizeInputSymbol(sym) {
  return sym.trim().toUpperCase().replaceAll("/", "").replaceAll("_", "");
}
function fmtFinnhubSymbol(raw) {
  // OANDA:EUR_USD -> EURUSD
  return raw.replace("OANDA:", "").replaceAll("_", "");
}
function fmtTwelveSymbol(raw) {
  return raw.toUpperCase();
}

// ================== LATEST STORE (no growth) ==================
const latestBySymbol = new Map();
const dirtySymbols = new Set();
const listedCfdFeed = new ListedCfdFeed(quote => {
  latestBySymbol.set(quote.symbol, quote);
  dirtySymbols.add(quote.symbol);
});

// Pre-seed so snapshot contains everything
for (const s of finnhubSymbols) {
  const key = fmtFinnhubSymbol(s);
  if (!latestBySymbol.has(key)) {
    latestBySymbol.set(key, { symbol: key, price: null, volume: null, timestamp: null, source: "finnhub" });
  }
}
for (const s of twelveSymbols) {
  const key = fmtTwelveSymbol(s);
  if (!latestBySymbol.has(key)) {
    latestBySymbol.set(key, { symbol: key, price: null, volume: null, timestamp: null, source: "twelvedata" });
  }
}

for (const instrument of LISTED_CFD_INSTRUMENTS) {
  latestBySymbol.set(instrument.symbol, { symbol: instrument.symbol, price: null, timestamp: null,
    currency: 'USD', source: 'twelvedata', isin: instrument.isin });
}

// ================== HTTP SERVER ==================
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url || "/", `http://${req.headers.host}`);

  if (url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      wsClients: clients.size,
      symbolsCached: latestBySymbol.size,
      finnhub: finnhubState(),
      twelve: twelveState(),
      listedCfds: listedCfdFeed.status(),
      time: new Date().toISOString()
    }));
    return;
  }

  if (url.pathname === "/snapshot") {
    const obj = {};
    for (const [k, v] of latestBySymbol.entries()) obj[k] = v;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
    return;
  }

  if (url.pathname === "/latest") {
    const sym = url.searchParams.get("symbol");
    if (!sym) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "Missing ?symbol=" }));
      return;
    }
    const key = normalizeInputSymbol(sym);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(latestBySymbol.get(key) || null));
    return;
  }

  if (url.pathname === "/latest-batch") {
    const syms = url.searchParams.get("symbols");
    if (!syms) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "Missing ?symbols=AAPL,EURUSD,..." }));
      return;
    }
    const out = {};
    for (const s of syms.split(",")) {
      const key = normalizeInputSymbol(s);
      out[s.trim()] = latestBySymbol.get(key) || null;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(out));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("✅ Market stream server running\n");
});

// ================== WS SERVER FOR CLIENTS ==================
const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 16 * 1024 });
const clients = new Set();

wss.on("connection", (ws) => {
  if (clients.size >= MAX_WS_CLIENTS) {
    ws.close(1013, "Server busy");
    return;
  }

  clients.add(ws);

  ws.send(JSON.stringify({
    type: "connected",
    message: "Connected to Market Stream Server",
    symbols: latestBySymbol.size,
    time: new Date().toISOString()
  }));

  // Snapshot on connect
  const snap = {};
  for (const [k, v] of latestBySymbol.entries()) snap[k] = v;
  ws.send(JSON.stringify({ type: "snapshot", data: snap, time: new Date().toISOString() }));

  // Local cache reads only: clients cannot create new provider subscriptions.
  ws.on('message', raw => {
    try {
      const message = JSON.parse(raw.toString());
      if (message.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
      if (message.type === 'request_snapshot' || message.type === 'subscribe') {
        ws.send(JSON.stringify({ type: 'snapshot', data: Object.fromEntries(latestBySymbol), time: new Date().toISOString() }));
      }
    } catch { /* Ignore invalid client messages. */ }
  });

  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

// Broadcast only changed symbols (prevents memory blowup)
setInterval(() => {
  if (dirtySymbols.size === 0) return;

  const updates = [];
  for (const sym of dirtySymbols) {
    const v = latestBySymbol.get(sym);
    if (v) updates.push(v);
  }
  dirtySymbols.clear();

  if (!updates.length) return;

  const msg = JSON.stringify({ type: "price_update", data: updates, time: new Date().toISOString() });
  for (const c of clients) {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  }
}, BROADCAST_INTERVAL_MS);

// ================== FINNHUB (cooldown on 1006 + 429) ==================
let finnhubWS = null;
let finnhubConnecting = false;
let finnhubBackoffMs = 60000; // start safe: 60s
const FINNHUB_BACKOFF_MAX = 300000; // 5 min
let finnhubPingInterval = null;
let finnhubReconnectTimer = null;
let finnhubStableTimer = null;

function finnhubState() {
  return {
    enabled: Boolean(FINNHUB_URL),
    readyState: finnhubWS ? finnhubWS.readyState : null,
    connecting: finnhubConnecting,
    backoffMs: finnhubBackoffMs
  };
}

function cleanupFinnhub() {
  if (finnhubReconnectTimer) clearTimeout(finnhubReconnectTimer);
  finnhubReconnectTimer = null;

  if (finnhubPingInterval) clearInterval(finnhubPingInterval);
  finnhubPingInterval = null;

  if (finnhubStableTimer) clearTimeout(finnhubStableTimer);
  finnhubStableTimer = null;

  if (finnhubWS) {
    try { finnhubWS.terminate(); } catch {}
  }
  finnhubWS = null;
  finnhubConnecting = false;
}

function scheduleFinnhubReconnect(reason) {
  if (!FINNHUB_URL) return;
  if (finnhubReconnectTimer) return;

  // Always cool down hard on abnormal close
  if (String(reason).includes("close_1006")) {
    finnhubBackoffMs = Math.max(finnhubBackoffMs, 60000);
  }

  // If 429 happened, minimum 2 minutes
  if (String(reason).includes("429")) {
    finnhubBackoffMs = Math.max(finnhubBackoffMs, 120000);
  }

  const jitter = Math.floor(Math.random() * 3000);
  const wait = Math.min(finnhubBackoffMs + jitter, FINNHUB_BACKOFF_MAX);

  console.log(`⚠️ Finnhub reconnect in ${wait}ms (reason: ${reason})`);

  finnhubReconnectTimer = setTimeout(() => {
    finnhubReconnectTimer = null;
    connectFinnhub();
  }, wait);

  // exponential backoff up to max
  finnhubBackoffMs = Math.min(Math.floor(finnhubBackoffMs * 1.5), FINNHUB_BACKOFF_MAX);
}

function connectFinnhub() {
  if (!FINNHUB_URL) return;
  if (finnhubConnecting) return;
  if (finnhubWS && finnhubWS.readyState === WebSocket.OPEN) return;

  finnhubConnecting = true;
  console.log("🔌 Connecting Finnhub...");

  finnhubWS = new WebSocket(FINNHUB_URL, {
    handshakeTimeout: 30000,
    perMessageDeflate: false
  });

  finnhubWS.on("open", () => {
    finnhubConnecting = false;

    for (const s of finnhubSymbols) {
      finnhubWS.send(JSON.stringify({ type: "subscribe", symbol: s }));
    }
    console.log("✅ Finnhub subscribed");

    // Only reset backoff if connection survives 30s
    if (finnhubStableTimer) clearTimeout(finnhubStableTimer);
    finnhubStableTimer = setTimeout(() => {
      finnhubBackoffMs = 60000; // keep it safe anyway
      console.log("✅ Finnhub stable (30s). Backoff normalized.");
    }, 30000);
  });

  finnhubWS.on("message", (buf) => {
    try {
      const msg = JSON.parse(buf.toString());
      if (msg.type !== "trade" || !Array.isArray(msg.data)) return;

      for (const t of msg.data) {
        const key = fmtFinnhubSymbol(t.s);
        const price = Number(t.p);
        const volume = Number(t.v || 0);
        if (!Number.isFinite(price)) continue;

        const val = {
          symbol: key,
          price,
          volume,
          timestamp: new Date(t.t).toISOString(),
          source: "finnhub"
        };

        latestBySymbol.set(key, val);
        dirtySymbols.add(key);
        listedCfdFeed.handleFxTick(key, price, t.t);
      }
    } catch {}
  });

  // IMPORTANT: no reconnect here; close schedules it once
  finnhubWS.on("error", (err) => {
    const msg = err?.message || String(err);
    console.error("❌ Finnhub error:", msg);

    // If 429, increase cooldown; close will reconnect later
    if (msg.includes("429")) {
      finnhubBackoffMs = Math.max(finnhubBackoffMs, 120000);
    }
  });

  finnhubWS.on("close", (code) => {
    console.log(`🔌 Finnhub closed (code ${code})`);

    // treat abnormal close as cooldown
    if (code === 1006) {
      finnhubBackoffMs = Math.max(finnhubBackoffMs, 60000);
    }

    cleanupFinnhub();
    scheduleFinnhubReconnect(`close_${code}`);
  });

  if (finnhubPingInterval) clearInterval(finnhubPingInterval);
  finnhubPingInterval = setInterval(() => {
    if (finnhubWS && finnhubWS.readyState === WebSocket.OPEN) {
      try { finnhubWS.ping(); } catch {}
    }
  }, 25000);
}

// ================== TWELVE DATA ==================
let twelveWS = null;
let twelveConnecting = false;
let twelveBackoffMs = 2000;
const TWELVE_BACKOFF_MAX = 120000;
let twelveReconnectTimer = null;
let twelveHeartbeatInterval = null;

function twelveState() {
  return {
    enabled: Boolean(TWELVE_URL),
    readyState: twelveWS ? twelveWS.readyState : null,
    connecting: twelveConnecting,
    backoffMs: twelveBackoffMs
  };
}

function cleanupTwelve() {
  if (twelveReconnectTimer) clearTimeout(twelveReconnectTimer);
  twelveReconnectTimer = null;

  if (twelveHeartbeatInterval) clearInterval(twelveHeartbeatInterval);
  twelveHeartbeatInterval = null;

  if (twelveWS) {
    try { twelveWS.terminate(); } catch {}
  }
  twelveWS = null;
  twelveConnecting = false;
}

function scheduleTwelveReconnect(reason) {
  if (!TWELVE_URL) return;
  if (twelveReconnectTimer) return;

  const jitter = Math.floor(Math.random() * 1000);
  const wait = Math.min(twelveBackoffMs + jitter, TWELVE_BACKOFF_MAX);

  console.log(`⚠️ Twelve reconnect in ${wait}ms (reason: ${reason})`);

  twelveReconnectTimer = setTimeout(() => {
    twelveReconnectTimer = null;
    connectTwelve();
  }, wait);

  twelveBackoffMs = Math.min(twelveBackoffMs * 2, TWELVE_BACKOFF_MAX);
}

function connectTwelve() {
  if (!TWELVE_URL) return;
  if (twelveConnecting) return;
  if (twelveWS && twelveWS.readyState === WebSocket.OPEN) return;

  twelveConnecting = true;
  console.log("🔌 Connecting Twelve...");

  twelveWS = new WebSocket(TWELVE_URL, {
    handshakeTimeout: 30000,
    perMessageDeflate: false
  });

  twelveWS.on("open", () => {
    twelveConnecting = false;
    twelveBackoffMs = 2000;
    listedCfdFeed.resetSubscriptions();

    const batches = chunk(twelveSymbols, 50);
    for (const b of batches) {
      twelveWS.send(JSON.stringify({
        action: "subscribe",
        params: { symbols: b.join(",") }
      }));
    }

    console.log(`✅ Twelve subscribed (${twelveSymbols.length} symbols, ${batches.length} batches)`);

    twelveWS.send(JSON.stringify({ action: 'subscribe', params: { symbols: LISTED_CFD_SUBSCRIPTIONS } }));

    if (twelveHeartbeatInterval) clearInterval(twelveHeartbeatInterval);
    twelveHeartbeatInterval = setInterval(() => {
      if (twelveWS && twelveWS.readyState === WebSocket.OPEN) {
        try { twelveWS.send(JSON.stringify({ action: "heartbeat" })); } catch {}
      }
    }, 10000);
  });

  twelveWS.on("message", (buf) => {
    try {
      const msg = JSON.parse(buf.toString());

      if (msg?.event === "subscribe-status") {
        listedCfdFeed.handleSubscriptionStatus(msg);
        console.log("TWELVE MSG:", msg);
        return;
      }

      if (msg?.event !== "price") return;

      const handledListing = listedCfdFeed.handleTick(msg);
      if (handledListing && !msg.symbol.includes('/')) return;

      const key = fmtTwelveSymbol(msg.symbol).replaceAll('/', '');
      const price = Number(msg.price);
      if (!Number.isFinite(price)) return;

      const ts = msg.timestamp ? new Date(msg.timestamp * 1000).toISOString() : new Date().toISOString();

      const val = {
        symbol: key,
        price,
        volume: msg.day_volume != null ? Number(msg.day_volume) : null,
        timestamp: ts,
        source: "twelvedata"
      };

      latestBySymbol.set(key, val);
      dirtySymbols.add(key);
    } catch {}
  });

  twelveWS.on("error", (err) => {
    console.error("❌ Twelve error:", err?.message || err);
    cleanupTwelve();
    scheduleTwelveReconnect("error");
  });

  twelveWS.on("close", (code) => {
    console.log(`🔌 Twelve closed (code ${code})`);
    cleanupTwelve();
    scheduleTwelveReconnect(`close_${code}`);
  });
}

// ================== START ==================
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server on ${PORT}`);
  if (FINNHUB_URL) connectFinnhub();
  if (TWELVE_URL) connectTwelve();
});

setInterval(() => {
  console.log(`❤️  Health | clients=${clients.size} cached=${latestBySymbol.size} time=${new Date().toISOString()}`);
}, 300000);

process.on("uncaughtException", (err) => console.error("Uncaught Exception:", err));
process.on("unhandledRejection", (reason) => console.error("Unhandled Rejection:", reason));
