import http from "http";
import WebSocket from "ws";

// deriv-probe-isolated V7 — 15m feed queued (150ms between proposals, RateLimit backoff 1s/2s, max 2 retries). V6 — adds 15m proposal feed (same WebSocket, same fan-out). V5 — one WebSocket, fan-out to: bench (/health), Deriv Lab tick buffer (/ticks), Deriv candle history for warm-up (/candles), payouts by duration, feed gaps.
const DERIV_WS = "wss://api.derivws.com/trading/v1/options/ws/public?app_id=1089";
const SYMBOLS = ["frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxAUDUSD", "frxUSDCAD", "frxEURGBP"];
const DURATIONS = [3, 5, 15]; // minutes; 3/5 = strategy expiries, 15 = bench display
const BUFFER_SECONDS = 30 * 60;
const PORT = process.env.PORT || 8080;

const state = { status: "starting", started_at: new Date().toISOString(), connected_at: null, last_tick_at: null, last_payout_at: null, total_ticks_received: 0, errors_count: 0, reconnect_count: 0 };
const quotes = {}, payouts = {}, buffer = [], gaps = [];
let ws = null, pingTimer = null, payoutTimer = null, p15Timer = null, reqId = 1, openGap = null;
const pending = {};
const hist = {}; // req_id -> resolver (ticks_history, read-only)

function connect() {
  ws = new WebSocket(DERIV_WS);
  ws.on("open", () => {
    state.status = "connected"; state.connected_at = new Date().toISOString();
    if (openGap) { openGap.end = Math.floor(Date.now() / 1000); openGap.ticks_after = state.total_ticks_received; openGap = null; }
    for (const s of SYMBOLS) ws.send(JSON.stringify({ ticks: s, subscribe: 1 }));
    clearInterval(pingTimer); pingTimer = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ ping: 1 })), 30000);
    clearInterval(payoutTimer); queryPayouts(); runProbe(); payoutTimer = setInterval(() => { queryPayouts(); runProbe(); checkContracts(); }, 60 * 1000);
    clearInterval(p15Timer); checkContracts(); runProbe15(); p15Timer = setInterval(runProbe15, 15 * 1000);
  });
  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.req_id && probePending[m.req_id]) {
      const q = probePending[m.req_id]; delete probePending[m.req_id];
      const pair = q.symbol.replace("frx", "").replace(/^(...)(...)$/, "$1/$2");
      probe[`${q.symbol}|${q.contract_type}|${q.duration}`] = { pair, contract_type: q.contract_type, duration: q.duration, duration_unit: "m", stake: 10,
        proposal_id: m.proposal?.id ?? null, proposal: m.proposal ? { payout: m.proposal.payout, ask_price: m.proposal.ask_price } : null, error: m.error ? { code: m.error.code, message: m.error.message } : null, at: new Date().toISOString() };
      if (q.done) q.done(m);
      return;
    }
    if (m.req_id && cfPending[m.req_id]) {
      const symbol = cfPending[m.req_id]; delete cfPending[m.req_id];
      const list = m.contracts_for?.available || [];
      const has = (ct) => list.some((c) => c.contract_type === ct && (c.expiry_type === "intraday" || c.expiry_type === "daily") && minDur(c.min_contract_duration) <= 15 && maxDur(c.max_contract_duration) >= 15);
      avail15[symbol] = { CALL: has("CALL"), PUT: has("PUT"), error: m.error ? m.error.message : null, at: new Date().toISOString() };
      return;
    }
    if (m.req_id && hist[m.req_id]) { const r = hist[m.req_id]; delete hist[m.req_id]; return r(m); }
    if (m.error && m.msg_type === "proposal" && pending[m.req_id]) {
      // Contract not offered (e.g. 3m/5m Forex): recorded as unavailable, not a feed error.
      const { symbol, duration } = pending[m.req_id]; delete pending[m.req_id];
      payouts[symbol] = payouts[symbol] || {};
      payouts[symbol][String(duration)] = { payout: null, unavailable: m.error.message, at: new Date().toISOString() };
      return;
    }
    if (m.error) { state.errors_count++; return; }
    if (m.msg_type === "tick" && m.tick) {
      const t = m.tick;
      const q = { quote: t.quote, ask: t.ask, bid: t.bid, epoch: t.epoch, updated_at: new Date(t.epoch * 1000).toISOString(), payout: payouts[t.symbol]?.["15"]?.payout ?? null };
      quotes[t.symbol] = q;
      buffer.push({ symbol: t.symbol, epoch: t.epoch, quote: t.quote, bid: t.bid, ask: t.ask });
      const cut = Math.floor(Date.now() / 1000) - BUFFER_SECONDS;
      while (buffer.length && buffer[0].epoch < cut) buffer.shift();
      state.total_ticks_received++; state.last_tick_at = q.updated_at;
    }
    if (m.msg_type === "proposal" && pending[m.req_id]) {
      const { symbol, duration } = pending[m.req_id]; delete pending[m.req_id];
      const p = m.proposal; if (!p) return;
      payouts[symbol] = payouts[symbol] || {};
      payouts[symbol][String(duration)] = { payout: Math.round(((p.payout - p.ask_price) / p.ask_price) * 10000) / 10000, at: new Date().toISOString() };
      state.last_payout_at = new Date().toISOString();
    }
  });
  ws.on("error", () => { state.errors_count++; });
  ws.on("close", () => {
    state.status = "disconnected"; clearInterval(pingTimer); clearInterval(payoutTimer); clearInterval(p15Timer);
    if (!openGap) { openGap = { start: Math.floor(Date.now() / 1000), end: null, pairs: SYMBOLS, ticks_before: state.total_ticks_received }; gaps.push(openGap); if (gaps.length > 50) gaps.shift(); }
    state.reconnect_count++; setTimeout(connect, 3000);
  });
}

// Price/payout queries only — NEVER buy/sell.
// BINARY CONTRACT CAPABILITY PROBE — real proposals CALL/PUT × 3m/5m, raw answer kept (never buy/sell).
const probe = {}; const probePending = {};
function runProbe() {
  if (!ws || ws.readyState !== 1) return;
  for (const symbol of SYMBOLS) for (const contract_type of ["CALL", "PUT"]) for (const duration of [3, 5]) {
    const id = reqId++; probePending[id] = { symbol, contract_type, duration };
    ws.send(JSON.stringify({ proposal: 1, amount: 10, basis: "stake", contract_type, currency: "USD", duration, duration_unit: "m", underlying_symbol: symbol, req_id: id }));
  }
}
// 15M PROPOSAL FEED — availability first (contracts_for), then real CALL/PUT 15m proposals every 15s. Never buy/sell.
const avail15 = {}; const cfPending = {};
const toMin = (d) => { const m = /^(\d+)([smhd])$/.exec(String(d || "")); if (!m) return null; return Number(m[1]) * ({ s: 1 / 60, m: 1, h: 60, d: 1440 })[m[2]]; };
const minDur = (d) => toMin(d) ?? Infinity, maxDur = (d) => toMin(d) ?? -Infinity;
function checkContracts() {
  if (!ws || ws.readyState !== 1) return;
  for (const symbol of SYMBOLS) { const id = reqId++; cfPending[id] = symbol; ws.send(JSON.stringify({ contracts_for: symbol, currency: "USD", req_id: id })); }
}
const GAP_MS = 150; const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const isRL = (e) => e && (e.code === "RateLimit" || /rate ?limit/i.test(e.message || ""));
let q15Busy = false; const q15Stats = { rate_limit_total: 0, retries_total: 0, failed_after_retries: 0, last_cycle_ms: null, gaps_ms: [] };
function ask15(symbol, contract_type) {
  return new Promise((resolve) => {
    if (!ws || ws.readyState !== 1) return resolve(null);
    const id = reqId++; const t = setTimeout(() => { delete probePending[id]; resolve(null); }, 5000);
    probePending[id] = { symbol, contract_type, duration: 15, done: (m) => { clearTimeout(t); resolve(m); } };
    ws.send(JSON.stringify({ proposal: 1, amount: 10, basis: "stake", contract_type, currency: "USD", duration: 15, duration_unit: "m", underlying_symbol: symbol, req_id: id }));
  });
}
async function runProbe15() {
  if (!ws || ws.readyState !== 1 || q15Busy) return;
  q15Busy = true; const t0 = Date.now(); let lastSend = 0; const gapsSeen = [];
  try {
    for (const symbol of SYMBOLS) for (const contract_type of ["CALL", "PUT"]) {
      if (avail15[symbol] && avail15[symbol][contract_type] === false && !avail15[symbol].error) continue;
      for (let attempt = 0; attempt <= 2; attempt++) {
        const wait = lastSend ? GAP_MS - (Date.now() - lastSend) : 0; if (wait > 0) await sleep(wait);
        if (lastSend) gapsSeen.push(Date.now() - lastSend); lastSend = Date.now();
        const m = await ask15(symbol, contract_type);
        if (!m || !isRL(m.error)) break;
        q15Stats.rate_limit_total++;
        if (attempt === 2) { q15Stats.failed_after_retries++; break; } // stays as error; next 15s cycle retries; never reuse stale
        q15Stats.retries_total++; await sleep(attempt === 0 ? 1000 : 2000);
      }
    }
  } finally { q15Stats.last_cycle_ms = Date.now() - t0; q15Stats.gaps_ms = gapsSeen; q15Busy = false; }
}
function feed15() {
  const rows = Object.values(probe).filter((p) => p.duration === 15);
  const ok = rows.filter((p) => p.proposal && !p.error);
  const last = ok.map((p) => p.at).sort().pop() || null;
  return { status: state.status === "connected" ? "CONNECTED" : "DISCONNECTED", pairs_available: new Set(ok.map((p) => p.pair)).size,
    contracts_available: ok.length, availability: avail15, last_proposal_timestamp: last, proposal_age_s: last ? Math.round((Date.now() - Date.parse(last)) / 1000) : null,
    queue: { interval_ms: GAP_MS, cycle_s: 15, ...q15Stats, min_gap_ms: q15Stats.gaps_ms.length ? Math.min(...q15Stats.gaps_ms) : null },
    errors: rows.filter((p) => p.error).map((p) => ({ pair: p.pair, contract_type: p.contract_type, error: p.error })), reconnections: state.reconnect_count,
    proposals: ok.map((p) => ({ pair: p.pair, contract_type: p.contract_type, duration: 15, ask_price: p.proposal.ask_price, payout: p.proposal.payout,
      potential_return: Math.round(((p.proposal.payout - p.proposal.ask_price) / p.proposal.ask_price) * 10000) / 10000, at: p.at })) };
}
function queryPayouts() {
  if (!ws || ws.readyState !== 1) return;
  for (const symbol of SYMBOLS) for (const duration of DURATIONS) {
    const id = reqId++; pending[id] = { symbol, duration };
    ws.send(JSON.stringify({ proposal: 1, amount: 10, basis: "stake", contract_type: "CALL", currency: "USD", duration, duration_unit: "m", underlying_symbol: symbol, req_id: id }));
  }
}

// Deriv 1m candle history (ticks_history style=candles, max 1000 per call → paged backwards). Read-only, never buy/sell.
function historyPage(symbol, end) {
  return new Promise((resolve) => {
    if (!ws || ws.readyState !== 1) return resolve([]);
    const id = reqId++; const t = setTimeout(() => { delete hist[id]; resolve([]); }, 15000);
    hist[id] = (m) => { clearTimeout(t); resolve(m.error ? [] : m.candles || []); };
    ws.send(JSON.stringify({ ticks_history: symbol, style: "candles", granularity: 60, count: 1000, end, req_id: id }));
  });
}
async function history(symbol, count) {
  let out = [], end = "latest";
  while (out.length < count) {
    const page = await historyPage(symbol, end); if (!page.length) break;
    out = [...page.filter((c) => !out.length || c.epoch < out[0].epoch), ...out];
    end = page[0].epoch - 1; if (page.length < 1000) break;
  }
  return out.slice(-count);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  res.setHeader("Access-Control-Allow-Origin", "*"); res.setHeader("Content-Type", "application/json");
  if (u.pathname === "/ticks") {
    const since = Number(u.searchParams.get("since") || 0);
    return res.end(JSON.stringify({ ticks: buffer.filter((t) => t.epoch >= since) }));
  }
  if (u.pathname === "/candles") {
    const symbol = u.searchParams.get("symbol"); const count = Math.min(5000, Number(u.searchParams.get("count") || 3000));
    if (!SYMBOLS.includes(symbol)) { res.statusCode = 400; return res.end("{}"); }
    return res.end(JSON.stringify({ symbol, source: "DERIV_TICKS_HISTORY", candles: await history(symbol, count) }));
  }
  if (u.pathname === "/health" || u.pathname === "/") {
    return res.end(JSON.stringify({ service: "deriv-probe-isolated", version: 7, deriv_15m_proposal_feed: feed15(), contract_probe: Object.values(probe), ...state, uptime_seconds: Math.floor(process.uptime()), active_symbols_count: Object.keys(quotes).length, quotes, payouts_by_duration: payouts, gaps, buffer_ticks: buffer.length }));
  }
  res.statusCode = 404; res.end("{}");
}).listen(PORT, () => connect());
