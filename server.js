import http from "http";
import WebSocket from "ws";

// deriv-probe-isolated V8 — 3m/5m transformed into DERIV_3M5M_CAPABILITY_MONITOR (checked on startup, reconnect, and every 6h).
// Continuous 3m/5m proposal polling eliminated while CONTRACT_AVAILABLE = false, completely stopping RateLimits.
// 15m queued feed (150ms gap, RateLimit backoff 1s/2s, max 2 retries) preserved 100%.
const DERIV_WS = "wss://api.derivws.com/trading/v1/options/ws/public?app_id=1089";
const SYMBOLS = ["frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxAUDUSD", "frxUSDCAD", "frxEURGBP"];
const BUFFER_SECONDS = 30 * 60;
const PORT = process.env.PORT || 8080;

const state = {
  status: "starting",
  started_at: new Date().toISOString(),
  connected_at: null,
  last_tick_at: null,
  last_payout_at: null,
  total_ticks_received: 0,
  errors_count: 0,
  reconnect_count: 0
};

const quotes = {}, payouts = {}, buffer = [], gaps = [], alerts = [];
let ws = null, pingTimer = null, p15Timer = null, capTimer = null, reqId = 1, openGap = null;
const pending = {};
const hist = {}; // req_id -> resolver (ticks_history, read-only)

// 3M / 5M CAPABILITY MONITOR
const capability3m = {
  status: "UNAVAILABLE", // "AVAILABLE" | "UNAVAILABLE"
  available: false,
  last_check: null,
  next_check: null,
  pairs_checked: 0,
  errors: [],
  last_change_timestamp: null,
  available_details: []
};

const capability5m = {
  status: "UNAVAILABLE",
  available: false,
  last_check: null,
  next_check: null,
  pairs_checked: 0,
  errors: [],
  last_change_timestamp: null,
  available_details: []
};

const toMin = (d) => {
  const m = /^(\d+)([smhd])$/.exec(String(d || ""));
  if (!m) return null;
  return Number(m[1]) * ({ s: 1 / 60, m: 1, h: 60, d: 1440 })[m[2]];
};
const minDur = (d) => toMin(d) ?? Infinity;
const maxDur = (d) => toMin(d) ?? -Infinity;

const probe = {};
const probePending = {};
const avail15 = {};
const cfPending = {};

const GAP_MS = 150;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const isRL = (e) => e && (e.code === "RateLimit" || /rate ?limit/i.test(e.message || ""));

function connect() {
  ws = new WebSocket(DERIV_WS);

  ws.on("open", () => {
    state.status = "connected";
    state.connected_at = new Date().toISOString();
    if (openGap) {
      openGap.end = Math.floor(Date.now() / 1000);
      openGap.ticks_after = state.total_ticks_received;
      openGap = null;
    }

    // 1. Subscribe to ticks for all 6 pairs
    for (const s of SYMBOLS) {
      ws.send(JSON.stringify({ ticks: s, subscribe: 1 }));
    }

    // 2. Keepalive ping every 30s
    clearInterval(pingTimer);
    pingTimer = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ ping: 1 })), 30000);

    // 3. Run capability monitor for 3m/5m (on startup and reconnect)
    runCapabilityCheck();
    clearInterval(capTimer);
    capTimer = setInterval(runCapabilityCheck, 6 * 60 * 60 * 1000); // every 6h

    // 4. Run 15m proposal feed immediately and every 15s
    clearInterval(p15Timer);
    runProbe15();
    p15Timer = setInterval(runProbe15, 15 * 1000);
  });

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    // Proposal probe response
    if (m.req_id && probePending[m.req_id]) {
      const q = probePending[m.req_id];
      delete probePending[m.req_id];
      const pair = q.symbol.replace("frx", "").replace(/^(...)(...)$/, "$1/$2");
      probe[`${q.symbol}|${q.contract_type}|${q.duration}`] = {
        pair,
        contract_type: q.contract_type,
        duration: q.duration,
        duration_unit: "m",
        stake: 10,
        proposal_id: m.proposal?.id ?? null,
        proposal: m.proposal ? { payout: m.proposal.payout, ask_price: m.proposal.ask_price } : null,
        error: m.error ? { code: m.error.code, message: m.error.message } : null,
        at: new Date().toISOString()
      };
      if (q.done) q.done(m);
      return;
    }

    // Contracts_for response
    if (m.req_id && cfPending[m.req_id]) {
      const q = cfPending[m.req_id];
      delete cfPending[m.req_id];
      const symbol = q.symbol;
      const list = m.contracts_for?.available || [];
      const has = (ct, dur) => list.some((c) =>
        c.contract_type === ct &&
        (c.expiry_type === "intraday" || c.expiry_type === "daily") &&
        minDur(c.min_contract_duration) <= dur &&
        maxDur(c.max_contract_duration) >= dur
      );

      // Check 15m availability
      avail15[symbol] = {
        CALL: has("CALL", 15),
        PUT: has("PUT", 15),
        error: m.error ? m.error.message : null,
        at: new Date().toISOString()
      };

      // Check 3m / 5m capability
      const has3mCall = has("CALL", 3), has3mPut = has("PUT", 3);
      const has5mCall = has("CALL", 5), has5mPut = has("PUT", 5);

      if (q.onResult) {
        q.onResult({ symbol, list, has3mCall, has3mPut, has5mCall, has5mPut, error: m.error });
      }
      return;
    }

    // Candle history response
    if (m.req_id && hist[m.req_id]) {
      const r = hist[m.req_id];
      delete hist[m.req_id];
      return r(m);
    }

    // Generic proposal error
    if (m.error && m.msg_type === "proposal" && pending[m.req_id]) {
      const { symbol, duration } = pending[m.req_id];
      delete pending[m.req_id];
      payouts[symbol] = payouts[symbol] || {};
      payouts[symbol][String(duration)] = { payout: null, unavailable: m.error.message, at: new Date().toISOString() };
      return;
    }

    if (m.error) {
      state.errors_count++;
      return;
    }

    // Tick stream
    if (m.msg_type === "tick" && m.tick) {
      const t = m.tick;
      const q = {
        quote: t.quote,
        ask: t.ask,
        bid: t.bid,
        epoch: t.epoch,
        updated_at: new Date(t.epoch * 1000).toISOString(),
        payout: payouts[t.symbol]?.["15"]?.payout ?? null
      };
      quotes[t.symbol] = q;
      buffer.push({ symbol: t.symbol, epoch: t.epoch, quote: t.quote, bid: t.bid, ask: t.ask });
      const cut = Math.floor(Date.now() / 1000) - BUFFER_SECONDS;
      while (buffer.length && buffer[0].epoch < cut) buffer.shift();
      state.total_ticks_received++;
      state.last_tick_at = q.updated_at;
    }

    // Proposal success
    if (m.msg_type === "proposal" && pending[m.req_id]) {
      const { symbol, duration } = pending[m.req_id];
      delete pending[m.req_id];
      const p = m.proposal;
      if (!p) return;
      payouts[symbol] = payouts[symbol] || {};
      payouts[symbol][String(duration)] = {
        payout: Math.round(((p.payout - p.ask_price) / p.ask_price) * 10000) / 10000,
        at: new Date().toISOString()
      };
      state.last_payout_at = new Date().toISOString();
    }
  });

  ws.on("error", () => { state.errors_count++; });

  ws.on("close", () => {
    state.status = "disconnected";
    clearInterval(pingTimer);
    clearInterval(p15Timer);
    clearInterval(capTimer);
    if (!openGap) {
      openGap = {
        start: Math.floor(Date.now() / 1000),
        end: null,
        pairs: SYMBOLS,
        ticks_before: state.total_ticks_received
      };
      gaps.push(openGap);
      if (gaps.length > 50) gaps.shift();
    }
    state.reconnect_count++;
    setTimeout(connect, 3000);
  });
}

// DERIV_3M5M_CAPABILITY_MONITOR execution
let capRunning = false;
async function runCapabilityCheck() {
  if (!ws || ws.readyState !== 1 || capRunning) return;
  capRunning = true;
  const nowIso = new Date().toISOString();
  capability3m.last_check = nowIso;
  capability5m.last_check = nowIso;
  capability3m.next_check = new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString();
  capability5m.next_check = new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString();
  capability3m.errors = [];
  capability5m.errors = [];

  let checked = 0;
  let any3m = false, any5m = false;

  for (const symbol of SYMBOLS) {
    const pair = symbol.replace("frx", "").replace(/^(...)(...)$/, "$1/$2");
    await sleep(GAP_MS); // spaced to prevent RateLimit

    try {
      const res = await new Promise((resolve) => {
        if (!ws || ws.readyState !== 1) return resolve(null);
        const id = reqId++;
        const t = setTimeout(() => { delete cfPending[id]; resolve(null); }, 6000);
        cfPending[id] = { symbol, onResult: (data) => { clearTimeout(t); resolve(data); } };
        // Note: NO currency param here, which fixes Deriv validation error
        ws.send(JSON.stringify({ contracts_for: symbol, req_id: id }));
      });

      checked++;

      if (res && !res.error) {
        if (res.has3mCall || res.has3mPut) any3m = true;
        if (res.has5mCall || res.has5mPut) any5m = true;

        // If not available, register clear status in probe and payouts without spamming proposals
        for (const ct of ["CALL", "PUT"]) {
          const has3 = ct === "CALL" ? res.has3mCall : res.has3mPut;
          const has5 = ct === "CALL" ? res.has5mCall : res.has5mPut;

          if (!has3) {
            probe[`${symbol}|${ct}|3`] = {
              pair,
              contract_type: ct,
              duration: 3,
              duration_unit: "m",
              stake: 10,
              proposal_id: null,
              proposal: null,
              error: { code: "ContractBuyValidationError", message: "Trading is not offered for this duration." },
              at: new Date().toISOString()
            };
            payouts[symbol] = payouts[symbol] || {};
            payouts[symbol]["3"] = { payout: null, unavailable: "Trading is not offered for this duration.", at: new Date().toISOString() };
          }
          if (!has5) {
            probe[`${symbol}|${ct}|5`] = {
              pair,
              contract_type: ct,
              duration: 5,
              duration_unit: "m",
              stake: 10,
              proposal_id: null,
              proposal: null,
              error: { code: "ContractBuyValidationError", message: "Trading is not offered for this duration." },
              at: new Date().toISOString()
            };
            payouts[symbol] = payouts[symbol] || {};
            payouts[symbol]["5"] = { payout: null, unavailable: "Trading is not offered for this duration.", at: new Date().toISOString() };
          }
        }
      } else if (res?.error) {
        capability3m.errors.push({ symbol, error: res.error.message || String(res.error) });
        capability5m.errors.push({ symbol, error: res.error.message || String(res.error) });
      }
    } catch (e) {
      capability3m.errors.push({ symbol, error: String(e) });
      capability5m.errors.push({ symbol, error: String(e) });
    }
  }

  capability3m.pairs_checked = checked;
  capability5m.pairs_checked = checked;

  // Check state transitions for 3m
  if (any3m && !capability3m.available) {
    capability3m.available = true;
    capability3m.status = "AVAILABLE";
    capability3m.last_change_timestamp = new Date().toISOString();
    alerts.push({
      level: "ALERT",
      message: "DERIV 3M CONTRACTS DETECTED AS AVAILABLE! Execution remains strictly paper-only (no auto-trading).",
      at: new Date().toISOString()
    });
  } else if (!any3m) {
    capability3m.available = false;
    capability3m.status = "UNAVAILABLE";
  }

  // Check state transitions for 5m
  if (any5m && !capability5m.available) {
    capability5m.available = true;
    capability5m.status = "AVAILABLE";
    capability5m.last_change_timestamp = new Date().toISOString();
    alerts.push({
      level: "ALERT",
      message: "DERIV 5M CONTRACTS DETECTED AS AVAILABLE! Execution remains strictly paper-only (no auto-trading).",
      at: new Date().toISOString()
    });
  } else if (!any5m) {
    capability5m.available = false;
    capability5m.status = "UNAVAILABLE";
  }

  capRunning = false;
}

// 15M PROPOSAL QUEUED FEED (Preserved 100% from V7)
let q15Busy = false;
const q15Stats = { rate_limit_total: 0, retries_total: 0, failed_after_retries: 0, last_cycle_ms: null, gaps_ms: [] };

function ask15(symbol, contract_type) {
  return new Promise((resolve) => {
    if (!ws || ws.readyState !== 1) return resolve(null);
    const id = reqId++;
    const t = setTimeout(() => { delete probePending[id]; resolve(null); }, 5000);
    probePending[id] = {
      symbol,
      contract_type,
      duration: 15,
      done: (m) => {
        clearTimeout(t);
        // Also keep payouts[symbol]["15"] in sync
        if (m.proposal) {
          payouts[symbol] = payouts[symbol] || {};
          payouts[symbol]["15"] = {
            payout: Math.round(((m.proposal.payout - m.proposal.ask_price) / m.proposal.ask_price) * 10000) / 10000,
            at: new Date().toISOString()
          };
          state.last_payout_at = new Date().toISOString();
        }
        resolve(m);
      }
    };
    ws.send(JSON.stringify({
      proposal: 1,
      amount: 10,
      basis: "stake",
      contract_type,
      currency: "USD",
      duration: 15,
      duration_unit: "m",
      underlying_symbol: symbol,
      req_id: id
    }));
  });
}

// If 3m or 5m ever becomes available, query their proposals smoothly
function askDur(symbol, contract_type, duration) {
  return new Promise((resolve) => {
    if (!ws || ws.readyState !== 1) return resolve(null);
    const id = reqId++;
    const t = setTimeout(() => { delete probePending[id]; resolve(null); }, 5000);
    probePending[id] = { symbol, contract_type, duration, done: (m) => { clearTimeout(t); resolve(m); } };
    ws.send(JSON.stringify({ proposal: 1, amount: 10, basis: "stake", contract_type, currency: "USD", duration, duration_unit: "m", underlying_symbol: symbol, req_id: id }));
  });
}

async function runProbe15() {
  if (!ws || ws.readyState !== 1 || q15Busy) return;
  q15Busy = true;
  const t0 = Date.now();
  let lastSend = 0;
  const gapsSeen = [];

  try {
    for (const symbol of SYMBOLS) {
      for (const contract_type of ["CALL", "PUT"]) {
        if (avail15[symbol] && avail15[symbol][contract_type] === false && !avail15[symbol].error) continue;
        for (let attempt = 0; attempt <= 2; attempt++) {
          const wait = lastSend ? GAP_MS - (Date.now() - lastSend) : 0;
          if (wait > 0) await sleep(wait);
          if (lastSend) gapsSeen.push(Date.now() - lastSend);
          lastSend = Date.now();

          const m = await ask15(symbol, contract_type);
          if (!m || !isRL(m.error)) break;
          q15Stats.rate_limit_total++;
          if (attempt === 2) {
            q15Stats.failed_after_retries++;
            break;
          }
          q15Stats.retries_total++;
          await sleep(attempt === 0 ? 1000 : 2000);
        }
      }
    }

    // If 3m or 5m capability ever becomes available, query them in queue without blocking
    if (capability3m.available) {
      for (const symbol of SYMBOLS) {
        for (const ct of ["CALL", "PUT"]) {
          await sleep(GAP_MS);
          await askDur(symbol, ct, 3);
        }
      }
    }
    if (capability5m.available) {
      for (const symbol of SYMBOLS) {
        for (const ct of ["CALL", "PUT"]) {
          await sleep(GAP_MS);
          await askDur(symbol, ct, 5);
        }
      }
    }
  } finally {
    q15Stats.last_cycle_ms = Date.now() - t0;
    q15Stats.gaps_ms = gapsSeen;
    q15Busy = false;
  }
}

function feed15() {
  const rows = Object.values(probe).filter((p) => p.duration === 15);
  const ok = rows.filter((p) => p.proposal && !p.error);
  const last = ok.map((p) => p.at).sort().pop() || null;
  return {
    status: state.status === "connected" ? "CONNECTED" : "DISCONNECTED",
    pairs_available: new Set(ok.map((p) => p.pair)).size,
    contracts_available: ok.length,
    availability: avail15,
    last_proposal_timestamp: last,
    proposal_age_s: last ? Math.round((Date.now() - Date.parse(last)) / 1000) : null,
    queue: {
      interval_ms: GAP_MS,
      cycle_s: 15,
      ...q15Stats,
      min_gap_ms: q15Stats.gaps_ms.length ? Math.min(...q15Stats.gaps_ms) : null
    },
    errors: rows.filter((p) => p.error).map((p) => ({ pair: p.pair, contract_type: p.contract_type, error: p.error })),
    reconnections: state.reconnect_count,
    proposals: ok.map((p) => ({
      pair: p.pair,
      contract_type: p.contract_type,
      duration: 15,
      ask_price: p.proposal.ask_price,
      payout: p.proposal.payout,
      potential_return: Math.round(((p.proposal.payout - p.proposal.ask_price) / p.proposal.ask_price) * 10000) / 10000,
      at: p.at
    }))
  };
}

// Deriv 1m candle history (ticks_history style=candles, max 1000 per call → paged backwards). Read-only, never buy/sell.
function historyPage(symbol, end) {
  return new Promise((resolve) => {
    if (!ws || ws.readyState !== 1) return resolve([]);
    const id = reqId++;
    const t = setTimeout(() => { delete hist[id]; resolve([]); }, 15000);
    hist[id] = (m) => { clearTimeout(t); resolve(m.error ? [] : m.candles || []); };
    ws.send(JSON.stringify({ ticks_history: symbol, style: "candles", granularity: 60, count: 1000, end, req_id: id }));
  });
}
async function history(symbol, count) {
  let out = [], end = "latest";
  while (out.length < count) {
    const page = await historyPage(symbol, end);
    if (!page.length) break;
    out = [...page.filter((c) => !out.length || c.epoch < out[0].epoch), ...out];
    end = page[0].epoch - 1;
    if (page.length < 1000) break;
  }
  return out.slice(-count);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");

  if (u.pathname === "/ticks") {
    const since = Number(u.searchParams.get("since") || 0);
    return res.end(JSON.stringify({ ticks: buffer.filter((t) => t.epoch >= since) }));
  }

  if (u.pathname === "/candles") {
    const symbol = u.searchParams.get("symbol");
    const count = Math.min(5000, Number(u.searchParams.get("count") || 3000));
    if (!SYMBOLS.includes(symbol)) {
      res.statusCode = 400;
      return res.end("{}");
    }
    return res.end(JSON.stringify({ symbol, source: "DERIV_TICKS_HISTORY", candles: await history(symbol, count) }));
  }

  if (u.pathname === "/health" || u.pathname === "/") {
    return res.end(JSON.stringify({
      service: "deriv-probe-isolated",
      version: 8,
      deriv_15m_proposal_feed: feed15(),
      DERIV_3M_CAPABILITY: capability3m,
      DERIV_5M_CAPABILITY: capability5m,
      contract_probe: Object.values(probe),
      alerts,
      ...state,
      uptime_seconds: Math.floor(process.uptime()),
      active_symbols_count: Object.keys(quotes).length,
      quotes,
      payouts_by_duration: payouts,
      gaps,
      buffer_ticks: buffer.length
    }));
  }

  res.statusCode = 404;
  res.end("{}");
}).listen(PORT, () => connect());
