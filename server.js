const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const DERIV_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public?app_id=1089";

const SYMBOLS = [
  "frxEURUSD",
  "frxGBPUSD",
  "frxUSDJPY",
  "frxAUDUSD",
  "frxUSDCAD",
  "frxEURGBP"
];

const state = {
  service: "deriv-probe-isolated",
  status: "STARTING",
  uptime_seconds: 0,
  connected_at: null,
  last_tick_at: null,
  last_payout_at: null,
  total_ticks_received: 0,
  errors_count: 0,
  reconnect_count: 0,
  active_symbols_count: 0,
  quotes: {}
};

SYMBOLS.forEach((sym) => {
  state.quotes[sym] = {
    quote: null,
    ask: null,
    bid: null,
    payout: null,
    epoch: null,
    updated_at: null
  };
});

const startTime = Date.now();
setInterval(() => {
  state.uptime_seconds = Math.floor((Date.now() - startTime) / 1000);
}, 1000);

let ws = null;
let reconnectTimer = null;
let pingTimer = null;
let payoutTimer = null;

function queryPayouts() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  SYMBOLS.forEach((sym, idx) => {
    try {
      ws.send(JSON.stringify({
        proposal: 1,
        amount: 10,
        basis: "stake",
        contract_type: "CALL",
        currency: "USD",
        duration: 15,
        duration_unit: "m",
        underlying_symbol: sym,
        req_id: 1000 + idx
      }));
    } catch (_) {}
  });
}

function connect() {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (pingTimer) clearInterval(pingTimer);
  if (payoutTimer) clearInterval(payoutTimer);

  state.status = "CONNECTING";

  try {
    ws = new WebSocket(DERIV_WS_URL);
  } catch (err) {
    state.status = "ERROR";
    state.errors_count++;
    scheduleReconnect();
    return;
  }

  ws.on("open", () => {
    state.status = "CONNECTED";
    state.connected_at = new Date().toISOString();

    // Inscreve no fluxo contínuo de ticks dos 6 pares
    SYMBOLS.forEach((sym) => {
      ws.send(JSON.stringify({ ticks: sym }));
    });

    // Pede os payouts logo na conexão e depois a cada 5 minutos
    queryPayouts();
    payoutTimer = setInterval(queryPayouts, 5 * 60 * 1000);

    // Ping a cada 30 segundos para manter a conexão aberta 24/7
    pingTimer = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ ping: 1 }));
      }
    }, 30000);
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Cotação em tempo real (tick)
      if (msg.msg_type === "tick" && msg.tick) {
        const t = msg.tick;
        const sym = t.symbol;
        if (state.quotes[sym]) {
          state.quotes[sym].quote = t.quote;
          state.quotes[sym].ask = t.ask;
          state.quotes[sym].bid = t.bid;
          state.quotes[sym].epoch = t.epoch;
          state.quotes[sym].updated_at = new Date().toISOString();
        }
        state.last_tick_at = new Date().toISOString();
        state.total_ticks_received++;
        state.active_symbols_count = Object.values(state.quotes).filter((q) => q.quote !== null).length;
      }

      // Resposta do Payout (proposal)
      if (msg.msg_type === "proposal" && msg.proposal && msg.echo_req) {
        const sym = msg.echo_req.underlying_symbol;
        const ask = Number(msg.proposal.ask_price);
        const payout = Number(msg.proposal.payout);
        if (state.quotes[sym] && ask > 0 && payout > ask) {
          // Calcula a porcentagem líquida do payout (ex: 85.5%)
          const netRate = Math.round(((payout - ask) / ask) * 1000) / 10;
          state.quotes[sym].payout = netRate;
          state.last_payout_at = new Date().toISOString();
        }
      }
    } catch (err) {
      state.errors_count++;
    }
  });

  ws.on("error", () => {
    state.errors_count++;
  });

  ws.on("close", () => {
    state.status = "DISCONNECTED";
    state.reconnect_count++;
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (pingTimer) clearInterval(pingTimer);
  if (payoutTimer) clearInterval(payoutTimer);
  reconnectTimer = setTimeout(() => {
    connect();
  }, 5000);
}

connect();

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    });
    res.end(JSON.stringify(state, null, 2));
    return;
  }
  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not Found");
});

server.listen(PORT, () => {
  console.log(`Deriv probe rodando na porta ${PORT}`);
});
