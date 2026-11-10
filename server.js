import WebSocket from 'ws';
import http from 'http';

// Porta que a Railway fornece para o serviço
const PORT = process.env.PORT || 3000;

// Os 6 pares de moedas monitorados pela Capital Trading
const SYMBOLS = [
  'frxEURUSD',
  'frxGBPUSD',
  'frxUSDJPY',
  'frxAUDUSD',
  'frxUSDCAD',
  'frxEURGBP'
];

// Endpoint oficial de nova geração da Deriv (Trading v1)
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';
const WS_URL = `wss://api.derivws.com/trading/v1/options/ws/public?app_id=${DERIV_APP_ID}`;

// Estado da conexão em memória
const state = {
  status: 'DISCONNECTED',
  connected_at: null,
  last_tick_at: null,
  total_ticks_received: 0,
  errors_count: 0,
  reconnect_count: 0,
  quotes: {}
};

let ws = null;
let pingInterval = null;

function connect() {
  state.status = 'CONNECTING';
  console.log(`[DERIV-PROBE] Conectando a ${WS_URL}...`);

  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    state.status = 'CONNECTED';
    state.connected_at = new Date().toISOString();
    console.log('[DERIV-PROBE] Conexao WebSocket estabelecida com sucesso!');

    // Inscreve nos 6 pares simultaneamente em uma unica ligacao
    for (const symbol of SYMBOLS) {
      ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
    }

    // Ping a cada 30 segundos para manter a conexao viva 24/7
    if (pingInterval) clearInterval(pingInterval);
    pingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ ping: 1 }));
      }
    }, 30000);
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.msg_type === 'tick' && msg.tick) {
        state.last_tick_at = new Date().toISOString();
        state.total_ticks_received++;
        state.quotes[msg.tick.symbol] = {
          quote: msg.tick.quote,
          ask: msg.tick.ask,
          bid: msg.tick.bid,
          epoch: msg.tick.epoch,
          updated_at: new Date().toISOString()
        };
      } else if (msg.error) {
        state.errors_count++;
        console.error('[DERIV-PROBE] Erro retornado pela Deriv:', msg.error);
      }
    } catch (err) {
      console.error('[DERIV-PROBE] Falha no parse:', err.message);
    }
  });

  ws.on('close', (code, reason) => {
    state.status = 'DISCONNECTED';
    state.reconnect_count++;
    if (pingInterval) clearInterval(pingInterval);
    console.warn(`[DERIV-PROBE] Conexao fechada (${code}). Reconectando em 5 segundos...`);
    setTimeout(connect, 5000);
  });

  ws.on('error', (err) => {
    state.errors_count++;
    console.error('[DERIV-PROBE] Erro no WebSocket:', err.message);
  });
}

// Inicia a conexao continua
connect();

// Servidor HTTP simples para checagem de saude e diagnostico da bancada isolada
const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200);
    res.end(JSON.stringify({
      service: 'deriv-probe-isolated',
      status: state.status,
      uptime_seconds: Math.floor(process.uptime()),
      connected_at: state.connected_at,
      last_tick_at: state.last_tick_at,
      total_ticks_received: state.total_ticks_received,
      errors_count: state.errors_count,
      reconnect_count: state.reconnect_count,
      active_symbols_count: Object.keys(state.quotes).length,
      quotes: state.quotes
    }, null, 2));
  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

server.listen(PORT, () => {
  console.log(`[DERIV-PROBE] Servidor de diagnostico rodando na porta ${PORT}`);
});
