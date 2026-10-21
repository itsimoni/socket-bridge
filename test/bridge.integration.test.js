import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:net';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import WebSocket, { WebSocketServer } from 'ws';
import { LISTED_CFD_INSTRUMENTS } from '../listed-cfds.js';

async function unusedPort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}

function nextMessage(socket, predicate) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { socket.off('message', onMessage); reject(new Error('Message timed out')); }, 5000);
    const onMessage = raw => {
      const data = JSON.parse(raw.toString());
      if (!predicate(data)) return;
      clearTimeout(timeout);
      socket.off('message', onMessage);
      resolve(data);
    };
    socket.on('message', onMessage);
  });
}

test('VPS bridge relays exchange-qualified USD quotes, snapshots and live updates to multiple clients', { timeout: 20_000 }, async t => {
  const upstream = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await once(upstream, 'listening');
  const port = await unusedPort();
  const timestamp = Math.floor(Date.now() / 1000) - 5;
  let providerSocket;
  let providerConnections = 0;
  let extendedSubscription;
  upstream.on('connection', socket => {
    providerConnections++;
    providerSocket = socket;
    socket.on('message', raw => {
      const message = JSON.parse(raw.toString());
      if (message.action !== 'subscribe' || !Array.isArray(message.params.symbols)) return;
      extendedSubscription = message.params.symbols;
      socket.send(JSON.stringify({ event: 'subscribe-status', success: extendedSubscription, fails: [] }));
      for (const [symbol, price] of [['EUR/USD', 1.1], ['USD/DKK', 8]]) {
        socket.send(JSON.stringify({ event: 'price', symbol, price, timestamp }));
      }
      for (const listing of LISTED_CFD_INSTRUMENTS) {
        socket.send(JSON.stringify({ event: 'price', symbol: listing.sourceSymbol, mic_code: listing.mic,
          currency: listing.quoteCurrency, price: listing.symbol === 'NOVO-B.CO' ? 400 : 40, timestamp }));
      }
    });
  });

  const child = spawn(process.execPath, ['index.js'], {
    cwd: fileURLToPath(new URL('../', import.meta.url)), windowsHide: true,
    env: { ...process.env, PORT: String(port), FINNHUB_API_KEY: '', TWELVE_DATA_API_KEY: 'integration-test',
      TWELVE_DATA_WS_URL: `ws://127.0.0.1:${upstream.address().port}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let logs = '';
  child.stdout.on('data', data => { logs += data; });
  child.stderr.on('data', data => { logs += data; });
  const clients = [];
  t.after(async () => {
    for (const client of clients) client.terminate();
    if (child.exitCode === null) {
      const exited = once(child, 'exit');
      child.kill();
      await exited;
    }
    for (const client of upstream.clients) client.terminate();
    await new Promise(resolve => upstream.close(resolve));
  });

  const baseUrl = `http://127.0.0.1:${port}`;
  let quote;
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      quote = await (await fetch(`${baseUrl}/latest?symbol=EWLD.PA`)).json();
      if (quote?.price === 44) break;
    } catch { /* The child may still be starting. */ }
    await delay(30);
  }
  assert.equal(quote?.price, 44, logs.slice(-1500));
  assert.equal(quote.currency, 'USD');
  assert.equal(quote.timestamp, new Date(timestamp * 1000).toISOString());
  assert.equal(extendedSubscription.length, 7);
  assert.ok(extendedSubscription.some(item => item.symbol === 'IWDA' && item.mic_code === 'XLON'));
  const snapshot = await (await fetch(`${baseUrl}/snapshot`)).json();
  assert.equal(snapshot['NOVO-B.CO'].price, 50);
  assert.equal(snapshot['CSPX.L'].price, 40);
  assert.equal(snapshot['WSRIA.SW'].isin, 'IE00BK72HJ67');

  for (let index = 0; index < 2; index++) {
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    clients.push(socket);
    const initial = nextMessage(socket, message => message.type === 'snapshot');
    await once(socket, 'open');
    assert.equal((await initial).data['EWLD.PA'].price, 44);
  }
  assert.equal(providerConnections, 1);
  const pong = nextMessage(clients[0], message => message.type === 'pong');
  clients[0].send(JSON.stringify({ type: 'ping' }));
  await pong;

  const updates = clients.map(socket => nextMessage(socket, message =>
    message.type === 'price_update' && message.data.some(item => item.symbol === 'EWLD.PA' && item.price > 50)));
  providerSocket.send(JSON.stringify({ event: 'price', symbol: 'EWLD', mic_code: 'XPAR',
    currency: 'EUR', price: 50, timestamp: timestamp + 1 }));
  for (const message of await Promise.all(updates)) {
    const updated = message.data.find(item => item.symbol === 'EWLD.PA');
    assert.ok(Math.abs(updated.price - 55) < 1e-8);
    assert.equal(updated.timestamp, new Date(timestamp * 1000).toISOString());
  }
  const requested = nextMessage(clients[0], message => message.type === 'snapshot');
  clients[0].send(JSON.stringify({ type: 'request_snapshot' }));
  assert.ok((await requested).data['EWLD.PA'].price > 50);
});
