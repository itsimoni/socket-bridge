import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import WebSocket from 'ws';
import { yahooPayload, yahooSymbolFromUrl } from './helpers/yahoo-fixtures.js';

function nextMessage(socket, predicate) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { socket.off('message', receive); reject(new Error('Message timed out')); }, 5000);
    const receive = raw => {
      const message = JSON.parse(raw.toString());
      if (!predicate(message)) return;
      clearTimeout(timeout);
      socket.off('message', receive);
      resolve(message);
    };
    socket.on('message', receive);
  });
}

test('Yahoo VPS polling supplies HTTP and WS clients once per cycle with no provider keys or client-triggered fetches', { timeout: 20_000 }, async t => {
  const timestamp = Math.floor(Date.now() / 1000) - 5;
  const requests = [];
  let releaseLastQuote;
  const upstream = createServer((req, res) => {
    const symbol = yahooSymbolFromUrl(`http://127.0.0.1${req.url}`);
    requests.push(symbol);
    const send = () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(yahooPayload(symbol, symbol.endsWith('=X') ? timestamp - 60 : timestamp)));
    };
    if (symbol === 'NOVO-B.CO') releaseLastQuote = send;
    else send();
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  // Reserve an ephemeral port for the child without assuming a fixed local port.
  const reservation = createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const child = spawn(process.execPath, ['index.js'], {
    cwd: fileURLToPath(new URL('../', import.meta.url)), windowsHide: true,
    env: { ...process.env, PORT: String(port), FINNHUB_API_KEY: '', TWELVE_DATA_API_KEY: '',
      LISTED_CFD_PROVIDER: '', YAHOO_POLL_INTERVAL_MS: '60000', YAHOO_REQUEST_TIMEOUT_MS: '10000',
      YAHOO_CHART_BASE_URL: `http://127.0.0.1:${upstream.address().port}/v8/finance/chart/` },
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
    upstream.closeAllConnections();
    await new Promise(resolve => upstream.close(resolve));
  });

  const deadline = Date.now() + 10_000;
  while (!releaseLastQuote && Date.now() < deadline) await delay(20);
  assert.ok(releaseLastQuote, logs.slice(-1500));
  assert.equal(requests.length, 8);
  for (let index = 0; index < 2; index++) {
    const client = new WebSocket(`ws://127.0.0.1:${port}`);
    clients.push(client);
    const initial = nextMessage(client, message => message.type === 'snapshot');
    await once(client, 'open');
    assert.equal((await initial).data['EWLD.PA'].price, null);
  }
  const updates = clients.map(client => nextMessage(client, message =>
    message.type === 'price_update' && message.data.some(quote => quote.symbol === 'EWLD.PA' && quote.price === 44)));
  releaseLastQuote();
  for (const message of await Promise.all(updates)) {
    const quote = message.data.find(item => item.symbol === 'WSRIA.SW');
    assert.equal(quote.price, 25);
    assert.equal(quote.currency, 'USD');
    assert.equal(quote.source_currency, 'CHF');
    assert.equal(quote.source, 'yahoo');
    assert.equal(quote.timestamp, new Date((timestamp - 60) * 1000).toISOString());
  }
  const base = `http://127.0.0.1:${port}`;
  const health = await (await fetch(`${base}/health`)).json();
  assert.equal(health.listedCfdProvider, 'yahoo');
  assert.equal(health.yahoo.running, true);
  assert.equal(health.twelve.enabled, false);
  assert.ok(health.listedCfds.every(item => item.received && !item.error));
  const latest = await (await fetch(`${base}/latest?symbol=NOVO-B.CO`)).json();
  assert.equal(latest.price, 60);
  assert.equal(latest.source_price, 400);
  const snapshot = await (await fetch(`${base}/snapshot`)).json();
  assert.equal(snapshot['IWDA.L'].price, 100);
  assert.equal(snapshot['CSPX.L'].price, 500);
  for (const client of clients) {
    const response = nextMessage(client, message => message.type === 'snapshot');
    client.send(JSON.stringify({ type: 'request_snapshot' }));
    assert.equal((await response).data['EWLD.PA'].timestamp, snapshot['EWLD.PA'].timestamp);
  }
  assert.equal(requests.length, 8, 'Client connections and cache reads must never trigger Yahoo polling');
});
