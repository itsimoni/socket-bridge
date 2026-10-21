import assert from 'node:assert/strict';
import { test } from 'node:test';
import { YahooCfdFeed, YAHOO_REQUESTS, parseYahooChart } from '../yahoo-cfds.js';
import { yahooPayload, yahooSymbolFromUrl } from './helpers/yahoo-fixtures.js';

const now = Date.UTC(2026, 9, 21, 12);
const seconds = now / 1000;
const expected = symbol => YAHOO_REQUESTS.find(item => item.symbol === symbol);

test('Yahoo parser validates exact symbol, exchange, type, currency, ISIN and original quote time', () => {
  const request = expected('IWDA.L');
  assert.deepEqual(parseYahooChart(yahooPayload('IWDA.L', seconds), request, now), { price: 100, timestamp: now });
  for (const overrides of [{ symbol: 'SWDA.L' }, { exchangeName: 'AMS' }, { currency: 'GBP' },
    { instrumentType: 'EQUITY' }, { isin: 'wrong' }, { regularMarketPrice: null },
    { regularMarketPrice: 0 }, { regularMarketPrice: Infinity }, { regularMarketTime: null },
    { regularMarketTime: 'invalid' }, { regularMarketTime: seconds + 61 }]) {
    assert.throws(() => parseYahooChart(yahooPayload('IWDA.L', seconds, overrides), request, now));
  }
  assert.throws(() => parseYahooChart({ chart: { error: { code: 'Not Found' } } }, request, now));
  assert.throws(() => parseYahooChart({ chart: { result: [] } }, request, now));
});

test('one shared Yahoo poll converts all five exact listings using EUR, CHF and DKK rates', async () => {
  const published = [], requests = [];
  const feed = new YahooCfdFeed(quote => published.push(quote), { clock: () => now,
    fetch: async (url, options) => {
      requests.push(String(url));
      assert.equal(new URL(url).hostname, 'query1.finance.yahoo.com');
      assert.equal(options.redirect, 'error');
      const symbol = yahooSymbolFromUrl(url);
      return Response.json(yahooPayload(symbol, symbol.endsWith('=X') ? seconds - 60 : seconds));
    } });
  await feed.poll();
  assert.equal(requests.length, 8);
  assert.equal(published.length, 5);
  const quotes = Object.fromEntries(published.map(quote => [quote.symbol, quote]));
  assert.equal(quotes['IWDA.L'].price, 100);
  assert.equal(quotes['EWLD.PA'].price, 44);
  assert.equal(quotes['WSRIA.SW'].price, 25);
  assert.equal(quotes['CSPX.L'].price, 500);
  assert.equal(quotes['NOVO-B.CO'].price, 60);
  assert.equal(quotes['WSRIA.SW'].source_currency, 'CHF');
  assert.equal(quotes['WSRIA.SW'].source_price, 20);
  assert.equal(quotes['WSRIA.SW'].isin, 'IE00BK72HJ67');
  for (const quote of published) {
    assert.equal(quote.currency, 'USD');
    assert.equal(quote.source, 'yahoo');
    assert.equal(quote.data_mode, 'polled');
    assert.equal(quote.timestamp, new Date(quote.source_currency === 'USD' ? now : now - 60_000).toISOString());
  }
  await feed.poll();
  assert.equal(published.length, 5, 'Polling unchanged timestamps/prices must not create new ticks');
});

test('stale securities, bad native currencies and stale/missing FX never publish a USD valuation', async () => {
  for (const mode of ['stale-security', 'stale-fx', 'bad-currency', 'missing-fx']) {
    const published = [];
    const feed = new YahooCfdFeed(quote => published.push(quote), { clock: () => now,
      fetch: async url => {
        const symbol = yahooSymbolFromUrl(url);
        if (mode === 'missing-fx' && symbol === 'CHFUSD=X') return new Response(null, { status: 404 });
        const stale = mode === 'stale-security' && symbol === 'WSRIA.SW' || mode === 'stale-fx' && symbol === 'CHFUSD=X';
        return Response.json(yahooPayload(symbol, stale ? seconds - 1801 : seconds,
          mode === 'bad-currency' && symbol === 'WSRIA.SW' ? { currency: 'USD' } : {}));
      } });
    await feed.poll();
    assert.equal(published.length, 4, mode);
    assert.ok(!published.some(quote => quote.symbol === 'WSRIA.SW'), mode);
    assert.ok(feed.status().find(item => item.symbol === 'WSRIA.SW').error, mode);
  }
});

test('invalid/out-of-order responses preserve the last verified quote and source timestamp', async () => {
  let phase = 0, current = now;
  const published = [];
  const feed = new YahooCfdFeed(quote => published.push(quote), { clock: () => current,
    fetch: async url => {
      const symbol = yahooSymbolFromUrl(url);
      return Response.json(yahooPayload(symbol, seconds - (phase === 1 ? 1 : 0),
        phase === 2 && symbol === 'IWDA.L' ? { currency: 'EUR', regularMarketPrice: 1 } : {}));
    } });
  await feed.poll();
  phase = 1;
  await feed.poll();
  phase = 2;
  await feed.poll();
  assert.equal(published.length, 5);
  assert.equal(feed.published.get('IWDA.L').price, 100);
  assert.equal(feed.published.get('IWDA.L').timestamp, new Date(now).toISOString());
  current = now + 1_800_001;
  assert.ok(feed.status().every(item => item.error));
});

test('rate limits halt the cycle and honor Retry-After without overlapping or retrying other hosts', async () => {
  let current = now, requests = 0, limited = true;
  const feed = new YahooCfdFeed(() => {}, { clock: () => current,
    fetch: async url => {
      requests++;
      if (limited) return new Response(null, { status: 429, headers: { 'Retry-After': '180' } });
      return Response.json(yahooPayload(yahooSymbolFromUrl(url), seconds));
    } });
  await feed.poll();
  assert.equal(requests, 1);
  assert.equal(feed.blockedUntil, now + 180_000);
  current += 120_000;
  await feed.poll();
  assert.equal(requests, 1);
  limited = false;
  current = now + 180_001;
  await feed.poll();
  assert.equal(requests, 9);
  assert.equal(feed.lastError, null);
  assert.equal(feed.backoffMs, 60_000);
});

test('concurrent poll calls share one request cycle and stop aborts an in-flight request', async () => {
  let release, requests = 0, aborted = false;
  const feed = new YahooCfdFeed(() => {}, { clock: () => now,
    fetch: (_url, { signal }) => {
      requests++;
      return new Promise((_resolve, reject) => {
        release = () => reject(new Error('test failure'));
        signal.addEventListener('abort', () => { aborted = true; reject(signal.reason); }, { once: true });
      });
    } });
  const first = feed.poll();
  assert.equal(feed.poll(), first);
  assert.equal(requests, 1);
  assert.ok(release);
  feed.stop();
  await first;
  assert.ok(aborted);
  assert.equal(requests, 1);
});

test('poll interval validation and backoff never shorten a configured hourly poll', async () => {
  assert.throws(() => new YahooCfdFeed(() => {}, { pollIntervalMs: 1 }), /YAHOO_POLL_INTERVAL_MS/);
  assert.throws(() => new YahooCfdFeed(() => {}, { baseUrl: 'http://example.com/' }), /HTTPS/);
  const feed = new YahooCfdFeed(() => {}, { clock: () => now, pollIntervalMs: 3_600_000,
    fetch: async () => new Response(null, { status: 429 }) });
  await feed.poll();
  assert.equal(feed.backoffMs, 3_600_000);
});
