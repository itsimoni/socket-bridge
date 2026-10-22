import { LISTED_CFD_INSTRUMENTS, CFD_QUOTE_MAX_AGE_MS, getCfdQuoteMaxAgeMs, toUsdCfdQuote } from './listed-cfds.js';

const EXCHANGES = { XLON: 'LSE', XPAR: 'PAR', XSWX: 'EBS', XCSE: 'CPH' };
const FX_REQUESTS = ['EUR', 'CHF', 'DKK'].map(currency => ({
  symbol: `${currency}USD=X`, currency: 'USD', exchange: 'CCY', type: 'CURRENCY', fxCurrency: currency,
}));
const SECURITY_REQUESTS = LISTED_CFD_INSTRUMENTS.map(instrument => ({
  symbol: instrument.symbol, currency: instrument.quoteCurrency, exchange: EXCHANGES[instrument.mic],
  type: instrument.type === 'etf' ? 'ETF' : 'EQUITY', instrument,
}));
export const YAHOO_REQUESTS = [...FX_REQUESTS, ...SECURITY_REQUESTS];

// Use regular-market metadata, never an adjusted close or a daily candle's time.
export function parseYahooChart(payload, expected, now = Date.now()) {
  const result = payload?.chart?.result;
  if (payload?.chart?.error || !Array.isArray(result) || result.length !== 1) {
    throw new Error(`Yahoo quote unavailable for ${expected.symbol}`);
  }
  const meta = result[0]?.meta;
  if (!meta || meta.symbol !== expected.symbol || meta.exchangeName !== expected.exchange ||
      meta.currency !== expected.currency || meta.instrumentType !== expected.type ||
      (meta.isin && expected.instrument && meta.isin !== expected.instrument.isin)) {
    throw new Error(`Yahoo listing identity or currency mismatch for ${expected.symbol}`);
  }
  const price = meta.regularMarketPrice;
  const timestamp = meta.regularMarketTime * 1000;
  if (typeof price !== 'number' || !Number.isFinite(price) || price <= 0 ||
      typeof meta.regularMarketTime !== 'number' || !Number.isFinite(timestamp) || timestamp <= 0 ||
      timestamp > now + 60_000) {
    throw new Error(`Yahoo price or source timestamp invalid for ${expected.symbol}`);
  }
  return { price, timestamp };
}

function boundedInteger(value, fallback, minimum, maximum, name) {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(number) || number < minimum || number > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return number;
}

function retryDelay(value, now) {
  if (!value) return 0;
  const seconds = Number(value);
  const ms = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(value) - now;
  return Number.isFinite(ms) ? Math.max(0, ms) : 0;
}

export class YahooCfdFeed {
  constructor(publish, options = {}) {
    this.publish = publish;
    this.fetch = options.fetch ?? fetch;
    this.clock = options.clock ?? Date.now;
    this.pollIntervalMs = boundedInteger(options.pollIntervalMs, 60_000, 60_000, 3_600_000, 'YAHOO_POLL_INTERVAL_MS');
    this.requestTimeoutMs = boundedInteger(options.requestTimeoutMs, 10_000, 1_000, 30_000, 'YAHOO_REQUEST_TIMEOUT_MS');
    this.baseUrl = new URL(options.baseUrl ?? 'https://query1.finance.yahoo.com/v8/finance/chart/');
    if (this.baseUrl.protocol !== 'https:' && !(this.baseUrl.protocol === 'http:' &&
        ['127.0.0.1', 'localhost', '[::1]'].includes(this.baseUrl.hostname))) {
      throw new Error('Yahoo chart URL requires HTTPS (HTTP is allowed only for loopback tests)');
    }
    this.nativeQuotes = new Map();
    this.fxQuotes = new Map();
    this.published = new Map();
    this.failures = new Map();
    this.running = false;
    this.stopped = false;
    this.inFlight = null;
    this.timer = null;
    this.requestController = null;
    this.lastPollAt = null;
    this.nextPollAt = null;
    this.lastError = null;
    this.failureCount = 0;
    this.backoffMs = this.pollIntervalMs;
    this.blockedUntil = 0;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.stopped = false;
    void this.runLoop();
  }

  stop() {
    this.running = false;
    this.stopped = true;
    clearTimeout(this.timer);
    this.timer = null;
    this.nextPollAt = null;
    this.requestController?.abort();
  }

  async runLoop() {
    try { await this.poll(); }
    catch { this.lastError = 'Yahoo poll failed'; }
    if (!this.running) return;
    const delay = Math.max(this.backoffMs, this.blockedUntil - this.clock());
    this.nextPollAt = new Date(this.clock() + delay).toISOString();
    this.timer = setTimeout(() => { void this.runLoop(); }, delay);
  }

  poll() {
    if (this.inFlight) return this.inFlight;
    if (this.stopped || this.clock() < this.blockedUntil) return Promise.resolve();
    this.inFlight = this.pollOnce().finally(() => { this.inFlight = null; });
    return this.inFlight;
  }

  async fetchQuote(expected) {
    const url = new URL(encodeURIComponent(expected.symbol), this.baseUrl);
    url.searchParams.set('interval', '1d');
    url.searchParams.set('range', '5d');
    const controller = new AbortController();
    this.requestController = controller;
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      const response = await this.fetch(url, {
        signal: controller.signal, redirect: 'error', headers: { Accept: 'application/json' },
      });
      if (!response.ok) {
        const error = new Error(`Yahoo HTTP ${response.status} for ${expected.symbol}`);
        error.status = response.status;
        error.retryMs = retryDelay(response.headers.get('retry-after'), this.clock());
        await response.body?.cancel();
        throw error;
      }
      return parseYahooChart(await response.json(), expected, this.clock());
    } catch (error) {
      if (controller.signal.aborted) throw new Error(`Yahoo request timed out for ${expected.symbol}`);
      throw error;
    } finally {
      clearTimeout(timeout);
      if (this.requestController === controller) this.requestController = null;
    }
  }

  async pollOnce() {
    this.lastPollAt = new Date(this.clock()).toISOString();
    this.lastError = null;
    let successes = 0;
    let blocked = false;
    // Sequential, shared polling avoids bursts and never scales with WS clients.
    for (const expected of YAHOO_REQUESTS) {
      if (this.stopped) break;
      try {
        const quote = await this.fetchQuote(expected);
        const store = expected.fxCurrency ? this.fxQuotes : this.nativeQuotes;
        const key = expected.fxCurrency ?? expected.symbol;
        if (store.get(key)?.timestamp > quote.timestamp) throw new Error(`Yahoo source time moved backwards for ${expected.symbol}`);
        store.set(key, quote);
        this.failures.delete(expected.symbol);
        successes++;
      } catch (error) {
        if (this.stopped) break;
        const message = error instanceof Error ? error.message : 'Yahoo request failed';
        this.failures.set(expected.symbol, message);
        this.lastError = message;
        if ([401, 403, 429, 503].includes(error?.status)) {
          this.failureCount++;
          this.backoffMs = Math.max(this.pollIntervalMs,
            Math.min(this.pollIntervalMs * 2 ** Math.min(this.failureCount, 10), 900_000));
          this.blockedUntil = this.clock() + Math.max(this.backoffMs, error.retryMs ?? 0);
          blocked = true;
          break;
        }
      }
    }
    if (this.stopped) return;
    if (!blocked) {
      this.failureCount = successes > 0 ? 0 : this.failureCount + 1;
      this.backoffMs = Math.max(this.pollIntervalMs,
        Math.min(this.pollIntervalMs * 2 ** Math.min(this.failureCount, 10), 900_000));
    }
    for (const instrument of LISTED_CFD_INSTRUMENTS) this.publishQuote(instrument);
  }

  publishQuote(instrument) {
    const native = this.nativeQuotes.get(instrument.symbol);
    if (!native) return;
    try {
      const fx = this.fxQuotes.get(instrument.quoteCurrency);
      const quote = toUsdCfdQuote(instrument, { symbol: instrument.sourceSymbol, mic_code: instrument.mic,
        currency: instrument.quoteCurrency, close: native.price, timestamp: native.timestamp },
      fx && { symbol: `${instrument.quoteCurrency}/USD`, rate: fx.price, timestamp: fx.timestamp }, this.clock());
      const previous = this.published.get(instrument.symbol);
      if (previous && previous.timestamp > quote.timestamp) return;
      if (previous && previous.timestamp === quote.timestamp && previous.price === quote.price) return;
      const downstream = {
        symbol: instrument.symbol, price: quote.price, bid: quote.price, ask: quote.price,
        timestamp: quote.timestamp, currency: 'USD', source: 'yahoo', data_mode: 'polled',
        isin: instrument.isin, mic_code: instrument.mic, source_symbol: instrument.symbol,
        source_currency: instrument.quoteCurrency, source_price: native.price,
        source_timestamp: new Date(native.timestamp).toISOString(),
        ...(fx && { fx_timestamp: new Date(fx.timestamp).toISOString() }),
      };
      this.publish(downstream);
      this.published.set(instrument.symbol, downstream);
    } catch { /* Preserve the last quote and its source time when prices/FX expire. */ }
  }

  status() {
    return LISTED_CFD_INSTRUMENTS.map(instrument => {
      const native = this.nativeQuotes.get(instrument.symbol);
      const published = this.published.get(instrument.symbol);
      let error = this.failures.get(instrument.symbol) ?? null;
      if (!error && native) {
        if (this.clock() - native.timestamp > getCfdQuoteMaxAgeMs(instrument.symbol)) error = 'Yahoo source quote is stale';
        else if (instrument.quoteCurrency !== 'USD') {
          const fx = this.fxQuotes.get(instrument.quoteCurrency);
          if (!fx || this.clock() - fx.timestamp > CFD_QUOTE_MAX_AGE_MS) error = 'Yahoo FX quote is missing or stale';
        }
      }
      return { symbol: instrument.symbol, mic: instrument.mic, provider: 'yahoo',
        received: Boolean(native), timestamp: published?.timestamp ?? null,
        sourceTimestamp: native ? new Date(native.timestamp).toISOString() : null, error };
    });
  }

  health() {
    return { enabled: true, running: this.running, inFlight: Boolean(this.inFlight),
      pollIntervalMs: this.pollIntervalMs, lastPollAt: this.lastPollAt, nextPollAt: this.nextPollAt,
      lastError: this.lastError, backoffMs: this.backoffMs };
  }
}
