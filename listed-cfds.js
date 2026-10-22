import { readFileSync } from 'node:fs';

export const LISTED_CFD_INSTRUMENTS = JSON.parse(readFileSync(new URL('./listed-cfd-instruments.json', import.meta.url), 'utf8'));
export const CFD_QUOTE_MAX_AGE_MS = 30 * 60 * 1000;
export const getCfdQuoteMaxAgeMs = symbol =>
  LISTED_CFD_INSTRUMENTS.find(instrument => instrument.symbol === symbol)?.quoteMaxAgeMs ?? CFD_QUOTE_MAX_AGE_MS;
export const LISTED_CFD_SUBSCRIPTIONS = [
  ...LISTED_CFD_INSTRUMENTS.map(item => ({ symbol: item.sourceSymbol, mic_code: item.mic })),
  { symbol: 'EUR/USD' }, { symbol: 'USD/CHF' }, { symbol: 'USD/DKK' },
];

function timeMs(value) {
  if (typeof value === 'number') return value < 1e12 ? value * 1000 : value;
  if (typeof value === 'string' && /^\d+(\.\d+)?$/.test(value)) return timeMs(Number(value));
  return typeof value === 'string' ? Date.parse(value) : NaN;
}

function fresh(price, timestamp, now, maxAgeMs = CFD_QUOTE_MAX_AGE_MS) {
  return Number.isFinite(price) && price > 0 && Number.isFinite(timestamp) && timestamp > 0 &&
    timestamp <= now + 60_000 && now - timestamp <= maxAgeMs;
}

// Conversion happens only here, on the VPS. Both the browser and Supabase
// receive the same USD quote and the oldest contributing provider timestamp.
export function toUsdCfdQuote(instrument, quote, fx, now = Date.now()) {
  if (quote.status === 'error' || quote.symbol !== instrument.sourceSymbol || quote.mic_code !== instrument.mic ||
      quote.currency !== instrument.quoteCurrency || (quote.isin && quote.isin !== instrument.isin)) {
    throw new Error(`Listing identity or currency mismatch for ${instrument.symbol}`);
  }
  const sourcePrice = Number(quote.close);
  const quoteTime = timeMs(quote.timestamp);
  if (!fresh(sourcePrice, quoteTime, now, getCfdQuoteMaxAgeMs(instrument.symbol))) throw new Error(`Missing, invalid or stale quote for ${instrument.symbol}`);
  let rate = 1;
  let timestamp = quoteTime;
  if (instrument.quoteCurrency !== 'USD') {
    rate = Number(fx?.rate);
    const fxTime = timeMs(fx?.timestamp);
    if (fx?.symbol !== `${instrument.quoteCurrency}/USD` || !fresh(rate, fxTime, now)) {
      throw new Error(`Missing, invalid or stale FX rate for ${instrument.symbol}`);
    }
    timestamp = Math.min(timestamp, fxTime);
  }
  const price = sourcePrice * rate;
  if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid USD valuation for ${instrument.symbol}`);
  return { symbol: instrument.symbol, price, timestamp: new Date(timestamp).toISOString(),
    updated_at: new Date(now).toISOString(), volume_24h: Number.isFinite(Number(quote.volume)) ? Math.max(0, Number(quote.volume)) : 0,
    change_24h: 0, high_price_24h: price, low_price_24h: price, bid_price: price, ask_price: price };
}

const normalizedExchange = value => String(value).replace(/[\s_-]/g, '').toUpperCase();

export class ListedCfdFeed {
  constructor(publish, clock = Date.now) {
    this.publish = publish;
    this.clock = clock;
    this.nativeQuotes = new Map();
    this.fxQuotes = new Map();
    this.confirmedVenues = new Map();
    this.failures = new Map();
  }

  resetSubscriptions() { this.confirmedVenues.clear(); }

  hasVenue(instrument, data) {
    if (data.mic_code) return data.mic_code === instrument.mic;
    if (data.exchange) return [instrument.mic, ...instrument.exchangeAliases]
      .some(name => normalizedExchange(name) === normalizedExchange(data.exchange));
    return this.confirmedVenues.get(instrument.sourceSymbol) === instrument.mic;
  }

  handleSubscriptionStatus(message) {
    for (const data of message.success ?? []) {
      const instrument = LISTED_CFD_INSTRUMENTS.find(item => item.sourceSymbol === data.symbol);
      if (instrument && this.hasVenue(instrument, data)) {
        this.confirmedVenues.set(instrument.sourceSymbol, instrument.mic);
        this.failures.delete(instrument.symbol);
      }
    }
    for (const data of message.fails ?? []) {
      const instrument = LISTED_CFD_INSTRUMENTS.find(item => item.sourceSymbol === data.symbol);
      if (instrument) {
        this.confirmedVenues.delete(instrument.sourceSymbol);
        this.failures.set(instrument.symbol, 'Provider subscription unavailable');
      }
    }
  }

  // Finnhub already provides these FX pairs; Twelve also subscribes to them
  // so USD conversion can keep working if either upstream is unavailable.
  handleFxTick(symbol, price, timestamp) {
    const key = symbol.replace(/[\/_]/g, '').toUpperCase();
    if (!['EURUSD', 'USDCHF', 'USDDKK'].includes(key)) return false;
    const currency = key === 'EURUSD' ? 'EUR' : key.slice(3);
    const rate = key === 'EURUSD' ? Number(price) : 1 / Number(price);
    const tickTime = timeMs(timestamp);
    if (!fresh(rate, tickTime, this.clock())) return true;
    const previous = this.fxQuotes.get(currency);
    if (previous && tickTime < previous.timestamp) return true;
    this.fxQuotes.set(currency, { symbol: `${currency}/USD`, rate, timestamp: tickTime });
    for (const instrument of LISTED_CFD_INSTRUMENTS.filter(item => item.quoteCurrency === currency)) this.publishQuote(instrument);
    return true;
  }

  handleTick(data) {
    if (!data || typeof data.symbol !== 'string') return false;
    if (this.handleFxTick(data.symbol, data.price, data.timestamp)) return true;
    const instrument = LISTED_CFD_INSTRUMENTS.find(item => item.sourceSymbol === data.symbol);
    if (!instrument) return false;
    if (!this.hasVenue(instrument, data) || (data.currency && data.currency !== instrument.quoteCurrency) ||
        (data.isin && data.isin !== instrument.isin)) return true;
    const timestamp = timeMs(data.timestamp);
    const price = Number(data.price);
    if (!fresh(price, timestamp, this.clock(), getCfdQuoteMaxAgeMs(instrument.symbol))) return true;
    const previous = this.nativeQuotes.get(instrument.symbol);
    if (previous && timestamp < previous.timestamp) return true;
    this.nativeQuotes.set(instrument.symbol, { symbol: instrument.sourceSymbol, mic_code: instrument.mic,
      currency: instrument.quoteCurrency, close: price, timestamp, volume: data.day_volume, isin: instrument.isin });
    this.publishQuote(instrument);
    return true;
  }

  publishQuote(instrument) {
    const nativeQuote = this.nativeQuotes.get(instrument.symbol);
    if (!nativeQuote) return;
    try {
      const quote = toUsdCfdQuote(instrument, nativeQuote, this.fxQuotes.get(instrument.quoteCurrency), this.clock());
      this.publish({ symbol: quote.symbol, price: quote.price, bid: quote.bid_price, ask: quote.ask_price,
        timestamp: quote.timestamp, currency: 'USD', source: 'twelvedata', isin: instrument.isin,
        source_symbol: instrument.sourceSymbol, source_currency: instrument.quoteCurrency,
        source_price: nativeQuote.close, mic_code: instrument.mic, volume: quote.volume_24h });
    } catch { /* Keep the last provider time; no fake quote when FX is missing/stale. */ }
  }

  status() {
    return LISTED_CFD_INSTRUMENTS.map(instrument => ({ symbol: instrument.symbol, mic: instrument.mic,
      received: this.nativeQuotes.has(instrument.symbol), error: this.failures.get(instrument.symbol) ?? null }));
  }
}
