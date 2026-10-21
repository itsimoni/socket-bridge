// Representative chart metadata, including the CHF trading currency for WSRIA.
const quotes = {
  'IWDA.L': ['USD', 'LSE', 'ETF', 100],
  'EWLD.PA': ['EUR', 'PAR', 'ETF', 40],
  'WSRIA.SW': ['CHF', 'EBS', 'ETF', 20],
  'CSPX.L': ['USD', 'LSE', 'ETF', 500],
  'NOVO-B.CO': ['DKK', 'CPH', 'EQUITY', 400],
  'EURUSD=X': ['USD', 'CCY', 'CURRENCY', 1.1],
  'CHFUSD=X': ['USD', 'CCY', 'CURRENCY', 1.25],
  'DKKUSD=X': ['USD', 'CCY', 'CURRENCY', 0.15],
};

export function yahooPayload(symbol, timestamp, overrides = {}) {
  const [currency, exchangeName, instrumentType, regularMarketPrice] = quotes[symbol];
  return { chart: { error: null, result: [{
    meta: { symbol, currency, exchangeName, instrumentType, regularMarketPrice,
      regularMarketTime: timestamp, ...overrides },
    timestamp: [timestamp - 3600], indicators: { adjclose: [{ adjclose: [99999] }] },
  }] } };
}

export const yahooSymbolFromUrl = url => decodeURIComponent(new URL(url).pathname.split('/').at(-1));
