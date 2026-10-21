# Valore VPS market stream

This service is deployed behind `wss://stream.valore.markets`. It owns the
Finnhub/Twelve Data credentials and upstream connections, and polls Yahoo Finance
for the five listed CFDs by default. Neither the frontend nor Supabase connects
directly to Yahoo Finance or Twelve Data.

```text
Finnhub + Twelve Data + Yahoo -> socket-bridge -> stream.valore.markets -> browser
                                            |
                                            +-> Supabase market_data mirror
```

## Run and deploy

Install this directory's dependencies (`npm ci` after the lockfile exists),
configure `.env` from `.env.example`, and run `npm start`. The existing VPS
process manager can continue to run `node index.js`.

Copy **index.js, listed-cfds.js, yahoo-cfds.js, listed-cfd-instruments.json, package.json and
package-lock.json** together. Keep the existing `.env` and reverse-proxy/TLS
configuration. The application connects to the `.markets` hostname; configure
the proxy and certificate for that hostname before restarting the service.

The catalogue JSON is shared with the frontend/Supabase source; changes should
be deployed to both the VPS and application from the same checkout. The legacy
symbol lists and provider connections are retained. Yahoo polling needs Node.js
18+ (Node.js 20+ recommended for the VPS) and no API key. Existing provider keys
continue serving the other instruments.

Add or update these settings in the VPS `.env`:

```dotenv
LISTED_CFD_PROVIDER=yahoo
YAHOO_POLL_INTERVAL_MS=60000
YAHOO_REQUEST_TIMEOUT_MS=10000
```

Yahoo is the default when `LISTED_CFD_PROVIDER` is unset. The poll interval must
be between 60,000 and 3,600,000 ms; the request timeout must be between 1,000 and
30,000 ms. One cycle sequentially requests the five exchange-specific tickers
and `EURUSD=X`, `CHFUSD=X`, `DKKUSD=X`. Browser connections and snapshot requests
only read the VPS cache. Cycles do not overlap. HTTP 401/403/429/503 responses
stop the cycle and trigger backoff, honoring `Retry-After`; there is no proxy,
credential or host rotation to bypass limits.

To use the previous exchange-qualified Twelve Data feed for these five instead,
set `LISTED_CFD_PROVIDER=twelvedata`. It needs appropriate exchange access and
also subscribes to EUR/USD, USD/CHF and USD/DKK. The two providers do not write
the five canonical symbols simultaneously. Neither provider affects the legacy
Finnhub/Twelve Data symbol lists.

Only the VPS performs conversion: native EUR/CHF/DKK prices become USD quotes
under `EWLD.PA`, `WSRIA.SW` and `NOVO-B.CO`. WSRIA's fund/share-class name includes
USD, but the selected Swiss quote is in CHF. Each quote retains the older security/FX source time.
The bridge does not publish a new USD valuation when required FX is unavailable
or stale. Other listings publish their USD source price under their canonical
symbols. Subscription failures do not cause mock quotes or ADR substitutions.

`TWELVE_DATA_WS_URL` is an optional upstream URL override used by the local
integration test; normal deployments use Twelve Data's default WebSocket URL.
`YAHOO_CHART_BASE_URL` is likewise a test override; leave it unset in production.
It defaults to `https://query1.finance.yahoo.com/v8/finance/chart/`.

Yahoo supplies regular-market last prices through an unofficial HTTP endpoint,
not a guaranteed real-time or bid/ask feed. Responses must match the expected
symbol, exchange, instrument type and quote currency. The bridge uses
`regularMarketTime`, not the fetch time, candle date or adjusted close. If a
security or its FX is older than 30 minutes, it publishes no replacement USD
quote. `/health` reports the stale/missing source; existing cached prices keep
their original time and the application/database freshness checks still apply.
Polling every minute does not make a delayed or illiquid listing's price fresh.

The [yfinance documentation](https://ranaroussi.github.io/yfinance/) describes
Yahoo's personal/research usage context. Technical access does not establish
commercial redistribution rights or a market-data service guarantee.

## Downstream interface

- `GET /health`: provider state, `listedCfdProvider`, Yahoo polling/backoff state,
  and per-listing source times, stale/missing FX and request errors.
- `GET /snapshot`: complete cached symbol object.
- `GET /latest?symbol=EWLD.PA`: one cached quote.
- `GET /latest-batch?symbols=EWLD.PA,NOVO-B.CO`: selected cached quotes.
- WebSocket: initial `snapshot`, then `price_update` arrays; handles `ping`,
  `subscribe` and `request_snapshot` locally without extra upstream connections.

Example downstream quote:

```json
{
  "symbol": "EWLD.PA",
  "price": 44,
  "bid": 44,
  "ask": 44,
  "currency": "USD",
  "timestamp": "2026-10-21T12:00:00.000Z",
  "source": "yahoo",
  "data_mode": "polled",
  "source_currency": "EUR",
  "source_price": 40,
  "isin": "LU2655993207",
  "mic_code": "XPAR"
}
```

Equal bid/ask reflects a last-price feed; the app applies its CFD spreads.
Re-requesting a snapshot never updates the source timestamps.

## Verify

Run `npm test` in this directory. Integration tests start local fake Yahoo HTTP
and Twelve Data WebSocket servers, launch the real bridge, and check USD
conversion, HTTP snapshots, multiple downstream clients, updates, shared polling
and ping/pong. Unit tests cover identity/currency rejection, source timestamps,
stale FX, out-of-order data, cancellation and rate-limit backoff. Tests do not
connect to vendor or production servers. Run `npm run test:cfd` in the repository root for decoder,
database mirror, instrument and stale-quote checks.

The existing TradingView chart remains independent. It displays the underlying
exchange chart; this service supplies Valore's order/valuation prices.

Provider protocol: [Twelve Data streaming guide](https://support.twelvedata.com/en/articles/5620516-how-to-stream-the-data),
[official SDK subscription/event types](https://github.com/twelvedata/twelvedata-go/tree/main/twelvedata/ws).
