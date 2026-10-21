# Valore VPS market stream

This service is deployed behind `wss://stream.valore.markets`. It owns the
Finnhub/Twelve Data credentials and upstream connections. Neither the frontend
nor Supabase connects directly to Twelve Data.

```text
Finnhub + Twelve Data -> socket-bridge -> stream.valore.markets -> browser
                                            |
                                            +-> Supabase market_data mirror
```

## Run and deploy

Install this directory's dependencies (`npm ci` after the lockfile exists),
configure `.env` from `.env.example`, and run `npm start`. The existing VPS
process manager can continue to run `node index.js`.

Copy **index.js, listed-cfds.js, listed-cfd-instruments.json, package.json and
package-lock.json** together. Keep the existing `.env` and reverse-proxy/TLS
configuration. The application connects to the `.markets` hostname; configure
the proxy and certificate for that hostname before restarting the service.

The catalogue JSON is shared with the frontend/Supabase source; changes should
be deployed to both the VPS and application from the same checkout. The legacy
symbol lists and provider connections are retained. The five new listings use
exchange-qualified Twelve Data WebSocket subscription objects, plus EUR/USD
and USD/DKK for valuation. Actual exchange access remains subject to the VPS
account's subscription. Failed subscriptions are listed in `/health`.

Only the VPS performs conversion: native EUR/DKK prices become USD quotes under
`EWLD.PA` and `NOVO-B.CO`. Each quote retains the older security/FX source time.
The bridge does not publish a new USD valuation when required FX is unavailable
or stale. Other listings publish their USD source price under their canonical
symbols. Subscription failures do not cause mock quotes or ADR substitutions.

`TWELVE_DATA_WS_URL` is an optional upstream URL override used by the local
integration test; normal deployments use Twelve Data's default WebSocket URL.

## Downstream interface

- `GET /health`: provider state, cached symbols, new listing subscription status.
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
  "source": "twelvedata",
  "source_currency": "EUR",
  "source_price": 40,
  "isin": "LU2655993207",
  "mic_code": "XPAR"
}
```

Equal bid/ask reflects a last-price feed; the app applies its CFD spreads.
Re-requesting a snapshot never updates the source timestamps.

## Verify

Run `npm test` in this directory. The integration test starts a local fake
Twelve Data WebSocket, launches the real bridge with test credentials, and
checks qualified subscriptions, USD conversion, HTTP snapshots, multiple
downstream clients, updates and ping/pong. It does not connect to vendor or
production servers. Run `npm run test:cfd` in the repository root for decoder,
database mirror, instrument and stale-quote checks.

The existing TradingView chart remains independent. It displays the underlying
exchange chart; this service supplies Valore's order/valuation prices.

Provider protocol: [Twelve Data streaming guide](https://support.twelvedata.com/en/articles/5620516-how-to-stream-the-data),
[official SDK subscription/event types](https://github.com/twelvedata/twelvedata-go/tree/main/twelvedata/ws).
