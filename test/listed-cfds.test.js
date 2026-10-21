import assert from 'node:assert/strict';
import { test } from 'node:test';
import { LISTED_CFD_INSTRUMENTS, LISTED_CFD_SUBSCRIPTIONS, ListedCfdFeed } from '../listed-cfds.js';

const now = Date.UTC(2026, 9, 21, 12);
const seconds = now / 1000;
const tick = (symbol, mic_code, price, currency, overrides = {}) =>
  ({ symbol, mic_code, price, currency, timestamp: seconds, ...overrides });

test('subscriptions identify the five exact exchanges and both FX dependencies', () => {
  assert.equal(LISTED_CFD_SUBSCRIPTIONS.length, 7);
  for (const listing of LISTED_CFD_INSTRUMENTS) {
    assert.ok(LISTED_CFD_SUBSCRIPTIONS.some(item => item.symbol === listing.sourceSymbol && item.mic_code === listing.mic));
  }
  assert.ok(!LISTED_CFD_SUBSCRIPTIONS.some(item => item.symbol === 'NVO'));
});

test('equity and FX events publish USD prices and revalue when FX changes', () => {
  const published = [];
  const feed = new ListedCfdFeed(quote => published.push(quote), () => now);
  feed.handleTick(tick('EWLD', 'XPAR', 40, 'EUR'));
  assert.equal(published.length, 0);
  feed.handleTick({ symbol: 'EUR/USD', price: 1.1, timestamp: seconds - 60 });
  assert.equal(published.at(-1).price, 44);
  assert.equal(published.at(-1).currency, 'USD');
  assert.equal(published.at(-1).timestamp, new Date(now - 60_000).toISOString());
  feed.handleFxTick('EURUSD', 1.2, now);
  assert.equal(published.at(-1).price, 48);
  feed.handleTick(tick('NOVO-B', 'XCSE', 400, 'DKK'));
  feed.handleTick({ symbol: 'USD/DKK', price: 8, timestamp: seconds });
  assert.equal(published.at(-1).price, 50);
  assert.equal(published.at(-1).symbol, 'NOVO-B.CO');
});

test('wrong venues/currencies, stale FX and out-of-order ticks cannot replace valid quotes', () => {
  const published = [];
  const feed = new ListedCfdFeed(quote => published.push(quote), () => now);
  feed.handleTick(tick('IWDA', 'XAMS', 80, 'EUR'));
  feed.handleTick(tick('IWDA', 'XLON', 80, 'GBP'));
  feed.handleTick(tick('IWDA', 'XLON', -10, 'USD'));
  assert.equal(published.length, 0);
  feed.handleTick(tick('IWDA', 'XLON', 100, 'USD'));
  feed.handleTick(tick('IWDA', 'XLON', 90, 'USD', { timestamp: seconds - 1 }));
  assert.equal(published.length, 1);
  assert.equal(published[0].price, 100);
  feed.handleTick(tick('EWLD', 'XPAR', 40, 'EUR'));
  feed.handleFxTick('EURUSD', 1.1, now - 1_800_001);
  assert.equal(published.length, 1);
});

test('venue-less events require a confirmed subscription; reconnect clears confirmation', () => {
  const published = [];
  const feed = new ListedCfdFeed(quote => published.push(quote), () => now);
  const data = tick('IWDA', undefined, 100, 'USD');
  feed.handleTick(data);
  assert.equal(published.length, 0);
  feed.handleSubscriptionStatus({ success: [{ symbol: 'IWDA', mic_code: 'XLON' }] });
  feed.handleTick(data);
  assert.equal(published.length, 1);
  feed.resetSubscriptions();
  feed.handleTick(data);
  assert.equal(published.length, 1);
  feed.handleSubscriptionStatus({ fails: [{ symbol: 'IWDA', mic_code: 'XLON' }] });
  assert.ok(feed.status().find(item => item.symbol === 'IWDA.L').error);
});
