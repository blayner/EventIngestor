// Shared helpers for the k6 scenarios.
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const PAYLOAD_BYTES = parseInt(__ENV.PAYLOAD_BYTES || '256', 10);

export const JSON_PARAMS = { headers: { 'Content-Type': 'application/json' } };
export const NDJSON_PARAMS = { headers: { 'Content-Type': 'application/x-ndjson' } };

/** Events acknowledged by the service (202 bodies). */
export const eventsAccepted = new Counter('events_accepted');
/** Fraction of requests answered with 429 (backpressure), tracked separately from errors. */
export const throttledRate = new Rate('throttled');

const SOURCES = ['checkout-api', 'billing', 'inventory', 'notifications', 'search'];
const TYPES = ['order.created', 'order.paid', 'invoice.issued', 'stock.reserved', 'email.sent', 'query.executed'];
const FILLER = 'x'.repeat(Math.max(0, PAYLOAD_BYTES - 64));

export function makeEvent(i) {
  return {
    source: SOURCES[i % SOURCES.length],
    type: TYPES[i % TYPES.length],
    timestamp: new Date().toISOString(),
    payload: { seq: i, vu: __VU, amount: (i % 1000) / 10, data: FILLER },
  };
}

export function makeBatch(size, offset) {
  const events = new Array(size);
  for (let i = 0; i < size; i++) {
    events[i] = makeEvent(offset + i);
  }
  return events;
}

export function makeNdjson(size, offset) {
  return makeBatch(size, offset).map((e) => JSON.stringify(e)).join('\n');
}

/** Records the outcome of an ingestion call. 202 and 429 are both valid answers; anything else is an error. */
export function recordIngest(res, expected) {
  throttledRate.add(res.status === 429);

  let accepted = 0;
  if (res.status === 202 || res.status === 429) {
    accepted = res.json('accepted') || 0;
    eventsAccepted.add(accepted);
  }

  return check(res, {
    'status is 202 or 429': (r) => r.status === 202 || r.status === 429,
    'no invalid events': (r) => r.status === 429 || r.json('rejected') === 0,
    'all events accounted for': () => res.status !== 202 || accepted === expected,
  });
}
