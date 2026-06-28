// Stress test: ramp batch traffic until the pipeline saturates, to observe how it degrades.
// Expected behaviour past saturation: 429 + Retry-After instead of errors, latency bounded by
// EnqueueTimeout, flat memory (the channel is bounded). 429s are therefore not failures here.
//   k6 run load-tests/k6/stress.js
import http from 'k6/http';
import { BASE_URL, JSON_PARAMS, makeBatch, recordIngest } from './lib/events.js';

const BATCH = parseInt(__ENV.BATCH_SIZE || '100', 10);

http.setResponseCallback(http.expectedStatuses(202, 429));

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 1000,
      stages: [
        { target: 1000, duration: '1m' },
        { target: 2500, duration: '2m' },
        { target: 5000, duration: '2m' },
        { target: 5000, duration: '3m' },
        { target: 0, duration: '1m' },
      ],
    },
  },
  thresholds: {
    // No 5xx / connection errors even when overloaded.
    'http_req_failed': ['rate<0.001'],
    // Backpressure bounds latency: an overloaded request waits at most EnqueueTimeout per batch.
    'http_req_duration': ['p(99)<500'],
  },
};

export default function () {
  recordIngest(http.post(`${BASE_URL}/api/events/batch`, JSON.stringify(makeBatch(BATCH, __ITER * BATCH)), JSON_PARAMS), BATCH);
}
