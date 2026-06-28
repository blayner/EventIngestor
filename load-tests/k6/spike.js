// Spike test: a steady baseline interrupted by a sudden 20x burst. Checks that the bounded channel
// absorbs the burst (or throttles cleanly) and that latency returns to baseline afterwards.
//   k6 run load-tests/k6/spike.js
import http from 'k6/http';
import { BASE_URL, JSON_PARAMS, makeEvent, recordIngest } from './lib/events.js';

http.setResponseCallback(http.expectedStatuses(202, 429));

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 200,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 1500,
      stages: [
        { target: 200, duration: '1m' },
        { target: 4000, duration: '10s' },
        { target: 4000, duration: '30s' },
        { target: 200, duration: '10s' },
        { target: 200, duration: '2m' },
      ],
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.001'],
    'http_req_duration': ['p(99)<250'],
  },
};

export default function () {
  recordIngest(http.post(`${BASE_URL}/api/events`, JSON.stringify(makeEvent(__ITER)), JSON_PARAMS), 1);
}
