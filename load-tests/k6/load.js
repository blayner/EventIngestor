// Load test: sustained traffic at the design target.
//   - 1,000 single-event requests/s
//   - 50 batch requests/s of 100 events (5,000 events/s)
// for 5 minutes. Thresholds encode the latency targets; they are pass/fail criteria, not results.
//   k6 run load-tests/k6/load.js
import http from 'k6/http';
import { BASE_URL, JSON_PARAMS, makeBatch, makeEvent, recordIngest } from './lib/events.js';

const DURATION = __ENV.DURATION || '5m';

http.setResponseCallback(http.expectedStatuses(202));

export const options = {
  discardResponseBodies: false,
  scenarios: {
    single: {
      executor: 'constant-arrival-rate',
      exec: 'single',
      rate: parseInt(__ENV.SINGLE_RATE || '1000', 10),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
    batch: {
      executor: 'constant-arrival-rate',
      exec: 'batch',
      rate: parseInt(__ENV.BATCH_RATE || '50', 10),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.001'],
    'http_req_duration{scenario:single}': ['p(95)<10', 'p(99)<25'],
    'http_req_duration{scenario:batch}': ['p(95)<50', 'p(99)<100'],
    'throttled': ['rate==0'],
    'dropped_iterations': ['count<100'],
  },
};

export function single() {
  recordIngest(http.post(`${BASE_URL}/api/events`, JSON.stringify(makeEvent(__ITER)), JSON_PARAMS), 1);
}

export function batch() {
  recordIngest(http.post(`${BASE_URL}/api/events/batch`, JSON.stringify(makeBatch(100, __ITER * 100)), JSON_PARAMS), 100);
}
