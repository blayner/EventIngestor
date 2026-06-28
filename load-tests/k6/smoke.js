// Smoke test: one virtual user exercising every HTTP endpoint. Runs in CI against the container.
//   k6 run load-tests/k6/smoke.js
import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { BASE_URL, JSON_PARAMS, NDJSON_PARAMS, makeBatch, makeEvent, makeNdjson, recordIngest } from './lib/events.js';

export const options = {
  vus: 1,
  duration: __ENV.DURATION || '30s',
  thresholds: {
    checks: ['rate==1.0'],
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

export default function () {
  group('single', () => {
    const res = http.post(`${BASE_URL}/api/events`, JSON.stringify(makeEvent(__ITER)), JSON_PARAMS);
    recordIngest(res, 1);
  });

  group('batch', () => {
    const res = http.post(`${BASE_URL}/api/events/batch`, JSON.stringify(makeBatch(50, __ITER * 50)), JSON_PARAMS);
    recordIngest(res, 50);
  });

  group('ndjson', () => {
    const res = http.post(`${BASE_URL}/api/events/ndjson`, makeNdjson(100, __ITER * 100), NDJSON_PARAMS);
    recordIngest(res, 100);
  });

  group('invalid', () => {
    const res = http.post(`${BASE_URL}/api/events`, JSON.stringify({ type: 'missing-source' }), {
      ...JSON_PARAMS,
      responseCallback: http.expectedStatuses(400),
    });
    check(res, { 'invalid event is a 400': (r) => r.status === 400 });
  });

  group('health', () => {
    check(http.get(`${BASE_URL}/health/ready`), { 'ready': (r) => r.status === 200 });
  });

  sleep(0.5);
}
