// gRPC load test (IngestBatch over HTTP/2 cleartext on :8081).
// Loads the proto from the repository (relative to this script), so it needs the full checkout,
// not just the k6 folder:
//   k6 run load-tests/k6/grpc.js
import grpc from 'k6/net/grpc';
import encoding from 'k6/encoding';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { makeBatch } from './lib/events.js';

const GRPC_ADDR = __ENV.GRPC_ADDR || 'localhost:8081';
const BATCH = parseInt(__ENV.BATCH_SIZE || '100', 10);

const grpcAccepted = new Counter('grpc_events_accepted');

const client = new grpc.Client();
client.load(['../../src/EventIngestor.Service/Protos'], 'ingest.proto');

export const options = {
  scenarios: {
    batches: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '200', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    checks: ['rate>0.999'],
    grpc_req_duration: ['p(95)<25', 'p(99)<50'],
  },
};

export default function () {
  if (__ITER === 0) {
    client.connect(GRPC_ADDR, { plaintext: true });
  }

  // Protobuf JSON mapping: bytes are base64, Timestamp is RFC 3339.
  const events = makeBatch(BATCH, __ITER * BATCH).map((e) => ({
    source: e.source,
    type: e.type,
    timestamp: e.timestamp,
    payload: encoding.b64encode(JSON.stringify(e.payload)),
  }));

  const res = client.invoke('eventingestor.v1.EventIngestion/IngestBatch', { events });

  check(res, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
    'nothing rejected': (r) => r && r.message && (r.message.rejected || 0) === 0,
  });

  if (res && res.message) {
    grpcAccepted.add(res.message.accepted || 0);
  }
}

export function teardown() {
  client.close();
}
