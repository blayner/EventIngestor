# Load tests (k6)

| Script | Purpose | Shape |
|---|---|---|
| `k6/smoke.js` | Every endpoint works; runs in CI | 1 VU, 30 s |
| `k6/load.js` | Sustained traffic at the design target | 1,000 single req/s + 50 batch req/s (100 events) for 5 min |
| `k6/stress.js` | Find the saturation point and watch degradation | batch traffic ramped to 5,000 req/s |
| `k6/spike.js` | Absorb a sudden burst and recover | 200 → 4,000 → 200 req/s |
| `k6/grpc.js` | Same as `load.js`, over gRPC `IngestBatch` | 200 req/s of 100 events for 2 min |

Thresholds in the scripts are **targets** (pass/fail criteria), not measured results.

## Running

Start the stack first (`docker compose up -d --build`), then:

```bash
k6 run load-tests/k6/smoke.js
k6 run load-tests/k6/load.js
k6 run -e DURATION=1m -e SINGLE_RATE=500 load-tests/k6/load.js   # shorter / lighter
k6 run load-tests/k6/stress.js
k6 run load-tests/k6/spike.js
k6 run load-tests/k6/grpc.js                                     # needs the full checkout (loads the .proto)
```

Without a local k6 install:

```bash
docker run --rm -i --network host -v "$PWD/load-tests/k6:/scripts" grafana/k6 run /scripts/smoke.js
```

Useful variables: `BASE_URL` (default `http://localhost:8080`), `GRPC_ADDR` (`localhost:8081`),
`PAYLOAD_BYTES` (256), `BATCH_SIZE` (100), `DURATION`.

## What to watch

The k6 summary only shows the client side. Keep Grafana (http://localhost:3000, dashboard
*EventIngestor*) open during a run:

- `ingestor_pipeline_queue_depth` vs capacity: is the pipeline keeping up or filling?
- `ingestor_events_rejected_total{reason="backpressure"}`: throttling starts here, before errors.
- `ingestor_batch_flush_duration_seconds`: is the sink the bottleneck?
- `ingestor_event_pool_created_total`: should flatten after warm-up (pool reuse working).
- Process memory / GC panels: memory should stay flat under stress thanks to the bounded channel.

Also: run the load generator on a different machine (or at least different cores) than the service,
otherwise k6 competes with the service for CPU and the numbers mean little.
