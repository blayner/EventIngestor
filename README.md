# EventIngestor

High-throughput event ingestion service built on .NET 10.

Work in progress. The goal is a small service that accepts events over gRPC and HTTP,
pushes them through a bounded in-memory pipeline and flushes them in batches to pluggable
sinks — while keeping allocations and tail latency under control.

## Layout

```
src/
  EventIngestor.Core/       models and abstractions
  EventIngestor.Pipeline/   channel pipeline, batching, parsing
  EventIngestor.Service/    ASP.NET Core host (gRPC + REST)
```

## Build

```bash
dotnet build
```
