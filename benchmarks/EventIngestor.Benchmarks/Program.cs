using BenchmarkDotNet.Running;

// dotnet run -c Release --project benchmarks/EventIngestor.Benchmarks -- --filter '*'
// dotnet run -c Release --project benchmarks/EventIngestor.Benchmarks -- --filter '*Parser*' --job short
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
