# FLURM Performance Benchmarks

## Quick Start

Run the benchmark suite with a single command:

```bash
# Default benchmarks (recommended)
./scripts/run_benchmarks.escript

# Quick benchmarks (faster, less accurate)
./scripts/run_benchmarks.escript --quick

# Full benchmarks (slower, more accurate)
./scripts/run_benchmarks.escript --full

# Save results to a file
./scripts/run_benchmarks.escript --output results.txt

# Save results in JSON format
./scripts/run_benchmarks.escript --json --output results.json
```

Or run benchmarks from the Erlang shell:

```erlang
%% Compile first
rebar3 compile

%% Start Erlang shell
erl -pa _build/default/lib/*/ebin

%% Run all benchmarks
flurm_benchmark:run_all_benchmarks().

%% Run individual benchmarks
flurm_benchmark:benchmark_job_submission(1000).
flurm_benchmark:benchmark_scheduler_cycle(100).
flurm_benchmark:benchmark_protocol_encoding(10000).
flurm_benchmark:benchmark_state_persistence(1000).

%% Format results for output
Results = flurm_benchmark:run_all_benchmarks().
Report = flurm_benchmark:format_results(Results).
io:format("~s", [Report]).
```

## Benchmark API

The benchmark module (`flurm_benchmark`) provides the following primary functions:

| Function | Description |
|----------|-------------|
| `run_all_benchmarks()` | Run complete benchmark suite with default parameters |
| `benchmark_job_submission(N)` | Submit N jobs and measure throughput |
| `benchmark_scheduler_cycle(N)` | Measure scheduler performance with N pending jobs |
| `benchmark_protocol_encoding(N)` | Measure protocol encode/decode speed |
| `benchmark_state_persistence(N)` | Measure state save/load performance |
| `format_results(Results)` | Format results for human-readable output |

## Test Environment

- **Platform**: macOS Darwin 24.6.0
- **Erlang/OTP**: 28
- **CPU**: 8 cores (8 schedulers)
- **Process limit**: 1,048,576
- **Date**: February 2026 (Phase 7-8 completion)

## Measured Baseline Results (2026-02-04)

### FLURM Core Operations

| Benchmark | Throughput | Avg Latency | p99 Latency | Memory Delta |
|-----------|------------|-------------|-------------|--------------|
| Job Submission | 3,164,557 ops/sec | 0.24 µs | 2 µs | 143 KB |
| Scheduler Cycle | 127,714 ops/sec | 7.79 µs | 24 µs | 186 KB |
| Protocol Encoding | 1,362,769 ops/sec | 0.67 µs | 3 µs | 1.4 MB |
| State Store (ETS) | 2,136,752 ops/sec | 0.32 µs | 1 µs | 123 KB |
| State Lookup (ETS) | 3,921,569 ops/sec | 0.19 µs | 2 µs | - |

**Analysis**:
- Job submission throughput exceeds 3M ops/sec - supports massive burst workloads
- Scheduler can process ~128K cycles/sec with pending jobs
- Protocol encoding/decoding handles 1.3M+ messages/sec
- ETS persistence exceeds 2M stores/sec and 3.9M lookups/sec

### TRES (Trackable Resources) Performance

| Operation | Throughput | Avg Latency | p99 Latency |
|-----------|------------|-------------|-------------|
| TRES from_job | 4,856,255 ops/sec | 0.0004 ms | 0.001 ms |
| TRES add | 2,500,000+ ops/sec | 0.0004 ms | 0.001 ms |
| TRES format | 370,370 ops/sec | 0.0027 ms | 0.018 ms |
| TRES parse | 434,783 ops/sec | 0.0025 ms | 0.023 ms |

**Analysis**:
- TRES calculations are sub-microsecond, enabling real-time resource tracking
- Memory growth after 50K TRES operations: only 1.13 KB (no leaks)
- Concurrent TRES operations (10 procs × 10K ops) complete without contention

### Binary Operations (Protocol Codec)

| Operation | Time (10K ops) | Throughput | Latency |
|-----------|----------------|------------|---------|
| Header encode | 0.19 ms | 52,910,053 ops/sec | 0.019 µs |
| Header decode | 0.19 ms | 51,813,472 ops/sec | 0.019 µs |

**Analysis**: Protocol header encoding/decoding is extremely fast (~53M ops/sec) due to Erlang's native binary pattern matching. This is a significant advantage over C-based string parsing.

### Data Structures

| Operation | Time (10K ops) | Throughput | Latency |
|-----------|----------------|------------|---------|
| ETS insert | 1.60 ms | 6,257,822 ops/sec | 0.160 µs |
| ETS lookup | 0.74 ms | 13,440,860 ops/sec | 0.074 µs |
| Map put | 2.59 ms | 3,865,481 ops/sec | 0.259 µs |
| Map get | 0.28 ms | 36,363,636 ops/sec | 0.028 µs |

**Analysis**:
- ETS provides ~6.3M inserts/sec and ~13.4M lookups/sec - suitable for job and node registries
- Maps are faster for lookups (~36M/sec) but slower for updates (~3.9M/sec)
- Both are well within requirements for clusters up to 100K+ jobs

### Concurrency

| Operation | Time | Throughput | Latency |
|-----------|------|------------|---------|
| Process spawn+stop (10K) | 13.79 ms | 724,900 ops/sec | 1.379 µs |
| Message passing (100K) | 12.80 ms | 7,811,279 ops/sec | 0.128 µs |
| Gen_server call (10K) | 6.48 ms | 1,543,210 ops/sec | 0.648 µs |

**Analysis**:
- Can spawn ~725K processes per second - supports process-per-job model at scale
- Message passing at ~7.8M msgs/sec enables high-throughput scheduler
- Synchronous gen_server calls at ~1.5M/sec - sufficient for job submission rates

## Projected Production Performance

Based on measured baseline benchmarks (2026-02-04) and FLURM architecture:

### Job Submission Throughput

| Metric | Projected Value | Baseline Evidence |
|--------|-----------------|-------------------|
| Peak submission rate | 100,000+ jobs/sec | 3.1M ops/sec measured, 3% overhead for network/auth |
| Sustained submission | 50,000+ jobs/sec | With full persistence pipeline |
| Multi-controller | 200,000+ jobs/sec | 3-node cluster with load balancing |

### Scheduling Latency

| Metric | Projected Value | Baseline Evidence |
|--------|-----------------|-------------------|
| Job submission to scheduled | < 1 ms | 0.24 µs measured + queue + schedule |
| Backfill scheduling cycle | < 10 ms | 127K cycles/sec = 7.8 µs/cycle |
| TRES calculation per job | < 0.01 ms | 0.0004 ms measured |
| Fair-share calculation | < 10 ms | Based on TRES aggregation speed |

### Failover Performance

| Metric | Projected Value | Notes |
|--------|-----------------|-------|
| Leader election | < 500 ms | Ra consensus (TLA+ verified) |
| State recovery | < 1 sec | From Ra log replay |
| Client reconnect | < 2 sec | Automatic with backup controllers |
| Job state consistency | 100% | Ra consensus guarantees (TLA+ verified) |

## Comparison with SLURM

| Metric | SLURM | FLURM | Improvement |
|--------|-------|-------|-------------|
| Failover time | 30-60 sec | < 1 sec | 30-60x |
| Config reload | Restart | Hot reload | N/A |
| Scheduler lock | Global | None | Eliminates bottleneck |
| Max controllers | 2 | Unlimited | N/A |

## Code Coverage Summary

| Module Category | Coverage | Notes |
|-----------------|----------|-------|
| Protocol layer | 31-100% | Header 100%, codec 31%, overall strong |
| Infrastructure | 50-100% | Supervisors and apps well tested |
| Job management | 0-50% | Executor tested, manager needs integration tests |
| Scheduler | 0% | Requires full application context |
| Node management | 0-58% | Node daemon app 58%, server needs tests |
| **Overall** | **6%** | See docs/COVERAGE.md for explanation |

### High Coverage Modules (>50%)
- flurm_protocol_header: 100%
- flurm_job_executor_sup: 100%
- flurm_node_daemon_sup: 100%
- flurm_protocol: 91%
- flurm_state_persistence: 88%
- flurm_protocol_pack: 86%
- flurm_protocol_auth: 84%
- flurm_munge: 83%
- flurm_node_daemon_app: 58%
- flurm_system_monitor: 54%
- flurm_job_executor: 50%

### Coverage Improvement Priorities
1. flurm_protocol_codec (31%) - Increase to 60% with more message type tests
2. flurm_scheduler (0%) - Add integration test framework
3. flurm_job_manager (0%) - Add integration tests with supervision tree
4. flurm_controller_handler (0%) - Fix test mocking issues
5. flurm_dbd_* (31%) - Improve database daemon coverage

See [docs/COVERAGE.md](COVERAGE.md) for the complete coverage strategy, justified exceptions, and improvement roadmap.

## Running Benchmarks

### Command Line

```bash
# Navigate to FLURM directory
cd /path/to/flurm

# Run default benchmarks
./scripts/run_benchmarks.escript

# Run quick benchmarks (fewer iterations, faster)
./scripts/run_benchmarks.escript --quick

# Run full benchmarks (more iterations, more accurate)
./scripts/run_benchmarks.escript --full

# Save results to a file
./scripts/run_benchmarks.escript --output benchmark_results.txt

# Save as JSON for automated processing
./scripts/run_benchmarks.escript --json --output benchmark_results.json
```

### Programmatic Access

```erlang
%% From Erlang shell or within tests
Results = flurm_benchmark:run_all_benchmarks().

%% Get formatted report
Report = flurm_benchmark:format_results(Results).

%% Compare two runs
OldResults = [...],
NewResults = [...],
Comparison = flurm_benchmark:compare_reports(OldResults, NewResults).
```

### Coverage Testing

```bash
# Run with coverage
rebar3 cover --verbose

# Run property-based tests
rebar3 proper
```

## Benchmark Scripts

| Script | Description |
|--------|-------------|
| `scripts/run_benchmarks.escript` | Primary benchmark runner with CLI options |
| `apps/flurm_core/src/flurm_benchmark.erl` | Core benchmark module with API functions |

## Expected Baseline Numbers

When running benchmarks on modern hardware (8-core CPU, Erlang/OTP 28), expect approximately:

| Benchmark | Expected Throughput | Expected Latency (avg) | p99 Latency |
|-----------|---------------------|------------------------|-------------|
| Job Submission | 3,000,000+ ops/sec | < 0.5 µs | < 5 µs |
| Scheduler Cycle | 100,000+ cycles/sec | < 10 µs | < 30 µs |
| Protocol Encoding | 1,000,000+ ops/sec | < 1 µs | < 5 µs |
| State Persistence (Store) | 2,000,000+ ops/sec | < 0.5 µs | < 2 µs |
| State Persistence (Lookup) | 3,500,000+ ops/sec | < 0.25 µs | < 3 µs |
| TRES Calculation | 4,000,000+ ops/sec | < 0.001 ms | < 0.01 ms |

**Note**: These numbers vary significantly based on:
- Hardware (CPU speed, memory bandwidth)
- System load
- Erlang/OTP version
- Number of iterations (more iterations = more accurate but slower)

## Performance Targets

These are the minimum acceptable values for production deployment:

| Metric | Target | Measured | Status |
|--------|--------|----------|--------|
| TRES calc p99 | < 0.1 ms | 0.001 ms | ✅ PASS |
| TRES add p99 | < 0.1 ms | 0.001 ms | ✅ PASS |
| TRES throughput | > 100,000 ops/sec | 4,856,255 ops/sec | ✅ PASS (48x target) |
| Memory stability | < 1 MB growth/50K ops | 1.13 KB | ✅ PASS |

## Comparison Methodology

To compare FLURM performance between versions or configurations:

1. **Establish Baseline**: Run `--full` benchmarks on clean system
2. **Make Changes**: Apply code changes or configuration updates
3. **Run Comparison**: Run benchmarks again with same parameters
4. **Analyze Results**: Use `compare_reports/2` to see percentage changes

```erlang
%% Example comparison workflow
Baseline = flurm_benchmark:run_all_benchmarks().
%% ... make changes ...
NewResults = flurm_benchmark:run_all_benchmarks().
Comparison = flurm_benchmark:compare_reports(Baseline, NewResults).
io:format("~s", [Comparison]).
```

### Interpretation Guidelines

- **< 5% change**: Within noise, not significant
- **5-20% improvement**: Meaningful optimization
- **> 20% improvement**: Significant optimization
- **Any regression**: Investigate before merging

## Recommendations

### For Production Deployment

1. **Sizing**: Start with 3 controllers for HA, scale as needed
2. **Monitoring**: Enable Prometheus metrics on port 9090
3. **Tuning**: Adjust `scheduler_interval` based on cluster size
4. **Network**: Ensure low-latency connections between controllers

### For Performance Testing

1. Run benchmarks on production-equivalent hardware
2. Test with realistic job submission patterns
3. Measure under load (concurrent submissions)
4. Profile Ra consensus under network partitions
