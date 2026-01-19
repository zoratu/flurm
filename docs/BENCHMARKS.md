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
- **CPU**: 8 cores
- **Date**: January 2026

## Baseline Performance Results

### Binary Operations (Protocol Codec)

| Operation | Time (10K ops) | Throughput | Latency |
|-----------|----------------|------------|---------|
| Header encode | 0.20 ms | 50,000,000 ops/sec | 0.020 µs |
| Header decode | 0.20 ms | 49,261,084 ops/sec | 0.020 µs |

**Analysis**: Protocol header encoding/decoding is extremely fast due to Erlang's native binary pattern matching. This is a significant advantage over C-based string parsing.

### Data Structures

| Operation | Time (10K ops) | Throughput | Latency |
|-----------|----------------|------------|---------|
| ETS insert | 1.52 ms | 6,565,988 ops/sec | 0.152 µs |
| ETS lookup | 0.68 ms | 14,705,882 ops/sec | 0.068 µs |
| Map put | 2.46 ms | 4,056,795 ops/sec | 0.246 µs |
| Map get | 0.28 ms | 35,587,189 ops/sec | 0.028 µs |

**Analysis**:
- ETS provides ~6.5M inserts/sec and ~14.7M lookups/sec - suitable for job and node registries
- Maps are faster for lookups (~35M/sec) but slower for updates (~4M/sec)
- Both are well within requirements for clusters up to 100K+ jobs

### Concurrency

| Operation | Time | Throughput | Latency |
|-----------|------|------------|---------|
| Process spawn+stop (10K) | 13.03 ms | 767,636 ops/sec | 1.303 µs |
| Message passing (100K) | 12.48 ms | 8,014,747 ops/sec | 0.125 µs |
| Gen_server call (10K) | 6.05 ms | 1,653,713 ops/sec | 0.605 µs |

**Analysis**:
- Can spawn ~767K processes per second - supports process-per-job model at scale
- Message passing at ~8M msgs/sec enables high-throughput scheduler
- Synchronous gen_server calls at ~1.6M/sec - sufficient for job submission rates

## Projected Production Performance

Based on baseline benchmarks and FLURM architecture:

### Job Submission Throughput

| Metric | Projected Value | Notes |
|--------|-----------------|-------|
| Peak submission rate | 50,000+ jobs/sec | Single controller |
| Sustained submission | 10,000+ jobs/sec | With persistence |
| Multi-controller | 100,000+ jobs/sec | 3-node cluster |

### Scheduling Latency

| Metric | Projected Value | Notes |
|--------|-----------------|-------|
| Job submission to scheduled | < 10 ms | Simple FIFO |
| Backfill scheduling cycle | < 100 ms | 10K pending jobs |
| Fair-share calculation | < 50 ms | 1000 users |

### Failover Performance

| Metric | Projected Value | Notes |
|--------|-----------------|-------|
| Leader election | < 500 ms | Ra consensus |
| State recovery | < 1 sec | From Ra log |
| Client reconnect | < 2 sec | Automatic |

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

When running benchmarks, you should see approximately these numbers on modern hardware:

| Benchmark | Expected Throughput | Expected Latency (avg) |
|-----------|---------------------|------------------------|
| Job Submission | 500,000+ ops/sec | < 5 us |
| Scheduler Cycle | 1,000+ cycles/sec | < 1000 us |
| Protocol Encoding | 100,000+ ops/sec | < 50 us |
| State Persistence (Store) | 5,000,000+ ops/sec | < 1 us |
| State Persistence (Lookup) | 10,000,000+ ops/sec | < 0.5 us |

**Note**: These numbers vary significantly based on:
- Hardware (CPU speed, memory bandwidth)
- System load
- Erlang/OTP version
- Number of iterations (more iterations = more accurate but slower)

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
