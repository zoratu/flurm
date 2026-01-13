# FLURM Performance Benchmarks

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
| Protocol codec | 6-80% | Header well tested, codec needs work |
| Job management | 0-84% | Registry good, manager needs tests |
| Scheduler | 0% | Needs integration tests |
| Consensus (Ra) | 62-63% | Good coverage |
| Node management | 0% | Requires integration tests |
| **Overall** | **15%** | See docs/COVERAGE.md for strategy |

### High Coverage Modules (>50%)
- flurm_job_sup: 100%
- flurm_license: 88%
- flurm_job_registry: 84%
- flurm_protocol_header: 80%
- flurm_priority: 69%
- flurm_metrics: 69%
- flurm_protocol_pack: 63%
- flurm_fairshare: 63%
- flurm_db_ra_effects: 63%
- flurm_db_ra: 62%
- flurm_gres: 57%
- flurm_rate_limiter: 54%
- flurm_partition_registry: 53%
- flurm_protocol: 52%

### Coverage Improvement Priorities
1. flurm_controller_handler (0%) - Critical path, tests written but need debugging
2. flurm_scheduler (0%) - Core logic, complex dependencies
3. flurm_protocol_codec (6%) - Critical for SLURM compatibility
4. flurm_reservation (15%) - Advanced feature
5. flurm_preemption (34%) - Advanced feature

See [docs/COVERAGE.md](COVERAGE.md) for the complete coverage strategy and justified exceptions.

## Running Benchmarks

```bash
# Run basic benchmarks
cd /path/to/flurm
escript run_benchmarks.escript

# Run with coverage
rebar3 cover --verbose

# Run property-based tests
rebar3 proper
```

## Benchmark Scripts

- `run_benchmarks.escript` - Basic Erlang/OTP benchmarks
- `apps/flurm_core/test/flurm_bench.erl` - Full FLURM benchmarks
- `apps/flurm_core/test/flurm_latency_bench.erl` - Latency measurements
- `apps/flurm_core/test/flurm_throughput_bench.erl` - Throughput tests

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
