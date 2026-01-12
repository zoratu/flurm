# FLURM Testing Guide

This guide covers testing FLURM at various levels.

## Unit Tests

Run unit tests with:

```bash
rebar3 eunit
```

## Common Tests

Run integration tests:

```bash
rebar3 ct
```

## Property-Based Testing

FLURM uses PropEr for property-based testing:

```bash
rebar3 proper
```

## Performance Benchmarks

### Running Benchmarks

```erlang
%% Start an Erlang shell
rebar3 shell

%% Run all benchmarks
flurm_bench:run_all().

%% Run specific benchmarks
flurm_bench:bench_protocol(10000).
flurm_bench:bench_scheduler(1000).
flurm_bench:bench_job_lifecycle(500).
```

### Throughput Testing

```erlang
%% Run throughput benchmarks
flurm_throughput_bench:run().

%% Test specific operations
flurm_throughput_bench:submission_throughput(10000).
flurm_throughput_bench:scheduler_throughput(5000).
flurm_throughput_bench:protocol_throughput(50000).
```

### Latency Testing

```erlang
%% Run latency benchmarks
flurm_latency_bench:run().

%% Measure specific latencies
flurm_latency_bench:measure_submission_latency(5000).
flurm_latency_bench:measure_query_latency(5000).
```

## Protocol Fuzzing

Test protocol robustness:

```erlang
%% Run comprehensive fuzzing
fuzz_protocol:run_all().

%% Run specific fuzz tests
fuzz_protocol:run(1000).          % Random fuzzing
fuzz_protocol:run_mutation(500).  % Mutation-based fuzzing
```

### AFL Integration

For external fuzzing with AFL:

```bash
# Generate corpus from captured traffic
tcpdump -i any port 6817 -w slurm_capture.pcap

# Convert to fuzzing corpus
./scripts/pcap_to_corpus.sh slurm_capture.pcap corpus/

# Run AFL
afl-fuzz -i corpus -o findings -- \
    escript fuzz_target.escript @@
```

## Deterministic Simulation

FLURM includes a FoundationDB-style deterministic simulation framework:

```erlang
%% Run single scenario
flurm_sim:run(single_job_lifecycle, 300000).

%% Run with specific seed (reproducible)
flurm_sim:run_with_seed(leader_crash_during_submit, 12345, 300000).

%% Run all scenarios with multiple seeds
flurm_sim:run_all_scenarios(100).  % 100 seeds per scenario
```

### Available Scenarios

| Scenario | Description |
|----------|-------------|
| `single_job_lifecycle` | Basic job submission and completion |
| `hundred_jobs_fifo` | FIFO scheduling of 100 jobs |
| `job_cancellation` | Random job cancellation |
| `leader_crash_during_submit` | Controller failure during submission |
| `node_failure_during_job` | Node crash with running jobs |
| `cascading_node_failures` | Multiple sequential node failures |
| `concurrent_submit_cancel` | Race between submit and cancel |
| `rapid_job_submission` | High-rate job submission |
| `many_nodes_few_jobs` | Large cluster, few jobs |
| `few_nodes_many_jobs` | Small cluster, many jobs |
| `balanced_load` | Balanced workload |

### Reproducing Failures

When a simulation fails, note the seed:

```
Scenario leader_crash_during_submit FAILED with seed 12345: double_allocation
```

Reproduce with:

```erlang
flurm_sim:run_with_seed(leader_crash_during_submit, 12345, 300000).
```

## Cluster Integration Tests

Run multi-node cluster tests:

```erlang
%% Run all cluster tests
flurm_cluster_tests:run_all().

%% Run specific tests
flurm_cluster_tests:test_controller_failover().
flurm_cluster_tests:test_job_distribution().
flurm_cluster_tests:test_node_failure_recovery().
```

## SLURM Protocol Compatibility

Test compatibility with real SLURM clients:

```bash
# Start FLURM controller
rebar3 shell

# In another terminal, use SLURM commands
export SLURM_CONF=/path/to/flurm/slurm.conf
export SLURMCTLD_HOST=localhost

# Test basic operations
sbatch --wrap="echo hello"
squeue
sinfo
scancel 1
```

## Chaos Testing

Enable runtime chaos injection (development only):

```erlang
%% Enable chaos mode
flurm_chaos:enable().

%% Run workload while chaos is active
%% Random process kills, message delays, etc.

%% Disable chaos
flurm_chaos:disable().
```

## Coverage Reports

Generate test coverage:

```bash
rebar3 cover

# View HTML report
open _build/test/cover/index.html
```

## Continuous Integration

Example GitHub Actions workflow:

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - uses: erlef/setup-beam@v1
      with:
        otp-version: '26.0'
        rebar3-version: '3.20'
    
    - name: Compile
      run: rebar3 compile
    
    - name: Unit Tests
      run: rebar3 eunit
    
    - name: Property Tests
      run: rebar3 proper -n 1000
    
    - name: Dialyzer
      run: rebar3 dialyzer
    
    - name: Coverage
      run: rebar3 cover
```

## Test Best Practices

1. **Always test with multiple seeds** - Use at least 100 seeds for simulation tests
2. **Reproduce failures first** - Before fixing, ensure you can reproduce the bug
3. **Test under load** - Use throughput benchmarks to find race conditions
4. **Monitor resource usage** - Watch for memory leaks and process accumulation
5. **Test upgrades** - Verify hot code reload doesn't break state
