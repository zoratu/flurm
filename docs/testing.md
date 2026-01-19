# FLURM Testing Guide

This comprehensive guide covers all aspects of testing FLURM, from running basic unit tests to writing new test cases and understanding the test architecture.

## Table of Contents

1. [Current Status](#current-status)
2. [Quick Start](#quick-start)
3. [Test Types](#test-types)
4. [Test Architecture](#test-architecture)
5. [Writing New Tests](#writing-new-tests)
6. [Debugging Tests](#debugging-tests)

---

## Current Status

| Metric | Value |
|--------|-------|
| **Total Tests** | 498 |
| **Tests Passing** | 488 |
| **Tests Cancelled** | 10 (environment-specific) |
| **Test Pass Rate** | 100% (of runnable tests) |
| **Coverage** | 6% overall (see [COVERAGE.md](COVERAGE.md) for details) |

---

## Quick Start

```bash
# Run all unit tests
rebar3 eunit

# Run tests with coverage
rebar3 eunit --cover && rebar3 cover --verbose

# Run tests for a specific app
rebar3 eunit --app=flurm_protocol

# Run a specific test module
rebar3 eunit --module=flurm_protocol_codec_tests

# View coverage report
open _build/test/cover/index.html
```

---

## Test Types

### Unit Tests (EUnit)

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

---

## Test Architecture

### Directory Structure

Tests are organized by application:

```
apps/
├── flurm_core/
│   └── test/
│       ├── flurm_scheduler_tests.erl
│       ├── flurm_job_tests.erl
│       ├── flurm_preemption_tests.erl
│       └── ...
├── flurm_protocol/
│   └── test/
│       ├── flurm_protocol_codec_tests.erl
│       ├── flurm_protocol_header_tests.erl
│       └── ...
├── flurm_controller/
│   └── test/
│       ├── flurm_controller_handler_tests.erl
│       └── ...
├── flurm_db/
│   └── test/
│       ├── flurm_db_ra_tests.erl
│       └── ...
└── flurm_dbd/
    └── test/
        ├── flurm_dbd_server_tests.erl
        └── ...
```

### Test Module Dependencies

Some test modules require specific setup:

| Module Category | Dependencies |
|-----------------|--------------|
| Protocol tests | None (standalone) |
| Config tests | May mock file system |
| DB tests | Require Ra/Mnesia |
| Controller tests | Require protocol, config, DB |
| Integration tests | Full application context |

---

## Writing New Tests

### Test Module Template

```erlang
-module(flurm_newfeature_tests).

%% @doc Unit tests for flurm_newfeature module.

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

basic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"can create item", fun test_create/0},
         {"can retrieve item", fun test_get/0}
     ]}.

error_handling_test_() ->
    [
        {"returns error for invalid input", fun test_invalid_input/0},
        {"handles missing item", fun test_not_found/0}
    ].

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Initialize test state
    ok.

cleanup(_) ->
    %% Clean up
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_create() ->
    Result = flurm_newfeature:create(#{name => <<"test">>}),
    ?assertMatch({ok, _}, Result).

test_get() ->
    {ok, Id} = flurm_newfeature:create(#{name => <<"test">>}),
    ?assertEqual({ok, #{name => <<"test">>}}, flurm_newfeature:get(Id)).

test_invalid_input() ->
    ?assertEqual({error, invalid_input}, flurm_newfeature:create(invalid)).

test_not_found() ->
    ?assertEqual({error, not_found}, flurm_newfeature:get(<<"nonexistent">>)).
```

### Using Meck for Mocking

```erlang
meck_test_() ->
    {setup,
     fun() ->
         meck:new(some_module, [passthrough]),
         meck:expect(some_module, some_function, fun(_) -> mocked_result end)
     end,
     fun(_) ->
         meck:unload(some_module)
     end,
     fun(_) ->
         [
             ?_assertEqual(mocked_result, some_module:some_function(arg))
         ]
     end}.
```

### Test Categories to Cover

Every module should have tests for:

1. **Happy Path** - Normal expected behavior
2. **Error Handling** - Invalid inputs, failure scenarios
3. **Edge Cases** - Boundary conditions, empty inputs, max values
4. **Concurrency** - Race conditions (where applicable)

---

## Debugging Tests

### Verbose Output

```bash
# See detailed test results
rebar3 eunit --verbose

# Run specific module with verbose output
rebar3 eunit --module=flurm_problem_tests --verbose
```

### Interactive Debugging

```erlang
%% Start test shell
rebar3 as test shell

%% Run specific test interactively
1> eunit:test(flurm_scheduler_tests, [verbose]).

%% Debug with observer
2> observer:start().
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `{no_abstract_code}` | Missing debug_info | Recompile: `rebar3 as test compile` |
| Meck errors | Module not unloaded | Add `meck:unload()` to cleanup |
| Timeout | Slow test | Use `{timeout, 60, fun test/0}` |
| Process leak | Processes not stopped | Stop processes in cleanup |

---

## Related Documentation

- [Code Coverage Strategy](COVERAGE.md) - Coverage targets and roadmap
- [Development Guide](development.md) - Contributing guidelines
- [Benchmarks](BENCHMARKS.md) - Performance test results
