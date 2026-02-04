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
| **EUnit Tests** | 2300+ |
| **Integration Tests (CT)** | 52 (50 pass, 2 expected skips) |
| **Test Pass Rate** | 100% (of runnable tests) |
| **Coverage** | 6% overall (see [COVERAGE.md](COVERAGE.md) for details) |

### Integration Test Suites

| Suite | Tests | Status |
|-------|-------|--------|
| Scheduler SUITE | 10 | All pass |
| Job Manager SUITE | 11 | 9 pass, 2 skip (array jobs) |
| Node Manager SUITE | 12 | All pass |
| Protocol SUITE | 10 | All pass |
| HA/Failover SUITE | 9 | All pass |

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

## Common Tests (Integration Tests)

FLURM includes comprehensive Common Test (CT) suites for integration testing:

### Running Integration Tests

```bash
# Run all integration tests
rebar3 ct

# Run a specific test suite
rebar3 ct --dir=apps/flurm_core/integration_test --suite=flurm_scheduler_SUITE
rebar3 ct --dir=apps/flurm_controller/integration_test --suite=flurm_job_manager_SUITE
rebar3 ct --dir=apps/flurm_node_daemon/integration_test --suite=flurm_node_manager_SUITE
rebar3 ct --dir=apps/flurm_protocol/integration_test --suite=flurm_protocol_SUITE
rebar3 ct --dir=apps/flurm_controller/integration_test --suite=flurm_ha_SUITE
```

### Available Test Suites

| Suite | Location | Tests | Description |
|-------|----------|-------|-------------|
| `flurm_scheduler_SUITE` | `apps/flurm_core/integration_test/` | 10 | Job scheduling, backfill, preemption |
| `flurm_job_manager_SUITE` | `apps/flurm_controller/integration_test/` | 11 | Job lifecycle, cancellation, dependencies |
| `flurm_node_manager_SUITE` | `apps/flurm_node_daemon/integration_test/` | 12 | Node registration, heartbeat, allocation |
| `flurm_protocol_SUITE` | `apps/flurm_protocol/integration_test/` | 10 | Protocol encoding, handler integration |
| `flurm_ha_SUITE` | `apps/flurm_controller/integration_test/` | 9 | HA failover, state recovery |

### Test Suite Details

#### Scheduler SUITE (`flurm_scheduler_SUITE`)
- Basic job submission and scheduling
- Backfill scheduling algorithm
- Preemption workflow (high-priority preempts low-priority)
- Resource allocation (CPU/memory)
- Partition constraints
- Fair-share enforcement

#### Job Manager SUITE (`flurm_job_manager_SUITE`)
- Job lifecycle: PENDING → RUNNING → COMPLETED
- Job cancellation at various states
- Job dependencies (afterok, afterany)
- Array job expansion (when flurm_job_array is running)
- Job requeue on failure

#### Node Manager SUITE (`flurm_node_manager_SUITE`)
- Node registration with controller
- Heartbeat updates timestamp
- Resource allocation/release
- Drain/undrain operations
- Dynamic node add/remove

#### Protocol SUITE (`flurm_protocol_SUITE`)
- Header encode/decode
- Ping request/response
- Job info request/response
- Node/partition info
- Build info
- Full batch job submission cycle
- Job cancellation protocol

#### HA/Failover SUITE (`flurm_ha_SUITE`)
- Controller startup verification
- Failover handler initialization
- Leadership status queries
- Job persistence across simulated failover
- State recovery verification
- Multiple failover transitions (robustness)

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

Test protocol robustness using both manual fuzzing and property-based testing with PropEr.

### Fuzzing Approach and Goals

FLURM protocol fuzzing is designed to ensure **crash resistance** - the protocol codec should never crash on malformed input, always returning proper error tuples instead.

**Goals:**
1. **No crashes on arbitrary input** - Any binary input should result in `{ok, Msg, Rest}` or `{error, Reason}`, never an unhandled exception
2. **Header parsing robustness** - Corrupted headers should be rejected gracefully
3. **Length field handling** - Manipulated length fields should not cause buffer overflows or crashes
4. **Message type safety** - Unknown/invalid message types should be handled
5. **Roundtrip integrity** - Encoded messages should decode correctly

**Fuzzing Strategies:**
- **Random byte fuzzing** - Test decoder against completely random binary data
- **Mutation fuzzing** - Start with valid messages and apply various mutations (bit flips, truncation, insertion, etc.)
- **Boundary testing** - Test edge cases for numeric fields (0, max values, SLURM_NO_VAL)
- **Header corruption** - Specifically target header parsing with invalid versions, flags, lengths
- **TRES string parsing** - Fuzz trackable resource specification strings

### Running PropEr Fuzz Tests

```bash
# Run all fuzzing properties with default iterations (100)
rebar3 proper -m flurm_protocol_fuzz_tests

# Run with more iterations for thorough testing (recommended: 1000+)
rebar3 proper -m flurm_protocol_fuzz_tests -n 1000

# Run specific property with high iteration count
rebar3 proper -m flurm_protocol_fuzz_tests -n 10000 -p prop_random_bytes_no_crash

# Run mutation fuzzing
rebar3 proper -m flurm_protocol_fuzz_tests -n 5000 -p prop_decode_survives_mutation

# Quick smoke test (for CI)
rebar3 eunit --module=flurm_protocol_fuzz_tests --test=smoke_test_
```

### Available PropEr Properties

| Property | Description | Recommended Iterations |
|----------|-------------|------------------------|
| `prop_random_bytes_no_crash` | Random binary input never crashes decoder | 10000+ |
| `prop_decode_survives_mutation` | Mutated valid messages don't crash | 5000+ |
| `prop_header_corruption_no_crash` | Corrupted headers handled gracefully | 5000+ |
| `prop_length_field_manipulation` | Length field changes don't crash | 5000+ |
| `prop_message_type_fuzzing` | Arbitrary message types don't crash | 5000+ |
| `prop_roundtrip_preserves_data` | Encode/decode roundtrip works | 1000+ |
| `prop_tres_string_parsing` | TRES string parsing is robust | 2000+ |
| `prop_job_array_boundaries` | Array size limits are enforced | 2000+ |

### Interpreting Results

**Success output:**
```
OK: Passed 10000 test(s).
```

**Failure output with shrinking:**
```
Failed! After 42 test(s).
{<<0,0,0,5,38,0,0,0,...>>, {bit_flip, 7}}
Shrinking .....(5 time(s))
{<<0,0,0,5,38,0,0,0>>, {bit_flip, 0}}
```

When a property fails:
1. PropEr will **shrink** the failing input to the minimal reproducing case
2. Note the **seed** shown in the output for reproducibility
3. The shrunk input shows the **exact mutation** that caused the failure
4. Use this to create a regression test

**Reproducing failures:**
```erlang
%% In Erlang shell
proper:check(flurm_protocol_fuzz_tests:prop_random_bytes_no_crash(),
    [{seed, {1234, 5678, 9012}}]).
```

### Adding New Fuzz Targets

To add a new fuzzing property:

```erlang
%% 1. Define the property
prop_new_feature_no_crash() ->
    ?FORALL(Input, your_generator(),
        begin
            Result = try
                your_module:function(Input)
            catch
                error:badarg -> {crash, badarg};
                error:function_clause -> {crash, function_clause};
                _:_ -> ok
            end,
            case Result of
                {crash, _} -> false;  % Bugs we want to find
                _ -> true
            end
        end).

%% 2. Add an EUnit wrapper for CI
fuzz_new_feature_test_() ->
    {timeout, 120, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_new_feature_no_crash(),
            [{numtests, 5000}, {to_file, user}]))
    end}.
```

### Manual Fuzzing

Test protocol robustness manually:

```erlang
%% Run comprehensive fuzzing
fuzz_protocol:run_all().

%% Run specific fuzz tests
fuzz_protocol:run(1000).          % Random fuzzing
fuzz_protocol:run_mutation(500).  % Mutation-based fuzzing

%% Use flurm_protocol_fuzz module
flurm_protocol_fuzz:run_random_fuzz(1000).
flurm_protocol_fuzz:run_mutation_fuzz(SeedMessage, 1000).
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

### Fuzzing Best Practices

1. **Run with high iteration counts** - 10000+ iterations for thorough coverage
2. **Test in CI** - Include smoke tests (100 iterations) in CI pipeline
3. **Save failing seeds** - When failures occur, save the seed for regression testing
4. **Focus on crash bugs** - The goal is crash resistance, not semantic correctness
5. **Add regression tests** - Convert discovered bugs into unit tests
6. **Monitor memory** - Watch for memory leaks during long fuzzing runs

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

## Docker-Based Integration Tests

Run Common Test suites against a Docker-based FLURM cluster:

```bash
# Run all CT suites with Docker
./test/run_integration_tests.sh

# Run a specific test suite
./test/run_integration_tests.sh --suite=flurm_ha_SUITE

# Keep cluster running after tests (for debugging)
./test/run_integration_tests.sh --keep

# Force rebuild of Docker images
./test/run_integration_tests.sh --rebuild
```

The Docker test environment includes:
- FLURM controller with health checks
- Two compute nodes for job execution
- SLURM client for protocol testing

### Docker Compose Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Basic single-cluster setup |
| `docker-compose.integration-test.yml` | CT suite execution |
| `docker-compose.federation.yml` | Multi-cluster federation testing |
| `docker-compose.slurm-interop.yml` | Real SLURM interoperability testing |

## SLURM Interoperability Testing (Phase 8B)

Test FLURM against real SLURM components using Docker containers with shared MUNGE authentication.

### Quick Start

```bash
# Run all SLURM interop tests
./scripts/run-slurm-interop-tests.sh

# Run with specific test group
./scripts/run-slurm-interop-tests.sh --suite=slurm_commands

# Keep containers running for debugging
./scripts/run-slurm-interop-tests.sh --keep

# Start interactive shell in test container
./scripts/run-slurm-interop-tests.sh --shell

# Force rebuild of images
./scripts/run-slurm-interop-tests.sh --rebuild
```

### Test Environment

The SLURM interop test environment includes:

| Service | Description |
|---------|-------------|
| `slurm-controller` | Real SLURM slurmctld daemon |
| `slurm-node` | Real SLURM slurmd compute daemon |
| `flurm-controller` | FLURM controller in bridge mode |
| `slurm-client-flurm` | SLURM client configured for FLURM |
| `slurm-client-real` | SLURM client configured for real SLURM |
| `munge-init` | Shared MUNGE key generator |
| `test-runner` | Common Test suite executor |

### Test Suite Groups

The `flurm_slurm_interop_SUITE` contains three test groups:

#### 1. SLURM Commands (`slurm_commands`)

Tests real SLURM client commands against FLURM:

| Test | Description |
|------|-------------|
| `sbatch_to_flurm_test` | Submit batch job via sbatch |
| `squeue_against_flurm_test` | Query jobs via squeue |
| `scancel_test` | Cancel job via scancel |
| `sinfo_test` | Query cluster info via sinfo |
| `srun_interactive_test` | Run interactive command via srun |

#### 2. Bridge Operations (`bridge_operations`)

Tests FLURM bridge functionality:

| Test | Description |
|------|-------------|
| `bridge_forward_to_slurm_test` | Forward job from FLURM to real SLURM |
| `munge_credential_validation_test` | Verify MUNGE auth between services |

#### 3. Protocol Compatibility (`protocol_compatibility`)

Tests low-level protocol compatibility:

| Test | Description |
|------|-------------|
| `protocol_ping_test` | SLURM ping request/response |
| `protocol_job_info_test` | Job info request/response |
| `protocol_node_info_test` | Node info request/response |
| `protocol_partition_info_test` | Partition info request/response |

### Running Individual Tests

```bash
# Run only SLURM command tests
./scripts/run-slurm-interop-tests.sh --suite=slurm_commands

# Run only bridge tests
./scripts/run-slurm-interop-tests.sh --suite=bridge_operations

# Run only protocol tests
./scripts/run-slurm-interop-tests.sh --suite=protocol_compatibility
```

### Manual Testing

Start the environment and interact manually:

```bash
# Start services in background
cd docker
docker compose -f docker-compose.slurm-interop.yml up -d

# Submit job to FLURM
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-flurm \
    sbatch --wrap="echo hello from flurm"

# Check queue on FLURM
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-flurm \
    squeue

# Submit job to real SLURM
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-real \
    sbatch --wrap="echo hello from slurm"

# Check queue on real SLURM
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-real \
    squeue

# Stop all services
docker compose -f docker-compose.slurm-interop.yml down -v
```

### Troubleshooting

**Services not starting:**
```bash
# Check container logs
docker compose -f docker-compose.slurm-interop.yml logs slurm-controller
docker compose -f docker-compose.slurm-interop.yml logs flurm-controller
```

**MUNGE errors:**
```bash
# Verify MUNGE key exists
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-flurm \
    ls -la /etc/munge/

# Test MUNGE
docker compose -f docker-compose.slurm-interop.yml exec slurm-client-flurm \
    munge -n | unmunge
```

**Protocol errors:**
```bash
# Enable verbose logging on FLURM
export FLURM_LOG_LEVEL=debug

# Check FLURM controller logs
docker compose -f docker-compose.slurm-interop.yml logs -f flurm-controller
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
