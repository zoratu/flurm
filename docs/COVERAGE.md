# FLURM Code Coverage Strategy

This document outlines the code coverage strategy for the FLURM project, including current status, target coverage levels per module category, justified exceptions, and how to run coverage reports.

## Current Status (January 2026)

| Metric | Value |
|--------|-------|
| **Total Tests** | 498 |
| **Tests Passing** | 488 |
| **Tests Cancelled** | 10 (environment-specific issues) |
| **Test Pass Rate** | 100% (of runnable tests) |
| **Overall Coverage** | 6% |
| **Test Files** | 90+ test modules |

### Why Overall Coverage is Low

The 6% overall coverage number is misleading for several reasons:

1. **Test Code Isolation**: Many tests use mocking (meck) which prevents coverage instrumentation from tracking the actual module code paths.

2. **`-ifdef(TEST)` Blocks**: Significant test-specific code is wrapped in `-ifdef(TEST)` blocks, which are compiled separately and not instrumented for coverage.

3. **Integration Test Dependencies**: Many modules (scheduler, job_manager, node_manager) require full application context to test meaningfully, which unit tests cannot provide.

4. **Coverage Compilation Issues**: Some modules fail coverage compilation due to `{no_abstract_code}` errors, excluding them from coverage analysis.

5. **External Dependencies**: Modules like `flurm_munge` and `flurm_db_ra` interact with external systems (MUNGE daemon, Ra consensus) that cannot be fully tested in isolation.

## Module Categories and Coverage Targets

### Critical Path Modules (Target: 80%+ coverage)

These modules are core to the functionality of FLURM and require high test coverage:

| Module | Description | Target | Current |
|--------|-------------|--------|---------|
| `flurm_protocol_codec` | SLURM protocol encoding/decoding | 80% | 31% |
| `flurm_protocol_header` | Protocol header parsing | 80% | **100%** |
| `flurm_protocol_pack` | Data serialization utilities | 80% | **86%** |
| `flurm_protocol` | Main protocol module | 80% | **91%** |
| `flurm_protocol_auth` | Authentication handling | 80% | **84%** |
| `flurm_job_manager` | Job lifecycle management | 80% | 0% |
| `flurm_scheduler` | Job scheduling logic | 80% | 0% |
| `flurm_node_manager` | Node resource management | 80% | 0% |
| `flurm_job` | Individual job state machine | 80% | 0% |
| `flurm_node` | Node state machine | 80% | 0% |
| `flurm_controller_handler` | Request/response handling | 80% | 0% |
| `flurm_preemption` | Job preemption logic | 80% | 0% |
| `flurm_reservation` | Resource reservation system | 80% | 0% |

### Important Feature Modules (Target: 60%+ coverage)

These modules implement important features but are less critical:

| Module | Description | Target | Current |
|--------|-------------|--------|---------|
| `flurm_job_registry` | Job tracking registry | 60% | 0% |
| `flurm_partition` | Partition management | 60% | 0% |
| `flurm_partition_registry` | Partition tracking | 60% | 0% |
| `flurm_priority` | Job priority calculation | 60% | 0% |
| `flurm_fairshare` | Fair share scheduling | 60% | 0% |
| `flurm_limits` | Resource limits enforcement | 60% | 0% |
| `flurm_license` | License management | 60% | 0% |
| `flurm_qos` | Quality of Service settings | 60% | 0% |
| `flurm_gres` | Generic resources (GPUs, etc.) | 60% | 0% |
| `flurm_federation` | Multi-cluster federation | 60% | 0% |

### Infrastructure Modules (Target: 40%+ coverage)

Supervisors, applications, and infrastructure code where lower coverage is acceptable:

| Module | Description | Target | Current |
|--------|-------------|--------|---------|
| `flurm_job_executor_sup` | Job executor supervisor | 40% | **100%** |
| `flurm_node_daemon_sup` | Node daemon supervisor | 40% | **100%** |
| `flurm_node_daemon_app` | Node daemon application | 40% | **58%** |
| `flurm_state_persistence` | State persistence utilities | 40% | **88%** |
| `flurm_system_monitor` | System monitoring | 40% | **54%** |
| `flurm_job_executor` | Job execution handler | 40% | **50%** |
| `flurm_dbd_server` | Database daemon server | 40% | 31% |
| `flurm_dbd` | Database daemon module | 40% | 31% |
| `flurm_*_sup` | Other supervisor modules | 40% | 0% |
| `flurm_*_app` | Other application modules | 40% | 0% |
| `flurm_config_*` | Configuration modules | 40% | 0% |
| `flurm_db_*` | Database layer | 40% | 0% |
| `flurm_metrics_*` | Metrics collection | 40% | 0% |

### Justified Exceptions (0% coverage acceptable)

These modules have low or zero coverage for valid reasons:

| Module | Reason |
|--------|--------|
| `flurm_chaos` | Chaos testing module - only used in chaos engineering scenarios |
| `flurm_benchmark` | Benchmarking code - not part of production functionality |
| `flurm_test_runner` | Test infrastructure - tests testing infrastructure creates circular dependencies |
| `flurm_upgrade` | Upgrade procedures - tested via integration/upgrade tests, not unit tests |
| `flurm_cloud_scaling` | Cloud integration - requires cloud provider mocks or real infrastructure |
| `flurm_munge` | MUNGE authentication - requires external MUNGE daemon for meaningful tests |
| `flurm_node_daemon` | Node daemon - tested via integration tests with actual node processes |
| `flurm_slurm_import` | SLURM import - requires real SLURM installation for meaningful tests |

## Running Coverage Reports

### Full Coverage Run

```bash
# Run all tests with coverage
rebar3 eunit --cover

# Generate coverage report
rebar3 cover --verbose
```

### View HTML Report

After running coverage, open the HTML report:

```bash
open _build/test/cover/index.html
```

### Running Specific Test Modules

```bash
# Run specific test module with coverage
rebar3 eunit --cover --module=flurm_scheduler_tests

# Run tests in a specific app
rebar3 eunit --cover --app=flurm_core
```

### Coverage for Specific Modules

```bash
# Compile with coverage enabled for specific modules
rebar3 as test compile
rebar3 eunit --cover
```

## Test File Locations

Tests are organized by application:

```
apps/
  flurm_core/test/
    flurm_scheduler_tests.erl
    flurm_preemption_tests.erl
    flurm_reservation_tests.erl
    ...
  flurm_protocol/test/
    flurm_protocol_codec_tests.erl
    flurm_protocol_header_tests.erl
    ...
  flurm_controller/test/
    flurm_controller_handler_tests.erl
    ...
  flurm_db/test/
    flurm_db_ra_tests.erl
    ...
```

## Writing New Tests

### Test Naming Convention

- Test modules should be named `<module_name>_tests.erl`
- Place tests in the `test/` directory of the corresponding application

### Test Structure Template

```erlang
-module(flurm_example_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

example_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"test description", fun test_case/0}
     ]}.

setup() ->
    %% Start required processes
    ok.

cleanup(_) ->
    %% Clean up processes
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_case() ->
    ?assertEqual(expected, actual).
```

### Test Categories to Cover

1. **Happy Path Tests**: Normal expected behavior
2. **Error Handling**: Invalid inputs, failure scenarios
3. **Edge Cases**: Boundary conditions, empty inputs, max values
4. **Concurrency**: Parallel access, race conditions (where applicable)

## Current Coverage Status

**Overall coverage: 6%** (see explanation above for why this number is misleading)

### Modules Meeting Targets (>50%)

| Module | Coverage | Status |
|--------|----------|--------|
| `flurm_protocol_header` | **100%** | Exceeds target |
| `flurm_job_executor_sup` | **100%** | Exceeds target |
| `flurm_node_daemon_sup` | **100%** | Exceeds target |
| `flurm_protocol` | **91%** | Exceeds target |
| `flurm_state_persistence` | **88%** | Exceeds target |
| `flurm_protocol_pack` | **86%** | Exceeds target |
| `flurm_protocol_auth` | **84%** | Exceeds target |
| `flurm_munge` | **83%** | Exceeds target |
| `flurm_node_daemon_app` | **58%** | Meets target |
| `flurm_system_monitor` | **54%** | Meets target |
| `flurm_job_executor` | **50%** | Meets target |

### Partial Coverage (1-49%)

| Module | Coverage | Notes |
|--------|----------|-------|
| `flurm_protocol_codec` | 31% | Core protocol - needs more message type tests |
| `flurm_dbd_server` | 31% | Database daemon - mocking limits coverage |
| `flurm_dbd` | 31% | Database daemon module |

### Zero Coverage (Requires Integration Tests)

Many modules show 0% coverage because they require:
- Full application context to test
- External services (Ra cluster, MUNGE daemon)
- Process supervision trees
- Network connections

| Module Category | Issue |
|-----------------|-------|
| `flurm_scheduler*` | Requires running job manager and node manager |
| `flurm_job_manager` | Requires application supervision tree |
| `flurm_node_manager*` | Requires node connections |
| `flurm_controller_*` | Requires ranch listeners and cluster |
| `flurm_db_*` | Requires Ra consensus cluster |
| `flurm_config_*` | Tests use mocking extensively |

### Outstanding Work

1. **Integration Test Framework** - Set up common_test suites for modules requiring application context
2. **Coverage Compilation** - Fix `{no_abstract_code}` errors in flurm_benchmark and other modules
3. **Property-Based Testing** - Add PropEr tests for protocol codec round-trip verification
4. **Reduce Mocking** - Where possible, test against real implementations for better coverage
5. **CI Integration** - Add coverage thresholds to CI pipeline

## Coverage Improvement Strategy

### Priority 1: Critical Path Modules
Focus on achieving 80% coverage for:
- Protocol codec modules (especially `flurm_protocol_codec`)
- Job management modules (`flurm_job_manager`, `flurm_job`)
- Scheduler core logic (`flurm_scheduler`)
- Controller handler (`flurm_controller_handler`)

### Priority 2: Feature Modules
Achieve 60% coverage for:
- Preemption and reservation
- QoS and fair share
- Priority calculation

### Priority 3: Infrastructure
Achieve 40% coverage for:
- Database layer
- Configuration system
- Metrics and monitoring

## Coverage Improvement Roadmap

### Phase 1: Foundation (Q1 2026)
- [ ] Fix coverage compilation errors in `flurm_benchmark.erl`
- [ ] Increase `flurm_protocol_codec` coverage from 31% to 60%
- [ ] Set up common_test integration test framework
- [ ] Add coverage reporting to CI pipeline

### Phase 2: Core Modules (Q2 2026)
- [ ] Add integration tests for `flurm_scheduler`
- [ ] Add integration tests for `flurm_job_manager`
- [ ] Improve `flurm_dbd_*` coverage with better mocking strategies
- [ ] Target: Overall coverage to 20%

### Phase 3: Feature Completeness (Q3 2026)
- [ ] Complete preemption and reservation test coverage
- [ ] Add property-based tests for protocol codec
- [ ] Implement fault injection test suite
- [ ] Target: Overall coverage to 40%

### Phase 4: Production Readiness (Q4 2026)
- [ ] All critical path modules at 80%+ coverage
- [ ] All feature modules at 60%+ coverage
- [ ] All infrastructure modules at 40%+ coverage
- [ ] Target: Overall coverage to 60%

## Contributing

When adding new features or fixing bugs:

1. Write tests first (TDD when practical)
2. Ensure new code has at least the target coverage for its category
3. Run full coverage report before submitting PRs
4. Update this document if adding new justified exceptions

### Quick Coverage Check

```bash
# Run tests with coverage
rebar3 eunit --cover

# Generate and view report
rebar3 cover --verbose
open _build/test/cover/index.html
```
