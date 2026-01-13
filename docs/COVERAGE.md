# FLURM Code Coverage Strategy

This document outlines the code coverage strategy for the FLURM project, including target coverage levels per module category, justified exceptions, and how to run coverage reports.

## Module Categories and Coverage Targets

### Critical Path Modules (Target: 80%+ coverage)

These modules are core to the functionality of FLURM and require high test coverage:

| Module | Description | Target |
|--------|-------------|--------|
| `flurm_protocol_codec` | SLURM protocol encoding/decoding | 80% |
| `flurm_protocol_header` | Protocol header parsing | 80% |
| `flurm_protocol_pack` | Data serialization utilities | 80% |
| `flurm_job_manager` | Job lifecycle management | 80% |
| `flurm_scheduler` | Job scheduling logic | 80% |
| `flurm_node_manager` | Node resource management | 80% |
| `flurm_job` | Individual job state machine | 80% |
| `flurm_node` | Node state machine | 80% |
| `flurm_controller_handler` | Request/response handling | 80% |
| `flurm_preemption` | Job preemption logic | 80% |
| `flurm_reservation` | Resource reservation system | 80% |

### Important Feature Modules (Target: 60%+ coverage)

These modules implement important features but are less critical:

| Module | Description | Target |
|--------|-------------|--------|
| `flurm_job_registry` | Job tracking registry | 60% |
| `flurm_partition` | Partition management | 60% |
| `flurm_partition_registry` | Partition tracking | 60% |
| `flurm_priority` | Job priority calculation | 60% |
| `flurm_fairshare` | Fair share scheduling | 60% |
| `flurm_limits` | Resource limits enforcement | 60% |
| `flurm_license` | License management | 60% |
| `flurm_qos` | Quality of Service settings | 60% |
| `flurm_gres` | Generic resources (GPUs, etc.) | 60% |
| `flurm_federation` | Multi-cluster federation | 60% |

### Infrastructure Modules (Target: 40%+ coverage)

Supervisors, applications, and infrastructure code where lower coverage is acceptable:

| Module | Description | Target |
|--------|-------------|--------|
| `flurm_*_sup` | Supervisor modules | 40% |
| `flurm_*_app` | Application modules | 40% |
| `flurm_config_*` | Configuration modules | 40% |
| `flurm_db_*` | Database layer | 40% |
| `flurm_dbd_*` | Database daemon | 40% |
| `flurm_metrics_*` | Metrics collection | 40% |

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

**Overall coverage: 15%**

### High Coverage Modules (>50%)

| Module | Coverage | Status |
|--------|----------|--------|
| `flurm_job_sup` | 100% | Meets target |
| `flurm_license` | 88% | Meets target |
| `flurm_job_registry` | 84% | Meets target |
| `flurm_protocol_header` | 80% | Meets target |
| `flurm_priority` | 69% | Meets target |
| `flurm_metrics` | 69% | Meets target |
| `flurm_protocol_pack` | 63% | Meets target |
| `flurm_fairshare` | 63% | Meets target |
| `flurm_db_ra_effects` | 63% | Meets target |
| `flurm_db_ra` | 62% | Meets target |
| `flurm_gres` | 57% | Close to target |
| `flurm_rate_limiter` | 54% | Close to target |
| `flurm_partition_registry` | 53% | Close to target |
| `flurm_protocol` | 52% | Close to target |

### Needs Improvement (Critical Path)

| Module | Coverage | Issue |
|--------|----------|-------|
| `flurm_controller_handler` | 0% | Tests written but need process lifecycle fixes |
| `flurm_scheduler` | 0% | Complex dependencies, needs integration test approach |
| `flurm_protocol_codec` | 6% | Core protocol - high priority |
| `flurm_job_manager` | 0% | Requires running application |
| `flurm_node_manager` | 0% | Requires running application |

### Needs Improvement (Features)

| Module | Coverage | Notes |
|--------|----------|-------|
| `flurm_reservation` | 15% | Tests written, coverage improving |
| `flurm_preemption` | 34% | Tests enhanced, coverage improved |
| `flurm_federation` | 36% | Adequate for current needs |
| `flurm_power` | 34% | Lower priority |
| `flurm_db_cluster` | 32% | Needs integration tests |

### Outstanding Work

1. **Fix test compilation issues** - `flurm_controller_handler_tests.erl` has record field name mismatches
2. **Add integration test harness** - Many modules require running application context
3. **Property-based testing** - Add PropEr tests for protocol codec round-trip
4. **Fault injection tests** - Test resilience with chaos engineering

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

## Contributing

When adding new features or fixing bugs:

1. Write tests first (TDD when practical)
2. Ensure new code has at least the target coverage for its category
3. Run full coverage report before submitting PRs
4. Update this document if adding new justified exceptions
