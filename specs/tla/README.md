# FLURM TLA+ Specifications

This directory contains TLA+ formal specifications for FLURM's core algorithms.

## Specifications

### FlurmScheduler.tla

Models the job scheduling algorithm and proves:
- **NoOverallocationCPUs**: Never allocate more CPUs than a node has
- **NoOverallocationMemory**: Never allocate more memory than a node has  
- **RunningJobsHaveNodes**: All running jobs have allocated resources
- **AllJobsTerminate**: Under fairness, all jobs eventually complete

### FlurmJobLifecycle.tla

Models the job state machine and proves:
- **ValidJobTransitions**: All state transitions follow defined rules
- **TerminalStatesStable**: Completed/failed/cancelled jobs don't change
- **StepStateConsistentWithJob**: Step states are consistent with job state
- **RunningJobsTerminate**: Running jobs eventually reach terminal state

### FlurmFailover.tla

Models multi-controller consensus and proves:
- **AtMostOneLeaderPerTerm**: Only one leader per Raft term
- **CommittedEntriesDurable**: Committed log entries are never lost
- **EventuallyLeader**: If quorum alive, leader is eventually elected

## Running the Model Checker

### Prerequisites

1. Install TLA+ Toolbox: https://lamport.azurewebsites.net/tla/toolbox.html
2. Or use command-line TLC

### Using TLA+ Toolbox

1. Open TLA+ Toolbox
2. File → Open Spec → Add New Spec
3. Select one of the `.tla` files
4. Create a new model (Model → New Model)
5. Configure constants:

```
For FlurmScheduler:
  Nodes <- {n1, n2, n3}
  MaxJobs <- 5
  MaxCPUsPerNode <- 8
  MaxMemoryPerNode <- 16

For FlurmJobLifecycle:
  JobIds <- {j1, j2, j3}
  StepIds <- {s1, s2}

For FlurmFailover:
  Controllers <- {c1, c2, c3}
  MaxTerm <- 5
  MaxLogLength <- 10
```

6. Add invariants to check:
   - Add `Safety` to "Invariants"
   - Add liveness properties to "Properties"

7. Run TLC (Model → Run Model)

### Command Line

```bash
# Install TLC
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

# Run model checker
java -jar tla2tools.jar -config FlurmScheduler.cfg FlurmScheduler.tla
```

### Example Configuration File (FlurmScheduler.cfg)

```
CONSTANTS
    Nodes = {n1, n2, n3}
    MaxJobs = 5
    MaxCPUsPerNode = 8
    MaxMemoryPerNode = 16
    
INVARIANTS
    TypeInvariant
    Safety
    
PROPERTIES
    AllJobsTerminate
```

## Interpreting Results

### Invariant Violations

If TLC finds an invariant violation, it provides:
1. A trace of states leading to the violation
2. The exact invariant that failed
3. Variable values at the failure point

Example output:
```
Error: Invariant NoOverallocationCPUs is violated.
The behavior up to this point is:
1: <Initial state>
2: SubmitJob(4, 8)
3: ScheduleJob
...
```

### State Space

TLC will report:
- Number of distinct states found
- Diameter (longest behavior)
- Time to check

Small constants = fast checking, large constants = more thorough but slower.

## Extending Specifications

### Adding New Properties

1. Define the property using temporal logic
2. Add to the specification file
3. Add to model configuration
4. Re-run TLC

### Modifying Algorithms

1. Update the TLA+ specification
2. Verify type invariant still holds
3. Check all safety properties
4. Verify liveness under fairness

## Integration with Implementation

The TLA+ specifications serve as:
1. **Documentation**: Precise algorithm description
2. **Verification**: Proof of correctness properties
3. **Test Oracle**: Reference for property-based tests

Map TLA+ properties to Erlang tests:
```erlang
%% From TLA+: NoOverallocationCPUs
prop_no_cpu_overallocation() ->
    ?FORALL(Events, event_sequence(),
        check_no_overallocation(apply_events(Events))).
```

## Files

| File | Description |
|------|-------------|
| `FlurmScheduler.tla` | Scheduler resource allocation |
| `FlurmJobLifecycle.tla` | Job state machine |
| `FlurmFailover.tla` | Controller consensus |
| `*.cfg` | Model configurations |
