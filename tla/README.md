# FLURM TLA+ Specifications

This directory contains formal TLA+ specifications for the FLURM workload manager. These specifications model critical components of the system to verify safety and liveness properties.

## Overview

| Specification | Description | Key Properties |
|--------------|-------------|----------------|
| `FlurmScheduler.tla` | Core scheduler and resource allocation | No over-allocation, running jobs have nodes |
| `FlurmJobLifecycle.tla` | Job state machine transitions | Valid transitions, terminal states absorbing |
| `FlurmConsensus.tla` | Raft-based consensus protocol | Leader election safety, log consistency |
| `FlurmFailover.tla` | Controller failover and recovery | No job loss, partition consistency |

## Prerequisites

### Installing TLC Model Checker

1. **Download TLA+ Toolbox** (recommended):
   ```bash
   # macOS (via Homebrew)
   brew install --cask tla-plus-toolbox

   # Or download from:
   # https://github.com/tlaplus/tlaplus/releases
   ```

2. **Command-line TLC**:
   ```bash
   # Download tla2tools.jar
   wget https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

   # Run with Java
   java -jar tla2tools.jar -config MySpec.cfg MySpec.tla
   ```

3. **VS Code Extension**:
   - Install "TLA+" extension by Markus Kuppe
   - Provides syntax highlighting, model checking, and PDF generation

## Running the Specifications

### Using TLA+ Toolbox

1. Open TLA+ Toolbox
2. File > Open Spec > Add New Spec
3. Select the `.tla` file
4. Create a new model (Model > New Model)
5. Configure constants and properties (see configurations below)
6. Run TLC (TLC Model Checker > Run Model)

### Using Command Line

Create a configuration file (`.cfg`) for each specification, then run:

```bash
java -XX:+UseParallelGC -Xmx4g -jar tla2tools.jar \
  -config FlurmScheduler.cfg \
  -workers auto \
  FlurmScheduler.tla
```

## Specification Details

### 1. FlurmScheduler.tla

Models the core scheduler responsible for resource allocation and job management.

**Constants Configuration:**
```tla
CONSTANTS
  Nodes = {n1, n2, n3}
  MaxJobs = 3
  MaxCPUsPerNode = 4
```

**Properties Verified:**
- `TypeInvariant` - Type correctness of all variables
- `Safety` - Combined safety properties:
  - `ResourceConstraint` - Never over-allocate CPUs
  - `RunningJobsHaveNodes` - Running jobs always have allocated nodes
  - `OnlyRunningJobsAllocated` - Non-running jobs have no allocations
- `AllJobsTerminate` (liveness) - Jobs reach terminal states
- `NoStarvation` (liveness) - Pending jobs eventually run

**Recommended Model Configuration:**
```
SPECIFICATION Spec
INVARIANT TypeInvariant
INVARIANT Safety
PROPERTY AllJobsTerminate
```

**Sample .cfg file (FlurmScheduler.cfg):**
```
SPECIFICATION Spec
CONSTANTS
  Nodes = {n1, n2, n3}
  MaxJobs = 3
  MaxCPUsPerNode = 4
INVARIANT TypeInvariant
INVARIANT Safety
```

**Expected Runtime:** ~30 seconds to 2 minutes with default configuration.

---

### 2. FlurmJobLifecycle.tla

Models the complete job state machine with all valid transitions.

**Job State Diagram:**
```
NULL -> PENDING -> RUNNING -> COMPLETING -> COMPLETED
                      |            |
                      v            v
                   SUSPENDED    FAILED
                      |            ^
                      v            |
                   CANCELLED <-----+
                      ^
                      |
                   TIMEOUT
```

**Constants Configuration:**
```tla
CONSTANTS
  MaxJobs = 2
  MaxTimeLimit = 3
```

**Properties Verified:**
- `TypeInvariant` - Type correctness
- `OnlyValidTransitions` - No invalid state transitions
- `TerminalStatesAbsorbing` - Terminal states cannot be escaped
- `TimeLimitsEnforced` - Time bounds are respected
- `AllJobsTerminate` (liveness) - All jobs reach terminal state

**Recommended Model Configuration:**
```
SPECIFICATION Spec
INVARIANT TypeInvariant
PROPERTY AllJobsTerminate
```

**Sample .cfg file (FlurmJobLifecycle.cfg):**
```
SPECIFICATION Spec
CONSTANTS
  MaxJobs = 2
  MaxTimeLimit = 3
INVARIANT TypeInvariant
```

**Expected Runtime:** ~15 seconds to 1 minute.

---

### 3. FlurmConsensus.tla

Models Raft consensus for controller coordination.

**Constants Configuration:**
```tla
CONSTANTS
  Servers = {s1, s2, s3}
  MaxTerm = 3
  MaxLogLength = 3
  Values = {v1, v2}
```

**Properties Verified:**
- `TypeInvariant` - Type correctness
- `ElectionSafety` - At most one leader per term
- `LogMatching` - Logs are consistent across servers
- `StateMachineSafety` - Same commands applied in same order
- `EventuallyLeader` (liveness) - A leader is eventually elected

**Key Safety Property (Election Safety):**
```tla
ElectionSafety ==
    \A s1, s2 \in Servers :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\
         currentTerm[s1] = currentTerm[s2])
        => s1 = s2
```

**Recommended Model Configuration:**
```
SPECIFICATION Spec
INVARIANT TypeInvariant
INVARIANT ElectionSafety
INVARIANT LogMatching
INVARIANT StateMachineSafety
```

**Sample .cfg file (FlurmConsensus.cfg):**
```
SPECIFICATION SafetySpec
CONSTANTS
  Servers = {s1, s2, s3}
  MaxTerm = 2
  MaxLogLength = 2
  Values = {v1}
INVARIANT TypeInvariant
INVARIANT ElectionSafety
INVARIANT LogMatching
```

**Expected Runtime:** 1-5 minutes (Raft generates many states).

---

### 4. FlurmFailover.tla

Models controller failover with network partitions.

**Constants Configuration:**
```tla
CONSTANTS
  Controllers = {c1, c2, c3}
  MaxJobs = 2
  MaxOperations = 5
```

**Properties Verified:**
- `TypeInvariant` - Type correctness
- `NoJobLoss` - Committed jobs are never lost
- `AtMostOneLeader` - Single leader per term
- `PartitionConsistency` - Consistency within partitions
- `EventuallyConsistent` (liveness) - Convergence after partition heals
- `EventuallyLeader` (liveness) - Leader elected when quorum available

**Key Safety Property (No Job Loss):**
```tla
NoJobLoss ==
    \A j \in committedJobs :
        \E c \in Controllers :
            /\ controllerState[c] # "down"
            /\ jobRegistry[c][j] # "none"
```

**Recommended Model Configuration:**
```
SPECIFICATION Spec
INVARIANT TypeInvariant
INVARIANT Safety
```

**Sample .cfg file (FlurmFailover.cfg):**
```
SPECIFICATION Spec
CONSTANTS
  Controllers = {c1, c2, c3}
  MaxJobs = 2
  MaxOperations = 4
INVARIANT TypeInvariant
INVARIANT NoJobLoss
INVARIANT AtMostOneLeader
```

**Expected Runtime:** 1-5 minutes.

## Model Checking Tips

### Reducing State Space

For faster model checking during development:

1. **Reduce constants:**
   ```tla
   Nodes = {n1, n2}      \* Instead of 5 nodes
   MaxJobs = 2           \* Instead of 10
   MaxCPUsPerNode = 2    \* Instead of 64
   ```

2. **Use symmetry sets** (TLA+ Toolbox):
   - Mark `Nodes`, `Servers`, `Controllers` as symmetry sets
   - Reduces state space by eliminating symmetric states

3. **Check safety first:**
   - Safety properties are typically faster to verify
   - Only check liveness after safety is confirmed

### Memory and Performance

```bash
# Large state space - increase heap
java -Xmx8g -jar tla2tools.jar ...

# Use parallel workers
java -jar tla2tools.jar -workers 8 ...

# Checkpoint for long runs
java -jar tla2tools.jar -checkpoint 5 ...
```

### Debugging Counterexamples

When TLC finds a violation:
1. The error trace shows the sequence of states leading to the violation
2. Look for the state where the invariant first becomes false
3. Examine which action caused the transition

## Known Limitations

### FlurmScheduler.tla
- Does not model memory allocation (only CPUs)
- Single-node allocation per job (no multi-node jobs)
- FIFO queue discipline only

### FlurmJobLifecycle.tla
- Time is modeled abstractly (discrete ticks)
- No job dependencies modeled
- Requeue not fully modeled

### FlurmConsensus.tla
- Simplified from full Raft:
  - No log compaction
  - No membership changes
  - Bounded message buffer (implicit)
- Network delays modeled as message ordering nondeterminism

### FlurmFailover.tla
- Simplified replication (full log copy on sync)
- No Byzantine failures
- Controller state assumed persistent

## Verification Status

| Property | FlurmScheduler | FlurmJobLifecycle | FlurmConsensus | FlurmFailover |
|----------|----------------|-------------------|----------------|---------------|
| Type Invariant | Verified | Verified | Verified | Verified |
| Safety | Verified | Verified | Verified | Verified |
| Liveness | Verified* | Verified* | Verified* | Verified* |

*Liveness verified under fairness assumptions.

## Integration with FLURM Development

These specifications serve as:

1. **Design Documentation**: Precise description of expected behavior
2. **Regression Testing**: Verify design changes don't violate properties
3. **Bug Discovery**: Find edge cases before implementation
4. **Code Review Aid**: Compare implementation against spec

### Mapping to Erlang Code

| TLA+ Module | Erlang Module(s) |
|-------------|------------------|
| FlurmScheduler | `flurm_scheduler_engine.erl`, `flurm_job_manager.erl` |
| FlurmJobLifecycle | `flurm_job_manager.erl` |
| FlurmConsensus | `flurm_consensus.erl` |
| FlurmFailover | `flurm_state_manager.erl`, `flurm_consensus.erl` |

## Further Reading

- [TLA+ Hyperbook](https://lamport.azurewebsites.net/tla/hyperbook.html)
- [Learn TLA+](https://learntla.com/)
- [Practical TLA+](https://www.apress.com/gp/book/9781484238288)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [TLA+ Examples Repository](https://github.com/tlaplus/Examples)

## License

These specifications are part of the FLURM project and are licensed under Apache 2.0.
