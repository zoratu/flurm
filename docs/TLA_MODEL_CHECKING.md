# TLA+ Model Checking for FLURM

This document describes the TLA+ model checking infrastructure for verifying FLURM's distributed system properties.

## Overview

FLURM uses TLA+ (Temporal Logic of Actions) specifications to formally verify critical safety and liveness properties of the distributed scheduler. The TLC model checker exhaustively explores all possible states to find invariant violations.

## Prerequisites

### Java Installation

TLC requires Java 11 or later. Install Java using your package manager:

**macOS (Homebrew):**
```bash
brew install openjdk@11
# Add to PATH
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

**Fedora/RHEL:**
```bash
sudo dnf install java-11-openjdk
```

**Windows:**
Download from [adoptium.net](https://adoptium.net/) or use Chocolatey:
```powershell
choco install temurin11
```

Verify installation:
```bash
java -version
```

### TLA+ Tools

The `tla2tools.jar` file will be automatically downloaded when running the model checker. To manually download:

```bash
cd tla/
curl -L -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
```

Or use the Makefile:
```bash
cd tla/
make download
```

## Specifications

FLURM includes the following TLA+ specifications:

| Specification | Description | Key Properties |
|--------------|-------------|----------------|
| `FlurmFederation.tla` | Federated job scheduling across clusters | SiblingExclusivity, NoJobLoss, OriginAwareness |
| `FlurmAccounting.tla` | TRES (resource) accounting consistency | TRESConsistency, NoDoubleCounting, LogPrefixConsistency |
| `FlurmMigration.tla` | SLURM to FLURM migration | ExclusiveJobOwnership, ModeConstraints, RollbackPossible |
| `FlurmConsensus.tla` | Raft-based consensus protocol | ElectionSafety, LogMatching, StateMachineSafety |
| `FlurmFailover.tla` | Controller failover handling | NoJobLoss, AtMostOneLeader, PartitionConsistency |
| `FlurmJobLifecycle.tla` | Job state machine | TerminalStatesAbsorbing, TimeMonotonicity |
| `FlurmScheduler.tla` | Core scheduler allocation | ResourceConstraint, RunningJobsHaveNodes |

## Running Model Checking

### Using the Shell Script

The `run_tlc.sh` script provides a convenient way to run model checking:

```bash
cd tla/

# Run all specifications
./run_tlc.sh

# Run a specific specification
./run_tlc.sh FlurmFederation

# Run with JSON output (for CI integration)
./run_tlc.sh --json

# Run with specific worker count
./run_tlc.sh --workers 8 FlurmConsensus

# Run with depth limit (faster, less thorough)
./run_tlc.sh --depth 100

# Generate state graph visualization
./run_tlc.sh --dot FlurmScheduler

# List available specifications
./run_tlc.sh --list

# Show help
./run_tlc.sh --help
```

### Using Make

```bash
cd tla/

# Run all specifications
make check

# Run specific specification
make check-federation
make check-accounting
make check-migration
make check-consensus
make check-failover
make check-lifecycle
make check-scheduler

# Quick check with reduced depth
make check-quick

# Run with JSON output
make check-json

# Clean generated files
make clean

# Download tla2tools.jar
make download

# Show help
make help
```

### Using Erlang Tests

The TLA+ model checker can be run from Erlang tests:

```bash
# Run all TLA+ integration tests
rebar3 eunit --module=flurm_tla_integration_tests

# Skip TLA+ tests (useful in CI for fast feedback)
SKIP_TLA_TESTS=1 rebar3 eunit
```

## Interpreting Results

### Successful Run

A successful model checking run will output:
```
Model checking completed. No error has been found.
  Fingerprint collision probability: calculated ...
  15234 states generated, 4521 distinct states found, 0 states left on queue.
```

### Invariant Violation

If an invariant is violated, TLC will output:
```
Error: Invariant SiblingExclusivity is violated.
Error: The following behavior constitutes a counter-example:

State 1: <Initial predicate>
/\ jobOrigin = ...
/\ siblingState = ...
...

State 2: <StartSibling line 205, col 5 to line 215, col 62>
...
```

This trace shows the sequence of states that led to the violation, helping identify the bug.

### Deadlock Detection

If a deadlock is found:
```
Error: Deadlock reached.
Error: The behavior up to this point is:
...
```

### Temporal Property Violation

For liveness properties:
```
Error: Temporal properties were violated.
Error: The following behavior constitutes a counter-example:
...
```

## Tuning Model Checking

### Worker Threads

By default, TLC uses all available CPU cores. Override with:

```bash
./run_tlc.sh --workers 4 FlurmConsensus
# Or
TLC_WORKERS=4 ./run_tlc.sh FlurmConsensus
```

### Memory

Default is 4GB heap. For larger state spaces:

```bash
./run_tlc.sh --memory 8g FlurmConsensus
# Or
TLC_MEMORY=8g ./run_tlc.sh FlurmConsensus
```

### Depth Limit

Limit exploration depth for faster (but less thorough) checking:

```bash
./run_tlc.sh --depth 50 FlurmFederation
```

### Configuration Files

Each specification has a `.cfg` file that controls:

- **CONSTANTS**: Model values and bounds
- **INVARIANTS**: Safety properties to check
- **PROPERTIES**: Temporal (liveness) properties
- **SYMMETRY**: Symmetry sets for state space reduction
- **SPECIFICATION**: Which spec to use (SafetySpec, Spec, or LivenessSpec)

Example `FlurmFederation.cfg`:
```
CONSTANTS
    Clusters = {c1, c2, c3}
    MaxJobs = 2
    MaxMessages = 6
    Nil = Nil

SYMMETRY ClusterSymmetry
CONSTANT ClusterSymmetry = Permutations(Clusters)

INVARIANT TypeInvariant
INVARIANT SiblingExclusivity
INVARIANT NoJobLoss

SPECIFICATION SafetySpec
```

### Adjusting Bounds

To check larger state spaces, increase bounds in the `.cfg` file:

```
CONSTANTS
    Clusters = {c1, c2, c3, c4}  \* More clusters
    MaxJobs = 3                   \* More jobs
    MaxMessages = 10              \* More messages
```

**Warning**: State space grows exponentially with bounds. Start small and increase gradually.

## Adding New Specifications

1. **Create the TLA+ file** in `tla/`:
   ```
   ---------------------------- MODULE FlurmNewFeature ----------------------------
   EXTENDS Integers, Sequences, FiniteSets, TLC

   CONSTANTS ...
   VARIABLES ...

   Init == ...
   Next == ...

   \* Safety invariants
   MyInvariant == ...

   \* Specification
   SafetySpec == Init /\ [][Next]_vars
   =============================================================================
   ```

2. **Create the configuration file** `tla/FlurmNewFeature.cfg`:
   ```
   CONSTANTS
       ...

   INVARIANT TypeInvariant
   INVARIANT MyInvariant

   SPECIFICATION SafetySpec
   ```

3. **Add to run_tlc.sh** (ALL_SPECS array):
   ```bash
   ALL_SPECS=(
       ...
       "FlurmNewFeature"
   )
   ```

4. **Add to Makefile**:
   ```makefile
   check-newfeature: $(TLA2TOOLS) $(STATES_DIR)
       @./run_tlc.sh FlurmNewFeature
   ```

5. **Update this documentation** with the new specification.

## CI Integration

### GitHub Actions

Add to your workflow:

```yaml
- name: Install Java
  uses: actions/setup-java@v3
  with:
    distribution: 'temurin'
    java-version: '11'

- name: Run TLA+ Model Checking
  run: |
    cd tla/
    make check-json
  timeout-minutes: 30

- name: Upload TLA+ Results
  uses: actions/upload-artifact@v3
  with:
    name: tla-results
    path: tla/states/
```

### JSON Output Format

The `--json` flag produces machine-readable output:

```json
{
  "spec": "FlurmFederation",
  "status": "PASS",
  "states": "4521 distinct",
  "elapsed_seconds": 12,
  "timestamp": "2024-01-15T10:30:00Z",
  "config": {
    "workers": "auto",
    "depth": "unlimited",
    "memory": "4g"
  }
}
```

Summary file (`states/summary.json`):
```json
{
  "timestamp": "2024-01-15T10:35:00Z",
  "total_pass": 7,
  "total_fail": 0,
  "total_error": 0,
  "total_skip": 0,
  "all_passed": true,
  "results": [...]
}
```

## Troubleshooting

### "Java not found"

Ensure Java is in your PATH:
```bash
which java
java -version
```

### "tla2tools.jar not found"

Download manually or use:
```bash
make download
```

### Out of Memory

Increase heap size:
```bash
./run_tlc.sh --memory 8g FlurmConsensus
```

### Too Many States

Reduce bounds in the `.cfg` file or add symmetry reduction:
```
SYMMETRY Sym
CONSTANT Sym = Permutations(Nodes)
```

### Slow Checking

- Use fewer workers on memory-constrained systems
- Add depth limit for initial exploration
- Add symmetry sets
- Reduce model bounds

### Deadlock False Positives

If TLC reports deadlocks that are expected (terminal states), add to the spec:
```tla+
Next ==
    \/ ... existing actions ...
    \/ UNCHANGED vars  \* Allow stuttering in terminal states
```

Or disable deadlock checking:
```bash
# Remove -deadlock flag from the command
java -cp tla2tools.jar tlc2.TLC -config spec.cfg spec.tla
```

## Resources

- [TLA+ Home Page](https://lamport.azurewebsites.net/tla/tla.html)
- [TLA+ Video Course](https://lamport.azurewebsites.net/video/videos.html)
- [Learn TLA+](https://learntla.com/)
- [TLA+ Toolbox](https://github.com/tlaplus/tlaplus)
- [Practical TLA+ Book](https://www.apress.com/gp/book/9781484238288)

## File Structure

```
tla/
├── FlurmFederation.tla     # Federation spec
├── FlurmFederation.cfg     # Federation config
├── FlurmAccounting.tla     # Accounting spec
├── FlurmAccounting.cfg     # Accounting config
├── FlurmMigration.tla      # Migration spec
├── FlurmMigration.cfg      # Migration config
├── FlurmConsensus.tla      # Consensus spec
├── FlurmConsensus.cfg      # Consensus config
├── FlurmFailover.tla       # Failover spec
├── FlurmFailover.cfg       # Failover config
├── FlurmJobLifecycle.tla   # Job lifecycle spec
├── FlurmJobLifecycle.cfg   # Job lifecycle config
├── FlurmScheduler.tla      # Scheduler spec
├── FlurmScheduler.cfg      # Scheduler config
├── tla2tools.jar           # TLC model checker (downloaded)
├── run_tlc.sh              # Shell script runner
├── Makefile                # Make targets
├── .gitignore              # Ignore generated files
└── states/                 # Generated output (gitignored)
    ├── *_output.txt        # TLC output logs
    ├── *_result.json       # JSON results
    └── summary.json        # Combined results
```
