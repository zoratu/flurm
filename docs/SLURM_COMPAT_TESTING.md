# SLURM Compatibility Testing

FLURM includes a comprehensive SLURM compatibility test suite derived from the [SchedMD SLURM testsuite](https://github.com/SchedMD/slurm/tree/master/testsuite). These tests use real SLURM command-line clients (sbatch, squeue, sinfo, scontrol, scancel) against a FLURM Docker cluster to verify protocol and behavioral compatibility.

## Results Summary

| Metric | Value |
|--------|-------|
| **Total Tests** | 108 |
| **Passing** | 105 |
| **Failing** | 0 |
| **Skipped** | 3 (expected) |

### Expected Skips

| Test | Reason |
|------|--------|
| test17.16 | `--output` path not shown in scontrol output |
| test2.9 | `scontrol hold` not fully implemented |
| test2.12 | `scontrol update JobName` not implemented |

## Test Categories

The test suite is organized by SLURM testsuite numbering conventions:

| Category | SLURM Tests | Count | Description |
|----------|-------------|-------|-------------|
| test4.x | sinfo | 10 | Node/partition info display and formatting |
| test17.x | sbatch | 16 | Job submission, options, directives, resource requests |
| test5.x | squeue | 16 | Job queue display, filtering, format options |
| test2.x | scontrol | 13 | Job/partition/node control and inspection |
| test6.x | scancel | 7 | Job cancellation, batch cancel, invalid IDs |
| test1.x | Job lifecycle | 6 | PENDING -> RUNNING -> COMPLETED transitions |
| Resource | Scheduling | 6 | CPU/memory allocation, oversubscription |
| Stress | Concurrent | 5 | Rapid submission, batch operations |
| Node | Tracking | 5 | Resource allocation/release tracking |
| Edge | Cases | 10 | Empty queues, invalid IDs, error handling |
| Directives | #SBATCH | 4 | In-script directive parsing |
| Time | Formats | 4 | MM:SS, HH:MM:SS, D-HH:MM:SS, minutes |
| Node detail | Tracking | 4 | CPU/memory state through job lifecycle |
| Mixed | Workload | 2 | Multi-job concurrent execution |

## Running the Tests

### Prerequisites

- Docker and Docker Compose
- FLURM Docker cluster running (`docker compose up -d` in `docker/`)

### Run the Full Suite

```bash
cd docker
./slurm_compat_tests.sh
```

### Run via Pre-commit Hook

The tests run automatically on commit when Docker containers are running:

```bash
# With containers running, tests run on every commit
git commit -m "your message"

# Skip if needed
git commit --no-verify -m "your message"

# Run full test level (includes SLURM compat)
FLURM_TEST_LEVEL=full git commit -m "your message"
```

### Run Manually in Docker

```bash
# Copy and run inside the client container
docker cp docker/slurm_compat_tests.sh docker-slurm-client-1:/tmp/
docker exec docker-slurm-client-1 bash /tmp/slurm_compat_tests.sh
```

## Test Script Details

The test script (`docker/slurm_compat_tests.sh`) performs the following:

1. **Environment check**: Verifies SLURM client connectivity to FLURM
2. **Job cleanup**: Cancels any pre-existing jobs for a clean baseline
3. **Test execution**: Runs 108 tests across 14 categories
4. **Result reporting**: Prints PASS/FAIL/SKIP counts with details on any failures

Each test follows this pattern:
- Submit or query using standard SLURM commands
- Validate output format, content, and behavior
- Clean up (cancel jobs) between tests where needed

## Relationship to SchedMD SLURM Testsuite

The compatibility tests are **derived from** the [SchedMD SLURM testsuite](https://github.com/SchedMD/slurm/tree/master/testsuite) but adapted for FLURM's scope:

- **test1.x** (srun): Adapted for job lifecycle testing via sbatch
- **test2.x** (scontrol): Direct adaptation of scontrol show/update tests
- **test4.x** (sinfo): Direct adaptation of sinfo format and output tests
- **test5.x** (squeue): Direct adaptation of squeue format and filtering tests
- **test6.x** (scancel): Direct adaptation of scancel functionality tests
- **test17.x** (sbatch): Direct adaptation of sbatch submission and option tests

The original SLURM testsuite uses Expect/TCL and Python; our tests use bash for portability within Docker containers.

### Coverage vs. Full SLURM Testsuite

The SchedMD testsuite contains 600+ Expect tests and 170+ Python tests covering advanced features like:
- Accounting (sacct, sacctmgr, slurmdbd)
- Job arrays with complex syntax
- Burst buffers
- Federations
- GPU/GRES scheduling
- Heterogeneous jobs
- Preemption policies
- Reservations
- Topology-aware scheduling

FLURM's test suite focuses on the core CLI compatibility that most users need. As FLURM implements more advanced features, additional tests from the SchedMD testsuite will be adapted.

## Adding New Tests

To add a new test to the compatibility suite:

1. Edit `docker/slurm_compat_tests.sh`
2. Add your test function following the existing pattern:

```bash
# Test description
RESULT=$(sbatch --your-option ... 2>&1)
if echo "$RESULT" | grep -q "expected output"; then
    pass "testXX.Y: description"
else
    fail "testXX.Y: description" "Expected: ... Got: $RESULT"
fi
```

3. Run the test suite to verify
4. Update this document's test counts if needed
