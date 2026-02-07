# FLURM E2E Tests

End-to-end tests for FLURM that run in a Docker environment with multiple controller nodes and compute nodes.

## Prerequisites

- Docker and Docker Compose
- At least 8GB RAM available for containers
- ~10GB disk space

## Quick Start

```bash
# From the docker/ directory
cd docker/

# Start the test environment
docker compose -f docker-compose.e2e-tests.yml up -d --build

# Wait for cluster to be healthy (check logs)
docker compose -f docker-compose.e2e-tests.yml logs -f flurm-ctrl-1

# Run all E2E tests
docker compose -f docker-compose.e2e-tests.yml exec test-runner /tests/run-all.sh

# View results
ls -la test-results/

# Clean up
docker compose -f docker-compose.e2e-tests.yml down -v
```

## Test Environment

The E2E test environment includes:

### Controllers (3-node HA cluster)
- `flurm-ctrl-1` (172.30.0.10) - Primary controller, port 6817
- `flurm-ctrl-2` (172.30.0.11) - Secondary controller
- `flurm-ctrl-3` (172.30.0.12) - Tertiary controller

### Compute Nodes (varied resources)
- `flurm-node-1` (172.30.0.20) - 4 CPUs, 8GB RAM, partitions: batch, debug
- `flurm-node-2` (172.30.0.21) - 8 CPUs, 16GB RAM, partitions: batch, large
- `flurm-node-3` (172.30.0.22) - 4 CPUs, 8GB RAM, 2 GPUs, partitions: gpu

### Test Runner
- `test-runner` (172.30.0.100) - SLURM client tools with MUNGE

## Test Suites

### Job Lifecycle Tests
- `test-job-lifecycle.sh` - Basic job submission, tracking, completion
- `test-job-constraints.sh` - Resource allocation (CPU, memory, nodes)
- `test-job-arrays.sh` - Array jobs, throttling, partial failures
- `test-job-dependencies.sh` - afterok, afterany, afternotok dependencies

### Failure Handling
- `test-job-failures.sh` - Exit codes, timeouts, OOM, retries
- `test-cascading-failures.sh` - System stability under stress

### HA and Clustering
- `test-ha-failover.sh` - Multi-controller availability
- `test-accounting-consistency.sh` - TRES tracking, aggregation

### Migration and Federation
- `test-migration-concurrent.sh` - Job handling during migration
- `test-federation-accounting.sh` - Cross-cluster accounting

### Features
- `test-munge-auth.sh` - MUNGE credential generation/validation
- `test-metrics.sh` - Prometheus metrics endpoint

## Running Individual Tests

```bash
# Run a specific test
docker compose -f docker-compose.e2e-tests.yml exec test-runner /tests/test-job-lifecycle.sh

# Interactive shell for debugging
docker compose -f docker-compose.e2e-tests.yml exec test-runner bash

# Inside the container, you can use SLURM commands:
sbatch /jobs/batch/job_cpu_small.sh
squeue
sacct
sinfo
```

## Results

Test results are saved to `./test-results/` with timestamps:
```
test-results/
└── 20260206_120000/
    ├── job_lifecycle_basic.log
    ├── job_lifecycle_constraints.log
    ├── ...
    └── metrics_endpoint.log
```

## Troubleshooting

### Cluster not starting
```bash
# Check controller logs
docker compose -f docker-compose.e2e-tests.yml logs flurm-ctrl-1

# Check if MUNGE key was created
docker compose -f docker-compose.e2e-tests.yml exec flurm-ctrl-1 ls -la /etc/munge/
```

### Tests failing with auth errors
```bash
# Verify MUNGE is working
docker compose -f docker-compose.e2e-tests.yml exec test-runner munge -n | unmunge

# Check SLURM config
docker compose -f docker-compose.e2e-tests.yml exec test-runner cat /etc/slurm/slurm.conf
```

### Jobs not running
```bash
# Check node status
docker compose -f docker-compose.e2e-tests.yml exec test-runner sinfo

# Check queue
docker compose -f docker-compose.e2e-tests.yml exec test-runner squeue

# Check controller health
curl http://localhost:9090/metrics
```

## Environment Variables

Set these before running tests to customize behavior:

- `FLURM_CTRL_1` - IP of controller 1 (default: 172.30.0.10)
- `FLURM_CTRL_2` - IP of controller 2 (default: 172.30.0.11)
- `FLURM_CTRL_3` - IP of controller 3 (default: 172.30.0.12)

## Notes

- Full HA failover testing (killing/restarting containers) requires external orchestration
- Full migration testing requires a running SLURM cluster
- Full federation testing requires multiple configured clusters
- Tests are designed to pass with minimal configuration and degrade gracefully
