# FLURM Hot Migration Test Environment

This directory contains Docker-based testing infrastructure for verifying
hot migration from SLURM to FLURM.

## Overview

The test environment simulates a production migration scenario:

1. **SLURM Cluster** - A minimal but functional SLURM installation with:
   - `slurm-controller` - slurmctld service
   - `slurm-node` - slurmd compute node

2. **FLURM Controller** - Starts in shadow mode to:
   - Import existing SLURM job state via `flurm_slurm_import`
   - Mirror SLURM operations
   - Take over when traffic is switched

3. **Test Runner** - Executes the migration test script

## Quick Start

```bash
# Build all images
docker-compose -f docker-compose.migration.yml build

# Start the test environment
docker-compose -f docker-compose.migration.yml up -d

# Wait for services to be healthy
docker-compose -f docker-compose.migration.yml ps

# Run the migration test
docker-compose -f docker-compose.migration.yml exec test-runner /scripts/test-migration.sh

# View logs
docker-compose -f docker-compose.migration.yml logs -f

# Clean up
docker-compose -f docker-compose.migration.yml down -v
```

## Test Phases

The `test-migration.sh` script executes these phases:

### Phase 1: Verify SLURM Cluster
- Confirms slurmctld is running
- Verifies compute node is registered
- Checks node state

### Phase 2: Submit Test Jobs
- Submits various job types to SLURM
- Creates short, long, and array jobs
- Establishes baseline job state

### Phase 3: Verify FLURM Shadow Mode
- Confirms FLURM is running
- Checks HTTP health endpoint
- Verifies shadow mode configuration

### Phase 4: Verify SLURM Import
- Triggers state import via `flurm_slurm_import`
- Verifies jobs were imported
- Checks nodes and partitions

### Phase 5: Simulate Traffic Switch
- Documents production switch steps
- Verifies FLURM is listening
- Tests protocol compatibility

### Phase 6: Verify Job Tracking
- Confirms original jobs still tracked
- Validates job state consistency
- Checks FLURM tracking

### Phase 7: Test FLURM Job Submission
- Attempts to submit new job via FLURM
- Verifies job acceptance
- Tests scheduling capability

### Phase 8: Summary
- Reports test results
- Saves results to JSON
- Cleans up test jobs

## Files

### Dockerfiles
- `Dockerfile.slurm-full` - Complete SLURM installation
- `Dockerfile.flurm-migration` - FLURM with import support
- `Dockerfile.test-runner` - Test execution environment

### Configuration
- `config/slurm-migration.conf` - SLURM controller config
- `config/slurm-client.conf` - SLURM client config
- `config/cgroup.conf` - SLURM cgroup config
- `config/flurm-migration.config` - FLURM Erlang config
- `config/vm.args.flurm` - Erlang VM arguments

### Scripts
- `scripts/test-migration.sh` - Main test script
- `scripts/start-slurm-controller.sh` - SLURM controller startup
- `scripts/start-slurm-node.sh` - SLURM node startup
- `scripts/start-flurm-shadow.sh` - FLURM shadow mode startup
- `scripts/start-flurm-active.sh` - FLURM active mode startup
- `scripts/submit-test-jobs.sh` - Submit test jobs
- `scripts/verify-import.sh` - Verify import results
- `scripts/switch-traffic.sh` - Simulate traffic switch
- `scripts/trigger-import.sh` - Trigger manual import

### Test Jobs
- `jobs/test_short.sh` - 5-minute job
- `jobs/test_long.sh` - 30-minute job
- `jobs/test_array.sh` - Array job (5 tasks)

## Network

All containers are on the `migration-net` network:
- `172.30.0.10` - slurm-controller
- `172.30.0.11` - slurm-node (compute-node-1)
- `172.30.0.20` - flurm-controller
- `172.30.0.100` - test-runner

## Ports

| Service | Port | Description |
|---------|------|-------------|
| SLURM Controller | 6817 | slurmctld |
| SLURM Node | 6818 | slurmd |
| FLURM Controller | 6820 | SLURM-compatible |
| FLURM HTTP | 8080 | API and metrics |

## Limitations and Simplifications

1. **Single Compute Node**: Only one slurmd for simplicity
2. **Minimal Job Complexity**: Basic job scripts, no MPI
3. **No GPU Resources**: GRES configuration simplified
4. **Containerized SLURM**: Some features disabled (cgroups)
5. **Authentication**: MUNGE shared via Docker volume
6. **No slurmdbd**: Accounting database not included
7. **Simulated Traffic Switch**: Uses config file changes

## Production Migration Notes

For a real production migration:

1. **DNS/Load Balancer**: Update to point to FLURM
2. **Client Updates**: Reconfigure all client slurm.conf files
3. **Gradual Rollout**: Consider canary deployments
4. **Monitoring**: Watch for job failures during switch
5. **Rollback Plan**: Keep SLURM running until confident
6. **User Communication**: Notify users of maintenance window

## Troubleshooting

### SLURM controller won't start
```bash
docker-compose -f docker-compose.migration.yml logs slurm-controller
```

### Compute node not registering
```bash
docker-compose -f docker-compose.migration.yml exec slurm-controller sinfo -N
docker-compose -f docker-compose.migration.yml exec slurm-node scontrol ping
```

### FLURM import failing
```bash
docker-compose -f docker-compose.migration.yml logs flurm-controller
docker-compose -f docker-compose.migration.yml exec flurm-controller \
    curl localhost:8080/health
```

### MUNGE authentication issues
```bash
# Check key exists and has correct permissions
docker-compose -f docker-compose.migration.yml exec slurm-controller \
    ls -la /etc/munge/
```

## Development

To modify the test setup:

1. Edit configuration files in `config/`
2. Rebuild: `docker-compose -f docker-compose.migration.yml build`
3. Restart: `docker-compose -f docker-compose.migration.yml up -d`
4. Re-run tests: `docker-compose -f docker-compose.migration.yml exec test-runner /scripts/test-migration.sh`
