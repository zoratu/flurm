# FLURM From-Scratch Docker Setup

Build and test FLURM with a single command. This setup creates a complete HA cluster from scratch, runs comprehensive E2E tests, and reports results.

## Quick Start

```bash
# Clone the repo (if you haven't already)
git clone https://github.com/zoratu/flurm.git
cd flurm/docker/from-scratch

# Run everything with one command
./run-from-scratch.sh
```

That's it! The script will:
1. Build FLURM from source using Erlang/OTP 27
2. Start a 3-node HA controller cluster
3. Start 3 compute nodes with varying resources
4. Run comprehensive E2E tests
5. Report results and clean up

## What Gets Built

The from-scratch setup creates:

| Component | Count | Description |
|-----------|-------|-------------|
| Controllers | 3 | HA cluster with Raft consensus |
| Compute Nodes | 3 | Varying CPU/memory/GPU configs |
| Test Runner | 1 | SLURM client tools + test suite |

### Network Topology

```
                    ┌─────────────────────────────────────────┐
                    │         flurm-scratch-net               │
                    │            172.31.0.0/16                │
                    └─────────────────────────────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │                             │                             │
   ┌────┴────┐                   ┌────┴────┐                   ┌────┴────┐
   │ctrl-1   │                   │ctrl-2   │                   │ctrl-3   │
   │.0.10    │◄─────────────────►│.0.11    │◄─────────────────►│.0.12    │
   │:6817    │   Raft Cluster    │:6827    │   Raft Cluster    │:6837    │
   └────┬────┘                   └─────────┘                   └─────────┘
        │
        │  Node Registration
        │
   ┌────┴────┬──────────────────┬──────────────────┐
   │         │                  │                  │
┌──┴───┐  ┌──┴───┐          ┌───┴──┐          ┌────┴───┐
│node-1│  │node-2│          │node-3│          │test-   │
│.0.20 │  │.0.21 │          │.0.22 │          │runner  │
│4CPU  │  │8CPU  │          │4CPU  │          │.0.100  │
│8GB   │  │16GB  │          │8GB+2GPU│        │        │
└──────┘  └──────┘          └──────┘          └────────┘
```

## Usage Options

### Basic Usage

```bash
# Build from local source and run all tests
./run-from-scratch.sh

# Build from a specific GitHub branch
./run-from-scratch.sh --repo https://github.com/zoratu/flurm.git --branch feature/my-branch

# Use a different Erlang version
./run-from-scratch.sh --erlang 26
```

### Development Workflow

```bash
# Skip the build step (use existing images)
./run-from-scratch.sh --skip-build

# Start cluster without running tests
./run-from-scratch.sh --skip-tests

# Keep cluster running after tests for debugging
./run-from-scratch.sh --keep-running

# Interactive mode: start cluster and drop to shell
./run-from-scratch.sh --skip-tests --shell
```

### Cleanup

```bash
# Clean everything before starting
./run-from-scratch.sh --clean

# Manual cleanup
docker compose -f docker-compose.from-scratch.yml down -v
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--repo URL` | Git repository URL | Use local source |
| `--branch BRANCH` | Git branch to build | `main` |
| `--erlang VERSION` | Erlang/OTP version | `27` |
| `--skip-build` | Skip image build step | Build images |
| `--skip-tests` | Skip test execution | Run tests |
| `--keep-running` | Keep cluster after tests | Stop cluster |
| `--shell` | Drop to shell after tests | Exit |
| `--verbose` | Show detailed build output | Quiet |
| `--clean` | Clean up before starting | Keep existing |
| `--help` | Show help message | - |

## Environment Variables

You can also configure the build using environment variables:

```bash
# Set Erlang version
export ERLANG_VERSION=26

# Set repository and branch
export FLURM_REPO_URL=https://github.com/myorg/flurm-fork.git
export FLURM_BRANCH=my-feature

# Run
./run-from-scratch.sh
```

## Manual Docker Compose Usage

If you prefer to use docker-compose directly:

```bash
# Build the image
docker compose -f docker-compose.from-scratch.yml build

# Start the cluster
docker compose -f docker-compose.from-scratch.yml up -d

# Wait for health
docker compose -f docker-compose.from-scratch.yml ps

# Run tests
docker compose -f docker-compose.from-scratch.yml exec test-runner /tests/run-all.sh

# Get a shell
docker compose -f docker-compose.from-scratch.yml exec test-runner bash

# View logs
docker compose -f docker-compose.from-scratch.yml logs -f flurm-ctrl-1

# Stop everything
docker compose -f docker-compose.from-scratch.yml down -v
```

## Test Results

Test results are saved to `./test-results/` with timestamps:

```
test-results/
├── 20240215_143022/
│   ├── job_lifecycle_basic.log
│   ├── ha_failover.log
│   ├── migration_concurrent.log
│   └── ...
└── 20240215_150811/
    └── ...
```

## CI Integration

### GitHub Actions

```yaml
name: FLURM E2E Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - uses: actions/checkout@v4

      - name: Run from-scratch tests
        run: |
          cd docker/from-scratch
          ./run-from-scratch.sh --verbose

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: test-results
          path: docker/from-scratch/test-results/
```

### GitLab CI

```yaml
e2e-tests:
  stage: test
  image: docker:24
  services:
    - docker:24-dind
  script:
    - cd docker/from-scratch
    - ./run-from-scratch.sh --verbose
  artifacts:
    when: always
    paths:
      - docker/from-scratch/test-results/
    expire_in: 1 week
  timeout: 30 minutes
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any

    stages {
        stage('E2E Tests') {
            steps {
                dir('docker/from-scratch') {
                    sh './run-from-scratch.sh --verbose'
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'docker/from-scratch/test-results/**/*'
        }
    }
}
```

## Troubleshooting

### Build Fails

**Symptom**: Docker build fails with dependency errors

**Solution**:
```bash
# Clean Docker cache and rebuild
docker builder prune -f
./run-from-scratch.sh --clean
```

### Cluster Won't Start

**Symptom**: Controllers fail to start or won't form cluster

**Solution**:
```bash
# Check controller logs
docker compose -f docker-compose.from-scratch.yml logs flurm-ctrl-1

# Ensure ports are free
lsof -i :6817 -i :6827 -i :6837

# Clean up and restart
./run-from-scratch.sh --clean
```

### Tests Time Out

**Symptom**: Tests hang or time out waiting for cluster

**Solution**:
```bash
# Check if cluster is healthy
docker compose -f docker-compose.from-scratch.yml ps

# Check node connectivity
docker compose -f docker-compose.from-scratch.yml exec test-runner bash
# Inside container:
nc -z flurm-ctrl-1 6817 && echo "OK" || echo "FAIL"
```

### MUNGE Authentication Errors

**Symptom**: SLURM client commands fail with authentication errors

**Solution**:
```bash
# Verify munge key is shared
docker compose -f docker-compose.from-scratch.yml exec test-runner bash
# Inside container:
ls -la /etc/munge/
munge -n </dev/null && echo "MUNGE OK"
```

### Out of Memory

**Symptom**: Docker or containers crash due to memory

**Solution**:
```bash
# Increase Docker memory limit (Docker Desktop)
# Settings -> Resources -> Memory -> 8GB or more

# Or reduce the number of nodes:
# Edit docker-compose.from-scratch.yml to comment out some nodes
```

## Architecture Notes

### Dockerfile Stages

1. **builder**: Clones repo, installs dependencies, compiles Erlang code
2. **runtime**: Minimal image with release, SLURM tools, and MUNGE

### Multi-Mode Entrypoint

The entrypoint script (`/entrypoint.sh`) supports multiple modes:

- `controller`: Runs FLURM controller daemon
- `node`: Runs FLURM compute node daemon
- `test`: Runs test suite and exits
- `shell`: Drops to interactive bash shell

Mode is set via `FLURM_MODE` environment variable.

### Health Checks

- Controllers: TCP connectivity to port 6817
- Nodes: Presence of Erlang BEAM process
- Full cluster: All 6 nodes healthy

### Volumes

- `munge-key`: Shared MUNGE authentication key
- `flurm-ctrl-*-data`: Controller Raft state
- `flurm-ctrl-*-logs`: Controller logs
- `flurm-node-*-data`: Node state

## Requirements

- Docker 24.0+ with Compose v2
- 8GB+ RAM recommended
- 10GB+ disk space for images
- Linux, macOS, or Windows with WSL2

## License

FLURM is licensed under the Apache License 2.0. See the LICENSE file in the root of the repository.
