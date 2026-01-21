# FLURM Demo Environment

This demo showcases FLURM's capabilities as a drop-in SLURM replacement. It runs a complete cluster with multiple compute nodes using standard SLURM client tools.

## Quick Start

```bash
cd docker

# Build and start the cluster (1 controller + 3 compute nodes + 1 client)
docker-compose -f docker-compose.demo.yml up --build -d

# Wait for services to start (check logs)
docker-compose -f docker-compose.demo.yml logs -f flurm-server

# Connect to the client container
docker exec -it flurm-client bash

# Run all demo jobs
./run_all_demos.sh
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌─────────────────┐                                            │
│  │  flurm-client   │   sbatch/squeue/scancel                    │
│  │  (submit-host)  │──────────────┐                             │
│  └─────────────────┘              │                             │
│                                   ▼                             │
│                         ┌─────────────────┐                     │
│                         │ flurm-controller│  Port 6817 (SLURM)  │
│                         │   (Erlang/OTP)  │  Port 9090 (metrics)│
│                         └────────┬────────┘                     │
│                                  │                              │
│            ┌─────────────────────┼─────────────────────┐        │
│            ▼                     ▼                     ▼        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   node001       │  │   node002       │  │   node003       │  │
│  │   4 CPUs        │  │   4 CPUs        │  │   8 CPUs + GPU  │  │
│  │   4GB RAM       │  │   4GB RAM       │  │   8GB RAM       │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Demo Jobs

### 1. Data Processing (`demo_data_processing.sh`)
Realistic data pipeline that:
- Generates CSV input data
- Computes statistics using AWK
- Outputs JSON summary report

```bash
sbatch demo_data_processing.sh
```

### 2. Failing Job (`demo_failing_job.sh`)
Demonstrates error handling:
- Runs initialization steps
- Simulates critical failure
- Exits with non-zero status

```bash
sbatch demo_failing_job.sh
squeue  # Watch for FAILED state
```

### 3. Job Array (`demo_job_array.sh`)
Parallel task processing:
- 10 array tasks
- Max 3 concurrent (`%3`)
- Each task processes different data

```bash
sbatch demo_job_array.sh
squeue  # Shows array tasks: JOBID_1, JOBID_2, etc.
```

### 4. Dependency Chain (`demo_dependency_chain.sh`)
4-stage pipeline with dependencies:
1. Generate → 2. Validate → 3. Process → 4. Report

```bash
# Generate job scripts
./demo_dependency_chain.sh

# Submit with dependencies
JOB1=$(sbatch --parsable /tmp/job1_generate.sh)
JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 /tmp/job2_validate.sh)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 /tmp/job3_process.sh)
JOB4=$(sbatch --parsable --dependency=afterok:$JOB3 /tmp/job4_report.sh)

# Monitor pipeline
squeue -j $JOB1,$JOB2,$JOB3,$JOB4
```

### 5. Retry Job (`demo_retry_job.sh`)
Transient failure handling:
- Fails on attempts 1 and 2
- Succeeds on attempt 3
- Uses `--requeue` for automatic retry

```bash
sbatch demo_retry_job.sh
# Watch for REQUEUED → PENDING → RUNNING → COMPLETED
```

### 6. Resource Intensive (`demo_resource_intensive.sh`)
Tests resource allocation:
- 4 CPUs with parallel workers
- 1GB memory allocation
- GPU detection
- I/O throughput benchmarks

```bash
sbatch demo_resource_intensive.sh
```

### 7. Timeout Job (`demo_timeout_job.sh`)
Timeout handling demonstration:
- 30-second time limit
- 60-second work (will timeout)
- Receives SIGUSR1 10 seconds before kill
- Writes checkpoint file

```bash
sbatch demo_timeout_job.sh
# Watch for timeout → checkpoint saved
```

## Monitoring Commands

```bash
# Queue status
squeue
squeue -l           # Long format
squeue -u $USER     # Your jobs only

# Job details
scontrol show job <JOBID>

# Node information
sinfo
sinfo -N -l         # Node details

# Job accounting (after completion)
sacct -j <JOBID>
sacct --format=JobID,JobName,State,ExitCode,Elapsed

# Cancel jobs
scancel <JOBID>
scancel -u $USER    # All your jobs

# Watch job output
tail -f /tmp/*.out
```

## Viewing Results

All job outputs go to `/tmp/`:

```bash
# List all output files
ls -la /tmp/*.out /tmp/*.err /tmp/*.json /tmp/*.txt 2>/dev/null

# View data processing results
cat /tmp/data_processing_*.out
cat /tmp/job_*_data/summary.json

# View array task outputs
cat /tmp/array_*_*.out

# View retry job progress
cat /tmp/retry_*.out

# View timeout checkpoint
cat /tmp/timeout_checkpoint_*.json
```

## Cleanup

```bash
# Stop containers
docker-compose -f docker-compose.demo.yml down

# Remove volumes (clean slate)
docker-compose -f docker-compose.demo.yml down -v

# Remove images
docker-compose -f docker-compose.demo.yml down --rmi all
```

## Troubleshooting

### Jobs stuck in PENDING
```bash
# Check partition state
sinfo

# Check node availability
scontrol show node

# Check job requirements vs. available resources
scontrol show job <JOBID> | grep -E "NumNodes|NumCPUs|MinMemory"
```

### Connection refused
```bash
# Check controller is running
docker-compose -f docker-compose.demo.yml ps
docker logs flurm-controller

# Verify network connectivity
docker exec flurm-client ping flurm-server
```

### MUNGE authentication errors
```bash
# Check MUNGE key is shared
docker exec flurm-controller ls -la /etc/munge/
docker exec flurm-client ls -la /etc/munge/

# Test MUNGE
docker exec flurm-client munge -n
```
