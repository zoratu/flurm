# FLURM Usage Guide

This guide explains how to use FLURM exactly as you would use SLURM. FLURM is designed as a drop-in replacement, so all standard SLURM commands work.

## Table of Contents

- [Quick Reference](#quick-reference)
- [Submitting Jobs](#submitting-jobs)
- [Monitoring Jobs](#monitoring-jobs)
- [Managing Jobs](#managing-jobs)
- [Cluster Information](#cluster-information)
- [Job Scripts](#job-scripts)
- [Advanced Features](#advanced-features)
- [Docker Quick Start](#docker-quick-start)

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `sbatch script.sh` | Submit a batch job |
| `srun command` | Run interactive job |
| `squeue` | List jobs in queue |
| `scancel <job_id>` | Cancel a job |
| `sinfo` | Show cluster/partition info |
| `scontrol show job <id>` | Show job details |
| `sacct` | Show accounting info |

---

## Submitting Jobs

### Batch Jobs (sbatch)

Submit a job script:

```bash
sbatch myjob.sh
```

Submit with inline command:

```bash
sbatch --wrap="echo Hello World"
```

Submit with options:

```bash
sbatch --job-name=mytest \
       --nodes=2 \
       --cpus-per-task=4 \
       --mem=8G \
       --time=01:00:00 \
       --partition=batch \
       myjob.sh
```

### Interactive Jobs (srun)

Run a single command:

```bash
srun hostname
```

Run with resource requests:

```bash
srun --nodes=1 --cpus-per-task=2 --mem=4G ./myprogram
```

Interactive shell:

```bash
srun --pty bash
```

### Common sbatch Options

| Option | Description | Example |
|--------|-------------|---------|
| `--job-name=NAME` | Job name | `--job-name=test` |
| `--nodes=N` | Number of nodes | `--nodes=2` |
| `--ntasks=N` | Number of tasks | `--ntasks=4` |
| `--cpus-per-task=N` | CPUs per task | `--cpus-per-task=2` |
| `--mem=SIZE` | Memory per node | `--mem=8G` |
| `--time=TIME` | Time limit | `--time=01:30:00` |
| `--partition=NAME` | Partition | `--partition=gpu` |
| `--output=FILE` | Output file | `--output=job_%j.out` |
| `--error=FILE` | Error file | `--error=job_%j.err` |
| `--array=RANGE` | Job array | `--array=1-100%10` |

---

## Monitoring Jobs

### List Jobs (squeue)

Show all jobs:

```bash
squeue
```

Show your jobs only:

```bash
squeue -u $USER
```

Show specific job:

```bash
squeue -j 12345
```

Show jobs in partition:

```bash
squeue -p gpu
```

Custom output format:

```bash
squeue -o "%.10i %.9P %.20j %.8u %.8T %.10M %.9l %.6D %R"
```

### Job States

| State | Code | Description |
|-------|------|-------------|
| PENDING | PD | Waiting for resources |
| RUNNING | R | Currently executing |
| COMPLETED | CD | Finished successfully |
| FAILED | F | Exited with error |
| CANCELLED | CA | Cancelled by user |
| TIMEOUT | TO | Exceeded time limit |
| SUSPENDED | S | Suspended |

---

## Managing Jobs

### Cancel Jobs (scancel)

Cancel a specific job:

```bash
scancel 12345
```

Cancel all your jobs:

```bash
scancel -u $USER
```

Cancel jobs by name:

```bash
scancel --name=mytest
```

Cancel pending jobs only:

```bash
scancel -t PENDING -u $USER
```

### Hold/Release Jobs

Hold a pending job:

```bash
scontrol hold 12345
```

Release a held job:

```bash
scontrol release 12345
```

### Requeue Jobs

Requeue a failed job:

```bash
scontrol requeue 12345
```

### Suspend/Resume Jobs

Suspend a running job:

```bash
scontrol suspend 12345
```

Resume a suspended job:

```bash
scontrol resume 12345
```

### Signal Jobs

Send signal to job:

```bash
scancel -s SIGUSR1 12345
```

---

## Cluster Information

### Partition/Node Status (sinfo)

Show all partitions:

```bash
sinfo
```

Show node details:

```bash
sinfo -N -l
```

Show specific partition:

```bash
sinfo -p batch
```

### Job Details (scontrol)

Show job details:

```bash
scontrol show job 12345
```

Show partition details:

```bash
scontrol show partition batch
```

Show node details:

```bash
scontrol show node node001
```

### Accounting (sacct)

Show completed jobs:

```bash
sacct
```

Show specific job:

```bash
sacct -j 12345
```

Show job with details:

```bash
sacct -j 12345 --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS
```

---

## Job Scripts

### Basic Job Script

```bash
#!/bin/bash
#SBATCH --job-name=myjob
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --output=output_%j.log
#SBATCH --error=error_%j.log

echo "Job started on $(hostname) at $(date)"
echo "Job ID: $SLURM_JOB_ID"

# Your commands here
./myprogram

echo "Job completed at $(date)"
```

### Multi-Node MPI Job

```bash
#!/bin/bash
#SBATCH --job-name=mpi_job
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --mem-per-cpu=2G
#SBATCH --time=02:00:00
#SBATCH --partition=batch

module load openmpi
mpirun ./my_mpi_program
```

### GPU Job

```bash
#!/bin/bash
#SBATCH --job-name=gpu_job
#SBATCH --nodes=1
#SBATCH --gres=gpu:2
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --partition=gpu

echo "Running on GPUs: $CUDA_VISIBLE_DEVICES"
python train_model.py
```

### Job Array

```bash
#!/bin/bash
#SBATCH --job-name=array_job
#SBATCH --array=1-100%10
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00

echo "Task ID: $SLURM_ARRAY_TASK_ID"
./process_file input_${SLURM_ARRAY_TASK_ID}.dat
```

### Job with Dependencies

```bash
# Submit first job
JOB1=$(sbatch --parsable stage1.sh)

# Submit second job after first completes successfully
JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 stage2.sh)

# Submit cleanup job after second (regardless of success)
sbatch --dependency=afterany:$JOB2 cleanup.sh
```

---

## Advanced Features

### Job Arrays

Submit array with throttling (max 10 concurrent):

```bash
sbatch --array=1-1000%10 array_job.sh
```

Array with step:

```bash
sbatch --array=0-100:5 array_job.sh  # 0, 5, 10, ..., 100
```

### Dependencies

| Type | Description |
|------|-------------|
| `afterok:jobid` | Run after job succeeds |
| `afternotok:jobid` | Run after job fails |
| `afterany:jobid` | Run after job completes |
| `singleton` | Only one job with same name runs |

### Generic Resources (GRES)

Request GPUs:

```bash
sbatch --gres=gpu:4 job.sh
```

Request specific GPU type:

```bash
sbatch --gres=gpu:a100:2 job.sh
```

### Licenses

Request software license:

```bash
sbatch --licenses=matlab:1 job.sh
```

### Reservations

Submit to reservation:

```bash
sbatch --reservation=maintenance job.sh
```

### QOS (Quality of Service)

Submit with QOS:

```bash
sbatch --qos=high job.sh
```

---

## Docker Quick Start

The fastest way to try FLURM:

```bash
# Clone and start
git clone https://github.com/zoratu/flurm.git
cd flurm/docker

# Start demo environment (controller + 3 nodes + client)
docker compose -f docker-compose.demo.yml up -d

# Wait for startup
sleep 15

# Submit a test job
docker exec flurm-client bash -c 'cat > /tmp/test.sh << "EOF"
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --nodes=1
echo "Hello from FLURM!"
hostname
date
EOF
chmod +x /tmp/test.sh && sbatch /tmp/test.sh'

# Check job status
docker exec flurm-client squeue

# View metrics
curl http://localhost:9090/metrics | grep flurm_jobs

# Clean up
docker compose -f docker-compose.demo.yml down
```

---

## Environment Variables

Jobs have access to these environment variables:

| Variable | Description |
|----------|-------------|
| `SLURM_JOB_ID` | Job ID |
| `SLURM_JOB_NAME` | Job name |
| `SLURM_JOB_NODELIST` | Allocated nodes |
| `SLURM_JOB_NUM_NODES` | Number of nodes |
| `SLURM_NTASKS` | Number of tasks |
| `SLURM_CPUS_PER_TASK` | CPUs per task |
| `SLURM_MEM_PER_NODE` | Memory per node |
| `SLURM_ARRAY_TASK_ID` | Array task ID |
| `SLURM_ARRAY_JOB_ID` | Array job ID |
| `CUDA_VISIBLE_DEVICES` | Assigned GPUs |

---

## FLURM-Specific Features

While FLURM is SLURM-compatible, it offers additional capabilities:

### Prometheus Metrics

```bash
curl http://controller:9090/metrics
```

Key metrics:
- `flurm_jobs_submitted_total` - Total jobs submitted
- `flurm_jobs_completed_total` - Total jobs completed
- `flurm_jobs_running` - Currently running jobs
- `flurm_nodes_up` - Nodes in UP state

### REST API (if enabled)

```bash
# Get bridge status
curl http://controller:8080/api/v1/slurm-bridge/status

# Get cluster info
curl http://controller:8080/api/v1/clusters
```

### Live Configuration Reload

FLURM supports hot-reloading configuration without restart:

```bash
scontrol reconfig
```

---

## Troubleshooting

### Job Won't Start

```bash
# Check why job is pending
scontrol show job <jobid> | grep Reason

# Common reasons:
# - Resources: Not enough CPUs/memory/nodes
# - Priority: Other jobs have higher priority
# - Dependency: Waiting for dependent job
# - QOSMaxJobsPerUser: QOS job limit reached
```

### Job Failed

```bash
# Check exit code
sacct -j <jobid> --format=ExitCode

# Check job output
cat slurm-<jobid>.out
cat slurm-<jobid>.err
```

### Node Issues

```bash
# Check node state
sinfo -N -l

# Check specific node
scontrol show node <nodename>

# View node reason
scontrol show node <nodename> | grep Reason
```

---

## See Also

- [QUICKSTART.md](QUICKSTART.md) - Quick installation guide
- [MIGRATION.md](MIGRATION.md) - Migrating from SLURM
- [OPERATIONS.md](OPERATIONS.md) - Operations guide
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Detailed troubleshooting
