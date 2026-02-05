# FLURM Quick Start Guide

Get FLURM running in 5 minutes.

## Prerequisites

- Erlang/OTP 26+ (verify with `erl -version`)
- rebar3 (install: `curl -O https://rebar3.s3.amazonaws.com/rebar3 && chmod +x rebar3`)
- Linux or macOS

## Option 1: Docker (Fastest)

```bash
# Clone and start
git clone https://github.com/zoratu/flurm.git
cd flurm/docker

# Start single-node FLURM
docker compose -f docker-compose.yml up -d

# Verify it's running
curl http://localhost:6820/health
```

**Test with SLURM clients:**
```bash
# Enter the client container
docker compose exec slurm-client bash

# Submit a job
sbatch --wrap="echo Hello FLURM"

# Check job status
squeue

# View job output
cat slurm-*.out
```

## Option 2: Local Build

```bash
# Clone and build
git clone https://github.com/zoratu/flurm.git
cd flurm
rebar3 release

# Start controller (foreground for testing)
_build/default/rel/flurmctld/bin/flurmctld foreground
```

In another terminal:
```bash
# Start a compute node
_build/default/rel/flurmd/bin/flurmd foreground
```

## Submit Your First Job

Create `test.sh`:
```bash
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --nodes=1
#SBATCH --time=00:01:00

echo "Hello from FLURM!"
hostname
sleep 10
echo "Done!"
```

Submit and monitor:
```bash
sbatch test.sh          # Submit job
squeue                  # List jobs
scontrol show job 1     # Job details
sacct -j 1              # Accounting info
```

## Key Ports

| Port | Service | Protocol |
|------|---------|----------|
| 6817 | Controller (slurmctld compat) | SLURM TCP |
| 6818 | Node Daemon (slurmd compat) | SLURM TCP |
| 6820 | REST API / Health | HTTP |

## Configuration

Minimal `/etc/flurm/flurm.conf`:
```ini
ClusterName=mycluster
ControllerHost=localhost
ControllerPort=6817
```

For detailed configuration, see [deployment.md](deployment.md).

## What's Different from SLURM?

FLURM is designed to be drop-in compatible with SLURM clients while offering:

- **No external database required** - Built-in distributed state
- **Active-active HA** - All controllers handle requests
- **Faster failover** - Target <30s leader election
- **Native federation** - Built-in multi-cluster support

See [SLURM_DIFFERENCES.md](SLURM_DIFFERENCES.md) for details.

## Next Steps

- [Deployment Guide](deployment.md) - Production deployment
- [Migration Guide](MIGRATION.md) - Migrate from SLURM
- [Operations Guide](OPERATIONS.md) - Day-to-day operations
- [Architecture](architecture.md) - System design

## Troubleshooting

**Controller won't start:**
```bash
# Check logs
tail -f /var/log/flurm/controller.log

# Verify port availability
netstat -ln | grep 6817
```

**Jobs stuck in PENDING:**
```bash
# Check node status
sinfo

# Check specific job
scontrol show job <jobid>

# Verify node connectivity
scontrol ping
```

**MUNGE authentication errors:**
```bash
# Verify MUNGE is running
systemctl status munge

# Test MUNGE
munge -n | unmunge
```

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for more.
