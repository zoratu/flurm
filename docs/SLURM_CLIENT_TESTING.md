# Testing FLURM with Real SLURM Clients

This document explains how to test FLURM's SLURM protocol compatibility using real SLURM command-line tools (sbatch, squeue, scancel, etc.).

## Prerequisites

### Option 1: Install SLURM with auth/none (Development Only)

For development and testing, you can build SLURM with the `auth/none` plugin which disables authentication:

```bash
# Build SLURM from source with auth/none support
./configure --prefix=/usr/local/slurm-dev --with-auth_none
make
make install

# Configure slurm.conf with:
# AuthType=auth/none
# SlurmctldPort=6817
```

**Warning**: Never use `auth/none` in production. It provides no security.

### Option 2: Use SLURM Docker Container

```bash
# Run SLURM in Docker
docker run -it --rm -p 6817:6817 giovtorres/slurm-docker-cluster
```

### Option 3: Capture Real Traffic

Use the provided packet capture tool to analyze traffic from an existing SLURM installation:

```bash
# On a machine with SLURM installed
cd flurm
./tools/capture_slurm_protocol.escript 16817

# In another terminal, point SLURM client at the capture server
SLURM_CONF=/dev/null squeue --cluster=localhost:16817
```

## Testing Procedure

### 1. Start FLURM Controller

```bash
cd flurm
rebar3 shell --apps flurm_controller

# In the Erlang shell:
flurm_controller:start().
```

### 2. Configure SLURM Clients

Create a minimal `slurm.conf` pointing to FLURM:

```ini
# /tmp/slurm.conf
ClusterName=flurm-test
SlurmctldHost=localhost
SlurmctldPort=6817
AuthType=auth/none
```

### 3. Test Basic Commands

```bash
export SLURM_CONF=/tmp/slurm.conf

# Test connectivity
squeue

# Test job submission
sbatch --wrap="echo hello" -p batch

# Test job cancellation
scancel 1

# Test node info
sinfo

# Test interactive jobs (srun)
srun hostname              # Display node hostname
srun echo "Hello World"    # Run command and get output
srun /path/to/script.sh    # Run a script interactively
srun true                  # Command that exits successfully
srun false                 # Command that exits with code 1
```

## Protocol Analysis Tool

The `tools/capture_slurm_protocol.escript` tool captures and decodes SLURM protocol messages:

```bash
./tools/capture_slurm_protocol.escript [port]
```

Example output:

```
=== Connection from 127.0.0.1:54321 ===

--- Message #1 (Length: 24 bytes) ---
Header:
  Version:     22050 (22.05)
  Flags:       0x0001
  Msg Index:   0
  Msg Type:    2003 (REQUEST_JOB_INFO)
  Body Length: 12
Body: 12 bytes
  0000: 00 00 00 00 00 00 00 00 00 00 00 00              ............
```

## Message Types Reference

| Code | Name | Description |
|------|------|-------------|
| 1008 | REQUEST_PING | Health check |
| 2003 | REQUEST_JOB_INFO | Query jobs (squeue) |
| 2007 | REQUEST_NODE_INFO | Query nodes (sinfo) |
| 2009 | REQUEST_PARTITION_INFO | Query partitions |
| 4003 | REQUEST_SUBMIT_BATCH_JOB | Submit job (sbatch) |
| 4006 | REQUEST_CANCEL_JOB | Cancel job (scancel) |
| 8001 | RESPONSE_SLURM_RC | Generic return code |

## Automated Compatibility Test Suite

FLURM now includes an automated SLURM compatibility test suite with **105/108 tests passing** (3 expected skips). The tests are derived from the [SchedMD SLURM testsuite](https://github.com/SchedMD/slurm/tree/master/testsuite) and cover sinfo, sbatch, squeue, scontrol, and scancel.

```bash
# Run in Docker
cd docker && docker compose up -d
./slurm_compat_tests.sh
```

See [SLURM Compatibility Testing](SLURM_COMPAT_TESTING.md) for full details.

## Known Compatibility Issues

1. **Authentication**: Real SLURM clients expect Munge authentication by default. Use `AuthType=auth/none` for testing.

2. **Protocol Version**: FLURM implements SLURM protocol version 22.05 (0x2600). Clients using newer versions may send additional fields.

3. **Message Fields**: Some advanced fields in batch job requests are not fully parsed. Basic job submission works, but advanced features may have limited support.

4. **srun I/O Forwarding**: srun now works with I/O forwarding. The node daemon connects back to srun's I/O port to forward stdout/stderr. Task exit codes are properly converted from Erlang format to SLURM's expected waitpid format.

5. **srun "don't know rc for type 6002" Warning**: SLURM 22.05 clients display this benign warning when receiving RESPONSE_LAUNCH_TASKS. This is a limitation in SLURM 22.05's `slurm_get_return_code()` function which doesn't handle this message type. The warning is harmless - srun defaults to success (0) and the actual return code is properly checked in the launch plugin. Newer SLURM versions may not show this warning.

## Running Integration Tests

The automated integration tests simulate SLURM client behavior:

```bash
cd flurm
rebar3 eunit --module=flurm_protocol_client_tests
```

These tests verify:
- Ping request/response
- Job info queries
- Batch job submission
- Job cancellation
