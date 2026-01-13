# FLURM Architecture

This document describes the internal architecture of FLURM.

## Overview

FLURM is built as an Erlang/OTP umbrella application with the following apps:

| App | Description |
|-----|-------------|
| `flurm_protocol` | SLURM binary protocol codec |
| `flurm_core` | Domain logic (jobs, nodes, scheduling) |
| `flurm_controller` | Controller daemon (slurmctld equivalent) |
| `flurm_node_daemon` | Node daemon (slurmd equivalent) |
| `flurm_dbd` | Accounting daemon (slurmdbd equivalent) |
| `flurm_db` | Persistence layer (Raft + Mnesia) |
| `flurm_config` | Configuration management |

## Application Dependencies

```mermaid
graph TD
    A[flurm_controller] --> B[flurm_core]
    A --> C[flurm_db]
    A --> D[flurm_dbd]
    B --> E[flurm_protocol]
    C --> E
    D --> E
    E --> F[flurm_config]
```

## Core Components

### flurm_protocol

Handles SLURM binary protocol encoding/decoding:

- **flurm_protocol_codec** - Main codec for all message types
- **Message types supported**:
  - Job operations (4001-4029)
  - Node operations (1001-1029)
  - Information queries (2001-2058)
  - Job steps (5001-5041)
  - Accounting (various)

### flurm_core

Core domain logic:

- **flurm_job** - Job state machine (gen_statem)
  - States: pending → configuring → running → completing → completed/failed/cancelled
  - Handles job lifecycle transitions

- **flurm_scheduler** - Scheduling engine
  - FIFO and backfill scheduling
  - Priority-based ordering
  - Fair-share calculations

- **flurm_node** - Node process (gen_server)
  - Resource tracking
  - Heartbeat handling
  - Job slot allocation

- **flurm_account_manager** - Accounting entity management
  - Accounts, users, associations
  - QOS and TRES tracking

### flurm_controller

Controller daemon implementation:

- **flurm_controller_acceptor** - Ranch TCP acceptor
- **flurm_controller_handler** - RPC request handler
- **flurm_controller_cluster** - Multi-controller coordination

### flurm_db

Persistence and consensus:

- **flurm_db_raft** - Ra (Raft) state machine
- **flurm_db_mnesia** - Mnesia backend for distributed storage

### flurm_dbd

Accounting daemon:

- **flurm_dbd_server** - Main accounting server
- **flurm_dbd_storage** - Storage backend (ETS/Mnesia)
- **flurm_dbd_acceptor** - TCP connection handler

## Process Architecture

### Controller Supervision Tree

```mermaid
graph TD
    A[flurmctld_app] --> B[flurmctld_sup<br/>one_for_one]
    B --> C[flurm_cluster_sup<br/>one_for_all]
    B --> D[flurm_network_sup<br/>one_for_one]
    B --> E[flurm_domain_sup<br/>rest_for_one]
    B --> F[flurm_scheduler_sup<br/>one_for_one]

    C --> C1[flurm_ra_server]
    C --> C2[flurm_cluster_monitor]

    D --> D1[ranch_listener]
    D --> D2[flurm_protocol_router]

    E --> E1[flurm_config_server]
    E --> E2[flurm_partition_sup]
    E --> E3[flurm_node_sup]
    E --> E4[flurm_job_sup]
    E --> E5[flurm_account_manager]

    F --> F1[flurm_scheduler_server]
```

## Message Flow

### Job Submission

```mermaid
sequenceDiagram
    participant Client
    participant Ranch as Ranch Acceptor
    participant Decoder as Protocol Decoder
    participant Handler as Controller Handler
    participant JobSup as Job Supervisor
    participant Job as Job Process
    participant Sched as Scheduler
    participant Node as Node Process
    participant Daemon as Node Daemon

    Client->>Ranch: TCP Connect
    Ranch->>Decoder: Raw bytes
    Decoder->>Handler: Decoded message
    Handler->>JobSup: spawn job
    JobSup->>Job: start_link
    Job->>Sched: register pending
    Sched->>Node: allocate resources
    Node->>Daemon: launch task
    Daemon-->>Client: Job started
```

### Consensus Replication

```mermaid
sequenceDiagram
    participant Leader as Leader Controller
    participant RaL as Ra Server (Leader)
    participant Log as Raft Log
    participant RaF as Ra Server (Follower)
    participant Follower as Follower Controller

    Leader->>RaL: Command
    RaL->>Log: Append entry
    Log->>RaF: Replicate
    RaF->>Follower: Apply
    RaF-->>RaL: Ack
    RaL-->>Leader: Committed
```

## State Management

### Job States

```mermaid
stateDiagram-v2
    [*] --> PENDING: submit
    PENDING --> CONFIGURING: allocate resources
    CONFIGURING --> RUNNING: launch tasks
    CONFIGURING --> FAILED: launch error
    CONFIGURING --> CANCELLED: user cancel
    RUNNING --> COMPLETING: tasks done
    RUNNING --> FAILED: task error
    RUNNING --> CANCELLED: user cancel
    COMPLETING --> COMPLETED: cleanup done
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

### Node States

```mermaid
stateDiagram-v2
    [*] --> UNKNOWN: initial
    UNKNOWN --> UP: registration
    UP --> DRAIN: admin drain
    UP --> DOWN: failure/timeout
    DRAIN --> UP: resume
    DOWN --> UP: recovery
```

## Configuration Hot Reload

```mermaid
flowchart TD
    A[scontrol reconfigure] --> B[flurm_config_server:reload]
    B --> C[Parse slurm.conf / sys.config]
    C --> D[Diff with current config]
    D --> E[Update partitions]
    D --> F[Update nodes]
    E --> G[Notify scheduler]
    F --> H[Update allocations]
```

## Fault Tolerance

### Controller Failover

1. Leader heartbeats to followers
2. Followers detect timeout
3. New leader election via Raft
4. Leader takes over job scheduling
5. Clients reconnect automatically

### Node Failure Handling

1. Node misses heartbeats
2. Controller marks node DOWN
3. Jobs on node marked FAILED
4. Jobs eligible for requeue
5. Resources deallocated

### Split-Brain Prevention

Ra (Raft) ensures only one leader can exist:
- Majority quorum required for commits
- Old leader cannot commit after partition
- State converges when partition heals

## Performance Characteristics

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Job submission | < 1ms | 50,000/sec |
| Scheduler decision | < 100us | 10,000/sec |
| Protocol encode | < 50us | 100,000/sec |
| Raft commit | < 10ms | 1,000/sec |
| Failover | < 1 sec | N/A |

## Resource Management

### Memory

- Each job process: ~10KB
- Each node process: ~5KB
- ETS tables: Proportional to cluster size
- Raft log: Configurable, typically < 100MB

### CPU

- Protocol codec: CPU-bound during high throughput
- Scheduler: Periodic bursts during scheduling cycles
- Raft: Low CPU, mostly I/O wait
