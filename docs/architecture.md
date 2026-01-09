# FLURM Architecture

This document describes the high-level architecture of FLURM, its OTP application structure, process supervision trees, and component interactions.

## Table of Contents

1. [Overview](#overview)
2. [OTP Application Structure](#otp-application-structure)
3. [Process Supervision Trees](#process-supervision-trees)
4. [Core Components](#core-components)
5. [Data Flow](#data-flow)
6. [State Management](#state-management)
7. [Consensus and Replication](#consensus-and-replication)
8. [Failure Handling](#failure-handling)

## Overview

FLURM is built on Erlang/OTP principles, leveraging the actor model for concurrent, fault-tolerant operation. The system is designed as a collection of loosely coupled processes that communicate through message passing.

### Design Principles

1. **Let It Crash**: Processes are designed to fail fast; supervision trees handle recovery
2. **Share Nothing**: Each process maintains its own state; no shared mutable memory
3. **Asynchronous Messaging**: Non-blocking communication between components
4. **Location Transparency**: Processes communicate the same way locally or across nodes

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FLURM Controller                                │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                        flurm_app (Application)                         │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │                    flurm_sup (Root Supervisor)                   │  │ │
│  │  │                         strategy: one_for_one                    │  │ │
│  │  │                                                                   │  │ │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │  │ │
│  │  │  │  Protocol   │  │    Core     │  │  Scheduler  │  │ Metrics │ │  │ │
│  │  │  │    Sup      │  │    Sup      │  │     Sup     │  │   Sup   │ │  │ │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │  │ │
│  │  │                                                                   │  │ │
│  │  └─────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                         │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## OTP Application Structure

FLURM consists of the following OTP applications:

```
flurm/
├── flurm              # Main application
├── flurm_protocol     # SLURM protocol handling
├── flurm_scheduler    # Scheduling algorithms
├── flurm_consensus    # Raft consensus implementation
├── flurm_metrics      # Monitoring and observability
└── flurm_storage      # Persistent storage layer
```

### Application Dependencies

```
                    ┌──────────────┐
                    │    flurm     │
                    │   (main)     │
                    └──────┬───────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
┌────────────────┐ ┌───────────────┐ ┌───────────────┐
│ flurm_protocol │ │flurm_scheduler│ │ flurm_metrics │
└────────┬───────┘ └───────┬───────┘ └───────────────┘
         │                 │
         ▼                 ▼
┌────────────────┐ ┌───────────────┐
│flurm_consensus │ │ flurm_storage │
└────────────────┘ └───────────────┘
         │                 │
         └────────┬────────┘
                  ▼
           ┌────────────┐
           │   mnesia   │
           └────────────┘
```

## Process Supervision Trees

### Root Supervisor (flurm_sup)

```erlang
%% flurm_sup.erl
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    Children = [
        #{id => flurm_protocol_sup,
          start => {flurm_protocol_sup, start_link, []},
          type => supervisor},
        #{id => flurm_core_sup,
          start => {flurm_core_sup, start_link, []},
          type => supervisor},
        #{id => flurm_scheduler_sup,
          start => {flurm_scheduler_sup, start_link, []},
          type => supervisor},
        #{id => flurm_metrics_sup,
          start => {flurm_metrics_sup, start_link, []},
          type => supervisor}
    ],
    {ok, {SupFlags, Children}}.
```

### Full Supervision Tree

```
flurm_sup (one_for_one)
│
├── flurm_protocol_sup (one_for_all)
│   ├── flurm_acceptor_pool (simple_one_for_one)
│   │   └── flurm_connection_handler (per connection)
│   ├── flurm_protocol_decoder
│   └── flurm_protocol_encoder
│
├── flurm_core_sup (rest_for_one)
│   ├── flurm_consensus (gen_statem - Raft)
│   ├── flurm_state_manager (gen_server)
│   ├── flurm_job_manager (gen_server)
│   ├── flurm_node_manager (gen_server)
│   ├── flurm_partition_manager (gen_server)
│   └── flurm_queue_manager (gen_server)
│
├── flurm_scheduler_sup (one_for_one)
│   ├── flurm_scheduler_engine (gen_server)
│   └── flurm_sched_plugin_sup (simple_one_for_one)
│       ├── flurm_sched_fifo
│       ├── flurm_sched_backfill
│       └── flurm_sched_fairshare
│
└── flurm_metrics_sup (one_for_one)
    ├── flurm_prometheus_exporter
    ├── flurm_tracer
    └── flurm_health_checker
```

### Supervisor Strategies Explained

| Supervisor | Strategy | Rationale |
|------------|----------|-----------|
| `flurm_sup` | `one_for_one` | Components are independent; restart only failed child |
| `flurm_protocol_sup` | `one_for_all` | Protocol components are tightly coupled |
| `flurm_core_sup` | `rest_for_one` | Core components depend on each other in order |
| `flurm_scheduler_sup` | `one_for_one` | Scheduler plugins are independent |
| `flurm_acceptor_pool` | `simple_one_for_one` | Dynamic connection handlers |

## Core Components

### 1. Protocol Handler (`flurm_protocol`)

Handles SLURM binary protocol encoding/decoding:

```
┌─────────────────────────────────────────────────────────────┐
│                    Protocol Handler                          │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │  Acceptor   │───>│  Decoder    │───>│  Router         │ │
│  │  (TCP)      │    │  (Binary)   │    │  (gen_server)   │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
│                                                ↓            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │  Socket     │<───│  Encoder    │<───│  Response       │ │
│  │  (Reply)    │    │  (Binary)   │    │  Builder        │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2. Job Manager (`flurm_job_manager`)

Maintains the state of all jobs in the system:

```erlang
%% Job state machine
%%
%%     ┌─────────┐
%%     │ PENDING │ ◄────────────────────────────┐
%%     └────┬────┘                              │
%%          │ schedule                          │ requeue
%%          ▼                                   │
%%     ┌─────────┐                              │
%%     │ RUNNING │ ─────────────────────────────┤
%%     └────┬────┘                              │
%%          │                                   │
%%     ┌────┴────┬───────────┐                 │
%%     │         │           │                 │
%%     ▼         ▼           ▼                 │
%% ┌────────┐ ┌────────┐ ┌────────┐           │
%% │COMPLETE│ │ FAILED │ │TIMEOUT │───────────┘
%% └────────┘ └────────┘ └────────┘
```

### 3. Node Manager (`flurm_node_manager`)

Tracks compute node state and health:

```
┌─────────────────────────────────────────────────────────────┐
│                     Node Manager                             │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Node Registry                      │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │   │
│  │  │ node001 │ │ node002 │ │ node003 │ │   ...   │   │   │
│  │  │ IDLE    │ │ ALLOC   │ │ DOWN    │ │         │   │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                 Heartbeat Monitor                    │   │
│  │         (timeout: 30s, max_failures: 3)             │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4. Scheduler Engine (`flurm_scheduler_engine`)

Orchestrates job scheduling across plugins:

```
┌─────────────────────────────────────────────────────────────┐
│                   Scheduler Engine                           │
│                                                             │
│  ┌─────────┐                                               │
│  │ Timer   │ ────► schedule_cycle()                        │
│  │ (100ms) │                                               │
│  └─────────┘           │                                   │
│                        ▼                                    │
│            ┌───────────────────────┐                       │
│            │   Plugin Selection    │                       │
│            │   (priority order)    │                       │
│            └───────────┬───────────┘                       │
│                        │                                    │
│         ┌──────────────┼──────────────┐                    │
│         ▼              ▼              ▼                    │
│    ┌─────────┐    ┌─────────┐    ┌─────────┐             │
│    │ Backfill│    │FairShare│    │  FIFO   │             │
│    │ Plugin  │    │ Plugin  │    │ Plugin  │             │
│    └─────────┘    └─────────┘    └─────────┘             │
│                        │                                    │
│                        ▼                                    │
│            ┌───────────────────────┐                       │
│            │   Allocation          │                       │
│            │   Decision            │                       │
│            └───────────────────────┘                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Job Submission Flow

```
┌────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Client │───>│ Protocol │───>│   Job    │───>│ Scheduler│
│(sbatch)│    │ Handler  │    │ Manager  │    │ Engine   │
└────────┘    └──────────┘    └──────────┘    └──────────┘
                   │                               │
                   │                               ▼
                   │                         ┌──────────┐
                   │                         │   Node   │
                   │                         │ Manager  │
                   │                         └──────────┘
                   │                               │
                   │         ┌─────────────────────┘
                   │         │
                   ▼         ▼
              ┌──────────────────┐
              │   Compute Node   │
              │     (slurmd)     │
              └──────────────────┘
```

### Detailed Message Flow

```
1. Client -> Controller (TCP)
   ┌─────────────────────────────────────────┐
   │ MSG_REQUEST_SUBMIT_BATCH_JOB            │
   │ ├── job_script: binary                  │
   │ ├── num_tasks: integer                  │
   │ ├── partition: string                   │
   │ └── ... (job parameters)                │
   └─────────────────────────────────────────┘

2. Protocol Handler -> Job Manager (Erlang msg)
   {submit_job, #job_spec{...}}

3. Job Manager -> Scheduler Engine (Erlang msg)
   {schedule_job, JobId}

4. Scheduler Engine -> Node Manager (Erlang msg)
   {allocate_nodes, JobId, Requirements}

5. Node Manager -> Compute Node (TCP)
   ┌─────────────────────────────────────────┐
   │ MSG_REQUEST_LAUNCH_TASKS                │
   │ ├── job_id: integer                     │
   │ ├── job_script: binary                  │
   │ └── ... (execution details)             │
   └─────────────────────────────────────────┘

6. Controller -> Client (TCP)
   ┌─────────────────────────────────────────┐
   │ MSG_RESPONSE_SUBMIT_BATCH_JOB           │
   │ ├── job_id: integer                     │
   │ └── error_code: integer                 │
   └─────────────────────────────────────────┘
```

## State Management

### State Layers

FLURM maintains state at multiple levels:

```
┌─────────────────────────────────────────────────────────────┐
│                    State Architecture                        │
│                                                             │
│  Layer 1: Transient State (Process Memory)                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - Active connections                                 │   │
│  │ - In-flight requests                                 │   │
│  │ - Computed caches                                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Layer 2: Replicated State (Mnesia RAM)                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - Job queue                                          │   │
│  │ - Node states                                        │   │
│  │ - Active allocations                                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Layer 3: Persistent State (Mnesia Disc)                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - Job history                                        │   │
│  │ - Accounting records                                 │   │
│  │ - Configuration                                      │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Mnesia Tables

```erlang
%% Table definitions
-record(flurm_job, {
    id              :: job_id(),
    name            :: binary(),
    state           :: pending | running | completed | failed,
    submit_time     :: timestamp(),
    start_time      :: timestamp() | undefined,
    end_time        :: timestamp() | undefined,
    user            :: binary(),
    partition       :: binary(),
    node_list       :: [node_name()],
    script          :: binary(),
    exit_code       :: integer() | undefined
}).

-record(flurm_node, {
    name            :: node_name(),
    state           :: idle | allocated | down | drain,
    cpus            :: non_neg_integer(),
    memory          :: non_neg_integer(),
    features        :: [binary()],
    partitions      :: [binary()],
    last_heartbeat  :: timestamp()
}).

-record(flurm_partition, {
    name            :: binary(),
    nodes           :: [node_name()],
    max_time        :: non_neg_integer(),
    default         :: boolean(),
    state           :: up | down
}).
```

## Consensus and Replication

### Raft Implementation

FLURM uses Raft consensus for leader election and log replication:

```
┌─────────────────────────────────────────────────────────────┐
│                    Raft Consensus                            │
│                                                             │
│  Controller 1          Controller 2          Controller 3   │
│  ┌──────────┐          ┌──────────┐          ┌──────────┐  │
│  │  LEADER  │ =======> │ FOLLOWER │          │ FOLLOWER │  │
│  │          │          │          │          │          │  │
│  │ Log: 1-5 │          │ Log: 1-5 │          │ Log: 1-5 │  │
│  └──────────┘          └──────────┘          └──────────┘  │
│       │                      ▲                     ▲        │
│       │                      │                     │        │
│       └──────────────────────┴─────────────────────┘        │
│                    AppendEntries RPC                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### State Machine Replication

```erlang
%% Consensus state machine
-type raft_state() :: follower | candidate | leader.

-record(raft, {
    state           :: raft_state(),
    current_term    :: non_neg_integer(),
    voted_for       :: node() | undefined,
    log             :: [log_entry()],
    commit_index    :: non_neg_integer(),
    last_applied    :: non_neg_integer(),
    %% Leader state
    next_index      :: #{node() => non_neg_integer()},
    match_index     :: #{node() => non_neg_integer()}
}).
```

## Failure Handling

### Controller Failover

```
Timeline of Leader Failure:

T+0ms:    Leader fails
          ┌──────────┐
          │  LEADER  │ ✗
          │ (dead)   │
          └──────────┘

T+150ms:  Election timeout on followers
          ┌──────────┐    ┌──────────┐
          │CANDIDATE │    │ FOLLOWER │
          │(term: 2) │───>│          │
          └──────────┘    └──────────┘

T+200ms:  New leader elected
          ┌──────────┐    ┌──────────┐
          │  LEADER  │<───│ FOLLOWER │
          │(term: 2) │    │          │
          └──────────┘    └──────────┘

T+250ms:  Client requests redirected
          Cluster fully operational
```

### Process Failure Recovery

```
Supervisor Action on Child Failure:

1. flurm_job_manager crashes
   ├── flurm_core_sup detects failure
   ├── Restarts flurm_job_manager
   ├── New process reads state from Mnesia
   └── Operations resume

2. flurm_connection_handler crashes
   ├── flurm_acceptor_pool detects failure
   ├── Connection terminated (client will reconnect)
   └── No state loss (stateless handler)

3. flurm_scheduler_engine crashes
   ├── flurm_scheduler_sup detects failure
   ├── Restarts scheduler
   ├── Resumes from last checkpoint
   └── In-progress schedules are re-evaluated
```

## Component Interaction Summary

```
┌─────────────────────────────────────────────────────────────┐
│              Component Interaction Matrix                    │
├─────────────────┬────────┬────────┬────────┬───────┬───────┤
│                 │Protocol│ JobMgr │NodeMgr │SchedEng│State │
├─────────────────┼────────┼────────┼────────┼───────┼───────┤
│ Protocol        │   -    │  sync  │   -    │   -   │   -   │
│ JobManager      │  sync  │   -    │ async  │ async │ sync  │
│ NodeManager     │   -    │ async  │   -    │ async │ sync  │
│ SchedulerEngine │   -    │ async  │ async  │   -   │ sync  │
│ StateManager    │   -    │  sync  │  sync  │ sync  │   -   │
└─────────────────┴────────┴────────┴────────┴───────┴───────┘

Legend:
- sync:  Synchronous call (gen_server:call)
- async: Asynchronous cast (gen_server:cast)
- -:     No direct communication
```

---

See also:
- [Protocol Reference](protocol.md) for wire protocol details
- [Development Guide](development.md) for implementation patterns
