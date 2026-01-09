# FLURM

**Fault-tolerant Linux Utility for Resource Management**

An Erlang-based, SLURM-compatible job scheduler designed for high availability, zero-downtime operations, and seamless horizontal scaling.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Erlang/OTP](https://img.shields.io/badge/Erlang%2FOTP-26%2B-red.svg)](https://www.erlang.org/)

## Overview

FLURM is a next-generation workload manager that speaks the SLURM protocol while leveraging Erlang/OTP's battle-tested concurrency primitives. It provides fault tolerance, hot code reloading, and distributed consensus without the operational complexity of traditional HPC schedulers.

### Key Features

- **Hot Code Reload**: Update scheduler logic without dropping jobs or connections
- **Zero-Downtime Failover**: Multi-controller consensus with automatic leader election
- **Dynamic Scaling**: Add or remove compute nodes without cluster restarts
- **No Global Locks**: Lock-free scheduling using Erlang's actor model
- **SLURM Protocol Compatible**: Drop-in replacement for existing SLURM clients
- **Built-in Observability**: Prometheus metrics, distributed tracing, and live introspection
- **Deterministic Testing**: TLA+ specifications and simulation-based testing

## Quick Start

### Prerequisites

- Erlang/OTP 26 or later
- rebar3
- (Optional) Docker for containerized deployment

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/flurm.git
cd flurm

# Build the release
rebar3 release

# Start a single-node cluster
_build/default/rel/flurm/bin/flurm foreground
```

### Basic Usage

```bash
# Submit a job (using standard SLURM commands)
sbatch --wrap="echo Hello FLURM"

# Check job status
squeue

# View node status
sinfo

# Cancel a job
scancel <job_id>
```

### Configuration

Create a `flurm.config` file:

```erlang
[
  {flurm, [
    {cluster_name, "my-cluster"},
    {controllers, [
      {"controller1.example.com", 6817},
      {"controller2.example.com", 6817},
      {"controller3.example.com", 6817}
    ]},
    {slurmd_port, 6818},
    {slurmctld_port, 6817},
    {scheduler_plugin, flurm_sched_backfill},
    {checkpoint_interval, 60000}
  ]}
].
```

## Architecture Overview

```
                                    FLURM Architecture
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                           Client Layer                                   │
    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
    │  │ sbatch  │  │ squeue  │  │ scancel │  │  sinfo  │  │ scontrol│       │
    │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
    │       └────────────┴───────────┬┴───────────┴────────────┘             │
    └────────────────────────────────┼────────────────────────────────────────┘
                                     │ SLURM Protocol (TCP)
    ┌────────────────────────────────┼────────────────────────────────────────┐
    │                        Controller Layer                                  │
    │  ┌─────────────────────────────┴─────────────────────────────┐          │
    │  │                    Protocol Decoder                        │          │
    │  │              (flurm_protocol.erl)                         │          │
    │  └─────────────────────────────┬─────────────────────────────┘          │
    │                                │                                         │
    │  ┌──────────────┬──────────────┼──────────────┬──────────────┐          │
    │  │              │              │              │              │          │
    │  ▼              ▼              ▼              ▼              ▼          │
    │ ┌────────┐  ┌────────┐  ┌──────────┐  ┌────────┐  ┌──────────┐         │
    │ │  Job   │  │ Queue  │  │Scheduler │  │ Node   │  │Partition │         │
    │ │Manager │  │Manager │  │  Engine  │  │Manager │  │ Manager  │         │
    │ └────┬───┘  └────┬───┘  └────┬─────┘  └────┬───┘  └────┬─────┘         │
    │      │           │           │             │           │               │
    │      └───────────┴───────────┼─────────────┴───────────┘               │
    │                              │                                          │
    │  ┌───────────────────────────┴───────────────────────────────┐         │
    │  │                    State Manager                           │         │
    │  │         (Raft Consensus + Mnesia Replication)             │         │
    │  └───────────────────────────────────────────────────────────┘         │
    └────────────────────────────────┬────────────────────────────────────────┘
                                     │
    ┌────────────────────────────────┼────────────────────────────────────────┐
    │                         Compute Layer                                    │
    │                                │                                         │
    │      ┌─────────────────────────┼─────────────────────────────┐          │
    │      │                         │                             │          │
    │      ▼                         ▼                             ▼          │
    │  ┌────────┐              ┌────────┐                    ┌────────┐       │
    │  │ Node 1 │              │ Node 2 │        ...         │ Node N │       │
    │  │(slurmd)│              │(slurmd)│                    │(slurmd)│       │
    │  └────────┘              └────────┘                    └────────┘       │
    │                                                                          │
    └──────────────────────────────────────────────────────────────────────────┘
```

## SLURM vs FLURM Capabilities

| Feature | SLURM | FLURM |
|---------|-------|-------|
| Job Scheduling | Yes | Yes |
| Fair Share | Yes | Yes |
| Backfill Scheduling | Yes | Yes |
| Preemption | Yes | Yes |
| Job Arrays | Yes | Yes |
| Node Health Monitoring | Yes | Yes |
| Accounting | Yes | Yes |
| **Hot Code Reload** | No | **Yes** |
| **Zero-Downtime Upgrades** | No | **Yes** |
| **Lock-Free Scheduling** | No | **Yes** |
| **Built-in Consensus** | Limited | **Raft** |
| **Live State Inspection** | Limited | **Full REPL** |
| **Deterministic Testing** | No | **TLA+ & SimTest** |
| **Protocol Version** | 23.x compatible | 23.x compatible |
| Max Controllers | 2 (active/passive) | Unlimited (Raft) |
| Failover Time | 30-60 seconds | < 1 second |
| Language | C | Erlang/OTP |

## Documentation

- [Architecture Overview](docs/architecture.md) - System design and OTP structure
- [Protocol Reference](docs/protocol.md) - SLURM binary protocol details
- [Testing Guide](docs/testing.md) - How to test FLURM
- [Deployment Guide](docs/deployment.md) - Production deployment
- [Development Guide](docs/development.md) - Contributing to FLURM
- [AI Agent Guide](docs/AGENT_GUIDE.md) - Guide for AI-assisted development

## Project Status

FLURM is currently in active development. The following components are implemented:

- [x] SLURM protocol decoder/encoder
- [x] Basic job submission and management
- [x] Node registration and heartbeat
- [x] Partition management
- [x] Fair share scheduler
- [ ] Accounting integration
- [ ] Federation support
- [ ] GPU scheduling (GRES)
- [ ] Burst buffer support

## Contributing

We welcome contributions! Please see our [Development Guide](docs/development.md) for details on:

- Setting up your development environment
- Code style and conventions
- Submitting pull requests
- Testing requirements

### Contribution Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Ensure all tests pass (`rebar3 eunit && rebar3 ct`)
5. Run the linter (`rebar3 lint`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

```
Copyright 2024 FLURM Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Acknowledgments

- The SLURM team at SchedMD for creating the industry-standard workload manager
- The Erlang/OTP team for the incredible runtime
- The TLA+ community for formal verification tools

---

**FLURM** - Because your HPC cluster deserves fault tolerance.
