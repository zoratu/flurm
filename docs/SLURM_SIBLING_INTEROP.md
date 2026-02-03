# SLURM Sibling Cluster Interoperability

This document outlines the implementation plan for enabling FLURM to federate with native SLURM clusters.

## Research Findings

### SLURM Federation Architecture

**Sources:**
- [SLURM Federation Guide](https://slurm.schedmd.com/federation.html)
- [SchedMD/slurm GitHub](https://github.com/SchedMD/slurm)

#### Key Concepts

1. **Sibling Jobs**: When a job is submitted to a federated cluster, copies (sibling jobs) are created on all eligible clusters. Each cluster independently attempts to schedule its copy.

2. **Origin Cluster**: The cluster where the job was originally submitted. It coordinates scheduling and notifies siblings when one cluster starts the job.

3. **Job ID Format** (32-bit unsigned integer):
   - Bits 0-25: Local Job ID (max ~67M jobs per cluster)
   - Bits 26-31: Cluster Origin ID (max 64 clusters in federation)

4. **Coordination**: When a cluster allocates resources, it notifies the origin cluster. The origin then tells other siblings to remove their copies.

### SLURM Protocol Messages

SLURM has only **2** federation-specific message types:

| Message | SLURM Value | FLURM Value | Notes |
|---------|-------------|-------------|-------|
| `REQUEST_FED_INFO` | 2049 | 2024 | **MISMATCH** |
| `RESPONSE_FED_INFO` | 2050 | 2025 | **MISMATCH** |

**Critical Finding**: SLURM does NOT have separate messages for cross-cluster job submission. Federation happens internally within slurmctld via:
- Standard `REQUEST_SUBMIT_BATCH_JOB` for initial submission
- Internal RPC between slurmctld daemons for sibling coordination
- `fed_mgr.c` handles all federation logic server-side

FLURM's custom messages (2032-2037: `REQUEST_FEDERATION_SUBMIT`, etc.) are FLURM-specific extensions, not SLURM-compatible.

### SLURM Federation Requirements

1. **slurmdbd**: All clusters must connect to the same slurmdbd (or cluster of slurmdbd instances)
2. **Authentication**: Either shared MUNGE key across all clusters, or alternative auth for slurmdbd
3. **Network**: All slurmctld daemons must be able to communicate directly
4. **Unique Cluster Names**: Each cluster must have a unique `ClusterName` in slurm.conf

---

## Current FLURM State

### What Exists

| Component | Status | Notes |
|-----------|--------|-------|
| `flurm_federation.erl` | ✅ Complete | 1,448 lines, HTTP-based |
| Protocol messages 2032-2037 | ✅ Defined | FLURM-specific, not SLURM |
| Cluster registry | ✅ Works | ETS-based tracking |
| Remote job tracking | ✅ Works | `fed_job`, `remote_job` records |
| Routing policies | ✅ Works | round-robin, least-loaded, etc. |
| Health monitoring | ✅ Works | 10-second checks |

### What's Missing for SLURM Interop

1. **Protocol constant mismatch**: `REQUEST_FED_INFO` is 2024 in FLURM, 2049 in SLURM
2. **No slurmctld-to-slurmctld RPC**: FLURM uses HTTP, SLURM uses internal binary RPC
3. **Job ID format**: FLURM doesn't use cluster bits in job IDs
4. **Sibling job semantics**: FLURM tracks remote jobs, doesn't create true siblings
5. **slurmdbd integration**: FLURM has its own accounting (flurm_dbd)

---

## Implementation Plan

### Phase 1: Protocol Constants Alignment

**Goal**: Align `REQUEST_FED_INFO` / `RESPONSE_FED_INFO` with SLURM values.

**Files to modify**:
- `apps/flurm_protocol/include/flurm_protocol.hrl`

**Changes**:
```erlang
%% OLD (FLURM-specific)
-define(REQUEST_FED_INFO, 2024).
-define(RESPONSE_FED_INFO, 2025).

%% NEW (SLURM-compatible)
-define(REQUEST_FED_INFO, 2049).
-define(RESPONSE_FED_INFO, 2050).

%% Keep FLURM extensions with different names
-define(REQUEST_FLURM_FEDERATION_SUBMIT, 2032).
-define(RESPONSE_FLURM_FEDERATION_SUBMIT, 2033).
%% ... etc
```

**⚠️ UPGRADE PROCEDURE NEEDED**: Existing FLURM clusters using federation will need migration.

### Phase 2: Job ID Format with Cluster Bits

**Goal**: Implement SLURM-compatible federated job IDs.

**Files to modify**:
- `apps/flurm_core/src/flurm_job_id.erl` (new)
- `apps/flurm_core/src/flurm_federation.erl`

**Implementation**:
```erlang
-module(flurm_job_id).

-define(CLUSTER_BITS, 6).
-define(LOCAL_BITS, 26).
-define(MAX_CLUSTER_ID, 63).        % 2^6 - 1
-define(MAX_LOCAL_ID, 67108863).    % 2^26 - 1

%% Create federated job ID
-spec make_fed_job_id(ClusterId :: 0..63, LocalId :: non_neg_integer()) ->
    non_neg_integer().
make_fed_job_id(ClusterId, LocalId) when
    ClusterId =< ?MAX_CLUSTER_ID, LocalId =< ?MAX_LOCAL_ID ->
    (ClusterId bsl ?LOCAL_BITS) bor LocalId.

%% Extract cluster ID from federated job ID
-spec get_cluster_id(FedJobId :: non_neg_integer()) -> 0..63.
get_cluster_id(FedJobId) ->
    FedJobId bsr ?LOCAL_BITS.

%% Extract local job ID
-spec get_local_id(FedJobId :: non_neg_integer()) -> non_neg_integer().
get_local_id(FedJobId) ->
    FedJobId band ?MAX_LOCAL_ID.
```

### Phase 3: FED_INFO Response Implementation

**Goal**: Properly respond to `REQUEST_FED_INFO` with federation metadata.

**Files to modify**:
- `apps/flurm_controller/src/flurm_controller_handler.erl`
- `apps/flurm_protocol/src/flurm_protocol_codec.erl`

**Response structure** (based on SLURM `fed_mgr_fed_rec`):
```erlang
-record(fed_info_response, {
    name = <<>> :: binary(),           % Federation name
    flags = 0 :: non_neg_integer(),
    clusters = [] :: [#fed_cluster_rec{}]
}).

-record(fed_cluster_rec, {
    name = <<>> :: binary(),
    control_host = <<>> :: binary(),
    control_port = 0 :: non_neg_integer(),
    features = <<>> :: binary(),
    fed_id = 0 :: non_neg_integer(),   % Cluster's federation ID (0-63)
    fed_state = 0 :: non_neg_integer(),
    cluster_id = 0 :: non_neg_integer()
}).
```

### Phase 4: Sibling Job Creation

**Goal**: Create sibling jobs on submission like SLURM does.

**Changes to `flurm_controller_handler.erl`**:
```erlang
handle_submit_batch_job(Job, State) ->
    case is_federated() of
        false ->
            %% Local submission only
            submit_local(Job, State);
        true ->
            %% Create sibling jobs on all federated clusters
            LocalId = submit_local(Job, State),
            FedJobId = flurm_job_id:make_fed_job_id(my_cluster_id(), LocalId),
            SiblingClusters = get_eligible_siblings(Job),
            lists:foreach(fun(Cluster) ->
                submit_sibling(Cluster, Job, FedJobId)
            end, SiblingClusters),
            {ok, FedJobId}
    end.
```

### Phase 5: Sibling Coordination Protocol

**Goal**: Implement coordination between FLURM and SLURM slurmctld.

This requires reverse-engineering SLURM's internal cluster-to-cluster RPC, which is not publicly documented.

**Options**:
1. **Proxy approach**: Run a lightweight SLURM slurmctld as a proxy
2. **Packet capture**: Analyze traffic between real SLURM controllers
3. **Source code analysis**: Study `fed_mgr.c` more deeply

**Known coordination events**:
- Job started on cluster → notify origin → cancel siblings
- Job cancelled → propagate to all siblings
- Resource updates → periodic sync

### Phase 6: slurmdbd Integration (Optional)

**Goal**: Allow FLURM to report to a real slurmdbd for unified accounting.

This is a larger undertaking that may not be necessary for basic federation.

---

## Upgrade Procedure

### For Existing FLURM Clusters

When upgrading FLURM clusters that use federation:

1. **Coordinate upgrade**: All federated FLURM clusters should upgrade together
2. **Drain federation**: Disable federation routing before upgrade
3. **Upgrade all nodes**: Apply new code to all controllers
4. **Re-enable federation**: Start federation with new protocol constants

### Migration Steps

```bash
# 1. Disable federation on all clusters
curl -X POST http://controller:6820/api/v1/federation/disable

# 2. Wait for in-flight jobs to complete
# 3. Upgrade FLURM on all clusters

# 4. Re-add clusters with new protocol
curl -X POST http://controller:6820/api/v1/federation/clusters \
  -d '{"name":"cluster-b","host":"...","port":6817}'
```

---

## Testing Strategy

### Unit Tests
- Job ID encoding/decoding with cluster bits
- FED_INFO request/response codec
- Sibling job tracking

### Integration Tests
- Two FLURM clusters federating
- Job submission routed to sibling
- Sibling cancellation on job start

### Interop Tests (Future)
- FLURM + real SLURM cluster
- Submit from SLURM, run on FLURM
- Submit from FLURM, run on SLURM

---

---

# Zero-Downtime Migration Playbook

This section provides a step-by-step guide for migrating from SLURM to FLURM with zero downtime.

## Migration Overview

The migration uses a **SLURM Bridge** module (`flurm_slurm_bridge.erl`) that enables:
- Observation of existing SLURM clusters
- Job forwarding to SLURM during transition
- Gradual workload migration
- Clean SLURM decommissioning

### Migration Modes

| Mode | Description | Job Handling |
|------|-------------|--------------|
| `shadow` | Observation only | All jobs go to SLURM |
| `active` | Parallel operation | FLURM handles new jobs, can forward to SLURM |
| `primary` | FLURM leads | SLURM draining, minimal forwarding |
| `standalone` | FLURM only | No SLURM integration |

---

## Scenario 1: SLURM Exists

**Goal**: Deploy FLURM alongside SLURM without disrupting existing workloads.

### Prerequisites
- Existing SLURM cluster operational
- FLURM controller installed
- Network connectivity between FLURM and SLURM

### Steps

1. **Deploy FLURM in shadow mode**
   ```erlang
   %% FLURM starts in shadow mode by default
   {ok, _} = flurm_slurm_bridge:start_link().
   shadow = flurm_slurm_bridge:get_mode().
   ```

2. **Register the SLURM cluster**
   ```erlang
   ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
       host => <<"slurmctld.your-cluster.internal">>,
       port => 6817
   }).
   ```

3. **Verify observation**
   ```erlang
   %% Check cluster tracking
   Clusters = flurm_slurm_bridge:list_slurm_clusters().
   %% Should show your SLURM cluster

   %% Check bridge status
   Status = flurm_slurm_bridge:get_bridge_status().
   %% mode => shadow, clusters_total => 1
   ```

4. **Monitor SLURM health**
   - FLURM automatically performs health checks every 10 seconds
   - View cluster state in `list_slurm_clusters()` output

### What Happens
- FLURM tracks SLURM cluster state
- All jobs continue to be submitted directly to SLURM
- FLURM does not intercept or handle any jobs
- No changes to user workflows

---

## Scenario 2: Add FLURM (Active Mode)

**Goal**: Enable FLURM to handle jobs while SLURM remains operational.

### Prerequisites
- Scenario 1 completed
- FLURM services fully operational
- Test jobs verified working with FLURM

### Steps

1. **Transition to active mode**
   ```erlang
   ok = flurm_slurm_bridge:set_mode(active).
   active = flurm_slurm_bridge:get_mode().
   ```

2. **Configure job routing**
   ```erlang
   %% Jobs can now be:
   %% 1. Handled locally by FLURM
   %% 2. Forwarded to SLURM if needed
   ```

3. **Test job forwarding** (optional)
   ```erlang
   JobSpec = #{
       name => <<"test-forward">>,
       script => <<"#!/bin/bash\necho 'forwarded'">>,
       partition => <<"default">>,
       num_cpus => 1
   },
   %% Forward to specific cluster
   {ok, FlurmJobId} = flurm_slurm_bridge:forward_job(JobSpec, <<"prod-slurm">>),

   %% Or auto-select cluster
   {ok, FlurmJobId} = flurm_slurm_bridge:forward_job(JobSpec, auto).
   ```

4. **Monitor forwarded jobs**
   ```erlang
   %% Check forwarded job status
   {ok, Status} = flurm_slurm_bridge:get_forwarded_job_status(FlurmJobId).
   %% Returns: #{state => pending|running|completed|failed, ...}
   ```

5. **Gradual workload migration**
   - Route a percentage of jobs to FLURM
   - Monitor performance and correctness
   - Increase FLURM's share over time

### What Happens
- FLURM can accept and run jobs
- Critical/legacy jobs can be forwarded to SLURM
- Both schedulers operate in parallel
- Users can submit to either system

### Rollback
If issues arise:
```erlang
%% Return to shadow mode
ok = flurm_slurm_bridge:set_mode(shadow).
%% All new jobs will go to SLURM again
```

---

## Scenario 3: FLURM Primary (Ditch SLURM)

**Goal**: Make FLURM the primary scheduler and decommission SLURM.

### Phase A: FLURM Becomes Primary

1. **Transition to primary mode**
   ```erlang
   ok = flurm_slurm_bridge:set_mode(primary).
   primary = flurm_slurm_bridge:get_mode().
   ```

2. **Stop new submissions to SLURM**
   - Update user documentation
   - Redirect all job submission to FLURM
   - Block direct SLURM access if needed

3. **Monitor SLURM drain**
   ```erlang
   %% Check for remaining forwarded jobs
   Status = flurm_slurm_bridge:get_bridge_status().
   %% jobs_running shows remaining forwarded jobs
   ```

### Phase B: Wait for SLURM Jobs to Complete

4. **Track remaining jobs**
   ```erlang
   %% SLURM jobs will complete naturally
   %% Check periodically:
   Clusters = flurm_slurm_bridge:list_slurm_clusters().
   %% job_count should decrease over time
   ```

5. **Cancel non-critical forwarded jobs** (optional)
   ```erlang
   ok = flurm_slurm_bridge:cancel_forwarded_job(FlurmJobId).
   ```

### Phase C: Decommission SLURM

6. **Remove SLURM cluster**
   ```erlang
   ok = flurm_slurm_bridge:remove_slurm_cluster(<<"prod-slurm">>).
   %% Fails if active jobs remain: {error, {active_jobs, N}}
   ```

7. **Transition to standalone mode**
   ```erlang
   ok = flurm_slurm_bridge:set_mode(standalone).
   standalone = flurm_slurm_bridge:get_mode().
   ```

8. **Verify standalone operation**
   ```erlang
   false = flurm_slurm_bridge:is_slurm_available().

   %% Job forwarding blocked
   {error, standalone_mode} = flurm_slurm_bridge:forward_job(#{}, auto).
   ```

### What Happens
- FLURM handles all new jobs
- SLURM finishes existing jobs (draining)
- Once SLURM is empty, it's removed
- FLURM operates independently

---

## Migration Timeline Example

```
Week 1-2: Shadow Mode (Scenario 1)
├── Deploy FLURM
├── Configure SLURM cluster tracking
├── Monitor and verify health checks
└── Run internal FLURM tests

Week 3-4: Active Mode (Scenario 2)
├── Enable active mode
├── Route 10% of jobs to FLURM
├── Monitor for issues
├── Increase to 50% of jobs
└── Verify job completion rates

Week 5-6: Primary Mode (Scenario 3A)
├── Enable primary mode
├── Route 100% of new jobs to FLURM
├── Stop direct SLURM access
└── Monitor SLURM drain

Week 7+: Standalone Mode (Scenario 3C)
├── Remove SLURM clusters
├── Enable standalone mode
└── Decommission SLURM infrastructure
```

---

## Troubleshooting

### Job forwarding fails
```erlang
{error, {connect_failed, Reason}} = flurm_slurm_bridge:forward_job(Job, Cluster).
%% Check: SLURM cluster reachable? Port open?
```

### Cluster shows as down
```erlang
[#{state := down, consecutive_failures := N}] = flurm_slurm_bridge:list_slurm_clusters().
%% Check: Network connectivity, SLURM controller running?
```

### Cannot remove cluster (active jobs)
```erlang
{error, {active_jobs, 5}} = flurm_slurm_bridge:remove_slurm_cluster(Name).
%% Wait for jobs to complete or cancel them
```

### Rollback needed
```erlang
%% From any mode, can return to previous mode:
ok = flurm_slurm_bridge:set_mode(active).   % or
ok = flurm_slurm_bridge:set_mode(shadow).
```

---

## REST API (Future)

The following REST endpoints will be available:

```
GET  /api/v1/slurm-bridge/status           - Get bridge status
GET  /api/v1/slurm-bridge/mode             - Get current mode
PUT  /api/v1/slurm-bridge/mode             - Set mode
GET  /api/v1/slurm-bridge/clusters         - List SLURM clusters
POST /api/v1/slurm-bridge/clusters         - Add cluster
DELETE /api/v1/slurm-bridge/clusters/:name - Remove cluster
POST /api/v1/slurm-bridge/forward          - Forward job
GET  /api/v1/slurm-bridge/jobs/:id         - Get forwarded job status
DELETE /api/v1/slurm-bridge/jobs/:id       - Cancel forwarded job
```

---

## Configuration Reference

### Application Environment

```erlang
%% sys.config
{flurm_core, [
    {slurm_bridge, [
        {mode, shadow},                    % Initial mode
        {primary_slurm, <<"prod-slurm">>}, % Primary cluster for forwarding
        {slurm_clusters, [
            {<<"prod-slurm">>, #{
                host => <<"slurmctld.internal">>,
                port => 6817
            }}
        ]}
    ]}
]}
```

### Supervisor Setup

```erlang
%% Add to flurm_core_sup.erl
{flurm_slurm_bridge,
    {flurm_slurm_bridge, start_link, [BridgeOpts]},
    permanent, 5000, worker, [flurm_slurm_bridge]}
```

---

## Open Questions

1. **SLURM version compatibility**: Which SLURM versions should we target? (22.05+?)
2. **slurmdbd requirement**: Can we federate without shared slurmdbd?
3. **MUNGE authentication**: How to handle auth between FLURM and SLURM?
4. **Protocol stability**: How often does SLURM change federation internals?

---

## References

- [SLURM Federation Guide](https://slurm.schedmd.com/federation.html)
- [SchedMD/slurm on GitHub](https://github.com/SchedMD/slurm)
- [SLURM Federation PDF (SLUG17)](https://slurm.schedmd.com/SLUG17/FederatedScheduling.pdf)
