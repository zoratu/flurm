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
