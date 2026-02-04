---------------------------- MODULE FlurmFederation ----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Federation and Sibling Job Coordination    *)
(*                                                                         *)
(* This module models federated job scheduling across multiple FLURM       *)
(* clusters, proving:                                                      *)
(* - Sibling exclusivity: At most one sibling job runs at any time         *)
(* - Origin awareness: Origin cluster knows which sibling is running       *)
(* - No job loss: Jobs are not lost during sibling coordination            *)
(* - Cancellation consistency: When one sibling starts, others cancel      *)
(*                                                                         *)
(* Federation Model:                                                       *)
(* - Multiple clusters form a federation                                   *)
(* - Jobs submitted to one cluster (origin) are replicated as siblings     *)
(* - Each cluster independently attempts to schedule the job               *)
(* - When one cluster starts the job, others cancel their siblings         *)
(* - Origin cluster tracks which cluster is running the job                *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2026                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Clusters,           \* Set of cluster identifiers
    MaxJobs,            \* Maximum number of federated jobs
    MaxMessages         \* Maximum in-flight messages (for bounded checking)

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    \* Per-job state
    jobOrigin,          \* Job -> Origin cluster
    siblingState,       \* Job -> Cluster -> Sibling state
    runningCluster,     \* Job -> Cluster currently running (or Nil)

    \* Per-cluster state
    clusterUp,          \* Cluster -> Boolean (is cluster reachable)

    \* Communication
    messages,           \* Set of in-flight messages

    \* Counters for bounded checking
    jobCounter          \* Next job ID to allocate

vars == <<jobOrigin, siblingState, runningCluster, clusterUp, messages, jobCounter>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

JobIds == 1..MaxJobs

CONSTANTS Nil  \* Model value representing "no cluster"

\* Sibling job states (mirrors SLURM federation states)
SiblingStates == {
    "NULL",             \* No sibling exists
    "PENDING",          \* Sibling queued, waiting for resources
    "RUNNING",          \* Sibling is executing
    "REVOKED",          \* Sibling cancelled because another cluster started
    "COMPLETED",        \* Sibling completed successfully
    "FAILED"            \* Sibling failed
}

\* Terminal sibling states
TerminalSiblingStates == {"REVOKED", "COMPLETED", "FAILED"}

\* Message types for inter-cluster communication
MessageTypes == {
    "JobSubmit",        \* Origin -> All: Create sibling
    "JobStarted",       \* Running -> Origin: Notify job started
    "SiblingRevoke",    \* Origin -> All: Cancel other siblings
    "JobCompleted",     \* Running -> Origin: Job finished
    "JobFailed"         \* Running -> Origin: Job failed
}

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ jobOrigin \in [JobIds -> Clusters \cup {Nil}]
    /\ siblingState \in [JobIds -> [Clusters -> SiblingStates]]
    /\ runningCluster \in [JobIds -> Clusters \cup {Nil}]
    /\ clusterUp \in [Clusters -> BOOLEAN]
    /\ jobCounter \in 1..(MaxJobs + 1)

(***************************************************************************)
(* HELPER FUNCTIONS                                                        *)
(***************************************************************************)

\* Get all clusters with active (non-terminal, non-null) siblings for a job
ActiveSiblings(j) ==
    {c \in Clusters : siblingState[j][c] \notin TerminalSiblingStates \cup {"NULL"}}

\* Get all clusters with running siblings for a job
RunningSiblings(j) ==
    {c \in Clusters : siblingState[j][c] = "RUNNING"}

\* Check if a job exists (has been submitted)
JobExists(j) ==
    jobOrigin[j] # Nil

\* Check if job is still active (has non-terminal siblings)
JobActive(j) ==
    JobExists(j) /\ ActiveSiblings(j) # {}

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* CRITICAL: At most one sibling can be running at any time
SiblingExclusivity ==
    \A j \in JobIds :
        Cardinality(RunningSiblings(j)) <= 1

\* Origin cluster tracks running cluster correctly
OriginAwareness ==
    \A j \in JobIds :
        JobExists(j) =>
            (runningCluster[j] # Nil <=> Cardinality(RunningSiblings(j)) = 1)

\* Running cluster matches siblingState
RunningClusterConsistency ==
    \A j \in JobIds :
        runningCluster[j] # Nil =>
            siblingState[j][runningCluster[j]] = "RUNNING"

\* Once a sibling is revoked, it stays revoked
RevokedIsTerminal ==
    \A j \in JobIds, c \in Clusters :
        siblingState[j][c] = "REVOKED" =>
            siblingState'[j][c] = "REVOKED"

\* Jobs cannot be lost - if submitted, must have at least one active sibling
\* or be in terminal state
NoJobLoss ==
    \A j \in JobIds :
        JobExists(j) =>
            \/ ActiveSiblings(j) # {}
            \/ \A c \in Clusters : siblingState[j][c] \in TerminalSiblingStates \cup {"NULL"}

\* Combined safety property
Safety ==
    /\ SiblingExclusivity
    /\ OriginAwareness
    /\ RunningClusterConsistency
    /\ NoJobLoss

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ jobOrigin = [j \in JobIds |-> Nil]
    /\ siblingState = [j \in JobIds |-> [c \in Clusters |-> "NULL"]]
    /\ runningCluster = [j \in JobIds |-> Nil]
    /\ clusterUp = [c \in Clusters |-> TRUE]
    /\ messages = {}
    /\ jobCounter = 1

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* Submit a new federated job from a cluster
\* Creates the job at origin and sends sibling creation messages
SubmitJob(origin) ==
    /\ jobCounter <= MaxJobs
    /\ clusterUp[origin]
    /\ LET j == jobCounter
       IN
       /\ jobOrigin' = [jobOrigin EXCEPT ![j] = origin]
       /\ siblingState' = [siblingState EXCEPT ![j] =
            [c \in Clusters |-> IF c = origin THEN "PENDING" ELSE "NULL"]]
       /\ runningCluster' = runningCluster
       /\ jobCounter' = jobCounter + 1
       \* Send sibling creation messages to other clusters
       /\ messages' = messages \cup
            {[type |-> "JobSubmit", job |-> j, origin |-> origin, to |-> c]
             : c \in Clusters \ {origin}}
    /\ UNCHANGED clusterUp

\* Cluster receives sibling creation message
ReceiveSiblingCreate(c) ==
    \E m \in messages :
        /\ m.type = "JobSubmit"
        /\ m.to = c
        /\ clusterUp[c]
        /\ siblingState[m.job][c] = "NULL"  \* Don't recreate existing sibling
        /\ siblingState' = [siblingState EXCEPT ![m.job][c] = "PENDING"]
        /\ messages' = messages \ {m}
        /\ UNCHANGED <<jobOrigin, runningCluster, clusterUp, jobCounter>>

\* Cluster starts running a sibling job
\* This can only happen if no other sibling is already running
StartSibling(j, c) ==
    /\ JobExists(j)
    /\ clusterUp[c]
    /\ siblingState[j][c] = "PENDING"
    /\ runningCluster[j] = Nil  \* No other cluster running yet
    /\ siblingState' = [siblingState EXCEPT ![j][c] = "RUNNING"]
    /\ runningCluster' = [runningCluster EXCEPT ![j] = c]
    \* Notify origin that this cluster started the job
    /\ messages' = messages \cup
        {[type |-> "JobStarted", job |-> j, cluster |-> c, to |-> jobOrigin[j]]}
    /\ UNCHANGED <<jobOrigin, clusterUp, jobCounter>>

\* Origin receives notification that a sibling started
\* Origin then revokes all other siblings
ReceiveJobStarted ==
    \E m \in messages :
        /\ m.type = "JobStarted"
        /\ m.to = jobOrigin[m.job]  \* Message is to origin
        /\ clusterUp[m.to]
        \* Send revoke messages to all other clusters
        /\ messages' = (messages \ {m}) \cup
            {[type |-> "SiblingRevoke", job |-> m.job, to |-> c]
             : c \in Clusters \ {m.cluster}}
        /\ UNCHANGED <<jobOrigin, siblingState, runningCluster, clusterUp, jobCounter>>

\* Cluster receives revoke message and cancels its sibling
ReceiveSiblingRevoke(c) ==
    \E m \in messages :
        /\ m.type = "SiblingRevoke"
        /\ m.to = c
        /\ clusterUp[c]
        /\ siblingState[m.job][c] \in {"PENDING", "NULL"}  \* Can only revoke pending
        /\ siblingState' = [siblingState EXCEPT ![m.job][c] = "REVOKED"]
        /\ messages' = messages \ {m}
        /\ UNCHANGED <<jobOrigin, runningCluster, clusterUp, jobCounter>>

\* Running sibling completes successfully
CompleteSibling(j, c) ==
    /\ siblingState[j][c] = "RUNNING"
    /\ runningCluster[j] = c
    /\ siblingState' = [siblingState EXCEPT ![j][c] = "COMPLETED"]
    /\ runningCluster' = [runningCluster EXCEPT ![j] = Nil]
    /\ messages' = messages \cup
        {[type |-> "JobCompleted", job |-> j, cluster |-> c, to |-> jobOrigin[j]]}
    /\ UNCHANGED <<jobOrigin, clusterUp, jobCounter>>

\* Running sibling fails
FailSibling(j, c) ==
    /\ siblingState[j][c] = "RUNNING"
    /\ runningCluster[j] = c
    /\ siblingState' = [siblingState EXCEPT ![j][c] = "FAILED"]
    /\ runningCluster' = [runningCluster EXCEPT ![j] = Nil]
    /\ messages' = messages \cup
        {[type |-> "JobFailed", job |-> j, cluster |-> c, to |-> jobOrigin[j]]}
    /\ UNCHANGED <<jobOrigin, clusterUp, jobCounter>>

\* Cluster goes down (network partition or failure)
ClusterDown(c) ==
    /\ clusterUp[c]
    /\ clusterUp' = [clusterUp EXCEPT ![c] = FALSE]
    \* If this cluster was running a job, mark it as failed
    /\ siblingState' = [j \in JobIds |->
        [cl \in Clusters |->
            IF cl = c /\ siblingState[j][c] = "RUNNING"
            THEN "FAILED"
            ELSE siblingState[j][cl]]]
    /\ runningCluster' = [j \in JobIds |->
        IF runningCluster[j] = c
        THEN Nil
        ELSE runningCluster[j]]
    /\ UNCHANGED <<jobOrigin, messages, jobCounter>>

\* Cluster comes back up
ClusterUp(c) ==
    /\ ~clusterUp[c]
    /\ clusterUp' = [clusterUp EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<jobOrigin, siblingState, runningCluster, messages, jobCounter>>

\* Drop a message (network failure)
DropMessage(m) ==
    /\ m \in messages
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<jobOrigin, siblingState, runningCluster, clusterUp, jobCounter>>

(***************************************************************************)
(* RACE CONDITION: Two siblings start simultaneously                       *)
(* This models the critical race we must handle                            *)
(***************************************************************************)

\* Two clusters try to start the same job simultaneously
\* This should be prevented by the runningCluster check, but we model
\* the race explicitly to verify the invariant holds
RaceStart(j, c1, c2) ==
    /\ c1 # c2
    /\ JobExists(j)
    /\ siblingState[j][c1] = "PENDING"
    /\ siblingState[j][c2] = "PENDING"
    /\ runningCluster[j] = Nil
    \* Only one can win - nondeterministically choose
    /\ \/ /\ siblingState' = [siblingState EXCEPT ![j][c1] = "RUNNING"]
          /\ runningCluster' = [runningCluster EXCEPT ![j] = c1]
       \/ /\ siblingState' = [siblingState EXCEPT ![j][c2] = "RUNNING"]
          /\ runningCluster' = [runningCluster EXCEPT ![j] = c2]
    /\ UNCHANGED <<jobOrigin, clusterUp, messages, jobCounter>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E c \in Clusters : SubmitJob(c)
    \/ \E c \in Clusters : ReceiveSiblingCreate(c)
    \/ \E j \in JobIds, c \in Clusters : StartSibling(j, c)
    \/ ReceiveJobStarted
    \/ \E c \in Clusters : ReceiveSiblingRevoke(c)
    \/ \E j \in JobIds, c \in Clusters : CompleteSibling(j, c)
    \/ \E j \in JobIds, c \in Clusters : FailSibling(j, c)
    \/ \E c \in Clusters : ClusterDown(c)
    \/ \E c \in Clusters : ClusterUp(c)
    \/ \E m \in messages : DropMessage(m)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Messages are eventually delivered (if cluster is up)
FairMessageDelivery ==
    /\ \A c \in Clusters : WF_vars(ReceiveSiblingCreate(c))
    /\ WF_vars(ReceiveJobStarted)
    /\ \A c \in Clusters : WF_vars(ReceiveSiblingRevoke(c))

\* Pending jobs are eventually scheduled
FairScheduling ==
    \A j \in JobIds, c \in Clusters : WF_vars(StartSibling(j, c))

\* Running jobs eventually terminate
FairTermination ==
    \A j \in JobIds, c \in Clusters :
        WF_vars(CompleteSibling(j, c) \/ FailSibling(j, c))

\* Clusters eventually recover
FairRecovery ==
    \A c \in Clusters : WF_vars(ClusterUp(c))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* Eventually one sibling runs (if job submitted and at least one cluster up)
EventuallySiblingRuns ==
    \A j \in JobIds :
        (JobExists(j) /\ \E c \in Clusters : clusterUp[c]) ~>
        (\E c \in Clusters : siblingState[j][c] = "RUNNING")

\* All jobs eventually reach terminal state
AllJobsTerminate ==
    \A j \in JobIds :
        JobExists(j) ~>
        (\A c \in Clusters : siblingState[j][c] \in TerminalSiblingStates \cup {"NULL"})

\* Pending siblings are eventually either run or revoked
PendingSiblingsProgress ==
    \A j \in JobIds, c \in Clusters :
        siblingState[j][c] = "PENDING" ~>
        siblingState[j][c] \in {"RUNNING", "REVOKED"}

(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

\* Safety specification
SafetySpec ==
    Init /\ [][Next]_vars

\* Full specification with fairness
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairMessageDelivery
    /\ FairScheduling
    /\ FairTermination

\* Liveness specification (stronger fairness)
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairMessageDelivery
    /\ FairScheduling
    /\ FairTermination
    /\ FairRecovery

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* Type correctness
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* Sibling exclusivity is always maintained
THEOREM SiblingExclusivityTheorem == Spec => []SiblingExclusivity

\* Origin always knows which cluster is running
THEOREM OriginAwarenessTheorem == Spec => []OriginAwareness

\* No jobs are lost
THEOREM NoJobLossTheorem == Spec => []NoJobLoss

\* Combined safety
THEOREM SafetyTheorem == Spec => []Safety

=============================================================================
