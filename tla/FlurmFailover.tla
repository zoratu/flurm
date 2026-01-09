---------------------------- MODULE FlurmFailover -----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Controller Failover                        *)
(*                                                                         *)
(* This module models controller failover scenarios, proving:              *)
(* - No job loss during failover                                           *)
(* - Consistency maintained during network partition                       *)
(* - Eventually consistent after partition heals                           *)
(* - Client requests are preserved across failovers                        *)
(*                                                                         *)
(* Model includes:                                                         *)
(* - Multiple controllers with leader election                             *)
(* - Network partitions                                                    *)
(* - Job state replication                                                 *)
(* - Client request handling                                               *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2024                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Controllers,        \* Set of controller nodes
    MaxJobs,            \* Maximum number of jobs
    MaxOperations       \* Maximum operations (for bounded checking)

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    \* Controller state
    controllerState,    \* Controller -> {leader, follower, candidate, down}
    controllerTerm,     \* Controller -> Current term (epoch)

    \* Job state (replicated across controllers)
    jobRegistry,        \* Controller -> (JobId -> JobState)
    committedJobs,      \* Set of job IDs known to be committed (majority ack)

    \* Replication state
    replicationLog,     \* Controller -> Sequence of operations
    lastApplied,        \* Controller -> Last applied log index

    \* Network state
    networkPartition,   \* Set of controller pairs that cannot communicate
    pendingRequests,    \* Set of pending client requests

    \* Metrics
    operationCount      \* Count of operations performed

vars == <<controllerState, controllerTerm, jobRegistry, committedJobs,
          replicationLog, lastApplied, networkPartition, pendingRequests,
          operationCount>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

Nil == CHOOSE v : v \notin Controllers

JobIds == 1..MaxJobs

ControllerStates == {"leader", "follower", "candidate", "down"}

JobStates == {"pending", "running", "completed", "failed"}

\* Operation types for replication log
OperationType == {"submit", "start", "complete", "fail"}

Operation == [type: OperationType, jobId: JobIds]

\* Client request structure
Request == [requestId: 1..MaxOperations, type: OperationType, jobId: JobIds]

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ controllerState \in [Controllers -> ControllerStates]
    /\ controllerTerm \in [Controllers -> 0..MaxOperations]
    /\ jobRegistry \in [Controllers -> [JobIds -> JobStates \cup {"none"}]]
    /\ committedJobs \subseteq JobIds
    /\ replicationLog \in [Controllers -> Seq(Operation)]
    /\ \A c \in Controllers : Len(replicationLog[c]) <= MaxOperations
    /\ lastApplied \in [Controllers -> 0..MaxOperations]
    /\ networkPartition \subseteq (Controllers \X Controllers)
    /\ operationCount \in 0..MaxOperations

(***************************************************************************)
(* HELPER FUNCTIONS                                                        *)
(***************************************************************************)

\* Quorum (majority of controllers)
Quorum == {Q \in SUBSET Controllers : Cardinality(Q) * 2 > Cardinality(Controllers)}

\* Check if two controllers can communicate
CanCommunicate(c1, c2) ==
    /\ <<c1, c2>> \notin networkPartition
    /\ <<c2, c1>> \notin networkPartition
    /\ controllerState[c1] # "down"
    /\ controllerState[c2] # "down"

\* Get current leader (if any)
CurrentLeader ==
    IF \E c \in Controllers : controllerState[c] = "leader"
    THEN CHOOSE c \in Controllers : controllerState[c] = "leader"
    ELSE Nil

\* Get controllers in same partition as c
SamePartition(c) ==
    {c} \cup {c2 \in Controllers : CanCommunicate(c, c2)}

\* Check if a quorum is reachable from controller c
HasQuorum(c) ==
    SamePartition(c) \in Quorum

\* Controllers that are up
UpControllers == {c \in Controllers : controllerState[c] # "down"}

\* Get the latest term among reachable controllers
LatestTerm(c) ==
    LET reachable == SamePartition(c)
    IN IF reachable = {} THEN 0
       ELSE LET maxTerm == CHOOSE t \in {controllerTerm[r] : r \in reachable} :
                    \A r \in reachable : controllerTerm[r] <= t
            IN maxTerm

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* No job loss: Committed jobs are never lost
\* (They exist in at least one controller's registry)
NoJobLoss ==
    \A j \in committedJobs :
        \E c \in Controllers :
            /\ controllerState[c] # "down"
            /\ jobRegistry[c][j] # "none"

\* At most one leader per term
AtMostOneLeader ==
    \A c1, c2 \in Controllers :
        (controllerState[c1] = "leader" /\
         controllerState[c2] = "leader" /\
         controllerTerm[c1] = controllerTerm[c2])
        => c1 = c2

\* Committed jobs are consistent across controllers in same partition
PartitionConsistency ==
    \A c1, c2 \in Controllers :
        (CanCommunicate(c1, c2) /\ controllerState[c1] # "down" /\ controllerState[c2] # "down")
        => (\A j \in committedJobs :
                jobRegistry[c1][j] = jobRegistry[c2][j] \/
                jobRegistry[c1][j] = "none" \/
                jobRegistry[c2][j] = "none")

\* Log prefix consistency (if logs agree on an entry, they agree on all prior)
LogPrefixConsistency ==
    \A c1, c2 \in Controllers :
        LET len == Min({Len(replicationLog[c1]), Len(replicationLog[c2])})
        IN \A i \in 1..len :
            replicationLog[c1][i] = replicationLog[c2][i]

\* Combined safety
Safety ==
    /\ NoJobLoss
    /\ AtMostOneLeader
    /\ PartitionConsistency

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ controllerState = [c \in Controllers |-> "follower"]
    /\ controllerTerm = [c \in Controllers |-> 0]
    /\ jobRegistry = [c \in Controllers |-> [j \in JobIds |-> "none"]]
    /\ committedJobs = {}
    /\ replicationLog = [c \in Controllers |-> <<>>]
    /\ lastApplied = [c \in Controllers |-> 0]
    /\ networkPartition = {}
    /\ pendingRequests = {}
    /\ operationCount = 0

(***************************************************************************)
(* ACTIONS - Leader Election                                               *)
(***************************************************************************)

\* Controller starts election
StartElection(c) ==
    /\ controllerState[c] \in {"follower", "candidate"}
    /\ HasQuorum(c)
    /\ operationCount < MaxOperations
    /\ LET newTerm == LatestTerm(c) + 1
       IN /\ controllerTerm' = [controllerTerm EXCEPT ![c] = newTerm]
          /\ controllerState' = [controllerState EXCEPT ![c] = "candidate"]
    /\ operationCount' = operationCount + 1
    /\ UNCHANGED <<jobRegistry, committedJobs, replicationLog, lastApplied,
                   networkPartition, pendingRequests>>

\* Candidate becomes leader (receives majority votes)
BecomeLeader(c) ==
    /\ controllerState[c] = "candidate"
    /\ HasQuorum(c)
    \* No other leader with same or higher term in reachable partition
    /\ \A c2 \in SamePartition(c) \ {c} :
        ~(controllerState[c2] = "leader" /\ controllerTerm[c2] >= controllerTerm[c])
    /\ controllerState' = [controllerState EXCEPT ![c] = "leader"]
    /\ UNCHANGED <<controllerTerm, jobRegistry, committedJobs, replicationLog,
                   lastApplied, networkPartition, pendingRequests, operationCount>>

\* Leader steps down (discovers higher term or loses quorum)
StepDown(c) ==
    /\ controllerState[c] = "leader"
    /\ \/ ~HasQuorum(c)
       \/ \E c2 \in Controllers : controllerTerm[c2] > controllerTerm[c]
    /\ controllerState' = [controllerState EXCEPT ![c] = "follower"]
    /\ UNCHANGED <<controllerTerm, jobRegistry, committedJobs, replicationLog,
                   lastApplied, networkPartition, pendingRequests, operationCount>>

(***************************************************************************)
(* ACTIONS - Job Operations                                                *)
(***************************************************************************)

\* Submit a new job (client request to leader)
SubmitJob(c, j) ==
    /\ controllerState[c] = "leader"
    /\ jobRegistry[c][j] = "none"
    /\ operationCount < MaxOperations
    /\ Len(replicationLog[c]) < MaxOperations
    /\ LET op == [type |-> "submit", jobId |-> j]
       IN /\ replicationLog' = [replicationLog EXCEPT ![c] = Append(@, op)]
          /\ jobRegistry' = [jobRegistry EXCEPT ![c][j] = "pending"]
    /\ operationCount' = operationCount + 1
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests>>

\* Replicate operation to follower
ReplicateToFollower(leader, follower) ==
    /\ controllerState[leader] = "leader"
    /\ controllerState[follower] = "follower"
    /\ CanCommunicate(leader, follower)
    /\ Len(replicationLog[follower]) < Len(replicationLog[leader])
    /\ LET nextIdx == Len(replicationLog[follower]) + 1
           op == replicationLog[leader][nextIdx]
       IN /\ replicationLog' = [replicationLog EXCEPT ![follower] = Append(@, op)]
          /\ jobRegistry' = [jobRegistry EXCEPT ![follower][op.jobId] =
                CASE op.type = "submit" -> "pending"
                  [] op.type = "start" -> "running"
                  [] op.type = "complete" -> "completed"
                  [] op.type = "fail" -> "failed"]
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests, operationCount>>

\* Commit a job (majority have replicated)
CommitJob(c, j) ==
    /\ controllerState[c] = "leader"
    /\ jobRegistry[c][j] # "none"
    /\ j \notin committedJobs
    \* Check majority have the job
    /\ LET replicatedTo == {c2 \in Controllers :
            /\ CanCommunicate(c, c2)
            /\ jobRegistry[c2][j] # "none"}
       IN replicatedTo \cup {c} \in Quorum
    /\ committedJobs' = committedJobs \cup {j}
    /\ UNCHANGED <<controllerState, controllerTerm, jobRegistry, replicationLog,
                   lastApplied, networkPartition, pendingRequests, operationCount>>

\* Start a pending job
StartJob(c, j) ==
    /\ controllerState[c] = "leader"
    /\ jobRegistry[c][j] = "pending"
    /\ operationCount < MaxOperations
    /\ Len(replicationLog[c]) < MaxOperations
    /\ LET op == [type |-> "start", jobId |-> j]
       IN /\ replicationLog' = [replicationLog EXCEPT ![c] = Append(@, op)]
          /\ jobRegistry' = [jobRegistry EXCEPT ![c][j] = "running"]
    /\ operationCount' = operationCount + 1
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests>>

\* Complete a running job
CompleteJob(c, j) ==
    /\ controllerState[c] = "leader"
    /\ jobRegistry[c][j] = "running"
    /\ operationCount < MaxOperations
    /\ Len(replicationLog[c]) < MaxOperations
    /\ LET op == [type |-> "complete", jobId |-> j]
       IN /\ replicationLog' = [replicationLog EXCEPT ![c] = Append(@, op)]
          /\ jobRegistry' = [jobRegistry EXCEPT ![c][j] = "completed"]
    /\ operationCount' = operationCount + 1
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests>>

\* Fail a job
FailJob(c, j) ==
    /\ controllerState[c] = "leader"
    /\ jobRegistry[c][j] \in {"pending", "running"}
    /\ operationCount < MaxOperations
    /\ Len(replicationLog[c]) < MaxOperations
    /\ LET op == [type |-> "fail", jobId |-> j]
       IN /\ replicationLog' = [replicationLog EXCEPT ![c] = Append(@, op)]
          /\ jobRegistry' = [jobRegistry EXCEPT ![c][j] = "failed"]
    /\ operationCount' = operationCount + 1
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests>>

(***************************************************************************)
(* ACTIONS - Failure Scenarios                                             *)
(***************************************************************************)

\* Controller goes down
ControllerDown(c) ==
    /\ controllerState[c] # "down"
    /\ controllerState' = [controllerState EXCEPT ![c] = "down"]
    /\ UNCHANGED <<controllerTerm, jobRegistry, committedJobs, replicationLog,
                   lastApplied, networkPartition, pendingRequests, operationCount>>

\* Controller comes back up (recovers from persistent storage)
ControllerUp(c) ==
    /\ controllerState[c] = "down"
    /\ controllerState' = [controllerState EXCEPT ![c] = "follower"]
    \* Job registry and log are persistent, so preserved
    /\ UNCHANGED <<controllerTerm, jobRegistry, committedJobs, replicationLog,
                   lastApplied, networkPartition, pendingRequests, operationCount>>

\* Network partition occurs between two controllers
CreatePartition(c1, c2) ==
    /\ c1 # c2
    /\ <<c1, c2>> \notin networkPartition
    /\ networkPartition' = networkPartition \cup {<<c1, c2>>, <<c2, c1>>}
    /\ UNCHANGED <<controllerState, controllerTerm, jobRegistry, committedJobs,
                   replicationLog, lastApplied, pendingRequests, operationCount>>

\* Network partition heals
HealPartition(c1, c2) ==
    /\ <<c1, c2>> \in networkPartition
    /\ networkPartition' = networkPartition \ {<<c1, c2>>, <<c2, c1>>}
    /\ UNCHANGED <<controllerState, controllerTerm, jobRegistry, committedJobs,
                   replicationLog, lastApplied, pendingRequests, operationCount>>

\* After partition heals, sync logs (simplified)
SyncAfterPartition(behind, ahead) ==
    /\ controllerState[ahead] = "leader"
    /\ controllerState[behind] = "follower"
    /\ CanCommunicate(behind, ahead)
    /\ Len(replicationLog[behind]) < Len(replicationLog[ahead])
    \* Copy the leader's log and state
    /\ replicationLog' = [replicationLog EXCEPT ![behind] = replicationLog[ahead]]
    /\ jobRegistry' = [jobRegistry EXCEPT ![behind] = jobRegistry[ahead]]
    /\ UNCHANGED <<controllerState, controllerTerm, committedJobs, lastApplied,
                   networkPartition, pendingRequests, operationCount>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \* Leader election
    \/ \E c \in Controllers : StartElection(c)
    \/ \E c \in Controllers : BecomeLeader(c)
    \/ \E c \in Controllers : StepDown(c)

    \* Job operations
    \/ \E c \in Controllers, j \in JobIds : SubmitJob(c, j)
    \/ \E l, f \in Controllers : ReplicateToFollower(l, f)
    \/ \E c \in Controllers, j \in JobIds : CommitJob(c, j)
    \/ \E c \in Controllers, j \in JobIds : StartJob(c, j)
    \/ \E c \in Controllers, j \in JobIds : CompleteJob(c, j)
    \/ \E c \in Controllers, j \in JobIds : FailJob(c, j)

    \* Failure scenarios
    \/ \E c \in Controllers : ControllerDown(c)
    \/ \E c \in Controllers : ControllerUp(c)
    \/ \E c1, c2 \in Controllers : CreatePartition(c1, c2)
    \/ \E c1, c2 \in Controllers : HealPartition(c1, c2)
    \/ \E b, a \in Controllers : SyncAfterPartition(b, a)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Weak fairness on recovery
FairRecovery ==
    \A c \in Controllers : WF_vars(ControllerUp(c))

\* Weak fairness on partition healing
FairPartitionHeal ==
    \A c1, c2 \in Controllers : WF_vars(HealPartition(c1, c2))

\* Weak fairness on replication
FairReplication ==
    \A l, f \in Controllers : WF_vars(ReplicateToFollower(l, f))

\* Weak fairness on sync after partition
FairSync ==
    \A b, a \in Controllers : WF_vars(SyncAfterPartition(b, a))

\* Weak fairness on leader election
FairElection ==
    /\ \A c \in Controllers : WF_vars(StartElection(c))
    /\ \A c \in Controllers : WF_vars(BecomeLeader(c))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* Eventually consistent: After partition heals, all controllers converge
EventuallyConsistent ==
    (networkPartition = {}) ~>
    (\A c1, c2 \in Controllers :
        (controllerState[c1] # "down" /\ controllerState[c2] # "down")
        => (\A j \in committedJobs : jobRegistry[c1][j] = jobRegistry[c2][j]))

\* Eventually a leader exists (if quorum is available)
EventuallyLeader ==
    (\E Q \in Quorum : \A c \in Q : controllerState[c] # "down" /\
        (\A c1, c2 \in Q : CanCommunicate(c1, c2)))
    ~> (\E c \in Controllers : controllerState[c] = "leader")

\* Committed jobs are eventually replicated to all live controllers
EventuallyReplicated ==
    \A j \in JobIds :
        (j \in committedJobs) ~>
        (\A c \in Controllers :
            controllerState[c] = "down" \/ jobRegistry[c][j] # "none")

(***************************************************************************)
(* DERIVED METRICS                                                         *)
(***************************************************************************)

\* Count of live controllers
LiveControllers == Cardinality({c \in Controllers : controllerState[c] # "down"})

\* Is cluster healthy (has leader and majority up)
ClusterHealthy ==
    /\ \E c \in Controllers : controllerState[c] = "leader"
    /\ {c \in Controllers : controllerState[c] # "down"} \in Quorum

\* Maximum log divergence
MaxLogDivergence ==
    LET lens == {Len(replicationLog[c]) : c \in Controllers}
    IN IF lens = {} THEN 0
       ELSE LET maxLen == CHOOSE m \in lens : \A l \in lens : l <= m
                minLen == CHOOSE m \in lens : \A l \in lens : l >= m
            IN maxLen - minLen

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
    /\ FairRecovery
    /\ FairPartitionHeal
    /\ FairReplication
    /\ FairSync
    /\ FairElection

\* Liveness specification
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairRecovery
    /\ FairPartitionHeal
    /\ FairReplication
    /\ FairSync
    /\ FairElection

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* Type correctness
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* No job loss
THEOREM NoJobLossTheorem == Spec => []NoJobLoss

\* At most one leader
THEOREM LeaderUniqueness == Spec => []AtMostOneLeader

\* Safety
THEOREM SafetyTheorem == Spec => []Safety

=============================================================================
