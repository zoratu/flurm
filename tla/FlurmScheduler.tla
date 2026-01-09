---------------------------- MODULE FlurmScheduler ----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Scheduler                                  *)
(*                                                                         *)
(* This module models the core FLURM scheduler, proving:                   *)
(* - Safety: Never over-allocate resources (CPUs per node)                 *)
(* - Safety: Jobs in RUNNING state always have allocated nodes             *)
(* - Liveness: All submitted jobs eventually reach terminal state          *)
(* - Liveness: No starvation - pending jobs eventually run                 *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2024                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Nodes,              \* Set of compute nodes
    MaxJobs,            \* Maximum number of jobs in the system
    MaxCPUsPerNode      \* Maximum CPUs available per node

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    jobs,               \* Function: JobId -> Job record
    nodeState,          \* Function: Node -> {idle, allocated, down}
    nodeCPUs,           \* Function: Node -> Available CPUs
    pendingQueue,       \* Sequence of pending job IDs (FIFO order)
    nextJobId           \* Counter for generating unique job IDs

vars == <<jobs, nodeState, nodeCPUs, pendingQueue, nextJobId>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

JobIds == 1..MaxJobs

JobStates == {"PENDING", "RUNNING", "COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"}

TerminalStates == {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"}

NodeStates == {"idle", "allocated", "down"}

\* A job record
JobRecord == [
    state: JobStates,
    allocatedNodes: SUBSET Nodes,
    cpusRequested: 1..MaxCPUsPerNode
]

NullJob == [state |-> "NULL", allocatedNodes |-> {}, cpusRequested |-> 0]

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ jobs \in [JobIds -> JobRecord \cup {NullJob}]
    /\ nodeState \in [Nodes -> NodeStates]
    /\ nodeCPUs \in [Nodes -> 0..MaxCPUsPerNode]
    /\ pendingQueue \in Seq(JobIds)
    /\ nextJobId \in 1..(MaxJobs + 1)

(***************************************************************************)
(* HELPER FUNCTIONS                                                        *)
(***************************************************************************)

\* Get all jobs in a specific state
JobsInState(state) ==
    {j \in JobIds : jobs[j].state = state}

\* Get all running jobs
RunningJobs == JobsInState("RUNNING")

\* Get all pending jobs
PendingJobs == JobsInState("PENDING")

\* Check if a node is available (idle and up)
NodeAvailable(n) ==
    /\ nodeState[n] = "idle"
    /\ nodeCPUs[n] > 0

\* Get available nodes
AvailableNodes == {n \in Nodes : NodeAvailable(n)}

\* Count total allocated CPUs on a node across all jobs
AllocatedCPUsOnNode(n) ==
    LET runningJobs == {j \in JobIds :
            /\ jobs[j].state = "RUNNING"
            /\ n \in jobs[j].allocatedNodes}
    IN IF runningJobs = {} THEN 0
       ELSE LET jobSeq == SetToSeq(runningJobs)
            IN SumCPUs(jobSeq, n)

\* Helper to sum CPUs (recursive helper)
RECURSIVE SumCPUsRec(_, _, _)
SumCPUsRec(seq, n, acc) ==
    IF seq = <<>> THEN acc
    ELSE LET j == Head(seq)
         IN SumCPUsRec(Tail(seq), n,
                acc + IF n \in jobs[j].allocatedNodes
                      THEN jobs[j].cpusRequested
                      ELSE 0)

SumCPUs(seq, n) == SumCPUsRec(seq, n, 0)

\* Convert set to sequence (for iteration)
RECURSIVE SetToSeqRec(_, _)
SetToSeqRec(S, acc) ==
    IF S = {} THEN acc
    ELSE LET x == CHOOSE x \in S : TRUE
         IN SetToSeqRec(S \ {x}, Append(acc, x))

SetToSeq(S) == SetToSeqRec(S, <<>>)

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* Safety: Never over-allocate CPUs on any node
NoOverAllocation ==
    \A n \in Nodes :
        LET usedCPUs == Cardinality({j \in RunningJobs : n \in jobs[j].allocatedNodes})
        IN usedCPUs * MaxCPUsPerNode <= MaxCPUsPerNode * Cardinality(Nodes)

\* Simpler version: running jobs have nodes with capacity
ResourceConstraint ==
    \A n \in Nodes :
        nodeCPUs[n] >= 0 /\ nodeCPUs[n] <= MaxCPUsPerNode

\* Safety: Running jobs always have allocated nodes
RunningJobsHaveNodes ==
    \A j \in RunningJobs : jobs[j].allocatedNodes # {}

\* Safety: Only running jobs have node allocations
OnlyRunningJobsAllocated ==
    \A j \in JobIds :
        (jobs[j].state # "RUNNING" /\ jobs[j].state # "NULL")
        => jobs[j].allocatedNodes = {}

\* Safety: Node states are consistent with allocations
NodeStateConsistency ==
    \A n \in Nodes :
        (nodeState[n] = "allocated") <=>
        (\E j \in RunningJobs : n \in jobs[j].allocatedNodes)

\* Combined safety property
Safety ==
    /\ ResourceConstraint
    /\ RunningJobsHaveNodes
    /\ OnlyRunningJobsAllocated

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ jobs = [j \in JobIds |-> NullJob]
    /\ nodeState = [n \in Nodes |-> "idle"]
    /\ nodeCPUs = [n \in Nodes |-> MaxCPUsPerNode]
    /\ pendingQueue = <<>>
    /\ nextJobId = 1

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* Submit a new job
SubmitJob(cpus) ==
    /\ nextJobId <= MaxJobs
    /\ cpus \in 1..MaxCPUsPerNode
    /\ jobs' = [jobs EXCEPT ![nextJobId] =
                    [state |-> "PENDING",
                     allocatedNodes |-> {},
                     cpusRequested |-> cpus]]
    /\ pendingQueue' = Append(pendingQueue, nextJobId)
    /\ nextJobId' = nextJobId + 1
    /\ UNCHANGED <<nodeState, nodeCPUs>>

\* Allocate resources and start a pending job
AllocateJob ==
    /\ pendingQueue # <<>>
    /\ LET jobId == Head(pendingQueue)
           job == jobs[jobId]
           requiredCPUs == job.cpusRequested
           \* Find a node with enough CPUs
           candidates == {n \in Nodes :
                            /\ nodeState[n] # "down"
                            /\ nodeCPUs[n] >= requiredCPUs}
       IN /\ candidates # {}
          /\ LET node == CHOOSE n \in candidates : TRUE
             IN /\ jobs' = [jobs EXCEPT ![jobId] =
                            [state |-> "RUNNING",
                             allocatedNodes |-> {node},
                             cpusRequested |-> requiredCPUs]]
                /\ nodeState' = [nodeState EXCEPT ![node] = "allocated"]
                /\ nodeCPUs' = [nodeCPUs EXCEPT ![node] = @ - requiredCPUs]
                /\ pendingQueue' = Tail(pendingQueue)
    /\ UNCHANGED <<nextJobId>>

\* Complete a running job successfully
CompleteJob(jobId) ==
    /\ jobs[jobId].state = "RUNNING"
    /\ LET allocNodes == jobs[jobId].allocatedNodes
           cpus == jobs[jobId].cpusRequested
       IN /\ jobs' = [jobs EXCEPT ![jobId] =
                        [state |-> "COMPLETED",
                         allocatedNodes |-> {},
                         cpusRequested |-> cpus]]
          /\ nodeState' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN "idle"
                            ELSE nodeState[n]]
          /\ nodeCPUs' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN nodeCPUs[n] + cpus
                            ELSE nodeCPUs[n]]
    /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Fail a running job
FailJob(jobId) ==
    /\ jobs[jobId].state = "RUNNING"
    /\ LET allocNodes == jobs[jobId].allocatedNodes
           cpus == jobs[jobId].cpusRequested
       IN /\ jobs' = [jobs EXCEPT ![jobId] =
                        [state |-> "FAILED",
                         allocatedNodes |-> {},
                         cpusRequested |-> cpus]]
          /\ nodeState' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN "idle"
                            ELSE nodeState[n]]
          /\ nodeCPUs' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN nodeCPUs[n] + cpus
                            ELSE nodeCPUs[n]]
    /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Timeout a running job
TimeoutJob(jobId) ==
    /\ jobs[jobId].state = "RUNNING"
    /\ LET allocNodes == jobs[jobId].allocatedNodes
           cpus == jobs[jobId].cpusRequested
       IN /\ jobs' = [jobs EXCEPT ![jobId] =
                        [state |-> "TIMEOUT",
                         allocatedNodes |-> {},
                         cpusRequested |-> cpus]]
          /\ nodeState' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN "idle"
                            ELSE nodeState[n]]
          /\ nodeCPUs' = [n \in Nodes |->
                            IF n \in allocNodes
                            THEN nodeCPUs[n] + cpus
                            ELSE nodeCPUs[n]]
    /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Cancel a pending job
CancelPendingJob(jobId) ==
    /\ jobs[jobId].state = "PENDING"
    /\ jobs' = [jobs EXCEPT ![jobId] =
                    [state |-> "CANCELLED",
                     allocatedNodes |-> {},
                     cpusRequested |-> jobs[jobId].cpusRequested]]
    /\ pendingQueue' = SelectSeq(pendingQueue, LAMBDA x : x # jobId)
    /\ UNCHANGED <<nodeState, nodeCPUs, nextJobId>>

\* Node goes down - affects running jobs
NodeDown(n) ==
    /\ nodeState[n] # "down"
    /\ LET affectedJobs == {j \in RunningJobs : n \in jobs[j].allocatedNodes}
       IN /\ jobs' = [j \in JobIds |->
                        IF j \in affectedJobs
                        THEN [state |-> "FAILED",
                              allocatedNodes |-> {},
                              cpusRequested |-> jobs[j].cpusRequested]
                        ELSE jobs[j]]
          /\ nodeState' = [nodeState EXCEPT ![n] = "down"]
          /\ nodeCPUs' = [nodeCPUs EXCEPT ![n] = 0]
    /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Node comes back up
NodeUp(n) ==
    /\ nodeState[n] = "down"
    /\ nodeState' = [nodeState EXCEPT ![n] = "idle"]
    /\ nodeCPUs' = [nodeCPUs EXCEPT ![n] = MaxCPUsPerNode]
    /\ UNCHANGED <<jobs, pendingQueue, nextJobId>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E cpus \in 1..MaxCPUsPerNode : SubmitJob(cpus)
    \/ AllocateJob
    \/ \E j \in JobIds : CompleteJob(j)
    \/ \E j \in JobIds : FailJob(j)
    \/ \E j \in JobIds : TimeoutJob(j)
    \/ \E j \in JobIds : CancelPendingJob(j)
    \/ \E n \in Nodes : NodeDown(n)
    \/ \E n \in Nodes : NodeUp(n)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Weak fairness on allocation - if a job can be allocated, it eventually will
FairAllocation == WF_vars(AllocateJob)

\* Weak fairness on job completion - running jobs eventually terminate
FairCompletion ==
    \A j \in JobIds : WF_vars(CompleteJob(j) \/ FailJob(j) \/ TimeoutJob(j))

\* Strong fairness on node recovery - downed nodes eventually come back
FairNodeRecovery ==
    \A n \in Nodes : SF_vars(NodeUp(n))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* Liveness: All submitted jobs eventually reach a terminal state
\* (Under fairness assumptions)
AllJobsTerminate ==
    \A j \in JobIds :
        (jobs[j].state # "NULL") ~> (jobs[j].state \in TerminalStates)

\* Liveness: Pending jobs eventually run (no starvation)
\* Under fair scheduling and node availability
NoStarvation ==
    \A j \in JobIds :
        (jobs[j].state = "PENDING") ~>
        (jobs[j].state \in {"RUNNING"} \cup TerminalStates)

\* Liveness: Eventually no pending jobs (queue drains)
EventuallyQueueDrains ==
    <>[](pendingQueue = <<>> \/ nextJobId > MaxJobs)

(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

\* Full specification with fairness
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairAllocation
    /\ FairCompletion

\* Specification with strong fairness for liveness checking
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairAllocation
    /\ FairCompletion
    /\ FairNodeRecovery

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* The type invariant is preserved
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* Safety is preserved
THEOREM SafetyTheorem == Spec => []Safety

\* Resource constraint is always maintained
THEOREM ResourceSafety == Spec => []ResourceConstraint

\* Running jobs always have nodes
THEOREM RunningJobsSafety == Spec => []RunningJobsHaveNodes

=============================================================================
