---------------------------- MODULE FlurmScheduler ----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Scheduler                                  *)
(*                                                                         *)
(* This specification models the core scheduling algorithm and proves:     *)
(* - No resource over-allocation                                           *)
(* - Jobs in running state have allocated resources                        *)
(* - All submitted jobs eventually complete (under fairness)               *)
(* - No job starvation                                                     *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS 
    Nodes,              \* Set of node identifiers
    MaxJobs,            \* Maximum number of jobs
    MaxCPUsPerNode,     \* CPUs per node
    MaxMemoryPerNode    \* Memory per node (MB)

VARIABLES
    jobs,               \* Map: JobId -> Job record
    nodes,              \* Map: NodeId -> Node record  
    pendingQueue,       \* Sequence of pending job IDs
    nextJobId           \* Counter for job IDs

(***************************************************************************)
(* Type Definitions                                                        *)
(***************************************************************************)

JobStates == {"pending", "running", "completed", "failed", "cancelled"}

Job == [
    id: 1..MaxJobs,
    state: JobStates,
    cpus: 1..MaxCPUsPerNode,
    memory: 1..MaxMemoryPerNode,
    allocatedNode: Nodes \cup {NULL}
]

Node == [
    id: Nodes,
    state: {"up", "down", "drain"},
    totalCpus: 1..MaxCPUsPerNode,
    totalMemory: 1..MaxMemoryPerNode,
    usedCpus: 0..MaxCPUsPerNode,
    usedMemory: 0..MaxMemoryPerNode
]

NULL == CHOOSE x : x \notin Nodes

(***************************************************************************)
(* Type Invariant                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ jobs \in [1..MaxJobs -> Job \cup {NULL}]
    /\ nodes \in [Nodes -> Node]
    /\ pendingQueue \in Seq(1..MaxJobs)
    /\ nextJobId \in 1..(MaxJobs + 1)

(***************************************************************************)
(* Initial State                                                           *)
(***************************************************************************)

Init ==
    /\ jobs = [j \in 1..MaxJobs |-> NULL]
    /\ nodes = [n \in Nodes |-> [
            id |-> n,
            state |-> "up",
            totalCpus |-> MaxCPUsPerNode,
            totalMemory |-> MaxMemoryPerNode,
            usedCpus |-> 0,
            usedMemory |-> 0
       ]]
    /\ pendingQueue = << >>
    /\ nextJobId = 1

(***************************************************************************)
(* Helper Functions                                                        *)
(***************************************************************************)

\* Get all running jobs on a node
RunningJobsOnNode(n) ==
    {j \in DOMAIN jobs : 
        /\ jobs[j] # NULL 
        /\ jobs[j].state = "running"
        /\ jobs[j].allocatedNode = n}

\* Calculate total CPUs used on a node
CpusUsedOnNode(n) ==
    LET runningJobs == RunningJobsOnNode(n)
    IN IF runningJobs = {} THEN 0
       ELSE SUM({jobs[j].cpus : j \in runningJobs})

\* Calculate total memory used on a node
MemoryUsedOnNode(n) ==
    LET runningJobs == RunningJobsOnNode(n)
    IN IF runningJobs = {} THEN 0
       ELSE SUM({jobs[j].memory : j \in runningJobs})

\* Check if node has resources for job
NodeHasResources(n, cpus, memory) ==
    /\ nodes[n].state = "up"
    /\ nodes[n].totalCpus - nodes[n].usedCpus >= cpus
    /\ nodes[n].totalMemory - nodes[n].usedMemory >= memory

\* Find a suitable node for job
FindSuitableNode(cpus, memory) ==
    CHOOSE n \in Nodes : NodeHasResources(n, cpus, memory)

\* Check if any node can run the job
CanScheduleJob(cpus, memory) ==
    \E n \in Nodes : NodeHasResources(n, cpus, memory)

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* Submit a new job
SubmitJob(cpus, memory) ==
    /\ nextJobId <= MaxJobs
    /\ cpus \in 1..MaxCPUsPerNode
    /\ memory \in 1..MaxMemoryPerNode
    /\ LET jobId == nextJobId
       IN /\ jobs' = [jobs EXCEPT ![jobId] = [
                id |-> jobId,
                state |-> "pending",
                cpus |-> cpus,
                memory |-> memory,
                allocatedNode |-> NULL
            ]]
          /\ pendingQueue' = Append(pendingQueue, jobId)
          /\ nextJobId' = nextJobId + 1
          /\ UNCHANGED nodes

\* Schedule a pending job to a node
ScheduleJob ==
    /\ pendingQueue # << >>
    /\ LET jobId == Head(pendingQueue)
           job == jobs[jobId]
       IN /\ job # NULL
          /\ job.state = "pending"
          /\ CanScheduleJob(job.cpus, job.memory)
          /\ LET node == FindSuitableNode(job.cpus, job.memory)
             IN /\ jobs' = [jobs EXCEPT ![jobId].state = "running",
                                        ![jobId].allocatedNode = node]
                /\ nodes' = [nodes EXCEPT ![node].usedCpus = 
                                nodes[node].usedCpus + job.cpus,
                            ![node].usedMemory = 
                                nodes[node].usedMemory + job.memory]
                /\ pendingQueue' = Tail(pendingQueue)
                /\ UNCHANGED nextJobId

\* Complete a running job
CompleteJob(jobId) ==
    /\ jobs[jobId] # NULL
    /\ jobs[jobId].state = "running"
    /\ LET job == jobs[jobId]
           node == job.allocatedNode
       IN /\ jobs' = [jobs EXCEPT ![jobId].state = "completed",
                                  ![jobId].allocatedNode = NULL]
          /\ nodes' = [nodes EXCEPT ![node].usedCpus = 
                            nodes[node].usedCpus - job.cpus,
                        ![node].usedMemory = 
                            nodes[node].usedMemory - job.memory]
          /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Cancel a job (pending or running)
CancelJob(jobId) ==
    /\ jobs[jobId] # NULL
    /\ jobs[jobId].state \in {"pending", "running"}
    /\ LET job == jobs[jobId]
       IN IF job.state = "running"
          THEN LET node == job.allocatedNode
               IN /\ jobs' = [jobs EXCEPT ![jobId].state = "cancelled",
                                          ![jobId].allocatedNode = NULL]
                  /\ nodes' = [nodes EXCEPT ![node].usedCpus = 
                                    nodes[node].usedCpus - job.cpus,
                                ![node].usedMemory = 
                                    nodes[node].usedMemory - job.memory]
                  /\ pendingQueue' = SelectSeq(pendingQueue, 
                                        LAMBDA x : x # jobId)
                  /\ UNCHANGED nextJobId
          ELSE /\ jobs' = [jobs EXCEPT ![jobId].state = "cancelled"]
               /\ pendingQueue' = SelectSeq(pendingQueue, 
                                    LAMBDA x : x # jobId)
               /\ UNCHANGED <<nodes, nextJobId>>

\* Node failure
NodeFails(n) ==
    /\ nodes[n].state = "up"
    /\ nodes' = [nodes EXCEPT ![n].state = "down",
                              ![n].usedCpus = 0,
                              ![n].usedMemory = 0]
    /\ LET affectedJobs == RunningJobsOnNode(n)
       IN jobs' = [j \in DOMAIN jobs |->
            IF j \in affectedJobs
            THEN [jobs[j] EXCEPT !.state = "failed",
                                 !.allocatedNode = NULL]
            ELSE jobs[j]]
    /\ UNCHANGED <<pendingQueue, nextJobId>>

\* Node recovery
NodeRecovers(n) ==
    /\ nodes[n].state = "down"
    /\ nodes' = [nodes EXCEPT ![n].state = "up"]
    /\ UNCHANGED <<jobs, pendingQueue, nextJobId>>

(***************************************************************************)
(* Next State Relation                                                     *)
(***************************************************************************)

Next ==
    \/ \E cpus \in 1..MaxCPUsPerNode, mem \in 1..MaxMemoryPerNode :
        SubmitJob(cpus, mem)
    \/ ScheduleJob
    \/ \E j \in 1..MaxJobs : CompleteJob(j)
    \/ \E j \in 1..MaxJobs : CancelJob(j)
    \/ \E n \in Nodes : NodeFails(n)
    \/ \E n \in Nodes : NodeRecovers(n)

(***************************************************************************)
(* Fairness Conditions                                                     *)
(***************************************************************************)

Fairness ==
    /\ WF_<<jobs, nodes, pendingQueue, nextJobId>>(ScheduleJob)
    /\ \A j \in 1..MaxJobs : WF_<<jobs, nodes, pendingQueue, nextJobId>>(CompleteJob(j))

Spec == Init /\ [][Next]_<<jobs, nodes, pendingQueue, nextJobId>> /\ Fairness

(***************************************************************************)
(* Safety Properties                                                       *)
(***************************************************************************)

\* INVARIANT: Never over-allocate CPUs on any node
NoOverallocationCPUs ==
    \A n \in Nodes :
        nodes[n].usedCpus <= nodes[n].totalCpus

\* INVARIANT: Never over-allocate memory on any node
NoOverallocationMemory ==
    \A n \in Nodes :
        nodes[n].usedMemory <= nodes[n].totalMemory

\* INVARIANT: Running jobs must have allocated node
RunningJobsHaveNodes ==
    \A j \in 1..MaxJobs :
        (jobs[j] # NULL /\ jobs[j].state = "running") =>
            jobs[j].allocatedNode \in Nodes

\* INVARIANT: Completed/cancelled/failed jobs have no allocated node
TerminatedJobsHaveNoNodes ==
    \A j \in 1..MaxJobs :
        (jobs[j] # NULL /\ jobs[j].state \in {"completed", "cancelled", "failed"}) =>
            jobs[j].allocatedNode = NULL

\* INVARIANT: Node resource tracking is consistent
ResourceTrackingConsistent ==
    \A n \in Nodes :
        /\ nodes[n].usedCpus = CpusUsedOnNode(n)
        /\ nodes[n].usedMemory = MemoryUsedOnNode(n)

\* Combined safety property
Safety ==
    /\ NoOverallocationCPUs
    /\ NoOverallocationMemory
    /\ RunningJobsHaveNodes
    /\ TerminatedJobsHaveNoNodes

(***************************************************************************)
(* Liveness Properties                                                     *)
(***************************************************************************)

\* LIVENESS: All submitted jobs eventually terminate
AllJobsTerminate ==
    \A j \in 1..MaxJobs :
        (jobs[j] # NULL /\ jobs[j].state = "pending") ~>
            (jobs[j].state \in {"completed", "cancelled", "failed"})

\* LIVENESS: Pending jobs eventually run (if resources available)
PendingJobsEventuallyRun ==
    \A j \in 1..MaxJobs :
        (jobs[j] # NULL /\ jobs[j].state = "pending") ~>
            (jobs[j].state \in {"running", "cancelled", "failed"})

=============================================================================
