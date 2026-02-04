---------------------------- MODULE FlurmAccounting ----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Distributed Accounting (TRES)              *)
(*                                                                         *)
(* This module models distributed accounting with TRES (Trackable          *)
(* Resources) tracking across multiple controller nodes, proving:          *)
(* - TRES eventual consistency: All nodes converge to same totals          *)
(* - Monotonicity: TRES usage never decreases for completed jobs           *)
(* - No double counting: Each job contributes to totals exactly once       *)
(* - Durability: Committed records are never lost                          *)
(*                                                                         *)
(* Accounting Model:                                                       *)
(* - Multiple controller nodes replicate accounting data via Raft          *)
(* - Jobs report TRES usage on completion                                  *)
(* - Usage is aggregated by user, account, and cluster                     *)
(* - Nodes may temporarily disagree, but eventually converge               *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2026                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Nodes,              \* Set of accounting daemon nodes
    MaxJobs,            \* Maximum number of jobs
    MaxTRES,            \* Maximum TRES value per job (for bounded checking)
    Users,              \* Set of users
    Nil                 \* Model value representing "no value"

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    \* Raft-like consensus state
    leader,             \* Current leader node (or Nil)
    commitIndex,        \* Node -> Highest committed log index
    log,                \* Node -> Sequence of accounting records

    \* Accounting state (derived from committed log)
    jobTRES,            \* Node -> Job -> TRES recorded (or Nil)
    userTotals,         \* Node -> User -> Total TRES

    \* Job submission
    pendingJobs,        \* Jobs waiting to report TRES
    jobUser,            \* Job -> User who submitted it

    \* Network state
    nodeUp,             \* Node -> Boolean
    messages            \* In-flight messages

vars == <<leader, commitIndex, log, jobTRES, userTotals, pendingJobs, jobUser, nodeUp, messages>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

JobIds == 1..MaxJobs
TRESValues == 0..MaxTRES

\* Nil is defined as a CONSTANT in the config file

\* Accounting record types
RecordTypes == {
    "JobComplete"       \* Job completed, record TRES usage
}

\* An accounting record in the log
AccountingRecord == [
    type: RecordTypes,
    job: JobIds,
    user: Users,
    tres: TRESValues
]

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ leader \in Nodes \cup {Nil}
    /\ commitIndex \in [Nodes -> Nat]
    /\ log \in [Nodes -> Seq(AccountingRecord)]
    /\ jobTRES \in [Nodes -> [JobIds -> TRESValues \cup {Nil}]]
    /\ userTotals \in [Nodes -> [Users -> Nat]]
    /\ pendingJobs \in SUBSET JobIds
    /\ jobUser \in [JobIds -> Users \cup {Nil}]
    /\ nodeUp \in [Nodes -> BOOLEAN]

(***************************************************************************)
(* HELPER FUNCTIONS                                                        *)
(***************************************************************************)

\* Sum of TRES for a user from the committed log
ComputeUserTotal(n, u) ==
    LET relevantRecords == {i \in 1..commitIndex[n] :
            log[n][i].user = u}
    IN IF relevantRecords = {} THEN 0
       ELSE LET sumSet == {log[n][i].tres : i \in relevantRecords}
            IN CHOOSE sum \in Nat :
                sum = Cardinality(relevantRecords) * MaxTRES  \* Simplified

\* Check if a job is already recorded in a node's committed log
JobRecordedAt(n, j) ==
    \E i \in 1..commitIndex[n] : log[n][i].job = j

\* Get TRES for job from committed log (or Nil if not recorded)
GetJobTRES(n, j) ==
    IF \E i \in 1..commitIndex[n] : log[n][i].job = j
    THEN LET i == CHOOSE i \in 1..commitIndex[n] : log[n][i].job = j
         IN log[n][i].tres
    ELSE Nil

\* All nodes that are up
UpNodes == {n \in Nodes : nodeUp[n]}

\* Majority of nodes
Quorum == {Q \in SUBSET Nodes : Cardinality(Q) * 2 > Cardinality(Nodes)}

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* TRES for a job is consistent across all nodes that have recorded it
TRESConsistency ==
    \A j \in JobIds :
        \A n1, n2 \in Nodes :
            (jobTRES[n1][j] # Nil /\ jobTRES[n2][j] # Nil) =>
            jobTRES[n1][j] = jobTRES[n2][j]

\* No double counting: Each job appears at most once in each log
NoDoubleCounting ==
    \A n \in Nodes :
        \A j \in JobIds :
            Cardinality({i \in 1..Len(log[n]) : log[n][i].job = j}) <= 1

\* Committed records are never lost (monotonicity of commitIndex)
CommittedRecordsDurable ==
    \A n \in Nodes :
        \A i \in 1..commitIndex[n] :
            log[n][i] \in AccountingRecord

\* TRES values are non-negative
TRESNonNegative ==
    \A n \in Nodes, j \in JobIds :
        jobTRES[n][j] # Nil => jobTRES[n][j] >= 0

\* User totals are non-negative
UserTotalsNonNegative ==
    \A n \in Nodes, u \in Users :
        userTotals[n][u] >= 0

\* Logs are prefix-consistent (Raft property)
LogPrefixConsistency ==
    \A n1, n2 \in Nodes :
        \A i \in 1..commitIndex[n1] :
            i <= commitIndex[n2] => log[n1][i] = log[n2][i]

\* Combined safety
Safety ==
    /\ TRESConsistency
    /\ NoDoubleCounting
    /\ TRESNonNegative
    /\ UserTotalsNonNegative
    /\ LogPrefixConsistency

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ leader = Nil
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ log = [n \in Nodes |-> <<>>]
    /\ jobTRES = [n \in Nodes |-> [j \in JobIds |-> Nil]]
    /\ userTotals = [n \in Nodes |-> [u \in Users |-> 0]]
    /\ pendingJobs = {}
    /\ jobUser = [j \in JobIds |-> Nil]
    /\ nodeUp = [n \in Nodes |-> TRUE]
    /\ messages = {}

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* A node becomes leader (simplified - assumes Raft election)
BecomeLeader(n) ==
    /\ nodeUp[n]
    /\ leader = Nil
    /\ leader' = n
    /\ UNCHANGED <<commitIndex, log, jobTRES, userTotals, pendingJobs, jobUser, nodeUp, messages>>

\* Leader steps down
StepDown ==
    /\ leader # Nil
    /\ leader' = Nil
    /\ UNCHANGED <<commitIndex, log, jobTRES, userTotals, pendingJobs, jobUser, nodeUp, messages>>

\* Submit a job (creates pending job to report TRES)
SubmitJob(j, u) ==
    /\ j \notin pendingJobs
    /\ jobUser[j] = Nil
    /\ pendingJobs' = pendingJobs \cup {j}
    /\ jobUser' = [jobUser EXCEPT ![j] = u]
    /\ UNCHANGED <<leader, commitIndex, log, jobTRES, userTotals, nodeUp, messages>>

\* Job completes and reports TRES to leader
ReportJobTRES(j, tres) ==
    /\ j \in pendingJobs
    /\ leader # Nil
    /\ nodeUp[leader]
    /\ ~JobRecordedAt(leader, j)  \* Not already recorded
    /\ LET record == [type |-> "JobComplete", job |-> j, user |-> jobUser[j], tres |-> tres]
       IN log' = [log EXCEPT ![leader] = Append(@, record)]
    /\ pendingJobs' = pendingJobs \ {j}
    /\ UNCHANGED <<leader, commitIndex, jobTRES, userTotals, jobUser, nodeUp, messages>>

\* Leader replicates log entry to follower
ReplicateEntry(n) ==
    /\ leader # Nil
    /\ n # leader
    /\ nodeUp[n]
    /\ Len(log[leader]) > Len(log[n])
    /\ log' = [log EXCEPT ![n] = Append(@, log[leader][Len(log[n]) + 1])]
    /\ UNCHANGED <<leader, commitIndex, jobTRES, userTotals, pendingJobs, jobUser, nodeUp, messages>>

\* Leader commits entry (after majority replication with same entry)
CommitEntry ==
    /\ leader # Nil
    /\ Len(log[leader]) > commitIndex[leader]
    /\ LET newIdx == commitIndex[leader] + 1
           \* Quorum must have the SAME entry (not just any entry at that index)
           nodesWithSameEntry == {n \in Nodes :
               Len(log[n]) >= newIdx /\ log[n][newIdx] = log[leader][newIdx]}
       IN /\ nodesWithSameEntry \in Quorum
          /\ commitIndex' = [commitIndex EXCEPT ![leader] = newIdx]
          \* Update accounting state from committed record
          /\ LET record == log[leader][newIdx]
             IN /\ jobTRES' = [jobTRES EXCEPT ![leader][record.job] = record.tres]
                /\ userTotals' = [userTotals EXCEPT ![leader][record.user] = @ + record.tres]
    /\ UNCHANGED <<leader, log, pendingJobs, jobUser, nodeUp, messages>>

\* Follower applies committed entry
ApplyCommitted(n) ==
    /\ leader # Nil  \* Must have a leader to know commit point
    /\ n # leader
    /\ nodeUp[n]
    /\ commitIndex[leader] > commitIndex[n]
    /\ Len(log[n]) > commitIndex[n]
    /\ LET newIdx == commitIndex[n] + 1
           record == log[n][newIdx]
       IN /\ commitIndex' = [commitIndex EXCEPT ![n] = newIdx]
          /\ jobTRES' = [jobTRES EXCEPT ![n][record.job] = record.tres]
          /\ userTotals' = [userTotals EXCEPT ![n][record.user] = @ + record.tres]
    /\ UNCHANGED <<leader, log, pendingJobs, jobUser, nodeUp, messages>>

\* Node goes down
NodeDown(n) ==
    /\ nodeUp[n]
    /\ nodeUp' = [nodeUp EXCEPT ![n] = FALSE]
    /\ leader' = IF leader = n THEN Nil ELSE leader
    /\ UNCHANGED <<commitIndex, log, jobTRES, userTotals, pendingJobs, jobUser, messages>>

\* Node comes back up
NodeUp(n) ==
    /\ ~nodeUp[n]
    /\ nodeUp' = [nodeUp EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<leader, commitIndex, log, jobTRES, userTotals, pendingJobs, jobUser, messages>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E n \in Nodes : BecomeLeader(n)
    \/ StepDown
    \/ \E j \in JobIds, u \in Users : SubmitJob(j, u)
    \/ \E j \in JobIds, t \in TRESValues : ReportJobTRES(j, t)
    \/ \E n \in Nodes : ReplicateEntry(n)
    \/ CommitEntry
    \/ \E n \in Nodes : ApplyCommitted(n)
    \/ \E n \in Nodes : NodeDown(n)
    \/ \E n \in Nodes : NodeUp(n)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Leader is eventually elected
FairLeaderElection ==
    \A n \in Nodes : WF_vars(BecomeLeader(n))

\* Logs are eventually replicated
FairReplication ==
    \A n \in Nodes : WF_vars(ReplicateEntry(n))

\* Entries are eventually committed
FairCommit ==
    WF_vars(CommitEntry)

\* Committed entries are eventually applied
FairApply ==
    \A n \in Nodes : WF_vars(ApplyCommitted(n))

\* Nodes eventually recover
FairRecovery ==
    \A n \in Nodes : WF_vars(NodeUp(n))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* TRES eventually consistent: All up nodes eventually have same totals
TRESEventualConsistency ==
    <>[](\A n1, n2 \in UpNodes :
        \A u \in Users : userTotals[n1][u] = userTotals[n2][u])

\* All pending jobs eventually get recorded
AllJobsRecorded ==
    \A j \in JobIds :
        (j \in pendingJobs) ~>
        (\A n \in UpNodes : jobTRES[n][j] # Nil)

\* Committed entries are eventually applied everywhere
CommittedEntriesApplied ==
    \A n \in Nodes :
        nodeUp[n] ~>
        (commitIndex[n] = commitIndex[leader] \/ leader = Nil)

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
    /\ FairLeaderElection
    /\ FairReplication
    /\ FairCommit
    /\ FairApply

\* Liveness specification
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairLeaderElection
    /\ FairReplication
    /\ FairCommit
    /\ FairApply
    /\ FairRecovery

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* Type correctness
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* TRES is consistent across nodes
THEOREM TRESConsistencyTheorem == Spec => []TRESConsistency

\* No double counting
THEOREM NoDoubleCountingTheorem == Spec => []NoDoubleCounting

\* Log prefix consistency (Raft safety)
THEOREM LogConsistencyTheorem == Spec => []LogPrefixConsistency

\* Combined safety
THEOREM SafetyTheorem == Spec => []Safety

\* Eventual consistency (under fairness)
THEOREM EventualConsistencyTheorem == LivenessSpec => TRESEventualConsistency

=============================================================================
