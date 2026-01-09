--------------------------- MODULE FlurmJobLifecycle ---------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Job State Machine                          *)
(*                                                                         *)
(* This module models the job lifecycle state machine, proving:            *)
(* - All valid state transitions are correctly modeled                     *)
(* - No invalid transitions are possible                                   *)
(* - Terminal states are absorbing (no escape)                             *)
(* - Time limits correctly trigger timeout transitions                     *)
(*                                                                         *)
(* Job States:                                                             *)
(*   PENDING   -> Job submitted, waiting for resources                     *)
(*   RUNNING   -> Job executing on allocated nodes                         *)
(*   COMPLETING -> Job finished, cleanup in progress                       *)
(*   COMPLETED -> Job finished successfully                                *)
(*   FAILED    -> Job terminated with error                                *)
(*   TIMEOUT   -> Job exceeded time limit                                  *)
(*   CANCELLED -> Job cancelled by user                                    *)
(*   SUSPENDED -> Job suspended (preempted or held)                        *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2024                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    MaxJobs,            \* Maximum number of jobs to track
    MaxTimeLimit        \* Maximum job time limit (in abstract time units)

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    jobState,           \* Function: JobId -> State
    jobTimeElapsed,     \* Function: JobId -> Time elapsed (for running jobs)
    jobTimeLimit,       \* Function: JobId -> Time limit
    clock               \* Global abstract clock for timeout checking

vars == <<jobState, jobTimeElapsed, jobTimeLimit, clock>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

JobIds == 1..MaxJobs

\* All possible job states
AllStates == {
    "NULL",         \* Job slot not yet used
    "PENDING",      \* Waiting in queue
    "RUNNING",      \* Executing
    "COMPLETING",   \* Finished, cleaning up
    "COMPLETED",    \* Success
    "FAILED",       \* Error
    "TIMEOUT",      \* Time limit exceeded
    "CANCELLED",    \* User cancelled
    "SUSPENDED"     \* Held/preempted
}

\* Terminal states - job cannot leave these
TerminalStates == {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"}

\* Active states - job is using resources
ActiveStates == {"RUNNING", "COMPLETING"}

\* Non-terminal states
NonTerminalStates == AllStates \ TerminalStates

(***************************************************************************)
(* VALID STATE TRANSITIONS                                                 *)
(*                                                                         *)
(* Transition graph:                                                       *)
(*                                                                         *)
(*     +---------+                                                         *)
(*     |  NULL   |                                                         *)
(*     +----+----+                                                         *)
(*          | submit                                                       *)
(*          v                                                              *)
(*     +---------+  cancel   +-----------+                                 *)
(*     | PENDING |---------->| CANCELLED |                                 *)
(*     +----+----+           +-----------+                                 *)
(*          |                      ^                                       *)
(*          | schedule             |                                       *)
(*          v                      |                                       *)
(*     +---------+  cancel    -----+                                       *)
(*     | RUNNING |--------------------+                                    *)
(*     +----+----+                    |                                    *)
(*      |   |   |                     |                                    *)
(*      |   |   | timeout             |                                    *)
(*      |   |   +-------> +---------+ |                                    *)
(*      |   |             | TIMEOUT | |                                    *)
(*      |   |             +---------+ |                                    *)
(*      |   | fail                    |                                    *)
(*      |   +-------> +---------+     |                                    *)
(*      |             | FAILED  |     |                                    *)
(*      |             +---------+     |                                    *)
(*      | complete                    |                                    *)
(*      v                             |                                    *)
(*  +------------+                    |                                    *)
(*  | COMPLETING |--------------------+                                    *)
(*  +-----+------+                                                         *)
(*        | finish                                                         *)
(*        v                                                                *)
(*  +-----------+                                                          *)
(*  | COMPLETED |                                                          *)
(*  +-----------+                                                          *)
(*                                                                         *)
(* Additional: RUNNING <-> SUSPENDED (preempt/resume)                      *)
(*                                                                         *)
(***************************************************************************)

\* Define valid transitions as a relation
ValidTransition(from, to) ==
    \/ from = "NULL"       /\ to = "PENDING"
    \/ from = "PENDING"    /\ to \in {"RUNNING", "CANCELLED", "SUSPENDED"}
    \/ from = "RUNNING"    /\ to \in {"COMPLETING", "FAILED", "TIMEOUT", "CANCELLED", "SUSPENDED"}
    \/ from = "COMPLETING" /\ to \in {"COMPLETED", "FAILED", "CANCELLED"}
    \/ from = "SUSPENDED"  /\ to \in {"PENDING", "RUNNING", "CANCELLED"}
    \* Terminal states have no outgoing transitions

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ jobState \in [JobIds -> AllStates]
    /\ jobTimeElapsed \in [JobIds -> 0..MaxTimeLimit]
    /\ jobTimeLimit \in [JobIds -> 0..MaxTimeLimit]
    /\ clock \in 0..(MaxTimeLimit * MaxJobs)

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* Terminal states are truly terminal (absorbing)
TerminalStatesAbsorbing ==
    \A j \in JobIds :
        jobState[j] \in TerminalStates =>
            jobState'[j] = jobState[j]

\* Only valid transitions occur
OnlyValidTransitions ==
    \A j \in JobIds :
        (jobState[j] # jobState'[j]) =>
            ValidTransition(jobState[j], jobState'[j])

\* Time elapsed only increases for running jobs
TimeMonotonicity ==
    \A j \in JobIds :
        (jobState[j] = "RUNNING") => jobTimeElapsed[j] <= jobTimeLimit[j]

\* Jobs in non-running states don't accumulate time
TimeOnlyWhenRunning ==
    \A j \in JobIds :
        (jobState[j] \notin ActiveStates) => jobTimeElapsed[j] = 0

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ jobState = [j \in JobIds |-> "NULL"]
    /\ jobTimeElapsed = [j \in JobIds |-> 0]
    /\ jobTimeLimit = [j \in JobIds |-> 0]
    /\ clock = 0

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* Submit a new job
SubmitJob(j, timeLimit) ==
    /\ jobState[j] = "NULL"
    /\ timeLimit \in 1..MaxTimeLimit
    /\ jobState' = [jobState EXCEPT ![j] = "PENDING"]
    /\ jobTimeLimit' = [jobTimeLimit EXCEPT ![j] = timeLimit]
    /\ UNCHANGED <<jobTimeElapsed, clock>>

\* Schedule a pending job to run
ScheduleJob(j) ==
    /\ jobState[j] = "PENDING"
    /\ jobState' = [jobState EXCEPT ![j] = "RUNNING"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

\* Job progresses (time passes while running)
\* This models the passage of time for a running job
TickJob(j) ==
    /\ jobState[j] = "RUNNING"
    /\ jobTimeElapsed[j] < jobTimeLimit[j]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = @ + 1]
    /\ clock' = clock + 1
    /\ UNCHANGED <<jobState, jobTimeLimit>>

\* Job times out (time limit exceeded)
TimeoutJob(j) ==
    /\ jobState[j] = "RUNNING"
    /\ jobTimeElapsed[j] >= jobTimeLimit[j]
    /\ jobState' = [jobState EXCEPT ![j] = "TIMEOUT"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

\* Job completes normally (enters completing phase)
CompleteJob(j) ==
    /\ jobState[j] = "RUNNING"
    /\ jobTimeElapsed[j] < jobTimeLimit[j]  \* Not timed out
    /\ jobState' = [jobState EXCEPT ![j] = "COMPLETING"]
    /\ UNCHANGED <<jobTimeElapsed, jobTimeLimit, clock>>

\* Job finishes cleanup (completing -> completed)
FinishJob(j) ==
    /\ jobState[j] = "COMPLETING"
    /\ jobState' = [jobState EXCEPT ![j] = "COMPLETED"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

\* Job fails (from running or completing)
FailJob(j) ==
    /\ jobState[j] \in {"RUNNING", "COMPLETING"}
    /\ jobState' = [jobState EXCEPT ![j] = "FAILED"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

\* Cancel a job (from non-terminal states)
CancelJob(j) ==
    /\ jobState[j] \in {"PENDING", "RUNNING", "COMPLETING", "SUSPENDED"}
    /\ jobState' = [jobState EXCEPT ![j] = "CANCELLED"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

\* Suspend a running job (preemption)
SuspendJob(j) ==
    /\ jobState[j] = "RUNNING"
    /\ jobState' = [jobState EXCEPT ![j] = "SUSPENDED"]
    \* Preserve elapsed time for resumption
    /\ UNCHANGED <<jobTimeElapsed, jobTimeLimit, clock>>

\* Resume a suspended job
ResumeJob(j) ==
    /\ jobState[j] = "SUSPENDED"
    /\ jobState' = [jobState EXCEPT ![j] = "RUNNING"]
    \* Continue from where it left off
    /\ UNCHANGED <<jobTimeElapsed, jobTimeLimit, clock>>

\* Requeue a suspended job back to pending
RequeueJob(j) ==
    /\ jobState[j] = "SUSPENDED"
    /\ jobState' = [jobState EXCEPT ![j] = "PENDING"]
    /\ jobTimeElapsed' = [jobTimeElapsed EXCEPT ![j] = 0]
    /\ UNCHANGED <<jobTimeLimit, clock>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E j \in JobIds, t \in 1..MaxTimeLimit : SubmitJob(j, t)
    \/ \E j \in JobIds : ScheduleJob(j)
    \/ \E j \in JobIds : TickJob(j)
    \/ \E j \in JobIds : TimeoutJob(j)
    \/ \E j \in JobIds : CompleteJob(j)
    \/ \E j \in JobIds : FinishJob(j)
    \/ \E j \in JobIds : FailJob(j)
    \/ \E j \in JobIds : CancelJob(j)
    \/ \E j \in JobIds : SuspendJob(j)
    \/ \E j \in JobIds : ResumeJob(j)
    \/ \E j \in JobIds : RequeueJob(j)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Running jobs must eventually terminate (complete, fail, or timeout)
FairTermination ==
    \A j \in JobIds :
        WF_vars(CompleteJob(j) \/ FailJob(j) \/ TimeoutJob(j) \/ CancelJob(j))

\* Time must progress for running jobs
FairTimePassing ==
    \A j \in JobIds : WF_vars(TickJob(j))

\* Completing jobs must finish
FairCompletion ==
    \A j \in JobIds : WF_vars(FinishJob(j))

\* Pending jobs get scheduled (weak fairness)
FairScheduling ==
    \A j \in JobIds : WF_vars(ScheduleJob(j))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* All non-null jobs eventually reach terminal state
AllJobsTerminate ==
    \A j \in JobIds :
        (jobState[j] # "NULL") ~> (jobState[j] \in TerminalStates)

\* Running jobs either complete, fail, or timeout
RunningJobsProgress ==
    \A j \in JobIds :
        (jobState[j] = "RUNNING") ~>
        (jobState[j] \in TerminalStates \cup {"COMPLETING", "SUSPENDED"})

\* Completing jobs reach terminal state
CompletingJobsFinish ==
    \A j \in JobIds :
        (jobState[j] = "COMPLETING") ~> (jobState[j] \in TerminalStates)

\* Suspended jobs don't stay suspended forever (under fairness)
SuspendedJobsProgress ==
    \A j \in JobIds :
        (jobState[j] = "SUSPENDED") ~>
        (jobState[j] \in {"RUNNING", "PENDING", "CANCELLED"})

\* Time limits are enforced
TimeLimitsEnforced ==
    \A j \in JobIds :
        [](jobTimeElapsed[j] <= jobTimeLimit[j] \/ jobState[j] \in TerminalStates)

(***************************************************************************)
(* DERIVED PROPERTIES                                                      *)
(***************************************************************************)

\* Count jobs in each state (for model checking)
JobsInState(s) == Cardinality({j \in JobIds : jobState[j] = s})

TotalPending == JobsInState("PENDING")
TotalRunning == JobsInState("RUNNING")
TotalTerminal == Cardinality({j \in JobIds : jobState[j] \in TerminalStates})

(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

\* Basic specification (safety only)
SafetySpec ==
    Init /\ [][Next]_vars

\* Full specification with fairness
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairTermination
    /\ FairTimePassing
    /\ FairCompletion

\* Specification for liveness checking
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairTermination
    /\ FairTimePassing
    /\ FairCompletion
    /\ FairScheduling

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* Type correctness
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* Terminal states are absorbing
THEOREM TerminalAbsorbing ==
    SafetySpec => [](
        \A j \in JobIds :
            (jobState[j] \in TerminalStates) =>
            [][jobState[j] = jobState'[j]]_vars
    )

\* Time bounds are respected
THEOREM TimeBounds ==
    Spec => [](\A j \in JobIds : jobTimeElapsed[j] <= MaxTimeLimit)

=============================================================================
