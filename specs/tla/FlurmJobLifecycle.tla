---------------------------- MODULE FlurmJobLifecycle -------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Job State Machine                          *)
(*                                                                         *)
(* This specification models the job lifecycle and proves:                 *)
(* - All state transitions are valid                                       *)
(* - Terminal states are stable                                            *)
(* - Job progresses through expected states                                *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    JobIds,             \* Set of job identifiers
    StepIds             \* Set of step identifiers per job

VARIABLES
    jobState,           \* Map: JobId -> State
    jobSteps,           \* Map: JobId -> Set of StepIds
    stepState,          \* Map: (JobId, StepId) -> State
    jobStartTime,       \* Map: JobId -> Time or NULL
    jobEndTime          \* Map: JobId -> Time or NULL

(***************************************************************************)
(* Type Definitions                                                        *)
(***************************************************************************)

\* Valid job states
JobStates == {
    "pending",      \* Waiting for resources
    "configuring",  \* Setting up execution environment
    "running",      \* Actively executing
    "completing",   \* Tasks finished, cleaning up
    "completed",    \* Successfully finished
    "failed",       \* Error during execution
    "cancelled",    \* User cancelled
    "timeout"       \* Time limit exceeded
}

\* Terminal states (no further transitions)
TerminalStates == {"completed", "failed", "cancelled", "timeout"}

\* Step states
StepStates == {"pending", "running", "completed", "failed", "cancelled"}

NULL == CHOOSE x : x \notin (JobIds \cup StepIds \cup Nat)

(***************************************************************************)
(* Valid State Transitions                                                 *)
(***************************************************************************)

\* Define valid transitions for jobs
ValidJobTransitions == [
    pending |-> {"configuring", "cancelled", "failed"},
    configuring |-> {"running", "failed", "cancelled"},
    running |-> {"completing", "failed", "cancelled", "timeout"},
    completing |-> {"completed", "failed"},
    completed |-> {},           \* Terminal - no transitions
    failed |-> {},              \* Terminal - no transitions
    cancelled |-> {},           \* Terminal - no transitions
    timeout |-> {}              \* Terminal - no transitions
]

\* Define valid transitions for steps
ValidStepTransitions == [
    pending |-> {"running", "cancelled", "failed"},
    running |-> {"completed", "failed", "cancelled"},
    completed |-> {},
    failed |-> {},
    cancelled |-> {}
]

(***************************************************************************)
(* Type Invariant                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ jobState \in [JobIds -> JobStates \cup {NULL}]
    /\ jobSteps \in [JobIds -> SUBSET StepIds]
    /\ stepState \in [JobIds \X StepIds -> StepStates \cup {NULL}]
    /\ jobStartTime \in [JobIds -> Nat \cup {NULL}]
    /\ jobEndTime \in [JobIds -> Nat \cup {NULL}]

(***************************************************************************)
(* Initial State                                                           *)
(***************************************************************************)

Init ==
    /\ jobState = [j \in JobIds |-> NULL]
    /\ jobSteps = [j \in JobIds |-> {}]
    /\ stepState = [p \in JobIds \X StepIds |-> NULL]
    /\ jobStartTime = [j \in JobIds |-> NULL]
    /\ jobEndTime = [j \in JobIds |-> NULL]

(***************************************************************************)
(* Helper Predicates                                                       *)
(***************************************************************************)

\* Check if job exists
JobExists(j) == jobState[j] # NULL

\* Check if job is in terminal state
JobTerminated(j) == 
    /\ JobExists(j)
    /\ jobState[j] \in TerminalStates

\* Check if all steps are complete or failed
AllStepsFinished(j) ==
    \A s \in jobSteps[j] :
        stepState[<<j, s>>] \in {"completed", "failed", "cancelled"}

\* Check if any step failed
AnyStepFailed(j) ==
    \E s \in jobSteps[j] : stepState[<<j, s>>] = "failed"

\* Get current time (modeled as increasing counter)
VARIABLE time
CurrentTime == time

(***************************************************************************)
(* Job Actions                                                             *)
(***************************************************************************)

\* Submit a new job
SubmitJob(j) ==
    /\ jobState[j] = NULL
    /\ jobState' = [jobState EXCEPT ![j] = "pending"]
    /\ UNCHANGED <<jobSteps, stepState, jobStartTime, jobEndTime>>

\* Transition job to configuring (resources allocated)
ConfigureJob(j) ==
    /\ jobState[j] = "pending"
    /\ jobState' = [jobState EXCEPT ![j] = "configuring"]
    /\ UNCHANGED <<jobSteps, stepState, jobStartTime, jobEndTime>>

\* Start job execution
StartJob(j) ==
    /\ jobState[j] = "configuring"
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ jobStartTime' = [jobStartTime EXCEPT ![j] = CurrentTime]
    /\ UNCHANGED <<jobSteps, stepState, jobEndTime>>

\* Job begins completion (all tasks done)
BeginCompleting(j) ==
    /\ jobState[j] = "running"
    /\ AllStepsFinished(j)
    /\ jobState' = [jobState EXCEPT ![j] = "completing"]
    /\ UNCHANGED <<jobSteps, stepState, jobStartTime, jobEndTime>>

\* Job completes successfully
CompleteJob(j) ==
    /\ jobState[j] = "completing"
    /\ ~AnyStepFailed(j)
    /\ jobState' = [jobState EXCEPT ![j] = "completed"]
    /\ jobEndTime' = [jobEndTime EXCEPT ![j] = CurrentTime]
    /\ UNCHANGED <<jobSteps, stepState, jobStartTime>>

\* Job fails
FailJob(j) ==
    /\ jobState[j] \in {"pending", "configuring", "running", "completing"}
    /\ jobState' = [jobState EXCEPT ![j] = "failed"]
    /\ jobEndTime' = [jobEndTime EXCEPT ![j] = CurrentTime]
    /\ \* Cancel all pending/running steps
       stepState' = [p \in JobIds \X StepIds |->
           IF p[1] = j /\ stepState[p] \in {"pending", "running"}
           THEN "cancelled"
           ELSE stepState[p]]
    /\ UNCHANGED <<jobSteps, jobStartTime>>

\* Cancel job
CancelJob(j) ==
    /\ jobState[j] \in {"pending", "configuring", "running"}
    /\ jobState' = [jobState EXCEPT ![j] = "cancelled"]
    /\ jobEndTime' = [jobEndTime EXCEPT ![j] = CurrentTime]
    /\ \* Cancel all pending/running steps
       stepState' = [p \in JobIds \X StepIds |->
           IF p[1] = j /\ stepState[p] \in {"pending", "running"}
           THEN "cancelled"
           ELSE stepState[p]]
    /\ UNCHANGED <<jobSteps, jobStartTime>>

\* Job times out
TimeoutJob(j) ==
    /\ jobState[j] = "running"
    /\ jobState' = [jobState EXCEPT ![j] = "timeout"]
    /\ jobEndTime' = [jobEndTime EXCEPT ![j] = CurrentTime]
    /\ \* Cancel all pending/running steps
       stepState' = [p \in JobIds \X StepIds |->
           IF p[1] = j /\ stepState[p] \in {"pending", "running"}
           THEN "cancelled"
           ELSE stepState[p]]
    /\ UNCHANGED <<jobSteps, jobStartTime>>

(***************************************************************************)
(* Step Actions                                                            *)
(***************************************************************************)

\* Create a new step for a job
CreateStep(j, s) ==
    /\ jobState[j] \in {"configuring", "running"}
    /\ s \notin jobSteps[j]
    /\ jobSteps' = [jobSteps EXCEPT ![j] = jobSteps[j] \cup {s}]
    /\ stepState' = [stepState EXCEPT ![<<j, s>>] = "pending"]
    /\ UNCHANGED <<jobState, jobStartTime, jobEndTime>>

\* Start a step
StartStep(j, s) ==
    /\ jobState[j] = "running"
    /\ s \in jobSteps[j]
    /\ stepState[<<j, s>>] = "pending"
    /\ stepState' = [stepState EXCEPT ![<<j, s>>] = "running"]
    /\ UNCHANGED <<jobState, jobSteps, jobStartTime, jobEndTime>>

\* Complete a step
CompleteStep(j, s) ==
    /\ s \in jobSteps[j]
    /\ stepState[<<j, s>>] = "running"
    /\ stepState' = [stepState EXCEPT ![<<j, s>>] = "completed"]
    /\ UNCHANGED <<jobState, jobSteps, jobStartTime, jobEndTime>>

\* Fail a step
FailStep(j, s) ==
    /\ s \in jobSteps[j]
    /\ stepState[<<j, s>>] \in {"pending", "running"}
    /\ stepState' = [stepState EXCEPT ![<<j, s>>] = "failed"]
    /\ UNCHANGED <<jobState, jobSteps, jobStartTime, jobEndTime>>

(***************************************************************************)
(* Next State Relation                                                     *)
(***************************************************************************)

Next ==
    \/ \E j \in JobIds : SubmitJob(j)
    \/ \E j \in JobIds : ConfigureJob(j)
    \/ \E j \in JobIds : StartJob(j)
    \/ \E j \in JobIds : BeginCompleting(j)
    \/ \E j \in JobIds : CompleteJob(j)
    \/ \E j \in JobIds : FailJob(j)
    \/ \E j \in JobIds : CancelJob(j)
    \/ \E j \in JobIds : TimeoutJob(j)
    \/ \E j \in JobIds, s \in StepIds : CreateStep(j, s)
    \/ \E j \in JobIds, s \in StepIds : StartStep(j, s)
    \/ \E j \in JobIds, s \in StepIds : CompleteStep(j, s)
    \/ \E j \in JobIds, s \in StepIds : FailStep(j, s)

Spec == Init /\ [][Next]_<<jobState, jobSteps, stepState, jobStartTime, jobEndTime>>

(***************************************************************************)
(* Safety Properties                                                       *)
(***************************************************************************)

\* INVARIANT: All job state transitions are valid
ValidJobTransitionsInvariant ==
    \A j \in JobIds :
        JobExists(j) =>
            \A nextState \in JobStates :
                (jobState'[j] = nextState /\ jobState[j] # nextState) =>
                    nextState \in ValidJobTransitions[jobState[j]]

\* INVARIANT: Terminal states are stable (no transitions out)
TerminalStatesStable ==
    \A j \in JobIds :
        JobTerminated(j) => jobState'[j] = jobState[j]

\* INVARIANT: Running jobs have start time
RunningJobsHaveStartTime ==
    \A j \in JobIds :
        (JobExists(j) /\ jobState[j] = "running") =>
            jobStartTime[j] # NULL

\* INVARIANT: Terminated jobs have end time
TerminatedJobsHaveEndTime ==
    \A j \in JobIds :
        JobTerminated(j) => jobEndTime[j] # NULL

\* INVARIANT: Steps only exist for existing jobs
StepsOnlyForExistingJobs ==
    \A j \in JobIds, s \in StepIds :
        stepState[<<j, s>>] # NULL => JobExists(j)

\* INVARIANT: Step state consistent with job state
StepStateConsistentWithJob ==
    \A j \in JobIds, s \in StepIds :
        /\ (stepState[<<j, s>>] = "running") => (jobState[j] = "running")
        /\ (jobState[j] \in TerminalStates) =>
            (stepState[<<j, s>>] = NULL \/ 
             stepState[<<j, s>>] \in {"completed", "failed", "cancelled"})

\* Combined safety
Safety ==
    /\ TerminalStatesStable
    /\ RunningJobsHaveStartTime
    /\ StepsOnlyForExistingJobs
    /\ StepStateConsistentWithJob

(***************************************************************************)
(* Liveness Properties                                                     *)
(***************************************************************************)

\* LIVENESS: Pending jobs eventually leave pending state
PendingJobsProgress ==
    \A j \in JobIds :
        (JobExists(j) /\ jobState[j] = "pending") ~>
            (jobState[j] # "pending")

\* LIVENESS: Running jobs eventually terminate
RunningJobsTerminate ==
    \A j \in JobIds :
        (JobExists(j) /\ jobState[j] = "running") ~>
            (jobState[j] \in TerminalStates)

=============================================================================
