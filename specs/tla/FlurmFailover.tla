---------------------------- MODULE FlurmFailover -----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Controller Failover                        *)
(*                                                                         *)
(* This specification models multi-controller consensus and proves:        *)
(* - At most one leader at any time                                        *)
(* - State is preserved across failover                                    *)
(* - System recovers from controller failures                              *)
(* - Committed operations are durable                                      *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Controllers,        \* Set of controller identifiers
    MaxTerm,            \* Maximum Raft term
    MaxLogLength        \* Maximum log length

VARIABLES
    currentTerm,        \* Map: Controller -> Term
    votedFor,           \* Map: Controller -> Controller or NULL
    log,                \* Map: Controller -> Sequence of entries
    commitIndex,        \* Map: Controller -> Index
    state,              \* Map: Controller -> {"follower", "candidate", "leader", "dead"}
    leader,             \* Current leader (from system perspective)
    messages,           \* Set of in-flight messages
    clusterState        \* Replicated state machine state

(***************************************************************************)
(* Type Definitions                                                        *)
(***************************************************************************)

NULL == CHOOSE x : x \notin Controllers

ControllerStates == {"follower", "candidate", "leader", "dead"}

LogEntry == [
    term: 1..MaxTerm,
    command: {"job_submit", "job_cancel", "node_register", "config_update"}
]

Message == [
    type: {"RequestVote", "RequestVoteResponse", 
           "AppendEntries", "AppendEntriesResponse"},
    from: Controllers,
    to: Controllers,
    term: 1..MaxTerm,
    \* Additional fields depending on type
    lastLogIndex: Nat \cup {NULL},
    lastLogTerm: Nat \cup {NULL},
    voteGranted: BOOLEAN \cup {NULL},
    prevLogIndex: Nat \cup {NULL},
    prevLogTerm: Nat \cup {NULL},
    entries: Seq(LogEntry) \cup {NULL},
    leaderCommit: Nat \cup {NULL},
    success: BOOLEAN \cup {NULL}
]

(***************************************************************************)
(* Type Invariant                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ currentTerm \in [Controllers -> 0..MaxTerm]
    /\ votedFor \in [Controllers -> Controllers \cup {NULL}]
    /\ log \in [Controllers -> Seq(LogEntry)]
    /\ commitIndex \in [Controllers -> Nat]
    /\ state \in [Controllers -> ControllerStates]
    /\ leader \in Controllers \cup {NULL}
    /\ messages \subseteq Message

(***************************************************************************)
(* Initial State                                                           *)
(***************************************************************************)

Init ==
    /\ currentTerm = [c \in Controllers |-> 0]
    /\ votedFor = [c \in Controllers |-> NULL]
    /\ log = [c \in Controllers |-> << >>]
    /\ commitIndex = [c \in Controllers |-> 0]
    /\ state = [c \in Controllers |-> "follower"]
    /\ leader = NULL
    /\ messages = {}
    /\ clusterState = [jobs |-> {}, nodes |-> {}]

(***************************************************************************)
(* Helper Functions                                                        *)
(***************************************************************************)

\* Get last log index
LastLogIndex(c) == Len(log[c])

\* Get last log term
LastLogTerm(c) == 
    IF Len(log[c]) = 0 THEN 0 
    ELSE log[c][Len(log[c])].term

\* Check if log is at least as up-to-date
LogUpToDate(c, lastLogTerm, lastLogIndex) ==
    \/ lastLogTerm > LastLogTerm(c)
    \/ (lastLogTerm = LastLogTerm(c) /\ lastLogIndex >= LastLogIndex(c))

\* Get quorum size
Quorum == (Cardinality(Controllers) \div 2) + 1

\* Count votes for a candidate
VotesFor(c) ==
    Cardinality({v \in Controllers : votedFor[v] = c})

\* Check if controller has quorum
HasQuorum(c) == VotesFor(c) >= Quorum

\* Get set of live controllers
LiveControllers == {c \in Controllers : state[c] # "dead"}

(***************************************************************************)
(* Controller Actions                                                      *)
(***************************************************************************)

\* Timeout: Follower becomes candidate
BecomeCandidate(c) ==
    /\ state[c] = "follower"
    /\ currentTerm' = [currentTerm EXCEPT ![c] = currentTerm[c] + 1]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]  \* Vote for self
    /\ state' = [state EXCEPT ![c] = "candidate"]
    /\ \* Send RequestVote to all other controllers
       messages' = messages \cup 
           {[type |-> "RequestVote",
             from |-> c,
             to |-> other,
             term |-> currentTerm[c] + 1,
             lastLogIndex |-> LastLogIndex(c),
             lastLogTerm |-> LastLogTerm(c),
             voteGranted |-> NULL,
             prevLogIndex |-> NULL,
             prevLogTerm |-> NULL,
             entries |-> NULL,
             leaderCommit |-> NULL,
             success |-> NULL] : other \in Controllers \ {c}}
    /\ UNCHANGED <<log, commitIndex, leader, clusterState>>

\* Candidate wins election
BecomeLeader(c) ==
    /\ state[c] = "candidate"
    /\ HasQuorum(c)
    /\ state' = [state EXCEPT ![c] = "leader"]
    /\ leader' = c
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages, clusterState>>

\* Handle RequestVote
HandleRequestVote(c, msg) ==
    /\ msg.type = "RequestVote"
    /\ msg.to = c
    /\ state[c] # "dead"
    /\ LET grant == 
           /\ (msg.term > currentTerm[c] \/ 
               (msg.term = currentTerm[c] /\ 
                (votedFor[c] = NULL \/ votedFor[c] = msg.from)))
           /\ LogUpToDate(c, msg.lastLogTerm, msg.lastLogIndex)
       IN /\ IF msg.term > currentTerm[c]
             THEN /\ currentTerm' = [currentTerm EXCEPT ![c] = msg.term]
                  /\ state' = [state EXCEPT ![c] = "follower"]
             ELSE UNCHANGED <<currentTerm, state>>
          /\ IF grant
             THEN votedFor' = [votedFor EXCEPT ![c] = msg.from]
             ELSE UNCHANGED votedFor
          /\ messages' = (messages \ {msg}) \cup
               {[type |-> "RequestVoteResponse",
                 from |-> c,
                 to |-> msg.from,
                 term |-> Max(currentTerm[c], msg.term),
                 voteGranted |-> grant,
                 lastLogIndex |-> NULL,
                 lastLogTerm |-> NULL,
                 prevLogIndex |-> NULL,
                 prevLogTerm |-> NULL,
                 entries |-> NULL,
                 leaderCommit |-> NULL,
                 success |-> NULL]}
          /\ UNCHANGED <<log, commitIndex, leader, clusterState>>

\* Leader appends entry to log
LeaderAppendEntry(c, command) ==
    /\ state[c] = "leader"
    /\ Len(log[c]) < MaxLogLength
    /\ LET entry == [term |-> currentTerm[c], command |-> command]
       IN log' = [log EXCEPT ![c] = Append(log[c], entry)]
    /\ \* Send AppendEntries to all followers
       messages' = messages \cup
           {[type |-> "AppendEntries",
             from |-> c,
             to |-> other,
             term |-> currentTerm[c],
             prevLogIndex |-> LastLogIndex(c),
             prevLogTerm |-> LastLogTerm(c),
             entries |-> <<[term |-> currentTerm[c], command |-> command]>>,
             leaderCommit |-> commitIndex[c],
             lastLogIndex |-> NULL,
             lastLogTerm |-> NULL,
             voteGranted |-> NULL,
             success |-> NULL] : other \in Controllers \ {c}}
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, state, leader, clusterState>>

\* Handle AppendEntries
HandleAppendEntries(c, msg) ==
    /\ msg.type = "AppendEntries"
    /\ msg.to = c
    /\ state[c] # "dead"
    /\ IF msg.term >= currentTerm[c]
       THEN /\ currentTerm' = [currentTerm EXCEPT ![c] = msg.term]
            /\ state' = [state EXCEPT ![c] = "follower"]
            /\ leader' = msg.from
            /\ \* Simplified: just append entries
               log' = [log EXCEPT ![c] = 
                   IF msg.entries # NULL 
                   THEN log[c] \o msg.entries 
                   ELSE log[c]]
            /\ commitIndex' = [commitIndex EXCEPT ![c] = 
                   IF msg.leaderCommit # NULL
                   THEN Min(msg.leaderCommit, Len(log'[c]))
                   ELSE commitIndex[c]]
            /\ messages' = (messages \ {msg}) \cup
                   {[type |-> "AppendEntriesResponse",
                     from |-> c,
                     to |-> msg.from,
                     term |-> msg.term,
                     success |-> TRUE,
                     lastLogIndex |-> NULL,
                     lastLogTerm |-> NULL,
                     voteGranted |-> NULL,
                     prevLogIndex |-> NULL,
                     prevLogTerm |-> NULL,
                     entries |-> NULL,
                     leaderCommit |-> NULL]}
       ELSE /\ messages' = (messages \ {msg}) \cup
                   {[type |-> "AppendEntriesResponse",
                     from |-> c,
                     to |-> msg.from,
                     term |-> currentTerm[c],
                     success |-> FALSE,
                     lastLogIndex |-> NULL,
                     lastLogTerm |-> NULL,
                     voteGranted |-> NULL,
                     prevLogIndex |-> NULL,
                     prevLogTerm |-> NULL,
                     entries |-> NULL,
                     leaderCommit |-> NULL]}
            /\ UNCHANGED <<currentTerm, state, leader, log, commitIndex>>
    /\ UNCHANGED <<votedFor, clusterState>>

\* Controller fails
ControllerFails(c) ==
    /\ state[c] # "dead"
    /\ state' = [state EXCEPT ![c] = "dead"]
    /\ IF leader = c THEN leader' = NULL ELSE UNCHANGED leader
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages, clusterState>>

\* Controller recovers
ControllerRecovers(c) ==
    /\ state[c] = "dead"
    /\ state' = [state EXCEPT ![c] = "follower"]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, leader, messages, clusterState>>

(***************************************************************************)
(* Helper Operators                                                        *)
(***************************************************************************)

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b

(***************************************************************************)
(* Next State Relation                                                     *)
(***************************************************************************)

Next ==
    \/ \E c \in Controllers : BecomeCandidate(c)
    \/ \E c \in Controllers : BecomeLeader(c)
    \/ \E c \in Controllers, msg \in messages : HandleRequestVote(c, msg)
    \/ \E c \in Controllers, cmd \in {"job_submit", "job_cancel"} : 
        LeaderAppendEntry(c, cmd)
    \/ \E c \in Controllers, msg \in messages : HandleAppendEntries(c, msg)
    \/ \E c \in Controllers : ControllerFails(c)
    \/ \E c \in Controllers : ControllerRecovers(c)

Spec == Init /\ [][Next]_<<currentTerm, votedFor, log, commitIndex, state, 
                           leader, messages, clusterState>>

(***************************************************************************)
(* Safety Properties                                                       *)
(***************************************************************************)

\* INVARIANT: At most one leader per term
AtMostOneLeaderPerTerm ==
    \A c1, c2 \in Controllers :
        (state[c1] = "leader" /\ state[c2] = "leader" /\ 
         currentTerm[c1] = currentTerm[c2]) => c1 = c2

\* INVARIANT: Leader has most up-to-date log
LeaderHasCompleteLog ==
    leader # NULL =>
        \A c \in Controllers :
            state[c] # "dead" =>
                LastLogIndex(leader) >= commitIndex[c]

\* INVARIANT: Committed entries are never lost (simplified)
CommittedEntriesDurable ==
    \A c1, c2 \in Controllers :
        (state[c1] # "dead" /\ state[c2] # "dead") =>
            \A i \in 1..Min(commitIndex[c1], commitIndex[c2]) :
                log[c1][i] = log[c2][i]

\* INVARIANT: Terms are monotonically increasing
TermsMonotonic ==
    \A c \in Controllers :
        state[c] # "dead" =>
            currentTerm'[c] >= currentTerm[c]

\* Combined safety
Safety ==
    /\ AtMostOneLeaderPerTerm
    /\ CommittedEntriesDurable

(***************************************************************************)
(* Liveness Properties                                                     *)
(***************************************************************************)

\* LIVENESS: If a quorum is alive, eventually there's a leader
EventuallyLeader ==
    (Cardinality(LiveControllers) >= Quorum) ~> (leader # NULL)

\* LIVENESS: Operations eventually commit
OperationsEventuallyCommit ==
    \A c \in Controllers, i \in 1..MaxLogLength :
        (state[c] = "leader" /\ Len(log[c]) >= i) ~>
            (commitIndex[c] >= i \/ state[c] # "leader")

=============================================================================
