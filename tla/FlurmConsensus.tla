---------------------------- MODULE FlurmConsensus ----------------------------
(***************************************************************************)
(* TLA+ Specification for FLURM Raft-based Consensus                       *)
(*                                                                         *)
(* This module models a simplified Raft consensus protocol, proving:       *)
(* - Leader election safety: At most one leader per term                   *)
(* - Log replication consistency: Logs are consistent across nodes         *)
(* - State machine safety: All nodes apply same commands in same order     *)
(* - Election safety: Only nodes with up-to-date logs become leader        *)
(*                                                                         *)
(* Simplifications from full Raft:                                         *)
(* - Network partitions modeled abstractly                                 *)
(* - Log compaction not modeled                                            *)
(* - Membership changes not modeled                                        *)
(* - Bounded log length and term numbers                                   *)
(*                                                                         *)
(* Author: FLURM Project                                                   *)
(* Date: 2024                                                              *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Servers,            \* Set of server nodes
    MaxTerm,            \* Maximum term number (for bounded checking)
    MaxLogLength,       \* Maximum log entries (for bounded checking)
    Values              \* Set of values that can be proposed

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    \* Per-server state
    currentTerm,        \* Server -> Current term
    votedFor,           \* Server -> Node voted for in current term (or Nil)
    state,              \* Server -> {Follower, Candidate, Leader}
    log,                \* Server -> Sequence of log entries

    \* Leader-specific state
    nextIndex,          \* Leader -> (Server -> next log index to send)
    matchIndex,         \* Leader -> (Server -> highest replicated index)

    \* Committed state
    commitIndex,        \* Server -> Highest committed log index

    \* Network state (abstract)
    messages            \* Set of in-flight messages

serverVars == <<currentTerm, votedFor, state>>
logVars == <<log, commitIndex>>
leaderVars == <<nextIndex, matchIndex>>
vars == <<serverVars, logVars, leaderVars, messages>>

(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

Nil == CHOOSE v : v \notin Servers

ServerStates == {"Follower", "Candidate", "Leader"}

\* Log entry structure
LogEntry == [term: 1..MaxTerm, value: Values]

\* Message types
RequestVoteRequest == [
    type: {"RequestVote"},
    term: 1..MaxTerm,
    candidateId: Servers,
    lastLogIndex: 0..MaxLogLength,
    lastLogTerm: 0..MaxTerm
]

RequestVoteResponse == [
    type: {"RequestVoteResponse"},
    term: 1..MaxTerm,
    voteGranted: BOOLEAN,
    from: Servers,
    to: Servers
]

AppendEntriesRequest == [
    type: {"AppendEntries"},
    term: 1..MaxTerm,
    leaderId: Servers,
    prevLogIndex: 0..MaxLogLength,
    prevLogTerm: 0..MaxTerm,
    entries: Seq(LogEntry),
    leaderCommit: 0..MaxLogLength,
    to: Servers
]

AppendEntriesResponse == [
    type: {"AppendEntriesResponse"},
    term: 1..MaxTerm,
    success: BOOLEAN,
    matchIndex: 0..MaxLogLength,
    from: Servers,
    to: Servers
]

Message ==
    RequestVoteRequest \cup RequestVoteResponse \cup
    AppendEntriesRequest \cup AppendEntriesResponse

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ currentTerm \in [Servers -> 0..MaxTerm]
    /\ votedFor \in [Servers -> Servers \cup {Nil}]
    /\ state \in [Servers -> ServerStates]
    /\ log \in [Servers -> Seq(LogEntry)]
    /\ \A s \in Servers : Len(log[s]) <= MaxLogLength
    /\ commitIndex \in [Servers -> 0..MaxLogLength]
    /\ nextIndex \in [Servers -> [Servers -> 1..(MaxLogLength+1)]]
    /\ matchIndex \in [Servers -> [Servers -> 0..MaxLogLength]]

(***************************************************************************)
(* HELPER FUNCTIONS                                                        *)
(***************************************************************************)

\* The set of all quorums (majority sets)
Quorum == {Q \in SUBSET Servers : Cardinality(Q) * 2 > Cardinality(Servers)}

\* Get the last log index for a server
LastLogIndex(s) == Len(log[s])

\* Get the last log term for a server
LastLogTerm(s) ==
    IF Len(log[s]) = 0 THEN 0
    ELSE log[s][Len(log[s])].term

\* Check if candidate's log is at least as up-to-date as voter's
LogUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex) ==
    \/ candidateLastTerm > voterLastTerm
    \/ (candidateLastTerm = voterLastTerm /\ candidateLastIndex >= voterLastIndex)

\* Find the minimum value in a set
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* Find the maximum value in a set
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

\* Get all servers that are currently leaders
Leaders == {s \in Servers : state[s] = "Leader"}

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* Election Safety: At most one leader per term
ElectionSafety ==
    \A s1, s2 \in Servers :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => s1 = s2

\* Alternative formulation
AtMostOneLeaderPerTerm ==
    \A t \in 1..MaxTerm :
        Cardinality({s \in Servers : state[s] = "Leader" /\ currentTerm[s] = t}) <= 1

\* Log Matching: If two logs contain an entry with same index and term,
\* the logs are identical up to that point
LogMatching ==
    \A s1, s2 \in Servers :
        \A i \in 1..Min({Len(log[s1]), Len(log[s2])}) :
            (log[s1][i].term = log[s2][i].term) =>
            SubSeq(log[s1], 1, i) = SubSeq(log[s2], 1, i)

\* Leader Completeness: If a log entry is committed in a given term,
\* that entry will be present in the logs of leaders for all higher terms
\* (This is implied by the log matching property and election rules)

\* State Machine Safety: If a server has applied an entry at index,
\* no other server will ever apply a different entry at that index
StateMachineSafety ==
    \A s1, s2 \in Servers :
        \A i \in 1..Min({commitIndex[s1], commitIndex[s2]}) :
            log[s1][i] = log[s2][i]

\* Committed entries are durable
CommittedEntriesExist ==
    \A s \in Servers :
        commitIndex[s] <= Len(log[s])

\* Combined safety property
Safety ==
    /\ ElectionSafety
    /\ LogMatching
    /\ StateMachineSafety
    /\ CommittedEntriesExist

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ currentTerm = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ state = [s \in Servers |-> "Follower"]
    /\ log = [s \in Servers |-> <<>>]
    /\ nextIndex = [s \in Servers |-> [t \in Servers |-> 1]]
    /\ matchIndex = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ commitIndex = [s \in Servers |-> 0]
    /\ messages = {}

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* Server s times out and starts an election
StartElection(s) ==
    /\ state[s] \in {"Follower", "Candidate"}
    /\ currentTerm[s] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]  \* Vote for self
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    \* Send RequestVote to all other servers
    /\ messages' = messages \cup {
        [type |-> "RequestVote",
         term |-> currentTerm[s] + 1,
         candidateId |-> s,
         lastLogIndex |-> LastLogIndex(s),
         lastLogTerm |-> LastLogTerm(s)] : t \in Servers \ {s}}
    /\ UNCHANGED <<logVars, leaderVars>>

\* Server s handles a RequestVote request
HandleRequestVote(s) ==
    \E m \in messages :
        /\ m.type = "RequestVote"
        /\ LET grant ==
                /\ (m.term > currentTerm[s] \/
                    (m.term = currentTerm[s] /\ votedFor[s] \in {Nil, m.candidateId}))
                /\ LogUpToDate(m.lastLogTerm, m.lastLogIndex,
                              LastLogTerm(s), LastLogIndex(s))
           IN
           /\ currentTerm' = [currentTerm EXCEPT ![s] = Max({@, m.term})]
           /\ votedFor' = [votedFor EXCEPT ![s] = IF grant THEN m.candidateId ELSE @]
           /\ state' = [state EXCEPT ![s] =
                        IF m.term > currentTerm[s] THEN "Follower" ELSE @]
           /\ messages' = (messages \ {m}) \cup {
                [type |-> "RequestVoteResponse",
                 term |-> Max({currentTerm[s], m.term}),
                 voteGranted |-> grant,
                 from |-> s,
                 to |-> m.candidateId]}
           /\ UNCHANGED <<logVars, leaderVars>>

\* Candidate s receives enough votes to become leader
BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ LET voteResponses == {m \in messages :
            /\ m.type = "RequestVoteResponse"
            /\ m.to = s
            /\ m.term = currentTerm[s]
            /\ m.voteGranted}
           votesReceived == {m.from : m \in voteResponses} \cup {s}
       IN /\ votesReceived \in Quorum
          /\ state' = [state EXCEPT ![s] = "Leader"]
          /\ nextIndex' = [nextIndex EXCEPT ![s] =
                            [t \in Servers |-> Len(log[s]) + 1]]
          /\ matchIndex' = [matchIndex EXCEPT ![s] =
                            [t \in Servers |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, logVars, messages>>

\* Leader s appends a new entry to its log (client request)
ClientRequest(s, v) ==
    /\ state[s] = "Leader"
    /\ Len(log[s]) < MaxLogLength
    /\ v \in Values
    /\ log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s], value |-> v])]
    /\ UNCHANGED <<serverVars, leaderVars, commitIndex, messages>>

\* Leader s sends AppendEntries to follower t
SendAppendEntries(s, t) ==
    /\ state[s] = "Leader"
    /\ s # t
    /\ LET prevLogIndex == nextIndex[s][t] - 1
           prevLogTerm == IF prevLogIndex = 0 THEN 0 ELSE log[s][prevLogIndex].term
           entries == IF nextIndex[s][t] > Len(log[s])
                      THEN <<>>
                      ELSE SubSeq(log[s], nextIndex[s][t], Len(log[s]))
       IN messages' = messages \cup {
            [type |-> "AppendEntries",
             term |-> currentTerm[s],
             leaderId |-> s,
             prevLogIndex |-> prevLogIndex,
             prevLogTerm |-> prevLogTerm,
             entries |-> entries,
             leaderCommit |-> commitIndex[s],
             to |-> t]}
    /\ UNCHANGED <<serverVars, logVars, leaderVars>>

\* Follower s handles AppendEntries from leader
HandleAppendEntries(s) ==
    \E m \in messages :
        /\ m.type = "AppendEntries"
        /\ m.to = s
        /\ LET success ==
                /\ m.term >= currentTerm[s]
                /\ (m.prevLogIndex = 0 \/
                    (m.prevLogIndex <= Len(log[s]) /\
                     log[s][m.prevLogIndex].term = m.prevLogTerm))
               newLog ==
                IF ~success THEN log[s]
                ELSE SubSeq(log[s], 1, m.prevLogIndex) \o m.entries
               newCommitIndex ==
                IF success /\ m.leaderCommit > commitIndex[s]
                THEN Min({m.leaderCommit, Len(newLog)})
                ELSE commitIndex[s]
           IN
           /\ currentTerm' = [currentTerm EXCEPT ![s] = Max({@, m.term})]
           /\ state' = [state EXCEPT ![s] =
                        IF m.term >= currentTerm[s] THEN "Follower" ELSE @]
           /\ votedFor' = [votedFor EXCEPT ![s] =
                          IF m.term > currentTerm[s] THEN Nil ELSE @]
           /\ log' = [log EXCEPT ![s] = newLog]
           /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIndex]
           /\ messages' = (messages \ {m}) \cup {
                [type |-> "AppendEntriesResponse",
                 term |-> Max({currentTerm[s], m.term}),
                 success |-> success,
                 matchIndex |-> IF success THEN m.prevLogIndex + Len(m.entries) ELSE 0,
                 from |-> s,
                 to |-> m.leaderId]}
           /\ UNCHANGED leaderVars

\* Leader s handles AppendEntriesResponse
HandleAppendEntriesResponse(s) ==
    /\ state[s] = "Leader"
    /\ \E m \in messages :
        /\ m.type = "AppendEntriesResponse"
        /\ m.to = s
        /\ m.term = currentTerm[s]
        /\ IF m.success
           THEN /\ nextIndex' = [nextIndex EXCEPT ![s][m.from] =
                                    Max({@, m.matchIndex + 1})]
                /\ matchIndex' = [matchIndex EXCEPT ![s][m.from] =
                                    Max({@, m.matchIndex})]
           ELSE /\ nextIndex' = [nextIndex EXCEPT ![s][m.from] =
                                    Max({1, @ - 1})]
                /\ UNCHANGED matchIndex
        /\ messages' = messages \ {m}
        /\ UNCHANGED <<serverVars, logVars>>

\* Leader advances commit index
AdvanceCommitIndex(s) ==
    /\ state[s] = "Leader"
    /\ LET newCommitIndex ==
            CHOOSE n \in (commitIndex[s]+1)..Len(log[s]) :
                /\ log[s][n].term = currentTerm[s]
                /\ {t \in Servers : matchIndex[s][t] >= n} \cup {s} \in Quorum
       IN /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIndex]
    /\ UNCHANGED <<serverVars, log, leaderVars, messages>>

\* Server discovers higher term and steps down
StepDown(s) ==
    \E m \in messages :
        /\ m.term > currentTerm[s]
        /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
        /\ state' = [state EXCEPT ![s] = "Follower"]
        /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
        /\ UNCHANGED <<logVars, leaderVars, messages>>

\* Drop a message (network failure)
DropMessage(m) ==
    /\ m \in messages
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<serverVars, logVars, leaderVars>>

\* Duplicate a message (network behavior)
DuplicateMessage(m) ==
    /\ m \in messages
    /\ UNCHANGED vars  \* Message already in set

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E s \in Servers : StartElection(s)
    \/ \E s \in Servers : HandleRequestVote(s)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E s \in Servers, v \in Values : ClientRequest(s, v)
    \/ \E s, t \in Servers : SendAppendEntries(s, t)
    \/ \E s \in Servers : HandleAppendEntries(s)
    \/ \E s \in Servers : HandleAppendEntriesResponse(s)
    \/ \E s \in Servers : AdvanceCommitIndex(s)
    \/ \E s \in Servers : StepDown(s)
    \/ \E m \in messages : DropMessage(m)

(***************************************************************************)
(* FAIRNESS CONDITIONS                                                     *)
(***************************************************************************)

\* Weak fairness on message handling
FairMessageHandling ==
    /\ \A s \in Servers : WF_vars(HandleRequestVote(s))
    /\ \A s \in Servers : WF_vars(HandleAppendEntries(s))
    /\ \A s \in Servers : WF_vars(HandleAppendEntriesResponse(s))

\* Weak fairness on leader actions
FairLeaderActions ==
    /\ \A s \in Servers : WF_vars(BecomeLeader(s))
    /\ \A s \in Servers : WF_vars(AdvanceCommitIndex(s))
    /\ \A s, t \in Servers : WF_vars(SendAppendEntries(s, t))

\* Strong fairness on elections (to handle repeated failures)
FairElections ==
    \A s \in Servers : SF_vars(StartElection(s))

(***************************************************************************)
(* LIVENESS PROPERTIES                                                     *)
(***************************************************************************)

\* Eventually a leader is elected (under fairness)
EventuallyLeader ==
    <>(\E s \in Servers : state[s] = "Leader")

\* If a client request is made, it eventually gets committed (under fairness)
\* This requires no permanent network partitions
RequestsEventuallyCommitted ==
    \A s \in Servers, v \in Values :
        (state[s] = "Leader" /\ Len(log[s]) < MaxLogLength) ~>
        (\E t \in Servers : \E i \in 1..Len(log[t]) :
            log[t][i].value = v /\ commitIndex[t] >= i)

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
    /\ FairMessageHandling
    /\ FairLeaderActions

\* Liveness specification
LivenessSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ FairMessageHandling
    /\ FairLeaderActions
    /\ FairElections

(***************************************************************************)
(* THEOREMS                                                                *)
(***************************************************************************)

\* Type correctness
THEOREM TypeCorrectness == Spec => []TypeInvariant

\* Election safety is preserved
THEOREM ElectionSafetyTheorem == Spec => []ElectionSafety

\* Log matching is preserved
THEOREM LogMatchingTheorem == Spec => []LogMatching

\* State machine safety
THEOREM StateMachineSafetyTheorem == Spec => []StateMachineSafety

\* Combined safety
THEOREM SafetyTheorem == Spec => []Safety

=============================================================================
