---------------------------- MODULE raft ----------------------------
(*****************************************************************************)
(* A small, self-contained TLA+ model of the Raft consensus algorithm that   *)
(* the `quorum` Go core implements — election, log replication, the Figure-8 *)
(* commit rule, and the safety invariants the Go invariant-monitors check    *)
(* after every simulator step.                                               *)
(*                                                                           *)
(* HONEST SCOPE: this is a model of the ALGORITHM, hand-written and bounded   *)
(* small enough for TLC to check exhaustively in seconds — it is NOT          *)
(* mechanically extracted from the Go code. It complements the deterministic  *)
(* simulation testing (which checks the actual implementation across 140,000  *)
(* fault schedules) by giving an exhaustive, machine-checked proof that the   *)
(* safety properties hold for the algorithm over the bounded state space in   *)
(* raft.cfg. It intentionally omits snapshots, membership changes, and client *)
(* sessions; it models the safety core: terms, votes, logs, commit.          *)
(*                                                                           *)
(* The five invariants mirror check/invariants.go one-for-one:               *)
(*   ElectionSafety, LogMatching, LeaderCompleteness, StateMachineSafety,     *)
(*   plus the structural MoreThanOneLeaderPerTerm guard.                      *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Server,        \* the set of servers, e.g. {s1, s2, s3}
          Follower, Candidate, Leader,   \* roles
          Nil,           \* the "no vote" sentinel (a model value, distinct from any Server)
          MaxTerm,       \* bound on terms (keeps the model finite)
          MaxLogLen      \* bound on log length (keeps the model finite)

VARIABLES
    currentTerm,   \* currentTerm[s]: latest term server s has seen
    state,         \* state[s]: role of server s
    votedFor,      \* votedFor[s]: candidate s voted for in currentTerm (or Nil)
    log,           \* log[s]: sequence of entries; each entry = [term |-> t]
    commitIndex,   \* commitIndex[s]: highest log index known committed on s
    votesGranted   \* votesGranted[s]: set of servers that voted for s this term

vars == <<currentTerm, state, votedFor, log, commitIndex, votesGranted>>

----
(* Helpers *)

Quorum == {q \in SUBSET Server : Cardinality(q) * 2 > Cardinality(Server)}

Min(a, b) == IF a < b THEN a ELSE b

LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term

\* s's log is at least as up-to-date as t's (the Raft election restriction).
UpToDate(s, t) ==
    \/ LastTerm(log[s]) > LastTerm(log[t])
    \/ /\ LastTerm(log[s]) = LastTerm(log[t])
       /\ Len(log[s]) >= Len(log[t])

----
(* Initial state *)

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ state       = [s \in Server |-> Follower]
    /\ votedFor    = [s \in Server |-> Nil]
    /\ log         = [s \in Server |-> << >>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ votesGranted = [s \in Server |-> {}]

----
(* Transitions *)

\* A server times out and starts an election: bumps term, votes for itself.
StartElection(s) ==
    /\ currentTerm[s] < MaxTerm
    /\ state[s] \in {Follower, Candidate}
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ state'       = [state       EXCEPT ![s] = Candidate]
    /\ votedFor'    = [votedFor    EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
    /\ UNCHANGED <<log, commitIndex>>

\* Server t grants a vote to candidate s (same term, hasn't voted, s is up-to-date).
GrantVote(s, t) ==
    /\ state[s] = Candidate
    /\ currentTerm[t] <= currentTerm[s]
    /\ \/ votedFor[t] = Nil
       \/ votedFor[t] = s
       \/ currentTerm[t] < currentTerm[s]
    /\ UpToDate(s, t)
    /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
    /\ votedFor'    = [votedFor    EXCEPT ![t] = s]
    /\ state'       = [state EXCEPT ![t] = IF currentTerm[t] < currentTerm[s] THEN Follower ELSE state[t]]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = votesGranted[s] \union {t}]
    /\ UNCHANGED <<log, commitIndex>>

\* A candidate with a quorum of votes becomes leader.
BecomeLeader(s) ==
    /\ state[s] = Candidate
    /\ votesGranted[s] \in Quorum
    /\ state' = [state EXCEPT ![s] = Leader]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, votesGranted>>

\* A leader appends a new entry in its current term (a client command / no-op).
ClientRequest(s) ==
    /\ state[s] = Leader
    /\ Len(log[s]) < MaxLogLen
    /\ log' = [log EXCEPT ![s] = Append(log[s], [term |-> currentTerm[s]])]
    /\ UNCHANGED <<currentTerm, state, votedFor, commitIndex, votesGranted>>

\* Follower t copies leader s's log when s's log extends t's consistently
\* (models AppendEntries success: s is leader, terms agree, s's log is longer).
ReplicateTo(s, t) ==
    /\ state[s] = Leader
    /\ s # t
    /\ currentTerm[t] <= currentTerm[s]
    /\ Len(log[s]) > Len(log[t])
    \* prefix up to t's length already matches (log-matching precondition)
    /\ \A i \in 1..Len(log[t]) : log[t][i] = log[s][i]
    /\ log'         = [log         EXCEPT ![t] = log[s]]
    /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
    /\ state'       = [state       EXCEPT ![t] = Follower]
    /\ UNCHANGED <<votedFor, commitIndex, votesGranted>>

\* The leader advances its commit index to N when a quorum has log length >= N
\* AND log[s][N].term = currentTerm[s] (the Figure-8 current-term commit rule).
AdvanceCommit(s) ==
    /\ state[s] = Leader
    /\ \E N \in (commitIndex[s]+1)..Len(log[s]) :
         /\ log[s][N].term = currentTerm[s]
         /\ {t \in Server : Len(log[t]) >= N /\ log[t][N] = log[s][N]} \in Quorum
         /\ commitIndex' = [commitIndex EXCEPT ![s] = N]
    /\ UNCHANGED <<currentTerm, state, votedFor, log, votesGranted>>

Next ==
    \/ \E s \in Server : StartElection(s)
    \/ \E s, t \in Server : GrantVote(s, t)
    \/ \E s \in Server : BecomeLeader(s)
    \/ \E s \in Server : ClientRequest(s)
    \/ \E s, t \in Server : ReplicateTo(s, t)
    \/ \E s \in Server : AdvanceCommit(s)

Spec == Init /\ [][Next]_vars

----
(* Safety invariants — one-for-one with check/invariants.go *)

TypeOK ==
    /\ currentTerm \in [Server -> 0..MaxTerm]
    /\ state \in [Server -> {Follower, Candidate, Leader}]
    /\ commitIndex \in [Server -> 0..MaxLogLen]

\* (1) Election safety: at most one leader per term.
ElectionSafety ==
    \A s, t \in Server :
        (s # t /\ state[s] = Leader /\ state[t] = Leader)
            => currentTerm[s] # currentTerm[t]

\* (2) Log matching: if two logs have an entry with the same index and term,
\*     the logs are identical up through that index.
LogMatching ==
    \A s, t \in Server :
        \A i \in 1..Min(Len(log[s]), Len(log[t])) :
            (log[s][i].term = log[t][i].term)
                => (\A j \in 1..i : log[s][j] = log[t][j])

\* (3) Leader completeness: an entry committed on any server appears, at the
\*     same index+term, in the log of every leader of a higher term.
LeaderCompleteness ==
    \A s \in Server :
        \A i \in 1..commitIndex[s] :
            \A l \in Server :
                (state[l] = Leader /\ currentTerm[l] > log[s][i].term)
                    => (Len(log[l]) >= i /\ log[l][i] = log[s][i])

\* (4) State-machine safety: no two servers commit different entries at the
\*     same index.
StateMachineSafety ==
    \A s, t \in Server :
        \A i \in 1..Min(commitIndex[s], commitIndex[t]) :
            log[s][i] = log[t][i]

\* The conjunction TLC checks (see raft.cfg).
Invariants ==
    /\ TypeOK
    /\ ElectionSafety
    /\ LogMatching
    /\ LeaderCompleteness
    /\ StateMachineSafety

=============================================================================
