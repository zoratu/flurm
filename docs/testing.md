# FLURM Testing Guide

This document describes FLURM's comprehensive testing strategy, including unit tests, property-based testing, formal verification, and chaos engineering.

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Testing Pyramid](#testing-pyramid)
3. [Unit Testing](#unit-testing)
4. [Property-Based Testing](#property-based-testing)
5. [TLA+ Specifications](#tla-specifications)
6. [Deterministic Simulation](#deterministic-simulation)
7. [Fuzz Testing](#fuzz-testing)
8. [Integration Testing](#integration-testing)
9. [Chaos Engineering](#chaos-engineering)
10. [Running Tests](#running-tests)

## Testing Philosophy

FLURM follows these testing principles:

1. **Correctness First**: Use formal methods where possible
2. **Property Over Example**: Prefer property-based tests over example-based
3. **Deterministic Simulation**: Test distributed behavior in a controlled environment
4. **Chaos in Production-Like Environments**: Validate fault tolerance under real conditions

## Testing Pyramid

```
                    /\
                   /  \
                  /    \
                 / E2E  \           Few, slow, high confidence
                /--------\
               /          \
              / Integration \       Some, medium speed
             /--------------\
            /                \
           / Property-Based   \     Many, fast, wide coverage
          /--------------------\
         /                      \
        /     Unit Tests         \  Many, very fast
       /--------------------------\
```

### Test Distribution Goals

| Test Type | Count | Execution Time | Coverage Target |
|-----------|-------|----------------|-----------------|
| Unit | 500+ | < 1 minute | 80%+ line coverage |
| Property | 100+ | < 5 minutes | Edge cases, invariants |
| Integration | 50+ | < 10 minutes | Component interactions |
| E2E | 20+ | < 30 minutes | Critical user flows |
| Simulation | 10+ | < 1 hour | Distributed scenarios |

## Unit Testing

### Framework

FLURM uses EUnit for unit testing:

```erlang
-module(flurm_protocol_tests).
-include_lib("eunit/include/eunit.hrl").

%% Test fixtures
-define(VALID_HEADER, <<
    16#17, 16#11,   % Version
    16#00, 16#00,   % Flags
    16#03, 16#E9,   % MsgType (1001)
    16#00, 16#00,   % Reserved
    16#00, 16#00, 16#00, 16#10,  % BodyLen (16)
    16#00, 16#00, 16#00, 16#00,  % ForwardCount
    16#00, 16#00, 16#00, 16#00,  % ReturnCode
    16#00, 16#00, 16#00, 16#01   % MsgId (1)
>>).

%% Header decoding tests
decode_header_test_() ->
    [
        {"Valid header decodes correctly",
         fun() ->
             {ok, Header} = flurm_protocol:decode_header(?VALID_HEADER),
             ?assertEqual(16#1711, Header#msg_header.version),
             ?assertEqual(1001, Header#msg_header.msg_type),
             ?assertEqual(16, Header#msg_header.body_length),
             ?assertEqual(1, Header#msg_header.msg_id)
         end},

        {"Incomplete header returns error",
         fun() ->
             ?assertEqual({error, incomplete_header},
                          flurm_protocol:decode_header(<<1, 2, 3>>))
         end},

        {"Empty binary returns error",
         fun() ->
             ?assertEqual({error, incomplete_header},
                          flurm_protocol:decode_header(<<>>))
         end}
    ].

%% String encoding/decoding round-trip
string_roundtrip_test_() ->
    [
        {"ASCII string",
         fun() ->
             Original = <<"hello">>,
             Encoded = flurm_protocol:encode_string(Original),
             {Decoded, <<>>} = flurm_protocol:decode_string(Encoded),
             ?assertEqual(Original, Decoded)
         end},

        {"UTF-8 string",
         fun() ->
             Original = <<"hello ", 228, 184, 150, 231, 149, 140>>,  % hello 世界
             Encoded = flurm_protocol:encode_string(Original),
             {Decoded, <<>>} = flurm_protocol:decode_string(Encoded),
             ?assertEqual(Original, Decoded)
         end},

        {"Empty string",
         fun() ->
             Encoded = flurm_protocol:encode_string(<<>>),
             {Decoded, <<>>} = flurm_protocol:decode_string(Encoded),
             ?assertEqual(<<>>, Decoded)
         end},

        {"Undefined encodes as NO_VAL",
         fun() ->
             Encoded = flurm_protocol:encode_string(undefined),
             ?assertEqual(<<16#FF, 16#FF, 16#FF, 16#FF>>, Encoded),
             {Decoded, <<>>} = flurm_protocol:decode_string(Encoded),
             ?assertEqual(undefined, Decoded)
         end}
    ].
```

### Running Unit Tests

```bash
# Run all unit tests
rebar3 eunit

# Run specific test module
rebar3 eunit --module=flurm_protocol_tests

# Run with coverage
rebar3 eunit --cover
rebar3 cover --verbose
```

## Property-Based Testing

### PropEr Setup

FLURM uses PropEr for property-based testing:

```erlang
-module(flurm_protocol_props).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Generators
job_name() ->
    ?LET(Name, non_empty(binary()),
         binary_to_list(Name)).

partition_name() ->
    oneof([<<"batch">>, <<"debug">>, <<"gpu">>, <<"high_mem">>]).

positive_int() ->
    pos_integer().

job_spec() ->
    #{
        name => job_name(),
        partition => partition_name(),
        num_tasks => positive_int(),
        cpus_per_task => range(1, 64),
        time_limit => range(1, 14400),  % 1 minute to 10 days
        memory => range(1, 1024000)     % 1MB to 1TB
    }.

%% Properties
prop_header_roundtrip() ->
    ?FORALL({Version, Flags, MsgType, BodyLen, MsgId},
            {range(16#1600, 16#1711),
             range(0, 16#FFFF),
             range(0, 16#FFFF),
             range(0, 16#FFFFFFFF),
             range(0, 16#FFFFFFFF)},
            begin
                Header = #msg_header{
                    version = Version,
                    flags = Flags,
                    msg_type = MsgType,
                    body_length = BodyLen,
                    forward_count = 0,
                    return_code = 0,
                    msg_id = MsgId
                },
                Encoded = flurm_protocol:encode_header(Header),
                {ok, Decoded} = flurm_protocol:decode_header(Encoded),
                Header =:= Decoded
            end).

prop_string_roundtrip() ->
    ?FORALL(String, binary(),
            begin
                Encoded = flurm_protocol:encode_string(String),
                {Decoded, <<>>} = flurm_protocol:decode_string(Encoded),
                String =:= Decoded
            end).

prop_job_queue_ordering() ->
    ?FORALL(Jobs, list(job_spec()),
            begin
                Queue = flurm_queue:new(),
                Queue2 = lists:foldl(
                    fun(Job, Q) -> flurm_queue:enqueue(Job, Q) end,
                    Queue,
                    Jobs
                ),
                %% Property: jobs dequeue in priority order
                {Dequeued, _} = flurm_queue:dequeue_all(Queue2),
                is_sorted_by_priority(Dequeued)
            end).

prop_scheduler_fairness() ->
    ?FORALL({Users, Jobs},
            {list(binary()), list(job_spec())},
            begin
                %% Assign random users to jobs
                AssignedJobs = assign_users(Users, Jobs),
                %% Run scheduler
                Scheduled = flurm_scheduler:schedule(AssignedJobs),
                %% Property: fair share is maintained within tolerance
                check_fairness(Users, Scheduled, 0.1)
            end).

%% EUnit wrapper for PropEr
prop_test_() ->
    {timeout, 60, [
        ?_assert(proper:quickcheck(prop_header_roundtrip(),
                                   [{numtests, 1000}])),
        ?_assert(proper:quickcheck(prop_string_roundtrip(),
                                   [{numtests, 1000}])),
        ?_assert(proper:quickcheck(prop_job_queue_ordering(),
                                   [{numtests, 500}])),
        ?_assert(proper:quickcheck(prop_scheduler_fairness(),
                                   [{numtests, 200}]))
    ]}.
```

### Running Property Tests

```bash
# Run PropEr tests
rebar3 proper

# With more iterations
rebar3 proper -n 10000

# With seed for reproducibility
rebar3 proper --seed={1234, 5678, 9012}
```

## TLA+ Specifications

### Overview

FLURM uses TLA+ to formally verify critical distributed algorithms:

```
specs/
├── Consensus.tla       # Raft consensus specification
├── JobScheduler.tla    # Scheduler correctness
├── NodeManager.tla     # Node state machine
└── MessageOrdering.tla # Protocol message ordering
```

### Consensus Specification

```tla
--------------------------- MODULE Consensus ---------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Nodes, Values, MaxTerm

VARIABLES
    currentTerm,    \* Current term for each node
    votedFor,       \* Who each node voted for
    log,            \* Log entries for each node
    state,          \* State: follower, candidate, leader
    commitIndex,    \* Commit index for each node
    messages        \* In-flight messages

TypeOK ==
    /\ currentTerm \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> Nodes \cup {Nil}]
    /\ state \in [Nodes -> {"follower", "candidate", "leader"}]
    /\ commitIndex \in [Nodes -> Nat]

\* At most one leader per term
ElectionSafety ==
    \A t \in 0..MaxTerm :
        Cardinality({n \in Nodes :
            state[n] = "leader" /\ currentTerm[n] = t}) <= 1

\* Leader contains all committed entries
LeaderCompleteness ==
    \A n \in Nodes :
        state[n] = "leader" =>
            \A i \in 1..commitIndex[n] :
                \E entry \in log[n] : entry.index = i

\* Committed entries are never lost
LogMatching ==
    \A i, j \in Nodes :
        \A k \in 1..Min(Len(log[i]), Len(log[j])) :
            log[i][k].term = log[j][k].term =>
                log[i][k].value = log[j][k].value

\* Invariants to check
Invariants == TypeOK /\ ElectionSafety /\ LeaderCompleteness /\ LogMatching

\* Initial state
Init ==
    /\ currentTerm = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> Nil]
    /\ log = [n \in Nodes |-> <<>>]
    /\ state = [n \in Nodes |-> "follower"]
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ messages = {}

\* State transitions (abbreviated)
Next ==
    \/ \E n \in Nodes : Timeout(n)
    \/ \E n \in Nodes : RequestVote(n)
    \/ \E n, m \in Nodes : HandleRequestVote(n, m)
    \/ \E n \in Nodes : BecomeLeader(n)
    \/ \E n \in Nodes, v \in Values : ClientRequest(n, v)
    \/ \E n, m \in Nodes : AppendEntries(n, m)
    \/ \E n, m \in Nodes : HandleAppendEntries(n, m)

Spec == Init /\ [][Next]_<<currentTerm, votedFor, log, state, commitIndex, messages>>
==========================================================================
```

### Job Scheduler Specification

```tla
--------------------------- MODULE JobScheduler ---------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Jobs, Nodes, MaxResources

VARIABLES
    jobQueue,       \* Queue of pending jobs
    runningJobs,    \* Currently running jobs
    nodeResources,  \* Available resources per node
    allocations     \* Job -> Node allocations

\* A job never runs on a node without sufficient resources
ResourceSafety ==
    \A j \in runningJobs :
        \A n \in allocations[j] :
            nodeResources[n] >= jobs[j].resources

\* No two jobs use the same resources simultaneously
NoDoubleAllocation ==
    \A n \in Nodes :
        LET jobsOnNode == {j \in runningJobs : n \in allocations[j]}
        IN SumResources(jobsOnNode) <= MaxResources[n]

\* Every job eventually completes (under fair scheduling)
Liveness ==
    \A j \in Jobs :
        j \in jobQueue ~> j \in completedJobs

\* Fair share: no user is starved
FairShare ==
    \A u1, u2 \in Users :
        LET share1 == ResourceShare(u1)
            share2 == ResourceShare(u2)
        IN share1 > 0 /\ share2 > 0 =>
            Abs(share1 - share2) < FairnessThreshold
==========================================================================
```

### Running TLA+ Model Checker

```bash
# Install TLA+ tools
brew install tla-plus-toolbox

# Check model with TLC
cd specs
tlc Consensus.tla -config Consensus.cfg -workers 4

# Check all specifications
for spec in *.tla; do
    echo "Checking $spec..."
    tlc "$spec" -config "${spec%.tla}.cfg"
done
```

## Deterministic Simulation

### Simulation Framework

FLURM includes a deterministic simulation framework for testing distributed scenarios:

```erlang
-module(flurm_sim).
-export([run/2, scenario/1]).

-record(sim_state, {
    clock = 0,
    nodes = #{},
    network = #{},
    events = [],
    rng_state
}).

%% Run simulation with seed for reproducibility
run(Scenario, Seed) ->
    RngState = rand:seed(exsss, Seed),
    InitState = #sim_state{rng_state = RngState},
    Nodes = init_scenario(Scenario, InitState),
    run_loop(Nodes#sim_state{nodes = Nodes}).

run_loop(#sim_state{events = []} = State) ->
    {ok, State};
run_loop(#sim_state{events = [{Time, Event} | Rest]} = State) ->
    NewState = State#sim_state{
        clock = Time,
        events = Rest
    },
    case handle_event(Event, NewState) of
        {ok, State2} ->
            run_loop(State2);
        {error, Reason} ->
            {error, Reason, State}
    end.

%% Inject network partition
inject_partition(Nodes1, Nodes2, Duration, State) ->
    schedule_event(
        State#sim_state.clock + Duration,
        {heal_partition, Nodes1, Nodes2},
        add_partition(Nodes1, Nodes2, State)
    ).

%% Inject node failure
inject_node_failure(Node, Duration, State) ->
    schedule_event(
        State#sim_state.clock + Duration,
        {recover_node, Node},
        kill_node(Node, State)
    ).
```

### Simulation Scenarios

```erlang
-module(flurm_sim_scenarios).

%% Scenario: Leader election under partition
leader_election_partition() ->
    #{
        nodes => [ctrl1, ctrl2, ctrl3],
        initial_leader => ctrl1,
        events => [
            {1000, {partition, [ctrl1], [ctrl2, ctrl3]}},
            {5000, {heal_partition}},
            {6000, {verify, fun(State) ->
                %% Verify: exactly one leader
                Leaders = [N || N <- maps:keys(State#sim_state.nodes),
                               is_leader(N, State)],
                length(Leaders) =:= 1
            end}}
        ]
    }.

%% Scenario: Job scheduling under controller failover
job_scheduling_failover() ->
    #{
        nodes => [ctrl1, ctrl2, ctrl3, node1, node2, node3],
        controllers => [ctrl1, ctrl2, ctrl3],
        compute_nodes => [node1, node2, node3],
        events => [
            {100, {submit_job, job1, #{cpus => 4, time => 3600}}},
            {200, {submit_job, job2, #{cpus => 2, time => 1800}}},
            {500, {kill_leader}},
            {2000, {verify, fun(State) ->
                %% Verify: jobs still running after failover
                Job1State = get_job_state(job1, State),
                Job2State = get_job_state(job2, State),
                Job1State =:= running andalso Job2State =:= running
            end}}
        ]
    }.

%% Scenario: Split-brain resolution
split_brain_resolution() ->
    #{
        nodes => [ctrl1, ctrl2, ctrl3, ctrl4, ctrl5],
        events => [
            {100, {partition, [ctrl1, ctrl2], [ctrl3, ctrl4, ctrl5]}},
            {200, {submit_job, job1, ctrl1}},  % Submit to minority
            {300, {submit_job, job2, ctrl3}},  % Submit to majority
            {5000, {heal_partition}},
            {6000, {verify, fun(State) ->
                %% Verify: job1 rejected, job2 accepted
                get_job_state(job1, State) =:= rejected andalso
                get_job_state(job2, State) =/= rejected
            end}}
        ]
    }.
```

### Running Simulations

```bash
# Run all simulation scenarios
rebar3 ct --suite=flurm_sim_SUITE

# Run specific scenario with seed
rebar3 ct --suite=flurm_sim_SUITE --case=leader_election_partition --seed=12345

# Run with multiple seeds for broader coverage
for seed in $(seq 1 100); do
    rebar3 ct --suite=flurm_sim_SUITE --seed=$seed
done
```

## Fuzz Testing

### Protocol Fuzzing

```erlang
-module(flurm_fuzz).
-export([fuzz_protocol/1, fuzz_message/1]).

%% Fuzz protocol decoder with random bytes
fuzz_protocol(Iterations) ->
    lists:foreach(
        fun(_) ->
            Size = rand:uniform(1000),
            Data = crypto:strong_rand_bytes(Size),
            %% Should not crash, may return error
            try
                case flurm_protocol:decode_message(Data) of
                    {ok, _} -> ok;
                    {error, _} -> ok
                end
            catch
                Class:Reason:Stack ->
                    io:format("CRASH: ~p:~p~n~p~n", [Class, Reason, Stack]),
                    io:format("Input: ~p~n", [Data]),
                    throw({fuzz_failure, Data})
            end
        end,
        lists:seq(1, Iterations)
    ).

%% Mutation-based fuzzing
fuzz_message(ValidMessage) ->
    Mutations = [
        fun flip_bit/1,
        fun insert_byte/1,
        fun delete_byte/1,
        fun swap_bytes/1,
        fun truncate/1,
        fun duplicate_section/1
    ],
    lists:foreach(
        fun(_) ->
            Mutation = lists:nth(rand:uniform(length(Mutations)), Mutations),
            Mutated = Mutation(ValidMessage),
            safe_decode(Mutated)
        end,
        lists:seq(1, 10000)
    ).

flip_bit(Data) ->
    Pos = rand:uniform(byte_size(Data)) - 1,
    BitPos = rand:uniform(8) - 1,
    <<Before:Pos/binary, Byte:8, After/binary>> = Data,
    NewByte = Byte bxor (1 bsl BitPos),
    <<Before/binary, NewByte:8, After/binary>>.

insert_byte(Data) ->
    Pos = rand:uniform(byte_size(Data) + 1) - 1,
    NewByte = rand:uniform(256) - 1,
    <<Before:Pos/binary, After/binary>> = Data,
    <<Before/binary, NewByte:8, After/binary>>.

delete_byte(Data) when byte_size(Data) > 0 ->
    Pos = rand:uniform(byte_size(Data)) - 1,
    <<Before:Pos/binary, _:8, After/binary>> = Data,
    <<Before/binary, After/binary>>;
delete_byte(Data) ->
    Data.
```

### Running Fuzz Tests

```bash
# Run protocol fuzzer
rebar3 shell
> flurm_fuzz:fuzz_protocol(100000).

# Run with AFL (American Fuzzy Lop)
afl-fuzz -i test/fuzz_corpus -o test/fuzz_output -- \
    ./_build/default/lib/flurm/ebin/flurm_fuzz_target

# Run with libFuzzer integration
rebar3 as fuzz compile
./fuzz_target -max_len=10000 -jobs=4
```

## Integration Testing

### Common Test Suites

```erlang
-module(flurm_integration_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([job_submit_test/1, node_registration_test/1, scheduler_test/1]).

all() ->
    [job_submit_test, node_registration_test, scheduler_test].

init_per_suite(Config) ->
    %% Start a test cluster
    {ok, _} = application:ensure_all_started(flurm),
    %% Register test nodes
    ok = flurm_test_utils:register_nodes([node1, node2, node3]),
    Config.

end_per_suite(_Config) ->
    application:stop(flurm),
    ok.

job_submit_test(_Config) ->
    %% Submit a job
    {ok, JobId} = flurm_client:submit_job(#{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho hello">>,
        partition => <<"batch">>,
        num_tasks => 1
    }),

    %% Wait for job to start
    ok = flurm_test_utils:wait_for_job_state(JobId, running, 5000),

    %% Verify job is running on a node
    {ok, JobInfo} = flurm_client:get_job_info(JobId),
    ?assert(JobInfo#job_info.node_list =/= []),

    %% Wait for completion
    ok = flurm_test_utils:wait_for_job_state(JobId, completed, 30000),

    %% Verify exit code
    {ok, FinalInfo} = flurm_client:get_job_info(JobId),
    ?assertEqual(0, FinalInfo#job_info.exit_code).

node_registration_test(_Config) ->
    %% Add a new node
    ok = flurm_node_manager:register_node(#{
        name => node4,
        cpus => 16,
        memory => 64000,
        partitions => [<<"batch">>]
    }),

    %% Verify node is visible
    {ok, Nodes} = flurm_client:get_node_info(),
    ?assert(lists:any(fun(N) -> N#node_info.name =:= node4 end, Nodes)),

    %% Simulate heartbeat timeout
    flurm_test_utils:advance_time(60000),

    %% Verify node marked as down
    {ok, Nodes2} = flurm_client:get_node_info(),
    Node4 = lists:keyfind(node4, #node_info.name, Nodes2),
    ?assertEqual(down, Node4#node_info.state).
```

### Running Integration Tests

```bash
# Run all integration tests
rebar3 ct

# Run specific suite
rebar3 ct --suite=flurm_integration_SUITE

# With verbose output
rebar3 ct --verbose
```

## Chaos Engineering

### Chaos Monkey

FLURM includes a chaos monkey for production-like testing:

```erlang
-module(flurm_chaos).
-behaviour(gen_server).

-export([start_link/1, enable/0, disable/0]).

-record(state, {
    enabled = false,
    failure_rate = 0.01,
    scenarios = []
}).

%% Chaos scenarios
scenarios() ->
    [
        {kill_process, 0.3},        % Kill random process
        {network_delay, 0.2},       % Add network latency
        {network_partition, 0.1},   % Create partition
        {disk_full, 0.1},           % Simulate disk full
        {memory_pressure, 0.1},     % Simulate memory pressure
        {cpu_burn, 0.1},            % CPU saturation
        {clock_skew, 0.1}           % Time drift
    ].

trigger_chaos(#state{enabled = true, failure_rate = Rate}) ->
    case rand:uniform() < Rate of
        true ->
            Scenario = weighted_choice(scenarios()),
            execute_scenario(Scenario);
        false ->
            ok
    end;
trigger_chaos(_) ->
    ok.

execute_scenario(kill_process) ->
    Processes = [
        flurm_job_manager,
        flurm_node_manager,
        flurm_scheduler_engine
    ],
    Victim = lists:nth(rand:uniform(length(Processes)), Processes),
    io:format("[CHAOS] Killing ~p~n", [Victim]),
    exit(whereis(Victim), kill);

execute_scenario(network_delay) ->
    Delay = rand:uniform(500),
    io:format("[CHAOS] Injecting ~pms network delay~n", [Delay]),
    flurm_network:inject_delay(Delay);

execute_scenario(network_partition) ->
    Nodes = nodes(),
    case Nodes of
        [] -> ok;
        _ ->
            Victim = lists:nth(rand:uniform(length(Nodes)), Nodes),
            Duration = rand:uniform(10000),
            io:format("[CHAOS] Partitioning ~p for ~pms~n", [Victim, Duration]),
            flurm_network:partition([node()], [Victim], Duration)
    end.
```

### Chaos Testing Configuration

```yaml
# chaos_config.yaml
chaos:
  enabled: true
  failure_rate: 0.01  # 1% chance per tick
  tick_interval: 1000  # Check every second

  scenarios:
    kill_process:
      weight: 30
      targets:
        - flurm_job_manager
        - flurm_node_manager
        - flurm_scheduler_engine

    network_partition:
      weight: 10
      min_duration: 1000
      max_duration: 30000

    network_delay:
      weight: 20
      min_delay: 10
      max_delay: 500

    process_crash:
      weight: 20
      restart_delay: 100

    memory_pressure:
      weight: 10
      target_usage: 0.9

    disk_slow:
      weight: 10
      latency: 100
```

### Running Chaos Tests

```bash
# Start FLURM with chaos enabled
FLURM_CHAOS_ENABLED=true rebar3 shell

# Run chaos for specific duration
flurm_chaos:run_for(3600000).  % 1 hour

# Run chaos suite
rebar3 ct --suite=flurm_chaos_SUITE --config=test/chaos_config.yaml
```

## Running Tests

### Full Test Suite

```bash
# Run everything
make test

# Or individually:
rebar3 eunit          # Unit tests
rebar3 proper         # Property tests
rebar3 ct             # Integration tests
rebar3 dialyzer       # Type checking
```

### CI Pipeline Configuration

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Setup Erlang
        uses: erlef/setup-beam@v1
        with:
          otp-version: '26.0'
          rebar3-version: '3.22'

      - name: Compile
        run: rebar3 compile

      - name: Unit Tests
        run: rebar3 eunit --cover

      - name: Property Tests
        run: rebar3 proper -n 1000

      - name: Integration Tests
        run: rebar3 ct

      - name: Dialyzer
        run: rebar3 dialyzer

      - name: Check TLA+ Specs
        run: |
          cd specs
          for spec in *.tla; do
            tlc "$spec" -config "${spec%.tla}.cfg"
          done

      - name: Coverage Report
        run: rebar3 cover --verbose
```

### Test Coverage Requirements

| Component | Minimum Coverage |
|-----------|-----------------|
| Protocol | 90% |
| Scheduler | 85% |
| Job Manager | 85% |
| Node Manager | 80% |
| Consensus | 80% |
| Overall | 80% |

---

See also:
- [Development Guide](development.md) for coding standards
- [Architecture](architecture.md) for system design
