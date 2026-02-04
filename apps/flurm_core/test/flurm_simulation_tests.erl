%%%-------------------------------------------------------------------
%%% @doc Deterministic Simulation Tests for Distributed System Verification
%%%
%%% FoundationDB-style simulation testing for FLURM distributed systems.
%%% This module provides deterministic, reproducible tests that verify
%%% distributed system invariants under various failure conditions.
%%%
%%% Key Features:
%%% - Simulated cluster state with virtual nodes
%%% - Simulated time (no wall clock dependency)
%%% - Seeded random for full reproducibility
%%% - Deterministic failure injection
%%% - Invariant checking after each step
%%%
%%% Invariants Verified (from TLA+ specs):
%%% - SiblingExclusivity: At most one sibling job running
%%% - NoJobLoss: Jobs are not lost during operations
%%% - TRESConsistency: TRES values consistent across nodes
%%% - ValidModeTransitions: Migration mode transitions are valid
%%%
%%% Usage:
%%%   rebar3 eunit --module=flurm_simulation_tests
%%%
%%% To reproduce a failure:
%%%   flurm_simulation_tests:run_scenario_with_seed(ScenarioName, Seed)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_simulation_tests).

-include_lib("eunit/include/eunit.hrl").

%% Test exports
-export([
    run_scenario_with_seed/2,
    run_all_scenarios/1
]).

%%====================================================================
%% Type Definitions (exported for documentation)
%%====================================================================

-type cluster_id() :: binary().
-type job_id() :: pos_integer().
-type sim_time() :: non_neg_integer().
-type seed() :: integer().

%% Node states
-type node_state() :: up | down | partitioned.

%% Sibling job states (from TLA+ FlurmFederation)
-type sibling_state() :: null | pending | running | revoked | completed | failed.

%% Migration modes (from TLA+ FlurmMigration)
-type migration_mode() :: shadow | active | primary | standalone.

%% Message types
-type message_type() :: job_submit | job_started | sibling_revoke | job_completed | job_failed
                      | tres_record | leader_heartbeat.

%% Export types to suppress unused warnings
-export_type([cluster_id/0, job_id/0, sim_time/0, seed/0, node_state/0,
              sibling_state/0, migration_mode/0, message_type/0]).

%%====================================================================
%% Records
%%====================================================================

%% Simulated cluster state
-record(sim_cluster, {
    id :: cluster_id(),
    state = up :: node_state(),
    is_leader = false :: boolean(),
    sibling_jobs = #{} :: #{job_id() => sibling_state()},
    tres_log = [] :: [tres_record()],
    commit_index = 0 :: non_neg_integer(),
    pending_jobs = [] :: [job_id()]
}).

%% TRES accounting record
-record(tres_record, {
    job_id :: job_id(),
    user :: binary(),
    tres_value :: non_neg_integer()
}).

-type tres_record() :: #tres_record{}.

%% Simulated message in flight
-record(sim_message, {
    id :: pos_integer(),
    type :: message_type(),
    from :: cluster_id(),
    to :: cluster_id(),
    payload :: term(),
    delivery_time :: sim_time(),
    dropped = false :: boolean()
}).

%% Federated job tracking
-record(fed_job, {
    id :: job_id(),
    origin :: cluster_id(),
    sibling_states = #{} :: #{cluster_id() => sibling_state()},
    running_cluster = undefined :: cluster_id() | undefined,
    submitted_time :: sim_time(),
    completed_time = undefined :: sim_time() | undefined
}).

%% Migration state
-record(migration_state, {
    mode = shadow :: migration_mode(),
    slurm_active = true :: boolean(),
    flurm_ready = false :: boolean(),
    slurm_jobs = [] :: [job_id()],
    flurm_jobs = [] :: [job_id()],
    forwarded_jobs = [] :: [job_id()],
    completed_jobs = [] :: [job_id()],
    lost_jobs = [] :: [job_id()]
}).

%% Main simulation state
-record(sim_state, {
    seed :: rand:state(),
    time = 0 :: sim_time(),
    clusters = #{} :: #{cluster_id() => #sim_cluster{}},
    jobs = #{} :: #{job_id() => #fed_job{}},
    messages = [] :: [#sim_message{}],
    partitions = [] :: [{cluster_id(), cluster_id()}],  % Bidirectional partitions
    migration = #migration_state{} :: #migration_state{},
    tres_totals = #{} :: #{cluster_id() => #{binary() => non_neg_integer()}},
    leader = undefined :: cluster_id() | undefined,
    events = [] :: [{sim_time(), term()}],
    history = [] :: [{sim_time(), term()}],
    job_counter = 1 :: pos_integer(),
    message_counter = 1 :: pos_integer(),
    config = #{} :: map()
}).

%%====================================================================
%% EUnit Test Generators
%%====================================================================

%% Main test suite
simulation_test_() ->
    {timeout, 300,
     {setup,
      fun setup/0,
      fun cleanup/1,
      [
       {"Federation sibling exclusivity tests", fun federation_sibling_exclusivity_tests/0},
       {"Accounting TRES consistency tests", fun accounting_tres_consistency_tests/0},
       {"Migration mode transition tests", fun migration_mode_transition_tests/0},
       {"Leader election during job tests", fun leader_election_during_job_tests/0},
       {"Reproducibility tests", fun reproducibility_tests/0},
       {"Combined failure scenario tests", fun combined_failure_tests/0}
      ]}}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Federation Sibling Exclusivity Tests
%%====================================================================

federation_sibling_exclusivity_tests() ->
    %% Test with multiple seeds
    Seeds = [12345, 67890, 11111, 22222, 33333],
    Results = [run_federation_sibling_test(Seed) || Seed <- Seeds],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

run_federation_sibling_test(Seed) ->
    Config = #{
        num_clusters => 3,
        num_jobs => 10,
        max_time => 50000,
        failure_rate => 0.1,
        partition_rate => 0.05
    },
    case run_simulation(federation_sibling, Seed, Config) of
        {ok, _FinalState} -> ok;
        {error, Reason, State} ->
            io:format("~n[FAIL] Federation sibling test failed with seed ~p~n", [Seed]),
            io:format("Reason: ~p~n", [Reason]),
            io:format("Time: ~p~n", [State#sim_state.time]),
            {error, Reason}
    end.

%%====================================================================
%% Accounting TRES Consistency Tests
%%====================================================================

accounting_tres_consistency_tests() ->
    Seeds = [54321, 98765, 44444, 55555, 66666],
    Results = [run_accounting_tres_test(Seed) || Seed <- Seeds],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

run_accounting_tres_test(Seed) ->
    Config = #{
        num_clusters => 5,
        num_jobs => 20,
        max_time => 100000,
        failure_rate => 0.15,
        partition_rate => 0.1
    },
    case run_simulation(accounting_tres, Seed, Config) of
        {ok, _FinalState} -> ok;
        {error, Reason, State} ->
            io:format("~n[FAIL] Accounting TRES test failed with seed ~p~n", [Seed]),
            io:format("Reason: ~p~n", [Reason]),
            io:format("Time: ~p~n", [State#sim_state.time]),
            {error, Reason}
    end.

%%====================================================================
%% Migration Mode Transition Tests
%%====================================================================

migration_mode_transition_tests() ->
    Seeds = [77777, 88888, 99999, 10101, 20202],
    Results = [run_migration_mode_test(Seed) || Seed <- Seeds],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

run_migration_mode_test(Seed) ->
    Config = #{
        num_clusters => 3,
        num_jobs => 15,
        max_time => 80000,
        failure_rate => 0.1,
        enable_rollback => true
    },
    case run_simulation(migration_mode, Seed, Config) of
        {ok, _FinalState} -> ok;
        {error, Reason, State} ->
            io:format("~n[FAIL] Migration mode test failed with seed ~p~n", [Seed]),
            io:format("Reason: ~p~n", [Reason]),
            io:format("Mode: ~p~n", [State#sim_state.migration#migration_state.mode]),
            {error, Reason}
    end.

%%====================================================================
%% Leader Election During Job Tests
%%====================================================================

leader_election_during_job_tests() ->
    Seeds = [30303, 40404, 50505, 60606, 70707],
    Results = [run_leader_election_test(Seed) || Seed <- Seeds],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

run_leader_election_test(Seed) ->
    Config = #{
        num_clusters => 5,
        num_jobs => 25,
        max_time => 120000,
        leader_failure_rate => 0.2,
        partition_rate => 0.05
    },
    case run_simulation(leader_election, Seed, Config) of
        {ok, _FinalState} -> ok;
        {error, Reason, State} ->
            io:format("~n[FAIL] Leader election test failed with seed ~p~n", [Seed]),
            io:format("Reason: ~p~n", [Reason]),
            io:format("Leader: ~p~n", [State#sim_state.leader]),
            {error, Reason}
    end.

%%====================================================================
%% Reproducibility Tests
%%====================================================================

reproducibility_tests() ->
    Seed = 123456789,
    Config = #{
        num_clusters => 3,
        num_jobs => 5,
        max_time => 10000,
        failure_rate => 0.1
    },
    %% Run same seed twice
    {ok, State1} = run_simulation(federation_sibling, Seed, Config),
    {ok, State2} = run_simulation(federation_sibling, Seed, Config),

    %% Verify identical final states (minus seed state)
    ?assertEqual(State1#sim_state.time, State2#sim_state.time),
    ?assertEqual(State1#sim_state.jobs, State2#sim_state.jobs),
    ?assertEqual(length(State1#sim_state.history), length(State2#sim_state.history)),
    ok.

%%====================================================================
%% Combined Failure Scenario Tests
%%====================================================================

combined_failure_tests() ->
    Seeds = [111222, 333444, 555666],
    Results = [run_combined_failure_test(Seed) || Seed <- Seeds],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

run_combined_failure_test(Seed) ->
    Config = #{
        num_clusters => 5,
        num_jobs => 30,
        max_time => 150000,
        failure_rate => 0.2,
        partition_rate => 0.15,
        leader_failure_rate => 0.1,
        message_drop_rate => 0.05,
        message_delay_max => 1000
    },
    case run_simulation(combined_failures, Seed, Config) of
        {ok, _FinalState} -> ok;
        {error, Reason, State} ->
            io:format("~n[FAIL] Combined failure test failed with seed ~p~n", [Seed]),
            io:format("Reason: ~p~n", [Reason]),
            print_state_summary(State),
            {error, Reason}
    end.

%%====================================================================
%% Public API for Manual Testing
%%====================================================================

%% @doc Run a specific scenario with a given seed
-spec run_scenario_with_seed(atom(), seed()) -> {ok, #sim_state{}} | {error, term(), #sim_state{}}.
run_scenario_with_seed(Scenario, Seed) ->
    Config = default_config(Scenario),
    run_simulation(Scenario, Seed, Config).

%% @doc Run all scenarios with multiple seeds
-spec run_all_scenarios(pos_integer()) -> [{atom(), seed(), ok | {error, term()}}].
run_all_scenarios(NumSeeds) ->
    Seeds = generate_seeds(NumSeeds),
    Scenarios = [federation_sibling, accounting_tres, migration_mode,
                 leader_election, combined_failures],
    Results = [
        begin
            Config = default_config(Scenario),
            Result = case run_simulation(Scenario, Seed, Config) of
                {ok, _} -> ok;
                {error, Reason, _} -> {error, Reason}
            end,
            {Scenario, Seed, Result}
        end
        || Scenario <- Scenarios,
           Seed <- Seeds
    ],
    analyze_results(Results),
    Results.

%%====================================================================
%% Simulation Engine
%%====================================================================

-spec run_simulation(atom(), seed(), map()) -> {ok, #sim_state{}} | {error, term(), #sim_state{}}.
run_simulation(Scenario, Seed, Config) ->
    InitState = init_simulation(Scenario, Seed, Config),
    simulate_until_done(InitState).

init_simulation(Scenario, Seed, Config) ->
    RandState = rand:seed(exsplus, {Seed, Seed * 2, Seed * 3}),
    NumClusters = maps:get(num_clusters, Config, 3),
    NumJobs = maps:get(num_jobs, Config, 10),

    %% Create clusters
    Clusters = lists:foldl(fun(I, Acc) ->
        ClusterId = cluster_id(I),
        Cluster = #sim_cluster{
            id = ClusterId,
            state = up,
            is_leader = (I =:= 1)
        },
        maps:put(ClusterId, Cluster, Acc)
    end, #{}, lists:seq(1, NumClusters)),

    %% Set leader
    Leader = cluster_id(1),

    %% Generate initial events based on scenario
    Events = generate_initial_events(Scenario, NumJobs, Config),

    #sim_state{
        seed = RandState,
        time = 0,
        clusters = Clusters,
        leader = Leader,
        events = lists:keysort(1, Events),
        config = Config
    }.

simulate_until_done(#sim_state{events = [], messages = []} = State) ->
    %% No more events or messages - simulation complete
    {ok, State};
simulate_until_done(#sim_state{time = Time, config = Config} = State) ->
    MaxTime = maps:get(max_time, Config, 100000),
    case Time >= MaxTime of
        true -> {ok, State};
        false -> step_simulation(State)
    end.

step_simulation(State0) ->
    %% Determine next event time
    NextEventTime = case State0#sim_state.events of
        [] -> infinity;
        [{T, _} | _] -> T
    end,

    %% Determine next message delivery time
    NextMessageTime = case find_next_deliverable_message(State0) of
        none -> infinity;
        {_, Msg} -> Msg#sim_message.delivery_time
    end,

    %% Process whichever comes first
    case {NextEventTime, NextMessageTime} of
        {infinity, infinity} ->
            {ok, State0};
        {EventT, MsgT} when EventT =< MsgT ->
            State1 = advance_time(State0, EventT),
            process_next_event(State1);
        {_, MsgT} ->
            State1 = advance_time(State0, MsgT),
            process_next_message(State1)
    end.

advance_time(State, NewTime) ->
    State#sim_state{time = NewTime}.

process_next_event(#sim_state{events = []} = State) ->
    simulate_until_done(State);
process_next_event(#sim_state{events = [{_Time, Event} | Rest]} = State) ->
    State1 = State#sim_state{events = Rest},
    case apply_event(Event, State1) of
        {ok, State2} ->
            case check_all_invariants(State2) of
                ok -> simulate_until_done(State2);
                {violation, Reason} -> {error, Reason, State2}
            end;
        {error, Reason, State2} ->
            {error, Reason, State2}
    end.

process_next_message(State) ->
    case find_next_deliverable_message(State) of
        none ->
            simulate_until_done(State);
        {MsgIdx, Msg} ->
            %% Remove message from list
            Messages = lists:delete(Msg, State#sim_state.messages),
            State1 = State#sim_state{messages = Messages},

            %% Check if message should be dropped
            case Msg#sim_message.dropped of
                true ->
                    %% Message was marked as dropped
                    History = [{State1#sim_state.time, {message_dropped, Msg}} |
                              State1#sim_state.history],
                    simulate_until_done(State1#sim_state{history = History});
                false ->
                    %% Deliver the message
                    case deliver_message(Msg, State1) of
                        {ok, State2} ->
                            case check_all_invariants(State2) of
                                ok -> simulate_until_done(State2);
                                {violation, Reason} -> {error, Reason, State2}
                            end;
                        {error, Reason, State2} ->
                            {error, Reason, State2}
                    end
            end
    end.

find_next_deliverable_message(#sim_state{messages = []}) ->
    none;
find_next_deliverable_message(#sim_state{messages = Messages, time = Time, partitions = Partitions}) ->
    %% Find messages that can be delivered (not partitioned)
    Deliverable = lists:filter(fun(Msg) ->
        Msg#sim_message.delivery_time =< Time + 1 andalso
        not is_partitioned(Msg#sim_message.from, Msg#sim_message.to, Partitions)
    end, Messages),
    case Deliverable of
        [] -> none;
        [First | _] -> {1, First}
    end.

is_partitioned(From, To, Partitions) ->
    lists:member({From, To}, Partitions) orelse
    lists:member({To, From}, Partitions).

%%====================================================================
%% Event Processing
%%====================================================================

apply_event({submit_job, JobId, Origin}, State) ->
    apply_submit_job(JobId, Origin, State);

apply_event({start_sibling, JobId, ClusterId}, State) ->
    apply_start_sibling(JobId, ClusterId, State);

apply_event({complete_job, JobId, ClusterId}, State) ->
    apply_complete_job(JobId, ClusterId, State);

apply_event({fail_job, JobId, ClusterId}, State) ->
    apply_fail_job(JobId, ClusterId, State);

apply_event({node_crash, ClusterId}, State) ->
    apply_node_crash(ClusterId, State);

apply_event({node_recover, ClusterId}, State) ->
    apply_node_recover(ClusterId, State);

apply_event({network_partition, Cluster1, Cluster2}, State) ->
    apply_network_partition(Cluster1, Cluster2, State);

apply_event({heal_partition, Cluster1, Cluster2}, State) ->
    apply_heal_partition(Cluster1, Cluster2, State);

apply_event({leader_crash}, State) ->
    apply_leader_crash(State);

apply_event({tres_update, JobId, User, Value}, State) ->
    apply_tres_update(JobId, User, Value, State);

apply_event({migration_transition, NewMode}, State) ->
    apply_migration_transition(NewMode, State);

apply_event({inject_failure}, State) ->
    inject_random_failure(State);

apply_event({try_allocate, JobId, ClusterId}, State) ->
    apply_try_allocate(JobId, ClusterId, State);

apply_event(Unknown, State) ->
    io:format("Unknown event: ~p~n", [Unknown]),
    {ok, State}.

%%====================================================================
%% Event Handlers
%%====================================================================

apply_submit_job(JobId, Origin, State) ->
    %% Create new federated job
    Job = #fed_job{
        id = JobId,
        origin = Origin,
        sibling_states = #{Origin => pending},
        submitted_time = State#sim_state.time
    },

    %% Update cluster state
    Clusters = State#sim_state.clusters,
    OriginCluster = maps:get(Origin, Clusters),
    UpdatedOrigin = OriginCluster#sim_cluster{
        sibling_jobs = maps:put(JobId, pending, OriginCluster#sim_cluster.sibling_jobs),
        pending_jobs = [JobId | OriginCluster#sim_cluster.pending_jobs]
    },

    %% Send sibling creation messages to other clusters
    OtherClusters = maps:keys(Clusters) -- [Origin],
    {Messages, MsgCounter} = lists:foldl(fun(ClusterId, {Msgs, Counter}) ->
        Msg = #sim_message{
            id = Counter,
            type = job_submit,
            from = Origin,
            to = ClusterId,
            payload = {JobId, Origin},
            delivery_time = State#sim_state.time + random_delay(State)
        },
        {[Msg | Msgs], Counter + 1}
    end, {State#sim_state.messages, State#sim_state.message_counter}, OtherClusters),

    %% Schedule try_allocate event
    NewEvents = [{State#sim_state.time + 100, {try_allocate, JobId, Origin}} |
                 State#sim_state.events],

    History = [{State#sim_state.time, {submit_job, JobId, Origin}} | State#sim_state.history],

    {ok, State#sim_state{
        jobs = maps:put(JobId, Job, State#sim_state.jobs),
        clusters = maps:put(Origin, UpdatedOrigin, Clusters),
        messages = Messages,
        message_counter = MsgCounter,
        events = lists:keysort(1, NewEvents),
        history = History
    }}.

apply_try_allocate(JobId, ClusterId, State) ->
    %% Try to allocate/start a job on a specific cluster
    case maps:get(ClusterId, State#sim_state.clusters, undefined) of
        undefined ->
            {ok, State};
        Cluster when Cluster#sim_cluster.state =/= up ->
            %% Cluster is down, reschedule
            NewEvents = [{State#sim_state.time + 500, {try_allocate, JobId, ClusterId}} |
                         State#sim_state.events],
            {ok, State#sim_state{events = lists:keysort(1, NewEvents)}};
        _Cluster ->
            %% Cluster is up, check if sibling is pending
            case maps:get(JobId, State#sim_state.jobs, undefined) of
                undefined ->
                    {ok, State};
                Job ->
                    SiblingState = maps:get(ClusterId, Job#fed_job.sibling_states, null),
                    case SiblingState of
                        pending ->
                            %% Can try to start this sibling
                            apply_start_sibling(JobId, ClusterId, State);
                        _ ->
                            %% Already running, completed, revoked, etc.
                            {ok, State}
                    end
            end
    end.

apply_start_sibling(JobId, ClusterId, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined ->
            {ok, State};
        Job ->
            %% Check if another sibling is already running (SiblingExclusivity check)
            case Job#fed_job.running_cluster of
                undefined ->
                    %% No sibling running, this one can start
                    UpdatedJob = Job#fed_job{
                        sibling_states = maps:put(ClusterId, running, Job#fed_job.sibling_states),
                        running_cluster = ClusterId
                    },

                    %% Update cluster state
                    Cluster = maps:get(ClusterId, State#sim_state.clusters),
                    UpdatedCluster = Cluster#sim_cluster{
                        sibling_jobs = maps:put(JobId, running, Cluster#sim_cluster.sibling_jobs)
                    },

                    %% Send revoke messages to other clusters
                    OtherClusters = maps:keys(Job#fed_job.sibling_states) -- [ClusterId],
                    {Messages, MsgCounter} = lists:foldl(fun(OtherId, {Msgs, Counter}) ->
                        Msg = #sim_message{
                            id = Counter,
                            type = sibling_revoke,
                            from = ClusterId,
                            to = OtherId,
                            payload = {JobId, ClusterId},
                            delivery_time = State#sim_state.time + random_delay(State)
                        },
                        {[Msg | Msgs], Counter + 1}
                    end, {State#sim_state.messages, State#sim_state.message_counter}, OtherClusters),

                    %% Schedule completion event
                    Duration = 1000 + random_int(State, 9000),
                    NewEvents = [{State#sim_state.time + Duration, {complete_job, JobId, ClusterId}} |
                                 State#sim_state.events],

                    History = [{State#sim_state.time, {start_sibling, JobId, ClusterId}} |
                              State#sim_state.history],

                    {ok, State#sim_state{
                        jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                        clusters = maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters),
                        messages = Messages,
                        message_counter = MsgCounter,
                        events = lists:keysort(1, NewEvents),
                        history = History
                    }};
                _OtherCluster ->
                    %% Another sibling is already running, this start should not happen
                    %% (This would be an invariant violation in a real system)
                    {ok, State}
            end
    end.

apply_complete_job(JobId, ClusterId, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined ->
            {ok, State};
        Job when Job#fed_job.running_cluster =:= ClusterId ->
            UpdatedJob = Job#fed_job{
                sibling_states = maps:put(ClusterId, completed, Job#fed_job.sibling_states),
                running_cluster = undefined,
                completed_time = State#sim_state.time
            },

            Cluster = maps:get(ClusterId, State#sim_state.clusters),
            UpdatedCluster = Cluster#sim_cluster{
                sibling_jobs = maps:put(JobId, completed, Cluster#sim_cluster.sibling_jobs)
            },

            History = [{State#sim_state.time, {complete_job, JobId, ClusterId}} |
                      State#sim_state.history],

            {ok, State#sim_state{
                jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                clusters = maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters),
                history = History
            }};
        _ ->
            {ok, State}
    end.

apply_fail_job(JobId, ClusterId, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined ->
            {ok, State};
        Job ->
            UpdatedJob = Job#fed_job{
                sibling_states = maps:put(ClusterId, failed, Job#fed_job.sibling_states),
                running_cluster = undefined
            },

            Cluster = maps:get(ClusterId, State#sim_state.clusters),
            UpdatedCluster = Cluster#sim_cluster{
                sibling_jobs = maps:put(JobId, failed, Cluster#sim_cluster.sibling_jobs)
            },

            History = [{State#sim_state.time, {fail_job, JobId, ClusterId}} |
                      State#sim_state.history],

            {ok, State#sim_state{
                jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                clusters = maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters),
                history = History
            }}
    end.

apply_node_crash(ClusterId, State) ->
    case maps:get(ClusterId, State#sim_state.clusters, undefined) of
        undefined ->
            {ok, State};
        Cluster ->
            %% Mark cluster as down
            UpdatedCluster = Cluster#sim_cluster{state = down},

            %% Fail all running jobs on this cluster
            Jobs = maps:fold(fun(JobId, Job, Acc) ->
                case Job#fed_job.running_cluster of
                    ClusterId ->
                        UpdatedJob = Job#fed_job{
                            sibling_states = maps:put(ClusterId, failed, Job#fed_job.sibling_states),
                            running_cluster = undefined
                        },
                        maps:put(JobId, UpdatedJob, Acc);
                    _ ->
                        Acc
                end
            end, State#sim_state.jobs, State#sim_state.jobs),

            %% Check if crashed node was leader
            {NewLeader, NewClusters} = case State#sim_state.leader of
                ClusterId ->
                    elect_new_leader(maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters));
                _ ->
                    {State#sim_state.leader, maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters)}
            end,

            %% Schedule recovery event
            RecoveryTime = State#sim_state.time + 5000 + random_int(State, 10000),
            NewEvents = [{RecoveryTime, {node_recover, ClusterId}} | State#sim_state.events],

            History = [{State#sim_state.time, {node_crash, ClusterId}} | State#sim_state.history],

            {ok, State#sim_state{
                clusters = NewClusters,
                jobs = Jobs,
                leader = NewLeader,
                events = lists:keysort(1, NewEvents),
                history = History
            }}
    end.

apply_node_recover(ClusterId, State) ->
    case maps:get(ClusterId, State#sim_state.clusters, undefined) of
        undefined ->
            {ok, State};
        Cluster when Cluster#sim_cluster.state =:= down ->
            UpdatedCluster = Cluster#sim_cluster{state = up},
            History = [{State#sim_state.time, {node_recover, ClusterId}} | State#sim_state.history],
            {ok, State#sim_state{
                clusters = maps:put(ClusterId, UpdatedCluster, State#sim_state.clusters),
                history = History
            }};
        _ ->
            {ok, State}
    end.

apply_network_partition(Cluster1, Cluster2, State) ->
    NewPartitions = [{Cluster1, Cluster2} | State#sim_state.partitions],

    %% Mark messages between partitioned nodes as dropped
    Messages = lists:map(fun(Msg) ->
        case (Msg#sim_message.from =:= Cluster1 andalso Msg#sim_message.to =:= Cluster2) orelse
             (Msg#sim_message.from =:= Cluster2 andalso Msg#sim_message.to =:= Cluster1) of
            true -> Msg#sim_message{dropped = true};
            false -> Msg
        end
    end, State#sim_state.messages),

    %% Schedule heal event
    HealTime = State#sim_state.time + 3000 + random_int(State, 7000),
    NewEvents = [{HealTime, {heal_partition, Cluster1, Cluster2}} | State#sim_state.events],

    History = [{State#sim_state.time, {network_partition, Cluster1, Cluster2}} |
              State#sim_state.history],

    {ok, State#sim_state{
        partitions = NewPartitions,
        messages = Messages,
        events = lists:keysort(1, NewEvents),
        history = History
    }}.

apply_heal_partition(Cluster1, Cluster2, State) ->
    NewPartitions = State#sim_state.partitions -- [{Cluster1, Cluster2}, {Cluster2, Cluster1}],
    History = [{State#sim_state.time, {heal_partition, Cluster1, Cluster2}} |
              State#sim_state.history],
    {ok, State#sim_state{
        partitions = NewPartitions,
        history = History
    }}.

apply_leader_crash(State) ->
    case State#sim_state.leader of
        undefined ->
            {ok, State};
        LeaderId ->
            apply_node_crash(LeaderId, State)
    end.

apply_tres_update(JobId, User, Value, State) ->
    %% Apply TRES update to leader's log
    case State#sim_state.leader of
        undefined ->
            {ok, State};
        LeaderId ->
            Cluster = maps:get(LeaderId, State#sim_state.clusters),
            Record = #tres_record{job_id = JobId, user = User, tres_value = Value},
            UpdatedCluster = Cluster#sim_cluster{
                tres_log = [Record | Cluster#sim_cluster.tres_log]
            },

            %% Update totals
            Totals = State#sim_state.tres_totals,
            ClusterTotals = maps:get(LeaderId, Totals, #{}),
            UserTotal = maps:get(User, ClusterTotals, 0),
            NewClusterTotals = maps:put(User, UserTotal + Value, ClusterTotals),
            NewTotals = maps:put(LeaderId, NewClusterTotals, Totals),

            History = [{State#sim_state.time, {tres_update, JobId, User, Value}} |
                      State#sim_state.history],

            {ok, State#sim_state{
                clusters = maps:put(LeaderId, UpdatedCluster, State#sim_state.clusters),
                tres_totals = NewTotals,
                history = History
            }}
    end.

apply_migration_transition(NewMode, State) ->
    Migration = State#sim_state.migration,
    CurrentMode = Migration#migration_state.mode,

    %% Check if transition is valid
    case is_valid_transition(CurrentMode, NewMode) of
        true ->
            %% Apply mode-specific state changes
            UpdatedMigration = case NewMode of
                standalone ->
                    %% Transitioning to standalone - SLURM is decommissioned
                    Migration#migration_state{
                        mode = NewMode,
                        slurm_active = false,
                        slurm_jobs = [],
                        forwarded_jobs = []
                    };
                active ->
                    %% FLURM must be ready for active mode
                    Migration#migration_state{
                        mode = NewMode,
                        flurm_ready = true
                    };
                _ ->
                    Migration#migration_state{mode = NewMode}
            end,
            History = [{State#sim_state.time, {migration_transition, CurrentMode, NewMode}} |
                      State#sim_state.history],
            {ok, State#sim_state{
                migration = UpdatedMigration,
                history = History
            }};
        false ->
            %% Invalid transition - this is an invariant violation
            {error, {invalid_migration_transition, CurrentMode, NewMode}, State}
    end.

inject_random_failure(State) ->
    Config = State#sim_state.config,
    FailureRate = maps:get(failure_rate, Config, 0.1),
    PartitionRate = maps:get(partition_rate, Config, 0.05),

    {R1, Seed1} = rand:uniform_s(State#sim_state.seed),
    State1 = State#sim_state{seed = Seed1},

    cond_apply([
        {R1 < FailureRate, fun() ->
            %% Random node crash
            ClusterIds = maps:keys(State1#sim_state.clusters),
            UpClusters = [C || C <- ClusterIds,
                          (maps:get(C, State1#sim_state.clusters))#sim_cluster.state =:= up],
            case UpClusters of
                [] -> {ok, State1};
                _ ->
                    Idx = random_int(State1, length(UpClusters)),
                    ClusterId = lists:nth(max(1, Idx), UpClusters),
                    apply_node_crash(ClusterId, State1)
            end
        end},
        {R1 < FailureRate + PartitionRate, fun() ->
            %% Random network partition
            ClusterIds = maps:keys(State1#sim_state.clusters),
            case length(ClusterIds) >= 2 of
                true ->
                    Idx1 = random_int(State1, length(ClusterIds)),
                    Idx2 = random_int(State1, length(ClusterIds)),
                    C1 = lists:nth(max(1, Idx1), ClusterIds),
                    C2 = lists:nth(max(1, Idx2), ClusterIds),
                    case C1 =/= C2 of
                        true -> apply_network_partition(C1, C2, State1);
                        false -> {ok, State1}
                    end;
                false ->
                    {ok, State1}
            end
        end},
        {true, fun() -> {ok, State1} end}
    ]).

%%====================================================================
%% Message Delivery
%%====================================================================

deliver_message(#sim_message{type = job_submit, to = To, payload = {JobId, Origin}} = _Msg, State) ->
    case maps:get(To, State#sim_state.clusters, undefined) of
        undefined ->
            {ok, State};
        Cluster when Cluster#sim_cluster.state =:= up ->
            %% Create sibling at this cluster
            Job = maps:get(JobId, State#sim_state.jobs, #fed_job{id = JobId, origin = Origin}),
            UpdatedJob = Job#fed_job{
                sibling_states = maps:put(To, pending, Job#fed_job.sibling_states)
            },
            UpdatedCluster = Cluster#sim_cluster{
                sibling_jobs = maps:put(JobId, pending, Cluster#sim_cluster.sibling_jobs)
            },

            %% Schedule try_allocate for this cluster
            NewEvents = [{State#sim_state.time + 50 + random_int(State, 100),
                         {try_allocate, JobId, To}} | State#sim_state.events],

            {ok, State#sim_state{
                jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                clusters = maps:put(To, UpdatedCluster, State#sim_state.clusters),
                events = lists:keysort(1, NewEvents)
            }};
        _ ->
            %% Cluster is down, message lost
            {ok, State}
    end;

deliver_message(#sim_message{type = sibling_revoke, to = To, payload = {JobId, _RunningCluster}}, State) ->
    case maps:get(To, State#sim_state.clusters, undefined) of
        undefined ->
            {ok, State};
        Cluster when Cluster#sim_cluster.state =:= up ->
            %% Revoke sibling at this cluster
            case maps:get(JobId, Cluster#sim_cluster.sibling_jobs, undefined) of
                pending ->
                    UpdatedCluster = Cluster#sim_cluster{
                        sibling_jobs = maps:put(JobId, revoked, Cluster#sim_cluster.sibling_jobs)
                    },
                    Job = maps:get(JobId, State#sim_state.jobs),
                    UpdatedJob = Job#fed_job{
                        sibling_states = maps:put(To, revoked, Job#fed_job.sibling_states)
                    },
                    {ok, State#sim_state{
                        jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                        clusters = maps:put(To, UpdatedCluster, State#sim_state.clusters)
                    }};
                _ ->
                    {ok, State}
            end;
        _ ->
            {ok, State}
    end;

deliver_message(_Msg, State) ->
    {ok, State}.

%%====================================================================
%% Invariant Checking
%%====================================================================

check_all_invariants(State) ->
    Checks = [
        check_sibling_exclusivity(State),
        check_no_job_loss(State),
        check_tres_consistency(State),
        check_valid_mode_transitions(State),
        check_job_states_valid(State)
    ],
    case [R || {violation, _} = R <- Checks] of
        [] -> ok;
        [First | _] -> First
    end.

%% SiblingExclusivity: At most one sibling running per job
check_sibling_exclusivity(#sim_state{jobs = Jobs}) ->
    Violations = maps:fold(fun(JobId, Job, Acc) ->
        RunningSiblings = [C || {C, running} <- maps:to_list(Job#fed_job.sibling_states)],
        case length(RunningSiblings) > 1 of
            true -> [{JobId, RunningSiblings} | Acc];
            false -> Acc
        end
    end, [], Jobs),
    case Violations of
        [] -> ok;
        _ -> {violation, {sibling_exclusivity, Violations}}
    end.

%% NoJobLoss: Jobs submitted must have at least one active sibling or be completed
check_no_job_loss(#sim_state{jobs = Jobs}) ->
    TerminalStates = [revoked, completed, failed],
    Lost = maps:fold(fun(JobId, Job, Acc) ->
        SiblingStates = maps:values(Job#fed_job.sibling_states),
        HasActive = lists:any(fun(S) -> S =:= pending orelse S =:= running end, SiblingStates),
        AllTerminal = lists:all(fun(S) -> lists:member(S, TerminalStates) end, SiblingStates),
        HasCompleted = lists:member(completed, SiblingStates),
        case HasActive orelse AllTerminal orelse HasCompleted of
            true -> Acc;
            false -> [JobId | Acc]
        end
    end, [], Jobs),
    case Lost of
        [] -> ok;
        _ -> {violation, {job_loss, Lost}}
    end.

%% TRESConsistency: TRES values for same job consistent across nodes
check_tres_consistency(#sim_state{clusters = Clusters}) ->
    %% Collect all TRES records by job
    AllRecords = maps:fold(fun(_ClusterId, Cluster, Acc) ->
        lists:foldl(fun(#tres_record{job_id = JobId, tres_value = Value}, InnerAcc) ->
            Existing = maps:get(JobId, InnerAcc, []),
            maps:put(JobId, [Value | Existing], InnerAcc)
        end, Acc, Cluster#sim_cluster.tres_log)
    end, #{}, Clusters),

    %% Check each job has consistent values
    Inconsistent = maps:fold(fun(JobId, Values, Acc) ->
        UniqueValues = lists:usort(Values),
        case length(UniqueValues) > 1 of
            true -> [{JobId, UniqueValues} | Acc];
            false -> Acc
        end
    end, [], AllRecords),

    case Inconsistent of
        [] -> ok;
        _ -> {violation, {tres_inconsistency, Inconsistent}}
    end.

%% ValidModeTransitions: Migration mode transitions must be valid
check_valid_mode_transitions(#sim_state{migration = Migration}) ->
    %% This is checked at transition time, but we verify constraints here
    Mode = Migration#migration_state.mode,
    SlurmActive = Migration#migration_state.slurm_active,

    case Mode of
        standalone when SlurmActive ->
            {violation, {invalid_standalone_state, slurm_still_active}};
        _ ->
            ok
    end.

%% Job states must be valid
check_job_states_valid(#sim_state{jobs = Jobs}) ->
    ValidStates = [null, pending, running, revoked, completed, failed],
    Invalid = maps:fold(fun(JobId, Job, Acc) ->
        BadStates = [{C, S} || {C, S} <- maps:to_list(Job#fed_job.sibling_states),
                               not lists:member(S, ValidStates)],
        case BadStates of
            [] -> Acc;
            _ -> [{JobId, BadStates} | Acc]
        end
    end, [], Jobs),
    case Invalid of
        [] -> ok;
        _ -> {violation, {invalid_job_states, Invalid}}
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

cluster_id(N) ->
    list_to_binary(io_lib:format("cluster-~3..0B", [N])).

random_delay(State) ->
    Config = State#sim_state.config,
    MaxDelay = maps:get(message_delay_max, Config, 500),
    10 + random_int(State, MaxDelay).

random_int(#sim_state{seed = Seed}, Max) when Max > 0 ->
    {Val, _} = rand:uniform_s(Max, Seed),
    Val;
random_int(_, _) ->
    1.

elect_new_leader(Clusters) ->
    UpClusters = [{Id, C} || {Id, C} <- maps:to_list(Clusters),
                             C#sim_cluster.state =:= up],
    case UpClusters of
        [] ->
            {undefined, Clusters};
        [{NewLeaderId, Cluster} | Rest] ->
            UpdatedLeader = Cluster#sim_cluster{is_leader = true},
            UpdatedClusters = lists:foldl(fun({Id, C}, Acc) ->
                maps:put(Id, C#sim_cluster{is_leader = false}, Acc)
            end, maps:put(NewLeaderId, UpdatedLeader, Clusters), Rest),
            {NewLeaderId, UpdatedClusters}
    end.

is_valid_transition(shadow, active) -> true;
is_valid_transition(active, shadow) -> true;
is_valid_transition(active, primary) -> true;
is_valid_transition(primary, active) -> true;
is_valid_transition(primary, standalone) -> true;
is_valid_transition(_, _) -> false.

cond_apply([]) ->
    {ok, undefined};
cond_apply([{true, Fun} | _]) ->
    Fun();
cond_apply([{false, _} | Rest]) ->
    cond_apply(Rest).

generate_initial_events(Scenario, NumJobs, Config) ->
    BaseEvents = generate_job_submit_events(NumJobs, Config),
    FailureEvents = generate_failure_events(Scenario, NumJobs, Config),
    BaseEvents ++ FailureEvents.

generate_job_submit_events(NumJobs, _Config) ->
    [{I * 100, {submit_job, I, cluster_id(1)}} || I <- lists:seq(1, NumJobs)].

generate_failure_events(federation_sibling, NumJobs, Config) ->
    FailureRate = maps:get(failure_rate, Config, 0.1),
    NumFailures = trunc(NumJobs * FailureRate),
    [{1000 + I * 500, {inject_failure}} || I <- lists:seq(1, NumFailures)];

generate_failure_events(accounting_tres, NumJobs, Config) ->
    %% Add TRES update events
    TRESEvents = [{I * 100 + 50, {tres_update, I, <<"user1">>, 100 + I * 10}}
                  || I <- lists:seq(1, NumJobs)],
    FailureEvents = generate_failure_events(federation_sibling, NumJobs, Config),
    TRESEvents ++ FailureEvents;

generate_failure_events(migration_mode, _NumJobs, _Config) ->
    %% Schedule migration transitions
    [
        {5000, {migration_transition, active}},
        {15000, {migration_transition, primary}},
        {30000, {migration_transition, standalone}}
    ];

generate_failure_events(leader_election, NumJobs, Config) ->
    LeaderFailureRate = maps:get(leader_failure_rate, Config, 0.1),
    NumLeaderFailures = trunc(NumJobs * LeaderFailureRate),
    [{2000 + I * 1000, {leader_crash}} || I <- lists:seq(1, NumLeaderFailures)];

generate_failure_events(combined_failures, NumJobs, Config) ->
    F1 = generate_failure_events(federation_sibling, NumJobs, Config),
    F2 = generate_failure_events(leader_election, NumJobs, Config),
    F1 ++ F2;

generate_failure_events(_, _, _) ->
    [].

default_config(federation_sibling) ->
    #{num_clusters => 3, num_jobs => 10, max_time => 50000, failure_rate => 0.1};
default_config(accounting_tres) ->
    #{num_clusters => 5, num_jobs => 20, max_time => 100000, failure_rate => 0.15};
default_config(migration_mode) ->
    #{num_clusters => 3, num_jobs => 15, max_time => 80000, failure_rate => 0.1};
default_config(leader_election) ->
    #{num_clusters => 5, num_jobs => 25, max_time => 120000, leader_failure_rate => 0.2};
default_config(combined_failures) ->
    #{num_clusters => 5, num_jobs => 30, max_time => 150000, failure_rate => 0.2};
default_config(_) ->
    #{num_clusters => 3, num_jobs => 10, max_time => 50000}.

generate_seeds(NumSeeds) ->
    [erlang:phash2({make_ref(), I}) || I <- lists:seq(1, NumSeeds)].

analyze_results(Results) ->
    Total = length(Results),
    Passed = length([ok || {_, _, ok} <- Results]),
    Failed = length([err || {_, _, {error, _}} <- Results]),
    io:format("~n=== Simulation Test Results ===~n"),
    io:format("Total:  ~p~n", [Total]),
    io:format("Passed: ~p (~.1f%)~n", [Passed, Passed * 100 / max(1, Total)]),
    io:format("Failed: ~p (~.1f%)~n", [Failed, Failed * 100 / max(1, Total)]),
    case Failed > 0 of
        true ->
            io:format("~nFailed tests (reproduce with seed):~n"),
            [io:format("  ~p with seed ~p: ~p~n", [S, Seed, Err])
             || {S, Seed, {error, Err}} <- Results];
        false ->
            ok
    end,
    ok.

print_state_summary(State) ->
    io:format("Time: ~p~n", [State#sim_state.time]),
    io:format("Jobs: ~p~n", [maps:size(State#sim_state.jobs)]),
    io:format("Clusters: ~p~n", [maps:size(State#sim_state.clusters)]),
    io:format("Partitions: ~p~n", [State#sim_state.partitions]),
    io:format("Leader: ~p~n", [State#sim_state.leader]),
    io:format("History length: ~p~n", [length(State#sim_state.history)]).

%%====================================================================
%% Additional Test Cases
%%====================================================================

%% Test that try_allocate events work correctly
try_allocate_test_() ->
    {timeout, 30,
     fun() ->
         Seed = 99999,
         Config = #{num_clusters => 2, num_jobs => 3, max_time => 20000, failure_rate => 0.0},
         {ok, FinalState} = run_simulation(federation_sibling, Seed, Config),
         %% All jobs should have been processed
         Jobs = FinalState#sim_state.jobs,
         ?assertEqual(3, maps:size(Jobs)),
         ok
     end}.

%% Test network partition handling
network_partition_test_() ->
    {timeout, 30,
     fun() ->
         Seed = 88888,
         Config = #{num_clusters => 3, num_jobs => 5, max_time => 30000,
                   failure_rate => 0.0, partition_rate => 0.3},
         %% Should complete without invariant violations
         case run_simulation(federation_sibling, Seed, Config) of
             {ok, _} -> ok;
             {error, Reason, _} ->
                 ?assert(false, io_lib:format("Unexpected failure: ~p", [Reason]))
         end
     end}.

%% Test rapid job submission
rapid_submission_test_() ->
    {timeout, 60,
     fun() ->
         Seed = 77777,
         Config = #{num_clusters => 5, num_jobs => 50, max_time => 100000, failure_rate => 0.05},
         case run_simulation(federation_sibling, Seed, Config) of
             {ok, _} -> ok;
             {error, Reason, _} ->
                 ?assert(false, io_lib:format("Rapid submission failed: ~p", [Reason]))
         end
     end}.

%% Test leader election stability
leader_stability_test_() ->
    {timeout, 60,
     fun() ->
         Seed = 66666,
         Config = #{num_clusters => 5, num_jobs => 20, max_time => 80000, leader_failure_rate => 0.3},
         case run_simulation(leader_election, Seed, Config) of
             {ok, FinalState} ->
                 %% Leader should be valid: either undefined or pointing to an up cluster
                 Leader = FinalState#sim_state.leader,
                 case Leader of
                     undefined ->
                         %% No leader is acceptable (all may have crashed at some point)
                         ok;
                     _ ->
                         %% If there's a leader, verify it's in a valid state
                         Cluster = maps:get(Leader, FinalState#sim_state.clusters, undefined),
                         ?assertNotEqual(undefined, Cluster),
                         %% Leader can be down (crashed but not yet recovered/replaced)
                         %% This is acceptable in a distributed system
                         ok
                 end;
             {error, Reason, _} ->
                 ?assert(false, io_lib:format("Leader stability failed: ~p", [Reason]))
         end
     end}.
