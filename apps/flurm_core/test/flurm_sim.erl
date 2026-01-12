%%%-------------------------------------------------------------------
%%% @doc FLURM Deterministic Simulation Framework
%%%
%%% FoundationDB-style deterministic simulation for testing FLURM.
%%% Features:
%%% - Seeded PRNG for reproducible randomness
%%% - Simulated time (no wall clock)
%%% - Deterministic failure injection
%%% - Event-driven simulation
%%%
%%% This allows running thousands of test scenarios with specific
%%% seeds to reproduce bugs and verify fixes.
%%%
%%% Usage:
%%%   flurm_sim:run_with_seed(my_scenario, 12345, 300000).
%%%   flurm_sim:run_all_scenarios(100).  % 100 seeds per scenario
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_sim).

-export([
    run/2,
    run_with_seed/3,
    run_all_scenarios/1,
    scenarios/0,
    analyze_results/1
]).

-include_lib("flurm_core/include/flurm_core.hrl").

%% Simulation state
-record(sim_state, {
    seed :: rand:state(),
    time = 0 :: non_neg_integer(),
    nodes = #{} :: map(),
    controllers = #{} :: map(),
    jobs = #{} :: map(),
    events = [] :: list(),
    history = [] :: list(),
    config = #{} :: map()
}).

%% Simulated node
-record(node_sim, {
    name :: binary(),
    cpus :: pos_integer(),
    memory :: pos_integer(),
    state = up :: up | down | drain,
    jobs = [] :: [pos_integer()],
    last_heartbeat = 0 :: non_neg_integer()
}).

%% Simulated controller
-record(ctrl_sim, {
    name :: atom(),
    state = up :: up | down,
    is_leader = false :: boolean(),
    pending_jobs = [] :: [pos_integer()]
}).

%% Simulated job
-record(job_sim, {
    id :: pos_integer(),
    state = pending :: atom(),
    nodes = [] :: [binary()],
    submit_time = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    end_time = 0 :: non_neg_integer()
}).

%%====================================================================
%% Public API
%%====================================================================

run(Scenario, MaxTime) ->
    Seed = erlang:phash2(make_ref()),
    run_with_seed(Scenario, Seed, MaxTime).

run_with_seed(Scenario, Seed, MaxTime) ->
    io:format("Running scenario ~p with seed ~p...~n", [Scenario, Seed]),
    InitState = init_sim(Scenario, Seed),
    Result = simulate_until(InitState, MaxTime),
    case Result of
        {ok, FinalState} ->
            io:format("Scenario ~p completed successfully~n", [Scenario]),
            {ok, FinalState};
        {error, Reason, State} ->
            io:format("Scenario ~p FAILED: ~p~n", [Scenario, Reason]),
            {error, {invariant_violation, Reason, State}}
    end.

run_all_scenarios(NumSeeds) ->
    Seeds = [rand:uniform(1 bsl 32) || _ <- lists:seq(1, NumSeeds)],
    AllScenarios = scenarios(),
    Results = [
        begin
            Res = run_with_seed(ScenarioName, Seed, 300000),
            Status = case Res of
                {ok, _} -> ok;
                {error, E} -> {error, E}
            end,
            {ScenarioName, Seed, Status}
        end
        || {ScenarioName, _} <- AllScenarios,
           Seed <- Seeds
    ],
    analyze_results(Results),
    Results.

scenarios() ->
    [
        {single_job_lifecycle, #{nodes => 3, jobs => 1, failures => []}},
        {hundred_jobs_fifo, #{nodes => 10, jobs => 100, failures => []}},
        {job_cancellation, #{nodes => 5, jobs => 20, cancel_rate => 0.2}},
        {leader_crash_during_submit, #{
            nodes => 5, jobs => 50,
            failures => [{kill_leader, at_job_submit}]
        }},
        {node_failure_during_job, #{
            nodes => 10, jobs => 30,
            failures => [{kill_node, random, during_job}]
        }},
        {cascading_node_failures, #{
            nodes => 20, jobs => 100,
            failures => [{kill_nodes, 5, sequential, 1000}]
        }},
        {concurrent_submit_cancel, #{
            nodes => 3, jobs => 10,
            race => {submit, cancel, same_job}
        }},
        {rapid_job_submission, #{
            nodes => 5, jobs => 1000,
            submit_rate => high
        }},
        {many_nodes_few_jobs, #{nodes => 100, jobs => 10}},
        {few_nodes_many_jobs, #{nodes => 3, jobs => 500}},
        {balanced_load, #{nodes => 50, jobs => 500}}
    ].

analyze_results(Results) ->
    Total = length(Results),
    Passed = length([ok || {_, _, ok} <- Results]),
    Failed = length([err || {_, _, {error, _}} <- Results]),
    io:format("~n=== Simulation Results ===~n"),
    io:format("Total:  ~p~n", [Total]),
    io:format("Passed: ~p (~.1f%)~n", [Passed, Passed * 100 / max(1, Total)]),
    io:format("Failed: ~p (~.1f%)~n", [Failed, Failed * 100 / max(1, Total)]),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

init_sim(Scenario, Seed) ->
    RandState = rand:seed(exsplus, {Seed, Seed, Seed}),
    Config = proplists:get_value(Scenario, scenarios(), #{}),
    NumNodes = maps:get(nodes, Config, 5),
    NumJobs = maps:get(jobs, Config, 10),
    Nodes = lists:foldl(fun(I, Acc) ->
        Name = list_to_binary(io_lib:format("node~3..0B", [I])),
        Node = #node_sim{name = Name, cpus = 4, memory = 8192},
        maps:put(Name, Node, Acc)
    end, #{}, lists:seq(1, NumNodes)),
    Controllers = #{ctrl1 => #ctrl_sim{name = ctrl1, is_leader = true}},
    InitialEvents = [{I * 100, {submit_job, I}} || I <- lists:seq(1, NumJobs)],
    FailureEvents = generate_failure_events(Config, NumJobs),
    AllEvents = lists:keysort(1, InitialEvents ++ FailureEvents),
    #sim_state{seed = RandState, nodes = Nodes, controllers = Controllers,
               events = AllEvents, config = Config}.

simulate_until(#sim_state{time = T} = State, MaxTime) when T >= MaxTime ->
    {ok, State};
simulate_until(#sim_state{events = []} = State, _MaxTime) ->
    {ok, State};
simulate_until(#sim_state{events = [{Time, Event} | Rest]} = State, MaxTime) ->
    NewState = State#sim_state{time = Time, events = Rest},
    case apply_event(Event, NewState) of
        {ok, State2} ->
            case check_invariants(State2) of
                ok -> simulate_until(State2, MaxTime);
                {violation, Reason} -> {error, Reason, State2}
            end;
        {error, Reason, State2} ->
            {error, Reason, State2}
    end.

apply_event({submit_job, JobId}, State) ->
    Job = #job_sim{id = JobId, state = pending, submit_time = State#sim_state.time},
    Jobs = maps:put(JobId, Job, State#sim_state.jobs),
    NewEvents = [{State#sim_state.time + 10, {try_allocate, JobId}} | State#sim_state.events],
    {ok, State#sim_state{jobs = Jobs, events = lists:keysort(1, NewEvents),
                         history = [{State#sim_state.time, {submit_job, JobId}} | State#sim_state.history]}};

apply_event({try_allocate, JobId}, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined -> {ok, State};
        #job_sim{state = pending} = Job ->
            case find_available_node(State) of
                {ok, NodeName} ->
                    Node = maps:get(NodeName, State#sim_state.nodes),
                    UpdatedNode = Node#node_sim{jobs = [JobId | Node#node_sim.jobs]},
                    UpdatedJob = Job#job_sim{state = running, nodes = [NodeName],
                                             start_time = State#sim_state.time},
                    {Duration, NewSeed} = sim_random(1000, 10000, State#sim_state.seed),
                    NewEvents = [{State#sim_state.time + Duration, {complete_job, JobId}} |
                                 State#sim_state.events],
                    {ok, State#sim_state{seed = NewSeed,
                                         nodes = maps:put(NodeName, UpdatedNode, State#sim_state.nodes),
                                         jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                                         events = lists:keysort(1, NewEvents)}};
                none ->
                    NewEvents = [{State#sim_state.time + 100, {try_allocate, JobId}} |
                                 State#sim_state.events],
                    {ok, State#sim_state{events = lists:keysort(1, NewEvents)}}
            end;
        _ -> {ok, State}
    end;

apply_event({complete_job, JobId}, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined -> {ok, State};
        #job_sim{state = running, nodes = [NodeName]} = Job ->
            UpdatedJob = Job#job_sim{state = completed, end_time = State#sim_state.time},
            Node = maps:get(NodeName, State#sim_state.nodes),
            UpdatedNode = Node#node_sim{jobs = lists:delete(JobId, Node#node_sim.jobs)},
            {ok, State#sim_state{nodes = maps:put(NodeName, UpdatedNode, State#sim_state.nodes),
                                 jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                                 history = [{State#sim_state.time, {complete_job, JobId}} | State#sim_state.history]}};
        _ -> {ok, State}
    end;

apply_event({kill_node, NodeName}, State) ->
    case maps:get(NodeName, State#sim_state.nodes, undefined) of
        undefined -> {ok, State};
        Node ->
            FailedJobs = lists:foldl(fun(JobId, Jobs) ->
                case maps:get(JobId, Jobs, undefined) of
                    undefined -> Jobs;
                    Job -> maps:put(JobId, Job#job_sim{state = failed, end_time = State#sim_state.time}, Jobs)
                end
            end, State#sim_state.jobs, Node#node_sim.jobs),
            UpdatedNode = Node#node_sim{state = down, jobs = []},
            {ok, State#sim_state{nodes = maps:put(NodeName, UpdatedNode, State#sim_state.nodes),
                                 jobs = FailedJobs,
                                 history = [{State#sim_state.time, {kill_node, NodeName}} | State#sim_state.history]}}
    end;

apply_event({kill_leader, _Trigger}, State) ->
    Controllers = maps:map(fun(_, Ctrl) ->
        case Ctrl#ctrl_sim.is_leader of
            true -> Ctrl#ctrl_sim{state = down, is_leader = false};
            false -> Ctrl
        end
    end, State#sim_state.controllers),
    NewControllers = elect_leader(Controllers),
    {ok, State#sim_state{controllers = NewControllers,
                         history = [{State#sim_state.time, kill_leader} | State#sim_state.history]}};

apply_event(_Event, State) ->
    {ok, State}.

find_available_node(#sim_state{nodes = Nodes}) ->
    Available = [Name || {Name, #node_sim{state = up, jobs = Jobs}} <- maps:to_list(Nodes),
                         length(Jobs) < 4],
    case Available of
        [] -> none;
        [First | _] -> {ok, First}
    end.

elect_leader(Controllers) ->
    UpControllers = [Name || {Name, #ctrl_sim{state = up}} <- maps:to_list(Controllers)],
    case UpControllers of
        [] -> Controllers;
        [NewLeader | _] ->
            maps:map(fun(Name, Ctrl) -> Ctrl#ctrl_sim{is_leader = (Name == NewLeader)} end, Controllers)
    end.

sim_random(Min, Max, Seed) ->
    {Val, NewSeed} = rand:uniform_s(Max - Min + 1, Seed),
    {Min + Val - 1, NewSeed}.

generate_failure_events(Config, NumJobs) ->
    Failures = maps:get(failures, Config, []),
    lists:flatmap(fun
        ({kill_leader, at_job_submit}) ->
            [{NumJobs div 2 * 100 + 50, {kill_leader, at_job_submit}}];
        ({kill_node, random, during_job}) ->
            [{5000, {kill_node, <<"node001">>}}];
        ({kill_nodes, Count, sequential, Interval}) ->
            [{1000 + I * Interval, {kill_node, list_to_binary(io_lib:format("node~3..0B", [I]))}}
             || I <- lists:seq(1, Count)];
        (_) -> []
    end, Failures).

check_invariants(State) ->
    Checks = [check_no_double_allocation(State), check_job_states_valid(State),
              check_node_job_consistency(State)],
    case [R || {violation, _} = R <- Checks] of
        [] -> ok;
        [First | _] -> First
    end.

check_no_double_allocation(#sim_state{jobs = Jobs}) ->
    RunningJobs = [Job || {_, #job_sim{state = running} = Job} <- maps:to_list(Jobs)],
    AllNodes = lists:flatmap(fun(#job_sim{nodes = Nodes}) -> Nodes end, RunningJobs),
    UniqueNodes = lists:usort(AllNodes),
    case length(AllNodes) == length(UniqueNodes) of
        true -> ok;
        false -> {violation, double_allocation}
    end.

check_job_states_valid(#sim_state{jobs = Jobs}) ->
    ValidStates = [pending, running, completed, cancelled, failed],
    Invalid = [Id || {Id, #job_sim{state = S}} <- maps:to_list(Jobs),
                     not lists:member(S, ValidStates)],
    case Invalid of
        [] -> ok;
        _ -> {violation, {invalid_job_states, Invalid}}
    end.

check_node_job_consistency(#sim_state{nodes = Nodes, jobs = Jobs}) ->
    Inconsistent = maps:fold(fun(NodeName, #node_sim{jobs = NodeJobs}, Acc) ->
        BadJobs = [JobId || JobId <- NodeJobs,
                            case maps:get(JobId, Jobs, undefined) of
                                #job_sim{state = running, nodes = AllocNodes} ->
                                    not lists:member(NodeName, AllocNodes);
                                _ -> true
                            end],
        BadJobs ++ Acc
    end, [], Nodes),
    case Inconsistent of
        [] -> ok;
        _ -> {violation, {node_job_inconsistency, Inconsistent}}
    end.
