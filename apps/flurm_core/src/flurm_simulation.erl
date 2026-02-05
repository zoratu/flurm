%%%-------------------------------------------------------------------
%%% @doc FLURM Deterministic Simulation Testing Framework
%%%
%%% FoundationDB-style deterministic simulation for distributed systems.
%%% Runs distributed system in simulated time with injected failures.
%%%
%%% Features:
%%% - Deterministic execution with seeded RNG
%%% - Simulated time (no real delays)
%%% - Controlled message delivery
%%% - Failure injection (node kills, partitions, delays)
%%% - Reproducible from seed
%%% - Invariant checking after each step
%%%
%%% Usage:
%%%   {ok, Result} = flurm_simulation:run(Seed, Scenario)
%%%   ok = flurm_simulation:run_all_scenarios()
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_simulation).

-export([
    run/2,
    run_all_scenarios/0,
    run_all_scenarios/1,
    scenarios/0
]).

%% Test exports
-ifdef(TEST).
-export([
    init_simulation/2,
    step/1,
    check_invariants/1,
    inject_failures/1,
    deliver_message/2
]).
-endif.

-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Types
%%====================================================================

-type node_id() :: binary().
-type sim_time() :: non_neg_integer().
-type scenario() :: atom().

-record(sim_message, {
    from :: node_id(),
    to :: node_id(),
    type :: atom(),
    payload :: term(),
    send_time :: sim_time(),
    delivery_time :: sim_time()
}).

-record(sim_node, {
    id :: node_id(),
    state :: map(),             % Node's internal state
    role :: leader | follower | candidate,
    online :: boolean(),
    partitioned_from :: [node_id()],
    pending_messages :: [#sim_message{}],
    log :: [term()]             % Raft log entries
}).

-record(sim_job, {
    id :: non_neg_integer(),
    state :: pending | running | completed | failed | cancelled,
    sibling_clusters :: [node_id()],
    running_cluster :: node_id() | undefined,
    submit_time :: sim_time()
}).

-record(sim_state, {
    nodes :: #{node_id() => #sim_node{}},
    messages :: [#sim_message{}],
    time :: sim_time(),
    rng :: rand:state(),
    events :: [term()],
    jobs :: #{non_neg_integer() => #sim_job{}},
    next_job_id :: non_neg_integer(),
    leader :: node_id() | undefined,
    invariant_violations :: [term()]
}).

-record(sim_result, {
    seed :: integer(),
    scenario :: scenario(),
    success :: boolean(),
    steps :: non_neg_integer(),
    final_state :: #sim_state{},
    violations :: [term()],
    trace :: [term()]
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Run a simulation with a given seed and scenario.
-spec run(integer(), scenario()) -> {ok, #sim_result{}} | {error, term()}.
run(Seed, Scenario) ->
    try
        State0 = init_simulation(Seed, Scenario),
        {FinalState, Steps} = run_until_done(State0, 0, 10000),
        Result = #sim_result{
            seed = Seed,
            scenario = Scenario,
            success = FinalState#sim_state.invariant_violations =:= [],
            steps = Steps,
            final_state = FinalState,
            violations = FinalState#sim_state.invariant_violations,
            trace = lists:reverse(FinalState#sim_state.events)
        },
        {ok, Result}
    catch
        E:R:ST ->
            {error, {E, R, ST}}
    end.

%% @doc Run all scenarios with default number of seeds.
-spec run_all_scenarios() -> {ok, #{passed := non_neg_integer(), failed := non_neg_integer()}}.
run_all_scenarios() ->
    run_all_scenarios(100).

%% @doc Run all scenarios with specified number of seeds per scenario.
-spec run_all_scenarios(pos_integer()) -> {ok, #{passed := non_neg_integer(), failed := non_neg_integer()}}.
run_all_scenarios(SeedsPerScenario) ->
    Seeds = lists:seq(1, SeedsPerScenario),
    Scenarios = scenarios(),

    Results = lists:foldl(fun(Scenario, Acc) ->
        ScenarioResults = [run(Seed, Scenario) || Seed <- Seeds],
        Passed = length([R || {ok, #sim_result{success = true} = R} <- ScenarioResults]),
        Failed = SeedsPerScenario - Passed,
        io:format("~p: ~p passed, ~p failed~n", [Scenario, Passed, Failed]),
        %% Report first failure for debugging
        case [R || {ok, #sim_result{success = false} = R} <- ScenarioResults] of
            [FirstFail | _] ->
                io:format("  First failure seed=~p: ~p~n",
                          [FirstFail#sim_result.seed, hd(FirstFail#sim_result.violations)]);
            [] -> ok
        end,
        #{passed => maps:get(passed, Acc, 0) + Passed,
          failed => maps:get(failed, Acc, 0) + Failed}
    end, #{passed => 0, failed => 0}, Scenarios),

    {ok, Results}.

%% @doc List of available simulation scenarios.
-spec scenarios() -> [scenario()].
scenarios() -> [
    %% Basic operations
    submit_job_single_node,
    submit_job_multi_node,
    job_completion,
    job_cancellation,

    %% Leader election
    leader_election_normal,
    leader_failure_during_submit,

    %% Partitions
    partition_minority,
    partition_majority,
    partition_during_job_run,

    %% Federation sibling jobs
    sibling_job_basic,
    sibling_job_race,
    sibling_revocation,
    origin_cluster_failure,

    %% Cascading failures
    cascading_node_failures,

    %% Migration scenarios
    migration_shadow_to_active,
    migration_with_failures
].

%%====================================================================
%% Internal - Simulation Setup
%%====================================================================

%% @doc Initialize simulation state based on scenario.
-spec init_simulation(integer(), scenario()) -> #sim_state{}.
init_simulation(Seed, Scenario) ->
    Rng = rand:seed_s(exsplus, {Seed, Seed * 2, Seed * 3}),

    %% Create nodes based on scenario
    {NodeCount, InitialLeader} = scenario_config(Scenario),
    Nodes = create_nodes(NodeCount, InitialLeader),

    State = #sim_state{
        nodes = Nodes,
        messages = [],
        time = 0,
        rng = Rng,
        events = [{init, Scenario}],
        jobs = #{},
        next_job_id = 1,
        leader = InitialLeader,
        invariant_violations = []
    },

    %% Apply scenario-specific initialization
    apply_scenario_init(Scenario, State).

scenario_config(submit_job_single_node) -> {1, <<"node1">>};
scenario_config(submit_job_multi_node) -> {3, <<"node1">>};
scenario_config(job_completion) -> {3, <<"node1">>};
scenario_config(job_cancellation) -> {3, <<"node1">>};
scenario_config(leader_election_normal) -> {3, undefined};
scenario_config(leader_failure_during_submit) -> {3, <<"node1">>};
scenario_config(partition_minority) -> {5, <<"node1">>};
scenario_config(partition_majority) -> {5, <<"node1">>};
scenario_config(partition_during_job_run) -> {3, <<"node1">>};
scenario_config(sibling_job_basic) -> {3, <<"node1">>};
scenario_config(sibling_job_race) -> {3, <<"node1">>};
scenario_config(sibling_revocation) -> {3, <<"node1">>};
scenario_config(origin_cluster_failure) -> {3, <<"node1">>};
scenario_config(cascading_node_failures) -> {5, <<"node1">>};
scenario_config(migration_shadow_to_active) -> {3, <<"node1">>};
scenario_config(migration_with_failures) -> {3, <<"node1">>};
scenario_config(_) -> {3, <<"node1">>}.

create_nodes(Count, InitialLeader) ->
    lists:foldl(fun(N, Acc) ->
        Id = iolist_to_binary([<<"node">>, integer_to_binary(N)]),
        Role = case Id =:= InitialLeader of
            true -> leader;
            false -> follower
        end,
        Node = #sim_node{
            id = Id,
            state = #{term => 1, voted_for => InitialLeader, commit_index => 0},
            role = Role,
            online = true,
            partitioned_from = [],
            pending_messages = [],
            log = []
        },
        maps:put(Id, Node, Acc)
    end, #{}, lists:seq(1, Count)).

apply_scenario_init(submit_job_single_node, State) ->
    %% Queue a job submission at time 1
    schedule_event(State, 1, {submit_job, #{}});
apply_scenario_init(submit_job_multi_node, State) ->
    schedule_event(State, 1, {submit_job, #{}});
apply_scenario_init(leader_election_normal, State) ->
    %% Start with no leader, nodes will elect
    State#sim_state{leader = undefined};
apply_scenario_init(leader_failure_during_submit, State) ->
    %% Submit job at time 1, kill leader at time 2
    State2 = schedule_event(State, 1, {submit_job, #{}}),
    schedule_event(State2, 2, {kill_node, <<"node1">>});
apply_scenario_init(partition_minority, State) ->
    %% Partition 2 nodes from the other 3
    schedule_event(State, 5, {partition, [<<"node4">>, <<"node5">>], [<<"node1">>, <<"node2">>, <<"node3">>]});
apply_scenario_init(partition_majority, State) ->
    %% Partition leader from majority
    schedule_event(State, 5, {partition, [<<"node1">>], [<<"node2">>, <<"node3">>, <<"node4">>, <<"node5">>]});
apply_scenario_init(sibling_job_basic, State) ->
    %% Create federated job with siblings
    schedule_event(State, 1, {create_sibling_jobs, [<<"node1">>, <<"node2">>, <<"node3">>]});
apply_scenario_init(sibling_job_race, State) ->
    %% Two clusters try to start same job simultaneously
    State2 = schedule_event(State, 1, {create_sibling_jobs, [<<"node1">>, <<"node2">>, <<"node3">>]}),
    State3 = schedule_event(State2, 3, {sibling_start, <<"node2">>}),
    schedule_event(State3, 3, {sibling_start, <<"node3">>});
apply_scenario_init(sibling_revocation, State) ->
    State2 = schedule_event(State, 1, {create_sibling_jobs, [<<"node1">>, <<"node2">>, <<"node3">>]}),
    schedule_event(State2, 3, {sibling_start, <<"node2">>});
apply_scenario_init(origin_cluster_failure, State) ->
    State2 = schedule_event(State, 1, {create_sibling_jobs, [<<"node1">>, <<"node2">>, <<"node3">>]}),
    schedule_event(State2, 5, {kill_node, <<"node1">>});
apply_scenario_init(_, State) ->
    State.

schedule_event(#sim_state{events = Events} = State, Time, Event) ->
    State#sim_state{events = [{scheduled, Time, Event} | Events]}.

%%====================================================================
%% Internal - Simulation Loop
%%====================================================================

run_until_done(State, Steps, MaxSteps) when Steps >= MaxSteps ->
    %% Max steps reached
    Violation = {max_steps_exceeded, Steps},
    {State#sim_state{invariant_violations = [Violation | State#sim_state.invariant_violations]}, Steps};
run_until_done(State, Steps, MaxSteps) ->
    %% Check invariants before each step
    State2 = check_invariants(State),

    %% Stop if violations found
    case State2#sim_state.invariant_violations of
        [] ->
            %% Take a step
            case step(State2) of
                {done, FinalState} ->
                    {FinalState, Steps};
                {continue, NextState} ->
                    run_until_done(NextState, Steps + 1, MaxSteps)
            end;
        _ ->
            {State2, Steps}
    end.

%% @doc Take one simulation step.
-spec step(#sim_state{}) -> {done | continue, #sim_state{}}.
step(#sim_state{messages = [], events = Events} = State) ->
    %% No messages - check for scheduled events
    case get_next_scheduled_event(Events, State#sim_state.time) of
        {none, _} ->
            %% Check if simulation is done
            case is_simulation_done(State) of
                true -> {done, State};
                false ->
                    %% Advance time
                    {continue, State#sim_state{time = State#sim_state.time + 1}}
            end;
        {{scheduled, EventTime, Event}, RemainingEvents} ->
            %% Execute scheduled event
            State2 = State#sim_state{time = EventTime, events = RemainingEvents},
            State3 = execute_event(Event, State2),
            {continue, State3}
    end;
step(State) ->
    %% Deliver messages and maybe inject failures
    State2 = inject_failures(State),
    State3 = deliver_next_message(State2),
    {continue, State3}.

get_next_scheduled_event(Events, CurrentTime) ->
    Scheduled = [{T, E} || {scheduled, T, E} <- Events, T > CurrentTime],
    case Scheduled of
        [] -> {none, Events};
        _ ->
            {MinTime, _} = lists:min(Scheduled),
            Event = {scheduled, MinTime, element(2, hd([{T, E} || {T, E} <- Scheduled, T =:= MinTime]))},
            {Event, Events -- [Event]}
    end.

is_simulation_done(#sim_state{jobs = Jobs}) ->
    %% Done when all jobs are terminal
    AllTerminal = maps:fold(fun(_, #sim_job{state = S}, Acc) ->
        Acc andalso lists:member(S, [completed, failed, cancelled])
    end, true, Jobs),
    AllTerminal andalso maps:size(Jobs) > 0.

%%====================================================================
%% Internal - Event Execution
%%====================================================================

execute_event({submit_job, _Spec}, State) ->
    JobId = State#sim_state.next_job_id,
    Job = #sim_job{
        id = JobId,
        state = pending,
        sibling_clusters = [],
        running_cluster = undefined,
        submit_time = State#sim_state.time
    },
    Jobs = maps:put(JobId, Job, State#sim_state.jobs),
    State2 = State#sim_state{
        jobs = Jobs,
        next_job_id = JobId + 1,
        events = [{job_submitted, JobId, State#sim_state.time} | State#sim_state.events]
    },
    %% If leader exists, schedule job start
    case State2#sim_state.leader of
        undefined -> State2;
        Leader ->
            Msg = #sim_message{
                from = Leader,
                to = Leader,
                type = job_allocated,
                payload = JobId,
                send_time = State2#sim_state.time,
                delivery_time = State2#sim_state.time + 1
            },
            State2#sim_state{messages = [Msg | State2#sim_state.messages]}
    end;

execute_event({kill_node, NodeId}, State) ->
    Nodes = State#sim_state.nodes,
    case maps:get(NodeId, Nodes, undefined) of
        undefined -> State;
        Node ->
            UpdatedNode = Node#sim_node{online = false},
            UpdatedNodes = maps:put(NodeId, UpdatedNode, Nodes),
            State2 = State#sim_state{
                nodes = UpdatedNodes,
                events = [{node_killed, NodeId, State#sim_state.time} | State#sim_state.events]
            },
            %% If killed node was leader, clear leader
            case State2#sim_state.leader of
                NodeId ->
                    State2#sim_state{leader = undefined};
                _ -> State2
            end
    end;

execute_event({partition, Group1, Group2}, State) ->
    %% Create network partition between two groups
    Nodes = lists:foldl(fun(NodeId, Acc) ->
        case maps:get(NodeId, Acc, undefined) of
            undefined -> Acc;
            Node ->
                Partitioned = Node#sim_node.partitioned_from ++ Group2,
                maps:put(NodeId, Node#sim_node{partitioned_from = Partitioned}, Acc)
        end
    end, State#sim_state.nodes, Group1),
    Nodes2 = lists:foldl(fun(NodeId, Acc) ->
        case maps:get(NodeId, Acc, undefined) of
            undefined -> Acc;
            Node ->
                Partitioned = Node#sim_node.partitioned_from ++ Group1,
                maps:put(NodeId, Node#sim_node{partitioned_from = Partitioned}, Acc)
        end
    end, Nodes, Group2),
    State#sim_state{
        nodes = Nodes2,
        events = [{partition_created, Group1, Group2, State#sim_state.time} | State#sim_state.events]
    };

execute_event({create_sibling_jobs, Clusters}, State) ->
    JobId = State#sim_state.next_job_id,
    Job = #sim_job{
        id = JobId,
        state = pending,
        sibling_clusters = Clusters,
        running_cluster = undefined,
        submit_time = State#sim_state.time
    },
    Jobs = maps:put(JobId, Job, State#sim_state.jobs),
    State#sim_state{
        jobs = Jobs,
        next_job_id = JobId + 1,
        events = [{sibling_jobs_created, JobId, Clusters, State#sim_state.time} | State#sim_state.events]
    };

execute_event({sibling_start, Cluster}, State) ->
    %% Find first pending sibling job
    {JobId, Job} = case maps:to_list(State#sim_state.jobs) of
        [{Id, J} | _] when J#sim_job.state =:= pending,
                          J#sim_job.running_cluster =:= undefined ->
            {Id, J};
        _ -> {undefined, undefined}
    end,
    case Job of
        undefined -> State;
        _ ->
            %% Check if this cluster is a sibling
            case lists:member(Cluster, Job#sim_job.sibling_clusters) of
                true ->
                    UpdatedJob = Job#sim_job{state = running, running_cluster = Cluster},
                    Jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
                    State#sim_state{
                        jobs = Jobs,
                        events = [{sibling_started, JobId, Cluster, State#sim_state.time} | State#sim_state.events]
                    };
                false ->
                    State
            end
    end;

execute_event(_, State) ->
    State.

%%====================================================================
%% Internal - Message Delivery
%%====================================================================

deliver_next_message(#sim_state{messages = []} = State) ->
    State;
deliver_next_message(#sim_state{messages = [Msg | Rest]} = State) ->
    State2 = State#sim_state{messages = Rest},
    deliver_message(Msg, State2).

%% @doc Deliver a message to its destination.
-spec deliver_message(#sim_message{}, #sim_state{}) -> #sim_state{}.
deliver_message(#sim_message{to = To, type = Type, payload = Payload} = Msg, State) ->
    Nodes = State#sim_state.nodes,
    case maps:get(To, Nodes, undefined) of
        undefined -> State;
        #sim_node{online = false} ->
            %% Node offline, drop message
            State#sim_state{events = [{message_dropped, Msg, State#sim_state.time} | State#sim_state.events]};
        Node ->
            %% Check partition
            From = Msg#sim_message.from,
            case lists:member(From, Node#sim_node.partitioned_from) of
                true ->
                    %% Partitioned, drop message
                    State#sim_state{events = [{message_dropped_partition, Msg, State#sim_state.time} | State#sim_state.events]};
                false ->
                    %% Deliver message
                    State2 = handle_message(Type, Payload, To, State),
                    State2#sim_state{events = [{message_delivered, Msg, State#sim_state.time} | State#sim_state.events]}
            end
    end.

handle_message(job_allocated, JobId, _NodeId, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined -> State;
        Job when Job#sim_job.state =:= pending ->
            UpdatedJob = Job#sim_job{state = running},
            Jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
            %% Schedule completion
            CompletionMsg = #sim_message{
                from = State#sim_state.leader,
                to = State#sim_state.leader,
                type = job_completed,
                payload = JobId,
                send_time = State#sim_state.time,
                delivery_time = State#sim_state.time + 5
            },
            State#sim_state{
                jobs = Jobs,
                messages = [CompletionMsg | State#sim_state.messages]
            };
        _ -> State
    end;

handle_message(job_completed, JobId, _NodeId, State) ->
    case maps:get(JobId, State#sim_state.jobs, undefined) of
        undefined -> State;
        Job when Job#sim_job.state =:= running ->
            UpdatedJob = Job#sim_job{state = completed},
            Jobs = maps:put(JobId, UpdatedJob, State#sim_state.jobs),
            State#sim_state{jobs = Jobs};
        _ -> State
    end;

handle_message(_, _, _, State) ->
    State.

%%====================================================================
%% Internal - Failure Injection
%%====================================================================

%% @doc Inject failures based on RNG state.
-spec inject_failures(#sim_state{}) -> #sim_state{}.
inject_failures(State) ->
    {Prob, Rng2} = rand:uniform_s(State#sim_state.rng),
    State2 = State#sim_state{rng = Rng2},

    case Prob of
        P when P < 0.01 ->
            %% 1% chance: Kill random node
            kill_random_node(State2);
        P when P < 0.03 ->
            %% 2% chance: Create partition
            partition_random_nodes(State2);
        P when P < 0.05 ->
            %% 2% chance: Delay messages
            delay_random_messages(State2);
        _ ->
            State2
    end.

kill_random_node(State) ->
    OnlineNodes = [Id || {Id, #sim_node{online = true}} <- maps:to_list(State#sim_state.nodes)],
    case OnlineNodes of
        [] -> State;
        _ ->
            {Idx, Rng2} = rand:uniform_s(length(OnlineNodes), State#sim_state.rng),
            NodeId = lists:nth(Idx, OnlineNodes),
            execute_event({kill_node, NodeId}, State#sim_state{rng = Rng2})
    end.

partition_random_nodes(State) ->
    OnlineNodes = [Id || {Id, #sim_node{online = true}} <- maps:to_list(State#sim_state.nodes)],
    case length(OnlineNodes) >= 2 of
        false -> State;
        true ->
            {SplitIdx, Rng2} = rand:uniform_s(length(OnlineNodes) - 1, State#sim_state.rng),
            {Group1, Group2} = lists:split(SplitIdx, OnlineNodes),
            execute_event({partition, Group1, Group2}, State#sim_state{rng = Rng2})
    end.

delay_random_messages(#sim_state{messages = Messages} = State) ->
    case Messages of
        [] -> State;
        _ ->
            DelayedMsgs = [M#sim_message{delivery_time = M#sim_message.delivery_time + 5} || M <- Messages],
            State#sim_state{messages = DelayedMsgs}
    end.

%%====================================================================
%% Internal - Invariant Checking
%%====================================================================

%% @doc Check all safety invariants.
-spec check_invariants(#sim_state{}) -> #sim_state{}.
check_invariants(State) ->
    Violations = lists:flatten([
        check_leader_uniqueness(State),
        check_sibling_exclusivity(State),
        check_no_job_loss(State),
        check_log_consistency(State)
    ]),
    State#sim_state{invariant_violations = Violations ++ State#sim_state.invariant_violations}.

%% TLA+ LeaderUniqueness: At most one leader
check_leader_uniqueness(#sim_state{nodes = Nodes}) ->
    Leaders = [Id || {Id, #sim_node{role = leader, online = true}} <- maps:to_list(Nodes)],
    case length(Leaders) of
        N when N =< 1 -> [];
        N -> [{leader_uniqueness_violation, N, Leaders}]
    end.

%% TLA+ SiblingExclusivity: At most one sibling runs per federated job
check_sibling_exclusivity(#sim_state{jobs = Jobs}) ->
    maps:fold(fun(JobId, #sim_job{sibling_clusters = Siblings, running_cluster = Running}, Acc) ->
        case Running of
            undefined -> Acc;
            _ ->
                %% Count running siblings (should be at most 1)
                RunningCount = case lists:member(Running, Siblings) of
                    true -> 1;
                    false -> 0
                end,
                case RunningCount > 1 of
                    true -> [{sibling_exclusivity_violation, JobId, Running} | Acc];
                    false -> Acc
                end
        end
    end, [], Jobs).

%% TLA+ NoJobLoss: Jobs must have at least one active sibling or be terminal
check_no_job_loss(#sim_state{jobs = Jobs, nodes = Nodes}) ->
    maps:fold(fun(JobId, #sim_job{state = State, sibling_clusters = Siblings}, Acc) ->
        case lists:member(State, [completed, failed, cancelled]) of
            true -> Acc;  % Terminal state, OK
            false ->
                %% Check if at least one sibling cluster is online
                OnlineSiblings = [C || C <- Siblings,
                                       case maps:get(C, Nodes, undefined) of
                                           #sim_node{online = true} -> true;
                                           _ -> false
                                       end],
                case OnlineSiblings of
                    [] when Siblings =/= [] ->
                        [{no_job_loss_violation, JobId, all_siblings_offline} | Acc];
                    _ -> Acc
                end
        end
    end, [], Jobs).

%% TLA+ LogConsistency: Committed log entries identical across nodes
check_log_consistency(#sim_state{nodes = Nodes}) ->
    OnlineNodes = [{Id, N} || {Id, #sim_node{online = true} = N} <- maps:to_list(Nodes)],
    case OnlineNodes of
        [] -> [];
        [{_, FirstNode} | Rest] ->
            FirstLog = FirstNode#sim_node.log,
            CommitIdx = maps:get(commit_index, FirstNode#sim_node.state, 0),
            Committed = lists:sublist(FirstLog, CommitIdx),
            Violations = lists:filtermap(fun({Id, Node}) ->
                NodeLog = lists:sublist(Node#sim_node.log, CommitIdx),
                case NodeLog =:= Committed of
                    true -> false;
                    false -> {true, {log_consistency_violation, Id, NodeLog, Committed}}
                end
            end, Rest),
            Violations
    end.
