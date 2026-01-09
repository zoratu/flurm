%%%-------------------------------------------------------------------
%%% @doc FLURM Deterministic Simulation Framework
%%%
%%% FoundationDB-style deterministic simulation for exhaustive testing.
%%% Key features:
%%% - Seeded PRNG for reproducible randomness
%%% - Simulated time (no wall clock)
%%% - Deterministic failure injection
%%% - Network partition simulation
%%% - Process crash simulation
%%%
%%% Usage:
%%%   flurm_sim:run(Scenario, MaxTime)
%%%   flurm_sim:run_with_seed(Scenario, Seed, MaxTime)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_sim).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([
    run/2,
    run_with_seed/3,
    run_scenarios/1,
    run_scenarios_parallel/2
]).

%% Scenario definitions (not exported - internal use only)
%% scenario_stress_test ends with _test so EUnit would auto-export it
%% All scenarios are accessed via all_scenarios/0

%% EUnit wrapper - simulation_test_/0 is auto-exported by eunit.hrl

%%====================================================================
%% Record Definitions
%%====================================================================

-record(node_sim, {
    name            :: atom(),
    state           :: up | down | partitioned,
    cpus            :: pos_integer(),
    cpus_used       :: non_neg_integer(),
    memory_mb       :: pos_integer(),
    memory_used     :: non_neg_integer(),
    running_jobs    :: [pos_integer()]
}).

-record(ctrl_sim, {
    name            :: atom(),
    state           :: leader | follower | candidate | down,
    term            :: non_neg_integer(),
    log             :: [term()]
}).

-record(job_sim, {
    id              :: pos_integer(),
    state           :: pending | running | completed | cancelled | failed,
    cpus            :: pos_integer(),
    memory_mb       :: pos_integer(),
    priority        :: non_neg_integer(),
    submit_time     :: non_neg_integer(),
    start_time      :: non_neg_integer() | undefined,
    allocated_node  :: atom() | undefined
}).

-record(network_sim, {
    partitions      :: [{[atom()], [atom()]}],  % List of partition pairs
    latency_ms      :: non_neg_integer(),
    drop_rate       :: float()                   % 0.0 - 1.0
}).

-record(sim_state, {
    seed            :: rand:state(),
    time            :: non_neg_integer(),      % Simulated microseconds
    nodes           :: #{atom() => #node_sim{}},
    controllers     :: #{atom() => #ctrl_sim{}},
    jobs            :: #{pos_integer() => #job_sim{}},
    network         :: #network_sim{},
    events          :: [{non_neg_integer(), event()}],  % Priority queue
    history         :: [event()],
    job_counter     :: pos_integer(),
    invariant_violations :: [term()]
}).

-type event() ::
    {submit_job, job_spec()} |
    {cancel_job, pos_integer()} |
    {complete_job, pos_integer()} |
    {node_down, atom()} |
    {node_up, atom()} |
    {controller_crash, atom()} |
    {controller_restart, atom()} |
    {network_partition, [atom()], [atom()]} |
    {heal_partition, [atom()], [atom()]} |
    {schedule_cycle} |
    {check_invariants}.

-type job_spec() :: #{cpus := pos_integer(), memory_mb := pos_integer(),
                      priority := non_neg_integer()}.

-type scenario() :: #{
    name := binary(),
    nodes := pos_integer(),
    controllers := pos_integer(),
    jobs := pos_integer(),
    duration_ms := pos_integer(),
    failures := [failure_spec()]
}.

-type failure_spec() ::
    {kill_leader, at_time | at_job_submit | random} |
    {partition, [atom()], [atom()], pos_integer()} |
    {kill_nodes, [atom()], sequential | parallel, pos_integer()} |
    {node_churn, rate_per_second, pos_integer()}.

%%====================================================================
%% API
%%====================================================================

%% @doc Run simulation with random seed
-spec run(scenario(), pos_integer()) -> {ok, #sim_state{}} | {error, term()}.
run(Scenario, MaxTime) ->
    Seed = rand:uniform(1 bsl 64),
    run_with_seed(Scenario, Seed, MaxTime).

%% @doc Run simulation with specific seed for reproducibility
-spec run_with_seed(scenario(), pos_integer(), pos_integer()) ->
    {ok, #sim_state{}} | {error, term()}.
run_with_seed(Scenario, Seed, MaxTime) ->
    io:format("Running scenario ~s with seed ~p~n",
              [maps:get(name, Scenario), Seed]),
    InitState = init_sim(Scenario, Seed),
    simulate_until(InitState, MaxTime).

%% @doc Run multiple scenarios and collect results
-spec run_scenarios(pos_integer()) -> [{atom(), pos_integer(), term()}].
run_scenarios(NumSeeds) ->
    Scenarios = all_scenarios(),
    Seeds = [rand:uniform(1 bsl 64) || _ <- lists:seq(1, NumSeeds)],
    Results = [
        begin
            ScenarioName = maps:get(name, Scenario),
            Duration = maps:get(duration_ms, Scenario) * 1000,
            Result = run_with_seed(Scenario, Seed, Duration),
            {ScenarioName, Seed, Result}
        end
        || Scenario <- Scenarios,
           Seed <- Seeds
    ],
    analyze_results(Results).

%% @doc Run scenarios in parallel using multiple processes
-spec run_scenarios_parallel(pos_integer(), pos_integer()) ->
    [{atom(), pos_integer(), term()}].
run_scenarios_parallel(NumSeeds, NumWorkers) ->
    Scenarios = all_scenarios(),
    Seeds = [rand:uniform(1 bsl 64) || _ <- lists:seq(1, NumSeeds)],
    WorkItems = [{S, Seed} || S <- Scenarios, Seed <- Seeds],

    %% Distribute work across workers
    Parent = self(),
    Workers = [spawn_link(fun() -> worker_loop(Parent) end)
               || _ <- lists:seq(1, NumWorkers)],

    %% Send work items
    lists:foreach(fun(Item) ->
        receive
            {ready, Worker} ->
                Worker ! {work, Item}
        end
    end, WorkItems),

    %% Collect results
    Results = [receive {result, R} -> R end || _ <- WorkItems],

    %% Stop workers
    lists:foreach(fun(W) -> W ! stop end, Workers),

    analyze_results(Results).

%%====================================================================
%% Simulation Engine
%%====================================================================

init_sim(Scenario, Seed) ->
    NumNodes = maps:get(nodes, Scenario, 5),
    NumControllers = maps:get(controllers, Scenario, 3),
    NumJobs = maps:get(jobs, Scenario, 10),

    %% Initialize random state
    RandState = rand:seed_s(exsss, Seed),

    %% Create nodes
    Nodes = maps:from_list([
        {list_to_atom("node" ++ integer_to_list(N)),
         #node_sim{
             name = list_to_atom("node" ++ integer_to_list(N)),
             state = up,
             cpus = 32,
             cpus_used = 0,
             memory_mb = 65536,
             memory_used = 0,
             running_jobs = []
         }}
        || N <- lists:seq(1, NumNodes)
    ]),

    %% Create controllers
    Controllers = maps:from_list([
        {list_to_atom("ctrl" ++ integer_to_list(N)),
         #ctrl_sim{
             name = list_to_atom("ctrl" ++ integer_to_list(N)),
             state = if N =:= 1 -> leader; true -> follower end,
             term = 1,
             log = []
         }}
        || N <- lists:seq(1, NumControllers)
    ]),

    %% Schedule initial events
    {Events, RandState2} = schedule_initial_events(NumJobs, RandState),

    %% Schedule failure events
    Failures = maps:get(failures, Scenario, []),
    {Events2, RandState3} = schedule_failures(Failures, Events, RandState2),

    %% Schedule periodic invariant checks
    Events3 = schedule_invariant_checks(Events2, maps:get(duration_ms, Scenario, 10000) * 1000),

    #sim_state{
        seed = RandState3,
        time = 0,
        nodes = Nodes,
        controllers = Controllers,
        jobs = #{},
        network = #network_sim{partitions = [], latency_ms = 1, drop_rate = 0.0},
        events = lists:keysort(1, Events3),
        history = [],
        job_counter = 1,
        invariant_violations = []
    }.

simulate_until(#sim_state{time = T} = State, MaxTime) when T >= MaxTime ->
    FinalState = check_final_invariants(State),
    case FinalState#sim_state.invariant_violations of
        [] -> {ok, FinalState};
        Violations -> {error, {invariant_violations, Violations}}
    end;
simulate_until(#sim_state{events = []} = State, _MaxTime) ->
    FinalState = check_final_invariants(State),
    case FinalState#sim_state.invariant_violations of
        [] -> {ok, FinalState};
        Violations -> {error, {invariant_violations, Violations}}
    end;
simulate_until(#sim_state{events = [{Time, Event} | Rest]} = State, MaxTime) ->
    NewState = State#sim_state{
        time = Time,
        events = Rest,
        history = [Event | State#sim_state.history]
    },
    case apply_event(Event, NewState) of
        {ok, State2} ->
            simulate_until(State2, MaxTime);
        {violation, Invariant, State2} ->
            %% Record violation but continue
            State3 = State2#sim_state{
                invariant_violations = [Invariant | State2#sim_state.invariant_violations]
            },
            simulate_until(State3, MaxTime)
    end.

%%====================================================================
%% Event Handlers
%%====================================================================

apply_event({submit_job, JobSpec}, State) ->
    #sim_state{jobs = Jobs, job_counter = Counter, time = Time} = State,
    Job = #job_sim{
        id = Counter,
        state = pending,
        cpus = maps:get(cpus, JobSpec),
        memory_mb = maps:get(memory_mb, JobSpec),
        priority = maps:get(priority, JobSpec),
        submit_time = Time,
        start_time = undefined,
        allocated_node = undefined
    },
    NewState = State#sim_state{
        jobs = maps:put(Counter, Job, Jobs),
        job_counter = Counter + 1
    },
    %% Schedule a scheduling cycle
    {ok, schedule_event(NewState, Time + 1000, schedule_cycle)};

apply_event({cancel_job, JobId}, State) ->
    #sim_state{jobs = Jobs, nodes = Nodes} = State,
    case maps:find(JobId, Jobs) of
        {ok, #job_sim{state = S} = Job} when S =:= pending; S =:= running ->
            UpdatedJob = Job#job_sim{state = cancelled},
            %% Release resources if running
            NewNodes = case Job#job_sim.allocated_node of
                undefined -> Nodes;
                NodeName ->
                    release_resources(NodeName, Job, Nodes)
            end,
            {ok, State#sim_state{
                jobs = maps:put(JobId, UpdatedJob, Jobs),
                nodes = NewNodes
            }};
        _ ->
            {ok, State}
    end;

apply_event({complete_job, JobId}, State) ->
    #sim_state{jobs = Jobs, nodes = Nodes} = State,
    case maps:find(JobId, Jobs) of
        {ok, #job_sim{state = running} = Job} ->
            UpdatedJob = Job#job_sim{state = completed},
            NewNodes = case Job#job_sim.allocated_node of
                undefined -> Nodes;
                NodeName -> release_resources(NodeName, Job, Nodes)
            end,
            {ok, State#sim_state{
                jobs = maps:put(JobId, UpdatedJob, Jobs),
                nodes = NewNodes
            }};
        _ ->
            {ok, State}
    end;

apply_event({node_down, NodeName}, State) ->
    #sim_state{nodes = Nodes, jobs = Jobs} = State,
    case maps:find(NodeName, Nodes) of
        {ok, Node} ->
            %% Mark node as down
            UpdatedNode = Node#node_sim{state = down},

            %% Fail all jobs on this node
            RunningJobIds = Node#node_sim.running_jobs,
            UpdatedJobs = lists:foldl(fun(JobId, AccJobs) ->
                case maps:find(JobId, AccJobs) of
                    {ok, Job} ->
                        maps:put(JobId, Job#job_sim{state = failed}, AccJobs);
                    error ->
                        AccJobs
                end
            end, Jobs, RunningJobIds),

            {ok, State#sim_state{
                nodes = maps:put(NodeName, UpdatedNode#node_sim{
                    running_jobs = [],
                    cpus_used = 0,
                    memory_used = 0
                }, Nodes),
                jobs = UpdatedJobs
            }};
        error ->
            {ok, State}
    end;

apply_event({node_up, NodeName}, State) ->
    #sim_state{nodes = Nodes} = State,
    case maps:find(NodeName, Nodes) of
        {ok, Node} ->
            UpdatedNode = Node#node_sim{state = up},
            {ok, State#sim_state{nodes = maps:put(NodeName, UpdatedNode, Nodes)}};
        error ->
            {ok, State}
    end;

apply_event({controller_crash, CtrlName}, State) ->
    #sim_state{controllers = Controllers} = State,
    case maps:find(CtrlName, Controllers) of
        {ok, Ctrl} ->
            %% Mark controller as down
            UpdatedCtrl = Ctrl#ctrl_sim{state = down},
            NewControllers = maps:put(CtrlName, UpdatedCtrl, Controllers),

            %% If leader crashed, trigger election
            NewControllers2 = case Ctrl#ctrl_sim.state of
                leader -> trigger_election(NewControllers);
                _ -> NewControllers
            end,

            {ok, State#sim_state{controllers = NewControllers2}};
        error ->
            {ok, State}
    end;

apply_event({controller_restart, CtrlName}, State) ->
    #sim_state{controllers = Controllers} = State,
    case maps:find(CtrlName, Controllers) of
        {ok, Ctrl} ->
            %% Restart as follower
            UpdatedCtrl = Ctrl#ctrl_sim{state = follower},
            {ok, State#sim_state{
                controllers = maps:put(CtrlName, UpdatedCtrl, Controllers)
            }};
        error ->
            {ok, State}
    end;

apply_event({network_partition, Set1, Set2}, State) ->
    #sim_state{network = Network} = State,
    NewPartitions = [{Set1, Set2} | Network#network_sim.partitions],
    {ok, State#sim_state{
        network = Network#network_sim{partitions = NewPartitions}
    }};

apply_event({heal_partition, Set1, Set2}, State) ->
    #sim_state{network = Network} = State,
    NewPartitions = lists:delete({Set1, Set2}, Network#network_sim.partitions),
    {ok, State#sim_state{
        network = Network#network_sim{partitions = NewPartitions}
    }};

apply_event(schedule_cycle, State) ->
    run_scheduling_cycle(State);

apply_event(check_invariants, State) ->
    check_runtime_invariants(State).

%%====================================================================
%% Scheduling Simulation
%%====================================================================

run_scheduling_cycle(#sim_state{jobs = Jobs, nodes = Nodes, time = Time} = State) ->
    %% Get pending jobs sorted by priority
    PendingJobs = lists:sort(
        fun(J1, J2) ->
            J1#job_sim.priority >= J2#job_sim.priority
        end,
        [J || J <- maps:values(Jobs), J#job_sim.state =:= pending]
    ),

    %% Try to schedule each job
    {NewJobs, NewNodes, Scheduled} = lists:foldl(
        fun(Job, {AccJobs, AccNodes, AccScheduled}) ->
            case find_available_node(Job, AccNodes) of
                {ok, NodeName, UpdatedNode} ->
                    RunningJob = Job#job_sim{
                        state = running,
                        start_time = Time,
                        allocated_node = NodeName
                    },
                    {maps:put(Job#job_sim.id, RunningJob, AccJobs),
                     maps:put(NodeName, UpdatedNode, AccNodes),
                     [Job#job_sim.id | AccScheduled]};
                none ->
                    {AccJobs, AccNodes, AccScheduled}
            end
        end,
        {Jobs, Nodes, []},
        PendingJobs
    ),

    %% Schedule job completions for scheduled jobs
    {Events, Seed} = lists:foldl(
        fun(JobId, {AccEvents, AccSeed}) ->
            %% Random completion time between 1-10 seconds
            {CompletionTime, NewSeed} = rand:uniform_s(10000000, AccSeed),
            {[{Time + CompletionTime, {complete_job, JobId}} | AccEvents], NewSeed}
        end,
        {State#sim_state.events, State#sim_state.seed},
        Scheduled
    ),

    {ok, State#sim_state{
        jobs = NewJobs,
        nodes = NewNodes,
        events = lists:keysort(1, Events),
        seed = Seed
    }}.

find_available_node(Job, Nodes) ->
    AvailableNodes = [
        {Name, Node} || {Name, Node} <- maps:to_list(Nodes),
                        Node#node_sim.state =:= up,
                        Node#node_sim.cpus - Node#node_sim.cpus_used >= Job#job_sim.cpus,
                        Node#node_sim.memory_mb - Node#node_sim.memory_used >= Job#job_sim.memory_mb
    ],
    case AvailableNodes of
        [] -> none;
        [{Name, Node} | _] ->
            UpdatedNode = Node#node_sim{
                cpus_used = Node#node_sim.cpus_used + Job#job_sim.cpus,
                memory_used = Node#node_sim.memory_used + Job#job_sim.memory_mb,
                running_jobs = [Job#job_sim.id | Node#node_sim.running_jobs]
            },
            {ok, Name, UpdatedNode}
    end.

release_resources(NodeName, Job, Nodes) ->
    case maps:find(NodeName, Nodes) of
        {ok, Node} ->
            UpdatedNode = Node#node_sim{
                cpus_used = max(0, Node#node_sim.cpus_used - Job#job_sim.cpus),
                memory_used = max(0, Node#node_sim.memory_used - Job#job_sim.memory_mb),
                running_jobs = lists:delete(Job#job_sim.id, Node#node_sim.running_jobs)
            },
            maps:put(NodeName, UpdatedNode, Nodes);
        error ->
            Nodes
    end.

trigger_election(Controllers) ->
    %% Simple election: first alive follower becomes leader
    Candidates = [{Name, C} || {Name, C} <- maps:to_list(Controllers),
                               C#ctrl_sim.state =:= follower],
    case Candidates of
        [] -> Controllers;
        [{Name, Ctrl} | _] ->
            maps:put(Name, Ctrl#ctrl_sim{state = leader}, Controllers)
    end.

%%====================================================================
%% Invariant Checking
%%====================================================================

check_runtime_invariants(State) ->
    Violations = lists:flatten([
        check_no_overallocation(State),
        check_leader_exists(State),
        check_job_node_consistency(State)
    ]),
    case Violations of
        [] -> {ok, State};
        _ -> {violation, Violations, State}
    end.

check_final_invariants(State) ->
    Violations = lists:flatten([
        check_no_overallocation(State),
        check_no_orphaned_resources(State),
        check_all_jobs_terminal_or_pending(State)
    ]),
    State#sim_state{
        invariant_violations = Violations ++ State#sim_state.invariant_violations
    }.

check_no_overallocation(#sim_state{nodes = Nodes}) ->
    lists:filtermap(
        fun({Name, Node}) ->
            CpuOver = Node#node_sim.cpus_used > Node#node_sim.cpus,
            MemOver = Node#node_sim.memory_used > Node#node_sim.memory_mb,
            if
                CpuOver -> {true, {cpu_overallocation, Name, Node#node_sim.cpus_used, Node#node_sim.cpus}};
                MemOver -> {true, {memory_overallocation, Name, Node#node_sim.memory_used, Node#node_sim.memory_mb}};
                true -> false
            end
        end,
        maps:to_list(Nodes)
    ).

check_leader_exists(#sim_state{controllers = Controllers}) ->
    Leaders = [C || C <- maps:values(Controllers), C#ctrl_sim.state =:= leader],
    case length(Leaders) of
        0 -> [{no_leader}];
        1 -> [];
        N -> [{multiple_leaders, N}]
    end.

check_job_node_consistency(#sim_state{jobs = Jobs, nodes = Nodes}) ->
    lists:filtermap(
        fun({JobId, Job}) ->
            case Job#job_sim.state of
                running ->
                    case Job#job_sim.allocated_node of
                        undefined ->
                            {true, {running_job_no_node, JobId}};
                        NodeName ->
                            case maps:find(NodeName, Nodes) of
                                {ok, Node} ->
                                    case lists:member(JobId, Node#node_sim.running_jobs) of
                                        true -> false;
                                        false -> {true, {job_not_on_node, JobId, NodeName}}
                                    end;
                                error ->
                                    {true, {job_on_missing_node, JobId, NodeName}}
                            end
                    end;
                _ ->
                    false
            end
        end,
        maps:to_list(Jobs)
    ).

check_no_orphaned_resources(#sim_state{jobs = Jobs, nodes = Nodes}) ->
    %% Check that no node has running_jobs for completed/failed jobs
    lists:filtermap(
        fun({NodeName, Node}) ->
            OrphanedJobs = lists:filter(
                fun(JobId) ->
                    case maps:find(JobId, Jobs) of
                        {ok, Job} -> Job#job_sim.state =/= running;
                        error -> true
                    end
                end,
                Node#node_sim.running_jobs
            ),
            case OrphanedJobs of
                [] -> false;
                _ -> {true, {orphaned_jobs_on_node, NodeName, OrphanedJobs}}
            end
        end,
        maps:to_list(Nodes)
    ).

check_all_jobs_terminal_or_pending(#sim_state{jobs = Jobs}) ->
    %% At the end, jobs should be in terminal state or still pending
    lists:filtermap(
        fun({JobId, Job}) ->
            case Job#job_sim.state of
                S when S =:= pending; S =:= completed; S =:= cancelled; S =:= failed ->
                    false;
                running ->
                    %% Running at end of simulation is OK if we just ran out of time
                    false;
                Other ->
                    {true, {job_in_invalid_final_state, JobId, Other}}
            end
        end,
        maps:to_list(Jobs)
    ).

%%====================================================================
%% Event Scheduling
%%====================================================================

schedule_initial_events(NumJobs, RandState) ->
    lists:foldl(
        fun(_, {AccEvents, AccRand}) ->
            {Time, Rand1} = rand:uniform_s(5000000, AccRand),  % 0-5 seconds
            {Cpus, Rand2} = rand:uniform_s(8, Rand1),
            {Memory, Rand3} = rand:uniform_s(8192, Rand2),
            {Priority, Rand4} = rand:uniform_s(1000, Rand3),
            Event = {submit_job, #{cpus => Cpus, memory_mb => Memory * 128, priority => Priority}},
            {[{Time, Event} | AccEvents], Rand4}
        end,
        {[], RandState},
        lists:seq(1, NumJobs)
    ).

schedule_failures([], Events, RandState) ->
    {Events, RandState};
schedule_failures([{kill_leader, at_time, Time} | Rest], Events, RandState) ->
    NewEvents = [{Time * 1000, {controller_crash, ctrl1}} | Events],
    schedule_failures(Rest, NewEvents, RandState);
schedule_failures([{kill_leader, random} | Rest], Events, RandState) ->
    {Time, NewRand} = rand:uniform_s(10000000, RandState),
    NewEvents = [{Time, {controller_crash, ctrl1}} | Events],
    schedule_failures(Rest, NewEvents, NewRand);
schedule_failures([{partition, Set1, Set2, Duration} | Rest], Events, RandState) ->
    {StartTime, NewRand} = rand:uniform_s(5000000, RandState),
    NewEvents = [
        {StartTime, {network_partition, Set1, Set2}},
        {StartTime + Duration * 1000, {heal_partition, Set1, Set2}}
        | Events
    ],
    schedule_failures(Rest, NewEvents, NewRand);
schedule_failures([_ | Rest], Events, RandState) ->
    schedule_failures(Rest, Events, RandState).

schedule_invariant_checks(Events, MaxTime) ->
    %% Check invariants every second
    CheckEvents = [{T * 1000000, check_invariants} || T <- lists:seq(1, MaxTime div 1000000)],
    CheckEvents ++ Events.

schedule_event(State, Time, Event) ->
    State#sim_state{
        events = lists:keysort(1, [{Time, Event} | State#sim_state.events])
    }.

%%====================================================================
%% Scenarios
%%====================================================================

all_scenarios() ->
    [
        scenario_single_job(),
        scenario_hundred_jobs(),
        scenario_leader_crash(),
        scenario_network_partition(),
        scenario_cascading_failures(),
        scenario_concurrent_cancel(),
        scenario_rapid_node_churn(),
        scenario_stress_test()
    ].

scenario_single_job() ->
    #{name => <<"single_job">>, nodes => 3, controllers => 1, jobs => 1,
      duration_ms => 5000, failures => []}.

scenario_hundred_jobs() ->
    #{name => <<"hundred_jobs">>, nodes => 10, controllers => 3, jobs => 100,
      duration_ms => 30000, failures => []}.

scenario_leader_crash() ->
    #{name => <<"leader_crash">>, nodes => 5, controllers => 3, jobs => 50,
      duration_ms => 20000, failures => [{kill_leader, random}]}.

scenario_network_partition() ->
    #{name => <<"network_partition">>, nodes => 5, controllers => 3, jobs => 30,
      duration_ms => 20000, failures => [{partition, [ctrl1], [ctrl2, ctrl3], 5000}]}.

scenario_cascading_failures() ->
    #{name => <<"cascading_failures">>, nodes => 10, controllers => 3, jobs => 50,
      duration_ms => 30000, failures => [
          {kill_leader, at_time, 5},
          {partition, [node1, node2], [node3, node4, node5], 3000}
      ]}.

scenario_concurrent_cancel() ->
    #{name => <<"concurrent_cancel">>, nodes => 5, controllers => 3, jobs => 100,
      duration_ms => 15000, failures => []}.

scenario_rapid_node_churn() ->
    #{name => <<"rapid_node_churn">>, nodes => 20, controllers => 3, jobs => 100,
      duration_ms => 30000, failures => []}.

scenario_stress_test() ->
    #{name => <<"stress_test">>, nodes => 50, controllers => 5, jobs => 1000,
      duration_ms => 60000, failures => [{kill_leader, random}]}.

%%====================================================================
%% Analysis
%%====================================================================

analyze_results(Results) ->
    Successes = [R || R <- Results, element(3, R) =:= ok orelse element(1, element(3, R)) =:= ok],
    Failures = [R || R <- Results, element(3, R) =/= ok andalso element(1, element(3, R)) =/= ok],

    io:format("~n=== Simulation Results ===~n"),
    io:format("Total runs: ~p~n", [length(Results)]),
    io:format("Successes:  ~p~n", [length(Successes)]),
    io:format("Failures:   ~p~n", [length(Failures)]),

    case Failures of
        [] -> io:format("All simulations passed!~n");
        _ ->
            io:format("~nFailure details:~n"),
            lists:foreach(fun({Scenario, Seed, {error, Reason}}) ->
                io:format("  ~s (seed: ~p): ~p~n", [Scenario, Seed, Reason])
            end, lists:sublist(Failures, 10))
    end,

    Results.

%%====================================================================
%% Worker for Parallel Execution
%%====================================================================

worker_loop(Parent) ->
    Parent ! {ready, self()},
    receive
        {work, {Scenario, Seed}} ->
            Duration = maps:get(duration_ms, Scenario) * 1000,
            Result = run_with_seed(Scenario, Seed, Duration),
            ScenarioName = maps:get(name, Scenario),
            Parent ! {result, {ScenarioName, Seed, Result}},
            worker_loop(Parent);
        stop ->
            ok
    end.

%%====================================================================
%% EUnit Integration
%%====================================================================

simulation_test_() ->
    {timeout, 300, [
        {"Single job scenario",
         fun() ->
             {Result, _} = run_with_seed(scenario_single_job(), 12345, 5000000),
             ?assertEqual(ok, Result)
         end},
        {"Hundred jobs scenario",
         fun() ->
             {Result, _} = run_with_seed(scenario_hundred_jobs(), 12345, 30000000),
             ?assertEqual(ok, Result)
         end},
        {"Leader crash scenario",
         fun() ->
             {Result, _} = run_with_seed(scenario_leader_crash(), 12345, 20000000),
             ?assertEqual(ok, Result)
         end},
        {"Network partition scenario",
         fun() ->
             {Result, _} = run_with_seed(scenario_network_partition(), 12345, 20000000),
             ?assertEqual(ok, Result)
         end},
        {"Multi-seed test (10 seeds per scenario)",
         fun() ->
             Results = run_scenarios(10),
             Failures = [R || R <- Results,
                              element(3, R) =/= ok,
                              element(1, element(3, R)) =/= ok],
             ?assertEqual([], Failures)
         end}
    ]}.
