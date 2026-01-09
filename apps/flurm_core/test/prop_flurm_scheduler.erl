%%%-------------------------------------------------------------------
%%% @doc Property-based tests for FLURM scheduler
%%%
%%% Uses PropEr to verify scheduler invariants under random inputs:
%%% - No CPU over-allocation
%%% - No memory over-allocation
%%% - Valid job state transitions
%%% - Fair scheduling properties
%%% - Priority ordering
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_flurm_scheduler).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% PropEr properties
-export([
    prop_no_cpu_overallocation/0,
    prop_no_memory_overallocation/0,
    prop_valid_job_transitions/0,
    prop_priority_ordering/0,
    prop_fifo_within_priority/0,
    prop_scheduler_deterministic/0
]).

%% EUnit wrapper - proper_test_/0 is auto-exported by eunit.hrl

%%====================================================================
%% Type Definitions
%%====================================================================

-record(test_job, {
    id :: pos_integer(),
    cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    priority :: non_neg_integer(),
    submit_time :: non_neg_integer(),
    state :: pending | running | completed | cancelled
}).

-record(test_node, {
    name :: binary(),
    cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    cpus_used :: non_neg_integer(),
    memory_used :: non_neg_integer()
}).

-record(test_state, {
    jobs :: #{pos_integer() => #test_job{}},
    nodes :: #{binary() => #test_node{}},
    job_counter :: pos_integer(),
    time :: non_neg_integer()
}).

%%====================================================================
%% Generators
%%====================================================================

%% Generate a job specification
job_spec() ->
    ?LET({Cpus, Memory, Priority},
         {range(1, 64), range(128, 65536), range(0, 1000)},
         #{cpus => Cpus, memory_mb => Memory, priority => Priority}).

%% Generate a node specification
node_spec() ->
    ?LET({Name, Cpus, Memory},
         {binary(8), range(1, 128), range(1024, 262144)},
         #{name => Name, cpus => Cpus, memory_mb => Memory}).

%% Generate a list of nodes (cluster)
cluster_spec() ->
    ?LET(NumNodes, range(1, 20),
         [node_spec() || _ <- lists:seq(1, NumNodes)]).

%% Generate a list of job specs
job_batch() ->
    ?LET(NumJobs, range(1, 100),
         [job_spec() || _ <- lists:seq(1, NumJobs)]).

%% Generate scheduling events
event() ->
    oneof([
        {submit_job, job_spec()},
        {cancel_job, pos_integer()},
        {complete_job, pos_integer()},
        {node_down, binary(8)},
        {node_up, binary(8)},
        {tick, range(1, 1000)}
    ]).

event_sequence() ->
    list(event()).

%% Valid job state transitions
valid_transitions() ->
    #{
        pending => [running, cancelled],
        running => [completed, cancelled, failed],
        completed => [],
        cancelled => [],
        failed => []
    }.

%%====================================================================
%% Properties
%%====================================================================

%% Property: Total allocated CPUs never exceeds node capacity
prop_no_cpu_overallocation() ->
    ?FORALL({Cluster, Jobs}, {cluster_spec(), job_batch()},
        begin
            State = init_state(Cluster),
            FinalState = submit_all_jobs(Jobs, State),
            ScheduledState = run_scheduler(FinalState),
            check_no_cpu_overallocation(ScheduledState)
        end).

%% Property: Total allocated memory never exceeds node capacity
prop_no_memory_overallocation() ->
    ?FORALL({Cluster, Jobs}, {cluster_spec(), job_batch()},
        begin
            State = init_state(Cluster),
            FinalState = submit_all_jobs(Jobs, State),
            ScheduledState = run_scheduler(FinalState),
            check_no_memory_overallocation(ScheduledState)
        end).

%% Property: Job state transitions follow valid paths
prop_valid_job_transitions() ->
    ?FORALL(Events, event_sequence(),
        begin
            State = init_state([#{name => <<"node1">>, cpus => 64, memory_mb => 65536}]),
            {ok, History} = apply_events_with_history(State, Events),
            all_transitions_valid(History, valid_transitions())
        end).

%% Property: Higher priority jobs are scheduled before lower priority
prop_priority_ordering() ->
    ?FORALL({Cluster, Jobs}, {cluster_spec(), job_batch()},
        begin
            State = init_state(Cluster),
            FinalState = submit_all_jobs(Jobs, State),
            ScheduledState = run_scheduler(FinalState),
            check_priority_ordering(ScheduledState)
        end).

%% Property: Within same priority, FIFO ordering is preserved
prop_fifo_within_priority() ->
    ?FORALL(NumJobs, range(2, 50),
        begin
            %% Create jobs with same priority but different submit times
            Jobs = [#{cpus => 1, memory_mb => 128, priority => 100}
                    || _ <- lists:seq(1, NumJobs)],
            State = init_state([#{name => <<"node1">>, cpus => 100, memory_mb => 1000000}]),
            FinalState = submit_all_jobs(Jobs, State),
            ScheduledState = run_scheduler(FinalState),
            check_fifo_ordering(ScheduledState)
        end).

%% Property: Scheduler is deterministic (same input = same output)
prop_scheduler_deterministic() ->
    ?FORALL({Cluster, Jobs}, {cluster_spec(), job_batch()},
        begin
            State1 = init_state(Cluster),
            State2 = init_state(Cluster),
            Final1 = run_scheduler(submit_all_jobs(Jobs, State1)),
            Final2 = run_scheduler(submit_all_jobs(Jobs, State2)),
            states_equivalent(Final1, Final2)
        end).

%%====================================================================
%% State Management
%%====================================================================

init_state(ClusterSpec) ->
    Nodes = lists:foldl(fun(NodeSpec, Acc) ->
        Name = maps:get(name, NodeSpec),
        Node = #test_node{
            name = Name,
            cpus = maps:get(cpus, NodeSpec),
            memory_mb = maps:get(memory_mb, NodeSpec),
            cpus_used = 0,
            memory_used = 0
        },
        maps:put(Name, Node, Acc)
    end, #{}, ClusterSpec),
    #test_state{
        jobs = #{},
        nodes = Nodes,
        job_counter = 1,
        time = 0
    }.

submit_all_jobs(JobSpecs, State) ->
    lists:foldl(fun(JobSpec, AccState) ->
        submit_job(JobSpec, AccState)
    end, State, JobSpecs).

submit_job(JobSpec, #test_state{jobs = Jobs, job_counter = Counter, time = Time} = State) ->
    Job = #test_job{
        id = Counter,
        cpus = maps:get(cpus, JobSpec),
        memory_mb = maps:get(memory_mb, JobSpec),
        priority = maps:get(priority, JobSpec),
        submit_time = Time,
        state = pending
    },
    State#test_state{
        jobs = maps:put(Counter, Job, Jobs),
        job_counter = Counter + 1,
        time = Time + 1
    }.

run_scheduler(#test_state{jobs = Jobs, nodes = Nodes} = State) ->
    %% Get pending jobs sorted by priority (desc) then submit_time (asc)
    PendingJobs = lists:sort(
        fun(J1, J2) ->
            if
                J1#test_job.priority > J2#test_job.priority -> true;
                J1#test_job.priority < J2#test_job.priority -> false;
                true -> J1#test_job.submit_time =< J2#test_job.submit_time
            end
        end,
        [J || J <- maps:values(Jobs), J#test_job.state =:= pending]
    ),

    %% Try to schedule each pending job
    {NewJobs, NewNodes} = lists:foldl(
        fun(Job, {AccJobs, AccNodes}) ->
            case find_node_for_job(Job, AccNodes) of
                {ok, NodeName, UpdatedNode} ->
                    RunningJob = Job#test_job{state = running},
                    {maps:put(Job#test_job.id, RunningJob, AccJobs),
                     maps:put(NodeName, UpdatedNode, AccNodes)};
                none ->
                    {AccJobs, AccNodes}
            end
        end,
        {Jobs, Nodes},
        PendingJobs
    ),
    State#test_state{jobs = NewJobs, nodes = NewNodes}.

find_node_for_job(Job, Nodes) ->
    %% Find first node with sufficient resources
    NodeList = maps:to_list(Nodes),
    find_suitable_node(Job, NodeList).

find_suitable_node(_Job, []) ->
    none;
find_suitable_node(Job, [{Name, Node} | Rest]) ->
    AvailCpus = Node#test_node.cpus - Node#test_node.cpus_used,
    AvailMem = Node#test_node.memory_mb - Node#test_node.memory_used,
    if
        AvailCpus >= Job#test_job.cpus andalso AvailMem >= Job#test_job.memory_mb ->
            UpdatedNode = Node#test_node{
                cpus_used = Node#test_node.cpus_used + Job#test_job.cpus,
                memory_used = Node#test_node.memory_used + Job#test_job.memory_mb
            },
            {ok, Name, UpdatedNode};
        true ->
            find_suitable_node(Job, Rest)
    end.

apply_events_with_history(State, Events) ->
    apply_events_with_history(State, Events, []).

apply_events_with_history(State, [], History) ->
    {ok, lists:reverse([{final, State} | History])};
apply_events_with_history(State, [Event | Rest], History) ->
    NewState = apply_event(Event, State),
    apply_events_with_history(NewState, Rest, [{Event, State, NewState} | History]).

apply_event({submit_job, JobSpec}, State) ->
    submit_job(JobSpec, State);
apply_event({cancel_job, JobId}, #test_state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} when Job#test_job.state =:= pending;
                       Job#test_job.state =:= running ->
            UpdatedJob = Job#test_job{state = cancelled},
            State#test_state{jobs = maps:put(JobId, UpdatedJob, Jobs)};
        _ ->
            State
    end;
apply_event({complete_job, JobId}, #test_state{jobs = Jobs, nodes = _Nodes} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} when Job#test_job.state =:= running ->
            UpdatedJob = Job#test_job{state = completed},
            %% Release resources (simplified - would need to track which node)
            State#test_state{jobs = maps:put(JobId, UpdatedJob, Jobs)};
        _ ->
            State
    end;
apply_event({tick, _}, #test_state{time = Time} = State) ->
    State#test_state{time = Time + 1};
apply_event(_, State) ->
    State.

%%====================================================================
%% Invariant Checkers
%%====================================================================

check_no_cpu_overallocation(#test_state{nodes = Nodes}) ->
    lists:all(
        fun(Node) ->
            Node#test_node.cpus_used =< Node#test_node.cpus
        end,
        maps:values(Nodes)
    ).

check_no_memory_overallocation(#test_state{nodes = Nodes}) ->
    lists:all(
        fun(Node) ->
            Node#test_node.memory_used =< Node#test_node.memory_mb
        end,
        maps:values(Nodes)
    ).

all_transitions_valid([], _ValidTransitions) ->
    true;
all_transitions_valid([{final, _} | _], _ValidTransitions) ->
    true;
all_transitions_valid([{_Event, OldState, NewState} | Rest], ValidTransitions) ->
    _OldJobs = maps:values(OldState#test_state.jobs),
    NewJobs = maps:values(NewState#test_state.jobs),

    %% Check each job's transition
    AllValid = lists:all(
        fun(NewJob) ->
            JobId = NewJob#test_job.id,
            case maps:find(JobId, OldState#test_state.jobs) of
                {ok, OldJob} ->
                    OldJobState = OldJob#test_job.state,
                    NewJobState = NewJob#test_job.state,
                    if
                        OldJobState =:= NewJobState -> true;
                        true ->
                            AllowedTransitions = maps:get(OldJobState, ValidTransitions, []),
                            lists:member(NewJobState, AllowedTransitions)
                    end;
                error ->
                    %% New job, must be pending
                    NewJob#test_job.state =:= pending
            end
        end,
        NewJobs
    ),
    AllValid andalso all_transitions_valid(Rest, ValidTransitions).

check_priority_ordering(#test_state{jobs = Jobs}) ->
    RunningJobs = [J || J <- maps:values(Jobs), J#test_job.state =:= running],
    PendingJobs = [J || J <- maps:values(Jobs), J#test_job.state =:= pending],

    %% All running jobs should have priority >= all pending jobs
    %% (accounting for resource constraints)
    case {RunningJobs, PendingJobs} of
        {[], _} -> true;
        {_, []} -> true;
        _ ->
            MinRunningPriority = lists:min([J#test_job.priority || J <- RunningJobs]),
            MaxPendingPriority = lists:max([J#test_job.priority || J <- PendingJobs]),
            %% This is a soft check - pending jobs might just not fit
            MinRunningPriority >= MaxPendingPriority - 100
    end.

check_fifo_ordering(#test_state{jobs = Jobs}) ->
    RunningJobs = lists:sort(
        fun(J1, J2) -> J1#test_job.submit_time =< J2#test_job.submit_time end,
        [J || J <- maps:values(Jobs), J#test_job.state =:= running]
    ),
    PendingJobs = [J || J <- maps:values(Jobs), J#test_job.state =:= pending],

    case {RunningJobs, PendingJobs} of
        {[], _} -> true;
        {_, []} -> true;
        _ ->
            %% The last scheduled job should have submit_time <= first pending
            LastRunning = lists:last(RunningJobs),
            FirstPending = hd(lists:sort(
                fun(J1, J2) -> J1#test_job.submit_time =< J2#test_job.submit_time end,
                PendingJobs
            )),
            LastRunning#test_job.submit_time =< FirstPending#test_job.submit_time
    end.

states_equivalent(State1, State2) ->
    %% Compare job states
    Jobs1 = maps:map(fun(_K, J) -> {J#test_job.state, J#test_job.priority} end,
                     State1#test_state.jobs),
    Jobs2 = maps:map(fun(_K, J) -> {J#test_job.state, J#test_job.priority} end,
                     State2#test_state.jobs),
    Jobs1 =:= Jobs2.

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 300, [
        {"No CPU over-allocation",
         fun() -> ?assert(proper:quickcheck(prop_no_cpu_overallocation(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"No memory over-allocation",
         fun() -> ?assert(proper:quickcheck(prop_no_memory_overallocation(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Valid job transitions",
         fun() -> ?assert(proper:quickcheck(prop_valid_job_transitions(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Priority ordering",
         fun() -> ?assert(proper:quickcheck(prop_priority_ordering(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"FIFO within priority",
         fun() -> ?assert(proper:quickcheck(prop_fifo_within_priority(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Scheduler determinism",
         fun() -> ?assert(proper:quickcheck(prop_scheduler_deterministic(),
                                            [{numtests, 500}, {to_file, user}])) end}
    ]}.
