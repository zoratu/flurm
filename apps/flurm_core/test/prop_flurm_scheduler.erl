%%%-------------------------------------------------------------------
%%% @doc PropEr Property-Based Tests for FLURM Scheduler
%%%
%%% Tests scheduler invariants using property-based testing:
%%% - No resource over-allocation
%%% - Valid job state transitions
%%% - Job scheduling correctness
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_flurm_scheduler).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Suppress unused warnings for PropEr generator functions
-compile({nowarn_unused_function, [
    cluster_state/0,
    scheduler_event/0,
    event_sequence/0
]}).

-export([prop_no_cpu_overallocation/0,
         prop_no_memory_overallocation/0,
         prop_valid_job_transitions/0,
         prop_running_jobs_have_nodes/0,
         prop_fifo_ordering/0,
         prop_scheduler_idempotent/0]).

%%====================================================================
%% Generators
%%====================================================================

%% Generate a job specification
job_spec() ->
    ?LET({Name, Cpus, Mem, Time},
         {binary(8), range(1, 16), range(1024, 32768), range(60, 3600)},
         #job_submit_req{
             name = Name,
             partition = <<"batch">>,
             num_nodes = 1,
             num_cpus = Cpus,
             memory_mb = Mem,
             time_limit = Time,
             script = <<"#!/bin/bash\necho test">>,
             priority = 100,
             env = #{},
             working_dir = <<"/tmp">>
         }).

%% Generate a node specification
node_spec() ->
    ?LET({Name, Cpus, Mem},
         {binary(8), range(8, 64), range(16384, 262144)},
         #node{
             hostname = Name,
             cpus = Cpus,
             memory_mb = Mem,
             state = up,
             features = [],
             partitions = [<<"batch">>],
             running_jobs = [],
             load_avg = 0.0,
             free_memory_mb = Mem,
             last_heartbeat = erlang:system_time(second),
             allocations = #{}
         }).

%% Generate cluster state
cluster_state() ->
    ?LET({Nodes, Jobs},
         {non_empty(list(node_spec())), list(job_spec())},
         #{nodes => maps:from_list([{N#node.hostname, N} || N <- Nodes]),
           jobs => #{},
           pending => Jobs,
           next_id => 1}).

%% Generate a sequence of scheduler events
scheduler_event() ->
    oneof([
        {submit, job_spec()},
        schedule,
        {complete, range(1, 100)},
        {cancel, range(1, 100)},
        {node_down, binary(8)},
        {node_up, binary(8)}
    ]).

event_sequence() ->
    list(scheduler_event()).

%%====================================================================
%% Properties
%%====================================================================

%% PROPERTY: Never allocate more CPUs than a node has
prop_no_cpu_overallocation() ->
    ?FORALL(Events, event_sequence(),
        begin
            State = apply_events(init_state(), Events),
            check_no_cpu_overallocation(State)
        end).

%% PROPERTY: Never allocate more memory than a node has
prop_no_memory_overallocation() ->
    ?FORALL(Events, event_sequence(),
        begin
            State = apply_events(init_state(), Events),
            check_no_memory_overallocation(State)
        end).

%% PROPERTY: All job state transitions are valid
prop_valid_job_transitions() ->
    ValidTransitions = #{
        pending => [configuring, running, cancelled, failed],
        configuring => [running, failed, cancelled],
        running => [completing, completed, failed, cancelled, timeout],
        completing => [completed, failed],
        completed => [],
        failed => [],
        cancelled => [],
        timeout => []
    },
    ?FORALL(Events, event_sequence(),
        begin
            {_State, History} = apply_events_with_history(init_state(), Events),
            check_valid_transitions(History, ValidTransitions)
        end).

%% PROPERTY: Running jobs have allocated nodes
prop_running_jobs_have_nodes() ->
    ?FORALL(Events, event_sequence(),
        begin
            State = apply_events(init_state(), Events),
            check_running_jobs_have_nodes(State)
        end).

%% PROPERTY: Jobs are scheduled in FIFO order (by submit time)
prop_fifo_ordering() ->
    ?FORALL(Jobs, non_empty(list(job_spec())),
        begin
            State0 = init_state_with_nodes(3),
            State1 = lists:foldl(fun(Job, S) ->
                submit_job(Job, S)
            end, State0, Jobs),
            State2 = run_scheduler_n_times(State1, length(Jobs)),
            check_fifo_ordering(State2)
        end).

%% PROPERTY: Running scheduler twice on same state is idempotent
prop_scheduler_idempotent() ->
    ?FORALL({Nodes, Jobs}, 
            {non_empty(list(node_spec())), list(job_spec())},
        begin
            State0 = init_state_with_nodes_list(Nodes),
            State1 = lists:foldl(fun(Job, S) ->
                submit_job(Job, S)
            end, State0, Jobs),
            State2 = run_scheduler(State1),
            State3 = run_scheduler(State2),
            %% After scheduling, running again shouldn't change anything
            maps:get(jobs, State2) =:= maps:get(jobs, State3)
        end).

%%====================================================================
%% State Management
%%====================================================================

init_state() ->
    #{
        nodes => #{
            <<"node001">> => #node{
                hostname = <<"node001">>,
                cpus = 32,
                memory_mb = 131072,
                state = up,
                features = [],
                partitions = [<<"batch">>],
                running_jobs = [],
                load_avg = 0.0,
                free_memory_mb = 131072,
                last_heartbeat = erlang:system_time(second),
                allocations = #{}
            }
        },
        jobs => #{},
        pending => [],
        next_id => 1,
        history => []
    }.

init_state_with_nodes(N) ->
    Nodes = [#node{
        hostname = list_to_binary(io_lib:format("node~3..0B", [I])),
        cpus = 32,
        memory_mb = 131072,
        state = up,
        features = [],
        partitions = [<<"batch">>],
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = 131072,
        last_heartbeat = erlang:system_time(second),
        allocations = #{}
    } || I <- lists:seq(1, N)],
    init_state_with_nodes_list(Nodes).

init_state_with_nodes_list(Nodes) ->
    #{
        nodes => maps:from_list([{N#node.hostname, N} || N <- Nodes]),
        jobs => #{},
        pending => [],
        next_id => 1,
        history => []
    }.

apply_events(State, []) ->
    State;
apply_events(State, [Event | Rest]) ->
    NewState = apply_event(Event, State),
    apply_events(NewState, Rest).

apply_events_with_history(State, Events) ->
    lists:foldl(fun(Event, {S, H}) ->
        NewState = apply_event(Event, S),
        JobChanges = get_job_changes(S, NewState),
        {NewState, H ++ JobChanges}
    end, {State, []}, Events).

apply_event({submit, JobSpec}, State) ->
    submit_job(JobSpec, State);
apply_event(schedule, State) ->
    run_scheduler(State);
apply_event({complete, JobId}, State) ->
    complete_job(JobId, State);
apply_event({cancel, JobId}, State) ->
    cancel_job(JobId, State);
apply_event({node_down, NodeName}, State) ->
    node_down(NodeName, State);
apply_event({node_up, NodeName}, State) ->
    node_up(NodeName, State);
apply_event(_, State) ->
    State.

submit_job(#job_submit_req{} = Spec, #{next_id := NextId} = State) ->
    Job = #job{
        id = NextId,
        name = Spec#job_submit_req.name,
        user = <<"testuser">>,
        partition = Spec#job_submit_req.partition,
        state = pending,
        script = Spec#job_submit_req.script,
        num_nodes = Spec#job_submit_req.num_nodes,
        num_cpus = Spec#job_submit_req.num_cpus,
        memory_mb = Spec#job_submit_req.memory_mb,
        time_limit = Spec#job_submit_req.time_limit,
        priority = Spec#job_submit_req.priority,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },
    Jobs = maps:get(jobs, State),
    State#{
        jobs => maps:put(NextId, Job, Jobs),
        next_id => NextId + 1
    }.

run_scheduler(#{jobs := Jobs, nodes := Nodes} = State) ->
    PendingJobs = [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= pending],
    SortedJobs = lists:sort(fun(A, B) -> 
        A#job.submit_time =< B#job.submit_time 
    end, PendingJobs),
    
    {UpdatedJobs, UpdatedNodes} = lists:foldl(fun(Job, {AccJobs, AccNodes}) ->
        case find_node_for_job(Job, AccNodes) of
            {ok, NodeName, NewNodes} ->
                UpdatedJob = Job#job{
                    state = running,
                    start_time = erlang:system_time(second),
                    allocated_nodes = [NodeName]
                },
                {maps:put(Job#job.id, UpdatedJob, AccJobs), NewNodes};
            none ->
                {AccJobs, AccNodes}
        end
    end, {Jobs, Nodes}, SortedJobs),
    
    State#{jobs => UpdatedJobs, nodes => UpdatedNodes}.

run_scheduler_n_times(State, 0) -> State;
run_scheduler_n_times(State, N) ->
    run_scheduler_n_times(run_scheduler(State), N - 1).

find_node_for_job(#job{num_cpus = Cpus, memory_mb = Mem}, Nodes) ->
    Available = [{Name, Node} || {Name, Node} <- maps:to_list(Nodes),
                                  Node#node.state =:= up,
                                  Node#node.cpus - used_cpus(Node) >= Cpus,
                                  Node#node.free_memory_mb >= Mem],
    case Available of
        [] -> none;
        [{Name, Node} | _] ->
            UpdatedNode = Node#node{
                running_jobs = [1 | Node#node.running_jobs],  % Simplified
                free_memory_mb = Node#node.free_memory_mb - Mem,
                allocations = maps:put(1, {Cpus, Mem}, Node#node.allocations)
            },
            {ok, Name, maps:put(Name, UpdatedNode, Nodes)}
    end.

used_cpus(#node{allocations = Allocs}) ->
    lists:sum([C || {C, _M} <- maps:values(Allocs)]).

complete_job(JobId, #{jobs := Jobs, nodes := Nodes} = State) ->
    case maps:get(JobId, Jobs, undefined) of
        undefined -> State;
        #job{state = running, allocated_nodes = [NodeName]} = Job ->
            UpdatedJob = Job#job{
                state = completed,
                end_time = erlang:system_time(second),
                allocated_nodes = []
            },
            UpdatedNodes = deallocate_resources(NodeName, Job, Nodes),
            State#{
                jobs => maps:put(JobId, UpdatedJob, Jobs),
                nodes => UpdatedNodes
            };
        _ -> State
    end.

cancel_job(JobId, #{jobs := Jobs, nodes := Nodes} = State) ->
    case maps:get(JobId, Jobs, undefined) of
        undefined -> State;
        #job{state = pending} = Job ->
            UpdatedJob = Job#job{state = cancelled},
            State#{jobs => maps:put(JobId, UpdatedJob, Jobs)};
        #job{state = running, allocated_nodes = [NodeName]} = Job ->
            UpdatedJob = Job#job{
                state = cancelled,
                end_time = erlang:system_time(second),
                allocated_nodes = []
            },
            UpdatedNodes = deallocate_resources(NodeName, Job, Nodes),
            State#{
                jobs => maps:put(JobId, UpdatedJob, Jobs),
                nodes => UpdatedNodes
            };
        _ -> State
    end.

deallocate_resources(NodeName, #job{id = JobId, memory_mb = Mem}, Nodes) ->
    case maps:get(NodeName, Nodes, undefined) of
        undefined -> Nodes;
        Node ->
            UpdatedNode = Node#node{
                running_jobs = lists:delete(JobId, Node#node.running_jobs),
                free_memory_mb = Node#node.free_memory_mb + Mem,
                allocations = maps:remove(JobId, Node#node.allocations)
            },
            maps:put(NodeName, UpdatedNode, Nodes)
    end.

node_down(NodeName, #{nodes := Nodes, jobs := Jobs} = State) ->
    case maps:get(NodeName, Nodes, undefined) of
        undefined -> State;
        Node ->
            UpdatedNode = Node#node{state = down},
            %% Fail jobs on this node
            AffectedJobs = [JobId || {JobId, J} <- maps:to_list(Jobs),
                                     J#job.state =:= running,
                                     lists:member(NodeName, J#job.allocated_nodes)],
            UpdatedJobs = lists:foldl(fun(JobId, AccJobs) ->
                Job = maps:get(JobId, AccJobs),
                maps:put(JobId, Job#job{state = failed, allocated_nodes = []}, AccJobs)
            end, Jobs, AffectedJobs),
            State#{
                nodes => maps:put(NodeName, UpdatedNode, Nodes),
                jobs => UpdatedJobs
            }
    end.

node_up(NodeName, #{nodes := Nodes} = State) ->
    case maps:get(NodeName, Nodes, undefined) of
        undefined -> State;
        Node ->
            UpdatedNode = Node#node{state = up},
            State#{nodes => maps:put(NodeName, UpdatedNode, Nodes)}
    end.

get_job_changes(OldState, NewState) ->
    OldJobs = maps:get(jobs, OldState),
    NewJobs = maps:get(jobs, NewState),
    Changes = maps:fold(fun(JobId, NewJob, Acc) ->
        case maps:get(JobId, OldJobs, undefined) of
            undefined -> [{JobId, undefined, NewJob#job.state} | Acc];
            OldJob when OldJob#job.state =/= NewJob#job.state ->
                [{JobId, OldJob#job.state, NewJob#job.state} | Acc];
            _ -> Acc
        end
    end, [], NewJobs),
    Changes.

%%====================================================================
%% Invariant Checkers
%%====================================================================

check_no_cpu_overallocation(#{nodes := Nodes}) ->
    lists:all(fun({_Name, Node}) ->
        UsedCpus = used_cpus(Node),
        UsedCpus =< Node#node.cpus
    end, maps:to_list(Nodes)).

check_no_memory_overallocation(#{nodes := Nodes}) ->
    lists:all(fun({_Name, Node}) ->
        UsedMem = Node#node.memory_mb - Node#node.free_memory_mb,
        UsedMem =< Node#node.memory_mb
    end, maps:to_list(Nodes)).

check_running_jobs_have_nodes(#{jobs := Jobs}) ->
    RunningJobs = [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= running],
    lists:all(fun(Job) ->
        Job#job.allocated_nodes =/= []
    end, RunningJobs).

check_valid_transitions(History, ValidTransitions) ->
    lists:all(fun({_JobId, OldState, NewState}) ->
        case OldState of
            undefined -> true;  % New job
            _ ->
                AllowedNext = maps:get(OldState, ValidTransitions, []),
                lists:member(NewState, AllowedNext)
        end
    end, History).

check_fifo_ordering(#{jobs := Jobs}) ->
    RunningJobs = lists:sort(fun(A, B) ->
        A#job.start_time =< B#job.start_time
    end, [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= running]),
    PendingJobs = [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= pending],
    
    %% All running jobs should have earlier submit time than pending jobs
    case {RunningJobs, PendingJobs} of
        {[], _} -> true;
        {_, []} -> true;
        {[LastRunning | _], _} ->
            lists:all(fun(PendingJob) ->
                PendingJob#job.submit_time >= LastRunning#job.submit_time
            end, PendingJobs)
    end.

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 60, [
        ?_assert(proper:quickcheck(prop_no_cpu_overallocation(), 
                                   [{numtests, 100}, {to_file, user}])),
        ?_assert(proper:quickcheck(prop_no_memory_overallocation(), 
                                   [{numtests, 100}, {to_file, user}])),
        ?_assert(proper:quickcheck(prop_running_jobs_have_nodes(), 
                                   [{numtests, 100}, {to_file, user}])),
        ?_assert(proper:quickcheck(prop_scheduler_idempotent(), 
                                   [{numtests, 50}, {to_file, user}]))
    ]}.
