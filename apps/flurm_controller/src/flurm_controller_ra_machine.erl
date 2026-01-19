%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Ra State Machine
%%%
%%% Implements the Ra state machine behaviour for the FLURM controller
%%% cluster. This module handles all state mutations that need to be
%%% replicated across the cluster.
%%%
%%% The state machine maintains:
%%% - Job registry
%%% - Node registry
%%% - Partition configuration
%%% - Sequence counters for ID generation
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_ra_machine).

-behaviour(ra_machine).

%% Ra machine callbacks
-export([init/1,
         apply/3,
         state_enter/2,
         snapshot_installed/2,
         overview/1]).

-include_lib("flurm_core/include/flurm_core.hrl").

-ifdef(TEST).
-export([create_job/2,
         update_job_state_internal/2,
         apply_job_updates/2,
         apply_heartbeat/2,
         apply_partition_updates/2]).
-endif.

%% State machine state
-record(state, {
    jobs = #{} :: #{job_id() => #job{}},
    nodes = #{} :: #{binary() => #node{}},
    partitions = #{} :: #{binary() => #partition{}},
    next_job_id = 1 :: pos_integer(),
    last_applied_index = 0 :: non_neg_integer()
}).

%% Commands that can be applied to the state machine
-type command() ::
    %% Job commands
    {submit_job, map()} |
    {cancel_job, job_id()} |
    {update_job_state, job_id(), job_state()} |
    {update_job, job_id(), map()} |
    %% Node commands
    {register_node, #node{}} |
    {unregister_node, binary()} |
    {update_node_state, binary(), node_state()} |
    {node_heartbeat, binary(), map()} |
    %% Partition commands
    {create_partition, #partition{}} |
    {update_partition, binary(), map()} |
    {delete_partition, binary()}.

%%====================================================================
%% Ra Machine Callbacks
%%====================================================================

%% @doc Initialize the state machine.
-spec init(map()) -> #state{}.
init(_Config) ->
    lager:info("Initializing FLURM controller Ra state machine"),
    #state{}.

%% @doc Apply a command to the state machine.
%% This is the core of the state machine - all state changes happen here.
-spec apply(ra_machine:meta(), command(), #state{}) ->
    {#state{}, term()} | {#state{}, term(), ra_machine:effects()}.

%% === Job Commands ===

apply(#{index := Index}, {submit_job, JobSpec}, #state{jobs = Jobs, next_job_id = NextId} = State) ->
    %% Create new job with next ID
    Job = create_job(NextId, JobSpec),
    NewJobs = maps:put(NextId, Job, Jobs),
    NewState = State#state{
        jobs = NewJobs,
        next_job_id = NextId + 1,
        last_applied_index = Index
    },
    %% Return the job ID as result
    {NewState, {ok, NextId}};

apply(#{index := Index}, {cancel_job, JobId}, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            UpdatedJob = Job#job{
                state = cancelled,
                end_time = erlang:system_time(second)
            },
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            NewState = State#state{jobs = NewJobs, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

apply(#{index := Index}, {update_job_state, JobId, NewJobState}, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            UpdatedJob = update_job_state_internal(Job, NewJobState),
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            NewState = State#state{jobs = NewJobs, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

apply(#{index := Index}, {update_job, JobId, Updates}, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            UpdatedJob = apply_job_updates(Job, Updates),
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            NewState = State#state{jobs = NewJobs, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

%% === Node Commands ===

apply(#{index := Index}, {register_node, Node}, #state{nodes = Nodes} = State) ->
    Hostname = Node#node.hostname,
    NewNodes = maps:put(Hostname, Node, Nodes),
    NewState = State#state{nodes = NewNodes, last_applied_index = Index},
    {NewState, ok};

apply(#{index := Index}, {unregister_node, Hostname}, #state{nodes = Nodes} = State) ->
    NewNodes = maps:remove(Hostname, Nodes),
    NewState = State#state{nodes = NewNodes, last_applied_index = Index},
    {NewState, ok};

apply(#{index := Index}, {update_node_state, Hostname, NodeState}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = Node#node{state = NodeState},
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            NewState = State#state{nodes = NewNodes, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

apply(#{index := Index}, {node_heartbeat, Hostname, HeartbeatData}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = apply_heartbeat(Node, HeartbeatData),
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            NewState = State#state{nodes = NewNodes, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

%% === Partition Commands ===

apply(#{index := Index}, {create_partition, Partition}, #state{partitions = Parts} = State) ->
    Name = Partition#partition.name,
    case maps:is_key(Name, Parts) of
        true ->
            {State#state{last_applied_index = Index}, {error, already_exists}};
        false ->
            NewParts = maps:put(Name, Partition, Parts),
            NewState = State#state{partitions = NewParts, last_applied_index = Index},
            {NewState, ok}
    end;

apply(#{index := Index}, {update_partition, Name, Updates}, #state{partitions = Parts} = State) ->
    case maps:find(Name, Parts) of
        {ok, Partition} ->
            UpdatedPartition = apply_partition_updates(Partition, Updates),
            NewParts = maps:put(Name, UpdatedPartition, Parts),
            NewState = State#state{partitions = NewParts, last_applied_index = Index},
            {NewState, ok};
        error ->
            {State#state{last_applied_index = Index}, {error, not_found}}
    end;

apply(#{index := Index}, {delete_partition, Name}, #state{partitions = Parts} = State) ->
    NewParts = maps:remove(Name, Parts),
    NewState = State#state{partitions = NewParts, last_applied_index = Index},
    {NewState, ok};

%% Catch-all for unknown commands
apply(#{index := Index}, Command, State) ->
    lager:warning("Unknown Ra command: ~p", [Command]),
    {State#state{last_applied_index = Index}, {error, unknown_command}}.

%% @doc Handle state entry events (e.g., becoming leader).
-spec state_enter(ra_server:ra_state(), #state{}) -> ra_machine:effects().
state_enter(leader, _State) ->
    lager:info("Ra state machine entered leader state"),
    %% Could add effects here, like notifying the cluster module
    [];
state_enter(follower, _State) ->
    lager:info("Ra state machine entered follower state"),
    [];
state_enter(candidate, _State) ->
    lager:debug("Ra state machine entered candidate state"),
    [];
state_enter(_StateName, _State) ->
    [].

%% @doc Handle snapshot installation.
-spec snapshot_installed(ra_machine:meta(), #state{}) -> ok.
snapshot_installed(_Meta, _State) ->
    lager:info("Ra snapshot installed"),
    ok.

%% @doc Provide an overview of the state machine state.
-spec overview(#state{}) -> map().
overview(#state{jobs = Jobs, nodes = Nodes, partitions = Parts,
                next_job_id = NextId, last_applied_index = LastIndex}) ->
    #{
        job_count => maps:size(Jobs),
        node_count => maps:size(Nodes),
        partition_count => maps:size(Parts),
        next_job_id => NextId,
        last_applied_index => LastIndex
    }.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Create a new job record from a job spec map.
-spec create_job(job_id(), map()) -> #job{}.
create_job(JobId, JobSpec) ->
    #job{
        id = JobId,
        name = maps:get(name, JobSpec, <<"unnamed">>),
        user = maps:get(user, JobSpec, <<>>),
        partition = maps:get(partition, JobSpec, <<"default">>),
        state = pending,
        script = maps:get(script, JobSpec, <<>>),
        num_nodes = maps:get(num_nodes, JobSpec, 1),
        num_cpus = maps:get(num_cpus, JobSpec, 1),
        memory_mb = maps:get(memory_mb, JobSpec, 1024),
        time_limit = maps:get(time_limit, JobSpec, 3600),
        priority = maps:get(priority, JobSpec, 100),
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    }.

%% @doc Update job state with appropriate timestamps.
-spec update_job_state_internal(#job{}, job_state()) -> #job{}.
update_job_state_internal(Job, running) ->
    Job#job{
        state = running,
        start_time = erlang:system_time(second)
    };
update_job_state_internal(Job, State) when State =:= completed;
                                           State =:= failed;
                                           State =:= cancelled;
                                           State =:= timeout;
                                           State =:= node_fail ->
    Job#job{
        state = State,
        end_time = erlang:system_time(second)
    };
update_job_state_internal(Job, State) ->
    Job#job{state = State}.

%% @doc Apply updates to a job record.
-spec apply_job_updates(#job{}, map()) -> #job{}.
apply_job_updates(Job, Updates) ->
    lists:foldl(fun({Key, Value}, J) ->
        case Key of
            state -> J#job{state = Value};
            allocated_nodes -> J#job{allocated_nodes = Value};
            start_time -> J#job{start_time = Value};
            end_time -> J#job{end_time = Value};
            exit_code -> J#job{exit_code = Value};
            priority -> J#job{priority = Value};
            _ -> J
        end
    end, Job, maps:to_list(Updates)).

%% @doc Apply heartbeat data to a node record.
-spec apply_heartbeat(#node{}, map()) -> #node{}.
apply_heartbeat(Node, HeartbeatData) ->
    Node#node{
        load_avg = maps:get(load_avg, HeartbeatData, Node#node.load_avg),
        free_memory_mb = maps:get(free_memory_mb, HeartbeatData, Node#node.free_memory_mb),
        running_jobs = maps:get(running_jobs, HeartbeatData, Node#node.running_jobs),
        last_heartbeat = erlang:system_time(second)
    }.

%% @doc Apply updates to a partition record.
-spec apply_partition_updates(#partition{}, map()) -> #partition{}.
apply_partition_updates(Partition, Updates) ->
    lists:foldl(fun({Key, Value}, P) ->
        case Key of
            state -> P#partition{state = Value};
            nodes -> P#partition{nodes = Value};
            max_time -> P#partition{max_time = Value};
            default_time -> P#partition{default_time = Value};
            max_nodes -> P#partition{max_nodes = Value};
            priority -> P#partition{priority = Value};
            _ -> P
        end
    end, Partition, maps:to_list(Updates)).
