%%%-------------------------------------------------------------------
%%% @doc FLURM Core - Domain Logic Module
%%%
%%% This module provides the core domain types and functions for
%%% managing jobs, nodes, and partitions in the FLURM system.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_core).

-export([
    %% Job functions
    new_job/1,
    job_id/1,
    job_state/1,
    update_job_state/2,

    %% Node functions
    new_node/1,
    node_hostname/1,
    node_state/1,
    update_node_state/2,

    %% Partition functions
    new_partition/1,
    partition_name/1,
    partition_nodes/1,
    add_node_to_partition/2
]).

-include("flurm_core.hrl").

%%====================================================================
%% Job API
%%====================================================================

%% @doc Create a new job from a map of properties
-spec new_job(map()) -> #job{}.
new_job(Props) ->
    #job{
        id = maps:get(id, Props, erlang:unique_integer([positive, monotonic])),
        name = maps:get(name, Props, <<"unnamed">>),
        user = maps:get(user, Props, <<"unknown">>),
        partition = maps:get(partition, Props, <<"default">>),
        state = maps:get(state, Props, pending),
        script = maps:get(script, Props, <<>>),
        num_nodes = maps:get(num_nodes, Props, 1),
        num_cpus = maps:get(num_cpus, Props, 1),
        memory_mb = maps:get(memory_mb, Props, 1024),
        time_limit = maps:get(time_limit, Props, 3600),
        priority = maps:get(priority, Props, 100),
        submit_time = maps:get(submit_time, Props, erlang:system_time(second)),
        start_time = maps:get(start_time, Props, undefined),
        end_time = maps:get(end_time, Props, undefined),
        allocated_nodes = maps:get(allocated_nodes, Props, []),
        exit_code = maps:get(exit_code, Props, undefined),
        account = maps:get(account, Props, <<>>),
        qos = maps:get(qos, Props, <<"normal">>)
    }.

%% @doc Get job ID
-spec job_id(#job{}) -> job_id().
job_id(#job{id = Id}) -> Id.

%% @doc Get job state
-spec job_state(#job{}) -> job_state().
job_state(#job{state = State}) -> State.

%% @doc Update job state
-spec update_job_state(#job{}, job_state()) -> #job{}.
update_job_state(Job, NewState) ->
    Now = erlang:system_time(second),
    case NewState of
        running ->
            Job#job{state = NewState, start_time = Now};
        completed ->
            Job#job{state = NewState, end_time = Now};
        failed ->
            Job#job{state = NewState, end_time = Now};
        cancelled ->
            Job#job{state = NewState, end_time = Now};
        timeout ->
            Job#job{state = NewState, end_time = Now};
        node_fail ->
            Job#job{state = NewState, end_time = Now};
        _ ->
            Job#job{state = NewState}
    end.

%%====================================================================
%% Node API
%%====================================================================

%% @doc Create a new node from a map of properties
-spec new_node(map()) -> #node{}.
new_node(Props) ->
    #node{
        hostname = maps:get(hostname, Props),
        cpus = maps:get(cpus, Props, 1),
        memory_mb = maps:get(memory_mb, Props, 1024),
        state = maps:get(state, Props, down),
        features = maps:get(features, Props, []),
        partitions = maps:get(partitions, Props, []),
        running_jobs = maps:get(running_jobs, Props, []),
        load_avg = maps:get(load_avg, Props, 0.0),
        free_memory_mb = maps:get(free_memory_mb, Props, 0),
        last_heartbeat = maps:get(last_heartbeat, Props, undefined)
    }.

%% @doc Get node hostname
-spec node_hostname(#node{}) -> binary().
node_hostname(#node{hostname = Hostname}) -> Hostname.

%% @doc Get node state
-spec node_state(#node{}) -> node_state().
node_state(#node{state = State}) -> State.

%% @doc Update node state
-spec update_node_state(#node{}, node_state()) -> #node{}.
update_node_state(Node, NewState) ->
    Node#node{state = NewState}.

%%====================================================================
%% Partition API
%%====================================================================

%% @doc Create a new partition from a map of properties
-spec new_partition(map()) -> #partition{}.
new_partition(Props) ->
    #partition{
        name = maps:get(name, Props),
        state = maps:get(state, Props, up),
        nodes = maps:get(nodes, Props, []),
        max_time = maps:get(max_time, Props, 86400),
        default_time = maps:get(default_time, Props, 3600),
        max_nodes = maps:get(max_nodes, Props, 100),
        priority = maps:get(priority, Props, 100),
        allow_root = maps:get(allow_root, Props, false)
    }.

%% @doc Get partition name
-spec partition_name(#partition{}) -> binary().
partition_name(#partition{name = Name}) -> Name.

%% @doc Get partition nodes
-spec partition_nodes(#partition{}) -> [binary()].
partition_nodes(#partition{nodes = Nodes}) -> Nodes.

%% @doc Add a node to a partition
-spec add_node_to_partition(#partition{}, binary()) -> #partition{}.
add_node_to_partition(#partition{nodes = Nodes} = Partition, NodeName) ->
    case lists:member(NodeName, Nodes) of
        true -> Partition;
        false -> Partition#partition{nodes = [NodeName | Nodes]}
    end.
