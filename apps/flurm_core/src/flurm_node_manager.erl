%%%-------------------------------------------------------------------
%%% @doc FLURM Node Manager
%%%
%%% Facade module for node management operations used by the scheduler.
%%% Provides an interface to flurm_node_registry and flurm_node for
%%% resource allocation and node queries.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager).

-include("flurm_core.hrl").

%% API
-export([
    get_available_nodes_for_job/3,
    get_available_nodes_with_gres/4,
    allocate_resources/4,
    release_resources/2,
    allocate_gres/4,
    release_gres/2
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get available nodes that can run a job with the given requirements.
%% Returns a list of #node{} records for nodes with sufficient resources.
-spec get_available_nodes_for_job(pos_integer(), pos_integer(), binary()) -> [#node{}].
get_available_nodes_for_job(NumCpus, MemoryMb, Partition) ->
    %% Get available nodes from registry with minimum resources
    AvailableEntries = flurm_node_registry:get_available_nodes({NumCpus, MemoryMb, 0}),

    %% Filter by partition if specified
    FilteredEntries = case Partition of
        <<>> -> AvailableEntries;
        <<"default">> -> AvailableEntries;  % Default partition accepts all nodes
        _ ->
            lists:filter(
                fun(#node_entry{partitions = Partitions}) ->
                    lists:member(Partition, Partitions)
                end,
                AvailableEntries
            )
    end,

    %% Convert node_entry records to node records for scheduler
    lists:map(fun entry_to_node/1, FilteredEntries).

%% @doc Get available nodes with GRES requirements.
-spec get_available_nodes_with_gres(pos_integer(), pos_integer(), binary(), binary()) -> [#node{}].
get_available_nodes_with_gres(NumCpus, MemoryMb, Partition, _GRESSpec) ->
    %% For now, just get regular nodes - GRES filtering is a stub
    get_available_nodes_for_job(NumCpus, MemoryMb, Partition).

%% @doc Allocate resources on a node for a job.
-spec allocate_resources(binary(), pos_integer(), pos_integer(), pos_integer()) -> ok | {error, term()}.
allocate_resources(NodeName, JobId, Cpus, Memory) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} ->
            flurm_node:allocate(Pid, JobId, {Cpus, Memory, 0});
        {error, not_found} ->
            {error, node_not_found}
    end.

%% @doc Release resources from a job on a node.
-spec release_resources(binary(), pos_integer()) -> ok | {error, term()}.
release_resources(NodeName, JobId) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} ->
            flurm_node:release(Pid, JobId);
        {error, not_found} ->
            {error, node_not_found}
    end.

%% @doc Allocate GRES on a node (stub).
-spec allocate_gres(binary(), pos_integer(), binary(), boolean()) -> {ok, []} | {error, term()}.
allocate_gres(_NodeName, _JobId, <<>>, _Exclusive) ->
    {ok, []};
allocate_gres(_NodeName, _JobId, _GRESSpec, _Exclusive) ->
    %% GRES allocation is a stub for now
    {ok, []}.

%% @doc Release GRES from a node (stub).
-spec release_gres(binary(), pos_integer()) -> ok.
release_gres(_NodeName, _JobId) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Convert a node_entry record to a node record for scheduler use
entry_to_node(#node_entry{
    name = Name,
    hostname = _Hostname,
    state = State,
    partitions = Partitions,
    cpus_total = CpusTotal,
    cpus_avail = _CpusAvail,
    memory_total = MemoryTotal,
    memory_avail = MemoryAvail
}) ->
    #node{
        hostname = Name,  % Use name as hostname for test compatibility
        cpus = CpusTotal,
        memory_mb = MemoryTotal,
        state = State,
        features = [],
        partitions = Partitions,
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = MemoryAvail,
        allocations = #{},
        %% Calculate used from available
        drain_reason = undefined,
        last_heartbeat = undefined,
        gres_config = [],
        gres_available = #{},
        gres_total = #{},
        gres_allocations = #{}
    }.
