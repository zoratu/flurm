%%%-------------------------------------------------------------------
%%% @doc FLURM Database - High-Level API
%%%
%%% Provides a unified interface to the FLURM distributed database.
%%% This module routes requests appropriately:
%%% - Write operations go through Ra (replicated)
%%% - Read operations can be local (stale) or consistent (through leader)
%%%
%%% For backward compatibility, this module also provides the legacy
%%% ETS-based API which is used as a fallback when Ra is not available.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db).

-include("flurm_db.hrl").

%% High-level API (Ra-backed)
-export([
    %% Job operations
    submit_job/1,
    cancel_job/1,
    get_job/1,
    get_job/2,
    list_jobs/0,
    list_jobs/1,
    list_pending_jobs/0,
    list_running_jobs/0,
    allocate_job/2,
    complete_job/2,

    %% Node operations
    register_node/1,
    unregister_node/1,
    update_node_state/2,
    get_node/1,
    get_node/2,
    list_nodes/0,
    list_nodes/1,
    list_available_nodes/0,
    list_nodes_in_partition/1,

    %% Partition operations
    create_partition/1,
    delete_partition/1,
    get_partition/1,
    list_partitions/0,

    %% Cluster operations
    cluster_status/0,
    is_leader/0,
    get_leader/0
]).

%% Legacy API (ETS-backed, for backward compatibility)
-export([
    init/0,
    put/3,
    get/2,
    delete/2,
    list/1,
    list_keys/1
]).

%% TEST exports for coverage of internal helper functions
-ifdef(TEST).
-export([
    ensure_tables/0,
    ensure_table/1,
    table_name/1
]).
-endif.

%%====================================================================
%% Job Operations
%%====================================================================

%% @doc Submit a new job for execution.
%% Returns the job ID on success.
-spec submit_job(#ra_job_spec{}) -> {ok, job_id()} | {error, term()}.
submit_job(#ra_job_spec{} = JobSpec) ->
    flurm_db_ra:submit_job(JobSpec).

%% @doc Cancel a job by ID.
-spec cancel_job(job_id()) -> ok | {error, term()}.
cancel_job(JobId) when is_integer(JobId) ->
    flurm_db_ra:cancel_job(JobId).

%% @doc Get a job by ID (local read, may be stale).
-spec get_job(job_id()) -> {ok, #ra_job{}} | {error, not_found}.
get_job(JobId) when is_integer(JobId) ->
    flurm_db_ra:get_job(JobId).

%% @doc Get a job by ID with consistency option.
%% Options: local | consistent
-spec get_job(job_id(), local | consistent) -> {ok, #ra_job{}} | {error, term()}.
get_job(JobId, local) ->
    flurm_db_ra:get_job(JobId);
get_job(JobId, consistent) ->
    flurm_db_ra:consistent_get_job(JobId).

%% @doc List all jobs (local read, may be stale).
-spec list_jobs() -> {ok, [#ra_job{}]}.
list_jobs() ->
    flurm_db_ra:list_jobs().

%% @doc List all jobs with consistency option.
-spec list_jobs(local | consistent) -> {ok, [#ra_job{}]} | {error, term()}.
list_jobs(local) ->
    flurm_db_ra:list_jobs();
list_jobs(consistent) ->
    flurm_db_ra:consistent_list_jobs().

%% @doc List all pending jobs.
-spec list_pending_jobs() -> {ok, [#ra_job{}]}.
list_pending_jobs() ->
    flurm_db_ra:get_jobs_by_state(pending).

%% @doc List all running jobs.
-spec list_running_jobs() -> {ok, [#ra_job{}]}.
list_running_jobs() ->
    flurm_db_ra:get_jobs_by_state(running).

%% @doc Allocate nodes to a job.
-spec allocate_job(job_id(), [node_name()]) -> ok | {error, term()}.
allocate_job(JobId, Nodes) when is_integer(JobId), is_list(Nodes) ->
    flurm_db_ra:allocate_job(JobId, Nodes).

%% @doc Mark a job as completed with an exit code.
-spec complete_job(job_id(), integer()) -> ok | {error, term()}.
complete_job(JobId, ExitCode) when is_integer(JobId), is_integer(ExitCode) ->
    flurm_db_ra:set_job_exit_code(JobId, ExitCode).

%%====================================================================
%% Node Operations
%%====================================================================

%% @doc Register a compute node.
-spec register_node(#ra_node_spec{}) -> {ok, registered | updated} | {error, term()}.
register_node(#ra_node_spec{} = NodeSpec) ->
    flurm_db_ra:register_node(NodeSpec).

%% @doc Unregister a compute node.
-spec unregister_node(node_name()) -> ok | {error, term()}.
unregister_node(NodeName) when is_binary(NodeName) ->
    flurm_db_ra:unregister_node(NodeName).

%% @doc Update the state of a node.
-spec update_node_state(node_name(), atom()) -> ok | {error, term()}.
update_node_state(NodeName, NewState) when is_binary(NodeName), is_atom(NewState) ->
    flurm_db_ra:update_node_state(NodeName, NewState).

%% @doc Get a node by name (local read, may be stale).
-spec get_node(node_name()) -> {ok, #ra_node{}} | {error, not_found}.
get_node(NodeName) when is_binary(NodeName) ->
    flurm_db_ra:get_node(NodeName).

%% @doc Get a node by name with consistency option.
-spec get_node(node_name(), local | consistent) -> {ok, #ra_node{}} | {error, term()}.
get_node(NodeName, local) ->
    flurm_db_ra:get_node(NodeName);
get_node(NodeName, consistent) ->
    %% For now, use local read - can add consistent query if needed
    flurm_db_ra:get_node(NodeName).

%% @doc List all nodes (local read, may be stale).
-spec list_nodes() -> {ok, [#ra_node{}]}.
list_nodes() ->
    flurm_db_ra:list_nodes().

%% @doc List all nodes with state filter.
-spec list_nodes(atom()) -> {ok, [#ra_node{}]}.
list_nodes(State) when is_atom(State) ->
    flurm_db_ra:get_nodes_by_state(State).

%% @doc List all available (up) nodes.
-spec list_available_nodes() -> {ok, [#ra_node{}]}.
list_available_nodes() ->
    flurm_db_ra:get_nodes_by_state(up).

%% @doc List all nodes in a partition.
-spec list_nodes_in_partition(partition_name()) -> {ok, [#ra_node{}]} | {error, not_found}.
list_nodes_in_partition(PartName) when is_binary(PartName) ->
    flurm_db_ra:get_nodes_in_partition(PartName).

%%====================================================================
%% Partition Operations
%%====================================================================

%% @doc Create a new partition.
-spec create_partition(#ra_partition_spec{}) -> ok | {error, term()}.
create_partition(#ra_partition_spec{} = PartSpec) ->
    flurm_db_ra:create_partition(PartSpec).

%% @doc Delete a partition.
-spec delete_partition(partition_name()) -> ok | {error, term()}.
delete_partition(PartName) when is_binary(PartName) ->
    flurm_db_ra:delete_partition(PartName).

%% @doc Get a partition by name.
-spec get_partition(partition_name()) -> {ok, #ra_partition{}} | {error, not_found}.
get_partition(PartName) when is_binary(PartName) ->
    flurm_db_ra:get_partition(PartName).

%% @doc List all partitions.
-spec list_partitions() -> {ok, [#ra_partition{}]}.
list_partitions() ->
    flurm_db_ra:list_partitions().

%%====================================================================
%% Cluster Operations
%%====================================================================

%% @doc Get the cluster status.
-spec cluster_status() -> {ok, map()} | {error, term()}.
cluster_status() ->
    flurm_db_cluster:status().

%% @doc Check if this node is the cluster leader.
-spec is_leader() -> boolean().
is_leader() ->
    flurm_db_cluster:is_leader().

%% @doc Get the current cluster leader.
-spec get_leader() -> {ok, ra:server_id()} | {error, term()}.
get_leader() ->
    flurm_db_cluster:get_leader().

%%====================================================================
%% Legacy API (ETS-backed)
%%====================================================================

%% @doc Initialize the legacy ETS tables.
%% This is kept for backward compatibility and fallback scenarios.
-spec init() -> ok | {error, term()}.
init() ->
    ensure_tables(),
    ok.

%% @doc Store a value in the legacy ETS table.
-spec put(table_name(), key(), value()) -> ok | {error, term()}.
put(Table, Key, Value) ->
    ensure_table(Table),
    ets:insert(table_name(Table), {Key, Value}),
    ok.

%% @doc Retrieve a value from the legacy ETS table.
-spec get(table_name(), key()) -> {ok, value()} | {error, not_found}.
get(Table, Key) ->
    ensure_table(Table),
    case ets:lookup(table_name(Table), Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, not_found}
    end.

%% @doc Delete a value from the legacy ETS table.
-spec delete(table_name(), key()) -> ok.
delete(Table, Key) ->
    ensure_table(Table),
    ets:delete(table_name(Table), Key),
    ok.

%% @doc List all values in a legacy ETS table.
-spec list(table_name()) -> [value()].
list(Table) ->
    ensure_table(Table),
    [Value || {_Key, Value} <- ets:tab2list(table_name(Table))].

%% @doc List all keys in a legacy ETS table.
-spec list_keys(table_name()) -> [key()].
list_keys(Table) ->
    ensure_table(Table),
    [Key || {Key, _Value} <- ets:tab2list(table_name(Table))].

%%====================================================================
%% Internal Functions
%%====================================================================

ensure_tables() ->
    lists:foreach(fun ensure_table/1, [jobs, nodes, partitions]).

ensure_table(Table) ->
    TableName = table_name(Table),
    case ets:whereis(TableName) of
        undefined ->
            ets:new(TableName, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ok
    end.

table_name(jobs) -> flurm_db_jobs;
table_name(nodes) -> flurm_db_nodes;
table_name(partitions) -> flurm_db_partitions;
table_name(Table) when is_atom(Table) ->
    list_to_atom("flurm_db_" ++ atom_to_list(Table)).
