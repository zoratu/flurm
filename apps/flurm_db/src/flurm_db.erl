%%%-------------------------------------------------------------------
%%% @doc FLURM Database - Persistence Layer
%%%
%%% Provides a distributed, consistent data store using the Ra
%%% library (Raft consensus). Used for storing jobs, nodes, and
%%% partition state.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db).

-export([
    init/0,
    put/3,
    get/2,
    delete/2,
    list/1,
    list_keys/1
]).

-include("flurm_db.hrl").

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize the database
-spec init() -> ok | {error, term()}.
init() ->
    %% In a real implementation, this would start the Ra cluster
    %% For now, we use ETS as a simple placeholder
    ensure_tables(),
    ok.

%% @doc Store a value in the database
-spec put(table_name(), key(), value()) -> ok | {error, term()}.
put(Table, Key, Value) ->
    ensure_table(Table),
    ets:insert(table_name(Table), {Key, Value}),
    ok.

%% @doc Retrieve a value from the database
-spec get(table_name(), key()) -> {ok, value()} | {error, not_found}.
get(Table, Key) ->
    ensure_table(Table),
    case ets:lookup(table_name(Table), Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, not_found}
    end.

%% @doc Delete a value from the database
-spec delete(table_name(), key()) -> ok.
delete(Table, Key) ->
    ensure_table(Table),
    ets:delete(table_name(Table), Key),
    ok.

%% @doc List all values in a table
-spec list(table_name()) -> [value()].
list(Table) ->
    ensure_table(Table),
    [Value || {_Key, Value} <- ets:tab2list(table_name(Table))].

%% @doc List all keys in a table
-spec list_keys(table_name()) -> [key()].
list_keys(Table) ->
    ensure_table(Table),
    [Key || {Key, _Value} <- ets:tab2list(table_name(Table))].

%%====================================================================
%% Internal functions
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
