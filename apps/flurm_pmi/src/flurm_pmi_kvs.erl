%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Key-Value Store
%%%
%%% Manages the keyval space for PMI jobs. MPI processes use this to
%%% exchange connection information (hostnames, ports, device names).
%%%
%%% Each job step gets its own KVS namespace.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_kvs).

-export([
    new/0,
    new/1,
    put/3,
    get/2,
    get_all/1,
    delete/2,
    size/1,
    keys/1,
    get_by_index/2,
    merge/2
]).

-type kvs() :: #{binary() => binary()}.
-type kvs_name() :: binary().

-export_type([kvs/0, kvs_name/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Create a new empty KVS
-spec new() -> kvs().
new() ->
    #{}.

%% @doc Create a new KVS with initial values
-spec new(#{binary() => binary()}) -> kvs().
new(Initial) when is_map(Initial) ->
    Initial.

%% @doc Put a key-value pair
-spec put(kvs(), binary(), binary()) -> kvs().
put(KVS, Key, Value) when is_binary(Key), is_binary(Value) ->
    maps:put(Key, Value, KVS);
put(KVS, Key, Value) ->
    %% Convert to binary if needed
    KeyBin = to_binary(Key),
    ValueBin = to_binary(Value),
    maps:put(KeyBin, ValueBin, KVS).

%% @doc Get a value by key
-spec get(kvs(), binary()) -> {ok, binary()} | {error, not_found}.
get(KVS, Key) ->
    KeyBin = to_binary(Key),
    case maps:find(KeyBin, KVS) of
        {ok, Value} -> {ok, Value};
        error -> {error, not_found}
    end.

%% @doc Get all key-value pairs
-spec get_all(kvs()) -> [{binary(), binary()}].
get_all(KVS) ->
    maps:to_list(KVS).

%% @doc Delete a key
-spec delete(kvs(), binary()) -> kvs().
delete(KVS, Key) ->
    KeyBin = to_binary(Key),
    maps:remove(KeyBin, KVS).

%% @doc Get KVS size
-spec size(kvs()) -> non_neg_integer().
size(KVS) ->
    maps:size(KVS).

%% @doc Get all keys
-spec keys(kvs()) -> [binary()].
keys(KVS) ->
    maps:keys(KVS).

%% @doc Get key-value by index (for PMI iteration)
-spec get_by_index(kvs(), non_neg_integer()) -> {ok, binary(), binary()} | {error, end_of_kvs}.
get_by_index(KVS, Index) ->
    Keys = lists:sort(maps:keys(KVS)),
    case Index < length(Keys) of
        true ->
            Key = lists:nth(Index + 1, Keys),
            {ok, Value} = maps:find(Key, KVS),
            {ok, Key, Value};
        false ->
            {error, end_of_kvs}
    end.

%% @doc Merge two KVS stores (second overwrites first on conflict)
-spec merge(kvs(), kvs()) -> kvs().
merge(KVS1, KVS2) ->
    maps:merge(KVS1, KVS2).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_binary(V) when is_integer(V) -> integer_to_binary(V).
