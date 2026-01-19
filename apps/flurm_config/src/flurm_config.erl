%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Management
%%%
%%% Provides configuration management for the FLURM system. Handles
%%% loading, parsing, and accessing configuration values from various
%%% sources (files, environment, application config).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config).

-export([
    get/1,
    get/2,
    set/2,
    load_file/1,
    reload/0
]).

-include("flurm_config.hrl").

-ifdef(TEST).
-export([
    key_to_env_name/1,
    key_to_app_key/1,
    parse_env_value/1
]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Get a configuration value
-spec get(config_key()) -> {ok, term()} | {error, not_found}.
get(Key) ->
    get(Key, undefined).

%% @doc Get a configuration value with a default
-spec get(config_key(), term()) -> term().
get(Key, Default) ->
    case get_from_env(Key) of
        {ok, Value} ->
            Value;
        error ->
            case get_from_app_env(Key) of
                {ok, Value} ->
                    Value;
                error ->
                    case get_from_ets(Key) of
                        {ok, Value} ->
                            Value;
                        error ->
                            Default
                    end
            end
    end.

%% @doc Set a configuration value (in-memory only)
-spec set(config_key(), term()) -> ok.
set(Key, Value) ->
    ensure_table(),
    ets:insert(?CONFIG_TABLE, {Key, Value}),
    ok.

%% @doc Load configuration from a file
-spec load_file(string()) -> ok | {error, term()}.
load_file(Filename) ->
    case file:consult(Filename) of
        {ok, Terms} ->
            ensure_table(),
            lists:foreach(fun({Key, Value}) ->
                set(Key, Value)
            end, Terms),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Reload configuration from the default config file
-spec reload() -> ok | {error, term()}.
reload() ->
    case application:get_env(flurm_config, config_file) of
        {ok, Filename} ->
            load_file(Filename);
        undefined ->
            {error, no_config_file}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

ensure_table() ->
    case ets:whereis(?CONFIG_TABLE) of
        undefined ->
            ets:new(?CONFIG_TABLE, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ok
    end.

get_from_env(Key) ->
    EnvKey = key_to_env_name(Key),
    case os:getenv(EnvKey) of
        false ->
            error;
        Value ->
            {ok, parse_env_value(Value)}
    end.

get_from_app_env(Key) ->
    {App, ConfigKey} = key_to_app_key(Key),
    case application:get_env(App, ConfigKey) of
        {ok, Value} ->
            {ok, Value};
        undefined ->
            error
    end.

get_from_ets(Key) ->
    ensure_table(),
    case ets:lookup(?CONFIG_TABLE, Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            error
    end.

%% Convert a config key to an environment variable name
%% e.g., {flurm_controller, listen_port} -> "FLURM_CONTROLLER_LISTEN_PORT"
key_to_env_name({App, Key}) when is_atom(App), is_atom(Key) ->
    string:uppercase(
        atom_to_list(App) ++ "_" ++ atom_to_list(Key)
    );
key_to_env_name(Key) when is_atom(Key) ->
    string:uppercase("FLURM_" ++ atom_to_list(Key)).

%% Convert a config key to application env format
key_to_app_key({App, Key}) when is_atom(App), is_atom(Key) ->
    {App, Key};
key_to_app_key(Key) when is_atom(Key) ->
    {flurm_config, Key}.

%% Parse environment variable values
parse_env_value("true") -> true;
parse_env_value("false") -> false;
parse_env_value(Value) ->
    case catch list_to_integer(Value) of
        Int when is_integer(Int) ->
            Int;
        _ ->
            case catch list_to_float(Value) of
                Float when is_float(Float) ->
                    Float;
                _ ->
                    list_to_binary(Value)
            end
    end.
