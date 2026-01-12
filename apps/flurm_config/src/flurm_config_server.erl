%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Server
%%%
%%% Gen_server that manages configuration with hot reload support.
%%% Features:
%%% - Loads configuration from slurm.conf or Erlang config files
%%% - Supports hot reload via reconfigure/0
%%% - Notifies subscribers of configuration changes
%%% - Validates configuration changes before applying
%%%
%%% Usage:
%%%   flurm_config_server:get(cluster_name) -> <<"mycluster">>
%%%   flurm_config_server:reconfigure() -> ok | {error, Reason}
%%%   flurm_config_server:subscribe() -> ok
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_server).

-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([
    get/1,
    get/2,
    set/2,
    get_all/0,
    reconfigure/0,
    reconfigure/1,
    reload/0,
    reload/1,
    subscribe/0,
    unsubscribe/0,
    subscribe_changes/1,
    unsubscribe_changes/0,
    get_nodes/0,
    get_partitions/0,
    get_node/1,
    get_partition/1,
    get_version/0,
    get_last_reload/0
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("flurm_config.hrl").

-record(state, {
    config :: map(),
    config_file :: string() | undefined,
    subscribers :: [pid()],
    %% Subscribers with key filters: #{Pid => [Key1, Key2, ...] | all}
    filtered_subscribers :: #{pid() => [atom()] | all},
    last_reload :: integer() | undefined,
    version :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    start_link([]).

start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

%% @doc Get a configuration value
-spec get(atom()) -> term() | undefined.
get(Key) ->
    get(Key, undefined).

%% @doc Get a configuration value with default
-spec get(atom(), term()) -> term().
get(Key, Default) ->
    gen_server:call(?MODULE, {get, Key, Default}).

%% @doc Set a configuration value (runtime only, not persisted)
-spec set(atom(), term()) -> ok.
set(Key, Value) ->
    gen_server:call(?MODULE, {set, Key, Value}).

%% @doc Get all configuration as a map
-spec get_all() -> map().
get_all() ->
    gen_server:call(?MODULE, get_all).

%% @doc Reload configuration from the config file
-spec reconfigure() -> ok | {error, term()}.
reconfigure() ->
    gen_server:call(?MODULE, reconfigure, 30000).

%% @doc Reload configuration from a specific file
-spec reconfigure(string()) -> ok | {error, term()}.
reconfigure(Filename) ->
    gen_server:call(?MODULE, {reconfigure, Filename}, 30000).

%% @doc Subscribe to configuration changes
-spec subscribe() -> ok.
subscribe() ->
    gen_server:call(?MODULE, {subscribe, self()}).

%% @doc Unsubscribe from configuration changes
-spec unsubscribe() -> ok.
unsubscribe() ->
    gen_server:call(?MODULE, {unsubscribe, self()}).

%% @doc Reload configuration (alias for reconfigure/0)
-spec reload() -> ok | {error, term()}.
reload() ->
    reconfigure().

%% @doc Reload configuration from a specific file (alias for reconfigure/1)
-spec reload(string()) -> ok | {error, term()}.
reload(Filename) ->
    reconfigure(Filename).

%% @doc Subscribe to configuration changes for specific keys.
%% Keys can be a list of atoms (e.g., [nodes, partitions]) or 'all' for all changes.
%% Notifications are sent as {config_changed, Key, OldValue, NewValue} messages.
-spec subscribe_changes([atom()] | all) -> ok.
subscribe_changes(Keys) when is_list(Keys); Keys =:= all ->
    gen_server:call(?MODULE, {subscribe_changes, self(), Keys}).

%% @doc Unsubscribe from filtered configuration changes
-spec unsubscribe_changes() -> ok.
unsubscribe_changes() ->
    gen_server:call(?MODULE, {unsubscribe_changes, self()}).

%% @doc Get the current configuration version number
-spec get_version() -> pos_integer().
get_version() ->
    gen_server:call(?MODULE, get_version).

%% @doc Get the timestamp of the last reload
-spec get_last_reload() -> integer() | undefined.
get_last_reload() ->
    gen_server:call(?MODULE, get_last_reload).

%% @doc Get all node definitions
-spec get_nodes() -> [map()].
get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

%% @doc Get all partition definitions
-spec get_partitions() -> [map()].
get_partitions() ->
    gen_server:call(?MODULE, get_partitions).

%% @doc Get a specific node definition
-spec get_node(binary()) -> map() | undefined.
get_node(NodeName) ->
    gen_server:call(?MODULE, {get_node, NodeName}).

%% @doc Get a specific partition definition
-spec get_partition(binary()) -> map() | undefined.
get_partition(PartitionName) ->
    gen_server:call(?MODULE, {get_partition, PartitionName}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Options) ->
    process_flag(trap_exit, true),

    %% Determine config file location
    ConfigFile = case proplists:get_value(config_file, Options) of
        undefined ->
            case application:get_env(flurm_config, config_file) of
                {ok, File} -> File;
                undefined -> find_default_config()
            end;
        File ->
            File
    end,

    %% Load initial configuration
    InitialConfig = case ConfigFile of
        undefined ->
            #{};
        _ ->
            case load_config_file(ConfigFile) of
                {ok, Config} -> Config;
                {error, Reason} ->
                    log(warning,"Failed to load config file ~s: ~p", [ConfigFile, Reason]),
                    #{}
            end
    end,

    log(info,"Config server started, loaded ~p keys", [maps:size(InitialConfig)]),

    {ok, #state{
        config = InitialConfig,
        config_file = ConfigFile,
        subscribers = [],
        filtered_subscribers = #{},
        last_reload = erlang:system_time(second),
        version = 1
    }}.

handle_call({get, Key, Default}, _From, #state{config = Config} = State) ->
    Value = maps:get(Key, Config, Default),
    {reply, Value, State};

handle_call({set, Key, Value}, _From, #state{config = Config, version = V} = State) ->
    OldValue = maps:get(Key, Config, undefined),
    NewConfig = Config#{Key => Value},
    NewState = State#state{config = NewConfig, version = V + 1},
    %% Notify subscribers of the change (using the new 3-tuple format)
    notify_subscribers([{Key, OldValue, Value}], NewState),
    {reply, ok, NewState};

handle_call(get_all, _From, #state{config = Config} = State) ->
    {reply, Config, State};

handle_call(reconfigure, _From, #state{config_file = undefined} = State) ->
    {reply, {error, no_config_file}, State};

handle_call(reconfigure, _From, #state{config_file = File} = State) ->
    Result = do_reconfigure(File, State),
    {reply, element(1, Result), element(2, Result)};

handle_call({reconfigure, File}, _From, State) ->
    Result = do_reconfigure(File, State),
    {reply, element(1, Result), element(2, Result)};

handle_call({subscribe, Pid}, _From, #state{subscribers = Subs} = State) ->
    case lists:member(Pid, Subs) of
        true ->
            {reply, ok, State};
        false ->
            monitor(process, Pid),
            {reply, ok, State#state{subscribers = [Pid | Subs]}}
    end;

handle_call({unsubscribe, Pid}, _From, #state{subscribers = Subs} = State) ->
    NewSubs = lists:delete(Pid, Subs),
    {reply, ok, State#state{subscribers = NewSubs}};

handle_call({subscribe_changes, Pid, Keys}, _From, #state{filtered_subscribers = FSubs} = State) ->
    %% Monitor the process if not already monitored
    case maps:is_key(Pid, FSubs) of
        false -> monitor(process, Pid);
        true -> ok
    end,
    NewFSubs = FSubs#{Pid => Keys},
    {reply, ok, State#state{filtered_subscribers = NewFSubs}};

handle_call({unsubscribe_changes, Pid}, _From, #state{filtered_subscribers = FSubs} = State) ->
    NewFSubs = maps:remove(Pid, FSubs),
    {reply, ok, State#state{filtered_subscribers = NewFSubs}};

handle_call(get_version, _From, #state{version = V} = State) ->
    {reply, V, State};

handle_call(get_last_reload, _From, #state{last_reload = LR} = State) ->
    {reply, LR, State};

handle_call(get_nodes, _From, #state{config = Config} = State) ->
    Nodes = maps:get(nodes, Config, []),
    {reply, Nodes, State};

handle_call(get_partitions, _From, #state{config = Config} = State) ->
    Partitions = maps:get(partitions, Config, []),
    {reply, Partitions, State};

handle_call({get_node, NodeName}, _From, #state{config = Config} = State) ->
    Nodes = maps:get(nodes, Config, []),
    Result = find_node(NodeName, Nodes),
    {reply, Result, State};

handle_call({get_partition, PartName}, _From, #state{config = Config} = State) ->
    Partitions = maps:get(partitions, Config, []),
    Result = find_partition(PartName, Partitions),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{subscribers = Subs, filtered_subscribers = FSubs} = State) ->
    NewSubs = lists:delete(Pid, Subs),
    NewFSubs = maps:remove(Pid, FSubs),
    {noreply, State#state{subscribers = NewSubs, filtered_subscribers = NewFSubs}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

find_default_config() ->
    Candidates = [
        "/etc/flurm/flurm.conf",
        "/etc/flurm/slurm.conf",
        "/etc/slurm/slurm.conf",
        "flurm.conf",
        "slurm.conf"
    ],
    case lists:filter(fun filelib:is_file/1, Candidates) of
        [First | _] -> First;
        [] -> undefined
    end.

load_config_file(Filename) ->
    %% Determine file type by extension
    Result = case filename:extension(Filename) of
        ".conf" ->
            %% Assume slurm.conf format
            flurm_config_slurm:parse_file(Filename);
        ".config" ->
            %% Erlang term format
            case file:consult(Filename) of
                {ok, [Config]} when is_map(Config) ->
                    {ok, Config};
                {ok, Terms} when is_list(Terms) ->
                    {ok, maps:from_list(Terms)};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            %% Try slurm.conf format first, fall back to Erlang
            case flurm_config_slurm:parse_file(Filename) of
                {ok, Config} ->
                    {ok, Config};
                {error, _} ->
                    case file:consult(Filename) of
                        {ok, [Config2]} when is_map(Config2) ->
                            {ok, Config2};
                        {ok, Terms} when is_list(Terms) ->
                            {ok, maps:from_list(Terms)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end,
    %% Apply defaults to the loaded configuration
    case Result of
        {ok, LoadedConfig} ->
            {ok, flurm_config_defaults:apply_defaults(LoadedConfig)};
        Error ->
            Error
    end.

do_reconfigure(File, #state{config = OldConfig, version = V} = State) ->
    case load_config_file(File) of
        {ok, NewConfig} ->
            %% Validate the new configuration
            case validate_config(NewConfig) of
                ok ->
                    %% Find changed keys
                    Changes = find_changes(OldConfig, NewConfig),
                    log(info,"Reconfiguration: ~p changes detected", [length(Changes)]),

                    NewState = State#state{
                        config = NewConfig,
                        config_file = File,
                        last_reload = erlang:system_time(second),
                        version = V + 1
                    },

                    %% Notify subscribers
                    notify_subscribers(Changes, NewState),

                    %% Apply configuration to running components
                    apply_config_changes(Changes, NewConfig),

                    {ok, NewState};
                {error, Reason} ->
                    log(error,"Configuration validation failed: ~p", [Reason]),
                    {{error, {validation_failed, Reason}}, State}
            end;
        {error, Reason} ->
            log(error,"Failed to load config file ~s: ~p", [File, Reason]),
            {{error, {load_failed, Reason}}, State}
    end.

validate_config(Config) ->
    %% Basic validation - can be extended
    Validators = [
        fun validate_required_keys/1,
        fun validate_port_numbers/1,
        fun validate_node_definitions/1
    ],
    run_validators(Validators, Config).

run_validators([], _Config) ->
    ok;
run_validators([Validator | Rest], Config) ->
    case Validator(Config) of
        ok -> run_validators(Rest, Config);
        {error, _} = Error -> Error
    end.

validate_required_keys(_Config) ->
    %% For now, no required keys
    ok.

validate_port_numbers(Config) ->
    PortKeys = [slurmctldport, slurmdport, slurmdbdport],
    InvalidPorts = lists:filter(fun(Key) ->
        case maps:get(Key, Config, undefined) of
            undefined -> false;
            Port when is_integer(Port), Port > 0, Port < 65536 -> false;
            _ -> true
        end
    end, PortKeys),
    case InvalidPorts of
        [] -> ok;
        _ -> {error, {invalid_ports, InvalidPorts}}
    end.

validate_node_definitions(Config) ->
    Nodes = maps:get(nodes, Config, []),
    %% Each node must have a nodename
    InvalidNodes = lists:filter(fun(Node) ->
        not maps:is_key(nodename, Node)
    end, Nodes),
    case InvalidNodes of
        [] -> ok;
        _ -> {error, {invalid_nodes, InvalidNodes}}
    end.

%% @doc Find changes between old and new config, returning {Key, OldValue, NewValue} tuples
find_changes(OldConfig, NewConfig) ->
    AllKeys = lists:usort(maps:keys(OldConfig) ++ maps:keys(NewConfig)),
    lists:filtermap(fun(Key) ->
        OldVal = maps:get(Key, OldConfig, undefined),
        NewVal = maps:get(Key, NewConfig, undefined),
        case OldVal =:= NewVal of
            true -> false;
            false -> {true, {Key, OldVal, NewVal}}
        end
    end, AllKeys).

notify_subscribers(Changes, #state{subscribers = Subs, filtered_subscribers = FSubs, version = V}) ->
    %% Legacy notification format for backward compatibility with subscribe/0
    LegacyChanges = [{Key, NewVal} || {Key, _OldVal, NewVal} <- Changes],
    Msg = {config_changed, V, LegacyChanges},
    lists:foreach(fun(Pid) ->
        Pid ! Msg
    end, Subs),

    %% Detailed notifications for subscribe_changes/1 subscribers
    %% Send individual {config_changed, Key, OldValue, NewValue} messages
    maps:foreach(fun(Pid, Keys) ->
        FilteredChanges = case Keys of
            all -> Changes;
            KeyList -> lists:filter(fun({K, _, _}) -> lists:member(K, KeyList) end, Changes)
        end,
        lists:foreach(fun({Key, OldVal, NewVal}) ->
            Pid ! {config_changed, Key, OldVal, NewVal}
        end, FilteredChanges)
    end, FSubs).

apply_config_changes(Changes, _Config) ->
    %% Apply relevant changes to running components
    lists:foreach(fun({Key, _OldValue, NewValue}) ->
        apply_single_change(Key, NewValue)
    end, Changes).

apply_single_change(slurmctldport, _Port) ->
    %% Would need to restart listener - log warning for now
    log(warning, "SlurmctldPort changed - restart required for listener", []);
apply_single_change(schedulertype, Type) ->
    %% Hot-swap scheduler type
    log(info, "SchedulerType changed to ~p - notifying scheduler", [Type]),
    %% Trigger scheduler to refresh with new configuration
    catch flurm_scheduler:trigger_schedule();
apply_single_change(nodes, Nodes) ->
    %% Update node registry with new node definitions
    log(info,"Node definitions updated: ~p nodes", [length(Nodes)]),
    %% Send direct notification to node registry if it's running
    catch notify_node_registry(Nodes);
apply_single_change(partitions, Partitions) ->
    %% Update partition registry with new partition definitions
    log(info,"Partition definitions updated: ~p partitions", [length(Partitions)]),
    %% Send direct notification to partition registry if it's running
    catch notify_partition_registry(Partitions);
apply_single_change(Key, _Value) ->
    log(debug,"Config key ~p updated", [Key]).

%% @doc Notify the node registry to update node definitions from config
notify_node_registry(NodeDefs) ->
    case whereis(flurm_node_registry) of
        undefined -> ok;
        Pid -> Pid ! {config_reload_nodes, NodeDefs}
    end.

%% @doc Notify the partition registry to update partition definitions from config
notify_partition_registry(PartitionDefs) ->
    case whereis(flurm_partition_registry) of
        undefined -> ok;
        Pid -> Pid ! {config_reload_partitions, PartitionDefs}
    end.

find_node(NodeName, Nodes) when is_binary(NodeName) ->
    lists:foldl(fun(Node, Acc) ->
        case Acc of
            undefined ->
                case maps:get(nodename, Node, undefined) of
                    Pattern when is_binary(Pattern) ->
                        Expanded = flurm_config_slurm:expand_hostlist(Pattern),
                        case lists:member(NodeName, Expanded) of
                            true -> Node;
                            false -> undefined
                        end;
                    _ ->
                        undefined
                end;
            Found ->
                Found
        end
    end, undefined, Nodes).

find_partition(PartName, Partitions) when is_binary(PartName) ->
    lists:foldl(fun(Part, Acc) ->
        case Acc of
            undefined ->
                case maps:get(partitionname, Part, undefined) of
                    PartName -> Part;
                    _ -> undefined
                end;
            Found ->
                Found
        end
    end, undefined, Partitions).

%%====================================================================
%% Safe Logging (works with or without lager)
%%====================================================================

%% Note: lager:info/2, lager:debug/2 etc. require parse transforms.
%% Without compile-time transforms, we must use lager:log/4 or error_logger.
%% For simplicity during startup, we use error_logger consistently.
log(Level, Fmt, Args) ->
    Msg = io_lib:format(Fmt, Args),
    case Level of
        debug -> ok;  % Skip debug for now
        info -> error_logger:info_msg("[flurm_config] ~s~n", [Msg]);
        warning -> error_logger:warning_msg("[flurm_config] ~s~n", [Msg]);
        error -> error_logger:error_msg("[flurm_config] ~s~n", [Msg])
    end.
