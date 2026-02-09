%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Top-Level Supervisor
%%%
%%% This supervisor manages all the child processes of the FLURM
%%% controller daemon, including the job manager, node manager,
%%% scheduler, partition manager, and the Ranch TCP listener.
%%%
%%% The Ranch listener accepts SLURM protocol connections on the
%%% configured port (default 6817) and handles up to max_connections
%%% concurrent connections.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% API for managing listeners
-export([start_listener/0, stop_listener/0, listener_info/0]).
-export([start_node_listener/0, stop_node_listener/0, node_listener_info/0]).
-export([start_http_api/0, stop_http_api/0, http_api_info/0]).

-ifdef(TEST).
-export([get_listener_config/0,
         get_node_listener_config/0,
         get_http_api_config/0,
         parse_address/1,
         is_cluster_enabled/0]).
-endif.

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, flurm_controller_listener).
-define(NODE_LISTENER_NAME, flurm_node_listener).
-define(HTTP_API_LISTENER, flurm_bridge_http_listener).

%% Default configuration
-define(DEFAULT_PORT, 6817).
-define(DEFAULT_NODE_PORT, 6818).
-define(DEFAULT_HTTP_API_PORT, 6820).
-define(DEFAULT_ADDRESS, "0.0.0.0").
-define(DEFAULT_NUM_ACCEPTORS, 10).
-define(DEFAULT_MAX_CONNECTIONS, 1000).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start the Ranch TCP listener.
%% Called during application startup or to restart after stop.
-spec start_listener() -> {ok, pid()} | {error, term()}.
start_listener() ->
    {Port, Address, NumAcceptors, MaxConns} = get_listener_config(),
    TransportOpts = #{
        socket_opts => [
            {ip, parse_address(Address)},
            {port, Port},
            {nodelay, true},           % TCP_NODELAY for low latency
            {keepalive, true},         % Enable TCP keepalive
            {reuseaddr, true},         % Allow quick restart
            {backlog, 1024}            % Connection queue size
        ],
        num_acceptors => NumAcceptors,
        max_connections => MaxConns
    },
    ProtocolOpts = #{
        handler_module => flurm_controller_handler
    },
    case ranch:start_listener(
        ?LISTENER_NAME,
        ranch_tcp,
        TransportOpts,
        flurm_controller_acceptor,
        ProtocolOpts
    ) of
        {ok, Pid} ->
            lager:info("FLURM Controller listener started on ~s:~p "
                       "(acceptors=~p, max_connections=~p)",
                       [Address, Port, NumAcceptors, MaxConns]),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            lager:debug("Listener already running"),
            {ok, Pid};
        {error, Reason} ->
            lager:error("Failed to start listener: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Stop the Ranch TCP listener.
-spec stop_listener() -> ok | {error, not_found}.
stop_listener() ->
    case ranch:stop_listener(?LISTENER_NAME) of
        ok ->
            lager:info("FLURM Controller listener stopped"),
            ok;
        {error, not_found} = Error ->
            lager:warning("Listener not found when stopping"),
            Error
    end.

%% @doc Get information about the listener.
-spec listener_info() -> map() | {error, not_found}.
listener_info() ->
    try
        #{
            port => ranch:get_port(?LISTENER_NAME),
            max_connections => ranch:get_max_connections(?LISTENER_NAME),
            active_connections => ranch:procs(?LISTENER_NAME, connections),
            status => running
        }
    catch
        _:_ ->
            {error, not_found}
    end.

%% @doc Start the Ranch TCP listener for node daemon connections.
%% Uses the internal FLURM protocol (not SLURM binary protocol).
-spec start_node_listener() -> {ok, pid()} | {error, term()}.
start_node_listener() ->
    {Port, Address, NumAcceptors, MaxConns} = get_node_listener_config(),
    TransportOpts = #{
        socket_opts => [
            {ip, parse_address(Address)},
            {port, Port},
            {nodelay, true},
            {keepalive, true},
            {reuseaddr, true},
            {backlog, 256}
        ],
        num_acceptors => NumAcceptors,
        max_connections => MaxConns
    },
    ProtocolOpts = #{},
    case ranch:start_listener(
        ?NODE_LISTENER_NAME,
        ranch_tcp,
        TransportOpts,
        flurm_node_acceptor,
        ProtocolOpts
    ) of
        {ok, Pid} ->
            log(info, "FLURM Node listener started on ~s:~p "
                "(acceptors=~p, max_connections=~p)",
                [Address, Port, NumAcceptors, MaxConns]),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            log(error, "Failed to start node listener: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Stop the node listener.
-spec stop_node_listener() -> ok | {error, not_found}.
stop_node_listener() ->
    case ranch:stop_listener(?NODE_LISTENER_NAME) of
        ok ->
            log(info, "FLURM Node listener stopped", []),
            ok;
        {error, not_found} = Error ->
            Error
    end.

%% @doc Get information about the node listener.
-spec node_listener_info() -> map() | {error, not_found}.
node_listener_info() ->
    try
        #{
            port => ranch:get_port(?NODE_LISTENER_NAME),
            max_connections => ranch:get_max_connections(?NODE_LISTENER_NAME),
            active_connections => ranch:procs(?NODE_LISTENER_NAME, connections),
            status => running
        }
    catch
        _:_ ->
            {error, not_found}
    end.

%% @doc Start the Cowboy HTTP API listener for the SLURM bridge REST API.
%% Provides HTTP endpoints for bridge management during migration.
-spec start_http_api() -> {ok, pid()} | {error, term()}.
start_http_api() ->
    {Port, Address, _NumAcceptors} = get_http_api_config(),
    Dispatch = cowboy_router:compile(flurm_bridge_http:routes()),
    TransportOpts = #{
        socket_opts => [
            {ip, parse_address(Address)},
            {port, Port}
        ]
    },
    ProtocolOpts = #{
        env => #{dispatch => Dispatch}
    },
    case cowboy:start_clear(
        ?HTTP_API_LISTENER,
        TransportOpts,
        ProtocolOpts
    ) of
        {ok, Pid} ->
            log(info, "FLURM Bridge HTTP API started on ~s:~p", [Address, Port]),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            log(error, "Failed to start HTTP API: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Stop the HTTP API listener.
-spec stop_http_api() -> ok | {error, not_found}.
stop_http_api() ->
    case cowboy:stop_listener(?HTTP_API_LISTENER) of
        ok ->
            log(info, "FLURM Bridge HTTP API stopped", []),
            ok;
        {error, not_found} = Error ->
            Error
    end.

%% @doc Get information about the HTTP API listener.
-spec http_api_info() -> map() | {error, not_found}.
http_api_info() ->
    try
        {Port, _, _} = get_http_api_config(),
        #{
            port => Port,
            status => running
        }
    catch
        _:_ ->
            {error, not_found}
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    %% Base children - always started
    BaseChildren = [
        %% Connection Limiter - tracks per-peer connection limits
        %% Must start early as it's used by acceptors
        #{
            id => flurm_connection_limiter,
            start => {flurm_connection_limiter, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_connection_limiter]
        },
        %% Metrics Server - collects Prometheus metrics
        #{
            id => flurm_metrics,
            start => {flurm_metrics, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_metrics]
        },
        %% Metrics HTTP Server - exposes /metrics endpoint
        #{
            id => flurm_metrics_http,
            start => {flurm_metrics_http, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_metrics_http]
        },
        %% Limits Manager - tracks resource limits (must start before job_manager)
        #{
            id => flurm_limits,
            start => {flurm_limits, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_limits]
        },
        %% Node Registry - tracks node processes (needed by backfill scheduler)
        #{
            id => flurm_node_registry,
            start => {flurm_node_registry, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_node_registry]
        },
        %% Job Manager - handles job lifecycle
        #{
            id => flurm_job_manager,
            start => {flurm_job_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_job_manager]
        },
        %% Job Dependency Tracker - tracks inter-job dependencies
        #{
            id => flurm_job_deps,
            start => {flurm_job_deps, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_job_deps]
        },
        %% Step Manager - handles job steps within jobs
        #{
            id => flurm_step_manager,
            start => {flurm_step_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_step_manager]
        },
        %% Node Manager - tracks node status
        #{
            id => flurm_node_manager_server,
            start => {flurm_node_manager_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_node_manager_server]
        },
        %% Node Connection Manager - tracks active node connections
        #{
            id => flurm_node_connection_manager,
            start => {flurm_node_connection_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_node_connection_manager]
        },
        %% Job Dispatcher - dispatches jobs to nodes
        #{
            id => flurm_job_dispatcher_server,
            start => {flurm_job_dispatcher_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_job_dispatcher_server]
        },
        %% Scheduler - assigns jobs to nodes
        #{
            id => flurm_scheduler,
            start => {flurm_scheduler, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_scheduler]
        },
        %% Partition Manager - manages partitions
        #{
            id => flurm_partition_manager,
            start => {flurm_partition_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_partition_manager]
        },
        %% Account Manager - manages accounting entities
        #{
            id => flurm_account_manager,
            start => {flurm_account_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_account_manager]
        },
        %% srun Callback Manager - handles callbacks to srun clients
        #{
            id => flurm_srun_callback,
            start => {flurm_srun_callback, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_srun_callback]
        }
    ],

    %% Add cluster-related children if cluster mode is enabled
    ClusterChildren = case is_cluster_enabled() of
        true ->
            [
                %% Cluster Coordinator - handles leader election and forwarding
                #{
                    id => flurm_controller_cluster,
                    start => {flurm_controller_cluster, start_link, []},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [flurm_controller_cluster]
                },
                %% Failover Handler - handles failover scenarios
                #{
                    id => flurm_controller_failover,
                    start => {flurm_controller_failover, start_link, []},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [flurm_controller_failover]
                }
            ];
        false ->
            []
    end,

    Children = BaseChildren ++ ClusterChildren,
    {ok, {SupFlags, Children}}.

%% @doc Check if cluster mode is enabled.
%% Cluster mode is enabled by default since Ra consensus works even with
%% single-node clusters, providing persistence and state machine guarantees.
is_cluster_enabled() ->
    case application:get_env(flurm_controller, enable_cluster, true) of
        false ->
            false;
        _ ->
            %% Check if distributed Erlang is available
            case node() of
                'nonode@nohost' ->
                    lager:warning("Distributed Erlang not enabled, cluster mode disabled"),
                    false;
                _ ->
                    true
            end
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Get listener configuration from application environment.
-spec get_listener_config() -> {Port :: pos_integer(),
                                Address :: string(),
                                NumAcceptors :: pos_integer(),
                                MaxConnections :: pos_integer()}.
get_listener_config() ->
    Port = application:get_env(flurm_controller, listen_port, ?DEFAULT_PORT),
    Address = application:get_env(flurm_controller, listen_address, ?DEFAULT_ADDRESS),
    NumAcceptors = application:get_env(flurm_controller, num_acceptors, ?DEFAULT_NUM_ACCEPTORS),
    MaxConns = application:get_env(flurm_controller, max_connections, ?DEFAULT_MAX_CONNECTIONS),
    {Port, Address, NumAcceptors, MaxConns}.

%% @doc Get node listener configuration from application environment.
-spec get_node_listener_config() -> {Port :: pos_integer(),
                                     Address :: string(),
                                     NumAcceptors :: pos_integer(),
                                     MaxConnections :: pos_integer()}.
get_node_listener_config() ->
    Port = application:get_env(flurm_controller, node_listen_port, ?DEFAULT_NODE_PORT),
    Address = application:get_env(flurm_controller, listen_address, ?DEFAULT_ADDRESS),
    NumAcceptors = application:get_env(flurm_controller, num_acceptors, ?DEFAULT_NUM_ACCEPTORS),
    MaxConns = application:get_env(flurm_controller, max_node_connections, 500),
    {Port, Address, NumAcceptors, MaxConns}.

%% @doc Get HTTP API configuration from application environment.
-spec get_http_api_config() -> {Port :: pos_integer(),
                                Address :: string(),
                                NumAcceptors :: pos_integer()}.
get_http_api_config() ->
    Port = application:get_env(flurm_controller, http_api_port, ?DEFAULT_HTTP_API_PORT),
    Address = application:get_env(flurm_controller, listen_address, ?DEFAULT_ADDRESS),
    NumAcceptors = application:get_env(flurm_controller, num_acceptors, ?DEFAULT_NUM_ACCEPTORS),
    {Port, Address, NumAcceptors}.

%% @doc Parse IP address string to tuple.
-spec parse_address(string()) -> inet:ip_address().
parse_address("0.0.0.0") ->
    {0, 0, 0, 0};
parse_address("::") ->
    {0, 0, 0, 0, 0, 0, 0, 0};
parse_address(Address) when is_list(Address) ->
    case inet:parse_address(Address) of
        {ok, Addr} -> Addr;
        {error, _} ->
            log(warning, "Invalid address '~s', using 0.0.0.0", [Address]),
            {0, 0, 0, 0}
    end;
parse_address(Address) when is_tuple(Address) ->
    Address.

%% Logging helpers
log(Level, Fmt, Args) ->
    case Level of
        debug -> lager:debug(Fmt, Args);
        info -> lager:info(Fmt, Args);
        warning -> lager:warning(Fmt, Args);
        error -> lager:error(Fmt, Args)
    end.
