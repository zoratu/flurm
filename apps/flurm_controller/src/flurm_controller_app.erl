%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Application
%%%
%%% This module implements the OTP application behaviour for the
%%% FLURM controller daemon (flurmctld). It is the central controller
%%% responsible for job scheduling, node management, and cluster
%%% coordination.
%%%
%%% The application manages:
%%% - TCP listener for SLURM protocol connections (port 6817 default)
%%% - Job manager for job lifecycle
%%% - Node manager for compute node tracking
%%% - Partition manager for queue management
%%% - Scheduler for job-to-node assignment
%%% - Cluster coordination (Ra-based) for high availability
%%%
%%% Configuration (via application environment):
%%% - listen_port: TCP port (default: 6817)
%%% - listen_address: Bind address (default: "0.0.0.0")
%%% - num_acceptors: Number of acceptor processes (default: 10)
%%% - max_connections: Maximum concurrent connections (default: 1000)
%%% - cluster_name: Name of the Ra cluster (default: flurm)
%%% - cluster_nodes: List of nodes in the cluster
%%% - ra_data_dir: Directory for Ra data (default: /var/lib/flurm/ra)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_app).

-behaviour(application).

-export([start/2, stop/1, prep_stop/1]).

%% API exports
-export([status/0, config/0, cluster_status/0]).

-ifdef(TEST).
-export([get_config/2,
         count_jobs_by_state/2,
         count_nodes_by_state/2]).
-endif.

%%====================================================================
%% Application callbacks
%%====================================================================

%% @doc Start the FLURM controller application.
%% Initializes configuration, starts supervisor tree, initializes
%% the Ra cluster (if configured), and launches the Ranch TCP listeners.
start(_StartType, _StartArgs) ->
    log(info, "========================================"),
    log(info, "Starting FLURM Controller (flurmctld)"),
    log(info, "========================================"),

    %% Validate configuration before proceeding
    case validate_config() of
        ok ->
            start_validated();
        {error, ConfigErrors} ->
            lists:foreach(fun(Err) ->
                log(error, "Configuration error: ~s", [Err])
            end, ConfigErrors),
            {error, {config_validation_failed, ConfigErrors}}
    end.

%% @doc Continue startup after config validation passes.
start_validated() ->
    %% Log configuration
    log_startup_config(),

    %% Setup distributed Erlang if cluster mode is enabled
    maybe_setup_distributed(),

    %% Start the supervisor
    case flurm_controller_sup:start_link() of
        {ok, Pid} ->
            %% Start Ranch listener for client connections (SLURM protocol)
            case flurm_controller_sup:start_listener() of
                {ok, _ListenerPid} ->
                    ok;
                {error, Reason} ->
                    log(error, "Failed to start client listener: ~p", [Reason])
            end,
            %% Start Ranch listener for node daemon connections (internal protocol)
            case flurm_controller_sup:start_node_listener() of
                {ok, _NodeListenerPid} ->
                    ok;
                {error, NodeReason} ->
                    log(error, "Failed to start node listener: ~p", [NodeReason])
            end,
            %% Start Cowboy HTTP API for bridge management
            case application:get_env(flurm_controller, enable_bridge_api, true) of
                true ->
                    case flurm_controller_sup:start_http_api() of
                        {ok, _HttpPid} ->
                            ok;
                        {error, HttpReason} ->
                            log(error, "Failed to start HTTP API: ~p", [HttpReason])
                    end;
                false ->
                    log(info, "Bridge HTTP API disabled by configuration")
            end,
            log_startup_complete(),
            {ok, Pid};
        {error, Reason} = Error ->
            log(error, "Failed to start supervisor: ~p", [Reason]),
            Error
    end.

%% @doc Prepare for application stop.
%% Performs graceful shutdown of listeners before supervisor stops.
prep_stop(State) ->
    log(info, "Preparing to stop FLURM Controller..."),
    %% Stop accepting new connections
    _ = flurm_controller_sup:stop_listener(),
    _ = flurm_controller_sup:stop_node_listener(),
    _ = flurm_controller_sup:stop_http_api(),
    %% Allow time for in-flight requests to complete (configurable)
    DrainTimeout = get_config(shutdown_drain_timeout, 5000),
    log(info, "Waiting ~p ms for in-flight requests to drain...", [DrainTimeout]),
    wait_for_connections_to_drain(DrainTimeout),
    State.

%% @doc Wait for connections to drain with timeout.
wait_for_connections_to_drain(Timeout) ->
    Start = erlang:monotonic_time(millisecond),
    wait_for_connections_to_drain(Start, Timeout).

wait_for_connections_to_drain(Start, Timeout) ->
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    case Elapsed >= Timeout of
        true ->
            %% Timeout reached, check final state
            case flurm_controller_sup:listener_info() of
                {error, not_found} ->
                    log(info, "All listeners stopped");
                #{active_connections := 0} ->
                    log(info, "All connections drained");
                #{active_connections := N} ->
                    log(warning, "Timeout: ~p connections still active", [N])
            end;
        false ->
            case flurm_controller_sup:listener_info() of
                {error, not_found} ->
                    ok;  % Listener already stopped
                #{active_connections := 0} ->
                    ok;  % All drained
                #{active_connections := _N} ->
                    timer:sleep(100),  % Wait 100ms and check again
                    wait_for_connections_to_drain(Start, Timeout)
            end
    end.

%% @doc Stop the FLURM controller application.
stop(_State) ->
    log(info, "FLURM Controller stopped"),
    ok.

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Get controller status information.
-spec status() -> map().
status() ->
    ListenerInfo = case flurm_controller_sup:listener_info() of
        {error, not_found} ->
            #{status => stopped};
        Info ->
            Info
    end,
    NodeListenerInfo = case flurm_controller_sup:node_listener_info() of
        {error, not_found} ->
            #{status => stopped};
        NInfo ->
            NInfo
    end,
    #{
        application => flurm_controller,
        status => running,
        listener => ListenerInfo,
        node_listener => NodeListenerInfo,
        jobs => job_stats(),
        nodes => node_stats(),
        partitions => partition_stats()
    }.

%% @doc Get current configuration.
-spec config() -> map().
config() ->
    #{
        listen_port => get_config(listen_port, 6817),
        listen_address => get_config(listen_address, "0.0.0.0"),
        num_acceptors => get_config(num_acceptors, 10),
        max_connections => get_config(max_connections, 1000),
        cluster_name => get_config(cluster_name, flurm),
        cluster_nodes => get_config(cluster_nodes, [node()]),
        ra_data_dir => get_config(ra_data_dir, "/var/lib/flurm/ra")
    }.

%% @doc Get cluster status information.
-spec cluster_status() -> map().
cluster_status() ->
    try
        flurm_controller_cluster:cluster_status()
    catch
        _:_ ->
            #{status => not_available, cluster_enabled => false}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Validate configuration before startup.
%% Returns ok if all configuration is valid, or {error, [Errors]} with
%% a list of human-readable error messages.
-spec validate_config() -> ok | {error, [string()]}.
validate_config() ->
    Validators = [
        fun validate_listen_port/0,
        fun validate_listen_address/0,
        fun validate_num_acceptors/0,
        fun validate_max_connections/0,
        fun validate_ra_data_dir/0,
        fun validate_http_api_port/0
    ],
    Errors = lists:filtermap(fun(V) ->
        case V() of
            ok -> false;
            {error, Msg} -> {true, Msg}
        end
    end, Validators),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

%% Configuration validators
validate_listen_port() ->
    Port = get_config(listen_port, 6817),
    case is_integer(Port) andalso Port >= 1 andalso Port =< 65535 of
        true -> ok;
        false -> {error, io_lib:format("listen_port must be 1-65535, got: ~p", [Port])}
    end.

validate_listen_address() ->
    Addr = get_config(listen_address, "0.0.0.0"),
    case Addr of
        A when is_list(A) ->
            case inet:parse_address(A) of
                {ok, _} -> ok;
                {error, _} -> {error, io_lib:format("listen_address invalid: ~s", [A])}
            end;
        _ ->
            {error, io_lib:format("listen_address must be a string, got: ~p", [Addr])}
    end.

validate_num_acceptors() ->
    N = get_config(num_acceptors, 10),
    case is_integer(N) andalso N >= 1 andalso N =< 1000 of
        true -> ok;
        false -> {error, io_lib:format("num_acceptors must be 1-1000, got: ~p", [N])}
    end.

validate_max_connections() ->
    Max = get_config(max_connections, 1000),
    case is_integer(Max) andalso Max >= 10 andalso Max =< 100000 of
        true -> ok;
        false -> {error, io_lib:format("max_connections must be 10-100000, got: ~p", [Max])}
    end.

validate_ra_data_dir() ->
    Dir = get_config(ra_data_dir, "/var/lib/flurm/ra"),
    ClusterNodes = get_config(cluster_nodes, [node()]),
    case length(ClusterNodes) > 1 of
        false ->
            ok;  % Ra not used in single-node mode
        true ->
            case filelib:is_dir(Dir) of
                true -> ok;
                false ->
                    %% Try to create the directory
                    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
                        ok -> ok;
                        {error, Reason} ->
                            {error, io_lib:format("ra_data_dir ~s cannot be created: ~p", [Dir, Reason])}
                    end
            end
    end.

validate_http_api_port() ->
    case get_config(enable_bridge_api, true) of
        false -> ok;
        true ->
            Port = get_config(http_api_port, 6820),
            case is_integer(Port) andalso Port >= 1 andalso Port =< 65535 of
                true -> ok;
                false -> {error, io_lib:format("http_api_port must be 1-65535, got: ~p", [Port])}
            end
    end.

%% @doc Log configuration at startup.
log_startup_config() ->
    Config = config(),
    log(info, "Configuration:"),
    log(info, "  Listen address: ~s", [maps:get(listen_address, Config)]),
    log(info, "  Listen port: ~p", [maps:get(listen_port, Config)]),
    log(info, "  Acceptors: ~p", [maps:get(num_acceptors, Config)]),
    log(info, "  Max connections: ~p", [maps:get(max_connections, Config)]),
    %% Log cluster configuration
    ClusterNodes = maps:get(cluster_nodes, Config),
    case length(ClusterNodes) > 1 of
        true ->
            log(info, "  Cluster mode: ENABLED"),
            log(info, "  Cluster name: ~p", [maps:get(cluster_name, Config)]),
            log(info, "  Cluster nodes: ~p", [ClusterNodes]),
            log(info, "  Ra data dir: ~s", [maps:get(ra_data_dir, Config)]);
        false ->
            log(info, "  Cluster mode: DISABLED (single node)")
    end.

%% @doc Setup distributed Erlang if cluster mode is enabled.
maybe_setup_distributed() ->
    ClusterNodes = get_config(cluster_nodes, [node()]),
    case length(ClusterNodes) > 1 of
        true ->
            log(info, "Setting up distributed Erlang for cluster mode"),
            %% Ensure this node is distributed
            case node() of
                'nonode@nohost' ->
                    log(warning, "Node not distributed! Cluster mode requires "
                                  "distributed Erlang. Start with -name or -sname");
                _ ->
                    %% Connect to other cluster nodes
                    connect_to_cluster_nodes(ClusterNodes)
            end;
        false ->
            log(info, "Single node mode, skipping distributed setup"),
            ok
    end.

%% @doc Connect to other nodes in the cluster.
connect_to_cluster_nodes(ClusterNodes) ->
    OtherNodes = [N || N <- ClusterNodes, N =/= node()],
    log(info, "Attempting to connect to cluster nodes: ~p", [OtherNodes]),
    Results = [{N, net_kernel:connect_node(N)} || N <- OtherNodes],
    lists:foreach(
        fun({N, true}) ->
            log(info, "Connected to cluster node: ~p", [N]);
           ({N, false}) ->
            log(warning, "Failed to connect to cluster node: ~p", [N]);
           ({N, ignored}) ->
            log(debug, "Connection to ~p ignored (already connected)", [N])
        end, Results),
    ok.

%% @doc Log startup completion.
log_startup_complete() ->
    Config = config(),
    NodePort = get_config(node_listen_port, 6818),
    HttpApiPort = get_config(http_api_port, 6820),
    log(info, "----------------------------------------"),
    log(info, "FLURM Controller ready"),
    log(info, "SLURM clients: ~s:~p",
               [maps:get(listen_address, Config),
                maps:get(listen_port, Config)]),
    log(info, "Node daemons: ~s:~p",
               [maps:get(listen_address, Config),
                NodePort]),
    case get_config(enable_bridge_api, true) of
        true ->
            log(info, "Bridge HTTP API: ~s:~p",
                       [maps:get(listen_address, Config),
                        HttpApiPort]);
        false ->
            ok
    end,
    log(info, "----------------------------------------").

%% @doc Get configuration value with default.
get_config(Key, Default) ->
    application:get_env(flurm_controller, Key, Default).

%% @doc Get job statistics.
job_stats() ->
    try
        Jobs = flurm_job_manager:list_jobs(),
        #{
            total => length(Jobs),
            pending => count_jobs_by_state(Jobs, pending),
            running => count_jobs_by_state(Jobs, running),
            completed => count_jobs_by_state(Jobs, completed)
        }
    catch
        _:_ -> #{error => not_available}
    end.

%% @doc Count jobs by state.
count_jobs_by_state(Jobs, State) ->
    length([J || J <- Jobs, element(5, J) =:= State]).

%% @doc Get node statistics.
node_stats() ->
    try
        Nodes = flurm_node_manager_server:list_nodes(),
        #{
            total => length(Nodes),
            up => count_nodes_by_state(Nodes, idle) +
                  count_nodes_by_state(Nodes, allocated) +
                  count_nodes_by_state(Nodes, mixed),
            down => count_nodes_by_state(Nodes, down)
        }
    catch
        _:_ -> #{error => not_available}
    end.

%% @doc Count nodes by state.
count_nodes_by_state(Nodes, State) ->
    length([N || N <- Nodes, element(4, N) =:= State]).

%% @doc Get partition statistics.
partition_stats() ->
    try
        Partitions = flurm_partition_manager:list_partitions(),
        #{
            total => length(Partitions),
            names => [element(2, P) || P <- Partitions]
        }
    catch
        _:_ -> #{error => not_available}
    end.

%%====================================================================
%% Logging helpers (avoids lager parse transform dependency)
%%====================================================================

log(Level, Fmt) ->
    log(Level, Fmt, []).

log(Level, Fmt, Args) ->
    case Level of
        debug -> lager:debug(Fmt, Args);
        info -> lager:info(Fmt, Args);
        warning -> lager:warning(Fmt, Args);
        error -> lager:error(Fmt, Args)
    end.
