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

%%====================================================================
%% Application callbacks
%%====================================================================

%% @doc Start the FLURM controller application.
%% Initializes configuration, starts supervisor tree, initializes
%% the Ra cluster (if configured), and launches the Ranch TCP listener.
start(_StartType, _StartArgs) ->
    lager:info("========================================"),
    lager:info("Starting FLURM Controller (flurmctld)"),
    lager:info("========================================"),

    %% Log configuration
    log_startup_config(),

    %% Setup distributed Erlang if cluster mode is enabled
    maybe_setup_distributed(),

    %% Start the supervisor
    case flurm_controller_sup:start_link() of
        {ok, Pid} ->
            %% Start Ranch listener for client connections
            case flurm_controller_sup:start_listener() of
                {ok, _ListenerPid} ->
                    log_startup_complete(),
                    {ok, Pid};
                {error, Reason} ->
                    lager:error("Failed to start listener: ~p", [Reason]),
                    %% Still return ok - the app can run without listener
                    %% and retry later
                    {ok, Pid}
            end;
        {error, Reason} = Error ->
            lager:error("Failed to start supervisor: ~p", [Reason]),
            Error
    end.

%% @doc Prepare for application stop.
%% Performs graceful shutdown of listener before supervisor stops.
prep_stop(State) ->
    lager:info("Preparing to stop FLURM Controller..."),
    %% Stop accepting new connections
    _ = flurm_controller_sup:stop_listener(),
    %% Allow time for in-flight requests to complete
    timer:sleep(1000),
    State.

%% @doc Stop the FLURM controller application.
stop(_State) ->
    lager:info("FLURM Controller stopped"),
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
    #{
        application => flurm_controller,
        status => running,
        listener => ListenerInfo,
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

%% @doc Log configuration at startup.
log_startup_config() ->
    Config = config(),
    lager:info("Configuration:"),
    lager:info("  Listen address: ~s", [maps:get(listen_address, Config)]),
    lager:info("  Listen port: ~p", [maps:get(listen_port, Config)]),
    lager:info("  Acceptors: ~p", [maps:get(num_acceptors, Config)]),
    lager:info("  Max connections: ~p", [maps:get(max_connections, Config)]),
    %% Log cluster configuration
    ClusterNodes = maps:get(cluster_nodes, Config),
    case length(ClusterNodes) > 1 of
        true ->
            lager:info("  Cluster mode: ENABLED"),
            lager:info("  Cluster name: ~p", [maps:get(cluster_name, Config)]),
            lager:info("  Cluster nodes: ~p", [ClusterNodes]),
            lager:info("  Ra data dir: ~s", [maps:get(ra_data_dir, Config)]);
        false ->
            lager:info("  Cluster mode: DISABLED (single node)")
    end.

%% @doc Setup distributed Erlang if cluster mode is enabled.
maybe_setup_distributed() ->
    ClusterNodes = get_config(cluster_nodes, [node()]),
    case length(ClusterNodes) > 1 of
        true ->
            lager:info("Setting up distributed Erlang for cluster mode"),
            %% Ensure this node is distributed
            case node() of
                'nonode@nohost' ->
                    lager:warning("Node not distributed! Cluster mode requires "
                                  "distributed Erlang. Start with -name or -sname");
                _ ->
                    %% Connect to other cluster nodes
                    connect_to_cluster_nodes(ClusterNodes)
            end;
        false ->
            lager:info("Single node mode, skipping distributed setup"),
            ok
    end.

%% @doc Connect to other nodes in the cluster.
connect_to_cluster_nodes(ClusterNodes) ->
    OtherNodes = [N || N <- ClusterNodes, N =/= node()],
    lager:info("Attempting to connect to cluster nodes: ~p", [OtherNodes]),
    Results = [{N, net_kernel:connect_node(N)} || N <- OtherNodes],
    lists:foreach(
        fun({N, true}) ->
            lager:info("Connected to cluster node: ~p", [N]);
           ({N, false}) ->
            lager:warning("Failed to connect to cluster node: ~p", [N]);
           ({N, ignored}) ->
            lager:debug("Connection to ~p ignored (already connected)", [N])
        end, Results),
    ok.

%% @doc Log startup completion.
log_startup_complete() ->
    Config = config(),
    lager:info("----------------------------------------"),
    lager:info("FLURM Controller ready"),
    lager:info("Listening on ~s:~p",
               [maps:get(listen_address, Config),
                maps:get(listen_port, Config)]),
    lager:info("----------------------------------------").

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
        Nodes = flurm_node_manager:list_nodes(),
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
