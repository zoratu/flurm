#!/usr/bin/env escript
%%! -pa _build/default/lib/*/ebin

%%%-------------------------------------------------------------------
%%% @doc FLURM Controller HA Startup Script
%%%
%%% Starts the FLURM controller in distributed mode with Ra consensus
%%% for high-availability multi-controller deployments.
%%%
%%% Environment variables:
%%%   FLURM_NODE_NAME    - Erlang node name (e.g., flurm@hostname)
%%%   FLURM_COOKIE       - Erlang cookie for cluster authentication
%%%   FLURM_CLUSTER_NODES - Comma-separated list of cluster nodes
%%%   FLURM_RA_DATA_DIR  - Directory for Ra data (default: /var/lib/flurm/ra)
%%%
%%% @end
%%%-------------------------------------------------------------------

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("  FLURM Controller (HA Mode)~n"),
    io:format("========================================~n~n"),

    %% Add code paths
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Get configuration from environment
    NodeName = get_env("FLURM_NODE_NAME", "flurm@localhost"),
    Cookie = get_env("FLURM_COOKIE", "flurm_secret"),
    ClusterNodesStr = get_env("FLURM_CLUSTER_NODES", ""),
    RaDataDir = get_env("FLURM_RA_DATA_DIR", "/var/lib/flurm/ra"),

    io:format("Configuration:~n"),
    io:format("  Node name: ~s~n", [NodeName]),
    io:format("  Cookie: ~s~n", [mask_cookie(Cookie)]),
    io:format("  Ra data dir: ~s~n", [RaDataDir]),

    %% Parse cluster nodes
    ClusterNodes = parse_cluster_nodes(ClusterNodesStr),
    io:format("  Cluster nodes: ~p~n~n", [ClusterNodes]),

    %% Start distributed Erlang
    case start_distribution(NodeName, Cookie) of
        ok ->
            io:format("[OK] Distributed Erlang started~n");
        {error, DistErr} ->
            io:format("[ERROR] Failed to start distribution: ~p~n", [DistErr]),
            halt(1)
    end,

    %% Set up Ra data directory
    application:load(ra),
    application:set_env(ra, data_dir, RaDataDir),

    %% Set up cluster configuration
    application:load(flurm_db),
    application:set_env(flurm_db, cluster_nodes, ClusterNodes),
    application:set_env(flurm_db, data_dir, RaDataDir),

    application:load(flurm_controller),
    application:set_env(flurm_controller, cluster_nodes, ClusterNodes),

    %% Start applications
    io:format("~nStarting applications...~n"),
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, info),

    case application:ensure_all_started(flurm_controller) of
        {ok, Started} ->
            io:format("[OK] Started: ~p~n", [Started]);
        {error, AppErr} ->
            io:format("[ERROR] Failed to start: ~p~n", [AppErr]),
            halt(1)
    end,

    %% Connect to other cluster nodes
    io:format("~nConnecting to cluster peers...~n"),
    connect_to_peers(ClusterNodes),

    %% Wait for cluster to stabilize
    timer:sleep(2000),

    %% Initialize or join Ra cluster
    io:format("~nInitializing Ra cluster...~n"),
    case init_ra_cluster(ClusterNodes) of
        ok ->
            io:format("[OK] Ra cluster initialized~n");
        {error, RaErr} ->
            io:format("[WARN] Ra cluster init: ~p (may already exist)~n", [RaErr])
    end,

    %% Print status
    io:format("~n========================================~n"),
    io:format("  FLURM Controller Ready (HA)~n"),
    io:format("  Node: ~s~n", [node()]),
    io:format("  SLURM port: 6817~n"),
    io:format("  Node port: 6818~n"),
    io:format("========================================~n~n"),

    %% Start monitoring loop
    monitor_loop(ClusterNodes).

%%====================================================================
%% Internal functions
%%====================================================================

get_env(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

mask_cookie(Cookie) ->
    case length(Cookie) of
        L when L > 4 ->
            lists:sublist(Cookie, 4) ++ "****";
        _ ->
            "****"
    end.

parse_cluster_nodes("") ->
    [node()];
parse_cluster_nodes(NodesStr) ->
    Nodes = string:tokens(NodesStr, ","),
    [list_to_atom(string:trim(N)) || N <- Nodes].

start_distribution(NodeNameStr, CookieStr) ->
    NodeName = list_to_atom(NodeNameStr),
    Cookie = list_to_atom(CookieStr),

    %% Check if already distributed
    case node() of
        'nonode@nohost' ->
            %% Start distribution with shortnames (Docker containers don't have FQDNs)
            case net_kernel:start([NodeName, shortnames]) of
                {ok, _} ->
                    erlang:set_cookie(node(), Cookie),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            %% Already distributed
            erlang:set_cookie(node(), Cookie),
            ok
    end.

connect_to_peers(ClusterNodes) ->
    MyNode = node(),
    Peers = [N || N <- ClusterNodes, N =/= MyNode],

    lists:foreach(fun(Peer) ->
        io:format("  Connecting to ~s... ", [Peer]),
        case net_kernel:connect_node(Peer) of
            true ->
                io:format("connected~n");
            false ->
                io:format("not available (will retry)~n");
            ignored ->
                io:format("ignored~n")
        end
    end, Peers).

init_ra_cluster(ClusterNodes) ->
    %% Try to start or join Ra cluster via flurm_db_cluster
    case code:ensure_loaded(flurm_db_cluster) of
        {module, _} ->
            try
                %% Check if cluster already exists
                case flurm_db_cluster:status() of
                    {ok, _Status} ->
                        io:format("  Ra cluster already running~n"),
                        ok;
                    {error, _} ->
                        %% Try to start new cluster
                        case flurm_db_cluster:start_cluster(ClusterNodes) of
                            ok -> ok;
                            {ok, _, _} -> ok;
                            {error, already_started} -> ok;
                            Other -> Other
                        end
                end
            catch
                _:Err ->
                    {error, Err}
            end;
        _ ->
            {error, module_not_loaded}
    end.

monitor_loop(ClusterNodes) ->
    receive
        stop ->
            io:format("Shutting down...~n"),
            ok
    after 30000 ->
        %% Periodic cluster status check
        print_cluster_status(ClusterNodes),
        monitor_loop(ClusterNodes)
    end.

print_cluster_status(ClusterNodes) ->
    ConnectedNodes = nodes(),
    MyNode = node(),

    %% Count connected cluster members
    ClusterConnected = [N || N <- ClusterNodes, N =:= MyNode orelse lists:member(N, ConnectedNodes)],

    io:format("[STATUS] ~s: ~p/~p cluster nodes connected~n",
              [format_time(), length(ClusterConnected), length(ClusterNodes)]),

    %% Check Ra leadership
    case catch flurm_db_cluster:is_leader() of
        true ->
            io:format("[STATUS] This node is the Ra leader~n");
        false ->
            case catch flurm_db_cluster:get_leader() of
                {ok, Leader} ->
                    io:format("[STATUS] Ra leader: ~p~n", [Leader]);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

format_time() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                  [Y, M, D, H, Mi, S]).
