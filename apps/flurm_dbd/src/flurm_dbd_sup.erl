%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Supervisor
%%%
%%% Supervises the accounting daemon components:
%%% - flurm_dbd_server: Main accounting server
%%% - flurm_dbd_storage: Storage backend
%%% - Ra cluster for distributed consensus accounting
%%% - Ranch listener for client connections
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup).

-behaviour(supervisor).

%% Suppress warnings for functions only exported during TEST
-ifndef(TEST).
-compile([{nowarn_unused_function, [{get_ra_cluster_nodes, 0}]}]).
-endif.

-export([start_link/0]).
-export([init/1]).

-export([start_listener/0, stop_listener/0, listener_info/0]).
-export([start_ra_cluster/0, start_ra_cluster/1, ra_cluster_info/0]).

-ifdef(TEST).
-export([
    get_listener_config/0,
    parse_address/1,
    get_ra_cluster_nodes/0
]).
-endif.

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, flurm_dbd_listener).
-define(DEFAULT_PORT, 6819).
-define(DEFAULT_ADDRESS, "0.0.0.0").
-define(DEFAULT_NUM_ACCEPTORS, 5).
-define(DEFAULT_MAX_CONNECTIONS, 100).
-define(RA_CLUSTER_NAME, flurm_dbd_ra).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start the Ranch TCP listener for client connections
-spec start_listener() -> {ok, pid()} | {error, term()}.
start_listener() ->
    {Port, Address, NumAcceptors, MaxConns} = get_listener_config(),
    TransportOpts = #{
        socket_opts => [
            {ip, parse_address(Address)},
            {port, Port},
            {nodelay, true},
            {keepalive, true},
            {reuseaddr, true},
            {backlog, 128}
        ],
        num_acceptors => NumAcceptors,
        max_connections => MaxConns
    },
    ProtocolOpts = #{},
    case ranch:start_listener(
        ?LISTENER_NAME,
        ranch_tcp,
        TransportOpts,
        flurm_dbd_acceptor,
        ProtocolOpts
    ) of
        {ok, Pid} ->
            lager:info("FLURM DBD listener started on ~s:~p "
                       "(acceptors=~p, max_connections=~p)",
                       [Address, Port, NumAcceptors, MaxConns]),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            lager:error("Failed to start DBD listener: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Stop the listener
-spec stop_listener() -> ok | {error, not_found}.
stop_listener() ->
    ranch:stop_listener(?LISTENER_NAME).

%% @doc Get listener info
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

%% @doc Start the Ra cluster for distributed accounting.
%% Uses the current node as the only cluster member.
-spec start_ra_cluster() -> ok | {error, term()}.
start_ra_cluster() ->
    start_ra_cluster([node()]).

%% @doc Start the Ra cluster with specified nodes.
%% Nodes is a list of Erlang nodes that will form the Ra cluster.
-spec start_ra_cluster([node()]) -> ok | {error, term()}.
start_ra_cluster(Nodes) when is_list(Nodes) ->
    %% Get configured nodes or use provided list
    ClusterNodes = case application:get_env(flurm_dbd, ra_cluster_nodes) of
        {ok, ConfiguredNodes} when is_list(ConfiguredNodes), ConfiguredNodes =/= [] ->
            ConfiguredNodes;
        _ ->
            Nodes
    end,

    lager:info("Starting FLURM DBD Ra cluster with nodes: ~p", [ClusterNodes]),

    case flurm_dbd_ra:start_cluster(ClusterNodes) of
        ok ->
            lager:info("FLURM DBD Ra cluster started successfully"),
            ok;
        {error, Reason} ->
            lager:error("Failed to start FLURM DBD Ra cluster: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Get Ra cluster nodes from configuration.
%% Returns the list of nodes configured for the Ra cluster.
-spec get_ra_cluster_nodes() -> [node()].
get_ra_cluster_nodes() ->
    case application:get_env(flurm_dbd, ra_cluster_nodes) of
        {ok, Nodes} when is_list(Nodes), Nodes =/= [] ->
            Nodes;
        _ ->
            [node()]
    end.

%% @doc Get Ra cluster information.
-spec ra_cluster_info() -> map() | {error, term()}.
ra_cluster_info() ->
    try
        Server = {?RA_CLUSTER_NAME, node()},
        case ra:members(Server) of
            {ok, Members, Leader} ->
                #{
                    cluster_name => ?RA_CLUSTER_NAME,
                    members => Members,
                    leader => Leader,
                    local_node => node(),
                    status => running
                };
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:Error ->
            {error, Error}
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

    Children = [
        %% Storage backend
        #{
            id => flurm_dbd_storage,
            start => {flurm_dbd_storage, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_dbd_storage]
        },
        %% Main DBD server
        #{
            id => flurm_dbd_server,
            start => {flurm_dbd_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_dbd_server]
        }
    ],

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

get_listener_config() ->
    Port = application:get_env(flurm_dbd, listen_port, ?DEFAULT_PORT),
    Address = application:get_env(flurm_dbd, listen_address, ?DEFAULT_ADDRESS),
    NumAcceptors = application:get_env(flurm_dbd, num_acceptors, ?DEFAULT_NUM_ACCEPTORS),
    MaxConns = application:get_env(flurm_dbd, max_connections, ?DEFAULT_MAX_CONNECTIONS),
    {Port, Address, NumAcceptors, MaxConns}.

parse_address("0.0.0.0") -> {0, 0, 0, 0};
parse_address("::") -> {0, 0, 0, 0, 0, 0, 0, 0};
parse_address(Address) when is_list(Address) ->
    case inet:parse_address(Address) of
        {ok, Addr} -> Addr;
        {error, _} -> {0, 0, 0, 0}
    end;
parse_address(Address) when is_tuple(Address) -> Address.
