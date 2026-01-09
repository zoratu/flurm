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

%% API for managing the listener
-export([start_listener/0, stop_listener/0, listener_info/0]).

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, flurm_controller_listener).

%% Default configuration
-define(DEFAULT_PORT, 6817).
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
        %% Job Manager - handles job lifecycle
        #{
            id => flurm_job_manager,
            start => {flurm_job_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_job_manager]
        },
        %% Node Manager - tracks node status
        #{
            id => flurm_node_manager,
            start => {flurm_node_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_node_manager]
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
        }
    ],

    {ok, {SupFlags, Children}}.

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
            lager:warning("Invalid address '~s', using 0.0.0.0", [Address]),
            {0, 0, 0, 0}
    end;
parse_address(Address) when is_tuple(Address) ->
    Address.
