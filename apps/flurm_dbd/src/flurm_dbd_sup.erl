%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Supervisor
%%%
%%% Supervises the accounting daemon components:
%%% - flurm_dbd_server: Main accounting server
%%% - flurm_dbd_storage: Storage backend
%%% - Ranch listener for client connections
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-export([start_listener/0, stop_listener/0, listener_info/0]).

-ifdef(TEST).
-export([
    get_listener_config/0,
    parse_address/1
]).
-endif.

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, flurm_dbd_listener).
-define(DEFAULT_PORT, 6819).
-define(DEFAULT_ADDRESS, "0.0.0.0").
-define(DEFAULT_NUM_ACCEPTORS, 5).
-define(DEFAULT_MAX_CONNECTIONS, 100).

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
