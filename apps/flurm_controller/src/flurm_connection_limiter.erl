%%%-------------------------------------------------------------------
%%% @doc FLURM Connection Rate Limiter
%%%
%%% Tracks active connections per peer IP and enforces per-peer limits
%%% to prevent a single client from consuming all resources.
%%%
%%% This module uses an ETS table for O(1) lookups and atomic updates.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_connection_limiter).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([connection_allowed/1, connection_opened/1, connection_closed/1]).
-export([get_connection_count/1, get_all_counts/0, get_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(ETS_TABLE, flurm_conn_limits).

%% Default limits - can be overridden via application config
-define(DEFAULT_MAX_CONNECTIONS_PER_PEER, 100).
-define(DEFAULT_CLEANUP_INTERVAL, 60000).  % 60 seconds

-record(state, {
    cleanup_timer :: reference() | undefined,
    max_per_peer :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Check if a new connection from the given IP is allowed.
%% Returns true if under the limit, false otherwise.
-spec connection_allowed(inet:ip_address()) -> boolean().
connection_allowed(IP) ->
    MaxPerPeer = get_max_per_peer(),
    case ets:lookup(?ETS_TABLE, IP) of
        [] -> true;  % No existing connections
        [{IP, Count}] -> Count < MaxPerPeer
    end.

%% @doc Record that a connection was opened from the given IP.
%% Call this after accepting a connection.
-spec connection_opened(inet:ip_address()) -> ok.
connection_opened(IP) ->
    try
        ets:update_counter(?ETS_TABLE, IP, {2, 1}, {IP, 0})
    catch
        error:badarg ->
            %% ETS table doesn't exist yet (during startup race)
            ok
    end,
    ok.

%% @doc Record that a connection was closed from the given IP.
%% Call this when a connection terminates.
-spec connection_closed(inet:ip_address()) -> ok.
connection_closed(IP) ->
    try
        case ets:update_counter(?ETS_TABLE, IP, {2, -1, 0, 0}) of
            0 ->
                %% Count dropped to zero, remove the entry
                ets:delete(?ETS_TABLE, IP);
            _ ->
                ok
        end
    catch
        error:badarg ->
            %% ETS table doesn't exist or entry not found
            ok
    end,
    ok.

%% @doc Get the current connection count for an IP.
-spec get_connection_count(inet:ip_address()) -> non_neg_integer().
get_connection_count(IP) ->
    case ets:lookup(?ETS_TABLE, IP) of
        [] -> 0;
        [{IP, Count}] -> Count
    end.

%% @doc Get all connection counts (for debugging/monitoring).
-spec get_all_counts() -> [{inet:ip_address(), non_neg_integer()}].
get_all_counts() ->
    ets:tab2list(?ETS_TABLE).

%% @doc Get statistics about connection tracking.
-spec get_stats() -> map().
get_stats() ->
    All = get_all_counts(),
    TotalPeers = length(All),
    TotalConns = lists:sum([C || {_, C} <- All]),
    MaxConns = case All of
        [] -> 0;
        _ -> lists:max([C || {_, C} <- All])
    end,
    #{
        total_peers => TotalPeers,
        total_connections => TotalConns,
        max_per_peer => MaxConns,
        limit_per_peer => get_max_per_peer()
    }.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for connection tracking
    ?ETS_TABLE = ets:new(?ETS_TABLE, [
        named_table,
        public,                % Allow access from acceptor processes
        set,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),

    MaxPerPeer = application:get_env(flurm_controller, max_connections_per_peer,
                                     ?DEFAULT_MAX_CONNECTIONS_PER_PEER),
    lager:info("Connection limiter started (max ~p connections per peer)", [MaxPerPeer]),

    %% Start periodic cleanup to remove stale entries
    CleanupInterval = application:get_env(flurm_controller, conn_cleanup_interval,
                                          ?DEFAULT_CLEANUP_INTERVAL),
    TimerRef = erlang:send_after(CleanupInterval, self(), cleanup),

    {ok, #state{cleanup_timer = TimerRef, max_per_peer = MaxPerPeer}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(cleanup, State) ->
    %% Remove entries with zero connections (shouldn't happen normally,
    %% but handles edge cases with crashes)
    ets:select_delete(?ETS_TABLE, [{{'_', 0}, [], [true]}]),

    %% Reschedule cleanup
    CleanupInterval = application:get_env(flurm_controller, conn_cleanup_interval,
                                          ?DEFAULT_CLEANUP_INTERVAL),
    TimerRef = erlang:send_after(CleanupInterval, self(), cleanup),
    {noreply, State#state{cleanup_timer = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{cleanup_timer = TimerRef}) ->
    case TimerRef of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

get_max_per_peer() ->
    application:get_env(flurm_controller, max_connections_per_peer,
                        ?DEFAULT_MAX_CONNECTIONS_PER_PEER).
