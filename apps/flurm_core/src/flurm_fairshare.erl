%%%-------------------------------------------------------------------
%%% @doc FLURM Fair-Share Scheduling
%%%
%%% Tracks resource usage per user/account and adjusts job priorities
%%% to ensure fair distribution of cluster resources over time.
%%%
%%% Key concepts:
%%% - RawShares: Configured share allocation for user/account
%%% - RawUsage: Actual resources consumed (CPU-seconds)
%%% - EffectiveUsage: Decayed usage over time window
%%% - FairShare Factor: Priority multiplier based on usage vs shares
%%%
%%% Users who have used less than their fair share get priority boost.
%%% Users who have used more get priority penalty.
%%%
%%% Based on SLURM's fair-share scheduling algorithm.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare).

-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    get_priority_factor/2,
    record_usage/4,
    get_usage/2,
    set_shares/3,
    get_shares/2,
    decay_usage/0,
    get_all_accounts/0,
    reset_usage/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

%% ETS tables
-define(USAGE_TABLE, flurm_fairshare_usage).
-define(SHARES_TABLE, flurm_fairshare_shares).

%% Decay period (how often to decay usage)
-define(DECAY_PERIOD, 3600000).  % 1 hour in milliseconds

%% Decay half-life (usage halves every this many seconds)
-define(DECAY_HALFLIFE, 604800).  % 7 days

%% Default shares for new accounts
-define(DEFAULT_SHARES, 1).

%% State record
-record(state, {
    decay_timer :: reference() | undefined,
    total_shares :: non_neg_integer(),
    total_usage :: float()
}).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    calculate_priority_factor/3
]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the fair-share server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get the fair-share priority factor for a user/account.
%% Returns a float from 0.0 to 1.0 where:
%% - 1.0 = user has used much less than their share (high priority)
%% - 0.5 = user has used exactly their share (normal priority)
%% - 0.0 = user has used much more than their share (low priority)
-spec get_priority_factor(binary(), binary()) -> float().
get_priority_factor(User, Account) ->
    gen_server:call(?SERVER, {get_priority_factor, User, Account}).

%% @doc Record resource usage for a completed job.
%% CpuSeconds = num_cpus * wall_time_seconds
-spec record_usage(binary(), binary(), pos_integer(), pos_integer()) -> ok.
record_usage(User, Account, CpuSeconds, WallTimeSeconds) ->
    gen_server:cast(?SERVER, {record_usage, User, Account, CpuSeconds, WallTimeSeconds}).

%% @doc Get current usage for a user/account.
-spec get_usage(binary(), binary()) -> {ok, float()} | {error, not_found}.
get_usage(User, Account) ->
    gen_server:call(?SERVER, {get_usage, User, Account}).

%% @doc Set shares for an account.
-spec set_shares(binary(), binary(), pos_integer()) -> ok.
set_shares(User, Account, Shares) ->
    gen_server:call(?SERVER, {set_shares, User, Account, Shares}).

%% @doc Get shares for a user/account.
-spec get_shares(binary(), binary()) -> {ok, pos_integer()}.
get_shares(User, Account) ->
    gen_server:call(?SERVER, {get_shares, User, Account}).

%% @doc Manually trigger usage decay.
-spec decay_usage() -> ok.
decay_usage() ->
    gen_server:cast(?SERVER, decay_usage).

%% @doc Get all accounts with their usage and shares.
-spec get_all_accounts() -> [{binary(), binary(), float(), pos_integer()}].
get_all_accounts() ->
    gen_server:call(?SERVER, get_all_accounts).

%% @doc Reset usage for a user/account.
-spec reset_usage(binary(), binary()) -> ok.
reset_usage(User, Account) ->
    gen_server:cast(?SERVER, {reset_usage, User, Account}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?USAGE_TABLE, [named_table, public, set]),
    ets:new(?SHARES_TABLE, [named_table, public, set]),

    %% Start decay timer
    Timer = erlang:send_after(?DECAY_PERIOD, self(), decay),

    {ok, #state{
        decay_timer = Timer,
        total_shares = 0,
        total_usage = 0.0
    }}.

handle_call({get_priority_factor, User, Account}, _From, State) ->
    Factor = calculate_priority_factor(User, Account, State),
    {reply, Factor, State};

handle_call({get_usage, User, Account}, _From, State) ->
    Key = {User, Account},
    Result = case ets:lookup(?USAGE_TABLE, Key) of
        [{Key, Usage, _LastUpdate}] -> {ok, Usage};
        [] -> {ok, 0.0}
    end,
    {reply, Result, State};

handle_call({set_shares, User, Account, Shares}, _From, State) ->
    Key = {User, Account},
    OldShares = case ets:lookup(?SHARES_TABLE, Key) of
        [{Key, S}] -> S;
        [] -> 0
    end,
    ets:insert(?SHARES_TABLE, {Key, Shares}),
    NewTotalShares = State#state.total_shares - OldShares + Shares,
    {reply, ok, State#state{total_shares = NewTotalShares}};

handle_call({get_shares, User, Account}, _From, State) ->
    Key = {User, Account},
    Shares = case ets:lookup(?SHARES_TABLE, Key) of
        [{Key, S}] -> S;
        [] -> ?DEFAULT_SHARES
    end,
    {reply, {ok, Shares}, State};

handle_call(get_all_accounts, _From, State) ->
    %% Combine usage and shares data
    AllUsage = ets:tab2list(?USAGE_TABLE),
    AllShares = ets:tab2list(?SHARES_TABLE),

    %% Build combined list
    SharesMap = maps:from_list([{{U, A}, S} || {{U, A}, S} <- AllShares]),

    Accounts = lists:map(
        fun({{User, Account}, Usage, _}) ->
            Shares = maps:get({User, Account}, SharesMap, ?DEFAULT_SHARES),
            {User, Account, Usage, Shares}
        end,
        AllUsage
    ),
    {reply, Accounts, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({record_usage, User, Account, CpuSeconds, _WallTimeSeconds}, State) ->
    Key = {User, Account},
    Now = erlang:system_time(second),

    %% Get current usage
    {OldUsage, _} = case ets:lookup(?USAGE_TABLE, Key) of
        [{Key, U, _}] -> {U, ok};
        [] -> {0.0, ok}
    end,

    %% Add new usage
    NewUsage = OldUsage + CpuSeconds,
    ets:insert(?USAGE_TABLE, {Key, NewUsage, Now}),

    NewState = State#state{
        total_usage = State#state.total_usage + CpuSeconds
    },
    {noreply, NewState};

handle_cast(decay_usage, State) ->
    NewState = do_decay_usage(State),
    {noreply, NewState};

handle_cast({reset_usage, User, Account}, State) ->
    Key = {User, Account},
    case ets:lookup(?USAGE_TABLE, Key) of
        [{Key, OldUsage, _}] ->
            ets:delete(?USAGE_TABLE, Key),
            {noreply, State#state{
                total_usage = max(0.0, State#state.total_usage - OldUsage)
            }};
        [] ->
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(decay, State) ->
    %% Decay all usage values
    NewState = do_decay_usage(State),

    %% Schedule next decay
    Timer = erlang:send_after(?DECAY_PERIOD, self(), decay),
    {noreply, NewState#state{decay_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.decay_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Calculate the fair-share priority factor for a user/account.
%% Uses the formula: F = 2^(-EffectiveUsage / Shares)
%% This gives exponential decay as usage exceeds shares.
calculate_priority_factor(User, Account, State) ->
    Key = {User, Account},

    %% Get user's usage
    Usage = case ets:lookup(?USAGE_TABLE, Key) of
        [{Key, U, _}] -> U;
        [] -> 0.0
    end,

    %% Get user's shares
    Shares = case ets:lookup(?SHARES_TABLE, Key) of
        [{Key, S}] -> S;
        [] -> ?DEFAULT_SHARES
    end,

    %% Get total shares and usage for normalization
    TotalShares = max(1, State#state.total_shares +
        case ets:lookup(?SHARES_TABLE, Key) of [] -> ?DEFAULT_SHARES; _ -> 0 end),
    TotalUsage = max(1.0, State#state.total_usage),

    %% Normalize usage and shares to fractions
    ShareFraction = Shares / TotalShares,
    UsageFraction = Usage / TotalUsage,

    %% Calculate fair-share factor
    %% If usage fraction < share fraction, user gets boost
    %% If usage fraction > share fraction, user gets penalty
    case ShareFraction == 0.0 of
        true ->
            0.0;  % No shares = lowest priority
        false ->
            Ratio = UsageFraction / ShareFraction,
            %% Use exponential decay: 2^(-Ratio) normalized to 0-1
            %% When Ratio=0, Factor=1.0 (haven't used anything)
            %% When Ratio=1, Factor=0.5 (used exactly share)
            %% When Ratio=2, Factor=0.25 (used double share)
            Factor = math:pow(2, -Ratio),
            %% Clamp to valid range
            max(0.0, min(1.0, Factor))
    end.

%% @private
%% Apply time-based decay to all usage values
do_decay_usage(State) ->
    Now = erlang:system_time(second),
    DecayFactor = math:pow(0.5, ?DECAY_PERIOD / 1000 / ?DECAY_HALFLIFE),

    %% Decay all entries
    NewTotalUsage = ets:foldl(
        fun({Key, Usage, LastUpdate}, AccUsage) ->
            %% Calculate time since last update
            TimeSince = Now - LastUpdate,
            %% Apply additional decay based on time since last update
            ExtraDecay = math:pow(0.5, TimeSince / ?DECAY_HALFLIFE),

            NewUsage = Usage * DecayFactor * ExtraDecay,

            if
                NewUsage < 0.01 ->
                    %% Remove negligible usage
                    ets:delete(?USAGE_TABLE, Key),
                    AccUsage;
                true ->
                    ets:insert(?USAGE_TABLE, {Key, NewUsage, Now}),
                    AccUsage + NewUsage
            end
        end,
        0.0,
        ?USAGE_TABLE
    ),

    State#state{total_usage = NewTotalUsage}.
