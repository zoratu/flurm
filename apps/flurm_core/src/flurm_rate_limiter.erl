%%%-------------------------------------------------------------------
%%% @doc FLURM Rate Limiter
%%%
%%% Implements rate limiting and backpressure to protect the controller
%%% from being overwhelmed by too many requests.
%%%
%%% Features:
%%% - Per-user rate limits (requests per second)
%%% - Per-IP rate limits
%%% - Global rate limits
%%% - Token bucket algorithm for smooth rate limiting
%%% - Adaptive backpressure based on system load
%%%
%%% Based on SLURM's rate limiting added in version 23.02.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_rate_limiter).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    check_rate/2,
    check_rate/3,
    record_request/2,
    get_stats/0,
    get_stats/1,
    set_limit/3,
    get_limit/2,
    reset_limits/0,
    enable/0,
    disable/0,
    is_enabled/0
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
-define(BUCKETS_TABLE, flurm_rate_buckets).
-define(LIMITS_TABLE, flurm_rate_limits).
-define(STATS_TABLE, flurm_rate_stats).

%% Default rate limits
-define(DEFAULT_USER_LIMIT, 100).      % requests per second per user
-define(DEFAULT_IP_LIMIT, 200).        % requests per second per IP
-define(DEFAULT_GLOBAL_LIMIT, 10000).  % total requests per second

%% Token bucket parameters
-define(BUCKET_REFILL_INTERVAL, 100).  % ms between refills
-define(BUCKET_CAPACITY_MULTIPLIER, 10). % burst capacity = limit * multiplier

%% Backpressure thresholds
-define(LOAD_CHECK_INTERVAL, 1000).    % ms between load checks
-define(HIGH_LOAD_THRESHOLD, 0.8).     % 80% of limit triggers backpressure
-define(CRITICAL_LOAD_THRESHOLD, 0.95). % 95% starts rejecting

-record(state, {
    enabled = true :: boolean(),
    refill_timer :: reference() | undefined,
    load_timer :: reference() | undefined,
    current_load = 0.0 :: float(),
    backpressure_active = false :: boolean()
}).

-record(bucket, {
    key :: term(),
    tokens :: float(),
    last_refill :: integer(),
    limit :: pos_integer()
}).

%% Test exports
-ifdef(TEST).
-export([
    %% Rate checking internals
    do_check_rate/4,
    check_bucket/3,
    check_global_limit/1,
    %% Bucket management
    create_bucket/3,
    get_limit_for/2,
    do_refill_buckets/0,
    %% Load checking
    do_check_load/1,
    %% Stats collection
    collect_stats/1,
    collect_key_stats/1
]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the rate limiter server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Check if a request is allowed under rate limits.
%% Returns ok if allowed, {error, rate_limited} if not.
-spec check_rate(user | ip | global, term()) -> ok | {error, rate_limited | backpressure}.
check_rate(Type, Key) ->
    check_rate(Type, Key, 1).

%% @doc Check if N requests are allowed.
-spec check_rate(user | ip | global, term(), pos_integer()) -> ok | {error, rate_limited | backpressure}.
check_rate(Type, Key, Count) ->
    case is_enabled() of
        false -> ok;
        true -> gen_server:call(?SERVER, {check_rate, Type, Key, Count})
    end.

%% @doc Record a completed request (for stats).
-spec record_request(user | ip | global, term()) -> ok.
record_request(Type, Key) ->
    gen_server:cast(?SERVER, {record_request, Type, Key}).

%% @doc Get rate limiting statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Get stats for a specific key.
-spec get_stats(term()) -> map().
get_stats(Key) ->
    gen_server:call(?SERVER, {get_stats, Key}).

%% @doc Set rate limit for a type/key combination.
-spec set_limit(user | ip | global, term(), pos_integer()) -> ok.
set_limit(Type, Key, Limit) when is_integer(Limit), Limit > 0 ->
    gen_server:call(?SERVER, {set_limit, Type, Key, Limit}).

%% @doc Get rate limit for a type/key combination.
-spec get_limit(user | ip | global, term()) -> pos_integer().
get_limit(Type, Key) ->
    gen_server:call(?SERVER, {get_limit, Type, Key}).

%% @doc Reset all rate limits and buckets.
-spec reset_limits() -> ok.
reset_limits() ->
    gen_server:call(?SERVER, reset_limits).

%% @doc Enable rate limiting.
-spec enable() -> ok.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable rate limiting.
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Check if rate limiting is enabled.
-spec is_enabled() -> boolean().
is_enabled() ->
    case ets:whereis(?LIMITS_TABLE) of
        undefined -> false;
        _ ->
            case ets:lookup(?LIMITS_TABLE, enabled) of
                [{enabled, Val}] -> Val;
                [] -> true
            end
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?BUCKETS_TABLE, [
        named_table, public, set,
        {keypos, #bucket.key},
        {write_concurrency, true}
    ]),
    ets:new(?LIMITS_TABLE, [named_table, public, set]),
    ets:new(?STATS_TABLE, [named_table, public, set]),

    %% Set default limits
    ets:insert(?LIMITS_TABLE, {{user, default}, ?DEFAULT_USER_LIMIT}),
    ets:insert(?LIMITS_TABLE, {{ip, default}, ?DEFAULT_IP_LIMIT}),
    ets:insert(?LIMITS_TABLE, {{global, default}, ?DEFAULT_GLOBAL_LIMIT}),
    ets:insert(?LIMITS_TABLE, {enabled, true}),

    %% Initialize global bucket
    Now = erlang:monotonic_time(millisecond),
    GlobalBucket = #bucket{
        key = {global, global},
        tokens = ?DEFAULT_GLOBAL_LIMIT * ?BUCKET_CAPACITY_MULTIPLIER,
        last_refill = Now,
        limit = ?DEFAULT_GLOBAL_LIMIT
    },
    ets:insert(?BUCKETS_TABLE, GlobalBucket),

    %% Start timers
    RefillTimer = erlang:send_after(?BUCKET_REFILL_INTERVAL, self(), refill_buckets),
    LoadTimer = erlang:send_after(?LOAD_CHECK_INTERVAL, self(), check_load),

    {ok, #state{
        refill_timer = RefillTimer,
        load_timer = LoadTimer
    }}.

handle_call({check_rate, Type, Key, Count}, _From, State) ->
    Result = do_check_rate(Type, Key, Count, State),
    {reply, Result, State};

handle_call(get_stats, _From, State) ->
    Stats = collect_stats(State),
    {reply, Stats, State};

handle_call({get_stats, Key}, _From, State) ->
    Stats = collect_key_stats(Key),
    {reply, Stats, State};

handle_call({set_limit, Type, Key, Limit}, _From, State) ->
    ets:insert(?LIMITS_TABLE, {{Type, Key}, Limit}),
    %% Update bucket capacity if it exists
    BucketKey = {Type, Key},
    case ets:lookup(?BUCKETS_TABLE, BucketKey) of
        [Bucket] ->
            ets:insert(?BUCKETS_TABLE, Bucket#bucket{limit = Limit});
        [] ->
            ok
    end,
    {reply, ok, State};

handle_call({get_limit, Type, Key}, _From, State) ->
    Limit = get_limit_for(Type, Key),
    {reply, Limit, State};

handle_call(reset_limits, _From, State) ->
    ets:delete_all_objects(?BUCKETS_TABLE),
    ets:delete_all_objects(?STATS_TABLE),
    %% Re-initialize global bucket
    Now = erlang:monotonic_time(millisecond),
    GlobalBucket = #bucket{
        key = {global, global},
        tokens = ?DEFAULT_GLOBAL_LIMIT * ?BUCKET_CAPACITY_MULTIPLIER,
        last_refill = Now,
        limit = ?DEFAULT_GLOBAL_LIMIT
    },
    ets:insert(?BUCKETS_TABLE, GlobalBucket),
    {reply, ok, State#state{current_load = 0.0, backpressure_active = false}};

handle_call(enable, _From, State) ->
    ets:insert(?LIMITS_TABLE, {enabled, true}),
    {reply, ok, State#state{enabled = true}};

handle_call(disable, _From, State) ->
    ets:insert(?LIMITS_TABLE, {enabled, false}),
    {reply, ok, State#state{enabled = false}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({record_request, Type, Key}, State) ->
    %% Update stats
    StatsKey = {Type, Key},
    case ets:lookup(?STATS_TABLE, StatsKey) of
        [{StatsKey, Count, _LastUpdate}] ->
            ets:insert(?STATS_TABLE, {StatsKey, Count + 1, erlang:system_time(second)});
        [] ->
            ets:insert(?STATS_TABLE, {StatsKey, 1, erlang:system_time(second)})
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refill_buckets, State) ->
    do_refill_buckets(),
    Timer = erlang:send_after(?BUCKET_REFILL_INTERVAL, self(), refill_buckets),
    {noreply, State#state{refill_timer = Timer}};

handle_info(check_load, State) ->
    NewState = do_check_load(State),
    Timer = erlang:send_after(?LOAD_CHECK_INTERVAL, self(), check_load),
    {noreply, NewState#state{load_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.refill_timer of
        undefined -> ok;
        Timer1 -> erlang:cancel_timer(Timer1)
    end,
    case State#state.load_timer of
        undefined -> ok;
        Timer2 -> erlang:cancel_timer(Timer2)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Check rate limit using token bucket
do_check_rate(Type, Key, Count, State) ->
    %% Check backpressure first
    case State#state.backpressure_active of
        true ->
            %% Under high load, randomly reject some requests
            case rand:uniform() < (State#state.current_load - ?HIGH_LOAD_THRESHOLD) * 5 of
                true -> {error, backpressure};
                false -> check_bucket(Type, Key, Count)
            end;
        false ->
            check_bucket(Type, Key, Count)
    end.

%% @private Check and consume tokens from bucket
check_bucket(Type, Key, Count) ->
    BucketKey = {Type, Key},
    Now = erlang:monotonic_time(millisecond),

    %% Get or create bucket
    Bucket = case ets:lookup(?BUCKETS_TABLE, BucketKey) of
        [B] -> B;
        [] -> create_bucket(Type, Key, Now)
    end,

    %% Calculate tokens to add since last refill
    Limit = Bucket#bucket.limit,
    TimeSinceRefill = Now - Bucket#bucket.last_refill,
    TokensToAdd = (TimeSinceRefill / 1000.0) * Limit,
    MaxTokens = Limit * ?BUCKET_CAPACITY_MULTIPLIER,
    CurrentTokens = min(MaxTokens, Bucket#bucket.tokens + TokensToAdd),

    %% Check if we have enough tokens
    case CurrentTokens >= Count of
        true ->
            %% Consume tokens
            NewBucket = Bucket#bucket{
                tokens = CurrentTokens - Count,
                last_refill = Now
            },
            ets:insert(?BUCKETS_TABLE, NewBucket),
            %% Also check global limit
            case Type of
                global -> ok;
                _ -> check_global_limit(Count)
            end;
        false ->
            {error, rate_limited}
    end.

%% @private Check global rate limit
check_global_limit(Count) ->
    check_bucket(global, global, Count).

%% @private Create a new token bucket
create_bucket(Type, Key, Now) ->
    Limit = get_limit_for(Type, Key),
    Bucket = #bucket{
        key = {Type, Key},
        tokens = Limit * ?BUCKET_CAPACITY_MULTIPLIER,
        last_refill = Now,
        limit = Limit
    },
    ets:insert(?BUCKETS_TABLE, Bucket),
    Bucket.

%% @private Get limit for type/key
get_limit_for(Type, Key) ->
    case ets:lookup(?LIMITS_TABLE, {Type, Key}) of
        [{{Type, Key}, Limit}] -> Limit;
        [] ->
            %% Fall back to default for type
            case ets:lookup(?LIMITS_TABLE, {Type, default}) of
                [{{Type, default}, Limit}] -> Limit;
                [] ->
                    case Type of
                        user -> ?DEFAULT_USER_LIMIT;
                        ip -> ?DEFAULT_IP_LIMIT;
                        global -> ?DEFAULT_GLOBAL_LIMIT
                    end
            end
    end.

%% @private Refill all buckets periodically
do_refill_buckets() ->
    Now = erlang:monotonic_time(millisecond),
    ets:foldl(
        fun(#bucket{key = _Key, tokens = Tokens, last_refill = LastRefill, limit = Limit} = Bucket, _Acc) ->
            TimeSinceRefill = Now - LastRefill,
            TokensToAdd = (TimeSinceRefill / 1000.0) * Limit,
            MaxTokens = Limit * ?BUCKET_CAPACITY_MULTIPLIER,
            NewTokens = min(MaxTokens, Tokens + TokensToAdd),
            ets:insert(?BUCKETS_TABLE, Bucket#bucket{tokens = NewTokens, last_refill = Now}),
            ok
        end,
        ok,
        ?BUCKETS_TABLE
    ).

%% @private Check system load and adjust backpressure
do_check_load(State) ->
    %% Calculate current load based on global bucket
    case ets:lookup(?BUCKETS_TABLE, {global, global}) of
        [#bucket{tokens = Tokens, limit = Limit}] ->
            MaxTokens = Limit * ?BUCKET_CAPACITY_MULTIPLIER,
            %% Load is inverse of available tokens
            Load = 1.0 - (Tokens / MaxTokens),
            Backpressure = Load > ?HIGH_LOAD_THRESHOLD,

            case Backpressure andalso not State#state.backpressure_active of
                true ->
                    error_logger:warning_msg("FLURM rate limiter: backpressure activated (load: ~.2f)~n", [Load]);
                false -> ok
            end,
            case not Backpressure andalso State#state.backpressure_active of
                true ->
                    error_logger:info_msg("FLURM rate limiter: backpressure deactivated (load: ~.2f)~n", [Load]);
                false -> ok
            end,

            State#state{
                current_load = Load,
                backpressure_active = Backpressure
            };
        [] ->
            State
    end.

%% @private Collect overall stats
collect_stats(State) ->
    BucketCount = ets:info(?BUCKETS_TABLE, size),
    GlobalBucket = case ets:lookup(?BUCKETS_TABLE, {global, global}) of
        [#bucket{tokens = T, limit = L}] ->
            #{tokens => T, limit => L, capacity => L * ?BUCKET_CAPACITY_MULTIPLIER};
        [] ->
            #{}
    end,
    #{
        enabled => State#state.enabled,
        current_load => State#state.current_load,
        backpressure_active => State#state.backpressure_active,
        bucket_count => BucketCount,
        global_bucket => GlobalBucket,
        high_load_threshold => ?HIGH_LOAD_THRESHOLD,
        critical_load_threshold => ?CRITICAL_LOAD_THRESHOLD
    }.

%% @private Collect stats for specific key
collect_key_stats(Key) ->
    case ets:lookup(?STATS_TABLE, Key) of
        [{Key, Count, LastUpdate}] ->
            #{key => Key, request_count => Count, last_update => LastUpdate};
        [] ->
            #{key => Key, request_count => 0}
    end.
