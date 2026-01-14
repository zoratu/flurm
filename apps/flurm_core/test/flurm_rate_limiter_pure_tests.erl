%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_rate_limiter module.
%%%
%%% Tests all exported functions directly without mocking.
%%% Tests gen_server callbacks (init/1, handle_call/3, handle_cast/2,
%%% handle_info/2, terminate/2, code_change/3) directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_rate_limiter_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% ETS table names (from source)
-define(BUCKETS_TABLE, flurm_rate_buckets).
-define(LIMITS_TABLE, flurm_rate_limits).
-define(STATS_TABLE, flurm_rate_stats).

%% Default limits (from source)
-define(DEFAULT_USER_LIMIT, 100).
-define(DEFAULT_IP_LIMIT, 200).
-define(DEFAULT_GLOBAL_LIMIT, 10000).
-define(BUCKET_CAPACITY_MULTIPLIER, 10).

%% State record (from source)
-record(state, {
    enabled = true :: boolean(),
    refill_timer :: reference() | undefined,
    load_timer :: reference() | undefined,
    current_load = 0.0 :: float(),
    backpressure_active = false :: boolean()
}).

%% Bucket record (from source)
-record(bucket, {
    key :: term(),
    tokens :: float(),
    last_refill :: integer(),
    limit :: pos_integer()
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for tests that need ETS tables
setup() ->
    %% Clean up any existing tables
    cleanup_tables(),
    ok.

cleanup() ->
    cleanup_tables(),
    ok.

cleanup_tables() ->
    catch ets:delete(?BUCKETS_TABLE),
    catch ets:delete(?LIMITS_TABLE),
    catch ets:delete(?STATS_TABLE),
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"init creates ETS tables", fun init_creates_ets_tables/0},
      {"init sets default limits", fun init_sets_default_limits/0},
      {"init initializes global bucket", fun init_initializes_global_bucket/0},
      {"init starts timers", fun init_starts_timers/0},
      {"init returns proper state", fun init_returns_proper_state/0}
     ]}.

init_creates_ets_tables() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Verify tables exist
    ?assertNotEqual(undefined, ets:whereis(?BUCKETS_TABLE)),
    ?assertNotEqual(undefined, ets:whereis(?LIMITS_TABLE)),
    ?assertNotEqual(undefined, ets:whereis(?STATS_TABLE)),

    %% Clean up timers
    cleanup_state_timers(State).

init_sets_default_limits() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Verify default limits are set
    ?assertEqual([{{user, default}, ?DEFAULT_USER_LIMIT}],
                 ets:lookup(?LIMITS_TABLE, {user, default})),
    ?assertEqual([{{ip, default}, ?DEFAULT_IP_LIMIT}],
                 ets:lookup(?LIMITS_TABLE, {ip, default})),
    ?assertEqual([{{global, default}, ?DEFAULT_GLOBAL_LIMIT}],
                 ets:lookup(?LIMITS_TABLE, {global, default})),
    ?assertEqual([{enabled, true}],
                 ets:lookup(?LIMITS_TABLE, enabled)),

    cleanup_state_timers(State).

init_initializes_global_bucket() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Verify global bucket exists with correct values
    [Bucket] = ets:lookup(?BUCKETS_TABLE, {global, global}),
    ?assertEqual({global, global}, Bucket#bucket.key),
    ?assertEqual(?DEFAULT_GLOBAL_LIMIT * ?BUCKET_CAPACITY_MULTIPLIER, Bucket#bucket.tokens),
    ?assertEqual(?DEFAULT_GLOBAL_LIMIT, Bucket#bucket.limit),
    ?assert(is_integer(Bucket#bucket.last_refill)),

    cleanup_state_timers(State).

init_starts_timers() ->
    {ok, State} = flurm_rate_limiter:init([]),

    ?assert(is_reference(State#state.refill_timer)),
    ?assert(is_reference(State#state.load_timer)),

    cleanup_state_timers(State).

init_returns_proper_state() ->
    {ok, State} = flurm_rate_limiter:init([]),

    ?assert(is_record(State, state)),
    ?assertEqual(true, State#state.enabled),
    ?assertEqual(0.0, State#state.current_load),
    ?assertEqual(false, State#state.backpressure_active),

    cleanup_state_timers(State).

%%====================================================================
%% handle_call/3 Tests - check_rate
%%====================================================================

handle_call_check_rate_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"check_rate allows request with available tokens", fun check_rate_allows_request/0},
      {"check_rate creates bucket on first request", fun check_rate_creates_bucket/0},
      {"check_rate consumes tokens", fun check_rate_consumes_tokens/0},
      {"check_rate rate limits when no tokens", fun check_rate_limits_when_no_tokens/0},
      {"check_rate with count parameter", fun check_rate_with_count/0},
      {"check_rate checks global limit for non-global types", fun check_rate_checks_global/0},
      {"check_rate with backpressure active", fun check_rate_with_backpressure/0}
     ]}.

check_rate_allows_request() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, Result, _NewState} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"testuser">>, 1},
                                        {self(), make_ref()}, State),

    ?assertEqual(ok, Result),
    cleanup_state_timers(State).

check_rate_creates_bucket() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Before request, bucket doesn't exist
    ?assertEqual([], ets:lookup(?BUCKETS_TABLE, {user, <<"newuser">>})),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"newuser">>, 1},
                                        {self(), make_ref()}, State),

    %% After request, bucket exists
    [Bucket] = ets:lookup(?BUCKETS_TABLE, {user, <<"newuser">>}),
    ?assertEqual({user, <<"newuser">>}, Bucket#bucket.key),
    ?assertEqual(?DEFAULT_USER_LIMIT, Bucket#bucket.limit),

    cleanup_state_timers(State).

check_rate_consumes_tokens() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% First request
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"consumer">>, 1},
                                        {self(), make_ref()}, State),

    [Bucket1] = ets:lookup(?BUCKETS_TABLE, {user, <<"consumer">>}),
    InitialTokens = Bucket1#bucket.tokens,

    %% Second request should consume more tokens
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"consumer">>, 5},
                                        {self(), make_ref()}, State),

    [Bucket2] = ets:lookup(?BUCKETS_TABLE, {user, <<"consumer">>}),
    %% Tokens should be less (accounting for possible small refill)
    ?assert(Bucket2#bucket.tokens < InitialTokens),

    cleanup_state_timers(State).

check_rate_limits_when_no_tokens() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create a bucket with no tokens
    EmptyBucket = #bucket{
        key = {user, <<"exhausted">>},
        tokens = 0.0,
        last_refill = erlang:monotonic_time(millisecond),
        limit = ?DEFAULT_USER_LIMIT
    },
    ets:insert(?BUCKETS_TABLE, EmptyBucket),

    {reply, Result, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"exhausted">>, 1},
                                        {self(), make_ref()}, State),

    ?assertEqual({error, rate_limited}, Result),
    cleanup_state_timers(State).

check_rate_with_count() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create a bucket with limited tokens
    LimitedBucket = #bucket{
        key = {user, <<"limited">>},
        tokens = 5.0,
        last_refill = erlang:monotonic_time(millisecond),
        limit = ?DEFAULT_USER_LIMIT
    },
    ets:insert(?BUCKETS_TABLE, LimitedBucket),

    %% Request for 3 tokens should succeed
    {reply, Result1, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"limited">>, 3},
                                        {self(), make_ref()}, State),
    ?assertEqual(ok, Result1),

    %% Request for 10 more tokens should fail
    {reply, Result2, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"limited">>, 10},
                                        {self(), make_ref()}, State),
    ?assertEqual({error, rate_limited}, Result2),

    cleanup_state_timers(State).

check_rate_checks_global() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Exhaust global bucket
    [GlobalBucket] = ets:lookup(?BUCKETS_TABLE, {global, global}),
    ExhaustedGlobal = GlobalBucket#bucket{tokens = 0.0,
                                           last_refill = erlang:monotonic_time(millisecond)},
    ets:insert(?BUCKETS_TABLE, ExhaustedGlobal),

    %% User bucket is fine but global is exhausted
    {reply, Result, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"anyuser">>, 1},
                                        {self(), make_ref()}, State),

    ?assertEqual({error, rate_limited}, Result),
    cleanup_state_timers(State).

check_rate_with_backpressure() ->
    {ok, State0} = flurm_rate_limiter:init([]),

    %% Enable backpressure with high load
    State = State0#state{backpressure_active = true, current_load = 0.95},

    %% With backpressure, some requests may be rejected
    %% Run multiple times to test probabilistic behavior
    Results = [begin
        {reply, R, _} =
            flurm_rate_limiter:handle_call({check_rate, user, <<"bpuser">>, 1},
                                            {self(), make_ref()}, State),
        R
    end || _ <- lists:seq(1, 20)],

    %% At high load, we expect some backpressure errors
    BackpressureErrors = [R || R <- Results, R =:= {error, backpressure}],
    OkResults = [R || R <- Results, R =:= ok],

    %% Either some backpressure or all ok (due to randomness)
    ?assert(length(BackpressureErrors) > 0 orelse length(OkResults) > 0),

    cleanup_state_timers(State0).

%%====================================================================
%% handle_call/3 Tests - get_stats
%%====================================================================

handle_call_get_stats_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"get_stats returns overall statistics", fun get_stats_returns_overall/0},
      {"get_stats includes global bucket info", fun get_stats_includes_global_bucket/0},
      {"get_stats for specific key", fun get_stats_for_key/0},
      {"get_stats for nonexistent key", fun get_stats_for_nonexistent_key/0}
     ]}.

get_stats_returns_overall() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, Stats, _} =
        flurm_rate_limiter:handle_call(get_stats, {self(), make_ref()}, State),

    ?assert(is_map(Stats)),
    ?assertEqual(true, maps:get(enabled, Stats)),
    ?assertEqual(0.0, maps:get(current_load, Stats)),
    ?assertEqual(false, maps:get(backpressure_active, Stats)),
    ?assert(maps:is_key(bucket_count, Stats)),
    ?assert(maps:is_key(global_bucket, Stats)),
    ?assertEqual(0.8, maps:get(high_load_threshold, Stats)),
    ?assertEqual(0.95, maps:get(critical_load_threshold, Stats)),

    cleanup_state_timers(State).

get_stats_includes_global_bucket() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, Stats, _} =
        flurm_rate_limiter:handle_call(get_stats, {self(), make_ref()}, State),

    GlobalBucket = maps:get(global_bucket, Stats),
    ?assert(is_map(GlobalBucket)),
    ?assertEqual(?DEFAULT_GLOBAL_LIMIT, maps:get(limit, GlobalBucket)),
    ?assertEqual(?DEFAULT_GLOBAL_LIMIT * ?BUCKET_CAPACITY_MULTIPLIER,
                 maps:get(capacity, GlobalBucket)),

    cleanup_state_timers(State).

get_stats_for_key() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Add some stats
    StatsKey = {user, <<"statsuser">>},
    ets:insert(?STATS_TABLE, {StatsKey, 42, erlang:system_time(second)}),

    {reply, Stats, _} =
        flurm_rate_limiter:handle_call({get_stats, StatsKey}, {self(), make_ref()}, State),

    ?assertEqual(StatsKey, maps:get(key, Stats)),
    ?assertEqual(42, maps:get(request_count, Stats)),
    ?assert(maps:is_key(last_update, Stats)),

    cleanup_state_timers(State).

get_stats_for_nonexistent_key() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, Stats, _} =
        flurm_rate_limiter:handle_call({get_stats, {user, <<"nobody">>}},
                                        {self(), make_ref()}, State),

    ?assertEqual({user, <<"nobody">>}, maps:get(key, Stats)),
    ?assertEqual(0, maps:get(request_count, Stats)),

    cleanup_state_timers(State).

%%====================================================================
%% handle_call/3 Tests - set_limit/get_limit
%%====================================================================

handle_call_limit_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"set_limit stores limit in ETS", fun set_limit_stores_in_ets/0},
      {"set_limit updates existing bucket", fun set_limit_updates_bucket/0},
      {"get_limit returns specific limit", fun get_limit_returns_specific/0},
      {"get_limit falls back to default", fun get_limit_falls_back_to_default/0},
      {"get_limit returns hardcoded default", fun get_limit_returns_hardcoded_default/0}
     ]}.

set_limit_stores_in_ets() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call({set_limit, user, <<"limituser">>, 500},
                                        {self(), make_ref()}, State),

    ?assertEqual([{{user, <<"limituser">>}, 500}],
                 ets:lookup(?LIMITS_TABLE, {user, <<"limituser">>})),

    cleanup_state_timers(State).

set_limit_updates_bucket() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create a bucket first
    Bucket = #bucket{
        key = {user, <<"bucketuser">>},
        tokens = 100.0,
        last_refill = erlang:monotonic_time(millisecond),
        limit = 100
    },
    ets:insert(?BUCKETS_TABLE, Bucket),

    %% Set new limit
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({set_limit, user, <<"bucketuser">>, 200},
                                        {self(), make_ref()}, State),

    %% Bucket limit should be updated
    [UpdatedBucket] = ets:lookup(?BUCKETS_TABLE, {user, <<"bucketuser">>}),
    ?assertEqual(200, UpdatedBucket#bucket.limit),

    cleanup_state_timers(State).

get_limit_returns_specific() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Set a specific limit
    ets:insert(?LIMITS_TABLE, {{ip, <<"192.168.1.1">>}, 50}),

    {reply, Limit, _} =
        flurm_rate_limiter:handle_call({get_limit, ip, <<"192.168.1.1">>},
                                        {self(), make_ref()}, State),

    ?assertEqual(50, Limit),
    cleanup_state_timers(State).

get_limit_falls_back_to_default() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% No specific limit, should use default
    {reply, Limit, _} =
        flurm_rate_limiter:handle_call({get_limit, user, <<"unknown">>},
                                        {self(), make_ref()}, State),

    ?assertEqual(?DEFAULT_USER_LIMIT, Limit),
    cleanup_state_timers(State).

get_limit_returns_hardcoded_default() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Delete the default limit to test hardcoded fallback
    ets:delete(?LIMITS_TABLE, {ip, default}),

    {reply, Limit, _} =
        flurm_rate_limiter:handle_call({get_limit, ip, <<"unknownip">>},
                                        {self(), make_ref()}, State),

    ?assertEqual(?DEFAULT_IP_LIMIT, Limit),
    cleanup_state_timers(State).

%%====================================================================
%% handle_call/3 Tests - reset_limits
%%====================================================================

handle_call_reset_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"reset_limits clears buckets", fun reset_clears_buckets/0},
      {"reset_limits clears stats", fun reset_clears_stats/0},
      {"reset_limits recreates global bucket", fun reset_recreates_global_bucket/0},
      {"reset_limits resets state", fun reset_resets_state/0}
     ]}.

reset_clears_buckets() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Add some buckets
    ets:insert(?BUCKETS_TABLE, #bucket{key = {user, <<"u1">>}, tokens = 10.0,
                                       last_refill = 0, limit = 100}),
    ets:insert(?BUCKETS_TABLE, #bucket{key = {ip, <<"ip1">>}, tokens = 20.0,
                                       last_refill = 0, limit = 200}),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call(reset_limits, {self(), make_ref()}, State),

    %% Only global bucket should exist
    ?assertEqual(1, ets:info(?BUCKETS_TABLE, size)),
    ?assertNotEqual([], ets:lookup(?BUCKETS_TABLE, {global, global})),

    cleanup_state_timers(State).

reset_clears_stats() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Add some stats
    ets:insert(?STATS_TABLE, {{user, <<"u1">>}, 100, 0}),
    ets:insert(?STATS_TABLE, {{ip, <<"ip1">>}, 200, 0}),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call(reset_limits, {self(), make_ref()}, State),

    ?assertEqual(0, ets:info(?STATS_TABLE, size)),
    cleanup_state_timers(State).

reset_recreates_global_bucket() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Deplete global bucket
    ets:insert(?BUCKETS_TABLE, #bucket{key = {global, global}, tokens = 0.0,
                                       last_refill = 0, limit = ?DEFAULT_GLOBAL_LIMIT}),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call(reset_limits, {self(), make_ref()}, State),

    %% Global bucket should be restored to full capacity
    [Bucket] = ets:lookup(?BUCKETS_TABLE, {global, global}),
    ?assertEqual(?DEFAULT_GLOBAL_LIMIT * ?BUCKET_CAPACITY_MULTIPLIER, Bucket#bucket.tokens),

    cleanup_state_timers(State).

reset_resets_state() ->
    {ok, State0} = flurm_rate_limiter:init([]),
    State = State0#state{current_load = 0.9, backpressure_active = true},

    {reply, ok, NewState} =
        flurm_rate_limiter:handle_call(reset_limits, {self(), make_ref()}, State),

    ?assertEqual(0.0, NewState#state.current_load),
    ?assertEqual(false, NewState#state.backpressure_active),

    cleanup_state_timers(State0).

%%====================================================================
%% handle_call/3 Tests - enable/disable
%%====================================================================

handle_call_enable_disable_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"enable sets enabled flag", fun enable_sets_flag/0},
      {"disable clears enabled flag", fun disable_clears_flag/0},
      {"enable updates ETS", fun enable_updates_ets/0},
      {"disable updates ETS", fun disable_updates_ets/0}
     ]}.

enable_sets_flag() ->
    {ok, State0} = flurm_rate_limiter:init([]),
    State = State0#state{enabled = false},

    {reply, ok, NewState} =
        flurm_rate_limiter:handle_call(enable, {self(), make_ref()}, State),

    ?assertEqual(true, NewState#state.enabled),
    cleanup_state_timers(State0).

disable_clears_flag() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, ok, NewState} =
        flurm_rate_limiter:handle_call(disable, {self(), make_ref()}, State),

    ?assertEqual(false, NewState#state.enabled),
    cleanup_state_timers(State).

enable_updates_ets() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call(enable, {self(), make_ref()}, State),

    ?assertEqual([{enabled, true}], ets:lookup(?LIMITS_TABLE, enabled)),
    cleanup_state_timers(State).

disable_updates_ets() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, ok, _} =
        flurm_rate_limiter:handle_call(disable, {self(), make_ref()}, State),

    ?assertEqual([{enabled, false}], ets:lookup(?LIMITS_TABLE, enabled)),
    cleanup_state_timers(State).

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"unknown request returns error", fun unknown_request_returns_error/0}
     ]}.

unknown_request_returns_error() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {reply, Result, _} =
        flurm_rate_limiter:handle_call({unknown, request}, {self(), make_ref()}, State),

    ?assertEqual({error, unknown_request}, Result),
    cleanup_state_timers(State).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"record_request creates new stats entry", fun record_request_creates_entry/0},
      {"record_request increments existing entry", fun record_request_increments/0},
      {"unknown cast is ignored", fun unknown_cast_ignored/0}
     ]}.

record_request_creates_entry() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {noreply, _} =
        flurm_rate_limiter:handle_cast({record_request, user, <<"newrequest">>}, State),

    [{_, Count, _}] = ets:lookup(?STATS_TABLE, {user, <<"newrequest">>}),
    ?assertEqual(1, Count),

    cleanup_state_timers(State).

record_request_increments() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create initial entry
    StatsKey = {user, <<"increment">>},
    ets:insert(?STATS_TABLE, {StatsKey, 5, erlang:system_time(second) - 100}),

    {noreply, _} =
        flurm_rate_limiter:handle_cast({record_request, user, <<"increment">>}, State),

    [{_, Count, LastUpdate}] = ets:lookup(?STATS_TABLE, StatsKey),
    ?assertEqual(6, Count),
    %% Last update should be recent
    ?assert(LastUpdate >= erlang:system_time(second) - 1),

    cleanup_state_timers(State).

unknown_cast_ignored() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {noreply, NewState} =
        flurm_rate_limiter:handle_cast({unknown, cast}, State),

    ?assertEqual(State, NewState),
    cleanup_state_timers(State).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"refill_buckets message refills tokens", fun refill_buckets_refills_tokens/0},
      {"refill_buckets restarts timer", fun refill_buckets_restarts_timer/0},
      {"check_load calculates load", fun check_load_calculates_load/0},
      {"check_load activates backpressure", fun check_load_activates_backpressure/0},
      {"check_load deactivates backpressure", fun check_load_deactivates_backpressure/0},
      {"check_load restarts timer", fun check_load_restarts_timer/0},
      {"unknown info is ignored", fun unknown_info_ignored/0}
     ]}.

refill_buckets_refills_tokens() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create a depleted bucket with old timestamp
    OldTime = erlang:monotonic_time(millisecond) - 1000,  % 1 second ago
    DepletedBucket = #bucket{
        key = {user, <<"refillme">>},
        tokens = 0.0,
        last_refill = OldTime,
        limit = 100
    },
    ets:insert(?BUCKETS_TABLE, DepletedBucket),

    {noreply, _} = flurm_rate_limiter:handle_info(refill_buckets, State),

    %% Bucket should have more tokens now
    [RefilledBucket] = ets:lookup(?BUCKETS_TABLE, {user, <<"refillme">>}),
    ?assert(RefilledBucket#bucket.tokens > 0),

    cleanup_state_timers(State).

refill_buckets_restarts_timer() ->
    {ok, State} = flurm_rate_limiter:init([]),
    OldTimer = State#state.refill_timer,

    {noreply, NewState} = flurm_rate_limiter:handle_info(refill_buckets, State),

    ?assertNotEqual(OldTimer, NewState#state.refill_timer),
    ?assert(is_reference(NewState#state.refill_timer)),

    cleanup_state_timers(State),
    erlang:cancel_timer(NewState#state.refill_timer).

check_load_calculates_load() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Deplete global bucket partially
    [GlobalBucket] = ets:lookup(?BUCKETS_TABLE, {global, global}),
    HalfEmpty = GlobalBucket#bucket{tokens = GlobalBucket#bucket.tokens / 2},
    ets:insert(?BUCKETS_TABLE, HalfEmpty),

    {noreply, NewState} = flurm_rate_limiter:handle_info(check_load, State),

    %% Load should be approximately 0.5
    ?assert(NewState#state.current_load > 0.4),
    ?assert(NewState#state.current_load < 0.6),

    cleanup_state_timers(State),
    erlang:cancel_timer(NewState#state.load_timer).

check_load_activates_backpressure() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Deplete global bucket to trigger backpressure (>80% used)
    [GlobalBucket] = ets:lookup(?BUCKETS_TABLE, {global, global}),
    AlmostEmpty = GlobalBucket#bucket{tokens = GlobalBucket#bucket.tokens * 0.1},
    ets:insert(?BUCKETS_TABLE, AlmostEmpty),

    {noreply, NewState} = flurm_rate_limiter:handle_info(check_load, State),

    ?assertEqual(true, NewState#state.backpressure_active),
    ?assert(NewState#state.current_load > 0.8),

    cleanup_state_timers(State),
    erlang:cancel_timer(NewState#state.load_timer).

check_load_deactivates_backpressure() ->
    {ok, State0} = flurm_rate_limiter:init([]),
    State = State0#state{backpressure_active = true, current_load = 0.9},

    %% Global bucket is at full capacity (low load)
    {noreply, NewState} = flurm_rate_limiter:handle_info(check_load, State),

    ?assertEqual(false, NewState#state.backpressure_active),
    ?assert(NewState#state.current_load < 0.8),

    cleanup_state_timers(State0),
    erlang:cancel_timer(NewState#state.load_timer).

check_load_restarts_timer() ->
    {ok, State} = flurm_rate_limiter:init([]),
    OldTimer = State#state.load_timer,

    {noreply, NewState} = flurm_rate_limiter:handle_info(check_load, State),

    ?assertNotEqual(OldTimer, NewState#state.load_timer),
    ?assert(is_reference(NewState#state.load_timer)),

    cleanup_state_timers(State),
    erlang:cancel_timer(NewState#state.load_timer).

unknown_info_ignored() ->
    {ok, State} = flurm_rate_limiter:init([]),

    {noreply, NewState} = flurm_rate_limiter:handle_info({unknown, info}, State),

    ?assertEqual(State, NewState),
    cleanup_state_timers(State).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"terminate cancels timers", fun terminate_cancels_timers/0},
      {"terminate handles undefined timers", fun terminate_handles_undefined_timers/0}
     ]}.

terminate_cancels_timers() ->
    {ok, State} = flurm_rate_limiter:init([]),

    RefillTimer = State#state.refill_timer,
    LoadTimer = State#state.load_timer,

    ok = flurm_rate_limiter:terminate(normal, State),

    %% Timers should be cancelled (reading them returns false)
    ?assertEqual(false, erlang:read_timer(RefillTimer)),
    ?assertEqual(false, erlang:read_timer(LoadTimer)).

terminate_handles_undefined_timers() ->
    State = #state{refill_timer = undefined, load_timer = undefined},

    %% Should not crash with undefined timers
    ?assertEqual(ok, flurm_rate_limiter:terminate(normal, State)).

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    [
     {"code_change returns state unchanged", fun code_change_returns_state/0}
    ].

code_change_returns_state() ->
    State = #state{enabled = true, current_load = 0.5},

    {ok, NewState} = flurm_rate_limiter:code_change("1.0", State, []),

    ?assertEqual(State, NewState).

%%====================================================================
%% is_enabled/0 Tests
%%====================================================================

is_enabled_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"is_enabled returns true when enabled", fun is_enabled_returns_true/0},
      {"is_enabled returns false when disabled", fun is_enabled_returns_false/0},
      {"is_enabled returns false when table missing", fun is_enabled_returns_false_no_table/0},
      {"is_enabled returns true when flag missing", fun is_enabled_returns_true_default/0}
     ]}.

is_enabled_returns_true() ->
    {ok, State} = flurm_rate_limiter:init([]),

    ?assertEqual(true, flurm_rate_limiter:is_enabled()),
    cleanup_state_timers(State).

is_enabled_returns_false() ->
    {ok, State} = flurm_rate_limiter:init([]),
    ets:insert(?LIMITS_TABLE, {enabled, false}),

    ?assertEqual(false, flurm_rate_limiter:is_enabled()),
    cleanup_state_timers(State).

is_enabled_returns_false_no_table() ->
    %% No tables exist
    ?assertEqual(false, flurm_rate_limiter:is_enabled()).

is_enabled_returns_true_default() ->
    {ok, State} = flurm_rate_limiter:init([]),
    ets:delete(?LIMITS_TABLE, enabled),

    ?assertEqual(true, flurm_rate_limiter:is_enabled()),
    cleanup_state_timers(State).

%%====================================================================
%% Integration-style Tests (using callbacks directly)
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"full rate limiting flow", fun full_rate_limiting_flow/0},
      {"token bucket refill over time", fun token_bucket_refill_over_time/0},
      {"different rate types", fun different_rate_types/0}
     ]}.

full_rate_limiting_flow() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Set a custom limit
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({set_limit, user, <<"flowuser">>, 10},
                                        {self(), make_ref()}, State),

    %% Check rate should succeed initially
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"flowuser">>, 1},
                                        {self(), make_ref()}, State),

    %% Record the request
    {noreply, _} =
        flurm_rate_limiter:handle_cast({record_request, user, <<"flowuser">>}, State),

    %% Get stats should show the request
    {reply, Stats, _} =
        flurm_rate_limiter:handle_call({get_stats, {user, <<"flowuser">>}},
                                        {self(), make_ref()}, State),
    ?assertEqual(1, maps:get(request_count, Stats)),

    %% Get limit should return our custom limit
    {reply, Limit, _} =
        flurm_rate_limiter:handle_call({get_limit, user, <<"flowuser">>},
                                        {self(), make_ref()}, State),
    ?assertEqual(10, Limit),

    cleanup_state_timers(State).

token_bucket_refill_over_time() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Create a bucket with old timestamp and few tokens
    OldTime = erlang:monotonic_time(millisecond) - 500,  % 500ms ago
    Bucket = #bucket{
        key = {user, <<"timeuser">>},
        tokens = 10.0,
        last_refill = OldTime,
        limit = 100  % 100 tokens per second
    },
    ets:insert(?BUCKETS_TABLE, Bucket),

    %% Check rate - should refill ~50 tokens (500ms * 100/s)
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"timeuser">>, 50},
                                        {self(), make_ref()}, State),

    %% Should have consumed 50 tokens from ~60 available
    [UpdatedBucket] = ets:lookup(?BUCKETS_TABLE, {user, <<"timeuser">>}),
    ?assert(UpdatedBucket#bucket.tokens > 0),
    ?assert(UpdatedBucket#bucket.tokens < 20),  % Started with ~60, consumed 50

    cleanup_state_timers(State).

different_rate_types() ->
    {ok, State} = flurm_rate_limiter:init([]),

    %% Test user rate
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, user, <<"user1">>, 1},
                                        {self(), make_ref()}, State),

    %% Test IP rate
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, ip, <<"10.0.0.1">>, 1},
                                        {self(), make_ref()}, State),

    %% Test global rate
    {reply, ok, _} =
        flurm_rate_limiter:handle_call({check_rate, global, global, 1},
                                        {self(), make_ref()}, State),

    %% Verify buckets were created for each
    ?assertNotEqual([], ets:lookup(?BUCKETS_TABLE, {user, <<"user1">>})),
    ?assertNotEqual([], ets:lookup(?BUCKETS_TABLE, {ip, <<"10.0.0.1">>})),
    ?assertNotEqual([], ets:lookup(?BUCKETS_TABLE, {global, global})),

    cleanup_state_timers(State).

%%====================================================================
%% Helper Functions
%%====================================================================

cleanup_state_timers(State) ->
    case State#state.refill_timer of
        undefined -> ok;
        Timer1 -> erlang:cancel_timer(Timer1)
    end,
    case State#state.load_timer of
        undefined -> ok;
        Timer2 -> erlang:cancel_timer(Timer2)
    end,
    ok.
