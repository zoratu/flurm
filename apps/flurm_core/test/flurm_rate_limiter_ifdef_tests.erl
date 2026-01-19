%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_rate_limiter internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_rate_limiter_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKETS_TABLE, flurm_rate_buckets).
-define(LIMITS_TABLE, flurm_rate_limits).
-define(STATS_TABLE, flurm_rate_stats).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Create ETS tables if they don't exist
    case ets:whereis(?BUCKETS_TABLE) of
        undefined ->
            ets:new(?BUCKETS_TABLE, [
                named_table, public, set,
                {keypos, 2},  % bucket.key position
                {write_concurrency, true}
            ]);
        _ -> ok
    end,
    case ets:whereis(?LIMITS_TABLE) of
        undefined ->
            ets:new(?LIMITS_TABLE, [named_table, public, set]);
        _ -> ok
    end,
    case ets:whereis(?STATS_TABLE) of
        undefined ->
            ets:new(?STATS_TABLE, [named_table, public, set]);
        _ -> ok
    end,
    %% Set default limits
    ets:insert(?LIMITS_TABLE, {{user, default}, 100}),
    ets:insert(?LIMITS_TABLE, {{ip, default}, 200}),
    ets:insert(?LIMITS_TABLE, {{global, default}, 10000}),
    ets:insert(?LIMITS_TABLE, {enabled, true}),
    ok.

cleanup(_) ->
    %% Clean up tables
    case ets:whereis(?BUCKETS_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?BUCKETS_TABLE)
    end,
    case ets:whereis(?LIMITS_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?LIMITS_TABLE)
    end,
    case ets:whereis(?STATS_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?STATS_TABLE)
    end,
    ok.

rate_limiter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun get_limit_for_user_default/0,
         fun get_limit_for_ip_default/0,
         fun get_limit_for_global_default/0,
         fun get_limit_for_custom/0,
         fun create_bucket_creates_entry/0,
         fun create_bucket_uses_limit/0,
         fun collect_key_stats_empty/0,
         fun collect_key_stats_with_data/0
     ]}.

%%====================================================================
%% Get Limit For Tests
%%====================================================================

get_limit_for_user_default() ->
    Limit = flurm_rate_limiter:get_limit_for(user, <<"testuser">>),
    ?assertEqual(100, Limit).

get_limit_for_ip_default() ->
    Limit = flurm_rate_limiter:get_limit_for(ip, <<"192.168.1.1">>),
    ?assertEqual(200, Limit).

get_limit_for_global_default() ->
    Limit = flurm_rate_limiter:get_limit_for(global, global),
    ?assertEqual(10000, Limit).

get_limit_for_custom() ->
    %% Set a custom limit
    ets:insert(?LIMITS_TABLE, {{user, <<"vip_user">>}, 500}),
    Limit = flurm_rate_limiter:get_limit_for(user, <<"vip_user">>),
    ?assertEqual(500, Limit).

get_limit_for_fallback_test() ->
    %% When no specific or default limit, should use hardcoded defaults
    ets:delete(?LIMITS_TABLE, {user, default}),
    Limit = flurm_rate_limiter:get_limit_for(user, <<"unknown">>),
    ?assertEqual(100, Limit).  % Default user limit

%%====================================================================
%% Create Bucket Tests
%%====================================================================

create_bucket_creates_entry() ->
    Now = erlang:monotonic_time(millisecond),
    _Bucket = flurm_rate_limiter:create_bucket(user, <<"newuser">>, Now),
    ?assertMatch([_], ets:lookup(?BUCKETS_TABLE, {user, <<"newuser">>})).

create_bucket_uses_limit() ->
    Now = erlang:monotonic_time(millisecond),
    Bucket = flurm_rate_limiter:create_bucket(user, <<"newuser2">>, Now),
    %% Bucket record: {bucket, key, tokens, last_refill, limit}
    ?assertEqual(100, element(5, Bucket)).

create_bucket_with_custom_limit_test() ->
    ets:insert(?LIMITS_TABLE, {{user, <<"premium">>}, 1000}),
    Now = erlang:monotonic_time(millisecond),
    Bucket = flurm_rate_limiter:create_bucket(user, <<"premium">>, Now),
    ?assertEqual(1000, element(5, Bucket)).

create_bucket_tokens_test() ->
    Now = erlang:monotonic_time(millisecond),
    Bucket = flurm_rate_limiter:create_bucket(user, <<"tokentest">>, Now),
    %% Tokens should be limit * capacity_multiplier (10)
    %% For limit 100, tokens = 100 * 10 = 1000
    Tokens = element(3, Bucket),
    ?assertEqual(1000, Tokens).

%%====================================================================
%% Collect Key Stats Tests
%%====================================================================

collect_key_stats_empty() ->
    Stats = flurm_rate_limiter:collect_key_stats({user, <<"nodata">>}),
    ?assertEqual(#{key => {user, <<"nodata">>}, request_count => 0}, Stats).

collect_key_stats_with_data() ->
    Key = {user, <<"hasdata">>},
    Now = erlang:system_time(second),
    ets:insert(?STATS_TABLE, {Key, 42, Now}),
    Stats = flurm_rate_limiter:collect_key_stats(Key),
    ?assertEqual(42, maps:get(request_count, Stats)),
    ?assertEqual(Key, maps:get(key, Stats)).

%%====================================================================
%% Check Bucket Tests
%%====================================================================

check_bucket_new_user_test() ->
    %% New user should be allowed
    Result = flurm_rate_limiter:check_bucket(user, <<"brand_new_user">>, 1),
    ?assertEqual(ok, Result).

check_bucket_consume_tokens_test() ->
    %% First request creates bucket with full tokens
    ok = flurm_rate_limiter:check_bucket(user, <<"consume_test">>, 1),
    %% Lookup bucket and verify tokens were consumed
    [{bucket, _, Tokens, _, _}] = ets:lookup(?BUCKETS_TABLE, {user, <<"consume_test">>}),
    %% Should be less than initial (1000 - 1 = 999, plus some refill)
    ?assert(Tokens < 1000.0).

check_bucket_rate_limited_test() ->
    %% Create a bucket with very few tokens
    Now = erlang:monotonic_time(millisecond),
    EmptyBucket = {bucket, {user, <<"empty">>}, 0.5, Now, 100},
    ets:insert(?BUCKETS_TABLE, EmptyBucket),
    %% Should be rate limited
    Result = flurm_rate_limiter:check_bucket(user, <<"empty">>, 1),
    ?assertEqual({error, rate_limited}, Result).

check_bucket_multiple_requests_test() ->
    BucketKey = {user, <<"multi_req">>},
    %% Create bucket with limited tokens
    Now = erlang:monotonic_time(millisecond),
    Bucket = {bucket, BucketKey, 5.0, Now, 100},
    ets:insert(?BUCKETS_TABLE, Bucket),
    %% Request 3 tokens
    Result = flurm_rate_limiter:check_bucket(user, <<"multi_req">>, 3),
    ?assertEqual(ok, Result),
    %% Request 3 more should fail (only ~2 tokens left)
    Result2 = flurm_rate_limiter:check_bucket(user, <<"multi_req">>, 3),
    ?assertEqual({error, rate_limited}, Result2).

%%====================================================================
%% Do Check Rate Tests
%%====================================================================

do_check_rate_no_backpressure_test() ->
    State = {state, true, undefined, undefined, 0.5, false},
    Result = flurm_rate_limiter:do_check_rate(user, <<"test">>, 1, State),
    ?assertEqual(ok, Result).

do_check_rate_with_backpressure_test() ->
    %% With high load, some requests may be rejected
    State = {state, true, undefined, undefined, 0.9, true},
    %% Run multiple times - with 0.9 load, rejection probability is (0.9 - 0.8) * 5 = 0.5
    %% Some should pass, some might fail with backpressure
    Results = [flurm_rate_limiter:do_check_rate(user, <<"bp_test_", (integer_to_binary(N))/binary>>, 1, State)
               || N <- lists:seq(1, 10)],
    %% At least some should succeed
    OkCount = length([R || R <- Results, R =:= ok]),
    ?assert(OkCount > 0).

%%====================================================================
%% Do Refill Buckets Tests
%%====================================================================

do_refill_buckets_test() ->
    %% Create a bucket with low tokens
    Now = erlang:monotonic_time(millisecond),
    OldTime = Now - 1000,  % 1 second ago
    Bucket = {bucket, {user, <<"refill_test">>}, 50.0, OldTime, 100},
    ets:insert(?BUCKETS_TABLE, Bucket),

    %% Run refill
    flurm_rate_limiter:do_refill_buckets(),

    %% Verify tokens increased
    [{bucket, _, NewTokens, _, _}] = ets:lookup(?BUCKETS_TABLE, {user, <<"refill_test">>}),
    %% Should have gained ~100 tokens (1 second * 100 limit)
    ?assert(NewTokens > 50.0).

do_refill_buckets_capped_test() ->
    %% Create a bucket near max capacity
    Now = erlang:monotonic_time(millisecond),
    OldTime = Now - 10000,  % 10 seconds ago
    Bucket = {bucket, {user, <<"cap_test">>}, 900.0, OldTime, 100},
    ets:insert(?BUCKETS_TABLE, Bucket),

    %% Run refill
    flurm_rate_limiter:do_refill_buckets(),

    %% Verify tokens are capped at max (100 * 10 = 1000)
    [{bucket, _, NewTokens, _, _}] = ets:lookup(?BUCKETS_TABLE, {user, <<"cap_test">>}),
    ?assertEqual(1000, NewTokens).

%%====================================================================
%% Do Check Load Tests
%%====================================================================

do_check_load_normal_test() ->
    %% Set up a global bucket with plenty of tokens (low load)
    %% With limit=10000 and multiplier=10, MaxTokens=100000
    %% For Load < 0.5, need tokens > 50000
    Now = erlang:monotonic_time(millisecond),
    GlobalBucket = {bucket, {global, global}, 60000.0, Now, 10000},
    ets:insert(?BUCKETS_TABLE, GlobalBucket),

    State = {state, true, undefined, undefined, 0.0, false},
    NewState = flurm_rate_limiter:do_check_load(State),

    %% Load should be low (1 - 60000/100000 = 0.4)
    Load = element(5, NewState),
    ?assert(Load < 0.5),
    ?assertNot(element(6, NewState)).  % Backpressure should be false

do_check_load_high_test() ->
    %% Set up a global bucket with few tokens (high load)
    Now = erlang:monotonic_time(millisecond),
    GlobalBucket = {bucket, {global, global}, 1000.0, Now, 10000},
    ets:insert(?BUCKETS_TABLE, GlobalBucket),

    State = {state, true, undefined, undefined, 0.0, false},
    NewState = flurm_rate_limiter:do_check_load(State),

    %% Load should be high (1 - 1000/100000 = 0.99)
    Load = element(5, NewState),
    ?assert(Load > 0.8),
    ?assert(element(6, NewState)).  % Backpressure should be true

%%====================================================================
%% Collect Stats Tests
%%====================================================================

collect_stats_basic_test() ->
    State = {state, true, undefined, undefined, 0.5, false},
    Stats = flurm_rate_limiter:collect_stats(State),
    ?assertEqual(true, maps:get(enabled, Stats)),
    ?assertEqual(0.5, maps:get(current_load, Stats)),
    ?assertEqual(false, maps:get(backpressure_active, Stats)).

collect_stats_with_global_bucket_test() ->
    Now = erlang:monotonic_time(millisecond),
    GlobalBucket = {bucket, {global, global}, 5000.0, Now, 10000},
    ets:insert(?BUCKETS_TABLE, GlobalBucket),

    State = {state, true, undefined, undefined, 0.5, false},
    Stats = flurm_rate_limiter:collect_stats(State),

    GlobalStats = maps:get(global_bucket, Stats),
    ?assertEqual(5000.0, maps:get(tokens, GlobalStats)),
    ?assertEqual(10000, maps:get(limit, GlobalStats)).
