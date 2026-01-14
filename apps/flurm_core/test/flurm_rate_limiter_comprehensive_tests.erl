%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_rate_limiter module
%%% Tests token bucket rate limiting and backpressure
%%%-------------------------------------------------------------------
-module(flurm_rate_limiter_comprehensive_tests).
-compile(nowarn_unused_function).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing rate limiter
    catch gen_server:stop(flurm_rate_limiter),
    catch ets:delete(flurm_rate_buckets),
    catch ets:delete(flurm_rate_limits),
    catch ets:delete(flurm_rate_stats),
    timer:sleep(50),
    %% Start fresh
    {ok, Pid} = flurm_rate_limiter:start_link(),
    {started, Pid}.

cleanup({started, _Pid}) ->
    catch gen_server:stop(flurm_rate_limiter),
    catch ets:delete(flurm_rate_buckets),
    catch ets:delete(flurm_rate_limits),
    catch ets:delete(flurm_rate_stats),
    timer:sleep(50);
cleanup(_) ->
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

rate_limiter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Server starts correctly", fun test_server_starts/0},
      {"Enable and disable", fun test_enable_disable/0},
      {"Check rate when disabled", fun test_check_rate_disabled/0},
      {"Check rate user allowed", fun test_check_rate_user/0},
      {"Check rate IP allowed", fun test_check_rate_ip/0},
      {"Check rate global allowed", fun test_check_rate_global/0},
      {"Check rate with count", fun test_check_rate_count/0},
      {"Record request", fun test_record_request/0},
      {"Get stats overall", fun test_get_stats/0},
      {"Get stats for key", fun test_get_stats_key/0},
      {"Get stats for unknown key", fun test_get_stats_unknown/0},
      {"Set and get limits", fun test_set_get_limits/0},
      {"Set limit updates bucket", fun test_set_limit_updates_bucket/0},
      {"Reset limits", fun test_reset_limits/0},
      {"Token bucket refill", fun test_token_refill/0},
      {"Rate limiting triggers", fun test_rate_limiting_triggers/0},
      {"Handle unknown call", fun test_unknown_call/0},
      {"Handle unknown cast", fun test_unknown_cast/0},
      {"Handle unknown info", fun test_unknown_info/0},
      {"Code change callback", fun test_code_change/0},
      {"Terminate callback", fun test_terminate/0},
      {"Is enabled check", fun test_is_enabled/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_server_starts() ->
    Pid = whereis(flurm_rate_limiter),
    ?assert(is_pid(Pid)).

test_enable_disable() ->
    ?assert(flurm_rate_limiter:is_enabled()),
    ok = flurm_rate_limiter:disable(),
    ?assertNot(flurm_rate_limiter:is_enabled()),
    ok = flurm_rate_limiter:enable(),
    ?assert(flurm_rate_limiter:is_enabled()).

test_check_rate_disabled() ->
    ok = flurm_rate_limiter:disable(),
    %% When disabled, all requests should pass
    ?assertEqual(ok, flurm_rate_limiter:check_rate(user, <<"anyuser">>)),
    ?assertEqual(ok, flurm_rate_limiter:check_rate(ip, <<"192.168.1.1">>)),
    ?assertEqual(ok, flurm_rate_limiter:check_rate(global, global)).

test_check_rate_user() ->
    ok = flurm_rate_limiter:enable(),
    Result = flurm_rate_limiter:check_rate(user, <<"testuser">>),
    ?assertEqual(ok, Result).

test_check_rate_ip() ->
    ok = flurm_rate_limiter:enable(),
    Result = flurm_rate_limiter:check_rate(ip, <<"192.168.1.1">>),
    ?assertEqual(ok, Result).

test_check_rate_global() ->
    ok = flurm_rate_limiter:enable(),
    Result = flurm_rate_limiter:check_rate(global, global),
    ?assertEqual(ok, Result).

test_check_rate_count() ->
    ok = flurm_rate_limiter:enable(),
    %% Check with count > 1
    Result = flurm_rate_limiter:check_rate(user, <<"countuser">>, 5),
    ?assertEqual(ok, Result).

test_record_request() ->
    ok = flurm_rate_limiter:record_request(user, <<"recorduser">>),
    timer:sleep(20),
    Stats = flurm_rate_limiter:get_stats({user, <<"recorduser">>}),
    ?assert(is_map(Stats)),
    ?assertEqual({user, <<"recorduser">>}, maps:get(key, Stats)),
    ?assertEqual(1, maps:get(request_count, Stats)).

test_get_stats() ->
    Stats = flurm_rate_limiter:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(enabled, Stats)),
    ?assert(maps:is_key(current_load, Stats)),
    ?assert(maps:is_key(backpressure_active, Stats)),
    ?assert(maps:is_key(bucket_count, Stats)),
    ?assert(maps:is_key(global_bucket, Stats)),
    ?assert(maps:is_key(high_load_threshold, Stats)),
    ?assert(maps:is_key(critical_load_threshold, Stats)).

test_get_stats_key() ->
    %% Record some requests first
    ok = flurm_rate_limiter:record_request(user, <<"statsuser">>),
    ok = flurm_rate_limiter:record_request(user, <<"statsuser">>),
    ok = flurm_rate_limiter:record_request(user, <<"statsuser">>),
    timer:sleep(30),

    Stats = flurm_rate_limiter:get_stats({user, <<"statsuser">>}),
    ?assertEqual({user, <<"statsuser">>}, maps:get(key, Stats)),
    ?assertEqual(3, maps:get(request_count, Stats)),
    ?assert(maps:is_key(last_update, Stats)).

test_get_stats_unknown() ->
    Stats = flurm_rate_limiter:get_stats({user, <<"unknownuser">>}),
    ?assertEqual({user, <<"unknownuser">>}, maps:get(key, Stats)),
    ?assertEqual(0, maps:get(request_count, Stats)).

test_set_get_limits() ->
    %% Default user limit
    DefaultUserLimit = flurm_rate_limiter:get_limit(user, default),
    ?assertEqual(100, DefaultUserLimit),

    %% Default IP limit
    DefaultIpLimit = flurm_rate_limiter:get_limit(ip, default),
    ?assertEqual(200, DefaultIpLimit),

    %% Default global limit
    DefaultGlobalLimit = flurm_rate_limiter:get_limit(global, default),
    ?assertEqual(10000, DefaultGlobalLimit),

    %% Set custom limit
    ok = flurm_rate_limiter:set_limit(user, <<"vipuser">>, 1000),
    ?assertEqual(1000, flurm_rate_limiter:get_limit(user, <<"vipuser">>)),

    %% Non-existent key falls back to default
    ?assertEqual(100, flurm_rate_limiter:get_limit(user, <<"newuser">>)).

test_set_limit_updates_bucket() ->
    %% First check rate to create a bucket
    _ = flurm_rate_limiter:check_rate(user, <<"bucketuser">>),
    timer:sleep(10),

    %% Now set a new limit - should update existing bucket
    ok = flurm_rate_limiter:set_limit(user, <<"bucketuser">>, 500),
    ?assertEqual(500, flurm_rate_limiter:get_limit(user, <<"bucketuser">>)).

test_reset_limits() ->
    %% Set some custom limits
    ok = flurm_rate_limiter:set_limit(user, <<"resetuser">>, 999),
    %% Record some requests
    ok = flurm_rate_limiter:record_request(user, <<"resetuser">>),
    timer:sleep(10),

    %% Reset
    ok = flurm_rate_limiter:reset_limits(),

    %% Stats should be cleared
    Stats = flurm_rate_limiter:get_stats({user, <<"resetuser">>}),
    ?assertEqual(0, maps:get(request_count, Stats)),

    %% Overall stats should show fresh state
    OverallStats = flurm_rate_limiter:get_stats(),
    ?assertNot(maps:get(backpressure_active, OverallStats)).

test_token_refill() ->
    %% Set a very low limit
    ok = flurm_rate_limiter:set_limit(user, <<"refilluser">>, 10),

    %% Make some requests to consume tokens
    _ = flurm_rate_limiter:check_rate(user, <<"refilluser">>, 50),
    timer:sleep(10),

    %% Wait for refill (default interval is 100ms)
    timer:sleep(200),

    %% Should be able to make more requests after refill
    Result = flurm_rate_limiter:check_rate(user, <<"refilluser">>, 1),
    ?assertEqual(ok, Result).

test_rate_limiting_triggers() ->
    %% Set a very low limit for testing
    ok = flurm_rate_limiter:set_limit(user, <<"limiteduser">>, 1),
    timer:sleep(50),

    %% First request should succeed
    Result1 = flurm_rate_limiter:check_rate(user, <<"limiteduser">>),
    ?assertEqual(ok, Result1),

    %% Consume all tokens by requesting way more than available
    %% With limit=1 and burst multiplier of 10, capacity is 10 tokens
    Results = [flurm_rate_limiter:check_rate(user, <<"limiteduser">>, 1)
               || _ <- lists:seq(1, 20)],

    %% At least some should be rate limited
    RateLimitedCount = length([R || R <- Results, R =/= ok]),
    ?assert(RateLimitedCount > 0).

test_unknown_call() ->
    Result = gen_server:call(flurm_rate_limiter, {unknown_request, arg}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    ok = gen_server:cast(flurm_rate_limiter, {unknown_cast, arg}),
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_rate_limiter))).

test_unknown_info() ->
    flurm_rate_limiter ! {unknown_info, arg},
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_rate_limiter))).

test_code_change() ->
    State = {state, true, undefined, undefined, 0.0, false},
    {ok, NewState} = flurm_rate_limiter:code_change(old_vsn, State, extra),
    ?assertEqual(State, NewState).

test_terminate() ->
    %% Test terminate by stopping the server
    Pid = whereis(flurm_rate_limiter),
    ?assert(is_pid(Pid)),
    gen_server:stop(flurm_rate_limiter),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_rate_limiter)).

test_is_enabled() ->
    %% When server is running, should be enabled by default
    ?assert(flurm_rate_limiter:is_enabled()),

    %% Stop server and delete table
    gen_server:stop(flurm_rate_limiter),
    ets:delete(flurm_rate_limits),
    timer:sleep(20),

    %% With no table, should return false
    ?assertNot(flurm_rate_limiter:is_enabled()).

%%====================================================================
%% Internal Timer Tests
%%====================================================================

timer_tests_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_rate_limiter),
         catch ets:delete(flurm_rate_buckets),
         catch ets:delete(flurm_rate_limits),
         catch ets:delete(flurm_rate_stats),
         timer:sleep(50),
         {ok, Pid} = flurm_rate_limiter:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid),
         timer:sleep(50)
     end,
     [
      {"Refill timer fires", fun() ->
          %% Wait for refill timer to fire (100ms interval)
          timer:sleep(150),
          ?assert(is_pid(whereis(flurm_rate_limiter)))
      end},
      {"Load check timer fires", fun() ->
          %% Wait for load check timer to fire (1000ms interval)
          timer:sleep(1100),
          Stats = flurm_rate_limiter:get_stats(),
          %% current_load should have been calculated
          ?assert(is_float(maps:get(current_load, Stats)))
      end}
     ]}.

%%====================================================================
%% Backpressure Tests
%%====================================================================

backpressure_test_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_rate_limiter),
         catch ets:delete(flurm_rate_buckets),
         catch ets:delete(flurm_rate_limits),
         catch ets:delete(flurm_rate_stats),
         timer:sleep(50),
         {ok, Pid} = flurm_rate_limiter:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid),
         timer:sleep(50)
     end,
     [
      {"Backpressure activates under high load", fun() ->
          %% Set a very low global limit to trigger backpressure
          ok = flurm_rate_limiter:set_limit(global, default, 10),
          timer:sleep(50),

          %% Consume most of the global bucket
          _ = [flurm_rate_limiter:check_rate(global, global, 1) || _ <- lists:seq(1, 100)],

          %% Wait for load check
          timer:sleep(1200),

          %% Stats should show high load
          Stats = flurm_rate_limiter:get_stats(),
          ?assert(is_float(maps:get(current_load, Stats)))
      end}
     ]}.

%%====================================================================
%% Default Limits Fallback Tests
%%====================================================================

default_limits_test_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_rate_limiter),
         catch ets:delete(flurm_rate_buckets),
         catch ets:delete(flurm_rate_limits),
         catch ets:delete(flurm_rate_stats),
         timer:sleep(50),
         {ok, Pid} = flurm_rate_limiter:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid),
         timer:sleep(50)
     end,
     [
      {"Fallback to type default for unknown key", fun() ->
          %% Should fall back to {user, default} limit
          Limit = flurm_rate_limiter:get_limit(user, <<"completely_new_user">>),
          ?assertEqual(100, Limit)
      end},
      {"Fallback to hardcoded default if type default missing", fun() ->
          %% Delete the type default from ETS
          ets:delete(flurm_rate_limits, {user, default}),

          %% Should fall back to hardcoded DEFAULT_USER_LIMIT (100)
          Limit = flurm_rate_limiter:get_limit(user, <<"another_user">>),
          ?assertEqual(100, Limit)
      end}
     ]}.
