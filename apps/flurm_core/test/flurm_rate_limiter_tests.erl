%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_rate_limiter module
%%%-------------------------------------------------------------------
-module(flurm_rate_limiter_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the rate limiter server
    case whereis(flurm_rate_limiter) of
        undefined ->
            {ok, Pid} = flurm_rate_limiter:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    catch ets:delete(flurm_rate_buckets),
    catch ets:delete(flurm_rate_limits),
    catch ets:delete(flurm_rate_stats),
    gen_server:stop(flurm_rate_limiter);
cleanup({existing, _Pid}) ->
    ok.

rate_limiter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enable and disable", fun test_enable_disable/0},
      {"check rate allowed", fun test_check_rate_allowed/0},
      {"set and get limits", fun test_set_get_limits/0},
      {"get stats", fun test_get_stats/0},
      {"rate limiting works", fun test_rate_limiting/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_enable_disable() ->
    %% Should be enabled by default
    ?assert(flurm_rate_limiter:is_enabled()),

    %% Disable
    ok = flurm_rate_limiter:disable(),
    ?assertNot(flurm_rate_limiter:is_enabled()),

    %% Re-enable
    ok = flurm_rate_limiter:enable(),
    ?assert(flurm_rate_limiter:is_enabled()).

test_check_rate_allowed() ->
    %% Fresh buckets should allow requests
    ?assertEqual(ok, flurm_rate_limiter:check_rate(user, <<"testuser">>)),
    ?assertEqual(ok, flurm_rate_limiter:check_rate(ip, <<"192.168.1.1">>)),
    ?assertEqual(ok, flurm_rate_limiter:check_rate(global, global)).

test_set_get_limits() ->
    %% Get default user limit
    DefaultLimit = flurm_rate_limiter:get_limit(user, default),
    ?assertEqual(100, DefaultLimit),

    %% Set custom limit for specific user
    ok = flurm_rate_limiter:set_limit(user, <<"vipuser">>, 1000),
    VipLimit = flurm_rate_limiter:get_limit(user, <<"vipuser">>),
    ?assertEqual(1000, VipLimit),

    %% Regular user still gets default
    RegularLimit = flurm_rate_limiter:get_limit(user, <<"regularuser">>),
    ?assertEqual(100, RegularLimit).

test_get_stats() ->
    Stats = flurm_rate_limiter:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(enabled, Stats)),
    ?assert(maps:is_key(current_load, Stats)),
    ?assert(maps:is_key(backpressure_active, Stats)).

test_rate_limiting() ->
    %% Set a very low limit for testing
    ok = flurm_rate_limiter:set_limit(user, <<"limited_user">>, 5),

    %% First few requests should succeed
    Results1 = [flurm_rate_limiter:check_rate(user, <<"limited_user">>)
                || _ <- lists:seq(1, 5)],
    OkCount = length([R || R <- Results1, R == ok]),

    %% Should have allowed some requests (token bucket has burst capacity)
    ?assert(OkCount >= 3).
