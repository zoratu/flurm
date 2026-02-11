%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_connection_limiter
%%%
%%% Covers connection rate limiting per peer IP, ETS-backed tracking,
%%% periodic cleanup, and stats reporting.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_connection_limiter_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_connection_limiter_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

connection_limiter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"process starts and ETS table exists",
         fun test_starts_with_ets/0},
        {"connection_allowed true when no connections",
         fun test_allowed_no_connections/0},
        {"connection_allowed true under limit",
         fun test_allowed_under_limit/0},
        {"connection_allowed false at limit",
         fun test_not_allowed_at_limit/0},
        {"connection_allowed true after close",
         fun test_allowed_after_close/0},
        {"connection_opened increments count",
         fun test_opened_increments/0},
        {"multiple opens accumulate",
         fun test_multiple_opens/0},
        {"connection_closed decrements count",
         fun test_closed_decrements/0},
        {"connection_closed removes entry at zero",
         fun test_closed_removes_at_zero/0},
        {"connection_closed no crash for unknown IP",
         fun test_closed_unknown_ip/0},
        {"multiple peers independent counts",
         fun test_multiple_peers/0},
        {"get_connection_count returns 0 for unknown",
         fun test_get_count_unknown/0},
        {"get_connection_count returns correct value",
         fun test_get_count_known/0},
        {"get_all_counts returns all mappings",
         fun test_get_all_counts/0},
        {"get_stats total_peers and total_connections",
         fun test_get_stats/0},
        {"get_stats empty when no connections",
         fun test_get_stats_empty/0},
        {"get_stats max_per_peer",
         fun test_get_stats_max/0},
        {"cleanup timer removes zero-count entries",
         fun test_cleanup_timer/0},
        {"unknown call returns error",
         fun test_unknown_call/0},
        {"unknown cast and info no crash",
         fun test_unknown_cast_info/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Set low limit for testing
    application:set_env(flurm_controller, max_connections_per_peer, 5),
    application:set_env(flurm_controller, conn_cleanup_interval, 600000),
    case whereis(flurm_connection_limiter) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,
    %% Clean up any leftover ETS table
    catch ets:delete(flurm_conn_limits),
    {ok, LimPid} = flurm_connection_limiter:start_link(),
    unlink(LimPid),
    LimPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end,
    catch ets:delete(flurm_conn_limits),
    application:unset_env(flurm_controller, max_connections_per_peer),
    application:unset_env(flurm_controller, conn_cleanup_interval).

%%====================================================================
%% Tests
%%====================================================================

test_starts_with_ets() ->
    ?assertNotEqual(undefined, ets:info(flurm_conn_limits)).

test_allowed_no_connections() ->
    ?assertEqual(true, flurm_connection_limiter:connection_allowed({127,0,0,1})).

test_allowed_under_limit() ->
    IP = {10,0,0,1},
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(true, flurm_connection_limiter:connection_allowed(IP)).

test_not_allowed_at_limit() ->
    IP = {10,0,0,2},
    lists:foreach(fun(_) ->
        flurm_connection_limiter:connection_opened(IP)
    end, lists:seq(1, 5)),
    ?assertEqual(false, flurm_connection_limiter:connection_allowed(IP)).

test_allowed_after_close() ->
    IP = {10,0,0,3},
    lists:foreach(fun(_) ->
        flurm_connection_limiter:connection_opened(IP)
    end, lists:seq(1, 5)),
    ?assertEqual(false, flurm_connection_limiter:connection_allowed(IP)),
    flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(true, flurm_connection_limiter:connection_allowed(IP)).

test_opened_increments() ->
    IP = {10,0,0,4},
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP)).

test_multiple_opens() ->
    IP = {10,0,0,5},
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(3, flurm_connection_limiter:get_connection_count(IP)).

test_closed_decrements() ->
    IP = {10,0,0,6},
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP)).

test_closed_removes_at_zero() ->
    IP = {10,0,0,7},
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP)),
    %% Entry should be removed from ETS
    ?assertEqual([], ets:lookup(flurm_conn_limits, IP)).

test_closed_unknown_ip() ->
    %% Should not crash
    ?assertEqual(ok, flurm_connection_limiter:connection_closed({192,168,0,1})).

test_multiple_peers() ->
    IP1 = {10,1,0,1},
    IP2 = {10,1,0,2},
    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP2),
    ?assertEqual(2, flurm_connection_limiter:get_connection_count(IP1)),
    ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP2)).

test_get_count_unknown() ->
    ?assertEqual(0, flurm_connection_limiter:get_connection_count({255,255,255,255})).

test_get_count_known() ->
    IP = {10,2,0,1},
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(2, flurm_connection_limiter:get_connection_count(IP)).

test_get_all_counts() ->
    IP1 = {10,3,0,1},
    IP2 = {10,3,0,2},
    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP2),
    flurm_connection_limiter:connection_opened(IP2),
    All = flurm_connection_limiter:get_all_counts(),
    ?assert(is_list(All)),
    ?assertEqual(1, proplists:get_value(IP1, All)),
    ?assertEqual(2, proplists:get_value(IP2, All)).

test_get_stats() ->
    IP1 = {10,4,0,1},
    IP2 = {10,4,0,2},
    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP2),
    flurm_connection_limiter:connection_opened(IP2),
    Stats = flurm_connection_limiter:get_stats(),
    ?assert(is_map(Stats)),
    ?assertEqual(2, maps:get(total_peers, Stats)),
    ?assertEqual(3, maps:get(total_connections, Stats)).

test_get_stats_empty() ->
    Stats = flurm_connection_limiter:get_stats(),
    ?assertEqual(0, maps:get(total_peers, Stats)),
    ?assertEqual(0, maps:get(total_connections, Stats)),
    ?assertEqual(0, maps:get(max_per_peer, Stats)).

test_get_stats_max() ->
    IP1 = {10,5,0,1},
    IP2 = {10,5,0,2},
    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP2),
    flurm_connection_limiter:connection_opened(IP2),
    flurm_connection_limiter:connection_opened(IP2),
    Stats = flurm_connection_limiter:get_stats(),
    ?assertEqual(3, maps:get(max_per_peer, Stats)).

test_cleanup_timer() ->
    IP = {10,6,0,1},
    %% Insert a zero-count entry directly (simulating edge case)
    ets:insert(flurm_conn_limits, {IP, 0}),
    ?assertEqual([{IP, 0}], ets:lookup(flurm_conn_limits, IP)),
    %% Trigger cleanup
    whereis(flurm_connection_limiter) ! cleanup,
    timer:sleep(100),
    %% Zero-count entry should be removed
    ?assertEqual([], ets:lookup(flurm_conn_limits, IP)).

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_connection_limiter, bogus)).

test_unknown_cast_info() ->
    gen_server:cast(flurm_connection_limiter, bogus),
    flurm_connection_limiter ! bogus,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_connection_limiter))).
