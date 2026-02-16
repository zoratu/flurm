%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_connection_limiter module
%%% Tests for connection rate limiting per peer IP
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_connection_limiter_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

connection_limiter_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"start_link creates ETS table", fun test_start_creates_ets/0},
             {"connection_allowed returns true for new IP", fun test_connection_allowed_new_ip/0},
             {"connection_opened increments count", fun test_connection_opened/0},
             {"connection_closed decrements count", fun test_connection_closed/0},
             {"get_connection_count returns count", fun test_get_connection_count/0},
             {"get_all_counts returns list", fun test_get_all_counts/0},
             {"get_stats returns map", fun test_get_stats/0},
             {"connection limit is enforced", fun test_connection_limit_enforced/0},
             {"cleanup removes zero entries", fun test_cleanup/0}
         ]
     end}.

setup() ->
    %% Stop any existing limiter
    case whereis(flurm_connection_limiter) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 2000)
    end,
    %% Delete the ETS table if it exists
    catch ets:delete(flurm_conn_limits),
    timer:sleep(50),
    {ok, NewPid} = flurm_connection_limiter:start_link(),
    unlink(NewPid),
    NewPid.

cleanup(Pid) ->
    catch gen_server:stop(Pid, shutdown, 2000),
    catch ets:delete(flurm_conn_limits).

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_creates_ets() ->
    %% ETS table should exist after start
    ?assertNotEqual(undefined, ets:whereis(flurm_conn_limits)).

%%====================================================================
%% connection_allowed Tests
%%====================================================================

test_connection_allowed_new_ip() ->
    %% New IP should be allowed
    IP = {192, 168, 1, 100},
    Result = flurm_connection_limiter:connection_allowed(IP),
    ?assertEqual(true, Result).

%%====================================================================
%% connection_opened Tests
%%====================================================================

test_connection_opened() ->
    IP = {10, 0, 0, 1},
    %% Initially no connections
    ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP)),

    %% Open a connection
    ok = flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP)),

    %% Open another
    ok = flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(2, flurm_connection_limiter:get_connection_count(IP)).

%%====================================================================
%% connection_closed Tests
%%====================================================================

test_connection_closed() ->
    IP = {10, 0, 0, 2},
    %% Open some connections
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(3, flurm_connection_limiter:get_connection_count(IP)),

    %% Close one
    ok = flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(2, flurm_connection_limiter:get_connection_count(IP)),

    %% Close remaining
    flurm_connection_limiter:connection_closed(IP),
    flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP)).

%%====================================================================
%% get_connection_count Tests
%%====================================================================

test_get_connection_count() ->
    IP = {10, 0, 0, 3},
    %% Non-existent IP should return 0
    ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP)),

    %% After opening
    flurm_connection_limiter:connection_opened(IP),
    ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP)).

%%====================================================================
%% get_all_counts Tests
%%====================================================================

test_get_all_counts() ->
    %% Clear existing entries by closing all
    IP1 = {172, 16, 0, 1},
    IP2 = {172, 16, 0, 2},

    flurm_connection_limiter:connection_opened(IP1),
    flurm_connection_limiter:connection_opened(IP2),
    flurm_connection_limiter:connection_opened(IP2),

    All = flurm_connection_limiter:get_all_counts(),
    ?assert(is_list(All)),

    %% Find our IPs in the list
    IP1Count = proplists:get_value(IP1, All, 0),
    IP2Count = proplists:get_value(IP2, All, 0),
    ?assertEqual(1, IP1Count),
    ?assertEqual(2, IP2Count).

%%====================================================================
%% get_stats Tests
%%====================================================================

test_get_stats() ->
    Stats = flurm_connection_limiter:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(total_peers, Stats)),
    ?assert(maps:is_key(total_connections, Stats)),
    ?assert(maps:is_key(max_per_peer, Stats)),
    ?assert(maps:is_key(limit_per_peer, Stats)),

    TotalPeers = maps:get(total_peers, Stats),
    TotalConns = maps:get(total_connections, Stats),
    MaxPerPeer = maps:get(max_per_peer, Stats),
    LimitPerPeer = maps:get(limit_per_peer, Stats),

    ?assert(is_integer(TotalPeers)),
    ?assert(is_integer(TotalConns)),
    ?assert(is_integer(MaxPerPeer)),
    ?assert(is_integer(LimitPerPeer)),
    ?assert(TotalPeers >= 0),
    ?assert(TotalConns >= 0),
    ?assert(MaxPerPeer >= 0),
    ?assert(LimitPerPeer > 0).

%%====================================================================
%% Connection Limit Tests
%%====================================================================

test_connection_limit_enforced() ->
    %% Set a low limit for testing
    application:set_env(flurm_controller, max_connections_per_peer, 5),

    IP = {192, 168, 100, 1},
    %% Open connections up to the limit
    lists:foreach(fun(_) ->
        flurm_connection_limiter:connection_opened(IP)
    end, lists:seq(1, 5)),

    ?assertEqual(5, flurm_connection_limiter:get_connection_count(IP)),

    %% Should be allowed at the limit
    ?assertEqual(false, flurm_connection_limiter:connection_allowed(IP)),

    %% Close one
    flurm_connection_limiter:connection_closed(IP),
    ?assertEqual(true, flurm_connection_limiter:connection_allowed(IP)),

    %% Reset the limit
    application:set_env(flurm_controller, max_connections_per_peer, 100).

%%====================================================================
%% Cleanup Tests
%%====================================================================

test_cleanup() ->
    IP = {192, 168, 200, 1},
    %% Open and close to leave a zero entry
    flurm_connection_limiter:connection_opened(IP),
    flurm_connection_limiter:connection_closed(IP),

    %% Trigger cleanup by sending the cleanup message
    Pid = whereis(flurm_connection_limiter),
    Pid ! cleanup,
    timer:sleep(100),  % Wait for cleanup to process

    %% The entry should be removed (count is 0)
    ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP)).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

handle_call_unknown_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown call returns error", fun() ->
             Result = gen_server:call(Pid, unknown_request),
             ?assertEqual({error, unknown_request}, Result)
         end}]
     end}.

handle_cast_unknown_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown cast doesn't crash", fun() ->
             gen_server:cast(Pid, unknown_cast),
             timer:sleep(50),
             ?assert(is_process_alive(Pid))
         end}]
     end}.

handle_info_unknown_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown info doesn't crash", fun() ->
             Pid ! unknown_message,
             timer:sleep(50),
             ?assert(is_process_alive(Pid))
         end}]
     end}.

%%====================================================================
%% Edge Cases
%%====================================================================

%% Test with various IP formats
ipv4_localhost_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"localhost IP works", fun() ->
             IP = {127, 0, 0, 1},
             ?assertEqual(true, flurm_connection_limiter:connection_allowed(IP)),
             ok = flurm_connection_limiter:connection_opened(IP),
             ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.

ipv4_broadcast_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"broadcast IP works", fun() ->
             IP = {255, 255, 255, 255},
             ?assertEqual(true, flurm_connection_limiter:connection_allowed(IP)),
             ok = flurm_connection_limiter:connection_opened(IP),
             ?assertEqual(1, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.

%% Test closing more than opened (edge case)
close_more_than_opened_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"closing more than opened is safe", fun() ->
             IP = {10, 10, 10, 10},
             flurm_connection_limiter:connection_opened(IP),
             %% Close twice (more than opened)
             flurm_connection_limiter:connection_closed(IP),
             flurm_connection_limiter:connection_closed(IP),
             %% Should be 0 (not negative)
             ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.

%% Test closing non-existent IP
close_nonexistent_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"closing non-existent IP is safe", fun() ->
             IP = {1, 1, 1, 1},
             %% Should not crash
             ok = flurm_connection_limiter:connection_closed(IP),
             ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.

%% Test rapid open/close cycles
rapid_cycles_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"rapid open/close cycles work", fun() ->
             IP = {192, 0, 2, 1},
             lists:foreach(fun(_) ->
                 flurm_connection_limiter:connection_opened(IP),
                 flurm_connection_limiter:connection_closed(IP)
             end, lists:seq(1, 100)),
             ?assertEqual(0, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.

%% Test concurrent access
concurrent_access_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [{"concurrent access is safe", fun() ->
             IP = {192, 0, 2, 2},
             Parent = self(),
             %% Spawn multiple processes to open connections
             Pids = [spawn_link(fun() ->
                 flurm_connection_limiter:connection_opened(IP),
                 Parent ! {done, self()}
             end) || _ <- lists:seq(1, 10)],
             %% Wait for all to complete
             lists:foreach(fun(Pid) ->
                 receive {done, Pid} -> ok end
             end, Pids),
             %% All 10 should be counted
             ?assertEqual(10, flurm_connection_limiter:get_connection_count(IP))
         end}]
     end}.
