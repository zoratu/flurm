%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_failover module
%%%
%%% Tests the failover handler including start_link, callbacks,
%%% leadership transitions, and recovery processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_failover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

failover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link starts the server", fun test_start_link/0},
         {"get_status returns valid map", fun test_get_status/0},
         {"on_became_leader triggers state change", fun test_on_became_leader/0},
         {"on_lost_leadership triggers state change", fun test_on_lost_leadership/0},
         {"handle_call for unknown request", fun test_unknown_request/0},
         {"handle_cast for unknown message", fun test_unknown_cast/0},
         {"handle_info for unknown message", fun test_unknown_info/0},
         {"code_change works", fun test_code_change/0},
         {"terminate cancels timer", fun test_terminate/0},
         {"calculate_leader_uptime works", fun test_calculate_leader_uptime/0}
     ]}.

setup() ->
    %% Start dependencies
    application:ensure_all_started(lager),

    %% Stop any existing failover process
    case whereis(flurm_controller_failover) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 1000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,
    ok.

cleanup(_) ->
    %% Stop failover process
    case whereis(flurm_controller_failover) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 1000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_controller_failover:start_link(),
    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_controller_failover)),
    ok.

test_get_status() ->
    ensure_started(),

    Status = flurm_controller_failover:get_status(),

    ?assert(is_map(Status)),
    ?assert(maps:is_key(is_leader, Status)),
    ?assert(maps:is_key(became_leader_time, Status)),
    ?assert(maps:is_key(recovery_status, Status)),
    ?assert(maps:is_key(recovery_error, Status)),
    ?assert(maps:is_key(uptime_as_leader, Status)),

    %% Initially should not be leader
    ?assertEqual(false, maps:get(is_leader, Status)),
    ?assertEqual(idle, maps:get(recovery_status, Status)),
    ?assertEqual(undefined, maps:get(recovery_error, Status)),
    ok.

test_on_became_leader() ->
    ensure_started(),

    %% Get initial status
    Status1 = flurm_controller_failover:get_status(),
    ?assertEqual(false, maps:get(is_leader, Status1)),

    %% Trigger became_leader
    ok = flurm_controller_failover:on_became_leader(),

    %% Sync with gen_server
    _ = sys:get_state(flurm_controller_failover),

    %% Get updated status
    Status2 = flurm_controller_failover:get_status(),
    ?assertEqual(true, maps:get(is_leader, Status2)),
    ?assertNotEqual(undefined, maps:get(became_leader_time, Status2)),
    ok.

test_on_lost_leadership() ->
    ensure_started(),

    %% First become leader
    ok = flurm_controller_failover:on_became_leader(),
    _ = sys:get_state(flurm_controller_failover),

    %% Verify we're leader
    Status1 = flurm_controller_failover:get_status(),
    ?assertEqual(true, maps:get(is_leader, Status1)),

    %% Now lose leadership
    ok = flurm_controller_failover:on_lost_leadership(),
    _ = sys:get_state(flurm_controller_failover),

    %% Verify we're not leader anymore
    Status2 = flurm_controller_failover:get_status(),
    ?assertEqual(false, maps:get(is_leader, Status2)),
    ?assertEqual(undefined, maps:get(became_leader_time, Status2)),
    ?assertEqual(idle, maps:get(recovery_status, Status2)),
    ok.

test_unknown_request() ->
    ensure_started(),

    %% Send unknown request - should return error
    Result = gen_server:call(flurm_controller_failover, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    ensure_started(),

    %% Send unknown cast - should not crash
    ok = gen_server:cast(flurm_controller_failover, {unknown_cast, test}),
    _ = sys:get_state(flurm_controller_failover),

    %% Server should still be running
    ?assertNotEqual(undefined, whereis(flurm_controller_failover)),
    ok.

test_unknown_info() ->
    ensure_started(),

    %% Send unknown info message - should not crash
    Pid = whereis(flurm_controller_failover),
    Pid ! {unknown_info, test},
    _ = sys:get_state(flurm_controller_failover),

    %% Server should still be running
    ?assert(is_process_alive(Pid)),
    ok.

test_code_change() ->
    %% Test code_change callback directly (it's a no-op)
    State = #{some => state},
    {ok, NewState} = flurm_controller_failover:code_change("1.0", State, []),
    ?assertEqual(State, NewState),
    ok.

test_terminate() ->
    ensure_started(),

    %% First trigger leadership to start timer
    ok = flurm_controller_failover:on_became_leader(),
    _ = sys:get_state(flurm_controller_failover),

    %% Now stop the server - should cancel timer without crashing
    Pid = whereis(flurm_controller_failover),
    ok = gen_server:stop(Pid, shutdown, 5000),

    %% Verify it's stopped
    ?assertEqual(undefined, whereis(flurm_controller_failover)),
    ok.

test_calculate_leader_uptime() ->
    ensure_started(),

    %% When not leader, uptime should be 0
    Status1 = flurm_controller_failover:get_status(),
    ?assertEqual(0, maps:get(uptime_as_leader, Status1)),

    %% Become leader
    ok = flurm_controller_failover:on_became_leader(),
    _ = sys:get_state(flurm_controller_failover),
    %% Uptime check: the became_leader_time is recorded, so uptime depends on wall clock
    %% We just verify that status returns consistent data

    %% Uptime should be at least 0 seconds (immediate check)
    Status2 = flurm_controller_failover:get_status(),
    ?assert(maps:get(uptime_as_leader, Status2) >= 0),
    ok.

%%====================================================================
%% Additional Tests - Recovery Process
%%====================================================================

recovery_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         %% Stop any existing process
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end,
         ok
     end,
     fun(_) ->
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end
     end,
     [
         {"recovery process starts on leadership", fun() ->
             {ok, _} = flurm_controller_failover:start_link(),
             ok = flurm_controller_failover:on_became_leader(),
             _ = sys:get_state(flurm_controller_failover),
             Status = flurm_controller_failover:get_status(),
             %% Recovery should have been triggered
             RecoveryStatus = maps:get(recovery_status, Status),
             %% It might be recovering, recovered, or failed depending on available services
             ?assert(RecoveryStatus =/= undefined),
             ok
         end}
     ]}.

%% Test that start_recovery message is handled correctly
start_recovery_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end,
         ok
     end,
     fun(_) ->
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end
     end,
     [
         {"start_recovery message handled", fun() ->
             {ok, _} = flurm_controller_failover:start_link(),
             Pid = whereis(flurm_controller_failover),

             %% Send start_recovery directly
             Pid ! start_recovery,
             _ = sys:get_state(flurm_controller_failover),

             %% Should not crash
             ?assert(is_process_alive(Pid)),
             ok
         end},
         {"recovery_complete success handled", fun() ->
             %% Try to start, handle already_started
             Pid = case whereis(flurm_controller_failover) of
                 undefined ->
                     {ok, P} = flurm_controller_failover:start_link(),
                     P;
                 P -> P
             end,

             %% Simulate recovery complete message
             Pid ! {recovery_complete, {ok, recovered}},
             _ = sys:get_state(flurm_controller_failover),

             Status = flurm_controller_failover:get_status(),
             ?assertEqual(recovered, maps:get(recovery_status, Status)),
             ok
         end},
         {"recovery_complete failure handled", fun() ->
             %% Try to start, handle already_started
             Pid = case whereis(flurm_controller_failover) of
                 undefined ->
                     {ok, P} = flurm_controller_failover:start_link(),
                     P;
                 P -> P
             end,

             %% Simulate recovery failure
             Pid ! {recovery_complete, {error, test_error}},
             _ = sys:get_state(flurm_controller_failover),

             Status = flurm_controller_failover:get_status(),
             ?assertEqual(failed, maps:get(recovery_status, Status)),
             ?assertEqual(test_error, maps:get(recovery_error, Status)),
             ok
         end}
     ]}.

%% Test health check behavior
health_check_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end,
         ok
     end,
     fun(_) ->
         case whereis(flurm_controller_failover) of
             undefined -> ok;
             Pid ->
                 catch unlink(Pid),
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 1000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 2000 ->
                     demonitor(Ref, [flush])
                 end
         end
     end,
     [
         {"health_check message when not leader", fun() ->
             {ok, _} = flurm_controller_failover:start_link(),
             Pid = whereis(flurm_controller_failover),

             %% Send health check when not leader
             Pid ! health_check,
             _ = sys:get_state(flurm_controller_failover),

             %% Should not crash
             ?assert(is_process_alive(Pid)),
             ok
         end}
     ]}.

%%====================================================================
%% Helper Functions
%%====================================================================

ensure_started() ->
    case whereis(flurm_controller_failover) of
        undefined ->
            {ok, Pid} = flurm_controller_failover:start_link(),
            unlink(Pid),
            ok;
        _Pid ->
            ok
    end.
