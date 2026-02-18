%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_test_utils module - 100% coverage target
%%%
%%% Tests all utility functions for proper test synchronization:
%%% - Process death synchronization (wait_for_death, kill_and_wait)
%%% - Process registration synchronization (wait_for_registered, wait_for_unregistered)
%%% - gen_server synchronization (sync_cast, sync_send)
%%% - Mock synchronization (wait_for_mock_call)
%%% - General utilities (flush_mailbox, with_timeout)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_utils_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_TIMEOUT, 5000).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    %% Clean up any test processes
    lists:foreach(fun(Name) ->
        case whereis(Name) of
            undefined -> ok;
            Pid -> exit(Pid, kill)
        end
    end, [test_server, test_gen_server, test_registered_proc]),
    timer:sleep(10),
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

flurm_test_utils_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         %% wait_for_death tests
         fun test_wait_for_death_immediate/1,
         fun test_wait_for_death_with_delay/1,
         fun test_wait_for_death_already_dead/1,
         fun test_wait_for_death_timeout/1,
         fun test_wait_for_death_custom_timeout/1,
         fun test_wait_for_death_after_timeout_check/1,

         %% kill_and_wait tests
         fun test_kill_and_wait_normal/1,
         fun test_kill_and_wait_linked_process/1,
         fun test_kill_and_wait_timeout_then_kill/1,
         fun test_kill_and_wait_custom_timeout/1,

         %% wait_for_registered tests
         fun test_wait_for_registered_already_registered/1,
         fun test_wait_for_registered_registers_later/1,
         fun test_wait_for_registered_timeout/1,
         fun test_wait_for_registered_custom_timeout/1,

         %% wait_for_unregistered tests
         fun test_wait_for_unregistered_already_unregistered/1,
         fun test_wait_for_unregistered_unregisters_later/1,
         fun test_wait_for_unregistered_timeout/1,
         fun test_wait_for_unregistered_custom_timeout/1,

         %% sync_cast tests
         fun test_sync_cast_basic/1,
         fun test_sync_cast_with_atom_name/1,

         %% sync_send tests
         fun test_sync_send_basic/1,
         fun test_sync_send_with_atom_name/1,

         %% flush_mailbox tests
         fun test_flush_mailbox_empty/1,
         fun test_flush_mailbox_with_messages/1,
         fun test_flush_mailbox_multiple_messages/1,

         %% with_timeout tests
         fun test_with_timeout_success/1,
         fun test_with_timeout_timeout/1,
         fun test_with_timeout_immediate_return/1,
         fun test_with_timeout_complex_result/1
     ]}.

%%====================================================================
%% wait_for_death Tests
%%====================================================================

test_wait_for_death_immediate(_) ->
    ?_test(begin
        Pid = spawn(fun() -> ok end),
        timer:sleep(10),  % Let it die
        Result = flurm_test_utils:wait_for_death(Pid),
        ?assertEqual(ok, Result)
    end).

test_wait_for_death_with_delay(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            timer:sleep(50),
            ok
        end),
        Result = flurm_test_utils:wait_for_death(Pid),
        ?assertEqual(ok, Result)
    end).

test_wait_for_death_already_dead(_) ->
    ?_test(begin
        Pid = spawn(fun() -> ok end),
        timer:sleep(50),  % Ensure it's dead
        ?assertNot(is_process_alive(Pid)),
        Result = flurm_test_utils:wait_for_death(Pid),
        ?assertEqual(ok, Result)
    end).

test_wait_for_death_timeout(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            receive stop -> ok end
        end),
        Result = flurm_test_utils:wait_for_death(Pid, 50),
        ?assertEqual({error, timeout}, Result),
        %% Clean up
        exit(Pid, kill)
    end).

test_wait_for_death_custom_timeout(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            timer:sleep(30),
            ok
        end),
        Result = flurm_test_utils:wait_for_death(Pid, 200),
        ?assertEqual(ok, Result)
    end).

test_wait_for_death_after_timeout_check(_) ->
    ?_test(begin
        %% Test the case where process dies after timeout but before is_process_alive check
        Pid = spawn(fun() ->
            timer:sleep(10),
            ok
        end),
        timer:sleep(50),  % Let it die
        Result = flurm_test_utils:wait_for_death(Pid, 1),  % Very short timeout
        ?assertEqual(ok, Result)  % Should still be ok because process is dead
    end).

%%====================================================================
%% kill_and_wait Tests
%%====================================================================

test_kill_and_wait_normal(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            receive stop -> ok end
        end),
        Result = flurm_test_utils:kill_and_wait(Pid),
        ?assertEqual(ok, Result),
        ?assertNot(is_process_alive(Pid))
    end).

test_kill_and_wait_linked_process(_) ->
    ?_test(begin
        Self = self(),
        Pid = spawn_link(fun() ->
            receive stop -> ok end
        end),
        %% Unlink before kill to prevent cascade
        unlink(Pid),
        Result = flurm_test_utils:kill_and_wait(Pid),
        ?assertEqual(ok, Result),
        ?assert(is_process_alive(Self))
    end).

test_kill_and_wait_timeout_then_kill(_) ->
    ?_test(begin
        %% Process that traps exits and ignores shutdown
        Pid = spawn(fun() ->
            process_flag(trap_exit, true),
            receive
                {'EXIT', _, kill} -> ok;
                _ -> timer:sleep(infinity)
            end
        end),
        Result = flurm_test_utils:kill_and_wait(Pid, 50),
        ?assertEqual(ok, Result),
        ?assertNot(is_process_alive(Pid))
    end).

test_kill_and_wait_custom_timeout(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            receive stop -> ok end
        end),
        Result = flurm_test_utils:kill_and_wait(Pid, 1000),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% wait_for_registered Tests
%%====================================================================

test_wait_for_registered_already_registered(_) ->
    ?_test(begin
        Pid = spawn(fun() -> receive stop -> ok end end),
        register(test_already_reg_cov, Pid),
        Result = flurm_test_utils:wait_for_registered(test_already_reg_cov),
        ?assertEqual(Pid, Result),
        exit(Pid, kill),
        unregister_safe(test_already_reg_cov)
    end).

test_wait_for_registered_registers_later(_) ->
    ?_test(begin
        Self = self(),
        spawn(fun() ->
            timer:sleep(50),
            Pid = spawn(fun() -> receive stop -> ok end end),
            register(test_later_reg_cov, Pid),
            Self ! {registered, Pid}
        end),
        Result = flurm_test_utils:wait_for_registered(test_later_reg_cov, 500),
        receive
            {registered, ExpectedPid} ->
                ?assertEqual(ExpectedPid, Result),
                exit(ExpectedPid, kill)
        after 1000 ->
            ?assert(false)
        end,
        unregister_safe(test_later_reg_cov)
    end).

test_wait_for_registered_timeout(_) ->
    ?_test(begin
        Result = flurm_test_utils:wait_for_registered(test_never_reg_cov, 50),
        ?assertEqual({error, timeout}, Result)
    end).

test_wait_for_registered_custom_timeout(_) ->
    ?_test(begin
        Result = flurm_test_utils:wait_for_registered(test_never_reg_custom_cov, 100),
        ?assertEqual({error, timeout}, Result)
    end).

%%====================================================================
%% wait_for_unregistered Tests
%%====================================================================

test_wait_for_unregistered_already_unregistered(_) ->
    ?_test(begin
        Result = flurm_test_utils:wait_for_unregistered(test_not_registered_cov),
        ?assertEqual(ok, Result)
    end).

test_wait_for_unregistered_unregisters_later(_) ->
    ?_test(begin
        Pid = spawn(fun() ->
            receive stop -> ok end
        end),
        register(test_unreg_later_cov, Pid),
        spawn(fun() ->
            timer:sleep(50),
            exit(Pid, kill)
        end),
        Result = flurm_test_utils:wait_for_unregistered(test_unreg_later_cov, 500),
        ?assertEqual(ok, Result)
    end).

test_wait_for_unregistered_timeout(_) ->
    ?_test(begin
        Pid = spawn(fun() -> receive stop -> ok end end),
        register(test_stays_registered_cov, Pid),
        Result = flurm_test_utils:wait_for_unregistered(test_stays_registered_cov, 50),
        ?assertEqual({error, timeout}, Result),
        exit(Pid, kill),
        unregister_safe(test_stays_registered_cov)
    end).

test_wait_for_unregistered_custom_timeout(_) ->
    ?_test(begin
        Pid = spawn(fun() -> receive stop -> ok end end),
        register(test_stays_reg_custom_cov, Pid),
        Result = flurm_test_utils:wait_for_unregistered(test_stays_reg_custom_cov, 100),
        ?assertEqual({error, timeout}, Result),
        exit(Pid, kill),
        unregister_safe(test_stays_reg_custom_cov)
    end).

%%====================================================================
%% sync_cast Tests (using flurm_trace as a real gen_server)
%%====================================================================

test_sync_cast_basic(_) ->
    ?_test(begin
        case whereis(flurm_trace) of
            undefined ->
                {ok, Pid} = flurm_trace:start_link(),
                flurm_test_utils:sync_cast(Pid, {record_event, test, data}),
                gen_server:stop(Pid);
            Pid ->
                flurm_test_utils:sync_cast(Pid, {record_event, test, data})
        end,
        ?assert(true)
    end).

test_sync_cast_with_atom_name(_) ->
    ?_test(begin
        case whereis(flurm_trace) of
            undefined ->
                {ok, _Pid} = flurm_trace:start_link();
            _ ->
                ok
        end,
        Result = flurm_test_utils:sync_cast(flurm_trace, {record_event, test, data}),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% sync_send Tests
%%====================================================================

test_sync_send_basic(_) ->
    ?_test(begin
        case whereis(flurm_trace) of
            undefined ->
                {ok, Pid} = flurm_trace:start_link(),
                flurm_test_utils:sync_send(Pid, test_info),
                gen_server:stop(Pid);
            Pid ->
                flurm_test_utils:sync_send(Pid, test_info)
        end,
        ?assert(true)
    end).

test_sync_send_with_atom_name(_) ->
    ?_test(begin
        case whereis(flurm_trace) of
            undefined ->
                {ok, _Pid} = flurm_trace:start_link();
            _ ->
                ok
        end,
        Result = flurm_test_utils:sync_send(flurm_trace, test_info),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% flush_mailbox Tests
%%====================================================================

test_flush_mailbox_empty(_) ->
    ?_test(begin
        Result = flurm_test_utils:flush_mailbox(),
        ?assertEqual(ok, Result)
    end).

test_flush_mailbox_with_messages(_) ->
    ?_test(begin
        self() ! message1,
        Result = flurm_test_utils:flush_mailbox(),
        ?assertEqual(ok, Result),
        %% Verify mailbox is empty
        receive
            _ -> ?assert(false)
        after 0 ->
            ok
        end
    end).

test_flush_mailbox_multiple_messages(_) ->
    ?_test(begin
        self() ! message1,
        self() ! message2,
        self() ! message3,
        self() ! {complex, message, [1, 2, 3]},
        Result = flurm_test_utils:flush_mailbox(),
        ?assertEqual(ok, Result),
        %% Verify mailbox is empty
        receive
            _ -> ?assert(false)
        after 0 ->
            ok
        end
    end).

%%====================================================================
%% with_timeout Tests
%%====================================================================

test_with_timeout_success(_) ->
    ?_test(begin
        Fun = fun() -> {computed, result} end,
        Result = flurm_test_utils:with_timeout(Fun, 1000),
        ?assertEqual({ok, {computed, result}}, Result)
    end).

test_with_timeout_timeout(_) ->
    ?_test(begin
        Fun = fun() ->
            timer:sleep(500),
            should_not_return
        end,
        Result = flurm_test_utils:with_timeout(Fun, 50),
        ?assertEqual({error, timeout}, Result)
    end).

test_with_timeout_immediate_return(_) ->
    ?_test(begin
        Fun = fun() -> immediate end,
        Result = flurm_test_utils:with_timeout(Fun, 1000),
        ?assertEqual({ok, immediate}, Result)
    end).

test_with_timeout_complex_result(_) ->
    ?_test(begin
        Fun = fun() ->
            #{key => value, list => [1, 2, 3], nested => #{a => b}}
        end,
        {ok, Result} = flurm_test_utils:with_timeout(Fun, 1000),
        ?assertEqual(#{key => value, list => [1, 2, 3], nested => #{a => b}}, Result)
    end).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"wait_for_death with default timeout",
         ?_test(begin
             Pid = spawn(fun() -> ok end),
             timer:sleep(10),
             Result = flurm_test_utils:wait_for_death(Pid),
             ?assertEqual(ok, Result)
         end)},

        {"kill_and_wait with default timeout",
         ?_test(begin
             Pid = spawn(fun() -> receive _ -> ok end end),
             Result = flurm_test_utils:kill_and_wait(Pid),
             ?assertEqual(ok, Result)
         end)},

        {"wait_for_registered with default timeout",
         ?_test(begin
             Pid = spawn(fun() -> receive _ -> ok end end),
             register(edge_test_reg_cov, Pid),
             Result = flurm_test_utils:wait_for_registered(edge_test_reg_cov),
             ?assertEqual(Pid, Result),
             exit(Pid, kill),
             unregister_safe(edge_test_reg_cov)
         end)},

        {"wait_for_unregistered with default timeout",
         ?_test(begin
             Result = flurm_test_utils:wait_for_unregistered(edge_test_unreg_cov),
             ?assertEqual(ok, Result)
         end)},

        {"flush_mailbox multiple times",
         ?_test(begin
             ok = flurm_test_utils:flush_mailbox(),
             ok = flurm_test_utils:flush_mailbox(),
             ok = flurm_test_utils:flush_mailbox()
         end)}
    ].

%%====================================================================
%% Stress Tests
%%====================================================================

stress_tests_test_() ->
    [
        {"Rapid process creation and death",
         ?_test(begin
             Results = lists:map(fun(_) ->
                 Pid = spawn(fun() -> ok end),
                 flurm_test_utils:wait_for_death(Pid)
             end, lists:seq(1, 50)),
             ?assert(lists:all(fun(R) -> R =:= ok end, Results))
         end)},

        {"Multiple concurrent registrations",
         ?_test(begin
             Pids = lists:map(fun(I) ->
                 Name = list_to_atom("stress_reg_cov_" ++ integer_to_list(I)),
                 Pid = spawn(fun() -> receive stop -> ok end end),
                 register(Name, Pid),
                 {Name, Pid}
             end, lists:seq(1, 10)),
             Results = lists:map(fun({Name, ExpectedPid}) ->
                 Result = flurm_test_utils:wait_for_registered(Name),
                 exit(ExpectedPid, kill),
                 unregister_safe(Name),
                 {Result, ExpectedPid}
             end, Pids),
             ?assert(lists:all(fun({R, E}) -> R =:= E end, Results))
         end)},

        {"Flush mailbox with many messages",
         ?_test(begin
             lists:foreach(fun(I) ->
                 self() ! {msg, I}
             end, lists:seq(1, 1000)),
             ok = flurm_test_utils:flush_mailbox(),
             receive _ -> ?assert(false) after 0 -> ok end
         end)}
    ].

%%====================================================================
%% Wait for registered loop coverage tests
%%====================================================================

registration_loop_tests_test_() ->
    [
        {"wait_for_registered_loop with yield",
         ?_test(begin
             %% Start a slow registration
             Self = self(),
             spawn(fun() ->
                 timer:sleep(30),
                 Pid = spawn(fun() -> receive stop -> ok end end),
                 register(slow_reg_test_cov, Pid),
                 Self ! {done, Pid}
             end),
             Result = flurm_test_utils:wait_for_registered(slow_reg_test_cov, 500),
             receive
                 {done, Pid} ->
                     ?assertEqual(Pid, Result),
                     exit(Pid, kill),
                     unregister_safe(slow_reg_test_cov)
             after 1000 ->
                 ?assert(false)
             end
         end)},

        {"wait_for_unregistered_loop with yield",
         ?_test(begin
             Pid = spawn(fun() -> receive stop -> ok end end),
             register(unreg_loop_test_cov, Pid),
             spawn(fun() ->
                 timer:sleep(30),
                 exit(Pid, kill)
             end),
             Result = flurm_test_utils:wait_for_unregistered(unreg_loop_test_cov, 500),
             ?assertEqual(ok, Result)
         end)}
    ].

%%====================================================================
%% Mock call tests (when meck is available)
%%====================================================================

wait_for_mock_call_tests_test_() ->
    case code:ensure_loaded(meck) of
        {module, meck} ->
            [
                {"wait_for_mock_call with default timeout",
                 ?_test(begin
                     meck:new(mock_test_mod_cov, [non_strict]),
                     meck:expect(mock_test_mod_cov, test_fun, fun() -> ok end),
                     spawn(fun() ->
                         timer:sleep(10),
                         mock_test_mod_cov:test_fun()
                     end),
                     Result = flurm_test_utils:wait_for_mock_call(mock_test_mod_cov, test_fun, 0),
                     ?assertEqual(ok, Result),
                     meck:unload(mock_test_mod_cov)
                 end)},

                {"wait_for_mock_call custom timeout",
                 ?_test(begin
                     meck:new(mock_test_mod_cov2, [non_strict]),
                     meck:expect(mock_test_mod_cov2, test_fun, fun() -> ok end),
                     spawn(fun() ->
                         timer:sleep(10),
                         mock_test_mod_cov2:test_fun()
                     end),
                     Result = flurm_test_utils:wait_for_mock_call(mock_test_mod_cov2, test_fun, 0, 500),
                     ?assertEqual(ok, Result),
                     meck:unload(mock_test_mod_cov2)
                 end)},

                {"wait_for_mock_call timeout",
                 ?_test(begin
                     meck:new(mock_test_mod_cov3, [non_strict]),
                     meck:expect(mock_test_mod_cov3, never_called, fun() -> ok end),
                     Result = flurm_test_utils:wait_for_mock_call(mock_test_mod_cov3, never_called, 0, 50),
                     ?assertEqual({error, timeout}, Result),
                     meck:unload(mock_test_mod_cov3)
                 end)}
            ];
        _ ->
            []
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Helper to safely unregister a name
unregister_safe(Name) ->
    try
        unregister(Name)
    catch
        error:badarg -> ok
    end.
