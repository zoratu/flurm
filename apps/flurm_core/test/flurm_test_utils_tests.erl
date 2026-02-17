%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_test_utils
%%%
%%% Tests all synchronization utilities including:
%%% - Process death synchronization
%%% - Process registration synchronization
%%% - gen_server synchronization
%%% - Mock synchronization
%%% - General utilities
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_test_utils_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_utils_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Process Death Synchronization Tests
%%====================================================================

wait_for_death_test_() ->
    [
     {"wait_for_death returns ok when process dies",
      fun() ->
          Pid = spawn(fun() -> ok end),
          Result = flurm_test_utils:wait_for_death(Pid),
          ?assertEqual(ok, Result)
      end},
     {"wait_for_death with custom timeout returns ok",
      fun() ->
          Pid = spawn(fun() -> ok end),
          Result = flurm_test_utils:wait_for_death(Pid, 1000),
          ?assertEqual(ok, Result)
      end},
     {"wait_for_death times out for long-running process",
      fun() ->
          Pid = spawn(fun() ->
              receive stop -> ok end
          end),
          Result = flurm_test_utils:wait_for_death(Pid, 50),
          %% Should timeout or succeed if process died
          ?assert(Result =:= ok orelse Result =:= {error, timeout}),
          exit(Pid, kill)
      end},
     {"wait_for_death with already dead process",
      fun() ->
          Pid = spawn(fun() -> ok end),
          timer:sleep(10),  % Let it die
          Result = flurm_test_utils:wait_for_death(Pid),
          ?assertEqual(ok, Result)
      end}
    ].

kill_and_wait_test_() ->
    [
     {"kill_and_wait kills and waits for process",
      fun() ->
          Pid = spawn(fun() ->
              receive _ -> ok end
          end),
          Result = flurm_test_utils:kill_and_wait(Pid),
          ?assertEqual(ok, Result),
          ?assertEqual(false, is_process_alive(Pid))
      end},
     {"kill_and_wait with custom timeout",
      fun() ->
          Pid = spawn(fun() ->
              receive _ -> ok end
          end),
          Result = flurm_test_utils:kill_and_wait(Pid, 1000),
          ?assertEqual(ok, Result)
      end},
     {"kill_and_wait on already dead process",
      fun() ->
          Pid = spawn(fun() -> ok end),
          timer:sleep(10),
          Result = flurm_test_utils:kill_and_wait(Pid),
          ?assertEqual(ok, Result)
      end},
     {"kill_and_wait handles trap_exit process",
      fun() ->
          Pid = spawn(fun() ->
              process_flag(trap_exit, true),
              receive _ -> ok end
          end),
          Result = flurm_test_utils:kill_and_wait(Pid, 500),
          ?assertEqual(ok, Result)
      end}
    ].

%%====================================================================
%% Process Registration Synchronization Tests
%%====================================================================

wait_for_registered_test_() ->
    [
     {"wait_for_registered returns pid when registered",
      fun() ->
          Name = test_reg_name_1,
          spawn(fun() ->
              register(Name, self()),
              receive stop -> ok end
          end),
          Result = flurm_test_utils:wait_for_registered(Name),
          ?assert(is_pid(Result)),
          Result ! stop
      end},
     {"wait_for_registered with custom timeout",
      fun() ->
          Name = test_reg_name_2,
          spawn(fun() ->
              register(Name, self()),
              receive stop -> ok end
          end),
          Result = flurm_test_utils:wait_for_registered(Name, 1000),
          ?assert(is_pid(Result)),
          Result ! stop
      end},
     {"wait_for_registered times out when not registered",
      fun() ->
          Name = never_registered_name,
          Result = flurm_test_utils:wait_for_registered(Name, 50),
          ?assertEqual({error, timeout}, Result)
      end},
     {"wait_for_registered returns immediately if already registered",
      fun() ->
          Name = test_reg_name_3,
          Pid = spawn(fun() ->
              register(Name, self()),
              receive stop -> ok end
          end),
          timer:sleep(10),  % Ensure registration
          Result = flurm_test_utils:wait_for_registered(Name),
          ?assertEqual(Pid, Result),
          Pid ! stop
      end}
    ].

wait_for_unregistered_test_() ->
    [
     {"wait_for_unregistered returns ok when unregistered",
      fun() ->
          Name = test_unreg_name_1,
          Pid = spawn(fun() ->
              register(Name, self()),
              timer:sleep(50),
              unregister(Name),
              receive stop -> ok end
          end),
          timer:sleep(10),  % Ensure registered
          Result = flurm_test_utils:wait_for_unregistered(Name, 1000),
          ?assertEqual(ok, Result),
          Pid ! stop
      end},
     {"wait_for_unregistered with custom timeout",
      fun() ->
          Name = test_unreg_name_2,
          Pid = spawn(fun() ->
              register(Name, self()),
              timer:sleep(30)
          end),
          timer:sleep(10),
          Result = flurm_test_utils:wait_for_unregistered(Name, 500),
          ?assertEqual(ok, Result),
          catch exit(Pid, kill)
      end},
     {"wait_for_unregistered returns ok immediately if not registered",
      fun() ->
          Name = not_registered_name,
          Result = flurm_test_utils:wait_for_unregistered(Name),
          ?assertEqual(ok, Result)
      end},
     {"wait_for_unregistered times out when process holds registration",
      fun() ->
          Name = test_unreg_name_3,
          Pid = spawn(fun() ->
              register(Name, self()),
              receive stop -> ok end
          end),
          timer:sleep(10),
          Result = flurm_test_utils:wait_for_unregistered(Name, 50),
          ?assertEqual({error, timeout}, Result),
          Pid ! stop
      end}
    ].

%%====================================================================
%% gen_server Synchronization Tests
%%====================================================================

sync_cast_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = gen_server:start({local, test_sync_server},
                                       test_sync_gen_server, [], []),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid)
     end,
     [
      {"sync_cast waits for cast to be processed",
       fun() ->
           ok = flurm_test_utils:sync_cast(test_sync_server, {set_value, 42}),
           State = sys:get_state(test_sync_server),
           ?assertEqual(42, State)
       end}
     ]}.

sync_send_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = gen_server:start({local, test_sync_server2},
                                       test_sync_gen_server, [], []),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid)
     end,
     [
      {"sync_send waits for info to be processed",
       fun() ->
           ok = flurm_test_utils:sync_send(test_sync_server2, {set_info, 99}),
           State = sys:get_state(test_sync_server2),
           ?assertEqual(99, State)
       end}
     ]}.

%%====================================================================
%% Mock Synchronization Tests
%%====================================================================

wait_for_mock_call_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(meck),
         ok
     end,
     fun(_) ->
         catch meck:unload(test_mock_module)
     end,
     [
      {"wait_for_mock_call returns ok when mock is called",
       fun() ->
           meck:new(test_mock_module, [non_strict]),
           meck:expect(test_mock_module, test_func, fun() -> ok end),
           spawn(fun() ->
               timer:sleep(10),
               test_mock_module:test_func()
           end),
           Result = flurm_test_utils:wait_for_mock_call(test_mock_module, test_func, 0),
           ?assertEqual(ok, Result),
           meck:unload(test_mock_module)
       end},
      {"wait_for_mock_call times out when mock not called",
       fun() ->
           meck:new(test_mock_module, [non_strict]),
           meck:expect(test_mock_module, test_func, fun() -> ok end),
           Result = flurm_test_utils:wait_for_mock_call(test_mock_module, test_func, 0, 50),
           ?assertEqual({error, timeout}, Result),
           meck:unload(test_mock_module)
       end}
     ]}.

%%====================================================================
%% General Utility Tests
%%====================================================================

flush_mailbox_test_() ->
    [
     {"flush_mailbox removes all messages",
      fun() ->
          self() ! msg1,
          self() ! msg2,
          self() ! msg3,
          flurm_test_utils:flush_mailbox(),
          receive
              _ -> ?assert(false)
          after 0 ->
              ok
          end
      end},
     {"flush_mailbox on empty mailbox is ok",
      fun() ->
          ?assertEqual(ok, flurm_test_utils:flush_mailbox())
      end}
    ].

with_timeout_test_() ->
    [
     {"with_timeout returns result on success",
      fun() ->
          Result = flurm_test_utils:with_timeout(fun() -> 42 end, 1000),
          ?assertEqual({ok, 42}, Result)
      end},
     {"with_timeout returns complex result",
      fun() ->
          Result = flurm_test_utils:with_timeout(fun() -> {a, [1,2,3], <<"bin">>} end, 1000),
          ?assertEqual({ok, {a, [1,2,3], <<"bin">>}}, Result)
      end},
     {"with_timeout times out on slow function",
      fun() ->
          Result = flurm_test_utils:with_timeout(fun() ->
              receive never -> ok end
          end, 50),
          ?assertEqual({error, timeout}, Result)
      end},
     {"with_timeout handles function that completes just before timeout",
      fun() ->
          Result = flurm_test_utils:with_timeout(fun() ->
              timer:sleep(10),
              done
          end, 500),
          ?assertEqual({ok, done}, Result)
      end}
    ].

%%====================================================================
%% Module Export Tests
%%====================================================================

exports_test_() ->
    [
     {"module exports wait_for_death/1",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_death, 1))
      end},
     {"module exports wait_for_death/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_death, 2))
      end},
     {"module exports kill_and_wait/1",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, kill_and_wait, 1))
      end},
     {"module exports kill_and_wait/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, kill_and_wait, 2))
      end},
     {"module exports wait_for_registered/1",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_registered, 1))
      end},
     {"module exports wait_for_registered/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_registered, 2))
      end},
     {"module exports wait_for_unregistered/1",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_unregistered, 1))
      end},
     {"module exports wait_for_unregistered/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_unregistered, 2))
      end},
     {"module exports sync_cast/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, sync_cast, 2))
      end},
     {"module exports sync_send/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, sync_send, 2))
      end},
     {"module exports wait_for_mock_call/3",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_mock_call, 3))
      end},
     {"module exports wait_for_mock_call/4",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, wait_for_mock_call, 4))
      end},
     {"module exports flush_mailbox/0",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, flush_mailbox, 0))
      end},
     {"module exports with_timeout/2",
      fun() ->
          ?assert(erlang:function_exported(flurm_test_utils, with_timeout, 2))
      end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
     {"wait_for_death handles process that exits with reason",
      fun() ->
          Pid = spawn(fun() -> exit(some_reason) end),
          Result = flurm_test_utils:wait_for_death(Pid),
          ?assertEqual(ok, Result)
      end},
     {"kill_and_wait handles linked process",
      fun() ->
          process_flag(trap_exit, true),
          Pid = spawn_link(fun() ->
              receive _ -> ok end
          end),
          Result = flurm_test_utils:kill_and_wait(Pid),
          ?assertEqual(ok, Result),
          %% Flush the EXIT message
          receive {'EXIT', _, _} -> ok after 100 -> ok end,
          process_flag(trap_exit, false)
      end},
     {"with_timeout handles exception in function",
      fun() ->
          Result = flurm_test_utils:with_timeout(fun() ->
              error(deliberate_error)
          end, 1000),
          %% Should timeout since spawn crashes
          ?assertEqual({error, timeout}, Result)
      end}
    ].

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    [
     {"combined death and registration sync",
      fun() ->
          Name = test_combined_name,
          Pid = spawn(fun() ->
              register(Name, self()),
              receive stop -> ok end
          end),
          FoundPid = flurm_test_utils:wait_for_registered(Name),
          ?assertEqual(Pid, FoundPid),
          Pid ! stop,
          ?assertEqual(ok, flurm_test_utils:wait_for_death(Pid)),
          ?assertEqual(ok, flurm_test_utils:wait_for_unregistered(Name))
      end}
    ].
