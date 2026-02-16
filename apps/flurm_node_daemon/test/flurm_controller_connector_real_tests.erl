%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_controller_connector module
%%%
%%% Tests the exported test functions and internal helpers without
%%% mocking external dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Helper Functions (exported via -ifdef(TEST))
%%====================================================================

%% Test decode_messages function
decode_messages_test_() ->
    [
     {"empty buffer returns empty list and buffer",
      fun() ->
          {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, []),
          ?assertEqual([], Messages),
          ?assertEqual(<<>>, Remaining)
      end},
     {"incomplete length prefix stays in buffer",
      fun() ->
          Buffer = <<0, 0, 0>>,  % Less than 4 bytes
          {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
          ?assertEqual([], Messages),
          ?assertEqual(Buffer, Remaining)
      end},
     {"incomplete message stays in buffer",
      fun() ->
          %% Length says 100 bytes, but we only have 10
          Buffer = <<100:32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
          {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
          ?assertEqual([], Messages),
          ?assertEqual(Buffer, Remaining)
      end}
    ].

%% Test find_job_by_pid function
find_job_by_pid_test_() ->
    [
     {"returns error for empty jobs map",
      fun() ->
          Jobs = #{},
          Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
          ?assertEqual(error, Result)
      end},
     {"returns error when pid not found",
      fun() ->
          OtherPid = spawn(fun() -> ok end),
          Jobs = #{1 => OtherPid},
          Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
          ?assertEqual(error, Result)
      end},
     {"returns job_id when pid found",
      fun() ->
          Jobs = #{42 => self()},
          Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
          ?assertEqual({ok, 42}, Result)
      end},
     {"finds correct job among multiple",
      fun() ->
          Pid1 = spawn(fun() -> receive _ -> ok end end),
          Pid2 = spawn(fun() -> receive _ -> ok end end),
          Jobs = #{1 => Pid1, 2 => self(), 3 => Pid2},
          Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
          ?assertEqual({ok, 2}, Result),
          Pid1 ! done,
          Pid2 ! done
      end}
    ].

%% Test cancel_timer function
cancel_timer_test_() ->
    [
     {"cancel_timer with undefined is ok",
      ?_assertEqual(ok, flurm_controller_connector:cancel_timer(undefined))},
     {"cancel_timer with valid ref cancels",
      fun() ->
          Ref = erlang:send_after(60000, self(), test),
          Result = flurm_controller_connector:cancel_timer(Ref),
          ?assertEqual(ok, Result),
          %% Verify timer was cancelled (no message should arrive)
          receive test -> ?assert(false) after 10 -> ok end
      end}
    ].

%% Test detect_features function
detect_features_test_() ->
    [
     {"detect_features returns a list",
      fun() ->
          Features = flurm_controller_connector:detect_features(),
          ?assert(is_list(Features))
      end},
     {"all features are binaries",
      fun() ->
          Features = flurm_controller_connector:detect_features(),
          lists:foreach(fun(F) -> ?assert(is_binary(F)) end, Features)
      end}
    ].

%% Test check_cpu_flag function
check_cpu_flag_test_() ->
    [
     {"check_cpu_flag returns boolean",
      fun() ->
          Result = flurm_controller_connector:check_cpu_flag("avx"),
          ?assert(is_boolean(Result))
      end},
     {"check_cpu_flag with nonexistent flag",
      fun() ->
          Result = flurm_controller_connector:check_cpu_flag("nonexistent_flag_xyz123"),
          ?assertEqual(false, Result)
      end}
    ].

%%====================================================================
%% Record State Tests
%%====================================================================

%% Test the state record behavior by checking gen_server call responses
%% when server is not started (these test error handling)

api_without_server_test_() ->
    {setup,
     fun() ->
         %% Ensure server is not running for these tests
         catch gen_server:stop(flurm_controller_connector),
         ok
     end,
     fun(_) -> ok end,
     [
      {"get_state when server not running",
       fun() ->
           %% This should fail since server is not started
           ?assertException(exit, _, flurm_controller_connector:get_state())
       end},
      {"send_message when server not running",
       fun() ->
           ?assertException(exit, _, flurm_controller_connector:send_message(#{}))
       end}
     ]}.
