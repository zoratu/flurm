%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_job_executor_sup module
%%%
%%% Tests the supervisor configuration and child spec generation
%%% without needing to start actual processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_sup_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Supervisor Configuration Tests
%%====================================================================

%% Test init function returns correct supervisor flags
init_test_() ->
    [
     {"init returns ok tuple with correct structure",
      fun() ->
          Result = flurm_job_executor_sup:init([]),
          ?assertMatch({ok, {_, _}}, Result)
      end},
     {"init uses simple_one_for_one strategy",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_job_executor_sup:init([]),
          ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags))
      end},
     {"init sets reasonable intensity and period",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_job_executor_sup:init([]),
          Intensity = maps:get(intensity, SupFlags),
          Period = maps:get(period, SupFlags),
          ?assert(Intensity > 0),
          ?assert(Period > 0)
      end},
     {"init has single child spec for flurm_job_executor",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_job_executor_sup:init([]),
          ?assertEqual(1, length(Children)),
          [ChildSpec] = Children,
          ?assertEqual(flurm_job_executor, maps:get(id, ChildSpec))
      end},
     {"child spec has correct start function",
      fun() ->
          {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
          {Module, Function, Args} = maps:get(start, ChildSpec),
          ?assertEqual(flurm_job_executor, Module),
          ?assertEqual(start_link, Function),
          ?assertEqual([], Args)
      end},
     {"child spec uses temporary restart",
      fun() ->
          {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
          ?assertEqual(temporary, maps:get(restart, ChildSpec))
      end},
     {"child spec is worker type",
      fun() ->
          {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
          ?assertEqual(worker, maps:get(type, ChildSpec))
      end},
     {"child spec has modules list",
      fun() ->
          {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
          Modules = maps:get(modules, ChildSpec),
          ?assertEqual([flurm_job_executor], Modules)
      end},
     {"child spec has reasonable shutdown timeout",
      fun() ->
          {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
          Shutdown = maps:get(shutdown, ChildSpec),
          %% Shutdown should be > 0 for graceful termination
          ?assert(is_integer(Shutdown) andalso Shutdown > 0)
      end}
    ].

%%====================================================================
%% API Function Tests (without starting supervisor)
%%====================================================================

%% Test that API functions exist and have correct arity
api_exports_test_() ->
    [
     {"start_link/0 is exported",
      fun() ->
          Exports = flurm_job_executor_sup:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"start_job/1 is exported",
      fun() ->
          Exports = flurm_job_executor_sup:module_info(exports),
          ?assert(lists:member({start_job, 1}, Exports))
      end},
     {"stop_job/1 is exported",
      fun() ->
          Exports = flurm_job_executor_sup:module_info(exports),
          ?assert(lists:member({stop_job, 1}, Exports))
      end},
     {"init/1 is exported",
      fun() ->
          Exports = flurm_job_executor_sup:module_info(exports),
          ?assert(lists:member({init, 1}, Exports))
      end}
    ].

%%====================================================================
%% Integration Tests with Real Supervisor
%%====================================================================

supervisor_lifecycle_test_() ->
    {setup,
     fun() ->
         %% Start required applications
         application:start(lager),
         ok
     end,
     fun(_) -> ok end,
     [
      {"start_link creates supervisor process",
       fun() ->
           %% Unregister if already running
           case whereis(flurm_job_executor_sup) of
               undefined -> ok;
               OldPid ->
                   gen_server:stop(OldPid),
                   timer:sleep(10)
           end,
           {ok, NewPid} = flurm_job_executor_sup:start_link(),
           ?assert(is_pid(NewPid)),
           ?assert(is_process_alive(NewPid)),
           %% Cleanup
           gen_server:stop(NewPid)
       end},
      {"supervisor is registered locally",
       fun() ->
           case whereis(flurm_job_executor_sup) of
               undefined -> ok;
               OldPid2 ->
                   gen_server:stop(OldPid2),
                   timer:sleep(10)
           end,
           {ok, NewPid2} = flurm_job_executor_sup:start_link(),
           ?assertEqual(NewPid2, whereis(flurm_job_executor_sup)),
           gen_server:stop(NewPid2)
       end}
     ]}.
