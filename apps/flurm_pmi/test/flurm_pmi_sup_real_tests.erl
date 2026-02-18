%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_pmi_sup module
%%%
%%% Tests the PMI supervisor configuration and child specs.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_sup_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Supervisor Configuration Tests
%%====================================================================

%% Test init function returns correct supervisor flags
init_test_() ->
    [
     {"init returns ok tuple with correct structure",
      fun() ->
          Result = flurm_pmi_sup:init([]),
          ?assertMatch({ok, {_, _}}, Result)
      end},
     {"init uses one_for_one strategy",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          ?assertEqual(one_for_one, maps:get(strategy, SupFlags))
      end},
     {"init sets reasonable intensity and period",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          Intensity = maps:get(intensity, SupFlags),
          Period = maps:get(period, SupFlags),
          ?assert(Intensity > 0),
          ?assert(Period > 0)
      end},
     {"init has child spec for flurm_pmi_manager",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          ?assert(length(Children) >= 1),
          ChildIds = [maps:get(id, C) || C <- Children],
          ?assert(lists:member(flurm_pmi_manager, ChildIds))
      end}
    ].

%% Test child spec structure
child_spec_test_() ->
    [
     {"flurm_pmi_manager child spec is valid",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          %% Check required fields
          ?assert(maps:is_key(start, ManagerSpec)),
          ?assert(maps:is_key(restart, ManagerSpec)),
          ?assert(maps:is_key(shutdown, ManagerSpec)),
          ?assert(maps:is_key(type, ManagerSpec))
      end},
     {"child spec has correct start function",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          {Module, Function, Args} = maps:get(start, ManagerSpec),
          ?assertEqual(flurm_pmi_manager, Module),
          ?assertEqual(start_link, Function),
          ?assertEqual([], Args)
      end},
     {"child spec uses permanent restart",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assertEqual(permanent, maps:get(restart, ManagerSpec))
      end},
     {"child spec is worker type",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assertEqual(worker, maps:get(type, ManagerSpec))
      end}
    ].

%%====================================================================
%% API Function Tests
%%====================================================================

%% Test that API functions exist and have correct arity
api_exports_test_() ->
    [
     {"start_link/0 is exported",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"init/1 is exported",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
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
         %% Ensure no meck mocks are active
         catch meck:unload(flurm_pmi_sup),
         catch meck:unload(flurm_pmi_manager),
         ok
     end,
     fun(_) -> ok end,
     [
      {"start_link creates supervisor process",
       fun() ->
           %% Unregister if already running
           case whereis(flurm_pmi_sup) of
               undefined -> ok;
               OldPid ->
                   gen_server:stop(OldPid),
                   timer:sleep(10)
           end,
           Result = flurm_pmi_sup:start_link(),
           NewPid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           ?assert(is_pid(NewPid)),
           ?assert(is_process_alive(NewPid)),
           %% Cleanup
           gen_server:stop(NewPid)
       end},
      {"supervisor is registered locally",
       fun() ->
           case whereis(flurm_pmi_sup) of
               undefined -> ok;
               OldPid2 ->
                   gen_server:stop(OldPid2),
                   timer:sleep(10)
           end,
           Result = flurm_pmi_sup:start_link(),
           NewPid2 = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           ?assertEqual(NewPid2, whereis(flurm_pmi_sup)),
           gen_server:stop(NewPid2)
       end}
     ]}.
