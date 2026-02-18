%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_sup module
%%%
%%% Tests the PMI supervisor configuration, child specs, and lifecycle.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_pmi_sup_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Ensure required applications are started
    application:ensure_all_started(lager),
    %% Ensure no meck mocks are active for modules we need real
    catch meck:unload(flurm_pmi_sup),
    catch meck:unload(flurm_pmi_manager),
    %% Stop existing supervisor if running
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        Pid ->
            try gen_server:stop(Pid, normal, 1000) catch _:_ -> ok end,
            timer:sleep(50)
    end,
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        Pid ->
            try gen_server:stop(Pid, normal, 1000) catch _:_ -> ok end
    end,
    ok.

%%====================================================================
%% Supervisor Exports Tests
%%====================================================================

exports_test_() ->
    [
     {"module exports start_link/0",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"module exports init/1",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(lists:member({init, 1}, Exports))
      end},
     {"module exports module_info/0",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(lists:member({module_info, 0}, Exports))
      end},
     {"module exports module_info/1",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(lists:member({module_info, 1}, Exports))
      end}
    ].

%%====================================================================
%% init/1 Tests - Supervisor Flags
%%====================================================================

init_sup_flags_test_() ->
    [
     {"init/1 returns ok tuple with correct structure",
      fun() ->
          Result = flurm_pmi_sup:init([]),
          ?assertMatch({ok, {_, _}}, Result)
      end},
     {"init/1 returns map-based supervisor flags",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          ?assert(is_map(SupFlags))
      end},
     {"init/1 uses one_for_one strategy",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          ?assertEqual(one_for_one, maps:get(strategy, SupFlags))
      end},
     {"init/1 sets intensity to 10",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          ?assertEqual(10, maps:get(intensity, SupFlags))
      end},
     {"init/1 sets period to 60",
      fun() ->
          {ok, {SupFlags, _Children}} = flurm_pmi_sup:init([]),
          ?assertEqual(60, maps:get(period, SupFlags))
      end},
     {"init/1 supervisor flags are valid",
      fun() ->
          {ok, {SupFlags, _}} = flurm_pmi_sup:init([]),
          Strategy = maps:get(strategy, SupFlags),
          Intensity = maps:get(intensity, SupFlags),
          Period = maps:get(period, SupFlags),
          %% Validate strategy
          ?assert(lists:member(Strategy, [one_for_one, one_for_all, rest_for_one, simple_one_for_one])),
          %% Validate intensity
          ?assert(Intensity >= 0),
          %% Validate period
          ?assert(Period > 0)
      end}
    ].

%%====================================================================
%% init/1 Tests - Children
%%====================================================================

init_children_test_() ->
    [
     {"init/1 returns children list",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          ?assert(is_list(Children))
      end},
     {"init/1 has at least one child",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          ?assert(length(Children) >= 1)
      end},
     {"init/1 includes flurm_pmi_manager child",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          ChildIds = [maps:get(id, C) || C <- Children],
          ?assert(lists:member(flurm_pmi_manager, ChildIds))
      end}
    ].

%%====================================================================
%% Child Spec Tests
%%====================================================================

child_spec_structure_test_() ->
    [
     {"flurm_pmi_manager child spec is a map",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(is_map(ManagerSpec))
      end},
     {"child spec has id field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(id, ManagerSpec)),
          ?assertEqual(flurm_pmi_manager, maps:get(id, ManagerSpec))
      end},
     {"child spec has start field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(start, ManagerSpec))
      end},
     {"child spec has restart field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(restart, ManagerSpec))
      end},
     {"child spec has shutdown field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(shutdown, ManagerSpec))
      end},
     {"child spec has type field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(type, ManagerSpec))
      end},
     {"child spec has modules field",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assert(maps:is_key(modules, ManagerSpec))
      end}
    ].

child_spec_values_test_() ->
    [
     {"child spec start is {Module, Function, Args} tuple",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          Start = maps:get(start, ManagerSpec),
          ?assertMatch({_, _, _}, Start)
      end},
     {"child spec start module is flurm_pmi_manager",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          {Module, _, _} = maps:get(start, ManagerSpec),
          ?assertEqual(flurm_pmi_manager, Module)
      end},
     {"child spec start function is start_link",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          {_, Function, _} = maps:get(start, ManagerSpec),
          ?assertEqual(start_link, Function)
      end},
     {"child spec start args is empty list",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          {_, _, Args} = maps:get(start, ManagerSpec),
          ?assertEqual([], Args)
      end},
     {"child spec restart is permanent",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assertEqual(permanent, maps:get(restart, ManagerSpec))
      end},
     {"child spec shutdown is 5000",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assertEqual(5000, maps:get(shutdown, ManagerSpec))
      end},
     {"child spec type is worker",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          ?assertEqual(worker, maps:get(type, ManagerSpec))
      end},
     {"child spec modules contains flurm_pmi_manager",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          [ManagerSpec | _] = [C || C <- Children, maps:get(id, C) =:= flurm_pmi_manager],
          Modules = maps:get(modules, ManagerSpec),
          ?assert(is_list(Modules)),
          ?assert(lists:member(flurm_pmi_manager, Modules))
      end}
    ].

%%====================================================================
%% start_link Tests
%%====================================================================

start_link_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link/0 starts supervisor process",
       fun() ->
           Result = flurm_pmi_sup:start_link(),
           Pid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           ?assert(is_pid(Pid)),
           ?assert(is_process_alive(Pid)),
           gen_server:stop(Pid)
       end},
      {"start_link/0 registers supervisor locally",
       fun() ->
           Result = flurm_pmi_sup:start_link(),
           Pid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           ?assertEqual(Pid, whereis(flurm_pmi_sup)),
           gen_server:stop(Pid)
       end},
      {"start_link/0 returns already_started when running",
       fun() ->
           Result1 = flurm_pmi_sup:start_link(),
           Pid1 = case Result1 of {ok, P} -> P; {error, {already_started, P}} -> P end,
           Result2 = flurm_pmi_sup:start_link(),
           ?assertMatch({error, {already_started, _}}, Result2),
           {error, {already_started, Pid2}} = Result2,
           ?assertEqual(Pid1, Pid2),
           gen_server:stop(Pid1)
       end}
     ]}.

%%====================================================================
%% Supervisor Integration Tests
%%====================================================================

supervisor_integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"supervisor starts child processes",
       fun() ->
           Result = flurm_pmi_sup:start_link(),
           SupPid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           timer:sleep(100),  % Allow children to start
           %% Check that flurm_pmi_manager was started
           ?assertNotEqual(undefined, whereis(flurm_pmi_manager)),
           gen_server:stop(SupPid)
       end},
      {"supervisor child is supervised",
       fun() ->
           Result = flurm_pmi_sup:start_link(),
           SupPid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           timer:sleep(100),
           %% Get child info
           Children = supervisor:which_children(SupPid),
           ?assert(length(Children) >= 1),
           %% Find flurm_pmi_manager
           ManagerChild = [C || {Id, _, _, _} = C <- Children, Id =:= flurm_pmi_manager],
           ?assertEqual(1, length(ManagerChild)),
           gen_server:stop(SupPid)
       end},
      {"supervisor returns child count",
       fun() ->
           Result = flurm_pmi_sup:start_link(),
           SupPid = case Result of {ok, P} -> P; {error, {already_started, P}} -> P end,
           timer:sleep(100),
           CountInfo = supervisor:count_children(SupPid),
           ?assert(is_list(CountInfo)),
           Specs = proplists:get_value(specs, CountInfo),
           Active = proplists:get_value(active, CountInfo),
           ?assert(Specs >= 1),
           ?assert(Active >= 1),
           gen_server:stop(SupPid)
       end}
     ]}.

%%====================================================================
%% Behaviour Compliance Tests
%%====================================================================

behaviour_compliance_test_() ->
    [
     {"implements supervisor behaviour",
      fun() ->
          Attributes = flurm_pmi_sup:module_info(attributes),
          Behaviours = proplists:get_value(behaviour, Attributes, []),
          ?assert(lists:member(supervisor, Behaviours))
      end},
     {"init/1 returns valid supervisor spec",
      fun() ->
          Result = flurm_pmi_sup:init([]),
          %% Can be either tuple-based or map-based supervisor flags
          case Result of
              {ok, {{_, _, _}, _}} -> ok;
              {ok, {#{}, _}} -> ok;
              {ok, {Flags, _}} when is_map(Flags) -> ok
          end
      end}
    ].

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
     {"module_info/0 returns proplist",
      fun() ->
          Info = flurm_pmi_sup:module_info(),
          ?assert(is_list(Info)),
          ?assert(proplists:is_defined(module, Info)),
          ?assert(proplists:is_defined(exports, Info))
      end},
     {"module_info/1 returns module name",
      fun() ->
          ?assertEqual(flurm_pmi_sup, flurm_pmi_sup:module_info(module))
      end},
     {"module_info/1 returns exports",
      fun() ->
          Exports = flurm_pmi_sup:module_info(exports),
          ?assert(is_list(Exports)),
          ?assert(length(Exports) >= 2)  % At least start_link/0 and init/1
      end}
    ].

%%====================================================================
%% Restart Strategy Tests
%%====================================================================

restart_strategy_test_() ->
    [
     {"one_for_one strategy allows individual restarts",
      fun() ->
          {ok, {SupFlags, _}} = flurm_pmi_sup:init([]),
          Strategy = maps:get(strategy, SupFlags),
          %% one_for_one means only the failed child restarts
          ?assertEqual(one_for_one, Strategy)
      end},
     {"intensity allows reasonable restart frequency",
      fun() ->
          {ok, {SupFlags, _}} = flurm_pmi_sup:init([]),
          Intensity = maps:get(intensity, SupFlags),
          Period = maps:get(period, SupFlags),
          %% 10 restarts in 60 seconds is reasonable
          ?assertEqual(10, Intensity),
          ?assertEqual(60, Period)
      end}
    ].

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    [
     {"init/1 works with empty list",
      fun() ->
          %% init/1 accepts empty list (standard supervisor interface)
          Result = flurm_pmi_sup:init([]),
          ?assertMatch({ok, {_, _}}, Result)
      end},
     {"child spec restart values are valid",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          lists:foreach(fun(Child) ->
              Restart = maps:get(restart, Child),
              ?assert(lists:member(Restart, [permanent, temporary, transient]))
          end, Children)
      end},
     {"child spec type values are valid",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          lists:foreach(fun(Child) ->
              Type = maps:get(type, Child),
              ?assert(lists:member(Type, [worker, supervisor]))
          end, Children)
      end},
     {"child spec shutdown values are valid",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          lists:foreach(fun(Child) ->
              Shutdown = maps:get(shutdown, Child),
              %% Shutdown should be brutal_kill, infinity, or positive integer
              ValidShutdown = (Shutdown =:= brutal_kill) orelse
                              (Shutdown =:= infinity) orelse
                              (is_integer(Shutdown) andalso Shutdown > 0),
              ?assert(ValidShutdown)
          end, Children)
      end}
    ].

%%====================================================================
%% Concurrency Tests
%%====================================================================

concurrent_init_test_() ->
    [
     {"multiple concurrent init calls",
      fun() ->
          Parent = self(),
          Pids = [spawn(fun() ->
              Result = flurm_pmi_sup:init([]),
              Parent ! {done, self(), Result}
          end) || _ <- lists:seq(1, 10)],
          Results = [receive {done, P, R} -> R after 5000 -> timeout end || P <- Pids],
          lists:foreach(fun(R) ->
              ?assertMatch({ok, {_, _}}, R)
          end, Results)
      end}
    ].

%%====================================================================
%% Supervisor Configuration Validation
%%====================================================================

config_validation_test_() ->
    [
     {"supervisor spec can be used to start supervisor",
      fun() ->
          {ok, {SupFlags, Children}} = flurm_pmi_sup:init([]),
          %% Validate the spec would work with supervisor:start_link
          ?assert(is_map(SupFlags) orelse is_tuple(SupFlags)),
          ?assert(is_list(Children)),
          lists:foreach(fun(Child) ->
              ?assert(is_map(Child))
          end, Children)
      end},
     {"all children have unique IDs",
      fun() ->
          {ok, {_SupFlags, Children}} = flurm_pmi_sup:init([]),
          ChildIds = [maps:get(id, C) || C <- Children],
          UniqueIds = lists:usort(ChildIds),
          ?assertEqual(length(ChildIds), length(UniqueIds))
      end}
    ].
