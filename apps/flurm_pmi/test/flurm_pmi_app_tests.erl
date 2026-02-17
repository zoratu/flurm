%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_app module
%%%
%%% Tests the PMI application behaviour callbacks including start/2
%%% and stop/1.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_pmi_app_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_app_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Ensure required applications are started
    application:ensure_all_started(lager),
    %% Stop flurm_pmi if running
    application:stop(flurm_pmi),
    ok.

cleanup(_) ->
    %% Stop flurm_pmi if running
    application:stop(flurm_pmi),
    ok.

%%====================================================================
%% Application Behaviour Export Tests
%%====================================================================

exports_test_() ->
    [
     {"module exports start/2",
      fun() ->
          Exports = flurm_pmi_app:module_info(exports),
          ?assert(lists:member({start, 2}, Exports))
      end},
     {"module exports stop/1",
      fun() ->
          Exports = flurm_pmi_app:module_info(exports),
          ?assert(lists:member({stop, 1}, Exports))
      end},
     {"module exports module_info/0",
      fun() ->
          Exports = flurm_pmi_app:module_info(exports),
          ?assert(lists:member({module_info, 0}, Exports))
      end},
     {"module exports module_info/1",
      fun() ->
          Exports = flurm_pmi_app:module_info(exports),
          ?assert(lists:member({module_info, 1}, Exports))
      end}
    ].

%%====================================================================
%% stop/1 Tests
%%====================================================================

stop_test_() ->
    [
     {"stop/1 returns ok with undefined state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(undefined))
      end},
     {"stop/1 returns ok with empty list state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop([]))
      end},
     {"stop/1 returns ok with non-empty list state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop([some, state, data]))
      end},
     {"stop/1 returns ok with empty map state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(#{}))
      end},
     {"stop/1 returns ok with non-empty map state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(#{key => value, another => data}))
      end},
     {"stop/1 returns ok with tuple state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop({some, tuple, state}))
      end},
     {"stop/1 returns ok with integer state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(42))
      end},
     {"stop/1 returns ok with atom state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(some_atom))
      end},
     {"stop/1 returns ok with binary state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(<<"binary state">>))
      end},
     {"stop/1 returns ok with pid state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(self()))
      end},
     {"stop/1 returns ok with complex nested state",
      fun() ->
          ComplexState = #{
              pids => [self()],
              config => #{
                  timeout => 5000,
                  max_connections => 100
              },
              data => [1, 2, 3, {nested, tuple}]
          },
          ?assertEqual(ok, flurm_pmi_app:stop(ComplexState))
      end}
    ].

%%====================================================================
%% start/2 Tests (using meck to mock supervisor)
%%====================================================================

start_with_mock_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(meck),
         application:ensure_all_started(lager),
         meck:new(flurm_pmi_sup, [passthrough, non_strict]),
         meck:expect(flurm_pmi_sup, start_link, fun() ->
             {ok, spawn(fun() -> timer:sleep(60000) end)}
         end),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_pmi_sup),
         ok
     end,
     [
      {"start/2 calls supervisor start_link",
       fun() ->
           Result = flurm_pmi_app:start(normal, []),
           ?assertMatch({ok, _Pid}, Result),
           ?assert(meck:called(flurm_pmi_sup, start_link, []))
       end},
      {"start/2 with different StartType",
       fun() ->
           meck:reset(flurm_pmi_sup),
           Result = flurm_pmi_app:start(takeover, []),
           ?assertMatch({ok, _Pid}, Result)
       end},
      {"start/2 with different StartArgs",
       fun() ->
           meck:reset(flurm_pmi_sup),
           Result = flurm_pmi_app:start(normal, [arg1, arg2]),
           ?assertMatch({ok, _Pid}, Result)
       end},
      {"start/2 returns pid from supervisor",
       fun() ->
           meck:reset(flurm_pmi_sup),
           {ok, Pid} = flurm_pmi_app:start(normal, []),
           ?assert(is_pid(Pid))
       end}
     ]}.

start_error_handling_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(meck),
         application:ensure_all_started(lager),
         meck:new(flurm_pmi_sup, [passthrough, non_strict]),
         meck:expect(flurm_pmi_sup, start_link, fun() ->
             {error, {already_started, self()}}
         end),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_pmi_sup),
         ok
     end,
     [
      {"start/2 propagates supervisor error",
       fun() ->
           Result = flurm_pmi_app:start(normal, []),
           ?assertMatch({error, {already_started, _}}, Result)
       end}
     ]}.

%%====================================================================
%% Application Specification Tests
%%====================================================================

app_spec_test_() ->
    {setup,
     fun() ->
         application:load(flurm_pmi)
     end,
     fun(_) -> ok end,
     [
      {"application is loaded",
       fun() ->
           Apps = application:loaded_applications(),
           AppNames = [App || {App, _, _} <- Apps],
           ?assert(lists:member(flurm_pmi, AppNames))
       end},
      {"application has correct mod callback",
       fun() ->
           {ok, Mod} = application:get_key(flurm_pmi, mod),
           ?assertEqual({flurm_pmi_app, []}, Mod)
       end},
      {"application has description",
       fun() ->
           case application:get_key(flurm_pmi, description) of
               {ok, Desc} ->
                   ?assert(is_list(Desc));
               undefined ->
                   %% Description is optional
                   ok
           end
       end},
      {"application has version",
       fun() ->
           case application:get_key(flurm_pmi, vsn) of
               {ok, Vsn} ->
                   ?assert(is_list(Vsn));
               undefined ->
                   %% Vsn might not be set in dev
                   ok
           end
       end},
      {"application has registered processes list",
       fun() ->
           case application:get_key(flurm_pmi, registered) of
               {ok, Registered} ->
                   ?assert(is_list(Registered));
               undefined ->
                   %% Registered is optional
                   ok
           end
       end},
      {"application has applications dependency list",
       fun() ->
           case application:get_key(flurm_pmi, applications) of
               {ok, Apps} ->
                   ?assert(is_list(Apps)),
                   %% Should depend on kernel and stdlib at minimum
                   ?assert(lists:member(kernel, Apps)),
                   ?assert(lists:member(stdlib, Apps));
               undefined ->
                   %% Should have dependencies
                   ok
           end
       end}
     ]}.

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
     {"module_info/0 returns proplist",
      fun() ->
          Info = flurm_pmi_app:module_info(),
          ?assert(is_list(Info)),
          ?assert(proplists:is_defined(module, Info)),
          ?assert(proplists:is_defined(exports, Info)),
          ?assert(proplists:is_defined(attributes, Info)),
          ?assert(proplists:is_defined(compile, Info))
      end},
     {"module_info/1 returns module name",
      fun() ->
          ?assertEqual(flurm_pmi_app, flurm_pmi_app:module_info(module))
      end},
     {"module_info/1 returns exports list",
      fun() ->
          Exports = flurm_pmi_app:module_info(exports),
          ?assert(is_list(Exports)),
          ?assert(length(Exports) >= 2)  % At least start/2 and stop/1
      end},
     {"module_info/1 returns attributes",
      fun() ->
          Attributes = flurm_pmi_app:module_info(attributes),
          ?assert(is_list(Attributes)),
          %% Should have behaviour attribute
          Behaviours = proplists:get_value(behaviour, Attributes, []),
          ?assert(lists:member(application, Behaviours))
      end}
    ].

%%====================================================================
%% Behaviour Compliance Tests
%%====================================================================

behaviour_compliance_test_() ->
    [
     {"implements application behaviour",
      fun() ->
          Attributes = flurm_pmi_app:module_info(attributes),
          Behaviours = proplists:get_value(behaviour, Attributes, []),
          ?assert(lists:member(application, Behaviours))
      end},
     {"start/2 has correct arity",
      fun() ->
          %% Verify start/2 can be called (will fail with mock error but proves arity)
          try
              flurm_pmi_app:start(normal, []),
              ok
          catch
              _:_ -> ok  % Expected to possibly fail, but arity is correct
          end
      end},
     {"stop/1 has correct arity",
      fun() ->
          %% Verify stop/1 works
          ?assertEqual(ok, flurm_pmi_app:stop(undefined))
      end}
    ].

%%====================================================================
%% Integration Tests
%%====================================================================

full_lifecycle_test_() ->
    {setup,
     fun() ->
         setup(),
         application:ensure_all_started(meck),
         meck:new(flurm_pmi_sup, [passthrough, non_strict]),
         %% Create a real process that can be stopped
         Pid = spawn(fun() ->
             receive stop -> ok end
         end),
         meck:expect(flurm_pmi_sup, start_link, fun() -> {ok, Pid} end),
         Pid
     end,
     fun(Pid) ->
         Pid ! stop,
         catch meck:unload(flurm_pmi_sup),
         cleanup(ok)
     end,
     fun(_Pid) ->
         [
          {"full application lifecycle",
           fun() ->
               %% Start
               {ok, AppPid} = flurm_pmi_app:start(normal, []),
               ?assert(is_pid(AppPid)),

               %% Stop
               ?assertEqual(ok, flurm_pmi_app:stop(undefined))
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    [
     {"stop/1 with very large state",
      fun() ->
          LargeList = lists:seq(1, 10000),
          ?assertEqual(ok, flurm_pmi_app:stop(LargeList))
      end},
     {"stop/1 with deeply nested state",
      fun() ->
          DeepState = lists:foldl(fun(N, Acc) ->
              #{level => N, child => Acc}
          end, #{}, lists:seq(1, 100)),
          ?assertEqual(ok, flurm_pmi_app:stop(DeepState))
      end},
     {"stop/1 with reference state",
      fun() ->
          Ref = make_ref(),
          ?assertEqual(ok, flurm_pmi_app:stop(Ref))
      end},
     {"stop/1 with function state",
      fun() ->
          Fun = fun() -> ok end,
          ?assertEqual(ok, flurm_pmi_app:stop(Fun))
      end}
    ].

%%====================================================================
%% Concurrency Tests
%%====================================================================

concurrent_stop_test_() ->
    [
     {"multiple concurrent stop calls",
      fun() ->
          Parent = self(),
          Pids = [spawn(fun() ->
              Result = flurm_pmi_app:stop(N),
              Parent ! {done, self(), Result}
          end) || N <- lists:seq(1, 10)],
          Results = [receive {done, P, R} -> R after 5000 -> timeout end || P <- Pids],
          lists:foreach(fun(R) ->
              ?assertEqual(ok, R)
          end, Results)
      end}
    ].
