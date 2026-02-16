%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_pmi_app module
%%%
%%% Tests the PMI application behaviour callbacks without mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_app_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Application Behaviour Tests
%%====================================================================

%% Test that the module implements application behaviour
behaviour_test_() ->
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
      end}
    ].

%% Test stop callback
stop_test_() ->
    [
     {"stop/1 returns ok with any state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(undefined))
      end},
     {"stop/1 returns ok with map state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop(#{some => state}))
      end},
     {"stop/1 returns ok with list state",
      fun() ->
          ?assertEqual(ok, flurm_pmi_app:stop([some, state]))
      end}
    ].

%%====================================================================
%% Application Specification Tests
%%====================================================================

%% Test that app file has correct configuration
app_spec_test_() ->
    {setup,
     fun() ->
         %% Ensure the application is loaded (not started)
         application:load(flurm_pmi)
     end,
     fun(_) -> ok end,
     [
      {"application is registered",
       fun() ->
           Apps = application:loaded_applications(),
           AppNames = [App || {App, _, _} <- Apps],
           ?assert(lists:member(flurm_pmi, AppNames))
       end},
      {"application has mod callback",
       fun() ->
           {ok, Mod} = application:get_key(flurm_pmi, mod),
           ?assertEqual({flurm_pmi_app, []}, Mod)
       end}
     ]}.
