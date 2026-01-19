%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_app application callbacks
%%%
%%% Tests the application callback functions directly. The start/2
%%% callback is tested via mocking to avoid actually starting processes.
%%%-------------------------------------------------------------------
-module(flurm_config_app_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% stop/1 tests
%%====================================================================

stop_returns_ok_test_() ->
    [
        {"stop/1 returns ok with any state",
         ?_assertEqual(ok, flurm_config_app:stop(undefined))},
        {"stop/1 returns ok with empty state",
         ?_assertEqual(ok, flurm_config_app:stop([]))},
        {"stop/1 returns ok with map state",
         ?_assertEqual(ok, flurm_config_app:stop(#{}))}
    ].

%%====================================================================
%% Module behavior tests
%%====================================================================

module_behavior_test_() ->
    [
        {"module exports start/2",
         ?_assert(erlang:function_exported(flurm_config_app, start, 2))},
        {"module exports stop/1",
         ?_assert(erlang:function_exported(flurm_config_app, stop, 1))},
        {"module has application behavior",
         fun() ->
             Behaviors = proplists:get_value(behaviour, flurm_config_app:module_info(attributes), []),
             ?assert(lists:member(application, Behaviors))
         end}
    ].
