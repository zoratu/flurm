%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_diagnostics_sup
%%%
%%% Covers supervisor configuration and child spec generation.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_diagnostics_sup_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

diagnostics_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init with defaults: one_for_one strategy",
         fun test_init_defaults/0},
        {"init with defaults: one child",
         fun test_init_defaults_one_child/0},
        {"init with custom interval passes to child",
         fun test_init_custom_interval/0},
        {"init with leak detector disabled: no children",
         fun test_init_disabled/0},
        {"child spec has correct restart",
         fun test_child_spec_restart/0},
        {"child spec has correct shutdown",
         fun test_child_spec_shutdown/0},
        {"child spec has correct type and modules",
         fun test_child_spec_type_modules/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_init_defaults() ->
    {ok, {Strategy, _Children}} = flurm_diagnostics_sup:init(#{}),
    ?assertEqual(one_for_one, maps:get(strategy, Strategy)),
    ?assertEqual(5, maps:get(intensity, Strategy)),
    ?assertEqual(60, maps:get(period, Strategy)).

test_init_defaults_one_child() ->
    {ok, {_Strategy, Children}} = flurm_diagnostics_sup:init(#{}),
    ?assertEqual(1, length(Children)).

test_init_custom_interval() ->
    {ok, {_Strategy, Children}} = flurm_diagnostics_sup:init(#{interval => 30000}),
    ?assertEqual(1, length(Children)),
    [Child] = Children,
    {flurm_diagnostics, start_link, [Args]} = maps:get(start, Child),
    ?assertEqual([30000], Args).

test_init_disabled() ->
    {ok, {_Strategy, Children}} = flurm_diagnostics_sup:init(#{enable_leak_detector => false}),
    ?assertEqual(0, length(Children)).

test_child_spec_restart() ->
    {ok, {_Strategy, [Child]}} = flurm_diagnostics_sup:init(#{}),
    ?assertEqual(permanent, maps:get(restart, Child)).

test_child_spec_shutdown() ->
    {ok, {_Strategy, [Child]}} = flurm_diagnostics_sup:init(#{}),
    ?assertEqual(5000, maps:get(shutdown, Child)).

test_child_spec_type_modules() ->
    {ok, {_Strategy, [Child]}} = flurm_diagnostics_sup:init(#{}),
    ?assertEqual(worker, maps:get(type, Child)),
    ?assertEqual([flurm_diagnostics], maps:get(modules, Child)),
    ?assertEqual(flurm_leak_detector, maps:get(id, Child)).
