%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_executor_sup module
%%% Tests the supervisor init/1 and child spec directly
%%%-------------------------------------------------------------------
-module(flurm_job_executor_sup_ifdef_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases for init/1
%%====================================================================

init_test_() ->
    [
     {"init returns simple_one_for_one supervisor spec", fun() ->
         {ok, {SupFlags, Children}} = flurm_job_executor_sup:init([]),

         %% Check supervisor flags for simple_one_for_one
         ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
         ?assertEqual(10, maps:get(intensity, SupFlags)),
         ?assertEqual(60, maps:get(period, SupFlags)),

         %% Should have exactly one child spec (template)
         ?assertEqual(1, length(Children)),

         %% Check the child spec
         [ChildSpec] = Children,
         ?assertEqual(flurm_job_executor, maps:get(id, ChildSpec)),
         ?assertEqual({flurm_job_executor, start_link, []}, maps:get(start, ChildSpec)),
         ?assertEqual(temporary, maps:get(restart, ChildSpec)),
         ?assertEqual(30000, maps:get(shutdown, ChildSpec)),
         ?assertEqual(worker, maps:get(type, ChildSpec)),
         ?assertEqual([flurm_job_executor], maps:get(modules, ChildSpec))
     end},
     {"init returns valid child spec for dynamic spawning", fun() ->
         {ok, {_SupFlags, Children}} = flurm_job_executor_sup:init([]),
         [ChildSpec] = Children,

         %% Verify the child spec is well-formed for simple_one_for_one
         ?assert(is_map(ChildSpec)),
         ?assert(maps:is_key(id, ChildSpec)),
         ?assert(maps:is_key(start, ChildSpec)),
         ?assert(maps:is_key(restart, ChildSpec)),
         ?assert(maps:is_key(shutdown, ChildSpec)),
         ?assert(maps:is_key(type, ChildSpec)),
         ?assert(maps:is_key(modules, ChildSpec))
     end},
     {"init child spec has proper MFA tuple", fun() ->
         {ok, {_SupFlags, [ChildSpec]}} = flurm_job_executor_sup:init([]),
         {Module, Function, Args} = maps:get(start, ChildSpec),
         ?assertEqual(flurm_job_executor, Module),
         ?assertEqual(start_link, Function),
         ?assertEqual([], Args)
     end}
    ].

%%====================================================================
%% API Function Tests (without starting supervisor)
%%====================================================================

%% Note: start_job/1 and stop_job/1 require the supervisor to be running.
%% These are tested in other test modules that set up the full environment.
%% Here we focus on testing init/1 directly for coverage.
