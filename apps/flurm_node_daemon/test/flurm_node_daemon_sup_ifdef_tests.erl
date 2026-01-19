%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_node_daemon_sup module
%%% Tests the supervisor init/1 and child specs directly
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup_ifdef_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases for init/1
%%====================================================================

init_test_() ->
    [
     {"init returns one_for_one supervisor spec", fun() ->
         {ok, {SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         %% Check supervisor flags
         ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
         ?assertEqual(5, maps:get(intensity, SupFlags)),
         ?assertEqual(10, maps:get(period, SupFlags)),

         %% Should have 3 children
         ?assertEqual(3, length(Children))
     end},
     {"init includes flurm_system_monitor child", fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         ChildIds = [maps:get(id, Child) || Child <- Children],
         ?assert(lists:member(flurm_system_monitor, ChildIds)),

         %% Find the specific child spec
         [SysMonChild] = [C || C <- Children, maps:get(id, C) =:= flurm_system_monitor],
         ?assertEqual({flurm_system_monitor, start_link, []}, maps:get(start, SysMonChild)),
         ?assertEqual(permanent, maps:get(restart, SysMonChild)),
         ?assertEqual(5000, maps:get(shutdown, SysMonChild)),
         ?assertEqual(worker, maps:get(type, SysMonChild))
     end},
     {"init includes flurm_controller_connector child", fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         ChildIds = [maps:get(id, Child) || Child <- Children],
         ?assert(lists:member(flurm_controller_connector, ChildIds)),

         %% Find the specific child spec
         [ConnChild] = [C || C <- Children, maps:get(id, C) =:= flurm_controller_connector],
         ?assertEqual({flurm_controller_connector, start_link, []}, maps:get(start, ConnChild)),
         ?assertEqual(permanent, maps:get(restart, ConnChild)),
         ?assertEqual(worker, maps:get(type, ConnChild))
     end},
     {"init includes flurm_job_executor_sup child", fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         ChildIds = [maps:get(id, Child) || Child <- Children],
         ?assert(lists:member(flurm_job_executor_sup, ChildIds)),

         %% Find the specific child spec
         [ExecSupChild] = [C || C <- Children, maps:get(id, C) =:= flurm_job_executor_sup],
         ?assertEqual({flurm_job_executor_sup, start_link, []}, maps:get(start, ExecSupChild)),
         ?assertEqual(permanent, maps:get(restart, ExecSupChild)),
         ?assertEqual(infinity, maps:get(shutdown, ExecSupChild)),
         ?assertEqual(supervisor, maps:get(type, ExecSupChild))
     end},
     {"all child specs are valid maps", fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         lists:foreach(fun(ChildSpec) ->
             ?assert(is_map(ChildSpec)),
             ?assert(maps:is_key(id, ChildSpec)),
             ?assert(maps:is_key(start, ChildSpec)),
             ?assert(maps:is_key(restart, ChildSpec)),
             ?assert(maps:is_key(shutdown, ChildSpec)),
             ?assert(maps:is_key(type, ChildSpec)),
             ?assert(maps:is_key(modules, ChildSpec))
         end, Children)
     end},
     {"child order is correct (system_monitor first)", fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         %% First child should be flurm_system_monitor (collects metrics)
         [FirstChild | _] = Children,
         ?assertEqual(flurm_system_monitor, maps:get(id, FirstChild))
     end}
    ].
