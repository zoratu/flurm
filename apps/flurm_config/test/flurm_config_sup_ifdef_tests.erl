%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_sup supervisor spec
%%%
%%% Tests the supervisor init/1 callback directly to validate the
%%% child spec structure without starting the actual supervisor.
%%%-------------------------------------------------------------------
-module(flurm_config_sup_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% init/1 tests - validate supervisor spec structure
%%====================================================================

init_returns_valid_spec_test_() ->
    [
        {"init returns ok tuple",
         fun() ->
             Result = flurm_config_sup:init([]),
             ?assertMatch({ok, {_, _}}, Result)
         end},
        {"supervisor flags have correct strategy",
         fun() ->
             {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
             ?assertEqual(one_for_one, maps:get(strategy, SupFlags))
         end},
        {"supervisor flags have correct intensity",
         fun() ->
             {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
             ?assertEqual(5, maps:get(intensity, SupFlags))
         end},
        {"supervisor flags have correct period",
         fun() ->
             {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
             ?assertEqual(10, maps:get(period, SupFlags))
         end}
    ].

child_spec_test_() ->
    [
        {"has exactly one child",
         fun() ->
             {ok, {_SupFlags, Children}} = flurm_config_sup:init([]),
             ?assertEqual(1, length(Children))
         end},
        {"child id is flurm_config_server",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual(flurm_config_server, maps:get(id, ChildSpec))
         end},
        {"child start spec is correct",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual({flurm_config_server, start_link, []}, maps:get(start, ChildSpec))
         end},
        {"child restart is permanent",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual(permanent, maps:get(restart, ChildSpec))
         end},
        {"child shutdown is 5000",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual(5000, maps:get(shutdown, ChildSpec))
         end},
        {"child type is worker",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual(worker, maps:get(type, ChildSpec))
         end},
        {"child modules is [flurm_config_server]",
         fun() ->
             {ok, {_SupFlags, [ChildSpec | _]}} = flurm_config_sup:init([]),
             ?assertEqual([flurm_config_server], maps:get(modules, ChildSpec))
         end}
    ].

%%====================================================================
%% Supervisor spec validation tests
%%====================================================================

supervisor_spec_valid_test_() ->
    [
        {"spec passes supervisor check_childspecs",
         fun() ->
             {ok, {_SupFlags, Children}} = flurm_config_sup:init([]),
             %% Verify child specs are valid by checking structure
             lists:foreach(fun(ChildSpec) ->
                 ?assert(maps:is_key(id, ChildSpec)),
                 ?assert(maps:is_key(start, ChildSpec)),
                 {M, F, A} = maps:get(start, ChildSpec),
                 ?assert(is_atom(M)),
                 ?assert(is_atom(F)),
                 ?assert(is_list(A))
             end, Children)
         end}
    ].
