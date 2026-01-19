%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_controller_sup
%%%
%%% Tests the supervisor init/1 callback and pure utility functions
%%% without starting actual processes or mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Store original values to restore later
    Keys = [listen_port, listen_address, num_acceptors, max_connections,
            node_listen_port, max_node_connections, enable_cluster],
    OriginalValues = [{K, application:get_env(flurm_controller, K)} || K <- Keys],
    OriginalValues.

cleanup(OriginalValues) ->
    lists:foreach(
        fun({Key, undefined}) ->
                application:unset_env(flurm_controller, Key);
           ({Key, {ok, Value}}) ->
                application:set_env(flurm_controller, Key, Value)
        end, OriginalValues).

%%====================================================================
%% Test Generators
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init returns valid supervisor spec",
       fun init_returns_valid_spec/0},
      {"init returns correct supervisor flags",
       fun init_returns_correct_flags/0},
      {"init includes base children",
       fun init_includes_base_children/0},
      {"init without cluster mode excludes cluster children",
       fun init_without_cluster_excludes_cluster_children/0}
     ]}.

%%====================================================================
%% Init Tests
%%====================================================================

init_returns_valid_spec() ->
    %% Disable cluster mode for predictable testing
    application:set_env(flurm_controller, enable_cluster, false),

    {ok, {SupFlags, Children}} = flurm_controller_sup:init([]),

    ?assert(is_map(SupFlags)),
    ?assert(is_list(Children)),
    ?assert(length(Children) > 0).

init_returns_correct_flags() ->
    application:set_env(flurm_controller, enable_cluster, false),

    {ok, {SupFlags, _Children}} = flurm_controller_sup:init([]),

    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)).

init_includes_base_children() ->
    application:set_env(flurm_controller, enable_cluster, false),

    {ok, {_SupFlags, Children}} = flurm_controller_sup:init([]),

    %% Get the IDs of all children
    ChildIds = [maps:get(id, Child) || Child <- Children],

    %% Verify base children are present
    ?assert(lists:member(flurm_metrics, ChildIds)),
    ?assert(lists:member(flurm_metrics_http, ChildIds)),
    ?assert(lists:member(flurm_job_manager, ChildIds)),
    ?assert(lists:member(flurm_step_manager, ChildIds)),
    ?assert(lists:member(flurm_node_manager_server, ChildIds)),
    ?assert(lists:member(flurm_node_connection_manager, ChildIds)),
    ?assert(lists:member(flurm_job_dispatcher_server, ChildIds)),
    ?assert(lists:member(flurm_scheduler, ChildIds)),
    ?assert(lists:member(flurm_partition_manager, ChildIds)),
    ?assert(lists:member(flurm_account_manager, ChildIds)).

init_without_cluster_excludes_cluster_children() ->
    application:set_env(flurm_controller, enable_cluster, false),

    {ok, {_SupFlags, Children}} = flurm_controller_sup:init([]),

    ChildIds = [maps:get(id, Child) || Child <- Children],

    %% Cluster children should NOT be present when cluster is disabled
    ?assertNot(lists:member(flurm_controller_cluster, ChildIds)),
    ?assertNot(lists:member(flurm_controller_failover, ChildIds)).

%%====================================================================
%% Child Spec Validation Tests
%%====================================================================

child_spec_validation_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, enable_cluster, false),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster)
     end,
     [
      {"all child specs have required fields",
       fun all_child_specs_have_required_fields/0},
      {"all child specs have valid restart strategy",
       fun all_child_specs_have_valid_restart/0},
      {"all child specs have valid type",
       fun all_child_specs_have_valid_type/0}
     ]}.

all_child_specs_have_required_fields() ->
    {ok, {_SupFlags, Children}} = flurm_controller_sup:init([]),

    RequiredKeys = [id, start, restart, shutdown, type, modules],
    lists:foreach(
        fun(Child) ->
            lists:foreach(
                fun(Key) ->
                    ?assert(maps:is_key(Key, Child),
                           io_lib:format("Child ~p missing key ~p",
                                        [maps:get(id, Child), Key]))
                end, RequiredKeys)
        end, Children).

all_child_specs_have_valid_restart() ->
    {ok, {_SupFlags, Children}} = flurm_controller_sup:init([]),

    ValidRestarts = [permanent, temporary, transient],
    lists:foreach(
        fun(Child) ->
            Restart = maps:get(restart, Child),
            ?assert(lists:member(Restart, ValidRestarts),
                   io_lib:format("Child ~p has invalid restart: ~p",
                                [maps:get(id, Child), Restart]))
        end, Children).

all_child_specs_have_valid_type() ->
    {ok, {_SupFlags, Children}} = flurm_controller_sup:init([]),

    ValidTypes = [worker, supervisor],
    lists:foreach(
        fun(Child) ->
            Type = maps:get(type, Child),
            ?assert(lists:member(Type, ValidTypes),
                   io_lib:format("Child ~p has invalid type: ~p",
                                [maps:get(id, Child), Type]))
        end, Children).

%%====================================================================
%% Listener Info Tests
%%====================================================================

listener_info_test_() ->
    [
     {"listener_info returns error when not started",
      fun listener_info_when_not_started/0},
     {"node_listener_info returns error when not started",
      fun node_listener_info_when_not_started/0}
    ].

listener_info_when_not_started() ->
    %% Without Ranch listener running, should return error
    Result = flurm_controller_sup:listener_info(),
    ?assertEqual({error, not_found}, Result).

node_listener_info_when_not_started() ->
    %% Without node listener running, should return error
    Result = flurm_controller_sup:node_listener_info(),
    ?assertEqual({error, not_found}, Result).
