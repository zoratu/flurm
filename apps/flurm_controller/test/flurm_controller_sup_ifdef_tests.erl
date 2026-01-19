%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_sup module
%%%
%%% Tests the top-level supervisor for controller processes.
%%% Validates init/1 returns valid specs, tests exported helper
%%% functions (available via -ifdef(TEST)), and validates child
%%% spec structure.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_valid_spec_test() ->
    %% Disable cluster mode to get predictable child specs
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {SupFlags, ChildSpecs}} = flurm_controller_sup:init([]),
        %% For one_for_one, should have multiple children
        ?assertMatch(#{strategy := one_for_one}, SupFlags),
        ?assert(length(ChildSpecs) > 0)
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

init_sup_flags_test() ->
    {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
    %% Verify all expected supervisor flags
    ?assertMatch(#{strategy := one_for_one,
                   intensity := 5,
                   period := 10}, SupFlags).

init_has_required_children_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        ChildIds = [maps:get(id, Spec) || Spec <- ChildSpecs],
        %% Verify essential children are present
        ?assert(lists:member(flurm_metrics, ChildIds)),
        ?assert(lists:member(flurm_metrics_http, ChildIds)),
        ?assert(lists:member(flurm_job_manager, ChildIds)),
        ?assert(lists:member(flurm_step_manager, ChildIds)),
        ?assert(lists:member(flurm_node_manager_server, ChildIds)),
        ?assert(lists:member(flurm_scheduler, ChildIds)),
        ?assert(lists:member(flurm_partition_manager, ChildIds)),
        ?assert(lists:member(flurm_account_manager, ChildIds))
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

%%====================================================================
%% Cluster Mode Tests
%%====================================================================

cluster_disabled_no_cluster_children_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        ChildIds = [maps:get(id, Spec) || Spec <- ChildSpecs],
        %% Cluster children should NOT be present
        ?assertNot(lists:member(flurm_controller_cluster, ChildIds)),
        ?assertNot(lists:member(flurm_controller_failover, ChildIds))
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

is_cluster_enabled_false_when_disabled_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        ?assertEqual(false, flurm_controller_sup:is_cluster_enabled())
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

is_cluster_enabled_nonode_test() ->
    %% Even if enabled, cluster mode is false on nonode@nohost
    application:set_env(flurm_controller, enable_cluster, true),
    try
        Result = flurm_controller_sup:is_cluster_enabled(),
        case node() of
            'nonode@nohost' ->
                ?assertEqual(false, Result);
            _ ->
                ?assertEqual(true, Result)
        end
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

%%====================================================================
%% Child Spec Validation Tests
%%====================================================================

all_child_specs_valid_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        lists:foreach(fun(Spec) ->
            validate_child_spec(Spec)
        end, ChildSpecs)
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

validate_child_spec(Spec) ->
    %% Validate id
    Id = maps:get(id, Spec),
    ?assert(is_atom(Id), io_lib:format("Invalid id: ~p", [Id])),

    %% Validate MFA
    {M, F, A} = maps:get(start, Spec),
    ?assert(is_atom(M), io_lib:format("Invalid module: ~p", [M])),
    ?assert(is_atom(F), io_lib:format("Invalid function: ~p", [F])),
    ?assert(is_list(A), io_lib:format("Invalid args: ~p", [A])),

    %% Validate restart
    Restart = maps:get(restart, Spec),
    ?assert(lists:member(Restart, [permanent, transient, temporary]),
            io_lib:format("Invalid restart: ~p", [Restart])),

    %% Validate shutdown
    Shutdown = maps:get(shutdown, Spec),
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown, io_lib:format("Invalid shutdown: ~p", [Shutdown])),

    %% Validate type
    Type = maps:get(type, Spec),
    ?assert(lists:member(Type, [worker, supervisor]),
            io_lib:format("Invalid type: ~p", [Type])),

    %% Validate modules
    Modules = maps:get(modules, Spec),
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules, io_lib:format("Invalid modules: ~p", [Modules])),
    ok.

%%====================================================================
%% Configuration Helper Tests (exported via -ifdef(TEST))
%%====================================================================

get_listener_config_defaults_test() ->
    %% Clear any existing config
    application:unset_env(flurm_controller, listen_port),
    application:unset_env(flurm_controller, listen_address),
    application:unset_env(flurm_controller, num_acceptors),
    application:unset_env(flurm_controller, max_connections),

    {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),

    %% Verify defaults
    ?assertEqual(6817, Port),
    ?assertEqual("0.0.0.0", Address),
    ?assertEqual(10, NumAcceptors),
    ?assertEqual(1000, MaxConns).

get_listener_config_custom_test() ->
    application:set_env(flurm_controller, listen_port, 7777),
    application:set_env(flurm_controller, listen_address, "192.168.1.1"),
    application:set_env(flurm_controller, num_acceptors, 20),
    application:set_env(flurm_controller, max_connections, 5000),
    try
        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),
        ?assertEqual(7777, Port),
        ?assertEqual("192.168.1.1", Address),
        ?assertEqual(20, NumAcceptors),
        ?assertEqual(5000, MaxConns)
    after
        application:unset_env(flurm_controller, listen_port),
        application:unset_env(flurm_controller, listen_address),
        application:unset_env(flurm_controller, num_acceptors),
        application:unset_env(flurm_controller, max_connections)
    end.

get_node_listener_config_defaults_test() ->
    %% Clear any existing config
    application:unset_env(flurm_controller, node_listen_port),
    application:unset_env(flurm_controller, listen_address),
    application:unset_env(flurm_controller, num_acceptors),
    application:unset_env(flurm_controller, max_node_connections),

    {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),

    %% Verify defaults
    ?assertEqual(6818, Port),
    ?assertEqual("0.0.0.0", Address),
    ?assertEqual(10, NumAcceptors),
    ?assertEqual(500, MaxConns).

get_node_listener_config_custom_test() ->
    application:set_env(flurm_controller, node_listen_port, 8888),
    application:set_env(flurm_controller, listen_address, "10.0.0.1"),
    application:set_env(flurm_controller, num_acceptors, 15),
    application:set_env(flurm_controller, max_node_connections, 2000),
    try
        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),
        ?assertEqual(8888, Port),
        ?assertEqual("10.0.0.1", Address),
        ?assertEqual(15, NumAcceptors),
        ?assertEqual(2000, MaxConns)
    after
        application:unset_env(flurm_controller, node_listen_port),
        application:unset_env(flurm_controller, listen_address),
        application:unset_env(flurm_controller, num_acceptors),
        application:unset_env(flurm_controller, max_node_connections)
    end.

%%====================================================================
%% Address Parsing Tests
%%====================================================================

parse_address_ipv4_any_test() ->
    ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("0.0.0.0")).

parse_address_ipv6_any_test() ->
    ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_controller_sup:parse_address("::")).

parse_address_localhost_test() ->
    ?assertEqual({127, 0, 0, 1}, flurm_controller_sup:parse_address("127.0.0.1")).

parse_address_specific_test() ->
    ?assertEqual({192, 168, 1, 100}, flurm_controller_sup:parse_address("192.168.1.100")).

parse_address_tuple_passthrough_test() ->
    ?assertEqual({10, 0, 0, 1}, flurm_controller_sup:parse_address({10, 0, 0, 1})).

parse_address_invalid_fallback_test() ->
    %% Invalid addresses should fall back to 0.0.0.0
    ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("not-an-ip")).

%%====================================================================
%% Supervisor Spec Validation Tests
%%====================================================================

validate_supervisor_spec_test_() ->
    [
        {"init returns {ok, {SupFlags, ChildSpecs}}", fun() ->
            Result = flurm_controller_sup:init([]),
            ?assertMatch({ok, {_, _}}, Result),
            {ok, {SupFlags, ChildSpecs}} = Result,
            ?assert(is_map(SupFlags)),
            ?assert(is_list(ChildSpecs))
        end},
        {"strategy is valid OTP strategy", fun() ->
            {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
            Strategy = maps:get(strategy, SupFlags),
            ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
            ?assert(lists:member(Strategy, ValidStrategies))
        end},
        {"intensity is non-negative integer", fun() ->
            {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
            Intensity = maps:get(intensity, SupFlags),
            ?assert(is_integer(Intensity)),
            ?assert(Intensity >= 0)
        end},
        {"period is positive integer", fun() ->
            {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
            Period = maps:get(period, SupFlags),
            ?assert(is_integer(Period)),
            ?assert(Period > 0)
        end},
        {"has at least one child spec", fun() ->
            {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
            ?assert(length(ChildSpecs) > 0)
        end},
        {"all child specs have required fields", fun() ->
            application:set_env(flurm_controller, enable_cluster, false),
            try
                {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
                lists:foreach(fun(Spec) ->
                    ?assert(maps:is_key(id, Spec)),
                    ?assert(maps:is_key(start, Spec)),
                    ?assert(maps:is_key(restart, Spec)),
                    ?assert(maps:is_key(shutdown, Spec)),
                    ?assert(maps:is_key(type, Spec)),
                    ?assert(maps:is_key(modules, Spec))
                end, ChildSpecs)
            after
                application:unset_env(flurm_controller, enable_cluster)
            end
        end}
    ].

%%====================================================================
%% Specific Child Spec Tests
%%====================================================================

metrics_child_spec_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        MetricsSpec = lists:keyfind(flurm_metrics, 1,
            [{maps:get(id, S), S} || S <- ChildSpecs]),
        ?assertNotEqual(false, MetricsSpec),
        {_, Spec} = MetricsSpec,
        ?assertEqual(permanent, maps:get(restart, Spec)),
        ?assertEqual(worker, maps:get(type, Spec)),
        ?assertEqual({flurm_metrics, start_link, []}, maps:get(start, Spec))
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

scheduler_child_spec_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        SchedulerSpec = lists:keyfind(flurm_scheduler, 1,
            [{maps:get(id, S), S} || S <- ChildSpecs]),
        ?assertNotEqual(false, SchedulerSpec),
        {_, Spec} = SchedulerSpec,
        ?assertEqual(permanent, maps:get(restart, Spec)),
        ?assertEqual(worker, maps:get(type, Spec)),
        ?assertEqual({flurm_scheduler, start_link, []}, maps:get(start, Spec))
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.

partition_manager_child_spec_test() ->
    application:set_env(flurm_controller, enable_cluster, false),
    try
        {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
        PartitionSpec = lists:keyfind(flurm_partition_manager, 1,
            [{maps:get(id, S), S} || S <- ChildSpecs]),
        ?assertNotEqual(false, PartitionSpec),
        {_, Spec} = PartitionSpec,
        ?assertEqual(permanent, maps:get(restart, Spec)),
        ?assertEqual(worker, maps:get(type, Spec)),
        ?assertEqual({flurm_partition_manager, start_link, []}, maps:get(start, Spec))
    after
        application:unset_env(flurm_controller, enable_cluster)
    end.
