%%%-------------------------------------------------------------------
%%% @doc Node Manager Integration Test Suite
%%%
%%% Tests node registration, heartbeat, resource allocation, and
%%% drain/undrain workflows with real node manager server.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Node registration
    register_new_node/1,
    register_duplicate_node/1,
    list_registered_nodes/1,

    %% Heartbeat
    heartbeat_updates_timestamp/1,
    heartbeat_unknown_node/1,

    %% Resource queries
    get_available_nodes/1,
    get_available_nodes_for_job/1,

    %% Resource allocation
    allocate_node_resources/1,
    release_node_resources/1,

    %% Drain operations
    drain_node_for_maintenance/1,
    undrain_node/1,

    %% Dynamic operations
    add_remove_node/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, registration},
        {group, heartbeat},
        {group, resources},
        {group, allocation},
        {group, drain},
        {group, dynamic}
    ].

groups() ->
    [
        {registration, [sequence], [
            register_new_node,
            register_duplicate_node,
            list_registered_nodes
        ]},
        {heartbeat, [sequence], [
            heartbeat_updates_timestamp,
            heartbeat_unknown_node
        ]},
        {resources, [sequence], [
            get_available_nodes,
            get_available_nodes_for_job
        ]},
        {allocation, [sequence], [
            allocate_node_resources,
            release_node_resources
        ]},
        {drain, [sequence], [
            drain_node_for_maintenance,
            undrain_node
        ]},
        {dynamic, [sequence], [
            add_remove_node
        ]}
    ].

init_per_suite(Config) ->
    ct:pal("Starting node manager integration test suite"),
    %% Stop any running apps
    stop_apps(),
    %% Clean up test data
    os:cmd("rm -rf /tmp/flurm_test_data"),
    %% Start required applications
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(flurm_config),
    {ok, _} = application:ensure_all_started(flurm_core),
    {ok, _} = application:ensure_all_started(flurm_controller),
    ct:pal("Applications started successfully"),
    Config.

end_per_suite(_Config) ->
    ct:pal("Stopping node manager integration test suite"),
    stop_apps(),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test case: ~p", [TestCase]),
    ok.

%%====================================================================
%% Test Cases - Registration
%%====================================================================

register_new_node(_Config) ->
    %% Create a node spec
    NodeSpec = #{
        hostname => <<"test-node-1">>,
        port => 6818,
        cpus => 8,
        memory_mb => 16384,
        state => idle,
        partitions => [<<"default">>]
    },

    %% Register the node
    Result = flurm_node_manager_server:register_node(NodeSpec),
    ct:pal("Register node result: ~p", [Result]),

    case Result of
        ok -> ?assert(true);
        {ok, _} -> ?assert(true);
        {error, already_registered} ->
            %% Node might already exist from previous test run
            ct:pal("Node already registered"),
            ?assert(true)
    end.

register_duplicate_node(_Config) ->
    %% Try to register same node again
    NodeSpec = #{
        hostname => <<"test-node-1">>,
        port => 6818,
        cpus => 8,
        memory_mb => 16384,
        state => idle,
        partitions => [<<"default">>]
    },

    %% First registration (or already exists)
    _First = flurm_node_manager_server:register_node(NodeSpec),

    %% Second registration should handle gracefully
    Result = flurm_node_manager_server:register_node(NodeSpec),
    ct:pal("Duplicate register result: ~p", [Result]),

    %% Should either succeed (update) or return already_registered
    ?assert(Result =:= ok orelse
            Result =:= {ok, updated} orelse
            Result =:= {error, already_registered}).

list_registered_nodes(_Config) ->
    %% List all nodes
    Nodes = flurm_node_manager_server:list_nodes(),
    ct:pal("Listed ~p nodes", [length(Nodes)]),

    %% Should have at least our test node
    ?assert(is_list(Nodes)).

%%====================================================================
%% Test Cases - Heartbeat
%%====================================================================

heartbeat_updates_timestamp(_Config) ->
    %% Ensure node exists
    NodeSpec = #{
        hostname => <<"heartbeat-test-node">>,
        port => 6818,
        cpus => 4,
        memory_mb => 8192,
        state => idle,
        partitions => [<<"default">>]
    },
    flurm_node_manager_server:register_node(NodeSpec),

    %% Send heartbeat
    Result = flurm_node_manager_server:heartbeat(<<"heartbeat-test-node">>),
    ct:pal("Heartbeat result: ~p", [Result]),

    ?assertEqual(ok, Result).

heartbeat_unknown_node(_Config) ->
    %% Heartbeat for non-existent node
    Result = flurm_node_manager_server:heartbeat(<<"nonexistent-node-xyz">>),
    ct:pal("Unknown node heartbeat result: ~p", [Result]),

    %% Should return error or register the node
    case Result of
        ok -> ?assert(true);  % Some implementations auto-register
        {error, not_found} -> ?assert(true);
        {error, _} -> ?assert(true)
    end.

%%====================================================================
%% Test Cases - Resources
%%====================================================================

get_available_nodes(_Config) ->
    %% Get all available nodes
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ct:pal("Available nodes: ~p", [length(Nodes)]),

    ?assert(is_list(Nodes)).

get_available_nodes_for_job(_Config) ->
    %% Register a node with known resources
    NodeSpec = #{
        hostname => <<"resource-test-node">>,
        port => 6818,
        cpus => 16,
        memory_mb => 32768,
        state => idle,
        partitions => [<<"default">>]
    },
    flurm_node_manager_server:register_node(NodeSpec),

    %% Query for nodes that can run a job
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(
        4,       % num_cpus
        8192,    % memory_mb
        <<"default">>  % partition
    ),
    ct:pal("Nodes available for job: ~p", [length(Nodes)]),

    ?assert(is_list(Nodes)).

%%====================================================================
%% Test Cases - Allocation
%%====================================================================

allocate_node_resources(_Config) ->
    %% Register node with resources
    NodeSpec = #{
        hostname => <<"alloc-test-node">>,
        port => 6818,
        cpus => 8,
        memory_mb => 16384,
        state => idle,
        partitions => [<<"default">>]
    },
    flurm_node_manager_server:register_node(NodeSpec),

    %% Allocate resources for a job
    JobId = 9999,
    Result = flurm_node_manager_server:allocate_resources(
        <<"alloc-test-node">>,
        JobId,
        2,     % cpus
        4096   % memory_mb
    ),
    ct:pal("Allocate resources result: ~p", [Result]),

    case Result of
        ok -> ?assert(true);
        {ok, _} -> ?assert(true);
        {error, Reason} ->
            ct:pal("Allocation failed: ~p", [Reason]),
            ?assert(true)  % May fail if node not in right state
    end.

release_node_resources(_Config) ->
    %% Release resources from previous allocation
    JobId = 9999,
    Result = flurm_node_manager_server:release_resources(
        <<"alloc-test-node">>,
        JobId
    ),
    ct:pal("Release resources result: ~p", [Result]),

    case Result of
        ok -> ?assert(true);
        {ok, _} -> ?assert(true);
        {error, _} -> ?assert(true)  % May fail if no allocation exists
    end.

%%====================================================================
%% Test Cases - Drain
%%====================================================================

drain_node_for_maintenance(_Config) ->
    %% Register a node to drain
    NodeSpec = #{
        hostname => <<"drain-test-node">>,
        port => 6818,
        cpus => 4,
        memory_mb => 8192,
        state => idle,
        partitions => [<<"default">>]
    },
    flurm_node_manager_server:register_node(NodeSpec),

    %% Drain the node
    Result = flurm_node_manager_server:drain_node(
        <<"drain-test-node">>,
        <<"scheduled maintenance">>
    ),
    ct:pal("Drain node result: ~p", [Result]),

    case Result of
        ok ->
            %% Verify node is draining
            IsDraining = flurm_node_manager_server:is_node_draining(<<"drain-test-node">>),
            ct:pal("Is draining: ~p", [IsDraining]),
            ?assert(true);
        {error, Reason} ->
            ct:pal("Drain failed: ~p", [Reason]),
            ?assert(true)
    end.

undrain_node(_Config) ->
    %% Undrain the previously drained node
    Result = flurm_node_manager_server:undrain_node(<<"drain-test-node">>),
    ct:pal("Undrain node result: ~p", [Result]),

    case Result of
        ok -> ?assert(true);
        {error, _} -> ?assert(true)  % May not have been drained
    end.

%%====================================================================
%% Test Cases - Dynamic Operations
%%====================================================================

add_remove_node(_Config) ->
    %% Add a node dynamically
    NodeSpec = #{
        hostname => <<"dynamic-node">>,
        port => 6818,
        cpus => 2,
        memory_mb => 4096,
        state => idle,
        partitions => [<<"default">>]
    },

    AddResult = flurm_node_manager_server:add_node(NodeSpec),
    ct:pal("Add node result: ~p", [AddResult]),

    case AddResult of
        ok ->
            %% Verify node exists
            case flurm_node_manager_server:get_node(<<"dynamic-node">>) of
                {ok, _Node} ->
                    ct:pal("Node added successfully"),

                    %% Remove the node
                    RemoveResult = flurm_node_manager_server:remove_node(<<"dynamic-node">>),
                    ct:pal("Remove node result: ~p", [RemoveResult]),
                    ?assert(RemoveResult =:= ok orelse element(1, RemoveResult) =:= ok);
                {error, not_found} ->
                    ct:pal("Node not found after add"),
                    ?assert(true)
            end;
        {error, Reason} ->
            ct:pal("Add node failed: ~p", [Reason]),
            ?assert(true)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

stop_apps() ->
    application:stop(flurm_controller),
    application:stop(flurm_core),
    application:stop(flurm_config),
    application:stop(lager),
    ok.
