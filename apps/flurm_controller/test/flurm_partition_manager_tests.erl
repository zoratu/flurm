%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_partition_manager module
%%%
%%% Tests cover:
%%% - Partition creation and deletion
%%% - Partition retrieval and listing
%%% - Partition updates
%%% - Node management within partitions
%%% - Default partition handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_manager_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure flurm_core module is accessible (for new_partition, etc.)
    %% Start the partition manager
    case whereis(flurm_partition_manager) of
        undefined ->
            {ok, Pid} = flurm_partition_manager:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    gen_server:stop(flurm_partition_manager);
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

partition_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Default partition tests
      {"default partition exists", fun test_default_partition_exists/0},
      {"cannot delete default partition", fun test_cannot_delete_default/0},

      %% CRUD operations
      {"create partition", fun test_create_partition/0},
      {"create partition already exists", fun test_create_already_exists/0},
      {"get partition", fun test_get_partition/0},
      {"get partition not found", fun test_get_partition_not_found/0},
      {"list partitions", fun test_list_partitions/0},
      {"delete partition", fun test_delete_partition/0},
      {"delete partition not found", fun test_delete_not_found/0},

      %% Update operations
      {"update partition state", fun test_update_state/0},
      {"update partition max_time", fun test_update_max_time/0},
      {"update partition default_time", fun test_update_default_time/0},
      {"update partition max_nodes", fun test_update_max_nodes/0},
      {"update partition priority", fun test_update_priority/0},
      {"update partition allow_root", fun test_update_allow_root/0},
      {"update partition not found", fun test_update_not_found/0},
      {"update multiple fields", fun test_update_multiple/0},

      %% Node management
      {"add node to partition", fun test_add_node/0},
      {"add node partition not found", fun test_add_node_not_found/0},
      {"remove node from partition", fun test_remove_node/0},
      {"remove node partition not found", fun test_remove_node_not_found/0}
     ]}.

%%====================================================================
%% Default Partition Tests
%%====================================================================

test_default_partition_exists() ->
    {ok, Partition} = flurm_partition_manager:get_partition(<<"default">>),
    ?assertEqual(<<"default">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state).

test_cannot_delete_default() ->
    Result = flurm_partition_manager:delete_partition(<<"default">>),
    ?assertEqual({error, cannot_delete_default}, Result),

    %% Verify it still exists
    {ok, _} = flurm_partition_manager:get_partition(<<"default">>).

%%====================================================================
%% CRUD Tests
%%====================================================================

test_create_partition() ->
    PartitionSpec = #{
        name => <<"gpu">>,
        state => up,
        nodes => [<<"gpu-node1">>, <<"gpu-node2">>],
        max_time => 172800,
        default_time => 7200,
        max_nodes => 50,
        priority => 200
    },

    ok = flurm_partition_manager:create_partition(PartitionSpec),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"gpu">>),
    ?assertEqual(<<"gpu">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state),
    ?assertEqual([<<"gpu-node1">>, <<"gpu-node2">>], Partition#partition.nodes),
    ?assertEqual(172800, Partition#partition.max_time),
    ?assertEqual(7200, Partition#partition.default_time),
    ?assertEqual(50, Partition#partition.max_nodes),
    ?assertEqual(200, Partition#partition.priority).

test_create_already_exists() ->
    %% Create first time
    ok = flurm_partition_manager:create_partition(#{
        name => <<"duplicate">>
    }),

    %% Try to create again
    Result = flurm_partition_manager:create_partition(#{
        name => <<"duplicate">>
    }),
    ?assertEqual({error, already_exists}, Result).

test_get_partition() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"gettest">>,
        priority => 500
    }),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"gettest">>),
    ?assertEqual(<<"gettest">>, Partition#partition.name),
    ?assertEqual(500, Partition#partition.priority).

test_get_partition_not_found() ->
    Result = flurm_partition_manager:get_partition(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_partitions() ->
    %% Create some partitions
    ok = flurm_partition_manager:create_partition(#{name => <<"list_a">>}),
    ok = flurm_partition_manager:create_partition(#{name => <<"list_b">>}),

    Partitions = flurm_partition_manager:list_partitions(),

    %% Should have at least default + 2 created
    ?assert(length(Partitions) >= 3),

    %% Check all are partition records
    lists:foreach(fun(P) ->
        ?assert(is_record(P, partition))
    end, Partitions),

    %% Check our partitions are in the list
    Names = [P#partition.name || P <- Partitions],
    ?assert(lists:member(<<"default">>, Names)),
    ?assert(lists:member(<<"list_a">>, Names)),
    ?assert(lists:member(<<"list_b">>, Names)).

test_delete_partition() ->
    ok = flurm_partition_manager:create_partition(#{name => <<"todelete">>}),
    {ok, _} = flurm_partition_manager:get_partition(<<"todelete">>),

    ok = flurm_partition_manager:delete_partition(<<"todelete">>),

    Result = flurm_partition_manager:get_partition(<<"todelete">>),
    ?assertEqual({error, not_found}, Result).

test_delete_not_found() ->
    Result = flurm_partition_manager:delete_partition(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Update Tests
%%====================================================================

test_update_state() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"statetest">>,
        state => up
    }),

    ok = flurm_partition_manager:update_partition(<<"statetest">>, #{state => down}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"statetest">>),
    ?assertEqual(down, Partition#partition.state),

    ok = flurm_partition_manager:update_partition(<<"statetest">>, #{state => drain}),
    {ok, Partition2} = flurm_partition_manager:get_partition(<<"statetest">>),
    ?assertEqual(drain, Partition2#partition.state).

test_update_max_time() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"maxtimetest">>,
        max_time => 3600
    }),

    ok = flurm_partition_manager:update_partition(<<"maxtimetest">>, #{max_time => 7200}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"maxtimetest">>),
    ?assertEqual(7200, Partition#partition.max_time).

test_update_default_time() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"defaulttimetest">>,
        default_time => 1800
    }),

    ok = flurm_partition_manager:update_partition(<<"defaulttimetest">>, #{default_time => 3600}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"defaulttimetest">>),
    ?assertEqual(3600, Partition#partition.default_time).

test_update_max_nodes() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"maxnodestest">>,
        max_nodes => 10
    }),

    ok = flurm_partition_manager:update_partition(<<"maxnodestest">>, #{max_nodes => 20}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"maxnodestest">>),
    ?assertEqual(20, Partition#partition.max_nodes).

test_update_priority() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"prioritytest">>,
        priority => 100
    }),

    ok = flurm_partition_manager:update_partition(<<"prioritytest">>, #{priority => 500}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"prioritytest">>),
    ?assertEqual(500, Partition#partition.priority).

test_update_allow_root() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"roottest">>
    }),

    ok = flurm_partition_manager:update_partition(<<"roottest">>, #{allow_root => true}),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"roottest">>),
    ?assertEqual(true, Partition#partition.allow_root).

test_update_not_found() ->
    Result = flurm_partition_manager:update_partition(<<"nonexistent">>, #{state => down}),
    ?assertEqual({error, not_found}, Result).

test_update_multiple() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"multiupdate">>,
        state => up,
        max_time => 3600,
        priority => 100
    }),

    Updates = #{
        state => drain,
        max_time => 7200,
        default_time => 1800,
        max_nodes => 50,
        priority => 200
    },
    ok = flurm_partition_manager:update_partition(<<"multiupdate">>, Updates),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"multiupdate">>),
    ?assertEqual(drain, Partition#partition.state),
    ?assertEqual(7200, Partition#partition.max_time),
    ?assertEqual(1800, Partition#partition.default_time),
    ?assertEqual(50, Partition#partition.max_nodes),
    ?assertEqual(200, Partition#partition.priority).

%%====================================================================
%% Node Management Tests
%%====================================================================

test_add_node() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"nodetest">>,
        nodes => []
    }),

    ok = flurm_partition_manager:add_node(<<"nodetest">>, <<"node001">>),

    {ok, Partition1} = flurm_partition_manager:get_partition(<<"nodetest">>),
    ?assert(lists:member(<<"node001">>, Partition1#partition.nodes)),

    %% Add more nodes
    ok = flurm_partition_manager:add_node(<<"nodetest">>, <<"node002">>),
    ok = flurm_partition_manager:add_node(<<"nodetest">>, <<"node003">>),

    {ok, Partition2} = flurm_partition_manager:get_partition(<<"nodetest">>),
    ?assertEqual(3, length(Partition2#partition.nodes)),
    ?assert(lists:member(<<"node001">>, Partition2#partition.nodes)),
    ?assert(lists:member(<<"node002">>, Partition2#partition.nodes)),
    ?assert(lists:member(<<"node003">>, Partition2#partition.nodes)).

test_add_node_not_found() ->
    Result = flurm_partition_manager:add_node(<<"nonexistent">>, <<"node">>),
    ?assertEqual({error, partition_not_found}, Result).

test_remove_node() ->
    ok = flurm_partition_manager:create_partition(#{
        name => <<"removetest">>,
        nodes => [<<"nodeA">>, <<"nodeB">>, <<"nodeC">>]
    }),

    ok = flurm_partition_manager:remove_node(<<"removetest">>, <<"nodeB">>),

    {ok, Partition} = flurm_partition_manager:get_partition(<<"removetest">>),
    ?assertEqual(2, length(Partition#partition.nodes)),
    ?assert(lists:member(<<"nodeA">>, Partition#partition.nodes)),
    ?assertNot(lists:member(<<"nodeB">>, Partition#partition.nodes)),
    ?assert(lists:member(<<"nodeC">>, Partition#partition.nodes)).

test_remove_node_not_found() ->
    Result = flurm_partition_manager:remove_node(<<"nonexistent">>, <<"node">>),
    ?assertEqual({error, partition_not_found}, Result).

%%====================================================================
%% Unknown Request Tests
%%====================================================================

unknown_request_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle unknown call", fun test_unknown_call/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_partition_manager, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% Initial State Tests
%%====================================================================

initial_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"default partition config", fun test_default_partition_config/0}
     ]}.

test_default_partition_config() ->
    {ok, Default} = flurm_partition_manager:get_partition(<<"default">>),

    ?assertEqual(<<"default">>, Default#partition.name),
    ?assertEqual(up, Default#partition.state),
    ?assertEqual([], Default#partition.nodes),
    ?assertEqual(86400, Default#partition.max_time),       % 24 hours
    ?assertEqual(3600, Default#partition.default_time),    % 1 hour
    ?assertEqual(1000, Default#partition.max_nodes),
    ?assertEqual(100, Default#partition.priority).
