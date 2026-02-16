%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_partition_manager module
%%% Tests for partition management gen_server
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_manager_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

partition_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"list_partitions returns default partition", fun test_list_partitions_default/0},
             {"get_partition for default", fun test_get_partition_default/0},
             {"get_partition not found", fun test_get_partition_not_found/0},
             {"create_partition success", fun test_create_partition/0},
             {"create_partition duplicate", fun test_create_partition_duplicate/0},
             {"update_partition success", fun test_update_partition/0},
             {"update_partition not found", fun test_update_partition_not_found/0},
             {"delete_partition success", fun test_delete_partition/0},
             {"delete_partition default fails", fun test_delete_default_partition/0},
             {"delete_partition not found", fun test_delete_partition_not_found/0},
             {"add_node success", fun test_add_node/0},
             {"add_node partition not found", fun test_add_node_not_found/0},
             {"remove_node success", fun test_remove_node/0},
             {"remove_node partition not found", fun test_remove_node_not_found/0}
         ]
     end}.

setup() ->
    %% Stop any existing partition manager
    case whereis(flurm_partition_manager) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 2000)
    end,
    timer:sleep(50),
    {ok, NewPid} = flurm_partition_manager:start_link(),
    unlink(NewPid),
    NewPid.

cleanup(Pid) ->
    catch gen_server:stop(Pid, shutdown, 2000).

%%====================================================================
%% list_partitions Tests
%%====================================================================

test_list_partitions_default() ->
    Partitions = flurm_partition_manager:list_partitions(),
    ?assert(is_list(Partitions)),
    %% Should have at least the default partition
    ?assert(length(Partitions) >= 1),
    %% Find default partition
    Default = lists:filter(fun(P) ->
        P#partition.name =:= <<"default">>
    end, Partitions),
    ?assertEqual(1, length(Default)).

%%====================================================================
%% get_partition Tests
%%====================================================================

test_get_partition_default() ->
    {ok, Partition} = flurm_partition_manager:get_partition(<<"default">>),
    ?assertEqual(<<"default">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state).

test_get_partition_not_found() ->
    Result = flurm_partition_manager:get_partition(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% create_partition Tests
%%====================================================================

test_create_partition() ->
    PartitionSpec = #{
        name => <<"test_partition">>,
        state => up,
        nodes => [],
        max_time => 7200,
        default_time => 1800,
        max_nodes => 10,
        priority => 50
    },
    Result = flurm_partition_manager:create_partition(PartitionSpec),
    ?assertEqual(ok, Result),

    %% Verify it was created
    {ok, Partition} = flurm_partition_manager:get_partition(<<"test_partition">>),
    ?assertEqual(<<"test_partition">>, Partition#partition.name),
    ?assertEqual(50, Partition#partition.priority),

    %% Cleanup
    flurm_partition_manager:delete_partition(<<"test_partition">>).

test_create_partition_duplicate() ->
    %% Try to create a partition with existing name
    PartitionSpec = #{
        name => <<"default">>,
        state => up
    },
    Result = flurm_partition_manager:create_partition(PartitionSpec),
    ?assertEqual({error, already_exists}, Result).

%%====================================================================
%% update_partition Tests
%%====================================================================

test_update_partition() ->
    %% First create a partition
    PartitionSpec = #{
        name => <<"update_test">>,
        state => up,
        max_time => 3600,
        priority => 100
    },
    ok = flurm_partition_manager:create_partition(PartitionSpec),

    %% Update it
    Updates = #{
        state => drain,
        priority => 200,
        max_time => 7200
    },
    Result = flurm_partition_manager:update_partition(<<"update_test">>, Updates),
    ?assertEqual(ok, Result),

    %% Verify updates
    {ok, Partition} = flurm_partition_manager:get_partition(<<"update_test">>),
    ?assertEqual(drain, Partition#partition.state),
    ?assertEqual(200, Partition#partition.priority),
    ?assertEqual(7200, Partition#partition.max_time),

    %% Cleanup
    flurm_partition_manager:delete_partition(<<"update_test">>).

test_update_partition_not_found() ->
    Result = flurm_partition_manager:update_partition(<<"nonexistent">>, #{state => down}),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% delete_partition Tests
%%====================================================================

test_delete_partition() ->
    %% Create a partition to delete
    PartitionSpec = #{
        name => <<"delete_test">>,
        state => up
    },
    ok = flurm_partition_manager:create_partition(PartitionSpec),

    %% Verify it exists
    {ok, _} = flurm_partition_manager:get_partition(<<"delete_test">>),

    %% Delete it
    Result = flurm_partition_manager:delete_partition(<<"delete_test">>),
    ?assertEqual(ok, Result),

    %% Verify it's gone
    ?assertEqual({error, not_found}, flurm_partition_manager:get_partition(<<"delete_test">>)).

test_delete_default_partition() ->
    Result = flurm_partition_manager:delete_partition(<<"default">>),
    ?assertEqual({error, cannot_delete_default}, Result).

test_delete_partition_not_found() ->
    Result = flurm_partition_manager:delete_partition(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% add_node Tests
%%====================================================================

test_add_node() ->
    %% Create a partition
    PartitionSpec = #{
        name => <<"node_test">>,
        state => up,
        nodes => []
    },
    ok = flurm_partition_manager:create_partition(PartitionSpec),

    %% Add a node
    Result = flurm_partition_manager:add_node(<<"node_test">>, <<"compute01">>),
    ?assertEqual(ok, Result),

    %% Verify node was added
    {ok, Partition} = flurm_partition_manager:get_partition(<<"node_test">>),
    ?assert(lists:member(<<"compute01">>, Partition#partition.nodes)),

    %% Cleanup
    flurm_partition_manager:delete_partition(<<"node_test">>).

test_add_node_not_found() ->
    Result = flurm_partition_manager:add_node(<<"nonexistent">>, <<"node1">>),
    ?assertEqual({error, partition_not_found}, Result).

%%====================================================================
%% remove_node Tests
%%====================================================================

test_remove_node() ->
    %% Create a partition with a node
    PartitionSpec = #{
        name => <<"remove_node_test">>,
        state => up,
        nodes => []
    },
    ok = flurm_partition_manager:create_partition(PartitionSpec),
    ok = flurm_partition_manager:add_node(<<"remove_node_test">>, <<"node1">>),

    %% Verify node is present
    {ok, P1} = flurm_partition_manager:get_partition(<<"remove_node_test">>),
    ?assert(lists:member(<<"node1">>, P1#partition.nodes)),

    %% Remove the node
    Result = flurm_partition_manager:remove_node(<<"remove_node_test">>, <<"node1">>),
    ?assertEqual(ok, Result),

    %% Verify node was removed
    {ok, P2} = flurm_partition_manager:get_partition(<<"remove_node_test">>),
    ?assertEqual(false, lists:member(<<"node1">>, P2#partition.nodes)),

    %% Cleanup
    flurm_partition_manager:delete_partition(<<"remove_node_test">>).

test_remove_node_not_found() ->
    Result = flurm_partition_manager:remove_node(<<"nonexistent">>, <<"node1">>),
    ?assertEqual({error, partition_not_found}, Result).

%%====================================================================
%% Internal Function Tests (-ifdef(TEST) exports)
%%====================================================================

apply_partition_updates_state_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{state => drain},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(drain, Result#partition.state).

apply_partition_updates_max_time_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{max_time => 7200},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(7200, Result#partition.max_time).

apply_partition_updates_default_time_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{default_time => 900},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(900, Result#partition.default_time).

apply_partition_updates_max_nodes_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{max_nodes => 50},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(50, Result#partition.max_nodes).

apply_partition_updates_priority_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{priority => 500},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(500, Result#partition.priority).

apply_partition_updates_allow_root_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{allow_root => true},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(true, Result#partition.allow_root).

apply_partition_updates_unknown_key_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{unknown_key => value},
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    %% Should remain unchanged
    ?assertEqual(Partition, Result).

apply_partition_updates_multiple_test() ->
    Partition = #partition{
        name = <<"test">>,
        state = up,
        nodes = [],
        max_time = 3600,
        default_time = 1800,
        max_nodes = 10,
        priority = 100,
        allow_root = false
    },
    Updates = #{
        state => drain,
        priority => 200,
        max_time => 7200,
        allow_root => true
    },
    Result = flurm_partition_manager:apply_partition_updates(Partition, Updates),
    ?assertEqual(drain, Result#partition.state),
    ?assertEqual(200, Result#partition.priority),
    ?assertEqual(7200, Result#partition.max_time),
    ?assertEqual(true, Result#partition.allow_root).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

unknown_call_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown call returns error", fun() ->
             Result = gen_server:call(Pid, unknown_request),
             ?assertEqual({error, unknown_request}, Result)
         end}]
     end}.

unknown_cast_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown cast doesn't crash", fun() ->
             gen_server:cast(Pid, unknown_cast),
             timer:sleep(50),
             ?assert(is_process_alive(Pid))
         end}]
     end}.

unknown_info_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [{"unknown info doesn't crash", fun() ->
             Pid ! unknown_message,
             timer:sleep(50),
             ?assert(is_process_alive(Pid))
         end}]
     end}.
