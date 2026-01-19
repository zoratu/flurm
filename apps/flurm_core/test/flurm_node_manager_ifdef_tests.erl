%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_node_manager internal functions
%%%
%%% Tests the internal pure functions exposed via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test: entry_to_node/1
%%====================================================================

entry_to_node_basic_test() ->
    Entry = #node_entry{
        name = <<"node01">>,
        hostname = <<"node01.cluster.local">>,
        state = idle,
        partitions = [<<"batch">>],
        cpus_total = 16,
        cpus_avail = 16,
        memory_total = 64000,
        memory_avail = 64000
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assert(is_record(Node, node)),
    %% Name is used as hostname for test compatibility
    ?assertEqual(<<"node01">>, Node#node.hostname),
    ?assertEqual(16, Node#node.cpus),
    ?assertEqual(64000, Node#node.memory_mb),
    ?assertEqual(idle, Node#node.state),
    ?assertEqual([<<"batch">>], Node#node.partitions),
    ?assertEqual(64000, Node#node.free_memory_mb).

entry_to_node_allocated_test() ->
    Entry = #node_entry{
        name = <<"compute001">>,
        hostname = <<"compute001.hpc.local">>,
        state = allocated,
        partitions = [<<"batch">>, <<"gpu">>],
        cpus_total = 64,
        cpus_avail = 32,
        memory_total = 256000,
        memory_avail = 128000
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assertEqual(<<"compute001">>, Node#node.hostname),
    ?assertEqual(64, Node#node.cpus),
    ?assertEqual(256000, Node#node.memory_mb),
    ?assertEqual(allocated, Node#node.state),
    ?assertEqual([<<"batch">>, <<"gpu">>], Node#node.partitions),
    ?assertEqual(128000, Node#node.free_memory_mb).

entry_to_node_default_fields_test() ->
    Entry = #node_entry{
        name = <<"minimal">>,
        hostname = <<"minimal.local">>,
        state = down,
        partitions = [],
        cpus_total = 1,
        cpus_avail = 0,
        memory_total = 1024,
        memory_avail = 0
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    %% Check default fields are set correctly
    ?assertEqual([], Node#node.features),
    ?assertEqual([], Node#node.running_jobs),
    ?assertEqual(0.0, Node#node.load_avg),
    ?assertEqual(#{}, Node#node.allocations),
    ?assertEqual(undefined, Node#node.drain_reason),
    ?assertEqual(undefined, Node#node.last_heartbeat),
    ?assertEqual([], Node#node.gres_config),
    ?assertEqual(#{}, Node#node.gres_available),
    ?assertEqual(#{}, Node#node.gres_total),
    ?assertEqual(#{}, Node#node.gres_allocations).

entry_to_node_multiple_partitions_test() ->
    Entry = #node_entry{
        name = <<"multi">>,
        hostname = <<"multi.local">>,
        state = idle,
        partitions = [<<"batch">>, <<"debug">>, <<"interactive">>, <<"gpu">>],
        cpus_total = 32,
        cpus_avail = 32,
        memory_total = 128000,
        memory_avail = 128000
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assertEqual([<<"batch">>, <<"debug">>, <<"interactive">>, <<"gpu">>],
                 Node#node.partitions).

entry_to_node_drain_state_test() ->
    Entry = #node_entry{
        name = <<"draining">>,
        hostname = <<"draining.local">>,
        state = drain,
        partitions = [<<"batch">>],
        cpus_total = 16,
        cpus_avail = 8,
        memory_total = 32000,
        memory_avail = 16000
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assertEqual(drain, Node#node.state),
    ?assertEqual(16000, Node#node.free_memory_mb).

entry_to_node_large_resources_test() ->
    Entry = #node_entry{
        name = <<"bignode">>,
        hostname = <<"bignode.cluster">>,
        state = idle,
        partitions = [<<"large">>],
        cpus_total = 256,
        cpus_avail = 256,
        memory_total = 4096000,  % 4TB
        memory_avail = 4096000
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assertEqual(256, Node#node.cpus),
    ?assertEqual(4096000, Node#node.memory_mb),
    ?assertEqual(4096000, Node#node.free_memory_mb).

entry_to_node_zero_available_test() ->
    Entry = #node_entry{
        name = <<"full">>,
        hostname = <<"full.local">>,
        state = allocated,
        partitions = [<<"batch">>],
        cpus_total = 16,
        cpus_avail = 0,
        memory_total = 64000,
        memory_avail = 0
    },
    Node = flurm_node_manager:entry_to_node(Entry),
    ?assertEqual(16, Node#node.cpus),
    ?assertEqual(64000, Node#node.memory_mb),
    ?assertEqual(0, Node#node.free_memory_mb).
