%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_node_manager_server module
%%%
%%% Tests the node manager server internal functions via the
%%% exported TEST interface. Tests gen_server callbacks directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_server_real_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Helper Functions (exported via -ifdef(TEST))
%%====================================================================

%% Test apply_node_updates function
apply_node_updates_test_() ->
    [
     {"updates state",
      fun() ->
          Node = #node{
              hostname = <<"node1">>,
              state = idle,
              cpus = 8,
              memory_mb = 16384,
              load_avg = 0.5,
              free_memory_mb = 8000,
              running_jobs = []
          },
          Updates = #{state => mixed},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(mixed, Result#node.state)
      end},
     {"updates load_avg",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = []},
          Updates = #{load_avg => 2.5},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(2.5, Result#node.load_avg)
      end},
     {"updates free_memory_mb",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = []},
          Updates = #{free_memory_mb => 2000},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(2000, Result#node.free_memory_mb)
      end},
     {"updates running_jobs",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = []},
          Updates = #{running_jobs => [1, 2, 3]},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual([1, 2, 3], Result#node.running_jobs)
      end},
     {"ignores unknown fields",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = []},
          Updates = #{unknown_field => value},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(Node, Result)
      end}
    ].

%% Test get_available_resources function
get_available_resources_test_() ->
    [
     {"returns full resources when no allocations",
      fun() ->
          Node = #node{cpus = 16, memory_mb = 32768, allocations = #{}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(16, CpusAvail),
          ?assertEqual(32768, MemAvail)
      end},
     {"subtracts allocated resources",
      fun() ->
          Node = #node{cpus = 16, memory_mb = 32768,
                       allocations = #{1 => {4, 8192}, 2 => {2, 4096}}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(10, CpusAvail),  %% 16 - 4 - 2
          ?assertEqual(20480, MemAvail)  %% 32768 - 8192 - 4096
      end},
     {"handles single allocation",
      fun() ->
          Node = #node{cpus = 8, memory_mb = 16384, allocations = #{42 => {8, 16384}}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(0, CpusAvail),
          ?assertEqual(0, MemAvail)
      end}
    ].

%% Test config_to_node_spec function
config_to_node_spec_test_() ->
    [
     {"converts minimal config",
      fun() ->
          Config = #{},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(<<"node1">>, maps:get(hostname, Spec)),
          ?assert(maps:is_key(cpus, Spec)),
          ?assert(maps:is_key(memory_mb, Spec))
      end},
     {"uses explicit cpus",
      fun() ->
          Config = #{cpus => 32},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(32, maps:get(cpus, Spec))
      end},
     {"calculates cpus from socket/core/thread",
      fun() ->
          Config = #{sockets => 2, corespersocket => 8, threadspercore => 2},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(32, maps:get(cpus, Spec))  %% 2 * 8 * 2
      end},
     {"uses realmemory for memory_mb",
      fun() ->
          Config = #{realmemory => 65536},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(65536, maps:get(memory_mb, Spec))
      end},
     {"includes features",
      fun() ->
          Config = #{feature => [<<"gpu">>, <<"nvme">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([<<"gpu">>, <<"nvme">>], maps:get(features, Spec))
      end},
     {"includes partitions",
      fun() ->
          Config = #{partitions => [<<"compute">>, <<"gpu">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([<<"compute">>, <<"gpu">>], maps:get(partitions, Spec))
      end}
    ].

%% Test apply_property_updates function
apply_property_updates_test_() ->
    [
     {"updates cpus",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{cpus => 16},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(16, Result#node.cpus)
      end},
     {"updates memory_mb",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{memory_mb => 32768},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(32768, Result#node.memory_mb)
      end},
     {"updates features",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{features => [<<"gpu">>]},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual([<<"gpu">>], Result#node.features)
      end},
     {"updates partitions",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{partitions => [<<"compute">>]},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual([<<"compute">>], Result#node.partitions)
      end},
     {"updates state",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{state => drain},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(drain, Result#node.state)
      end},
     {"handles multiple updates",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{cpus => 32, memory_mb => 65536, features => [<<"fast">>]},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(32, Result#node.cpus),
          ?assertEqual(65536, Result#node.memory_mb),
          ?assertEqual([<<"fast">>], Result#node.features)
      end}
    ].

%% Test build_gres_maps function
build_gres_maps_test_() ->
    [
     {"empty gres list",
      fun() ->
          {Config, Total, Available} = flurm_node_manager_server:build_gres_maps([]),
          ?assertEqual([], Config),
          ?assertEqual(#{}, Total),
          ?assertEqual(#{}, Available)
      end},
     {"single gpu device",
      fun() ->
          GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
          {Config, Total, Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(GRESList, Config),
          ?assertEqual(4, maps:get(<<"gpu">>, Total)),
          ?assertEqual(4, maps:get(<<"gpu:a100">>, Total)),
          ?assertEqual(4, maps:get(<<"gpu">>, Available)),
          ?assertEqual(4, maps:get(<<"gpu:a100">>, Available))
      end},
     {"multiple gres devices",
      fun() ->
          GRESList = [
              #{type => gpu, name => <<"a100">>, count => 4},
              #{type => gpu, name => <<"v100">>, count => 2},
              #{type => fpga, count => 1}
          ],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(6, maps:get(<<"gpu">>, Total)),  %% 4 + 2
          ?assertEqual(4, maps:get(<<"gpu:a100">>, Total)),
          ?assertEqual(2, maps:get(<<"gpu:v100">>, Total)),
          ?assertEqual(1, maps:get(<<"fpga">>, Total))
      end}
    ].

%% Test atom_to_gres_key function
atom_to_gres_key_test_() ->
    [
     {"gpu atom",
      ?_assertEqual(<<"gpu">>, flurm_node_manager_server:atom_to_gres_key(gpu))},
     {"fpga atom",
      ?_assertEqual(<<"fpga">>, flurm_node_manager_server:atom_to_gres_key(fpga))},
     {"mic atom",
      ?_assertEqual(<<"mic">>, flurm_node_manager_server:atom_to_gres_key(mic))},
     {"mps atom",
      ?_assertEqual(<<"mps">>, flurm_node_manager_server:atom_to_gres_key(mps))},
     {"shard atom",
      ?_assertEqual(<<"shard">>, flurm_node_manager_server:atom_to_gres_key(shard))},
     {"unknown atom converts to binary",
      ?_assertEqual(<<"custom">>, flurm_node_manager_server:atom_to_gres_key(custom))},
     {"binary stays binary",
      ?_assertEqual(<<"mytype">>, flurm_node_manager_server:atom_to_gres_key(<<"mytype">>))}
    ].

%% Test check_node_gres_availability function
check_node_gres_availability_test_() ->
    [
     {"empty gres spec is always available",
      fun() ->
          Node = #node{gres_available = #{<<"gpu">> => 4}},
          Result = flurm_node_manager_server:check_node_gres_availability(Node, <<>>),
          ?assertEqual(true, Result)
      end}
    ].

%% Test build_updates_from_config function
build_updates_from_config_test_() ->
    [
     {"no changes returns empty map",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{cpus => 8, realmemory => 16384, feature => [], partitions => []},
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(#{}, Updates)
      end},
     {"cpu change detected",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{cpus => 16},
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(16, maps:get(cpus, Updates))
      end},
     {"memory change detected",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{realmemory => 32768},
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(32768, maps:get(memory_mb, Updates))
      end},
     {"feature change detected",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{feature => [<<"gpu">>]},
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual([<<"gpu">>], maps:get(features, Updates))
      end}
    ].

%%====================================================================
%% API Function Export Tests
%%====================================================================

api_exports_test_() ->
    [
     {"start_link/0 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"register_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({register_node, 1}, Exports))
      end},
     {"update_node/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({update_node, 2}, Exports))
      end},
     {"get_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({get_node, 1}, Exports))
      end},
     {"list_nodes/0 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({list_nodes, 0}, Exports))
      end},
     {"heartbeat/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({heartbeat, 1}, Exports))
      end},
     {"get_available_nodes/0 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({get_available_nodes, 0}, Exports))
      end},
     {"drain_node/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({drain_node, 2}, Exports))
      end},
     {"undrain_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({undrain_node, 1}, Exports))
      end},
     {"allocate_resources/4 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({allocate_resources, 4}, Exports))
      end},
     {"release_resources/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({release_resources, 2}, Exports))
      end}
    ].
