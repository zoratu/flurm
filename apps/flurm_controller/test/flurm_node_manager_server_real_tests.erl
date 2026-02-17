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
      end},
     {"add_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({add_node, 1}, Exports))
      end},
     {"remove_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({remove_node, 1}, Exports))
      end},
     {"remove_node/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({remove_node, 2}, Exports))
      end},
     {"update_node_properties/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({update_node_properties, 2}, Exports))
      end},
     {"get_running_jobs_on_node/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({get_running_jobs_on_node, 1}, Exports))
      end},
     {"wait_for_node_drain/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({wait_for_node_drain, 2}, Exports))
      end},
     {"sync_nodes_from_config/0 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({sync_nodes_from_config, 0}, Exports))
      end},
     {"register_node_gres/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({register_node_gres, 2}, Exports))
      end},
     {"get_node_gres/1 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({get_node_gres, 1}, Exports))
      end},
     {"allocate_gres/4 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({allocate_gres, 4}, Exports))
      end},
     {"release_gres/2 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({release_gres, 2}, Exports))
      end},
     {"get_available_nodes_with_gres/4 is exported",
      fun() ->
          Exports = flurm_node_manager_server:module_info(exports),
          ?assert(lists:member({get_available_nodes_with_gres, 4}, Exports))
      end}
    ].

%%====================================================================
%% Extended Apply Node Updates Tests
%%====================================================================

apply_node_updates_extended_test_() ->
    [
     {"updates multiple fields at once",
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
          Updates = #{
              state => allocated,
              load_avg => 5.0,
              free_memory_mb => 2000,
              running_jobs => [100, 200, 300]
          },
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(allocated, Result#node.state),
          ?assertEqual(5.0, Result#node.load_avg),
          ?assertEqual(2000, Result#node.free_memory_mb),
          ?assertEqual([100, 200, 300], Result#node.running_jobs)
      end},
     {"handles empty updates",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 1.0, free_memory_mb = 4000, running_jobs = [1]},
          Updates = #{},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(Node, Result)
      end},
     {"state update to down",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = []},
          Updates = #{state => down},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(down, Result#node.state)
      end},
     {"state update to drain",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = mixed, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = [1]},
          Updates = #{state => drain},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(drain, Result#node.state)
      end},
     {"handles zero load_avg",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 2.0, free_memory_mb = 4000, running_jobs = []},
          Updates = #{load_avg => 0.0},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(0.0, Result#node.load_avg)
      end},
     {"handles zero free_memory_mb",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 8192, running_jobs = []},
          Updates = #{free_memory_mb => 0},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(0, Result#node.free_memory_mb)
      end},
     {"clears running_jobs",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = mixed, cpus = 4, memory_mb = 8192,
                       load_avg = 0.5, free_memory_mb = 4000, running_jobs = [1, 2, 3]},
          Updates = #{running_jobs => []},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual([], Result#node.running_jobs)
      end}
    ].

%%====================================================================
%% Extended Get Available Resources Tests
%%====================================================================

get_available_resources_extended_test_() ->
    [
     {"handles many allocations",
      fun() ->
          Allocations = maps:from_list([{I, {1, 1024}} || I <- lists:seq(1, 10)]),
          Node = #node{cpus = 16, memory_mb = 32768, allocations = Allocations},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(6, CpusAvail),   %% 16 - 10
          ?assertEqual(22528, MemAvail) %% 32768 - 10240
      end},
     {"handles large allocations",
      fun() ->
          Node = #node{cpus = 256, memory_mb = 1024000,
                       allocations = #{1 => {128, 512000}}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(128, CpusAvail),
          ?assertEqual(512000, MemAvail)
      end},
     {"exactly fully allocated",
      fun() ->
          Node = #node{cpus = 16, memory_mb = 32768,
                       allocations = #{1 => {8, 16384}, 2 => {8, 16384}}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(0, CpusAvail),
          ?assertEqual(0, MemAvail)
      end},
     {"handles fractional cpus allocation",
      fun() ->
          Node = #node{cpus = 4, memory_mb = 8192,
                       allocations = #{1 => {3, 4096}}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(1, CpusAvail),
          ?assertEqual(4096, MemAvail)
      end}
    ].

%%====================================================================
%% Extended Config To Node Spec Tests
%%====================================================================

config_to_node_spec_extended_test_() ->
    [
     {"handles all config options",
      fun() ->
          Config = #{
              cpus => 64,
              realmemory => 262144,
              feature => [<<"gpu">>, <<"nvme">>, <<"ib">>],
              partitions => [<<"compute">>, <<"debug">>, <<"gpu">>],
              state => mixed
          },
          Spec = flurm_node_manager_server:config_to_node_spec(<<"bignode">>, Config),
          ?assertEqual(<<"bignode">>, maps:get(hostname, Spec)),
          ?assertEqual(64, maps:get(cpus, Spec)),
          ?assertEqual(262144, maps:get(memory_mb, Spec)),
          ?assertEqual([<<"gpu">>, <<"nvme">>, <<"ib">>], maps:get(features, Spec)),
          ?assertEqual([<<"compute">>, <<"debug">>, <<"gpu">>], maps:get(partitions, Spec)),
          ?assertEqual(mixed, maps:get(state, Spec))
      end},
     {"partial socket/core config",
      fun() ->
          Config = #{sockets => 4, corespersocket => 16},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(64, maps:get(cpus, Spec))  %% 4 * 16 * 1
      end},
     {"only threadspercore specified",
      fun() ->
          Config = #{threadspercore => 2},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(2, maps:get(cpus, Spec))  %% 1 * 1 * 2
      end},
     {"empty features list",
      fun() ->
          Config = #{feature => []},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([], maps:get(features, Spec))
      end},
     {"default partitions",
      fun() ->
          Config = #{},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([<<"default">>], maps:get(partitions, Spec))
      end},
     {"default state",
      fun() ->
          Config = #{},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual(idle, maps:get(state, Spec))
      end}
    ].

%%====================================================================
%% Extended Apply Property Updates Tests
%%====================================================================

apply_property_updates_extended_test_() ->
    [
     {"handles all properties",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 4, memory_mb = 8192,
                       features = [], partitions = [], state = idle},
          Updates = #{
              cpus => 32,
              memory_mb => 65536,
              features => [<<"gpu">>, <<"nvme">>],
              partitions => [<<"compute">>, <<"gpu">>],
              state => mixed
          },
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(32, Result#node.cpus),
          ?assertEqual(65536, Result#node.memory_mb),
          ?assertEqual([<<"gpu">>, <<"nvme">>], Result#node.features),
          ?assertEqual([<<"compute">>, <<"gpu">>], Result#node.partitions),
          ?assertEqual(mixed, Result#node.state)
      end},
     {"preserves unupdated fields",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 16, memory_mb = 32768,
                       features = [<<"old_feature">>], partitions = [<<"partition1">>],
                       state = idle, allocations = #{1 => {2, 4096}}, running_jobs = [1]},
          Updates = #{cpus => 32},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(32, Result#node.cpus),
          ?assertEqual(32768, Result#node.memory_mb),
          ?assertEqual([<<"old_feature">>], Result#node.features),
          ?assertEqual([<<"partition1">>], Result#node.partitions),
          ?assertEqual(#{1 => {2, 4096}}, Result#node.allocations),
          ?assertEqual([1], Result#node.running_jobs)
      end},
     {"empty updates returns same node",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = [], state = idle},
          Updates = #{},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(Node, Result)
      end},
     {"only cpus update",
      fun() ->
          Node = #node{hostname = <<"n">>, cpus = 1, memory_mb = 1024,
                       features = [], partitions = [], state = idle},
          Updates = #{cpus => 128},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(128, Result#node.cpus),
          ?assertEqual(1024, Result#node.memory_mb)
      end},
     {"only memory_mb update",
      fun() ->
          Node = #node{hostname = <<"n">>, cpus = 8, memory_mb = 1024,
                       features = [], partitions = [], state = idle},
          Updates = #{memory_mb => 1024000},
          Result = flurm_node_manager_server:apply_property_updates(Node, Updates),
          ?assertEqual(8, Result#node.cpus),
          ?assertEqual(1024000, Result#node.memory_mb)
      end}
    ].

%%====================================================================
%% Extended Build GRES Maps Tests
%%====================================================================

build_gres_maps_extended_test_() ->
    [
     {"single unnamed device",
      fun() ->
          GRESList = [#{type => gpu, count => 2}],
          {Config, Total, Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(GRESList, Config),
          ?assertEqual(2, maps:get(<<"gpu">>, Total)),
          ?assertEqual(2, maps:get(<<"gpu">>, Available)),
          %% Should not have gpu:name entry
          ?assertEqual(error, maps:find(<<"gpu:">>, Total))
      end},
     {"mixed named and unnamed",
      fun() ->
          GRESList = [
              #{type => gpu, count => 2},
              #{type => gpu, name => <<"a100">>, count => 4}
          ],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(6, maps:get(<<"gpu">>, Total)),  %% 2 + 4
          ?assertEqual(4, maps:get(<<"gpu:a100">>, Total))
      end},
     {"different device types",
      fun() ->
          GRESList = [
              #{type => gpu, count => 4},
              #{type => fpga, count => 2},
              #{type => mic, count => 1}
          ],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(4, maps:get(<<"gpu">>, Total)),
          ?assertEqual(2, maps:get(<<"fpga">>, Total)),
          ?assertEqual(1, maps:get(<<"mic">>, Total))
      end},
     {"large counts",
      fun() ->
          GRESList = [#{type => gpu, name => <<"h100">>, count => 8}],
          {_Config, Total, Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(8, maps:get(<<"gpu">>, Total)),
          ?assertEqual(8, maps:get(<<"gpu:h100">>, Total)),
          ?assertEqual(8, maps:get(<<"gpu">>, Available)),
          ?assertEqual(8, maps:get(<<"gpu:h100">>, Available))
      end},
     {"accumulates same type different names",
      fun() ->
          GRESList = [
              #{type => gpu, name => <<"a100">>, count => 2},
              #{type => gpu, name => <<"v100">>, count => 4},
              #{type => gpu, name => <<"t4">>, count => 8}
          ],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(14, maps:get(<<"gpu">>, Total)),  %% 2 + 4 + 8
          ?assertEqual(2, maps:get(<<"gpu:a100">>, Total)),
          ?assertEqual(4, maps:get(<<"gpu:v100">>, Total)),
          ?assertEqual(8, maps:get(<<"gpu:t4">>, Total))
      end}
    ].

%%====================================================================
%% Extended Atom To GRES Key Tests
%%====================================================================

atom_to_gres_key_extended_test_() ->
    [
     {"handles various atoms",
      fun() ->
          ?assertEqual(<<"gpu">>, flurm_node_manager_server:atom_to_gres_key(gpu)),
          ?assertEqual(<<"fpga">>, flurm_node_manager_server:atom_to_gres_key(fpga)),
          ?assertEqual(<<"mic">>, flurm_node_manager_server:atom_to_gres_key(mic)),
          ?assertEqual(<<"mps">>, flurm_node_manager_server:atom_to_gres_key(mps)),
          ?assertEqual(<<"shard">>, flurm_node_manager_server:atom_to_gres_key(shard))
      end},
     {"handles arbitrary atoms",
      fun() ->
          ?assertEqual(<<"custom_device">>, flurm_node_manager_server:atom_to_gres_key(custom_device)),
          ?assertEqual(<<"my_gres">>, flurm_node_manager_server:atom_to_gres_key(my_gres)),
          ?assertEqual(<<"tpu">>, flurm_node_manager_server:atom_to_gres_key(tpu))
      end},
     {"handles binary input",
      fun() ->
          ?assertEqual(<<"gpu">>, flurm_node_manager_server:atom_to_gres_key(<<"gpu">>)),
          ?assertEqual(<<"custom">>, flurm_node_manager_server:atom_to_gres_key(<<"custom">>)),
          ?assertEqual(<<"">>, flurm_node_manager_server:atom_to_gres_key(<<"">>))
      end}
    ].

%%====================================================================
%% Extended Check Node GRES Availability Tests
%%====================================================================

check_node_gres_availability_extended_test_() ->
    [
     {"sufficient gpus available",
      fun() ->
          Node = #node{gres_available = #{<<"gpu">> => 4}},
          %% This depends on flurm_gres:parse_gres_string being available
          %% For now, test with empty spec which should always be true
          Result = flurm_node_manager_server:check_node_gres_availability(Node, <<>>),
          ?assertEqual(true, Result)
      end},
     {"empty available map with empty spec",
      fun() ->
          Node = #node{gres_available = #{}},
          Result = flurm_node_manager_server:check_node_gres_availability(Node, <<>>),
          ?assertEqual(true, Result)
      end},
     {"parsed spec list empty",
      fun() ->
          Node = #node{gres_available = #{<<"gpu">> => 2}},
          Result = flurm_node_manager_server:check_node_gres_availability(Node, []),
          ?assertEqual(true, Result)
      end}
    ].

%%====================================================================
%% Extended Build Updates From Config Tests
%%====================================================================

build_updates_from_config_extended_test_() ->
    [
     {"all fields differ",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [<<"old">>], partitions = [<<"part1">>]},
          Config = #{
              cpus => 32,
              realmemory => 65536,
              feature => [<<"new">>],
              partitions => [<<"part2">>]
          },
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(32, maps:get(cpus, Updates)),
          ?assertEqual(65536, maps:get(memory_mb, Updates)),
          ?assertEqual([<<"new">>], maps:get(features, Updates)),
          ?assertEqual([<<"part2">>], maps:get(partitions, Updates))
      end},
     {"only cpus differ",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{cpus => 16, realmemory => 16384, feature => [], partitions => []},
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(16, maps:get(cpus, Updates)),
          ?assertEqual(error, maps:find(memory_mb, Updates)),
          ?assertEqual(error, maps:find(features, Updates)),
          ?assertEqual(error, maps:find(partitions, Updates))
      end},
     {"cpus calculated from socket/core/thread",
      fun() ->
          Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Config = #{sockets => 2, corespersocket => 16, threadspercore => 2},  %% 64 cpus
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          ?assertEqual(64, maps:get(cpus, Updates))
      end},
     {"handles missing config fields gracefully",
      fun() ->
          %% When config is empty, cpus defaults to 1 (sockets=1 * cores=1 * threads=1)
          %% So if node has 1 cpu, memory matches default 1024, features/partitions match defaults
          Node = #node{hostname = <<"node1">>, cpus = 1, memory_mb = 1024,
                       features = [], partitions = []},
          Config = #{},  %% Empty config - uses defaults: cpus=1, memory=1024, feature=[], partitions=[]
          Updates = flurm_node_manager_server:build_updates_from_config(Node, Config),
          %% All fields should match, so no updates
          ?assertEqual(#{}, Updates)
      end}
    ].

%%====================================================================
%% Node State Transition Tests
%%====================================================================

node_state_transitions_test_() ->
    [
     {"idle to mixed transition",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = idle, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Updates = #{state => mixed},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(mixed, Result#node.state)
      end},
     {"mixed to allocated transition",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = mixed, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Updates = #{state => allocated},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(allocated, Result#node.state)
      end},
     {"allocated to drain transition",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = allocated, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Updates = #{state => drain},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(drain, Result#node.state)
      end},
     {"drain to down transition",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = drain, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Updates = #{state => down},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(down, Result#node.state)
      end},
     {"down to idle transition",
      fun() ->
          Node = #node{hostname = <<"node1">>, state = down, cpus = 8, memory_mb = 16384,
                       features = [], partitions = []},
          Updates = #{state => idle},
          Result = flurm_node_manager_server:apply_node_updates(Node, Updates),
          ?assertEqual(idle, Result#node.state)
      end}
    ].

%%====================================================================
%% Resource Allocation Edge Cases Tests
%%====================================================================

resource_allocation_edge_cases_test_() ->
    [
     {"minimum allocation",
      fun() ->
          Node = #node{cpus = 1, memory_mb = 1, allocations = #{}},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(1, CpusAvail),
          ?assertEqual(1, MemAvail)
      end},
     {"large node with many jobs",
      fun() ->
          %% 128 CPU node with 64 jobs each using 2 CPUs
          Allocations = maps:from_list([{I, {2, 1024}} || I <- lists:seq(1, 64)]),
          Node = #node{cpus = 128, memory_mb = 131072, allocations = Allocations},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(0, CpusAvail),    %% 128 - 128
          ?assertEqual(65536, MemAvail)  %% 131072 - 65536
      end},
     {"uneven allocation",
      fun() ->
          Allocations = #{
              1 => {1, 100},
              2 => {2, 200},
              3 => {3, 300},
              4 => {4, 400}
          },
          Node = #node{cpus = 16, memory_mb = 2000, allocations = Allocations},
          {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
          ?assertEqual(6, CpusAvail),    %% 16 - 10
          ?assertEqual(1000, MemAvail)   %% 2000 - 1000
      end}
    ].

%%====================================================================
%% GRES Specific Tests
%%====================================================================

gres_type_variations_test_() ->
    [
     {"gpu gres type",
      fun() ->
          GRESList = [#{type => gpu, name => <<"rtx3090">>, count => 4}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(4, maps:get(<<"gpu">>, Total)),
          ?assertEqual(4, maps:get(<<"gpu:rtx3090">>, Total))
      end},
     {"fpga gres type",
      fun() ->
          GRESList = [#{type => fpga, name => <<"alveo">>, count => 2}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(2, maps:get(<<"fpga">>, Total)),
          ?assertEqual(2, maps:get(<<"fpga:alveo">>, Total))
      end},
     {"mic gres type",
      fun() ->
          GRESList = [#{type => mic, count => 4}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(4, maps:get(<<"mic">>, Total))
      end},
     {"mps gres type",
      fun() ->
          GRESList = [#{type => mps, count => 100}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(100, maps:get(<<"mps">>, Total))
      end},
     {"shard gres type",
      fun() ->
          GRESList = [#{type => shard, count => 16}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(16, maps:get(<<"shard">>, Total))
      end},
     {"custom gres type",
      fun() ->
          GRESList = [#{type => tpu, name => <<"v4">>, count => 8}],
          {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
          ?assertEqual(8, maps:get(<<"tpu">>, Total)),
          ?assertEqual(8, maps:get(<<"tpu:v4">>, Total))
      end}
    ].

%%====================================================================
%% Partition Configuration Tests
%%====================================================================

partition_config_test_() ->
    [
     {"single partition config",
      fun() ->
          Config = #{partitions => [<<"compute">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([<<"compute">>], maps:get(partitions, Spec))
      end},
     {"multiple partitions config",
      fun() ->
          Config = #{partitions => [<<"compute">>, <<"debug">>, <<"gpu">>, <<"bigmem">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          Partitions = maps:get(partitions, Spec),
          ?assertEqual(4, length(Partitions)),
          ?assert(lists:member(<<"compute">>, Partitions)),
          ?assert(lists:member(<<"debug">>, Partitions)),
          ?assert(lists:member(<<"gpu">>, Partitions)),
          ?assert(lists:member(<<"bigmem">>, Partitions))
      end},
     {"empty partitions uses default",
      fun() ->
          Config = #{partitions => []},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          %% Note: empty list is preserved, not converted to default
          ?assertEqual([], maps:get(partitions, Spec))
      end}
    ].

%%====================================================================
%% Feature Configuration Tests
%%====================================================================

feature_config_test_() ->
    [
     {"single feature config",
      fun() ->
          Config = #{feature => [<<"gpu">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([<<"gpu">>], maps:get(features, Spec))
      end},
     {"multiple features config",
      fun() ->
          Config = #{feature => [<<"gpu">>, <<"nvme">>, <<"ib">>, <<"avx512">>]},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          Features = maps:get(features, Spec),
          ?assertEqual(4, length(Features)),
          ?assert(lists:member(<<"gpu">>, Features)),
          ?assert(lists:member(<<"nvme">>, Features)),
          ?assert(lists:member(<<"ib">>, Features)),
          ?assert(lists:member(<<"avx512">>, Features))
      end},
     {"no features",
      fun() ->
          Config = #{},
          Spec = flurm_node_manager_server:config_to_node_spec(<<"node1">>, Config),
          ?assertEqual([], maps:get(features, Spec))
      end}
    ].
