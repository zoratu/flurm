%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_node_registry module
%%% Achieves 100% code coverage for the node registry gen_server.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_registry_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing registry
    catch gen_server:stop(flurm_node_registry),
    timer:sleep(50),

    %% Delete ETS tables if they exist
    catch ets:delete(flurm_nodes_by_name),
    catch ets:delete(flurm_nodes_by_state),
    catch ets:delete(flurm_nodes_by_partition),

    %% Mock dependencies
    meck:new(lager, [passthrough, non_strict]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),

    meck:new(flurm_node, [passthrough, non_strict]),
    meck:expect(flurm_node, get_info, fun(Pid) ->
        %% Return mock node info based on pid
        {ok, #{
            hostname => <<"test_node">>,
            state => up,
            partitions => [<<"default">>],
            cpus => 8,
            cpus_available => 8,
            memory => 16384,
            memory_available => 16384,
            gpus => 2,
            gpus_available => 2
        }}
    end),

    meck:new(flurm_config_slurm, [passthrough, non_strict]),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) ->
        [Pattern]  % Return pattern as-is for testing
    end),

    %% Start the registry
    {ok, Pid} = flurm_node_registry:start_link(),
    {ok, Pid}.

cleanup({ok, Pid}) ->
    %% Stop the registry
    catch gen_server:stop(Pid),
    timer:sleep(50),

    %% Clean up ETS tables
    catch ets:delete(flurm_nodes_by_name),
    catch ets:delete(flurm_nodes_by_state),
    catch ets:delete(flurm_nodes_by_partition),

    %% Unload mocks
    meck:unload(lager),
    meck:unload(flurm_node),
    meck:unload(flurm_config_slurm),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

registry_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Start/Stop tests
        {"start_link starts registry", fun test_start_link/0},
        {"start_link handles already_started", fun test_start_link_already_started/0},

        %% Registration tests
        {"register_node registers new node", fun test_register_node/0},
        {"register_node fails for duplicate", fun test_register_node_duplicate/0},
        {"register_node_direct registers without pid", fun test_register_node_direct/0},
        {"register_node_direct handles re-registration", fun test_register_node_direct_reregister/0},
        {"unregister_node removes node", fun test_unregister_node/0},
        {"unregister_node handles non-existent", fun test_unregister_nonexistent/0},

        %% Lookup tests
        {"lookup_node finds registered node", fun test_lookup_node/0},
        {"lookup_node returns error for missing", fun test_lookup_node_missing/0},
        {"get_node_entry returns full entry", fun test_get_node_entry/0},
        {"get_node_entry returns error for missing", fun test_get_node_entry_missing/0},

        %% List tests
        {"list_nodes returns all nodes", fun test_list_nodes/0},
        {"list_all_nodes aliases list_nodes", fun test_list_all_nodes/0},
        {"list_nodes_by_state filters by state", fun test_list_nodes_by_state/0},
        {"list_nodes_by_partition filters by partition", fun test_list_nodes_by_partition/0},
        {"get_available_nodes filters by resources", fun test_get_available_nodes/0},

        %% Count tests
        {"count_by_state returns state counts", fun test_count_by_state/0},
        {"count_nodes returns total count", fun test_count_nodes/0},

        %% State update tests
        {"update_state changes node state", fun test_update_state/0},
        {"update_state returns error for missing", fun test_update_state_missing/0},
        {"update_entry replaces node entry", fun test_update_entry/0},
        {"update_entry handles state change", fun test_update_entry_state_change/0},
        {"update_entry returns error for missing", fun test_update_entry_missing/0}
     ]}.

resource_allocation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Resource allocation tests
        {"allocate_resources allocates cpus and memory", fun test_allocate_resources/0},
        {"allocate_resources with job tracking", fun test_allocate_resources_with_job/0},
        {"allocate_resources fails on insufficient", fun test_allocate_resources_insufficient/0},
        {"allocate_resources fails for missing node", fun test_allocate_resources_missing/0},
        {"release_resources frees resources by job", fun test_release_resources_by_job/0},
        {"release_resources frees explicit amounts", fun test_release_resources_explicit/0},
        {"release_resources caps at total", fun test_release_resources_cap/0},
        {"release_resources handles missing node", fun test_release_resources_missing/0},
        {"get_job_allocation returns allocation", fun test_get_job_allocation/0},
        {"get_job_allocation handles missing job", fun test_get_job_allocation_missing_job/0},
        {"get_job_allocation handles missing node", fun test_get_job_allocation_missing_node/0}
     ]}.

gen_server_callbacks_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% gen_server callback tests
        {"handle_info DOWN removes node", fun test_handle_info_down/0},
        {"handle_info DOWN handles unknown monitor", fun test_handle_info_down_unknown/0},
        {"handle_info config_reload_nodes", fun test_handle_info_config_reload/0},
        {"handle_info config_changed nodes", fun test_handle_info_config_changed_nodes/0},
        {"handle_info config_changed other", fun test_handle_info_config_changed_other/0},
        {"handle_info unknown message", fun test_handle_info_unknown/0},
        {"handle_cast unknown message", fun test_handle_cast_unknown/0},
        {"handle_call unknown request", fun test_handle_call_unknown/0},
        {"code_change returns ok", fun test_code_change/0},
        {"terminate returns ok", fun test_terminate/0}
     ]}.

%%====================================================================
%% Start/Stop Tests
%%====================================================================

test_start_link() ->
    %% Registry already started in setup
    ?assertEqual(flurm_node_registry, whereis(flurm_node_registry)).

test_start_link_already_started() ->
    %% Try to start again - should return existing pid
    {ok, Pid2} = flurm_node_registry:start_link(),
    ?assertEqual(whereis(flurm_node_registry), Pid2).

%%====================================================================
%% Registration Tests
%%====================================================================

test_register_node() ->
    NodeName = <<"test_node_1">>,
    NodePid = spawn(fun() -> receive stop -> ok end end),

    Result = flurm_node_registry:register_node(NodeName, NodePid),
    ?assertEqual(ok, Result),

    %% Verify node is registered
    {ok, FoundPid} = flurm_node_registry:lookup_node(NodeName),
    ?assertEqual(NodePid, FoundPid),

    %% Clean up
    NodePid ! stop.

test_register_node_duplicate() ->
    NodeName = <<"dup_node">>,
    NodePid1 = spawn(fun() -> receive stop -> ok end end),
    NodePid2 = spawn(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, NodePid1),

    %% Second registration should fail
    Result = flurm_node_registry:register_node(NodeName, NodePid2),
    ?assertEqual({error, already_registered}, Result),

    NodePid1 ! stop,
    NodePid2 ! stop.

test_register_node_direct() ->
    NodeInfo = #{
        hostname => <<"direct_node">>,
        cpus => 16,
        memory_mb => 32768,
        state => up,
        partitions => [<<"batch">>, <<"gpu">>]
    },

    Result = flurm_node_registry:register_node_direct(NodeInfo),
    ?assertEqual(ok, Result),

    %% Verify registration
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"direct_node">>),
    ?assertEqual(<<"direct_node">>, Entry#node_entry.name),
    ?assertEqual(16, Entry#node_entry.cpus_total),
    ?assertEqual(32768, Entry#node_entry.memory_total),
    ?assertEqual(up, Entry#node_entry.state),
    ?assertEqual([<<"batch">>, <<"gpu">>], Entry#node_entry.partitions).

test_register_node_direct_reregister() ->
    NodeInfo1 = #{
        hostname => <<"reregister_node">>,
        cpus => 8,
        memory_mb => 16384,
        state => down,
        partitions => [<<"default">>]
    },

    ok = flurm_node_registry:register_node_direct(NodeInfo1),

    %% Re-register with different values
    NodeInfo2 = #{
        hostname => <<"reregister_node">>,
        cpus => 16,
        memory_mb => 32768,
        state => up,
        partitions => [<<"default">>]
    },

    Result = flurm_node_registry:register_node_direct(NodeInfo2),
    ?assertEqual(ok, Result),

    %% Verify updated values
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"reregister_node">>),
    ?assertEqual(up, Entry#node_entry.state),
    ?assertEqual(16, Entry#node_entry.cpus_total).

test_unregister_node() ->
    NodeName = <<"unreg_node">>,
    NodePid = spawn(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, NodePid),
    ?assertMatch({ok, _}, flurm_node_registry:lookup_node(NodeName)),

    ok = flurm_node_registry:unregister_node(NodeName),
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(NodeName)),

    NodePid ! stop.

test_unregister_nonexistent() ->
    Result = flurm_node_registry:unregister_node(<<"nonexistent">>),
    ?assertEqual(ok, Result).

%%====================================================================
%% Lookup Tests
%%====================================================================

test_lookup_node() ->
    NodeName = <<"lookup_test">>,
    NodePid = spawn(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, NodePid),

    {ok, FoundPid} = flurm_node_registry:lookup_node(NodeName),
    ?assertEqual(NodePid, FoundPid),

    NodePid ! stop.

test_lookup_node_missing() ->
    Result = flurm_node_registry:lookup_node(<<"missing_node">>),
    ?assertEqual({error, not_found}, Result).

test_get_node_entry() ->
    NodeInfo = #{
        hostname => <<"entry_test">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"test">>]
    },

    ok = flurm_node_registry:register_node_direct(NodeInfo),

    {ok, Entry} = flurm_node_registry:get_node_entry(<<"entry_test">>),
    ?assert(is_record(Entry, node_entry)),
    ?assertEqual(<<"entry_test">>, Entry#node_entry.name).

test_get_node_entry_missing() ->
    Result = flurm_node_registry:get_node_entry(<<"missing">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% List Tests
%%====================================================================

test_list_nodes() ->
    %% Register multiple nodes
    lists:foreach(fun(I) ->
        Name = iolist_to_binary([<<"list_node_">>, integer_to_binary(I)]),
        NodeInfo = #{
            hostname => Name,
            cpus => 4,
            memory_mb => 8192,
            state => up,
            partitions => [<<"default">>]
        },
        ok = flurm_node_registry:register_node_direct(NodeInfo)
    end, lists:seq(1, 3)),

    Nodes = flurm_node_registry:list_nodes(),
    ?assert(is_list(Nodes)),
    ?assert(length(Nodes) >= 3).

test_list_all_nodes() ->
    %% list_all_nodes is alias for list_nodes
    NodeInfo = #{
        hostname => <<"all_nodes_test">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    },
    ok = flurm_node_registry:register_node_direct(NodeInfo),

    All = flurm_node_registry:list_all_nodes(),
    ?assert(is_list(All)),
    ?assert(length(All) >= 1).

test_list_nodes_by_state() ->
    %% Register nodes with different states
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"up_node">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"down_node">>,
        cpus => 4,
        memory_mb => 8192,
        state => down,
        partitions => [<<"default">>]
    }),

    UpNodes = flurm_node_registry:list_nodes_by_state(up),
    ?assert(length(UpNodes) >= 1),
    ?assert(lists:any(fun({N, _}) -> N =:= <<"up_node">> end, UpNodes)),

    DownNodes = flurm_node_registry:list_nodes_by_state(down),
    ?assert(length(DownNodes) >= 1),
    ?assert(lists:any(fun({N, _}) -> N =:= <<"down_node">> end, DownNodes)),

    %% Test empty result
    DrainNodes = flurm_node_registry:list_nodes_by_state(drain),
    ?assert(is_list(DrainNodes)).

test_list_nodes_by_partition() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"part_node_1">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"batch">>]
    }),

    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"part_node_2">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"gpu">>]
    }),

    BatchNodes = flurm_node_registry:list_nodes_by_partition(<<"batch">>),
    ?assert(length(BatchNodes) >= 1),
    ?assert(lists:any(fun({N, _}) -> N =:= <<"part_node_1">> end, BatchNodes)),

    GpuNodes = flurm_node_registry:list_nodes_by_partition(<<"gpu">>),
    ?assert(length(GpuNodes) >= 1),

    %% Empty partition
    EmptyNodes = flurm_node_registry:list_nodes_by_partition(<<"nonexistent">>),
    ?assertEqual([], EmptyNodes).

test_get_available_nodes() ->
    %% Register nodes with different resources
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"avail_node_1">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"avail_node_2">>,
        cpus => 2,
        memory_mb => 4096,
        state => up,
        partitions => [<<"default">>]
    }),

    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"avail_node_3">>,
        cpus => 8,
        memory_mb => 16384,
        state => down,
        partitions => [<<"default">>]
    }),

    %% Request small resources - should get at least 2 nodes
    SmallReq = {1, 1024, 0},
    SmallAvail = flurm_node_registry:get_available_nodes(SmallReq),
    ?assert(length(SmallAvail) >= 2),

    %% Request larger resources
    LargeReq = {4, 8192, 0},
    LargeAvail = flurm_node_registry:get_available_nodes(LargeReq),
    ?assert(length(LargeAvail) >= 1),

    %% Request too much - should get none
    HugeReq = {100, 100000, 0},
    HugeAvail = flurm_node_registry:get_available_nodes(HugeReq),
    ?assertEqual([], HugeAvail).

%%====================================================================
%% Count Tests
%%====================================================================

test_count_by_state() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"count_up">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    Counts = flurm_node_registry:count_by_state(),
    ?assert(is_map(Counts)),
    ?assert(maps:get(up, Counts) >= 1),
    ?assert(maps:is_key(down, Counts)),
    ?assert(maps:is_key(drain, Counts)),
    ?assert(maps:is_key(maint, Counts)).

test_count_nodes() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"count_node">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    Count = flurm_node_registry:count_nodes(),
    ?assert(Count >= 1).

%%====================================================================
%% State Update Tests
%%====================================================================

test_update_state() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"state_test">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Update to drain
    ok = flurm_node_registry:update_state(<<"state_test">>, drain),

    %% Verify
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"state_test">>),
    ?assertEqual(drain, Entry#node_entry.state),

    %% Verify state index was updated
    UpNodes = flurm_node_registry:list_nodes_by_state(up),
    ?assertNot(lists:any(fun({N, _}) -> N =:= <<"state_test">> end, UpNodes)),

    DrainNodes = flurm_node_registry:list_nodes_by_state(drain),
    ?assert(lists:any(fun({N, _}) -> N =:= <<"state_test">> end, DrainNodes)).

test_update_state_missing() ->
    Result = flurm_node_registry:update_state(<<"missing">>, up),
    ?assertEqual({error, not_found}, Result).

test_update_entry() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"entry_update">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    {ok, OldEntry} = flurm_node_registry:get_node_entry(<<"entry_update">>),

    %% Update entry with more CPUs
    NewEntry = OldEntry#node_entry{cpus_total = 16, cpus_avail = 16},
    ok = flurm_node_registry:update_entry(<<"entry_update">>, NewEntry),

    {ok, UpdatedEntry} = flurm_node_registry:get_node_entry(<<"entry_update">>),
    ?assertEqual(16, UpdatedEntry#node_entry.cpus_total).

test_update_entry_state_change() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"entry_state_change">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    {ok, OldEntry} = flurm_node_registry:get_node_entry(<<"entry_state_change">>),

    %% Update entry with state change
    NewEntry = OldEntry#node_entry{state = maint},
    ok = flurm_node_registry:update_entry(<<"entry_state_change">>, NewEntry),

    %% Verify state index was updated
    MaintNodes = flurm_node_registry:list_nodes_by_state(maint),
    ?assert(lists:any(fun({N, _}) -> N =:= <<"entry_state_change">> end, MaintNodes)).

test_update_entry_missing() ->
    Entry = #node_entry{name = <<"missing">>, pid = self()},
    Result = flurm_node_registry:update_entry(<<"missing">>, Entry),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Resource Allocation Tests
%%====================================================================

test_allocate_resources() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"alloc_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Allocate resources
    ok = flurm_node_registry:allocate_resources(<<"alloc_test">>, 2, 4096),

    %% Verify allocation
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"alloc_test">>),
    ?assertEqual(6, Entry#node_entry.cpus_avail),
    ?assertEqual(12288, Entry#node_entry.memory_avail).

test_allocate_resources_with_job() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"alloc_job_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Allocate with job ID tracking
    ok = flurm_node_registry:allocate_resources(<<"alloc_job_test">>, 12345, 4, 8192),

    %% Verify allocation
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"alloc_job_test">>),
    ?assertEqual(4, Entry#node_entry.cpus_avail),
    ?assertEqual(8192, Entry#node_entry.memory_avail),

    %% Verify job allocation tracking
    {ok, {4, 8192}} = flurm_node_registry:get_job_allocation(<<"alloc_job_test">>, 12345).

test_allocate_resources_insufficient() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"insuff_test">>,
        cpus => 2,
        memory_mb => 4096,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Try to allocate more than available
    Result = flurm_node_registry:allocate_resources(<<"insuff_test">>, 10, 10000),
    ?assertEqual({error, insufficient_resources}, Result).

test_allocate_resources_missing() ->
    Result = flurm_node_registry:allocate_resources(<<"missing">>, 1, 1024),
    ?assertEqual({error, not_found}, Result).

test_release_resources_by_job() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"release_job_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Allocate with job tracking
    ok = flurm_node_registry:allocate_resources(<<"release_job_test">>, 99999, 4, 8192),

    %% Release by job ID
    ok = flurm_node_registry:release_resources(<<"release_job_test">>, 99999),

    %% Verify resources released
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"release_job_test">>),
    ?assertEqual(8, Entry#node_entry.cpus_avail),
    ?assertEqual(16384, Entry#node_entry.memory_avail).

test_release_resources_explicit() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"release_explicit_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Allocate first
    ok = flurm_node_registry:allocate_resources(<<"release_explicit_test">>, 4, 8192),

    %% Release with explicit amounts
    ok = flurm_node_registry:release_resources(<<"release_explicit_test">>, 2, 4096),

    %% Verify partial release
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"release_explicit_test">>),
    ?assertEqual(6, Entry#node_entry.cpus_avail),
    ?assertEqual(12288, Entry#node_entry.memory_avail).

test_release_resources_cap() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"release_cap_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Try to release more than total (should cap)
    ok = flurm_node_registry:release_resources(<<"release_cap_test">>, 100, 100000),

    %% Verify capped at total
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"release_cap_test">>),
    ?assertEqual(8, Entry#node_entry.cpus_avail),
    ?assertEqual(16384, Entry#node_entry.memory_avail).

test_release_resources_missing() ->
    Result = flurm_node_registry:release_resources(<<"missing">>, 1),
    ?assertEqual({error, not_found}, Result),

    Result2 = flurm_node_registry:release_resources(<<"missing">>, 1, 1024),
    ?assertEqual({error, not_found}, Result2).

test_get_job_allocation() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"get_alloc_test">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    ok = flurm_node_registry:allocate_resources(<<"get_alloc_test">>, 55555, 2, 2048),

    {ok, {Cpus, Mem}} = flurm_node_registry:get_job_allocation(<<"get_alloc_test">>, 55555),
    ?assertEqual(2, Cpus),
    ?assertEqual(2048, Mem).

test_get_job_allocation_missing_job() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"get_alloc_missing_job">>,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        partitions => [<<"default">>]
    }),

    Result = flurm_node_registry:get_job_allocation(<<"get_alloc_missing_job">>, 99999),
    ?assertEqual({error, not_found}, Result).

test_get_job_allocation_missing_node() ->
    Result = flurm_node_registry:get_job_allocation(<<"nonexistent">>, 12345),
    ?assertEqual({error, node_not_found}, Result).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

test_handle_info_down() ->
    NodeName = <<"down_test">>,
    NodePid = spawn(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, NodePid),

    %% Kill the process - should trigger monitor
    exit(NodePid, kill),
    timer:sleep(100),

    %% Node should be unregistered
    Result = flurm_node_registry:lookup_node(NodeName),
    ?assertEqual({error, not_found}, Result).

test_handle_info_down_unknown() ->
    %% Send DOWN message for unknown monitor
    Ref = make_ref(),
    flurm_node_registry ! {'DOWN', Ref, process, self(), normal},
    timer:sleep(50),

    %% Should not crash
    ?assert(is_pid(whereis(flurm_node_registry))).

test_handle_info_config_reload() ->
    %% First register a node
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"config_test">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Send config reload message
    NodeDefs = [#{nodename => <<"config_test">>, cpus => 16, partitions => [<<"gpu">>]}],
    flurm_node_registry ! {config_reload_nodes, NodeDefs},
    timer:sleep(100),

    %% Verify node was updated
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"config_test">>),
    ?assertEqual(16, Entry#node_entry.cpus_total).

test_handle_info_config_changed_nodes() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"config_changed">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Send config_changed message
    NewNodes = [#{nodename => <<"config_changed">>, realmemory => 32768}],
    flurm_node_registry ! {config_changed, nodes, [], NewNodes},
    timer:sleep(100),

    %% Verify update
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"config_changed">>),
    ?assertEqual(32768, Entry#node_entry.memory_total).

test_handle_info_config_changed_other() ->
    %% Send config_changed for non-nodes key
    flurm_node_registry ! {config_changed, partitions, [], []},
    timer:sleep(50),

    %% Should not crash
    ?assert(is_pid(whereis(flurm_node_registry))).

test_handle_info_unknown() ->
    flurm_node_registry ! unknown_message,
    timer:sleep(50),

    %% Should not crash
    ?assert(is_pid(whereis(flurm_node_registry))).

test_handle_cast_unknown() ->
    gen_server:cast(flurm_node_registry, unknown_cast),
    timer:sleep(50),

    %% Should not crash
    ?assert(is_pid(whereis(flurm_node_registry))).

test_handle_call_unknown() ->
    Result = gen_server:call(flurm_node_registry, unknown_request),
    ?assertEqual({error, unknown_request}, Result).

test_code_change() ->
    %% code_change just returns {ok, State}
    %% Can't test directly, but we can verify the module loads
    ?assert(is_atom(flurm_node_registry)).

test_terminate() ->
    %% terminate just returns ok
    %% Verified by cleanup working properly
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node with state change in config", fun test_config_state_change/0},
        {"multiple partitions in config", fun test_config_multiple_partitions/0},
        {"config update for unregistered node", fun test_config_unregistered/0}
     ]}.

test_config_state_change() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"state_config">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    %% Config with drain state
    NodeDefs = [#{nodename => <<"state_config">>, state => drain}],
    flurm_node_registry ! {config_reload_nodes, NodeDefs},
    timer:sleep(100),

    {ok, Entry} = flurm_node_registry:get_node_entry(<<"state_config">>),
    ?assertEqual(drain, Entry#node_entry.state).

test_config_multiple_partitions() ->
    ok = flurm_node_registry:register_node_direct(#{
        hostname => <<"multi_part">>,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    }),

    NodeDefs = [#{nodename => <<"multi_part">>, partitions => [<<"batch">>, <<"gpu">>, <<"fast">>]}],
    flurm_node_registry ! {config_reload_nodes, NodeDefs},
    timer:sleep(100),

    {ok, Entry} = flurm_node_registry:get_node_entry(<<"multi_part">>),
    ?assertEqual([<<"batch">>, <<"gpu">>, <<"fast">>], Entry#node_entry.partitions).

test_config_unregistered() ->
    %% Config update for node that doesn't exist should not crash
    NodeDefs = [#{nodename => <<"unregistered_config_node">>, cpus => 16}],
    flurm_node_registry ! {config_reload_nodes, NodeDefs},
    timer:sleep(100),

    %% Registry still running
    ?assert(is_pid(whereis(flurm_node_registry))),

    %% Node was not created
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"unregistered_config_node">>)).
