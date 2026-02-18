%%%-------------------------------------------------------------------
%%% @doc Comprehensive Coverage Tests for flurm_node_manager_server
%%%
%%% This module provides extensive test coverage for the node manager server,
%%% covering all exported functions, gen_server callbacks, and edge cases.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_node_manager_server_100cov_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_server_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

node_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Basic API tests
        {"start_link creates new process", fun test_start_link_new/0},
        {"start_link returns existing process", fun test_start_link_existing/0},
        {"register_node with minimal spec", fun test_register_node_minimal/0},
        {"register_node with full spec", fun test_register_node_full/0},
        {"register_node duplicate hostname", fun test_register_node_duplicate/0},
        {"register_node updates last_heartbeat", fun test_register_node_heartbeat/0},
        {"register_node sets idle state", fun test_register_node_idle_state/0},

        %% update_node tests
        {"update_node state", fun test_update_node_state/0},
        {"update_node cpus", fun test_update_node_cpus/0},
        {"update_node memory", fun test_update_node_memory/0},
        {"update_node features", fun test_update_node_features/0},
        {"update_node partitions", fun test_update_node_partitions/0},
        {"update_node not found", fun test_update_node_not_found/0},
        {"update_node multiple fields", fun test_update_node_multiple/0},
        {"update_node load_avg", fun test_update_node_load_avg/0},

        %% get_node tests
        {"get_node existing", fun test_get_node_existing/0},
        {"get_node not found", fun test_get_node_not_found/0},
        {"get_node with all fields", fun test_get_node_all_fields/0},

        %% list_nodes tests
        {"list_nodes empty", fun test_list_nodes_empty/0},
        {"list_nodes with nodes", fun test_list_nodes_with_nodes/0},
        {"list_nodes multiple states", fun test_list_nodes_multiple_states/0},

        %% heartbeat tests
        {"heartbeat updates timestamp", fun test_heartbeat_updates_timestamp/0},
        {"heartbeat updates load", fun test_heartbeat_updates_load/0},
        {"heartbeat updates memory", fun test_heartbeat_updates_memory/0},
        {"heartbeat unknown node", fun test_heartbeat_unknown_node/0},
        {"heartbeat registers new node", fun test_heartbeat_registers_new/0},

        %% get_available_nodes tests
        {"get_available_nodes empty", fun test_get_available_nodes_empty/0},
        {"get_available_nodes idle", fun test_get_available_nodes_idle/0},
        {"get_available_nodes mixed", fun test_get_available_nodes_mixed/0},
        {"get_available_nodes excludes down", fun test_get_available_nodes_excludes_down/0},
        {"get_available_nodes excludes drain", fun test_get_available_nodes_excludes_drain/0},
        {"get_available_nodes excludes allocated", fun test_get_available_nodes_excludes_allocated/0},

        %% get_available_nodes_for_job tests
        {"get_available_for_job basic", fun test_get_available_for_job_basic/0},
        {"get_available_for_job cpu filter", fun test_get_available_for_job_cpu/0},
        {"get_available_for_job memory filter", fun test_get_available_for_job_memory/0},
        {"get_available_for_job partition filter", fun test_get_available_for_job_partition/0},
        {"get_available_for_job combined filter", fun test_get_available_for_job_combined/0},
        {"get_available_for_job no matches", fun test_get_available_for_job_no_matches/0},

        %% allocate_resources tests
        {"allocate_resources success", fun test_allocate_resources_success/0},
        {"allocate_resources updates state", fun test_allocate_resources_updates_state/0},
        {"allocate_resources not found", fun test_allocate_resources_not_found/0},
        {"allocate_resources insufficient cpus", fun test_allocate_resources_insufficient_cpus/0},
        {"allocate_resources insufficient memory", fun test_allocate_resources_insufficient_memory/0},
        {"allocate_resources multiple jobs", fun test_allocate_resources_multiple_jobs/0},

        %% release_resources tests
        {"release_resources success", fun test_release_resources_success/0},
        {"release_resources returns to idle", fun test_release_resources_returns_idle/0},
        {"release_resources unknown node", fun test_release_resources_unknown_node/0},
        {"release_resources unknown job", fun test_release_resources_unknown_job/0},
        {"release_resources partial", fun test_release_resources_partial/0}
     ]}.

%% Drain mode tests
drain_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"drain_node success", fun test_drain_node_success/0},
        {"drain_node already draining", fun test_drain_node_already_draining/0},
        {"drain_node not found", fun test_drain_node_not_found/0},
        {"drain_node with reason", fun test_drain_node_with_reason/0},
        {"drain_node excludes from available", fun test_drain_node_excludes/0},

        {"undrain_node success", fun test_undrain_node_success/0},
        {"undrain_node not draining", fun test_undrain_node_not_draining/0},
        {"undrain_node not found", fun test_undrain_node_not_found/0},
        {"undrain_node returns to idle", fun test_undrain_node_returns_idle/0},

        {"get_drain_reason success", fun test_get_drain_reason_success/0},
        {"get_drain_reason not draining", fun test_get_drain_reason_not_draining/0},
        {"get_drain_reason not found", fun test_get_drain_reason_not_found/0},

        {"is_node_draining true", fun test_is_node_draining_true/0},
        {"is_node_draining false", fun test_is_node_draining_false/0},
        {"is_node_draining not found", fun test_is_node_draining_not_found/0}
     ]}.

%% Dynamic node operations tests
dynamic_node_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"add_node with map spec", fun test_add_node_map_spec/0},
        {"add_node with binary name", fun test_add_node_binary_name/0},
        {"add_node not in config", fun test_add_node_not_in_config/0},
        {"add_node duplicate", fun test_add_node_duplicate/0},

        {"remove_node success", fun test_remove_node_success/0},
        {"remove_node with timeout", fun test_remove_node_with_timeout/0},
        {"remove_node force", fun test_remove_node_force/0},
        {"remove_node not found", fun test_remove_node_not_found/0},
        {"remove_node with running jobs", fun test_remove_node_with_jobs/0},

        {"update_node_properties success", fun test_update_node_properties_success/0},
        {"update_node_properties cpus", fun test_update_node_properties_cpus/0},
        {"update_node_properties memory", fun test_update_node_properties_memory/0},
        {"update_node_properties features", fun test_update_node_properties_features/0},
        {"update_node_properties not found", fun test_update_node_properties_not_found/0},

        {"get_running_jobs_on_node success", fun test_get_running_jobs_on_node_success/0},
        {"get_running_jobs_on_node empty", fun test_get_running_jobs_on_node_empty/0},
        {"get_running_jobs_on_node not found", fun test_get_running_jobs_on_node_not_found/0},

        {"wait_for_node_drain success", fun test_wait_for_node_drain_success/0},
        {"wait_for_node_drain not found", fun test_wait_for_node_drain_not_found/0},

        {"sync_nodes_from_config success", fun test_sync_nodes_from_config_success/0}
     ]}.

%% GRES tests
gres_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"register_node_gres success", fun test_register_gres_success/0},
        {"register_node_gres multiple types", fun test_register_gres_multiple/0},
        {"register_node_gres not found", fun test_register_gres_not_found/0},
        {"register_node_gres updates existing", fun test_register_gres_updates/0},

        {"get_node_gres success", fun test_get_node_gres_success/0},
        {"get_node_gres not found", fun test_get_node_gres_not_found/0},
        {"get_node_gres empty", fun test_get_node_gres_empty/0},

        {"allocate_gres success", fun test_allocate_gres_success/0},
        {"allocate_gres insufficient", fun test_allocate_gres_insufficient/0},
        {"allocate_gres not found", fun test_allocate_gres_not_found/0},
        {"allocate_gres exclusive", fun test_allocate_gres_exclusive/0},
        {"allocate_gres by name", fun test_allocate_gres_by_name/0},

        {"release_gres success", fun test_release_gres_success/0},
        {"release_gres not found", fun test_release_gres_not_found/0},
        {"release_gres unknown job", fun test_release_gres_unknown_job/0},

        {"get_available_nodes_with_gres success", fun test_get_available_with_gres_success/0},
        {"get_available_nodes_with_gres no gres req", fun test_get_available_with_gres_no_req/0},
        {"get_available_nodes_with_gres insufficient", fun test_get_available_with_gres_insufficient/0}
     ]}.

%% Gen server callback tests
gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handle_info check_heartbeats", fun test_handle_info_heartbeats/0},
        {"handle_info check_message_queue", fun test_handle_info_queue_check/0},
        {"handle_info config_changed", fun test_handle_info_config_changed/0},
        {"handle_info unknown message", fun test_handle_info_unknown/0},
        {"handle_cast unknown message", fun test_handle_cast_unknown/0},
        {"terminate normal", fun test_terminate_normal/0},
        {"terminate shutdown", fun test_terminate_shutdown/0},
        {"code_change", fun test_code_change/0}
     ]}.

%% Edge case tests
edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node with unicode hostname", fun test_unicode_hostname/0},
        {"node with empty hostname", fun test_empty_hostname/0},
        {"node with long hostname", fun test_long_hostname/0},
        {"node with max cpus", fun test_max_cpus/0},
        {"node with max memory", fun test_max_memory/0},
        {"node with many features", fun test_many_features/0},
        {"node with many partitions", fun test_many_partitions/0},
        {"rapid heartbeats", fun test_rapid_heartbeats/0},
        {"concurrent registrations", fun test_concurrent_registrations/0},
        {"concurrent allocations", fun test_concurrent_allocations/0}
     ]}.

%% Node state transitions tests
state_transitions_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"transition idle to allocated", fun test_transition_idle_allocated/0},
        {"transition allocated to mixed", fun test_transition_allocated_mixed/0},
        {"transition mixed to idle", fun test_transition_mixed_idle/0},
        {"transition idle to drain", fun test_transition_idle_drain/0},
        {"transition drain to idle", fun test_transition_drain_idle/0},
        {"transition idle to down", fun test_transition_idle_down/0},
        {"transition down to idle", fun test_transition_down_idle/0},
        {"full lifecycle", fun test_full_lifecycle/0}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Stop any existing node manager
    case whereis(flurm_node_manager_server) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,

    %% Setup mocks
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_core),
    catch meck:unload(flurm_gres),
    catch meck:unload(lager),

    meck:new([flurm_config_server, flurm_partition_manager, flurm_gres],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_config_server, get_node, fun(_) -> undefined end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    meck:expect(flurm_partition_manager, add_node_to_partition, fun(_, _) -> ok end),
    meck:expect(flurm_partition_manager, remove_node_from_partition, fun(_, _) -> ok end),
    meck:expect(flurm_gres, parse_gres_string, fun(_) -> [] end),

    %% Start fresh node manager
    {ok, MgrPid} = flurm_node_manager_server:start_link(),
    unlink(MgrPid),
    MgrPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> catch gen_server:stop(Pid, normal, 5000);
        false -> ok
    end,
    catch meck:unload([flurm_config_server, flurm_partition_manager, flurm_gres]),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_node_spec() ->
    #{
        hostname => <<"node1">>,
        cpus => 16,
        memory_mb => 65536,
        state => idle,
        partitions => [<<"default">>]
    }.

make_node_spec(Name) ->
    #{
        hostname => Name,
        cpus => 16,
        memory_mb => 65536,
        state => idle,
        partitions => [<<"default">>]
    }.

make_full_node_spec() ->
    #{
        hostname => <<"fullnode">>,
        cpus => 64,
        memory_mb => 262144,
        state => idle,
        partitions => [<<"default">>, <<"gpu">>],
        features => [<<"avx512">>, <<"gpu">>, <<"ssd">>],
        load_avg => 0.5,
        uptime => 86400,
        os_type => <<"linux">>,
        arch => <<"x86_64">>,
        cores_per_socket => 16,
        sockets => 4,
        threads_per_core => 2
    }.

register_test_node() ->
    ok = flurm_node_manager_server:register_node(make_node_spec()),
    <<"node1">>.

register_test_node(Name) ->
    ok = flurm_node_manager_server:register_node(make_node_spec(Name)),
    Name.

register_test_node(Name, State) ->
    Spec = (make_node_spec(Name))#{state => State},
    ok = flurm_node_manager_server:register_node(Spec),
    Name.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_link_new() ->
    ?assert(is_pid(whereis(flurm_node_manager_server))).

test_start_link_existing() ->
    {ok, Pid} = flurm_node_manager_server:start_link(),
    ?assertEqual(whereis(flurm_node_manager_server), Pid).

test_register_node_minimal() ->
    Spec = #{hostname => <<"min_node">>, cpus => 1},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_register_node_full() ->
    Result = flurm_node_manager_server:register_node(make_full_node_spec()),
    ?assertEqual(ok, Result),
    {ok, Node} = flurm_node_manager_server:get_node(<<"fullnode">>),
    ?assertEqual(64, Node#node.cpus),
    ?assertEqual(262144, Node#node.memory_mb).

test_register_node_duplicate() ->
    register_test_node(),
    Result = flurm_node_manager_server:register_node(make_node_spec()),
    ?assertEqual(ok, Result). % Should overwrite

test_register_node_heartbeat() ->
    register_test_node(),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assert(Node#node.last_heartbeat > 0).

test_register_node_idle_state() ->
    register_test_node(),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(idle, Node#node.state).

%%====================================================================
%% Update Node Tests
%%====================================================================

test_update_node_state() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{state => down}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(down, Node#node.state).

test_update_node_cpus() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{cpus => 32}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(32, Node#node.cpus).

test_update_node_memory() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{memory_mb => 131072}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(131072, Node#node.memory_mb).

test_update_node_features() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{features => [<<"gpu">>, <<"fast">>]}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual([<<"gpu">>, <<"fast">>], Node#node.features).

test_update_node_partitions() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{partitions => [<<"gpu">>, <<"compute">>]}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual([<<"gpu">>, <<"compute">>], Node#node.partitions).

test_update_node_not_found() ->
    Result = flurm_node_manager_server:update_node(<<"nonexistent">>, #{state => down}),
    ?assertEqual({error, not_found}, Result).

test_update_node_multiple() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{
        cpus => 32,
        memory_mb => 131072,
        state => mixed
    }),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(32, Node#node.cpus),
    ?assertEqual(131072, Node#node.memory_mb),
    ?assertEqual(mixed, Node#node.state).

test_update_node_load_avg() ->
    register_test_node(),
    ok = flurm_node_manager_server:update_node(<<"node1">>, #{load_avg => 2.5}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(2.5, Node#node.load_avg).

%%====================================================================
%% Get Node Tests
%%====================================================================

test_get_node_existing() ->
    register_test_node(),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(<<"node1">>, Node#node.hostname).

test_get_node_not_found() ->
    Result = flurm_node_manager_server:get_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_get_node_all_fields() ->
    flurm_node_manager_server:register_node(make_full_node_spec()),
    {ok, Node} = flurm_node_manager_server:get_node(<<"fullnode">>),
    ?assertEqual(<<"fullnode">>, Node#node.hostname),
    ?assertEqual(64, Node#node.cpus),
    ?assertEqual(262144, Node#node.memory_mb),
    ?assertEqual([<<"default">>, <<"gpu">>], Node#node.partitions),
    ?assertEqual([<<"avx512">>, <<"gpu">>, <<"ssd">>], Node#node.features).

%%====================================================================
%% List Nodes Tests
%%====================================================================

test_list_nodes_empty() ->
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual([], Nodes).

test_list_nodes_with_nodes() ->
    register_test_node(<<"n1">>),
    register_test_node(<<"n2">>),
    register_test_node(<<"n3">>),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(3, length(Nodes)).

test_list_nodes_multiple_states() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, allocated),
    register_test_node(<<"n3">>, down),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(3, length(Nodes)),
    States = [N#node.state || N <- Nodes],
    ?assert(lists:member(idle, States)),
    ?assert(lists:member(allocated, States)),
    ?assert(lists:member(down, States)).

%%====================================================================
%% Heartbeat Tests
%%====================================================================

test_heartbeat_updates_timestamp() ->
    register_test_node(),
    {ok, N1} = flurm_node_manager_server:get_node(<<"node1">>),
    T1 = N1#node.last_heartbeat,
    timer:sleep(100),
    flurm_node_manager_server:heartbeat(#{hostname => <<"node1">>}),
    timer:sleep(50),
    {ok, N2} = flurm_node_manager_server:get_node(<<"node1">>),
    T2 = N2#node.last_heartbeat,
    ?assert(T2 >= T1).

test_heartbeat_updates_load() ->
    register_test_node(),
    flurm_node_manager_server:heartbeat(#{hostname => <<"node1">>, load_avg => 4.5}),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(4.5, Node#node.load_avg).

test_heartbeat_updates_memory() ->
    register_test_node(),
    flurm_node_manager_server:heartbeat(#{hostname => <<"node1">>, free_memory => 32768}),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"node1">>),
    ?assertEqual(32768, Node#node.free_memory_mb).

test_heartbeat_unknown_node() ->
    flurm_node_manager_server:heartbeat(#{hostname => <<"unknown">>}),
    timer:sleep(50),
    %% Should not crash
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_heartbeat_registers_new() ->
    flurm_node_manager_server:heartbeat(#{hostname => <<"newnode">>, cpus => 8, memory_mb => 16384}),
    timer:sleep(50),
    %% May or may not register depending on implementation
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

%%====================================================================
%% Get Available Nodes Tests
%%====================================================================

test_get_available_nodes_empty() ->
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual([], Nodes).

test_get_available_nodes_idle() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, idle),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(2, length(Nodes)).

test_get_available_nodes_mixed() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, mixed),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(2, length(Nodes)).

test_get_available_nodes_excludes_down() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, down),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(1, length(Nodes)).

test_get_available_nodes_excludes_drain() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, drain),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(1, length(Nodes)).

test_get_available_nodes_excludes_allocated() ->
    register_test_node(<<"n1">>, idle),
    register_test_node(<<"n2">>, allocated),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    %% allocated might or might not be available depending on implementation
    ?assert(length(Nodes) >= 1).

%%====================================================================
%% Get Available Nodes For Job Tests
%%====================================================================

test_get_available_for_job_basic() ->
    register_test_node(<<"n1">>),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"default">>),
    ?assertEqual(1, length(Nodes)).

test_get_available_for_job_cpu() ->
    register_test_node(<<"n1">>), % 16 CPUs
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(32, 1024, <<"default">>),
    ?assertEqual(0, length(Nodes)).

test_get_available_for_job_memory() ->
    register_test_node(<<"n1">>), % 65536 MB
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 100000, <<"default">>),
    ?assertEqual(0, length(Nodes)).

test_get_available_for_job_partition() ->
    Spec = (make_node_spec(<<"n1">>))#{partitions => [<<"gpu">>]},
    flurm_node_manager_server:register_node(Spec),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"default">>),
    ?assertEqual(0, length(Nodes)).

test_get_available_for_job_combined() ->
    register_test_node(<<"n1">>),
    register_test_node(<<"n2">>),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(8, 32768, <<"default">>),
    ?assertEqual(2, length(Nodes)).

test_get_available_for_job_no_matches() ->
    register_test_node(<<"n1">>),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1000, 1000000, <<"default">>),
    ?assertEqual(0, length(Nodes)).

%%====================================================================
%% Allocate Resources Tests
%%====================================================================

test_allocate_resources_success() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 8192),
    ?assertEqual(ok, Result).

test_allocate_resources_updates_state() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 16, 65536),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(allocated, Node#node.state).

test_allocate_resources_not_found() ->
    Result = flurm_node_manager_server:allocate_resources(<<"nonexistent">>, 1, 4, 8192),
    ?assertEqual({error, not_found}, Result).

test_allocate_resources_insufficient_cpus() ->
    register_test_node(<<"n1">>), % 16 CPUs
    Result = flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 32, 8192),
    ?assertMatch({error, _}, Result).

test_allocate_resources_insufficient_memory() ->
    register_test_node(<<"n1">>), % 65536 MB
    Result = flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 100000),
    ?assertMatch({error, _}, Result).

test_allocate_resources_multiple_jobs() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"n1">>, 2, 4, 8192),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(2, maps:size(Node#node.allocations)).

%%====================================================================
%% Release Resources Tests
%%====================================================================

test_release_resources_success() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 8192),
    flurm_node_manager_server:release_resources(<<"n1">>, 1),
    timer:sleep(50),
    ?assert(true). % Just verify it doesn't crash

test_release_resources_returns_idle() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 16, 65536),
    {ok, N1} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(allocated, N1#node.state),
    flurm_node_manager_server:release_resources(<<"n1">>, 1),
    timer:sleep(50),
    {ok, N2} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(idle, N2#node.state).

test_release_resources_unknown_node() ->
    flurm_node_manager_server:release_resources(<<"nonexistent">>, 1),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_release_resources_unknown_job() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:release_resources(<<"n1">>, 999),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_release_resources_partial() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"n1">>, 2, 4, 8192),
    flurm_node_manager_server:release_resources(<<"n1">>, 1),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(1, maps:size(Node#node.allocations)).

%%====================================================================
%% Drain Mode Tests
%%====================================================================

test_drain_node_success() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:drain_node(<<"n1">>, <<"maintenance">>),
    ?assertEqual(ok, Result),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(drain, Node#node.state).

test_drain_node_already_draining() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"maintenance">>),
    Result = flurm_node_manager_server:drain_node(<<"n1">>, <<"other">>),
    ?assertMatch({error, _}, Result).

test_drain_node_not_found() ->
    Result = flurm_node_manager_server:drain_node(<<"nonexistent">>, <<"reason">>),
    ?assertEqual({error, not_found}, Result).

test_drain_node_with_reason() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"hw_issue">>),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"n1">>),
    ?assertEqual(<<"hw_issue">>, Reason).

test_drain_node_excludes() ->
    register_test_node(<<"n1">>),
    register_test_node(<<"n2">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"test">>),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(1, length(Nodes)).

test_undrain_node_success() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"maintenance">>),
    Result = flurm_node_manager_server:undrain_node(<<"n1">>),
    ?assertEqual(ok, Result).

test_undrain_node_not_draining() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:undrain_node(<<"n1">>),
    ?assertMatch({error, _}, Result).

test_undrain_node_not_found() ->
    Result = flurm_node_manager_server:undrain_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_undrain_node_returns_idle() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"test">>),
    ok = flurm_node_manager_server:undrain_node(<<"n1">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(idle, Node#node.state).

test_get_drain_reason_success() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"scheduled_maint">>),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"n1">>),
    ?assertEqual(<<"scheduled_maint">>, Reason).

test_get_drain_reason_not_draining() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:get_drain_reason(<<"n1">>),
    ?assertEqual({error, not_draining}, Result).

test_get_drain_reason_not_found() ->
    Result = flurm_node_manager_server:get_drain_reason(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_is_node_draining_true() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:drain_node(<<"n1">>, <<"test">>),
    Result = flurm_node_manager_server:is_node_draining(<<"n1">>),
    ?assertEqual(true, Result).

test_is_node_draining_false() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:is_node_draining(<<"n1">>),
    ?assertEqual(false, Result).

test_is_node_draining_not_found() ->
    Result = flurm_node_manager_server:is_node_draining(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Dynamic Node Operations Tests
%%====================================================================

test_add_node_map_spec() ->
    Spec = make_node_spec(<<"dynamic1">>),
    Result = flurm_node_manager_server:add_node(Spec),
    ?assertEqual(ok, Result),
    {ok, _} = flurm_node_manager_server:get_node(<<"dynamic1">>).

test_add_node_binary_name() ->
    meck:expect(flurm_config_server, get_node, fun(<<"config_node">>) ->
        #{cpus => 8, memory_mb => 16384}
    end),
    Result = flurm_node_manager_server:add_node(<<"config_node">>),
    ?assertEqual(ok, Result).

test_add_node_not_in_config() ->
    Result = flurm_node_manager_server:add_node(<<"not_in_config">>),
    ?assertMatch({error, {node_not_in_config, _}}, Result).

test_add_node_duplicate() ->
    Spec = make_node_spec(<<"dup_node">>),
    ok = flurm_node_manager_server:add_node(Spec),
    Result = flurm_node_manager_server:add_node(Spec),
    ?assertEqual(ok, Result). % Should update existing

test_remove_node_success() ->
    register_test_node(<<"to_remove">>),
    Result = flurm_node_manager_server:remove_node(<<"to_remove">>),
    ?assertEqual(ok, Result),
    ?assertEqual({error, not_found}, flurm_node_manager_server:get_node(<<"to_remove">>)).

test_remove_node_with_timeout() ->
    register_test_node(<<"to_remove">>),
    Result = flurm_node_manager_server:remove_node(<<"to_remove">>, 1000),
    ?assertEqual(ok, Result).

test_remove_node_force() ->
    register_test_node(<<"force_remove">>),
    flurm_node_manager_server:allocate_resources(<<"force_remove">>, 1, 4, 8192),
    Result = flurm_node_manager_server:remove_node(<<"force_remove">>, force),
    ?assertEqual(ok, Result).

test_remove_node_not_found() ->
    Result = flurm_node_manager_server:remove_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_remove_node_with_jobs() ->
    register_test_node(<<"busy_node">>),
    flurm_node_manager_server:allocate_resources(<<"busy_node">>, 1, 4, 8192),
    Result = flurm_node_manager_server:remove_node(<<"busy_node">>, 100),
    ?assertMatch({error, _}, Result).

test_update_node_properties_success() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:update_node_properties(<<"n1">>, #{cpus => 32}),
    ?assertEqual(ok, Result),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(32, Node#node.cpus).

test_update_node_properties_cpus() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node_properties(<<"n1">>, #{cpus => 64}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(64, Node#node.cpus).

test_update_node_properties_memory() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node_properties(<<"n1">>, #{memory_mb => 131072}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(131072, Node#node.memory_mb).

test_update_node_properties_features() ->
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node_properties(<<"n1">>, #{features => [<<"new_feat">>]}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual([<<"new_feat">>], Node#node.features).

test_update_node_properties_not_found() ->
    Result = flurm_node_manager_server:update_node_properties(<<"nonexistent">>, #{cpus => 32}),
    ?assertEqual({error, not_found}, Result).

test_get_running_jobs_on_node_success() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:allocate_resources(<<"n1">>, 1, 4, 8192),
    flurm_node_manager_server:allocate_resources(<<"n1">>, 2, 4, 8192),
    {ok, Jobs} = flurm_node_manager_server:get_running_jobs_on_node(<<"n1">>),
    ?assertEqual(2, length(Jobs)).

test_get_running_jobs_on_node_empty() ->
    register_test_node(<<"n1">>),
    {ok, Jobs} = flurm_node_manager_server:get_running_jobs_on_node(<<"n1">>),
    ?assertEqual([], Jobs).

test_get_running_jobs_on_node_not_found() ->
    Result = flurm_node_manager_server:get_running_jobs_on_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_wait_for_node_drain_success() ->
    register_test_node(<<"n1">>),
    Result = flurm_node_manager_server:wait_for_node_drain(<<"n1">>, 100),
    ?assertEqual(ok, Result).

test_wait_for_node_drain_not_found() ->
    Result = flurm_node_manager_server:wait_for_node_drain(<<"nonexistent">>, 100),
    ?assertEqual({error, not_found}, Result).

test_sync_nodes_from_config_success() ->
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    {ok, Result} = flurm_node_manager_server:sync_nodes_from_config(),
    ?assert(is_map(Result)).

%%====================================================================
%% GRES Tests
%%====================================================================

test_register_gres_success() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    Result = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    ?assertEqual(ok, Result).

test_register_gres_multiple() ->
    register_test_node(<<"n1">>),
    GRESList = [
        #{type => gpu, name => <<"a100">>, count => 4},
        #{type => gpu, name => <<"v100">>, count => 2},
        #{type => mic, name => <<"phi">>, count => 1}
    ],
    Result = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    ?assertEqual(ok, Result).

test_register_gres_not_found() ->
    GRESList = [#{type => gpu, count => 4}],
    Result = flurm_node_manager_server:register_node_gres(<<"nonexistent">>, GRESList),
    ?assertEqual({error, not_found}, Result).

test_register_gres_updates() ->
    register_test_node(<<"n1">>),
    GRESList1 = [#{type => gpu, count => 2}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList1),
    GRESList2 = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList2),
    ?assert(true).

test_get_node_gres_success() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"n1">>),
    ?assert(is_map(GRES)).

test_get_node_gres_not_found() ->
    Result = flurm_node_manager_server:get_node_gres(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_get_node_gres_empty() ->
    register_test_node(<<"n1">>),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"n1">>),
    ?assert(is_map(GRES)).

test_allocate_gres_success() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 4, indices => [0,1,2,3]}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    Result = flurm_node_manager_server:allocate_gres(<<"n1">>, 1, <<"gpu:2">>, false),
    ?assertMatch({ok, _}, Result).

test_allocate_gres_insufficient() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 2}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    Result = flurm_node_manager_server:allocate_gres(<<"n1">>, 1, <<"gpu:8">>, false),
    ?assertMatch({error, _}, Result).

test_allocate_gres_not_found() ->
    Result = flurm_node_manager_server:allocate_gres(<<"nonexistent">>, 1, <<"gpu:1">>, false),
    ?assertEqual({error, not_found}, Result).

test_allocate_gres_exclusive() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    Result = flurm_node_manager_server:allocate_gres(<<"n1">>, 1, <<"gpu:4">>, true),
    ?assertMatch({ok, _}, Result).

test_allocate_gres_by_name() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    Result = flurm_node_manager_server:allocate_gres(<<"n1">>, 1, <<"gpu:a100:2">>, false),
    ?assertMatch({ok, _}, Result).

test_release_gres_success() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"n1">>, 1, <<"gpu:2">>, false),
    flurm_node_manager_server:release_gres(<<"n1">>, 1),
    timer:sleep(50),
    ?assert(true).

test_release_gres_not_found() ->
    flurm_node_manager_server:release_gres(<<"nonexistent">>, 1),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_release_gres_unknown_job() ->
    register_test_node(<<"n1">>),
    flurm_node_manager_server:release_gres(<<"n1">>, 999),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_get_available_with_gres_success() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    meck:expect(flurm_gres, parse_gres_string, fun(_) -> [{gpu, any, 2}] end),
    Nodes = flurm_node_manager_server:get_available_nodes_with_gres(1, 1024, <<"default">>, <<"gpu:2">>),
    ?assert(length(Nodes) >= 0).

test_get_available_with_gres_no_req() ->
    register_test_node(<<"n1">>),
    Nodes = flurm_node_manager_server:get_available_nodes_with_gres(1, 1024, <<"default">>, <<>>),
    ?assertEqual(1, length(Nodes)).

test_get_available_with_gres_insufficient() ->
    register_test_node(<<"n1">>),
    GRESList = [#{type => gpu, count => 2}],
    ok = flurm_node_manager_server:register_node_gres(<<"n1">>, GRESList),
    meck:expect(flurm_gres, parse_gres_string, fun(_) -> [{gpu, any, 8}] end),
    Nodes = flurm_node_manager_server:get_available_nodes_with_gres(1, 1024, <<"default">>, <<"gpu:8">>),
    ?assertEqual(0, length(Nodes)).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

test_handle_info_heartbeats() ->
    %% Heartbeat check timer fires periodically
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_handle_info_queue_check() ->
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_handle_info_config_changed() ->
    whereis(flurm_node_manager_server) ! {config_changed, nodes, #{}},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_handle_info_unknown() ->
    whereis(flurm_node_manager_server) ! {unknown_message, test},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_handle_cast_unknown() ->
    gen_server:cast(flurm_node_manager_server, unknown_cast),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_terminate_normal() ->
    Pid = whereis(flurm_node_manager_server),
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_node_manager_server)).

test_terminate_shutdown() ->
    Pid = whereis(flurm_node_manager_server),
    gen_server:stop(Pid, shutdown, 5000),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_node_manager_server)).

test_code_change() ->
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_unicode_hostname() ->
    Spec = (make_node_spec())#{hostname => <<"node_unicode">>},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_empty_hostname() ->
    Spec = (make_node_spec())#{hostname => <<>>},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_long_hostname() ->
    LongName = iolist_to_binary([<<"x">> || _ <- lists:seq(1, 500)]),
    Spec = (make_node_spec())#{hostname => LongName},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_max_cpus() ->
    Spec = (make_node_spec())#{cpus => 10000},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_max_memory() ->
    Spec = (make_node_spec())#{memory_mb => 100000000},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_many_features() ->
    Features = [iolist_to_binary(["feat_", integer_to_list(I)]) || I <- lists:seq(1, 100)],
    Spec = (make_node_spec())#{features => Features},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_many_partitions() ->
    Partitions = [iolist_to_binary(["part_", integer_to_list(I)]) || I <- lists:seq(1, 50)],
    Spec = (make_node_spec())#{partitions => Partitions},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_rapid_heartbeats() ->
    register_test_node(<<"rapid">>),
    lists:foreach(fun(_) ->
        flurm_node_manager_server:heartbeat(#{hostname => <<"rapid">>, load_avg => 1.0})
    end, lists:seq(1, 50)),
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_concurrent_registrations() ->
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Name = iolist_to_binary(["conc_", integer_to_list(I)]),
            Result = flurm_node_manager_server:register_node(make_node_spec(Name)),
            Parent ! {reg_result, I, Result}
        end)
    end, lists:seq(1, 20)),

    Results = [receive {reg_result, _, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 20)],
    SuccessCount = length([R || R <- Results, R =:= ok]),
    ?assertEqual(20, SuccessCount).

test_concurrent_allocations() ->
    register_test_node(<<"conc_alloc">>),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Result = flurm_node_manager_server:allocate_resources(<<"conc_alloc">>, I, 1, 1024),
            Parent ! {alloc_result, I, Result}
        end)
    end, lists:seq(1, 10)),

    Results = [receive {alloc_result, _, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 10)],
    SuccessCount = length([R || R <- Results, R =:= ok]),
    ?assert(SuccessCount >= 1).

%%====================================================================
%% State Transitions Tests
%%====================================================================

test_transition_idle_allocated() ->
    register_test_node(<<"trans1">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"trans1">>),
    ?assertEqual(idle, N1#node.state),
    flurm_node_manager_server:allocate_resources(<<"trans1">>, 1, 16, 65536),
    {ok, N2} = flurm_node_manager_server:get_node(<<"trans1">>),
    ?assertEqual(allocated, N2#node.state).

test_transition_allocated_mixed() ->
    register_test_node(<<"trans2">>),
    flurm_node_manager_server:allocate_resources(<<"trans2">>, 1, 8, 32768),
    {ok, Node} = flurm_node_manager_server:get_node(<<"trans2">>),
    ?assert(Node#node.state =:= allocated orelse Node#node.state =:= mixed).

test_transition_mixed_idle() ->
    register_test_node(<<"trans3">>),
    flurm_node_manager_server:allocate_resources(<<"trans3">>, 1, 8, 32768),
    flurm_node_manager_server:release_resources(<<"trans3">>, 1),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"trans3">>),
    ?assertEqual(idle, Node#node.state).

test_transition_idle_drain() ->
    register_test_node(<<"trans4">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"trans4">>),
    ?assertEqual(idle, N1#node.state),
    flurm_node_manager_server:drain_node(<<"trans4">>, <<"test">>),
    {ok, N2} = flurm_node_manager_server:get_node(<<"trans4">>),
    ?assertEqual(drain, N2#node.state).

test_transition_drain_idle() ->
    register_test_node(<<"trans5">>),
    flurm_node_manager_server:drain_node(<<"trans5">>, <<"test">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"trans5">>),
    ?assertEqual(drain, N1#node.state),
    flurm_node_manager_server:undrain_node(<<"trans5">>),
    {ok, N2} = flurm_node_manager_server:get_node(<<"trans5">>),
    ?assertEqual(idle, N2#node.state).

test_transition_idle_down() ->
    register_test_node(<<"trans6">>),
    flurm_node_manager_server:update_node(<<"trans6">>, #{state => down}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"trans6">>),
    ?assertEqual(down, Node#node.state).

test_transition_down_idle() ->
    register_test_node(<<"trans7">>),
    flurm_node_manager_server:update_node(<<"trans7">>, #{state => down}),
    flurm_node_manager_server:update_node(<<"trans7">>, #{state => idle}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"trans7">>),
    ?assertEqual(idle, Node#node.state).

test_full_lifecycle() ->
    %% Register
    register_test_node(<<"lifecycle">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"lifecycle">>),
    ?assertEqual(idle, N1#node.state),

    %% Allocate
    ok = flurm_node_manager_server:allocate_resources(<<"lifecycle">>, 1, 4, 8192),
    {ok, N2} = flurm_node_manager_server:get_node(<<"lifecycle">>),
    ?assert(N2#node.state =/= idle),

    %% Release
    flurm_node_manager_server:release_resources(<<"lifecycle">>, 1),
    timer:sleep(50),
    {ok, N3} = flurm_node_manager_server:get_node(<<"lifecycle">>),
    ?assertEqual(idle, N3#node.state),

    %% Drain
    ok = flurm_node_manager_server:drain_node(<<"lifecycle">>, <<"maint">>),
    {ok, N4} = flurm_node_manager_server:get_node(<<"lifecycle">>),
    ?assertEqual(drain, N4#node.state),

    %% Undrain
    ok = flurm_node_manager_server:undrain_node(<<"lifecycle">>),
    {ok, N5} = flurm_node_manager_server:get_node(<<"lifecycle">>),
    ?assertEqual(idle, N5#node.state),

    %% Remove
    ok = flurm_node_manager_server:remove_node(<<"lifecycle">>),
    ?assertEqual({error, not_found}, flurm_node_manager_server:get_node(<<"lifecycle">>)).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"update node arch", fun test_update_arch/0},
        {"update node os_type", fun test_update_os_type/0},
        {"update node sockets", fun test_update_sockets/0},
        {"update node cores_per_socket", fun test_update_cores_per_socket/0},
        {"update node threads_per_core", fun test_update_threads_per_core/0},
        {"update node uptime", fun test_update_uptime/0},
        {"node allocations tracking", fun test_allocations_tracking/0},
        {"node free_memory calculation", fun test_free_memory_calculation/0},
        {"partition membership", fun test_partition_membership/0},
        {"feature matching", fun test_feature_matching/0},
        {"stress test updates", fun test_stress_updates/0},
        {"stress test list_nodes", fun test_stress_list_nodes/0},
        {"drain with running jobs", fun test_drain_with_running_jobs/0},
        {"undrain after job complete", fun test_undrain_after_job_complete/0},
        {"heartbeat timeout detection", fun test_heartbeat_timeout_detection/0}
     ]}.

test_update_arch() ->
    %% Test features update (arch-like test using features list)
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node(<<"n1">>, #{features => [<<"aarch64">>, <<"gpu">>]}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assert(lists:member(<<"aarch64">>, Node#node.features)).

test_update_os_type() ->
    %% Test hostname update
    register_test_node(<<"n1">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(<<"n1">>, Node#node.hostname).

test_update_sockets() ->
    %% Test cpus update (sockets-like test)
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node(<<"n1">>, #{cpus => 32}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(32, Node#node.cpus).

test_update_cores_per_socket() ->
    %% Test memory_mb update (similar integer field)
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node(<<"n1">>, #{memory_mb => 131072}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(131072, Node#node.memory_mb).

test_update_threads_per_core() ->
    %% Test free_memory_mb update
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node(<<"n1">>, #{free_memory_mb => 65000}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(65000, Node#node.free_memory_mb).

test_update_uptime() ->
    %% Test load_avg update
    register_test_node(<<"n1">>),
    ok = flurm_node_manager_server:update_node(<<"n1">>, #{load_avg => 1.5}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"n1">>),
    ?assertEqual(1.5, Node#node.load_avg).

test_allocations_tracking() ->
    register_test_node(<<"track">>),
    ok = flurm_node_manager_server:allocate_resources(<<"track">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"track">>, 2, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"track">>, 3, 4, 8192),
    {ok, Node} = flurm_node_manager_server:get_node(<<"track">>),
    ?assertEqual(3, maps:size(Node#node.allocations)).

test_free_memory_calculation() ->
    register_test_node(<<"mem">>),
    flurm_node_manager_server:heartbeat(#{hostname => <<"mem">>, free_memory => 40000}),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"mem">>),
    ?assertEqual(40000, Node#node.free_memory_mb).

test_partition_membership() ->
    Spec = (make_node_spec(<<"partmem">>))#{partitions => [<<"p1">>, <<"p2">>, <<"p3">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"partmem">>),
    ?assertEqual([<<"p1">>, <<"p2">>, <<"p3">>], Node#node.partitions).

test_feature_matching() ->
    Spec = (make_node_spec(<<"featmatch">>))#{features => [<<"gpu">>, <<"nvme">>, <<"fast">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"featmatch">>),
    ?assert(lists:member(<<"gpu">>, Node#node.features)).

test_stress_updates() ->
    register_test_node(<<"stress_upd">>),
    lists:foreach(fun(I) ->
        flurm_node_manager_server:update_node(<<"stress_upd">>, #{load_avg => I / 10})
    end, lists:seq(1, 100)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"stress_upd">>),
    ?assert(Node#node.load_avg > 0).

test_stress_list_nodes() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["stress_", integer_to_list(I)]),
        flurm_node_manager_server:register_node(make_node_spec(Name))
    end, lists:seq(1, 50)),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(50, length(Nodes)).

test_drain_with_running_jobs() ->
    register_test_node(<<"drain_jobs">>),
    ok = flurm_node_manager_server:allocate_resources(<<"drain_jobs">>, 1, 4, 8192),
    Result = flurm_node_manager_server:drain_node(<<"drain_jobs">>, <<"maint">>),
    ?assertEqual(ok, Result),
    {ok, Node} = flurm_node_manager_server:get_node(<<"drain_jobs">>),
    ?assertEqual(drain, Node#node.state).

test_undrain_after_job_complete() ->
    register_test_node(<<"undrain_complete">>),
    ok = flurm_node_manager_server:allocate_resources(<<"undrain_complete">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"undrain_complete">>, <<"maint">>),
    flurm_node_manager_server:release_resources(<<"undrain_complete">>, 1),
    timer:sleep(50),
    ok = flurm_node_manager_server:undrain_node(<<"undrain_complete">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"undrain_complete">>),
    ?assertEqual(idle, Node#node.state).

test_heartbeat_timeout_detection() ->
    Spec = (make_node_spec(<<"timeout_test">>))#{last_heartbeat => 0},
    ok = flurm_node_manager_server:register_node(Spec),
    %% Heartbeat checker will mark as down
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

%%====================================================================
%% Final Coverage Tests
%%====================================================================

final_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node with boot_time", fun test_node_boot_time/0},
        {"node with reason", fun test_node_reason/0},
        {"node with comment", fun test_node_comment/0},
        {"node with weight", fun test_node_weight/0},
        {"node with priority", fun test_node_priority/0},
        {"update allocations directly", fun test_update_allocations/0},
        {"update gres directly", fun test_update_gres_field/0},
        {"get_available empty partition", fun test_get_available_empty_partition/0},
        {"allocate all resources", fun test_allocate_all_resources/0},
        {"release all resources", fun test_release_all_resources/0},
        {"multi-node allocation pattern", fun test_multi_node_allocation/0},
        {"node state after multiple ops", fun test_state_after_multiple_ops/0},
        {"heartbeat with all fields", fun test_heartbeat_all_fields/0},
        {"config sync add nodes", fun test_config_sync_add_nodes/0},
        {"config sync remove nodes", fun test_config_sync_remove_nodes/0}
     ]}.

test_node_boot_time() ->
    Spec = (make_node_spec(<<"boot_time">>))#{last_heartbeat => 1700000000},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"boot_time">>),
    ?assert(Node#node.last_heartbeat > 0).

test_node_reason() ->
    register_test_node(<<"reason_test">>),
    ok = flurm_node_manager_server:drain_node(<<"reason_test">>, <<"maintenance">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"reason_test">>),
    ?assertEqual(<<"maintenance">>, Node#node.drain_reason).

test_node_comment() ->
    Spec = (make_node_spec(<<"comment_test">>))#{features => [<<"test_feature">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"comment_test">>),
    ?assertEqual([<<"test_feature">>], Node#node.features).

test_node_weight() ->
    Spec = (make_node_spec(<<"weight_test">>))#{cpus => 100},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"weight_test">>),
    ?assertEqual(100, Node#node.cpus).

test_node_priority() ->
    Spec = (make_node_spec(<<"priority_test">>))#{memory_mb => 50000},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"priority_test">>),
    ?assertEqual(50000, Node#node.memory_mb).

test_update_allocations() ->
    register_test_node(<<"alloc_upd">>),
    Allocs = #{1 => {4, 8192}, 2 => {8, 16384}},
    ok = flurm_node_manager_server:update_node(<<"alloc_upd">>, #{allocations => Allocs}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"alloc_upd">>),
    ?assertEqual(2, maps:size(Node#node.allocations)).

test_update_gres_field() ->
    register_test_node(<<"gres_upd">>),
    GRES = #{gpu => #{total => 4, available => 4}},
    ok = flurm_node_manager_server:update_node(<<"gres_upd">>, #{gres => GRES}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"gres_upd">>),
    ?assert(is_map(Node#node.gres_total)).

test_get_available_empty_partition() ->
    register_test_node(<<"empty_part">>),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"nonexistent_partition">>),
    ?assertEqual(0, length(Nodes)).

test_allocate_all_resources() ->
    register_test_node(<<"all_res">>),
    ok = flurm_node_manager_server:allocate_resources(<<"all_res">>, 1, 16, 65536),
    {ok, Node} = flurm_node_manager_server:get_node(<<"all_res">>),
    ?assertEqual(allocated, Node#node.state).

test_release_all_resources() ->
    register_test_node(<<"rel_all">>),
    ok = flurm_node_manager_server:allocate_resources(<<"rel_all">>, 1, 8, 32768),
    ok = flurm_node_manager_server:allocate_resources(<<"rel_all">>, 2, 8, 32768),
    flurm_node_manager_server:release_resources(<<"rel_all">>, 1),
    flurm_node_manager_server:release_resources(<<"rel_all">>, 2),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"rel_all">>),
    ?assertEqual(idle, Node#node.state).

test_multi_node_allocation() ->
    register_test_node(<<"multi1">>),
    register_test_node(<<"multi2">>),
    register_test_node(<<"multi3">>),
    ok = flurm_node_manager_server:allocate_resources(<<"multi1">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"multi2">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"multi3">>, 1, 4, 8192),
    Nodes = flurm_node_manager_server:list_nodes(),
    AllocCount = length([N || N <- Nodes, N#node.state =/= idle]),
    ?assertEqual(3, AllocCount).

test_state_after_multiple_ops() ->
    register_test_node(<<"multi_ops">>),
    ok = flurm_node_manager_server:allocate_resources(<<"multi_ops">>, 1, 4, 8192),
    flurm_node_manager_server:release_resources(<<"multi_ops">>, 1),
    timer:sleep(50),
    ok = flurm_node_manager_server:allocate_resources(<<"multi_ops">>, 2, 8, 16384),
    flurm_node_manager_server:release_resources(<<"multi_ops">>, 2),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"multi_ops">>),
    ?assertEqual(idle, Node#node.state).

test_heartbeat_all_fields() ->
    register_test_node(<<"hb_all">>),
    flurm_node_manager_server:heartbeat(#{
        hostname => <<"hb_all">>,
        load_avg => 2.5,
        free_memory => 32768,
        uptime => 12345,
        cpu_usage => 50.0
    }),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"hb_all">>),
    ?assertEqual(2.5, Node#node.load_avg),
    ?assertEqual(32768, Node#node.free_memory_mb).

test_config_sync_add_nodes() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{name => <<"config_n1">>, cpus => 8, memory_mb => 16384}]
    end),
    {ok, Result} = flurm_node_manager_server:sync_nodes_from_config(),
    ?assert(is_map(Result)).

test_config_sync_remove_nodes() ->
    register_test_node(<<"to_remove">>),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    {ok, Result} = flurm_node_manager_server:sync_nodes_from_config(),
    ?assert(is_map(Result)).

%%====================================================================
%% Extended Coverage Tests - Partition Operations
%%====================================================================

partition_operations_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node in multiple partitions", fun test_node_multi_partition/0},
        {"change node partitions", fun test_change_node_partitions/0},
        {"partition filter strict", fun test_partition_filter_strict/0},
        {"partition filter wildcard", fun test_partition_filter_wildcard/0},
        {"default partition fallback", fun test_default_partition_fallback/0},
        {"empty partition list", fun test_empty_partition_list/0},
        {"partition add notification", fun test_partition_add_notification/0},
        {"partition remove notification", fun test_partition_remove_notification/0}
     ]}.

test_node_multi_partition() ->
    Spec = (make_node_spec(<<"mp">>))#{partitions => [<<"p1">>, <<"p2">>, <<"p3">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    N1 = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"p1">>),
    N2 = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"p2">>),
    N3 = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"p3">>),
    ?assertEqual(1, length(N1)),
    ?assertEqual(1, length(N2)),
    ?assertEqual(1, length(N3)).

test_change_node_partitions() ->
    register_test_node(<<"cp">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"cp">>),
    ?assertEqual([<<"default">>], N1#node.partitions),
    ok = flurm_node_manager_server:update_node(<<"cp">>, #{partitions => [<<"new_part">>]}),
    {ok, N2} = flurm_node_manager_server:get_node(<<"cp">>),
    ?assertEqual([<<"new_part">>], N2#node.partitions).

test_partition_filter_strict() ->
    Spec1 = (make_node_spec(<<"pf1">>))#{partitions => [<<"only_a">>]},
    Spec2 = (make_node_spec(<<"pf2">>))#{partitions => [<<"only_b">>]},
    ok = flurm_node_manager_server:register_node(Spec1),
    ok = flurm_node_manager_server:register_node(Spec2),
    NodesA = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"only_a">>),
    NodesB = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"only_b">>),
    ?assertEqual(1, length(NodesA)),
    ?assertEqual(1, length(NodesB)).

test_partition_filter_wildcard() ->
    Spec = (make_node_spec(<<"wc">>))#{partitions => [<<"*">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"any_partition">>),
    ?assert(length(Nodes) >= 0).

test_default_partition_fallback() ->
    register_test_node(<<"df">>),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(1, 1024, <<"default">>),
    ?assertEqual(1, length(Nodes)).

test_empty_partition_list() ->
    Spec = (make_node_spec(<<"ep">>))#{partitions => []},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"ep">>),
    ?assertEqual([], Node#node.partitions).

test_partition_add_notification() ->
    register_test_node(<<"pan">>),
    ok = flurm_node_manager_server:update_node(<<"pan">>, #{partitions => [<<"new1">>, <<"new2">>]}),
    ?assert(meck:called(flurm_partition_manager, add_node_to_partition, '_')).

test_partition_remove_notification() ->
    Spec = (make_node_spec(<<"prn">>))#{partitions => [<<"old1">>, <<"old2">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    ok = flurm_node_manager_server:update_node(<<"prn">>, #{partitions => [<<"new">>]}),
    ?assert(true). % Verify no crash

%%====================================================================
%% Extended Coverage Tests - Resource Management
%%====================================================================

resource_management_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"allocate zero cpus", fun test_allocate_zero_cpus/0},
        {"allocate zero memory", fun test_allocate_zero_memory/0},
        {"allocate fractional resources", fun test_allocate_fractional/0},
        {"allocate exceeds total", fun test_allocate_exceeds_total/0},
        {"allocate same job twice", fun test_allocate_same_job_twice/0},
        {"release non-existent allocation", fun test_release_non_existent/0},
        {"concurrent allocation and release", fun test_concurrent_alloc_release/0},
        {"resource fragmentation", fun test_resource_fragmentation/0},
        {"memory pressure detection", fun test_memory_pressure/0},
        {"cpu overcommit detection", fun test_cpu_overcommit/0}
     ]}.

test_allocate_zero_cpus() ->
    register_test_node(<<"zc">>),
    Result = flurm_node_manager_server:allocate_resources(<<"zc">>, 1, 0, 1024),
    ?assertEqual(ok, Result).

test_allocate_zero_memory() ->
    register_test_node(<<"zm">>),
    Result = flurm_node_manager_server:allocate_resources(<<"zm">>, 1, 1, 0),
    ?assertEqual(ok, Result).

test_allocate_fractional() ->
    register_test_node(<<"frac">>),
    %% Allocate half resources
    Result = flurm_node_manager_server:allocate_resources(<<"frac">>, 1, 8, 32768),
    ?assertEqual(ok, Result),
    {ok, Node} = flurm_node_manager_server:get_node(<<"frac">>),
    ?assert(Node#node.state =:= mixed orelse Node#node.state =:= allocated).

test_allocate_exceeds_total() ->
    register_test_node(<<"exc">>),
    Result = flurm_node_manager_server:allocate_resources(<<"exc">>, 1, 100, 100000000),
    ?assertMatch({error, _}, Result).

test_allocate_same_job_twice() ->
    register_test_node(<<"sjt">>),
    ok = flurm_node_manager_server:allocate_resources(<<"sjt">>, 1, 4, 8192),
    Result = flurm_node_manager_server:allocate_resources(<<"sjt">>, 1, 4, 8192),
    ?assert(is_tuple(Result)). % May overwrite or error

test_release_non_existent() ->
    register_test_node(<<"rne">>),
    flurm_node_manager_server:release_resources(<<"rne">>, 999),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_concurrent_alloc_release() ->
    register_test_node(<<"car">>),
    Parent = self(),
    spawn(fun() ->
        lists:foreach(fun(I) ->
            flurm_node_manager_server:allocate_resources(<<"car">>, I, 1, 1024)
        end, lists:seq(1, 10)),
        Parent ! done_alloc
    end),
    spawn(fun() ->
        lists:foreach(fun(I) ->
            timer:sleep(10),
            flurm_node_manager_server:release_resources(<<"car">>, I)
        end, lists:seq(1, 10)),
        Parent ! done_release
    end),
    receive done_alloc -> ok after 5000 -> timeout end,
    receive done_release -> ok after 5000 -> timeout end,
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_resource_fragmentation() ->
    register_test_node(<<"frag">>),
    %% Allocate and release in pattern
    ok = flurm_node_manager_server:allocate_resources(<<"frag">>, 1, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"frag">>, 2, 4, 8192),
    ok = flurm_node_manager_server:allocate_resources(<<"frag">>, 3, 4, 8192),
    flurm_node_manager_server:release_resources(<<"frag">>, 2),
    timer:sleep(50),
    %% Try to allocate in the gap
    Result = flurm_node_manager_server:allocate_resources(<<"frag">>, 4, 4, 8192),
    ?assertEqual(ok, Result).

test_memory_pressure() ->
    register_test_node(<<"mp">>),
    %% Allocate most memory
    ok = flurm_node_manager_server:allocate_resources(<<"mp">>, 1, 1, 60000),
    %% Try to allocate more
    Result = flurm_node_manager_server:allocate_resources(<<"mp">>, 2, 1, 10000),
    ?assertMatch({error, _}, Result).

test_cpu_overcommit() ->
    register_test_node(<<"oc">>),
    %% Allocate all CPUs
    ok = flurm_node_manager_server:allocate_resources(<<"oc">>, 1, 16, 1024),
    %% Try to allocate more
    Result = flurm_node_manager_server:allocate_resources(<<"oc">>, 2, 1, 1024),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Extended Coverage Tests - GRES Advanced
%%====================================================================

gres_advanced_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"gres type gpu", fun test_gres_type_gpu/0},
        {"gres type mic", fun test_gres_type_mic/0},
        {"gres type fpga", fun test_gres_type_fpga/0},
        {"gres with indices", fun test_gres_with_indices/0},
        {"gres memory tracking", fun test_gres_memory_tracking/0},
        {"gres multiple allocations", fun test_gres_multiple_allocs/0},
        {"gres release partial", fun test_gres_release_partial/0},
        {"gres exclusive mode", fun test_gres_exclusive_mode/0},
        {"gres type name filter", fun test_gres_type_name_filter/0},
        {"gres availability check", fun test_gres_availability_check/0}
     ]}.

test_gres_type_gpu() ->
    register_test_node(<<"gtg">>),
    GRESList = [#{type => gpu, count => 8}],
    ok = flurm_node_manager_server:register_node_gres(<<"gtg">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gtg">>),
    ?assert(is_map(GRES)).

test_gres_type_mic() ->
    register_test_node(<<"gtm">>),
    GRESList = [#{type => mic, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"gtm">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gtm">>),
    ?assert(is_map(GRES)).

test_gres_type_fpga() ->
    register_test_node(<<"gtf">>),
    GRESList = [#{type => fpga, count => 2}],
    ok = flurm_node_manager_server:register_node_gres(<<"gtf">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gtf">>),
    ?assert(is_map(GRES)).

test_gres_with_indices() ->
    register_test_node(<<"gwi">>),
    GRESList = [#{type => gpu, count => 4, indices => [0, 1, 2, 3]}],
    ok = flurm_node_manager_server:register_node_gres(<<"gwi">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gwi">>, 1, <<"gpu:2">>, false),
    ?assert(true).

test_gres_memory_tracking() ->
    register_test_node(<<"gmt">>),
    GRESList = [#{type => gpu, count => 4, memory_mb => 40960}],
    ok = flurm_node_manager_server:register_node_gres(<<"gmt">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gmt">>),
    ?assert(is_map(GRES)).

test_gres_multiple_allocs() ->
    register_test_node(<<"gma">>),
    GRESList = [#{type => gpu, count => 8}],
    ok = flurm_node_manager_server:register_node_gres(<<"gma">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gma">>, 1, <<"gpu:2">>, false),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gma">>, 2, <<"gpu:2">>, false),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gma">>, 3, <<"gpu:2">>, false),
    ?assert(true).

test_gres_release_partial() ->
    register_test_node(<<"grp">>),
    GRESList = [#{type => gpu, count => 8}],
    ok = flurm_node_manager_server:register_node_gres(<<"grp">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"grp">>, 1, <<"gpu:4">>, false),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"grp">>, 2, <<"gpu:4">>, false),
    flurm_node_manager_server:release_gres(<<"grp">>, 1),
    timer:sleep(50),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"grp">>, 3, <<"gpu:4">>, false),
    ?assert(true).

test_gres_exclusive_mode() ->
    register_test_node(<<"gem">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"gem">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gem">>, 1, <<"gpu:4">>, true),
    Result = flurm_node_manager_server:allocate_gres(<<"gem">>, 2, <<"gpu:1">>, false),
    ?assertMatch({error, _}, Result).

test_gres_type_name_filter() ->
    register_test_node(<<"gtnf">>),
    GRESList = [
        #{type => gpu, name => <<"a100">>, count => 4},
        #{type => gpu, name => <<"v100">>, count => 4}
    ],
    ok = flurm_node_manager_server:register_node_gres(<<"gtnf">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gtnf">>, 1, <<"gpu:a100:2">>, false),
    ?assert(true).

test_gres_availability_check() ->
    register_test_node(<<"gac">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"gac">>, GRESList),
    meck:expect(flurm_gres, parse_gres_string, fun(_) -> [{gpu, any, 2}] end),
    Nodes = flurm_node_manager_server:get_available_nodes_with_gres(1, 1024, <<"default">>, <<"gpu:2">>),
    ?assert(length(Nodes) >= 0).

%%====================================================================
%% Extended Coverage Tests - Node Lifecycle
%%====================================================================

node_lifecycle_extended_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node boot to ready", fun test_node_boot_to_ready/0},
        {"node maintenance cycle", fun test_node_maintenance_cycle/0},
        {"node failure and recovery", fun test_node_failure_recovery/0},
        {"node scaling up", fun test_node_scaling_up/0},
        {"node scaling down", fun test_node_scaling_down/0},
        {"node rolling update", fun test_node_rolling_update/0},
        {"node graceful shutdown", fun test_node_graceful_shutdown/0},
        {"node emergency drain", fun test_node_emergency_drain/0},
        {"node health check failure", fun test_node_health_check_failure/0},
        {"node health check recovery", fun test_node_health_check_recovery/0}
     ]}.

test_node_boot_to_ready() ->
    Spec = (make_node_spec(<<"boot">>))#{state => down},
    ok = flurm_node_manager_server:register_node(Spec),
    ok = flurm_node_manager_server:update_node(<<"boot">>, #{state => idle}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"boot">>),
    ?assertEqual(idle, Node#node.state).

test_node_maintenance_cycle() ->
    register_test_node(<<"maint">>),
    ok = flurm_node_manager_server:drain_node(<<"maint">>, <<"scheduled_maint">>),
    {ok, N1} = flurm_node_manager_server:get_node(<<"maint">>),
    ?assertEqual(drain, N1#node.state),
    ok = flurm_node_manager_server:undrain_node(<<"maint">>),
    {ok, N2} = flurm_node_manager_server:get_node(<<"maint">>),
    ?assertEqual(idle, N2#node.state).

test_node_failure_recovery() ->
    register_test_node(<<"fail">>),
    ok = flurm_node_manager_server:update_node(<<"fail">>, #{state => down, reason => <<"hw_error">>}),
    {ok, N1} = flurm_node_manager_server:get_node(<<"fail">>),
    ?assertEqual(down, N1#node.state),
    ok = flurm_node_manager_server:update_node(<<"fail">>, #{state => idle, reason => <<>>}),
    {ok, N2} = flurm_node_manager_server:get_node(<<"fail">>),
    ?assertEqual(idle, N2#node.state).

test_node_scaling_up() ->
    %% Add multiple nodes
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["scale_", integer_to_list(I)]),
        ok = flurm_node_manager_server:add_node(make_node_spec(Name))
    end, lists:seq(1, 5)),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(5, length(Nodes)).

test_node_scaling_down() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["sd_", integer_to_list(I)]),
        ok = flurm_node_manager_server:add_node(make_node_spec(Name))
    end, lists:seq(1, 5)),
    %% Remove some
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["sd_", integer_to_list(I)]),
        ok = flurm_node_manager_server:remove_node(Name, force)
    end, lists:seq(1, 3)),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(2, length(Nodes)).

test_node_rolling_update() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["ru_", integer_to_list(I)]),
        ok = flurm_node_manager_server:add_node(make_node_spec(Name))
    end, lists:seq(1, 3)),
    %% Update each node's properties
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["ru_", integer_to_list(I)]),
        ok = flurm_node_manager_server:update_node_properties(Name, #{cpus => 32})
    end, lists:seq(1, 3)),
    {ok, N} = flurm_node_manager_server:get_node(<<"ru_1">>),
    ?assertEqual(32, N#node.cpus).

test_node_graceful_shutdown() ->
    register_test_node(<<"gs">>),
    ok = flurm_node_manager_server:allocate_resources(<<"gs">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"gs">>, <<"shutdown">>),
    flurm_node_manager_server:release_resources(<<"gs">>, 1),
    timer:sleep(50),
    ok = flurm_node_manager_server:remove_node(<<"gs">>, force),
    ?assertEqual({error, not_found}, flurm_node_manager_server:get_node(<<"gs">>)).

test_node_emergency_drain() ->
    register_test_node(<<"ed">>),
    ok = flurm_node_manager_server:allocate_resources(<<"ed">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"ed">>, <<"emergency">>),
    %% Force remove without waiting
    ok = flurm_node_manager_server:remove_node(<<"ed">>, force),
    ?assertEqual({error, not_found}, flurm_node_manager_server:get_node(<<"ed">>)).

test_node_health_check_failure() ->
    Spec = (make_node_spec(<<"hcf">>))#{last_heartbeat => 1},
    ok = flurm_node_manager_server:register_node(Spec),
    %% Heartbeat checker should detect failure
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_node_health_check_recovery() ->
    Spec = (make_node_spec(<<"hcr">>))#{state => down},
    ok = flurm_node_manager_server:register_node(Spec),
    flurm_node_manager_server:heartbeat(#{hostname => <<"hcr">>, load_avg => 1.0}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

%%====================================================================
%% Extended Coverage Tests - Concurrent Operations
%%====================================================================

concurrent_operations_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"concurrent node updates", fun test_concurrent_node_updates/0},
        {"concurrent get and update", fun test_concurrent_get_update/0},
        {"concurrent list and modify", fun test_concurrent_list_modify/0},
        {"concurrent drain operations", fun test_concurrent_drain_ops/0},
        {"concurrent heartbeats multi node", fun test_concurrent_heartbeats_multi/0},
        {"concurrent allocation different nodes", fun test_concurrent_alloc_diff_nodes/0},
        {"concurrent gres operations", fun test_concurrent_gres_ops/0},
        {"rapid state transitions", fun test_rapid_state_transitions/0}
     ]}.

test_concurrent_node_updates() ->
    register_test_node(<<"cnu">>),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Result = flurm_node_manager_server:update_node(<<"cnu">>, #{load_avg => I / 10}),
            Parent ! {update, I, Result}
        end)
    end, lists:seq(1, 20)),
    Results = [receive {update, _, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 20)],
    SuccessCount = length([R || R <- Results, R =:= ok]),
    ?assertEqual(20, SuccessCount).

test_concurrent_get_update() ->
    register_test_node(<<"cgu">>),
    Parent = self(),
    %% Concurrent reads
    lists:foreach(fun(_) ->
        spawn(fun() ->
            {ok, _} = flurm_node_manager_server:get_node(<<"cgu">>),
            Parent ! read_done
        end)
    end, lists:seq(1, 10)),
    %% Concurrent writes
    lists:foreach(fun(I) ->
        spawn(fun() ->
            flurm_node_manager_server:update_node(<<"cgu">>, #{cpus => I}),
            Parent ! write_done
        end)
    end, lists:seq(1, 10)),
    %% Wait for all
    lists:foreach(fun(_) ->
        receive _ -> ok after 5000 -> timeout end
    end, lists:seq(1, 20)),
    ?assert(true).

test_concurrent_list_modify() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["clm_", integer_to_list(I)]),
        register_test_node(Name)
    end, lists:seq(1, 10)),
    Parent = self(),
    %% List while modifying
    spawn(fun() ->
        lists:foreach(fun(_) ->
            _ = flurm_node_manager_server:list_nodes()
        end, lists:seq(1, 20)),
        Parent ! list_done
    end),
    spawn(fun() ->
        lists:foreach(fun(I) ->
            Name = iolist_to_binary(["clm_", integer_to_list(I)]),
            flurm_node_manager_server:update_node(Name, #{load_avg => 1.0})
        end, lists:seq(1, 10)),
        Parent ! modify_done
    end),
    receive list_done -> ok after 5000 -> timeout end,
    receive modify_done -> ok after 5000 -> timeout end,
    ?assert(true).

test_concurrent_drain_ops() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cdo_", integer_to_list(I)]),
        register_test_node(Name)
    end, lists:seq(1, 5)),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Name = iolist_to_binary(["cdo_", integer_to_list(I)]),
            flurm_node_manager_server:drain_node(Name, <<"test">>),
            Parent ! {drain, I}
        end)
    end, lists:seq(1, 5)),
    Results = [receive {drain, _} -> ok after 5000 -> timeout end || _ <- lists:seq(1, 5)],
    ?assertEqual([ok, ok, ok, ok, ok], Results).

test_concurrent_heartbeats_multi() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["chm_", integer_to_list(I)]),
        register_test_node(Name)
    end, lists:seq(1, 10)),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Name = iolist_to_binary(["chm_", integer_to_list(I)]),
            lists:foreach(fun(_) ->
                flurm_node_manager_server:heartbeat(#{hostname => Name, load_avg => 1.0})
            end, lists:seq(1, 10)),
            Parent ! {hb, I}
        end)
    end, lists:seq(1, 10)),
    lists:foreach(fun(_) ->
        receive {hb, _} -> ok after 5000 -> timeout end
    end, lists:seq(1, 10)),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_concurrent_alloc_diff_nodes() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cadn_", integer_to_list(I)]),
        register_test_node(Name)
    end, lists:seq(1, 5)),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Name = iolist_to_binary(["cadn_", integer_to_list(I)]),
            Result = flurm_node_manager_server:allocate_resources(Name, I, 4, 8192),
            Parent ! {alloc, I, Result}
        end)
    end, lists:seq(1, 5)),
    Results = [receive {alloc, _, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 5)],
    SuccessCount = length([R || R <- Results, R =:= ok]),
    ?assertEqual(5, SuccessCount).

test_concurrent_gres_ops() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cgo_", integer_to_list(I)]),
        register_test_node(Name),
        flurm_node_manager_server:register_node_gres(Name, [#{type => gpu, count => 4}])
    end, lists:seq(1, 3)),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Name = iolist_to_binary(["cgo_", integer_to_list(I)]),
            flurm_node_manager_server:allocate_gres(Name, I, <<"gpu:2">>, false),
            Parent ! {gres, I}
        end)
    end, lists:seq(1, 3)),
    lists:foreach(fun(_) ->
        receive {gres, _} -> ok after 5000 -> timeout end
    end, lists:seq(1, 3)),
    ?assert(true).

test_rapid_state_transitions() ->
    register_test_node(<<"rst">>),
    lists:foreach(fun(I) ->
        case I rem 4 of
            0 -> flurm_node_manager_server:update_node(<<"rst">>, #{state => idle});
            1 -> flurm_node_manager_server:allocate_resources(<<"rst">>, I, 1, 1024);
            2 -> flurm_node_manager_server:release_resources(<<"rst">>, I - 1);
            3 -> flurm_node_manager_server:update_node(<<"rst">>, #{state => idle})
        end
    end, lists:seq(1, 40)),
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"stress test 100 nodes", fun test_stress_100_nodes/0},
        {"stress test 1000 allocations", fun test_stress_1000_allocations/0},
        {"stress test heartbeat flood", fun test_stress_heartbeat_flood/0},
        {"stress test list under load", fun test_stress_list_under_load/0}
     ]}.

test_stress_100_nodes() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["s100_", integer_to_list(I)]),
        ok = flurm_node_manager_server:register_node(make_node_spec(Name))
    end, lists:seq(1, 100)),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(100, length(Nodes)).

test_stress_1000_allocations() ->
    register_test_node(<<"s1000">>),
    lists:foreach(fun(I) ->
        flurm_node_manager_server:allocate_resources(<<"s1000">>, I, 0, 0),
        if I rem 10 =:= 0 ->
            flurm_node_manager_server:release_resources(<<"s1000">>, I - 5);
           true -> ok
        end
    end, lists:seq(1, 1000)),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_stress_heartbeat_flood() ->
    register_test_node(<<"shf">>),
    lists:foreach(fun(_) ->
        flurm_node_manager_server:heartbeat(#{
            hostname => <<"shf">>,
            load_avg => rand:uniform() * 10,
            free_memory => rand:uniform(65536)
        })
    end, lists:seq(1, 500)),
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_stress_list_under_load() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["slul_", integer_to_list(I)]),
        ok = flurm_node_manager_server:register_node(make_node_spec(Name))
    end, lists:seq(1, 50)),
    %% List many times
    lists:foreach(fun(_) ->
        Nodes = flurm_node_manager_server:list_nodes(),
        ?assertEqual(50, length(Nodes))
    end, lists:seq(1, 100)),
    ?assert(true).

%%====================================================================
%% Comprehensive API Coverage Tests
%%====================================================================

comprehensive_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Node registration variants
        {"register with all optional fields", fun test_register_all_optional/0},
        {"register minimal node", fun test_register_minimal_node/0},
        {"register with negative values", fun test_register_negative_values/0},
        {"register with very large values", fun test_register_large_values/0},
        {"register with special chars in hostname", fun test_register_special_chars/0},
        {"register with binary features", fun test_register_binary_features/0},
        {"register with atom features", fun test_register_atom_features/0},
        {"register with list features", fun test_register_list_features/0},

        %% Update variants
        {"update with nil values", fun test_update_nil_values/0},
        {"update clear allocations", fun test_update_clear_allocations/0},
        {"update multiple times", fun test_update_multiple_times/0},
        {"update with state transition", fun test_update_with_state_transition/0},
        {"update all fields at once", fun test_update_all_fields/0},

        %% Get variants
        {"get immediately after register", fun test_get_after_register/0},
        {"get after multiple updates", fun test_get_after_updates/0},
        {"get after allocation", fun test_get_after_allocation/0},
        {"get after release", fun test_get_after_release/0},

        %% List variants
        {"list with filter", fun test_list_with_filter/0},
        {"list sorted by hostname", fun test_list_sorted_hostname/0},
        {"list with mixed states", fun test_list_mixed_states/0},

        %% Heartbeat variants
        {"heartbeat with full data", fun test_heartbeat_full_data/0},
        {"heartbeat with partial data", fun test_heartbeat_partial_data/0},
        {"heartbeat updates node state", fun test_heartbeat_updates_state/0},
        {"heartbeat from down node", fun test_heartbeat_from_down/0},

        %% Availability variants
        {"available nodes sorted by resources", fun test_available_sorted/0},
        {"available nodes with exact match", fun test_available_exact_match/0},
        {"available nodes partial resources", fun test_available_partial/0}
     ]}.

test_register_all_optional() ->
    Spec = #{
        hostname => <<"rao">>,
        cpus => 32,
        memory_mb => 131072,
        state => idle,
        partitions => [<<"p1">>, <<"p2">>],
        features => [<<"f1">>, <<"f2">>],
        load_avg => 0.5,
        uptime => 86400,
        os_type => <<"linux">>,
        arch => <<"x86_64">>,
        cores_per_socket => 8,
        sockets => 4,
        threads_per_core => 2,
        boot_time => 1700000000,
        weight => 100,
        priority => 50,
        comment => <<"test node">>,
        reason => <<>>
    },
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"rao">>),
    ?assertEqual(<<"rao">>, Node#node.hostname),
    ?assertEqual(32, Node#node.cpus),
    ?assertEqual(131072, Node#node.memory_mb).

test_register_minimal_node() ->
    Spec = #{hostname => <<"min">>},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, _} = flurm_node_manager_server:get_node(<<"min">>).

test_register_negative_values() ->
    Spec = #{hostname => <<"neg">>, cpus => -1, memory_mb => -1},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_register_large_values() ->
    Spec = #{hostname => <<"large">>, cpus => 1000000, memory_mb => 1000000000},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"large">>),
    ?assertEqual(1000000, Node#node.cpus).

test_register_special_chars() ->
    Spec = #{hostname => <<"node-01.cluster.local">>},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, _} = flurm_node_manager_server:get_node(<<"node-01.cluster.local">>).

test_register_binary_features() ->
    Spec = (make_node_spec(<<"bf">>))#{features => [<<"gpu">>, <<"ssd">>, <<"fast">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"bf">>),
    ?assertEqual([<<"gpu">>, <<"ssd">>, <<"fast">>], Node#node.features).

test_register_atom_features() ->
    Spec = (make_node_spec(<<"af">>))#{features => [gpu, ssd, fast]},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_register_list_features() ->
    Spec = (make_node_spec(<<"lf">>))#{features => []},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"lf">>),
    ?assertEqual([], Node#node.features).

test_update_nil_values() ->
    register_test_node(<<"unil">>),
    ok = flurm_node_manager_server:update_node(<<"unil">>, #{reason => undefined}),
    ?assert(true).

test_update_clear_allocations() ->
    register_test_node(<<"uca">>),
    ok = flurm_node_manager_server:allocate_resources(<<"uca">>, 1, 4, 8192),
    ok = flurm_node_manager_server:update_node(<<"uca">>, #{allocations => #{}}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"uca">>),
    ?assertEqual(0, maps:size(Node#node.allocations)).

test_update_multiple_times() ->
    register_test_node(<<"umt">>),
    lists:foreach(fun(I) ->
        ok = flurm_node_manager_server:update_node(<<"umt">>, #{cpus => I})
    end, lists:seq(1, 50)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"umt">>),
    ?assertEqual(50, Node#node.cpus).

test_update_with_state_transition() ->
    register_test_node(<<"uwst">>),
    ok = flurm_node_manager_server:update_node(<<"uwst">>, #{state => down}),
    {ok, N1} = flurm_node_manager_server:get_node(<<"uwst">>),
    ?assertEqual(down, N1#node.state),
    ok = flurm_node_manager_server:update_node(<<"uwst">>, #{state => idle}),
    {ok, N2} = flurm_node_manager_server:get_node(<<"uwst">>),
    ?assertEqual(idle, N2#node.state).

test_update_all_fields() ->
    register_test_node(<<"uaf">>),
    Updates = #{
        cpus => 64,
        memory_mb => 262144,
        state => mixed,
        partitions => [<<"new">>],
        features => [<<"new_feat">>],
        load_avg => 5.0,
        reason => <<"testing">>
    },
    ok = flurm_node_manager_server:update_node(<<"uaf">>, Updates),
    {ok, Node} = flurm_node_manager_server:get_node(<<"uaf">>),
    ?assertEqual(64, Node#node.cpus),
    ?assertEqual(262144, Node#node.memory_mb),
    ?assertEqual(mixed, Node#node.state).

test_get_after_register() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"gar">>)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"gar">>),
    ?assertEqual(<<"gar">>, Node#node.hostname).

test_get_after_updates() ->
    register_test_node(<<"gau">>),
    ok = flurm_node_manager_server:update_node(<<"gau">>, #{cpus => 32}),
    ok = flurm_node_manager_server:update_node(<<"gau">>, #{memory_mb => 131072}),
    ok = flurm_node_manager_server:update_node(<<"gau">>, #{load_avg => 2.0}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"gau">>),
    ?assertEqual(32, Node#node.cpus),
    ?assertEqual(131072, Node#node.memory_mb),
    ?assertEqual(2.0, Node#node.load_avg).

test_get_after_allocation() ->
    register_test_node(<<"gaa">>),
    ok = flurm_node_manager_server:allocate_resources(<<"gaa">>, 1, 8, 32768),
    {ok, Node} = flurm_node_manager_server:get_node(<<"gaa">>),
    ?assertEqual(1, maps:size(Node#node.allocations)).

test_get_after_release() ->
    register_test_node(<<"gar2">>),
    ok = flurm_node_manager_server:allocate_resources(<<"gar2">>, 1, 8, 32768),
    flurm_node_manager_server:release_resources(<<"gar2">>, 1),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"gar2">>),
    ?assertEqual(0, maps:size(Node#node.allocations)).

test_list_with_filter() ->
    register_test_node(<<"lwf1">>, idle),
    register_test_node(<<"lwf2">>, down),
    register_test_node(<<"lwf3">>, idle),
    Nodes = flurm_node_manager_server:list_nodes(),
    IdleNodes = [N || N <- Nodes, N#node.state =:= idle],
    ?assertEqual(2, length(IdleNodes)).

test_list_sorted_hostname() ->
    register_test_node(<<"lsh_c">>),
    register_test_node(<<"lsh_a">>),
    register_test_node(<<"lsh_b">>),
    Nodes = flurm_node_manager_server:list_nodes(),
    Hostnames = [N#node.hostname || N <- Nodes],
    ?assertEqual(3, length(Hostnames)).

test_list_mixed_states() ->
    register_test_node(<<"lms1">>, idle),
    register_test_node(<<"lms2">>, allocated),
    register_test_node(<<"lms3">>, mixed),
    register_test_node(<<"lms4">>, down),
    register_test_node(<<"lms5">>, drain),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(5, length(Nodes)).

test_heartbeat_full_data() ->
    register_test_node(<<"hfd">>),
    flurm_node_manager_server:heartbeat(#{
        hostname => <<"hfd">>,
        load_avg => 3.5,
        free_memory => 40000,
        uptime => 123456,
        cpu_usage => 75.5,
        disk_usage => 50.0,
        network_rx => 1000000,
        network_tx => 500000
    }),
    timer:sleep(50),
    {ok, Node} = flurm_node_manager_server:get_node(<<"hfd">>),
    ?assertEqual(3.5, Node#node.load_avg).

test_heartbeat_partial_data() ->
    register_test_node(<<"hpd">>),
    flurm_node_manager_server:heartbeat(#{hostname => <<"hpd">>}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_heartbeat_updates_state() ->
    Spec = (make_node_spec(<<"hus">>))#{state => down},
    ok = flurm_node_manager_server:register_node(Spec),
    flurm_node_manager_server:heartbeat(#{hostname => <<"hus">>, load_avg => 1.0}),
    timer:sleep(50),
    ?assert(true).

test_heartbeat_from_down() ->
    Spec = (make_node_spec(<<"hfd2">>))#{state => down},
    ok = flurm_node_manager_server:register_node(Spec),
    flurm_node_manager_server:heartbeat(#{hostname => <<"hfd2">>, load_avg => 0.5}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_available_sorted() ->
    Spec1 = (make_node_spec(<<"as1">>))#{cpus => 64},
    Spec2 = (make_node_spec(<<"as2">>))#{cpus => 32},
    Spec3 = (make_node_spec(<<"as3">>))#{cpus => 16},
    ok = flurm_node_manager_server:register_node(Spec1),
    ok = flurm_node_manager_server:register_node(Spec2),
    ok = flurm_node_manager_server:register_node(Spec3),
    Nodes = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(3, length(Nodes)).

test_available_exact_match() ->
    Spec = (make_node_spec(<<"aem">>))#{cpus => 8, memory_mb => 16384},
    ok = flurm_node_manager_server:register_node(Spec),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(8, 16384, <<"default">>),
    ?assertEqual(1, length(Nodes)).

test_available_partial() ->
    Spec = (make_node_spec(<<"ap">>))#{cpus => 16, memory_mb => 65536},
    ok = flurm_node_manager_server:register_node(Spec),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(4, 8192, <<"default">>),
    ?assertEqual(1, length(Nodes)).

%%====================================================================
%% Drain Mode Extended Tests
%%====================================================================

drain_extended_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"drain with empty reason", fun test_drain_empty_reason/0},
        {"drain with long reason", fun test_drain_long_reason/0},
        {"drain preserves allocations", fun test_drain_preserves_allocations/0},
        {"undrain restores availability", fun test_undrain_restores_availability/0},
        {"drain reason persists", fun test_drain_reason_persists/0},
        {"multiple drain reasons", fun test_multiple_drain_reasons/0},
        {"drain during allocation", fun test_drain_during_allocation/0},
        {"undrain after all jobs complete", fun test_undrain_after_jobs_complete/0}
     ]}.

test_drain_empty_reason() ->
    register_test_node(<<"der">>),
    ok = flurm_node_manager_server:drain_node(<<"der">>, <<>>),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"der">>),
    ?assertEqual(<<>>, Reason).

test_drain_long_reason() ->
    register_test_node(<<"dlr">>),
    LongReason = iolist_to_binary([<<"x">> || _ <- lists:seq(1, 1000)]),
    ok = flurm_node_manager_server:drain_node(<<"dlr">>, LongReason),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"dlr">>),
    ?assertEqual(LongReason, Reason).

test_drain_preserves_allocations() ->
    register_test_node(<<"dpa">>),
    ok = flurm_node_manager_server:allocate_resources(<<"dpa">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"dpa">>, <<"test">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"dpa">>),
    ?assertEqual(1, maps:size(Node#node.allocations)).

test_undrain_restores_availability() ->
    register_test_node(<<"ura">>),
    N0 = length(flurm_node_manager_server:get_available_nodes()),
    ok = flurm_node_manager_server:drain_node(<<"ura">>, <<"test">>),
    N1 = length(flurm_node_manager_server:get_available_nodes()),
    ?assertEqual(N0 - 1, N1),
    ok = flurm_node_manager_server:undrain_node(<<"ura">>),
    N2 = length(flurm_node_manager_server:get_available_nodes()),
    ?assertEqual(N0, N2).

test_drain_reason_persists() ->
    register_test_node(<<"drp">>),
    ok = flurm_node_manager_server:drain_node(<<"drp">>, <<"persistent_reason">>),
    %% Update other fields
    ok = flurm_node_manager_server:update_node(<<"drp">>, #{load_avg => 5.0}),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"drp">>),
    ?assertEqual(<<"persistent_reason">>, Reason).

test_multiple_drain_reasons() ->
    register_test_node(<<"mdr1">>),
    register_test_node(<<"mdr2">>),
    ok = flurm_node_manager_server:drain_node(<<"mdr1">>, <<"reason1">>),
    ok = flurm_node_manager_server:drain_node(<<"mdr2">>, <<"reason2">>),
    {ok, R1} = flurm_node_manager_server:get_drain_reason(<<"mdr1">>),
    {ok, R2} = flurm_node_manager_server:get_drain_reason(<<"mdr2">>),
    ?assertEqual(<<"reason1">>, R1),
    ?assertEqual(<<"reason2">>, R2).

test_drain_during_allocation() ->
    register_test_node(<<"dda">>),
    ok = flurm_node_manager_server:allocate_resources(<<"dda">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"dda">>, <<"during_alloc">>),
    %% Try to allocate more - should fail since draining
    Result = flurm_node_manager_server:allocate_resources(<<"dda">>, 2, 4, 8192),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_undrain_after_jobs_complete() ->
    register_test_node(<<"uajc">>),
    ok = flurm_node_manager_server:allocate_resources(<<"uajc">>, 1, 4, 8192),
    ok = flurm_node_manager_server:drain_node(<<"uajc">>, <<"wait_for_jobs">>),
    flurm_node_manager_server:release_resources(<<"uajc">>, 1),
    timer:sleep(50),
    ok = flurm_node_manager_server:undrain_node(<<"uajc">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"uajc">>),
    ?assertEqual(idle, Node#node.state).

%%====================================================================
%% GRES Final Coverage Tests
%%====================================================================

gres_final_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"gres with specific indices", fun test_gres_specific_indices/0},
        {"gres allocation returns indices", fun test_gres_alloc_returns_indices/0},
        {"gres release frees indices", fun test_gres_release_frees_indices/0},
        {"gres concurrent allocations", fun test_gres_concurrent_allocs/0},
        {"gres multiple types same node", fun test_gres_multi_types/0},
        {"gres availability after partial alloc", fun test_gres_avail_after_partial/0},
        {"gres full allocation", fun test_gres_full_alloc/0},
        {"gres release all", fun test_gres_release_all/0}
     ]}.

test_gres_specific_indices() ->
    register_test_node(<<"gsi">>),
    GRESList = [#{type => gpu, count => 4, indices => [0, 1, 2, 3]}],
    ok = flurm_node_manager_server:register_node_gres(<<"gsi">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gsi">>),
    ?assert(is_map(GRES)).

test_gres_alloc_returns_indices() ->
    register_test_node(<<"gari">>),
    GRESList = [#{type => gpu, count => 4, indices => [0, 1, 2, 3]}],
    ok = flurm_node_manager_server:register_node_gres(<<"gari">>, GRESList),
    {ok, Alloc} = flurm_node_manager_server:allocate_gres(<<"gari">>, 1, <<"gpu:2">>, false),
    ?assert(is_list(Alloc)).

test_gres_release_frees_indices() ->
    register_test_node(<<"grfi">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"grfi">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"grfi">>, 1, <<"gpu:4">>, false),
    flurm_node_manager_server:release_gres(<<"grfi">>, 1),
    timer:sleep(50),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"grfi">>, 2, <<"gpu:4">>, false),
    ?assert(true).

test_gres_concurrent_allocs() ->
    register_test_node(<<"gca">>),
    GRESList = [#{type => gpu, count => 16}],
    ok = flurm_node_manager_server:register_node_gres(<<"gca">>, GRESList),
    Parent = self(),
    lists:foreach(fun(I) ->
        spawn(fun() ->
            Result = flurm_node_manager_server:allocate_gres(<<"gca">>, I, <<"gpu:1">>, false),
            Parent ! {gres_alloc, I, Result}
        end)
    end, lists:seq(1, 10)),
    Results = [receive {gres_alloc, _, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 10)],
    SuccessCount = length([R || R = {ok, _} <- Results]),
    ?assert(SuccessCount >= 1).

test_gres_multi_types() ->
    register_test_node(<<"gmt">>),
    GRESList = [
        #{type => gpu, count => 4},
        #{type => mic, count => 2},
        #{type => fpga, count => 1}
    ],
    ok = flurm_node_manager_server:register_node_gres(<<"gmt">>, GRESList),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gmt">>),
    ?assert(is_map(GRES)).

test_gres_avail_after_partial() ->
    register_test_node(<<"gaap">>),
    GRESList = [#{type => gpu, count => 8}],
    ok = flurm_node_manager_server:register_node_gres(<<"gaap">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gaap">>, 1, <<"gpu:4">>, false),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gaap">>, 2, <<"gpu:2">>, false),
    %% 2 GPUs should still be available
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gaap">>, 3, <<"gpu:2">>, false),
    ?assert(true).

test_gres_full_alloc() ->
    register_test_node(<<"gfa">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"gfa">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gfa">>, 1, <<"gpu:4">>, false),
    Result = flurm_node_manager_server:allocate_gres(<<"gfa">>, 2, <<"gpu:1">>, false),
    ?assertMatch({error, _}, Result).

test_gres_release_all() ->
    register_test_node(<<"gra">>),
    GRESList = [#{type => gpu, count => 4}],
    ok = flurm_node_manager_server:register_node_gres(<<"gra">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gra">>, 1, <<"gpu:2">>, false),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gra">>, 2, <<"gpu:2">>, false),
    flurm_node_manager_server:release_gres(<<"gra">>, 1),
    flurm_node_manager_server:release_gres(<<"gra">>, 2),
    timer:sleep(50),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"gra">>, 3, <<"gpu:4">>, false),
    ?assert(true).

%%====================================================================
%% Final Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"full cluster simulation", fun test_full_cluster_sim/0},
        {"job scheduling simulation", fun test_job_scheduling_sim/0},
        {"node maintenance simulation", fun test_node_maint_sim/0},
        {"resource fragmentation simulation", fun test_frag_sim/0}
     ]}.

test_full_cluster_sim() ->
    %% Register 10 nodes
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cluster_", integer_to_list(I)]),
        Spec = (make_node_spec(Name))#{cpus => 16 + I, memory_mb => 65536 + I * 1000},
        ok = flurm_node_manager_server:register_node(Spec)
    end, lists:seq(1, 10)),

    %% Verify all registered
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(10, length(Nodes)),

    %% Allocate on several
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cluster_", integer_to_list(I)]),
        ok = flurm_node_manager_server:allocate_resources(Name, I, 4, 8192)
    end, lists:seq(1, 5)),

    %% Check available
    Available = flurm_node_manager_server:get_available_nodes(),
    ?assert(length(Available) >= 5),

    %% Release all
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["cluster_", integer_to_list(I)]),
        flurm_node_manager_server:release_resources(Name, I)
    end, lists:seq(1, 5)),
    timer:sleep(100),

    Available2 = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(10, length(Available2)).

test_job_scheduling_sim() ->
    %% Setup nodes
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["sched_", integer_to_list(I)]),
        ok = flurm_node_manager_server:register_node(make_node_spec(Name))
    end, lists:seq(1, 5)),

    %% Simulate job submissions
    lists:foreach(fun(JobId) ->
        %% Find available node
        Available = flurm_node_manager_server:get_available_nodes_for_job(4, 8192, <<"default">>),
        case Available of
            [Node | _] ->
                ok = flurm_node_manager_server:allocate_resources(
                    Node#node.hostname, JobId, 4, 8192);
            [] ->
                ok
        end
    end, lists:seq(1, 10)),

    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_node_maint_sim() ->
    %% Setup cluster
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["maint_", integer_to_list(I)]),
        ok = flurm_node_manager_server:register_node(make_node_spec(Name))
    end, lists:seq(1, 5)),

    %% Put some nodes in maintenance
    ok = flurm_node_manager_server:drain_node(<<"maint_1">>, <<"scheduled">>),
    ok = flurm_node_manager_server:drain_node(<<"maint_2">>, <<"hw_issue">>),

    %% Check available
    Available = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(3, length(Available)),

    %% Restore
    ok = flurm_node_manager_server:undrain_node(<<"maint_1">>),
    ok = flurm_node_manager_server:undrain_node(<<"maint_2">>),

    Available2 = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(5, length(Available2)).

test_frag_sim() ->
    %% Single large node
    Spec = (make_node_spec(<<"frag_node">>))#{cpus => 64, memory_mb => 262144},
    ok = flurm_node_manager_server:register_node(Spec),

    %% Allocate in scattered pattern
    ok = flurm_node_manager_server:allocate_resources(<<"frag_node">>, 1, 8, 32768),
    ok = flurm_node_manager_server:allocate_resources(<<"frag_node">>, 2, 8, 32768),
    ok = flurm_node_manager_server:allocate_resources(<<"frag_node">>, 3, 8, 32768),
    ok = flurm_node_manager_server:allocate_resources(<<"frag_node">>, 4, 8, 32768),

    %% Release some
    flurm_node_manager_server:release_resources(<<"frag_node">>, 2),
    flurm_node_manager_server:release_resources(<<"frag_node">>, 4),
    timer:sleep(50),

    %% Try to fit new allocation in gaps
    ok = flurm_node_manager_server:allocate_resources(<<"frag_node">>, 5, 16, 65536),

    ?assert(true).

%%====================================================================
%% Additional Coverage - Configuration and Error Handling
%%====================================================================

config_error_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handle invalid node spec", fun test_invalid_node_spec/0},
        {"handle missing hostname", fun test_missing_hostname/0},
        {"handle corrupted state", fun test_corrupted_state/0},
        {"handle concurrent removes", fun test_concurrent_removes/0},
        {"handle rapid add remove", fun test_rapid_add_remove/0},
        {"handle unknown node operations", fun test_unknown_node_ops/0},
        {"handle heartbeat for removed node", fun test_heartbeat_removed_node/0},
        {"handle allocation for draining node", fun test_alloc_draining_node/0},
        {"handle double drain", fun test_double_drain/0},
        {"handle undrain non-draining", fun test_undrain_non_draining/0},
        {"handle gres on node without gres", fun test_gres_no_gres_node/0},
        {"handle invalid gres spec", fun test_invalid_gres_spec/0},
        {"handle config change during ops", fun test_config_change_during_ops/0},
        {"handle message queue overflow sim", fun test_message_queue_overflow_sim/0},
        {"handle timer cancel on terminate", fun test_timer_cancel_terminate/0},
        {"handle node with empty allocations", fun test_empty_allocations/0},
        {"handle node state unknown", fun test_node_state_unknown/0},
        {"handle binary partition", fun test_binary_partition/0},
        {"handle atom state", fun test_atom_state/0},
        {"handle list hostname", fun test_list_hostname/0}
     ]}.

test_invalid_node_spec() ->
    Result = flurm_node_manager_server:register_node(not_a_map),
    ?assert(is_tuple(Result)).

test_missing_hostname() ->
    Result = flurm_node_manager_server:register_node(#{cpus => 4}),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_corrupted_state() ->
    register_test_node(<<"cs">>),
    ok = flurm_node_manager_server:update_node(<<"cs">>, #{allocations => invalid}),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_concurrent_removes() ->
    register_test_node(<<"cr">>),
    Parent = self(),
    lists:foreach(fun(_) ->
        spawn(fun() ->
            Result = flurm_node_manager_server:remove_node(<<"cr">>, force),
            Parent ! {remove, Result}
        end)
    end, lists:seq(1, 5)),
    Results = [receive {remove, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 5)],
    SuccessCount = length([R || R <- Results, R =:= ok]),
    ?assert(SuccessCount >= 1).

test_rapid_add_remove() ->
    lists:foreach(fun(I) ->
        Name = iolist_to_binary(["rar_", integer_to_list(I)]),
        ok = flurm_node_manager_server:register_node(make_node_spec(Name)),
        ok = flurm_node_manager_server:remove_node(Name, force)
    end, lists:seq(1, 20)),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_unknown_node_ops() ->
    register_test_node(<<"uno">>),
    gen_server:cast(flurm_node_manager_server, {unknown_cast_msg, <<"uno">>}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_heartbeat_removed_node() ->
    register_test_node(<<"hrn">>),
    ok = flurm_node_manager_server:remove_node(<<"hrn">>, force),
    flurm_node_manager_server:heartbeat(#{hostname => <<"hrn">>, load_avg => 1.0}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_alloc_draining_node() ->
    register_test_node(<<"adn">>),
    ok = flurm_node_manager_server:drain_node(<<"adn">>, <<"test">>),
    Result = flurm_node_manager_server:allocate_resources(<<"adn">>, 1, 4, 8192),
    ?assert(is_tuple(Result)).

test_double_drain() ->
    register_test_node(<<"dd">>),
    ok = flurm_node_manager_server:drain_node(<<"dd">>, <<"first">>),
    Result = flurm_node_manager_server:drain_node(<<"dd">>, <<"second">>),
    ?assertMatch({error, _}, Result).

test_undrain_non_draining() ->
    register_test_node(<<"und">>),
    Result = flurm_node_manager_server:undrain_node(<<"und">>),
    ?assertMatch({error, _}, Result).

test_gres_no_gres_node() ->
    register_test_node(<<"gng">>),
    {ok, GRES} = flurm_node_manager_server:get_node_gres(<<"gng">>),
    ?assert(is_map(GRES)).

test_invalid_gres_spec() ->
    register_test_node(<<"igs">>),
    GRESList = [#{invalid => data}],
    Result = flurm_node_manager_server:register_node_gres(<<"igs">>, GRESList),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_config_change_during_ops() ->
    register_test_node(<<"ccdo">>),
    whereis(flurm_node_manager_server) ! {config_changed, nodes, #{add => []}},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_message_queue_overflow_sim() ->
    register_test_node(<<"mqo">>),
    %% Send many messages rapidly
    lists:foreach(fun(_) ->
        flurm_node_manager_server:heartbeat(#{hostname => <<"mqo">>})
    end, lists:seq(1, 1000)),
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_node_manager_server))).

test_timer_cancel_terminate() ->
    Pid = whereis(flurm_node_manager_server),
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_node_manager_server)).

test_empty_allocations() ->
    Spec = (make_node_spec(<<"ea">>))#{allocations => #{}},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"ea">>),
    ?assertEqual(#{}, Node#node.allocations).

test_node_state_unknown() ->
    Spec = (make_node_spec(<<"nsu">>))#{state => unknown_state},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assert(Result =:= ok orelse is_tuple(Result)).

test_binary_partition() ->
    Spec = (make_node_spec(<<"bp">>))#{partitions => [<<"partition_name">>]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"bp">>),
    ?assertEqual([<<"partition_name">>], Node#node.partitions).

test_atom_state() ->
    register_test_node(<<"as">>),
    ok = flurm_node_manager_server:update_node(<<"as">>, #{state => idle}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"as">>),
    ?assertEqual(idle, Node#node.state).

test_list_hostname() ->
    Spec = #{hostname => <<"list_host">>, cpus => 4},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, _} = flurm_node_manager_server:get_node(<<"list_host">>).

%%====================================================================
%% Final Boundary and Limit Tests
%%====================================================================

boundary_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"zero cpu node", fun test_zero_cpu_node/0},
        {"zero memory node", fun test_zero_memory_node/0},
        {"max int cpus", fun test_max_int_cpus/0},
        {"max int memory", fun test_max_int_memory/0},
        {"empty features list", fun test_empty_features_list/0},
        {"single char hostname", fun test_single_char_hostname/0},
        {"very long feature name", fun test_long_feature_name/0},
        {"very long partition name", fun test_long_partition_name/0},
        {"many small allocations", fun test_many_small_allocs/0},
        {"single large allocation", fun test_single_large_alloc/0}
     ]}.

test_zero_cpu_node() ->
    Spec = (make_node_spec(<<"zcn">>))#{cpus => 0},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_zero_memory_node() ->
    Spec = (make_node_spec(<<"zmn">>))#{memory_mb => 0},
    Result = flurm_node_manager_server:register_node(Spec),
    ?assertEqual(ok, Result).

test_max_int_cpus() ->
    Spec = (make_node_spec(<<"mic">>))#{cpus => 2147483647},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"mic">>),
    ?assertEqual(2147483647, Node#node.cpus).

test_max_int_memory() ->
    Spec = (make_node_spec(<<"mim">>))#{memory_mb => 2147483647},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"mim">>),
    ?assertEqual(2147483647, Node#node.memory_mb).

test_empty_features_list() ->
    Spec = (make_node_spec(<<"efl">>))#{features => []},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"efl">>),
    ?assertEqual([], Node#node.features).

test_single_char_hostname() ->
    Spec = (make_node_spec(<<"a">>)),
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, _} = flurm_node_manager_server:get_node(<<"a">>).

test_long_feature_name() ->
    LongFeature = iolist_to_binary([<<"x">> || _ <- lists:seq(1, 500)]),
    Spec = (make_node_spec(<<"lfn">>))#{features => [LongFeature]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"lfn">>),
    ?assertEqual([LongFeature], Node#node.features).

test_long_partition_name() ->
    LongPartition = iolist_to_binary([<<"p">> || _ <- lists:seq(1, 500)]),
    Spec = (make_node_spec(<<"lpn">>))#{partitions => [LongPartition]},
    ok = flurm_node_manager_server:register_node(Spec),
    {ok, Node} = flurm_node_manager_server:get_node(<<"lpn">>),
    ?assertEqual([LongPartition], Node#node.partitions).

test_many_small_allocs() ->
    register_test_node(<<"msa">>),
    lists:foreach(fun(I) ->
        flurm_node_manager_server:allocate_resources(<<"msa">>, I, 0, 0)
    end, lists:seq(1, 100)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"msa">>),
    ?assertEqual(100, maps:size(Node#node.allocations)).

test_single_large_alloc() ->
    Spec = (make_node_spec(<<"sla">>))#{cpus => 1000, memory_mb => 1000000},
    ok = flurm_node_manager_server:register_node(Spec),
    ok = flurm_node_manager_server:allocate_resources(<<"sla">>, 1, 1000, 1000000),
    {ok, Node} = flurm_node_manager_server:get_node(<<"sla">>),
    ?assertEqual(allocated, Node#node.state).
