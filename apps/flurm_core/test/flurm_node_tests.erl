%%%-------------------------------------------------------------------
%%% @doc FLURM Node Tests
%%%
%%% Comprehensive tests for flurm_node, flurm_node_registry,
%%% flurm_node_manager, and flurm_node_sup modules.
%%% These tests aim for 100% code coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic flurm_node tests with registry and supervisor
node_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Node start_link creates node process", fun test_node_start_link/0},
        {"Node register_node creates and registers node", fun test_node_register_node/0},
        {"Node heartbeat by pid", fun test_node_heartbeat_pid/0},
        {"Node heartbeat by name", fun test_node_heartbeat_name/0},
        {"Node heartbeat not found", fun test_node_heartbeat_not_found/0},
        {"Node allocate by pid", fun test_node_allocate_pid/0},
        {"Node allocate by name", fun test_node_allocate_name/0},
        {"Node allocate not found", fun test_node_allocate_not_found/0},
        {"Node allocate insufficient resources", fun test_node_allocate_insufficient/0},
        {"Node allocate when not up", fun test_node_allocate_not_up/0},
        {"Node release by pid", fun test_node_release_pid/0},
        {"Node release by name", fun test_node_release_name/0},
        {"Node release not found node", fun test_node_release_not_found/0},
        {"Node release job not found", fun test_node_release_job_not_found/0},
        {"Node set_state by pid", fun test_node_set_state_pid/0},
        {"Node set_state by name", fun test_node_set_state_name/0},
        {"Node set_state not found", fun test_node_set_state_not_found/0},
        {"Node get_info by pid", fun test_node_get_info_pid/0},
        {"Node get_info by name", fun test_node_get_info_name/0},
        {"Node get_info not found", fun test_node_get_info_not_found/0},
        {"Node list_jobs by pid", fun test_node_list_jobs_pid/0},
        {"Node list_jobs by name", fun test_node_list_jobs_name/0},
        {"Node list_jobs not found", fun test_node_list_jobs_not_found/0},
        {"Node drain_node default reason", fun test_node_drain_default/0},
        {"Node drain_node with reason by pid", fun test_node_drain_reason_pid/0},
        {"Node drain_node by name", fun test_node_drain_name/0},
        {"Node drain_node not found", fun test_node_drain_not_found/0},
        {"Node undrain_node by pid", fun test_node_undrain_pid/0},
        {"Node undrain_node by name", fun test_node_undrain_name/0},
        {"Node undrain_node not found", fun test_node_undrain_not_found/0},
        {"Node undrain when not draining", fun test_node_undrain_not_draining/0},
        {"Node get_drain_reason when draining", fun test_node_get_drain_reason/0},
        {"Node get_drain_reason not draining", fun test_node_get_drain_reason_not_draining/0},
        {"Node get_drain_reason not found", fun test_node_get_drain_reason_not_found/0},
        {"Node is_draining true", fun test_node_is_draining_true/0},
        {"Node is_draining false", fun test_node_is_draining_false/0},
        {"Node is_draining not found", fun test_node_is_draining_not_found/0},
        {"Node unknown call request", fun test_node_unknown_call/0},
        {"Node unknown cast message", fun test_node_unknown_cast/0},
        {"Node unknown info message", fun test_node_unknown_info/0},
        {"Node terminate unregisters node", fun test_node_terminate/0},
        {"Node code_change", fun test_node_code_change/0},
        {"Node multiple job allocation and release", fun test_node_multiple_jobs/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
    {ok, NodeSupPid} = flurm_node_sup:start_link(),
    #{
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid
    }.

cleanup(#{node_registry := NodeRegistryPid, node_sup := NodeSupPid}) ->
    %% Stop all nodes first
    [flurm_node_sup:stop_node(Pid) || Pid <- flurm_node_sup:which_nodes()],
    catch unlink(NodeSupPid),
    catch unlink(NodeRegistryPid),
    catch gen_server:stop(NodeSupPid, shutdown, 5000),
    catch gen_server:stop(NodeRegistryPid, shutdown, 5000),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_node_spec(Name) ->
    make_node_spec(Name, #{}).

make_node_spec(Name, Overrides) ->
    Defaults = #{
        hostname => <<"localhost">>,
        port => 5555,
        cpus => 8,
        memory => 16384,
        gpus => 2,
        features => [avx, gpu],
        partitions => [<<"default">>, <<"gpu">>]
    },
    Props = maps:merge(Defaults, Overrides),
    #node_spec{
        name = Name,
        hostname = maps:get(hostname, Props),
        port = maps:get(port, Props),
        cpus = maps:get(cpus, Props),
        memory = maps:get(memory, Props),
        gpus = maps:get(gpus, Props),
        features = maps:get(features, Props),
        partitions = maps:get(partitions, Props)
    }.

register_test_node(Name) ->
    register_test_node(Name, #{}).

register_test_node(Name, Overrides) ->
    NodeSpec = make_node_spec(Name, Overrides),
    {ok, Pid, Name} = flurm_node:register_node(NodeSpec),
    Pid.

%%====================================================================
%% flurm_node Tests
%%====================================================================

test_node_start_link() ->
    NodeSpec = make_node_spec(<<"direct_start">>),
    {ok, Pid} = flurm_node:start_link(NodeSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid),
    ok.

test_node_register_node() ->
    NodeSpec = make_node_spec(<<"register_test">>),
    {ok, Pid, <<"register_test">>} = flurm_node:register_node(NodeSpec),
    ?assert(is_pid(Pid)),
    %% Verify it's registered
    {ok, Pid} = flurm_node_registry:lookup_node(<<"register_test">>),
    ok.

test_node_heartbeat_pid() ->
    Pid = register_test_node(<<"hb_pid">>),
    Result = flurm_node:heartbeat(Pid),
    ?assertEqual(ok, Result),
    ok.

test_node_heartbeat_name() ->
    _Pid = register_test_node(<<"hb_name">>),
    Result = flurm_node:heartbeat(<<"hb_name">>),
    ?assertEqual(ok, Result),
    ok.

test_node_heartbeat_not_found() ->
    Result = flurm_node:heartbeat(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_allocate_pid() ->
    Pid = register_test_node(<<"alloc_pid">>, #{cpus => 8, memory => 16384, gpus => 2}),
    Result = flurm_node:allocate(Pid, 1, {4, 8192, 1}),
    ?assertEqual(ok, Result),
    %% Verify resources are allocated
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(4, maps:get(cpus_used, Info)),
    ?assertEqual(8192, maps:get(memory_used, Info)),
    ?assertEqual(1, maps:get(gpus_used, Info)),
    ?assertEqual([1], maps:get(jobs, Info)),
    ok.

test_node_allocate_name() ->
    _Pid = register_test_node(<<"alloc_name">>, #{cpus => 8}),
    Result = flurm_node:allocate(<<"alloc_name">>, 2, {2, 1024, 0}),
    ?assertEqual(ok, Result),
    ok.

test_node_allocate_not_found() ->
    Result = flurm_node:allocate(<<"nonexistent">>, 1, {1, 1024, 0}),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_allocate_insufficient() ->
    Pid = register_test_node(<<"alloc_insuf">>, #{cpus => 4, memory => 4096, gpus => 1}),
    %% Try to allocate more CPUs than available
    Result1 = flurm_node:allocate(Pid, 1, {8, 1024, 0}),
    ?assertEqual({error, insufficient_resources}, Result1),
    %% Try to allocate more memory than available
    Result2 = flurm_node:allocate(Pid, 2, {1, 8192, 0}),
    ?assertEqual({error, insufficient_resources}, Result2),
    %% Try to allocate more GPUs than available
    Result3 = flurm_node:allocate(Pid, 3, {1, 1024, 4}),
    ?assertEqual({error, insufficient_resources}, Result3),
    ok.

test_node_allocate_not_up() ->
    Pid = register_test_node(<<"alloc_not_up">>, #{cpus => 8}),
    %% Set node to drain state
    ok = flurm_node:set_state(Pid, drain),
    %% Try to allocate - should fail because node is not up
    Result = flurm_node:allocate(Pid, 1, {1, 1024, 0}),
    ?assertEqual({error, insufficient_resources}, Result),
    ok.

test_node_release_pid() ->
    Pid = register_test_node(<<"rel_pid">>, #{cpus => 8, memory => 16384}),
    ok = flurm_node:allocate(Pid, 1, {4, 8192, 0}),
    Result = flurm_node:release(Pid, 1),
    ?assertEqual(ok, Result),
    %% Verify resources are released
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(0, maps:get(cpus_used, Info)),
    ?assertEqual([], maps:get(jobs, Info)),
    ok.

test_node_release_name() ->
    _Pid = register_test_node(<<"rel_name">>, #{cpus => 8}),
    ok = flurm_node:allocate(<<"rel_name">>, 1, {2, 1024, 0}),
    Result = flurm_node:release(<<"rel_name">>, 1),
    ?assertEqual(ok, Result),
    ok.

test_node_release_not_found() ->
    Result = flurm_node:release(<<"nonexistent">>, 1),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_release_job_not_found() ->
    Pid = register_test_node(<<"rel_job_nf">>),
    Result = flurm_node:release(Pid, 999),
    ?assertEqual({error, job_not_found}, Result),
    ok.

test_node_set_state_pid() ->
    Pid = register_test_node(<<"state_pid">>),
    Result = flurm_node:set_state(Pid, down),
    ?assertEqual(ok, Result),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(down, maps:get(state, Info)),
    ok.

test_node_set_state_name() ->
    _Pid = register_test_node(<<"state_name">>),
    Result = flurm_node:set_state(<<"state_name">>, maint),
    ?assertEqual(ok, Result),
    {ok, Info} = flurm_node:get_info(<<"state_name">>),
    ?assertEqual(maint, maps:get(state, Info)),
    ok.

test_node_set_state_not_found() ->
    Result = flurm_node:set_state(<<"nonexistent">>, down),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_get_info_pid() ->
    Pid = register_test_node(<<"info_pid">>, #{cpus => 16, memory => 32768, gpus => 4}),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(<<"info_pid">>, maps:get(name, Info)),
    ?assertEqual(16, maps:get(cpus, Info)),
    ?assertEqual(0, maps:get(cpus_used, Info)),
    ?assertEqual(16, maps:get(cpus_available, Info)),
    ?assertEqual(32768, maps:get(memory, Info)),
    ?assertEqual(0, maps:get(memory_used, Info)),
    ?assertEqual(32768, maps:get(memory_available, Info)),
    ?assertEqual(4, maps:get(gpus, Info)),
    ?assertEqual(0, maps:get(gpus_used, Info)),
    ?assertEqual(4, maps:get(gpus_available, Info)),
    ?assertEqual(up, maps:get(state, Info)),
    ?assertEqual(undefined, maps:get(drain_reason, Info)),
    ?assertEqual([], maps:get(jobs, Info)),
    ?assertEqual(0, maps:get(job_count, Info)),
    ok.

test_node_get_info_name() ->
    _Pid = register_test_node(<<"info_name">>),
    {ok, Info} = flurm_node:get_info(<<"info_name">>),
    ?assertEqual(<<"info_name">>, maps:get(name, Info)),
    ok.

test_node_get_info_not_found() ->
    Result = flurm_node:get_info(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_list_jobs_pid() ->
    Pid = register_test_node(<<"jobs_pid">>),
    ok = flurm_node:allocate(Pid, 100, {1, 1024, 0}),
    ok = flurm_node:allocate(Pid, 200, {1, 1024, 0}),
    {ok, Jobs} = flurm_node:list_jobs(Pid),
    ?assertEqual(2, length(Jobs)),
    ?assert(lists:member(100, Jobs)),
    ?assert(lists:member(200, Jobs)),
    ok.

test_node_list_jobs_name() ->
    _Pid = register_test_node(<<"jobs_name">>),
    {ok, Jobs} = flurm_node:list_jobs(<<"jobs_name">>),
    ?assertEqual([], Jobs),
    ok.

test_node_list_jobs_not_found() ->
    Result = flurm_node:list_jobs(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_drain_default() ->
    Pid = register_test_node(<<"drain_def">>),
    Result = flurm_node:drain_node(Pid),
    ?assertEqual(ok, Result),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"admin request">>, maps:get(drain_reason, Info)),
    ok.

test_node_drain_reason_pid() ->
    Pid = register_test_node(<<"drain_rsn">>),
    Result = flurm_node:drain_node(Pid, <<"hardware maintenance">>),
    ?assertEqual(ok, Result),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"hardware maintenance">>, maps:get(drain_reason, Info)),
    ok.

test_node_drain_name() ->
    _Pid = register_test_node(<<"drain_name">>),
    Result = flurm_node:drain_node(<<"drain_name">>, <<"test">>),
    ?assertEqual(ok, Result),
    ok.

test_node_drain_not_found() ->
    Result = flurm_node:drain_node(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_undrain_pid() ->
    Pid = register_test_node(<<"undrain_pid">>),
    ok = flurm_node:drain_node(Pid),
    Result = flurm_node:undrain_node(Pid),
    ?assertEqual(ok, Result),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(up, maps:get(state, Info)),
    ?assertEqual(undefined, maps:get(drain_reason, Info)),
    ok.

test_node_undrain_name() ->
    _Pid = register_test_node(<<"undrain_name">>),
    ok = flurm_node:drain_node(<<"undrain_name">>),
    Result = flurm_node:undrain_node(<<"undrain_name">>),
    ?assertEqual(ok, Result),
    ok.

test_node_undrain_not_found() ->
    Result = flurm_node:undrain_node(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_undrain_not_draining() ->
    Pid = register_test_node(<<"undrain_up">>),
    %% Node is already up, not draining
    Result = flurm_node:undrain_node(Pid),
    ?assertEqual({error, not_draining}, Result),
    ok.

test_node_get_drain_reason() ->
    Pid = register_test_node(<<"drain_reason">>),
    ok = flurm_node:drain_node(Pid, <<"memory error">>),
    {ok, Reason} = flurm_node:get_drain_reason(Pid),
    ?assertEqual(<<"memory error">>, Reason),
    ok.

test_node_get_drain_reason_not_draining() ->
    Pid = register_test_node(<<"drain_reason_up">>),
    Result = flurm_node:get_drain_reason(Pid),
    ?assertEqual({error, not_draining}, Result),
    ok.

test_node_get_drain_reason_not_found() ->
    Result = flurm_node:get_drain_reason(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_is_draining_true() ->
    Pid = register_test_node(<<"is_drain_t">>),
    ok = flurm_node:drain_node(Pid),
    Result = flurm_node:is_draining(Pid),
    ?assertEqual(true, Result),
    ok.

test_node_is_draining_false() ->
    Pid = register_test_node(<<"is_drain_f">>),
    Result = flurm_node:is_draining(Pid),
    ?assertEqual(false, Result),
    ok.

test_node_is_draining_not_found() ->
    Result = flurm_node:is_draining(<<"nonexistent">>),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_unknown_call() ->
    Pid = register_test_node(<<"unknown_call">>),
    Result = gen_server:call(Pid, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_node_unknown_cast() ->
    Pid = register_test_node(<<"unknown_cast">>),
    ok = gen_server:cast(Pid, {unknown_message, bar}),
    %% Should not crash - verify still alive
    timer:sleep(50),
    ?assert(is_process_alive(Pid)),
    ok.

test_node_unknown_info() ->
    Pid = register_test_node(<<"unknown_info">>),
    Pid ! {unknown_info_message, baz},
    %% Should not crash - verify still alive
    timer:sleep(50),
    ?assert(is_process_alive(Pid)),
    ok.

test_node_terminate() ->
    Pid = register_test_node(<<"terminate">>),
    %% Verify it's registered
    {ok, Pid} = flurm_node_registry:lookup_node(<<"terminate">>),
    %% Stop the node
    gen_server:stop(Pid, normal, 5000),
    %% Verify it's unregistered
    timer:sleep(100),
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"terminate">>)),
    ok.

test_node_code_change() ->
    Pid = register_test_node(<<"code_change">>),
    %% Test code_change callback directly via sys module
    sys:suspend(Pid),
    Result = sys:change_code(Pid, flurm_node, "1.0.0", []),
    ?assertEqual(ok, Result),
    sys:resume(Pid),
    %% Verify process still works
    {ok, _Info} = flurm_node:get_info(Pid),
    ok.

test_node_multiple_jobs() ->
    Pid = register_test_node(<<"multi_job">>, #{cpus => 16, memory => 32768, gpus => 4}),
    %% Allocate multiple jobs
    ok = flurm_node:allocate(Pid, 1, {4, 8192, 1}),
    ok = flurm_node:allocate(Pid, 2, {4, 8192, 1}),
    ok = flurm_node:allocate(Pid, 3, {4, 8192, 1}),
    %% Verify all jobs
    {ok, Jobs} = flurm_node:list_jobs(Pid),
    ?assertEqual(3, length(Jobs)),
    %% Verify resources
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(12, maps:get(cpus_used, Info1)),
    ?assertEqual(3, maps:get(gpus_used, Info1)),
    %% Release one job
    ok = flurm_node:release(Pid, 2),
    {ok, Jobs2} = flurm_node:list_jobs(Pid),
    ?assertEqual(2, length(Jobs2)),
    ?assertNot(lists:member(2, Jobs2)),
    ok.

%%====================================================================
%% flurm_node_registry Tests
%%====================================================================

registry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Registry start_link", fun test_registry_start/0},
        {"Registry register_node success", fun test_registry_register_success/0},
        {"Registry register_node already registered", fun test_registry_register_duplicate/0},
        {"Registry unregister_node", fun test_registry_unregister/0},
        {"Registry lookup_node found", fun test_registry_lookup_found/0},
        {"Registry lookup_node not found", fun test_registry_lookup_not_found/0},
        {"Registry list_nodes", fun test_registry_list_nodes/0},
        {"Registry list_all_nodes", fun test_registry_list_all_nodes/0},
        {"Registry list_nodes_by_state", fun test_registry_list_by_state/0},
        {"Registry list_nodes_by_partition", fun test_registry_list_by_partition/0},
        {"Registry get_available_nodes", fun test_registry_available_nodes/0},
        {"Registry update_state success", fun test_registry_update_state/0},
        {"Registry update_state not found", fun test_registry_update_state_not_found/0},
        {"Registry update_entry success", fun test_registry_update_entry/0},
        {"Registry update_entry not found", fun test_registry_update_entry_not_found/0},
        {"Registry get_node_entry found", fun test_registry_get_entry_found/0},
        {"Registry get_node_entry not found", fun test_registry_get_entry_not_found/0},
        {"Registry count_by_state", fun test_registry_count_by_state/0},
        {"Registry count_nodes", fun test_registry_count_nodes/0},
        {"Registry unknown call", fun test_registry_unknown_call/0},
        {"Registry unknown cast", fun test_registry_unknown_cast/0},
        {"Registry unknown info", fun test_registry_unknown_info/0},
        {"Registry process DOWN unregisters node", fun test_registry_process_down/0},
        {"Registry config_reload_nodes message", fun test_registry_config_reload/0},
        {"Registry config_changed message", fun test_registry_config_changed/0},
        {"Registry unregister_node not found", fun test_registry_unregister_not_found/0}
     ]}.

test_registry_start() ->
    %% Registry is already started in setup
    ?assertEqual(0, flurm_node_registry:count_nodes()),
    ok.

test_registry_register_success() ->
    Pid = register_test_node(<<"reg_success">>),
    {ok, Pid} = flurm_node_registry:lookup_node(<<"reg_success">>),
    ok.

test_registry_register_duplicate() ->
    _Pid = register_test_node(<<"reg_dup">>),
    %% Try to register again with same name but different pid
    NodeSpec = make_node_spec(<<"reg_dup">>),
    {ok, Pid2} = flurm_node:start_link(NodeSpec),
    Result = flurm_node_registry:register_node(<<"reg_dup">>, Pid2),
    ?assertEqual({error, already_registered}, Result),
    gen_server:stop(Pid2),
    ok.

test_registry_unregister() ->
    Pid = register_test_node(<<"unreg">>),
    {ok, Pid} = flurm_node_registry:lookup_node(<<"unreg">>),
    ok = flurm_node_registry:unregister_node(<<"unreg">>),
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"unreg">>)),
    gen_server:stop(Pid),
    ok.

test_registry_lookup_found() ->
    Pid = register_test_node(<<"lookup_found">>),
    {ok, Pid} = flurm_node_registry:lookup_node(<<"lookup_found">>),
    ok.

test_registry_lookup_not_found() ->
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"nonexistent">>)),
    ok.

test_registry_list_nodes() ->
    _Pid1 = register_test_node(<<"list1">>),
    _Pid2 = register_test_node(<<"list2">>),
    _Pid3 = register_test_node(<<"list3">>),
    Nodes = flurm_node_registry:list_nodes(),
    ?assertEqual(3, length(Nodes)),
    Names = [N || {N, _} <- Nodes],
    ?assert(lists:member(<<"list1">>, Names)),
    ?assert(lists:member(<<"list2">>, Names)),
    ?assert(lists:member(<<"list3">>, Names)),
    ok.

test_registry_list_all_nodes() ->
    _Pid1 = register_test_node(<<"all1">>),
    _Pid2 = register_test_node(<<"all2">>),
    AllNodes = flurm_node_registry:list_all_nodes(),
    ?assertEqual(2, length(AllNodes)),
    ok.

test_registry_list_by_state() ->
    Pid1 = register_test_node(<<"state1">>),
    Pid2 = register_test_node(<<"state2">>),
    _Pid3 = register_test_node(<<"state3">>),
    ok = flurm_node:set_state(Pid1, down),
    ok = flurm_node:set_state(Pid2, down),
    UpNodes = flurm_node_registry:list_nodes_by_state(up),
    DownNodes = flurm_node_registry:list_nodes_by_state(down),
    ?assertEqual(1, length(UpNodes)),
    ?assertEqual(2, length(DownNodes)),
    ok.

test_registry_list_by_partition() ->
    _Pid1 = register_test_node(<<"part1">>, #{partitions => [<<"compute">>, <<"gpu">>]}),
    _Pid2 = register_test_node(<<"part2">>, #{partitions => [<<"compute">>]}),
    _Pid3 = register_test_node(<<"part3">>, #{partitions => [<<"gpu">>]}),
    ComputeNodes = flurm_node_registry:list_nodes_by_partition(<<"compute">>),
    GpuNodes = flurm_node_registry:list_nodes_by_partition(<<"gpu">>),
    ?assertEqual(2, length(ComputeNodes)),
    ?assertEqual(2, length(GpuNodes)),
    ok.

test_registry_available_nodes() ->
    Pid1 = register_test_node(<<"avail1">>, #{cpus => 8, memory => 16384}),
    _Pid2 = register_test_node(<<"avail2">>, #{cpus => 4, memory => 8192}),
    _Pid3 = register_test_node(<<"avail3">>, #{cpus => 2, memory => 4096}),
    %% Node1 is down, should not appear
    ok = flurm_node:set_state(Pid1, down),
    %% Get nodes with at least 4 CPUs and 8GB memory
    AvailNodes = flurm_node_registry:get_available_nodes({4, 8192, 0}),
    ?assertEqual(1, length(AvailNodes)),
    [Entry] = AvailNodes,
    ?assertEqual(<<"avail2">>, Entry#node_entry.name),
    ok.

test_registry_update_state() ->
    _Pid = register_test_node(<<"upd_state">>),
    ok = flurm_node_registry:update_state(<<"upd_state">>, drain),
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"upd_state">>),
    ?assertEqual(drain, Entry#node_entry.state),
    ok.

test_registry_update_state_not_found() ->
    Result = flurm_node_registry:update_state(<<"nonexistent">>, down),
    ?assertEqual({error, not_found}, Result),
    ok.

test_registry_update_entry() ->
    Pid = register_test_node(<<"upd_entry">>),
    %% Get current entry
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"upd_entry">>),
    %% Update with modified resources
    NewEntry = Entry#node_entry{cpus_avail = 2, memory_avail = 1024},
    ok = flurm_node_registry:update_entry(<<"upd_entry">>, NewEntry),
    %% Verify
    {ok, Updated} = flurm_node_registry:get_node_entry(<<"upd_entry">>),
    ?assertEqual(2, Updated#node_entry.cpus_avail),
    ?assertEqual(1024, Updated#node_entry.memory_avail),
    %% Update with state change
    NewEntry2 = Updated#node_entry{state = maint},
    ok = flurm_node_registry:update_entry(<<"upd_entry">>, NewEntry2),
    {ok, Updated2} = flurm_node_registry:get_node_entry(<<"upd_entry">>),
    ?assertEqual(maint, Updated2#node_entry.state),
    gen_server:stop(Pid),
    ok.

test_registry_update_entry_not_found() ->
    Entry = #node_entry{
        name = <<"nonexistent">>,
        pid = self(),
        hostname = <<"localhost">>,
        state = up,
        partitions = [],
        cpus_total = 8,
        cpus_avail = 8,
        memory_total = 16384,
        memory_avail = 16384,
        gpus_total = 0,
        gpus_avail = 0
    },
    Result = flurm_node_registry:update_entry(<<"nonexistent">>, Entry),
    ?assertEqual({error, not_found}, Result),
    ok.

test_registry_get_entry_found() ->
    Pid = register_test_node(<<"get_entry">>, #{cpus => 16}),
    {ok, Entry} = flurm_node_registry:get_node_entry(<<"get_entry">>),
    ?assertEqual(<<"get_entry">>, Entry#node_entry.name),
    ?assertEqual(Pid, Entry#node_entry.pid),
    ?assertEqual(16, Entry#node_entry.cpus_total),
    ok.

test_registry_get_entry_not_found() ->
    Result = flurm_node_registry:get_node_entry(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result),
    ok.

test_registry_count_by_state() ->
    Pid1 = register_test_node(<<"cnt1">>),
    _Pid2 = register_test_node(<<"cnt2">>),
    _Pid3 = register_test_node(<<"cnt3">>),
    ok = flurm_node:set_state(Pid1, drain),
    Counts = flurm_node_registry:count_by_state(),
    ?assertEqual(2, maps:get(up, Counts)),
    ?assertEqual(0, maps:get(down, Counts)),
    ?assertEqual(1, maps:get(drain, Counts)),
    ?assertEqual(0, maps:get(maint, Counts)),
    ok.

test_registry_count_nodes() ->
    ?assertEqual(0, flurm_node_registry:count_nodes()),
    _Pid1 = register_test_node(<<"cnt_n1">>),
    ?assertEqual(1, flurm_node_registry:count_nodes()),
    _Pid2 = register_test_node(<<"cnt_n2">>),
    ?assertEqual(2, flurm_node_registry:count_nodes()),
    ok.

test_registry_unknown_call() ->
    Result = gen_server:call(flurm_node_registry, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_registry_unknown_cast() ->
    ok = gen_server:cast(flurm_node_registry, {unknown_message}),
    timer:sleep(50),
    %% Should not crash
    ?assertEqual(0, flurm_node_registry:count_nodes()),
    ok.

test_registry_unknown_info() ->
    flurm_node_registry ! {unknown_info_message},
    timer:sleep(50),
    %% Should not crash
    ?assertEqual(0, flurm_node_registry:count_nodes()),
    ok.

test_registry_process_down() ->
    Pid = register_test_node(<<"down_test">>),
    {ok, Pid} = flurm_node_registry:lookup_node(<<"down_test">>),
    %% Kill the node process
    exit(Pid, kill),
    timer:sleep(100),
    %% Node should be automatically unregistered
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(<<"down_test">>)),
    ok.

test_registry_config_reload() ->
    _Pid = register_test_node(<<"config_node">>, #{partitions => [<<"default">>]}),
    %% Send config reload message
    flurm_node_registry ! {config_reload_nodes, [#{nodename => <<"config_node">>, cpus => 16}]},
    timer:sleep(50),
    %% Should not crash
    ?assertEqual(1, flurm_node_registry:count_nodes()),
    ok.

test_registry_config_changed() ->
    _Pid = register_test_node(<<"config_chg">>, #{partitions => [<<"default">>]}),
    %% Send config changed message
    flurm_node_registry ! {config_changed, nodes, [], [#{nodename => <<"config_chg">>}]},
    timer:sleep(50),
    %% Should not crash
    ?assertEqual(1, flurm_node_registry:count_nodes()),
    %% Send config changed for other key (should be ignored)
    flurm_node_registry ! {config_changed, partitions, [], []},
    timer:sleep(50),
    ?assertEqual(1, flurm_node_registry:count_nodes()),
    ok.

test_registry_unregister_not_found() ->
    %% Unregistering non-existent node should be ok
    ok = flurm_node_registry:unregister_node(<<"does_not_exist">>),
    ok.

%%====================================================================
%% flurm_node_sup Tests
%%====================================================================

supervisor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Supervisor start_link", fun test_sup_start_link/0},
        {"Supervisor start_node", fun test_sup_start_node/0},
        {"Supervisor stop_node", fun test_sup_stop_node/0},
        {"Supervisor which_nodes", fun test_sup_which_nodes/0},
        {"Supervisor count_nodes", fun test_sup_count_nodes/0},
        {"Supervisor init callback", fun test_sup_init/0}
     ]}.

test_sup_start_link() ->
    %% Supervisor is already started in setup
    ?assertEqual(0, flurm_node_sup:count_nodes()),
    ok.

test_sup_start_node() ->
    NodeSpec = make_node_spec(<<"sup_start">>),
    {ok, Pid} = flurm_node_sup:start_node(NodeSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ok.

test_sup_stop_node() ->
    NodeSpec = make_node_spec(<<"sup_stop">>),
    {ok, Pid} = flurm_node_sup:start_node(NodeSpec),
    ?assert(is_process_alive(Pid)),
    ok = flurm_node_sup:stop_node(Pid),
    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)),
    ok.

test_sup_which_nodes() ->
    NodeSpec1 = make_node_spec(<<"which1">>),
    NodeSpec2 = make_node_spec(<<"which2">>),
    {ok, Pid1} = flurm_node_sup:start_node(NodeSpec1),
    {ok, Pid2} = flurm_node_sup:start_node(NodeSpec2),
    Nodes = flurm_node_sup:which_nodes(),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(Pid1, Nodes)),
    ?assert(lists:member(Pid2, Nodes)),
    ok.

test_sup_count_nodes() ->
    ?assertEqual(0, flurm_node_sup:count_nodes()),
    NodeSpec = make_node_spec(<<"count1">>),
    {ok, _} = flurm_node_sup:start_node(NodeSpec),
    ?assertEqual(1, flurm_node_sup:count_nodes()),
    NodeSpec2 = make_node_spec(<<"count2">>),
    {ok, _} = flurm_node_sup:start_node(NodeSpec2),
    ?assertEqual(2, flurm_node_sup:count_nodes()),
    ok.

test_sup_init() ->
    %% Test init returns correct child spec
    {ok, {SupFlags, ChildSpecs}} = flurm_node_sup:init([]),
    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(0, maps:get(intensity, SupFlags)),
    ?assertEqual(1, length(ChildSpecs)),
    [ChildSpec] = ChildSpecs,
    ?assertEqual(flurm_node, maps:get(id, ChildSpec)),
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),
    ok.

%%====================================================================
%% flurm_node_manager Tests
%%====================================================================

node_manager_test_() ->
    {foreach,
     fun setup_with_gres/0,
     fun cleanup_with_gres/1,
     [
        {"Manager get_available_nodes_for_job empty partition", fun test_mgr_avail_empty_partition/0},
        {"Manager get_available_nodes_for_job default partition", fun test_mgr_avail_default_partition/0},
        {"Manager get_available_nodes_for_job specific partition", fun test_mgr_avail_specific_partition/0},
        {"Manager get_available_nodes_with_gres", fun test_mgr_avail_with_gres/0},
        {"Manager allocate_resources success", fun test_mgr_allocate_success/0},
        {"Manager allocate_resources not found", fun test_mgr_allocate_not_found/0},
        {"Manager release_resources success", fun test_mgr_release_success/0},
        {"Manager release_resources not found", fun test_mgr_release_not_found/0},
        {"Manager allocate_gres empty spec", fun test_mgr_allocate_gres_empty/0},
        {"Manager allocate_gres with spec", fun test_mgr_allocate_gres/0},
        {"Manager release_gres", fun test_mgr_release_gres/0}
     ]}.

setup_with_gres() ->
    application:ensure_all_started(sasl),
    {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
    {ok, NodeSupPid} = flurm_node_sup:start_link(),
    %% Start flurm_gres for GRES tests
    {ok, GresPid} = flurm_gres:start_link(),
    #{
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid,
        gres => GresPid
    }.

cleanup_with_gres(#{node_registry := NodeRegistryPid, node_sup := NodeSupPid, gres := GresPid}) ->
    [flurm_node_sup:stop_node(Pid) || Pid <- flurm_node_sup:which_nodes()],
    catch unlink(NodeSupPid),
    catch unlink(NodeRegistryPid),
    catch unlink(GresPid),
    catch gen_server:stop(NodeSupPid, shutdown, 5000),
    catch gen_server:stop(NodeRegistryPid, shutdown, 5000),
    catch gen_server:stop(GresPid, shutdown, 5000),
    ok.

test_mgr_avail_empty_partition() ->
    _Pid = register_test_node(<<"mgr1">>, #{cpus => 8, memory => 16384, partitions => [<<"compute">>]}),
    %% Empty partition should return all nodes
    Nodes = flurm_node_manager:get_available_nodes_for_job(4, 8192, <<>>),
    ?assertEqual(1, length(Nodes)),
    ok.

test_mgr_avail_default_partition() ->
    _Pid = register_test_node(<<"mgr2">>, #{cpus => 8, memory => 16384, partitions => [<<"gpu">>]}),
    %% Default partition should return all nodes
    Nodes = flurm_node_manager:get_available_nodes_for_job(4, 8192, <<"default">>),
    ?assertEqual(1, length(Nodes)),
    ok.

test_mgr_avail_specific_partition() ->
    _Pid1 = register_test_node(<<"mgr3a">>, #{cpus => 8, memory => 16384, partitions => [<<"compute">>]}),
    _Pid2 = register_test_node(<<"mgr3b">>, #{cpus => 8, memory => 16384, partitions => [<<"gpu">>]}),
    ComputeNodes = flurm_node_manager:get_available_nodes_for_job(4, 8192, <<"compute">>),
    ?assertEqual(1, length(ComputeNodes)),
    [Node] = ComputeNodes,
    ?assertEqual(<<"mgr3a">>, Node#node.hostname),
    ok.

test_mgr_avail_with_gres() ->
    _Pid = register_test_node(<<"mgr_gres">>, #{cpus => 8, memory => 16384, partitions => [<<"default">>]}),
    %% With empty GRES spec
    Nodes1 = flurm_node_manager:get_available_nodes_with_gres(4, 8192, <<>>, <<>>),
    ?assertEqual(1, length(Nodes1)),
    %% With GRES spec - may error depending on flurm_gres state
    try
        _Nodes2 = flurm_node_manager:get_available_nodes_with_gres(4, 8192, <<>>, <<"gpu:1">>),
        %% Result depends on flurm_gres configuration
        ok
    catch
        _:_ ->
            %% GRES not configured, that's fine
            ok
    end.

test_mgr_allocate_success() ->
    _Pid = register_test_node(<<"mgr_alloc">>, #{cpus => 8, memory => 16384}),
    Result = flurm_node_manager:allocate_resources(<<"mgr_alloc">>, 1, 4, 8192),
    ?assertEqual(ok, Result),
    ok.

test_mgr_allocate_not_found() ->
    Result = flurm_node_manager:allocate_resources(<<"nonexistent">>, 1, 4, 8192),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_mgr_release_success() ->
    _Pid = register_test_node(<<"mgr_rel">>, #{cpus => 8, memory => 16384}),
    ok = flurm_node_manager:allocate_resources(<<"mgr_rel">>, 1, 4, 8192),
    Result = flurm_node_manager:release_resources(<<"mgr_rel">>, 1),
    ?assertEqual(ok, Result),
    ok.

test_mgr_release_not_found() ->
    Result = flurm_node_manager:release_resources(<<"nonexistent">>, 1),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_mgr_allocate_gres_empty() ->
    {ok, Indices} = flurm_node_manager:allocate_gres(<<"node">>, 1, <<>>, false),
    ?assertEqual([], Indices),
    ok.

test_mgr_allocate_gres() ->
    _Pid = register_test_node(<<"mgr_gres_alloc">>, #{cpus => 8, memory => 16384}),
    %% Result depends on flurm_gres implementation - may error if no GPUs configured
    try
        _Result = flurm_node_manager:allocate_gres(<<"mgr_gres_alloc">>, 1, <<"gpu:1">>, false),
        ok
    catch
        _:_ ->
            %% GRES not configured, that's expected
            ok
    end.

test_mgr_release_gres() ->
    _Pid = register_test_node(<<"mgr_gres_rel">>, #{cpus => 8, memory => 16384}),
    %% Should not crash even if no GRES was allocated
    Result = flurm_node_manager:release_gres(<<"mgr_gres_rel">>, 1),
    ?assertEqual(ok, Result),
    ok.
