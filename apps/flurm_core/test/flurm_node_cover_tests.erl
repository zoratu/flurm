%%%-------------------------------------------------------------------
%%% @doc FLURM Node Coverage Integration Tests
%%%
%%% Integration tests designed to call through public APIs to improve
%%% code coverage tracking. Cover only tracks calls from test modules
%%% to the actual module code, so these tests call public functions
%%% that exercise internal code paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

node_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates node process", fun test_start_link/0},
        {"register_node creates and registers node", fun test_register_node/0},
        {"heartbeat updates timestamp", fun test_heartbeat/0},
        {"allocate reserves resources", fun test_allocate/0},
        {"release frees resources", fun test_release/0},
        {"set_state changes node state", fun test_set_state/0},
        {"get_info returns node data", fun test_get_info/0},
        {"list_jobs returns running jobs", fun test_list_jobs/0},
        {"drain_node prevents allocations", fun test_drain_node/0},
        {"drain_node with reason", fun test_drain_node_with_reason/0},
        {"undrain_node allows allocations", fun test_undrain_node/0},
        {"get_drain_reason returns reason", fun test_get_drain_reason/0},
        {"is_draining returns correct status", fun test_is_draining/0},
        {"node name API variants", fun test_node_name_variants/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, RegistryPid} = flurm_node_registry:start_link(),
    {ok, SupPid} = flurm_node_sup:start_link(),
    #{registry => RegistryPid, supervisor => SupPid}.

cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    [catch flurm_node_sup:stop_node(P) || P <- flurm_node_sup:which_nodes()],
    catch unlink(SupPid),
    catch unlink(RegistryPid),
    catch gen_server:stop(SupPid, shutdown, 5000),
    catch gen_server:stop(RegistryPid, shutdown, 5000),
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
        features => [fast],
        partitions => [<<"default">>, <<"compute">>]
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

%%====================================================================
%% Coverage Tests - Public API Calls
%%====================================================================

test_start_link() ->
    %% Test start_link directly
    NodeSpec = make_node_spec(<<"testnode">>),
    {ok, Pid} = flurm_node:start_link(NodeSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Verify get_info works
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(<<"testnode">>, maps:get(name, Info)),
    gen_server:stop(Pid),
    ok.

test_register_node() ->
    %% Test register_node which creates via supervisor and registers
    NodeSpec = make_node_spec(<<"node1">>),
    {ok, Pid, <<"node1">>} = flurm_node:register_node(NodeSpec),
    ?assert(is_pid(Pid)),
    %% Verify registered
    {ok, Pid} = flurm_node_registry:lookup_node(<<"node1">>),
    ok.

test_heartbeat() ->
    NodeSpec = make_node_spec(<<"node2">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Get initial heartbeat time
    {ok, Info1} = flurm_node:get_info(Pid),
    HB1 = maps:get(last_heartbeat, Info1),
    %% Send heartbeat after state sync
    ok = flurm_node:heartbeat(Pid),
    %% Verify heartbeat was updated
    {ok, Info2} = flurm_node:get_info(Pid),
    HB2 = maps:get(last_heartbeat, Info2),
    ?assertNotEqual(HB1, HB2),
    ok.

test_allocate() ->
    NodeSpec = make_node_spec(<<"node3">>, #{cpus => 8, memory => 16384, gpus => 2}),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Verify initial resources
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(0, maps:get(cpus_used, Info1)),
    ?assertEqual(8, maps:get(cpus_available, Info1)),
    ?assertEqual(0, maps:get(memory_used, Info1)),
    ?assertEqual(0, maps:get(gpus_used, Info1)),
    %% Allocate resources
    ok = flurm_node:allocate(Pid, 1, {4, 8192, 1}),
    %% Verify resources allocated
    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(4, maps:get(cpus_used, Info2)),
    ?assertEqual(4, maps:get(cpus_available, Info2)),
    ?assertEqual(8192, maps:get(memory_used, Info2)),
    ?assertEqual(1, maps:get(gpus_used, Info2)),
    %% Verify job is listed
    {ok, Jobs} = flurm_node:list_jobs(Pid),
    ?assertEqual([1], Jobs),
    %% Try to allocate more than available
    {error, insufficient_resources} = flurm_node:allocate(Pid, 2, {8, 1024, 0}),
    ok.

test_release() ->
    NodeSpec = make_node_spec(<<"node4">>, #{cpus => 8, memory => 16384, gpus => 2}),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Allocate then release
    ok = flurm_node:allocate(Pid, 1, {4, 8192, 1}),
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(4, maps:get(cpus_used, Info1)),
    ok = flurm_node:release(Pid, 1),
    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(0, maps:get(cpus_used, Info2)),
    ?assertEqual([], maps:get(jobs, Info2)),
    %% Release non-existent job returns error
    {error, job_not_found} = flurm_node:release(Pid, 999),
    ok.

test_set_state() ->
    NodeSpec = make_node_spec(<<"node5">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Initial state is up
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(up, maps:get(state, Info1)),
    %% Set to drain
    ok = flurm_node:set_state(Pid, drain),
    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info2)),
    %% Set to down
    ok = flurm_node:set_state(Pid, down),
    {ok, Info3} = flurm_node:get_info(Pid),
    ?assertEqual(down, maps:get(state, Info3)),
    %% Set to maint
    ok = flurm_node:set_state(Pid, maint),
    {ok, Info4} = flurm_node:get_info(Pid),
    ?assertEqual(maint, maps:get(state, Info4)),
    %% Set back to up
    ok = flurm_node:set_state(Pid, up),
    {ok, Info5} = flurm_node:get_info(Pid),
    ?assertEqual(up, maps:get(state, Info5)),
    ok.

test_get_info() ->
    NodeSpec = make_node_spec(<<"node6">>, #{
        hostname => <<"compute01.local">>,
        port => 6666,
        cpus => 16,
        memory => 32768,
        gpus => 4,
        features => [fast, gpu],
        partitions => [<<"gpu">>, <<"large">>]
    }),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    {ok, Info} = flurm_node:get_info(Pid),
    %% Verify all fields
    ?assertEqual(<<"node6">>, maps:get(name, Info)),
    ?assertEqual(<<"compute01.local">>, maps:get(hostname, Info)),
    ?assertEqual(6666, maps:get(port, Info)),
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
    ?assertEqual([fast, gpu], maps:get(features, Info)),
    ?assertEqual([<<"gpu">>, <<"large">>], maps:get(partitions, Info)),
    ?assertEqual([], maps:get(jobs, Info)),
    ?assertEqual(0, maps:get(job_count, Info)),
    ok.

test_list_jobs() ->
    NodeSpec = make_node_spec(<<"node7">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Initially no jobs
    {ok, []} = flurm_node:list_jobs(Pid),
    %% Allocate some jobs
    ok = flurm_node:allocate(Pid, 1, {2, 1024, 0}),
    ok = flurm_node:allocate(Pid, 2, {2, 1024, 0}),
    ok = flurm_node:allocate(Pid, 3, {2, 1024, 0}),
    {ok, Jobs} = flurm_node:list_jobs(Pid),
    ?assertEqual(3, length(Jobs)),
    ?assert(lists:member(1, Jobs)),
    ?assert(lists:member(2, Jobs)),
    ?assert(lists:member(3, Jobs)),
    ok.

test_drain_node() ->
    NodeSpec = make_node_spec(<<"node8">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Drain the node (default reason)
    ok = flurm_node:drain_node(Pid),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"admin request">>, maps:get(drain_reason, Info)),
    %% Cannot allocate on drained node
    {error, insufficient_resources} = flurm_node:allocate(Pid, 1, {1, 1024, 0}),
    ok.

test_drain_node_with_reason() ->
    NodeSpec = make_node_spec(<<"node9">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Drain with custom reason
    ok = flurm_node:drain_node(Pid, <<"hardware maintenance">>),
    {ok, Info} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info)),
    ?assertEqual(<<"hardware maintenance">>, maps:get(drain_reason, Info)),
    ok.

test_undrain_node() ->
    NodeSpec = make_node_spec(<<"node10">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Drain then undrain
    ok = flurm_node:drain_node(Pid),
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(drain, maps:get(state, Info1)),
    ok = flurm_node:undrain_node(Pid),
    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(up, maps:get(state, Info2)),
    ?assertEqual(undefined, maps:get(drain_reason, Info2)),
    %% Can allocate again
    ok = flurm_node:allocate(Pid, 1, {1, 1024, 0}),
    %% Undrain when not draining returns error
    {error, not_draining} = flurm_node:undrain_node(Pid),
    ok.

test_get_drain_reason() ->
    NodeSpec = make_node_spec(<<"node11">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Not draining - error
    {error, not_draining} = flurm_node:get_drain_reason(Pid),
    %% Drain with reason
    ok = flurm_node:drain_node(Pid, <<"test reason">>),
    {ok, Reason} = flurm_node:get_drain_reason(Pid),
    ?assertEqual(<<"test reason">>, Reason),
    ok.

test_is_draining() ->
    NodeSpec = make_node_spec(<<"node12">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Not draining
    ?assertEqual(false, flurm_node:is_draining(Pid)),
    %% Start draining
    ok = flurm_node:drain_node(Pid),
    ?assertEqual(true, flurm_node:is_draining(Pid)),
    %% Undrain
    ok = flurm_node:undrain_node(Pid),
    ?assertEqual(false, flurm_node:is_draining(Pid)),
    ok.

test_node_name_variants() ->
    NodeSpec = make_node_spec(<<"node13">>),
    {ok, _Pid, <<"node13">>} = flurm_node:register_node(NodeSpec),
    %% Test API calls with node name instead of PID
    {ok, Info} = flurm_node:get_info(<<"node13">>),
    ?assertEqual(<<"node13">>, maps:get(name, Info)),
    ok = flurm_node:heartbeat(<<"node13">>),
    ok = flurm_node:allocate(<<"node13">>, 1, {1, 1024, 0}),
    {ok, Jobs} = flurm_node:list_jobs(<<"node13">>),
    ?assertEqual([1], Jobs),
    ok = flurm_node:release(<<"node13">>, 1),
    ok = flurm_node:set_state(<<"node13">>, drain),
    ?assertEqual(true, flurm_node:is_draining(<<"node13">>)),
    {ok, _Reason} = flurm_node:get_drain_reason(<<"node13">>),
    ok = flurm_node:undrain_node(<<"node13">>),
    ok = flurm_node:drain_node(<<"node13">>, <<"custom reason">>),
    ok = flurm_node:undrain_node(<<"node13">>),
    %% Test with non-existent node name
    {error, node_not_found} = flurm_node:get_info(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:heartbeat(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:allocate(<<"nonexistent">>, 1, {1, 1, 0}),
    {error, node_not_found} = flurm_node:release(<<"nonexistent">>, 1),
    {error, node_not_found} = flurm_node:set_state(<<"nonexistent">>, up),
    {error, node_not_found} = flurm_node:list_jobs(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:drain_node(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:drain_node(<<"nonexistent">>, <<"r">>),
    {error, node_not_found} = flurm_node:undrain_node(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:get_drain_reason(<<"nonexistent">>),
    {error, node_not_found} = flurm_node:is_draining(<<"nonexistent">>),
    ok.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

node_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0},
        {"allocation on non-up state fails", fun test_allocation_non_up/0},
        {"multiple allocations and releases", fun test_multiple_alloc_release/0}
     ]}.

test_unknown_call() ->
    NodeSpec = make_node_spec(<<"node14">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    Result = gen_server:call(Pid, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    NodeSpec = make_node_spec(<<"node15">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    gen_server:cast(Pid, {unknown_cast, test}),
    _ = sys:get_state(Pid),
    %% Verify node still works
    {ok, _Info} = flurm_node:get_info(Pid),
    ok.

test_unknown_info() ->
    NodeSpec = make_node_spec(<<"node16">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    Pid ! {unknown_info, test},
    _ = sys:get_state(Pid),
    %% Verify node still works
    {ok, _Info} = flurm_node:get_info(Pid),
    ok.

test_allocation_non_up() ->
    NodeSpec = make_node_spec(<<"node17">>),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Set to down
    ok = flurm_node:set_state(Pid, down),
    %% Cannot allocate on down node
    {error, insufficient_resources} = flurm_node:allocate(Pid, 1, {1, 1024, 0}),
    %% Set to maint
    ok = flurm_node:set_state(Pid, maint),
    %% Cannot allocate on maint node
    {error, insufficient_resources} = flurm_node:allocate(Pid, 1, {1, 1024, 0}),
    ok.

test_multiple_alloc_release() ->
    NodeSpec = make_node_spec(<<"node18">>, #{cpus => 16, memory => 32768, gpus => 0}),
    {ok, Pid, _Name} = flurm_node:register_node(NodeSpec),
    %% Allocate multiple jobs
    ok = flurm_node:allocate(Pid, 1, {4, 4096, 0}),
    ok = flurm_node:allocate(Pid, 2, {4, 4096, 0}),
    ok = flurm_node:allocate(Pid, 3, {4, 4096, 0}),
    {ok, Info1} = flurm_node:get_info(Pid),
    ?assertEqual(12, maps:get(cpus_used, Info1)),
    ?assertEqual(12288, maps:get(memory_used, Info1)),
    ?assertEqual(3, maps:get(job_count, Info1)),
    %% Release in different order
    ok = flurm_node:release(Pid, 2),
    {ok, Jobs1} = flurm_node:list_jobs(Pid),
    ?assertEqual(2, length(Jobs1)),
    ?assertNot(lists:member(2, Jobs1)),
    ok = flurm_node:release(Pid, 1),
    ok = flurm_node:release(Pid, 3),
    {ok, Info2} = flurm_node:get_info(Pid),
    ?assertEqual(0, maps:get(cpus_used, Info2)),
    ?assertEqual([], maps:get(jobs, Info2)),
    ok.
