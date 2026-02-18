%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_controller_handler
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle PING request", fun test_handle_ping/0},
      {"handle job info request - all jobs", fun test_handle_job_info_all/0},
      {"handle job info request - specific job", fun test_handle_job_info_specific/0},
      {"handle job info request - job not found", fun test_handle_job_info_not_found/0},
      {"handle node info request", fun test_handle_node_info/0},
      {"handle partition info request", fun test_handle_partition_info/0},
      {"handle build info request", fun test_handle_build_info/0},
      {"handle batch job submit success", fun test_handle_batch_submit_success/0},
      {"handle batch job submit failure", fun test_handle_batch_submit_failure/0},
      {"handle cancel job success", fun test_handle_cancel_job_success/0},
      {"handle cancel job not found", fun test_handle_cancel_job_not_found/0},
      {"handle kill job request", fun test_handle_kill_job/0},
      {"handle kill job with job_id_str", fun test_handle_kill_job_str/0},
      {"handle job user info request", fun test_handle_job_user_info/0},
      {"handle reservation info request", fun test_handle_reservation_info/0},
      {"handle license info request", fun test_handle_license_info/0},
      {"handle topo info request", fun test_handle_topo_info/0},
      {"handle front end info request", fun test_handle_front_end_info/0},
      {"handle burst buffer info request", fun test_handle_burst_buffer_info/0},
      {"handle job info single request", fun test_handle_job_info_single/0},
      {"handle update job request", fun test_handle_update_job/0},
      {"handle update job binary fallback", fun test_handle_update_job_binary/0}
     ]}.

setup() ->
    catch meck:unload(flurm_job_manager),
    meck:new(flurm_job_manager, [passthrough, non_strict]),
    catch meck:unload(flurm_node_manager_server),
    meck:new(flurm_node_manager_server, [passthrough, non_strict]),
    catch meck:unload(flurm_partition_manager),
    meck:new(flurm_partition_manager, [passthrough, non_strict]),
    catch meck:unload(flurm_config_server),
    meck:new(flurm_config_server, [passthrough, non_strict]),
    catch meck:unload(flurm_controller_cluster),
    meck:new(flurm_controller_cluster, [passthrough, non_strict]),
    catch meck:unload(flurm_reservation),
    meck:new(flurm_reservation, [passthrough, non_strict]),
    catch meck:unload(flurm_license),
    meck:new(flurm_license, [passthrough, non_strict]),
    catch meck:unload(flurm_burst_buffer),
    meck:new(flurm_burst_buffer, [passthrough, non_strict]),
    catch meck:unload(flurm_step_manager),
    meck:new(flurm_step_manager, [passthrough, non_strict]),
    catch meck:unload(flurm_scheduler),
    meck:new(flurm_scheduler, [passthrough, non_strict]),
    catch meck:unload(flurm_node_connection_manager),
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),

    %% Default mocks - cluster disabled for simple testing
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> ok end),
    %% Default mock for node manager - return empty list
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    %% Default mock for node connection manager - return list of {Node, ok} tuples
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(Nodes, _) ->
        [{N, ok} || N <- Nodes]
    end),
    ok.

cleanup(_) ->
    meck:unload(flurm_job_manager),
    meck:unload(flurm_node_manager_server),
    meck:unload(flurm_partition_manager),
    meck:unload(flurm_config_server),
    meck:unload(flurm_controller_cluster),
    meck:unload(flurm_reservation),
    meck:unload(flurm_license),
    meck:unload(flurm_burst_buffer),
    meck:unload(flurm_step_manager),
    meck:unload(flurm_scheduler),
    catch meck:unload(flurm_node_connection_manager),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_handle_ping() ->
    Header = #slurm_header{msg_type = ?REQUEST_PING},
    Result = flurm_controller_handler:handle(Header, #ping_request{}),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_handle_job_info_all() ->
    Job1 = #job{id = 1, name = <<"job1">>, user = <<"user1">>, partition = <<"default">>,
                state = pending, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
                time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = []},
    Job2 = #job{id = 2, name = <<"job2">>, user = <<"user2">>, partition = <<"default">>,
                state = running, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
                time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = [<<"node1">>]},
    meck:expect(flurm_job_manager, list_jobs, fun() -> [Job1, Job2] end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_INFO},
    Body = #job_info_request{job_id = 0},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_INFO, #job_info_response{job_count = 2}}, Result).

test_handle_job_info_specific() ->
    Job1 = #job{id = 1, name = <<"job1">>, user = <<"user1">>, partition = <<"default">>,
                state = pending, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
                time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = []},
    meck:expect(flurm_job_manager, get_job, fun(1) -> {ok, Job1} end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_INFO},
    Body = #job_info_request{job_id = 1},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_INFO, #job_info_response{job_count = 1}}, Result).

test_handle_job_info_not_found() ->
    meck:expect(flurm_job_manager, get_job, fun(999) -> {error, not_found} end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_INFO},
    Body = #job_info_request{job_id = 999},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_INFO, #job_info_response{job_count = 0}}, Result).

test_handle_node_info() ->
    Node1 = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                  features = [], partitions = [<<"default">>], running_jobs = [],
                  load_avg = 0.5, free_memory_mb = 8192},
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node1] end),

    Header = #slurm_header{msg_type = ?REQUEST_NODE_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_NODE_INFO, #node_info_response{node_count = 1}}, Result).

test_handle_partition_info() ->
    Part1 = #partition{name = <<"default">>, state = up, nodes = [<<"node1">>],
                       max_time = 86400, default_time = 3600, max_nodes = 100,
                       priority = 100, allow_root = false},
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part1] end),

    Header = #slurm_header{msg_type = ?REQUEST_PARTITION_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_PARTITION_INFO, #partition_info_response{partition_count = 1}}, Result).

test_handle_build_info() ->
    Header = #slurm_header{msg_type = ?REQUEST_BUILD_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_BUILD_INFO, #build_info_response{}}, Result),
    {ok, _, Response} = Result,
    ?assertEqual(<<"22.05.0">>, Response#build_info_response.version).

test_handle_batch_submit_success() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 123} end),

    Header = #slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
    Body = #batch_job_request{
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"default">>,
        user_id = 1000
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SUBMIT_BATCH_JOB, #batch_job_response{job_id = 123, error_code = 0}}, Result).

test_handle_batch_submit_failure() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, no_resources} end),

    Header = #slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
    Body = #batch_job_request{
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"default">>,
        user_id = 1000
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SUBMIT_BATCH_JOB, #batch_job_response{job_id = 0}}, Result).

test_handle_cancel_job_success() ->
    meck:expect(flurm_job_manager, cancel_job, fun(1) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_CANCEL_JOB},
    Body = #cancel_job_request{job_id = 1},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_handle_cancel_job_not_found() ->
    meck:expect(flurm_job_manager, cancel_job, fun(999) -> {error, not_found} end),

    Header = #slurm_header{msg_type = ?REQUEST_CANCEL_JOB},
    Body = #cancel_job_request{job_id = 999},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

test_handle_kill_job() ->
    meck:expect(flurm_job_manager, cancel_job, fun(5) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_KILL_JOB},
    Body = #kill_job_request{job_id = 5, job_id_str = <<>>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_handle_kill_job_str() ->
    meck:expect(flurm_job_manager, cancel_job, fun(42) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_KILL_JOB},
    Body = #kill_job_request{job_id = 0, job_id_str = <<"42">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_handle_job_user_info() ->
    Job1 = #job{id = 1, name = <<"job1">>, user = <<"user1">>, partition = <<"default">>,
                state = pending, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
                time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = []},
    meck:expect(flurm_job_manager, list_jobs, fun() -> [Job1] end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_USER_INFO},
    %% Body with job_id = 0 means all jobs
    Body = <<0:32/big>>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_INFO, _}, Result).

test_handle_reservation_info() ->
    meck:expect(flurm_reservation, list, fun() -> [] end),

    Header = #slurm_header{msg_type = ?REQUEST_RESERVATION_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_RESERVATION_INFO, #reservation_info_response{}}, Result).

test_handle_license_info() ->
    meck:expect(flurm_license, list, fun() -> [] end),

    Header = #slurm_header{msg_type = ?REQUEST_LICENSE_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_LICENSE_INFO, #license_info_response{}}, Result).

test_handle_topo_info() ->
    Header = #slurm_header{msg_type = ?REQUEST_TOPO_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_TOPO_INFO, #topo_info_response{topo_count = 0}}, Result).

test_handle_front_end_info() ->
    Header = #slurm_header{msg_type = ?REQUEST_FRONT_END_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_FRONT_END_INFO, #front_end_info_response{front_end_count = 0}}, Result).

test_handle_burst_buffer_info() ->
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() -> #{} end),

    Header = #slurm_header{msg_type = ?REQUEST_BURST_BUFFER_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_BURST_BUFFER_INFO, #burst_buffer_info_response{}}, Result).

test_handle_job_info_single() ->
    Job1 = #job{id = 1, name = <<"job1">>, user = <<"user1">>, partition = <<"default">>,
                state = pending, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
                time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = []},
    meck:expect(flurm_job_manager, get_job, fun(1) -> {ok, Job1} end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_INFO_SINGLE},
    Body = #job_info_request{job_id = 1},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_INFO, #job_info_response{job_count = 1}}, Result).

test_handle_update_job() ->
    meck:expect(flurm_job_manager, update_job, fun(1, _) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_UPDATE_JOB},
    Body = #update_job_request{job_id = 1, priority = 500},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_handle_update_job_binary() ->
    Header = #slurm_header{msg_type = ?REQUEST_UPDATE_JOB},
    Body = <<"some binary data">>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

%%====================================================================
%% Cluster Mode Tests (simplified - cluster mode covered elsewhere)
%%====================================================================

cluster_mode_placeholder_test_() ->
    [
     {"cluster mode forwarding tested in integration", fun() -> ok end}
    ].

%%====================================================================
%% Resource Allocation Tests
%%====================================================================

resource_allocation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle resource allocation success", fun test_resource_allocation_success/0},
      {"handle resource allocation failure", fun test_resource_allocation_failure/0},
      {"handle resource allocation decode failure", fun test_resource_allocation_fallback/0}
     ]}.

test_resource_allocation_success() ->
    Job = #job{id = 10, name = <<"srun_job">>, user = <<"user1">>, partition = <<"default">>,
               state = running, script = <<>>, num_nodes = 1, num_cpus = 4, memory_mb = 4096,
               time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = [<<"node1">>]},
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 10} end),
    meck:expect(flurm_job_manager, get_job, fun(10) -> {ok, Job} end),

    Header = #slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
    Body = #resource_allocation_request{
        name = <<"srun_job">>,
        min_nodes = 1,
        min_cpus = 4,
        partition = <<"default">>
    },
    Result = flurm_controller_handler:handle(Header, Body),
    %% Handler returns 4-tuple with CallbackInfo on success
    ?assertMatch({ok, ?RESPONSE_RESOURCE_ALLOCATION, #resource_allocation_response{job_id = 10}, _CallbackInfo}, Result).

test_resource_allocation_failure() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, no_resources} end),

    Header = #slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
    Body = #resource_allocation_request{name = <<"srun_job">>, min_nodes = 1},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_RESOURCE_ALLOCATION, #resource_allocation_response{error_code = 1}}, Result).

test_resource_allocation_fallback() ->
    Header = #slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
    Body = <<"invalid binary">>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_RESOURCE_ALLOCATION, #resource_allocation_response{error_code = 1}}, Result).

%%====================================================================
%% Job Step Tests
%%====================================================================

job_step_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle job step create success", fun test_step_create_success/0},
      {"handle job step create failure", fun test_step_create_failure/0},
      {"handle job step info", fun test_step_info/0}
     ]}.

test_step_create_success() ->
    %% Mock the step manager to return success
    meck:expect(flurm_step_manager, create_step, fun(_, _) -> {ok, 0} end),
    %% Mock get_job for get_job_cred_info which is called on success
    Job = #job{id = 1, name = <<"test_job">>, user = <<"user1">>, partition = <<"default">>,
               state = running, script = <<>>, num_nodes = 1, num_cpus = 4, memory_mb = 4096,
               time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = [<<"node1">>]},
    meck:expect(flurm_job_manager, get_job, fun(1) -> {ok, Job} end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_STEP_CREATE},
    Body = #job_step_create_request{job_id = 1, name = <<"step0">>, num_tasks = 4},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_STEP_CREATE, #job_step_create_response{error_code = 0}}, Result).

test_step_create_failure() ->
    meck:expect(flurm_step_manager, create_step, fun(_, _) -> {error, job_not_found} end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_STEP_CREATE},
    Body = #job_step_create_request{job_id = 999, name = <<"step0">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_STEP_CREATE, #job_step_create_response{error_code = 1}}, Result).

test_step_info() ->
    meck:expect(flurm_step_manager, list_steps, fun(_) -> [] end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_STEP_INFO},
    Body = #job_step_info_request{job_id = 1, step_id = -1},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_STEP_INFO, #job_step_info_response{step_count = 0}}, Result).

%%====================================================================
%% Reconfigure Tests
%%====================================================================

reconfigure_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle reconfigure request success", fun test_reconfigure_success/0},
      {"handle reconfigure request error", fun test_reconfigure_error/0},
      {"handle reconfigure with config success", fun test_reconfigure_with_config_success/0},
      {"handle reconfigure with config error", fun test_reconfigure_with_config_error/0},
      {"handle shutdown request", fun test_shutdown/0}
     ]}.

test_reconfigure_success() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),
    %% do_reconfigure calls apply_node_changes which calls get_nodes
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    %% apply_partition_changes calls get_partitions
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    %% Also need list_partitions and sync_nodes_from_config
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [] end),
    meck:expect(flurm_node_manager_server, sync_nodes_from_config, fun(_) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_RECONFIGURE},
    Body = #reconfigure_request{flags = 0},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_reconfigure_error() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> {error, file_not_found} end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),

    Header = #slurm_header{msg_type = ?REQUEST_RECONFIGURE},
    Body = <<>>,  % raw binary fallback
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

test_reconfigure_with_config_success() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),

    Header = #slurm_header{msg_type = ?REQUEST_RECONFIGURE_WITH_CONFIG},
    Body = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_reconfigure_with_config_error() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, invalid_config} end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),

    Header = #slurm_header{msg_type = ?REQUEST_RECONFIGURE_WITH_CONFIG},
    Body = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{invalid_key => invalid_value},
        force = false,
        notify_nodes = true
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

test_shutdown() ->
    Header = #slurm_header{msg_type = ?REQUEST_SHUTDOWN},
    Body = <<>>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{}}, Result).

%%====================================================================
%% Stats and Config Info Tests
%%====================================================================

stats_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle stats info request", fun test_stats_info/0},
      {"handle config info request", fun test_config_info/0}
     ]}.

test_stats_info() ->
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    meck:expect(flurm_scheduler, get_stats, fun() -> #{} end),

    Header = #slurm_header{msg_type = ?REQUEST_STATS_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_STATS_INFO, #stats_info_response{}}, Result).

test_config_info() ->
    meck:expect(flurm_config_server, get_all, fun() -> #{} end),

    Header = #slurm_header{msg_type = ?REQUEST_CONFIG_INFO},
    Result = flurm_controller_handler:handle(Header, <<>>),
    ?assertMatch({ok, ?RESPONSE_CONFIG_INFO, #config_info_response{}}, Result).

%%====================================================================
%% Job Will Run Tests
%%====================================================================

job_will_run_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle job will run - can run", fun test_job_will_run_can/0},
      {"handle job will run - cannot run", fun test_job_will_run_cannot/0},
      {"handle job will run fallback", fun test_job_will_run_fallback/0}
     ]}.

test_job_will_run_can() ->
    meck:expect(flurm_node_manager_server, get_available_nodes_for_job,
                fun(_, _, _) -> [<<"node1">>, <<"node2">>] end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN},
    Body = #job_will_run_request{partition = <<"default">>, min_nodes = 1, min_cpus = 4},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_WILL_RUN, #job_will_run_response{error_code = 0}}, Result).

test_job_will_run_cannot() ->
    meck:expect(flurm_node_manager_server, get_available_nodes_for_job,
                fun(_, _, _) -> [] end),

    Header = #slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN},
    Body = #job_will_run_request{partition = <<"default">>, min_nodes = 10, min_cpus = 100},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_WILL_RUN, #job_will_run_response{}}, Result).

test_job_will_run_fallback() ->
    Header = #slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN},
    Body = <<"invalid">>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_JOB_WILL_RUN, #job_will_run_response{}}, Result).

%%====================================================================
%% Reservation CRUD Tests
%%====================================================================

reservation_crud_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle create reservation success", fun test_create_reservation_success/0},
      {"handle create reservation error", fun test_create_reservation_error/0},
      {"handle update reservation success", fun test_update_reservation_success/0},
      {"handle update reservation error", fun test_update_reservation_error/0},
      {"handle delete reservation success", fun test_delete_reservation_success/0},
      {"handle delete reservation error", fun test_delete_reservation_error/0}
     ]}.

test_create_reservation_success() ->
    meck:expect(flurm_reservation, create, fun(_) -> {ok, <<"resv1">>} end),

    Header = #slurm_header{msg_type = ?REQUEST_CREATE_RESERVATION},
    Body = #create_reservation_request{
        name = <<"resv1">>,
        start_time = erlang:system_time(second),
        duration = 3600,
        nodes = <<"node[01-04]">>,
        users = <<"user1">>
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_CREATE_RESERVATION, #create_reservation_response{error_code = 0}}, Result).

test_create_reservation_error() ->
    meck:expect(flurm_reservation, create, fun(_) -> {error, invalid_nodes} end),

    Header = #slurm_header{msg_type = ?REQUEST_CREATE_RESERVATION},
    Body = #create_reservation_request{name = <<"bad_resv">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_CREATE_RESERVATION, #create_reservation_response{error_code = 1}}, Result).

test_update_reservation_success() ->
    meck:expect(flurm_reservation, update, fun(_, _) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_UPDATE_RESERVATION},
    Body = #update_reservation_request{name = <<"resv1">>, duration = 7200},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_update_reservation_error() ->
    meck:expect(flurm_reservation, update, fun(_, _) -> {error, not_found} end),

    Header = #slurm_header{msg_type = ?REQUEST_UPDATE_RESERVATION},
    Body = #update_reservation_request{name = <<"nonexistent">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

test_delete_reservation_success() ->
    meck:expect(flurm_reservation, delete, fun(_) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_DELETE_RESERVATION},
    Body = #delete_reservation_request{name = <<"resv1">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_delete_reservation_error() ->
    meck:expect(flurm_reservation, delete, fun(_) -> {error, not_found} end),

    Header = #slurm_header{msg_type = ?REQUEST_DELETE_RESERVATION},
    Body = #delete_reservation_request{name = <<"nonexistent">>},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

%%====================================================================
%% Unknown Message Type Test
%%====================================================================

unknown_msg_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle unknown message type", fun test_unknown_msg_type/0}
     ]}.

test_unknown_msg_type() ->
    Header = #slurm_header{msg_type = 99999},
    Body = <<>>,
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

%%====================================================================
%% Cluster Mode Routing Tests
%%====================================================================

cluster_routing_test_() ->
    {setup,
     fun cluster_setup/0,
     fun cluster_cleanup/1,
     [
      {"leader handles kill job locally", fun test_leader_kill_job/0},
      {"follower forwards kill job to leader", fun test_follower_kill_job_forward/0},
      {"follower handles no_leader error", fun test_follower_no_leader/0},
      {"leader handles suspend request", fun test_leader_suspend/0},
      {"follower forwards batch submit to leader", fun test_follower_batch_submit_forward/0},
      {"resource allocation 4-tuple includes callback info", fun test_resource_allocation_callback_info/0}
     ]}.

cluster_setup() ->
    setup(),
    %% Enable cluster mode by setting cluster_nodes app env
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    ok.

cluster_cleanup(_) ->
    application:unset_env(flurm_controller, cluster_nodes),
    cleanup(ok).

test_leader_kill_job() ->
    %% When is_leader returns true, kill job locally
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_job_manager, cancel_job, fun(100) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_KILL_JOB},
    Body = #kill_job_request{job_id = 100, signal = 9},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_follower_kill_job_forward() ->
    %% When is_leader returns false, forward to leader
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader,
        fun(cancel_job, 100) -> {ok, ok} end),

    Header = #slurm_header{msg_type = ?REQUEST_KILL_JOB},
    Body = #kill_job_request{job_id = 100, signal = 9},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

test_follower_no_leader() ->
    %% When forward_to_leader fails with no_leader
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader,
        fun(cancel_job, _) -> {error, no_leader} end),

    Header = #slurm_header{msg_type = ?REQUEST_KILL_JOB},
    Body = #kill_job_request{job_id = 100, signal = 9},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

test_leader_suspend() ->
    %% Leader handles suspend directly
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_job_manager, get_job, fun(200) ->
        {ok, #job{id = 200, name = <<"test">>, user = <<"u">>, partition = <<"p">>,
                  state = running, script = <<>>, num_nodes = 1, num_cpus = 1,
                  memory_mb = 256, time_limit = 60, priority = 100, submit_time = 0,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(200, _) -> ok end),

    Header = #slurm_header{msg_type = ?REQUEST_SUSPEND},
    Body = #suspend_request{job_id = 200, suspend = true},
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{}}, Result).

test_follower_batch_submit_forward() ->
    %% Follower forwards batch submit to leader
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader,
        fun(submit_job, _) -> {ok, {ok, 42}} end),

    Header = #slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
    Body = #batch_job_request{
        name = <<"forwarded_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"default">>,
        min_nodes = 1,
        min_cpus = 1,
        user_id = 1000,
        group_id = 1000
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_SUBMIT_BATCH_JOB, #batch_job_response{job_id = 42}}, Result).

test_resource_allocation_callback_info() ->
    %% Verify the 4-tuple return includes callback map with expected fields
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 55} end),
    %% Return a node with resolvable hostname to avoid DNS timeout
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [#node{hostname = <<"localhost">>, state = idle, cpus = 4, memory_mb = 8192,
               partitions = [<<"default">>]}]
    end),

    Header = #slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
    Body = #resource_allocation_request{
        name = <<"srun_job">>, min_nodes = 1, min_cpus = 2, partition = <<"default">>
    },
    Result = flurm_controller_handler:handle(Header, Body),
    ?assertMatch({ok, ?RESPONSE_RESOURCE_ALLOCATION, #resource_allocation_response{job_id = 55}, _}, Result),
    {ok, _, _, CallbackInfo} = Result,
    ?assert(is_map(CallbackInfo)),
    ?assertEqual(55, maps:get(job_id, CallbackInfo)),
    ?assert(maps:is_key(node_list, CallbackInfo)),
    %% Restore default list_nodes mock
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end).
