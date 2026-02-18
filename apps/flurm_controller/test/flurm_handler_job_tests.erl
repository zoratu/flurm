%%%-------------------------------------------------------------------
%%% @doc FLURM Job Handler Tests
%%%
%%% Comprehensive tests for the flurm_handler_job module.
%%% Tests all handle/2 clauses for job-related operations:
%%% - Batch job submission
%%% - Resource allocation (srun)
%%% - Job cancellation and killing
%%% - Job suspension/resume
%%% - Job signaling
%%% - Job updates (hold, release, requeue)
%%% - Job will_run checks
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_job_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% SLURM error code (matches definition in flurm_handler_job.erl)
-define(ESLURM_CONTROLLER_NOT_FOUND, 1).

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_job_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_SUBMIT_BATCH_JOB (4003) tests
        {"Batch job submission - success", fun test_batch_job_submission_success/0},
        {"Batch job submission - array job", fun test_batch_job_submission_array/0},
        {"Batch job submission - failure", fun test_batch_job_submission_failure/0},
        {"Batch job submission - partition validation", fun test_batch_job_partition_validation/0},
        {"Batch job submission - default values", fun test_batch_job_default_values/0},
        {"Batch job submission - with dependency", fun test_batch_job_with_dependency/0},
        {"Batch job submission - with licenses", fun test_batch_job_with_licenses/0},
        {"Batch job submission - with account", fun test_batch_job_with_account/0},

        %% REQUEST_RESOURCE_ALLOCATION (4001) tests
        {"Resource allocation - srun success", fun test_resource_allocation_success/0},
        {"Resource allocation - srun with callback", fun test_resource_allocation_with_callback/0},
        {"Resource allocation - srun failure", fun test_resource_allocation_failure/0},
        {"Resource allocation - raw binary fallback", fun test_resource_allocation_raw_binary/0},
        {"Resource allocation - node resolution", fun test_resource_allocation_node_resolution/0},

        %% REQUEST_JOB_ALLOCATION_INFO (4014) tests
        {"Job allocation info - valid job", fun test_job_allocation_info_valid/0},
        {"Job allocation info - invalid body", fun test_job_allocation_info_invalid/0},
        {"Job allocation info - node address resolution", fun test_job_allocation_info_node_addr/0},

        %% REQUEST_JOB_READY (4019) tests
        {"Job ready - valid job id", fun test_job_ready_valid/0},
        {"Job ready - zero job id", fun test_job_ready_zero/0},
        {"Job ready - empty body", fun test_job_ready_empty_body/0},
        {"Job ready - return code bits", fun test_job_ready_return_code/0},

        %% REQUEST_KILL_TIMELIMIT (5017) tests
        {"Kill timelimit - with job and step id", fun test_kill_timelimit_with_ids/0},
        {"Kill timelimit - job id only", fun test_kill_timelimit_job_only/0},
        {"Kill timelimit - empty body", fun test_kill_timelimit_empty/0},

        %% REQUEST_KILL_JOB (5032) tests
        {"Kill job - with job_id", fun test_kill_job_with_id/0},
        {"Kill job - with job_id_str", fun test_kill_job_with_str/0},
        {"Kill job - not found", fun test_kill_job_not_found/0},
        {"Kill job - zero job_id with str", fun test_kill_job_zero_id_with_str/0},
        {"Kill job - general error", fun test_kill_job_error/0},

        %% REQUEST_CANCEL_JOB (4006) tests
        {"Cancel job - success", fun test_cancel_job_success/0},
        {"Cancel job - not found", fun test_cancel_job_not_found/0},
        {"Cancel job - other error", fun test_cancel_job_error/0},

        %% REQUEST_SUSPEND (5014) tests
        {"Suspend job - suspend true", fun test_suspend_job_true/0},
        {"Suspend job - suspend false (resume)", fun test_suspend_job_false/0},
        {"Suspend job - not found", fun test_suspend_job_not_found/0},
        {"Suspend job - error handling", fun test_suspend_job_error/0},

        %% REQUEST_SIGNAL_JOB (5018) tests
        {"Signal job - SIGTERM", fun test_signal_job_sigterm/0},
        {"Signal job - SIGKILL", fun test_signal_job_sigkill/0},
        {"Signal job - SIGHUP", fun test_signal_job_sighup/0},
        {"Signal job - not found", fun test_signal_job_not_found/0},
        {"Signal job - error", fun test_signal_job_error/0},

        %% REQUEST_UPDATE_JOB (4014) tests
        {"Update job - hold (priority=0)", fun test_update_job_hold/0},
        {"Update job - release (priority=NO_VAL)", fun test_update_job_release_no_val/0},
        {"Update job - release (priority>0)", fun test_update_job_release_priority/0},
        {"Update job - requeue", fun test_update_job_requeue/0},
        {"Update job - time limit", fun test_update_job_time_limit/0},
        {"Update job - name change", fun test_update_job_name/0},
        {"Update job - with job_id_str", fun test_update_job_with_str/0},
        {"Update job - not found", fun test_update_job_not_found/0},
        {"Update job - raw binary fallback", fun test_update_job_raw_binary/0},
        {"Update job - multiple fields", fun test_update_job_multiple_fields/0},

        %% REQUEST_JOB_WILL_RUN (4012) tests
        {"Job will run - resources available", fun test_job_will_run_available/0},
        {"Job will run - no resources", fun test_job_will_run_unavailable/0},
        {"Job will run - default partition", fun test_job_will_run_default_partition/0},
        {"Job will run - fallback raw body", fun test_job_will_run_fallback/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Disable cluster mode
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Start meck for mocking
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_controller_cluster),
    catch meck:unload(flurm_federation),
    catch meck:unload(flurm_srun_callback),
    catch meck:unload(flurm_node_connection_manager),
    meck:new([flurm_job_manager, flurm_node_manager_server, flurm_partition_manager,
              flurm_config_server, flurm_controller_cluster, flurm_federation,
              flurm_srun_callback, flurm_node_connection_manager],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_config_server, get, fun(Key, Default) ->
        case Key of
            default_node -> <<"test-node">>;
            slurmd_port -> 6818;
            _ -> Default
        end
    end),

    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {ok, #partition{}} end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_srun_callback, notify_job_complete, fun(_, _, _) -> ok end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),

    ok.

cleanup(_) ->
    %% Unload all mocks
    catch meck:unload([flurm_job_manager, flurm_node_manager_server, flurm_partition_manager,
                       flurm_config_server, flurm_controller_cluster, flurm_federation,
                       flurm_srun_callback, flurm_node_connection_manager]),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_header(MsgType) ->
    #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = MsgType,
        body_length = 0
    }.

make_batch_job_request() ->
    make_batch_job_request(#{}).

make_batch_job_request(Overrides) ->
    Defaults = #{
        name => <<"test_job">>,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        min_nodes => 1,
        min_cpus => 1,
        num_tasks => 1,
        cpus_per_task => 1,
        min_mem_per_node => 512,
        time_limit => 3600,
        priority => 100,
        work_dir => <<"/tmp">>,
        std_out => <<"/tmp/job.out">>,
        std_err => <<"/tmp/job.err">>,
        script => <<"#!/bin/bash\necho hello">>,
        account => <<"default">>,
        licenses => <<>>,
        dependency => <<>>,
        array_inx => <<>>
    },
    Props = maps:merge(Defaults, Overrides),
    #batch_job_request{
        name = maps:get(name, Props),
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        min_nodes = maps:get(min_nodes, Props),
        min_cpus = maps:get(min_cpus, Props),
        num_tasks = maps:get(num_tasks, Props),
        cpus_per_task = maps:get(cpus_per_task, Props),
        min_mem_per_node = maps:get(min_mem_per_node, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        work_dir = maps:get(work_dir, Props),
        std_out = maps:get(std_out, Props),
        std_err = maps:get(std_err, Props),
        script = maps:get(script, Props),
        account = maps:get(account, Props),
        licenses = maps:get(licenses, Props),
        dependency = maps:get(dependency, Props),
        array_inx = maps:get(array_inx, Props)
    }.

make_resource_allocation_request() ->
    make_resource_allocation_request(#{}).

make_resource_allocation_request(Overrides) ->
    Defaults = #{
        name => <<"srun_job">>,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        min_nodes => 1,
        min_cpus => 1,
        time_limit => 3600,
        priority => 100,
        work_dir => <<"/tmp">>,
        account => <<"default">>,
        licenses => <<>>,
        alloc_resp_port => 12345,
        srun_pid => 9999
    },
    Props = maps:merge(Defaults, Overrides),
    #resource_allocation_request{
        name = maps:get(name, Props),
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        min_nodes = maps:get(min_nodes, Props),
        min_cpus = maps:get(min_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        work_dir = maps:get(work_dir, Props),
        account = maps:get(account, Props),
        licenses = maps:get(licenses, Props),
        alloc_resp_port = maps:get(alloc_resp_port, Props),
        srun_pid = maps:get(srun_pid, Props)
    }.

%%====================================================================
%% Batch Job Submission Tests (REQUEST_SUBMIT_BATCH_JOB - 4003)
%%====================================================================

test_batch_job_submission_success() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1001} end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    ?assertEqual(1001, Response#batch_job_response.job_id),
    ?assertEqual(0, Response#batch_job_response.error_code),
    ?assertEqual(<<"Job submitted successfully">>, Response#batch_job_response.job_submit_user_msg).

test_batch_job_submission_array() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, {array, 1002}} end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{array_inx => <<"0-9">>}),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    ?assertEqual(1002, Response#batch_job_response.job_id),
    ?assertEqual(0, Response#batch_job_response.error_code).

test_batch_job_submission_failure() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, resource_limit_exceeded} end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    ?assertEqual(0, Response#batch_job_response.job_id),
    ?assertEqual(?ESLURM_CONTROLLER_NOT_FOUND, Response#batch_job_response.error_code),
    ?assertNotEqual(<<>>, Response#batch_job_response.job_submit_user_msg).

test_batch_job_partition_validation() ->
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{partition => <<"nonexistent">>}),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    ?assertEqual(0, Response#batch_job_response.job_id),
    ?assertNotEqual(0, Response#batch_job_response.error_code).

test_batch_job_default_values() ->
    meck:expect(flurm_job_manager, submit_job, fun(JobSpec) ->
        %% Verify defaults are applied
        ?assertEqual(1, maps:get(num_nodes, JobSpec)),
        ?assertEqual(1, maps:get(num_cpus, JobSpec)),
        ?assertEqual(1, maps:get(num_tasks, JobSpec)),
        ?assertEqual(1, maps:get(cpus_per_task, JobSpec)),
        {ok, 1003}
    end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{min_nodes => 0, min_cpus => 0, num_tasks => 0}),
    {ok, _, _} = flurm_handler_job:handle(Header, Request).

test_batch_job_with_dependency() ->
    meck:expect(flurm_job_manager, submit_job, fun(JobSpec) ->
        ?assertEqual(<<"afterok:100">>, maps:get(dependency, JobSpec)),
        {ok, 1004}
    end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{dependency => <<"afterok:100">>}),
    {ok, _, Response} = flurm_handler_job:handle(Header, Request),
    ?assertEqual(1004, Response#batch_job_response.job_id).

test_batch_job_with_licenses() ->
    meck:expect(flurm_job_manager, submit_job, fun(JobSpec) ->
        ?assertEqual(<<"matlab:1,gaussian:2">>, maps:get(licenses, JobSpec)),
        {ok, 1005}
    end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{licenses => <<"matlab:1,gaussian:2">>}),
    {ok, _, Response} = flurm_handler_job:handle(Header, Request),
    ?assertEqual(1005, Response#batch_job_response.job_id).

test_batch_job_with_account() ->
    meck:expect(flurm_job_manager, submit_job, fun(JobSpec) ->
        ?assertEqual(<<"research">>, maps:get(account, JobSpec)),
        {ok, 1006}
    end),

    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{account => <<"research">>}),
    {ok, _, Response} = flurm_handler_job:handle(Header, Request),
    ?assertEqual(1006, Response#batch_job_response.job_id).

%%====================================================================
%% Resource Allocation Tests (REQUEST_RESOURCE_ALLOCATION - 4001)
%%====================================================================

test_resource_allocation_success() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 2001} end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [#node{hostname = <<"compute-1">>, cpus = 4, memory_mb = 8192}]
    end),

    Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
    Request = make_resource_allocation_request(),
    {ok, MsgType, Response, CallbackInfo} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_RESOURCE_ALLOCATION, MsgType),
    ?assertEqual(2001, Response#resource_allocation_response.job_id),
    ?assertEqual(0, Response#resource_allocation_response.error_code),
    ?assertEqual(1, Response#resource_allocation_response.num_cpu_groups),
    ?assertEqual([1], Response#resource_allocation_response.cpus_per_node),
    ?assertEqual(2001, maps:get(job_id, CallbackInfo)),
    ?assertEqual(12345, maps:get(port, CallbackInfo)).

test_resource_allocation_with_callback() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 2002} end),

    Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
    Request = make_resource_allocation_request(#{alloc_resp_port => 54321, srun_pid => 1234}),
    {ok, _, _, CallbackInfo} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(54321, maps:get(port, CallbackInfo)),
    ?assertEqual(1234, maps:get(srun_pid, CallbackInfo)).

test_resource_allocation_failure() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, no_resources} end),

    Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
    Request = make_resource_allocation_request(),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_RESOURCE_ALLOCATION, MsgType),
    ?assertEqual(0, Response#resource_allocation_response.job_id),
    ?assertEqual(1, Response#resource_allocation_response.error_code),
    ?assertEqual(0, Response#resource_allocation_response.num_cpu_groups).

test_resource_allocation_raw_binary() ->
    Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, <<"invalid binary">>),

    ?assertEqual(?RESPONSE_RESOURCE_ALLOCATION, MsgType),
    ?assertEqual(0, Response#resource_allocation_response.job_id),
    ?assertEqual(1, Response#resource_allocation_response.error_code),
    ?assertEqual(<<"Failed to decode request">>, Response#resource_allocation_response.job_submit_user_msg).

test_resource_allocation_node_resolution() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 2003} end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, get, fun(default_node, _) -> <<"default-node">>; (_, D) -> D end),

    Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
    Request = make_resource_allocation_request(),
    {ok, _, Response, _} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(<<"default-node">>, Response#resource_allocation_response.node_list).

%%====================================================================
%% Job Allocation Info Tests (REQUEST_JOB_ALLOCATION_INFO - 4014)
%%====================================================================

test_job_allocation_info_valid() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [#node{hostname = <<"node1">>}]
    end),

    Header = make_header(?REQUEST_JOB_ALLOCATION_INFO),
    Body = <<3001:32/big, 0:32>>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_ALLOCATION_INFO, MsgType),
    ?assertEqual(3001, Response#resource_allocation_response.job_id),
    ?assertEqual(0, Response#resource_allocation_response.error_code).

test_job_allocation_info_invalid() ->
    Header = make_header(?REQUEST_JOB_ALLOCATION_INFO),
    Body = <<"invalid">>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_ALLOCATION_INFO, MsgType),
    %% Handler parses first 4 bytes as job_id (<<"inva">> = 1768846945 big-endian)
    ?assertEqual(1768846945, Response#resource_allocation_response.job_id).

test_job_allocation_info_node_addr() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [#node{hostname = <<"localhost">>}]
    end),

    Header = make_header(?REQUEST_JOB_ALLOCATION_INFO),
    Body = <<3002:32/big>>,
    {ok, _, Response} = flurm_handler_job:handle(Header, Body),

    ?assert(length(Response#resource_allocation_response.node_addrs) > 0).

%%====================================================================
%% Job Ready Tests (REQUEST_JOB_READY - 4019)
%%====================================================================

test_job_ready_valid() ->
    Header = make_header(?REQUEST_JOB_READY),
    Body = <<4001:32/big, 0:16>>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_READY, MsgType),
    ?assertEqual(7, maps:get(return_code, Response)).

test_job_ready_zero() ->
    Header = make_header(?REQUEST_JOB_READY),
    Body = <<0:32/big>>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_READY, MsgType),
    ?assertEqual(7, maps:get(return_code, Response)).

test_job_ready_empty_body() ->
    Header = make_header(?REQUEST_JOB_READY),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_JOB_READY, MsgType),
    ?assert(is_map(Response)).

test_job_ready_return_code() ->
    %% Verify return code contains all ready bits
    Header = make_header(?REQUEST_JOB_READY),
    Body = <<4002:32/big>>,
    {ok, _, Response} = flurm_handler_job:handle(Header, Body),

    ReturnCode = maps:get(return_code, Response),
    %% READY_NODE_STATE | READY_JOB_STATE | READY_PROLOG_STATE = 7
    ?assertEqual(7, ReturnCode band 7).

%%====================================================================
%% Kill Timelimit Tests (REQUEST_KILL_TIMELIMIT - 5017)
%%====================================================================

test_kill_timelimit_with_ids() ->
    Header = make_header(?REQUEST_KILL_TIMELIMIT),
    Body = <<5001:32/big, 1:32/big, 0:32>>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_timelimit_job_only() ->
    Header = make_header(?REQUEST_KILL_TIMELIMIT),
    Body = <<5002:32/big>>,
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Body),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_timelimit_empty() ->
    Header = make_header(?REQUEST_KILL_TIMELIMIT),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Kill Job Tests (REQUEST_KILL_JOB - 5032)
%%====================================================================

test_kill_job_with_id() ->
    meck:expect(flurm_job_manager, cancel_job, fun(6001) -> ok end),

    Header = make_header(?REQUEST_KILL_JOB),
    Request = #kill_job_request{job_id = 6001, job_id_str = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_with_str() ->
    meck:expect(flurm_job_manager, cancel_job, fun(6002) -> ok end),

    Header = make_header(?REQUEST_KILL_JOB),
    Request = #kill_job_request{job_id = 0, job_id_str = <<"6002">>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_not_found() ->
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_KILL_JOB),
    Request = #kill_job_request{job_id = 999999, job_id_str = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    %% SLURM returns 0 even for not found
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_zero_id_with_str() ->
    meck:expect(flurm_job_manager, cancel_job, fun(6003) -> ok end),

    Header = make_header(?REQUEST_KILL_JOB),
    Request = #kill_job_request{job_id = 0, job_id_str = <<"6003">>},
    {ok, _, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_error() ->
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> {error, internal_error} end),

    Header = make_header(?REQUEST_KILL_JOB),
    Request = #kill_job_request{job_id = 6004, job_id_str = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

%%====================================================================
%% Cancel Job Tests (REQUEST_CANCEL_JOB - 4006)
%%====================================================================

test_cancel_job_success() ->
    meck:expect(flurm_job_manager, cancel_job, fun(7001) -> ok end),

    Header = make_header(?REQUEST_CANCEL_JOB),
    Request = #cancel_job_request{job_id = 7001},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_cancel_job_not_found() ->
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_CANCEL_JOB),
    Request = #cancel_job_request{job_id = 999999},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_cancel_job_error() ->
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> {error, permission_denied} end),

    Header = make_header(?REQUEST_CANCEL_JOB),
    Request = #cancel_job_request{job_id = 7002},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

%%====================================================================
%% Suspend Job Tests (REQUEST_SUSPEND - 5014)
%%====================================================================

test_suspend_job_true() ->
    meck:expect(flurm_job_manager, suspend_job, fun(8001) -> ok end),

    Header = make_header(?REQUEST_SUSPEND),
    Request = #suspend_request{job_id = 8001, suspend = true},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_suspend_job_false() ->
    meck:expect(flurm_job_manager, resume_job, fun(8002) -> ok end),

    Header = make_header(?REQUEST_SUSPEND),
    Request = #suspend_request{job_id = 8002, suspend = false},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_suspend_job_not_found() ->
    meck:expect(flurm_job_manager, suspend_job, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_SUSPEND),
    Request = #suspend_request{job_id = 999999, suspend = true},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_suspend_job_error() ->
    meck:expect(flurm_job_manager, suspend_job, fun(_) -> {error, invalid_state} end),

    Header = make_header(?REQUEST_SUSPEND),
    Request = #suspend_request{job_id = 8003, suspend = true},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

%%====================================================================
%% Signal Job Tests (REQUEST_SIGNAL_JOB - 5018)
%%====================================================================

test_signal_job_sigterm() ->
    meck:expect(flurm_job_manager, signal_job, fun(9001, 15) -> ok end),

    Header = make_header(?REQUEST_SIGNAL_JOB),
    Request = #signal_job_request{job_id = 9001, signal = 15},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_signal_job_sigkill() ->
    meck:expect(flurm_job_manager, signal_job, fun(9002, 9) -> ok end),

    Header = make_header(?REQUEST_SIGNAL_JOB),
    Request = #signal_job_request{job_id = 9002, signal = 9},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_signal_job_sighup() ->
    meck:expect(flurm_job_manager, signal_job, fun(9003, 1) -> ok end),

    Header = make_header(?REQUEST_SIGNAL_JOB),
    Request = #signal_job_request{job_id = 9003, signal = 1},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_signal_job_not_found() ->
    meck:expect(flurm_job_manager, signal_job, fun(_, _) -> {error, not_found} end),

    Header = make_header(?REQUEST_SIGNAL_JOB),
    Request = #signal_job_request{job_id = 999999, signal = 15},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_signal_job_error() ->
    meck:expect(flurm_job_manager, signal_job, fun(_, _) -> {error, not_running} end),

    Header = make_header(?REQUEST_SIGNAL_JOB),
    Request = #signal_job_request{job_id = 9004, signal = 15},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

%%====================================================================
%% Update Job Tests (REQUEST_UPDATE_JOB - 4014)
%%====================================================================

test_update_job_hold() ->
    meck:expect(flurm_job_manager, hold_job, fun(10001) -> ok end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10001, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_release_no_val() ->
    meck:expect(flurm_job_manager, get_job, fun(10002) ->
        {ok, #job{id = 10002, state = held, priority = 0}}
    end),
    meck:expect(flurm_job_manager, release_job, fun(10002) -> ok end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10002, priority = 16#FFFFFFFE, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_release_priority() ->
    meck:expect(flurm_job_manager, get_job, fun(10003) ->
        {ok, #job{id = 10003, state = held, priority = 0}}
    end),
    meck:expect(flurm_job_manager, release_job, fun(10003) -> ok end),
    meck:expect(flurm_job_manager, update_job, fun(10003, _) -> ok end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10003, priority = 200, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_requeue() ->
    meck:expect(flurm_job_manager, requeue_job, fun(10004) -> ok end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10004, priority = 16#FFFFFFFE, time_limit = 16#FFFFFFFE, requeue = 1, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_time_limit() ->
    meck:expect(flurm_job_manager, get_job, fun(10005) ->
        {ok, #job{id = 10005, state = pending, priority = 100}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(10005, Updates) ->
        %% Time limit should be converted from minutes to seconds
        ?assertEqual(7200, maps:get(time_limit, Updates)),
        ok
    end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10005, priority = 16#FFFFFFFE, time_limit = 120, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_name() ->
    meck:expect(flurm_job_manager, get_job, fun(10006) ->
        {ok, #job{id = 10006, state = pending, priority = 100}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(10006, Updates) ->
        ?assertEqual(<<"new_name">>, maps:get(name, Updates)),
        ok
    end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10006, priority = 16#FFFFFFFE, time_limit = 16#FFFFFFFE, requeue = 0, name = <<"new_name">>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_with_str() ->
    meck:expect(flurm_job_manager, hold_job, fun(10007) -> ok end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 0, job_id_str = <<"10007">>, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_job_not_found() ->
    meck:expect(flurm_job_manager, hold_job, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 999999, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_update_job_raw_binary() ->
    Header = make_header(?REQUEST_UPDATE_JOB),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, <<"invalid binary">>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_update_job_multiple_fields() ->
    meck:expect(flurm_job_manager, get_job, fun(10008) ->
        {ok, #job{id = 10008, state = pending, priority = 100}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(10008, Updates) ->
        ?assertEqual(3600, maps:get(time_limit, Updates)),
        ?assertEqual(<<"updated_job">>, maps:get(name, Updates)),
        ok
    end),

    Header = make_header(?REQUEST_UPDATE_JOB),
    Request = #update_job_request{job_id = 10008, priority = 16#FFFFFFFE, time_limit = 60, requeue = 0, name = <<"updated_job">>},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Job Will Run Tests (REQUEST_JOB_WILL_RUN - 4012)
%%====================================================================

test_job_will_run_available() ->
    meck:expect(flurm_node_manager_server, get_available_nodes_for_job, fun(_, _, _) ->
        [<<"node1">>, <<"node2">>]
    end),

    Header = make_header(?REQUEST_JOB_WILL_RUN),
    Request = #job_will_run_request{partition = <<"default">>, min_nodes = 1, min_cpus = 1},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_WILL_RUN, MsgType),
    ?assertEqual(0, Response#job_will_run_response.error_code),
    ?assertNotEqual(0, Response#job_will_run_response.start_time),
    ?assertNotEqual(<<>>, Response#job_will_run_response.node_list).

test_job_will_run_unavailable() ->
    meck:expect(flurm_node_manager_server, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    Header = make_header(?REQUEST_JOB_WILL_RUN),
    Request = #job_will_run_request{partition = <<"default">>, min_nodes = 10, min_cpus = 100},
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_WILL_RUN, MsgType),
    ?assertEqual(0, Response#job_will_run_response.error_code),
    ?assertEqual(0, Response#job_will_run_response.start_time),
    ?assertEqual(<<>>, Response#job_will_run_response.node_list).

test_job_will_run_default_partition() ->
    meck:expect(flurm_node_manager_server, get_available_nodes_for_job, fun(Part, _, _) ->
        ?assertEqual(<<"default">>, Part),
        [<<"node1">>]
    end),

    Header = make_header(?REQUEST_JOB_WILL_RUN),
    Request = #job_will_run_request{partition = <<>>, min_nodes = 1, min_cpus = 1},
    {ok, MsgType, _} = flurm_handler_job:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_WILL_RUN, MsgType).

test_job_will_run_fallback() ->
    Header = make_header(?REQUEST_JOB_WILL_RUN),
    {ok, MsgType, Response} = flurm_handler_job:handle(Header, <<"invalid body">>),

    ?assertEqual(?RESPONSE_JOB_WILL_RUN, MsgType),
    ?assertEqual(0, Response#job_will_run_response.error_code).

%%====================================================================
%% Helper Function Tests
%%====================================================================

batch_request_to_job_spec_test_() ->
    [
        {"Convert batch request to job spec", fun() ->
            Request = make_batch_job_request(#{
                name => <<"test_job">>,
                min_nodes => 2,
                min_cpus => 4,
                time_limit => 7200,
                partition => <<"compute">>
            }),
            Spec = flurm_handler_job:batch_request_to_job_spec(Request),
            ?assertEqual(<<"test_job">>, maps:get(name, Spec)),
            ?assertEqual(2, maps:get(num_nodes, Spec)),
            ?assertEqual(4, maps:get(num_cpus, Spec))
        end},
        {"Batch request with zero values uses defaults", fun() ->
            Request = make_batch_job_request(#{min_nodes => 0, min_cpus => 0}),
            Spec = flurm_handler_job:batch_request_to_job_spec(Request),
            ?assertEqual(1, maps:get(num_nodes, Spec)),
            ?assertEqual(1, maps:get(num_cpus, Spec))
        end}
    ].

resource_request_to_job_spec_test_() ->
    [
        {"Convert resource request to job spec", fun() ->
            Request = make_resource_allocation_request(#{name => <<"srun_job">>, min_nodes => 1}),
            Spec = flurm_handler_job:resource_request_to_job_spec(Request),
            ?assertEqual(<<"srun_job">>, maps:get(name, Spec)),
            ?assertEqual(true, maps:get(interactive, Spec)),
            ?assertEqual(<<>>, maps:get(script, Spec))
        end}
    ].

parse_job_id_str_test_() ->
    [
        {"Parse empty string returns 0", fun() ->
            ?assertEqual(0, flurm_handler_job:parse_job_id_str(<<>>))
        end},
        {"Parse simple integer", fun() ->
            ?assertEqual(12345, flurm_handler_job:parse_job_id_str(<<"12345">>))
        end},
        {"Parse array job format", fun() ->
            ?assertEqual(12345, flurm_handler_job:parse_job_id_str(<<"12345_0">>))
        end},
        {"Parse with null terminator", fun() ->
            ?assertEqual(12345, flurm_handler_job:parse_job_id_str(<<"12345", 0>>))
        end}
    ].

safe_binary_to_integer_test_() ->
    [
        {"Convert valid integer string", fun() ->
            ?assertEqual(42, flurm_handler_job:safe_binary_to_integer(<<"42">>))
        end},
        {"Convert invalid string returns 0", fun() ->
            ?assertEqual(0, flurm_handler_job:safe_binary_to_integer(<<"abc">>))
        end},
        {"Convert empty string returns 0", fun() ->
            ?assertEqual(0, flurm_handler_job:safe_binary_to_integer(<<>>))
        end}
    ].

validate_partition_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_partition_manager),
         meck:new(flurm_partition_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_partition_manager),
         ok
     end,
     [
        {"Valid partition returns ok", fun() ->
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {ok, #partition{}} end),
            ?assertEqual(ok, flurm_handler_job:validate_partition(<<"default">>))
        end},
        {"Invalid partition returns error", fun() ->
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
            ?assertEqual({error, {invalid_partition, <<"bad">>}}, flurm_handler_job:validate_partition(<<"bad">>))
        end}
     ]}.

is_cluster_enabled_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"Cluster disabled when no nodes configured", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            ?assertEqual(false, flurm_handler_job:is_cluster_enabled())
        end},
        {"Cluster disabled with single node", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1]),
            ?assertEqual(false, flurm_handler_job:is_cluster_enabled()),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

build_field_updates_test_() ->
    [
        {"Empty updates when NO_VAL", fun() ->
            ?assertEqual(#{}, flurm_handler_job:build_field_updates(16#FFFFFFFE, <<>>))
        end},
        {"Time limit converted to seconds", fun() ->
            Updates = flurm_handler_job:build_field_updates(60, <<>>),
            ?assertEqual(3600, maps:get(time_limit, Updates))
        end},
        {"Name included in updates", fun() ->
            Updates = flurm_handler_job:build_field_updates(16#FFFFFFFE, <<"new_name">>),
            ?assertEqual(<<"new_name">>, maps:get(name, Updates))
        end},
        {"Both time limit and name", fun() ->
            Updates = flurm_handler_job:build_field_updates(120, <<"job_name">>),
            ?assertEqual(7200, maps:get(time_limit, Updates)),
            ?assertEqual(<<"job_name">>, maps:get(name, Updates))
        end}
    ].

%%====================================================================
%% Federation Routing Tests
%%====================================================================

federation_routing_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         catch meck:unload(flurm_job_manager),
         meck:new([flurm_federation, flurm_job_manager], [passthrough, no_link, non_strict]),
         application:set_env(flurm_controller, federation_routing, true),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_federation, flurm_job_manager]),
         application:unset_env(flurm_controller, federation_routing),
         ok
     end,
     [
        {"Federation routing disabled returns local submit", fun() ->
            application:set_env(flurm_controller, federation_routing, false),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 11001} end),
            ?assertEqual({ok, 11001}, flurm_handler_job:submit_job_with_federation(#{})),
            application:set_env(flurm_controller, federation_routing, true)
        end},
        {"Get local cluster name from federation", fun() ->
            meck:expect(flurm_federation, get_local_cluster, fun() -> <<"cluster1">> end),
            ?assertEqual(<<"cluster1">>, flurm_handler_job:get_local_cluster_name())
        end},
        {"Get local cluster name fallback to config", fun() ->
            meck:expect(flurm_federation, get_local_cluster, fun() -> error(noproc) end),
            application:set_env(flurm_controller, cluster_name, <<"config_cluster">>),
            ?assertEqual(<<"config_cluster">>, flurm_handler_job:get_local_cluster_name()),
            application:unset_env(flurm_controller, cluster_name)
        end}
     ]}.

%%====================================================================
%% Cluster Mode Tests
%%====================================================================

cluster_mode_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_controller_cluster),
         catch meck:unload(flurm_job_manager),
         meck:new([flurm_controller_cluster, flurm_job_manager], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_controller_cluster, flurm_job_manager]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Submit job as leader in cluster mode", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 12001} end),
            ?assertEqual({ok, 12001}, flurm_handler_job:submit_job_locally(#{})),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Submit job as follower forwards to leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(submit_job, _) ->
                {ok, {ok, 12002}}
            end),
            ?assertEqual({ok, 12002}, flurm_handler_job:submit_job_locally(#{})),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Forward to leader fails with no_leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, no_leader}
            end),
            ?assertEqual({error, controller_not_found}, flurm_handler_job:submit_job_locally(#{})),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

%%====================================================================
%% Execute Suspend Tests
%%====================================================================

execute_suspend_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Execute suspend calls suspend_job", fun() ->
            meck:expect(flurm_job_manager, suspend_job, fun(13001) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_suspend(13001, true))
        end},
        {"Execute resume calls resume_job", fun() ->
            meck:expect(flurm_job_manager, resume_job, fun(13002) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_suspend(13002, false))
        end}
     ]}.

%%====================================================================
%% Execute Job Update Tests
%%====================================================================

execute_job_update_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Requeue takes precedence", fun() ->
            meck:expect(flurm_job_manager, requeue_job, fun(14001) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_job_update(14001, 100, 60, 1, <<"name">>))
        end},
        {"Priority 0 triggers hold", fun() ->
            meck:expect(flurm_job_manager, hold_job, fun(14002) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_job_update(14002, 0, 16#FFFFFFFE, 0, <<>>))
        end},
        {"Priority NO_VAL on held job triggers release", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(14003) ->
                {ok, #job{id = 14003, state = held, priority = 0}}
            end),
            meck:expect(flurm_job_manager, release_job, fun(14003) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_job_update(14003, 16#FFFFFFFE, 16#FFFFFFFE, 0, <<>>))
        end},
        {"Non-zero priority on held job triggers release", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(14004) ->
                {ok, #job{id = 14004, state = held, priority = 0}}
            end),
            meck:expect(flurm_job_manager, release_job, fun(14004) -> ok end),
            meck:expect(flurm_job_manager, update_job, fun(14004, _) -> ok end),
            ?assertEqual(ok, flurm_handler_job:execute_job_update(14004, 200, 16#FFFFFFFE, 0, <<>>))
        end},
        {"Normal priority update on non-held job", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(14005) ->
                {ok, #job{id = 14005, state = pending, priority = 100}}
            end),
            meck:expect(flurm_job_manager, update_job, fun(14005, Updates) ->
                ?assertEqual(200, maps:get(priority, Updates)),
                ok
            end),
            ?assertEqual(ok, flurm_handler_job:execute_job_update(14005, 200, 16#FFFFFFFE, 0, <<>>))
        end}
     ]}.

%%====================================================================
%% Apply Base Updates Tests
%%====================================================================

apply_base_updates_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Empty updates returns ok without call", fun() ->
            ?assertEqual(ok, flurm_handler_job:apply_base_updates(15001, #{}))
        end},
        {"Non-empty updates calls update_job", fun() ->
            meck:expect(flurm_job_manager, update_job, fun(15002, Updates) ->
                ?assertEqual(3600, maps:get(time_limit, Updates)),
                ok
            end),
            ?assertEqual(ok, flurm_handler_job:apply_base_updates(15002, #{time_limit => 3600}))
        end}
     ]}.

%%====================================================================
%% Federation Routing Extended Tests
%%====================================================================

federation_routing_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         catch meck:unload(flurm_job_manager),
         meck:new([flurm_federation, flurm_job_manager], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_federation, flurm_job_manager]),
         application:unset_env(flurm_controller, federation_routing),
         ok
     end,
     [
        {"Federation routing to remote cluster with tracking", fun() ->
            application:set_env(flurm_controller, federation_routing, true),
            meck:expect(flurm_federation, is_federated, fun() -> true end),
            meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"remote_cluster">>} end),
            meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local_cluster">> end),
            meck:expect(flurm_federation, submit_job, fun(<<"remote_cluster">>, _) -> {ok, 16001} end),
            meck:expect(flurm_federation, track_remote_job, fun(_, _, _) -> ok end),
            ?assertEqual({ok, 16001}, flurm_handler_job:submit_job_with_federation(#{})),
            application:unset_env(flurm_controller, federation_routing)
        end},
        {"Federation routing to local cluster", fun() ->
            application:set_env(flurm_controller, federation_routing, true),
            meck:expect(flurm_federation, is_federated, fun() -> true end),
            meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"local_cluster">>} end),
            meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local_cluster">> end),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 16002} end),
            ?assertEqual({ok, 16002}, flurm_handler_job:submit_job_with_federation(#{})),
            application:unset_env(flurm_controller, federation_routing)
        end},
        {"Federation remote submit fails falls back to local", fun() ->
            application:set_env(flurm_controller, federation_routing, true),
            meck:expect(flurm_federation, is_federated, fun() -> true end),
            meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"remote_cluster">>} end),
            meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local_cluster">> end),
            meck:expect(flurm_federation, submit_job, fun(_, _) -> {error, remote_unavailable} end),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 16003} end),
            ?assertEqual({ok, 16003}, flurm_handler_job:submit_job_with_federation(#{})),
            application:unset_env(flurm_controller, federation_routing)
        end},
        {"Federation returns no_eligible_clusters", fun() ->
            application:set_env(flurm_controller, federation_routing, true),
            meck:expect(flurm_federation, is_federated, fun() -> true end),
            meck:expect(flurm_federation, route_job, fun(_) -> {error, no_eligible_clusters} end),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 16004} end),
            ?assertEqual({ok, 16004}, flurm_handler_job:submit_job_with_federation(#{})),
            application:unset_env(flurm_controller, federation_routing)
        end},
        {"Federation not federated falls back to local", fun() ->
            application:set_env(flurm_controller, federation_routing, true),
            meck:expect(flurm_federation, is_federated, fun() -> false end),
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 16005} end),
            ?assertEqual({ok, 16005}, flurm_handler_job:submit_job_with_federation(#{})),
            application:unset_env(flurm_controller, federation_routing)
        end},
        {"Get local cluster name from config fallback", fun() ->
            meck:expect(flurm_federation, get_local_cluster, fun() -> error(noproc) end),
            application:unset_env(flurm_controller, cluster_name),
            ?assertEqual(<<"local">>, flurm_handler_job:get_local_cluster_name())
        end},
        {"Get local cluster name from list config", fun() ->
            meck:expect(flurm_federation, get_local_cluster, fun() -> error(noproc) end),
            application:set_env(flurm_controller, cluster_name, "list_cluster"),
            ?assertEqual(<<"list_cluster">>, flurm_handler_job:get_local_cluster_name()),
            application:unset_env(flurm_controller, cluster_name)
        end}
     ]}.

%%====================================================================
%% Resource Allocation Extended Tests
%%====================================================================

resource_allocation_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_node_manager_server),
         catch meck:unload(flurm_config_server),
         meck:new([flurm_job_manager, flurm_node_manager_server, flurm_config_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_node_manager_server, flurm_config_server]),
         ok
     end,
     [
        {"Resource allocation with map node format", fun() ->
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 17001} end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [#{hostname => <<"map-node">>}]
            end),
            meck:expect(flurm_config_server, get, fun(_, D) -> D end),
            Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
            Request = make_resource_allocation_request(),
            {ok, _, Response, _} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(<<"map-node">>, Response#resource_allocation_response.node_list)
        end},
        {"Resource allocation with invalid node format falls back to default", fun() ->
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 17002} end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [invalid_node_format]
            end),
            meck:expect(flurm_config_server, get, fun(default_node, _) -> <<"default-fallback">>; (_, D) -> D end),
            Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
            Request = make_resource_allocation_request(),
            {ok, _, Response, _} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(<<"default-fallback">>, Response#resource_allocation_response.node_list)
        end},
        {"Resource allocation with config_server exception", fun() ->
            meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 17003} end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            meck:expect(flurm_config_server, get, fun(_, _) -> error(crash) end),
            Header = make_header(?REQUEST_RESOURCE_ALLOCATION),
            Request = make_resource_allocation_request(),
            {ok, _, Response, _} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(17003, Response#resource_allocation_response.job_id)
        end}
     ]}.

%%====================================================================
%% Kill Job Extended Tests
%%====================================================================

kill_job_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_controller_cluster),
         meck:new([flurm_job_manager, flurm_controller_cluster], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_controller_cluster]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Kill job cluster mode forward success", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(cancel_job, 18001) ->
                {ok, ok}
            end),
            Header = make_header(?REQUEST_KILL_JOB),
            Request = #kill_job_request{job_id = 18001, job_id_str = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Kill job cluster mode cluster_not_ready error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, cluster_not_ready}
            end),
            Header = make_header(?REQUEST_KILL_JOB),
            Request = #kill_job_request{job_id = 18002, job_id_str = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Kill job with invalid job_id_str", fun() ->
            meck:expect(flurm_job_manager, cancel_job, fun(0) -> ok end),
            Header = make_header(?REQUEST_KILL_JOB),
            Request = #kill_job_request{job_id = 0, job_id_str = <<"not_a_number">>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end}
     ]}.

%%====================================================================
%% Cancel Job Extended Tests
%%====================================================================

cancel_job_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_controller_cluster),
         meck:new([flurm_job_manager, flurm_controller_cluster], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_controller_cluster]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Cancel job cluster mode leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
            meck:expect(flurm_job_manager, cancel_job, fun(19001) -> ok end),
            Header = make_header(?REQUEST_CANCEL_JOB),
            Request = #cancel_job_request{job_id = 19001},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Cancel job cluster mode forward cluster_not_ready", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, cluster_not_ready}
            end),
            Header = make_header(?REQUEST_CANCEL_JOB),
            Request = #cancel_job_request{job_id = 19002},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Cancel job cluster mode forward generic error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, timeout}
            end),
            Header = make_header(?REQUEST_CANCEL_JOB),
            Request = #cancel_job_request{job_id = 19003},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

%%====================================================================
%% Signal Job Extended Tests
%%====================================================================

signal_job_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_controller_cluster),
         meck:new([flurm_job_manager, flurm_controller_cluster], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_controller_cluster]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Signal job cluster mode leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
            meck:expect(flurm_job_manager, signal_job, fun(20001, 15) -> ok end),
            Header = make_header(?REQUEST_SIGNAL_JOB),
            Request = #signal_job_request{job_id = 20001, signal = 15},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Signal job cluster mode forward success", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(signal_job, {20002, 9}) ->
                {ok, ok}
            end),
            Header = make_header(?REQUEST_SIGNAL_JOB),
            Request = #signal_job_request{job_id = 20002, signal = 9},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Signal job cluster mode forward no_leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_SIGNAL_JOB),
            Request = #signal_job_request{job_id = 20003, signal = 1},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

%%====================================================================
%% Suspend Job Extended Tests
%%====================================================================

suspend_job_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_controller_cluster),
         meck:new([flurm_job_manager, flurm_controller_cluster], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_controller_cluster]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Suspend job cluster mode leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
            meck:expect(flurm_job_manager, suspend_job, fun(21001) -> ok end),
            Header = make_header(?REQUEST_SUSPEND),
            Request = #suspend_request{job_id = 21001, suspend = true},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Resume job cluster mode forward success", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(resume_job, 21002) ->
                {ok, ok}
            end),
            Header = make_header(?REQUEST_SUSPEND),
            Request = #suspend_request{job_id = 21002, suspend = false},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Suspend job cluster mode forward error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, some_error}
            end),
            Header = make_header(?REQUEST_SUSPEND),
            Request = #suspend_request{job_id = 21003, suspend = true},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

%%====================================================================
%% Update Job Extended Tests
%%====================================================================

update_job_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_controller_cluster),
         meck:new([flurm_job_manager, flurm_controller_cluster], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_controller_cluster]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Update job cluster mode leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
            meck:expect(flurm_job_manager, hold_job, fun(22001) -> ok end),
            Header = make_header(?REQUEST_UPDATE_JOB),
            Request = #update_job_request{job_id = 22001, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Update job cluster mode forward success", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_job_ext, _) ->
                {ok, ok}
            end),
            Header = make_header(?REQUEST_UPDATE_JOB),
            Request = #update_job_request{job_id = 22002, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Update job cluster mode forward no_leader", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_UPDATE_JOB),
            Request = #update_job_request{job_id = 22003, priority = 0, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Update job with running priority 0 job release", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(22004) ->
                {ok, #job{id = 22004, state = running, priority = 0}}
            end),
            meck:expect(flurm_job_manager, release_job, fun(22004) -> ok end),
            Header = make_header(?REQUEST_UPDATE_JOB),
            Request = #update_job_request{job_id = 22004, priority = 16#FFFFFFFE, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end},
        {"Update job get_job returns error", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(22005) ->
                {error, not_found}
            end),
            Header = make_header(?REQUEST_UPDATE_JOB),
            Request = #update_job_request{job_id = 22005, priority = 200, time_limit = 16#FFFFFFFE, requeue = 0, name = <<>>},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code)
        end}
     ]}.

%%====================================================================
%% Job Will Run Extended Tests
%%====================================================================

job_will_run_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_node_manager_server),
         meck:new(flurm_node_manager_server, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_node_manager_server),
         ok
     end,
     [
        {"Job will run with exception in get_available_nodes", fun() ->
            meck:expect(flurm_node_manager_server, get_available_nodes_for_job, fun(_, _, _) ->
                error(crash)
            end),
            Header = make_header(?REQUEST_JOB_WILL_RUN),
            Request = #job_will_run_request{partition = <<"default">>, min_nodes = 1, min_cpus = 1},
            {ok, MsgType, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(?RESPONSE_JOB_WILL_RUN, MsgType),
            %% Should return 0 start_time when resources unavailable
            ?assertEqual(0, Response#job_will_run_response.start_time)
        end},
        {"Job will run with more nodes than available", fun() ->
            meck:expect(flurm_node_manager_server, get_available_nodes_for_job, fun(_, _, _) ->
                [<<"node1">>]
            end),
            Header = make_header(?REQUEST_JOB_WILL_RUN),
            Request = #job_will_run_request{partition = <<"default">>, min_nodes = 5, min_cpus = 1},
            {ok, _, Response} = flurm_handler_job:handle(Header, Request),
            ?assertEqual(0, Response#job_will_run_response.start_time),
            ?assertEqual(<<>>, Response#job_will_run_response.node_list)
        end}
     ]}.

%%====================================================================
%% Cluster Enabled Edge Cases
%%====================================================================

cluster_enabled_edge_cases_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) ->
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Cluster enabled with process running", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            %% Simulate process running by registering a dummy process
            Pid = spawn(fun() -> receive stop -> ok end end),
            register(flurm_controller_cluster, Pid),
            ?assertEqual(true, flurm_handler_job:is_cluster_enabled()),
            Pid ! stop,
            unregister(flurm_controller_cluster)
        end},
        {"Cluster disabled with empty list", fun() ->
            application:set_env(flurm_controller, cluster_nodes, []),
            ?assertEqual(false, flurm_handler_job:is_cluster_enabled())
        end}
     ]}.

%%====================================================================
%% Parse Job ID Str Extended Tests
%%====================================================================

parse_job_id_str_extended_test_() ->
    [
        {"Parse job id with underscore and trailing null", fun() ->
            ?assertEqual(54321, flurm_handler_job:parse_job_id_str(<<"54321_10", 0>>))
        end},
        {"Parse job id from binary with only numbers", fun() ->
            ?assertEqual(99999, flurm_handler_job:parse_job_id_str(<<"99999">>))
        end},
        {"Parse job id with multiple underscores", fun() ->
            ?assertEqual(11111, flurm_handler_job:parse_job_id_str(<<"11111_0_extra">>))
        end}
    ].

%%====================================================================
%% Validate Partition Extended Tests
%%====================================================================

validate_partition_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_partition_manager),
         meck:new(flurm_partition_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_partition_manager),
         ok
     end,
     [
        {"Validate partition with EXIT error allows submission", fun() ->
            meck:expect(flurm_partition_manager, get_partition, fun(_) ->
                exit(crashed)
            end),
            ?assertEqual(ok, flurm_handler_job:validate_partition(<<"crashed_partition">>))
        end}
     ]}.

%%====================================================================
%% Job Allocation Info Extended Tests
%%====================================================================

job_allocation_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_node_manager_server),
         catch meck:unload(flurm_config_server),
         meck:new([flurm_node_manager_server, flurm_config_server], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_node_manager_server, flurm_config_server]),
         ok
     end,
     [
        {"Job allocation info with map node", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [#{hostname => <<"map-node-alloc">>}]
            end),
            meck:expect(flurm_config_server, get, fun(_, D) -> D end),
            Header = make_header(?REQUEST_JOB_ALLOCATION_INFO),
            Body = <<23001:32/big>>,
            {ok, _, Response} = flurm_handler_job:handle(Header, Body),
            ?assertEqual(<<"map-node-alloc">>, Response#resource_allocation_response.node_list)
        end},
        {"Job allocation info with invalid node format", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [invalid_format]
            end),
            meck:expect(flurm_config_server, get, fun(default_node, _) -> <<"fallback">>; (_, D) -> D end),
            Header = make_header(?REQUEST_JOB_ALLOCATION_INFO),
            Body = <<23002:32/big>>,
            {ok, _, Response} = flurm_handler_job:handle(Header, Body),
            ?assertEqual(<<"fallback">>, Response#resource_allocation_response.node_list)
        end}
     ]}.
