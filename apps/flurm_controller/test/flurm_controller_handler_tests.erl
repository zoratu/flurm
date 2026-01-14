%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler Tests
%%%
%%% Comprehensive tests for the flurm_controller_handler module.
%%% Tests all major message types and error handling.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Handle PING request", fun test_ping_request/0},
        {"Handle batch job submission - happy path", fun test_batch_job_submission/0},
        {"Handle batch job submission - failure", fun test_batch_job_submission_failure/0},
        {"Handle job info request - all jobs", fun test_job_info_all/0},
        {"Handle job info request - specific job", fun test_job_info_specific/0},
        {"Handle job info request - not found", fun test_job_info_not_found/0},
        {"Handle cancel job request - success", fun test_cancel_job_success/0},
        {"Handle cancel job request - not found", fun test_cancel_job_not_found/0},
        {"Handle node info request", fun test_node_info_request/0},
        {"Handle partition info request", fun test_partition_info_request/0},
        {"Handle build info request", fun test_build_info_request/0},
        {"Handle scontrol info request", fun test_scontrol_info_request/0},
        {"Handle kill job request - with job_id", fun test_kill_job_with_id/0},
        {"Handle kill job request - with job_id_str", fun test_kill_job_with_str/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),

    %% Disable cluster mode
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Start core dependencies - handle already_started gracefully
    JobRegistryPid = start_or_get(flurm_job_registry, fun flurm_job_registry:start_link/0),
    JobSupPid = start_or_get(flurm_job_sup, fun flurm_job_sup:start_link/0),
    NodeRegistryPid = start_or_get(flurm_node_registry, fun flurm_node_registry:start_link/0),
    NodeSupPid = start_or_get(flurm_node_sup, fun flurm_node_sup:start_link/0),
    LimitsPid = start_or_get(flurm_limits, fun flurm_limits:start_link/0),
    LicensePid = start_or_get(flurm_license, fun flurm_license:start_link/0),
    JobManagerPid = start_or_get(flurm_job_manager, fun flurm_job_manager:start_link/0),
    SchedulerPid = start_or_get(flurm_scheduler, fun flurm_scheduler:start_link/0),
    StepManagerPid = start_or_get(flurm_step_manager, fun flurm_step_manager:start_link/0),
    NodeManagerPid = start_or_get(flurm_node_manager_server, fun flurm_node_manager_server:start_link/0),
    PartitionManagerPid = start_or_get(flurm_partition_manager, fun flurm_partition_manager:start_link/0),
    AccountManagerPid = start_or_get(flurm_account_manager, fun flurm_account_manager:start_link/0),

    #{
        job_registry => JobRegistryPid,
        job_sup => JobSupPid,
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid,
        limits => LimitsPid,
        license => LicensePid,
        job_manager => JobManagerPid,
        scheduler => SchedulerPid,
        step_manager => StepManagerPid,
        node_manager => NodeManagerPid,
        partition_manager => PartitionManagerPid,
        account_manager => AccountManagerPid
    }.

%% Helper to start a process or return existing pid
start_or_get(Name, StartFun) ->
    case whereis(Name) of
        undefined ->
            case StartFun() of
                {ok, Pid} -> Pid;
                {error, {already_started, Pid}} -> Pid
            end;
        Pid -> Pid
    end.

cleanup(Pids) ->
    %% Stop all jobs first
    catch [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Stop all nodes
    catch [flurm_node_sup:stop_node(Pid) || Pid <- flurm_node_sup:which_nodes()],

    %% Don't stop processes that might be shared with other tests
    %% Just let them continue running
    _ = Pids,
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
        min_mem_per_node => 512,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>
    },
    Props = maps:merge(Defaults, Overrides),
    #batch_job_request{
        name = maps:get(name, Props),
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        min_nodes = maps:get(min_nodes, Props),
        min_cpus = maps:get(min_cpus, Props),
        min_mem_per_node = maps:get(min_mem_per_node, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props)
    }.

%%====================================================================
%% Test Cases
%%====================================================================

test_ping_request() ->
    Header = make_header(?REQUEST_PING),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_batch_job_submission() ->
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, Request),
    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    ?assert(Response#batch_job_response.job_id > 0),
    ?assertEqual(0, Response#batch_job_response.error_code).

test_batch_job_submission_failure() ->
    %% Request too many nodes (will fail resource check)
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{min_nodes => 9999}),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, Request),
    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, MsgType),
    %% Job still gets created, just won't be scheduled
    ?assert(Response#batch_job_response.job_id >= 0).

test_job_info_all() ->
    %% Submit a job first
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
    _JobId = SubmitResp#batch_job_response.job_id,

    %% Query all jobs
    InfoHeader = make_header(?REQUEST_JOB_INFO),
    InfoRequest = #job_info_request{job_id = 0},  % 0 = all jobs
    {ok, MsgType, Response} = flurm_controller_handler:handle(InfoHeader, InfoRequest),
    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assert(Response#job_info_response.job_count >= 1).

test_job_info_specific() ->
    %% Submit a job first
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(#{name => <<"specific_job">>}),
    {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
    JobId = SubmitResp#batch_job_response.job_id,

    %% Query specific job
    InfoHeader = make_header(?REQUEST_JOB_INFO),
    InfoRequest = #job_info_request{job_id = JobId},
    {ok, MsgType, Response} = flurm_controller_handler:handle(InfoHeader, InfoRequest),
    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(1, Response#job_info_response.job_count).

test_job_info_not_found() ->
    %% Query non-existent job
    InfoHeader = make_header(?REQUEST_JOB_INFO),
    InfoRequest = #job_info_request{job_id = 999999},
    {ok, MsgType, Response} = flurm_controller_handler:handle(InfoHeader, InfoRequest),
    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(0, Response#job_info_response.job_count).

test_cancel_job_success() ->
    %% Submit a job first
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
    JobId = SubmitResp#batch_job_response.job_id,

    %% Cancel the job
    CancelHeader = make_header(?REQUEST_CANCEL_JOB),
    CancelRequest = #cancel_job_request{job_id = JobId},
    {ok, MsgType, Response} = flurm_controller_handler:handle(CancelHeader, CancelRequest),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_cancel_job_not_found() ->
    CancelHeader = make_header(?REQUEST_CANCEL_JOB),
    CancelRequest = #cancel_job_request{job_id = 999999},
    {ok, MsgType, Response} = flurm_controller_handler:handle(CancelHeader, CancelRequest),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    %% Not found returns -1
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_node_info_request() ->
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_NODE_INFO, MsgType),
    %% Should return a valid response with node count >= 0
    ?assert(Response#node_info_response.node_count >= 0).

test_partition_info_request() ->
    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_PARTITION_INFO, MsgType),
    %% Should return a valid response with partition count >= 0
    ?assert(Response#partition_info_response.partition_count >= 0).

test_build_info_request() ->
    Header = make_header(?REQUEST_BUILD_INFO),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_BUILD_INFO, MsgType),
    %% Verify version info is present
    ?assertNotEqual(<<>>, Response#build_info_response.version),
    ?assertEqual(22, Response#build_info_response.version_major).

test_scontrol_info_request() ->
    Header = make_header(?REQUEST_SCONTROL_INFO),
    {ok, MsgType, Response} = flurm_controller_handler:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_with_id() ->
    %% Submit a job first
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
    JobId = SubmitResp#batch_job_response.job_id,

    %% Kill the job using job_id
    KillHeader = make_header(?REQUEST_KILL_JOB),
    KillRequest = #kill_job_request{job_id = JobId, job_id_str = <<>>},
    {ok, MsgType, Response} = flurm_controller_handler:handle(KillHeader, KillRequest),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_kill_job_with_str() ->
    %% Submit a job first
    Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
    Request = make_batch_job_request(),
    {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
    JobId = SubmitResp#batch_job_response.job_id,

    %% Kill the job using job_id_str
    KillHeader = make_header(?REQUEST_KILL_JOB),
    JobIdStr = integer_to_binary(JobId),
    KillRequest = #kill_job_request{job_id = 0, job_id_str = JobIdStr},
    {ok, MsgType, Response} = flurm_controller_handler:handle(KillHeader, KillRequest),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Edge Case Tests
%%====================================================================

%% Test job user info request (2021)
job_user_info_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         {"Handle job user info request (2021)", fun() ->
             Header = make_header(?REQUEST_JOB_USER_INFO),
             Body = <<0:32/big>>,  % job_id = 0 = all jobs
             {ok, MsgType, Response} = flurm_controller_handler:handle(Header, Body),
             ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
             ?assert(Response#job_info_response.job_count >= 0)
         end}
     end}.

%% Test job info single request (2005)
job_info_single_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         {"Handle job info single request (2005)", fun() ->
             %% First submit a job
             Header = make_header(?REQUEST_SUBMIT_BATCH_JOB),
             Request = make_batch_job_request(),
             {ok, _, SubmitResp} = flurm_controller_handler:handle(Header, Request),
             JobId = SubmitResp#batch_job_response.job_id,

             %% Query using single job request type
             InfoHeader = make_header(?REQUEST_JOB_INFO_SINGLE),
             InfoRequest = #job_info_request{job_id = JobId},
             {ok, MsgType, Response} = flurm_controller_handler:handle(InfoHeader, InfoRequest),
             ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
             ?assertEqual(1, Response#job_info_response.job_count)
         end}
     end}.
