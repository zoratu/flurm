%%%-------------------------------------------------------------------
%%% @doc FLURM Step Handler Tests
%%%
%%% Comprehensive tests for the flurm_handler_step module.
%%% Tests all handle/2 clauses for step-related operations:
%%% - Job step creation
%%% - Job step info queries
%%% - Prolog completion notifications
%%% - Epilog completion notifications
%%% - Task exit notifications
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_step_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_step_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_JOB_STEP_CREATE (5001) tests
        {"Step create - success", fun test_step_create_success/0},
        {"Step create - with name", fun test_step_create_with_name/0},
        {"Step create - with tasks", fun test_step_create_with_tasks/0},
        {"Step create - failure", fun test_step_create_failure/0},
        {"Step create - cluster follower", fun test_step_create_cluster_follower/0},
        {"Step create - dispatch to nodes", fun test_step_create_dispatch/0},
        {"Step create - missing job", fun test_step_create_missing_job/0},

        %% REQUEST_JOB_STEP_INFO (5003) tests
        {"Step info - all steps", fun test_step_info_all/0},
        {"Step info - specific step", fun test_step_info_specific/0},
        {"Step info - step not found", fun test_step_info_not_found/0},
        {"Step info - for job", fun test_step_info_for_job/0},
        {"Step info - response structure", fun test_step_info_response_structure/0},

        %% REQUEST_COMPLETE_PROLOG (5019) tests
        {"Prolog complete - success (rc=0)", fun test_prolog_complete_success/0},
        {"Prolog complete - failure (rc=1)", fun test_prolog_complete_failure/0},
        {"Prolog complete - cluster follower", fun test_prolog_complete_cluster_follower/0},
        {"Prolog complete - different node", fun test_prolog_complete_different_node/0},

        %% MESSAGE_EPILOG_COMPLETE (6012) tests
        {"Epilog complete - success (rc=0)", fun test_epilog_complete_success/0},
        {"Epilog complete - failure (rc=1)", fun test_epilog_complete_failure/0},
        {"Epilog complete - cluster follower", fun test_epilog_complete_cluster_follower/0},
        {"Epilog complete - different node", fun test_epilog_complete_different_node/0},

        %% MESSAGE_TASK_EXIT (6003) tests
        {"Task exit - normal (rc=0)", fun test_task_exit_normal/0},
        {"Task exit - failure (rc=1)", fun test_task_exit_failure/0},
        {"Task exit - with task ids", fun test_task_exit_with_task_ids/0},
        {"Task exit - cluster follower", fun test_task_exit_cluster_follower/0},
        {"Task exit - step not found", fun test_task_exit_step_not_found/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Disable cluster mode
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Start meck for mocking
    catch meck:unload(flurm_step_manager),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_controller_cluster),
    catch meck:unload(flurm_srun_callback),
    catch meck:unload(flurm_node_connection_manager),
    meck:new([flurm_step_manager, flurm_job_manager, flurm_node_manager_server,
              flurm_controller_cluster, flurm_srun_callback, flurm_node_connection_manager],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_step_manager, create_step, fun(_, _) -> {ok, 0} end),
    meck:expect(flurm_step_manager, list_steps, fun(_) -> [] end),
    meck:expect(flurm_step_manager, get_step, fun(_, _) -> {error, not_found} end),
    meck:expect(flurm_step_manager, complete_step, fun(_, _, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job_manager, update_prolog_status, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, update_epilog_status, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_srun_callback, notify_job_complete, fun(_, _, _) -> ok end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),

    ok.

cleanup(_) ->
    %% Unload all mocks
    catch meck:unload([flurm_step_manager, flurm_job_manager, flurm_node_manager_server,
                       flurm_controller_cluster, flurm_srun_callback, flurm_node_connection_manager]),
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

make_job(JobId) ->
    make_job(JobId, #{}).

make_job(JobId, Overrides) ->
    Defaults = #{
        id => JobId,
        name => <<"test_job">>,
        state => running,
        partition => <<"default">>,
        user => <<"root">>,
        work_dir => <<"/tmp">>,
        allocated_nodes => [<<"node1">>]
    },
    Props = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Props),
        name = maps:get(name, Props),
        state = maps:get(state, Props),
        partition = maps:get(partition, Props),
        user = maps:get(user, Props),
        work_dir = maps:get(work_dir, Props),
        allocated_nodes = maps:get(allocated_nodes, Props)
    }.

make_step_request(JobId) ->
    make_step_request(JobId, #{}).

make_step_request(JobId, Overrides) ->
    Defaults = #{
        job_id => JobId,
        name => <<"step_0">>,
        min_nodes => 1,
        num_tasks => 1,
        cpus_per_task => 1,
        time_limit => 3600
    },
    Props = maps:merge(Defaults, Overrides),
    #job_step_create_request{
        job_id = maps:get(job_id, Props),
        name = maps:get(name, Props),
        min_nodes = maps:get(min_nodes, Props),
        num_tasks = maps:get(num_tasks, Props),
        cpus_per_task = maps:get(cpus_per_task, Props),
        time_limit = maps:get(time_limit, Props)
    }.

%%====================================================================
%% Step Create Tests (REQUEST_JOB_STEP_CREATE - 5001)
%%====================================================================

test_step_create_success() ->
    meck:expect(flurm_step_manager, create_step, fun(1001, _) -> {ok, 0} end),
    meck:expect(flurm_job_manager, get_job, fun(1001) -> {ok, make_job(1001)} end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [{<<"node1">>, ok}] end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(1001),
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_CREATE, MsgType),
    ?assertEqual(0, Response#job_step_create_response.job_step_id),
    ?assertEqual(1001, Response#job_step_create_response.job_id),
    ?assertEqual(0, Response#job_step_create_response.error_code).

test_step_create_with_name() ->
    meck:expect(flurm_step_manager, create_step, fun(1002, Spec) ->
        ?assertEqual(<<"my_step">>, maps:get(name, Spec)),
        {ok, 1}
    end),
    meck:expect(flurm_job_manager, get_job, fun(1002) -> {ok, make_job(1002)} end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(1002, #{name => <<"my_step">>}),
    {ok, _, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(1, Response#job_step_create_response.job_step_id).

test_step_create_with_tasks() ->
    meck:expect(flurm_step_manager, create_step, fun(1003, Spec) ->
        ?assertEqual(4, maps:get(num_tasks, Spec)),
        {ok, 2}
    end),
    meck:expect(flurm_job_manager, get_job, fun(1003) -> {ok, make_job(1003)} end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(1003, #{num_tasks => 4}),
    {ok, _, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(4, Response#job_step_create_response.num_tasks).

test_step_create_failure() ->
    meck:expect(flurm_step_manager, create_step, fun(_, _) -> {error, job_not_found} end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(9999),
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_CREATE, MsgType),
    ?assertEqual(0, Response#job_step_create_response.job_step_id),
    ?assertEqual(1, Response#job_step_create_response.error_code),
    ?assertNotEqual(<<>>, Response#job_step_create_response.error_msg).

test_step_create_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(create_step, {1004, _}) ->
        {ok, {ok, 3}}
    end),
    meck:expect(flurm_job_manager, get_job, fun(1004) -> {ok, make_job(1004)} end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(1004),
    {ok, _, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(3, Response#job_step_create_response.job_step_id),
    application:unset_env(flurm_controller, cluster_nodes).

test_step_create_dispatch() ->
    meck:expect(flurm_step_manager, create_step, fun(1005, _) -> {ok, 0} end),
    meck:expect(flurm_job_manager, get_job, fun(1005) ->
        {ok, make_job(1005, #{allocated_nodes => [<<"node1">>, <<"node2">>]})}
    end),
    meck:expect(flurm_node_connection_manager, send_to_nodes, fun(Nodes, Msg) ->
        ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
        ?assertEqual(step_launch, maps:get(type, Msg)),
        [{<<"node1">>, ok}, {<<"node2">>, ok}]
    end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(1005),
    {ok, _, _} = flurm_handler_step:handle(Header, Request).

test_step_create_missing_job() ->
    meck:expect(flurm_step_manager, create_step, fun(_, _) -> {error, job_not_found} end),

    Header = make_header(?REQUEST_JOB_STEP_CREATE),
    Request = make_step_request(99999),
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_CREATE, MsgType),
    ?assertEqual(1, Response#job_step_create_response.error_code).

%%====================================================================
%% Step Info Tests (REQUEST_JOB_STEP_INFO - 5003)
%%====================================================================

test_step_info_all() ->
    meck:expect(flurm_step_manager, list_steps, fun(_) ->
        [
            #{job_id => 2001, step_id => 0, name => <<"step0">>, state => running, num_tasks => 1},
            #{job_id => 2001, step_id => 1, name => <<"step1">>, state => completed, num_tasks => 2}
        ]
    end),

    Header = make_header(?REQUEST_JOB_STEP_INFO),
    Request = #job_step_info_request{job_id = 2001, step_id = -1},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_INFO, MsgType),
    ?assertEqual(2, Response#job_step_info_response.step_count).

test_step_info_specific() ->
    meck:expect(flurm_step_manager, get_step, fun(2002, 0) ->
        {ok, #{job_id => 2002, step_id => 0, name => <<"step0">>, state => running}}
    end),

    Header = make_header(?REQUEST_JOB_STEP_INFO),
    Request = #job_step_info_request{job_id = 2002, step_id = 0},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_INFO, MsgType),
    ?assertEqual(1, Response#job_step_info_response.step_count).

test_step_info_not_found() ->
    meck:expect(flurm_step_manager, get_step, fun(_, _) -> {error, not_found} end),

    Header = make_header(?REQUEST_JOB_STEP_INFO),
    Request = #job_step_info_request{job_id = 9999, step_id = 0},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_STEP_INFO, MsgType),
    ?assertEqual(0, Response#job_step_info_response.step_count).

test_step_info_for_job() ->
    meck:expect(flurm_step_manager, list_steps, fun(2003) ->
        [
            #{job_id => 2003, step_id => 0, state => completed},
            #{job_id => 2003, step_id => 1, state => running}
        ]
    end),

    Header = make_header(?REQUEST_JOB_STEP_INFO),
    Request = #job_step_info_request{job_id = 2003, step_id = -1},
    {ok, _, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(2, Response#job_step_info_response.step_count).

test_step_info_response_structure() ->
    meck:expect(flurm_step_manager, list_steps, fun(_) ->
        [#{
            job_id => 2004,
            step_id => 0,
            name => <<"my_step">>,
            state => running,
            num_tasks => 4,
            start_time => 1700000000,
            allocated_nodes => [<<"node1">>, <<"node2">>],
            num_nodes => 2
        }]
    end),

    Header = make_header(?REQUEST_JOB_STEP_INFO),
    Request = #job_step_info_request{job_id = 2004, step_id = -1},
    {ok, _, Response} = flurm_handler_step:handle(Header, Request),

    ?assert(Response#job_step_info_response.last_update > 0),
    [StepInfo] = Response#job_step_info_response.steps,
    ?assertEqual(2004, StepInfo#job_step_info.job_id),
    ?assertEqual(0, StepInfo#job_step_info.step_id),
    ?assertEqual(<<"my_step">>, StepInfo#job_step_info.step_name),
    ?assertEqual(4, StepInfo#job_step_info.num_tasks).

%%====================================================================
%% Prolog Complete Tests (REQUEST_COMPLETE_PROLOG - 5019)
%%====================================================================

test_prolog_complete_success() ->
    meck:expect(flurm_job_manager, update_prolog_status, fun(3001, complete) -> ok end),

    Header = make_header(?REQUEST_COMPLETE_PROLOG),
    Request = #complete_prolog_request{job_id = 3001, prolog_rc = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_prolog_complete_failure() ->
    meck:expect(flurm_job_manager, update_prolog_status, fun(3002, failed) -> ok end),

    Header = make_header(?REQUEST_COMPLETE_PROLOG),
    Request = #complete_prolog_request{job_id = 3002, prolog_rc = 1, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_prolog_complete_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_prolog_status, {3003, complete}) ->
        {ok, ok}
    end),

    Header = make_header(?REQUEST_COMPLETE_PROLOG),
    Request = #complete_prolog_request{job_id = 3003, prolog_rc = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_prolog_complete_different_node() ->
    meck:expect(flurm_job_manager, update_prolog_status, fun(3004, complete) -> ok end),

    Header = make_header(?REQUEST_COMPLETE_PROLOG),
    Request = #complete_prolog_request{job_id = 3004, prolog_rc = 0, node_name = <<"compute-42">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Epilog Complete Tests (MESSAGE_EPILOG_COMPLETE - 6012)
%%====================================================================

test_epilog_complete_success() ->
    meck:expect(flurm_job_manager, update_epilog_status, fun(4001, complete) -> ok end),

    Header = make_header(?MESSAGE_EPILOG_COMPLETE),
    Request = #epilog_complete_msg{job_id = 4001, epilog_rc = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_epilog_complete_failure() ->
    meck:expect(flurm_job_manager, update_epilog_status, fun(4002, failed) -> ok end),

    Header = make_header(?MESSAGE_EPILOG_COMPLETE),
    Request = #epilog_complete_msg{job_id = 4002, epilog_rc = 1, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_epilog_complete_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_epilog_status, {4003, complete}) ->
        {ok, ok}
    end),

    Header = make_header(?MESSAGE_EPILOG_COMPLETE),
    Request = #epilog_complete_msg{job_id = 4003, epilog_rc = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_epilog_complete_different_node() ->
    meck:expect(flurm_job_manager, update_epilog_status, fun(4004, complete) -> ok end),

    Header = make_header(?MESSAGE_EPILOG_COMPLETE),
    Request = #epilog_complete_msg{job_id = 4004, epilog_rc = 0, node_name = <<"compute-100">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Task Exit Tests (MESSAGE_TASK_EXIT - 6003)
%%====================================================================

test_task_exit_normal() ->
    meck:expect(flurm_step_manager, complete_step, fun(5001, 0, 0) -> ok end),

    Header = make_header(?MESSAGE_TASK_EXIT),
    Request = #task_exit_msg{job_id = 5001, step_id = 0, return_code = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_task_exit_failure() ->
    meck:expect(flurm_step_manager, complete_step, fun(5002, 0, 1) -> ok end),

    Header = make_header(?MESSAGE_TASK_EXIT),
    Request = #task_exit_msg{job_id = 5002, step_id = 0, return_code = 1, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_task_exit_with_task_ids() ->
    meck:expect(flurm_step_manager, complete_step, fun(5003, 0, 0) -> ok end),

    Header = make_header(?MESSAGE_TASK_EXIT),
    Request = #task_exit_msg{job_id = 5003, step_id = 0, return_code = 0,
                             task_ids = [0, 1, 2, 3], node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_task_exit_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(complete_step, {5004, 0, 0}) ->
        {ok, ok}
    end),

    Header = make_header(?MESSAGE_TASK_EXIT),
    Request = #task_exit_msg{job_id = 5004, step_id = 0, return_code = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_task_exit_step_not_found() ->
    meck:expect(flurm_step_manager, complete_step, fun(_, _, _) -> {error, not_found} end),

    Header = make_header(?MESSAGE_TASK_EXIT),
    Request = #task_exit_msg{job_id = 5005, step_id = 99, return_code = 0, node_name = <<"node1">>},
    {ok, MsgType, Response} = flurm_handler_step:handle(Header, Request),

    %% Still returns success - step may have been created by srun directly
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Helper Function Tests
%%====================================================================

step_map_to_info_test_() ->
    [
        {"Convert step map to step_info", fun() ->
            StepMap = #{
                job_id => 6001,
                step_id => 0,
                name => <<"my_step">>,
                state => running,
                num_tasks => 4,
                start_time => 1700000000,
                num_nodes => 2,
                allocated_nodes => [<<"node1">>, <<"node2">>]
            },
            StepInfo = flurm_handler_step:step_map_to_info(StepMap),
            ?assertEqual(6001, StepInfo#job_step_info.job_id),
            ?assertEqual(0, StepInfo#job_step_info.step_id),
            ?assertEqual(<<"my_step">>, StepInfo#job_step_info.step_name),
            ?assertEqual(4, StepInfo#job_step_info.num_tasks)
        end},
        {"Convert step map with undefined start_time", fun() ->
            StepMap = #{
                job_id => 6002,
                step_id => 0,
                state => pending,
                start_time => undefined
            },
            StepInfo = flurm_handler_step:step_map_to_info(StepMap),
            ?assertEqual(0, StepInfo#job_step_info.run_time)
        end},
        {"Convert step map with zero start_time", fun() ->
            StepMap = #{
                job_id => 6003,
                step_id => 0,
                state => pending,
                start_time => 0
            },
            StepInfo = flurm_handler_step:step_map_to_info(StepMap),
            ?assertEqual(0, StepInfo#job_step_info.run_time)
        end},
        {"Convert step map with exit code", fun() ->
            StepMap = #{
                job_id => 6004,
                step_id => 0,
                state => completed,
                exit_code => 42
            },
            StepInfo = flurm_handler_step:step_map_to_info(StepMap),
            ?assertEqual(42, StepInfo#job_step_info.exit_code)
        end}
    ].

step_state_to_slurm_test_() ->
    [
        {"pending -> JOB_PENDING", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_handler_step:step_state_to_slurm(pending))
        end},
        {"running -> JOB_RUNNING", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_handler_step:step_state_to_slurm(running))
        end},
        {"completing -> JOB_RUNNING", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_handler_step:step_state_to_slurm(completing))
        end},
        {"completed -> JOB_COMPLETE", fun() ->
            ?assertEqual(?JOB_COMPLETE, flurm_handler_step:step_state_to_slurm(completed))
        end},
        {"cancelled -> JOB_CANCELLED", fun() ->
            ?assertEqual(?JOB_CANCELLED, flurm_handler_step:step_state_to_slurm(cancelled))
        end},
        {"failed -> JOB_FAILED", fun() ->
            ?assertEqual(?JOB_FAILED, flurm_handler_step:step_state_to_slurm(failed))
        end},
        {"unknown -> JOB_PENDING", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_handler_step:step_state_to_slurm(some_unknown_state))
        end}
    ].

is_cluster_enabled_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) ->
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Cluster disabled when no nodes configured", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            ?assertEqual(false, flurm_handler_step:is_cluster_enabled())
        end},
        {"Cluster disabled with single node", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1]),
            ?assertEqual(false, flurm_handler_step:is_cluster_enabled())
        end},
        {"Cluster enabled with multiple nodes", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            ?assertEqual(true, flurm_handler_step:is_cluster_enabled())
        end}
     ]}.

%%====================================================================
%% Dispatch Step Tests
%%====================================================================

dispatch_step_to_nodes_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_node_connection_manager),
         catch meck:unload(flurm_srun_callback),
         meck:new([flurm_job_manager, flurm_node_connection_manager, flurm_srun_callback],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_node_connection_manager, flurm_srun_callback]),
         ok
     end,
     [
        {"Dispatch to nodes with allocated nodes", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(7001) ->
                {ok, make_job(7001, #{allocated_nodes => [<<"node1">>, <<"node2">>]})}
            end),
            meck:expect(flurm_node_connection_manager, send_to_nodes, fun(Nodes, _) ->
                ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
                [{<<"node1">>, ok}, {<<"node2">>, ok}]
            end),
            Request = make_step_request(7001),
            ?assertEqual(ok, flurm_handler_step:dispatch_step_to_nodes(7001, 0, Request))
        end},
        {"Dispatch fails with no allocated nodes", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(7002) ->
                {ok, make_job(7002, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_srun_callback, notify_job_complete, fun(7002, 1, _) -> ok end),
            Request = make_step_request(7002),
            ?assertEqual(ok, flurm_handler_step:dispatch_step_to_nodes(7002, 0, Request))
        end},
        {"Dispatch with job not found", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(7003) -> {error, not_found} end),
            meck:expect(flurm_srun_callback, notify_job_complete, fun(7003, 1, _) -> ok end),
            Request = make_step_request(7003),
            ?assertEqual(ok, flurm_handler_step:dispatch_step_to_nodes(7003, 0, Request))
        end}
     ]}.

%%====================================================================
%% Build Step Launch Message Tests
%%====================================================================

build_step_launch_message_test_() ->
    [
        {"Build step launch message", fun() ->
            Request = make_step_request(8001, #{name => <<"hostname">>, num_tasks => 4, cpus_per_task => 2}),
            Job = make_job(8001, #{name => <<"test_job">>, work_dir => <<"/home/user">>}),
            Msg = flurm_handler_step:build_step_launch_message(8001, 0, Request, Job),
            ?assertEqual(step_launch, maps:get(type, Msg)),
            Payload = maps:get(payload, Msg),
            ?assertEqual(8001, maps:get(<<"job_id">>, Payload)),
            ?assertEqual(0, maps:get(<<"step_id">>, Payload)),
            ?assertEqual(<<"hostname">>, maps:get(<<"command">>, Payload)),
            ?assertEqual(4, maps:get(<<"num_tasks">>, Payload)),
            ?assertEqual(2, maps:get(<<"cpus_per_task">>, Payload))
        end},
        {"Build step launch message with empty name uses default", fun() ->
            Request = make_step_request(8002, #{name => <<>>}),
            Job = make_job(8002),
            Msg = flurm_handler_step:build_step_launch_message(8002, 0, Request, Job),
            Payload = maps:get(payload, Msg),
            ?assertEqual(<<"hostname">>, maps:get(<<"command">>, Payload))
        end},
        {"Build step launch message includes environment", fun() ->
            Request = make_step_request(8003, #{num_tasks => 8}),
            Job = make_job(8003, #{name => <<"my_job">>}),
            Msg = flurm_handler_step:build_step_launch_message(8003, 1, Request, Job),
            Payload = maps:get(payload, Msg),
            Env = maps:get(<<"environment">>, Payload),
            ?assertEqual(<<"8003">>, maps:get(<<"SLURM_JOB_ID">>, Env)),
            ?assertEqual(<<"1">>, maps:get(<<"SLURM_STEP_ID">>, Env)),
            ?assertEqual(<<"my_job">>, maps:get(<<"SLURM_JOB_NAME">>, Env)),
            ?assertEqual(<<"8">>, maps:get(<<"SLURM_NTASKS">>, Env))
        end}
    ].

%%====================================================================
%% Get Job Cred Info Tests
%%====================================================================

get_job_cred_info_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_node_manager_server),
         meck:new([flurm_job_manager, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_node_manager_server]),
         ok
     end,
     [
        {"Get job cred info for existing job", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(9001) ->
                {ok, make_job(9001, #{
                    user => <<"testuser">>,
                    partition => <<"compute">>,
                    allocated_nodes => [<<"node1">>, <<"node2">>]
                })}
            end),
            {Uid, Gid, User, NodeList, Partition} = flurm_handler_step:get_job_cred_info(9001),
            ?assertEqual(0, Uid),
            ?assertEqual(0, Gid),
            ?assertEqual(<<"testuser">>, User),
            %% NodeList may be first node or comma-separated list
            ?assert(NodeList =:= <<"node1,node2">> orelse NodeList =:= <<"node1">>),
            ?assertEqual(<<"compute">>, Partition)
        end},
        {"Get job cred info for job not found", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(9002) -> {error, not_found} end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            {Uid, Gid, User, _NodeList, Partition} = flurm_handler_step:get_job_cred_info(9002),
            ?assertEqual(0, Uid),
            ?assertEqual(0, Gid),
            ?assertEqual(<<"root">>, User),
            ?assertEqual(<<"default">>, Partition)
        end},
        {"Get job cred info with empty allocated nodes", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(9003) ->
                {ok, make_job(9003, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            {_, _, _, NodeList, _} = flurm_handler_step:get_job_cred_info(9003),
            ?assertEqual(<<"unknown">>, NodeList)
        end}
     ]}.

%%====================================================================
%% Get First Registered Node Tests
%%====================================================================

get_first_registered_node_test_() ->
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
        {"Get first node when nodes exist", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [#node{hostname = <<"compute-1">>}, #node{hostname = <<"compute-2">>}]
            end),
            ?assertEqual(<<"compute-1">>, flurm_handler_step:get_first_registered_node())
        end},
        {"Get first node when no nodes", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            ?assertEqual(<<"unknown">>, flurm_handler_step:get_first_registered_node())
        end},
        {"Get first node handles error", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> error(some_error) end),
            ?assertEqual(<<"unknown">>, flurm_handler_step:get_first_registered_node())
        end}
     ]}.

%%====================================================================
%% Update Prolog/Epilog Status Tests
%%====================================================================

update_job_prolog_status_test_() ->
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
        {"Update prolog status success", fun() ->
            meck:expect(flurm_job_manager, update_prolog_status, fun(10001, complete) -> ok end),
            ?assertEqual(ok, flurm_handler_step:update_job_prolog_status(10001, complete))
        end},
        {"Update prolog status failure", fun() ->
            meck:expect(flurm_job_manager, update_prolog_status, fun(_, _) -> {error, not_found} end),
            ?assertEqual(ok, flurm_handler_step:update_job_prolog_status(10002, failed))
        end}
     ]}.

update_job_epilog_status_test_() ->
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
        {"Update epilog status success", fun() ->
            meck:expect(flurm_job_manager, update_epilog_status, fun(11001, complete) -> ok end),
            ?assertEqual(ok, flurm_handler_step:update_job_epilog_status(11001, complete))
        end},
        {"Update epilog status failure", fun() ->
            meck:expect(flurm_job_manager, update_epilog_status, fun(_, _) -> {error, not_found} end),
            ?assertEqual(ok, flurm_handler_step:update_job_epilog_status(11002, failed))
        end}
     ]}.

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_step_manager),
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_node_connection_manager),
         meck:new([flurm_step_manager, flurm_job_manager, flurm_node_connection_manager],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_step_manager, flurm_job_manager, flurm_node_connection_manager]),
         ok
     end,
     [
        {"Step create with zero tasks uses default", fun() ->
            meck:expect(flurm_step_manager, create_step, fun(12001, Spec) ->
                ?assertEqual(1, maps:get(num_tasks, Spec)),
                {ok, 0}
            end),
            meck:expect(flurm_job_manager, get_job, fun(12001) -> {ok, make_job(12001)} end),
            meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),
            Header = make_header(?REQUEST_JOB_STEP_CREATE),
            Request = make_step_request(12001, #{num_tasks => 0}),
            {ok, _, _} = flurm_handler_step:handle(Header, Request)
        end},
        {"Step create with zero nodes uses default", fun() ->
            meck:expect(flurm_step_manager, create_step, fun(12002, Spec) ->
                ?assertEqual(1, maps:get(num_nodes, Spec)),
                {ok, 0}
            end),
            meck:expect(flurm_job_manager, get_job, fun(12002) -> {ok, make_job(12002)} end),
            meck:expect(flurm_node_connection_manager, send_to_nodes, fun(_, _) -> [] end),
            Header = make_header(?REQUEST_JOB_STEP_CREATE),
            Request = make_step_request(12002, #{min_nodes => 0}),
            {ok, _, _} = flurm_handler_step:handle(Header, Request)
        end}
     ]}.

%%====================================================================
%% Additional Helper Tests
%%====================================================================

helper_functions_test_() ->
    [
        {"make_header creates valid header", fun() ->
            Header = make_header(?REQUEST_JOB_STEP_CREATE),
            ?assertEqual(?REQUEST_JOB_STEP_CREATE, Header#slurm_header.msg_type),
            ?assertEqual(?SLURM_PROTOCOL_VERSION, Header#slurm_header.version)
        end},
        {"make_job creates job with defaults", fun() ->
            Job = make_job(100),
            ?assertEqual(100, Job#job.id),
            ?assertEqual(<<"test_job">>, Job#job.name),
            ?assertEqual(running, Job#job.state)
        end},
        {"make_job with overrides", fun() ->
            Job = make_job(101, #{name => <<"custom">>, state => pending}),
            ?assertEqual(101, Job#job.id),
            ?assertEqual(<<"custom">>, Job#job.name),
            ?assertEqual(pending, Job#job.state)
        end},
        {"make_step_request creates request with defaults", fun() ->
            Req = make_step_request(200),
            ?assertEqual(200, Req#job_step_create_request.job_id),
            ?assertEqual(1, Req#job_step_create_request.num_tasks)
        end}
    ].
