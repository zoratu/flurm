%%%-------------------------------------------------------------------
%%% @doc Integration Tests for FLURM Controller TCP Acceptor
%%%
%%% Tests the TCP acceptor and handler by connecting to the controller,
%%% sending SLURM protocol messages, and verifying responses.
%%%
%%% These tests require the full application stack to be running.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Generate unique port for each test run (avoid conflicts)
%% Using timestamp-based port offset similar to cluster_tests
-define(TEST_PORT_BASE, 16817).
-define(TEST_TIMEOUT, 5000).

%% Application environment key for storing the unique test port
-define(TEST_PORT_KEY, acceptor_test_port).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for integration tests
acceptor_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60,  % 60 second timeout for all tests
      [
       {"PING test", fun test_ping_pong/0},
       {"Submit batch job test", fun test_submit_batch_job/0},
       {"Query job info test", fun test_query_job_info/0},
       {"Cancel job test", fun test_cancel_job/0},
       {"Query node info test", fun test_query_node_info/0},
       {"Query partition info test", fun test_query_partition_info/0},
       {"Multiple messages test", fun test_multiple_messages/0},
       {"Partial read handling test", fun test_partial_read/0},
       {"Invalid message test", fun test_invalid_message/0},
       {"Connection close test", fun test_connection_close/0}
      ]
     }
    }.

%% @doc Setup test environment
setup() ->
    %% Generate unique port for this test run to avoid conflicts
    %% Use microsecond precision and larger range for better uniqueness
    TestPort = ?TEST_PORT_BASE + (erlang:unique_integer([positive]) rem 10000),

    %% Store port in application env for access by test functions
    application:set_env(flurm_controller, ?TEST_PORT_KEY, TestPort),

    %% Start required applications
    application:ensure_all_started(lager),
    application:ensure_all_started(ranch),

    %% Stop any existing listener from previous runs to avoid conflicts
    catch ranch:stop_listener(flurm_controller_listener),
    timer:sleep(200),

    %% Set test port with unique value
    application:set_env(flurm_controller, listen_port, TestPort),
    application:set_env(flurm_controller, listen_address, "127.0.0.1"),
    application:set_env(flurm_controller, max_connections, 100),

    %% Start flurm_limits first (dependency for job_manager)
    case whereis(flurm_limits) of
        undefined ->
            case flurm_limits:start_link() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ -> ok
    end,

    %% Try to start the controller application
    AppResult = application:ensure_all_started(flurm_controller),

    %% Regardless of app start result, ensure we have a listener on our port
    %% First stop any listener that might be running on old port
    catch flurm_controller_sup:stop_listener(),
    timer:sleep(200),

    %% Now start listener with new port config
    case AppResult of
        {ok, _} ->
            %% App started, but listener might be on wrong port, restart it
            start_listener_with_retry(TestPort, 5);
        {error, {already_started, _}} ->
            %% App was already started, restart listener with new port
            start_listener_with_retry(TestPort, 5);
        {error, _Reason} ->
            %% If app not fully available, start supervisor directly
            lager:info("Starting test environment manually"),
            case whereis(flurm_controller_sup) of
                undefined ->
                    {ok, _} = flurm_controller_sup:start_link();
                _ ->
                    ok
            end,
            start_listener_with_retry(TestPort, 5)
    end,

    %% Wait for listener to be ready
    timer:sleep(100),
    TestPort.

%% @doc Start listener with retry logic for port conflicts
start_listener_with_retry(Port, 0) ->
    error({listener_start_failed, Port, max_retries});
start_listener_with_retry(Port, Retries) ->
    %% Ensure port is set before each attempt
    application:set_env(flurm_controller, listen_port, Port),
    case flurm_controller_sup:start_listener() of
        {ok, Pid} ->
            {ok, Pid};
        {error, eaddrinuse} ->
            %% Port still in TIME_WAIT, wait and retry
            timer:sleep(500),
            start_listener_with_retry(Port, Retries - 1);
        {error, {listen_error, _, eaddrinuse}} ->
            timer:sleep(500),
            start_listener_with_retry(Port, Retries - 1);
        {error, Other} ->
            error({listener_start_failed, Port, Other})
    end.

%% @doc Cleanup test environment
cleanup(_TestPort) ->
    %% Stop listener and wait for port to be released
    catch flurm_controller_sup:stop_listener(),

    %% Give the OS time to release the port
    timer:sleep(200),

    %% Clean up application env (optional, will be overwritten on next run)
    application:unset_env(flurm_controller, ?TEST_PORT_KEY),

    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test PING request returns PONG (success response)
test_ping_pong() ->
    {ok, Socket} = connect(),

    %% Send PING request
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),
    ok = gen_tcp:send(Socket, PingBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test batch job submission
test_submit_batch_job() ->
    {ok, Socket} = connect(),

    %% Create batch job request
    JobRequest = #batch_job_request{
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho Hello">>,
        partition = <<"default">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 1,
        num_tasks = 1,
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000,
        work_dir = <<"/tmp">>,
        account = <<"test">>
    },

    {ok, RequestBin} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobRequest),
    ok = gen_tcp:send(Socket, RequestBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, Header#slurm_header.msg_type),
    ?assert(Body#batch_job_response.job_id > 0),
    ?assertEqual(0, Body#batch_job_response.error_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying job information
test_query_job_info() ->
    {ok, Socket} = connect(),

    %% First submit a job
    JobRequest = #batch_job_request{
        name = <<"query_test_job">>,
        script = <<"#!/bin/bash\necho Test">>,
        partition = <<"default">>,
        min_nodes = 1,
        min_cpus = 1
    },
    {ok, SubmitBin} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobRequest),
    ok = gen_tcp:send(Socket, SubmitBin),
    {ok, SubmitResp} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{body = SubmitBody}, _} = flurm_protocol_codec:decode_response(SubmitResp),
    JobId = SubmitBody#batch_job_response.job_id,

    %% Query job info
    InfoRequest = #job_info_request{
        show_flags = 0,
        job_id = JobId,
        user_id = 0
    },
    {ok, InfoBin} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, InfoRequest),
    ok = gen_tcp:send(Socket, InfoBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_JOB_INFO, Header#slurm_header.msg_type),
    ?assert(Body#job_info_response.job_count >= 1),

    gen_tcp:close(Socket),
    ok.

%% @doc Test cancelling a job
test_cancel_job() ->
    {ok, Socket} = connect(),

    %% First submit a job
    JobRequest = #batch_job_request{
        name = <<"cancel_test_job">>,
        script = <<"#!/bin/bash\nsleep 100">>,
        partition = <<"default">>,
        min_nodes = 1
    },
    {ok, SubmitBin} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobRequest),
    ok = gen_tcp:send(Socket, SubmitBin),
    {ok, SubmitResp} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{body = SubmitBody}, _} = flurm_protocol_codec:decode_response(SubmitResp),
    JobId = SubmitBody#batch_job_response.job_id,

    %% Cancel the job
    CancelRequest = #cancel_job_request{
        job_id = JobId,
        signal = 9,
        flags = 0
    },
    {ok, CancelBin} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, CancelRequest),
    ok = gen_tcp:send(Socket, CancelBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify response - should be success
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying node information
test_query_node_info() ->
    {ok, Socket} = connect(),

    %% Query node info (all nodes)
    InfoRequest = #node_info_request{
        show_flags = 0,
        node_name = <<>>
    },
    {ok, InfoBin} = flurm_protocol_codec:encode(?REQUEST_NODE_INFO, InfoRequest),
    ok = gen_tcp:send(Socket, InfoBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = _Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify we got a node info response
    ?assertEqual(?RESPONSE_NODE_INFO, Header#slurm_header.msg_type),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying partition information
test_query_partition_info() ->
    {ok, Socket} = connect(),

    %% Query partition info
    InfoRequest = #partition_info_request{
        show_flags = 0,
        partition_name = <<>>
    },
    {ok, InfoBin} = flurm_protocol_codec:encode(?REQUEST_PARTITION_INFO, InfoRequest),
    ok = gen_tcp:send(Socket, InfoBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_PARTITION_INFO, Header#slurm_header.msg_type),
    %% Should have at least the default partition
    ?assert(Body#partition_info_response.partition_count >= 1),

    gen_tcp:close(Socket),
    ok.

%% @doc Test sending multiple messages on same connection
test_multiple_messages() ->
    {ok, Socket} = connect(),

    %% Send multiple PING requests
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),

    %% Send 5 pings
    lists:foreach(fun(_) ->
        ok = gen_tcp:send(Socket, PingBin),
        {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
        {ok, #slurm_msg{header = Header, body = Body}, _} =
            flurm_protocol_codec:decode_response(ResponseBin),
        ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
        ?assertEqual(0, Body#slurm_rc_response.return_code)
    end, lists:seq(1, 5)),

    gen_tcp:close(Socket),
    ok.

%% @doc Test handling of partial reads (message split across packets)
test_partial_read() ->
    {ok, Socket} = connect(),

    %% Create a message and split it
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),

    %% Split into two parts
    Size = byte_size(PingBin),
    SplitPoint = Size div 2,
    <<Part1:SplitPoint/binary, Part2/binary>> = PingBin,

    %% Send first part
    ok = gen_tcp:send(Socket, Part1),
    timer:sleep(50),

    %% Send second part
    ok = gen_tcp:send(Socket, Part2),

    %% Should still get valid response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode_response(ResponseBin),

    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test handling of invalid/malformed messages
test_invalid_message() ->
    {ok, Socket} = connect(),

    %% Send garbage data with valid length prefix but invalid content
    InvalidMsg = <<14:32/big, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
    ok = gen_tcp:send(Socket, InvalidMsg),

    %% Should get error response (RESPONSE_SLURM_RC with error code)
    case gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT) of
        {ok, ResponseBin} ->
            {ok, #slurm_msg{header = Header, body = _Body}, _} =
                flurm_protocol_codec:decode_response(ResponseBin),
            ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type);
        {error, closed} ->
            %% Also acceptable - connection closed on invalid data
            ok
    end,

    gen_tcp:close(Socket),
    ok.

%% @doc Test graceful handling of client disconnect
test_connection_close() ->
    {ok, Socket} = connect(),

    %% Send a valid message
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),
    ok = gen_tcp:send(Socket, PingBin),
    {ok, _} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),

    %% Close connection abruptly
    gen_tcp:close(Socket),

    %% Server should handle this gracefully (no crash)
    timer:sleep(100),

    %% Verify we can still connect
    {ok, Socket2} = connect(),
    ok = gen_tcp:send(Socket2, PingBin),
    {ok, _} = gen_tcp:recv(Socket2, 0, ?TEST_TIMEOUT),
    gen_tcp:close(Socket2),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Get the unique test port for this test run
get_test_port() ->
    case application:get_env(flurm_controller, ?TEST_PORT_KEY) of
        {ok, Port} -> Port;
        undefined ->
            %% Fallback to listen_port if test port not set
            application:get_env(flurm_controller, listen_port, ?TEST_PORT_BASE)
    end.

%% @doc Connect to the test controller
connect() ->
    connect("127.0.0.1", get_test_port()).

connect(Host, Port) ->
    gen_tcp:connect(Host, Port, [
        binary,
        {packet, raw},
        {active, false},
        {nodelay, true}
    ], ?TEST_TIMEOUT).
