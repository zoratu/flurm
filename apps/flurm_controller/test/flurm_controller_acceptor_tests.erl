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

%% Test port for isolation from production
-define(TEST_PORT, 16817).
-define(TEST_TIMEOUT, 5000).

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
       {"PING test", fun ping_pong_test/0},
       {"Submit batch job test", fun submit_batch_job_test/0},
       {"Query job info test", fun query_job_info_test/0},
       {"Cancel job test", fun cancel_job_test/0},
       {"Query node info test", fun query_node_info_test/0},
       {"Query partition info test", fun query_partition_info_test/0},
       {"Multiple messages test", fun multiple_messages_test/0},
       {"Partial read handling test", fun partial_read_test/0},
       {"Invalid message test", fun invalid_message_test/0},
       {"Connection close test", fun connection_close_test/0}
      ]
     }
    }.

%% @doc Setup test environment
setup() ->
    %% Start required applications
    application:ensure_all_started(lager),
    application:ensure_all_started(ranch),

    %% Set test port
    application:set_env(flurm_controller, listen_port, ?TEST_PORT),
    application:set_env(flurm_controller, listen_address, "127.0.0.1"),
    application:set_env(flurm_controller, max_connections, 100),

    %% Start the controller application
    case application:ensure_all_started(flurm_controller) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, _Reason} ->
            %% If app not fully available, start supervisor directly
            lager:info("Starting test environment manually"),
            {ok, _} = flurm_controller_sup:start_link(),
            {ok, _} = flurm_controller_sup:start_listener()
    end,

    %% Wait for listener to be ready
    timer:sleep(100),
    ok.

%% @doc Cleanup test environment
cleanup(_) ->
    %% Stop listener
    flurm_controller_sup:stop_listener(),
    timer:sleep(100),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test PING request returns PONG (success response)
ping_pong_test() ->
    {ok, Socket} = connect(),

    %% Send PING request
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),
    ok = gen_tcp:send(Socket, PingBin),

    %% Receive response
    {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
    {ok, #slurm_msg{header = Header, body = Body}, _Rest} =
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test batch job submission
submit_batch_job_test() ->
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
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, Header#slurm_header.msg_type),
    ?assert(Body#batch_job_response.job_id > 0),
    ?assertEqual(0, Body#batch_job_response.error_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying job information
query_job_info_test() ->
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
    {ok, #slurm_msg{body = SubmitBody}, _} = flurm_protocol_codec:decode(SubmitResp),
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
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_JOB_INFO, Header#slurm_header.msg_type),
    ?assert(Body#job_info_response.job_count >= 1),

    gen_tcp:close(Socket),
    ok.

%% @doc Test cancelling a job
cancel_job_test() ->
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
    {ok, #slurm_msg{body = SubmitBody}, _} = flurm_protocol_codec:decode(SubmitResp),
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
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify response - should be success
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying node information
query_node_info_test() ->
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
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify we got a node info response
    ?assertEqual(?RESPONSE_NODE_INFO, Header#slurm_header.msg_type),

    gen_tcp:close(Socket),
    ok.

%% @doc Test querying partition information
query_partition_info_test() ->
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
        flurm_protocol_codec:decode(ResponseBin),

    %% Verify response
    ?assertEqual(?RESPONSE_PARTITION_INFO, Header#slurm_header.msg_type),
    %% Should have at least the default partition
    ?assert(Body#partition_info_response.partition_count >= 1),

    gen_tcp:close(Socket),
    ok.

%% @doc Test sending multiple messages on same connection
multiple_messages_test() ->
    {ok, Socket} = connect(),

    %% Send multiple PING requests
    PingRequest = #ping_request{},
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, PingRequest),

    %% Send 5 pings
    lists:foreach(fun(_) ->
        ok = gen_tcp:send(Socket, PingBin),
        {ok, ResponseBin} = gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT),
        {ok, #slurm_msg{header = Header, body = Body}, _} =
            flurm_protocol_codec:decode(ResponseBin),
        ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
        ?assertEqual(0, Body#slurm_rc_response.return_code)
    end, lists:seq(1, 5)),

    gen_tcp:close(Socket),
    ok.

%% @doc Test handling of partial reads (message split across packets)
partial_read_test() ->
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
        flurm_protocol_codec:decode(ResponseBin),

    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    ?assertEqual(0, Body#slurm_rc_response.return_code),

    gen_tcp:close(Socket),
    ok.

%% @doc Test handling of invalid/malformed messages
invalid_message_test() ->
    {ok, Socket} = connect(),

    %% Send garbage data with valid length prefix but invalid content
    InvalidMsg = <<14:32/big, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
    ok = gen_tcp:send(Socket, InvalidMsg),

    %% Should get error response (RESPONSE_SLURM_RC with error code)
    case gen_tcp:recv(Socket, 0, ?TEST_TIMEOUT) of
        {ok, ResponseBin} ->
            {ok, #slurm_msg{header = Header, body = _Body}, _} =
                flurm_protocol_codec:decode(ResponseBin),
            ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type);
        {error, closed} ->
            %% Also acceptable - connection closed on invalid data
            ok
    end,

    gen_tcp:close(Socket),
    ok.

%% @doc Test graceful handling of client disconnect
connection_close_test() ->
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

%% @doc Connect to the test controller
connect() ->
    connect("127.0.0.1", ?TEST_PORT).

connect(Host, Port) ->
    gen_tcp:connect(Host, Port, [
        binary,
        {packet, raw},
        {active, false},
        {nodelay, true}
    ], ?TEST_TIMEOUT).
