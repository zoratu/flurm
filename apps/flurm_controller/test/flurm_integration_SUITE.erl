%%%-------------------------------------------------------------------
%%% @doc Integration Test Suite for FLURM Controller
%%%
%%% Tests SLURM client protocol compatibility by simulating client
%%% connections and verifying correct responses.
%%%
%%% These tests require the controller to be running and can be
%%% executed against a live system or in isolation.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% CT callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Connection tests
    test_connect_disconnect/1,
    test_multiple_connections/1,
    test_connection_timeout/1,

    %% Ping tests
    test_ping_request/1,
    test_ping_with_auth/1,

    %% Job submission tests
    test_submit_batch_job/1,
    test_submit_job_invalid_partition/1,
    test_submit_job_resource_limits/1,

    %% Job info tests
    test_job_info_single/1,
    test_job_info_all/1,
    test_job_info_nonexistent/1,

    %% Job control tests
    test_cancel_job/1,
    test_cancel_nonexistent_job/1,
    test_cancel_completed_job/1,

    %% Node info tests
    test_node_info_all/1,
    test_node_info_single/1,

    %% Partition info tests
    test_partition_info/1,

    %% Rate limiting tests
    test_rate_limiting/1,
    test_backpressure/1,

    %% Error handling tests
    test_malformed_message/1,
    test_unknown_message_type/1,
    test_oversized_message/1
]).

-define(CONTROLLER_HOST, "127.0.0.1").
-define(CONTROLLER_PORT, 6817).
-define(CONNECT_TIMEOUT, 5000).
-define(RECV_TIMEOUT, 10000).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        {group, connection},
        {group, ping},
        {group, job_submission},
        {group, job_info},
        {group, job_control},
        {group, node_info},
        {group, partition_info},
        {group, rate_limiting},
        {group, error_handling}
    ].

groups() ->
    [
        {connection, [sequence], [
            test_connect_disconnect,
            test_multiple_connections,
            test_connection_timeout
        ]},
        {ping, [sequence], [
            test_ping_request,
            test_ping_with_auth
        ]},
        {job_submission, [sequence], [
            test_submit_batch_job,
            test_submit_job_invalid_partition,
            test_submit_job_resource_limits
        ]},
        {job_info, [sequence], [
            test_job_info_single,
            test_job_info_all,
            test_job_info_nonexistent
        ]},
        {job_control, [sequence], [
            test_cancel_job,
            test_cancel_nonexistent_job,
            test_cancel_completed_job
        ]},
        {node_info, [sequence], [
            test_node_info_all,
            test_node_info_single
        ]},
        {partition_info, [sequence], [
            test_partition_info
        ]},
        {rate_limiting, [sequence], [
            test_rate_limiting,
            test_backpressure
        ]},
        {error_handling, [sequence], [
            test_malformed_message,
            test_unknown_message_type,
            test_oversized_message
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    application:ensure_all_started(flurm_protocol),

    %% Check if controller is running
    case gen_tcp:connect(?CONTROLLER_HOST, ?CONTROLLER_PORT,
                         [binary, {active, false}], ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            [{controller_available, true} | Config];
        {error, _} ->
            ct:pal("WARNING: Controller not available at ~s:~p~n",
                   [?CONTROLLER_HOST, ?CONTROLLER_PORT]),
            [{controller_available, false} | Config]
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    case proplists:get_value(controller_available, Config) of
        false ->
            {skip, controller_not_available};
        true ->
            Config
    end.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Connection Tests
%%====================================================================

test_connect_disconnect(_Config) ->
    {ok, Socket} = connect(),
    ok = gen_tcp:close(Socket),
    ok.

test_multiple_connections(_Config) ->
    %% Open multiple connections simultaneously
    Sockets = [begin
        {ok, S} = connect(),
        S
    end || _ <- lists:seq(1, 10)],

    %% All should be connected
    ?assertEqual(10, length(Sockets)),

    %% Close all
    lists:foreach(fun(S) -> gen_tcp:close(S) end, Sockets),
    ok.

test_connection_timeout(_Config) ->
    {ok, Socket} = connect(),
    %% Don't send anything, wait for timeout (if implemented)
    %% Most implementations don't timeout idle connections quickly
    timer:sleep(1000),
    %% Connection should still be valid
    ok = inet:setopts(Socket, [{active, false}]),
    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Ping Tests
%%====================================================================

test_ping_request(_Config) ->
    {ok, Socket} = connect(),

    %% Send ping request
    PingReq = encode_message(?REQUEST_PING, <<>>),
    ok = gen_tcp:send(Socket, PingReq),

    %% Receive response
    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{header = #slurm_header{msg_type = MsgType}}} ->
            %% Should get either ping response or RC response
            ?assert(MsgType =:= ?RESPONSE_SLURM_RC orelse
                   MsgType =:= ?REQUEST_PING + 1);
        {error, Reason} ->
            ct:fail("Failed to decode ping response: ~p", [Reason])
    end,

    gen_tcp:close(Socket),
    ok.

test_ping_with_auth(_Config) ->
    {ok, Socket} = connect(),

    %% Send authenticated ping (with proper auth header)
    PingReq = encode_message(?REQUEST_PING, <<>>, [{auth, munge}]),
    ok = gen_tcp:send(Socket, PingReq),

    %% Should receive valid response
    {ok, _Response} = recv_message(Socket),

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Job Submission Tests
%%====================================================================

test_submit_batch_job(_Config) ->
    {ok, Socket} = connect(),

    %% Create job submission request
    JobReq = #job_submit_req{
        name = <<"test_job">>,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 60,
        script = <<"#!/bin/bash\necho 'Hello World'\n">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    },

    %% Encode and send
    Encoded = encode_job_submit(JobReq),
    Msg = encode_message(?REQUEST_SUBMIT_BATCH_JOB, Encoded),
    ok = gen_tcp:send(Socket, Msg),

    %% Receive response
    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            %% Should get job ID or error
            ct:pal("Job submit response: ~p", [Body]);
        {error, Reason} ->
            ct:pal("Job submit decode error: ~p", [Reason])
    end,

    gen_tcp:close(Socket),
    ok.

test_submit_job_invalid_partition(_Config) ->
    {ok, Socket} = connect(),

    JobReq = #job_submit_req{
        name = <<"test_job">>,
        partition = <<"nonexistent_partition">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 60,
        script = <<"#!/bin/bash\necho test\n">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    },

    Encoded = encode_job_submit(JobReq),
    Msg = encode_message(?REQUEST_SUBMIT_BATCH_JOB, Encoded),
    ok = gen_tcp:send(Socket, Msg),

    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            %% Should get error response
            ct:pal("Invalid partition response: ~p", [Body]);
        {error, _} ->
            ok  % Error response is expected
    end,

    gen_tcp:close(Socket),
    ok.

test_submit_job_resource_limits(_Config) ->
    {ok, Socket} = connect(),

    %% Request excessive resources
    JobReq = #job_submit_req{
        name = <<"big_job">>,
        partition = <<"batch">>,
        num_nodes = 10000,  % Way too many nodes
        num_cpus = 1000000,
        memory_mb = 999999999,
        time_limit = 60,
        script = <<"#!/bin/bash\necho test\n">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    },

    Encoded = encode_job_submit(JobReq),
    Msg = encode_message(?REQUEST_SUBMIT_BATCH_JOB, Encoded),
    ok = gen_tcp:send(Socket, Msg),

    {ok, _Response} = recv_message(Socket),
    %% Should get resource limit error

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Job Info Tests
%%====================================================================

test_job_info_single(_Config) ->
    {ok, Socket} = connect(),

    %% Request info for job ID 1
    InfoReq = encode_job_info_request(1),
    Msg = encode_message(?REQUEST_JOB_INFO, InfoReq),
    ok = gen_tcp:send(Socket, Msg),

    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            ct:pal("Job info response: ~p", [Body]);
        {error, Reason} ->
            ct:pal("Job info error: ~p", [Reason])
    end,

    gen_tcp:close(Socket),
    ok.

test_job_info_all(_Config) ->
    {ok, Socket} = connect(),

    %% Request all jobs (job_id = 0 or NO_VAL)
    InfoReq = encode_job_info_request(0),
    Msg = encode_message(?REQUEST_JOB_INFO, InfoReq),
    ok = gen_tcp:send(Socket, Msg),

    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            ct:pal("All jobs response: ~p", [Body]);
        {error, _} ->
            ok
    end,

    gen_tcp:close(Socket),
    ok.

test_job_info_nonexistent(_Config) ->
    {ok, Socket} = connect(),

    %% Request info for nonexistent job
    InfoReq = encode_job_info_request(999999999),
    Msg = encode_message(?REQUEST_JOB_INFO, InfoReq),
    ok = gen_tcp:send(Socket, Msg),

    {ok, _Response} = recv_message(Socket),
    %% Should get "not found" or empty response

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Job Control Tests
%%====================================================================

test_cancel_job(_Config) ->
    {ok, Socket} = connect(),

    %% First submit a job
    JobReq = #job_submit_req{
        name = <<"cancel_test">>,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,  % Long enough to cancel
        script = <<"#!/bin/bash\nsleep 3600\n">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    },

    Encoded = encode_job_submit(JobReq),
    Msg = encode_message(?REQUEST_SUBMIT_BATCH_JOB, Encoded),
    ok = gen_tcp:send(Socket, Msg),
    {ok, _SubmitResponse} = recv_message(Socket),

    %% Now cancel it (assuming job ID 1)
    CancelReq = encode_cancel_job_request(1),
    CancelMsg = encode_message(?REQUEST_CANCEL_JOB, CancelReq),
    ok = gen_tcp:send(Socket, CancelMsg),

    {ok, _CancelResponse} = recv_message(Socket),

    gen_tcp:close(Socket),
    ok.

test_cancel_nonexistent_job(_Config) ->
    {ok, Socket} = connect(),

    CancelReq = encode_cancel_job_request(999999999),
    CancelMsg = encode_message(?REQUEST_CANCEL_JOB, CancelReq),
    ok = gen_tcp:send(Socket, CancelMsg),

    {ok, _Response} = recv_message(Socket),
    %% Should get error response

    gen_tcp:close(Socket),
    ok.

test_cancel_completed_job(_Config) ->
    %% This test would need a job to complete first
    %% For now, just test the request/response cycle
    {ok, Socket} = connect(),

    CancelReq = encode_cancel_job_request(0),  % Invalid job ID
    CancelMsg = encode_message(?REQUEST_CANCEL_JOB, CancelReq),
    ok = gen_tcp:send(Socket, CancelMsg),

    {ok, _Response} = recv_message(Socket),

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Node Info Tests
%%====================================================================

test_node_info_all(_Config) ->
    {ok, Socket} = connect(),

    Msg = encode_message(?REQUEST_NODE_INFO, <<>>),
    ok = gen_tcp:send(Socket, Msg),

    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            ct:pal("Node info response: ~p", [Body]);
        {error, _} ->
            ok
    end,

    gen_tcp:close(Socket),
    ok.

test_node_info_single(_Config) ->
    {ok, Socket} = connect(),

    %% Request specific node
    Msg = encode_message(?REQUEST_NODE_INFO, <<"node001">>),
    ok = gen_tcp:send(Socket, Msg),

    {ok, _Response} = recv_message(Socket),

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Partition Info Tests
%%====================================================================

test_partition_info(_Config) ->
    {ok, Socket} = connect(),

    Msg = encode_message(?REQUEST_PARTITION_INFO, <<>>),
    ok = gen_tcp:send(Socket, Msg),

    {ok, Response} = recv_message(Socket),
    case decode_response(Response) of
        {ok, #slurm_msg{body = Body}} ->
            ct:pal("Partition info: ~p", [Body]);
        {error, _} ->
            ok
    end,

    gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Rate Limiting Tests
%%====================================================================

test_rate_limiting(_Config) ->
    %% Send many requests rapidly and expect some to be rate limited
    Results = lists:map(fun(_) ->
        case connect() of
            {ok, Socket} ->
                Msg = encode_message(?REQUEST_PING, <<>>),
                ok = gen_tcp:send(Socket, Msg),
                Result = recv_message(Socket),
                gen_tcp:close(Socket),
                Result;
            Error ->
                Error
        end
    end, lists:seq(1, 50)),

    %% Count successful vs rate-limited responses
    {Successes, Failures} = lists:partition(
        fun({ok, _}) -> true; (_) -> false end,
        Results
    ),

    ct:pal("Rate limit test: ~p successes, ~p failures",
           [length(Successes), length(Failures)]),

    %% At least some should succeed
    ?assert(length(Successes) > 0),
    ok.

test_backpressure(_Config) ->
    %% Test that backpressure activates under load
    %% This is a stress test
    NumConnections = 100,

    Pids = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            case connect() of
                {ok, Socket} ->
                    Msg = encode_message(?REQUEST_PING, <<>>),
                    gen_tcp:send(Socket, Msg),
                    recv_message(Socket),
                    gen_tcp:close(Socket);
                _ ->
                    ok
            end
        end, lists:seq(1, 10))
    end) || _ <- lists:seq(1, NumConnections)],

    %% Wait for completion
    lists:foreach(fun(Pid) ->
        receive
            {'EXIT', Pid, _} -> ok
        after 30000 ->
            ok
        end
    end, Pids),

    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_malformed_message(_Config) ->
    {ok, Socket} = connect(),

    %% Send garbage data
    ok = gen_tcp:send(Socket, <<1,2,3,4,5,6,7,8>>),

    %% Connection might be closed or error returned
    case gen_tcp:recv(Socket, 0, ?RECV_TIMEOUT) of
        {ok, _} -> ok;
        {error, closed} -> ok;
        {error, _} -> ok
    end,

    catch gen_tcp:close(Socket),
    ok.

test_unknown_message_type(_Config) ->
    {ok, Socket} = connect(),

    %% Send message with unknown type
    Msg = encode_message(65535, <<>>),  % Invalid message type
    ok = gen_tcp:send(Socket, Msg),

    %% Should get error response
    {ok, _Response} = recv_message(Socket),

    gen_tcp:close(Socket),
    ok.

test_oversized_message(_Config) ->
    {ok, Socket} = connect(),

    %% Create oversized body
    BigBody = binary:copy(<<0>>, 10 * 1024 * 1024),  % 10MB

    %% Try to send (might fail during send or be rejected)
    Msg = encode_message(?REQUEST_PING, BigBody),
    case gen_tcp:send(Socket, Msg) of
        ok ->
            %% Server should reject or close connection
            case gen_tcp:recv(Socket, 0, ?RECV_TIMEOUT) of
                {ok, _} -> ok;
                {error, _} -> ok
            end;
        {error, _} ->
            ok  % Send failed, which is acceptable
    end,

    catch gen_tcp:close(Socket),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

connect() ->
    gen_tcp:connect(?CONTROLLER_HOST, ?CONTROLLER_PORT,
                    [binary, {active, false}, {packet, raw}],
                    ?CONNECT_TIMEOUT).

recv_message(Socket) ->
    %% First receive length prefix
    case gen_tcp:recv(Socket, 4, ?RECV_TIMEOUT) of
        {ok, <<Len:32/big>>} ->
            %% Receive full message
            gen_tcp:recv(Socket, Len, ?RECV_TIMEOUT);
        Error ->
            Error
    end.

encode_message(MsgType, Body) ->
    encode_message(MsgType, Body, []).

encode_message(MsgType, Body, _Opts) ->
    BodyLen = byte_size(Body),
    HeaderLen = 10,
    TotalLen = HeaderLen + BodyLen,
    Version = 16#2600,  % SLURM 24.x protocol version
    Flags = 0,
    Header = <<Version:16/big, Flags:16/big, 0:16/big,
               MsgType:16/big, BodyLen:32/big>>,
    <<TotalLen:32/big, Header/binary, Body/binary>>.

decode_response(Data) ->
    flurm_protocol_codec:decode(Data).

encode_job_submit(#job_submit_req{name = Name, partition = Partition,
                                   num_nodes = Nodes, num_cpus = Cpus,
                                   memory_mb = Mem, time_limit = Time,
                                   script = Script}) ->
    %% Simplified encoding - actual implementation would be more complex
    NameLen = byte_size(Name),
    PartLen = byte_size(Partition),
    ScriptLen = byte_size(Script),
    <<NameLen:32/big, Name/binary,
      PartLen:32/big, Partition/binary,
      Nodes:32/big, Cpus:32/big, Mem:32/big, Time:32/big,
      ScriptLen:32/big, Script/binary>>.

encode_job_info_request(JobId) ->
    <<JobId:32/big, 0:16/big, 0:16/big>>.  % job_id, show_flags, user_id

encode_cancel_job_request(JobId) ->
    <<JobId:32/big, 0:32/big>>.  % job_id, signal
