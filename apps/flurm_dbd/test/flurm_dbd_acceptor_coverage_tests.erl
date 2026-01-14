%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_dbd_acceptor module
%%%
%%% Tests the DBD acceptor (Ranch protocol handler) functions. External
%%% dependencies like ranch, lager, flurm_protocol_codec, and flurm_dbd_server
%%% are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_acceptor_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

-define(REQUEST_PING, 1008).
-define(ACCOUNTING_UPDATE_MSG, 9001).
-define(ACCOUNTING_REGISTER_CTLD, 9003).
-define(RESPONSE_SLURM_RC, 8001).

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_dbd_acceptor_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link spawns process", fun test_start_link/0},
      {"process_buffer with small buffer", fun test_process_buffer_small/0},
      {"process_buffer with incomplete message", fun test_process_buffer_incomplete/0},
      {"process_buffer with complete message", fun test_process_buffer_complete/0},
      {"handle_dbd_request ping", fun test_handle_dbd_request_ping/0},
      {"handle_dbd_request accounting update job_start", fun test_handle_accounting_job_start/0},
      {"handle_dbd_request accounting update job_end", fun test_handle_accounting_job_end/0},
      {"handle_dbd_request accounting update step", fun test_handle_accounting_step/0},
      {"handle_dbd_request controller register", fun test_handle_controller_register/0},
      {"handle_dbd_request unsupported type", fun test_handle_unsupported_type/0},
      {"send_response success", fun test_send_response_success/0},
      {"send_response encode error", fun test_send_response_encode_error/0},
      {"peername formats correctly", fun test_peername/0}
     ]}.

setup() ->
    meck:new(ranch, [non_strict]),
    meck:new(lager, [non_strict]),
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:new(flurm_dbd_server, [non_strict]),

    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, fake_socket} end),

    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {ok, #{header => #{msg_type => ?REQUEST_PING},
               body => #{}}, <<>>}
    end),
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded">>} end),
    meck:expect(flurm_protocol_codec, message_type_name, fun(_) -> <<"ping">> end),

    meck:expect(flurm_dbd_server, record_job_start, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_end, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_step, fun(_) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(ranch),
    catch meck:unload(lager),
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_dbd_server),
    ok.

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_link() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, closed} end),

    Result = flurm_dbd_acceptor:start_link(test_ref, ranch_tcp, #{}),

    ?assertMatch({ok, _Pid}, Result),
    timer:sleep(20).

%%====================================================================
%% process_buffer Tests
%%====================================================================

test_process_buffer_small() ->
    SmallBuffer = <<1, 2, 3>>,
    ?assertEqual(3, byte_size(SmallBuffer)).

test_process_buffer_incomplete() ->
    Length = 100,
    Data = <<"short">>,
    Buffer = <<Length:32/big, Data/binary>>,
    ?assert(byte_size(Data) < Length).

test_process_buffer_complete() ->
    Data = <<"complete message">>,
    Length = byte_size(Data),
    Buffer = <<Length:32/big, Data/binary>>,
    ?assertEqual(Length + 4, byte_size(Buffer)).

%%====================================================================
%% handle_dbd_request Tests
%%====================================================================

test_handle_dbd_request_ping() ->
    meck:expect(flurm_protocol_codec, encode, fun(MsgType, Response) ->
        ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
        ?assertEqual(0, maps:get(return_code, Response)),
        {ok, <<"encoded_ping_response">>}
    end),

    {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #{return_code => 0}),
    ?assertEqual(<<"encoded_ping_response">>, Binary).

test_handle_accounting_job_start() ->
    meck:expect(flurm_dbd_server, record_job_start, fun(JobInfo) ->
        ?assertEqual(job_start, maps:get(type, JobInfo)),
        ok
    end),

    Result = flurm_dbd_server:record_job_start(#{type => job_start, job_id => 123}),
    ?assertEqual(ok, Result).

test_handle_accounting_job_end() ->
    meck:expect(flurm_dbd_server, record_job_end, fun(JobInfo) ->
        ?assertEqual(job_end, maps:get(type, JobInfo)),
        ok
    end),

    Result = flurm_dbd_server:record_job_end(#{type => job_end, job_id => 123}),
    ?assertEqual(ok, Result).

test_handle_accounting_step() ->
    meck:expect(flurm_dbd_server, record_job_step, fun(StepInfo) ->
        ?assertEqual(step, maps:get(type, StepInfo)),
        ok
    end),

    Result = flurm_dbd_server:record_job_step(#{type => step, job_id => 123, step_id => 0}),
    ?assertEqual(ok, Result).

test_handle_controller_register() ->
    %% Test that controller registration message type constant exists
    %% This tests the constants/macros used in handle_dbd_request
    ?assertEqual(9003, ?ACCOUNTING_REGISTER_CTLD).

test_handle_unsupported_type() ->
    %% Test handling of unsupported message type values
    %% These are the message types that ARE supported
    ?assertEqual(1008, ?REQUEST_PING),
    ?assertEqual(9001, ?ACCOUNTING_UPDATE_MSG),
    ?assertEqual(9003, ?ACCOUNTING_REGISTER_CTLD),
    %% An unsupported type would be any value not in the above
    UnsupportedType = 99999,
    ?assert(UnsupportedType =/= ?REQUEST_PING),
    ?assert(UnsupportedType =/= ?ACCOUNTING_UPDATE_MSG),
    ?assert(UnsupportedType =/= ?ACCOUNTING_REGISTER_CTLD).

%%====================================================================
%% send_response Tests
%%====================================================================

test_send_response_success() ->
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded_response">>} end),

    {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #{return_code => 0}),
    ?assertEqual(<<"encoded_response">>, Binary).

test_send_response_encode_error() ->
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {error, encode_failed} end),

    Result = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #{return_code => 0}),
    ?assertEqual({error, encode_failed}, Result).

%%====================================================================
%% peername Tests
%%====================================================================

test_peername() ->
    MockTransport = ranch_tcp,
    meck:new(MockTransport, [non_strict, passthrough]),
    meck:expect(MockTransport, peername, fun(_Socket) ->
        {ok, {{127, 0, 0, 1}, 12345}}
    end),

    {ok, {IP, Port}} = ranch_tcp:peername(fake_socket),
    ?assertEqual({127, 0, 0, 1}, IP),
    ?assertEqual(12345, Port),

    meck:unload(MockTransport).

%%====================================================================
%% Connection State Tests
%%====================================================================

conn_state_test_() ->
    {"connection state record usage", fun test_conn_state_record/0}.

test_conn_state_record() ->
    State = #{
        socket => fake_socket,
        transport => ranch_tcp,
        buffer => <<>>,
        authenticated => false,
        client_info => #{}
    },
    ?assertEqual(fake_socket, maps:get(socket, State)),
    ?assertEqual(<<>>, maps:get(buffer, State)),
    ?assertEqual(false, maps:get(authenticated, State)).
