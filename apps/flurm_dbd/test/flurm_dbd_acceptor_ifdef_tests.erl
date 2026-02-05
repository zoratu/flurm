%%%-------------------------------------------------------------------
%%% @doc Test suite for flurm_dbd_acceptor internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_acceptor_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

dbd_acceptor_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"peername with mock socket", fun peername_test/0},
      {"process_buffer needs more data for length", fun process_buffer_short_test/0},
      {"process_buffer needs more data for message", fun process_buffer_incomplete_test/0},
      {"handle_dbd_request ping", fun handle_dbd_request_ping_test/0},
      {"handle_dbd_request register ctld", fun handle_dbd_request_register_test/0},
      {"handle_dbd_request unsupported", fun handle_dbd_request_unsupported_test/0},
      {"handle_dbd_request accounting update job_start", fun handle_dbd_request_job_start_test/0},
      {"handle_dbd_request accounting update job_end", fun handle_dbd_request_job_end_test/0}
     ]}.

%%====================================================================
%% Tests
%%====================================================================

peername_test() ->
    %% Create a mock transport module
    meck:new(mock_transport, [non_strict]),

    %% Test successful peername
    meck:expect(mock_transport, peername, fun(_Socket) ->
        {ok, {{127, 0, 0, 1}, 12345}}
    end),

    Result1 = flurm_dbd_acceptor:peername(fake_socket, mock_transport),
    ?assertMatch([_, _, _], Result1),  % Should be a formatted string

    %% Test failed peername
    meck:expect(mock_transport, peername, fun(_Socket) ->
        {error, einval}
    end),

    Result2 = flurm_dbd_acceptor:peername(fake_socket, mock_transport),
    ?assertEqual("unknown", Result2),

    meck:unload(mock_transport).

process_buffer_short_test() ->
    %% Buffer too short for length prefix (< 4 bytes)
    State = make_conn_state(),

    {ok, Remaining, NewState} = flurm_dbd_acceptor:process_buffer(<<1, 2, 3>>, State),
    ?assertEqual(<<1, 2, 3>>, Remaining),
    ?assertEqual(State, NewState).

process_buffer_incomplete_test() ->
    %% Buffer has length but not enough data
    State = make_conn_state(),

    %% Length says 100 bytes, but we only have 10
    Buffer = <<100:32/big, "1234567890">>,

    {ok, Remaining, NewState} = flurm_dbd_acceptor:process_buffer(Buffer, State),
    ?assertEqual(Buffer, Remaining),
    ?assertEqual(State, NewState).

handle_dbd_request_ping_test() ->
    Header = #slurm_header{msg_type = ?REQUEST_PING},

    Result = flurm_dbd_acceptor:handle_dbd_request(Header, #{}),

    ?assertMatch({?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

handle_dbd_request_register_test() ->
    Header = #slurm_header{msg_type = ?ACCOUNTING_REGISTER_CTLD},

    Result = flurm_dbd_acceptor:handle_dbd_request(Header, #{}),

    ?assertMatch({?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result).

handle_dbd_request_unsupported_test() ->
    %% Use a made-up message type
    Header = #slurm_header{msg_type = 99999},

    Result = flurm_dbd_acceptor:handle_dbd_request(Header, #{}),

    ?assertMatch({?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

handle_dbd_request_job_start_test() ->
    %% Mock flurm_dbd_server
    meck:new(flurm_dbd_server, [passthrough, non_strict]),
    meck:expect(flurm_dbd_server, record_job_start, fun(_) -> ok end),

    Header = #slurm_header{msg_type = ?ACCOUNTING_UPDATE_MSG},
    Body = #{type => job_start, job_id => 123},

    Result = flurm_dbd_acceptor:handle_dbd_request(Header, Body),

    ?assertMatch({?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result),
    ?assert(meck:called(flurm_dbd_server, record_job_start, '_')),

    meck:unload(flurm_dbd_server).

handle_dbd_request_job_end_test() ->
    %% Mock flurm_dbd_server
    meck:new(flurm_dbd_server, [passthrough, non_strict]),
    meck:expect(flurm_dbd_server, record_job_end, fun(_) -> ok end),

    Header = #slurm_header{msg_type = ?ACCOUNTING_UPDATE_MSG},
    Body = #{type => job_end, job_id => 123, exit_code => 0},

    Result = flurm_dbd_acceptor:handle_dbd_request(Header, Body),

    ?assertMatch({?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}, Result),
    ?assert(meck:called(flurm_dbd_server, record_job_end, '_')),

    meck:unload(flurm_dbd_server).

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a test connection state record
make_conn_state() ->
    {conn_state,
     undefined,      % socket
     undefined,      % transport
     <<>>,           % buffer
     false,          % authenticated
     #{}             % client_info
    }.
