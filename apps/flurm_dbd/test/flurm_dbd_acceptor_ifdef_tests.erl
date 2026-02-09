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
      {"handle_dbd_request DBD_INIT", fun handle_dbd_request_dbd_init_test/0},
      {"handle_dbd_request DBD_GET_JOBS_COND", fun handle_dbd_request_get_jobs_test/0},
      {"handle_dbd_request unsupported", fun handle_dbd_request_unsupported_test/0},
      {"handle_dbd_request DBD_REGISTER_CTLD", fun handle_dbd_request_register_test/0},
      {"handle_dbd_request DBD_FINI", fun handle_dbd_request_fini_test/0}
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

handle_dbd_request_dbd_init_test() ->
    %% DBD_CLUSTER_TRES = 1407
    Result = flurm_dbd_acceptor:handle_dbd_request(1407, <<>>),
    ?assertMatch({rc, 0}, Result).

handle_dbd_request_get_jobs_test() ->
    %% DBD_GET_JOBS_COND = 1444 (sacct query)
    Result = flurm_dbd_acceptor:handle_dbd_request(1444, <<>>),
    ?assertMatch({rc, 0}, Result).

handle_dbd_request_register_test() ->
    %% DBD_REGISTER_CTLD = 1434
    Result = flurm_dbd_acceptor:handle_dbd_request(1434, <<>>),
    ?assertMatch({rc, 0}, Result).

handle_dbd_request_unsupported_test() ->
    %% Use a made-up message type
    Result = flurm_dbd_acceptor:handle_dbd_request(99999, <<>>),
    ?assertMatch({rc, 0}, Result).

handle_dbd_request_fini_test() ->
    %% DBD_FINI = 1401
    Result = flurm_dbd_acceptor:handle_dbd_request(1401, <<>>),
    ?assertMatch({rc, 0}, Result).

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a test connection state record
%% conn_state record now has 6 fields: socket, transport, buffer, authenticated, client_version, client_info
make_conn_state() ->
    {conn_state,
     undefined,      % socket
     undefined,      % transport
     <<>>,           % buffer
     false,          % authenticated
     0,              % client_version
     #{}             % client_info
    }.
