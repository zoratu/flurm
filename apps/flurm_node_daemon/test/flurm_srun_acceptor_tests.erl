%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_srun_acceptor
%%%
%%% Covers the exported connect_io_socket/3 function with mocked
%%% TCP connections and I/O protocol encoding.
%%%
%%% Note: flurm_srun_acceptor is a ranch_protocol with a loop-based
%%% design (not gen_server), so many internals aren't directly testable.
%%% We focus on the exported connect_io_socket/3 function.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_srun_acceptor_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_acceptor_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

srun_acceptor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"connect_io_socket success",
         fun test_connect_io_socket_success/0},
        {"connect_io_socket connect failure returns undefined",
         fun test_connect_io_socket_connect_failure/0},
        {"connect_io_socket send failure returns undefined",
         fun test_connect_io_socket_send_failure/0},
        {"connect_io_socket verifies init message sent",
         fun test_connect_io_socket_init_message/0},
        {"connect_io_socket with different node IDs",
         fun test_connect_io_socket_different_nodes/0},
        {"connect_io_socket with zero port connects attempt",
         fun test_connect_io_socket_zero_port/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_io_protocol),
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:new(flurm_io_protocol, [non_strict]),
    %% Default: connect and send succeed
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {ok, make_ref()}
    end),
    meck:expect(gen_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(gen_tcp, close, fun(_Socket) -> ok end),
    %% Mock io protocol to return a binary
    meck:expect(flurm_io_protocol, encode_io_init_msg, 5, <<"fake_io_init_msg">>),
    ok.

cleanup(_) ->
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_io_protocol),
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_connect_io_socket_success() ->
    Result = flurm_srun_acceptor:connect_io_socket({127,0,0,1}, 12345, <<"key">>),
    ?assertNotEqual(undefined, Result).

test_connect_io_socket_connect_failure() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, econnrefused}
    end),
    Result = flurm_srun_acceptor:connect_io_socket({127,0,0,1}, 12345, <<"key">>),
    ?assertEqual(undefined, Result).

test_connect_io_socket_send_failure() ->
    meck:expect(gen_tcp, send, fun(_Socket, _Data) ->
        {error, closed}
    end),
    Result = flurm_srun_acceptor:connect_io_socket({127,0,0,1}, 12345, <<"key">>),
    ?assertEqual(undefined, Result).

test_connect_io_socket_init_message() ->
    _Result = flurm_srun_acceptor:connect_io_socket({127,0,0,1}, 12345, <<"key">>),
    %% Verify encode_io_init_msg/5 was called
    ?assert(meck:called(flurm_io_protocol, encode_io_init_msg,
                        ['_', '_', '_', '_', '_'])).

test_connect_io_socket_different_nodes() ->
    _R1 = flurm_srun_acceptor:connect_io_socket({10,0,0,1}, 12345, <<"key1">>),
    _R2 = flurm_srun_acceptor:connect_io_socket({10,0,0,2}, 12346, <<"key2">>),
    %% Both should succeed (connect mocked)
    ?assert(meck:num_calls(gen_tcp, connect, '_') >= 2).

test_connect_io_socket_zero_port() ->
    meck:expect(gen_tcp, connect, fun(_Host, 0, _Opts, _Timeout) ->
        {error, einval}
    end),
    Result = flurm_srun_acceptor:connect_io_socket({127,0,0,1}, 0, <<"key">>),
    ?assertEqual(undefined, Result).
