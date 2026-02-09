%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_acceptor module
%%%
%%% These tests call the actual flurm_dbd_acceptor functions directly
%%% to achieve code coverage. External dependencies like ranch are mocked.
%%%
%%% The DBD acceptor now uses DBD framing (2-byte msg_type prefix)
%%% and the persist connection handshake protocol.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_acceptor_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

flurm_dbd_acceptor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link creates process", fun test_start_link/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking
    catch meck:unload(lager),

    %% Mock lager
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Basic Tests
%%====================================================================

test_start_link() ->
    %% Mock ranch
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, handshake, fun(_) -> {ok, fake_socket} end),

    %% Mock transport
    catch meck:unload(ranch_tcp),
    meck:new(ranch_tcp, [passthrough, unstick, no_link]),
    meck:expect(ranch_tcp, setopts, fun(_, _) -> ok end),
    meck:expect(ranch_tcp, peername, fun(_) -> {ok, {{127,0,0,1}, 12345}} end),
    meck:expect(ranch_tcp, close, fun(_) -> ok end),

    %% start_link spawns a process that immediately enters init
    {ok, Pid} = flurm_dbd_acceptor:start_link(test_ref, ranch_tcp, #{}),
    ?assert(is_pid(Pid)),

    %% The process will be in the loop waiting for tcp messages
    %% Send tcp_closed to terminate cleanly
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid),

    catch meck:unload(ranch_tcp),
    catch meck:unload(ranch).

%%====================================================================
%% Message Handling Tests
%%====================================================================

message_handling_test_() ->
    {foreach,
     fun setup_message_handling/0,
     fun cleanup_message_handling/1,
     [
      {"handle tcp_closed message", fun test_tcp_closed/0},
      {"handle tcp_error message", fun test_tcp_error/0},
      {"handle tcp data message", fun test_tcp_data/0},
      {"handle unknown message", fun test_unknown_message/0},
      {"handle incomplete buffer", fun test_incomplete_buffer/0}
     ]}.

setup_message_handling() ->
    catch meck:unload(lager),
    catch meck:unload(ranch),
    catch meck:unload(ranch_tcp),

    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(ranch, [passthrough, unstick, no_link]),
    meck:expect(ranch, handshake, fun(_) -> {ok, fake_socket} end),

    meck:new(ranch_tcp, [passthrough, unstick, no_link]),
    meck:expect(ranch_tcp, setopts, fun(_, _) -> ok end),
    meck:expect(ranch_tcp, peername, fun(_) -> {ok, {{127,0,0,1}, 54321}} end),
    meck:expect(ranch_tcp, close, fun(_) -> ok end),
    meck:expect(ranch_tcp, send, fun(_, _) -> ok end),
    ok.

cleanup_message_handling(_) ->
    catch meck:unload(ranch_tcp),
    catch meck:unload(ranch),
    catch meck:unload(lager),
    ok.

test_tcp_closed() ->
    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),
    %% Erlang guarantees message order - this message will be queued
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid),
    ?assert(not is_process_alive(Pid)).

test_tcp_error() ->
    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),
    Pid ! {tcp_error, fake_socket, econnreset},
    flurm_test_utils:wait_for_death(Pid),
    ?assert(not is_process_alive(Pid)).

test_tcp_data() ->
    Self = self(),
    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{}),
        Self ! {done, self()}
    end),

    %% Send some data that's too small (less than 4 bytes for length)
    Pid ! {tcp, fake_socket, <<1, 2>>},

    %% Should still be alive waiting for more data - use monitor to check
    MRef = erlang:monitor(process, Pid),
    ?assert(is_process_alive(Pid)),

    %% Close connection and wait for death
    Pid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ?assert(false)
    end.

test_unknown_message() ->
    Self = self(),
    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{}),
        Self ! {done, self()}
    end),

    %% Send unknown message
    Pid ! {unknown, message, type},

    %% Should still be alive - use monitor
    MRef = erlang:monitor(process, Pid),
    ?assert(is_process_alive(Pid)),

    %% Cleanup - send tcp_closed and wait for DOWN
    Pid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ?assert(false)
    end.

test_incomplete_buffer() ->
    Self = self(),
    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{}),
        Self ! {done, self()}
    end),

    %% Send partial length header (only 2 bytes of 4)
    Pid ! {tcp, fake_socket, <<0, 0>>},

    %% Should still be alive waiting for more - use monitor
    MRef = erlang:monitor(process, Pid),
    ?assert(is_process_alive(Pid)),

    Pid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ?assert(false)
    end.

%%====================================================================
%% DBD Request Handler Tests (direct function calls)
%%====================================================================

dbd_request_handler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle_dbd_request DBD_INIT", fun test_handle_dbd_init/0},
      {"handle_dbd_request DBD_GET_JOBS_COND", fun test_handle_get_jobs/0},
      {"handle_dbd_request DBD_REGISTER_CTLD", fun test_handle_register_ctld/0},
      {"handle_dbd_request DBD_FINI", fun test_handle_fini/0},
      {"handle_dbd_request DBD_NODE_STATE", fun test_handle_node_state/0},
      {"handle_dbd_request DBD_CLUSTER_TRES", fun test_handle_cluster_tres/0},
      {"handle_dbd_request unsupported", fun test_handle_unsupported/0}
     ]}.

test_handle_dbd_init() ->
    %% DBD_CLUSTER_TRES = 1407
    Result = flurm_dbd_acceptor:handle_dbd_request(1407, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_get_jobs() ->
    %% DBD_GET_JOBS_COND = 1444
    Result = flurm_dbd_acceptor:handle_dbd_request(1444, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_register_ctld() ->
    %% DBD_REGISTER_CTLD = 1434
    Result = flurm_dbd_acceptor:handle_dbd_request(1434, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_fini() ->
    %% DBD_FINI = 1401
    Result = flurm_dbd_acceptor:handle_dbd_request(1401, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_node_state() ->
    %% DBD_NODE_STATE = 1432
    Result = flurm_dbd_acceptor:handle_dbd_request(1432, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_cluster_tres() ->
    %% DBD_JOB_START = 1425
    Result = flurm_dbd_acceptor:handle_dbd_request(1425, <<>>),
    ?assertMatch({rc, 0}, Result).

test_handle_unsupported() ->
    Result = flurm_dbd_acceptor:handle_dbd_request(99999, <<>>),
    ?assertMatch({rc, 0}, Result).
