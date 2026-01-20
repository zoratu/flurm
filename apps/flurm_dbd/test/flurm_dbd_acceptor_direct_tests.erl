%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_acceptor module
%%%
%%% These tests call the actual flurm_dbd_acceptor functions directly
%%% to achieve code coverage. External dependencies like ranch and
%%% flurm_protocol_codec are mocked.
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
      {"process incomplete buffer", fun test_incomplete_buffer/0}
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
    catch meck:unload(flurm_protocol_codec),
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
    %% Mock protocol codec
    catch meck:unload(flurm_protocol_codec),
    meck:new(flurm_protocol_codec, [passthrough, no_link]),
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {error, incomplete}
    end),

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
%% Protocol Message Type Tests
%%====================================================================

protocol_handling_test_() ->
    {foreach,
     fun setup_protocol/0,
     fun cleanup_protocol/1,
     [
      {"handle ping request", fun test_handle_ping/0},
      {"handle accounting update - job start", fun test_accounting_update_job_start/0},
      {"handle accounting update - job end", fun test_accounting_update_job_end/0},
      {"handle accounting update - step", fun test_accounting_update_step/0},
      {"handle controller registration", fun test_controller_registration/0},
      {"handle unsupported message", fun test_unsupported_message/0},
      {"handle decode error", fun test_decode_error/0}
     ]}.

setup_protocol() ->
    catch meck:unload(lager),
    catch meck:unload(ranch),
    catch meck:unload(ranch_tcp),
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(flurm_protocol_codec),

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

    meck:new(flurm_dbd_server, [passthrough, no_link]),
    meck:expect(flurm_dbd_server, record_job_start, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_end, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_step, fun(_) -> ok end),

    meck:new(flurm_protocol_codec, [passthrough, no_link]),
    meck:expect(flurm_protocol_codec, message_type_name, fun(_) -> unknown end),
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<0,0,0,4,"resp">>} end),
    ok.

cleanup_protocol(_) ->
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(ranch_tcp),
    catch meck:unload(ranch),
    catch meck:unload(lager),
    ok.

make_msg(MsgType, Body) ->
    %% Build message using proper records
    Header = #slurm_header{msg_type = MsgType},
    #slurm_msg{header = Header, body = Body}.

test_handle_ping() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {ok, make_msg(?REQUEST_PING, #{}), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 4, 0, 0, 0, 1>>},
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid).

test_accounting_update_job_start() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        Body = #{type => job_start, job_id => 1},
        {ok, make_msg(?ACCOUNTING_UPDATE_MSG, Body), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 10, "job_start_">>},
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid),

    ?assert(meck:called(flurm_dbd_server, record_job_start, '_')).

test_accounting_update_job_end() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        Body = #{type => job_end, job_id => 1, exit_code => 0},
        {ok, make_msg(?ACCOUNTING_UPDATE_MSG, Body), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 8, "job_end_">>},
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid),

    ?assert(meck:called(flurm_dbd_server, record_job_end, '_')).

test_accounting_update_step() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        Body = #{type => step, job_id => 1, step_id => 0},
        {ok, make_msg(?ACCOUNTING_UPDATE_MSG, Body), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 8, "step____">>},
    Pid ! {tcp_closed, fake_socket},
    flurm_test_utils:wait_for_death(Pid),

    ?assert(meck:called(flurm_dbd_server, record_job_step, '_')).

test_controller_registration() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {ok, make_msg(?ACCOUNTING_REGISTER_CTLD, #{}), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 4, "ctld">>},
    %% Use monitor to check process is still alive after processing
    MRef = erlang:monitor(process, Pid),
    ?assert(is_process_alive(Pid)),

    Pid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ?assert(false)
    end.

test_unsupported_message() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {ok, make_msg(9999, #{}), <<>>}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 4, "unkn">>},
    %% Use monitor to check process is still alive after processing
    MRef = erlang:monitor(process, Pid),
    ?assert(is_process_alive(Pid)),

    Pid ! {tcp_closed, fake_socket},
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ?assert(false)
    end.

test_decode_error() ->
    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {error, invalid_message}
    end),

    Pid = spawn(fun() ->
        flurm_dbd_acceptor:init(test_ref, ranch_tcp, #{})
    end),

    Pid ! {tcp, fake_socket, <<0, 0, 0, 4, "bad!">>},
    flurm_test_utils:wait_for_death(Pid),

    %% Connection should be closed on decode error
    ?assert(not is_process_alive(Pid)).
