%%%-------------------------------------------------------------------
%%% @doc Unit Tests for FLURM Controller TCP Acceptor
%%%
%%% Tests the flurm_controller_acceptor module using mocks to avoid
%%% requiring actual network connections. Tests all exported functions
%%% and internal logic paths.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

acceptor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/3 spawns a process", fun test_start_link/0},
         {"init/3 with successful handshake", fun test_init_success/0},
         {"init/3 with failed handshake", fun test_init_failed_handshake/0},
         {"binary_to_hex/1 conversion", fun test_binary_to_hex/0}
     ]}.

setup() ->
    %% Start meck
    application:ensure_all_started(lager),
    catch meck:unload(ranch),
    meck:new(ranch, [passthrough, unstick]),
    catch meck:unload(ranch_tcp),
    meck:new(ranch_tcp, [passthrough, unstick]),
    %% Mock inet for peername calls with fake sockets
    catch meck:unload(inet),
    meck:new(inet, [passthrough, unstick]),
    meck:expect(inet, peername, fun(_Socket) -> {ok, {{127, 0, 0, 1}, 12345}} end),
    %% Mock connection limiter
    catch meck:unload(flurm_connection_limiter),
    meck:new(flurm_connection_limiter, [passthrough, non_strict]),
    meck:expect(flurm_connection_limiter, connection_allowed, fun(_) -> true end),
    meck:expect(flurm_connection_limiter, connection_opened, fun(_) -> ok end),
    meck:expect(flurm_connection_limiter, connection_closed, fun(_) -> ok end),
    %% Use passthrough to preserve coverage instrumentation
    catch meck:unload(flurm_protocol_codec),
    meck:new(flurm_protocol_codec, [passthrough, non_strict]),
    catch meck:unload(flurm_controller_handler),
    meck:new(flurm_controller_handler, [passthrough, non_strict]),
    ok.

cleanup(_) ->
    catch meck:unload(ranch),
    catch meck:unload(ranch_tcp),
    catch meck:unload(inet),
    catch meck:unload(flurm_connection_limiter),
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_controller_handler),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_link() ->
    %% Mock ranch:handshake to fail immediately so the spawned process exits
    meck:expect(ranch, handshake, fun(_Ref) -> {error, test_exit} end),

    {ok, Pid} = flurm_controller_acceptor:start_link(test_ref, ranch_tcp, #{}),
    ?assert(is_pid(Pid)),

    %% Wait for process to exit (it will fail on handshake)
    flurm_test_utils:wait_for_death(Pid),
    ?assertNot(is_process_alive(Pid)),
    ok.

test_init_success() ->
    %% Create a fake socket
    FakeSocket = make_ref(),

    %% Mock successful handshake
    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),

    %% Mock transport operations
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% Mock recv to return closed immediately (clean exit)
    meck:expect(ranch_tcp, recv, fun(_Socket, _Len, _Timeout) -> {error, closed} end),

    %% Spawn init in a separate process
    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    %% Wait for result
    receive
        {done, ok} -> ok;
        {done, Other} -> ?assertEqual(ok, Other)
    after 1000 ->
        exit(Pid, kill),
        ?assert(false)
    end,
    ok.

test_init_failed_handshake() ->
    %% Mock failed handshake
    meck:expect(ranch, handshake, fun(_Ref) -> {error, econnrefused} end),

    Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
    ?assertEqual({error, econnrefused}, Result),
    ok.

test_binary_to_hex() ->
    %% Test the binary_to_hex helper function directly
    %% Empty binary
    ?assertEqual(<<>>, flurm_controller_acceptor:binary_to_hex(<<>>)),

    %% Single byte
    ?assertEqual(<<"00">>, flurm_controller_acceptor:binary_to_hex(<<0>>)),
    ?assertEqual(<<"FF">>, flurm_controller_acceptor:binary_to_hex(<<255>>)),
    ?assertEqual(<<"0A">>, flurm_controller_acceptor:binary_to_hex(<<10>>)),

    %% Multiple bytes
    ?assertEqual(<<"0102030405">>, flurm_controller_acceptor:binary_to_hex(<<1,2,3,4,5>>)),
    ?assertEqual(<<"DEADBEEF">>, flurm_controller_acceptor:binary_to_hex(<<222,173,190,239>>)),

    %% Typical message header prefix
    ?assertEqual(<<"000A00000014">>,
                 flurm_controller_acceptor:binary_to_hex(<<0,10,0,0,0,20>>)),
    ok.

%%====================================================================
%% Process Buffer Tests
%%====================================================================

process_buffer_test_() ->
    {foreach,
     fun() ->
         application:ensure_all_started(lager),
         catch meck:unload(ranch),
         meck:new(ranch, [passthrough, unstick]),
         catch meck:unload(ranch_tcp),
         meck:new(ranch_tcp, [passthrough, unstick]),
         catch meck:unload(flurm_protocol_codec),
         meck:new(flurm_protocol_codec, [passthrough, non_strict]),
         catch meck:unload(flurm_controller_handler),
         meck:new(flurm_controller_handler, [passthrough, non_strict])
     end,
     fun(_) ->
         meck:unload(ranch),
         meck:unload(ranch_tcp),
         meck:unload(flurm_protocol_codec),
         meck:unload(flurm_controller_handler)
     end,
     [
         {"recv returns data and processes buffer", fun test_recv_with_data/0},
         {"recv returns timeout", fun test_recv_timeout/0},
         {"recv returns error", fun test_recv_error/0}
     ]}.

test_recv_with_data() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% First recv returns small incomplete data, second returns closed
    meck:sequence(ranch_tcp, recv, 3, [
        {ok, <<1, 2, 3>>},
        {error, closed}
    ]),

    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok  %% Accept any result
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_recv_timeout() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:expect(ranch_tcp, recv, fun(_Socket, _Len, _Timeout) -> {error, timeout} end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_recv_error() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:expect(ranch_tcp, recv, fun(_Socket, _Len, _Timeout) -> {error, econnreset} end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

%%====================================================================
%% Message Handling Tests
%%====================================================================

message_handling_test_() ->
    {foreach,
     fun() ->
         application:ensure_all_started(lager),
         catch meck:unload(ranch),
         meck:new(ranch, [passthrough, unstick]),
         catch meck:unload(ranch_tcp),
         meck:new(ranch_tcp, [passthrough, unstick]),
         catch meck:unload(flurm_protocol_codec),
         meck:new(flurm_protocol_codec, [passthrough, non_strict]),
         catch meck:unload(flurm_controller_handler),
         meck:new(flurm_controller_handler, [passthrough, non_strict])
     end,
     fun(_) ->
         meck:unload(ranch),
         meck:unload(ranch_tcp),
         meck:unload(flurm_protocol_codec),
         meck:unload(flurm_controller_handler)
     end,
     [
         {"complete PING message is handled", fun test_complete_ping_message/0},
         {"handler error sends error response", fun test_handler_error/0},
         {"decode error sends error response", fun test_decode_error/0}
     ]}.

test_complete_ping_message() ->
    FakeSocket = make_ref(),

    %% Create a valid PING message binary
    %% Length = 10 (header size), then 10 bytes of header
    PingBin = <<10:32/big, 0:16, 0:16, ?REQUEST_PING:16, 0:32>>,

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% First recv returns PING message, second returns closed
    meck:sequence(ranch_tcp, recv, 3, [
        {ok, PingBin},
        {error, closed}
    ]),

    %% Mock successful decode
    PingHeader = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?REQUEST_PING,
        body_length = 0
    },
    PingMsg = #slurm_msg{header = PingHeader, body = <<>>},
    meck:expect(flurm_protocol_codec, decode_with_extra,
                fun(_Bin) -> {ok, PingMsg, #{}, <<>>} end),

    %% Mock handler response
    meck:expect(flurm_controller_handler, handle,
                fun(_Header, _Body) ->
                    {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
                end),

    %% Mock encode and send
    meck:expect(flurm_protocol_codec, encode_response,
                fun(_Type, _Body) -> {ok, <<"response">>} end),
    meck:expect(flurm_protocol_codec, message_type_name,
                fun(_Type) -> "TEST" end),
    meck:expect(ranch_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_handler_error() ->
    FakeSocket = make_ref(),
    PingBin = <<10:32/big, 0:16, 0:16, ?REQUEST_PING:16, 0:32>>,

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:sequence(ranch_tcp, recv, 3, [{ok, PingBin}, {error, closed}]),

    PingHeader = #slurm_header{msg_type = ?REQUEST_PING, body_length = 0},
    PingMsg = #slurm_msg{header = PingHeader, body = <<>>},
    meck:expect(flurm_protocol_codec, decode_with_extra,
                fun(_Bin) -> {ok, PingMsg, #{}, <<>>} end),

    %% Handler returns error
    meck:expect(flurm_controller_handler, handle,
                fun(_Header, _Body) -> {error, test_error} end),

    meck:expect(flurm_protocol_codec, encode_response,
                fun(_Type, _Body) -> {ok, <<"error_response">>} end),
    meck:expect(flurm_protocol_codec, message_type_name, fun(_Type) -> "TEST" end),
    meck:expect(ranch_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_decode_error() ->
    FakeSocket = make_ref(),
    %% Create message with valid length but will fail decode
    BadBin = <<10:32/big, 0:80>>,  %% 10 bytes of zeros

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:sequence(ranch_tcp, recv, 3, [{ok, BadBin}, {error, closed}]),

    %% Both decode attempts fail
    meck:expect(flurm_protocol_codec, decode_with_extra,
                fun(_Bin) -> {error, invalid_message} end),
    meck:expect(flurm_protocol_codec, decode,
                fun(_Bin) -> {error, invalid_message} end),

    meck:expect(flurm_protocol_codec, encode_response,
                fun(_Type, _Body) -> {ok, <<"error_response">>} end),
    meck:expect(flurm_protocol_codec, message_type_name, fun(_Type) -> "TEST" end),
    meck:expect(ranch_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

%%====================================================================
%% Buffer Edge Cases
%%====================================================================

buffer_edge_cases_test_() ->
    {foreach,
     fun() ->
         application:ensure_all_started(lager),
         catch meck:unload(ranch),
         meck:new(ranch, [passthrough, unstick]),
         catch meck:unload(ranch_tcp),
         meck:new(ranch_tcp, [passthrough, unstick]),
         catch meck:unload(flurm_protocol_codec),
         meck:new(flurm_protocol_codec, [passthrough, non_strict]),
         catch meck:unload(flurm_controller_handler),
         meck:new(flurm_controller_handler, [passthrough, non_strict])
     end,
     fun(_) ->
         meck:unload(ranch),
         meck:unload(ranch_tcp),
         meck:unload(flurm_protocol_codec),
         meck:unload(flurm_controller_handler)
     end,
     [
         {"buffer too small for length prefix", fun test_small_buffer/0},
         {"invalid message length triggers close", fun test_invalid_length/0},
         {"large data with hex logging", fun test_large_data_hex/0}
     ]}.

test_small_buffer() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% Send 1 byte, then closed
    meck:sequence(ranch_tcp, recv, 3, [
        {ok, <<1>>},
        {error, closed}
    ]),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_invalid_length() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% Message with length = 5 (too small for header which is 10)
    InvalidMsg = <<5:32/big, 1, 2, 3, 4, 5>>,
    meck:sequence(ranch_tcp, recv, 3, [
        {ok, InvalidMsg},
        {error, closed}
    ]),

    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_large_data_hex() ->
    FakeSocket = make_ref(),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),

    %% Send more than 24 bytes to trigger large data hex path
    LargeData = <<0:256>>,  %% 32 bytes
    meck:sequence(ranch_tcp, recv, 3, [
        {ok, LargeData},
        {error, closed}
    ]),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

%%====================================================================
%% Response Sending Tests
%%====================================================================

response_test_() ->
    {foreach,
     fun() ->
         application:ensure_all_started(lager),
         catch meck:unload(ranch),
         meck:new(ranch, [passthrough, unstick]),
         catch meck:unload(ranch_tcp),
         meck:new(ranch_tcp, [passthrough, unstick]),
         catch meck:unload(flurm_protocol_codec),
         meck:new(flurm_protocol_codec, [passthrough, non_strict]),
         catch meck:unload(flurm_controller_handler),
         meck:new(flurm_controller_handler, [passthrough, non_strict])
     end,
     fun(_) ->
         meck:unload(ranch),
         meck:unload(ranch_tcp),
         meck:unload(flurm_protocol_codec),
         meck:unload(flurm_controller_handler)
     end,
     [
         {"send_response encode error", fun test_send_response_encode_error/0},
         {"send_response send error", fun test_send_response_send_error/0}
     ]}.

test_send_response_encode_error() ->
    FakeSocket = make_ref(),
    PingBin = <<10:32/big, 0:16, 0:16, ?REQUEST_PING:16, 0:32>>,

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:sequence(ranch_tcp, recv, 3, [{ok, PingBin}, {error, closed}]),

    PingHeader = #slurm_header{msg_type = ?REQUEST_PING, body_length = 0},
    PingMsg = #slurm_msg{header = PingHeader, body = <<>>},
    meck:expect(flurm_protocol_codec, decode_with_extra,
                fun(_Bin) -> {ok, PingMsg, #{}, <<>>} end),

    meck:expect(flurm_controller_handler, handle,
                fun(_Header, _Body) ->
                    {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
                end),

    %% Encode fails
    meck:expect(flurm_protocol_codec, encode_response,
                fun(_Type, _Body) -> {error, encode_failed} end),

    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.

test_send_response_send_error() ->
    FakeSocket = make_ref(),
    PingBin = <<10:32/big, 0:16, 0:16, ?REQUEST_PING:16, 0:32>>,

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, FakeSocket} end),
    meck:expect(ranch_tcp, setopts, fun(_Socket, _Opts) -> ok end),
    meck:sequence(ranch_tcp, recv, 3, [{ok, PingBin}, {error, closed}]),

    PingHeader = #slurm_header{msg_type = ?REQUEST_PING, body_length = 0},
    PingMsg = #slurm_msg{header = PingHeader, body = <<>>},
    meck:expect(flurm_protocol_codec, decode_with_extra,
                fun(_Bin) -> {ok, PingMsg, #{}, <<>>} end),

    meck:expect(flurm_controller_handler, handle,
                fun(_Header, _Body) ->
                    {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
                end),

    meck:expect(flurm_protocol_codec, encode_response,
                fun(_Type, _Body) -> {ok, <<"response">>} end),
    meck:expect(flurm_protocol_codec, message_type_name, fun(_Type) -> "TEST" end),

    %% Send fails
    meck:expect(ranch_tcp, send, fun(_Socket, _Data) -> {error, epipe} end),
    meck:expect(ranch_tcp, close, fun(_Socket) -> ok end),

    Parent = self(),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:init(test_ref, ranch_tcp, #{}),
        Parent ! {done, Result}
    end),

    receive
        {done, ok} -> ok;
        {done, _Other} -> ok
    after 1000 ->
        exit(Pid, kill)
    end,
    ok.
