%%%-------------------------------------------------------------------
%%% @doc FLURM srun Callback Handler Comprehensive Coverage Tests
%%%
%%% Complete coverage tests for flurm_srun_callback module.
%%% Tests all callback registration and notification operations:
%%% - start_link/0
%%% - register_callback/4
%%% - register_existing_socket/2
%%% - notify_job_ready/2
%%% - notify_job_complete/3
%%% - get_callback/1
%%% - send_task_output/3
%%%
%%% Also tests gen_server callbacks:
%%% - init/1
%%% - handle_call/3
%%% - handle_cast/2
%%% - handle_info/2
%%% - terminate/2
%%%
%%% And internal functions:
%%% - connect_to_srun/2
%%% - do_notify_job_ready/3
%%% - do_notify_job_complete/4
%%% - send_job_ready_message/3
%%% - send_job_complete_message/3
%%% - send_task_output_message/4
%%% - binary_to_hex/1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_callback_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

srun_callback_100cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% start_link/0 tests
        {"Start link success", fun test_start_link_success/0},
        {"Process is registered", fun test_process_registered/0},

        %% register_callback/4 tests
        {"Register callback success", fun test_register_callback_success/0},
        {"Register callback connect failure", fun test_register_callback_connect_failure/0},
        {"Register callback invalid port 0", fun test_register_callback_invalid_port_zero/0},
        {"Register callback with binary host", fun test_register_callback_binary_host/0},
        {"Register callback with IPv4 address", fun test_register_callback_ipv4/0},
        {"Register callback high port number", fun test_register_callback_high_port/0},
        {"Register callback stores srun_pid", fun test_register_callback_stores_srun_pid/0},
        {"Register callback overwrites existing", fun test_register_callback_overwrites/0},

        %% register_existing_socket/2 tests
        {"Register existing socket success", fun test_register_existing_socket_success/0},
        {"Register existing socket stores socket", fun test_register_existing_socket_stores/0},
        {"Register existing socket default host", fun test_register_existing_socket_default_host/0},
        {"Register existing socket default port", fun test_register_existing_socket_default_port/0},

        %% notify_job_ready/2 tests
        {"Notify job ready success", fun test_notify_job_ready_success/0},
        {"Notify job ready no callback", fun test_notify_job_ready_no_callback/0},
        {"Notify job ready send fails", fun test_notify_job_ready_send_fails/0},
        {"Notify job ready encode fails", fun test_notify_job_ready_encode_fails/0},
        {"Notify job ready with node list", fun test_notify_job_ready_with_node_list/0},
        {"Notify job ready multiple times", fun test_notify_job_ready_multiple/0},

        %% notify_job_complete/3 tests
        {"Notify job complete success", fun test_notify_job_complete_success/0},
        {"Notify job complete removes callback", fun test_notify_job_complete_removes_callback/0},
        {"Notify job complete no callback", fun test_notify_job_complete_no_callback/0},
        {"Notify job complete exit code 0", fun test_notify_job_complete_exit_code_0/0},
        {"Notify job complete exit code 1", fun test_notify_job_complete_exit_code_1/0},
        {"Notify job complete exit code 255", fun test_notify_job_complete_exit_code_255/0},
        {"Notify job complete send fails", fun test_notify_job_complete_send_fails/0},
        {"Notify job complete encode fails", fun test_notify_job_complete_encode_fails/0},
        {"Notify job complete with output", fun test_notify_job_complete_with_output/0},

        %% get_callback/1 tests
        {"Get callback success", fun test_get_callback_success/0},
        {"Get callback not found", fun test_get_callback_not_found/0},
        {"Get callback returns full info", fun test_get_callback_returns_info/0},
        {"Get callback after registration", fun test_get_callback_after_registration/0},

        %% send_task_output/3 tests
        {"Send task output success", fun test_send_task_output_success/0},
        {"Send task output no callback", fun test_send_task_output_no_callback/0},
        {"Send task output empty data", fun test_send_task_output_empty/0},
        {"Send task output binary data", fun test_send_task_output_binary/0},
        {"Send task output with exit code", fun test_send_task_output_exit_code/0},
        {"Send task output encode fails", fun test_send_task_output_encode_fails/0},
        {"Send task output send fails", fun test_send_task_output_send_fails/0},

        %% handle_info tcp_closed tests
        {"TCP closed removes callback", fun test_tcp_closed_removes_callback/0},
        {"TCP closed unknown socket ignored", fun test_tcp_closed_unknown_socket/0},
        {"TCP closed multiple callbacks", fun test_tcp_closed_multiple_callbacks/0},

        %% handle_info tcp_error tests
        {"TCP error removes callback", fun test_tcp_error_removes_callback/0},
        {"TCP error with reason", fun test_tcp_error_with_reason/0},
        {"TCP error unknown socket ignored", fun test_tcp_error_unknown_socket/0},

        %% handle_info unknown message tests
        {"Unknown info ignored", fun test_unknown_info_ignored/0},
        {"Unknown info process survives", fun test_unknown_info_survives/0},

        %% handle_call unknown tests
        {"Unknown call returns error", fun test_unknown_call_error/0},

        %% handle_cast tests
        {"Unknown cast ignored", fun test_unknown_cast_ignored/0},
        {"Cast process survives", fun test_cast_process_survives/0},

        %% terminate/2 tests
        {"Terminate closes all sockets", fun test_terminate_closes_sockets/0}
     ]}.

%%====================================================================
%% connect_to_srun tests
%%====================================================================

connect_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Connect to srun success", fun test_connect_success/0},
        {"Connect to srun failure", fun test_connect_failure/0},
        {"Connect to srun timeout", fun test_connect_timeout/0},
        {"Connect with invalid port returns error", fun test_connect_invalid_port/0},
        {"Connect uses binary host", fun test_connect_binary_host/0},
        {"Connect sets socket options", fun test_connect_socket_options/0}
     ]}.

%%====================================================================
%% Message encoding/sending tests
%%====================================================================

message_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Send job ready message success", fun test_send_ready_message_success/0},
        {"Send job ready message encode fails", fun test_send_ready_message_encode_fails/0},
        {"Send job ready message send fails", fun test_send_ready_message_send_fails/0},
        {"Send job complete message success", fun test_send_complete_message_success/0},
        {"Send job complete message closes socket", fun test_send_complete_closes_socket/0},
        {"Send job complete message encode fails", fun test_send_complete_encode_fails/0},
        {"Send job complete message send fails", fun test_send_complete_send_fails/0},
        {"Send task output message success", fun test_send_output_message_success/0},
        {"Send task output message encode fails", fun test_send_output_message_encode_fails/0}
     ]}.

%%====================================================================
%% binary_to_hex tests
%%====================================================================

binary_to_hex_test_() ->
    [
        {"Empty binary returns empty", fun test_binary_to_hex_empty/0},
        {"Single byte", fun test_binary_to_hex_single/0},
        {"Multiple bytes", fun test_binary_to_hex_multiple/0},
        {"Zero byte", fun test_binary_to_hex_zero/0},
        {"FF byte", fun test_binary_to_hex_ff/0}
    ].

%%====================================================================
%% Additional edge case tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Job ID 0 is valid", fun test_job_id_zero/0},
        {"Large job ID is valid", fun test_job_id_large/0},
        {"Multiple concurrent callbacks", fun test_multiple_callbacks/0},
        {"Callback isolation", fun test_callback_isolation/0},
        {"Re-register same job ID", fun test_reregister_same_job/0},
        {"Process state consistency", fun test_state_consistency/0}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Unload any existing mocks
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_protocol_codec),

    %% Start meck for mocking
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:new(flurm_protocol_codec, [non_strict]),

    %% Default mock behaviors
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {ok, make_ref()}
    end),
    meck:expect(gen_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(gen_tcp, close, fun(_Socket) -> ok end),
    meck:expect(flurm_protocol_codec, encode_response, fun(_MsgType, _Record) ->
        {ok, <<"fake_encoded_message">>}
    end),

    %% Stop any existing process
    case whereis(flurm_srun_callback) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,

    %% Start fresh process
    {ok, CbPid} = flurm_srun_callback:start_link(),
    unlink(CbPid),
    CbPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end,
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_protocol_codec),
    ok.

%%====================================================================
%% start_link/0 Tests
%%====================================================================

test_start_link_success() ->
    ?assertNotEqual(undefined, whereis(flurm_srun_callback)).

test_process_registered() ->
    Pid = whereis(flurm_srun_callback),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

%%====================================================================
%% register_callback/4 Tests
%%====================================================================

test_register_callback_success() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1, <<"host1">>, 12345, 42)).

test_register_callback_connect_failure() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, econnrefused}
    end),
    ?assertEqual({error, econnrefused},
                 flurm_srun_callback:register_callback(2, <<"host2">>, 12346, 43)).

test_register_callback_invalid_port_zero() ->
    ?assertEqual({error, invalid_port},
                 flurm_srun_callback:register_callback(3, <<"host3">>, 0, 44)).

test_register_callback_binary_host() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(4, <<"compute-001">>, 5000, 1)).

test_register_callback_ipv4() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(5, <<"192.168.1.100">>, 5001, 2)).

test_register_callback_high_port() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(6, <<"host">>, 65535, 3)).

test_register_callback_stores_srun_pid() ->
    ok = flurm_srun_callback:register_callback(7, <<"host">>, 6000, 12345),
    {ok, Info} = flurm_srun_callback:get_callback(7),
    ?assertEqual(12345, maps:get(srun_pid, Info)).

test_register_callback_overwrites() ->
    ok = flurm_srun_callback:register_callback(8, <<"host1">>, 7000, 1),
    ok = flurm_srun_callback:register_callback(8, <<"host2">>, 7001, 2),
    {ok, Info} = flurm_srun_callback:get_callback(8),
    ?assertEqual(7001, maps:get(port, Info)).

%%====================================================================
%% register_existing_socket/2 Tests
%%====================================================================

test_register_existing_socket_success() ->
    FakeSocket = make_ref(),
    ?assertEqual(ok, flurm_srun_callback:register_existing_socket(10, FakeSocket)).

test_register_existing_socket_stores() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(11, FakeSocket),
    {ok, Info} = flurm_srun_callback:get_callback(11),
    ?assertEqual(FakeSocket, maps:get(socket, Info)).

test_register_existing_socket_default_host() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(12, FakeSocket),
    {ok, Info} = flurm_srun_callback:get_callback(12),
    ?assertEqual(<<"unknown">>, maps:get(host, Info)).

test_register_existing_socket_default_port() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(13, FakeSocket),
    {ok, Info} = flurm_srun_callback:get_callback(13),
    ?assertEqual(0, maps:get(port, Info)).

%%====================================================================
%% notify_job_ready/2 Tests
%%====================================================================

test_notify_job_ready_success() ->
    ok = flurm_srun_callback:register_callback(20, <<"host">>, 8000, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(20, <<"node1">>)).

test_notify_job_ready_no_callback() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:notify_job_ready(999, <<"node1">>)).

test_notify_job_ready_send_fails() ->
    ok = flurm_srun_callback:register_callback(21, <<"host">>, 8001, 1),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_srun_callback:notify_job_ready(21, <<"node">>)).

test_notify_job_ready_encode_fails() ->
    ok = flurm_srun_callback:register_callback(22, <<"host">>, 8002, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed}, flurm_srun_callback:notify_job_ready(22, <<"node">>)).

test_notify_job_ready_with_node_list() ->
    ok = flurm_srun_callback:register_callback(23, <<"host">>, 8003, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(23, <<"node[1-10]">>)).

test_notify_job_ready_multiple() ->
    ok = flurm_srun_callback:register_callback(24, <<"host">>, 8004, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(24, <<"node1">>)),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(24, <<"node2">>)),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(24, <<"node3">>)).

%%====================================================================
%% notify_job_complete/3 Tests
%%====================================================================

test_notify_job_complete_success() ->
    ok = flurm_srun_callback:register_callback(30, <<"host">>, 9000, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(30, 0, <<>>)).

test_notify_job_complete_removes_callback() ->
    ok = flurm_srun_callback:register_callback(31, <<"host">>, 9001, 1),
    ok = flurm_srun_callback:notify_job_complete(31, 0, <<>>),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(31)).

test_notify_job_complete_no_callback() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:notify_job_complete(999, 0, <<>>)).

test_notify_job_complete_exit_code_0() ->
    ok = flurm_srun_callback:register_callback(32, <<"host">>, 9002, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(32, 0, <<>>)).

test_notify_job_complete_exit_code_1() ->
    ok = flurm_srun_callback:register_callback(33, <<"host">>, 9003, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(33, 1, <<>>)).

test_notify_job_complete_exit_code_255() ->
    ok = flurm_srun_callback:register_callback(34, <<"host">>, 9004, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(34, 255, <<>>)).

test_notify_job_complete_send_fails() ->
    ok = flurm_srun_callback:register_callback(35, <<"host">>, 9005, 1),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_srun_callback:notify_job_complete(35, 0, <<>>)).

test_notify_job_complete_encode_fails() ->
    ok = flurm_srun_callback:register_callback(36, <<"host">>, 9006, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed}, flurm_srun_callback:notify_job_complete(36, 0, <<>>)).

test_notify_job_complete_with_output() ->
    ok = flurm_srun_callback:register_callback(37, <<"host">>, 9007, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(37, 0, <<"output data">>)).

%%====================================================================
%% get_callback/1 Tests
%%====================================================================

test_get_callback_success() ->
    ok = flurm_srun_callback:register_callback(40, <<"host">>, 10000, 1),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(40)).

test_get_callback_not_found() ->
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(99999)).

test_get_callback_returns_info() ->
    ok = flurm_srun_callback:register_callback(41, <<"myhost">>, 10001, 42),
    {ok, Info} = flurm_srun_callback:get_callback(41),
    ?assertEqual(<<"myhost">>, maps:get(host, Info)),
    ?assertEqual(10001, maps:get(port, Info)),
    ?assertEqual(42, maps:get(srun_pid, Info)),
    ?assert(maps:is_key(socket, Info)).

test_get_callback_after_registration() ->
    ok = flurm_srun_callback:register_callback(42, <<"host">>, 10002, 1),
    {ok, _Info1} = flurm_srun_callback:get_callback(42),
    ok = flurm_srun_callback:register_callback(43, <<"host">>, 10003, 2),
    {ok, _Info2} = flurm_srun_callback:get_callback(43),
    %% Both should still be accessible
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(42)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(43)).

%%====================================================================
%% send_task_output/3 Tests
%%====================================================================

test_send_task_output_success() ->
    ok = flurm_srun_callback:register_callback(50, <<"host">>, 11000, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(50, <<"output">>, 0)).

test_send_task_output_no_callback() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:send_task_output(99999, <<"output">>, 0)).

test_send_task_output_empty() ->
    ok = flurm_srun_callback:register_callback(51, <<"host">>, 11001, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(51, <<>>, 0)).

test_send_task_output_binary() ->
    ok = flurm_srun_callback:register_callback(52, <<"host">>, 11002, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(52, <<1,2,3,4,5>>, 0)).

test_send_task_output_exit_code() ->
    ok = flurm_srun_callback:register_callback(53, <<"host">>, 11003, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(53, <<"output">>, 42)).

test_send_task_output_encode_fails() ->
    ok = flurm_srun_callback:register_callback(54, <<"host">>, 11004, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed}, flurm_srun_callback:send_task_output(54, <<"output">>, 0)).

test_send_task_output_send_fails() ->
    ok = flurm_srun_callback:register_callback(55, <<"host">>, 11005, 1),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_srun_callback:send_task_output(55, <<"output">>, 0)).

%%====================================================================
%% handle_info tcp_closed Tests
%%====================================================================

test_tcp_closed_removes_callback() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(60, FakeSocket),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(60)),
    whereis(flurm_srun_callback) ! {tcp_closed, FakeSocket},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(60)).

test_tcp_closed_unknown_socket() ->
    UnknownSocket = make_ref(),
    ok = flurm_srun_callback:register_callback(61, <<"host">>, 12000, 1),
    whereis(flurm_srun_callback) ! {tcp_closed, UnknownSocket},
    timer:sleep(50),
    %% Callback should still exist
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(61)).

test_tcp_closed_multiple_callbacks() ->
    Socket1 = make_ref(),
    Socket2 = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(62, Socket1),
    ok = flurm_srun_callback:register_existing_socket(63, Socket2),
    whereis(flurm_srun_callback) ! {tcp_closed, Socket1},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(62)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(63)).

%%====================================================================
%% handle_info tcp_error Tests
%%====================================================================

test_tcp_error_removes_callback() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(70, FakeSocket),
    whereis(flurm_srun_callback) ! {tcp_error, FakeSocket, econnreset},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(70)).

test_tcp_error_with_reason() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(71, FakeSocket),
    whereis(flurm_srun_callback) ! {tcp_error, FakeSocket, etimedout},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(71)).

test_tcp_error_unknown_socket() ->
    UnknownSocket = make_ref(),
    ok = flurm_srun_callback:register_callback(72, <<"host">>, 13000, 1),
    whereis(flurm_srun_callback) ! {tcp_error, UnknownSocket, error},
    timer:sleep(50),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(72)).

%%====================================================================
%% handle_info unknown message Tests
%%====================================================================

test_unknown_info_ignored() ->
    Pid = whereis(flurm_srun_callback),
    Pid ! {unknown, message},
    timer:sleep(20),
    ?assert(is_process_alive(Pid)).

test_unknown_info_survives() ->
    Pid = whereis(flurm_srun_callback),
    [Pid ! bad_msg || _ <- lists:seq(1, 10)],
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).

%%====================================================================
%% handle_call unknown Tests
%%====================================================================

test_unknown_call_error() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_srun_callback, bogus_request)).

%%====================================================================
%% handle_cast Tests
%%====================================================================

test_unknown_cast_ignored() ->
    gen_server:cast(flurm_srun_callback, bogus_cast),
    timer:sleep(20),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

test_cast_process_survives() ->
    [gen_server:cast(flurm_srun_callback, {cast, N}) || N <- lists:seq(1, 10)],
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

test_terminate_closes_sockets() ->
    ok = flurm_srun_callback:register_callback(80, <<"host">>, 14000, 1),
    ok = flurm_srun_callback:register_callback(81, <<"host">>, 14001, 2),
    ok = flurm_srun_callback:register_callback(82, <<"host">>, 14002, 3),
    gen_server:stop(whereis(flurm_srun_callback), normal, 5000),
    timer:sleep(50),
    ?assert(meck:num_calls(gen_tcp, close, '_') >= 3).

%%====================================================================
%% connect_to_srun Tests
%%====================================================================

test_connect_success() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {ok, make_ref()}
    end),
    ?assertEqual(ok, flurm_srun_callback:register_callback(100, <<"host">>, 15000, 1)).

test_connect_failure() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, econnrefused}
    end),
    ?assertEqual({error, econnrefused},
                 flurm_srun_callback:register_callback(101, <<"host">>, 15001, 1)).

test_connect_timeout() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, timeout}
    end),
    ?assertEqual({error, timeout},
                 flurm_srun_callback:register_callback(102, <<"host">>, 15002, 1)).

test_connect_invalid_port() ->
    ?assertEqual({error, invalid_port},
                 flurm_srun_callback:register_callback(103, <<"host">>, 0, 1)).

test_connect_binary_host() ->
    meck:expect(gen_tcp, connect, fun(Host, _Port, _Opts, _Timeout) ->
        ?assert(is_list(Host)),  %% Should be converted to list
        {ok, make_ref()}
    end),
    ?assertEqual(ok, flurm_srun_callback:register_callback(104, <<"hostname.local">>, 15003, 1)).

test_connect_socket_options() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, Opts, _Timeout) ->
        ?assert(lists:member(binary, Opts)),
        ?assert(lists:member({packet, raw}, Opts)),
        ?assert(lists:member({active, true}, Opts)),
        ?assert(lists:member({nodelay, true}, Opts)),
        {ok, make_ref()}
    end),
    ?assertEqual(ok, flurm_srun_callback:register_callback(105, <<"host">>, 15004, 1)).

%%====================================================================
%% Message Encoding/Sending Tests
%%====================================================================

test_send_ready_message_success() ->
    ok = flurm_srun_callback:register_callback(110, <<"host">>, 16000, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(?RESPONSE_SLURM_RC, _) ->
        {ok, <<"ready_msg">>}
    end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(110, <<"node">>)).

test_send_ready_message_encode_fails() ->
    ok = flurm_srun_callback:register_callback(111, <<"host">>, 16001, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, bad_format} end),
    ?assertEqual({error, bad_format}, flurm_srun_callback:notify_job_ready(111, <<"node">>)).

test_send_ready_message_send_fails() ->
    ok = flurm_srun_callback:register_callback(112, <<"host">>, 16002, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<"msg">>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, epipe} end),
    ?assertEqual({error, epipe}, flurm_srun_callback:notify_job_ready(112, <<"node">>)).

test_send_complete_message_success() ->
    ok = flurm_srun_callback:register_callback(113, <<"host">>, 16003, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(?RESPONSE_SLURM_RC, _) ->
        {ok, <<"complete_msg">>}
    end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(113, 0, <<>>)).

test_send_complete_closes_socket() ->
    ok = flurm_srun_callback:register_callback(114, <<"host">>, 16004, 1),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(114, 0, <<>>)),
    ?assert(meck:called(gen_tcp, close, '_')).

test_send_complete_encode_fails() ->
    ok = flurm_srun_callback:register_callback(115, <<"host">>, 16005, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, bad_format} end),
    ?assertEqual({error, bad_format}, flurm_srun_callback:notify_job_complete(115, 0, <<>>)).

test_send_complete_send_fails() ->
    ok = flurm_srun_callback:register_callback(116, <<"host">>, 16006, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<"msg">>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_srun_callback:notify_job_complete(116, 0, <<>>)).

test_send_output_message_success() ->
    ok = flurm_srun_callback:register_callback(117, <<"host">>, 16007, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(?SRUN_JOB_COMPLETE, _) ->
        {ok, <<"output_msg">>}
    end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(117, <<"output">>, 0)).

test_send_output_message_encode_fails() ->
    ok = flurm_srun_callback:register_callback(118, <<"host">>, 16008, 1),
    meck:expect(flurm_protocol_codec, encode_response, fun(?SRUN_JOB_COMPLETE, _) ->
        {error, encode_error}
    end),
    ?assertEqual({error, encode_error}, flurm_srun_callback:send_task_output(118, <<"output">>, 0)).

%%====================================================================
%% binary_to_hex Tests
%%====================================================================

test_binary_to_hex_empty() ->
    Result = flurm_srun_callback:binary_to_hex(<<>>),
    ?assertEqual(<<>>, Result).

test_binary_to_hex_single() ->
    Result = flurm_srun_callback:binary_to_hex(<<16#AB>>),
    ?assertEqual(<<"AB">>, Result).

test_binary_to_hex_multiple() ->
    Result = flurm_srun_callback:binary_to_hex(<<16#DE, 16#AD, 16#BE, 16#EF>>),
    ?assertEqual(<<"DEADBEEF">>, Result).

test_binary_to_hex_zero() ->
    Result = flurm_srun_callback:binary_to_hex(<<0>>),
    ?assertEqual(<<"00">>, Result).

test_binary_to_hex_ff() ->
    Result = flurm_srun_callback:binary_to_hex(<<16#FF>>),
    ?assertEqual(<<"FF">>, Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_job_id_zero() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(0, <<"host">>, 17000, 1)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(0)).

test_job_id_large() ->
    LargeId = 999999999,
    ?assertEqual(ok, flurm_srun_callback:register_callback(LargeId, <<"host">>, 17001, 1)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(LargeId)).

test_multiple_callbacks() ->
    [flurm_srun_callback:register_callback(200 + N, <<"host">>, 18000 + N, N)
     || N <- lists:seq(1, 10)],
    Results = [flurm_srun_callback:get_callback(200 + N) || N <- lists:seq(1, 10)],
    ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)).

test_callback_isolation() ->
    ok = flurm_srun_callback:register_callback(300, <<"host1">>, 19000, 1),
    ok = flurm_srun_callback:register_callback(301, <<"host2">>, 19001, 2),
    ok = flurm_srun_callback:notify_job_complete(300, 0, <<>>),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(300)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(301)).

test_reregister_same_job() ->
    ok = flurm_srun_callback:register_callback(400, <<"host1">>, 20000, 1),
    {ok, Info1} = flurm_srun_callback:get_callback(400),
    ok = flurm_srun_callback:register_callback(400, <<"host2">>, 20001, 2),
    {ok, Info2} = flurm_srun_callback:get_callback(400),
    ?assertNotEqual(maps:get(port, Info1), maps:get(port, Info2)).

test_state_consistency() ->
    %% Register multiple callbacks
    [flurm_srun_callback:register_callback(500 + N, <<"host">>, 21000 + N, N)
     || N <- lists:seq(1, 5)],

    %% Complete some
    ok = flurm_srun_callback:notify_job_complete(501, 0, <<>>),
    ok = flurm_srun_callback:notify_job_complete(503, 0, <<>>),

    %% Check remaining
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(501)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(502)),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(503)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(504)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(505)).

%%====================================================================
%% Additional API Tests
%%====================================================================

api_test_() ->
    [
        {"start_link/0 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:start_link/0, 0))
        end},
        {"register_callback/4 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:register_callback/4, 4))
        end},
        {"register_existing_socket/2 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:register_existing_socket/2, 2))
        end},
        {"notify_job_ready/2 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:notify_job_ready/2, 2))
        end},
        {"notify_job_complete/3 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:notify_job_complete/3, 3))
        end},
        {"get_callback/1 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:get_callback/1, 1))
        end},
        {"send_task_output/3 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:send_task_output/3, 3))
        end}
    ].

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init returns ok with empty state", fun test_init_returns_ok/0},
        {"handle_call get_callback returns not_found for unknown", fun test_handle_call_get_unknown/0},
        {"handle_call send_task_output returns no_callback for unknown", fun test_handle_call_send_unknown/0}
     ]}.

test_init_returns_ok() ->
    %% The process should have started successfully
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

test_handle_call_get_unknown() ->
    Result = gen_server:call(flurm_srun_callback, {get_callback, 88888}),
    ?assertEqual({error, not_found}, Result).

test_handle_call_send_unknown() ->
    Result = gen_server:call(flurm_srun_callback, {send_task_output, 88889, <<"data">>, 0}),
    ?assertEqual({error, no_callback}, Result).

%%====================================================================
%% Concurrent Operation Tests
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Concurrent registrations", fun test_concurrent_registrations/0},
        {"Concurrent notifications", fun test_concurrent_notifications/0},
        {"Concurrent get_callback", fun test_concurrent_get_callback/0}
     ]}.

test_concurrent_registrations() ->
    Results = [flurm_srun_callback:register_callback(600 + N, <<"host">>, 22000 + N, N)
               || N <- lists:seq(1, 20)],
    ?assert(lists:all(fun(ok) -> true; (_) -> false end, Results)).

test_concurrent_notifications() ->
    [flurm_srun_callback:register_callback(700 + N, <<"host">>, 23000 + N, N)
     || N <- lists:seq(1, 10)],
    Results = [flurm_srun_callback:notify_job_ready(700 + N, <<"node">>)
               || N <- lists:seq(1, 10)],
    ?assert(lists:all(fun(ok) -> true; (_) -> false end, Results)).

test_concurrent_get_callback() ->
    [flurm_srun_callback:register_callback(800 + N, <<"host">>, 24000 + N, N)
     || N <- lists:seq(1, 10)],
    Results = [flurm_srun_callback:get_callback(800 + N)
               || N <- lists:seq(1, 10)],
    ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)).

%%====================================================================
%% Callback Info Structure Tests
%%====================================================================

callback_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Callback info is map", fun test_callback_info_is_map/0},
        {"Callback info has all keys", fun test_callback_info_has_keys/0},
        {"Callback info host is binary", fun test_callback_info_host_binary/0},
        {"Callback info port is integer", fun test_callback_info_port_integer/0},
        {"Callback info socket is reference", fun test_callback_info_socket_ref/0}
     ]}.

test_callback_info_is_map() ->
    ok = flurm_srun_callback:register_callback(900, <<"host">>, 25000, 1),
    {ok, Info} = flurm_srun_callback:get_callback(900),
    ?assert(is_map(Info)).

test_callback_info_has_keys() ->
    ok = flurm_srun_callback:register_callback(901, <<"host">>, 25001, 1),
    {ok, Info} = flurm_srun_callback:get_callback(901),
    ?assert(maps:is_key(host, Info)),
    ?assert(maps:is_key(port, Info)),
    ?assert(maps:is_key(socket, Info)),
    ?assert(maps:is_key(srun_pid, Info)).

test_callback_info_host_binary() ->
    ok = flurm_srun_callback:register_callback(902, <<"myhost">>, 25002, 1),
    {ok, Info} = flurm_srun_callback:get_callback(902),
    ?assert(is_binary(maps:get(host, Info))).

test_callback_info_port_integer() ->
    ok = flurm_srun_callback:register_callback(903, <<"host">>, 25003, 1),
    {ok, Info} = flurm_srun_callback:get_callback(903),
    ?assert(is_integer(maps:get(port, Info))).

test_callback_info_socket_ref() ->
    ok = flurm_srun_callback:register_callback(904, <<"host">>, 25004, 1),
    {ok, Info} = flurm_srun_callback:get_callback(904),
    Socket = maps:get(socket, Info),
    ?assert(is_reference(Socket) orelse is_port(Socket)).

%%====================================================================
%% Error Recovery Tests
%%====================================================================

error_recovery_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Recover from failed connect", fun test_recover_failed_connect/0},
        {"Recover from failed send", fun test_recover_failed_send/0},
        {"Recover from tcp_error", fun test_recover_tcp_error/0}
     ]}.

test_recover_failed_connect() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, econnrefused}
    end),
    ?assertEqual({error, econnrefused},
                 flurm_srun_callback:register_callback(1000, <<"host">>, 26000, 1)),
    %% Reset and try again
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {ok, make_ref()}
    end),
    ?assertEqual(ok, flurm_srun_callback:register_callback(1001, <<"host">>, 26001, 1)).

test_recover_failed_send() ->
    ok = flurm_srun_callback:register_callback(1002, <<"host">>, 26002, 1),
    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_srun_callback:notify_job_ready(1002, <<"node">>)),
    %% Reset and try again with new callback
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    ok = flurm_srun_callback:register_callback(1003, <<"host">>, 26003, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(1003, <<"node">>)).

test_recover_tcp_error() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(1004, FakeSocket),
    whereis(flurm_srun_callback) ! {tcp_error, FakeSocket, econnreset},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(1004)),
    %% Can still register new callbacks
    ok = flurm_srun_callback:register_callback(1005, <<"host">>, 26005, 1),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(1005)).
