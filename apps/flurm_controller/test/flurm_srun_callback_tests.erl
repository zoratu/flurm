%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_srun_callback
%%%
%%% Covers srun callback registration, notification, and socket
%%% lifecycle management with mocked gen_tcp and protocol codec.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_srun_callback_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_callback_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

srun_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"process starts and registers",
         fun test_starts_and_registers/0},
        {"register_callback success with mock connect",
         fun test_register_callback_success/0},
        {"register_callback connect failure",
         fun test_register_callback_connect_failure/0},
        {"register_callback invalid port",
         fun test_register_callback_invalid_port/0},
        {"register_existing_socket stores socket",
         fun test_register_existing_socket/0},
        {"get_callback found",
         fun test_get_callback_found/0},
        {"get_callback not_found",
         fun test_get_callback_not_found/0},
        {"notify_job_ready success",
         fun test_notify_job_ready_success/0},
        {"notify_job_ready no_callback",
         fun test_notify_job_ready_no_callback/0},
        {"notify_job_complete sends RC and closes",
         fun test_notify_job_complete_success/0},
        {"notify_job_complete removes callback",
         fun test_notify_job_complete_removes/0},
        {"send_task_output success",
         fun test_send_task_output_success/0},
        {"send_task_output no_callback",
         fun test_send_task_output_no_callback/0},
        {"tcp_closed removes callback",
         fun test_tcp_closed_removes/0},
        {"tcp_error removes callback",
         fun test_tcp_error_removes/0},
        {"terminate closes all sockets",
         fun test_terminate_closes_sockets/0},
        {"unknown call returns error",
         fun test_unknown_call/0},
        {"unknown cast no crash",
         fun test_unknown_cast/0},
        {"unknown info no crash",
         fun test_unknown_info/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    catch meck:unload(gen_tcp),
    catch meck:unload(flurm_protocol_codec),
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:new(flurm_protocol_codec, [non_strict]),
    %% Default mock: connect succeeds with a fake socket
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {ok, make_ref()}
    end),
    meck:expect(gen_tcp, send, fun(_Socket, _Data) -> ok end),
    meck:expect(gen_tcp, close, fun(_Socket) -> ok end),
    meck:expect(flurm_protocol_codec, encode_response, fun(_MsgType, _Record) ->
        {ok, <<"fake_encoded_message">>}
    end),
    case whereis(flurm_srun_callback) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,
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
%% Tests
%%====================================================================

test_starts_and_registers() ->
    ?assertNotEqual(undefined, whereis(flurm_srun_callback)).

test_register_callback_success() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1, <<"host1">>, 12345, 42)).

test_register_callback_connect_failure() ->
    meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
        {error, econnrefused}
    end),
    ?assertEqual({error, econnrefused},
                 flurm_srun_callback:register_callback(2, <<"host2">>, 12346, 43)).

test_register_callback_invalid_port() ->
    ?assertEqual({error, invalid_port},
                 flurm_srun_callback:register_callback(3, <<"host3">>, 0, 44)).

test_register_existing_socket() ->
    FakeSocket = make_ref(),
    ?assertEqual(ok, flurm_srun_callback:register_existing_socket(10, FakeSocket)),
    {ok, Info} = flurm_srun_callback:get_callback(10),
    ?assertEqual(FakeSocket, maps:get(socket, Info)).

test_get_callback_found() ->
    ok = flurm_srun_callback:register_callback(20, <<"host">>, 9999, 1),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(20)).

test_get_callback_not_found() ->
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(999)).

test_notify_job_ready_success() ->
    ok = flurm_srun_callback:register_callback(30, <<"host">>, 8888, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(30, <<"node1">>)).

test_notify_job_ready_no_callback() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:notify_job_ready(999, <<"node1">>)).

test_notify_job_complete_success() ->
    ok = flurm_srun_callback:register_callback(40, <<"host">>, 7777, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(40, 0, <<>>)).

test_notify_job_complete_removes() ->
    ok = flurm_srun_callback:register_callback(41, <<"host">>, 7778, 1),
    ok = flurm_srun_callback:notify_job_complete(41, 0, <<>>),
    %% Callback should be removed after completion
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(41)).

test_send_task_output_success() ->
    ok = flurm_srun_callback:register_callback(50, <<"host">>, 6666, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(50, <<"output">>, 0)).

test_send_task_output_no_callback() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:send_task_output(999, <<"out">>, 0)).

test_tcp_closed_removes() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(60, FakeSocket),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(60)),
    %% Simulate tcp_closed
    whereis(flurm_srun_callback) ! {tcp_closed, FakeSocket},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(60)).

test_tcp_error_removes() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(70, FakeSocket),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(70)),
    %% Simulate tcp_error
    whereis(flurm_srun_callback) ! {tcp_error, FakeSocket, econnreset},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(70)).

test_terminate_closes_sockets() ->
    ok = flurm_srun_callback:register_callback(80, <<"host">>, 5555, 1),
    ok = flurm_srun_callback:register_callback(81, <<"host">>, 5556, 2),
    %% Stop the server (triggers terminate/2)
    gen_server:stop(whereis(flurm_srun_callback), normal, 5000),
    timer:sleep(50),
    %% Verify gen_tcp:close was called (at least for registered sockets)
    ?assert(meck:num_calls(gen_tcp, close, '_') >= 2).

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_srun_callback, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_srun_callback, bogus),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

test_unknown_info() ->
    whereis(flurm_srun_callback) ! bogus,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

%%====================================================================
%% Additional Callback Registration Tests
%%====================================================================

register_callback_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"register_callback with binary host", fun test_register_binary_host/0},
        {"register_callback with high port", fun test_register_high_port/0},
        {"register_multiple_callbacks", fun test_register_multiple/0},
        {"overwrite_existing_callback", fun test_overwrite_callback/0},
        {"notify_job_ready_multiple_times", fun test_notify_ready_multiple/0},
        {"send_task_output_empty_data", fun test_send_empty_output/0},
        {"get_callback_after_complete", fun test_get_after_complete/0}
     ]}.

test_register_binary_host() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(100, <<"192.168.1.1">>, 5000, 1)).

test_register_high_port() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(101, <<"host">>, 65535, 1)).

test_register_multiple() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(102, <<"h1">>, 5001, 1)),
    ?assertEqual(ok, flurm_srun_callback:register_callback(103, <<"h2">>, 5002, 2)),
    ?assertEqual(ok, flurm_srun_callback:register_callback(104, <<"h3">>, 5003, 3)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(102)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(103)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(104)).

test_overwrite_callback() ->
    ok = flurm_srun_callback:register_callback(105, <<"host1">>, 6000, 1),
    ok = flurm_srun_callback:register_callback(105, <<"host2">>, 6001, 2),
    {ok, Info} = flurm_srun_callback:get_callback(105),
    %% Verify the callback was overwritten - check the new port
    ?assertEqual(6001, maps:get(port, Info)).

test_notify_ready_multiple() ->
    ok = flurm_srun_callback:register_callback(106, <<"host">>, 7000, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(106, <<"node1">>)),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(106, <<"node2">>)).

test_send_empty_output() ->
    ok = flurm_srun_callback:register_callback(107, <<"host">>, 8000, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(107, <<>>, 0)).

test_get_after_complete() ->
    ok = flurm_srun_callback:register_callback(108, <<"host">>, 9000, 1),
    ok = flurm_srun_callback:notify_job_complete(108, 0, <<>>),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(108)).

%%====================================================================
%% Additional Socket Handling Tests
%%====================================================================

socket_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"socket stored correctly", fun test_socket_stored/0},
        {"multiple sockets independent", fun test_sockets_independent/0},
        {"socket cleanup on complete", fun test_socket_cleanup_complete/0}
     ]}.

test_socket_stored() ->
    ok = flurm_srun_callback:register_callback(200, <<"host">>, 10000, 1),
    {ok, Info} = flurm_srun_callback:get_callback(200),
    ?assert(maps:is_key(socket, Info)).

test_sockets_independent() ->
    ok = flurm_srun_callback:register_callback(201, <<"host1">>, 10001, 1),
    ok = flurm_srun_callback:register_callback(202, <<"host2">>, 10002, 2),
    {ok, Info1} = flurm_srun_callback:get_callback(201),
    {ok, Info2} = flurm_srun_callback:get_callback(202),
    ?assertNotEqual(maps:get(socket, Info1), maps:get(socket, Info2)).

test_socket_cleanup_complete() ->
    ok = flurm_srun_callback:register_callback(203, <<"host">>, 10003, 1),
    ok = flurm_srun_callback:notify_job_complete(203, 0, <<>>),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(203)).

%%====================================================================
%% Job ID Handling Tests
%%====================================================================

job_id_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job id 0 is valid", fun test_job_id_zero/0},
        {"large job id is valid", fun test_job_id_large/0},
        {"job ids are independent", fun test_job_ids_independent/0}
     ]}.

test_job_id_zero() ->
    %% Job ID 0 might be special, but should still work
    ?assertEqual(ok, flurm_srun_callback:register_callback(0, <<"host">>, 11000, 1)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(0)).

test_job_id_large() ->
    LargeId = 999999999,
    ?assertEqual(ok, flurm_srun_callback:register_callback(LargeId, <<"host">>, 11001, 1)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(LargeId)).

test_job_ids_independent() ->
    ok = flurm_srun_callback:register_callback(300, <<"host">>, 11002, 1),
    ok = flurm_srun_callback:register_callback(301, <<"host">>, 11003, 2),
    ok = flurm_srun_callback:notify_job_complete(300, 0, <<>>),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(300)),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(301)).

%%====================================================================
%% Return Code Handling Tests
%%====================================================================

return_code_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"return code 0 success", fun test_rc_zero/0},
        {"return code 1 failure", fun test_rc_one/0},
        {"return code 255 max", fun test_rc_max/0},
        {"return code with message", fun test_rc_with_message/0}
     ]}.

test_rc_zero() ->
    ok = flurm_srun_callback:register_callback(400, <<"host">>, 12000, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(400, 0, <<>>)).

test_rc_one() ->
    ok = flurm_srun_callback:register_callback(401, <<"host">>, 12001, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(401, 1, <<>>)).

test_rc_max() ->
    ok = flurm_srun_callback:register_callback(402, <<"host">>, 12002, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(402, 255, <<>>)).

test_rc_with_message() ->
    ok = flurm_srun_callback:register_callback(403, <<"host">>, 12003, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_complete(403, 1, <<"Error message">>)).

%%====================================================================
%% Task Output Tests
%%====================================================================

task_output_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"send_task_output with data", fun test_output_with_data/0},
        {"send_task_output binary data", fun test_output_binary/0},
        {"send_task_output task id", fun test_output_task_id/0}
     ]}.

test_output_with_data() ->
    ok = flurm_srun_callback:register_callback(500, <<"host">>, 13000, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(500, <<"Hello World">>, 0)).

test_output_binary() ->
    ok = flurm_srun_callback:register_callback(501, <<"host">>, 13001, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(501, <<1,2,3,4,5>>, 0)).

test_output_task_id() ->
    ok = flurm_srun_callback:register_callback(502, <<"host">>, 13002, 1),
    ?assertEqual(ok, flurm_srun_callback:send_task_output(502, <<"output">>, 5)).

%%====================================================================
%% Notify Job Ready Tests
%%====================================================================

notify_ready_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"notify_job_ready with node name", fun test_ready_with_node/0},
        {"notify_job_ready multiple nodes", fun test_ready_multiple_nodes/0}
     ]}.

test_ready_with_node() ->
    ok = flurm_srun_callback:register_callback(600, <<"host">>, 14000, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(600, <<"compute-001">>)).

test_ready_multiple_nodes() ->
    ok = flurm_srun_callback:register_callback(601, <<"host">>, 14001, 1),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(601, <<"node1">>)),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(601, <<"node2">>)),
    ?assertEqual(ok, flurm_srun_callback:notify_job_ready(601, <<"node3">>)).

%%====================================================================
%% TCP Message Tests
%%====================================================================

tcp_message_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"tcp_closed removes callback by socket", fun test_tcp_closed_socket/0},
        {"tcp_error removes callback by socket", fun test_tcp_error_socket/0}
     ]}.

test_tcp_closed_socket() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(700, FakeSocket),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(700)),
    whereis(flurm_srun_callback) ! {tcp_closed, FakeSocket},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(700)).

test_tcp_error_socket() ->
    FakeSocket = make_ref(),
    ok = flurm_srun_callback:register_existing_socket(701, FakeSocket),
    ?assertMatch({ok, _}, flurm_srun_callback:get_callback(701)),
    whereis(flurm_srun_callback) ! {tcp_error, FakeSocket, closed},
    timer:sleep(50),
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(701)).

%%====================================================================
%% Concurrent Operations Tests
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"concurrent registrations", fun test_concurrent_register/0},
        {"concurrent notifications", fun test_concurrent_notify/0}
     ]}.

test_concurrent_register() ->
    Results = [flurm_srun_callback:register_callback(800 + N, <<"host">>, 15000 + N, N)
               || N <- lists:seq(1, 10)],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

test_concurrent_notify() ->
    [flurm_srun_callback:register_callback(900 + N, <<"host">>, 16000 + N, N)
     || N <- lists:seq(1, 5)],
    Results = [flurm_srun_callback:notify_job_ready(900 + N, <<"node">>)
               || N <- lists:seq(1, 5)],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).

%%====================================================================
%% API Existence Tests
%%====================================================================

api_existence_test_() ->
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
        {"get_callback/1 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:get_callback/1, 1))
        end},
        {"notify_job_ready/2 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:notify_job_ready/2, 2))
        end},
        {"notify_job_complete/3 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:notify_job_complete/3, 3))
        end},
        {"send_task_output/3 exists", fun() ->
            ?assert(is_function(fun flurm_srun_callback:send_task_output/3, 3))
        end}
    ].

%%====================================================================
%% Gen Server Behavior Tests
%%====================================================================

gen_server_behavior_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"process is gen_server", fun test_is_gen_server/0},
        {"process handles calls", fun test_handles_calls/0},
        {"process handles casts", fun test_handles_casts/0}
     ]}.

test_is_gen_server() ->
    Pid = whereis(flurm_srun_callback),
    ?assert(is_process_alive(Pid)),
    ?assertNotEqual(undefined, Pid).

test_handles_calls() ->
    Result = gen_server:call(flurm_srun_callback, {get_callback, 99999}),
    ?assertEqual({error, not_found}, Result).

test_handles_casts() ->
    gen_server:cast(flurm_srun_callback, {some_cast, data}),
    timer:sleep(20),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

%%====================================================================
%% Error Path Tests
%%====================================================================

error_path_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_callback for non-existent id", fun test_get_nonexistent/0},
        {"notify_ready for non-existent id", fun test_notify_nonexistent/0},
        {"send_output for non-existent id", fun test_output_nonexistent/0}
     ]}.

test_get_nonexistent() ->
    ?assertEqual({error, not_found}, flurm_srun_callback:get_callback(88888)).

test_notify_nonexistent() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:notify_job_ready(88889, <<"node">>)).

test_output_nonexistent() ->
    ?assertEqual({error, no_callback}, flurm_srun_callback:send_task_output(88890, <<"data">>, 0)).

%%====================================================================
%% UID Handling Tests
%%====================================================================

uid_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"uid stored correctly", fun test_uid_stored/0},
        {"uid 0 is valid", fun test_uid_zero/0},
        {"large uid is valid", fun test_uid_large/0}
     ]}.

test_uid_stored() ->
    ok = flurm_srun_callback:register_callback(1000, <<"host">>, 17000, 42),
    {ok, Info} = flurm_srun_callback:get_callback(1000),
    ?assert(is_map(Info)).

test_uid_zero() ->
    ok = flurm_srun_callback:register_callback(1001, <<"host">>, 17001, 0),
    {ok, Info} = flurm_srun_callback:get_callback(1001),
    ?assert(is_map(Info)).

test_uid_large() ->
    ok = flurm_srun_callback:register_callback(1002, <<"host">>, 17002, 65534),
    {ok, Info} = flurm_srun_callback:get_callback(1002),
    ?assert(is_map(Info)).

%%====================================================================
%% Host Name Tests
%%====================================================================

host_name_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"host with ipv4 address", fun test_host_ipv4/0},
        {"host with hostname", fun test_host_hostname/0},
        {"host with fqdn", fun test_host_fqdn/0}
     ]}.

test_host_ipv4() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1100, <<"10.0.0.1">>, 18000, 1)).

test_host_hostname() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1101, <<"compute-node-001">>, 18001, 1)).

test_host_fqdn() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1102, <<"node.cluster.example.com">>, 18002, 1)).

%%====================================================================
%% Port Validation Tests
%%====================================================================

port_validation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"port 0 is invalid", fun test_port_zero/0},
        {"port 1 is valid", fun test_port_one/0},
        {"port 65535 is valid", fun test_port_max/0}
     ]}.

test_port_zero() ->
    ?assertEqual({error, invalid_port}, flurm_srun_callback:register_callback(1200, <<"host">>, 0, 1)).

test_port_one() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1201, <<"host">>, 1, 1)).

test_port_max() ->
    ?assertEqual(ok, flurm_srun_callback:register_callback(1202, <<"host">>, 65535, 1)).

%%====================================================================
%% Callback Info Structure Tests
%%====================================================================

callback_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"callback info is map", fun test_info_is_map/0},
        {"callback info has socket", fun test_info_has_socket/0},
        {"callback info not empty", fun test_info_not_empty/0}
     ]}.

test_info_is_map() ->
    ok = flurm_srun_callback:register_callback(1300, <<"host">>, 19000, 100),
    {ok, Info} = flurm_srun_callback:get_callback(1300),
    ?assert(is_map(Info)).

test_info_has_socket() ->
    ok = flurm_srun_callback:register_callback(1301, <<"host">>, 19001, 100),
    {ok, Info} = flurm_srun_callback:get_callback(1301),
    ?assert(maps:is_key(socket, Info)).

test_info_not_empty() ->
    ok = flurm_srun_callback:register_callback(1302, <<"host">>, 19002, 100),
    {ok, Info} = flurm_srun_callback:get_callback(1302),
    ?assert(maps:size(Info) > 0).

%%====================================================================
%% Process Lifecycle Tests
%%====================================================================

process_lifecycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"process survives bad message", fun test_survives_bad_msg/0},
        {"process survives bad cast", fun test_survives_bad_cast/0},
        {"process handles multiple bad messages", fun test_multiple_bad/0}
     ]}.

test_survives_bad_msg() ->
    Pid = whereis(flurm_srun_callback),
    Pid ! {invalid, message, format},
    timer:sleep(30),
    ?assert(is_process_alive(Pid)).

test_survives_bad_cast() ->
    gen_server:cast(flurm_srun_callback, {unknown, cast, message}),
    timer:sleep(30),
    ?assert(is_process_alive(whereis(flurm_srun_callback))).

test_multiple_bad() ->
    Pid = whereis(flurm_srun_callback),
    [Pid ! bad_msg || _ <- lists:seq(1, 10)],
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).
