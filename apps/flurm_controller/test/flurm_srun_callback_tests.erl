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
