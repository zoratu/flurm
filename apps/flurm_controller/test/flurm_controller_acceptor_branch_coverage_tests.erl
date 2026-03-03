%%%-------------------------------------------------------------------
%%% @doc Branch-focused coverage tests for flurm_controller_acceptor.
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_branch_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

acceptor_branch_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"verify_munge branches", fun test_verify_munge_branches/0},
      {"send_response branches", fun test_send_response_branches/0},
      {"call_handler branches", fun test_call_handler_branches/0},
      {"handle_message branches", fun test_handle_message_branches/0},
      {"callback sync branches", fun test_try_callback_silent_sync_branches/0},
      {"spawn handler error branch", fun test_spawn_callback_handler_error_branch/0},
      {"callback handler loop branches", fun test_callback_handler_loop_branches/0},
      {"parse callback response branches", fun test_parse_callback_response_branches/0},
      {"silent callback branches", fun test_try_callback_silent_branches/0},
      {"retry callback branches", fun test_try_callback_connection_branches/0},
      {"callback socket loop branches", fun test_callback_socket_loop_branches/0},
     {"send ping branches", fun test_send_srun_ping_message_branches/0},
      {"close and cleanup branches", fun test_close_and_cleanup_branches/0},
      {"peer host and format ip branches", fun test_peer_host_and_format_ip_branches/0},
      {"loop branches", fun test_loop_branches/0},
      {"init/process light branches", fun test_init_and_process_buffer_branches/0},
      {"timeout/fallback branches", fun test_timeout_and_fallback_branches/0}
     ]}.

setup() ->
    application:set_env(flurm_controller, munge_auth_enabled, true),

    catch meck:unload(gen_tcp),
    catch meck:unload(inet),
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_controller_handler),
    catch meck:unload(flurm_munge),
    catch meck:unload(flurm_connection_limiter),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(ranch),
    catch meck:unload(acc_transport),
    catch meck:unload(acc_loop_transport),

    meck:new(gen_tcp, [unstick, non_strict]),
    meck:new(inet, [unstick, non_strict]),
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:new(flurm_controller_handler, [non_strict]),
    meck:new(flurm_munge, [non_strict]),
    meck:new(flurm_connection_limiter, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),
    meck:new(ranch, [unstick, non_strict]),
    meck:new(acc_transport, [non_strict]),
    meck:new(acc_loop_transport, [non_strict]),

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    meck:expect(gen_tcp, recv, fun(_, _, _) -> {error, timeout} end),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    meck:expect(gen_tcp, controlling_process, fun(_, _) -> ok end),

    meck:expect(inet, peername, fun(_) -> {error, enotconn} end),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(inet, ntoa, fun(_) -> "127.0.0.1" end),

    meck:expect(flurm_protocol_codec, message_type_name, fun(_) -> <<"unknown">> end),
    meck:expect(flurm_protocol_codec, decode_with_extra, fun(_) -> {error, no_auth} end),
    meck:expect(flurm_protocol_codec, decode, fun(_) -> {error, bad_decode} end),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<1,2,3>>} end),

    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    end),

    meck:expect(flurm_munge, is_available, fun() -> false end),
    meck:expect(flurm_munge, verify, fun(_) -> {error, unavailable} end),

    meck:expect(flurm_connection_limiter, connection_allowed, fun(_) -> true end),
    meck:expect(flurm_connection_limiter, connection_opened, fun(_) -> ok end),
    meck:expect(flurm_connection_limiter, connection_closed, fun(_) -> ok end),
    meck:expect(ranch, handshake, fun(_) -> {error, handshake_failed} end),

    meck:expect(flurm_job_manager, cancel_job, fun(_) -> ok end),

    meck:expect(acc_transport, send, fun(_, _) -> ok end),
    meck:expect(acc_transport, recv, fun(_, _, _) -> {error, closed} end),
    meck:expect(acc_transport, close, fun(_) -> ok end),
    meck:expect(acc_transport, setopts, fun(_, _) -> ok end),

    meck:expect(acc_loop_transport, send, fun(_, _) -> ok end),
    meck:expect(acc_loop_transport, recv, fun(_, _, _) -> {error, closed} end),
    meck:expect(acc_loop_transport, close, fun(_) -> ok end),
    meck:expect(acc_loop_transport, setopts, fun(_, _) -> ok end),

    ok.

cleanup(_) ->
    catch meck:unload(gen_tcp),
    catch meck:unload(inet),
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_controller_handler),
    catch meck:unload(flurm_munge),
    catch meck:unload(flurm_connection_limiter),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(ranch),
    catch meck:unload(acc_transport),
    catch meck:unload(acc_loop_transport),
    application:unset_env(flurm_controller, munge_auth_enabled),
    ok.

test_verify_munge_branches() ->
    ?assertEqual({error, no_credential},
                 flurm_controller_acceptor:do_verify_munge_credential(#{auth_type => munge}, strict)),
    ?assertEqual({ok, no_credential},
                 flurm_controller_acceptor:do_verify_munge_credential(#{auth_type => munge}, true)),
    ?assertEqual({error, empty_credential},
                 flurm_controller_acceptor:do_verify_munge_credential(
                   #{auth_type => munge, credential => <<>>}, strict)),
    ?assertEqual({ok, empty_credential},
                 flurm_controller_acceptor:do_verify_munge_credential(
                   #{auth_type => munge, credential => <<>>}, true)),
    ?assertEqual({error, no_munge_auth},
                 flurm_controller_acceptor:do_verify_munge_credential(#{auth_type => none}, strict)),
    ?assertEqual({ok, skipped},
                 flurm_controller_acceptor:do_verify_munge_credential(#{auth_type => none}, true)),
    ?assertMatch({ok, _},
                 flurm_controller_acceptor:do_verify_munge_credential(
                   #{auth_type => munge, credential => <<"cred">>}, true)),

    meck:expect(flurm_munge, is_available, fun() -> true end),
    meck:expect(flurm_munge, verify, fun(_) -> ok end),
    ?assertEqual({ok, verified},
                 flurm_controller_acceptor:verify_munge_credential_binary(<<"cred">>, strict)),

    meck:expect(flurm_munge, verify, fun(_) -> {error, bad_cred} end),
    ?assertEqual({ok, {verification_failed, bad_cred}},
                 flurm_controller_acceptor:verify_munge_credential_binary(<<"cred">>, true)),
    ?assertEqual({error, bad_cred},
                 flurm_controller_acceptor:verify_munge_credential_binary(<<"cred">>, strict)),

    meck:expect(flurm_munge, is_available, fun() -> false end),
    ?assertEqual({ok, munge_unavailable},
                 flurm_controller_acceptor:verify_munge_credential_binary(<<"cred">>, true)),
    ?assertEqual({error, munge_unavailable},
                 flurm_controller_acceptor:verify_munge_credential_binary(<<"cred">>, strict)).

test_send_response_branches() ->
    Sock = make_ref(),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(acc_transport, send, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_controller_acceptor:send_response(
                      Sock, acc_transport, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0})),

    meck:expect(acc_transport, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_controller_acceptor:send_response(
                                 Sock, acc_transport, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0})),

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed}, flurm_controller_acceptor:send_response(
                                        Sock, acc_transport, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0})).

test_call_handler_branches() ->
    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    end),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, _},
                 flurm_controller_acceptor:call_handler_with_timeout(#slurm_header{}, #ping_request{})),

    meck:expect(flurm_controller_handler, handle, fun(_, _) -> exit(handler_crash) end),
    ?assertEqual({error, {handler_crash, handler_crash}},
                 flurm_controller_acceptor:call_handler_with_timeout(#slurm_header{}, #ping_request{})).

test_handle_message_branches() ->
    Socket = make_ref(),
    State0 = #{socket => Socket, transport => acc_transport, request_count => 0},
    Msg = #slurm_msg{
        header = #slurm_header{msg_type = ?REQUEST_PING},
        body = #ping_request{}
    },
    meck:expect(flurm_protocol_codec, decode_with_extra, fun(_) ->
        {ok, Msg, #{hostname => <<"node1">>}, <<>>}
    end),
    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    end),
    {ok, State1} = flurm_controller_acceptor:handle_message(<<1,2,3>>, State0),
    ?assertEqual(1, maps:get(request_count, State1)),

    meck:expect(flurm_controller_handler, handle, fun(_, _) -> {error, handler_failed} end),
    {ok, State2} = flurm_controller_acceptor:handle_message(<<1,2,3>>, State0),
    ?assertEqual(1, maps:get(request_count, State2)),

    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}, #{job_id => 77, port => 1234}}
    end),
    meck:expect(inet, peername, fun(_) -> {ok, {{10, 0, 0, 7}, 4444}} end),
    meck:expect(inet, ntoa, fun(_) -> "10.0.0.7" end),
    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
    {ok, State3} = flurm_controller_acceptor:handle_message(<<1,2,3>>, State0),
    ?assertEqual([77], maps:get(interactive_jobs, State3)),

    meck:expect(inet, peername, fun(_) -> {error, enotconn} end),
    {ok, _State4} = flurm_controller_acceptor:handle_message(<<1,2,3>>, State0),

    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}, #{job_id => 78, port => 0}}
    end),
    {ok, _State5} = flurm_controller_acceptor:handle_message(<<1,2,3>>, State0),

    application:set_env(flurm_controller, munge_auth_enabled, strict),
    meck:expect(flurm_protocol_codec, decode_with_extra, fun(_) ->
        {ok, Msg, #{auth_type => none}, <<>>}
    end),
    ?assertEqual({error, {auth_failed, no_munge_auth}},
                 flurm_controller_acceptor:handle_message(<<1,2,3>>, State0)),
    application:set_env(flurm_controller, munge_auth_enabled, true),

    meck:expect(flurm_protocol_codec, decode_with_extra, fun(_) -> {error, no_auth} end),
    meck:expect(flurm_protocol_codec, decode, fun(_) -> {error, {incomplete_message, 1, 2}} end),
    ?assertEqual({error, incomplete_message},
                 flurm_controller_acceptor:handle_message(<<1,2,3>>, State0)),

    meck:expect(flurm_protocol_codec, decode, fun(_) -> {error, bad_decode} end),
    ?assertEqual({error, bad_decode},
                 flurm_controller_acceptor:handle_message(<<1,2,3>>, State0)).

test_try_callback_silent_sync_branches() ->
    Sock = make_ref(),
    Self = self(),
    Response = <<0:32/big, 0:16, 0:16, 8001:16, 0:32>>,
    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {ok, Sock} end),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    meck:expect(gen_tcp, recv, fun(_, _, _) -> {ok, Response} end),
    meck:expect(gen_tcp, controlling_process, fun(_, Pid) -> Self ! {handler_pid, Pid}, ok end),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 1)),
    HandlerPid = wait_for_handler_pid(),
    HandlerPid ! {tcp_closed, Sock},

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed},
                 flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 2)),

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(gen_tcp, recv, fun(_, _, _) -> {error, timeout} end),
    meck:expect(gen_tcp, controlling_process, fun(_, Pid) -> Self ! {handler_pid, Pid}, ok end),
    ?assertEqual(ok, flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 3)),
    wait_for_handler_pid() ! {tcp_closed, Sock},

    meck:expect(gen_tcp, recv, fun(_, _, _) -> {error, eagain} end),
    meck:expect(gen_tcp, controlling_process, fun(_, Pid) -> Self ! {handler_pid, Pid}, ok end),
    ?assertEqual(ok, flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 4)),
    wait_for_handler_pid() ! {tcp_closed, Sock},

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
    ?assertEqual({error, econnrefused},
                 flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 5)),
    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, enetdown} end),
    ?assertEqual({error, enetdown},
                 flurm_controller_acceptor:try_callback_silent_sync("127.0.0.1", 1000, 6)).

test_spawn_callback_handler_error_branch() ->
    Sock = make_ref(),
    meck:expect(gen_tcp, controlling_process, fun(_, _) -> {error, denied} end),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    ok = flurm_controller_acceptor:spawn_callback_handler(Sock, 10).

test_callback_handler_loop_branches() ->
    Parent = self(),
    Sock = make_ref(),
    Pid1 = spawn(fun() ->
        flurm_controller_acceptor:callback_handler_loop(Sock, 1),
        Parent ! done_1
    end),
    Pid1 ! {tcp, Sock, <<16#AA,16#BB>>},
    Pid1 ! {tcp_error, Sock, econnreset},
    receive done_1 -> ok after 1000 -> ?assert(false) end,

    Pid2 = spawn(fun() ->
        flurm_controller_acceptor:callback_handler_loop(Sock, 2),
        Parent ! done_2
    end),
    Pid2 ! {tcp_closed, Sock},
    receive done_2 -> ok after 1000 -> ?assert(false) end.

test_parse_callback_response_branches() ->
    flurm_controller_acceptor:parse_callback_response(<<0:32,0:16,0:16,8001:16,0:32>>, 10),
    flurm_controller_acceptor:parse_callback_response(<<0:32,0:16,0:16,9999:16,0:32>>, 11),
    flurm_controller_acceptor:parse_callback_response(<<1,2,3>>, 12),
    ok.

test_try_callback_silent_branches() ->
    Sock = make_ref(),
    Parent = self(),
    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
    ?assertEqual({error, econnrefused},
                 flurm_controller_acceptor:try_callback_silent("127.0.0.1", 1111, 1)),

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, enetdown} end),
    ?assertEqual({error, enetdown},
                 flurm_controller_acceptor:try_callback_silent("127.0.0.1", 1111, 2)),

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {ok, Sock} end),
    Pid = spawn(fun() ->
        Result = flurm_controller_acceptor:try_callback_silent("127.0.0.1", 1111, 3),
        Parent ! {silent_result, Result}
    end),
    timer:sleep(20),
    Pid ! {tcp_closed, Sock},
    receive
        {silent_result, Result} ->
            ?assert(Result =:= ok orelse Result =:= {error, lager_not_running})
    after 1000 ->
        ?assert(false)
    end.

test_try_callback_connection_branches() ->
    Sock = make_ref(),
    Self = self(),

    ?assertEqual({error, max_retries},
                 flurm_controller_acceptor:try_callback_connection("127.0.0.1", 1, 1, 0)),

    put(connect_calls, 0),
    meck:expect(gen_tcp, connect, fun(_, _, _, _) ->
        Calls = get(connect_calls),
        put(connect_calls, Calls + 1),
        case Calls of
            0 -> {error, econnrefused};
            _ -> {error, enetunreach}
        end
    end),
    ?assertEqual({error, enetunreach},
                 flurm_controller_acceptor:try_callback_connection("127.0.0.1", 1, 2, 2)),

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {ok, Sock} end),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    meck:expect(inet, setopts, fun(_, _) -> ok end),
    meck:expect(gen_tcp, controlling_process, fun(_, Pid) -> Self ! {handler_pid, Pid}, ok end),
    ?assertEqual(ok, flurm_controller_acceptor:try_callback_connection("127.0.0.1", 1, 3)),
    wait_for_handler_pid() ! stop,

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    meck:expect(gen_tcp, controlling_process, fun(_, Pid) -> Self ! {handler_pid, Pid}, ok end),
    ?assertEqual(ok, flurm_controller_acceptor:try_callback_connection("127.0.0.1", 1, 4)),
    wait_for_handler_pid() ! stop,

    meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, ehostunreach} end),
    ?assertEqual({error, ehostunreach},
                 flurm_controller_acceptor:try_callback_connection("127.0.0.1", 1, 5, 1)).

test_callback_socket_loop_branches() ->
    Parent = self(),
    Sock = make_ref(),
    Header14 = <<0:32,0:16,0:16,1234:16,0:32>>,

    Pid1 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 10),
        Parent ! cb_done_1
    end),
    Pid1 ! {tcp, Sock, Header14},
    Pid1 ! stop,
    receive cb_done_1 -> ok after 1000 -> ?assert(false) end,

    Pid2 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 11),
        Parent ! cb_done_2
    end),
    Pid2 ! {tcp, Sock, <<1,2,3>>},
    Pid2 ! stop,
    receive cb_done_2 -> ok after 1000 -> ?assert(false) end,

    Pid3 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 12),
        Parent ! cb_done_3
    end),
    Pid3 ! {tcp_closed, Sock},
    receive cb_done_3 -> ok after 1000 -> ?assert(false) end,

    Pid4 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 13),
        Parent ! cb_done_4
    end),
    Pid4 ! {tcp_error, Sock, econnreset},
    receive cb_done_4 -> ok after 1000 -> ?assert(false) end,

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    meck:expect(gen_tcp, close, fun(_) -> ok end),
    Pid5 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 14),
        Parent ! cb_done_5
    end),
    Pid5 ! {send_job_complete, 0},
    receive cb_done_5 -> ok after 1000 -> ?assert(false) end,

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    Pid6 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 15),
        Parent ! cb_done_6
    end),
    Pid6 ! {send_job_complete, 1},
    receive cb_done_6 -> ok after 1000 -> ?assert(false) end.

test_send_srun_ping_message_branches() ->
    Sock = make_ref(),
    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {ok, <<16#AA>>} end),
    meck:expect(gen_tcp, send, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_controller_acceptor:send_srun_ping_message(Sock, 1)),

    meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
    ?assertEqual({error, closed}, flurm_controller_acceptor:send_srun_ping_message(Sock, 2)),

    meck:expect(flurm_protocol_codec, encode_response, fun(_, _) -> {error, encode_failed} end),
    ?assertEqual({error, encode_failed}, flurm_controller_acceptor:send_srun_ping_message(Sock, 3)).

test_close_and_cleanup_branches() ->
    Sock1 = make_ref(),
    Sock2 = make_ref(),
    meck:expect(acc_transport, close, fun(_) -> ok end),
    meck:expect(flurm_connection_limiter, connection_closed, fun(_) -> ok end),
    meck:expect(flurm_job_manager, cancel_job, fun(10) -> ok; (11) -> exit(test_exit) end),

    State1 = #{
        socket => Sock1,
        transport => acc_transport,
        request_count => 2,
        start_time => erlang:system_time(millisecond) - 10
    },
    ?assertEqual(ok, flurm_controller_acceptor:close_connection(State1)),

    State2 = #{
        socket => Sock2,
        transport => acc_transport,
        request_count => 4,
        start_time => erlang:system_time(millisecond) - 10,
        peer_ip => {127,0,0,1},
        interactive_jobs => [10, 11]
    },
    ?assertEqual(ok, flurm_controller_acceptor:close_connection(State2)),
    ?assertEqual(ok, flurm_controller_acceptor:cleanup_interactive_jobs(#{interactive_jobs => []})).

test_peer_host_and_format_ip_branches() ->
    meck:expect(inet, peername, fun(_) -> {ok, {{127,0,0,1}, 9000}} end),
    meck:expect(inet, ntoa, fun(_) -> "127.0.0.1" end),
    ?assertEqual({ok, "127.0.0.1"}, flurm_controller_acceptor:get_peer_host(make_ref())),

    meck:expect(inet, peername, fun(_) -> {error, enotconn} end),
    ?assertEqual({error, enotconn}, flurm_controller_acceptor:get_peer_host(make_ref())),

    meck:expect(inet, peername, fun(_) -> {ok, {{16#2001,16#db8,0,0,0,0,0,1}, 9000}} end),
    meck:expect(inet, ntoa, fun(_) -> "2001:db8::1" end),
    ?assertEqual({ok, "2001:db8::1"}, flurm_controller_acceptor:get_peer_host(make_ref())),

    ?assertEqual("1.2.3.4", flurm_controller_acceptor:format_ip({1,2,3,4})),
    ?assertEqual("1:2:3:4:5:6:7:8",
                 flurm_controller_acceptor:format_ip({1,2,3,4,5,6,7,8})).

test_loop_branches() ->
    Sock = make_ref(),

    Data24 = <<100:32/big, 0:16, 0:16, ?REQUEST_PING:16, 0:32, 0:80>>,
    put(loop_calls, 0),
    meck:expect(acc_loop_transport, recv, fun(_, _, _) ->
        Calls = get(loop_calls),
        put(loop_calls, Calls + 1),
        case Calls of
            0 -> {ok, Data24};
            _ -> {error, closed}
        end
    end),
    meck:expect(acc_loop_transport, close, fun(_) -> ok end),
    meck:expect(flurm_connection_limiter, connection_closed, fun(_) -> ok end),
    ?assertEqual(ok, flurm_controller_acceptor:loop(#{
        socket => Sock,
        transport => acc_loop_transport,
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond),
        peer_ip => {127,0,0,1}
    })),

    meck:expect(acc_loop_transport, recv, fun(_, _, _) -> {error, timeout} end),
    ?assertEqual(ok, flurm_controller_acceptor:loop(#{
        socket => Sock,
        transport => acc_loop_transport,
        buffer => <<>>,
        request_count => 1,
        start_time => erlang:system_time(millisecond)
    })),

    meck:expect(acc_loop_transport, recv, fun(_, _, _) -> {error, econnreset} end),
    ?assertEqual(ok, flurm_controller_acceptor:loop(#{
        socket => Sock,
        transport => acc_loop_transport,
        buffer => <<>>,
        request_count => 2,
        start_time => erlang:system_time(millisecond)
    })),

    meck:expect(acc_loop_transport, recv, fun(_, _, _) -> {ok, <<0>>} end),
    ?assertEqual(ok, flurm_controller_acceptor:loop(#{
        socket => Sock,
        transport => acc_loop_transport,
        buffer => binary:copy(<<0>>, 10000000),
        request_count => 3,
        start_time => erlang:system_time(millisecond)
    })).

test_init_and_process_buffer_branches() ->
    Sock = make_ref(),
    meck:expect(ranch, handshake, fun(_) -> {ok, Sock} end),
    meck:expect(inet, peername, fun(_) -> {ok, {{10, 10, 10, 10}, 9999}} end),
    meck:expect(inet, ntoa, fun(_) -> "10.10.10.10" end),
    meck:expect(acc_transport, setopts, fun(_, _) -> ok end),
    meck:expect(acc_transport, close, fun(_) -> ok end),
    meck:expect(flurm_connection_limiter, connection_allowed, fun(_) -> false end),
    ?assertEqual({error, peer_limit_exceeded},
                 flurm_controller_acceptor:init(test_ref, acc_transport, #{})),

    meck:expect(inet, peername, fun(_) -> {error, bad_peer} end),
    ?assertEqual({error, bad_peer},
                 flurm_controller_acceptor:init(test_ref, acc_transport, #{})),

    State = #{
        socket => Sock,
        transport => acc_transport,
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond)
    },
    ?assertMatch({continue, _}, flurm_controller_acceptor:process_buffer(<<1,2,3>>, State)),

    Len = ?SLURM_HEADER_SIZE,
    Payload = binary:copy(<<0>>, Len),
    meck:expect(flurm_protocol_codec, decode_with_extra, fun(_) -> {error, no_auth} end),
    meck:expect(flurm_protocol_codec, decode, fun(_) -> {error, bad_decode} end),
    ?assertMatch({continue, _},
                 flurm_controller_acceptor:process_buffer(<<Len:32/big, Payload/binary>>, State)),

    meck:expect(flurm_protocol_codec, decode, fun(_) ->
        {ok, #slurm_msg{
            header = #slurm_header{msg_type = ?REQUEST_PING},
            body = #ping_request{}
        }, <<>>}
    end),
    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    end),
    ?assertMatch({continue, _},
                 flurm_controller_acceptor:process_buffer(
                   <<Len:32/big, Payload/binary, 16#00, 16#00, 16#00>>, State)),

    meck:expect(acc_loop_transport, recv, fun(_, _, _) -> {error, closed} end),
    ?assertEqual(ok, flurm_controller_acceptor:loop(#{
        socket => Sock,
        transport => acc_loop_transport,
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond)
    })).

test_timeout_and_fallback_branches() ->
    Sock = make_ref(),
    Parent = self(),

    meck:expect(flurm_controller_handler, handle, fun(_, _) ->
        timer:sleep(300),
        {ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    end),
    ?assertEqual({error, handler_timeout},
                 flurm_controller_acceptor:call_handler_with_timeout(#slurm_header{}, #ping_request{})),

    Pid1 = spawn(fun() ->
        flurm_controller_acceptor:callback_handler_loop(Sock, 9001),
        Parent ! callback_handler_timeout_done
    end),
    ?assert(is_pid(Pid1)),
    receive
        callback_handler_timeout_done -> ok
    after 1000 ->
        ?assert(false)
    end,

    Pid2 = spawn(fun() ->
        flurm_controller_acceptor:callback_socket_loop(Sock, 9002),
        Parent ! callback_socket_timeout_done
    end),
    ?assert(is_pid(Pid2)),
    receive
        callback_socket_timeout_done -> ok
    after 1000 ->
        ?assert(false)
    end,

    meck:expect(gen_tcp, controlling_process, fun(_, _) ->
        timer:sleep(200),
        ok
    end),
    _ = flurm_controller_acceptor:spawn_callback_handler(Sock, 9003),

    State = #{
        socket => Sock,
        transport => acc_transport,
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond)
    },
    ?assertMatch({continue, _}, flurm_controller_acceptor:process_buffer(not_binary, State)),

    Ref = make_ref(),
    self() ! {handler_result, Ref, late_result},
    ?assertEqual(ok, flurm_controller_acceptor:drain_late_handler_result(Ref)),
    ?assertEqual(ok, flurm_controller_acceptor:drain_late_handler_result(make_ref())).

wait_for_handler_pid() ->
    receive
        {handler_pid, Pid} -> Pid
    after 1000 ->
        ?assert(false)
    end.
