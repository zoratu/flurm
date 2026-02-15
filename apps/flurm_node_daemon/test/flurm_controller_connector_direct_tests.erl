%%%-------------------------------------------------------------------
%%% Direct callback tests for flurm_controller_connector.
%%% Exercises real callback branches with controlled state and mocks.
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_direct_tests).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    socket,
    host,
    port,
    heartbeat_interval,
    heartbeat_timer,
    connected,
    registered,
    node_id,
    reconnect_interval,
    buffer,
    running_jobs,
    draining,
    drain_reason
}).

base_state() ->
    #state{
        socket = undefined,
        host = "localhost",
        port = 6817,
        heartbeat_interval = 10,
        heartbeat_timer = undefined,
        connected = false,
        registered = false,
        node_id = undefined,
        reconnect_interval = 1000,
        buffer = <<>>,
        running_jobs = #{},
        draining = false,
        drain_reason = undefined
    }.

with_meck(Mods, Fun) ->
    lists:foreach(fun(M) -> meck:new(M, [non_strict]) end, Mods),
    try Fun()
    after
        lists:foreach(fun(M) -> meck:unload(M) end, Mods)
    end.

handle_call_paths_test_() ->
    [
        fun handle_call_send_not_connected_test/0,
        fun handle_call_get_state_test/0,
        fun handle_call_unknown_request_test/0
    ].

handle_cast_paths_test_() ->
    [
        fun handle_cast_job_complete_missing_job_test/0,
        fun handle_cast_job_failed_missing_job_test/0,
        fun handle_cast_unknown_test/0
    ].

handle_info_paths_test_() ->
    [
        fun handle_info_heartbeat_disconnected_test/0,
        fun handle_info_tcp_node_register_ack_test/0,
        fun handle_info_tcp_ack_and_unknown_test/0,
        fun handle_info_tcp_closed_test/0,
        fun handle_info_tcp_error_test/0,
        fun handle_info_down_found_job_test/0,
        fun handle_info_down_unknown_job_test/0,
        fun handle_info_unknown_test/0,
        fun handle_info_connect_failure_test/0
    ].

helpers_and_terminate_test_() ->
    [
        fun decode_messages_decode_error_test/0,
        fun terminate_with_undefined_socket_test/0
    ].

handle_call_send_not_connected_test() ->
    State = base_state(),
    {reply, Reply, _} = flurm_controller_connector:handle_call(
        {send_message, #{}}, self(), State
    ),
    ?assertEqual({error, not_connected}, Reply).

handle_call_get_state_test() ->
    Jobs = #{1 => self(), 2 => self()},
    State = (base_state())#state{connected = true, registered = true, node_id = <<"n1">>, running_jobs = Jobs,
                                 draining = true, drain_reason = <<"maint">>},
    {reply, Reply, _} = flurm_controller_connector:handle_call(get_state, self(), State),
    ?assertEqual(true, maps:get(connected, Reply)),
    ?assertEqual(true, maps:get(registered, Reply)),
    ?assertEqual(<<"n1">>, maps:get(node_id, Reply)),
    ?assertEqual(2, maps:get(running_jobs, Reply)),
    ?assertEqual(true, maps:get(draining, Reply)),
    ?assertEqual(<<"maint">>, maps:get(drain_reason, Reply)).

handle_call_unknown_request_test() ->
    State = base_state(),
    {reply, Reply, _} = flurm_controller_connector:handle_call(unknown, self(), State),
    ?assertEqual({error, unknown_request}, Reply).

handle_cast_job_complete_missing_job_test() ->
    State = base_state(),
    {noreply, NewState} = flurm_controller_connector:handle_cast(
        {job_complete, 42, 0, <<"ok">>, 0}, State
    ),
    ?assertEqual(#{}, NewState#state.running_jobs).

handle_cast_job_failed_missing_job_test() ->
    State = base_state(),
    {noreply, NewState} = flurm_controller_connector:handle_cast(
        {job_failed, 42, timeout, <<"fail">>, 0}, State
    ),
    ?assertEqual(#{}, NewState#state.running_jobs).

handle_cast_unknown_test() ->
    State = base_state(),
    {noreply, NewState} = flurm_controller_connector:handle_cast(unexpected, State),
    ?assertEqual(State, NewState).

handle_info_heartbeat_disconnected_test() ->
    State = base_state(),
    {noreply, NewState} = flurm_controller_connector:handle_info(heartbeat, State),
    ?assertEqual(State, NewState).

handle_info_tcp_node_register_ack_test() ->
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(_Body) ->
            {ok, #{type => node_register_ack, payload => #{<<"node_id">> => <<"node-x">>}}}
        end),
        Frame = <<1:32, 0>>,
        State = (base_state())#state{socket = fake_socket},
        {noreply, NewState} = flurm_controller_connector:handle_info({tcp, fake_socket, Frame}, State),
        ?assertEqual(true, NewState#state.registered),
        ?assertEqual(<<"node-x">>, NewState#state.node_id),
        ?assertEqual(<<>>, NewState#state.buffer)
    end).

handle_info_tcp_ack_and_unknown_test() ->
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun
            (<<1>>) -> {ok, #{type => ack, payload => #{status => ok}}};
            (<<2>>) -> {ok, #{type => totally_unknown, payload => #{}}}
        end),
        Frame = <<1:32, 1, 1:32, 2>>,
        State = (base_state())#state{socket = fake_socket},
        {noreply, NewState} = flurm_controller_connector:handle_info({tcp, fake_socket, Frame}, State),
        ?assertEqual(State#state.running_jobs, NewState#state.running_jobs),
        ?assertEqual(State#state.connected, NewState#state.connected)
    end).

handle_info_tcp_closed_test() ->
    Timer = erlang:send_after(5000, self(), never),
    State = (base_state())#state{socket = fake_socket, connected = true, registered = true, heartbeat_timer = Timer,
                                 reconnect_interval = 1234, buffer = <<1,2,3>>},
    {noreply, NewState} = flurm_controller_connector:handle_info({tcp_closed, fake_socket}, State),
    ?assertEqual(undefined, NewState#state.socket),
    ?assertEqual(false, NewState#state.connected),
    ?assertEqual(false, NewState#state.registered),
    ?assertEqual(undefined, NewState#state.heartbeat_timer),
    ?assertEqual(<<>>, NewState#state.buffer).

handle_info_tcp_error_test() ->
    Timer = erlang:send_after(5000, self(), never2),
    State = (base_state())#state{socket = fake_socket, connected = true, registered = true, heartbeat_timer = Timer,
                                 reconnect_interval = 1234, buffer = <<9>>},
    {noreply, NewState} = flurm_controller_connector:handle_info({tcp_error, fake_socket, econnreset}, State),
    ?assertEqual(undefined, NewState#state.socket),
    ?assertEqual(false, NewState#state.connected),
    ?assertEqual(false, NewState#state.registered),
    ?assertEqual(undefined, NewState#state.heartbeat_timer),
    ?assertEqual(<<>>, NewState#state.buffer).

handle_info_down_found_job_test() ->
    JobPid = spawn(fun() -> receive stop -> ok end end),
    State = (base_state())#state{socket = undefined, running_jobs = #{123 => JobPid}},
    {noreply, NewState} = flurm_controller_connector:handle_info({'DOWN', make_ref(), process, JobPid, crash}, State),
    ?assertEqual(false, maps:is_key(123, NewState#state.running_jobs)),
    JobPid ! stop.

handle_info_down_unknown_job_test() ->
    JobPid = spawn(fun() -> receive stop -> ok end end),
    OtherPid = spawn(fun() -> receive stop -> ok end end),
    State = (base_state())#state{running_jobs = #{123 => JobPid}},
    {noreply, NewState} = flurm_controller_connector:handle_info({'DOWN', make_ref(), process, OtherPid, crash}, State),
    ?assertEqual(true, maps:is_key(123, NewState#state.running_jobs)),
    JobPid ! stop,
    OtherPid ! stop.

handle_info_unknown_test() ->
    State = base_state(),
    {noreply, NewState} = flurm_controller_connector:handle_info(something_else, State),
    ?assertEqual(State, NewState).

handle_info_connect_failure_test() ->
    %% Port 1 on localhost should fail fast with econnrefused on test hosts.
    State = (base_state())#state{host = "127.0.0.1", port = 1, reconnect_interval = 1000},
    {noreply, NewState} = flurm_controller_connector:handle_info(connect, State),
    ?assertEqual(false, NewState#state.connected),
    ?assert(NewState#state.reconnect_interval >= 2000).

decode_messages_decode_error_test() ->
    with_meck([flurm_protocol], fun() ->
        meck:expect(flurm_protocol, decode, fun(_Body) -> {error, bad_payload} end),
        %% One full framed message with decode error should be skipped.
        Buffer = <<1:32, 0>>,
        {Messages, Rest} = flurm_controller_connector:decode_messages(Buffer, []),
        ?assertEqual([], Messages),
        ?assertEqual(<<>>, Rest)
    end).

terminate_with_undefined_socket_test() ->
    Timer = erlang:send_after(5000, self(), will_be_cancelled),
    State = (base_state())#state{heartbeat_timer = Timer, socket = undefined},
    ?assertEqual(ok, flurm_controller_connector:terminate(normal, State)).
