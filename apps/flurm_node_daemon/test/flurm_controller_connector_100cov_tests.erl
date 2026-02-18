%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Connector 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_controller_connector module covering:
%%% - Connection management and reconnection
%%% - Message sending and receiving
%%% - Job completion reporting
%%% - Heartbeat handling
%%% - Node draining
%%% - Feature detection
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(gen_tcp),
    catch meck:unload(lager),
    catch meck:unload(application),
    catch meck:unload(flurm_protocol),
    catch meck:unload(flurm_system_monitor),
    catch meck:unload(flurm_job_executor),
    catch meck:unload(flurm_job_executor_sup),
    catch meck:unload(filelib),
    catch meck:unload(file),
    catch meck:unload(os),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Decode Messages Tests
%%====================================================================

decode_messages_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"decode_messages with empty buffer",
         fun() ->
             {Messages, Remaining} = flurm_controller_connector:decode_messages(<<>>, []),
             ?assertEqual([], Messages),
             ?assertEqual(<<>>, Remaining)
         end},
        {"decode_messages with incomplete length",
         fun() ->
             Buffer = <<0, 0, 0>>,  %% Only 3 bytes, need 4 for length
             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
             ?assertEqual([], Messages),
             ?assertEqual(Buffer, Remaining)
         end},
        {"decode_messages with incomplete message",
         fun() ->
             Buffer = <<0, 0, 0, 10, 1, 2, 3>>,  %% Length says 10, only 3 data bytes
             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
             ?assertEqual([], Messages),
             ?assertEqual(Buffer, Remaining)
         end},
        {"decode_messages with one complete message",
         fun() ->
             meck:new(flurm_protocol, [passthrough]),
             meck:expect(flurm_protocol, decode, fun(_) -> {ok, #{type => test_msg}} end),

             MsgData = <<"data">>,
             Len = byte_size(MsgData),
             Buffer = <<Len:32, MsgData/binary>>,

             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
             ?assertEqual([#{type => test_msg}], Messages),
             ?assertEqual(<<>>, Remaining)
         end},
        {"decode_messages with multiple complete messages",
         fun() ->
             meck:new(flurm_protocol, [passthrough]),

             Counter = ets:new(counter, [public]),
             ets:insert(Counter, {n, 0}),

             meck:expect(flurm_protocol, decode, fun(_) ->
                 [{n, N}] = ets:lookup(Counter, n),
                 ets:insert(Counter, {n, N + 1}),
                 {ok, #{type => msg, id => N + 1}}
             end),

             Msg1 = <<"msg1">>,
             Msg2 = <<"msg2">>,
             Buffer = <<4:32, Msg1/binary, 4:32, Msg2/binary>>,

             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),

             ets:delete(Counter),
             ?assertEqual(2, length(Messages)),
             ?assertEqual(<<>>, Remaining)
         end},
        {"decode_messages with partial trailing message",
         fun() ->
             meck:new(flurm_protocol, [passthrough]),
             meck:expect(flurm_protocol, decode, fun(_) -> {ok, #{type => complete}} end),

             Complete = <<"done">>,
             Partial = <<0, 0, 0, 20, 1, 2>>,  %% Incomplete second message
             Buffer = <<4:32, Complete/binary, Partial/binary>>,

             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),

             ?assertEqual([#{type => complete}], Messages),
             ?assertEqual(Partial, Remaining)
         end},
        {"decode_messages handles decode error",
         fun() ->
             meck:new(flurm_protocol, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(flurm_protocol, decode, fun(_) -> {error, invalid_msg} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             MsgData = <<"bad">>,
             Buffer = <<3:32, MsgData/binary>>,

             {Messages, Remaining} = flurm_controller_connector:decode_messages(Buffer, []),
             ?assertEqual([], Messages),
             ?assertEqual(<<>>, Remaining)
         end}
     ]}.

%%====================================================================
%% Find Job By Pid Tests
%%====================================================================

find_job_by_pid_test_() ->
    {"find_job_by_pid tests",
     [
        {"find_job_by_pid with matching pid",
         fun() ->
             Jobs = #{1 => self(), 2 => spawn(fun() -> ok end)},
             Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
             ?assertEqual({ok, 1}, Result)
         end},
        {"find_job_by_pid with no match",
         fun() ->
             Jobs = #{1 => spawn(fun() -> ok end), 2 => spawn(fun() -> ok end)},
             Result = flurm_controller_connector:find_job_by_pid(self(), Jobs),
             ?assertEqual(error, Result)
         end},
        {"find_job_by_pid with empty map",
         fun() ->
             Result = flurm_controller_connector:find_job_by_pid(self(), #{}),
             ?assertEqual(error, Result)
         end}
     ]}.

%%====================================================================
%% Cancel Timer Tests
%%====================================================================

cancel_timer_test_() ->
    {"cancel_timer tests",
     [
        {"cancel_timer with undefined",
         fun() ->
             ?assertEqual(ok, flurm_controller_connector:cancel_timer(undefined))
         end},
        {"cancel_timer with valid reference",
         fun() ->
             Ref = erlang:send_after(10000, self(), test_msg),
             ?assertEqual(ok, flurm_controller_connector:cancel_timer(Ref))
         end}
     ]}.

%%====================================================================
%% Detect Features Tests
%%====================================================================

detect_features_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"detect_features with no hardware",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_file, fun("/dev/nvidia0") -> false end),
             meck:expect(filelib, is_dir, fun("/sys/class/infiniband") -> false end),
             meck:expect(file, read_file, fun("/proc/cpuinfo") -> {error, enoent} end),

             Features = flurm_controller_connector:detect_features(),
             ?assertEqual([], Features)
         end},
        {"detect_features with GPU",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_file, fun("/dev/nvidia0") -> true end),
             meck:expect(filelib, is_dir, fun("/sys/class/infiniband") -> false end),
             meck:expect(file, read_file, fun("/proc/cpuinfo") -> {error, enoent} end),

             Features = flurm_controller_connector:detect_features(),
             ?assert(lists:member(<<"gpu">>, Features))
         end},
        {"detect_features with InfiniBand",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_file, fun("/dev/nvidia0") -> false end),
             meck:expect(filelib, is_dir, fun("/sys/class/infiniband") -> true end),
             meck:expect(file, read_file, fun("/proc/cpuinfo") -> {error, enoent} end),

             Features = flurm_controller_connector:detect_features(),
             ?assert(lists:member(<<"ib">>, Features))
         end},
        {"detect_features with AVX",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_file, fun("/dev/nvidia0") -> false end),
             meck:expect(filelib, is_dir, fun("/sys/class/infiniband") -> false end),
             meck:expect(file, read_file, fun("/proc/cpuinfo") ->
                 {ok, <<"flags : fpu avx avx2 sse sse2\n">>}
             end),

             Features = flurm_controller_connector:detect_features(),
             ?assert(lists:member(<<"avx">>, Features))
         end},
        {"detect_features with all hardware",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_file, fun("/dev/nvidia0") -> true end),
             meck:expect(filelib, is_dir, fun("/sys/class/infiniband") -> true end),
             meck:expect(file, read_file, fun("/proc/cpuinfo") ->
                 {ok, <<"flags : avx\n">>}
             end),

             Features = flurm_controller_connector:detect_features(),
             ?assertEqual(3, length(Features)),
             ?assert(lists:member(<<"gpu">>, Features)),
             ?assert(lists:member(<<"ib">>, Features)),
             ?assert(lists:member(<<"avx">>, Features))
         end}
     ]}.

%%====================================================================
%% Check CPU Flag Tests
%%====================================================================

check_cpu_flag_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"check_cpu_flag finds flag",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun("/proc/cpuinfo") ->
                 {ok, <<"model name : Intel\nflags : sse avx avx2\n">>}
             end),

             ?assertEqual(true, flurm_controller_connector:check_cpu_flag("avx")),
             ?assertEqual(true, flurm_controller_connector:check_cpu_flag("sse"))
         end},
        {"check_cpu_flag flag not found",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun("/proc/cpuinfo") ->
                 {ok, <<"flags : sse sse2\n">>}
             end),

             ?assertEqual(false, flurm_controller_connector:check_cpu_flag("avx512"))
         end},
        {"check_cpu_flag file not readable",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun("/proc/cpuinfo") -> {error, enoent} end),

             ?assertEqual(false, flurm_controller_connector:check_cpu_flag("avx"))
         end}
     ]}.

%%====================================================================
%% API Function Tests
%%====================================================================

api_functions_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         %% Register a mock process to handle gen_server calls
         catch unregister(flurm_controller_connector),
         Pid = spawn(fun() -> api_mock_loop() end),
         register(flurm_controller_connector, Pid),
         Pid
     end,
     fun(Pid) ->
         catch unregister(flurm_controller_connector),
         exit(Pid, kill),
         meck_cleanup(ok)
     end,
     [
        {"send_message returns ok when connected",
         fun() ->
             %% Send message to our mock
             flurm_controller_connector ! {set_connected, true},
             Result = flurm_controller_connector:send_message(#{type => test}),
             ?assertEqual(ok, Result)
         end},
        {"send_message returns error when not connected",
         fun() ->
             flurm_controller_connector ! {set_connected, false},
             Result = flurm_controller_connector:send_message(#{type => test}),
             ?assertEqual({error, not_connected}, Result)
         end},
        {"report_job_complete casts message",
         fun() ->
             %% This should not crash
             ok = flurm_controller_connector:report_job_complete(1, 0, <<"output">>)
         end},
        {"report_job_complete with energy",
         fun() ->
             ok = flurm_controller_connector:report_job_complete(1, 0, <<"output">>, 1000)
         end},
        {"report_job_failed casts message",
         fun() ->
             ok = flurm_controller_connector:report_job_failed(1, timeout, <<"error">>)
         end},
        {"report_job_failed with energy",
         fun() ->
             ok = flurm_controller_connector:report_job_failed(1, timeout, <<"error">>, 500)
         end},
        {"get_state returns state map",
         fun() ->
             State = flurm_controller_connector:get_state(),
             ?assert(is_map(State)),
             ?assert(maps:is_key(connected, State))
         end}
     ]}.

api_mock_loop() ->
    api_mock_loop(false).

api_mock_loop(Connected) ->
    receive
        {set_connected, NewConnected} ->
            api_mock_loop(NewConnected);
        {'$gen_call', From, {send_message, _Msg}} ->
            case Connected of
                true -> gen_server:reply(From, ok);
                false -> gen_server:reply(From, {error, not_connected})
            end,
            api_mock_loop(Connected);
        {'$gen_call', From, get_state} ->
            gen_server:reply(From, #{connected => Connected, registered => false,
                                     node_id => undefined, host => "localhost",
                                     port => 6818, running_jobs => 0,
                                     draining => false, drain_reason => undefined}),
            api_mock_loop(Connected);
        {'$gen_cast', _Msg} ->
            api_mock_loop(Connected);
        _Other ->
            api_mock_loop(Connected)
    end.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init sets up state and schedules connect",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun
                 (flurm_node_daemon, controller_host) -> {ok, "controller.local"};
                 (flurm_node_daemon, controller_port) -> {ok, 6818};
                 (flurm_node_daemon, heartbeat_interval) -> {ok, 30000}
             end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, State} = flurm_controller_connector:init([]),

             %% Verify state fields
             ?assertEqual("controller.local", element(3, State)),  %% host
             ?assertEqual(6818, element(4, State)),  %% port
             ?assertEqual(30000, element(5, State)),  %% heartbeat_interval
             ?assertEqual(false, element(8, State)),  %% connected
             ?assertEqual(false, element(9, State)),  %% registered
             ?assertEqual(#{}, element(12, State)),  %% running_jobs

             %% Clean up - a connect message was scheduled
             receive connect -> ok after 100 -> ok end
         end}
     ]}.

%%====================================================================
%% Handle Call Tests
%%====================================================================

handle_call_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_call send_message when connected",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),

             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<"encoded">>} end),
             meck:expect(gen_tcp, send, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {reply, Result, _NewState} = flurm_controller_connector:handle_call(
                 {send_message, #{type => test}}, {self(), make_ref()}, State),

             ?assertEqual(ok, Result)
         end},
        {"handle_call send_message when not connected",
         fun() ->
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {reply, Result, _NewState} = flurm_controller_connector:handle_call(
                 {send_message, #{type => test}}, {self(), make_ref()}, State),

             ?assertEqual({error, not_connected}, Result)
         end},
        {"handle_call get_state returns info map",
         fun() ->
             State = {state, make_ref(), "myhost", 6818, 30000, undefined, true, true,
                      <<"node1">>, 1000, <<>>, #{1 => self()}, true, <<"maintenance">>},

             {reply, Info, _NewState} = flurm_controller_connector:handle_call(
                 get_state, {self(), make_ref()}, State),

             ?assertEqual(true, maps:get(connected, Info)),
             ?assertEqual(true, maps:get(registered, Info)),
             ?assertEqual(<<"node1">>, maps:get(node_id, Info)),
             ?assertEqual("myhost", maps:get(host, Info)),
             ?assertEqual(6818, maps:get(port, Info)),
             ?assertEqual(1, maps:get(running_jobs, Info)),
             ?assertEqual(true, maps:get(draining, Info)),
             ?assertEqual(<<"maintenance">>, maps:get(drain_reason, Info))
         end},
        {"handle_call unknown request",
         fun() ->
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {reply, Result, _NewState} = flurm_controller_connector:handle_call(
                 unknown_request, {self(), make_ref()}, State),

             ?assertEqual({error, unknown_request}, Result)
         end}
     ]}.

%%====================================================================
%% Handle Cast Tests
%%====================================================================

handle_cast_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_cast job_complete reports to controller",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),

             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(gen_tcp, send, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{1 => self()}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_cast(
                 {job_complete, 1, 0, <<"output">>, 500}, State),

             %% Job should be removed from running_jobs
             ?assertEqual(#{}, element(12, NewState))
         end},
        {"handle_cast job_complete for unknown job",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_cast(
                 {job_complete, 999, 0, <<"output">>, 0}, State),

             %% State should be unchanged
             ?assertEqual(State, NewState)
         end},
        {"handle_cast job_failed reports failure",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),

             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(gen_tcp, send, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{2 => self()}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_cast(
                 {job_failed, 2, timeout, <<"timed out">>, 100}, State),

             ?assertEqual(#{}, element(12, NewState))
         end},
        {"handle_cast unknown message",
         fun() ->
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_cast(unknown_msg, State),
             ?assertEqual(State, NewState)
         end}
     ]}.

%%====================================================================
%% Handle Info Connect Tests
%%====================================================================

handle_info_connect_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_info connect success",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),
             meck:new(flurm_system_monitor, [passthrough]),
             meck:new(lager, [passthrough]),

             Socket = make_ref(),
             meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {ok, Socket} end),
             meck:expect(gen_tcp, send, fun(_, _) -> ok end),
             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(flurm_system_monitor, get_metrics, fun() ->
                 #{hostname => <<"node1">>, cpus => 4, total_memory_mb => 8192}
             end),
             meck:expect(flurm_system_monitor, get_gpus, fun() -> [] end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(connect, State),

             ?assertEqual(Socket, element(2, NewState)),
             ?assertEqual(true, element(7, NewState)),  %% connected
             ?assertEqual(1000, element(10, NewState))  %% reconnect_interval reset
         end},
        {"handle_info connect failure retries",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(connect, State),

             ?assertEqual(false, element(7, NewState)),  %% still not connected
             ?assertEqual(2000, element(10, NewState))  %% doubled reconnect interval
         end},
        {"handle_info connect backs off up to max",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(gen_tcp, connect, fun(_, _, _, _) -> {error, econnrefused} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             %% Start with 30s, should cap at 60s
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 30000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(connect, State),

             ?assertEqual(60000, element(10, NewState))  %% capped at max
         end}
     ]}.

%%====================================================================
%% Handle Info Heartbeat Tests
%%====================================================================

handle_info_heartbeat_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_info heartbeat when connected",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),
             meck:new(flurm_system_monitor, [passthrough]),
             meck:new(flurm_job_executor, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(gen_tcp, send, fun(_, _) -> ok end),
             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(flurm_system_monitor, get_metrics, fun() ->
                 #{hostname => <<"node1">>, cpus => 4, total_memory_mb => 8192,
                   free_memory_mb => 4096, available_memory_mb => 4000,
                   load_avg => 1.5, load_avg_5 => 1.2, load_avg_15 => 1.0}
             end),
             meck:expect(flurm_system_monitor, get_gpus, fun() -> [] end),
             meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
             meck:expect(flurm_job_executor, get_current_power, fun() -> 100.5 end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(heartbeat, State),

             %% New heartbeat timer should be set
             ?assertNotEqual(undefined, element(6, NewState))
         end},
        {"handle_info heartbeat when not connected is skipped",
         fun() ->
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(heartbeat, State),
             ?assertEqual(State, NewState)
         end},
        {"handle_info heartbeat handles send failure",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),
             meck:new(flurm_system_monitor, [passthrough]),
             meck:new(flurm_job_executor, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(gen_tcp, send, fun(_, _) -> {error, closed} end),
             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(flurm_system_monitor, get_metrics, fun() ->
                 #{hostname => <<"n1">>, cpus => 1, total_memory_mb => 1024,
                   free_memory_mb => 512, load_avg => 0.5}
             end),
             meck:expect(flurm_system_monitor, get_gpus, fun() -> [] end),
             meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
             meck:expect(flurm_job_executor, get_current_power, fun() -> 0.0 end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             %% Should not crash
             {noreply, _NewState} = flurm_controller_connector:handle_info(heartbeat, State)
         end}
     ]}.

%%====================================================================
%% Handle Info TCP Events Tests
%%====================================================================

handle_info_tcp_events_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_info tcp_closed schedules reconnect",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Timer = erlang:send_after(100000, self(), heartbeat),
             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, Timer, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(
                 {tcp_closed, Socket}, State),

             ?assertEqual(undefined, element(2, NewState)),  %% socket
             ?assertEqual(false, element(7, NewState)),  %% connected
             ?assertEqual(false, element(8, NewState)),  %% registered
             ?assertEqual(undefined, element(6, NewState)),  %% heartbeat_timer
             ?assertEqual(<<>>, element(11, NewState))  %% buffer cleared
         end},
        {"handle_info tcp_error schedules reconnect",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:expect(lager, error, fun(_, _) -> ok end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(
                 {tcp_error, Socket, econnreset}, State),

             ?assertEqual(undefined, element(2, NewState)),
             ?assertEqual(false, element(7, NewState))
         end},
        {"handle_info tcp data accumulates buffer",
         fun() ->
             meck:new(flurm_protocol, [passthrough]),
             meck:expect(flurm_protocol, decode, fun(_) -> {ok, #{type => ping}} end),

             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             %% Send incomplete message
             Data = <<0, 0, 0, 100>>,  %% Just length prefix
             {noreply, NewState} = flurm_controller_connector:handle_info(
                 {tcp, Socket, Data}, State),

             ?assertEqual(Data, element(11, NewState))  %% buffer
         end}
     ]}.

%%====================================================================
%% Handle Info DOWN Tests
%%====================================================================

handle_info_down_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"handle_info DOWN for job executor reports failure",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:new(flurm_protocol, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(gen_tcp, send, fun(_, _) -> ok end),
             meck:expect(flurm_protocol, encode, fun(_) -> {ok, <<>>} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             JobPid = spawn(fun() -> ok end),
             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, undefined, true, true,
                      undefined, 1000, <<>>, #{42 => JobPid}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(
                 {'DOWN', make_ref(), process, JobPid, normal}, State),

             %% Job should be removed
             ?assertEqual(#{}, element(12, NewState))
         end},
        {"handle_info DOWN for unknown process ignored",
         fun() ->
             UnknownPid = spawn(fun() -> ok end),
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             {noreply, NewState} = flurm_controller_connector:handle_info(
                 {'DOWN', make_ref(), process, UnknownPid, normal}, State),

             ?assertEqual(State, NewState)
         end}
     ]}.

%%====================================================================
%% Handle Info Unknown Tests
%%====================================================================

handle_info_unknown_test_() ->
    {"handle_info unknown message",
     fun() ->
         State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                  undefined, 1000, <<>>, #{}, false, undefined},

         {noreply, NewState} = flurm_controller_connector:handle_info(unknown_msg, State),
         ?assertEqual(State, NewState)
     end}.

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"terminate closes socket",
         fun() ->
             meck:new(gen_tcp, [passthrough, unstick]),
             meck:expect(gen_tcp, close, fun(_) -> ok end),

             Timer = erlang:send_after(100000, self(), heartbeat),
             Socket = make_ref(),
             State = {state, Socket, "host", 6818, 30000, Timer, true, true,
                      undefined, 1000, <<>>, #{}, false, undefined},

             Result = flurm_controller_connector:terminate(normal, State),
             ?assertEqual(ok, Result),
             ?assert(meck:called(gen_tcp, close, [Socket]))
         end},
        {"terminate handles undefined socket",
         fun() ->
             State = {state, undefined, "host", 6818, 30000, undefined, false, false,
                      undefined, 1000, <<>>, #{}, false, undefined},

             Result = flurm_controller_connector:terminate(shutdown, State),
             ?assertEqual(ok, Result)
         end}
     ]}.
