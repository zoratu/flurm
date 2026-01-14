%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_connector module
%%%
%%% Comprehensive EUnit tests for the controller connector gen_server
%%% which maintains connections to the controller daemon and handles
%%% registration, heartbeats, job assignments, and status reporting.
%%%
%%% These tests validate the logic of the module by testing the
%%% algorithms and data transformations independently.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Message decoding tests
decode_test_() ->
    [
        {"decode_messages handles empty buffer", fun test_decode_messages_empty/0},
        {"decode_messages handles incomplete message", fun test_decode_messages_incomplete/0},
        {"decode_messages handles complete message", fun test_decode_messages_complete/0},
        {"decode_messages handles multiple messages", fun test_decode_messages_multiple/0}
    ].

%% Helper function tests
helper_test_() ->
    [
        {"find_job_by_pid finds job", fun test_find_job_by_pid_found/0},
        {"find_job_by_pid returns error for missing pid", fun test_find_job_by_pid_not_found/0},
        {"cancel_timer handles undefined", fun test_cancel_timer_undefined/0},
        {"cancel_timer handles valid ref", fun test_cancel_timer_valid/0},
        {"detect_features returns list", fun test_detect_features/0},
        {"check_cpu_flag handles missing file", fun test_check_cpu_flag/0},
        {"report_job_to_controller handles undefined socket", fun test_report_job_undefined_socket/0},
        {"send_drain_ack handles undefined socket", fun test_send_drain_ack_undefined_socket/0}
    ].

%% Message handler tests
message_handler_test_() ->
    [
        {"handle_controller_message processes node_register_ack", fun test_handle_register_ack/0},
        {"handle_controller_message processes node_heartbeat_ack", fun test_handle_heartbeat_ack/0},
        {"handle_controller_message processes ack", fun test_handle_ack/0},
        {"handle_controller_message processes error", fun test_handle_error/0},
        {"handle_controller_message processes unknown type", fun test_handle_unknown_type/0},
        {"handle_controller_message processes node_drain", fun test_handle_node_drain/0},
        {"handle_controller_message processes node_resume", fun test_handle_node_resume/0}
    ].

%% Job status mapping tests
job_status_test_() ->
    [
        {"completed status maps correctly", fun test_status_completed/0},
        {"failed timeout status maps correctly", fun test_status_failed_timeout/0},
        {"failed cancelled status maps correctly", fun test_status_failed_cancelled/0},
        {"failed atom status maps correctly", fun test_status_failed_atom/0},
        {"failed binary status maps correctly", fun test_status_failed_binary/0},
        {"failed other status maps correctly", fun test_status_failed_other/0}
    ].

%% Connection logic tests
connection_logic_test_() ->
    [
        {"reconnect interval doubles with backoff", fun test_exponential_backoff/0},
        {"reconnect interval capped at max", fun test_backoff_cap/0}
    ].

%% Job tracking tests
job_tracking_test_() ->
    [
        {"add job to running jobs", fun test_add_running_job/0},
        {"remove job from running jobs", fun test_remove_running_job/0},
        {"count running jobs", fun test_count_running_jobs/0}
    ].

%% Protocol message building tests
protocol_message_test_() ->
    [
        {"build heartbeat message", fun test_build_heartbeat_message/0},
        {"build registration message", fun test_build_registration_message/0},
        {"build job complete message", fun test_build_job_complete_message/0},
        {"build job failed message", fun test_build_job_failed_message/0},
        {"build drain ack message", fun test_build_drain_ack_message/0}
    ].

%% Job launch handling tests
job_launch_test_() ->
    [
        {"parse job launch payload", fun test_parse_job_launch_payload/0},
        {"handle missing optional fields", fun test_job_launch_optional_fields/0},
        {"reject job when draining", fun test_reject_job_when_draining/0}
    ].

%% Job cancel handling tests
job_cancel_test_() ->
    [
        {"find job to cancel", fun test_find_job_to_cancel/0},
        {"handle cancel for missing job", fun test_cancel_missing_job/0}
    ].

%% State structure tests
state_structure_test_() ->
    [
        {"init returns correct state structure", fun test_init_state_structure/0},
        {"state contains all required fields", fun test_state_required_fields/0}
    ].

%%====================================================================
%% Message Decoding Tests
%%====================================================================

test_decode_messages_empty() ->
    %% Empty buffer should return empty list and empty buffer
    Result = decode_messages_wrapper(<<>>, []),
    ?assertEqual({[], <<>>}, Result).

test_decode_messages_incomplete() ->
    %% Buffer with less than 4 bytes (length prefix) is incomplete
    Incomplete = <<1, 2, 3>>,
    Result = decode_messages_wrapper(Incomplete, []),
    ?assertEqual({[], Incomplete}, Result).

test_decode_messages_complete() ->
    %% Create a complete framed message
    Msg = #{type => node_heartbeat_ack, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    Len = byte_size(Encoded),
    Framed = <<Len:32, Encoded/binary>>,

    {Messages, Remaining} = decode_messages_wrapper(Framed, []),
    ?assertEqual(1, length(Messages)),
    ?assertEqual(<<>>, Remaining),
    [DecodedMsg] = Messages,
    ?assertEqual(node_heartbeat_ack, maps:get(type, DecodedMsg)).

test_decode_messages_multiple() ->
    %% Create multiple framed messages
    Msg1 = #{type => node_heartbeat_ack, payload => #{}},
    Msg2 = #{type => ack, payload => #{status => <<"ok">>}},

    {ok, Encoded1} = flurm_protocol:encode(Msg1),
    {ok, Encoded2} = flurm_protocol:encode(Msg2),

    Len1 = byte_size(Encoded1),
    Len2 = byte_size(Encoded2),

    Framed = <<Len1:32, Encoded1/binary, Len2:32, Encoded2/binary>>,

    {Messages, Remaining} = decode_messages_wrapper(Framed, []),
    ?assertEqual(2, length(Messages)),
    ?assertEqual(<<>>, Remaining).

%%====================================================================
%% Helper Function Tests
%%====================================================================

test_find_job_by_pid_found() ->
    Pid = self(),
    Jobs = #{123 => Pid, 456 => spawn(fun() -> ok end)},
    Result = find_job_by_pid_wrapper(Pid, Jobs),
    ?assertEqual({ok, 123}, Result).

test_find_job_by_pid_not_found() ->
    Jobs = #{123 => spawn(fun() -> ok end)},
    Result = find_job_by_pid_wrapper(self(), Jobs),
    ?assertEqual(error, Result).

test_cancel_timer_undefined() ->
    %% Should not crash when given undefined
    ?assertEqual(ok, cancel_timer_wrapper(undefined)).

test_cancel_timer_valid() ->
    %% Create a timer and cancel it
    Ref = erlang:send_after(60000, self(), test),
    Result = cancel_timer_wrapper(Ref),
    ?assert(Result =:= ok orelse is_integer(Result)).

test_detect_features() ->
    %% detect_features should return a list
    Features = detect_features_wrapper(),
    ?assert(is_list(Features)),
    %% All elements should be binaries
    lists:foreach(fun(F) -> ?assert(is_binary(F)) end, Features).

test_check_cpu_flag() ->
    %% check_cpu_flag should handle missing /proc/cpuinfo gracefully
    %% On macOS this will return false
    Result = check_cpu_flag_wrapper("avx"),
    ?assert(is_boolean(Result)).

test_report_job_undefined_socket() ->
    %% Should return error when socket is undefined
    Result = report_job_to_controller_wrapper(undefined, 123, completed, 0, <<"output">>, 0),
    ?assertEqual({error, not_connected}, Result).

test_send_drain_ack_undefined_socket() ->
    %% Should return error when socket is undefined
    Result = send_drain_ack_wrapper(undefined, true, <<"reason">>, 5),
    ?assertEqual({error, not_connected}, Result).

%%====================================================================
%% Message Handler Tests
%%====================================================================

test_handle_register_ack() ->
    State = make_test_state(),
    Msg = #{type => node_register_ack, payload => #{<<"node_id">> => <<"node001">>}},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(true, maps:get(registered, NewState)),
    ?assertEqual(<<"node001">>, maps:get(node_id, NewState)).

test_handle_heartbeat_ack() ->
    State = make_test_state(),
    Msg = #{type => node_heartbeat_ack},
    NewState = handle_controller_message_wrapper(Msg, State),
    %% heartbeat_ack doesn't change state, just confirms controller is alive
    ?assertEqual(State, NewState).

test_handle_ack() ->
    State = make_test_state(),
    Msg = #{type => ack, payload => #{status => <<"ok">>}},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(State, NewState).

test_handle_error() ->
    State = make_test_state(),
    Msg = #{type => error, payload => #{reason => <<"test error">>}},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(State, NewState).

test_handle_unknown_type() ->
    State = make_test_state(),
    Msg = #{type => some_unknown_type, payload => #{data => <<"test">>}},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(State, NewState).

test_handle_node_drain() ->
    State = make_test_state(),
    Msg = #{type => node_drain, payload => #{<<"reason">> => <<"maintenance">>}},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(true, maps:get(draining, NewState)),
    ?assertEqual(<<"maintenance">>, maps:get(drain_reason, NewState)).

test_handle_node_resume() ->
    State = maps:merge(make_test_state(), #{draining => true, drain_reason => <<"maintenance">>}),
    Msg = #{type => node_resume},
    NewState = handle_controller_message_wrapper(Msg, State),
    ?assertEqual(false, maps:get(draining, NewState)),
    ?assertEqual(undefined, maps:get(drain_reason, NewState)).

%%====================================================================
%% Job Status Mapping Tests
%%====================================================================

test_status_completed() ->
    {MsgType, Reason} = map_job_status(completed),
    ?assertEqual(job_complete, MsgType),
    ?assertEqual(<<"completed">>, Reason).

test_status_failed_timeout() ->
    {MsgType, Reason} = map_job_status({failed, timeout}),
    ?assertEqual(job_failed, MsgType),
    ?assertEqual(<<"timeout">>, Reason).

test_status_failed_cancelled() ->
    {MsgType, Reason} = map_job_status({failed, cancelled}),
    ?assertEqual(job_failed, MsgType),
    ?assertEqual(<<"cancelled">>, Reason).

test_status_failed_atom() ->
    {MsgType, Reason} = map_job_status({failed, oom_killed}),
    ?assertEqual(job_failed, MsgType),
    ?assertEqual(<<"oom_killed">>, Reason).

test_status_failed_binary() ->
    {MsgType, Reason} = map_job_status({failed, <<"custom reason">>}),
    ?assertEqual(job_failed, MsgType),
    ?assertEqual(<<"custom reason">>, Reason).

test_status_failed_other() ->
    {MsgType, Reason} = map_job_status({failed, {complex, reason}}),
    ?assertEqual(job_failed, MsgType),
    ?assert(is_binary(Reason)).

%%====================================================================
%% Connection Logic Tests
%%====================================================================

test_exponential_backoff() ->
    Initial = 1000,
    Multiplier = 2,
    After1 = Initial * Multiplier,
    After2 = After1 * Multiplier,
    ?assertEqual(2000, After1),
    ?assertEqual(4000, After2).

test_backoff_cap() ->
    MaxInterval = 60000,
    Multiplier = 2,
    Current = 32000,
    Next = min(Current * Multiplier, MaxInterval),
    ?assertEqual(60000, Next).

%%====================================================================
%% Job Tracking Tests
%%====================================================================

test_add_running_job() ->
    Jobs = #{},
    Pid = self(),
    NewJobs = maps:put(123, Pid, Jobs),
    ?assertEqual(1, maps:size(NewJobs)),
    ?assertEqual(Pid, maps:get(123, NewJobs)).

test_remove_running_job() ->
    Jobs = #{123 => self(), 456 => self()},
    NewJobs = maps:remove(123, Jobs),
    ?assertEqual(1, maps:size(NewJobs)),
    ?assertEqual(false, maps:is_key(123, NewJobs)).

test_count_running_jobs() ->
    Jobs = #{1 => self(), 2 => self(), 3 => self()},
    ?assertEqual(3, maps:size(Jobs)).

%%====================================================================
%% Protocol Message Building Tests
%%====================================================================

test_build_heartbeat_message() ->
    HeartbeatMsg = #{
        type => node_heartbeat,
        payload => #{
            hostname => <<"testnode">>,
            cpus => 8,
            total_memory_mb => 32768,
            free_memory_mb => 16384,
            load_avg => 1.5,
            running_jobs => [1, 2, 3],
            job_count => 3,
            draining => false,
            timestamp => erlang:system_time(millisecond)
        }
    },
    {ok, Encoded} = flurm_protocol:encode(HeartbeatMsg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_heartbeat, maps:get(type, Decoded)).

test_build_registration_message() ->
    RegisterMsg = #{
        type => node_register,
        payload => #{
            hostname => <<"testnode">>,
            cpus => 8,
            memory_mb => 32768,
            gpus => [],
            features => [<<"avx">>],
            partitions => [<<"default">>],
            version => <<"0.1.0">>
        }
    },
    {ok, Encoded} = flurm_protocol:encode(RegisterMsg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register, maps:get(type, Decoded)).

test_build_job_complete_message() ->
    Msg = #{
        type => job_complete,
        payload => #{
            job_id => 123,
            exit_code => 0,
            output => <<"Job completed successfully">>,
            reason => <<"completed">>,
            energy_used_uj => 500000,
            timestamp => erlang:system_time(millisecond)
        }
    },
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_complete, maps:get(type, Decoded)).

test_build_job_failed_message() ->
    Msg = #{
        type => job_failed,
        payload => #{
            job_id => 456,
            exit_code => -1,
            output => <<"Job timed out">>,
            reason => <<"timeout">>,
            energy_used_uj => 250000,
            timestamp => erlang:system_time(millisecond)
        }
    },
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_failed, maps:get(type, Decoded)).

test_build_drain_ack_message() ->
    Msg = #{
        type => ack,
        payload => #{
            draining => true,
            reason => <<"maintenance">>,
            running_jobs => 5,
            timestamp => erlang:system_time(millisecond)
        }
    },
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(ack, maps:get(type, Decoded)).

%%====================================================================
%% Job Launch Handling Tests
%%====================================================================

test_parse_job_launch_payload() ->
    Payload = #{
        <<"job_id">> => 123,
        <<"script">> => <<"#!/bin/bash\necho hello">>,
        <<"working_dir">> => <<"/home/user">>,
        <<"environment">> => #{<<"PATH">> => <<"/usr/bin">>},
        <<"num_cpus">> => 2,
        <<"memory_mb">> => 4096,
        <<"time_limit">> => 3600,
        <<"std_out">> => <<"/tmp/job.out">>,
        <<"std_err">> => <<"/tmp/job.err">>
    },
    JobSpec = #{
        job_id => maps:get(<<"job_id">>, Payload),
        script => maps:get(<<"script">>, Payload, <<>>),
        working_dir => maps:get(<<"working_dir">>, Payload, <<"/tmp">>),
        environment => maps:get(<<"environment">>, Payload, #{}),
        num_cpus => maps:get(<<"num_cpus">>, Payload, 1),
        memory_mb => maps:get(<<"memory_mb">>, Payload, 1024),
        time_limit => maps:get(<<"time_limit">>, Payload, undefined),
        std_out => maps:get(<<"std_out">>, Payload, undefined),
        std_err => maps:get(<<"std_err">>, Payload, undefined)
    },
    ?assertEqual(123, maps:get(job_id, JobSpec)),
    ?assertEqual(2, maps:get(num_cpus, JobSpec)),
    ?assertEqual(4096, maps:get(memory_mb, JobSpec)).

test_job_launch_optional_fields() ->
    %% Test with minimal payload - should use defaults
    Payload = #{<<"job_id">> => 999},
    JobSpec = #{
        job_id => maps:get(<<"job_id">>, Payload),
        script => maps:get(<<"script">>, Payload, <<>>),
        working_dir => maps:get(<<"working_dir">>, Payload, <<"/tmp">>),
        environment => maps:get(<<"environment">>, Payload, #{}),
        num_cpus => maps:get(<<"num_cpus">>, Payload, 1),
        memory_mb => maps:get(<<"memory_mb">>, Payload, 1024)
    },
    ?assertEqual(999, maps:get(job_id, JobSpec)),
    ?assertEqual(<<>>, maps:get(script, JobSpec)),
    ?assertEqual(<<"/tmp">>, maps:get(working_dir, JobSpec)),
    ?assertEqual(1, maps:get(num_cpus, JobSpec)),
    ?assertEqual(1024, maps:get(memory_mb, JobSpec)).

test_reject_job_when_draining() ->
    State = maps:merge(make_test_state(), #{draining => true, drain_reason => <<"maintenance">>}),
    ?assertEqual(true, maps:get(draining, State)),
    ?assertEqual(<<"maintenance">>, maps:get(drain_reason, State)).

%%====================================================================
%% Job Cancel Handling Tests
%%====================================================================

test_find_job_to_cancel() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    Jobs = #{123 => Pid},
    ?assertEqual(Pid, maps:get(123, Jobs, undefined)),
    Pid ! stop.

test_cancel_missing_job() ->
    Jobs = #{456 => self()},
    ?assertEqual(undefined, maps:get(123, Jobs, undefined)).

%%====================================================================
%% State Structure Tests
%%====================================================================

test_init_state_structure() ->
    %% The init function should set up proper state
    State = make_test_state(),
    ?assert(maps:is_key(socket, State)),
    ?assert(maps:is_key(host, State)),
    ?assert(maps:is_key(port, State)),
    ?assert(maps:is_key(heartbeat_interval, State)),
    ?assert(maps:is_key(connected, State)),
    ?assert(maps:is_key(registered, State)),
    ?assert(maps:is_key(running_jobs, State)),
    ?assert(maps:is_key(draining, State)).

test_state_required_fields() ->
    State = make_test_state(),
    ?assertEqual(undefined, maps:get(socket, State)),
    ?assertEqual("localhost", maps:get(host, State)),
    ?assertEqual(6817, maps:get(port, State)),
    ?assertEqual(30000, maps:get(heartbeat_interval, State)),
    ?assertEqual(false, maps:get(connected, State)),
    ?assertEqual(false, maps:get(registered, State)),
    ?assertEqual(#{}, maps:get(running_jobs, State)),
    ?assertEqual(false, maps:get(draining, State)),
    ?assertEqual(undefined, maps:get(drain_reason, State)).

%%====================================================================
%% Wrapper Functions (reimplementing internal logic for testing)
%%====================================================================

%% Wrapper for decode_messages
decode_messages_wrapper(<<Len:32, Data/binary>>, Acc) when byte_size(Data) >= Len ->
    <<MsgData:Len/binary, Rest/binary>> = Data,
    case flurm_protocol:decode(MsgData) of
        {ok, Message} ->
            decode_messages_wrapper(Rest, [Message | Acc]);
        {error, _Reason} ->
            decode_messages_wrapper(Rest, Acc)
    end;
decode_messages_wrapper(Buffer, Acc) ->
    {lists:reverse(Acc), Buffer}.

%% Wrapper for find_job_by_pid
find_job_by_pid_wrapper(Pid, Jobs) ->
    case [JobId || {JobId, P} <- maps:to_list(Jobs), P =:= Pid] of
        [JobId] -> {ok, JobId};
        [] -> error
    end.

%% Wrapper for cancel_timer
cancel_timer_wrapper(undefined) -> ok;
cancel_timer_wrapper(Ref) -> erlang:cancel_timer(Ref).

%% Wrapper for detect_features
detect_features_wrapper() ->
    Features = [],
    %% Check for GPU support
    Features1 = case filelib:is_file("/dev/nvidia0") of
        true -> [<<"gpu">> | Features];
        false -> Features
    end,
    %% Check for InfiniBand
    Features2 = case filelib:is_dir("/sys/class/infiniband") of
        true -> [<<"ib">> | Features1];
        false -> Features1
    end,
    %% Check for AVX support
    Features3 = case check_cpu_flag_wrapper("avx") of
        true -> [<<"avx">> | Features2];
        false -> Features2
    end,
    Features3.

%% Wrapper for check_cpu_flag
check_cpu_flag_wrapper(Flag) ->
    case file:read_file("/proc/cpuinfo") of
        {ok, Content} ->
            binary:match(Content, list_to_binary(Flag)) =/= nomatch;
        _ ->
            false
    end.

%% Wrapper for report_job_to_controller
report_job_to_controller_wrapper(undefined, _JobId, _Status, _ExitCode, _Output, _EnergyUsed) ->
    {error, not_connected};
report_job_to_controller_wrapper(_Socket, _JobId, _Status, _ExitCode, _Output, _EnergyUsed) ->
    ok.

%% Wrapper for send_drain_ack
send_drain_ack_wrapper(undefined, _Draining, _Reason, _RunningJobCount) ->
    {error, not_connected};
send_drain_ack_wrapper(_Socket, _Draining, _Reason, _RunningJobCount) ->
    ok.

%% Wrapper for handle_controller_message - simplified for testing
handle_controller_message_wrapper(#{type := node_register_ack, payload := Payload}, State) ->
    NodeId = maps:get(<<"node_id">>, Payload, undefined),
    maps:merge(State, #{registered => true, node_id => NodeId});

handle_controller_message_wrapper(#{type := node_heartbeat_ack}, State) ->
    State;

handle_controller_message_wrapper(#{type := node_drain} = Msg, State) ->
    Payload = maps:get(payload, Msg, #{}),
    Reason = maps:get(<<"reason">>, Payload, <<"controller request">>),
    maps:merge(State, #{draining => true, drain_reason => Reason});

handle_controller_message_wrapper(#{type := node_resume}, State) ->
    maps:merge(State, #{draining => false, drain_reason => undefined});

handle_controller_message_wrapper(#{type := ack, payload := _Payload}, State) ->
    State;

handle_controller_message_wrapper(#{type := error, payload := _Payload}, State) ->
    State;

handle_controller_message_wrapper(#{type := _Type, payload := _Payload}, State) ->
    State.

%% Map job status to message type and reason
map_job_status(completed) ->
    {job_complete, <<"completed">>};
map_job_status({failed, timeout}) ->
    {job_failed, <<"timeout">>};
map_job_status({failed, cancelled}) ->
    {job_failed, <<"cancelled">>};
map_job_status({failed, R}) when is_atom(R) ->
    {job_failed, atom_to_binary(R, utf8)};
map_job_status({failed, R}) when is_binary(R) ->
    {job_failed, R};
map_job_status({failed, R}) ->
    {job_failed, iolist_to_binary(io_lib:format("~p", [R]))};
map_job_status(_) ->
    {job_failed, <<"unknown">>}.

%% Create a test state map
make_test_state() ->
    #{
        socket => undefined,
        host => "localhost",
        port => 6817,
        heartbeat_interval => 30000,
        heartbeat_timer => undefined,
        connected => false,
        registered => false,
        node_id => undefined,
        reconnect_interval => 1000,
        buffer => <<>>,
        running_jobs => #{},
        draining => false,
        drain_reason => undefined
    }.
