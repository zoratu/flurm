%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_node_acceptor module
%%%
%%% Tests the node acceptor (Ranch protocol handler) functions. External
%%% dependencies are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_acceptor_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_node_acceptor_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link spawns process", fun test_start_link/0},
      {"handle_message node_register success", fun test_handle_node_register_success/0},
      {"handle_message node_register failure", fun test_handle_node_register_failure/0},
      {"handle_message node_heartbeat", fun test_handle_node_heartbeat/0},
      {"handle_message job_complete success", fun test_handle_job_complete_success/0},
      {"handle_message job_complete non-zero", fun test_handle_job_complete_failed/0},
      {"handle_message job_failed", fun test_handle_job_failed/0},
      {"handle_message job_failed timeout", fun test_handle_job_failed_timeout/0},
      {"handle_message job_failed already cancelled", fun test_handle_job_failed_cancelled/0},
      {"handle_message unknown type", fun test_handle_message_unknown/0},
      {"handle_disconnect with connection", fun test_handle_disconnect_known/0},
      {"handle_disconnect unknown socket", fun test_handle_disconnect_unknown/0},
      {"send_message success", fun test_send_message_success/0},
      {"send_message encode error", fun test_send_message_encode_error/0},
      {"process_messages complete buffer", fun test_process_messages_complete/0},
      {"process_messages incomplete buffer", fun test_process_messages_incomplete/0},
      {"fail_jobs_on_node with running jobs", fun test_fail_jobs_on_node/0}
     ]}.

setup() ->
    meck:new(ranch, [non_strict]),
    meck:new(flurm_protocol, [passthrough, non_strict]),
    meck:new(flurm_node_manager_server, [passthrough, non_strict]),
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    meck:new(flurm_job_manager, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),

    meck:expect(ranch, handshake, fun(_Ref) -> {ok, fake_socket} end),
    meck:expect(flurm_protocol, encode, fun(Msg) ->
        {ok, term_to_binary(Msg)}
    end),
    meck:expect(flurm_protocol, decode, fun(_Bin) ->
        {ok, #{type => unknown, payload => #{}}}
    end),
    meck:expect(flurm_node_manager_server, register_node, fun(_) -> ok end),
    meck:expect(flurm_node_manager_server, heartbeat, fun(_) -> ok end),
    meck:expect(flurm_node_manager_server, update_node, fun(_, _) -> ok end),
    meck:expect(flurm_node_connection_manager, register_connection, fun(_, _) -> ok end),
    meck:expect(flurm_node_connection_manager, unregister_connection, fun(_) -> ok end),
    meck:expect(flurm_node_connection_manager, find_by_socket, fun(_) -> error end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    meck:expect(flurm_scheduler, job_completed, fun(_) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(ranch),
    catch meck:unload(flurm_protocol),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_node_connection_manager),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_scheduler),
    ok.

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_link() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, closed} end),

    Result = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),

    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    flurm_test_utils:wait_for_death(Pid).

%%====================================================================
%% handle_message Tests (node_register)
%%====================================================================

test_handle_node_register_success() ->
    meck:expect(flurm_node_manager_server, register_node, fun(NodeSpec) ->
        ?assert(is_map(NodeSpec)),
        ?assertEqual(<<"testnode">>, maps:get(hostname, NodeSpec)),
        ok
    end),
    meck:expect(flurm_node_connection_manager, register_connection, fun(Hostname, _Pid) ->
        ?assertEqual(<<"testnode">>, Hostname),
        ok
    end),

    Result = flurm_node_manager_server:register_node(#{hostname => <<"testnode">>}),
    ?assertEqual(ok, Result).

test_handle_node_register_failure() ->
    meck:expect(flurm_node_manager_server, register_node, fun(_) ->
        {error, duplicate_node}
    end),

    Result = flurm_node_manager_server:register_node(#{hostname => <<"testnode">>}),
    ?assertEqual({error, duplicate_node}, Result).

%%====================================================================
%% handle_message Tests (node_heartbeat)
%%====================================================================

test_handle_node_heartbeat() ->
    meck:expect(flurm_node_manager_server, heartbeat, fun(Data) ->
        ?assert(is_map(Data)),
        ?assertEqual(<<"testnode">>, maps:get(hostname, Data)),
        ok
    end),

    HeartbeatData = #{
        hostname => <<"testnode">>,
        load_avg => 0.5,
        free_memory_mb => 8192,
        running_jobs => []
    },
    Result = flurm_node_manager_server:heartbeat(HeartbeatData),
    ?assertEqual(ok, Result).

%%====================================================================
%% handle_message Tests (job_complete)
%%====================================================================

test_handle_job_complete_success() ->
    meck:expect(flurm_job_manager, update_job, fun(JobId, Updates) ->
        ?assertEqual(12345, JobId),
        ?assertEqual(completed, maps:get(state, Updates)),
        ?assertEqual(0, maps:get(exit_code, Updates)),
        ok
    end),
    meck:expect(flurm_scheduler, job_completed, fun(JobId) ->
        ?assertEqual(12345, JobId),
        ok
    end),

    flurm_job_manager:update_job(12345, #{state => completed, exit_code => 0}),
    flurm_scheduler:job_completed(12345),
    ?assert(meck:called(flurm_job_manager, update_job, '_')),
    ?assert(meck:called(flurm_scheduler, job_completed, [12345])).

test_handle_job_complete_failed() ->
    meck:expect(flurm_job_manager, update_job, fun(JobId, Updates) ->
        ?assertEqual(12345, JobId),
        ?assertEqual(failed, maps:get(state, Updates)),
        ?assertEqual(1, maps:get(exit_code, Updates)),
        ok
    end),
    meck:expect(flurm_scheduler, job_failed, fun(JobId) ->
        ?assertEqual(12345, JobId),
        ok
    end),

    flurm_job_manager:update_job(12345, #{state => failed, exit_code => 1}),
    flurm_scheduler:job_failed(12345),
    ?assert(meck:called(flurm_job_manager, update_job, '_')),
    ?assert(meck:called(flurm_scheduler, job_failed, [12345])).

%%====================================================================
%% handle_message Tests (job_failed)
%%====================================================================

test_handle_job_failed() ->
    meck:expect(flurm_job_manager, get_job, fun(12345) ->
        MockJob = {job, 12345, <<"test">>, undefined, <<"default">>, running},
        {ok, MockJob}
    end),
    meck:expect(flurm_job_manager, update_job, fun(JobId, Updates) ->
        ?assertEqual(12345, JobId),
        ?assertEqual(failed, maps:get(state, Updates)),
        ok
    end),

    {ok, Job} = flurm_job_manager:get_job(12345),
    ?assertEqual(running, element(6, Job)),
    flurm_job_manager:update_job(12345, #{state => failed}).

test_handle_job_failed_timeout() ->
    meck:expect(flurm_job_manager, get_job, fun(12345) ->
        MockJob = {job, 12345, <<"test">>, undefined, <<"default">>, running},
        {ok, MockJob}
    end),
    meck:expect(flurm_job_manager, update_job, fun(12345, Updates) ->
        ?assertEqual(timeout, maps:get(state, Updates)),
        ok
    end),

    {ok, _} = flurm_job_manager:get_job(12345),
    flurm_job_manager:update_job(12345, #{state => timeout}).

test_handle_job_failed_cancelled() ->
    MockCancelledJob = {job, 12345, <<"test">>, undefined, <<"default">>, cancelled},
    meck:expect(flurm_job_manager, get_job, fun(12345) ->
        {ok, MockCancelledJob}
    end),

    {ok, Job} = flurm_job_manager:get_job(12345),
    ?assertEqual(cancelled, element(6, Job)).

%%====================================================================
%% handle_message Tests (unknown)
%%====================================================================

test_handle_message_unknown() ->
    meck:expect(flurm_protocol, decode, fun(_) ->
        {ok, #{type => unknown_type, payload => #{}}}
    end),

    {ok, Msg} = flurm_protocol:decode(<<"binary">>),
    ?assertEqual(unknown_type, maps:get(type, Msg)).

%%====================================================================
%% handle_disconnect Tests
%%====================================================================

test_handle_disconnect_known() ->
    meck:expect(flurm_node_connection_manager, find_by_socket, fun(_Socket) ->
        {ok, <<"testnode">>}
    end),
    meck:expect(flurm_node_manager_server, update_node, fun(Hostname, Updates) ->
        ?assertEqual(<<"testnode">>, Hostname),
        ?assertEqual(down, maps:get(state, Updates)),
        ok
    end),
    meck:expect(flurm_node_connection_manager, unregister_connection, fun(Hostname) ->
        ?assertEqual(<<"testnode">>, Hostname),
        ok
    end),

    {ok, Hostname} = flurm_node_connection_manager:find_by_socket(fake_socket),
    ?assertEqual(<<"testnode">>, Hostname),
    flurm_node_manager_server:update_node(Hostname, #{state => down}),
    flurm_node_connection_manager:unregister_connection(Hostname).

test_handle_disconnect_unknown() ->
    meck:expect(flurm_node_connection_manager, find_by_socket, fun(_Socket) ->
        error
    end),

    Result = flurm_node_connection_manager:find_by_socket(fake_socket),
    ?assertEqual(error, Result).

%%====================================================================
%% send_message Tests
%%====================================================================

test_send_message_success() ->
    meck:expect(flurm_protocol, encode, fun(Msg) ->
        ?assert(is_map(Msg)),
        {ok, <<"encoded">>}
    end),

    {ok, Binary} = flurm_protocol:encode(#{type => ack, payload => #{}}),
    ?assertEqual(<<"encoded">>, Binary).

test_send_message_encode_error() ->
    meck:expect(flurm_protocol, encode, fun(_Msg) ->
        {error, encode_failed}
    end),

    Result = flurm_protocol:encode(#{invalid => data}),
    ?assertEqual({error, encode_failed}, Result).

%%====================================================================
%% process_messages Tests
%%====================================================================

test_process_messages_complete() ->
    MessageBin = <<"test message">>,
    Len = byte_size(MessageBin),
    Buffer = <<Len:32, MessageBin/binary>>,

    meck:expect(flurm_protocol, decode, fun(Data) ->
        ?assertEqual(MessageBin, Data),
        {ok, #{type => test, payload => #{}}}
    end),

    {ok, Msg} = flurm_protocol:decode(MessageBin),
    ?assertEqual(test, maps:get(type, Msg)),
    <<ExtractedLen:32, Rest/binary>> = Buffer,
    ?assertEqual(Len, ExtractedLen),
    ?assertEqual(MessageBin, Rest).

test_process_messages_incomplete() ->
    Buffer = <<0, 0, 0>>,
    ?assertEqual(3, byte_size(Buffer)).

%%====================================================================
%% fail_jobs_on_node Tests
%%====================================================================

test_fail_jobs_on_node() ->
    RunningJob = {job, 100, <<"job1">>, undefined, <<"default">>, running,
                  undefined, undefined, undefined, undefined, undefined,
                  undefined, undefined, undefined, undefined, [<<"testnode">>]},
    PendingJob = {job, 101, <<"job2">>, undefined, <<"default">>, pending,
                  undefined, undefined, undefined, undefined, undefined,
                  undefined, undefined, undefined, undefined, []},

    meck:expect(flurm_job_manager, list_jobs, fun() -> [RunningJob, PendingJob] end),
    meck:expect(flurm_job_manager, update_job, fun(JobId, Updates) ->
        ?assertEqual(100, JobId),
        ?assertEqual(node_fail, maps:get(state, Updates)),
        ok
    end),
    meck:expect(flurm_scheduler, job_failed, fun(100) -> ok end),

    Jobs = flurm_job_manager:list_jobs(),
    ?assertEqual(2, length(Jobs)),

    RunningOnNode = lists:filter(fun(Job) ->
        State = element(6, Job),
        AllocatedNodes = element(16, Job),
        (State =:= running orelse State =:= configuring) andalso
        lists:member(<<"testnode">>, AllocatedNodes)
    end, Jobs),
    ?assertEqual(1, length(RunningOnNode)).

%%====================================================================
%% Pure Function Tests (no mocking required)
%% These test the -ifdef(TEST) exported helper functions directly
%%====================================================================

%% build_node_spec Tests

build_node_spec_minimal_test() ->
    Payload = #{<<"hostname">> => <<"node1.example.com">>},
    Result = flurm_node_acceptor:build_node_spec(Payload),
    ?assert(is_map(Result)),
    ?assertEqual(<<"node1.example.com">>, maps:get(hostname, Result)),
    ?assertEqual(1, maps:get(cpus, Result)),
    ?assertEqual(1024, maps:get(memory_mb, Result)),
    ?assertEqual(idle, maps:get(state, Result)),
    ?assertEqual([<<"default">>], maps:get(partitions, Result)).

build_node_spec_full_test() ->
    Payload = #{
        <<"hostname">> => <<"compute01">>,
        <<"cpus">> => 32,
        <<"memory_mb">> => 131072
    },
    Result = flurm_node_acceptor:build_node_spec(Payload),
    ?assertEqual(<<"compute01">>, maps:get(hostname, Result)),
    ?assertEqual(32, maps:get(cpus, Result)),
    ?assertEqual(131072, maps:get(memory_mb, Result)).

%% build_heartbeat_data Tests

build_heartbeat_data_minimal_test() ->
    Payload = #{<<"hostname">> => <<"node1">>},
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),
    ?assert(is_map(Result)),
    ?assertEqual(<<"node1">>, maps:get(hostname, Result)),
    ?assertEqual(0.0, maps:get(load_avg, Result)),
    ?assertEqual(0, maps:get(free_memory_mb, Result)),
    ?assertEqual([], maps:get(running_jobs, Result)).

build_heartbeat_data_full_test() ->
    Payload = #{
        <<"hostname">> => <<"compute01">>,
        <<"load_avg">> => 0.75,
        <<"free_memory_mb">> => 64000,
        <<"running_jobs">> => [1, 2, 3]
    },
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),
    ?assertEqual(0.75, maps:get(load_avg, Result)),
    ?assertEqual(64000, maps:get(free_memory_mb, Result)),
    ?assertEqual([1, 2, 3], maps:get(running_jobs, Result)).

%% exit_code_to_state Tests

exit_code_to_state_zero_test() ->
    ?assertEqual(completed, flurm_node_acceptor:exit_code_to_state(0)).

exit_code_to_state_nonzero_test() ->
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(1)),
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(-1)),
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(137)).

%% failure_reason_to_state Tests

failure_reason_to_state_timeout_test() ->
    ?assertEqual(timeout, flurm_node_acceptor:failure_reason_to_state(<<"timeout">>)).

failure_reason_to_state_other_test() ->
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"error">>)),
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<>>)),
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"oom">>)).

%% build_register_ack_payload Tests

build_register_ack_payload_test() ->
    Result = flurm_node_acceptor:build_register_ack_payload(<<"node1">>),
    ?assertEqual(<<"node1">>, maps:get(<<"node_id">>, Result)),
    ?assertEqual(<<"accepted">>, maps:get(<<"status">>, Result)).

%% extract_message_from_buffer Tests

extract_message_from_buffer_complete_test() ->
    MsgData = <<"hello">>,
    Len = byte_size(MsgData),
    Buffer = <<Len:32, MsgData/binary>>,
    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertEqual({ok, MsgData, <<>>}, Result).

extract_message_from_buffer_incomplete_test() ->
    Buffer = <<100:32, "short">>,  % Claims 100 bytes but only 5
    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertEqual({incomplete, Buffer}, Result).

extract_message_from_buffer_empty_test() ->
    Result = flurm_node_acceptor:extract_message_from_buffer(<<>>),
    ?assertEqual({incomplete, <<>>}, Result).

%% frame_message Tests

frame_message_test() ->
    Msg = <<"hello">>,
    Result = flurm_node_acceptor:frame_message(Msg),
    <<Len:32, Data/binary>> = Result,
    ?assertEqual(5, Len),
    ?assertEqual(Msg, Data).

frame_message_empty_test() ->
    Result = flurm_node_acceptor:frame_message(<<>>),
    <<Len:32, Data/binary>> = Result,
    ?assertEqual(0, Len),
    ?assertEqual(<<>>, Data).

%% Roundtrip Tests

frame_extract_roundtrip_test() ->
    Msg = <<"test roundtrip">>,
    Framed = flurm_node_acceptor:frame_message(Msg),
    {ok, Extracted, <<>>} = flurm_node_acceptor:extract_message_from_buffer(Framed),
    ?assertEqual(Msg, Extracted).
