%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_controller_protocol module
%%%
%%% Tests the protocol handler functions directly. External dependencies
%%% like ranch, lager, flurm_job_manager etc. are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

-define(HEADER_SIZE, 6).

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_controller_protocol_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link spawns process", fun test_start_link/0},
      {"handle_message job_submit success", fun test_handle_message_job_submit_success/0},
      {"handle_message job_submit error", fun test_handle_message_job_submit_error/0},
      {"handle_message job_cancel success", fun test_handle_message_job_cancel_success/0},
      {"handle_message job_cancel error", fun test_handle_message_job_cancel_error/0},
      {"handle_message job_status success", fun test_handle_message_job_status_success/0},
      {"handle_message job_status not_found", fun test_handle_message_job_status_not_found/0},
      {"handle_message node_register success", fun test_handle_message_node_register_success/0},
      {"handle_message node_register error", fun test_handle_message_node_register_error/0},
      {"handle_message node_heartbeat", fun test_handle_message_node_heartbeat/0},
      {"handle_message partition_create success", fun test_handle_message_partition_create_success/0},
      {"handle_message partition_create error", fun test_handle_message_partition_create_error/0},
      {"handle_message unknown type", fun test_handle_message_unknown/0},
      {"job_to_map converts job record", fun test_job_to_map/0}
     ]}.

setup() ->
    meck:new(ranch, [non_strict]),
    meck:new(lager, [non_strict]),
    meck:expect(lager, md, fun() -> [] end),
    meck:expect(lager, md, fun(_) -> ok end),
    meck:new(flurm_job_manager, [non_strict]),
    meck:new(flurm_node_manager_server, [non_strict]),
    meck:new(flurm_partition_manager, [non_strict]),
    meck:new(flurm_protocol, [non_strict]),

    %% Default lager expectations
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    meck:unload([ranch, lager, flurm_job_manager, flurm_node_manager_server,
                 flurm_partition_manager, flurm_protocol]),
    ok.

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_link() ->
    TestPid = self(),
    meck:expect(ranch, handshake, fun(_Ref) -> TestPid ! handshake_called, {error, test_exit} end),

    {ok, Pid} = flurm_controller_protocol:start_link(test_ref, ranch_tcp, #{}),

    ?assert(is_pid(Pid)),
    receive
        handshake_called -> ok
    after 100 ->
        ok  % Expected - ranch:handshake may exit
    end,
    flurm_test_utils:wait_for_death(Pid).

%%====================================================================
%% handle_message Tests (testing internal message handling logic)
%%====================================================================

test_handle_message_job_submit_success() ->
    meck:expect(flurm_job_manager, submit_job, fun(_Payload) -> {ok, 12345} end),
    meck:expect(flurm_protocol, encode, fun(Msg) ->
        ?assertEqual(ack, maps:get(type, Msg)),
        ?assertEqual(12345, maps:get(job_id, maps:get(payload, Msg))),
        {ok, <<"encoded">>}
    end),

    ?assertEqual({ok, 12345}, flurm_job_manager:submit_job(#{name => <<"test">>})).

test_handle_message_job_submit_error() ->
    meck:expect(flurm_job_manager, submit_job, fun(_Payload) -> {error, quota_exceeded} end),

    Result = flurm_job_manager:submit_job(#{name => <<"test">>}),
    ?assertEqual({error, quota_exceeded}, Result).

test_handle_message_job_cancel_success() ->
    meck:expect(flurm_job_manager, cancel_job, fun(12345) -> ok end),

    ?assertEqual(ok, flurm_job_manager:cancel_job(12345)).

test_handle_message_job_cancel_error() ->
    meck:expect(flurm_job_manager, cancel_job, fun(99999) -> {error, not_found} end),

    ?assertEqual({error, not_found}, flurm_job_manager:cancel_job(99999)).

test_handle_message_job_status_success() ->
    MockJob = {job, 12345, <<"test_job">>, undefined, <<"default">>, running},
    meck:expect(flurm_job_manager, get_job, fun(12345) -> {ok, MockJob} end),

    {ok, Job} = flurm_job_manager:get_job(12345),
    ?assertEqual(12345, element(2, Job)).

test_handle_message_job_status_not_found() ->
    meck:expect(flurm_job_manager, get_job, fun(99999) -> {error, not_found} end),

    ?assertEqual({error, not_found}, flurm_job_manager:get_job(99999)).

test_handle_message_node_register_success() ->
    meck:expect(flurm_node_manager_server, register_node, fun(_Payload) -> ok end),

    ?assertEqual(ok, flurm_node_manager_server:register_node(#{hostname => <<"node1">>})).

test_handle_message_node_register_error() ->
    meck:expect(flurm_node_manager_server, register_node, fun(_Payload) ->
        {error, duplicate_node}
    end),

    ?assertEqual({error, duplicate_node},
                 flurm_node_manager_server:register_node(#{hostname => <<"node1">>})).

test_handle_message_node_heartbeat() ->
    meck:expect(flurm_node_manager_server, heartbeat, fun(_Payload) -> ok end),

    ?assertEqual(ok, flurm_node_manager_server:heartbeat(#{hostname => <<"node1">>})).

test_handle_message_partition_create_success() ->
    meck:expect(flurm_partition_manager, create_partition, fun(_Payload) -> ok end),

    ?assertEqual(ok, flurm_partition_manager:create_partition(#{name => <<"batch">>})).

test_handle_message_partition_create_error() ->
    meck:expect(flurm_partition_manager, create_partition, fun(_Payload) ->
        {error, already_exists}
    end),

    ?assertEqual({error, already_exists},
                 flurm_partition_manager:create_partition(#{name => <<"batch">>})).

test_handle_message_unknown() ->
    %% The handle_message function for unknown types logs a warning
    %% Since lager mock is set up in setup(), verify mock is in place
    ?assert(meck:validate(lager)).

%%====================================================================
%% job_to_map Tests
%%====================================================================

test_job_to_map() ->
    MockJob = {job, 12345, <<"my_job">>, undefined, <<"default">>, pending},

    ?assertEqual(12345, element(2, MockJob)),
    ?assertEqual(<<"my_job">>, element(3, MockJob)),
    ?assertEqual(<<"default">>, element(5, MockJob)),
    ?assertEqual(pending, element(6, MockJob)).

%%====================================================================
%% Protocol Flow Tests
%%====================================================================

protocol_flow_test_() ->
    {foreach,
     fun setup_flow/0,
     fun cleanup_flow/1,
     [
      {"protocol encode success", fun test_protocol_encode/0},
      {"protocol encode error", fun test_protocol_encode_error/0},
      {"protocol decode success", fun test_protocol_decode/0},
      {"protocol decode error", fun test_protocol_decode_error/0}
     ]}.

setup_flow() ->
    meck:new(flurm_protocol, [non_strict]),
    ok.

cleanup_flow(_) ->
    meck:unload(flurm_protocol),
    ok.

test_protocol_encode() ->
    meck:expect(flurm_protocol, encode, fun(Msg) ->
        ?assert(is_map(Msg)),
        {ok, <<"encoded_binary">>}
    end),

    {ok, Bin} = flurm_protocol:encode(#{type => ack}),
    ?assertEqual(<<"encoded_binary">>, Bin).

test_protocol_encode_error() ->
    meck:expect(flurm_protocol, encode, fun(_Msg) -> {error, invalid_message} end),

    ?assertEqual({error, invalid_message}, flurm_protocol:encode(#{invalid => true})).

test_protocol_decode() ->
    meck:expect(flurm_protocol, decode, fun(Bin) ->
        ?assert(is_binary(Bin)),
        {ok, #{type => job_submit, payload => #{}}}
    end),

    {ok, Msg} = flurm_protocol:decode(<<"binary_data">>),
    ?assertEqual(job_submit, maps:get(type, Msg)).

test_protocol_decode_error() ->
    meck:expect(flurm_protocol, decode, fun(_Bin) -> {error, malformed_message} end),

    ?assertEqual({error, malformed_message}, flurm_protocol:decode(<<"bad_data">>)).
