%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_controller_protocol.
%%%
%%% Focuses on branches in the socket loop and message handlers that are
%%% difficult to hit from higher-level suites.
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol_loop_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% handle_message/1 branch coverage
%%====================================================================

handle_message_branches_test_() ->
    {setup,
     fun setup_message_mocks/0,
     fun cleanup_message_mocks/1,
     [
      fun submit_ok_branch_test/0,
      fun submit_error_branch_test/0,
      fun cancel_ok_branch_test/0,
      fun cancel_error_branch_test/0,
      fun status_ok_branch_test/0,
      fun status_not_found_branch_test/0,
      fun node_register_ok_branch_test/0,
      fun node_register_error_branch_test/0,
      fun node_heartbeat_branch_test/0,
      fun partition_create_ok_branch_test/0,
      fun partition_create_error_branch_test/0
     ]}.

setup_message_mocks() ->
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_partition_manager),
    meck:new(flurm_job_manager, [non_strict]),
    meck:new(flurm_node_manager_server, [non_strict]),
    meck:new(flurm_partition_manager, [non_strict]),
    %% Safe defaults avoid function_clause leaks if any other test hits these mocks.
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, default_submit} end),
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> {error, default_cancel} end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_node_manager_server, register_node, fun(_) -> {error, default_register} end),
    meck:expect(flurm_node_manager_server, heartbeat, fun(_) -> ok end),
    meck:expect(flurm_partition_manager, create_partition, fun(_) -> {error, default_partition} end),
    ok.

cleanup_message_mocks(_) ->
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_partition_manager),
    ok.

submit_ok_branch_test() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 101} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_submit, payload => #{name => <<"t">>}}),
    ?assertEqual(#{type => ack, payload => #{job_id => 101}}, Resp).

submit_error_branch_test() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, quota_exceeded} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_submit, payload => #{name => <<"t">>}}),
    ?assertEqual(#{type => error, payload => #{reason => quota_exceeded}}, Resp).

cancel_ok_branch_test() ->
    meck:expect(flurm_job_manager, cancel_job, fun(202) -> ok end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_cancel, payload => #{job_id => 202}}),
    ?assertEqual(#{type => ack, payload => #{status => <<"cancelled">>}}, Resp).

cancel_error_branch_test() ->
    meck:expect(flurm_job_manager, cancel_job, fun(203) -> {error, busy} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_cancel, payload => #{job_id => 203}}),
    ?assertEqual(#{type => error, payload => #{reason => busy}}, Resp).

status_ok_branch_test() ->
    Job = {job, 77, <<"job-77">>, ignored, <<"main">>, running},
    meck:expect(flurm_job_manager, get_job, fun(77) -> {ok, Job} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_status, payload => #{job_id => 77}}),
    ?assertEqual(
       #{type => ack,
         payload => #{job_id => 77,
                      name => <<"job-77">>,
                      state => running,
                      partition => <<"main">>}},
       Resp).

status_not_found_branch_test() ->
    meck:expect(flurm_job_manager, get_job, fun(78) -> {error, not_found} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => job_status, payload => #{job_id => 78}}),
    ?assertEqual(
       #{type => error, payload => #{reason => <<"not_found">>}},
       Resp).

node_register_ok_branch_test() ->
    meck:expect(flurm_node_manager_server, register_node, fun(_) -> ok end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => node_register, payload => #{hostname => <<"n1">>}}),
    ?assertEqual(#{type => ack, payload => #{status => <<"registered">>}}, Resp).

node_register_error_branch_test() ->
    meck:expect(flurm_node_manager_server, register_node, fun(_) -> {error, duplicate} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => node_register, payload => #{hostname => <<"n2">>}}),
    ?assertEqual(#{type => error, payload => #{reason => duplicate}}, Resp).

node_heartbeat_branch_test() ->
    meck:expect(flurm_node_manager_server, heartbeat, fun(_) -> ok end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => node_heartbeat, payload => #{hostname => <<"n3">>}}),
    ?assertEqual(#{type => ack, payload => #{status => <<"ok">>}}, Resp).

partition_create_ok_branch_test() ->
    meck:expect(flurm_partition_manager, create_partition, fun(_) -> ok end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => partition_create, payload => #{name => <<"p1">>}}),
    ?assertEqual(#{type => ack, payload => #{status => <<"created">>}}, Resp).

partition_create_error_branch_test() ->
    meck:expect(flurm_partition_manager, create_partition, fun(_) -> {error, invalid} end),
    Resp = flurm_controller_protocol:handle_message(
             #{type => partition_create, payload => #{name => <<"p2">>}}),
    ?assertEqual(#{type => error, payload => #{reason => invalid}}, Resp).

%%====================================================================
%% process_buffer/1 decode error branch
%%====================================================================

process_buffer_decode_error_branch_test_() ->
    fun() ->
        %% Valid framing with payload that fails protocol decode.
        Buffer = <<9:16, 4:32, 1, 2, 3, 4>>,
        ?assertEqual({incomplete, Buffer}, flurm_controller_protocol:process_buffer(Buffer))
    end.

process_buffer_incomplete_payload_branch_test() ->
    %% byte_size(Rest) < PayloadSize -> false branch at line 78
    Buffer = <<1:16, 8:32, 1, 2, 3>>,
    ?assertEqual({incomplete, Buffer}, flurm_controller_protocol:process_buffer(Buffer)).

%%====================================================================
%% init/3 loop branch coverage
%%====================================================================

loop_branches_test_() ->
    {setup,
     fun setup_loop_mocks/0,
     fun cleanup_loop_mocks/1,
     [
      fun loop_ok_send_branch_test/0,
      fun loop_encode_error_branch_test/0,
      fun loop_incomplete_buffer_branch_test/0,
      fun loop_timeout_branch_test/0,
      fun loop_generic_error_branch_test/0
     ]}.

setup_loop_mocks() ->
    catch meck:unload(flurm_job_manager),
    flurm_controller_protocol_test_transport:reset(),
    ok.

cleanup_loop_mocks(_) ->
    catch meck:unload(flurm_job_manager),
    flurm_controller_protocol_test_transport:reset(),
    ok.

loop_ok_send_branch_test() ->
    {ok, Data} = flurm_protocol:encode(#{type => ack, payload => #{}}),
    flurm_controller_protocol_test_transport:set_sequence([{ok, Data}, {error, closed}]),
    ?assertEqual(ok, run_loop()),
    Calls = flurm_controller_protocol_test_transport:calls(),
    ?assertEqual(true, maps:get(sent_count, Calls) >= 1).

loop_encode_error_branch_test() ->
    catch meck:unload(flurm_job_manager),
    meck:new(flurm_job_manager, [non_strict]),
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, {bad, reason}} end),
    {ok, Data} = flurm_protocol:encode(
                   #{type => job_submit, payload => #{name => <<"bad-encode">>}}),
    flurm_controller_protocol_test_transport:set_sequence([{ok, Data}, {error, closed}]),
    ?assertEqual(ok, run_loop()),
    Calls = flurm_controller_protocol_test_transport:calls(),
    ?assertEqual(0, maps:get(sent_count, Calls)),
    catch meck:unload(flurm_job_manager).

loop_incomplete_buffer_branch_test() ->
    Partial = <<1, 2, 3>>,
    flurm_controller_protocol_test_transport:set_sequence([{ok, Partial}, {error, closed}]),
    ?assertEqual(ok, run_loop()).

loop_timeout_branch_test() ->
    flurm_controller_protocol_test_transport:set_sequence([{error, timeout}]),
    ?assertEqual(ok, run_loop()),
    Calls = flurm_controller_protocol_test_transport:calls(),
    ?assertEqual(true, maps:get(closed, Calls)).

loop_generic_error_branch_test() ->
    flurm_controller_protocol_test_transport:set_sequence([{error, econnreset}]),
    ?assertEqual(ok, run_loop()),
    Calls = flurm_controller_protocol_test_transport:calls(),
    ?assertEqual(true, maps:get(closed, Calls)).

run_loop() ->
    flurm_controller_protocol:loop(
      make_ref(),
      flurm_controller_protocol_test_transport,
      <<>>).

%%====================================================================
%% start_link/init branch coverage
%%====================================================================

start_link_init_test_() ->
    {setup,
     fun() ->
         catch meck:unload(ranch),
         meck:new(ranch, [non_strict]),
         flurm_controller_protocol_test_transport:reset(),
         ok
     end,
     fun(_) ->
         catch meck:unload(ranch),
         flurm_controller_protocol_test_transport:reset(),
         ok
     end,
     [
      fun init_calls_handshake_and_loops_test/0,
      fun start_link_spawns_process_test/0,
      fun process_buffer_incomplete_payload_branch_test/0
     ]}.

init_calls_handshake_and_loops_test() ->
    flurm_controller_protocol_test_transport:set_sequence([{error, closed}]),
    meck:expect(ranch, handshake, fun(test_ref_init) -> {ok, make_ref()} end),
    ?assertEqual(
       ok,
       flurm_controller_protocol:init(
         test_ref_init,
         flurm_controller_protocol_test_transport,
         #{})).

start_link_spawns_process_test() ->
    flurm_controller_protocol_test_transport:set_sequence([{error, closed}]),
    meck:expect(ranch, handshake, fun(test_ref_start) -> {ok, make_ref()} end),
    {ok, Pid} = flurm_controller_protocol:start_link(
                  test_ref_start,
                  flurm_controller_protocol_test_transport,
                  #{}),
    ?assert(is_pid(Pid)),
    ?assertEqual(ok, flurm_test_utils:wait_for_death(Pid)),
    ?assertEqual(false, is_process_alive(Pid)).
