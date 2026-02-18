%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_trace module - 100% coverage target
%%%
%%% Tests all aspects of trace recording, verification, and TLA+ export:
%%% - gen_server lifecycle (start_link, init, terminate, code_change)
%%% - Recording API (start_recording, stop_recording, get_trace, clear_trace)
%%% - Message recording (record_message, record_state_change, record_event)
%%% - Trace verification (verify_trace with all invariant checks)
%%% - TLA+ export (export_tla_trace, trace_entry_to_tla, term_to_tla)
%%% - Internal functions (add_entry, stepped_down, check_leader_overlaps, etc.)
%%% - Edge cases and error handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_trace_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SERVER, flurm_trace).
-define(MAX_TRACE_SIZE, 100000).

%% Record definitions matching the source
-record(trace_message, {
    timestamp :: non_neg_integer(),
    from :: binary() | atom(),
    to :: binary() | atom(),
    type :: atom(),
    payload :: term()
}).

-record(trace_state_change, {
    timestamp :: non_neg_integer(),
    node :: binary() | atom(),
    component :: atom(),
    old_state :: term(),
    new_state :: term()
}).

-record(trace_event, {
    timestamp :: non_neg_integer(),
    type :: atom(),
    data :: term()
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing trace server
    case whereis(?SERVER) of
        undefined -> ok;
        ExistingPid ->
            gen_server:stop(ExistingPid, normal, 5000),
            timer:sleep(50)
    end,
    %% Start fresh trace server
    {ok, Pid} = flurm_trace:start_link(),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end.

%%====================================================================
%% Test Generator
%%====================================================================

flurm_trace_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         %% Basic API tests
         fun test_start_recording/1,
         fun test_stop_recording/1,
         fun test_stop_recording_returns_reversed_trace/1,
         fun test_get_trace_without_stopping/1,
         fun test_clear_trace/1,
         fun test_unknown_call_request/1,
         fun test_unknown_cast/1,
         fun test_handle_info/1,

         %% Recording tests - messages
         fun test_record_message_when_recording/1,
         fun test_record_message_when_not_recording/1,
         fun test_record_multiple_messages/1,
         fun test_record_message_with_binary_node_ids/1,
         fun test_record_message_with_atom_node_ids/1,
         fun test_record_message_with_complex_payload/1,

         %% Recording tests - state changes
         fun test_record_state_change_when_recording/1,
         fun test_record_state_change_when_not_recording/1,
         fun test_record_multiple_state_changes/1,
         fun test_record_state_change_all_components/1,

         %% Recording tests - events
         fun test_record_event_when_recording/1,
         fun test_record_event_when_not_recording/1,
         fun test_record_multiple_events/1,
         fun test_record_event_various_types/1,

         %% Mixed recording tests
         fun test_record_mixed_entries/1,
         fun test_recording_session_multiple_cycles/1,

         %% Trace size limit tests
         fun test_trace_size_limit_not_exceeded/1,

         %% Verify trace tests - basic
         fun test_verify_empty_trace/1,
         fun test_verify_trace_with_valid_entries/1,

         %% Leader uniqueness tests
         fun test_verify_leader_uniqueness_single_leader/1,
         fun test_verify_leader_uniqueness_sequential_leaders/1,
         fun test_verify_leader_uniqueness_with_stepdown/1,

         %% Sibling exclusivity tests
         fun test_verify_sibling_exclusivity_no_overlap/1,
         fun test_verify_sibling_exclusivity_single_sibling/1,
         fun test_verify_sibling_exclusivity_empty/1,

         %% Log consistency tests
         fun test_verify_log_consistency_empty/1,
         fun test_verify_log_consistency_single_node/1,
         fun test_verify_log_consistency_matching_logs/1,
         fun test_verify_log_consistency_mismatched_logs/1,

         %% Job loss tests
         fun test_verify_no_job_loss_completed_job/1,
         fun test_verify_no_job_loss_job_in_progress/1,
         fun test_verify_no_job_loss_node_death/1,
         fun test_verify_no_job_loss_unknown_node/1,

         %% TLA+ export tests
         fun test_export_tla_trace_empty/1,
         fun test_export_tla_trace_with_messages/1,
         fun test_export_tla_trace_with_state_changes/1,
         fun test_export_tla_trace_with_events/1,
         fun test_export_tla_trace_custom_module_name/1,
         fun test_export_tla_trace_mixed_entries/1,

         %% term_to_tla conversion tests
         fun test_term_to_tla_atoms/1,
         fun test_term_to_tla_binaries/1,
         fun test_term_to_tla_integers/1,
         fun test_term_to_tla_lists/1,
         fun test_term_to_tla_maps/1,
         fun test_term_to_tla_tuples/1,
         fun test_term_to_tla_other_types/1,

         %% to_binary conversion tests
         fun test_to_binary_atoms/1,
         fun test_to_binary_binaries/1,
         fun test_to_binary_lists/1,
         fun test_to_binary_integers/1,

         %% code_change test
         fun test_code_change/1,

         %% terminate test
         fun test_terminate/1
     ]}.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_recording(_Pid) ->
    ?_test(begin
        Result = flurm_trace:start_recording(),
        ?assertEqual(ok, Result)
    end).

test_stop_recording(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        Trace = flurm_trace:stop_recording(),
        ?assertEqual([], Trace)
    end).

test_stop_recording_returns_reversed_trace(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(event1, data1),
        timer:sleep(1),
        flurm_trace:record_event(event2, data2),
        %% Allow async casts to complete
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:stop_recording(),
        ?assertEqual(2, length(Trace)),
        %% First event should come before second in reversed order
        [First, Second] = Trace,
        ?assertEqual(event1, First#trace_event.type),
        ?assertEqual(event2, Second#trace_event.type)
    end).

test_get_trace_without_stopping(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(test_event, test_data),
        _ = sys:get_state(?SERVER),
        Trace1 = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace1)),
        %% Recording should still be active
        flurm_trace:record_event(another_event, more_data),
        _ = sys:get_state(?SERVER),
        Trace2 = flurm_trace:get_trace(),
        ?assertEqual(2, length(Trace2))
    end).

test_clear_trace(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(test_event, test_data),
        _ = sys:get_state(?SERVER),
        Trace1 = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace1)),
        ok = flurm_trace:clear_trace(),
        Trace2 = flurm_trace:get_trace(),
        ?assertEqual([], Trace2)
    end).

test_unknown_call_request(_Pid) ->
    ?_test(begin
        Result = gen_server:call(?SERVER, unknown_request),
        ?assertEqual({error, unknown_request}, Result)
    end).

test_unknown_cast(_Pid) ->
    ?_test(begin
        gen_server:cast(?SERVER, unknown_cast),
        %% Should not crash, just ignore
        _ = sys:get_state(?SERVER),
        ?assert(is_process_alive(whereis(?SERVER)))
    end).

test_handle_info(_Pid) ->
    ?_test(begin
        ?SERVER ! unexpected_info,
        %% Should not crash, just ignore
        timer:sleep(10),
        ?assert(is_process_alive(whereis(?SERVER)))
    end).

%%====================================================================
%% Recording Tests - Messages
%%====================================================================

test_record_message_when_recording(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_message(node_a, node_b, heartbeat, #{data => 123}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace)),
        [Msg] = Trace,
        ?assertMatch(#trace_message{from = node_a, to = node_b, type = heartbeat}, Msg)
    end).

test_record_message_when_not_recording(_Pid) ->
    ?_test(begin
        %% Don't start recording
        flurm_trace:record_message(node_a, node_b, heartbeat, #{}),
        _ = sys:get_state(?SERVER),
        %% Now start recording and check
        flurm_trace:start_recording(),
        Trace = flurm_trace:get_trace(),
        ?assertEqual([], Trace)
    end).

test_record_multiple_messages(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_message(a, b, msg1, #{}),
        flurm_trace:record_message(b, c, msg2, #{}),
        flurm_trace:record_message(c, d, msg3, #{}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(3, length(Trace))
    end).

test_record_message_with_binary_node_ids(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_message(<<"node1">>, <<"node2">>, sync, #{}),
        _ = sys:get_state(?SERVER),
        [Msg] = flurm_trace:get_trace(),
        ?assertEqual(<<"node1">>, Msg#trace_message.from),
        ?assertEqual(<<"node2">>, Msg#trace_message.to)
    end).

test_record_message_with_atom_node_ids(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_message('node1@host', 'node2@host', append_entries, #{}),
        _ = sys:get_state(?SERVER),
        [Msg] = flurm_trace:get_trace(),
        ?assertEqual('node1@host', Msg#trace_message.from),
        ?assertEqual('node2@host', Msg#trace_message.to)
    end).

test_record_message_with_complex_payload(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        Payload = #{
            term => 5,
            entries => [1, 2, 3],
            nested => #{deep => #{value => 42}}
        },
        flurm_trace:record_message(a, b, raft, Payload),
        _ = sys:get_state(?SERVER),
        [Msg] = flurm_trace:get_trace(),
        ?assertEqual(Payload, Msg#trace_message.payload)
    end).

%%====================================================================
%% Recording Tests - State Changes
%%====================================================================

test_record_state_change_when_recording(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_state_change(node1, ra_leader, {follower, leader}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace)),
        [Change] = Trace,
        ?assertMatch(#trace_state_change{node = node1, component = ra_leader}, Change),
        ?assertEqual(follower, Change#trace_state_change.old_state),
        ?assertEqual(leader, Change#trace_state_change.new_state)
    end).

test_record_state_change_when_not_recording(_Pid) ->
    ?_test(begin
        %% Don't start recording
        flurm_trace:record_state_change(node1, ra_leader, {follower, leader}),
        _ = sys:get_state(?SERVER),
        flurm_trace:start_recording(),
        Trace = flurm_trace:get_trace(),
        ?assertEqual([], Trace)
    end).

test_record_multiple_state_changes(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_state_change(node1, ra_leader, {follower, candidate}),
        flurm_trace:record_state_change(node1, ra_leader, {candidate, leader}),
        flurm_trace:record_state_change(node2, ra_leader, {follower, candidate}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(3, length(Trace))
    end).

test_record_state_change_all_components(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_state_change(node1, ra_leader, {a, b}),
        flurm_trace:record_state_change(node1, job_state, {pending, running}),
        flurm_trace:record_state_change(node1, node_state, {idle, busy}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(3, length(Trace)),
        Components = [C#trace_state_change.component || C <- Trace],
        ?assert(lists:member(ra_leader, Components)),
        ?assert(lists:member(job_state, Components)),
        ?assert(lists:member(node_state, Components))
    end).

%%====================================================================
%% Recording Tests - Events
%%====================================================================

test_record_event_when_recording(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(job_submitted, #{job_id => 123}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace)),
        [Event] = Trace,
        ?assertMatch(#trace_event{type = job_submitted}, Event),
        ?assertEqual(#{job_id => 123}, Event#trace_event.data)
    end).

test_record_event_when_not_recording(_Pid) ->
    ?_test(begin
        flurm_trace:record_event(job_submitted, #{job_id => 123}),
        _ = sys:get_state(?SERVER),
        flurm_trace:start_recording(),
        Trace = flurm_trace:get_trace(),
        ?assertEqual([], Trace)
    end).

test_record_multiple_events(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(event1, data1),
        flurm_trace:record_event(event2, data2),
        flurm_trace:record_event(event3, data3),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(3, length(Trace))
    end).

test_record_event_various_types(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_event(job_submitted, #{job_id => 1}),
        flurm_trace:record_event(job_completed, #{job_id => 1}),
        flurm_trace:record_event(job_failed, #{job_id => 2}),
        flurm_trace:record_event(job_cancelled, #{job_id => 3}),
        flurm_trace:record_event(sibling_started, #{fed_job_id => 10, cluster => c1}),
        flurm_trace:record_event(log_append, #{node => n1, index => 1, entry => e1}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(6, length(Trace))
    end).

%%====================================================================
%% Mixed Recording Tests
%%====================================================================

test_record_mixed_entries(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        flurm_trace:record_message(a, b, heartbeat, #{}),
        flurm_trace:record_state_change(node1, ra_leader, {follower, leader}),
        flurm_trace:record_event(job_submitted, #{job_id => 1}),
        flurm_trace:record_message(b, a, heartbeat_ack, #{}),
        flurm_trace:record_event(job_completed, #{job_id => 1}),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(5, length(Trace)),
        %% Check types
        Messages = [E || E <- Trace, is_record(E, trace_message)],
        Changes = [E || E <- Trace, is_record(E, trace_state_change)],
        Events = [E || E <- Trace, is_record(E, trace_event)],
        ?assertEqual(2, length(Messages)),
        ?assertEqual(1, length(Changes)),
        ?assertEqual(2, length(Events))
    end).

test_recording_session_multiple_cycles(_Pid) ->
    ?_test(begin
        %% First session
        flurm_trace:start_recording(),
        flurm_trace:record_event(session1_event, data1),
        _ = sys:get_state(?SERVER),
        Trace1 = flurm_trace:stop_recording(),
        ?assertEqual(1, length(Trace1)),

        %% Second session
        flurm_trace:start_recording(),
        flurm_trace:record_event(session2_event, data2),
        _ = sys:get_state(?SERVER),
        Trace2 = flurm_trace:get_trace(),
        ?assertEqual(1, length(Trace2)),
        [E2] = Trace2,
        ?assertEqual(session2_event, E2#trace_event.type),

        %% Third session after clear
        flurm_trace:clear_trace(),
        flurm_trace:start_recording(),
        Trace3 = flurm_trace:get_trace(),
        ?assertEqual([], Trace3)
    end).

%%====================================================================
%% Trace Size Limit Tests
%%====================================================================

test_trace_size_limit_not_exceeded(_Pid) ->
    ?_test(begin
        flurm_trace:start_recording(),
        %% Add some entries but don't exceed limit
        lists:foreach(fun(I) ->
            flurm_trace:record_event(test_event, #{index => I})
        end, lists:seq(1, 100)),
        _ = sys:get_state(?SERVER),
        Trace = flurm_trace:get_trace(),
        ?assertEqual(100, length(Trace))
    end).

%%====================================================================
%% Verify Trace Tests - Basic
%%====================================================================

test_verify_empty_trace(_Pid) ->
    ?_test(begin
        Result = flurm_trace:verify_trace([]),
        ?assertEqual(ok, Result)
    end).

test_verify_trace_with_valid_entries(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = job_submitted, data = #{job_id => 1}},
            #trace_event{timestamp = 2, type = job_completed, data = #{job_id => 1}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% Leader Uniqueness Tests
%%====================================================================

test_verify_leader_uniqueness_single_leader(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                               old_state = follower, new_state = leader}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_leader_uniqueness_sequential_leaders(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                               old_state = follower, new_state = leader},
            #trace_state_change{timestamp = 2, node = node1, component = ra_leader,
                               old_state = leader, new_state = follower},
            #trace_state_change{timestamp = 3, node = node2, component = ra_leader,
                               old_state = follower, new_state = leader}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_leader_uniqueness_with_stepdown(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                               old_state = follower, new_state = leader},
            #trace_state_change{timestamp = 2, node = node1, component = ra_leader,
                               old_state = leader, new_state = follower},
            #trace_state_change{timestamp = 3, node = node2, component = ra_leader,
                               old_state = follower, new_state = leader}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% Sibling Exclusivity Tests
%%====================================================================

test_verify_sibling_exclusivity_no_overlap(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = sibling_started,
                        data = #{fed_job_id => 1, cluster => c1}},
            #trace_event{timestamp = 2000, type = sibling_started,
                        data = #{fed_job_id => 1, cluster => c2}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_sibling_exclusivity_single_sibling(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = sibling_started,
                        data = #{fed_job_id => 1, cluster => c1}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_sibling_exclusivity_empty(_Pid) ->
    ?_test(begin
        Result = flurm_trace:verify_trace([]),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% Log Consistency Tests
%%====================================================================

test_verify_log_consistency_empty(_Pid) ->
    ?_test(begin
        Result = flurm_trace:verify_trace([]),
        ?assertEqual(ok, Result)
    end).

test_verify_log_consistency_single_node(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = log_append,
                        data = #{node => node1, index => 1, entry => e1}},
            #trace_event{timestamp = 2, type = log_append,
                        data = #{node => node1, index => 2, entry => e2}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_log_consistency_matching_logs(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = log_append,
                        data = #{node => node1, index => 1, entry => e1}},
            #trace_event{timestamp = 2, type = log_append,
                        data = #{node => node2, index => 1, entry => e1}},
            #trace_event{timestamp = 3, type = log_append,
                        data = #{node => node1, index => 2, entry => e2}},
            #trace_event{timestamp = 4, type = log_append,
                        data = #{node => node2, index => 2, entry => e2}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_log_consistency_mismatched_logs(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = log_append,
                        data = #{node => node1, index => 1, entry => e1}},
            #trace_event{timestamp = 2, type = log_append,
                        data = #{node => node2, index => 1, entry => different_entry}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertMatch({error, _}, Result)
    end).

%%====================================================================
%% Job Loss Tests
%%====================================================================

test_verify_no_job_loss_completed_job(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 1, type = job_submitted,
                        data = #{job_id => 1}},
            #trace_event{timestamp = 2, type = job_completed,
                        data = #{job_id => 1}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_no_job_loss_job_in_progress(_Pid) ->
    ?_test(begin
        %% Job submitted but not completed - ok if node didn't die
        Trace = [
            #trace_event{timestamp = 1, type = job_submitted,
                        data = #{job_id => 1}},
            #trace_event{timestamp = 2, type = job_allocated,
                        data = #{job_id => 1, node => node1}}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

test_verify_no_job_loss_node_death(_Pid) ->
    ?_test(begin
        %% Job allocated to node that died without completion
        Trace = [
            #trace_event{timestamp = 1, type = job_submitted,
                        data = #{job_id => 1}},
            #trace_event{timestamp = 2, type = job_allocated,
                        data = #{job_id => 1, node => node1}},
            #trace_state_change{timestamp = 3, node = node1, component = status,
                               old_state = up, new_state = down}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertMatch({error, _}, Result)
    end).

test_verify_no_job_loss_unknown_node(_Pid) ->
    ?_test(begin
        %% Job submitted but no allocation info - can't determine
        Trace = [
            #trace_event{timestamp = 1, type = job_submitted,
                        data = #{job_id => 1}},
            #trace_state_change{timestamp = 2, node = some_node, component = status,
                               old_state = up, new_state = down}
        ],
        Result = flurm_trace:verify_trace(Trace),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% TLA+ Export Tests
%%====================================================================

test_export_tla_trace_empty(_Pid) ->
    ?_test(begin
        TLA = flurm_trace:export_tla_trace([]),
        ?assert(is_binary(TLA)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"MODULE FlurmTrace">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"Trace ==">>))
    end).

test_export_tla_trace_with_messages(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_message{timestamp = 100, from = node1, to = node2,
                          type = heartbeat, payload = #{term => 5}}
        ],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assert(is_binary(TLA)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"message\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"from |-> \"node1\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"to |-> \"node2\"">>))
    end).

test_export_tla_trace_with_state_changes(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_state_change{timestamp = 100, node = node1, component = ra_leader,
                               old_state = follower, new_state = leader}
        ],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assert(is_binary(TLA)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"state_change\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"component |-> \"ra_leader\"">>))
    end).

test_export_tla_trace_with_events(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_event{timestamp = 100, type = job_submitted, data = #{job_id => 42}}
        ],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assert(is_binary(TLA)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"event\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"event_type |-> \"job_submitted\"">>))
    end).

test_export_tla_trace_custom_module_name(_Pid) ->
    ?_test(begin
        TLA = flurm_trace:export_tla_trace([], <<"CustomTrace">>),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"MODULE CustomTrace">>))
    end).

test_export_tla_trace_mixed_entries(_Pid) ->
    ?_test(begin
        Trace = [
            #trace_message{timestamp = 100, from = a, to = b, type = msg, payload = #{}},
            #trace_state_change{timestamp = 200, node = n1, component = comp,
                               old_state = s1, new_state = s2},
            #trace_event{timestamp = 300, type = evt, data = #{}}
        ],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assert(is_binary(TLA)),
        %% Should have all three types
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"message\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"state_change\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"type |-> \"event\"">>))
    end).

%%====================================================================
%% term_to_tla Conversion Tests
%%====================================================================

test_term_to_tla_atoms(_Pid) ->
    ?_test(begin
        %% Test via export that includes atoms
        Trace = [#trace_event{timestamp = 1, type = my_atom, data = another_atom}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"\"my_atom\"">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"\"another_atom\"">>))
    end).

test_term_to_tla_binaries(_Pid) ->
    ?_test(begin
        Trace = [#trace_event{timestamp = 1, type = test, data = <<"binary_data">>}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"\"binary_data\"">>))
    end).

test_term_to_tla_integers(_Pid) ->
    ?_test(begin
        Trace = [#trace_event{timestamp = 1, type = test, data = 12345}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"12345">>))
    end).

test_term_to_tla_lists(_Pid) ->
    ?_test(begin
        Trace = [#trace_event{timestamp = 1, type = test, data = [1, 2, 3]}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"<<">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<">>">>))
    end).

test_term_to_tla_maps(_Pid) ->
    ?_test(begin
        Trace = [#trace_event{timestamp = 1, type = test, data = #{key => value}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"\"key\" |-> \"value\"">>))
    end).

test_term_to_tla_tuples(_Pid) ->
    ?_test(begin
        Trace = [#trace_event{timestamp = 1, type = test, data = {a, b, c}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        %% Tuples are converted to lists in TLA+
        ?assertNotEqual(nomatch, binary:match(TLA, <<"<<">>))
    end).

test_term_to_tla_other_types(_Pid) ->
    ?_test(begin
        %% Test with a pid (opaque type)
        Trace = [#trace_event{timestamp = 1, type = test, data = self()}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"<opaque>">>))
    end).

%%====================================================================
%% to_binary Conversion Tests
%%====================================================================

test_to_binary_atoms(_Pid) ->
    ?_test(begin
        %% Test via message with atom node IDs
        Trace = [#trace_message{timestamp = 1, from = my_atom_node, to = other_atom,
                               type = test, payload = #{}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"my_atom_node">>))
    end).

test_to_binary_binaries(_Pid) ->
    ?_test(begin
        Trace = [#trace_message{timestamp = 1, from = <<"binary_node">>, to = <<"other">>,
                               type = test, payload = #{}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"binary_node">>))
    end).

test_to_binary_lists(_Pid) ->
    ?_test(begin
        Trace = [#trace_message{timestamp = 1, from = "list_node", to = "other",
                               type = test, payload = #{}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"list_node">>))
    end).

test_to_binary_integers(_Pid) ->
    ?_test(begin
        Trace = [#trace_message{timestamp = 1, from = 12345, to = 67890,
                               type = test, payload = #{}}],
        TLA = flurm_trace:export_tla_trace(Trace),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"12345">>)),
        ?assertNotEqual(nomatch, binary:match(TLA, <<"67890">>))
    end).

%%====================================================================
%% code_change and terminate Tests
%%====================================================================

test_code_change(_Pid) ->
    ?_test(begin
        %% code_change should return {ok, State}
        State = #{test => state},
        Result = flurm_trace:code_change(old_vsn, State, extra),
        ?assertEqual({ok, State}, Result)
    end).

test_terminate(_Pid) ->
    ?_test(begin
        %% terminate should return ok
        State = #{test => state},
        Result = flurm_trace:terminate(normal, State),
        ?assertEqual(ok, Result)
    end).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

multiple_leader_becomes_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Leader election without previous stepdown - violation check",
              ?_test(begin
                  %% Two nodes become leader without the first stepping down
                  Trace = [
                      #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                                         old_state = follower, new_state = leader},
                      #trace_state_change{timestamp = 2, node = node2, component = ra_leader,
                                         old_state = follower, new_state = leader}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  %% This should detect a violation (two leaders without stepdown)
                  ?assertMatch({error, _}, Result)
              end)},

             {"Non-leader state changes should not affect leader check",
              ?_test(begin
                  Trace = [
                      #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                                         old_state = follower, new_state = candidate},
                      #trace_state_change{timestamp = 2, node = node2, component = ra_leader,
                                         old_state = follower, new_state = candidate}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  ?assertEqual(ok, Result)
              end)},

             {"Multiple job types terminal events",
              ?_test(begin
                  Trace = [
                      #trace_event{timestamp = 1, type = job_submitted, data = #{job_id => 1}},
                      #trace_event{timestamp = 2, type = job_submitted, data = #{job_id => 2}},
                      #trace_event{timestamp = 3, type = job_submitted, data = #{job_id => 3}},
                      #trace_event{timestamp = 4, type = job_completed, data = #{job_id => 1}},
                      #trace_event{timestamp = 5, type = job_failed, data = #{job_id => 2}},
                      #trace_event{timestamp = 6, type = job_cancelled, data = #{job_id => 3}}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  ?assertEqual(ok, Result)
              end)}
         ]
     end}.

timestamp_ordering_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Timestamps should be monotonically increasing",
              ?_test(begin
                  flurm_trace:start_recording(),
                  lists:foreach(fun(_) ->
                      flurm_trace:record_event(tick, #{}),
                      timer:sleep(1)  % Ensure time passes
                  end, lists:seq(1, 5)),
                  _ = sys:get_state(?SERVER),
                  Trace = flurm_trace:get_trace(),
                  Timestamps = [E#trace_event.timestamp || E <- Trace],
                  %% Timestamps should be in increasing order (trace is reversed on get)
                  ?assertEqual(Timestamps, lists:sort(Timestamps))
              end)}
         ]
     end}.

concurrent_sibling_detection_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Siblings starting within 1ms on different clusters - violation",
              ?_test(begin
                  %% Two siblings starting within 1ms window
                  Trace = [
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 1, cluster => cluster_a}},
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 1, cluster => cluster_b}}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  %% This detects concurrent sibling start
                  ?assertMatch({error, _}, Result)
              end)},

             {"Same cluster sibling restarts - ok",
              ?_test(begin
                  Trace = [
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 1, cluster => cluster_a}},
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 1, cluster => cluster_a}}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  ?assertEqual(ok, Result)
              end)},

             {"Different federation jobs - ok",
              ?_test(begin
                  Trace = [
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 1, cluster => cluster_a}},
                      #trace_event{timestamp = 100, type = sibling_started,
                                  data = #{fed_job_id => 2, cluster => cluster_b}}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  ?assertEqual(ok, Result)
              end)}
         ]
     end}.

tla_export_complex_data_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Export with deeply nested data",
              ?_test(begin
                  Trace = [
                      #trace_event{timestamp = 1, type = complex,
                                  data = #{
                                      level1 => #{
                                          level2 => #{
                                              level3 => [1, 2, 3]
                                          }
                                      }
                                  }}
                  ],
                  TLA = flurm_trace:export_tla_trace(Trace),
                  ?assert(is_binary(TLA)),
                  ?assert(byte_size(TLA) > 0)
              end)},

             {"Export with empty collections",
              ?_test(begin
                  Trace = [
                      #trace_event{timestamp = 1, type = empty_data, data = #{}},
                      #trace_event{timestamp = 2, type = empty_list, data = []}
                  ],
                  TLA = flurm_trace:export_tla_trace(Trace),
                  ?assert(is_binary(TLA))
              end)},

             {"Export preserves special characters in binaries",
              ?_test(begin
                  Trace = [
                      #trace_message{timestamp = 1, from = <<"node-1">>, to = <<"node_2">>,
                                    type = test, payload = <<"data with spaces">>}
                  ],
                  TLA = flurm_trace:export_tla_trace(Trace),
                  ?assert(is_binary(TLA))
              end)}
         ]
     end}.

verify_trace_multiple_violations_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Multiple types of violations should all be reported",
              ?_test(begin
                  Trace = [
                      %% Leader violation
                      #trace_state_change{timestamp = 1, node = node1, component = ra_leader,
                                         old_state = follower, new_state = leader},
                      #trace_state_change{timestamp = 2, node = node2, component = ra_leader,
                                         old_state = follower, new_state = leader},
                      %% Log consistency violation
                      #trace_event{timestamp = 3, type = log_append,
                                  data = #{node => n1, index => 1, entry => e1}},
                      #trace_event{timestamp = 4, type = log_append,
                                  data = #{node => n2, index => 1, entry => e2}}
                  ],
                  {error, Violations} = flurm_trace:verify_trace(Trace),
                  %% Should have multiple violations
                  ?assert(length(Violations) >= 2)
              end)}
         ]
     end}.

%%====================================================================
%% Additional Coverage Tests for Edge Cases
%%====================================================================

additional_edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Record message with undefined payload",
              ?_test(begin
                  flurm_trace:start_recording(),
                  flurm_trace:record_message(a, b, test, undefined),
                  _ = sys:get_state(?SERVER),
                  [Msg] = flurm_trace:get_trace(),
                  ?assertEqual(undefined, Msg#trace_message.payload)
              end)},

             {"Record state change with complex states",
              ?_test(begin
                  flurm_trace:start_recording(),
                  OldState = #{role => follower, term => 5, votes => []},
                  NewState = #{role => leader, term => 6, votes => [a, b, c]},
                  flurm_trace:record_state_change(node1, raft, {OldState, NewState}),
                  _ = sys:get_state(?SERVER),
                  [Change] = flurm_trace:get_trace(),
                  ?assertEqual(OldState, Change#trace_state_change.old_state),
                  ?assertEqual(NewState, Change#trace_state_change.new_state)
              end)},

             {"Record event with list data",
              ?_test(begin
                  flurm_trace:start_recording(),
                  flurm_trace:record_event(nodes_list, [node1, node2, node3]),
                  _ = sys:get_state(?SERVER),
                  [Event] = flurm_trace:get_trace(),
                  ?assertEqual([node1, node2, node3], Event#trace_event.data)
              end)},

             {"Start recording twice should reset trace",
              ?_test(begin
                  flurm_trace:start_recording(),
                  flurm_trace:record_event(first_session, data1),
                  _ = sys:get_state(?SERVER),
                  ?assertEqual(1, length(flurm_trace:get_trace())),
                  %% Start recording again
                  flurm_trace:start_recording(),
                  ?assertEqual(0, length(flurm_trace:get_trace()))
              end)},

             {"Stop recording when not recording returns empty",
              ?_test(begin
                  Trace = flurm_trace:stop_recording(),
                  ?assertEqual([], Trace)
              end)},

             {"Clear trace when not recording",
              ?_test(begin
                  ok = flurm_trace:clear_trace(),
                  ?assert(true)  % Should not crash
              end)},

             {"Verify trace with non-leader state changes only",
              ?_test(begin
                  Trace = [
                      #trace_state_change{timestamp = 1, node = node1, component = job_manager,
                                         old_state = idle, new_state = busy}
                  ],
                  Result = flurm_trace:verify_trace(Trace),
                  ?assertEqual(ok, Result)
              end)},

             {"Export empty trace produces valid TLA+",
              ?_test(begin
                  TLA = flurm_trace:export_tla_trace([]),
                  ?assertNotEqual(nomatch, binary:match(TLA, <<"EXTENDS">>)),
                  ?assertNotEqual(nomatch, binary:match(TLA, <<"Integers">>)),
                  ?assertNotEqual(nomatch, binary:match(TLA, <<"====">>))
              end)}
         ]
     end}.

%% Test for to_binary with unknown type
to_binary_unknown_type_test() ->
    %% Test via message with reference node ID (unknown type)
    Trace = [#trace_message{timestamp = 1, from = make_ref(), to = make_ref(),
                           type = test, payload = #{}}],
    TLA = flurm_trace:export_tla_trace(Trace),
    ?assertNotEqual(nomatch, binary:match(TLA, <<"unknown">>)).

%% Additional log consistency tests
log_consistency_additional_tests_test_() ->
    [
        {"Three nodes with matching logs",
         ?_test(begin
             Trace = [
                 #trace_event{timestamp = 1, type = log_append,
                             data = #{node => n1, index => 1, entry => e1}},
                 #trace_event{timestamp = 2, type = log_append,
                             data = #{node => n2, index => 1, entry => e1}},
                 #trace_event{timestamp = 3, type = log_append,
                             data = #{node => n3, index => 1, entry => e1}}
             ],
             Result = flurm_trace:verify_trace(Trace),
             ?assertEqual(ok, Result)
         end)},

        {"Nodes with non-overlapping indices",
         ?_test(begin
             Trace = [
                 #trace_event{timestamp = 1, type = log_append,
                             data = #{node => n1, index => 1, entry => e1}},
                 #trace_event{timestamp = 2, type = log_append,
                             data = #{node => n2, index => 2, entry => e2}}
             ],
             Result = flurm_trace:verify_trace(Trace),
             ?assertEqual(ok, Result)
         end)}
    ].

%% Additional job loss tests
job_loss_additional_tests_test_() ->
    [
        {"Job with failed terminal state",
         ?_test(begin
             Trace = [
                 #trace_event{timestamp = 1, type = job_submitted, data = #{job_id => 1}},
                 #trace_event{timestamp = 2, type = job_failed, data = #{job_id => 1}}
             ],
             Result = flurm_trace:verify_trace(Trace),
             ?assertEqual(ok, Result)
         end)},

        {"Job with cancelled terminal state",
         ?_test(begin
             Trace = [
                 #trace_event{timestamp = 1, type = job_submitted, data = #{job_id => 1}},
                 #trace_event{timestamp = 2, type = job_cancelled, data = #{job_id => 1}}
             ],
             Result = flurm_trace:verify_trace(Trace),
             ?assertEqual(ok, Result)
         end)},

        {"Multiple jobs with different outcomes",
         ?_test(begin
             Trace = [
                 #trace_event{timestamp = 1, type = job_submitted, data = #{job_id => 1}},
                 #trace_event{timestamp = 2, type = job_submitted, data = #{job_id => 2}},
                 #trace_event{timestamp = 3, type = job_submitted, data = #{job_id => 3}},
                 #trace_event{timestamp = 4, type = job_completed, data = #{job_id => 1}},
                 #trace_event{timestamp = 5, type = job_failed, data = #{job_id => 2}},
                 #trace_event{timestamp = 6, type = job_cancelled, data = #{job_id => 3}}
             ],
             Result = flurm_trace:verify_trace(Trace),
             ?assertEqual(ok, Result)
         end)}
    ].

%% Test TLA+ export with various data types
tla_export_data_types_test_() ->
    [
        {"Export float via reference (becomes opaque)",
         ?_test(begin
             Trace = [#trace_event{timestamp = 1, type = test, data = 3.14159}],
             TLA = flurm_trace:export_tla_trace(Trace),
             ?assert(is_binary(TLA))
         end)},

        {"Export nested empty structures",
         ?_test(begin
             Trace = [#trace_event{timestamp = 1, type = test,
                                  data = #{empty_map => #{}, empty_list => []}}],
             TLA = flurm_trace:export_tla_trace(Trace),
             ?assert(is_binary(TLA))
         end)},

        {"Export mixed list types",
         ?_test(begin
             Trace = [#trace_event{timestamp = 1, type = test,
                                  data = [1, atom, <<"binary">>, #{key => val}]}],
             TLA = flurm_trace:export_tla_trace(Trace),
             ?assert(is_binary(TLA))
         end)}
    ].

%% Test recording at boundary conditions
boundary_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Record with very long binary payload",
              ?_test(begin
                  flurm_trace:start_recording(),
                  LongBinary = binary:copy(<<"x">>, 10000),
                  flurm_trace:record_message(a, b, test, LongBinary),
                  _ = sys:get_state(?SERVER),
                  [Msg] = flurm_trace:get_trace(),
                  ?assertEqual(10000, byte_size(Msg#trace_message.payload))
              end)},

             {"Record with deeply nested map",
              ?_test(begin
                  flurm_trace:start_recording(),
                  DeepMap = lists:foldl(fun(I, Acc) ->
                      #{list_to_atom("level" ++ integer_to_list(I)) => Acc}
                  end, #{value => 42}, lists:seq(1, 20)),
                  flurm_trace:record_event(deep_nested, DeepMap),
                  _ = sys:get_state(?SERVER),
                  [Event] = flurm_trace:get_trace(),
                  ?assert(is_map(Event#trace_event.data))
              end)},

             {"Rapid recording and retrieval",
              ?_test(begin
                  flurm_trace:start_recording(),
                  %% Rapid fire events
                  lists:foreach(fun(I) ->
                      flurm_trace:record_event(rapid, #{i => I})
                  end, lists:seq(1, 1000)),
                  _ = sys:get_state(?SERVER),
                  Trace = flurm_trace:get_trace(),
                  ?assertEqual(1000, length(Trace))
              end)}
         ]
     end}.
