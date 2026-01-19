%%%-------------------------------------------------------------------
%%% @doc Tests for -ifdef(TEST) exported functions in flurm_controller_failover
%%%
%%% These tests directly call the internal helper functions that are
%%% exported via -ifdef(TEST) to provide real code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_failover_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% State Record Definition (matches module's internal record)
%%====================================================================

-record(state, {
    is_leader = false :: boolean(),
    became_leader_time :: erlang:timestamp() | undefined,
    recovery_status = idle :: idle | recovering | recovered | failed,
    recovery_error :: term() | undefined,
    health_check_ref :: reference() | undefined,
    pending_operations = [] :: [{reference(), term()}]
}).

%%====================================================================
%% calculate_leader_uptime/1 Tests
%%====================================================================

calculate_leader_uptime_undefined_test() ->
    %% When became_leader_time is undefined, uptime should be 0
    Result = flurm_controller_failover:calculate_leader_uptime(undefined),
    ?assertEqual(0, Result).

calculate_leader_uptime_recent_test() ->
    %% Test with a recent timestamp - should return small number
    Now = erlang:timestamp(),
    Result = flurm_controller_failover:calculate_leader_uptime(Now),
    ?assert(Result >= 0),
    ?assert(Result < 5).  % Should be less than 5 seconds

calculate_leader_uptime_past_test() ->
    %% Test with a timestamp from the past
    {MegaSecs, Secs, MicroSecs} = erlang:timestamp(),
    PastTime = {MegaSecs, Secs - 10, MicroSecs},  % 10 seconds ago
    Result = flurm_controller_failover:calculate_leader_uptime(PastTime),
    ?assert(Result >= 9),
    ?assert(Result =< 11).

calculate_leader_uptime_valid_timestamp_test() ->
    %% Test that result is always a non-negative integer
    Timestamp = erlang:timestamp(),
    timer:sleep(100),  % Wait a bit
    Result = flurm_controller_failover:calculate_leader_uptime(Timestamp),
    ?assert(is_integer(Result)),
    ?assert(Result >= 0).

%%====================================================================
%% handle_became_leader/1 Tests
%%====================================================================

handle_became_leader_sets_leader_flag_test() ->
    %% Test that handle_became_leader sets is_leader to true
    InitialState = #state{is_leader = false},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    ?assertEqual(true, NewState#state.is_leader).

handle_became_leader_sets_timestamp_test() ->
    %% Test that handle_became_leader sets became_leader_time
    InitialState = #state{became_leader_time = undefined},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    ?assertNotEqual(undefined, NewState#state.became_leader_time),
    %% Check it's a valid timestamp tuple
    {MegaSecs, Secs, MicroSecs} = NewState#state.became_leader_time,
    ?assert(is_integer(MegaSecs)),
    ?assert(is_integer(Secs)),
    ?assert(is_integer(MicroSecs)).

handle_became_leader_resets_recovery_status_test() ->
    %% Test that recovery_status is reset to idle
    InitialState = #state{recovery_status = failed},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    ?assertEqual(idle, NewState#state.recovery_status).

handle_became_leader_clears_recovery_error_test() ->
    %% Test that recovery_error is cleared
    InitialState = #state{recovery_error = some_error},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    ?assertEqual(undefined, NewState#state.recovery_error).

handle_became_leader_starts_health_check_test() ->
    %% Test that health_check_ref is set
    InitialState = #state{health_check_ref = undefined},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    ?assertNotEqual(undefined, NewState#state.health_check_ref),
    ?assert(is_reference(NewState#state.health_check_ref)),
    %% Clean up the timer
    erlang:cancel_timer(NewState#state.health_check_ref),
    %% Clean up the start_recovery message
    receive start_recovery -> ok after 100 -> ok end.

handle_became_leader_cancels_old_timer_test() ->
    %% Test that existing health_check_ref is cancelled
    OldRef = erlang:send_after(60000, self(), old_health_check),
    InitialState = #state{health_check_ref = OldRef},
    NewState = flurm_controller_failover:handle_became_leader(InitialState),
    %% New timer should be different
    ?assertNotEqual(OldRef, NewState#state.health_check_ref),
    %% Clean up
    erlang:cancel_timer(NewState#state.health_check_ref),
    receive start_recovery -> ok after 100 -> ok end.

%%====================================================================
%% handle_lost_leadership/1 Tests
%%====================================================================

handle_lost_leadership_clears_leader_flag_test() ->
    %% Test that is_leader is set to false
    InitialState = #state{is_leader = true},
    NewState = flurm_controller_failover:handle_lost_leadership(InitialState),
    ?assertEqual(false, NewState#state.is_leader).

handle_lost_leadership_clears_timestamp_test() ->
    %% Test that became_leader_time is cleared
    InitialState = #state{became_leader_time = erlang:timestamp()},
    NewState = flurm_controller_failover:handle_lost_leadership(InitialState),
    ?assertEqual(undefined, NewState#state.became_leader_time).

handle_lost_leadership_resets_recovery_status_test() ->
    %% Test that recovery_status is reset to idle
    InitialState = #state{recovery_status = recovered},
    NewState = flurm_controller_failover:handle_lost_leadership(InitialState),
    ?assertEqual(idle, NewState#state.recovery_status).

handle_lost_leadership_clears_timer_test() ->
    %% Test that health_check_ref is cleared and timer cancelled
    Ref = erlang:send_after(60000, self(), health_check),
    InitialState = #state{health_check_ref = Ref},
    NewState = flurm_controller_failover:handle_lost_leadership(InitialState),
    ?assertEqual(undefined, NewState#state.health_check_ref).

%%====================================================================
%% handle_recovery_complete/2 Tests
%%====================================================================

handle_recovery_complete_success_test() ->
    %% Test successful recovery completion
    InitialState = #state{recovery_status = recovering},
    NewState = flurm_controller_failover:handle_recovery_complete({ok, recovered}, InitialState),
    ?assertEqual(recovered, NewState#state.recovery_status),
    ?assertEqual(undefined, NewState#state.recovery_error).

handle_recovery_complete_failure_test() ->
    %% Test failed recovery completion
    InitialState = #state{recovery_status = recovering},
    Reason = {recovery_failed, test_error},
    NewState = flurm_controller_failover:handle_recovery_complete({error, Reason}, InitialState),
    ?assertEqual(failed, NewState#state.recovery_status),
    ?assertEqual(Reason, NewState#state.recovery_error),
    %% Should schedule a retry - clean up the timer message
    receive start_recovery -> ok after 6000 -> ok end.

handle_recovery_complete_preserves_other_fields_test() ->
    %% Test that other fields are preserved
    InitialState = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        recovery_status = recovering
    },
    NewState = flurm_controller_failover:handle_recovery_complete({ok, recovered}, InitialState),
    ?assertEqual(true, NewState#state.is_leader),
    ?assertNotEqual(undefined, NewState#state.became_leader_time).

%%====================================================================
%% do_health_check/1 Tests
%%====================================================================

do_health_check_not_leader_test() ->
    %% When not leader, health check should be skipped (state returned unchanged)
    InitialState = #state{is_leader = false, recovery_status = recovered},
    NewState = flurm_controller_failover:do_health_check(InitialState),
    ?assertEqual(InitialState, NewState).

do_health_check_not_recovered_test() ->
    %% When not recovered, health check should be skipped
    InitialState = #state{is_leader = true, recovery_status = recovering},
    NewState = flurm_controller_failover:do_health_check(InitialState),
    ?assertEqual(InitialState, NewState).

do_health_check_idle_test() ->
    %% When idle (not recovered), health check should be skipped
    InitialState = #state{is_leader = true, recovery_status = idle},
    NewState = flurm_controller_failover:do_health_check(InitialState),
    ?assertEqual(InitialState, NewState).

do_health_check_failed_test() ->
    %% When failed, health check should be skipped
    InitialState = #state{is_leader = true, recovery_status = failed},
    NewState = flurm_controller_failover:do_health_check(InitialState),
    ?assertEqual(InitialState, NewState).

%%====================================================================
%% Test Fixtures
%%====================================================================

calculate_leader_uptime_test_() ->
    [
        {"undefined returns 0", fun calculate_leader_uptime_undefined_test/0},
        {"recent timestamp returns small value", fun calculate_leader_uptime_recent_test/0},
        {"past timestamp returns correct seconds", fun calculate_leader_uptime_past_test/0},
        {"always returns non-negative integer", fun calculate_leader_uptime_valid_timestamp_test/0}
    ].

handle_became_leader_test_() ->
    [
        {"sets is_leader to true", fun handle_became_leader_sets_leader_flag_test/0},
        {"sets became_leader_time", fun handle_became_leader_sets_timestamp_test/0},
        {"resets recovery_status", fun handle_became_leader_resets_recovery_status_test/0},
        {"clears recovery_error", fun handle_became_leader_clears_recovery_error_test/0},
        {"starts health check timer", fun handle_became_leader_starts_health_check_test/0},
        {"cancels old timer", fun handle_became_leader_cancels_old_timer_test/0}
    ].

handle_lost_leadership_test_() ->
    [
        {"clears is_leader flag", fun handle_lost_leadership_clears_leader_flag_test/0},
        {"clears became_leader_time", fun handle_lost_leadership_clears_timestamp_test/0},
        {"resets recovery_status", fun handle_lost_leadership_resets_recovery_status_test/0},
        {"clears health check timer", fun handle_lost_leadership_clears_timer_test/0}
    ].

handle_recovery_complete_test_() ->
    [
        {"success sets recovered status", fun handle_recovery_complete_success_test/0},
        {"failure sets failed status and error", fun handle_recovery_complete_failure_test/0},
        {"preserves other state fields", fun handle_recovery_complete_preserves_other_fields_test/0}
    ].

do_health_check_test_() ->
    [
        {"skipped when not leader", fun do_health_check_not_leader_test/0},
        {"skipped when not recovered", fun do_health_check_not_recovered_test/0},
        {"skipped when idle", fun do_health_check_idle_test/0},
        {"skipped when failed", fun do_health_check_failed_test/0}
    ].
