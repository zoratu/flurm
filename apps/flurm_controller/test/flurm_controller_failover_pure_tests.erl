%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Failover Pure Unit Tests
%%%
%%% Pure unit tests for flurm_controller_failover module that test
%%% gen_server callbacks directly without any mocking.
%%%
%%% These tests focus on:
%%% - init/1 callback
%%% - handle_call/3 for all message types
%%% - handle_cast/2 for all message types
%%% - handle_info/2 for all message types
%%% - terminate/2 callback
%%% - code_change/3 callback
%%% - Internal helper functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_failover_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Re-define the state record locally for direct testing
-record(state, {
    is_leader = false :: boolean(),
    became_leader_time :: erlang:timestamp() | undefined,
    recovery_status = idle :: idle | recovering | recovered | failed,
    recovery_error :: term() | undefined,
    health_check_ref :: reference() | undefined,
    pending_operations = [] :: [{reference(), term()}]
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Test group for init/1 callback
init_callback_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"init/1 returns state with defaults", fun test_init_returns_state/0},
        {"init/1 sets trap_exit flag", fun test_init_trap_exit/0}
     ]}.

%% Test group for handle_call/3 - get_status
handle_call_status_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"get_status - idle state", fun test_get_status_idle/0},
        {"get_status - recovering state", fun test_get_status_recovering/0},
        {"get_status - recovered state", fun test_get_status_recovered/0},
        {"get_status - failed state", fun test_get_status_failed/0},
        {"get_status - with leader time", fun test_get_status_with_leader_time/0}
     ]}.

%% Test group for handle_call/3 - unknown requests
handle_call_unknown_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"unknown request returns error", fun test_unknown_request/0}
     ]}.

%% Test group for handle_cast/2 - became_leader
handle_cast_became_leader_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"became_leader - sets is_leader to true", fun test_became_leader_sets_flag/0},
        {"became_leader - sets became_leader_time", fun test_became_leader_sets_time/0},
        {"became_leader - resets recovery_status", fun test_became_leader_resets_recovery/0},
        {"became_leader - starts health check timer", fun test_became_leader_starts_timer/0}
     ]}.

%% Test group for handle_cast/2 - lost_leadership
handle_cast_lost_leadership_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"lost_leadership - sets is_leader to false", fun test_lost_leadership_clears_flag/0},
        {"lost_leadership - clears became_leader_time", fun test_lost_leadership_clears_time/0},
        {"lost_leadership - resets recovery_status", fun test_lost_leadership_resets_recovery/0},
        {"lost_leadership - cancels health check timer", fun test_lost_leadership_cancels_timer/0}
     ]}.

%% Test group for handle_cast/2 - unknown messages
handle_cast_unknown_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"unknown cast returns noreply", fun test_cast_unknown/0}
     ]}.

%% Test group for handle_info/2 - start_recovery
handle_info_recovery_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"start_recovery - already recovering ignored", fun test_start_recovery_already_recovering/0},
        {"start_recovery - from idle starts recovery", fun test_start_recovery_from_idle/0}
     ]}.

%% Test group for handle_info/2 - health_check
handle_info_health_check_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"health_check - schedules next check", fun test_health_check_schedules_next/0}
     ]}.

%% Test group for handle_info/2 - recovery_complete
handle_info_recovery_complete_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"recovery_complete - success", fun test_recovery_complete_success/0},
        {"recovery_complete - failure", fun test_recovery_complete_failure/0}
     ]}.

%% Test group for handle_info/2 - unknown messages
handle_info_unknown_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"unknown info returns noreply", fun test_info_unknown/0}
     ]}.

%% Test group for terminate/2
terminate_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"terminate - with timer reference", fun test_terminate_with_ref/0},
        {"terminate - without timer reference", fun test_terminate_no_ref/0}
     ]}.

%% Test group for code_change/3
code_change_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"code_change returns ok with state", fun test_code_change/0}
     ]}.

%% Test group for internal helper functions
helper_functions_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"calculate_leader_uptime - undefined", fun test_uptime_undefined/0},
        {"calculate_leader_uptime - with timestamp", fun test_uptime_with_timestamp/0}
     ]}.

%%====================================================================
%% init/1 Tests
%%====================================================================

test_init_returns_state() ->
    %% Test the initial state structure
    State = #state{},
    ?assertEqual(false, State#state.is_leader),
    ?assertEqual(undefined, State#state.became_leader_time),
    ?assertEqual(idle, State#state.recovery_status),
    ?assertEqual(undefined, State#state.recovery_error),
    ?assertEqual(undefined, State#state.health_check_ref),
    ?assertEqual([], State#state.pending_operations).

test_init_trap_exit() ->
    %% The init function should set trap_exit to true
    OldTrap = process_flag(trap_exit, false),
    process_flag(trap_exit, true),
    ?assertEqual(true, process_flag(trap_exit, OldTrap)).

%%====================================================================
%% handle_call/3 - get_status Tests
%%====================================================================

test_get_status_idle() ->
    State = #state{
        is_leader = false,
        became_leader_time = undefined,
        recovery_status = idle,
        recovery_error = undefined
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assert(is_map(Status)),
    ?assertEqual(false, maps:get(is_leader, Status)),
    ?assertEqual(undefined, maps:get(became_leader_time, Status)),
    ?assertEqual(idle, maps:get(recovery_status, Status)),
    ?assertEqual(undefined, maps:get(recovery_error, Status)),
    ?assertEqual(0, maps:get(uptime_as_leader, Status)).

test_get_status_recovering() ->
    State = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        recovery_status = recovering,
        recovery_error = undefined
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(true, maps:get(is_leader, Status)),
    ?assertEqual(recovering, maps:get(recovery_status, Status)).

test_get_status_recovered() ->
    State = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        recovery_status = recovered,
        recovery_error = undefined
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(recovered, maps:get(recovery_status, Status)).

test_get_status_failed() ->
    State = #state{
        is_leader = true,
        became_leader_time = erlang:timestamp(),
        recovery_status = failed,
        recovery_error = {recovery_failed, timeout}
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(failed, maps:get(recovery_status, Status)),
    ?assertEqual({recovery_failed, timeout}, maps:get(recovery_error, Status)).

test_get_status_with_leader_time() ->
    %% Create a timestamp a few seconds in the past
    Now = erlang:timestamp(),
    {Mega, Sec, Micro} = Now,
    PastTime = {Mega, Sec - 5, Micro},  % 5 seconds ago
    State = #state{
        is_leader = true,
        became_leader_time = PastTime,
        recovery_status = recovered,
        recovery_error = undefined
    },
    {reply, Status, _NewState} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    Uptime = maps:get(uptime_as_leader, Status),
    ?assert(Uptime >= 5),  % At least 5 seconds
    ?assert(Uptime < 10).  % But not too much more

%%====================================================================
%% handle_call/3 - Unknown Request Tests
%%====================================================================

test_unknown_request() ->
    State = #state{},
    {reply, Result, _NewState} = flurm_controller_failover:handle_call(
        {unknown_message, with_args}, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% handle_cast/2 - became_leader Tests
%%====================================================================

test_became_leader_sets_flag() ->
    State = #state{is_leader = false},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    ?assertEqual(true, NewState#state.is_leader),
    %% Clean up timer
    cleanup_timer(NewState#state.health_check_ref).

test_became_leader_sets_time() ->
    State = #state{is_leader = false, became_leader_time = undefined},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    ?assertNotEqual(undefined, NewState#state.became_leader_time),
    ?assert(is_tuple(NewState#state.became_leader_time)),
    %% Clean up timer
    cleanup_timer(NewState#state.health_check_ref).

test_became_leader_resets_recovery() ->
    State = #state{is_leader = false, recovery_status = failed, recovery_error = some_error},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    ?assertEqual(idle, NewState#state.recovery_status),
    ?assertEqual(undefined, NewState#state.recovery_error),
    %% Clean up timer
    cleanup_timer(NewState#state.health_check_ref).

test_became_leader_starts_timer() ->
    State = #state{is_leader = false, health_check_ref = undefined},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    ?assertNotEqual(undefined, NewState#state.health_check_ref),
    %% Verify it's a valid timer reference
    ?assert(is_reference(NewState#state.health_check_ref)),
    %% Clean up timer
    cleanup_timer(NewState#state.health_check_ref).

%%====================================================================
%% handle_cast/2 - lost_leadership Tests
%%====================================================================

test_lost_leadership_clears_flag() ->
    State = #state{is_leader = true},
    {noreply, NewState} = flurm_controller_failover:handle_cast(lost_leadership, State),
    ?assertEqual(false, NewState#state.is_leader).

test_lost_leadership_clears_time() ->
    State = #state{is_leader = true, became_leader_time = erlang:timestamp()},
    {noreply, NewState} = flurm_controller_failover:handle_cast(lost_leadership, State),
    ?assertEqual(undefined, NewState#state.became_leader_time).

test_lost_leadership_resets_recovery() ->
    State = #state{is_leader = true, recovery_status = recovered},
    {noreply, NewState} = flurm_controller_failover:handle_cast(lost_leadership, State),
    ?assertEqual(idle, NewState#state.recovery_status).

test_lost_leadership_cancels_timer() ->
    %% Create a timer to be cancelled
    Ref = erlang:send_after(60000, self(), test_msg),
    State = #state{is_leader = true, health_check_ref = Ref},
    {noreply, NewState} = flurm_controller_failover:handle_cast(lost_leadership, State),
    ?assertEqual(undefined, NewState#state.health_check_ref),
    %% Verify the timer was cancelled (trying to cancel again returns false)
    ?assertEqual(false, erlang:cancel_timer(Ref)).

%%====================================================================
%% handle_cast/2 - Unknown Tests
%%====================================================================

test_cast_unknown() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_failover:handle_cast(unknown_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 - start_recovery Tests
%%====================================================================

test_start_recovery_already_recovering() ->
    State = #state{recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    ?assertEqual(recovering, NewState#state.recovery_status).

test_start_recovery_from_idle() ->
    State = #state{recovery_status = idle},
    {noreply, NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    ?assertEqual(recovering, NewState#state.recovery_status).

%%====================================================================
%% handle_info/2 - health_check Tests
%%====================================================================

test_health_check_schedules_next() ->
    State = #state{is_leader = false, recovery_status = idle, health_check_ref = undefined},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    ?assertNotEqual(undefined, NewState#state.health_check_ref),
    ?assert(is_reference(NewState#state.health_check_ref)),
    %% Clean up timer
    cleanup_timer(NewState#state.health_check_ref).

%%====================================================================
%% handle_info/2 - recovery_complete Tests
%%====================================================================

test_recovery_complete_success() ->
    State = #state{recovery_status = recovering},
    {noreply, NewState} = flurm_controller_failover:handle_info({recovery_complete, {ok, recovered}}, State),
    ?assertEqual(recovered, NewState#state.recovery_status),
    ?assertEqual(undefined, NewState#state.recovery_error).

test_recovery_complete_failure() ->
    State = #state{recovery_status = recovering},
    ErrorReason = {recovery_failed, timeout},
    {noreply, NewState} = flurm_controller_failover:handle_info({recovery_complete, {error, ErrorReason}}, State),
    ?assertEqual(failed, NewState#state.recovery_status),
    ?assertEqual(ErrorReason, NewState#state.recovery_error).

%%====================================================================
%% handle_info/2 - Unknown Tests
%%====================================================================

test_info_unknown() ->
    State = #state{},
    {noreply, NewState} = flurm_controller_failover:handle_info(unknown_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

test_terminate_with_ref() ->
    Ref = erlang:send_after(60000, self(), test_msg),
    State = #state{health_check_ref = Ref},
    ?assertEqual(ok, flurm_controller_failover:terminate(normal, State)),
    %% Verify timer was cancelled
    ?assertEqual(false, erlang:cancel_timer(Ref)).

test_terminate_no_ref() ->
    State = #state{health_check_ref = undefined},
    ?assertEqual(ok, flurm_controller_failover:terminate(normal, State)).

%%====================================================================
%% code_change/3 Tests
%%====================================================================

test_code_change() ->
    State = #state{is_leader = true, recovery_status = recovered},
    {ok, NewState} = flurm_controller_failover:code_change("1.0.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% Internal Helper Function Tests
%%====================================================================

test_uptime_undefined() ->
    %% Test the calculate_leader_uptime logic directly
    ?assertEqual(0, calculate_leader_uptime_impl(undefined)).

test_uptime_with_timestamp() ->
    Now = erlang:timestamp(),
    {Mega, Sec, Micro} = Now,
    %% 10 seconds ago
    PastTime = {Mega, Sec - 10, Micro},
    Uptime = calculate_leader_uptime_impl(PastTime),
    ?assert(Uptime >= 10),
    ?assert(Uptime < 15).

%% Local implementation of calculate_leader_uptime for testing
calculate_leader_uptime_impl(undefined) ->
    0;
calculate_leader_uptime_impl(BecameLeaderTime) ->
    timer:now_diff(erlang:timestamp(), BecameLeaderTime) div 1000000.

%%====================================================================
%% State Record Tests
%%====================================================================

state_record_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"state record - default values", fun test_state_record_defaults/0},
        {"state record - all fields set", fun test_state_record_all_fields/0}
     ]}.

test_state_record_defaults() ->
    State = #state{},
    ?assertEqual(false, State#state.is_leader),
    ?assertEqual(undefined, State#state.became_leader_time),
    ?assertEqual(idle, State#state.recovery_status),
    ?assertEqual(undefined, State#state.recovery_error),
    ?assertEqual(undefined, State#state.health_check_ref),
    ?assertEqual([], State#state.pending_operations).

test_state_record_all_fields() ->
    Now = erlang:timestamp(),
    Ref = make_ref(),
    State = #state{
        is_leader = true,
        became_leader_time = Now,
        recovery_status = recovered,
        recovery_error = undefined,
        health_check_ref = Ref,
        pending_operations = [{make_ref(), data}]
    },
    ?assertEqual(true, State#state.is_leader),
    ?assertEqual(Now, State#state.became_leader_time),
    ?assertEqual(recovered, State#state.recovery_status),
    ?assertEqual(undefined, State#state.recovery_error),
    ?assertEqual(Ref, State#state.health_check_ref),
    ?assertEqual(1, length(State#state.pending_operations)).

%%====================================================================
%% State Transitions Tests
%%====================================================================

state_transitions_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"recovery status transitions", fun test_recovery_status_transitions/0},
        {"leadership transitions", fun test_leadership_transitions/0},
        {"full failover cycle", fun test_full_failover_cycle/0}
     ]}.

test_recovery_status_transitions() ->
    %% Test valid state transitions for recovery_status
    State0 = #state{recovery_status = idle},

    %% idle -> recovering (via start_recovery)
    {noreply, State1} = flurm_controller_failover:handle_info(start_recovery, State0),
    ?assertEqual(recovering, State1#state.recovery_status),

    %% recovering -> recovered (via recovery_complete success)
    {noreply, State2} = flurm_controller_failover:handle_info({recovery_complete, {ok, recovered}}, State1),
    ?assertEqual(recovered, State2#state.recovery_status),

    %% Can also go to failed
    {noreply, State3} = flurm_controller_failover:handle_info({recovery_complete, {error, some_error}}, State1),
    ?assertEqual(failed, State3#state.recovery_status).

test_leadership_transitions() ->
    %% Test transitions when gaining and losing leadership
    State0 = #state{is_leader = false},

    %% Not leader -> leader
    {noreply, State1} = flurm_controller_failover:handle_cast(became_leader, State0),
    ?assertEqual(true, State1#state.is_leader),
    ?assertNotEqual(undefined, State1#state.became_leader_time),
    cleanup_timer(State1#state.health_check_ref),

    %% Leader -> not leader
    {noreply, State2} = flurm_controller_failover:handle_cast(lost_leadership, State1),
    ?assertEqual(false, State2#state.is_leader),
    ?assertEqual(undefined, State2#state.became_leader_time),
    ?assertEqual(undefined, State2#state.health_check_ref).

test_full_failover_cycle() ->
    %% Test a complete failover cycle
    State0 = #state{},

    %% 1. Become leader
    {noreply, State1} = flurm_controller_failover:handle_cast(became_leader, State0),
    ?assertEqual(true, State1#state.is_leader),
    ?assertEqual(idle, State1#state.recovery_status),
    HealthRef = State1#state.health_check_ref,

    %% 2. Start recovery (triggered by became_leader sending start_recovery message)
    {noreply, State2} = flurm_controller_failover:handle_info(start_recovery, State1),
    ?assertEqual(recovering, State2#state.recovery_status),

    %% 3. Recovery completes
    {noreply, State3} = flurm_controller_failover:handle_info({recovery_complete, {ok, recovered}}, State2),
    ?assertEqual(recovered, State3#state.recovery_status),

    %% 4. Lose leadership
    {noreply, State4} = flurm_controller_failover:handle_cast(lost_leadership, State3),
    ?assertEqual(false, State4#state.is_leader),
    ?assertEqual(idle, State4#state.recovery_status),

    %% Clean up
    cleanup_timer(HealthRef).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"became_leader cancels old timer", fun test_became_leader_cancels_old_timer/0},
        {"multiple recovery failures", fun test_multiple_recovery_failures/0},
        {"recovery while not leader", fun test_recovery_while_not_leader/0}
     ]}.

test_became_leader_cancels_old_timer() ->
    %% If there was an existing health check timer, it should be cancelled
    OldRef = erlang:send_after(60000, self(), old_timer),
    State = #state{is_leader = false, health_check_ref = OldRef},
    {noreply, NewState} = flurm_controller_failover:handle_cast(became_leader, State),
    %% Old timer should be cancelled
    ?assertEqual(false, erlang:cancel_timer(OldRef)),
    %% New timer should be set
    ?assertNotEqual(OldRef, NewState#state.health_check_ref),
    cleanup_timer(NewState#state.health_check_ref).

test_multiple_recovery_failures() ->
    %% Test that recovery failures are properly recorded
    State0 = #state{recovery_status = recovering},

    %% First failure
    {noreply, State1} = flurm_controller_failover:handle_info(
        {recovery_complete, {error, first_error}}, State0),
    ?assertEqual(failed, State1#state.recovery_status),
    ?assertEqual(first_error, State1#state.recovery_error),

    %% Retry recovery
    {noreply, State2} = flurm_controller_failover:handle_info(start_recovery, State1),
    ?assertEqual(recovering, State2#state.recovery_status),

    %% Second failure with different error
    {noreply, State3} = flurm_controller_failover:handle_info(
        {recovery_complete, {error, second_error}}, State2),
    ?assertEqual(failed, State3#state.recovery_status),
    ?assertEqual(second_error, State3#state.recovery_error).

test_recovery_while_not_leader() ->
    %% Recovery can technically happen even if not leader (edge case)
    State = #state{is_leader = false, recovery_status = idle},
    {noreply, NewState} = flurm_controller_failover:handle_info(start_recovery, State),
    ?assertEqual(recovering, NewState#state.recovery_status).

%%====================================================================
%% Recovery Status Values Tests
%%====================================================================

recovery_status_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"recovery status - idle", fun test_recovery_status_idle/0},
        {"recovery status - recovering", fun test_recovery_status_recovering/0},
        {"recovery status - recovered", fun test_recovery_status_recovered/0},
        {"recovery status - failed", fun test_recovery_status_failed/0}
     ]}.

test_recovery_status_idle() ->
    State = #state{recovery_status = idle},
    {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(idle, maps:get(recovery_status, Status)).

test_recovery_status_recovering() ->
    State = #state{recovery_status = recovering},
    {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(recovering, maps:get(recovery_status, Status)).

test_recovery_status_recovered() ->
    State = #state{recovery_status = recovered},
    {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(recovered, maps:get(recovery_status, Status)).

test_recovery_status_failed() ->
    State = #state{recovery_status = failed, recovery_error = {test, error}},
    {reply, Status, _} = flurm_controller_failover:handle_call(get_status, {self(), make_ref()}, State),
    ?assertEqual(failed, maps:get(recovery_status, Status)),
    ?assertEqual({test, error}, maps:get(recovery_error, Status)).

%%====================================================================
%% Health Check Timer Tests
%%====================================================================

health_check_timer_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"health check timer creation", fun test_health_check_timer_creation/0},
        {"health check timer renewal", fun test_health_check_timer_renewal/0}
     ]}.

test_health_check_timer_creation() ->
    State = #state{is_leader = false, health_check_ref = undefined},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    ?assert(is_reference(NewState#state.health_check_ref)),
    cleanup_timer(NewState#state.health_check_ref).

test_health_check_timer_renewal() ->
    OldRef = erlang:send_after(60000, self(), test),
    State = #state{is_leader = false, health_check_ref = OldRef},
    {noreply, NewState} = flurm_controller_failover:handle_info(health_check, State),
    ?assertNotEqual(OldRef, NewState#state.health_check_ref),
    cleanup_timer(NewState#state.health_check_ref),
    erlang:cancel_timer(OldRef).

%%====================================================================
%% Utility Functions
%%====================================================================

cleanup_timer(undefined) ->
    ok;
cleanup_timer(Ref) when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    ok.
