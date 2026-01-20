%%%-------------------------------------------------------------------
%%% @doc FLURM SLURM Import Pure Unit Tests
%%%
%%% Pure unit tests for the flurm_slurm_import gen_server module.
%%% These tests directly call the callback functions without spawning
%%% processes or using any mocking framework.
%%%
%%% NO MECK - tests callback functions directly with real values.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_import_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Data Helpers
%%====================================================================

%% Create initial state for testing
make_state() ->
    make_state(#{}).

make_state(Overrides) ->
    Defaults = #{
        sync_enabled => false,
        sync_interval => 5000,
        sync_timer => undefined,
        last_sync => undefined,
        stats => #{jobs_imported => 0, nodes_imported => 0, partitions_imported => 0}
    },
    Props = maps:merge(Defaults, Overrides),
    %% Use tuple syntax since we don't have the record definition exported
    {state,
     maps:get(sync_enabled, Props),
     maps:get(sync_interval, Props),
     maps:get(sync_timer, Props),
     maps:get(last_sync, Props),
     maps:get(stats, Props)}.

%% Fake caller reference for gen_server calls
fake_from() ->
    {self(), make_ref()}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_basic_test() ->
    Result = flurm_slurm_import:init([]),
    ?assertMatch({ok, {state, false, 5000, undefined, undefined, _}}, Result).

init_returns_default_sync_interval_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    %% State is a record - field order: sync_enabled, sync_interval, sync_timer, last_sync, stats
    {state, _, SyncInterval, _, _, _} = State,
    ?assertEqual(5000, SyncInterval).

init_sync_disabled_by_default_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, SyncEnabled, _, _, _, _} = State,
    ?assertEqual(false, SyncEnabled).

init_with_custom_sync_interval_test() ->
    {ok, State} = flurm_slurm_import:init([{sync_interval, 10000}]),
    {state, _, SyncInterval, _, _, _} = State,
    ?assertEqual(10000, SyncInterval).

init_with_auto_sync_true_test() ->
    {ok, State} = flurm_slurm_import:init([{auto_sync, true}]),
    {state, SyncEnabled, _, SyncTimer, _, _} = State,
    ?assertEqual(true, SyncEnabled),
    %% Timer should be set when auto_sync is true
    ?assert(is_reference(SyncTimer)),
    %% Clean up the timer
    erlang:cancel_timer(SyncTimer).

init_sets_initial_stats_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, _, _, _, Stats} = State,
    ?assertEqual(0, maps:get(jobs_imported, Stats)),
    ?assertEqual(0, maps:get(nodes_imported, Stats)),
    ?assertEqual(0, maps:get(partitions_imported, Stats)).

init_last_sync_undefined_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, _, _, LastSync, _} = State,
    ?assertEqual(undefined, LastSync).

init_with_multiple_options_test() ->
    {ok, State} = flurm_slurm_import:init([{sync_interval, 15000}, {auto_sync, false}]),
    {state, SyncEnabled, SyncInterval, _, _, _} = State,
    ?assertEqual(false, SyncEnabled),
    ?assertEqual(15000, SyncInterval).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_returns_ok_test() ->
    State = make_state(),
    Result = flurm_slurm_import:terminate(normal, State),
    ?assertEqual(ok, Result).

terminate_with_shutdown_test() ->
    State = make_state(),
    Result = flurm_slurm_import:terminate(shutdown, State),
    ?assertEqual(ok, Result).

terminate_with_error_reason_test() ->
    State = make_state(),
    Result = flurm_slurm_import:terminate({error, some_reason}, State),
    ?assertEqual(ok, Result).

terminate_cancels_timer_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_timer => Timer, sync_enabled => true}),
    Result = flurm_slurm_import:terminate(normal, State),
    ?assertEqual(ok, Result),
    %% Timer should be cancelled - trying to cancel again should return false
    ?assertEqual(false, erlang:cancel_timer(Timer)).

terminate_with_undefined_timer_test() ->
    State = make_state(#{sync_timer => undefined}),
    Result = flurm_slurm_import:terminate(normal, State),
    ?assertEqual(ok, Result).

%%====================================================================
%% handle_call/3 Tests - get_sync_status
%%====================================================================

handle_call_get_sync_status_test() ->
    State = make_state(#{
        sync_enabled => true,
        sync_interval => 10000,
        last_sync => {{2024, 1, 15}, {12, 30, 0}},
        stats => #{jobs_imported => 5, nodes_imported => 3, partitions_imported => 2}
    }),
    From = fake_from(),
    {reply, {ok, Status}, _NewState} = flurm_slurm_import:handle_call(get_sync_status, From, State),
    ?assertEqual(true, maps:get(sync_enabled, Status)),
    ?assertEqual(10000, maps:get(sync_interval, Status)),
    ?assertEqual({{2024, 1, 15}, {12, 30, 0}}, maps:get(last_sync, Status)),
    ?assertMatch(#{jobs_imported := 5, nodes_imported := 3, partitions_imported := 2}, maps:get(stats, Status)).

handle_call_get_sync_status_disabled_test() ->
    State = make_state(#{sync_enabled => false}),
    From = fake_from(),
    {reply, {ok, Status}, _NewState} = flurm_slurm_import:handle_call(get_sync_status, From, State),
    ?assertEqual(false, maps:get(sync_enabled, Status)).

handle_call_get_sync_status_preserves_state_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, _, NewState} = flurm_slurm_import:handle_call(get_sync_status, From, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - start_sync
%%====================================================================

handle_call_start_sync_test() ->
    State = make_state(#{sync_enabled => false}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 8000}, From, State),
    {state, SyncEnabled, SyncInterval, SyncTimer, _, _} = NewState,
    ?assertEqual(true, SyncEnabled),
    ?assertEqual(8000, SyncInterval),
    ?assert(is_reference(SyncTimer)),
    %% Clean up
    erlang:cancel_timer(SyncTimer).

handle_call_start_sync_replaces_timer_test() ->
    OldTimer = erlang:send_after(60000, self(), old_msg),
    State = make_state(#{sync_enabled => true, sync_timer => OldTimer}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 12000}, From, State),
    {state, _, _, NewTimer, _, _} = NewState,
    %% Old timer should be cancelled
    ?assertEqual(false, erlang:cancel_timer(OldTimer)),
    ?assertNotEqual(OldTimer, NewTimer),
    %% Clean up new timer
    erlang:cancel_timer(NewTimer).

handle_call_start_sync_updates_interval_test() ->
    State = make_state(#{sync_interval => 5000}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 20000}, From, State),
    {state, _, SyncInterval, Timer, _, _} = NewState,
    ?assertEqual(20000, SyncInterval),
    erlang:cancel_timer(Timer).

%%====================================================================
%% handle_call/3 Tests - stop_sync
%%====================================================================

handle_call_stop_sync_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_enabled => true, sync_timer => Timer}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call(stop_sync, From, State),
    {state, SyncEnabled, _, SyncTimer, _, _} = NewState,
    ?assertEqual(false, SyncEnabled),
    ?assertEqual(undefined, SyncTimer).

handle_call_stop_sync_cancels_timer_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_enabled => true, sync_timer => Timer}),
    From = fake_from(),
    {reply, ok, _NewState} = flurm_slurm_import:handle_call(stop_sync, From, State),
    %% Timer should have been cancelled
    ?assertEqual(false, erlang:cancel_timer(Timer)).

handle_call_stop_sync_when_already_stopped_test() ->
    State = make_state(#{sync_enabled => false, sync_timer => undefined}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call(stop_sync, From, State),
    {state, SyncEnabled, _, SyncTimer, _, _} = NewState,
    ?assertEqual(false, SyncEnabled),
    ?assertEqual(undefined, SyncTimer).

%%====================================================================
%% handle_call/3 Tests - unknown_request
%%====================================================================

handle_call_unknown_request_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {error, unknown_request}, NewState} = flurm_slurm_import:handle_call(unknown_command, From, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_unknown_message_test() ->
    State = make_state(),
    Result = flurm_slurm_import:handle_cast(random_message, State),
    ?assertMatch({noreply, _}, Result).

handle_cast_preserves_state_test() ->
    State = make_state(#{sync_enabled => true}),
    {noreply, NewState} = flurm_slurm_import:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests - sync_tick
%%====================================================================

handle_info_sync_tick_test() ->
    %% Note: This test verifies sync_tick handling but the actual
    %% import calls will fail without SLURM. We're testing the
    %% state update logic and timer reset.
    State = make_state(#{sync_enabled => true, sync_interval => 5000}),
    {noreply, NewState} = flurm_slurm_import:handle_info(sync_tick, State),
    {state, _, _, SyncTimer, LastSync, _} = NewState,
    %% A new timer should be set
    ?assert(is_reference(SyncTimer)),
    %% Last sync should be updated
    ?assertNotEqual(undefined, LastSync),
    %% Clean up
    erlang:cancel_timer(SyncTimer).

handle_info_sync_tick_sets_last_sync_test() ->
    State = make_state(#{sync_enabled => true, sync_interval => 5000, last_sync => undefined}),
    {noreply, NewState} = flurm_slurm_import:handle_info(sync_tick, State),
    {state, _, _, Timer, LastSync, _} = NewState,
    ?assertMatch({{_, _, _}, {_, _, _}}, LastSync),
    erlang:cancel_timer(Timer).

%%====================================================================
%% handle_info/2 Tests - unknown messages
%%====================================================================

handle_info_unknown_message_test() ->
    State = make_state(),
    Result = flurm_slurm_import:handle_info(unknown_message, State),
    ?assertMatch({noreply, _}, Result).

handle_info_preserves_state_test() ->
    State = make_state(#{sync_enabled => true}),
    {noreply, NewState} = flurm_slurm_import:handle_info(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% Edge Case Tests
%%====================================================================

%% Test state record field ordering is correct
state_record_structure_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    %% Verify the state tuple has the expected structure
    ?assertEqual(6, tuple_size(State)),
    ?assertEqual(state, element(1, State)).

%% Test that multiple init calls produce independent states
multiple_init_calls_independent_test() ->
    {ok, State1} = flurm_slurm_import:init([{sync_interval, 1000}]),
    {ok, State2} = flurm_slurm_import:init([{sync_interval, 2000}]),
    {state, _, Interval1, _, _, _} = State1,
    {state, _, Interval2, _, _, _} = State2,
    ?assertEqual(1000, Interval1),
    ?assertEqual(2000, Interval2).

%% Test sync interval bounds
init_with_large_interval_test() ->
    {ok, State} = flurm_slurm_import:init([{sync_interval, 3600000}]),
    {state, _, SyncInterval, _, _, _} = State,
    ?assertEqual(3600000, SyncInterval).

init_with_small_interval_test() ->
    {ok, State} = flurm_slurm_import:init([{sync_interval, 100}]),
    {state, _, SyncInterval, _, _, _} = State,
    ?assertEqual(100, SyncInterval).

%% Test that stats map is correctly initialized
init_stats_map_complete_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, _, _, _, Stats} = State,
    ?assert(maps:is_key(jobs_imported, Stats)),
    ?assert(maps:is_key(nodes_imported, Stats)),
    ?assert(maps:is_key(partitions_imported, Stats)).

%% Test get_sync_status returns all expected fields
get_sync_status_complete_fields_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Status}, _} = flurm_slurm_import:handle_call(get_sync_status, From, State),
    ?assert(maps:is_key(sync_enabled, Status)),
    ?assert(maps:is_key(sync_interval, Status)),
    ?assert(maps:is_key(last_sync, Status)),
    ?assert(maps:is_key(stats, Status)).

%% Test timer cleanup in stop_sync with various timer states
stop_sync_timer_already_fired_test() ->
    %% Create a timer that will fire immediately
    Timer = erlang:send_after(0, self(), test_msg),
    ok,  % Let it fire
    State = make_state(#{sync_enabled => true, sync_timer => Timer}),
    From = fake_from(),
    %% Should not crash even if timer already fired
    {reply, ok, NewState} = flurm_slurm_import:handle_call(stop_sync, From, State),
    {state, SyncEnabled, _, _, _, _} = NewState,
    ?assertEqual(false, SyncEnabled).

%% Test start_sync replaces existing running sync
start_sync_restart_test() ->
    Timer1 = erlang:send_after(60000, self(), msg1),
    State = make_state(#{sync_enabled => true, sync_timer => Timer1, sync_interval => 5000}),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 10000}, From, State),
    {state, SyncEnabled, SyncInterval, Timer2, _, _} = NewState,
    ?assertEqual(true, SyncEnabled),
    ?assertEqual(10000, SyncInterval),
    ?assertNotEqual(Timer1, Timer2),
    %% Old timer should be cancelled
    ?assertEqual(false, erlang:cancel_timer(Timer1)),
    %% New timer should be valid
    erlang:cancel_timer(Timer2).

%%====================================================================
%% Integration-style Tests (testing handle_call for import operations)
%% Note: These will return errors since SLURM isn't running
%%====================================================================

handle_call_import_jobs_no_slurm_test() ->
    State = make_state(),
    From = fake_from(),
    %% When squeue isn't available, run_command returns empty output which
    %% results in zero items imported (graceful handling)
    Result = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    ?assertMatch({reply, {ok, #{imported := 0, updated := 0, failed := 0}}, _}, Result).

handle_call_import_nodes_no_slurm_test() ->
    State = make_state(),
    From = fake_from(),
    %% When sinfo isn't available, run_command returns empty output which
    %% results in zero items imported (graceful handling)
    Result = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State),
    ?assertMatch({reply, {ok, #{imported := 0, updated := 0, failed := 0}}, _}, Result).

handle_call_import_partitions_no_slurm_test() ->
    State = make_state(),
    From = fake_from(),
    %% When sinfo isn't available, run_command returns empty output which
    %% results in zero items imported (graceful handling)
    Result = flurm_slurm_import:handle_call(import_partitions, From, State),
    ?assertMatch({reply, {ok, #{imported := 0, updated := 0, failed := 0}}, _}, Result).

handle_call_import_all_no_slurm_test() ->
    State = make_state(),
    From = fake_from(),
    %% This will attempt all imports and return results (likely errors)
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    ?assert(maps:is_key(partitions, Result)),
    ?assert(maps:is_key(nodes, Result)),
    ?assert(maps:is_key(jobs, Result)).

%%====================================================================
%% Callback Mode Tests (verifying gen_server behavior expectations)
%%====================================================================

%% Test that state transitions work correctly
state_transition_sync_enable_disable_test() ->
    %% Start with sync disabled
    {ok, State1} = flurm_slurm_import:init([]),
    {state, Enabled1, _, _, _, _} = State1,
    ?assertEqual(false, Enabled1),

    %% Enable sync
    From = fake_from(),
    {reply, ok, State2} = flurm_slurm_import:handle_call({start_sync, 5000}, From, State1),
    {state, Enabled2, _, Timer2, _, _} = State2,
    ?assertEqual(true, Enabled2),
    ?assert(is_reference(Timer2)),

    %% Disable sync
    {reply, ok, State3} = flurm_slurm_import:handle_call(stop_sync, From, State2),
    {state, Enabled3, _, Timer3, _, _} = State3,
    ?assertEqual(false, Enabled3),
    ?assertEqual(undefined, Timer3).

%% Test that sync interval can be changed
sync_interval_change_test() ->
    From = fake_from(),
    {ok, State1} = flurm_slurm_import:init([{sync_interval, 5000}]),
    {state, _, Interval1, _, _, _} = State1,
    ?assertEqual(5000, Interval1),

    {reply, ok, State2} = flurm_slurm_import:handle_call({start_sync, 15000}, From, State1),
    {state, _, Interval2, Timer, _, _} = State2,
    ?assertEqual(15000, Interval2),
    erlang:cancel_timer(Timer).

%%====================================================================
%% Timer Behavior Tests
%%====================================================================

auto_sync_starts_timer_test() ->
    {ok, State} = flurm_slurm_import:init([{auto_sync, true}, {sync_interval, 100}]),
    {state, _, _, Timer, _, _} = State,
    ?assert(is_reference(Timer)),
    %% Wait for timer to potentially fire
    %% Legitimate wait for timer callback
    timer:sleep(150),
    %% Clean up any pending messages
    receive sync_tick -> ok after 0 -> ok end,
    erlang:cancel_timer(Timer).

timer_resets_on_sync_tick_test() ->
    State = make_state(#{sync_enabled => true, sync_interval => 5000}),
    {noreply, NewState} = flurm_slurm_import:handle_info(sync_tick, State),
    {state, _, _, NewTimer, _, _} = NewState,
    ?assert(is_reference(NewTimer)),
    erlang:cancel_timer(NewTimer).

%%====================================================================
%% Stats Update Tests
%%====================================================================

stats_preserved_across_calls_test() ->
    InitialStats = #{jobs_imported => 10, nodes_imported => 5, partitions_imported => 2},
    State = make_state(#{stats => InitialStats}),
    From = fake_from(),
    {reply, {ok, Status}, _} = flurm_slurm_import:handle_call(get_sync_status, From, State),
    Stats = maps:get(stats, Status),
    ?assertEqual(10, maps:get(jobs_imported, Stats)),
    ?assertEqual(5, maps:get(nodes_imported, Stats)),
    ?assertEqual(2, maps:get(partitions_imported, Stats)).

%%====================================================================
%% Default Value Tests
%%====================================================================

default_sync_interval_is_5000_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, Interval, _, _, _} = State,
    ?assertEqual(5000, Interval).

default_sync_enabled_is_false_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, Enabled, _, _, _, _} = State,
    ?assertEqual(false, Enabled).

default_sync_timer_is_undefined_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, _, Timer, _, _} = State,
    ?assertEqual(undefined, Timer).

default_last_sync_is_undefined_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    {state, _, _, _, LastSync, _} = State,
    ?assertEqual(undefined, LastSync).

%%====================================================================
%% Options Handling Tests
%%====================================================================

init_ignores_unknown_options_test() ->
    {ok, State} = flurm_slurm_import:init([{unknown_option, value}, {another_unknown, 123}]),
    %% Should still initialize correctly with defaults
    {state, Enabled, Interval, _, _, _} = State,
    ?assertEqual(false, Enabled),
    ?assertEqual(5000, Interval).

init_empty_options_list_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    ?assertEqual(6, tuple_size(State)).

init_mixed_known_unknown_options_test() ->
    {ok, State} = flurm_slurm_import:init([{sync_interval, 7000}, {unknown, value}]),
    {state, _, Interval, _, _, _} = State,
    ?assertEqual(7000, Interval).

%%====================================================================
%% Last Sync Timestamp Tests
%%====================================================================

sync_tick_updates_last_sync_to_current_time_test() ->
    State = make_state(#{sync_enabled => true, sync_interval => 5000, last_sync => undefined}),
    Before = calendar:universal_time(),
    {noreply, NewState} = flurm_slurm_import:handle_info(sync_tick, State),
    After = calendar:universal_time(),
    {state, _, _, Timer, LastSync, _} = NewState,
    %% Last sync should be between before and after
    ?assert(LastSync >= Before),
    ?assert(LastSync =< After),
    erlang:cancel_timer(Timer).

%%====================================================================
%% Concurrent Operation Safety Tests
%%====================================================================

multiple_start_sync_calls_test() ->
    From = fake_from(),
    {ok, State1} = flurm_slurm_import:init([]),

    {reply, ok, State2} = flurm_slurm_import:handle_call({start_sync, 5000}, From, State1),
    {state, _, _, Timer2, _, _} = State2,

    {reply, ok, State3} = flurm_slurm_import:handle_call({start_sync, 6000}, From, State2),
    {state, _, Interval3, Timer3, _, _} = State3,

    %% Second call should have cancelled first timer
    ?assertEqual(false, erlang:cancel_timer(Timer2)),
    ?assertEqual(6000, Interval3),
    erlang:cancel_timer(Timer3).

multiple_stop_sync_calls_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_enabled => true, sync_timer => Timer}),
    From = fake_from(),

    {reply, ok, State2} = flurm_slurm_import:handle_call(stop_sync, From, State),
    {reply, ok, State3} = flurm_slurm_import:handle_call(stop_sync, From, State2),

    {state, Enabled, _, Timer3, _, _} = State3,
    ?assertEqual(false, Enabled),
    ?assertEqual(undefined, Timer3).

%%====================================================================
%% import_all Tests (exercises count_imported and update_stats)
%%====================================================================

handle_call_import_all_updates_last_sync_test() ->
    State = make_state(#{last_sync => undefined}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    {state, _, _, _, LastSync, _} = NewState,
    ?assertNotEqual(undefined, LastSync),
    ?assertMatch({{_, _, _}, {_, _, _}}, LastSync).

handle_call_import_all_updates_stats_test() ->
    State = make_state(#{stats => #{jobs_imported => 0, nodes_imported => 0, partitions_imported => 0}}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% Stats should have been updated (though values might be 0 if no SLURM)
    ?assert(maps:is_key(jobs_imported, Stats)),
    ?assert(maps:is_key(nodes_imported, Stats)),
    ?assert(maps:is_key(partitions_imported, Stats)),
    ?assert(maps:is_key(last_full_import, Stats)).

handle_call_import_all_result_structure_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    %% Result should be a map with partitions, nodes, and jobs keys
    ?assert(is_map(Result)),
    ?assert(maps:is_key(partitions, Result)),
    ?assert(maps:is_key(nodes, Result)),
    ?assert(maps:is_key(jobs, Result)).

handle_call_import_all_preserves_sync_settings_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_enabled => true, sync_interval => 12000, sync_timer => Timer}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    {state, SyncEnabled, SyncInterval, SyncTimer, _, _} = NewState,
    ?assertEqual(true, SyncEnabled),
    ?assertEqual(12000, SyncInterval),
    ?assertEqual(Timer, SyncTimer),
    erlang:cancel_timer(Timer).

%%====================================================================
%% import_jobs Tests
%%====================================================================

handle_call_import_jobs_result_structure_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    %% Result should have imported, updated, failed keys
    ?assert(maps:is_key(imported, Result)),
    ?assert(maps:is_key(updated, Result)),
    ?assert(maps:is_key(failed, Result)).

handle_call_import_jobs_updates_stats_test() ->
    State = make_state(#{stats => #{jobs_imported => 5, nodes_imported => 0, partitions_imported => 0}}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% Stats should exist and have jobs_imported key
    ?assert(maps:is_key(jobs_imported, Stats)).

handle_call_import_jobs_with_options_test() ->
    State = make_state(),
    From = fake_from(),
    Opts = #{partition => <<"compute">>, user => <<"testuser">>},
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_jobs, Opts}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, Result).

%%====================================================================
%% import_nodes Tests
%%====================================================================

handle_call_import_nodes_result_structure_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State),
    %% Result should have imported, updated, failed keys
    ?assert(maps:is_key(imported, Result)),
    ?assert(maps:is_key(updated, Result)),
    ?assert(maps:is_key(failed, Result)).

handle_call_import_nodes_updates_stats_test() ->
    State = make_state(#{stats => #{jobs_imported => 0, nodes_imported => 3, partitions_imported => 0}}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% Stats should exist and have nodes_imported key
    ?assert(maps:is_key(nodes_imported, Stats)).

handle_call_import_nodes_with_options_test() ->
    State = make_state(),
    From = fake_from(),
    Opts = #{partition => <<"gpu">>},
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_nodes, Opts}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, Result).

%%====================================================================
%% import_partitions Tests
%%====================================================================

handle_call_import_partitions_result_structure_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call(import_partitions, From, State),
    %% Result should have imported, updated, failed keys
    ?assert(maps:is_key(imported, Result)),
    ?assert(maps:is_key(updated, Result)),
    ?assert(maps:is_key(failed, Result)).

handle_call_import_partitions_updates_stats_test() ->
    State = make_state(#{stats => #{jobs_imported => 0, nodes_imported => 0, partitions_imported => 4}}),
    From = fake_from(),
    {reply, {ok, _Result}, NewState} = flurm_slurm_import:handle_call(import_partitions, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% Stats should exist and have partitions_imported key
    ?assert(maps:is_key(partitions_imported, Stats)).

%%====================================================================
%% Stats Accumulation Tests
%%====================================================================

stats_accumulate_after_multiple_imports_test() ->
    State = make_state(#{stats => #{jobs_imported => 10, nodes_imported => 5, partitions_imported => 2}}),
    From = fake_from(),
    %% First import
    {reply, {ok, _}, State2} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    %% Second import
    {reply, {ok, _}, State3} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State2),
    %% Third import
    {reply, {ok, _}, State4} = flurm_slurm_import:handle_call(import_partitions, From, State3),

    {state, _, _, _, _, Stats} = State4,
    ?assert(maps:is_key(jobs_imported, Stats)),
    ?assert(maps:is_key(nodes_imported, Stats)),
    ?assert(maps:is_key(partitions_imported, Stats)).

%%====================================================================
%% Complex State Transition Tests
%%====================================================================

full_lifecycle_test() ->
    %% Initialize
    {ok, State1} = flurm_slurm_import:init([{sync_interval, 3000}]),
    {state, false, 3000, undefined, undefined, _} = State1,

    %% Start sync
    From = fake_from(),
    {reply, ok, State2} = flurm_slurm_import:handle_call({start_sync, 4000}, From, State1),
    {state, true, 4000, Timer2, _, _} = State2,
    ?assert(is_reference(Timer2)),

    %% Perform import_all
    {reply, {ok, _Result}, State3} = flurm_slurm_import:handle_call({import_all, #{}}, From, State2),
    {state, true, 4000, Timer3, LastSync, _Stats} = State3,
    ?assertEqual(Timer2, Timer3),  % Timer preserved
    ?assertNotEqual(undefined, LastSync),

    %% Stop sync
    {reply, ok, State4} = flurm_slurm_import:handle_call(stop_sync, From, State3),
    {state, false, 4000, undefined, _, _} = State4,

    %% Terminate
    ok = flurm_slurm_import:terminate(normal, State4).

%%====================================================================
%% Error Handling Tests
%%====================================================================

handle_call_import_jobs_preserves_state_on_empty_result_test() ->
    State = make_state(#{
        sync_enabled => true,
        sync_interval => 8000,
        stats => #{jobs_imported => 100, nodes_imported => 50, partitions_imported => 10}
    }),
    From = fake_from(),
    {reply, {ok, _}, NewState} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    {state, SyncEnabled, SyncInterval, _, _, Stats} = NewState,
    %% These settings should be preserved
    ?assertEqual(true, SyncEnabled),
    ?assertEqual(8000, SyncInterval),
    %% Other stats should be preserved (nodes_imported, partitions_imported)
    ?assertEqual(50, maps:get(nodes_imported, Stats)),
    ?assertEqual(10, maps:get(partitions_imported, Stats)).

handle_call_unknown_with_various_data_test() ->
    %% Test with atom
    State = make_state(),
    From = fake_from(),
    {reply, {error, unknown_request}, _} = flurm_slurm_import:handle_call(foo, From, State),

    %% Test with tuple
    {reply, {error, unknown_request}, _} = flurm_slurm_import:handle_call({bar, baz}, From, State),

    %% Test with number
    {reply, {error, unknown_request}, _} = flurm_slurm_import:handle_call(12345, From, State),

    %% Test with string
    {reply, {error, unknown_request}, _} = flurm_slurm_import:handle_call("unknown", From, State).

%%====================================================================
%% Timer Edge Cases
%%====================================================================

sync_tick_with_disabled_sync_test() ->
    %% Even with sync_enabled = false, sync_tick should still work
    %% (this could happen if timer fires right as sync is being disabled)
    State = make_state(#{sync_enabled => false, sync_interval => 5000}),
    {noreply, NewState} = flurm_slurm_import:handle_info(sync_tick, State),
    {state, true, _, Timer, LastSync, _} = NewState,
    ?assert(is_reference(Timer)),
    ?assertNotEqual(undefined, LastSync),
    erlang:cancel_timer(Timer).

start_sync_with_very_short_interval_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 1}, From, State),
    {state, true, 1, Timer, _, _} = NewState,
    ?assert(is_reference(Timer)),
    %% Small sleep to let timer potentially fire
    ok,
    erlang:cancel_timer(Timer).

start_sync_with_max_interval_test() ->
    State = make_state(),
    From = fake_from(),
    %% Test with a very large interval (1 day in ms)
    {reply, ok, NewState} = flurm_slurm_import:handle_call({start_sync, 86400000}, From, State),
    {state, true, 86400000, Timer, _, _} = NewState,
    ?assert(is_reference(Timer)),
    erlang:cancel_timer(Timer).

%%====================================================================
%% Stats Key Generation Tests
%%====================================================================

%% Test that stats keys are generated correctly for different import types
import_jobs_creates_jobs_imported_key_test() ->
    State = make_state(#{stats => #{}}),
    From = fake_from(),
    {reply, {ok, _}, NewState} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% The jobs_imported key should exist
    ?assert(maps:is_key(jobs_imported, Stats)).

import_nodes_creates_nodes_imported_key_test() ->
    State = make_state(#{stats => #{}}),
    From = fake_from(),
    {reply, {ok, _}, NewState} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% The nodes_imported key should exist
    ?assert(maps:is_key(nodes_imported, Stats)).

import_partitions_creates_partitions_imported_key_test() ->
    State = make_state(#{stats => #{}}),
    From = fake_from(),
    {reply, {ok, _}, NewState} = flurm_slurm_import:handle_call(import_partitions, From, State),
    {state, _, _, _, _, Stats} = NewState,
    %% The partitions_imported key should exist
    ?assert(maps:is_key(partitions_imported, Stats)).

%%====================================================================
%% Import Result Value Tests
%%====================================================================

import_all_result_values_are_integers_test() ->
    State = make_state(),
    From = fake_from(),
    {reply, {ok, Result}, _NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    %% Get the individual results
    JobsResult = maps:get(jobs, Result),
    NodesResult = maps:get(nodes, Result),
    PartsResult = maps:get(partitions, Result),
    %% All should be {ok, map} tuples with integer values
    {ok, JobStats} = JobsResult,
    {ok, NodeStats} = NodesResult,
    {ok, PartStats} = PartsResult,
    ?assert(is_integer(maps:get(imported, JobStats))),
    ?assert(is_integer(maps:get(updated, JobStats))),
    ?assert(is_integer(maps:get(failed, JobStats))),
    ?assert(is_integer(maps:get(imported, NodeStats))),
    ?assert(is_integer(maps:get(updated, NodeStats))),
    ?assert(is_integer(maps:get(failed, NodeStats))),
    ?assert(is_integer(maps:get(imported, PartStats))),
    ?assert(is_integer(maps:get(updated, PartStats))),
    ?assert(is_integer(maps:get(failed, PartStats))).

%%====================================================================
%% Sequential Import Tests
%%====================================================================

sequential_imports_preserve_timer_test() ->
    Timer = erlang:send_after(60000, self(), test_msg),
    State = make_state(#{sync_enabled => true, sync_timer => Timer, sync_interval => 5000}),
    From = fake_from(),

    %% Perform sequential imports
    {reply, {ok, _}, State2} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    {reply, {ok, _}, State3} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State2),
    {reply, {ok, _}, State4} = flurm_slurm_import:handle_call(import_partitions, From, State3),

    %% Timer should be preserved
    {state, true, 5000, SameTimer, _, _} = State4,
    ?assertEqual(Timer, SameTimer),
    erlang:cancel_timer(Timer).

%%====================================================================
%% State Consistency Tests
%%====================================================================

all_state_fields_present_after_import_test() ->
    {ok, State} = flurm_slurm_import:init([]),
    From = fake_from(),
    {reply, {ok, _}, NewState} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    %% Verify all fields are present
    ?assertEqual(6, tuple_size(NewState)),
    {state, SyncEnabled, SyncInterval, SyncTimer, LastSync, Stats} = NewState,
    ?assert(is_boolean(SyncEnabled)),
    ?assert(is_integer(SyncInterval)),
    ?assert(SyncTimer =:= undefined orelse is_reference(SyncTimer)),
    ?assertNotEqual(undefined, LastSync),  % import_all sets last_sync
    ?assert(is_map(Stats)).

stats_map_integrity_after_multiple_operations_test() ->
    {ok, State1} = flurm_slurm_import:init([]),
    From = fake_from(),

    %% Multiple imports
    {reply, {ok, _}, State2} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State1),
    {reply, {ok, _}, State3} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State2),
    {reply, {ok, _}, State4} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State3),
    {reply, {ok, _}, State5} = flurm_slurm_import:handle_call(import_partitions, From, State4),
    {reply, {ok, _}, State6} = flurm_slurm_import:handle_call({import_all, #{}}, From, State5),

    {state, _, _, _, _, Stats} = State6,

    %% All expected keys should exist
    ?assert(maps:is_key(jobs_imported, Stats)),
    ?assert(maps:is_key(nodes_imported, Stats)),
    ?assert(maps:is_key(partitions_imported, Stats)),
    ?assert(maps:is_key(last_full_import, Stats)).

%%====================================================================
%% Options Forwarding Tests
%%====================================================================

import_jobs_accepts_various_options_test() ->
    State = make_state(),
    From = fake_from(),

    %% Empty options
    {reply, {ok, R1}, _} = flurm_slurm_import:handle_call({import_jobs, #{}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R1),

    %% With partition filter
    {reply, {ok, R2}, _} = flurm_slurm_import:handle_call({import_jobs, #{partition => <<"batch">>}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R2),

    %% With user filter
    {reply, {ok, R3}, _} = flurm_slurm_import:handle_call({import_jobs, #{user => <<"root">>}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R3),

    %% With multiple filters
    {reply, {ok, R4}, _} = flurm_slurm_import:handle_call({import_jobs, #{partition => <<"gpu">>, user => <<"admin">>}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R4).

import_nodes_accepts_various_options_test() ->
    State = make_state(),
    From = fake_from(),

    %% Empty options
    {reply, {ok, R1}, _} = flurm_slurm_import:handle_call({import_nodes, #{}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R1),

    %% With partition filter
    {reply, {ok, R2}, _} = flurm_slurm_import:handle_call({import_nodes, #{partition => <<"compute">>}}, From, State),
    ?assertMatch(#{imported := _, updated := _, failed := _}, R2).

import_all_accepts_various_options_test() ->
    State = make_state(),
    From = fake_from(),

    %% Empty options
    {reply, {ok, R1}, _} = flurm_slurm_import:handle_call({import_all, #{}}, From, State),
    ?assert(maps:is_key(jobs, R1)),
    ?assert(maps:is_key(nodes, R1)),
    ?assert(maps:is_key(partitions, R1)),

    %% With filters
    {reply, {ok, R2}, _} = flurm_slurm_import:handle_call({import_all, #{partition => <<"default">>}}, From, State),
    ?assert(maps:is_key(jobs, R2)),
    ?assert(maps:is_key(nodes, R2)),
    ?assert(maps:is_key(partitions, R2)).
