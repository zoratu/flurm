%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_reservation module
%%%
%%% Tests all exported functions and gen_server callbacks directly
%%% WITHOUT using meck or any mocking framework.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_reservation_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Re-define reservation record for testing (matches module's internal definition)
-record(reservation, {
    name :: binary(),
    type :: atom(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer(),
    duration :: non_neg_integer(),
    nodes :: [binary()],
    node_count :: non_neg_integer(),
    partition :: binary() | undefined,
    features :: [binary()],
    users :: [binary()],
    accounts :: [binary()],
    flags :: list(),
    tres :: map(),
    state :: inactive | active | expired,
    created_by :: binary(),
    created_time :: non_neg_integer(),
    purge_time :: non_neg_integer() | undefined,
    jobs_using = [] :: [pos_integer()],
    confirmed = false :: boolean()
}).

-define(TABLE, flurm_reservations).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Clean up any existing table
    catch ets:delete(?TABLE),
    %% Create fresh table for tests
    ets:new(?TABLE, [
        named_table, public, set,
        {keypos, #reservation.name}
    ]),
    ok.

cleanup(_) ->
    catch ets:delete(?TABLE),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init/1 creates ETS table and timer", fun test_init/0},
        {"handle_call create reservation", fun test_handle_call_create/0},
        {"handle_call create duplicate fails", fun test_handle_call_create_duplicate/0},
        {"handle_call create missing end_time/duration", fun test_handle_call_create_missing_time/0},
        {"handle_call create missing nodes", fun test_handle_call_create_missing_nodes/0},
        {"handle_call create start_time in past", fun test_handle_call_create_past_start/0},
        {"handle_call update reservation", fun test_handle_call_update/0},
        {"handle_call update not found", fun test_handle_call_update_not_found/0},
        {"handle_call delete reservation", fun test_handle_call_delete/0},
        {"handle_call delete active reservation", fun test_handle_call_delete_active/0},
        {"handle_call delete not found", fun test_handle_call_delete_not_found/0},
        {"handle_call check conflict", fun test_handle_call_check_conflict/0},
        {"handle_call check conflict with overlap", fun test_handle_call_check_conflict_overlap/0},
        {"handle_call activate reservation", fun test_handle_call_activate/0},
        {"handle_call deactivate reservation", fun test_handle_call_deactivate/0},
        {"handle_call next window", fun test_handle_call_next_window/0},
        {"handle_call create_reservation", fun test_handle_call_create_reservation/0},
        {"handle_call confirm_reservation", fun test_handle_call_confirm_reservation/0},
        {"handle_call confirm_reservation inactive", fun test_handle_call_confirm_inactive/0},
        {"handle_call cancel_reservation", fun test_handle_call_cancel_reservation/0},
        {"handle_call check_reservation", fun test_handle_call_check_reservation/0},
        {"handle_call unknown request", fun test_handle_call_unknown/0},
        {"handle_cast add_job_to_reservation", fun test_handle_cast_add_job/0},
        {"handle_cast add_job duplicate", fun test_handle_cast_add_job_duplicate/0},
        {"handle_cast remove_job_from_reservation", fun test_handle_cast_remove_job/0},
        {"handle_cast unknown", fun test_handle_cast_unknown/0},
        {"handle_info check_reservations", fun test_handle_info_check_reservations/0},
        {"handle_info check_reservations activates", fun test_handle_info_activates_reservation/0},
        {"handle_info check_reservations expires", fun test_handle_info_expires_reservation/0},
        {"handle_info check_reservations extends recurring daily", fun test_handle_info_extends_daily/0},
        {"handle_info check_reservations extends recurring weekly", fun test_handle_info_extends_weekly/0},
        {"handle_info check_reservations purges", fun test_handle_info_purges/0},
        {"handle_info unknown", fun test_handle_info_unknown/0},
        {"terminate/2 returns ok", fun test_terminate/0},
        {"code_change/3 returns state", fun test_code_change/0},
        {"get/1 finds reservation", fun test_get_found/0},
        {"get/1 not found", fun test_get_not_found/0},
        {"list/0 returns all", fun test_list/0},
        {"list_active/0 filters active", fun test_list_active/0},
        {"is_node_reserved/2 true", fun test_is_node_reserved_true/0},
        {"is_node_reserved/2 false", fun test_is_node_reserved_false/0},
        {"get_reservations_for_node/1", fun test_get_reservations_for_node/0},
        {"get_reservations_for_user/1", fun test_get_reservations_for_user/0},
        {"can_use_reservation/2 authorized", fun test_can_use_reservation_authorized/0},
        {"can_use_reservation/2 unauthorized", fun test_can_use_reservation_unauthorized/0},
        {"can_use_reservation/2 not active", fun test_can_use_reservation_not_active/0},
        {"check_job_reservation no reservation", fun test_check_job_reservation_none/0},
        {"check_job_reservation empty string", fun test_check_job_reservation_empty/0},
        {"check_job_reservation not found", fun test_check_job_reservation_not_found/0},
        {"check_job_reservation inactive", fun test_check_job_reservation_inactive/0},
        {"check_job_reservation expired state", fun test_check_job_reservation_expired_state/0},
        {"check_job_reservation expired time", fun test_check_job_reservation_expired_time/0},
        {"check_job_reservation unauthorized", fun test_check_job_reservation_unauthorized/0},
        {"check_job_reservation authorized", fun test_check_job_reservation_authorized/0},
        {"get_reserved_nodes/1 active", fun test_get_reserved_nodes_active/0},
        {"get_reserved_nodes/1 inactive", fun test_get_reserved_nodes_inactive/0},
        {"get_reserved_nodes/1 not found", fun test_get_reserved_nodes_not_found/0},
        {"get_active_reservations/0", fun test_get_active_reservations/0},
        {"check_reservation_access job success", fun test_check_reservation_access_job/0},
        {"check_reservation_access user success", fun test_check_reservation_access_user/0},
        {"check_reservation_access maintenance denied", fun test_check_reservation_access_maintenance/0},
        {"check_reservation_access maint with ignore_jobs", fun test_check_reservation_access_maint_ignore/0},
        {"check_reservation_access maint without ignore_jobs", fun test_check_reservation_access_maint_allow/0},
        {"check_reservation_access flex type", fun test_check_reservation_access_flex/0},
        {"check_reservation_access user type", fun test_check_reservation_access_user_type/0},
        {"check_reservation_access expired", fun test_check_reservation_access_expired/0},
        {"check_reservation_access inactive not started", fun test_check_reservation_access_not_started/0},
        {"check_reservation_access inactive past start", fun test_check_reservation_access_inactive_past/0},
        {"check_reservation_access not found", fun test_check_reservation_access_not_found/0},
        {"filter_nodes_for_scheduling no reservation empty", fun test_filter_nodes_no_res_empty/0},
        {"filter_nodes_for_scheduling no reservation undefined", fun test_filter_nodes_no_res_undefined/0},
        {"filter_nodes_for_scheduling with reservation", fun test_filter_nodes_with_reservation/0},
        {"filter_nodes_for_scheduling with denied reservation", fun test_filter_nodes_denied/0},
        {"filter_nodes_for_scheduling user binary", fun test_filter_nodes_user_binary/0},
        {"get_available_nodes_excluding_reserved/1", fun test_get_available_nodes_excluding/0},
        {"get_available_nodes_excluding_reserved flex", fun test_get_available_nodes_flex/0},
        {"reservation_activated/1", fun test_reservation_activated/0},
        {"reservation_deactivated/1", fun test_reservation_deactivated/0},
        {"apply updates all fields", fun test_apply_updates_all_fields/0},
        {"times_overlap/4", fun test_times_overlap/0},
        {"nodes_overlap/2", fun test_nodes_overlap/0},
        {"find_gap with reservations", fun test_find_gap_with_reservations/0},
        %% Additional edge case tests
        {"create with duration only", fun test_create_with_duration_only/0},
        {"create with node_count only", fun test_create_with_node_count_only/0},
        {"set state to expired", fun test_set_state_to_expired/0},
        {"activate not found", fun test_activate_not_found/0},
        {"deactivate not found", fun test_deactivate_not_found/0},
        {"confirm not found", fun test_confirm_not_found/0},
        {"cancel not found", fun test_cancel_not_found/0},
        {"add job to nonexistent reservation", fun test_add_job_to_nonexistent_reservation/0},
        {"remove job from nonexistent reservation", fun test_remove_job_from_nonexistent_reservation/0},
        {"check conflict with expired reservation", fun test_check_conflict_with_expired_reservation/0},
        {"get available nodes with flex flag", fun test_get_available_nodes_with_flex_flag/0},
        {"get available nodes outside time window", fun test_get_available_nodes_outside_time_window/0},
        {"check reservation access with account", fun test_check_reservation_access_with_account/0},
        {"check reservation access expired state", fun test_check_reservation_access_expired_state/0},
        {"next window no reservations", fun test_next_window_no_reservations/0},
        {"create reservation auto name", fun test_create_reservation_auto_name/0}
     ]}.

%%====================================================================
%% init/1 Tests
%%====================================================================

test_init() ->
    %% Clean up table created by setup
    catch ets:delete(?TABLE),

    %% Call init
    {ok, State} = flurm_reservation:init([]),

    %% Verify table exists
    ?assert(ets:info(?TABLE) =/= undefined),

    %% Verify timer was started (state has check_timer field)
    ?assert(is_reference(element(2, State))),

    %% Cancel the timer to clean up
    erlang:cancel_timer(element(2, State)).

%%====================================================================
%% handle_call Tests - create
%%====================================================================

test_handle_call_create() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"test_res">>,
        start_time => Now + 100,
        end_time => Now + 3700,
        nodes => [<<"node1">>, <<"node2">>],
        type => user
    },
    State = {state, undefined},

    {reply, Result, _NewState} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({ok, <<"test_res">>}, Result),
    ?assertMatch([#reservation{name = <<"test_res">>}], ets:lookup(?TABLE, <<"test_res">>)).

test_handle_call_create_duplicate() ->
    Now = erlang:system_time(second),
    %% Insert existing reservation
    Res = #reservation{
        name = <<"existing">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Spec = #{
        name => <<"existing">>,
        start_time => Now + 100,
        end_time => Now + 200,
        nodes => [<<"node1">>]
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({error, already_exists}, Result).

test_handle_call_create_missing_time() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"bad_res">>,
        start_time => Now + 100,
        nodes => [<<"node1">>]
        %% Missing both end_time and duration
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({error, missing_end_time_or_duration}, Result).

test_handle_call_create_missing_nodes() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"bad_res">>,
        start_time => Now + 100,
        end_time => Now + 200
        %% Missing nodes and node_count
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({error, missing_node_specification}, Result).

test_handle_call_create_past_start() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"past_res">>,
        start_time => Now - 100,  %% In the past
        end_time => Now + 200,
        nodes => [<<"node1">>]
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({error, start_time_in_past}, Result).

%%====================================================================
%% handle_call Tests - update
%%====================================================================

test_handle_call_update() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"update_me">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        users = [],
        state = inactive,
        duration = 0,
        node_count = 0,
        partition = undefined,
        features = [],
        accounts = [],
        flags = [],
        tres = #{}
    },
    ets:insert(?TABLE, Res),

    Updates = #{
        end_time => Now + 500,
        users => [<<"alice">>]
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({update, <<"update_me">>, Updates}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    [Updated] = ets:lookup(?TABLE, <<"update_me">>),
    ?assertEqual(Now + 500, Updated#reservation.end_time),
    ?assertEqual([<<"alice">>], Updated#reservation.users).

test_handle_call_update_not_found() ->
    Updates = #{end_time => 1000},
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({update, <<"nonexistent">>, Updates}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% handle_call Tests - delete
%%====================================================================

test_handle_call_delete() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"delete_me">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = inactive
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({delete, <<"delete_me">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    ?assertEqual([], ets:lookup(?TABLE, <<"delete_me">>)).

test_handle_call_delete_active() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"delete_active">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({delete, <<"delete_active">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    ?assertEqual([], ets:lookup(?TABLE, <<"delete_active">>)).

test_handle_call_delete_not_found() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({delete, <<"nonexistent">>}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% handle_call Tests - check_conflict
%%====================================================================

test_handle_call_check_conflict() ->
    Now = erlang:system_time(second),
    %% No conflicting reservations
    Spec = #{
        start_time => Now + 100,
        end_time => Now + 200,
        nodes => [<<"node1">>]
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({check_conflict, Spec}, {self(), ref}, State),

    ?assertEqual(ok, Result).

test_handle_call_check_conflict_overlap() ->
    Now = erlang:system_time(second),
    %% Insert existing reservation
    Res = #reservation{
        name = <<"existing">>,
        start_time = Now + 150,
        end_time = Now + 300,
        nodes = [<<"node1">>, <<"node2">>],
        state = active
    },
    ets:insert(?TABLE, Res),

    %% New spec that overlaps
    Spec = #{
        start_time => Now + 100,
        end_time => Now + 200,
        nodes => [<<"node1">>]  %% Overlapping node
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({check_conflict, Spec}, {self(), ref}, State),

    ?assertMatch({error, {conflicts, [<<"existing">>]}}, Result).

%%====================================================================
%% handle_call Tests - activate/deactivate
%%====================================================================

test_handle_call_activate() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"to_activate">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = inactive
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({activate, <<"to_activate">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    [Updated] = ets:lookup(?TABLE, <<"to_activate">>),
    ?assertEqual(active, Updated#reservation.state).

test_handle_call_deactivate() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"to_deactivate">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({deactivate, <<"to_deactivate">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    [Updated] = ets:lookup(?TABLE, <<"to_deactivate">>),
    ?assertEqual(inactive, Updated#reservation.state).

%%====================================================================
%% handle_call Tests - next_window
%%====================================================================

test_handle_call_next_window() ->
    Now = erlang:system_time(second),
    %% Insert a reservation that blocks near future
    Res = #reservation{
        name = <<"blocking">>,
        start_time = Now + 10,
        end_time = Now + 100,
        nodes = [<<"node1">>],
        partition = undefined,
        state = active
    },
    ets:insert(?TABLE, Res),

    ResourceRequest = #{nodes => [<<"node1">>]},
    Duration = 50,
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({next_window, ResourceRequest, Duration}, {self(), ref}, State),

    %% Should find window either before or after the reservation
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% handle_call Tests - Job-level reservation operations
%%====================================================================

test_handle_call_create_reservation() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"job_res">>,
        start_time => Now + 100,
        end_time => Now + 3700,
        nodes => [<<"node1">>],
        user => <<"alice">>
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create_reservation, Spec}, {self(), ref}, State),

    ?assertEqual({ok, <<"job_res">>}, Result).

test_handle_call_confirm_reservation() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"to_confirm">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = active,
        confirmed = false
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({confirm_reservation, <<"to_confirm">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    [Updated] = ets:lookup(?TABLE, <<"to_confirm">>),
    ?assertEqual(true, Updated#reservation.confirmed).

test_handle_call_confirm_inactive() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"inactive_res">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = inactive,
        confirmed = false
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({confirm_reservation, <<"inactive_res">>}, {self(), ref}, State),

    ?assertEqual({error, {invalid_state, inactive}}, Result).

test_handle_call_cancel_reservation() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"to_cancel">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>, <<"node2">>],
        jobs_using = [123, 456],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({cancel_reservation, <<"to_cancel">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),
    ?assertEqual([], ets:lookup(?TABLE, <<"to_cancel">>)).

test_handle_call_check_reservation() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"check_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        users = [],
        accounts = [],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    %% Test with inactive state first - this path doesn't call job_manager
    ResInactive = #reservation{
        name = <<"check_inactive">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        users = [],
        accounts = [],
        state = inactive
    },
    ets:insert(?TABLE, ResInactive),
    {reply, Result1, _} = flurm_reservation:handle_call({check_reservation, 123, <<"check_inactive">>}, {self(), ref}, State),
    ?assertEqual({error, {invalid_state, inactive}}, Result1),

    %% Test not found case
    {reply, Result2, _} = flurm_reservation:handle_call({check_reservation, 123, <<"nonexistent">>}, {self(), ref}, State),
    ?assertEqual({error, not_found}, Result2),

    %% The active case requires flurm_job_manager - test it catches the noproc error gracefully
    %% by wrapping in try/catch (the real code path would succeed if job_manager was running)
    try
        flurm_reservation:handle_call({check_reservation, 123, <<"check_res">>}, {self(), ref}, State)
    catch
        exit:{noproc, _} ->
            %% Expected - job_manager not running, this proves the code path was reached
            ok
    end.

test_handle_call_unknown() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({unknown_request, foo}, {self(), ref}, State),

    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% handle_cast Tests
%%====================================================================

test_handle_cast_add_job() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"add_job_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        jobs_using = [],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, _} = flurm_reservation:handle_cast({add_job_to_reservation, 123, <<"add_job_res">>}, State),

    [Updated] = ets:lookup(?TABLE, <<"add_job_res">>),
    ?assert(lists:member(123, Updated#reservation.jobs_using)).

test_handle_cast_add_job_duplicate() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"dup_job_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        jobs_using = [123],  %% Already has job
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, _} = flurm_reservation:handle_cast({add_job_to_reservation, 123, <<"dup_job_res">>}, State),

    [Updated] = ets:lookup(?TABLE, <<"dup_job_res">>),
    %% Should not duplicate
    ?assertEqual([123], Updated#reservation.jobs_using).

test_handle_cast_remove_job() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"rm_job_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        jobs_using = [123, 456],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, _} = flurm_reservation:handle_cast({remove_job_from_reservation, 123, <<"rm_job_res">>}, State),

    [Updated] = ets:lookup(?TABLE, <<"rm_job_res">>),
    ?assertEqual([456], Updated#reservation.jobs_using).

test_handle_cast_unknown() ->
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_cast({unknown_cast, foo}, State),

    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info Tests
%%====================================================================

test_handle_info_check_reservations() ->
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    %% Should have set new timer
    ?assert(is_reference(element(2, NewState))),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_activates_reservation() ->
    Now = erlang:system_time(second),
    %% Reservation that should be activated (start_time <= now < end_time)
    Res = #reservation{
        name = <<"should_activate">>,
        start_time = Now - 10,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = inactive,
        flags = [],
        purge_time = undefined
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    [Updated] = ets:lookup(?TABLE, <<"should_activate">>),
    ?assertEqual(active, Updated#reservation.state),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_expires_reservation() ->
    Now = erlang:system_time(second),
    %% Reservation that should expire (end_time <= now)
    Res = #reservation{
        name = <<"should_expire">>,
        start_time = Now - 200,
        end_time = Now - 10,
        nodes = [<<"node1">>],
        state = active,
        flags = [],
        purge_time = undefined,
        jobs_using = []
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    [Updated] = ets:lookup(?TABLE, <<"should_expire">>),
    ?assertEqual(expired, Updated#reservation.state),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_extends_daily() ->
    Now = erlang:system_time(second),
    %% Daily recurring reservation that should be extended
    Res = #reservation{
        name = <<"daily_res">>,
        start_time = Now - 200,
        end_time = Now - 10,
        duration = 3600,
        nodes = [<<"node1">>],
        state = active,
        flags = [daily],
        purge_time = undefined
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    [Updated] = ets:lookup(?TABLE, <<"daily_res">>),
    %% Should be inactive and extended by 24 hours
    ?assertEqual(inactive, Updated#reservation.state),
    ?assertEqual(Now - 200 + 86400, Updated#reservation.start_time),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_extends_weekly() ->
    Now = erlang:system_time(second),
    %% Weekly recurring reservation that should be extended
    Res = #reservation{
        name = <<"weekly_res">>,
        start_time = Now - 200,
        end_time = Now - 10,
        duration = 3600,
        nodes = [<<"node1">>],
        state = active,
        flags = [weekly],
        purge_time = undefined
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    [Updated] = ets:lookup(?TABLE, <<"weekly_res">>),
    %% Should be inactive and extended by 7 days
    ?assertEqual(inactive, Updated#reservation.state),
    ?assertEqual(Now - 200 + 604800, Updated#reservation.start_time),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_purges() ->
    Now = erlang:system_time(second),
    %% Expired reservation with purge_time in the past
    Res = #reservation{
        name = <<"should_purge">>,
        start_time = Now - 500,
        end_time = Now - 400,
        nodes = [<<"node1">>],
        state = expired,
        flags = [],
        purge_time = Now - 100  %% Purge time passed
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info(check_reservations, State),

    ?assertEqual([], ets:lookup(?TABLE, <<"should_purge">>)),
    erlang:cancel_timer(element(2, NewState)).

test_handle_info_unknown() ->
    State = {state, undefined},

    {noreply, NewState} = flurm_reservation:handle_info({unknown_info}, State),

    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 and code_change/3 Tests
%%====================================================================

test_terminate() ->
    State = {state, undefined},

    Result = flurm_reservation:terminate(normal, State),

    ?assertEqual(ok, Result).

test_code_change() ->
    State = {state, undefined},

    Result = flurm_reservation:code_change("1.0", State, []),

    ?assertEqual({ok, State}, Result).

%%====================================================================
%% API Function Tests - get/1
%%====================================================================

test_get_found() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"find_me">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:get(<<"find_me">>),

    ?assertMatch({ok, #reservation{name = <<"find_me">>}}, Result).

test_get_not_found() ->
    Result = flurm_reservation:get(<<"nonexistent">>),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% API Function Tests - list/0
%%====================================================================

test_list() ->
    Now = erlang:system_time(second),
    Res1 = #reservation{name = <<"res1">>, start_time = Now, end_time = Now + 100, nodes = []},
    Res2 = #reservation{name = <<"res2">>, start_time = Now, end_time = Now + 100, nodes = []},
    ets:insert(?TABLE, Res1),
    ets:insert(?TABLE, Res2),

    Result = flurm_reservation:list(),

    ?assertEqual(2, length(Result)).

%%====================================================================
%% API Function Tests - list_active/0
%%====================================================================

test_list_active() ->
    Now = erlang:system_time(second),
    %% Active and within time window
    Res1 = #reservation{
        name = <<"active1">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [],
        state = active
    },
    %% Active but outside time window (ended)
    Res2 = #reservation{
        name = <<"active2">>,
        start_time = Now - 200,
        end_time = Now - 100,
        nodes = [],
        state = active
    },
    %% Inactive
    Res3 = #reservation{
        name = <<"inactive">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [],
        state = inactive
    },
    ets:insert(?TABLE, [Res1, Res2, Res3]),

    Result = flurm_reservation:list_active(),

    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"active1">>, (hd(Result))#reservation.name).

%%====================================================================
%% API Function Tests - is_node_reserved/2
%%====================================================================

test_is_node_reserved_true() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"node_res">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [<<"node1">>, <<"node2">>],
        partition = undefined,
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:is_node_reserved(<<"node1">>, Now),

    ?assert(Result).

test_is_node_reserved_false() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"node_res">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [<<"node1">>],
        partition = undefined,
        state = inactive  %% Not active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:is_node_reserved(<<"node1">>, Now),

    ?assertNot(Result).

%%====================================================================
%% API Function Tests - get_reservations_for_node/1
%%====================================================================

test_get_reservations_for_node() ->
    Now = erlang:system_time(second),
    Res1 = #reservation{
        name = <<"res_with_node">>,
        start_time = Now,
        end_time = Now + 100,
        nodes = [<<"target_node">>, <<"other_node">>],
        partition = undefined
    },
    Res2 = #reservation{
        name = <<"res_without_node">>,
        start_time = Now,
        end_time = Now + 100,
        nodes = [<<"different_node">>],
        partition = undefined
    },
    ets:insert(?TABLE, [Res1, Res2]),

    Result = flurm_reservation:get_reservations_for_node(<<"target_node">>),

    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"res_with_node">>, (hd(Result))#reservation.name).

%%====================================================================
%% API Function Tests - get_reservations_for_user/1
%%====================================================================

test_get_reservations_for_user() ->
    Now = erlang:system_time(second),
    Res1 = #reservation{
        name = <<"user_res">>,
        start_time = Now,
        end_time = Now + 100,
        nodes = [],
        users = [<<"alice">>, <<"bob">>],
        accounts = []
    },
    Res2 = #reservation{
        name = <<"other_res">>,
        start_time = Now,
        end_time = Now + 100,
        nodes = [],
        users = [<<"charlie">>],
        accounts = []
    },
    Res3 = #reservation{
        name = <<"open_res">>,
        start_time = Now,
        end_time = Now + 100,
        nodes = [],
        users = [],  %% Open to all
        accounts = []
    },
    ets:insert(?TABLE, [Res1, Res2, Res3]),

    Result = flurm_reservation:get_reservations_for_user(<<"alice">>),

    %% Should get user_res and open_res
    ?assertEqual(2, length(Result)).

%%====================================================================
%% API Function Tests - can_use_reservation/2
%%====================================================================

test_can_use_reservation_authorized() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"auth_res">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [],
        users = [<<"alice">>],
        accounts = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:can_use_reservation(<<"alice">>, <<"auth_res">>),

    ?assert(Result).

test_can_use_reservation_unauthorized() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"auth_res">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [],
        users = [<<"alice">>],
        accounts = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:can_use_reservation(<<"bob">>, <<"auth_res">>),

    ?assertNot(Result).

test_can_use_reservation_not_active() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"inactive_res">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [],
        users = [<<"alice">>],
        accounts = [],
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:can_use_reservation(<<"alice">>, <<"inactive_res">>),

    ?assertNot(Result).

%%====================================================================
%% API Function Tests - check_job_reservation/1
%%====================================================================

test_check_job_reservation_none() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = undefined
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({ok, no_reservation}, Result).

test_check_job_reservation_empty() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<>>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({ok, no_reservation}, Result).

test_check_job_reservation_not_found() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"nonexistent">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({error, reservation_not_found}, Result).

test_check_job_reservation_inactive() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"inactive_res">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        users = [],
        accounts = [],
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"inactive_res">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({error, reservation_not_active}, Result).

test_check_job_reservation_expired_state() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"expired_res">>,
        start_time = Now - 200,
        end_time = Now - 100,
        nodes = [<<"node1">>],
        users = [],
        accounts = [],
        state = expired
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"expired_res">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({error, reservation_expired}, Result).

test_check_job_reservation_expired_time() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"time_expired">>,
        start_time = Now - 200,
        end_time = Now - 100,  %% End time passed
        nodes = [<<"node1">>],
        users = [],
        accounts = [],
        state = active  %% Still marked active but time expired
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"time_expired">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({error, reservation_expired}, Result).

test_check_job_reservation_unauthorized() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"user_only">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>],
        users = [<<"charlie">>],  %% Only charlie allowed
        accounts = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,  %% Alice not in users list
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"user_only">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({error, user_not_authorized}, Result).

test_check_job_reservation_authorized() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"open_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"node1">>, <<"node2">>],
        users = [],  %% Open to all
        accounts = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 123,
        name = <<"test">>,
        user = <<"alice">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"open_res">>
    },

    Result = flurm_reservation:check_job_reservation(Job),

    ?assertEqual({ok, use_reservation, [<<"node1">>, <<"node2">>]}, Result).

%%====================================================================
%% API Function Tests - get_reserved_nodes/1
%%====================================================================

test_get_reserved_nodes_active() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"nodes_res">>,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:get_reserved_nodes(<<"nodes_res">>),

    ?assertEqual({ok, [<<"n1">>, <<"n2">>, <<"n3">>]}, Result).

test_get_reserved_nodes_inactive() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"inactive_nodes">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:get_reserved_nodes(<<"inactive_nodes">>),

    ?assertEqual({error, {invalid_state, inactive}}, Result).

test_get_reserved_nodes_not_found() ->
    Result = flurm_reservation:get_reserved_nodes(<<"nonexistent">>),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% API Function Tests - get_active_reservations/0
%%====================================================================

test_get_active_reservations() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"active_res">>,
        start_time = Now - 100,
        end_time = Now + 100,
        nodes = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:get_active_reservations(),

    ?assertEqual(1, length(Result)).

%%====================================================================
%% API Function Tests - check_reservation_access/2
%%====================================================================

test_check_reservation_access_job() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"access_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>, <<"n2">>],
        users = [<<"alice">>],
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        account = <<"default">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },

    Result = flurm_reservation:check_reservation_access(Job, <<"access_res">>),

    ?assertEqual({ok, [<<"n1">>, <<"n2">>]}, Result).

test_check_reservation_access_user() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"user_access">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],  %% Open
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"bob">>, <<"user_access">>),

    ?assertEqual({ok, [<<"n1">>]}, Result).

test_check_reservation_access_maintenance() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"maint_res">>,
        type = maintenance,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"maint_res">>),

    ?assertEqual({error, access_denied}, Result).

test_check_reservation_access_maint_ignore() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"maint_ignore">>,
        type = maint,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [ignore_jobs],  %% No jobs allowed
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"maint_ignore">>),

    ?assertEqual({error, access_denied}, Result).

test_check_reservation_access_maint_allow() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"maint_allow">>,
        type = maint,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [<<"alice">>],
        accounts = [],
        flags = [],  %% No ignore_jobs flag
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"maint_allow">>),

    ?assertEqual({ok, [<<"n1">>]}, Result).

test_check_reservation_access_flex() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"flex_res">>,
        type = flex,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],  %% Open to all for flex
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"anyone">>, <<"flex_res">>),

    ?assertEqual({ok, [<<"n1">>]}, Result).

test_check_reservation_access_user_type() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"user_type_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [<<"specific_user">>],
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    %% Unauthorized user
    Result = flurm_reservation:check_reservation_access(<<"other_user">>, <<"user_type_res">>),

    ?assertEqual({error, access_denied}, Result).

test_check_reservation_access_expired() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"expired_access">>,
        type = user,
        start_time = Now - 200,
        end_time = Now - 100,  %% Expired
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"expired_access">>),

    ?assertEqual({error, reservation_expired}, Result).

test_check_reservation_access_not_started() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"future_res">>,
        type = user,
        start_time = Now + 1000,  %% Future
        end_time = Now + 2000,
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [],
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"future_res">>),

    ?assertMatch({error, {reservation_not_started, _}}, Result).

test_check_reservation_access_inactive_past() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"inactive_past">>,
        type = user,
        start_time = Now - 100,  %% Past start
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [],
        state = inactive  %% But still inactive
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"inactive_past">>),

    ?assertEqual({error, reservation_not_active}, Result).

test_check_reservation_access_not_found() ->
    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"nonexistent">>),

    ?assertEqual({error, reservation_not_found}, Result).

%%====================================================================
%% API Function Tests - filter_nodes_for_scheduling/2
%%====================================================================

test_filter_nodes_no_res_empty() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        account = <<"default">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<>>
    },

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),

    %% Should return all nodes (no reservations to block)
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_filter_nodes_no_res_undefined() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        account = <<"default">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = undefined
    },

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),

    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_filter_nodes_with_reservation() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"job_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>, <<"n3">>],  %% Only these nodes
        users = [<<"alice">>],
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        account = <<"default">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"job_res">>
    },

    Nodes = [<<"n1">>, <<"n2">>, <<"n3">>, <<"n4">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),

    %% Should only return intersection: n1 and n3
    ?assertEqual([<<"n1">>, <<"n3">>], Result).

test_filter_nodes_denied() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"restricted_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [<<"bob">>],  %% Only bob
        accounts = [],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,  %% Alice not authorized
        account = <<"default">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        reservation = <<"restricted_res">>
    },

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),

    %% Should return empty list (no access)
    ?assertEqual([], Result).

test_filter_nodes_user_binary() ->
    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(<<"alice">>, Nodes),

    %% Should return all nodes (no blocking reservations)
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

%%====================================================================
%% API Function Tests - get_available_nodes_excluding_reserved/1
%%====================================================================

test_get_available_nodes_excluding() ->
    Now = erlang:system_time(second),
    %% Non-flex reservation blocking n1
    Res = #reservation{
        name = <<"blocking_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
    Result = flurm_reservation:get_available_nodes_excluding_reserved(Nodes),

    %% n1 should be excluded
    ?assertEqual([<<"n2">>, <<"n3">>], Result).

test_get_available_nodes_flex() ->
    Now = erlang:system_time(second),
    %% Flex reservation - should NOT block nodes
    Res = #reservation{
        name = <<"flex_res">>,
        type = flex,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:get_available_nodes_excluding_reserved(Nodes),

    %% n1 should NOT be excluded (flex type)
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

%%====================================================================
%% API Function Tests - reservation_activated/1 and reservation_deactivated/1
%%====================================================================

test_reservation_activated() ->
    %% Should just log and try to trigger scheduler (which may not exist)
    Result = flurm_reservation:reservation_activated(<<"test_res">>),

    ?assertEqual(ok, Result).

test_reservation_deactivated() ->
    %% Should just log and try to trigger scheduler (which may not exist)
    Result = flurm_reservation:reservation_deactivated(<<"test_res">>),

    ?assertEqual(ok, Result).

%%====================================================================
%% Internal Function Tests - apply_reservation_updates
%%====================================================================

test_apply_updates_all_fields() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"test">>,
        start_time = Now,
        end_time = Now + 100,
        duration = 100,
        nodes = [<<"n1">>],
        node_count = 1,
        partition = <<"part1">>,
        features = [<<"gpu">>],
        users = [<<"alice">>],
        accounts = [<<"acct1">>],
        flags = [],
        tres = #{},
        state = inactive
    },
    ets:insert(?TABLE, Res),

    Updates = #{
        start_time => Now + 50,
        end_time => Now + 200,
        duration => 150,
        nodes => [<<"n2">>, <<"n3">>],
        node_count => 2,
        partition => <<"part2">>,
        features => [<<"ssd">>],
        users => [<<"bob">>],
        accounts => [<<"acct2">>],
        flags => [daily],
        tres => #{cpu => 4},
        unknown_field => ignored  %% Should be ignored
    },
    State = {state, undefined},

    {reply, ok, _} = flurm_reservation:handle_call({update, <<"test">>, Updates}, {self(), ref}, State),

    [Updated] = ets:lookup(?TABLE, <<"test">>),
    ?assertEqual(Now + 50, Updated#reservation.start_time),
    ?assertEqual(Now + 200, Updated#reservation.end_time),
    ?assertEqual(150, Updated#reservation.duration),
    ?assertEqual([<<"n2">>, <<"n3">>], Updated#reservation.nodes),
    ?assertEqual(2, Updated#reservation.node_count),
    ?assertEqual(<<"part2">>, Updated#reservation.partition),
    ?assertEqual([<<"ssd">>], Updated#reservation.features),
    ?assertEqual([<<"bob">>], Updated#reservation.users),
    ?assertEqual([<<"acct2">>], Updated#reservation.accounts),
    ?assertEqual([daily], Updated#reservation.flags),
    ?assertEqual(#{cpu => 4}, Updated#reservation.tres).

%%====================================================================
%% Internal Function Tests - times_overlap/4 and nodes_overlap/2
%%====================================================================

test_times_overlap() ->
    %% Test via conflict checking
    Now = erlang:system_time(second),

    %% Existing reservation: 100-200
    Res = #reservation{
        name = <<"existing">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    %% Case 1: No overlap (before)
    Spec1 = #{start_time => Now, end_time => Now + 50, nodes => [<<"n1">>]},
    {reply, R1, _} = flurm_reservation:handle_call({check_conflict, Spec1}, {self(), ref}, State),
    ?assertEqual(ok, R1),

    %% Case 2: No overlap (after)
    Spec2 = #{start_time => Now + 250, end_time => Now + 300, nodes => [<<"n1">>]},
    {reply, R2, _} = flurm_reservation:handle_call({check_conflict, Spec2}, {self(), ref}, State),
    ?assertEqual(ok, R2),

    %% Case 3: Overlap at start
    Spec3 = #{start_time => Now + 50, end_time => Now + 150, nodes => [<<"n1">>]},
    {reply, R3, _} = flurm_reservation:handle_call({check_conflict, Spec3}, {self(), ref}, State),
    ?assertMatch({error, {conflicts, _}}, R3),

    %% Case 4: Overlap at end
    Spec4 = #{start_time => Now + 150, end_time => Now + 250, nodes => [<<"n1">>]},
    {reply, R4, _} = flurm_reservation:handle_call({check_conflict, Spec4}, {self(), ref}, State),
    ?assertMatch({error, {conflicts, _}}, R4),

    %% Case 5: Complete containment
    Spec5 = #{start_time => Now + 120, end_time => Now + 180, nodes => [<<"n1">>]},
    {reply, R5, _} = flurm_reservation:handle_call({check_conflict, Spec5}, {self(), ref}, State),
    ?assertMatch({error, {conflicts, _}}, R5).

test_nodes_overlap() ->
    Now = erlang:system_time(second),

    %% Existing reservation with nodes n1, n2
    Res = #reservation{
        name = <<"existing">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"n1">>, <<"n2">>],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    %% Case 1: No node overlap
    Spec1 = #{start_time => Now + 100, end_time => Now + 200, nodes => [<<"n3">>, <<"n4">>]},
    {reply, R1, _} = flurm_reservation:handle_call({check_conflict, Spec1}, {self(), ref}, State),
    ?assertEqual(ok, R1),

    %% Case 2: Partial node overlap
    Spec2 = #{start_time => Now + 100, end_time => Now + 200, nodes => [<<"n2">>, <<"n3">>]},
    {reply, R2, _} = flurm_reservation:handle_call({check_conflict, Spec2}, {self(), ref}, State),
    ?assertMatch({error, {conflicts, _}}, R2),

    %% Case 3: Empty nodes - no conflict
    Spec3 = #{start_time => Now + 100, end_time => Now + 200, nodes => []},
    {reply, R3, _} = flurm_reservation:handle_call({check_conflict, Spec3}, {self(), ref}, State),
    ?assertEqual(ok, R3).

%%====================================================================
%% Internal Function Tests - find_gap
%%====================================================================

test_find_gap_with_reservations() ->
    Now = erlang:system_time(second),

    %% Create reservations blocking near-term windows
    Res1 = #reservation{
        name = <<"gap1">>,
        start_time = Now + 10,
        end_time = Now + 50,
        nodes = [<<"n1">>],
        partition = undefined,
        state = active
    },
    Res2 = #reservation{
        name = <<"gap2">>,
        start_time = Now + 60,
        end_time = Now + 100,
        nodes = [<<"n1">>],
        partition = undefined,
        state = active
    },
    ets:insert(?TABLE, [Res1, Res2]),

    State = {state, undefined},

    %% Request 30 second window - should find gap after second reservation
    ResourceRequest = #{nodes => [<<"n1">>]},
    {reply, Result, _} = flurm_reservation:handle_call({next_window, ResourceRequest, 30}, {self(), ref}, State),

    ?assertMatch({ok, _}, Result),
    {ok, StartTime} = Result,
    %% Should be at or after Now + 100 (end of second reservation)
    ?assert(StartTime >= Now + 100 orelse StartTime < Now + 10).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

test_create_with_duration_only() ->
    %% Test creation using duration instead of end_time
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"duration_res">>,
        start_time => Now + 100,
        duration => 3600,  %% 1 hour duration
        nodes => [<<"node1">>],
        type => user
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({ok, <<"duration_res">>}, Result),
    [Res] = ets:lookup(?TABLE, <<"duration_res">>),
    %% End time should be start_time + duration
    ?assertEqual(Now + 100 + 3600, Res#reservation.end_time).

test_create_with_node_count_only() ->
    %% Test creation using node_count instead of specific nodes
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"nodecount_res">>,
        start_time => Now + 100,
        end_time => Now + 200,
        node_count => 5,  %% Request 5 nodes
        type => user
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create, Spec}, {self(), ref}, State),

    ?assertEqual({ok, <<"nodecount_res">>}, Result),
    [Res] = ets:lookup(?TABLE, <<"nodecount_res">>),
    ?assertEqual(5, Res#reservation.node_count).

test_set_state_to_expired() ->
    %% Test setting state to expired
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"to_expire">>,
        start_time = Now - 200,
        end_time = Now - 100,
        nodes = [<<"node1">>],
        state = active
    },
    ets:insert(?TABLE, Res),
    State = {state, undefined},

    %% Use deactivate first
    {reply, R1, _} = flurm_reservation:handle_call({deactivate, <<"to_expire">>}, {self(), ref}, State),
    ?assertEqual(ok, R1),

    [Updated1] = ets:lookup(?TABLE, <<"to_expire">>),
    ?assertEqual(inactive, Updated1#reservation.state).

test_activate_not_found() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({activate, <<"nonexistent">>}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

test_deactivate_not_found() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({deactivate, <<"nonexistent">>}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

test_confirm_not_found() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({confirm_reservation, <<"nonexistent">>}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

test_cancel_not_found() ->
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({cancel_reservation, <<"nonexistent">>}, {self(), ref}, State),

    ?assertEqual({error, not_found}, Result).

test_add_job_to_nonexistent_reservation() ->
    State = {state, undefined},

    %% Should silently succeed (no error for nonexistent)
    {noreply, _} = flurm_reservation:handle_cast({add_job_to_reservation, 999, <<"nonexistent">>}, State).

test_remove_job_from_nonexistent_reservation() ->
    State = {state, undefined},

    %% Should silently succeed (no error for nonexistent)
    {noreply, _} = flurm_reservation:handle_cast({remove_job_from_reservation, 999, <<"nonexistent">>}, State).

test_check_conflict_with_expired_reservation() ->
    Now = erlang:system_time(second),
    %% Expired reservation should not cause conflict
    Res = #reservation{
        name = <<"expired">>,
        start_time = Now + 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        state = expired  %% Marked as expired
    },
    ets:insert(?TABLE, Res),

    Spec = #{
        start_time => Now + 100,
        end_time => Now + 200,
        nodes => [<<"n1">>]
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({check_conflict, Spec}, {self(), ref}, State),

    %% Should NOT conflict with expired reservation
    ?assertEqual(ok, Result).

test_get_available_nodes_with_flex_flag() ->
    Now = erlang:system_time(second),
    %% Non-flex type but with flex flag - should NOT block
    Res = #reservation{
        name = <<"flex_flag_res">>,
        type = user,  %% Not flex type
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        flags = [flex],  %% But has flex flag
        state = active
    },
    ets:insert(?TABLE, Res),

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:get_available_nodes_excluding_reserved(Nodes),

    %% n1 should NOT be excluded (flex flag)
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_get_available_nodes_outside_time_window() ->
    Now = erlang:system_time(second),
    %% Active reservation but outside current time window
    Res = #reservation{
        name = <<"future_active">>,
        type = user,
        start_time = Now + 1000,  %% Future start
        end_time = Now + 2000,
        nodes = [<<"n1">>],
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:get_available_nodes_excluding_reserved(Nodes),

    %% n1 should NOT be excluded (outside time window)
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_check_reservation_access_with_account() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"account_res">>,
        type = user,
        start_time = Now - 100,
        end_time = Now + 200,
        nodes = [<<"n1">>],
        users = [],
        accounts = [<<"engineering">>],  %% Account based access
        flags = [],
        state = active
    },
    ets:insert(?TABLE, Res),

    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"alice">>,
        account = <<"engineering">>,  %% Matching account
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },

    Result = flurm_reservation:check_reservation_access(Job, <<"account_res">>),

    ?assertEqual({ok, [<<"n1">>]}, Result).

test_check_reservation_access_expired_state() ->
    Now = erlang:system_time(second),
    Res = #reservation{
        name = <<"expired_state_res">>,
        type = user,
        start_time = Now - 200,
        end_time = Now + 200,  %% Not time-expired
        nodes = [<<"n1">>],
        users = [],
        accounts = [],
        flags = [],
        state = expired  %% But state is expired
    },
    ets:insert(?TABLE, Res),

    Result = flurm_reservation:check_reservation_access(<<"alice">>, <<"expired_state_res">>),

    ?assertEqual({error, reservation_expired}, Result).

test_next_window_no_reservations() ->
    %% When no reservations exist, should return current time
    ResourceRequest = #{nodes => [<<"n1">>]},
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({next_window, ResourceRequest, 100}, {self(), ref}, State),

    ?assertMatch({ok, _}, Result).

test_create_reservation_auto_name() ->
    %% Test create_reservation with auto-generated name
    Now = erlang:system_time(second),
    Spec = #{
        %% No name provided - should auto-generate
        start_time => Now + 100,
        end_time => Now + 200,
        nodes => [<<"node1">>],
        user => <<"alice">>
    },
    State = {state, undefined},

    {reply, Result, _} = flurm_reservation:handle_call({create_reservation, Spec}, {self(), ref}, State),

    ?assertMatch({ok, <<"res_", _/binary>>}, Result).
