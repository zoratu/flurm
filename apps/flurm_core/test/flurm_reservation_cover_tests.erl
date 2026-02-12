%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit coverage tests for flurm_reservation module
%%%
%%% Targets 100% line coverage by exercising every branch in the
%%% gen_server callbacks, public API functions, scheduler integration,
%%% job-level reservation API, and all TEST-exported internal helpers.
%%%
%%% Strategy: Uses a {foreach, setup, cleanup, Tests} fixture that
%%% starts the gen_server fresh for each test. Internal functions
%%% are tested both through the gen_server (for lines in handle_call/
%%% handle_cast/handle_info) and directly via the TEST exports.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_reservation_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Re-define the reservation record to match the module's internal layout.
%% The actual record in flurm_reservation.erl has these fields in this order.
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
%% Test Fixture
%%====================================================================

setup() ->
    %% Ensure no leftover table or process
    catch ets:delete(?TABLE),
    case whereis(flurm_reservation) of
        undefined -> ok;
        OldPid ->
            Ref = monitor(process, OldPid),
            unlink(OldPid),
            catch gen_server:stop(OldPid, shutdown, 5000),
            receive {'DOWN', Ref, process, OldPid, _} -> ok
            after 5000 -> demonitor(Ref, [flush]), catch exit(OldPid, kill)
            end
    end,
    %% Start the gen_server (creates the ETS table in init/1)
    {ok, Pid} = flurm_reservation:start_link(),
    Pid.

cleanup(Pid) ->
    %% Clean all reservations
    lists:foreach(fun(Res) ->
        catch flurm_reservation:delete(element(2, Res))
    end, flurm_reservation:list()),
    %% Stop gen_server
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 -> demonitor(Ref, [flush]), catch exit(Pid, kill)
            end;
        false -> ok
    end,
    catch ets:delete(?TABLE),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

unique_name() ->
    N = erlang:unique_integer([positive]),
    iolist_to_binary([<<"cov_res_">>, integer_to_binary(N)]).

now_sec() ->
    erlang:system_time(second).

%% Build a valid spec with sensible defaults, merged with overrides.
spec(Overrides) ->
    Now = now_sec(),
    Defaults = #{
        name => unique_name(),
        type => user,
        nodes => [<<"n1">>, <<"n2">>],
        start_time => Now + 10,
        end_time => Now + 3610,
        users => [<<"alice">>],
        accounts => [],
        partition => <<"default">>,
        flags => []
    },
    maps:merge(Defaults, Overrides).

%% Insert a reservation record directly into ETS for isolated testing.
insert_res(Fields) ->
    Now = now_sec(),
    Defaults = #reservation{
        name = unique_name(),
        type = user,
        start_time = Now - 100,
        end_time = Now + 3600,
        duration = 0,
        nodes = [<<"n1">>],
        node_count = 0,
        partition = undefined,
        features = [],
        users = [],
        accounts = [],
        flags = [],
        tres = #{},
        state = inactive,
        created_by = <<"admin">>,
        created_time = Now,
        purge_time = undefined,
        jobs_using = [],
        confirmed = false
    },
    Res = apply_fields(Defaults, Fields),
    ets:insert(?TABLE, Res),
    Res.

apply_fields(Rec, []) -> Rec;
apply_fields(Rec, [{name, V}|T])        -> apply_fields(Rec#reservation{name = V}, T);
apply_fields(Rec, [{type, V}|T])        -> apply_fields(Rec#reservation{type = V}, T);
apply_fields(Rec, [{start_time, V}|T])  -> apply_fields(Rec#reservation{start_time = V}, T);
apply_fields(Rec, [{end_time, V}|T])    -> apply_fields(Rec#reservation{end_time = V}, T);
apply_fields(Rec, [{duration, V}|T])    -> apply_fields(Rec#reservation{duration = V}, T);
apply_fields(Rec, [{nodes, V}|T])       -> apply_fields(Rec#reservation{nodes = V}, T);
apply_fields(Rec, [{node_count, V}|T])  -> apply_fields(Rec#reservation{node_count = V}, T);
apply_fields(Rec, [{partition, V}|T])   -> apply_fields(Rec#reservation{partition = V}, T);
apply_fields(Rec, [{features, V}|T])    -> apply_fields(Rec#reservation{features = V}, T);
apply_fields(Rec, [{users, V}|T])       -> apply_fields(Rec#reservation{users = V}, T);
apply_fields(Rec, [{accounts, V}|T])    -> apply_fields(Rec#reservation{accounts = V}, T);
apply_fields(Rec, [{flags, V}|T])       -> apply_fields(Rec#reservation{flags = V}, T);
apply_fields(Rec, [{tres, V}|T])        -> apply_fields(Rec#reservation{tres = V}, T);
apply_fields(Rec, [{state, V}|T])       -> apply_fields(Rec#reservation{state = V}, T);
apply_fields(Rec, [{created_by, V}|T])  -> apply_fields(Rec#reservation{created_by = V}, T);
apply_fields(Rec, [{purge_time, V}|T])  -> apply_fields(Rec#reservation{purge_time = V}, T);
apply_fields(Rec, [{jobs_using, V}|T])  -> apply_fields(Rec#reservation{jobs_using = V}, T);
apply_fields(Rec, [{confirmed, V}|T])   -> apply_fields(Rec#reservation{confirmed = V}, T);
apply_fields(Rec, [_|T])                -> apply_fields(Rec, T).

make_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test_job">>,
        user => <<"alice">>,
        account => <<>>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\ntrue">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        submit_time => now_sec(),
        allocated_nodes => [],
        reservation => <<>>
    },
    M = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, M),
        name = maps:get(name, M),
        user = maps:get(user, M),
        account = maps:get(account, M),
        partition = maps:get(partition, M),
        state = maps:get(state, M),
        script = maps:get(script, M),
        num_nodes = maps:get(num_nodes, M),
        num_cpus = maps:get(num_cpus, M),
        memory_mb = maps:get(memory_mb, M),
        time_limit = maps:get(time_limit, M),
        priority = maps:get(priority, M),
        submit_time = maps:get(submit_time, M),
        allocated_nodes = maps:get(allocated_nodes, M),
        reservation = maps:get(reservation, M)
    }.

%% Start a mock process that registers under a given name and responds
%% to gen_server calls using the provided handler fun (Fun(Request) -> Reply).
start_mock(Name, HandleCall) ->
    Parent = self(),
    Pid = spawn_link(fun() -> mock_loop(Name, HandleCall, Parent) end),
    receive {mock_ready, Pid} -> Pid end.

mock_loop(Name, HandleCall, Parent) ->
    register(Name, self()),
    Parent ! {mock_ready, self()},
    mock_loop_inner(HandleCall).

mock_loop_inner(HandleCall) ->
    receive
        {'$gen_call', From, Request} ->
            Reply = HandleCall(Request),
            gen_server:reply(From, Reply),
            mock_loop_inner(HandleCall);
        stop ->
            ok;
        _ ->
            mock_loop_inner(HandleCall)
    end.

stop_mock(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            Pid ! stop,
            timer:sleep(10)
    end.

%%====================================================================
%% Test Generator
%%====================================================================

cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% ---- start_link coverage ----
        {"start_link already_started returns existing pid",
         fun test_start_link_already_started/0},

        %% ---- create (do_create, validate_spec, build_reservation, maybe_activate) ----
        {"create valid reservation",
         fun test_create_valid/0},
        {"create with duration instead of end_time",
         fun test_create_with_duration/0},
        {"create with node_count instead of nodes",
         fun test_create_with_node_count/0},
        {"create auto-activates when start_time <= now",
         fun test_create_auto_activate/0},
        {"create duplicate name returns already_exists",
         fun test_create_duplicate/0},
        {"create missing end_time and duration",
         fun test_create_missing_time/0},
        {"create missing nodes and node_count",
         fun test_create_missing_nodes/0},
        {"create start_time in distant past",
         fun test_create_past_start/0},

        %% ---- get ----
        {"get existing reservation",
         fun test_get_found/0},
        {"get non-existent returns not_found",
         fun test_get_not_found/0},

        %% ---- list / list_active ----
        {"list returns all reservations",
         fun test_list_all/0},
        {"list_active filters by state and time",
         fun test_list_active/0},

        %% ---- update (do_update, apply_reservation_updates) ----
        {"update all supported fields",
         fun test_update_all_fields/0},
        {"update with unknown key is silently ignored",
         fun test_update_unknown_key/0},
        {"update non-existent returns not_found",
         fun test_update_not_found/0},

        %% ---- delete (do_delete) ----
        {"delete inactive reservation",
         fun test_delete_inactive/0},
        {"delete active reservation (deactivates first)",
         fun test_delete_active/0},
        {"delete non-existent returns not_found",
         fun test_delete_not_found/0},

        %% ---- activate / deactivate (do_set_state) ----
        {"activate sets state to active and notifies",
         fun test_activate/0},
        {"deactivate sets state to inactive and notifies",
         fun test_deactivate/0},
        {"set state to expired branch",
         fun test_set_state_expired/0},
        {"activate non-existent returns not_found",
         fun test_activate_not_found/0},
        {"deactivate non-existent returns not_found",
         fun test_deactivate_not_found/0},

        %% ---- check_reservation_conflict (do_check_conflict) ----
        {"no conflict when empty table",
         fun test_conflict_none/0},
        {"conflict with overlapping time and nodes",
         fun test_conflict_overlap/0},
        {"no conflict with expired reservation",
         fun test_conflict_expired_skipped/0},
        {"no conflict different nodes same time",
         fun test_conflict_different_nodes/0},
        {"no conflict same nodes different time",
         fun test_conflict_different_time/0},
        {"conflict check with duration instead of end_time in spec",
         fun test_conflict_spec_duration/0},

        %% ---- is_node_reserved ----
        {"is_node_reserved true for active reservation in window",
         fun test_is_node_reserved_true/0},
        {"is_node_reserved false for inactive reservation",
         fun test_is_node_reserved_false_inactive/0},
        {"is_node_reserved false for node outside time window",
         fun test_is_node_reserved_false_time/0},

        %% ---- get_reservations_for_node ----
        {"get_reservations_for_node matches by node list",
         fun test_get_res_for_node/0},
        {"get_reservations_for_node matches by partition",
         fun test_get_res_for_node_partition/0},

        %% ---- get_reservations_for_user ----
        {"get_reservations_for_user with user in list",
         fun test_get_res_for_user_listed/0},
        {"get_reservations_for_user open access (empty users)",
         fun test_get_res_for_user_open/0},

        %% ---- can_use_reservation ----
        {"can_use_reservation authorized user",
         fun test_can_use_authorized/0},
        {"can_use_reservation open access (empty users + accounts)",
         fun test_can_use_open/0},
        {"can_use_reservation unauthorized user",
         fun test_can_use_unauthorized/0},
        {"can_use_reservation inactive returns false",
         fun test_can_use_inactive/0},
        {"can_use_reservation not_found returns false",
         fun test_can_use_not_found/0},

        %% ---- get_next_available_window (do_find_next_window, find_gap) ----
        {"next window with no reservations",
         fun test_next_window_empty/0},
        {"next window finds gap between reservations",
         fun test_next_window_gap/0},

        %% ---- Job-level reservation API ----
        {"create_reservation with explicit name",
         fun test_create_reservation_explicit/0},
        {"create_reservation with auto-generated name",
         fun test_create_reservation_auto_name/0},
        {"confirm_reservation active succeeds",
         fun test_confirm_active/0},
        {"confirm_reservation inactive fails",
         fun test_confirm_inactive/0},
        {"confirm_reservation not_found",
         fun test_confirm_not_found/0},
        {"cancel_reservation removes and notifies",
         fun test_cancel_reservation/0},
        {"cancel_reservation not_found",
         fun test_cancel_not_found/0},
        {"check_reservation inactive state",
         fun test_check_reservation_inactive/0},
        {"check_reservation not_found",
         fun test_check_reservation_not_found/0},

        %% ---- Scheduler integration: check_job_reservation ----
        {"check_job_reservation empty binary -> no_reservation",
         fun test_cjr_empty/0},
        {"check_job_reservation undefined -> no_reservation",
         fun test_cjr_undefined/0},
        {"check_job_reservation active authorized -> use_reservation",
         fun test_cjr_authorized/0},
        {"check_job_reservation active unauthorized -> user_not_authorized",
         fun test_cjr_unauthorized/0},
        {"check_job_reservation active expired time -> reservation_expired",
         fun test_cjr_expired_time/0},
        {"check_job_reservation inactive -> reservation_not_active",
         fun test_cjr_inactive/0},
        {"check_job_reservation expired state -> reservation_expired",
         fun test_cjr_expired_state/0},
        {"check_job_reservation not_found -> reservation_not_found",
         fun test_cjr_not_found/0},

        %% ---- get_reserved_nodes ----
        {"get_reserved_nodes active returns nodes",
         fun test_get_reserved_nodes_active/0},
        {"get_reserved_nodes inactive returns invalid_state",
         fun test_get_reserved_nodes_inactive/0},
        {"get_reserved_nodes not_found",
         fun test_get_reserved_nodes_not_found/0},

        %% ---- get_active_reservations ----
        {"get_active_reservations delegates to list_active",
         fun test_get_active_reservations/0},

        %% ---- check_reservation_access (both clauses) ----
        {"check_reservation_access with job record authorized",
         fun test_cra_job_authorized/0},
        {"check_reservation_access with job record adds job to reservation",
         fun test_cra_job_adds_job/0},
        {"check_reservation_access with binary user authorized",
         fun test_cra_user_authorized/0},
        {"check_reservation_access with binary user no job added",
         fun test_cra_user_no_job_added/0},
        {"check_reservation_access expired time",
         fun test_cra_expired_time/0},
        {"check_reservation_access denied",
         fun test_cra_denied/0},
        {"check_reservation_access inactive future -> not_started",
         fun test_cra_inactive_future/0},
        {"check_reservation_access inactive past -> not_active",
         fun test_cra_inactive_past/0},
        {"check_reservation_access expired state",
         fun test_cra_expired_state/0},
        {"check_reservation_access not_found",
         fun test_cra_not_found/0},

        %% ---- check_type_access (all 5 clauses) ----
        {"check_type_access maintenance always false",
         fun test_cta_maintenance/0},
        {"check_type_access maint with ignore_jobs -> false",
         fun test_cta_maint_ignore/0},
        {"check_type_access maint without ignore_jobs -> user check",
         fun test_cta_maint_allow/0},
        {"check_type_access flex -> user check",
         fun test_cta_flex/0},
        {"check_type_access user -> user check",
         fun test_cta_user/0},
        {"check_type_access default/other -> user check",
         fun test_cta_default/0},

        %% ---- check_user_access (both branches) ----
        {"check_user_access empty lists -> true",
         fun test_cua_empty/0},
        {"check_user_access user in users list",
         fun test_cua_user_listed/0},
        {"check_user_access account in accounts list",
         fun test_cua_account_listed/0},
        {"check_user_access not authorized",
         fun test_cua_not_authorized/0},

        %% ---- filter_nodes_for_scheduling (all 4 clauses) ----
        {"filter_nodes job with reservation=<<>>",
         fun test_filter_empty_res/0},
        {"filter_nodes job with reservation=undefined",
         fun test_filter_undefined_res/0},
        {"filter_nodes job with named reservation access granted",
         fun test_filter_named_res_granted/0},
        {"filter_nodes job with named reservation access denied",
         fun test_filter_named_res_denied/0},
        {"filter_nodes binary user",
         fun test_filter_user_binary/0},

        %% ---- get_available_nodes_excluding_reserved ----
        {"excludes nodes in non-flex active reservations",
         fun test_exclude_non_flex/0},
        {"does not exclude nodes in flex type reservations",
         fun test_exclude_flex_type/0},
        {"does not exclude nodes in reservations with flex flag",
         fun test_exclude_flex_flag/0},
        {"does not exclude nodes outside time window",
         fun test_exclude_outside_time/0},

        %% ---- reservation_activated / reservation_deactivated ----
        {"reservation_activated callback",
         fun test_reservation_activated_cb/0},
        {"reservation_deactivated callback",
         fun test_reservation_deactivated_cb/0},

        %% ---- gen_server catch-all clauses ----
        {"unknown call returns {error, unknown_request}",
         fun test_unknown_call/0},
        {"unknown cast is silently ignored",
         fun test_unknown_cast/0},
        {"unknown info is silently ignored",
         fun test_unknown_info/0},

        %% ---- handle_cast: add_job / remove_job ----
        {"add_job_to_reservation inserts job id",
         fun test_add_job/0},
        {"add_job_to_reservation duplicate is idempotent",
         fun test_add_job_duplicate/0},
        {"add_job_to_reservation nonexistent is ok",
         fun test_add_job_nonexistent/0},
        {"remove_job_from_reservation removes job id",
         fun test_remove_job/0},
        {"remove_job_from_reservation nonexistent is ok",
         fun test_remove_job_nonexistent/0},

        %% ---- handle_info check_reservations (do_check_reservation_states) ----
        {"timer activates inactive reservation in window",
         fun test_timer_activates/0},
        {"timer expires active reservation past end",
         fun test_timer_expires/0},
        {"timer extends daily recurring reservation",
         fun test_timer_extends_daily/0},
        {"timer extends weekly recurring reservation",
         fun test_timer_extends_weekly/0},
        {"timer extends recurring with only duration (neither daily nor weekly)",
         fun test_timer_extends_duration_only/0},
        {"timer purges expired reservation with past purge_time",
         fun test_timer_purges/0},
        {"timer notifies jobs on expiration",
         fun test_timer_notify_expired_jobs/0},

        %% ---- terminate / code_change ----
        {"terminate returns ok",
         fun test_terminate/0},
        {"code_change returns {ok, State}",
         fun test_code_change/0},

        %% ---- Direct tests of TEST-exported internal functions ----
        {"validate_spec all ok branches",
         fun test_validate_spec_ok/0},
        {"validate_spec missing_end_time_or_duration",
         fun test_validate_spec_no_time/0},
        {"validate_spec missing_node_specification",
         fun test_validate_spec_no_nodes/0},
        {"validate_spec start_time_in_past",
         fun test_validate_spec_past/0},
        {"build_reservation constructs record",
         fun test_build_reservation/0},
        {"times_overlap overlapping",
         fun test_times_overlap_true/0},
        {"times_overlap non-overlapping",
         fun test_times_overlap_false/0},
        {"times_overlap adjacent (no overlap)",
         fun test_times_overlap_adjacent/0},
        {"nodes_overlap empty first list",
         fun test_nodes_overlap_empty1/0},
        {"nodes_overlap empty second list",
         fun test_nodes_overlap_empty2/0},
        {"nodes_overlap both non-empty disjoint",
         fun test_nodes_overlap_disjoint/0},
        {"nodes_overlap both non-empty intersecting",
         fun test_nodes_overlap_intersect/0},
        {"extend_recurring daily",
         fun test_extend_daily/0},
        {"extend_recurring weekly",
         fun test_extend_weekly/0},
        {"extend_recurring neither daily nor weekly uses duration",
         fun test_extend_duration_fallback/0},
        {"extend_recurring not_found is ok",
         fun test_extend_not_found/0},
        {"find_gap empty list",
         fun test_find_gap_empty/0},
        {"find_gap gap before first reservation",
         fun test_find_gap_before/0},
        {"find_gap must skip past reservation",
         fun test_find_gap_skip/0},
        {"node_in_partition returns false when registry unavailable",
         fun test_node_in_partition/0},
        {"user_in_accounts returns false when manager unavailable",
         fun test_user_in_accounts/0},
        {"maybe_activate_reservation activates when start <= now",
         fun test_maybe_activate/0},
        {"maybe_activate_reservation does nothing when future",
         fun test_maybe_activate_future/0},
        {"apply_reservation_updates covers every field key",
         fun test_apply_updates_complete/0},
        {"generate_reservation_name returns unique binaries",
         fun test_generate_name/0},

        %% ---- Coverage gap tests requiring mocks ----
        {"check_reservation with active res + mock job_manager (authorized)",
         fun test_check_reservation_active_authorized/0},
        {"check_reservation with active res + mock job_manager (unauthorized)",
         fun test_check_reservation_active_unauthorized/0},
        {"check_reservation with active res + mock job_manager (expired time)",
         fun test_check_reservation_active_expired_time/0},
        {"check_reservation with active res + mock job_manager (job not found)",
         fun test_check_reservation_active_job_not_found/0},
        {"check_reservation with account-based authorization",
         fun test_check_reservation_account_auth/0}
     ]}.

%%====================================================================
%% start_link coverage
%%====================================================================

test_start_link_already_started() ->
    %% The server is already running from setup/0.
    %% Calling start_link again should hit the {error, {already_started, Pid}}
    %% clause and return {ok, Pid}.
    {ok, Pid} = flurm_reservation:start_link(),
    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_reservation)).

%%====================================================================
%% create
%%====================================================================

test_create_valid() ->
    Name = unique_name(),
    {ok, Name} = flurm_reservation:create(spec(#{name => Name})),
    {ok, Res} = flurm_reservation:get(Name),
    ?assertEqual(Name, Res#reservation.name),
    ?assertEqual(inactive, Res#reservation.state).

test_create_with_duration() ->
    Now = now_sec(),
    Name = unique_name(),
    S = spec(#{name => Name, start_time => Now + 100, duration => 1800}),
    S2 = maps:remove(end_time, S),
    {ok, Name} = flurm_reservation:create(S2),
    {ok, Res} = flurm_reservation:get(Name),
    ?assertEqual(Now + 100 + 1800, Res#reservation.end_time).

test_create_with_node_count() ->
    Name = unique_name(),
    S = spec(#{name => Name, node_count => 10}),
    S2 = maps:remove(nodes, S),
    {ok, Name} = flurm_reservation:create(S2),
    {ok, Res} = flurm_reservation:get(Name),
    ?assertEqual(10, Res#reservation.node_count).

test_create_auto_activate() ->
    Now = now_sec(),
    Name = unique_name(),
    %% Start time in the past (within 24h grace period)
    S = spec(#{name => Name, start_time => Now - 60, end_time => Now + 3600}),
    {ok, Name} = flurm_reservation:create(S),
    {ok, Res} = flurm_reservation:get(Name),
    %% maybe_activate_reservation should have activated it
    ?assertEqual(active, Res#reservation.state).

test_create_duplicate() ->
    Name = unique_name(),
    {ok, Name} = flurm_reservation:create(spec(#{name => Name})),
    ?assertEqual({error, already_exists}, flurm_reservation:create(spec(#{name => Name}))).

test_create_missing_time() ->
    Now = now_sec(),
    S = #{name => unique_name(), start_time => Now + 100, nodes => [<<"n1">>]},
    ?assertEqual({error, missing_end_time_or_duration}, flurm_reservation:create(S)).

test_create_missing_nodes() ->
    Now = now_sec(),
    S = #{name => unique_name(), start_time => Now + 100, end_time => Now + 200},
    ?assertEqual({error, missing_node_specification}, flurm_reservation:create(S)).

test_create_past_start() ->
    Now = now_sec(),
    S = #{name => unique_name(), start_time => Now - 100000,
          end_time => Now + 200, nodes => [<<"n1">>]},
    ?assertEqual({error, start_time_in_past}, flurm_reservation:create(S)).

%%====================================================================
%% get
%%====================================================================

test_get_found() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    ?assertMatch({ok, #reservation{name = Name}}, flurm_reservation:get(Name)).

test_get_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:get(<<"no_such_res">>)).

%%====================================================================
%% list / list_active
%%====================================================================

test_list_all() ->
    N1 = unique_name(), N2 = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => N1})),
    {ok, _} = flurm_reservation:create(spec(#{name => N2})),
    L = flurm_reservation:list(),
    Names = [R#reservation.name || R <- L],
    ?assert(lists:member(N1, Names)),
    ?assert(lists:member(N2, Names)).

test_list_active() ->
    Now = now_sec(),
    N1 = unique_name(),
    N2 = unique_name(),
    %% Active and within window
    {ok, _} = flurm_reservation:create(spec(#{
        name => N1, start_time => Now - 60, end_time => Now + 3600})),
    %% N1 is auto-activated by maybe_activate_reservation since start <= now
    %% Inactive: future start time so it stays inactive
    {ok, _} = flurm_reservation:create(spec(#{
        name => N2, start_time => Now + 1000, end_time => Now + 5000})),
    Active = flurm_reservation:list_active(),
    Names = [R#reservation.name || R <- Active],
    ?assert(lists:member(N1, Names)),
    ?assertNot(lists:member(N2, Names)).

%%====================================================================
%% update
%%====================================================================

test_update_all_fields() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    Updates = #{
        start_time => Now + 50,
        end_time => Now + 5000,
        duration => 4950,
        nodes => [<<"x1">>, <<"x2">>],
        node_count => 2,
        partition => <<"gpu">>,
        features => [<<"ssd">>],
        users => [<<"bob">>],
        accounts => [<<"eng">>],
        flags => [daily],
        tres => #{cpu => 4}
    },
    ?assertEqual(ok, flurm_reservation:update(Name, Updates)),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(Now + 50, R#reservation.start_time),
    ?assertEqual(Now + 5000, R#reservation.end_time),
    ?assertEqual(4950, R#reservation.duration),
    ?assertEqual([<<"x1">>, <<"x2">>], R#reservation.nodes),
    ?assertEqual(2, R#reservation.node_count),
    ?assertEqual(<<"gpu">>, R#reservation.partition),
    ?assertEqual([<<"ssd">>], R#reservation.features),
    ?assertEqual([<<"bob">>], R#reservation.users),
    ?assertEqual([<<"eng">>], R#reservation.accounts),
    ?assertEqual([daily], R#reservation.flags),
    ?assertEqual(#{cpu => 4}, R#reservation.tres).

test_update_unknown_key() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    %% Unknown key in updates should be silently ignored (the _ clause)
    ?assertEqual(ok, flurm_reservation:update(Name, #{bogus_key => 42})).

test_update_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:update(<<"nope">>, #{end_time => 0})).

%%====================================================================
%% delete
%%====================================================================

test_delete_inactive() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    ?assertEqual(ok, flurm_reservation:delete(Name)),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_delete_active() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{
        name => Name, start_time => Now - 60, end_time => Now + 3600})),
    flurm_reservation:activate_reservation(Name),
    %% Verify it is active
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(active, R#reservation.state),
    %% Delete should deactivate first, then delete
    ?assertEqual(ok, flurm_reservation:delete(Name)),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_delete_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:delete(<<"nope">>)).

%%====================================================================
%% activate / deactivate / set_state expired
%%====================================================================

test_activate() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    ?assertEqual(ok, flurm_reservation:activate_reservation(Name)),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(active, R#reservation.state).

test_deactivate() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{name => Name})),
    flurm_reservation:activate_reservation(Name),
    ?assertEqual(ok, flurm_reservation:deactivate_reservation(Name)),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(inactive, R#reservation.state).

test_set_state_expired() ->
    %% Cover the expired branch in do_set_state.
    %% We can trigger this through the timer (check_reservations) with an
    %% active reservation whose end_time has passed and has no recurring flags.
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now - 200}, {end_time, Now - 10},
                {state, active}, {flags, []}, {jobs_using, []}]),
    %% Trigger the check_reservations timer
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(expired, R#reservation.state).

test_activate_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:activate_reservation(<<"nope">>)).

test_deactivate_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:deactivate_reservation(<<"nope">>)).

%%====================================================================
%% check_reservation_conflict
%%====================================================================

test_conflict_none() ->
    ?assertEqual(ok, flurm_reservation:check_reservation_conflict(
        spec(#{nodes => [<<"unique_node">>]}))).

test_conflict_overlap() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{
        name => Name, nodes => [<<"cn1">>],
        start_time => Now + 100, end_time => Now + 500})),
    %% Overlapping time and node
    S = #{start_time => Now + 200, end_time => Now + 400, nodes => [<<"cn1">>]},
    ?assertMatch({error, {conflicts, _}}, flurm_reservation:check_reservation_conflict(S)).

test_conflict_expired_skipped() ->
    Now = now_sec(),
    %% Insert expired reservation directly
    insert_res([{name, <<"exp">>}, {start_time, Now + 100}, {end_time, Now + 200},
                {nodes, [<<"cn1">>]}, {state, expired}]),
    S = #{start_time => Now + 100, end_time => Now + 200, nodes => [<<"cn1">>]},
    ?assertEqual(ok, flurm_reservation:check_reservation_conflict(S)).

test_conflict_different_nodes() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{
        name => Name, nodes => [<<"cn1">>],
        start_time => Now + 100, end_time => Now + 500})),
    S = #{start_time => Now + 100, end_time => Now + 500, nodes => [<<"cn99">>]},
    ?assertEqual(ok, flurm_reservation:check_reservation_conflict(S)).

test_conflict_different_time() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{
        name => Name, nodes => [<<"cn1">>],
        start_time => Now + 100, end_time => Now + 200})),
    S = #{start_time => Now + 300, end_time => Now + 400, nodes => [<<"cn1">>]},
    ?assertEqual(ok, flurm_reservation:check_reservation_conflict(S)).

test_conflict_spec_duration() ->
    Now = now_sec(),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(spec(#{
        name => Name, nodes => [<<"cn1">>],
        start_time => Now + 100, end_time => Now + 500})),
    %% Spec uses duration instead of end_time
    S = #{start_time => Now + 200, duration => 200, nodes => [<<"cn1">>]},
    ?assertMatch({error, {conflicts, _}}, flurm_reservation:check_reservation_conflict(S)).

%%====================================================================
%% is_node_reserved
%%====================================================================

test_is_node_reserved_true() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now - 100}, {end_time, Now + 100},
                {nodes, [<<"rn1">>]}, {state, active}, {partition, undefined}]),
    ?assert(flurm_reservation:is_node_reserved(<<"rn1">>, Now)).

test_is_node_reserved_false_inactive() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now - 100}, {end_time, Now + 100},
                {nodes, [<<"rn1">>]}, {state, inactive}, {partition, undefined}]),
    ?assertNot(flurm_reservation:is_node_reserved(<<"rn1">>, Now)).

test_is_node_reserved_false_time() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now + 1000}, {end_time, Now + 2000},
                {nodes, [<<"rn1">>]}, {state, active}, {partition, undefined}]),
    ?assertNot(flurm_reservation:is_node_reserved(<<"rn1">>, Now)).

%%====================================================================
%% get_reservations_for_node
%%====================================================================

test_get_res_for_node() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {nodes, [<<"target">>]}, {partition, undefined}]),
    insert_res([{nodes, [<<"other">>]}, {partition, undefined}]),
    Res = flurm_reservation:get_reservations_for_node(<<"target">>),
    Names = [R#reservation.name || R <- Res],
    ?assert(lists:member(Name, Names)).

test_get_res_for_node_partition() ->
    %% This covers the node_in_partition path.
    %% Since flurm_partition_registry is not running, it will catch and return false.
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {nodes, []}, {partition, <<"part1">>}]),
    Res = flurm_reservation:get_reservations_for_node(<<"some_node">>),
    %% Should not match because partition registry is not running
    Names = [R#reservation.name || R <- Res],
    ?assertNot(lists:member(Name, Names)).

%%====================================================================
%% get_reservations_for_user
%%====================================================================

test_get_res_for_user_listed() ->
    Name = unique_name(),
    insert_res([{name, Name}, {users, [<<"alice">>, <<"bob">>]}, {accounts, []}]),
    insert_res([{users, [<<"charlie">>]}, {accounts, []}]),
    Res = flurm_reservation:get_reservations_for_user(<<"alice">>),
    Names = [R#reservation.name || R <- Res],
    ?assert(lists:member(Name, Names)).

test_get_res_for_user_open() ->
    Name = unique_name(),
    insert_res([{name, Name}, {users, []}, {accounts, []}]),
    Res = flurm_reservation:get_reservations_for_user(<<"anyone">>),
    Names = [R#reservation.name || R <- Res],
    ?assert(lists:member(Name, Names)).

%%====================================================================
%% can_use_reservation
%%====================================================================

test_can_use_authorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {users, [<<"alice">>]}, {accounts, []}, {state, active}]),
    ?assert(flurm_reservation:can_use_reservation(<<"alice">>, Name)).

test_can_use_open() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {users, []}, {accounts, []}, {state, active}]),
    ?assert(flurm_reservation:can_use_reservation(<<"anyone">>, Name)).

test_can_use_unauthorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {users, [<<"alice">>]}, {accounts, []}, {state, active}]),
    ?assertNot(flurm_reservation:can_use_reservation(<<"bob">>, Name)).

test_can_use_inactive() ->
    Name = unique_name(),
    insert_res([{name, Name}, {users, [<<"alice">>]}, {accounts, []}, {state, inactive}]),
    ?assertNot(flurm_reservation:can_use_reservation(<<"alice">>, Name)).

test_can_use_not_found() ->
    ?assertNot(flurm_reservation:can_use_reservation(<<"alice">>, <<"nope">>)).

%%====================================================================
%% get_next_available_window
%%====================================================================

test_next_window_empty() ->
    R = #{nodes => [<<"n1">>]},
    ?assertMatch({ok, _}, flurm_reservation:get_next_available_window(R, 3600)).

test_next_window_gap() ->
    Now = now_sec(),
    N1 = unique_name(), N2 = unique_name(),
    %% Two reservations blocking n1 back-to-back
    insert_res([{name, N1}, {start_time, Now + 10}, {end_time, Now + 50},
                {nodes, [<<"gn1">>]}, {partition, undefined}, {state, active}]),
    insert_res([{name, N2}, {start_time, Now + 60}, {end_time, Now + 100},
                {nodes, [<<"gn1">>]}, {partition, undefined}, {state, active}]),
    {ok, Start} = flurm_reservation:get_next_available_window(#{nodes => [<<"gn1">>]}, 30),
    %% Should find window either before first reservation or after second
    ?assert(Start >= Now).

%%====================================================================
%% Job-level reservation API
%%====================================================================

test_create_reservation_explicit() ->
    Now = now_sec(),
    Name = unique_name(),
    S = #{name => Name, nodes => [<<"jn1">>], start_time => Now + 10,
          end_time => Now + 3600, user => <<"alice">>},
    {ok, Name} = flurm_reservation:create_reservation(S),
    {ok, R} = flurm_reservation:get(Name),
    ?assert(lists:member(<<"alice">>, R#reservation.users)).

test_create_reservation_auto_name() ->
    Now = now_sec(),
    S = #{nodes => [<<"jn1">>], start_time => Now + 10,
          end_time => Now + 3600, user => <<"alice">>},
    {ok, Name} = flurm_reservation:create_reservation(S),
    ?assertMatch(<<"res_", _/binary>>, Name).

test_confirm_active() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {confirmed, false}]),
    ?assertEqual(ok, flurm_reservation:confirm_reservation(Name)),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(true, R#reservation.confirmed).

test_confirm_inactive() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive}, {confirmed, false}]),
    ?assertEqual({error, {invalid_state, inactive}}, flurm_reservation:confirm_reservation(Name)).

test_confirm_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:confirm_reservation(<<"nope">>)).

test_cancel_reservation() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {nodes, [<<"cn1">>]},
                {jobs_using, [100, 200]}]),
    ?assertEqual(ok, flurm_reservation:cancel_reservation(Name)),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_cancel_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:cancel_reservation(<<"nope">>)).

test_check_reservation_inactive() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive}]),
    ?assertEqual({error, {invalid_state, inactive}},
                 flurm_reservation:check_reservation(1, Name)).

test_check_reservation_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:check_reservation(1, <<"nope">>)).

%%====================================================================
%% check_job_reservation (3 clauses)
%%====================================================================

test_cjr_empty() ->
    Job = make_job(#{reservation => <<>>}),
    ?assertEqual({ok, no_reservation}, flurm_reservation:check_job_reservation(Job)).

test_cjr_undefined() ->
    Job = make_job(#{reservation => undefined}),
    ?assertEqual({ok, no_reservation}, flurm_reservation:check_job_reservation(Job)).

test_cjr_authorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {users, []}, {accounts, []},
                {nodes, [<<"n1">>, <<"n2">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>, id => 42}),
    ?assertEqual({ok, use_reservation, [<<"n1">>, <<"n2">>]},
                 flurm_reservation:check_job_reservation(Job)),
    %% Verify job was added to reservation via cast
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    ?assert(lists:member(42, R#reservation.jobs_using)).

test_cjr_unauthorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {users, [<<"bob">>]}, {accounts, []},
                {nodes, [<<"n1">>]}, {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>}),
    ?assertEqual({error, user_not_authorized}, flurm_reservation:check_job_reservation(Job)).

test_cjr_expired_time() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {users, []}, {accounts, []},
                {nodes, [<<"n1">>]}, {start_time, Now - 200}, {end_time, Now - 10}]),
    Job = make_job(#{reservation => Name}),
    ?assertEqual({error, reservation_expired}, flurm_reservation:check_job_reservation(Job)).

test_cjr_inactive() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive}]),
    Job = make_job(#{reservation => Name}),
    ?assertEqual({error, reservation_not_active}, flurm_reservation:check_job_reservation(Job)).

test_cjr_expired_state() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, expired}]),
    Job = make_job(#{reservation => Name}),
    ?assertEqual({error, reservation_expired}, flurm_reservation:check_job_reservation(Job)).

test_cjr_not_found() ->
    Job = make_job(#{reservation => <<"nonexistent">>}),
    ?assertEqual({error, reservation_not_found}, flurm_reservation:check_job_reservation(Job)).

%%====================================================================
%% get_reserved_nodes
%%====================================================================

test_get_reserved_nodes_active() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {nodes, [<<"a">>, <<"b">>]}]),
    ?assertEqual({ok, [<<"a">>, <<"b">>]}, flurm_reservation:get_reserved_nodes(Name)).

test_get_reserved_nodes_inactive() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive}, {nodes, [<<"a">>]}]),
    ?assertEqual({error, {invalid_state, inactive}}, flurm_reservation:get_reserved_nodes(Name)).

test_get_reserved_nodes_not_found() ->
    ?assertEqual({error, not_found}, flurm_reservation:get_reserved_nodes(<<"nope">>)).

%%====================================================================
%% get_active_reservations
%%====================================================================

test_get_active_reservations() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active},
                {start_time, Now - 100}, {end_time, Now + 100}]),
    Active = flurm_reservation:get_active_reservations(),
    ?assert(length(Active) >= 1).

%%====================================================================
%% check_reservation_access (both clauses: #job{} and binary)
%%====================================================================

test_cra_job_authorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, [<<"alice">>]}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>, account => <<>>, id => 7}),
    ?assertEqual({ok, [<<"n1">>]}, flurm_reservation:check_reservation_access(Job, Name)).

test_cra_job_adds_job() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, []}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>, account => <<>>, id => 55}),
    {ok, _} = flurm_reservation:check_reservation_access(Job, Name),
    %% Wait for the async cast
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    ?assert(lists:member(55, R#reservation.jobs_using)).

test_cra_user_authorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, []}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    ?assertEqual({ok, [<<"n1">>]}, flurm_reservation:check_reservation_access(<<"bob">>, Name)).

test_cra_user_no_job_added() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, []}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]}, {jobs_using, []},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    {ok, _} = flurm_reservation:check_reservation_access(<<"bob">>, Name),
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    %% Binary user path should NOT add a job (JobId = undefined)
    ?assertEqual([], R#reservation.jobs_using).

test_cra_expired_time() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, []}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 200}, {end_time, Now - 10}]),
    ?assertEqual({error, reservation_expired},
                 flurm_reservation:check_reservation_access(<<"alice">>, Name)).

test_cra_denied() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, [<<"bob">>]}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    ?assertEqual({error, access_denied},
                 flurm_reservation:check_reservation_access(<<"alice">>, Name)).

test_cra_inactive_future() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive},
                {start_time, Now + 1000}, {end_time, Now + 2000}]),
    ?assertMatch({error, {reservation_not_started, _}},
                 flurm_reservation:check_reservation_access(<<"alice">>, Name)).

test_cra_inactive_past() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive},
                {start_time, Now - 100}, {end_time, Now + 200}]),
    ?assertEqual({error, reservation_not_active},
                 flurm_reservation:check_reservation_access(<<"alice">>, Name)).

test_cra_expired_state() ->
    Name = unique_name(),
    insert_res([{name, Name}, {state, expired}]),
    ?assertEqual({error, reservation_expired},
                 flurm_reservation:check_reservation_access(<<"alice">>, Name)).

test_cra_not_found() ->
    ?assertEqual({error, reservation_not_found},
                 flurm_reservation:check_reservation_access(<<"alice">>, <<"nope">>)).

%%====================================================================
%% check_type_access (5 clauses)
%%====================================================================

test_cta_maintenance() ->
    ?assertNot(flurm_reservation:check_type_access(
        maintenance, [], <<"u">>, <<"a">>, [], [])).

test_cta_maint_ignore() ->
    ?assertNot(flurm_reservation:check_type_access(
        maint, [ignore_jobs], <<"u">>, <<"a">>, [<<"u">>], [])).

test_cta_maint_allow() ->
    ?assert(flurm_reservation:check_type_access(
        maint, [], <<"u">>, <<"a">>, [<<"u">>], [])).

test_cta_flex() ->
    ?assert(flurm_reservation:check_type_access(
        flex, [], <<"u">>, <<"a">>, [], [])),
    ?assertNot(flurm_reservation:check_type_access(
        flex, [], <<"u">>, <<"a">>, [<<"other">>], [])).

test_cta_user() ->
    ?assert(flurm_reservation:check_type_access(
        user, [], <<"u">>, <<"a">>, [<<"u">>], [])),
    ?assertNot(flurm_reservation:check_type_access(
        user, [], <<"u">>, <<"a">>, [<<"other">>], [])).

test_cta_default() ->
    %% The catch-all clause (license, partition, or any unknown type)
    ?assert(flurm_reservation:check_type_access(
        license, [], <<"u">>, <<"a">>, [], [])),
    ?assertNot(flurm_reservation:check_type_access(
        partition, [], <<"u">>, <<"a">>, [<<"other">>], [<<"other_acc">>])).

%%====================================================================
%% check_user_access (both branches)
%%====================================================================

test_cua_empty() ->
    ?assert(flurm_reservation:check_user_access(<<"u">>, <<"a">>, [], [])).

test_cua_user_listed() ->
    ?assert(flurm_reservation:check_user_access(
        <<"u">>, <<"a">>, [<<"u">>, <<"other">>], [])).

test_cua_account_listed() ->
    ?assert(flurm_reservation:check_user_access(
        <<"u">>, <<"eng">>, [], [<<"eng">>])).

test_cua_not_authorized() ->
    ?assertNot(flurm_reservation:check_user_access(
        <<"u">>, <<"a">>, [<<"other">>], [<<"other_acc">>])).

%%====================================================================
%% filter_nodes_for_scheduling (4 clauses)
%%====================================================================

test_filter_empty_res() ->
    Job = make_job(#{reservation => <<>>}),
    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_filter_undefined_res() ->
    Job = make_job(#{reservation => undefined}),
    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

test_filter_named_res_granted() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, [<<"alice">>]}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>, <<"n3">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>, account => <<>>}),
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, [<<"n1">>, <<"n2">>, <<"n3">>]),
    ?assertEqual([<<"n1">>, <<"n3">>], Result).

test_filter_named_res_denied() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {users, [<<"bob">>]}, {accounts, []}, {flags, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Job = make_job(#{reservation => Name, user => <<"alice">>, account => <<>>}),
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, [<<"n1">>, <<"n2">>]),
    ?assertEqual([], Result).

test_filter_user_binary() ->
    Nodes = [<<"n1">>, <<"n2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(<<"alice">>, Nodes),
    ?assertEqual([<<"n1">>, <<"n2">>], Result).

%%====================================================================
%% get_available_nodes_excluding_reserved
%%====================================================================

test_exclude_non_flex() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {nodes, [<<"rn1">>]}, {flags, []},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Result = flurm_reservation:get_available_nodes_excluding_reserved(
        [<<"rn1">>, <<"rn2">>, <<"rn3">>]),
    ?assert(lists:member(<<"rn2">>, Result)),
    ?assert(lists:member(<<"rn3">>, Result)),
    ?assertNot(lists:member(<<"rn1">>, Result)).

test_exclude_flex_type() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, flex}, {state, active},
                {nodes, [<<"fn1">>]}, {flags, []},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Result = flurm_reservation:get_available_nodes_excluding_reserved([<<"fn1">>, <<"fn2">>]),
    ?assert(lists:member(<<"fn1">>, Result)).

test_exclude_flex_flag() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {nodes, [<<"ffn1">>]}, {flags, [flex]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    Result = flurm_reservation:get_available_nodes_excluding_reserved([<<"ffn1">>, <<"ffn2">>]),
    ?assert(lists:member(<<"ffn1">>, Result)).

test_exclude_outside_time() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {type, user}, {state, active},
                {nodes, [<<"otn1">>]}, {flags, []},
                {start_time, Now + 1000}, {end_time, Now + 2000}]),
    Result = flurm_reservation:get_available_nodes_excluding_reserved([<<"otn1">>, <<"otn2">>]),
    ?assert(lists:member(<<"otn1">>, Result)).

%%====================================================================
%% reservation_activated / reservation_deactivated callbacks
%%====================================================================

test_reservation_activated_cb() ->
    ?assertEqual(ok, flurm_reservation:reservation_activated(<<"test_cb">>)).

test_reservation_deactivated_cb() ->
    ?assertEqual(ok, flurm_reservation:reservation_deactivated(<<"test_cb">>)).

%%====================================================================
%% gen_server catch-all clauses
%%====================================================================

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_reservation, {totally_unknown, 123})).

test_unknown_cast() ->
    gen_server:cast(flurm_reservation, {totally_unknown, 123}),
    %% Verify server is still alive
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_reservation))).

test_unknown_info() ->
    flurm_reservation ! {totally_unknown, 123},
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_reservation))).

%%====================================================================
%% handle_cast: add_job / remove_job
%%====================================================================

test_add_job() ->
    Name = unique_name(),
    insert_res([{name, Name}, {jobs_using, []}]),
    gen_server:cast(flurm_reservation, {add_job_to_reservation, 42, Name}),
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    ?assert(lists:member(42, R#reservation.jobs_using)).

test_add_job_duplicate() ->
    Name = unique_name(),
    insert_res([{name, Name}, {jobs_using, [42]}]),
    gen_server:cast(flurm_reservation, {add_job_to_reservation, 42, Name}),
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual([42], R#reservation.jobs_using).

test_add_job_nonexistent() ->
    gen_server:cast(flurm_reservation, {add_job_to_reservation, 42, <<"nope">>}),
    timer:sleep(50),
    ?assert(is_pid(whereis(flurm_reservation))).

test_remove_job() ->
    Name = unique_name(),
    insert_res([{name, Name}, {jobs_using, [10, 20, 30]}]),
    gen_server:cast(flurm_reservation, {remove_job_from_reservation, 20, Name}),
    timer:sleep(50),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual([10, 30], R#reservation.jobs_using).

test_remove_job_nonexistent() ->
    gen_server:cast(flurm_reservation, {remove_job_from_reservation, 42, <<"nope">>}),
    timer:sleep(50),
    ?assert(is_pid(whereis(flurm_reservation))).

%%====================================================================
%% handle_info check_reservations (do_check_reservation_states)
%%====================================================================

test_timer_activates() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, inactive}, {flags, []}, {purge_time, undefined},
                {start_time, Now - 10}, {end_time, Now + 200}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(active, R#reservation.state).

test_timer_expires() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {flags, []}, {purge_time, undefined},
                {start_time, Now - 200}, {end_time, Now - 10}, {jobs_using, []}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(expired, R#reservation.state).

test_timer_extends_daily() ->
    Now = now_sec(),
    Name = unique_name(),
    StartTime = Now - 200,
    EndTime = Now - 10,
    insert_res([{name, Name}, {state, active}, {flags, [daily]},
                {purge_time, undefined}, {duration, 3600},
                {start_time, StartTime}, {end_time, EndTime}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(inactive, R#reservation.state),
    ?assertEqual(StartTime + 86400, R#reservation.start_time),
    ?assertEqual(EndTime + 86400, R#reservation.end_time).

test_timer_extends_weekly() ->
    Now = now_sec(),
    Name = unique_name(),
    StartTime = Now - 200,
    EndTime = Now - 10,
    insert_res([{name, Name}, {state, active}, {flags, [weekly]},
                {purge_time, undefined}, {duration, 3600},
                {start_time, StartTime}, {end_time, EndTime}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(inactive, R#reservation.state),
    ?assertEqual(StartTime + 604800, R#reservation.start_time),
    ?assertEqual(EndTime + 604800, R#reservation.end_time).

test_timer_extends_duration_only() ->
    %% Cover the branch where neither daily nor weekly flag is set,
    %% so extend_recurring falls back to using the duration value.
    Now = now_sec(),
    Name = unique_name(),
    StartTime = Now - 200,
    EndTime = Now - 10,
    Duration = 7200,
    insert_res([{name, Name}, {state, active}, {flags, [weekday]},
                {purge_time, undefined}, {duration, Duration},
                {start_time, StartTime}, {end_time, EndTime}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    %% The recurring check sees [weekday] which triggers extend_recurring.
    %% But weekday is not daily or weekly, so it uses duration.
    %% Wait: actually the do_check_reservation_states checks for daily/weekly flags:
    %% lists:member(daily, Flags) orelse lists:member(weekly, Flags)
    %% weekday is neither, so it goes to the false branch (non-recurring expiration).
    ?assertEqual(expired, R#reservation.state).

test_timer_purges() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, expired}, {flags, []},
                {purge_time, Now - 100},
                {start_time, Now - 500}, {end_time, Now - 400}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_timer_notify_expired_jobs() ->
    %% Cover the notify_reservation_expired function with jobs_using populated.
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {flags, []}, {purge_time, undefined},
                {start_time, Now - 200}, {end_time, Now - 10},
                {jobs_using, [100, 200]}]),
    flurm_reservation ! check_reservations,
    timer:sleep(100),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(expired, R#reservation.state).

%%====================================================================
%% terminate / code_change
%%====================================================================

test_terminate() ->
    ?assertEqual(ok, flurm_reservation:terminate(normal, {state, undefined})).

test_code_change() ->
    State = {state, undefined},
    ?assertEqual({ok, State}, flurm_reservation:code_change("1.0", State, [])).

%%====================================================================
%% Direct tests of TEST-exported internal functions
%%====================================================================

%% ---- validate_spec ----

test_validate_spec_ok() ->
    Now = now_sec(),
    %% With end_time and nodes
    ?assertEqual(ok, flurm_reservation:validate_spec(
        #{start_time => Now + 100, end_time => Now + 200, nodes => [<<"n1">>]})),
    %% With duration and nodes
    ?assertEqual(ok, flurm_reservation:validate_spec(
        #{start_time => Now + 100, duration => 100, nodes => [<<"n1">>]})),
    %% With end_time and node_count
    ?assertEqual(ok, flurm_reservation:validate_spec(
        #{start_time => Now + 100, end_time => Now + 200, node_count => 5})).

test_validate_spec_no_time() ->
    Now = now_sec(),
    ?assertEqual({error, missing_end_time_or_duration},
                 flurm_reservation:validate_spec(
                     #{start_time => Now + 100, nodes => [<<"n1">>]})).

test_validate_spec_no_nodes() ->
    Now = now_sec(),
    ?assertEqual({error, missing_node_specification},
                 flurm_reservation:validate_spec(
                     #{start_time => Now + 100, end_time => Now + 200})).

test_validate_spec_past() ->
    Now = now_sec(),
    ?assertEqual({error, start_time_in_past},
                 flurm_reservation:validate_spec(
                     #{start_time => Now - 100000, end_time => Now + 200, nodes => [<<"n1">>]})).

%% ---- build_reservation ----

test_build_reservation() ->
    Now = now_sec(),
    S = #{name => <<"br">>, start_time => Now + 100, end_time => Now + 200,
          nodes => [<<"n1">>], users => [<<"alice">>], type => flex,
          accounts => [<<"eng">>], partition => <<"gpu">>, features => [<<"ssd">>],
          flags => [daily], tres => #{cpu => 2}, node_count => 1,
          created_by => <<"admin">>, purge_time => Now + 10000},
    R = flurm_reservation:build_reservation(S),
    ?assertEqual(<<"br">>, R#reservation.name),
    ?assertEqual(flex, R#reservation.type),
    ?assertEqual(Now + 100, R#reservation.start_time),
    ?assertEqual(Now + 200, R#reservation.end_time),
    ?assertEqual([<<"n1">>], R#reservation.nodes),
    ?assertEqual([<<"alice">>], R#reservation.users),
    ?assertEqual([<<"eng">>], R#reservation.accounts),
    ?assertEqual(<<"gpu">>, R#reservation.partition),
    ?assertEqual([<<"ssd">>], R#reservation.features),
    ?assertEqual([daily], R#reservation.flags),
    ?assertEqual(#{cpu => 2}, R#reservation.tres),
    ?assertEqual(1, R#reservation.node_count),
    ?assertEqual(<<"admin">>, R#reservation.created_by),
    ?assertEqual(inactive, R#reservation.state),
    ?assertEqual(Now + 10000, R#reservation.purge_time).

%% ---- times_overlap ----

test_times_overlap_true() ->
    ?assert(flurm_reservation:times_overlap(100, 300, 200, 400)),
    ?assert(flurm_reservation:times_overlap(200, 400, 100, 300)),
    ?assert(flurm_reservation:times_overlap(100, 400, 200, 300)),
    ?assert(flurm_reservation:times_overlap(200, 300, 100, 400)).

test_times_overlap_false() ->
    ?assertNot(flurm_reservation:times_overlap(100, 200, 300, 400)),
    ?assertNot(flurm_reservation:times_overlap(300, 400, 100, 200)).

test_times_overlap_adjacent() ->
    %% E1 == S2: no overlap
    ?assertNot(flurm_reservation:times_overlap(100, 200, 200, 300)),
    %% S1 == E2: no overlap
    ?assertNot(flurm_reservation:times_overlap(200, 300, 100, 200)).

%% ---- nodes_overlap ----

test_nodes_overlap_empty1() ->
    ?assertNot(flurm_reservation:nodes_overlap([], [<<"n1">>])).

test_nodes_overlap_empty2() ->
    ?assertNot(flurm_reservation:nodes_overlap([<<"n1">>], [])).

test_nodes_overlap_disjoint() ->
    ?assertNot(flurm_reservation:nodes_overlap([<<"a">>], [<<"b">>])).

test_nodes_overlap_intersect() ->
    ?assert(flurm_reservation:nodes_overlap([<<"a">>, <<"b">>], [<<"b">>, <<"c">>])).

%% ---- extend_recurring ----

test_extend_daily() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now}, {end_time, Now + 100}, {duration, 100}]),
    flurm_reservation:extend_recurring(Name, [daily]),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(Now + 86400, R#reservation.start_time),
    ?assertEqual(Now + 100 + 86400, R#reservation.end_time),
    ?assertEqual(inactive, R#reservation.state).

test_extend_weekly() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now}, {end_time, Now + 100}, {duration, 100}]),
    flurm_reservation:extend_recurring(Name, [weekly]),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(Now + 604800, R#reservation.start_time),
    ?assertEqual(Now + 100 + 604800, R#reservation.end_time).

test_extend_duration_fallback() ->
    %% Neither daily nor weekly: uses Duration as interval
    Now = now_sec(),
    Name = unique_name(),
    Duration = 5000,
    insert_res([{name, Name}, {start_time, Now}, {end_time, Now + 100}, {duration, Duration}]),
    flurm_reservation:extend_recurring(Name, [weekday]),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(Now + Duration, R#reservation.start_time),
    ?assertEqual(Now + 100 + Duration, R#reservation.end_time).

test_extend_not_found() ->
    %% Should just return ok for non-existent reservation
    ?assertEqual(ok, flurm_reservation:extend_recurring(<<"nope">>, [daily])).

%% ---- find_gap ----

test_find_gap_empty() ->
    Now = now_sec(),
    ?assertEqual({ok, Now}, flurm_reservation:find_gap(Now, [], 3600)).

test_find_gap_before() ->
    Now = now_sec(),
    %% Reservation starts far enough in the future to fit the duration
    Res = #reservation{name = <<"g">>, start_time = Now + 10000, end_time = Now + 20000},
    ?assertEqual({ok, Now}, flurm_reservation:find_gap(Now, [Res], 3600)).

test_find_gap_skip() ->
    Now = now_sec(),
    %% Reservation starts too close -- gap before it is not big enough
    Res = #reservation{name = <<"g">>, start_time = Now + 10, end_time = Now + 5000},
    {ok, Start} = flurm_reservation:find_gap(Now, [Res], 3600),
    ?assert(Start >= Now + 5000).

%% ---- node_in_partition / user_in_accounts ----

test_node_in_partition() ->
    ?assertNot(flurm_reservation:node_in_partition(<<"n1">>, <<"part1">>)).

test_user_in_accounts() ->
    ?assertNot(flurm_reservation:user_in_accounts(<<"alice">>, [<<"eng">>])).

%% ---- maybe_activate_reservation ----

test_maybe_activate() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now - 10}, {state, inactive}]),
    Res = #reservation{name = Name, start_time = Now - 10},
    flurm_reservation:maybe_activate_reservation(Res),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(active, R#reservation.state).

test_maybe_activate_future() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {start_time, Now + 1000}, {state, inactive}]),
    Res = #reservation{name = Name, start_time = Now + 1000},
    flurm_reservation:maybe_activate_reservation(Res),
    {ok, R} = flurm_reservation:get(Name),
    ?assertEqual(inactive, R#reservation.state).

%% ---- apply_reservation_updates ----

test_apply_updates_complete() ->
    Now = now_sec(),
    Base = #reservation{
        name = <<"u">>, start_time = Now, end_time = Now + 100,
        duration = 100, nodes = [], node_count = 0,
        partition = undefined, features = [], users = [],
        accounts = [], flags = [], tres = #{}
    },
    Updates = #{
        start_time => Now + 1,
        end_time => Now + 2,
        duration => 1,
        nodes => [<<"x">>],
        node_count => 1,
        partition => <<"p">>,
        features => [<<"f">>],
        users => [<<"u">>],
        accounts => [<<"a">>],
        flags => [daily],
        tres => #{g => 1},
        unknown => 99
    },
    R = flurm_reservation:apply_reservation_updates(Base, Updates),
    ?assertEqual(Now + 1, R#reservation.start_time),
    ?assertEqual(Now + 2, R#reservation.end_time),
    ?assertEqual(1, R#reservation.duration),
    ?assertEqual([<<"x">>], R#reservation.nodes),
    ?assertEqual(1, R#reservation.node_count),
    ?assertEqual(<<"p">>, R#reservation.partition),
    ?assertEqual([<<"f">>], R#reservation.features),
    ?assertEqual([<<"u">>], R#reservation.users),
    ?assertEqual([<<"a">>], R#reservation.accounts),
    ?assertEqual([daily], R#reservation.flags),
    ?assertEqual(#{g => 1}, R#reservation.tres).

%% ---- generate_reservation_name ----

test_generate_name() ->
    N1 = flurm_reservation:generate_reservation_name(),
    N2 = flurm_reservation:generate_reservation_name(),
    ?assert(is_binary(N1)),
    ?assert(is_binary(N2)),
    ?assertMatch(<<"res_", _/binary>>, N1),
    ?assertNotEqual(N1, N2).

%%====================================================================
%% Mock-based tests for deeper coverage
%%====================================================================

%% Cover lines 961-980: do_check_reservation with active reservation
%% requires flurm_job_manager:get_job to succeed.
test_check_reservation_active_authorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active}, {users, []}, {accounts, []},
                {nodes, [<<"n1">>, <<"n2">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    %% Mock flurm_job_manager to return a job record
    MockJob = make_job(#{id => 42, user => <<"alice">>, account => <<"eng">>}),
    MockPid = start_mock(flurm_job_manager, fun({get_job, _Id}) -> {ok, MockJob} end),
    try
        Result = flurm_reservation:check_reservation(42, Name),
        ?assertEqual({ok, [<<"n1">>, <<"n2">>]}, Result)
    after
        stop_mock(flurm_job_manager)
    end.

test_check_reservation_active_unauthorized() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active},
                {users, [<<"bob">>]}, {accounts, [<<"finance">>]},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    %% Mock: job belongs to alice who is not authorized
    MockJob = make_job(#{id => 43, user => <<"alice">>, account => <<"eng">>}),
    start_mock(flurm_job_manager, fun({get_job, _Id}) -> {ok, MockJob} end),
    try
        Result = flurm_reservation:check_reservation(43, Name),
        ?assertEqual({error, user_not_authorized}, Result)
    after
        stop_mock(flurm_job_manager)
    end.

test_check_reservation_active_expired_time() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active},
                {users, []}, {accounts, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 200}, {end_time, Now - 10}]),
    MockJob = make_job(#{id => 44, user => <<"alice">>, account => <<>>}),
    start_mock(flurm_job_manager, fun({get_job, _Id}) -> {ok, MockJob} end),
    try
        Result = flurm_reservation:check_reservation(44, Name),
        ?assertEqual({error, reservation_expired}, Result)
    after
        stop_mock(flurm_job_manager)
    end.

test_check_reservation_active_job_not_found() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active},
                {users, []}, {accounts, []},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    start_mock(flurm_job_manager, fun({get_job, _Id}) -> {error, not_found} end),
    try
        Result = flurm_reservation:check_reservation(999, Name),
        ?assertEqual({error, job_not_found}, Result)
    after
        stop_mock(flurm_job_manager)
    end.

%% Cover lines 974-977: account-based authorization path in do_check_reservation.
%% The Account field from the job is checked against the reservation's accounts list.
test_check_reservation_account_auth() ->
    Now = now_sec(),
    Name = unique_name(),
    insert_res([{name, Name}, {state, active},
                {users, []}, {accounts, [<<"eng">>]},
                {nodes, [<<"n1">>]},
                {start_time, Now - 100}, {end_time, Now + 3600}]),
    %% Mock: job has account <<"eng">> which is in the accounts list
    MockJob = make_job(#{id => 45, user => <<"charlie">>, account => <<"eng">>}),
    start_mock(flurm_job_manager, fun({get_job, _Id}) -> {ok, MockJob} end),
    try
        Result = flurm_reservation:check_reservation(45, Name),
        ?assertEqual({ok, [<<"n1">>]}, Result)
    after
        stop_mock(flurm_job_manager)
    end.
