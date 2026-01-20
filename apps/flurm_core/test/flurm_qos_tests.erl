%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_qos module (Quality of Service)
%%%
%%% Tests cover:
%%% - QOS creation and deletion
%%% - QOS retrieval and listing
%%% - QOS updates
%%% - Limit checking
%%% - Priority adjustment
%%% - Preemption rules
%%% - Usage factors
%%% - Default QOS handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_qos_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up any existing state first
    catch ets:delete(flurm_qos),
    case whereis(flurm_qos) of
        undefined ->
            ok;
        ExistingPid ->
            Ref = monitor(process, ExistingPid),
            catch gen_server:stop(ExistingPid, shutdown, 5000),
            receive {'DOWN', Ref, process, ExistingPid, _} -> ok after 5000 -> ok end
    end,
    %% Start the QOS server
    {ok, Pid} = flurm_qos:start_link(),
    %% Unlink to prevent EUnit process from receiving EXIT signals
    unlink(Pid),
    {started, Pid}.

cleanup({started, Pid}) ->
    %% Stop the server first, wait for it to terminate
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end;
        false ->
            ok
    end,
    %% Clean up ETS table
    catch ets:delete(flurm_qos),
    ok;
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

qos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Default QOS tests
      {"default QOS entries exist", fun test_default_qos_entries/0},
      {"get default QOS", fun test_get_default/0},
      {"set default QOS", fun test_set_default/0},
      {"init default QOS", fun test_init_default_qos/0},

      %% CRUD operations
      {"create QOS with defaults", fun test_create_qos_defaults/0},
      {"create QOS with options", fun test_create_qos_options/0},
      {"create QOS already exists", fun test_create_qos_exists/0},
      {"get QOS - found", fun test_get_qos_found/0},
      {"get QOS - not found", fun test_get_qos_not_found/0},
      {"list all QOS", fun test_list_qos/0},
      {"update QOS", fun test_update_qos/0},
      {"update QOS - not found", fun test_update_qos_not_found/0},
      {"delete QOS", fun test_delete_qos/0},
      {"delete default QOS fails", fun test_delete_default_qos/0},

      %% Limit checking
      {"check limits - ok", fun test_check_limits_ok/0},
      {"check limits - wall time exceeded", fun test_check_limits_wall_exceeded/0},
      {"check limits - invalid QOS", fun test_check_limits_invalid_qos/0},
      {"check TRES limits - ok", fun test_check_tres_limits_ok/0},
      {"check TRES limits - per job exceeded", fun test_check_tres_per_job_exceeded/0},
      {"check TRES limits - invalid QOS", fun test_check_tres_invalid_qos/0},

      %% Priority
      {"get priority adjustment", fun test_get_priority_adjustment/0},
      {"get priority adjustment - unknown QOS", fun test_get_priority_unknown/0},

      %% Preemption
      {"get preemptable QOS", fun test_get_preemptable_qos/0},
      {"can preempt - true", fun test_can_preempt_true/0},
      {"can preempt - false", fun test_can_preempt_false/0},
      {"can preempt - unknown QOS", fun test_can_preempt_unknown/0},

      %% Usage factor
      {"apply usage factor", fun test_apply_usage_factor/0},
      {"apply usage factor - unknown QOS", fun test_apply_usage_factor_unknown/0}
     ]}.

%%====================================================================
%% Default QOS Tests
%%====================================================================

test_default_qos_entries() ->
    %% Verify standard QOS levels exist
    QOSList = flurm_qos:list(),
    Names = [QOS#qos.name || QOS <- QOSList],

    ?assert(lists:member(<<"normal">>, Names)),
    ?assert(lists:member(<<"high">>, Names)),
    ?assert(lists:member(<<"low">>, Names)),
    ?assert(lists:member(<<"interactive">>, Names)),
    ?assert(lists:member(<<"standby">>, Names)).

test_get_default() ->
    Default = flurm_qos:get_default(),
    ?assertEqual(<<"normal">>, Default).

test_set_default() ->
    %% Set to high
    ok = flurm_qos:set_default(<<"high">>),
    ?assertEqual(<<"high">>, flurm_qos:get_default()),

    %% Set to non-existent should fail
    Result = flurm_qos:set_default(<<"nonexistent">>),
    ?assertEqual({error, qos_not_found}, Result),

    %% Default should still be high
    ?assertEqual(<<"high">>, flurm_qos:get_default()).

test_init_default_qos() ->
    %% This reinitializes defaults (should be idempotent)
    ok = flurm_qos:init_default_qos(),

    %% Standard QOS should still exist
    {ok, Normal} = flurm_qos:get(<<"normal">>),
    ?assertEqual(<<"normal">>, Normal#qos.name).

%%====================================================================
%% CRUD Tests
%%====================================================================

test_create_qos_defaults() ->
    %% Create with just a name
    ok = flurm_qos:create(<<"custom">>),

    {ok, QOS} = flurm_qos:get(<<"custom">>),
    ?assertEqual(<<"custom">>, QOS#qos.name),
    ?assertEqual(<<>>, QOS#qos.description),
    ?assertEqual(0, QOS#qos.priority),
    ?assertEqual(1.0, QOS#qos.usage_factor).

test_create_qos_options() ->
    Options = #{
        description => <<"High memory QOS">>,
        priority => 500,
        max_jobs_pu => 10,
        max_submit_jobs_pu => 20,
        max_wall_per_job => 86400,
        max_tres_per_job => #{cpu => 100, mem => 1048576},
        preempt => [<<"low">>],
        preempt_mode => requeue,
        usage_factor => 2.0,
        grace_time => 60
    },

    ok = flurm_qos:create(<<"highmem">>, Options),

    {ok, QOS} = flurm_qos:get(<<"highmem">>),
    ?assertEqual(<<"highmem">>, QOS#qos.name),
    ?assertEqual(<<"High memory QOS">>, QOS#qos.description),
    ?assertEqual(500, QOS#qos.priority),
    ?assertEqual(10, QOS#qos.max_jobs_pu),
    ?assertEqual(20, QOS#qos.max_submit_jobs_pu),
    ?assertEqual(86400, QOS#qos.max_wall_per_job),
    ?assertEqual([<<"low">>], QOS#qos.preempt),
    ?assertEqual(requeue, QOS#qos.preempt_mode),
    ?assertEqual(2.0, QOS#qos.usage_factor),
    ?assertEqual(60, QOS#qos.grace_time).

test_create_qos_exists() ->
    %% normal already exists
    Result = flurm_qos:create(<<"normal">>),
    ?assertEqual({error, already_exists}, Result).

test_get_qos_found() ->
    {ok, QOS} = flurm_qos:get(<<"normal">>),
    ?assertEqual(<<"normal">>, QOS#qos.name),
    ?assert(is_binary(QOS#qos.description)).

test_get_qos_not_found() ->
    Result = flurm_qos:get(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_qos() ->
    QOSList = flurm_qos:list(),
    ?assert(is_list(QOSList)),
    ?assert(length(QOSList) >= 5),  % At least the 5 defaults

    %% All should be #qos{} records
    lists:foreach(fun(QOS) ->
        ?assert(is_record(QOS, qos))
    end, QOSList).

test_update_qos() ->
    %% Create a QOS to update
    ok = flurm_qos:create(<<"toupdate">>, #{priority => 100}),

    %% Update multiple fields
    Updates = #{
        priority => 200,
        description => <<"Updated description">>,
        max_jobs_pu => 50,
        usage_factor => 0.5
    },
    ok = flurm_qos:update(<<"toupdate">>, Updates),

    %% Verify updates
    {ok, QOS} = flurm_qos:get(<<"toupdate">>),
    ?assertEqual(200, QOS#qos.priority),
    ?assertEqual(<<"Updated description">>, QOS#qos.description),
    ?assertEqual(50, QOS#qos.max_jobs_pu),
    ?assertEqual(0.5, QOS#qos.usage_factor).

test_update_qos_not_found() ->
    Result = flurm_qos:update(<<"nonexistent">>, #{priority => 100}),
    ?assertEqual({error, not_found}, Result).

test_delete_qos() ->
    %% Create then delete
    ok = flurm_qos:create(<<"todelete">>),
    {ok, _} = flurm_qos:get(<<"todelete">>),

    ok = flurm_qos:delete(<<"todelete">>),
    ?assertEqual({error, not_found}, flurm_qos:get(<<"todelete">>)).

test_delete_default_qos() ->
    %% Cannot delete the normal QOS
    Result = flurm_qos:delete(<<"normal">>),
    ?assertEqual({error, cannot_delete_default}, Result),

    %% Should still exist
    {ok, _} = flurm_qos:get(<<"normal">>).

%%====================================================================
%% Limit Checking Tests
%%====================================================================

test_check_limits_ok() ->
    %% Normal QOS has 24-hour wall limit
    JobSpec = #{
        user => <<"testuser">>,
        time_limit => 3600  % 1 hour
    },

    Result = flurm_qos:check_limits(<<"normal">>, JobSpec),
    ?assertEqual(ok, Result).

test_check_limits_wall_exceeded() ->
    %% Interactive has 1-hour limit
    {ok, Interactive} = flurm_qos:get(<<"interactive">>),
    MaxWall = Interactive#qos.max_wall_per_job,

    JobSpec = #{
        user => <<"testuser">>,
        time_limit => MaxWall + 1000  % Exceed the limit
    },

    Result = flurm_qos:check_limits(<<"interactive">>, JobSpec),
    ?assertMatch({error, {exceeds_wall_limit, _, _}}, Result).

test_check_limits_invalid_qos() ->
    JobSpec = #{user => <<"testuser">>, time_limit => 100},
    Result = flurm_qos:check_limits(<<"nonexistent">>, JobSpec),
    ?assertEqual({error, {invalid_qos, <<"nonexistent">>}}, Result).

test_check_tres_limits_ok() ->
    %% Create QOS with TRES limits
    ok = flurm_qos:create(<<"treslimited">>, #{
        max_tres_per_job => #{cpu => 100, gpu => 4}
    }),

    TRESRequest = #{cpu => 50, gpu => 2},
    Result = flurm_qos:check_tres_limits(<<"treslimited">>, <<"user">>, TRESRequest),
    ?assertEqual(ok, Result).

test_check_tres_per_job_exceeded() ->
    ok = flurm_qos:create(<<"stricttres">>, #{
        max_tres_per_job => #{cpu => 10, gpu => 1}
    }),

    TRESRequest = #{cpu => 20, gpu => 1},  % CPU exceeds
    Result = flurm_qos:check_tres_limits(<<"stricttres">>, <<"user">>, TRESRequest),
    ?assertMatch({error, {tres_limit_exceeded, _}}, Result).

test_check_tres_invalid_qos() ->
    Result = flurm_qos:check_tres_limits(<<"nonexistent">>, <<"user">>, #{cpu => 1}),
    ?assertEqual({error, {invalid_qos, <<"nonexistent">>}}, Result).

%%====================================================================
%% Priority Tests
%%====================================================================

test_get_priority_adjustment() ->
    %% High has priority 1000
    Priority = flurm_qos:get_priority_adjustment(<<"high">>),
    ?assertEqual(1000, Priority),

    %% Low has priority -500
    LowPriority = flurm_qos:get_priority_adjustment(<<"low">>),
    ?assertEqual(-500, LowPriority),

    %% Standby has priority -1000
    StandbyPriority = flurm_qos:get_priority_adjustment(<<"standby">>),
    ?assertEqual(-1000, StandbyPriority),

    %% Interactive has priority 500
    InteractivePriority = flurm_qos:get_priority_adjustment(<<"interactive">>),
    ?assertEqual(500, InteractivePriority).

test_get_priority_unknown() ->
    Priority = flurm_qos:get_priority_adjustment(<<"nonexistent">>),
    ?assertEqual(0, Priority).

%%====================================================================
%% Preemption Tests
%%====================================================================

test_get_preemptable_qos() ->
    %% High can preempt low and standby
    Preemptable = flurm_qos:get_preemptable_qos(<<"high">>),
    ?assert(lists:member(<<"low">>, Preemptable)),
    ?assert(lists:member(<<"standby">>, Preemptable)),

    %% Normal has empty preempt list
    NormalPreemptable = flurm_qos:get_preemptable_qos(<<"normal">>),
    ?assertEqual([], NormalPreemptable).

test_can_preempt_true() ->
    %% High can preempt low
    ?assertEqual(true, flurm_qos:can_preempt(<<"high">>, <<"low">>)),
    %% High can preempt standby
    ?assertEqual(true, flurm_qos:can_preempt(<<"high">>, <<"standby">>)).

test_can_preempt_false() ->
    %% High cannot preempt normal
    ?assertEqual(false, flurm_qos:can_preempt(<<"high">>, <<"normal">>)),
    %% Low cannot preempt anything
    ?assertEqual(false, flurm_qos:can_preempt(<<"low">>, <<"normal">>)),
    %% Normal cannot preempt low
    ?assertEqual(false, flurm_qos:can_preempt(<<"normal">>, <<"low">>)).

test_can_preempt_unknown() ->
    %% Unknown QOS cannot preempt anything
    ?assertEqual(false, flurm_qos:can_preempt(<<"nonexistent">>, <<"low">>)).

%%====================================================================
%% Usage Factor Tests
%%====================================================================

test_apply_usage_factor() ->
    %% Normal has factor 1.0
    Result1 = flurm_qos:apply_usage_factor(<<"normal">>, 100.0),
    ?assertEqual(100.0, Result1),

    %% Low has factor 0.5
    Result2 = flurm_qos:apply_usage_factor(<<"low">>, 100.0),
    ?assertEqual(50.0, Result2),

    %% Standby has factor 0.0
    Result3 = flurm_qos:apply_usage_factor(<<"standby">>, 100.0),
    ?assertEqual(0.0, Result3),

    %% Create custom with factor 2.0
    ok = flurm_qos:create(<<"double">>, #{usage_factor => 2.0}),
    Result4 = flurm_qos:apply_usage_factor(<<"double">>, 50.0),
    ?assertEqual(100.0, Result4).

test_apply_usage_factor_unknown() ->
    %% Unknown QOS returns usage unchanged
    Result = flurm_qos:apply_usage_factor(<<"nonexistent">>, 75.0),
    ?assertEqual(75.0, Result).

%%====================================================================
%% QOS Field Update Tests
%%====================================================================

update_all_fields_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"update all QOS fields", fun test_update_all_fields/0}
     ]}.

test_update_all_fields() ->
    %% Create a QOS
    ok = flurm_qos:create(<<"allfields">>),

    %% Update every possible field
    Updates = #{
        description => <<"Full update">>,
        priority => 999,
        flags => [some_flag],
        grace_time => 120,
        max_jobs_pa => 100,
        max_jobs_pu => 50,
        max_submit_jobs_pa => 200,
        max_submit_jobs_pu => 100,
        max_tres_pa => #{cpu => 1000},
        max_tres_pu => #{cpu => 500},
        max_tres_per_job => #{cpu => 64},
        max_tres_per_node => #{cpu => 32},
        max_tres_per_user => #{cpu => 256},
        max_wall_per_job => 172800,
        min_tres_per_job => #{cpu => 1},
        preempt => [<<"low">>, <<"standby">>],
        preempt_mode => suspend,
        usage_factor => 1.5,
        usage_threshold => 0.8
    },

    ok = flurm_qos:update(<<"allfields">>, Updates),

    {ok, QOS} = flurm_qos:get(<<"allfields">>),

    ?assertEqual(<<"Full update">>, QOS#qos.description),
    ?assertEqual(999, QOS#qos.priority),
    ?assertEqual([some_flag], QOS#qos.flags),
    ?assertEqual(120, QOS#qos.grace_time),
    ?assertEqual(100, QOS#qos.max_jobs_pa),
    ?assertEqual(50, QOS#qos.max_jobs_pu),
    ?assertEqual(200, QOS#qos.max_submit_jobs_pa),
    ?assertEqual(100, QOS#qos.max_submit_jobs_pu),
    ?assertEqual(#{cpu => 1000}, QOS#qos.max_tres_pa),
    ?assertEqual(#{cpu => 500}, QOS#qos.max_tres_pu),
    ?assertEqual(#{cpu => 64}, QOS#qos.max_tres_per_job),
    ?assertEqual(#{cpu => 32}, QOS#qos.max_tres_per_node),
    ?assertEqual(#{cpu => 256}, QOS#qos.max_tres_per_user),
    ?assertEqual(172800, QOS#qos.max_wall_per_job),
    ?assertEqual(#{cpu => 1}, QOS#qos.min_tres_per_job),
    ?assertEqual([<<"low">>, <<"standby">>], QOS#qos.preempt),
    ?assertEqual(suspend, QOS#qos.preempt_mode),
    ?assertEqual(1.5, QOS#qos.usage_factor),
    ?assertEqual(0.8, QOS#qos.usage_threshold).

%%====================================================================
%% Preempt Mode Tests
%%====================================================================

preempt_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"verify default preempt modes", fun test_default_preempt_modes/0}
     ]}.

test_default_preempt_modes() ->
    %% High uses requeue
    {ok, High} = flurm_qos:get(<<"high">>),
    ?assertEqual(requeue, High#qos.preempt_mode),

    %% Standby uses cancel
    {ok, Standby} = flurm_qos:get(<<"standby">>),
    ?assertEqual(cancel, Standby#qos.preempt_mode),

    %% Normal uses off
    {ok, Normal} = flurm_qos:get(<<"normal">>),
    ?assertEqual(off, Normal#qos.preempt_mode).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast is ignored", fun test_unknown_cast/0},
      {"unknown info is ignored", fun test_unknown_info/0},
      {"code_change succeeds", fun test_code_change/0},
      {"terminate succeeds", fun test_terminate/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_qos, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    %% Unknown cast should not crash the server
    ok = gen_server:cast(flurm_qos, {unknown_cast_message}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_qos))),
    %% Should still work
    _ = flurm_qos:list().

test_unknown_info() ->
    %% Unknown info message should not crash the server
    flurm_qos ! {unknown_info_message, foo, bar},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_qos))),
    %% Should still work
    _ = flurm_qos:list().

test_code_change() ->
    Pid = whereis(flurm_qos),
    sys:suspend(Pid),
    Result = sys:change_code(Pid, flurm_qos, "1.0.0", []),
    ?assertEqual(ok, Result),
    sys:resume(Pid),
    %% Should still work
    _ = flurm_qos:list().

test_terminate() ->
    %% Start a fresh QOS server for terminate test
    catch gen_server:stop(flurm_qos),
    catch ets:delete(flurm_qos),
    timer:sleep(50),
    {ok, Pid} = flurm_qos:start_link(),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)).

%%====================================================================
%% TRES Limit Edge Case Tests
%%====================================================================

tres_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"check TRES with empty limits", fun test_check_tres_empty_limits/0},
      {"check TRES with per-user limits", fun test_check_tres_per_user/0}
     ]}.

test_check_tres_empty_limits() ->
    %% Create QOS with no TRES limits
    ok = flurm_qos:create(<<"nolimits">>, #{}),

    %% Any TRES request should pass
    TRESRequest = #{cpu => 1000, gpu => 100, mem => 999999999},
    Result = flurm_qos:check_tres_limits(<<"nolimits">>, <<"user">>, TRESRequest),
    ?assertEqual(ok, Result).

test_check_tres_per_user() ->
    %% Create QOS with per-user TRES limits
    ok = flurm_qos:create(<<"peruser">>, #{
        max_tres_pu => #{cpu => 100}
    }),

    %% Small request should pass (assuming no prior usage)
    TRESRequest = #{cpu => 50},
    Result = flurm_qos:check_tres_limits(<<"peruser">>, <<"testuser">>, TRESRequest),
    ?assertEqual(ok, Result).

%%====================================================================
%% Job Count Limit Tests
%%====================================================================

job_count_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"check limits with job count limits", fun test_check_job_count_limits/0}
     ]}.

test_check_job_count_limits() ->
    %% Normal QOS has no job limits by default (0 = unlimited)
    JobSpec = #{
        user => <<"testuser">>,
        time_limit => 100
    },
    Result = flurm_qos:check_limits(<<"normal">>, JobSpec),
    ?assertEqual(ok, Result).

%%====================================================================
%% Update with Unknown Key Tests
%%====================================================================

update_unknown_key_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"update with unknown key is ignored", fun test_update_unknown_key/0}
     ]}.

test_update_unknown_key() ->
    ok = flurm_qos:create(<<"forunkown">>, #{priority => 100}),

    %% Update with unknown key - should be ignored
    ok = flurm_qos:update(<<"forunkown">>, #{
        priority => 200,
        unknown_key => <<"should_be_ignored">>,
        another_unknown => 12345
    }),

    {ok, QOS} = flurm_qos:get(<<"forunkown">>),
    ?assertEqual(200, QOS#qos.priority).
