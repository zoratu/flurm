%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_qos internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_qos_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

-define(QOS_TABLE, flurm_qos).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Create ETS table if it doesn't exist
    case ets:whereis(?QOS_TABLE) of
        undefined ->
            ets:new(?QOS_TABLE, [
                named_table, public, set,
                {keypos, 2}  % qos.name position
            ]);
        _ -> ok
    end,
    ok.

cleanup(_) ->
    case ets:whereis(?QOS_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?QOS_TABLE)
    end,
    ok.

qos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun do_create_basic/0,
         fun do_create_already_exists/0,
         fun do_create_with_options/0,
         fun check_tres_map_empty_limits/0,
         fun check_tres_map_no_violations/0,
         fun check_tres_map_violations/0,
         fun combine_tres_empty/0,
         fun combine_tres_no_overlap/0,
         fun combine_tres_overlap/0
     ]}.

%%====================================================================
%% Do Create Tests
%%====================================================================

do_create_basic() ->
    Result = flurm_qos:do_create(<<"test_qos">>, #{}),
    ?assertEqual(ok, Result),
    %% Verify it was created
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"test_qos">>)).

do_create_already_exists() ->
    flurm_qos:do_create(<<"dup_qos">>, #{}),
    Result = flurm_qos:do_create(<<"dup_qos">>, #{}),
    ?assertEqual({error, already_exists}, Result).

do_create_with_options() ->
    Options = #{
        description => <<"High priority QOS">>,
        priority => 1000,
        max_wall_per_job => 86400,
        max_jobs_pu => 10,
        preempt => [<<"low">>, <<"standby">>],
        preempt_mode => requeue,
        usage_factor => 2.0
    },
    flurm_qos:do_create(<<"custom_qos">>, Options),
    %% qos record: {qos, name, desc, priority, flags, grace_time, max_jobs_pa, max_jobs_pu,
    %%              max_submit_jobs_pa, max_submit_jobs_pu, max_tres_pa, max_tres_pu,
    %%              max_tres_per_job, max_tres_per_node, max_tres_per_user, max_wall_per_job,
    %%              min_tres_per_job, preempt, preempt_mode, usage_factor, usage_threshold}
    [{qos, Name, Desc, Priority, _, _, _, MaxJobsPU, _, _, _, _, _, _, _, MaxWall,
      _, Preempt, PreemptMode, UsageFactor, _}] = ets:lookup(?QOS_TABLE, <<"custom_qos">>),
    ?assertEqual(<<"custom_qos">>, Name),
    ?assertEqual(<<"High priority QOS">>, Desc),
    ?assertEqual(1000, Priority),
    ?assertEqual(86400, MaxWall),
    ?assertEqual(10, MaxJobsPU),
    ?assertEqual([<<"low">>, <<"standby">>], Preempt),
    ?assertEqual(requeue, PreemptMode),
    ?assertEqual(2.0, UsageFactor).

do_create_defaults_test() ->
    flurm_qos:do_create(<<"default_test">>, #{}),
    [{qos, _, _, Priority, Flags, GraceTime, _, _, _, _, _, _, _, _, _, MaxWall,
      _, Preempt, PreemptMode, UsageFactor, _}] = ets:lookup(?QOS_TABLE, <<"default_test">>),
    ?assertEqual(0, Priority),
    ?assertEqual([], Flags),
    ?assertEqual(0, GraceTime),
    ?assertEqual(0, MaxWall),
    ?assertEqual([], Preempt),
    ?assertEqual(off, PreemptMode),
    ?assertEqual(1.0, UsageFactor).

%%====================================================================
%% Do Update Tests
%%====================================================================

do_update_not_found_test() ->
    Result = flurm_qos:do_update(<<"nonexistent">>, #{priority => 100}),
    ?assertEqual({error, not_found}, Result).

do_update_priority_test() ->
    flurm_qos:do_create(<<"update_test">>, #{priority => 100}),
    ok = flurm_qos:do_update(<<"update_test">>, #{priority => 500}),
    [{qos, _, _, Priority, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _}] =
        ets:lookup(?QOS_TABLE, <<"update_test">>),
    ?assertEqual(500, Priority).

do_update_multiple_fields_test() ->
    flurm_qos:do_create(<<"multi_update">>, #{}),
    ok = flurm_qos:do_update(<<"multi_update">>, #{
        priority => 200,
        max_jobs_pu => 50,
        usage_factor => 1.5
    }),
    [{qos, _, _, Priority, _, _, _, MaxJobsPU, _, _, _, _, _, _, _, _, _, _, _, UsageFactor, _}] =
        ets:lookup(?QOS_TABLE, <<"multi_update">>),
    ?assertEqual(200, Priority),
    ?assertEqual(50, MaxJobsPU),
    ?assertEqual(1.5, UsageFactor).

%%====================================================================
%% Apply Updates Tests
%%====================================================================

apply_updates_description_test() ->
    QOS = {qos, <<"test">>, <<>>, 0, [], 0, 0, 0, 0, 0, #{}, #{}, #{}, #{}, #{}, 0, #{}, [], off, 1.0, 0.0},
    Updated = flurm_qos:apply_updates(QOS, #{description => <<"New description">>}),
    ?assertEqual(<<"New description">>, element(3, Updated)).

apply_updates_priority_test() ->
    QOS = {qos, <<"test">>, <<>>, 0, [], 0, 0, 0, 0, 0, #{}, #{}, #{}, #{}, #{}, 0, #{}, [], off, 1.0, 0.0},
    Updated = flurm_qos:apply_updates(QOS, #{priority => 500}),
    ?assertEqual(500, element(4, Updated)).

apply_updates_preempt_test() ->
    QOS = {qos, <<"test">>, <<>>, 0, [], 0, 0, 0, 0, 0, #{}, #{}, #{}, #{}, #{}, 0, #{}, [], off, 1.0, 0.0},
    Updated = flurm_qos:apply_updates(QOS, #{preempt => [<<"low">>], preempt_mode => cancel}),
    %% preempt is at position 18, preempt_mode at position 19
    ?assertEqual([<<"low">>], element(18, Updated)),
    ?assertEqual(cancel, element(19, Updated)).

apply_updates_unknown_key_test() ->
    QOS = {qos, <<"test">>, <<>>, 0, [], 0, 0, 0, 0, 0, #{}, #{}, #{}, #{}, #{}, 0, #{}, [], off, 1.0, 0.0},
    %% Unknown keys should be ignored
    Updated = flurm_qos:apply_updates(QOS, #{unknown_key => value}),
    ?assertEqual(QOS, Updated).

%%====================================================================
%% Check TRES Map Tests
%%====================================================================

check_tres_map_empty_limits() ->
    %% Empty limits means no restrictions
    Request = #{cpu => 100, mem => 1024, gpu => 4},
    ?assertEqual(ok, flurm_qos:check_tres_map(Request, #{})).

check_tres_map_no_violations() ->
    Request = #{cpu => 10, mem => 512},
    Limits = #{cpu => 100, mem => 1024},
    ?assertEqual(ok, flurm_qos:check_tres_map(Request, Limits)).

check_tres_map_violations() ->
    Request = #{cpu => 150, mem => 2048},
    Limits = #{cpu => 100, mem => 1024},
    {error, {tres_limit_exceeded, Violations}} = flurm_qos:check_tres_map(Request, Limits),
    %% Both cpu and mem exceeded
    ?assertEqual(2, length(Violations)).

check_tres_map_partial_violation_test() ->
    Request = #{cpu => 50, mem => 2048},
    Limits = #{cpu => 100, mem => 1024},
    {error, {tres_limit_exceeded, Violations}} = flurm_qos:check_tres_map(Request, Limits),
    %% Only mem exceeded
    ?assertEqual(1, length(Violations)),
    ?assertMatch([{mem, 2048, 1024}], Violations).

check_tres_map_zero_limit_test() ->
    %% Zero limit means unlimited
    Request = #{cpu => 1000},
    Limits = #{cpu => 0},
    ?assertEqual(ok, flurm_qos:check_tres_map(Request, Limits)).

check_tres_map_unlisted_tres_test() ->
    %% TRES not in limits should be allowed
    Request = #{gpu => 8},
    Limits = #{cpu => 100},
    ?assertEqual(ok, flurm_qos:check_tres_map(Request, Limits)).

%%====================================================================
%% Combine TRES Tests
%%====================================================================

combine_tres_empty() ->
    ?assertEqual(#{}, flurm_qos:combine_tres(#{}, #{})).

combine_tres_no_overlap() ->
    Map1 = #{cpu => 4},
    Map2 = #{mem => 1024},
    Result = flurm_qos:combine_tres(Map1, Map2),
    ?assertEqual(#{cpu => 4, mem => 1024}, Result).

combine_tres_overlap() ->
    Map1 = #{cpu => 4, mem => 512},
    Map2 = #{cpu => 2, mem => 512},
    Result = flurm_qos:combine_tres(Map1, Map2),
    ?assertEqual(#{cpu => 6, mem => 1024}, Result).

combine_tres_first_empty_test() ->
    Map2 = #{cpu => 4, mem => 1024},
    Result = flurm_qos:combine_tres(#{}, Map2),
    ?assertEqual(Map2, Result).

combine_tres_second_empty_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    Result = flurm_qos:combine_tres(Map1, #{}),
    ?assertEqual(Map1, Result).

%%====================================================================
%% Create Default QOS Entries Tests
%%====================================================================

create_default_qos_entries_test() ->
    flurm_qos:create_default_qos_entries(),

    %% Verify all default QOS entries were created
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"normal">>)),
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"high">>)),
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"low">>)),
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"interactive">>)),
    ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"standby">>)).

create_default_qos_priorities_test() ->
    flurm_qos:create_default_qos_entries(),

    [{qos, _, _, NormalPriority, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _}] =
        ets:lookup(?QOS_TABLE, <<"normal">>),
    [{qos, _, _, HighPriority, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _}] =
        ets:lookup(?QOS_TABLE, <<"high">>),
    [{qos, _, _, LowPriority, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _}] =
        ets:lookup(?QOS_TABLE, <<"low">>),
    [{qos, _, _, StandbyPriority, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _}] =
        ets:lookup(?QOS_TABLE, <<"standby">>),

    ?assertEqual(0, NormalPriority),
    ?assertEqual(1000, HighPriority),
    ?assertEqual(-500, LowPriority),
    ?assertEqual(-1000, StandbyPriority).

create_default_qos_preemption_test() ->
    flurm_qos:create_default_qos_entries(),

    [{qos, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, Preempt, PreemptMode, _, _}] =
        ets:lookup(?QOS_TABLE, <<"high">>),

    ?assertEqual([<<"low">>, <<"standby">>], Preempt),
    ?assertEqual(requeue, PreemptMode).

%%====================================================================
%% Get User Job Counts Tests
%%====================================================================

get_user_job_counts_no_registry_test() ->
    %% When registry is not available, should return {0, 0}
    {Running, Pending} = flurm_qos:get_user_job_counts(<<"testuser">>),
    ?assertEqual(0, Running),
    ?assertEqual(0, Pending).

%%====================================================================
%% Get User TRES Usage Tests
%%====================================================================

get_user_tres_usage_no_registry_test() ->
    %% When registry is not available, should return empty map
    TRES = flurm_qos:get_user_tres_usage(<<"testuser">>),
    ?assertEqual(#{}, TRES).
