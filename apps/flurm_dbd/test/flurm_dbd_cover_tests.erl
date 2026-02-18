%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd (facade), flurm_dbd_app, and
%%% flurm_dbd_ra_effects.
%%%
%%% flurm_dbd is a pure delegation module -- every function calls
%%% flurm_dbd_server:X(...).  We mock flurm_dbd_server with meck
%%% and verify each delegation works.
%%%
%%% flurm_dbd_app: start/2, stop/1.
%%% flurm_dbd_ra_effects: job_recorded/1, became_leader/1,
%%%                        became_follower/1.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test generators
%%====================================================================

%% ---------- flurm_dbd delegation tests (meck flurm_dbd_server) -----
dbd_delegation_test_() ->
    {foreach,
     fun setup_delegation/0,
     fun cleanup_delegation/1,
     [
      {"record_job_submit delegates",          fun test_record_job_submit/0},
      {"record_job_start delegates",           fun test_record_job_start/0},
      {"record_job_end delegates",             fun test_record_job_end/0},
      {"record_job_cancelled delegates",       fun test_record_job_cancelled/0},
      {"get_job delegates",                    fun test_get_job/0},
      {"get_jobs/0 delegates",                 fun test_get_jobs_0/0},
      {"get_jobs/1 delegates",                 fun test_get_jobs_1/0},
      {"get_jobs_by_user delegates",           fun test_get_jobs_by_user/0},
      {"get_jobs_by_account delegates",        fun test_get_jobs_by_account/0},
      {"get_jobs_in_range delegates",          fun test_get_jobs_in_range/0},
      {"sacct/0 delegates",                    fun test_sacct_0/0},
      {"sacct/1 delegates",                    fun test_sacct_1/0},
      {"format_sacct delegates",               fun test_format_sacct/0},
      {"get_user_usage delegates",             fun test_get_user_usage/0},
      {"get_account_usage delegates",          fun test_get_account_usage/0},
      {"get_cluster_usage delegates",          fun test_get_cluster_usage/0},
      {"reset_usage delegates",                fun test_reset_usage/0},
      {"add_association/3 delegates",          fun test_add_association_3/0},
      {"add_association/4 delegates",          fun test_add_association_4/0},
      {"get_associations delegates",           fun test_get_associations/0},
      {"get_association delegates",            fun test_get_association/0},
      {"remove_association delegates",         fun test_remove_association/0},
      {"update_association delegates",         fun test_update_association/0},
      {"get_user_associations delegates",      fun test_get_user_associations/0},
      {"get_account_associations delegates",   fun test_get_account_associations/0},
      {"archive_old_jobs delegates",           fun test_archive_old_jobs/0},
      {"purge_old_jobs delegates",             fun test_purge_old_jobs/0},
      {"get_archived_jobs/0 delegates",        fun test_get_archived_jobs_0/0},
      {"get_archived_jobs/1 delegates",        fun test_get_archived_jobs_1/0},
      {"get_stats delegates",                  fun test_get_stats/0}
     ]}.

setup_delegation() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [non_strict, no_link]),
    %% Set up stubs for every function that flurm_dbd delegates to.
    %% Casts return ok; calls return identifiable sentinel values.
    meck:expect(flurm_dbd_server, record_job_submit,    fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_start,     fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_end,       fun(_, _, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_cancelled, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, get_job_record,       fun(_) -> {ok, #{}} end),
    meck:expect(flurm_dbd_server, list_job_records,     fun() -> [#{}] end),
    meck:expect(flurm_dbd_server, list_job_records,     fun(_) -> [#{}] end),
    meck:expect(flurm_dbd_server, get_jobs_by_user,     fun(_) -> [#{}] end),
    meck:expect(flurm_dbd_server, get_jobs_by_account,  fun(_) -> [#{}] end),
    meck:expect(flurm_dbd_server, get_jobs_in_range,    fun(_, _) -> [#{}] end),
    meck:expect(flurm_dbd_server, format_sacct_output,  fun(_) -> <<"formatted">> end),
    meck:expect(flurm_dbd_server, get_user_usage,       fun(_) -> #{cpu_seconds => 0} end),
    meck:expect(flurm_dbd_server, get_account_usage,    fun(_) -> #{cpu_seconds => 0} end),
    meck:expect(flurm_dbd_server, get_cluster_usage,    fun() -> #{cpu_seconds => 0} end),
    meck:expect(flurm_dbd_server, reset_usage,          fun() -> ok end),
    meck:expect(flurm_dbd_server, add_association,      fun(_, _, _) -> {ok, 1} end),
    meck:expect(flurm_dbd_server, add_association,      fun(_, _, _, _) -> {ok, 1} end),
    meck:expect(flurm_dbd_server, get_associations,     fun(_) -> [] end),
    meck:expect(flurm_dbd_server, get_association,      fun(_) -> {ok, #{}} end),
    meck:expect(flurm_dbd_server, remove_association,   fun(_) -> ok end),
    meck:expect(flurm_dbd_server, update_association,   fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, get_user_associations,    fun(_) -> [] end),
    meck:expect(flurm_dbd_server, get_account_associations, fun(_) -> [] end),
    meck:expect(flurm_dbd_server, archive_old_jobs,     fun(_) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, purge_old_jobs,       fun(_) -> {ok, 3} end),
    meck:expect(flurm_dbd_server, get_archived_jobs,    fun() -> [] end),
    meck:expect(flurm_dbd_server, get_archived_jobs,    fun(_) -> [] end),
    meck:expect(flurm_dbd_server, get_stats,            fun() -> #{} end),
    ok.

cleanup_delegation(_) ->
    meck:unload(flurm_dbd_server),
    meck:unload(lager),
    ok.

test_record_job_submit() ->
    ?assertEqual(ok, flurm_dbd:record_job_submit(#{job_id => 1})),
    ?assert(meck:called(flurm_dbd_server, record_job_submit, [#{job_id => 1}])).

test_record_job_start() ->
    ?assertEqual(ok, flurm_dbd:record_job_start(1, [<<"n1">>])),
    ?assert(meck:called(flurm_dbd_server, record_job_start, [1, [<<"n1">>]])).

test_record_job_end() ->
    ?assertEqual(ok, flurm_dbd:record_job_end(1, 0, completed)),
    ?assert(meck:called(flurm_dbd_server, record_job_end, [1, 0, completed])).

test_record_job_cancelled() ->
    ?assertEqual(ok, flurm_dbd:record_job_cancelled(1, user_request)),
    ?assert(meck:called(flurm_dbd_server, record_job_cancelled, [1, user_request])).

test_get_job() ->
    ?assertEqual({ok, #{}}, flurm_dbd:get_job(42)),
    ?assert(meck:called(flurm_dbd_server, get_job_record, [42])).

test_get_jobs_0() ->
    ?assertEqual([#{}], flurm_dbd:get_jobs()),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [])).

test_get_jobs_1() ->
    Filters = #{user => <<"alice">>},
    ?assertEqual([#{}], flurm_dbd:get_jobs(Filters)),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [Filters])).

test_get_jobs_by_user() ->
    ?assertEqual([#{}], flurm_dbd:get_jobs_by_user(<<"alice">>)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_by_user, [<<"alice">>])).

test_get_jobs_by_account() ->
    ?assertEqual([#{}], flurm_dbd:get_jobs_by_account(<<"acct">>)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_by_account, [<<"acct">>])).

test_get_jobs_in_range() ->
    ?assertEqual([#{}], flurm_dbd:get_jobs_in_range(100, 200)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_in_range, [100, 200])).

test_sacct_0() ->
    %% sacct/0 calls get_jobs() then format_sacct
    Result = flurm_dbd:sacct(),
    ?assertEqual(<<"formatted">>, Result).

test_sacct_1() ->
    Filters = #{state => running},
    Result = flurm_dbd:sacct(Filters),
    ?assertEqual(<<"formatted">>, Result).

test_format_sacct() ->
    ?assertEqual(<<"formatted">>, flurm_dbd:format_sacct([#{}])),
    ?assert(meck:called(flurm_dbd_server, format_sacct_output, [[#{}]])).

test_get_user_usage() ->
    ?assertMatch(#{cpu_seconds := 0}, flurm_dbd:get_user_usage(<<"alice">>)),
    ?assert(meck:called(flurm_dbd_server, get_user_usage, [<<"alice">>])).

test_get_account_usage() ->
    ?assertMatch(#{cpu_seconds := 0}, flurm_dbd:get_account_usage(<<"acct">>)),
    ?assert(meck:called(flurm_dbd_server, get_account_usage, [<<"acct">>])).

test_get_cluster_usage() ->
    ?assertMatch(#{cpu_seconds := 0}, flurm_dbd:get_cluster_usage()),
    ?assert(meck:called(flurm_dbd_server, get_cluster_usage, [])).

test_reset_usage() ->
    ?assertEqual(ok, flurm_dbd:reset_usage()),
    ?assert(meck:called(flurm_dbd_server, reset_usage, [])).

test_add_association_3() ->
    ?assertEqual({ok, 1}, flurm_dbd:add_association(<<"c">>, <<"a">>, <<"u">>)),
    ?assert(meck:called(flurm_dbd_server, add_association, [<<"c">>, <<"a">>, <<"u">>])).

test_add_association_4() ->
    Opts = #{shares => 10},
    ?assertEqual({ok, 1}, flurm_dbd:add_association(<<"c">>, <<"a">>, <<"u">>, Opts)),
    ?assert(meck:called(flurm_dbd_server, add_association,
                        [<<"c">>, <<"a">>, <<"u">>, Opts])).

test_get_associations() ->
    ?assertEqual([], flurm_dbd:get_associations(<<"c">>)),
    ?assert(meck:called(flurm_dbd_server, get_associations, [<<"c">>])).

test_get_association() ->
    ?assertEqual({ok, #{}}, flurm_dbd:get_association(1)),
    ?assert(meck:called(flurm_dbd_server, get_association, [1])).

test_remove_association() ->
    ?assertEqual(ok, flurm_dbd:remove_association(1)),
    ?assert(meck:called(flurm_dbd_server, remove_association, [1])).

test_update_association() ->
    Updates = #{shares => 5},
    ?assertEqual(ok, flurm_dbd:update_association(1, Updates)),
    ?assert(meck:called(flurm_dbd_server, update_association, [1, Updates])).

test_get_user_associations() ->
    ?assertEqual([], flurm_dbd:get_user_associations(<<"u">>)),
    ?assert(meck:called(flurm_dbd_server, get_user_associations, [<<"u">>])).

test_get_account_associations() ->
    ?assertEqual([], flurm_dbd:get_account_associations(<<"a">>)),
    ?assert(meck:called(flurm_dbd_server, get_account_associations, [<<"a">>])).

test_archive_old_jobs() ->
    ?assertEqual({ok, 5}, flurm_dbd:archive_old_jobs(30)),
    ?assert(meck:called(flurm_dbd_server, archive_old_jobs, [30])).

test_purge_old_jobs() ->
    ?assertEqual({ok, 3}, flurm_dbd:purge_old_jobs(90)),
    ?assert(meck:called(flurm_dbd_server, purge_old_jobs, [90])).

test_get_archived_jobs_0() ->
    ?assertEqual([], flurm_dbd:get_archived_jobs()),
    ?assert(meck:called(flurm_dbd_server, get_archived_jobs, [])).

test_get_archived_jobs_1() ->
    F = #{state => completed},
    ?assertEqual([], flurm_dbd:get_archived_jobs(F)),
    ?assert(meck:called(flurm_dbd_server, get_archived_jobs, [F])).

test_get_stats() ->
    ?assertEqual(#{}, flurm_dbd:get_stats()),
    ?assert(meck:called(flurm_dbd_server, get_stats, [])).

%% ---------- flurm_dbd_app tests -----------------------------------
dbd_app_test_() ->
    {foreach,
     fun setup_app/0,
     fun cleanup_app/1,
     [
      {"start/2 returns {ok, Pid}",  fun test_app_start/0},
      {"stop/1 returns ok",          fun test_app_stop/0}
     ]}.

setup_app() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_sup),
    meck:new(flurm_dbd_sup, [non_strict, no_link]),
    meck:expect(flurm_dbd_sup, start_link, fun() -> {ok, self()} end),
    ok.

cleanup_app(_) ->
    meck:unload(flurm_dbd_sup),
    meck:unload(lager),
    ok.

test_app_start() ->
    ?assertMatch({ok, _Pid}, flurm_dbd_app:start(normal, [])).

test_app_stop() ->
    ?assertEqual(ok, flurm_dbd_app:stop(unused_state)).

%% ---------- flurm_dbd_ra_effects tests ----------------------------
ra_effects_test_() ->
    {foreach,
     fun setup_ra/0,
     fun cleanup_ra/1,
     [
      {"job_recorded returns ok",    fun test_job_recorded/0},
      {"became_leader returns ok",   fun test_became_leader/0},
      {"became_follower returns ok", fun test_became_follower/0}
     ]}.

setup_ra() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,  fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    ok.

cleanup_ra(_) ->
    meck:unload(lager),
    ok.

test_job_recorded() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(#{job_id => 1})).

test_became_leader() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_leader(node())).

test_became_follower() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_follower(node())).
