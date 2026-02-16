%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd module.
%%%
%%% Tests the high-level API module which delegates to flurm_dbd_server.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

dbd_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Job recording
      {"record_job_submit", fun test_record_job_submit/0},
      {"record_job_start", fun test_record_job_start/0},
      {"record_job_end", fun test_record_job_end/0},
      {"record_job_cancelled", fun test_record_job_cancelled/0},

      %% Job queries
      {"get_job found", fun test_get_job_found/0},
      {"get_job not found", fun test_get_job_not_found/0},
      {"get_jobs all", fun test_get_jobs_all/0},
      {"get_jobs with filters", fun test_get_jobs_filters/0},
      {"get_jobs_by_user", fun test_get_jobs_by_user/0},
      {"get_jobs_by_account", fun test_get_jobs_by_account/0},
      {"get_jobs_in_range", fun test_get_jobs_in_range/0},

      %% sacct output
      {"sacct all", fun test_sacct_all/0},
      {"sacct filtered", fun test_sacct_filtered/0},
      {"format_sacct", fun test_format_sacct/0},

      %% TRES tracking
      {"get_user_usage", fun test_get_user_usage/0},
      {"get_account_usage", fun test_get_account_usage/0},
      {"get_cluster_usage", fun test_get_cluster_usage/0},
      {"reset_usage", fun test_reset_usage/0},

      %% Association management
      {"add_association 3 args", fun test_add_association_3/0},
      {"add_association 4 args", fun test_add_association_4/0},
      {"get_associations", fun test_get_associations/0},
      {"get_association", fun test_get_association/0},
      {"remove_association", fun test_remove_association/0},
      {"update_association", fun test_update_association/0},
      {"get_user_associations", fun test_get_user_associations/0},
      {"get_account_associations", fun test_get_account_associations/0},

      %% Archive/purge
      {"archive_old_jobs", fun test_archive_old_jobs/0},
      {"purge_old_jobs", fun test_purge_old_jobs/0},
      {"get_archived_jobs all", fun test_get_archived_jobs_all/0},
      {"get_archived_jobs filtered", fun test_get_archived_jobs_filtered/0},

      %% Statistics
      {"get_stats", fun test_get_stats/0}
     ]}.

setup() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, record_job_submit, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_start, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_end, fun(_, _, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_cancelled, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, get_job_record, fun(1) -> {ok, #{job_id => 1}}; (_) -> {error, not_found} end),
    meck:expect(flurm_dbd_server, list_job_records, fun() -> sample_jobs() end),
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, get_jobs_by_user, fun(_) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, get_jobs_by_account, fun(_) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, get_jobs_in_range, fun(_, _) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, format_sacct_output, fun(Jobs) -> io_lib:format("~p~n", [Jobs]) end),
    meck:expect(flurm_dbd_server, get_user_usage, fun(_) -> sample_usage() end),
    meck:expect(flurm_dbd_server, get_account_usage, fun(_) -> sample_usage() end),
    meck:expect(flurm_dbd_server, get_cluster_usage, fun() -> sample_usage() end),
    meck:expect(flurm_dbd_server, reset_usage, fun() -> ok end),
    meck:expect(flurm_dbd_server, add_association, fun(_, _, _) -> {ok, 1} end),
    meck:expect(flurm_dbd_server, add_association, fun(_, _, _, _) -> {ok, 2} end),
    meck:expect(flurm_dbd_server, get_associations, fun(_) -> sample_associations() end),
    meck:expect(flurm_dbd_server, get_association, fun(1) -> {ok, #{id => 1}}; (_) -> {error, not_found} end),
    meck:expect(flurm_dbd_server, remove_association, fun(1) -> ok; (_) -> {error, not_found} end),
    meck:expect(flurm_dbd_server, update_association, fun(1, _) -> ok; (_, _) -> {error, not_found} end),
    meck:expect(flurm_dbd_server, get_user_associations, fun(_) -> sample_associations() end),
    meck:expect(flurm_dbd_server, get_account_associations, fun(_) -> sample_associations() end),
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_) -> {ok, 10} end),
    meck:expect(flurm_dbd_server, purge_old_jobs, fun(_) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> sample_jobs() end),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun(_) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> sample_stats() end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(lager),
    ok.

sample_jobs() ->
    [
        #{job_id => 1, user_name => <<"alice">>, account => <<"research">>, state => completed},
        #{job_id => 2, user_name => <<"bob">>, account => <<"research">>, state => running}
    ].

sample_usage() ->
    #{cpu_seconds => 3600, mem_seconds => 1024000, job_count => 5}.

sample_associations() ->
    [
        #{id => 1, user => <<"alice">>, account => <<"research">>},
        #{id => 2, user => <<"bob">>, account => <<"research">>}
    ].

sample_stats() ->
    #{jobs_submitted => 100, jobs_completed => 90, total_job_records => 50}.

%% === Job recording ===

test_record_job_submit() ->
    ?assertEqual(ok, flurm_dbd:record_job_submit(#{job_id => 1})).

test_record_job_start() ->
    ?assertEqual(ok, flurm_dbd:record_job_start(1, [<<"node1">>])).

test_record_job_end() ->
    ?assertEqual(ok, flurm_dbd:record_job_end(1, 0, completed)).

test_record_job_cancelled() ->
    ?assertEqual(ok, flurm_dbd:record_job_cancelled(1, user_cancel)).

%% === Job queries ===

test_get_job_found() ->
    {ok, Job} = flurm_dbd:get_job(1),
    ?assertEqual(1, maps:get(job_id, Job)).

test_get_job_not_found() ->
    ?assertEqual({error, not_found}, flurm_dbd:get_job(9999)).

test_get_jobs_all() ->
    Jobs = flurm_dbd:get_jobs(),
    ?assertEqual(2, length(Jobs)).

test_get_jobs_filters() ->
    Jobs = flurm_dbd:get_jobs(#{user => <<"alice">>}),
    ?assert(is_list(Jobs)).

test_get_jobs_by_user() ->
    Jobs = flurm_dbd:get_jobs_by_user(<<"alice">>),
    ?assert(is_list(Jobs)).

test_get_jobs_by_account() ->
    Jobs = flurm_dbd:get_jobs_by_account(<<"research">>),
    ?assert(is_list(Jobs)).

test_get_jobs_in_range() ->
    Now = erlang:system_time(second),
    Jobs = flurm_dbd:get_jobs_in_range(Now - 3600, Now),
    ?assert(is_list(Jobs)).

%% === sacct output ===

test_sacct_all() ->
    Output = flurm_dbd:sacct(),
    ?assert(is_list(Output)).

test_sacct_filtered() ->
    Output = flurm_dbd:sacct(#{user => <<"alice">>}),
    ?assert(is_list(Output)).

test_format_sacct() ->
    Output = flurm_dbd:format_sacct(sample_jobs()),
    ?assert(is_list(Output)).

%% === TRES tracking ===

test_get_user_usage() ->
    Usage = flurm_dbd:get_user_usage(<<"alice">>),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)).

test_get_account_usage() ->
    Usage = flurm_dbd:get_account_usage(<<"research">>),
    ?assert(is_map(Usage)).

test_get_cluster_usage() ->
    Usage = flurm_dbd:get_cluster_usage(),
    ?assert(is_map(Usage)).

test_reset_usage() ->
    ?assertEqual(ok, flurm_dbd:reset_usage()).

%% === Association management ===

test_add_association_3() ->
    {ok, Id} = flurm_dbd:add_association(<<"cluster">>, <<"account">>, <<"user">>),
    ?assertEqual(1, Id).

test_add_association_4() ->
    {ok, Id} = flurm_dbd:add_association(<<"cluster">>, <<"account">>, <<"user">>, #{shares => 100}),
    ?assertEqual(2, Id).

test_get_associations() ->
    Assocs = flurm_dbd:get_associations(<<"cluster">>),
    ?assertEqual(2, length(Assocs)).

test_get_association() ->
    {ok, Assoc} = flurm_dbd:get_association(1),
    ?assertEqual(1, maps:get(id, Assoc)).

test_remove_association() ->
    ?assertEqual(ok, flurm_dbd:remove_association(1)),
    ?assertEqual({error, not_found}, flurm_dbd:remove_association(9999)).

test_update_association() ->
    ?assertEqual(ok, flurm_dbd:update_association(1, #{shares => 200})),
    ?assertEqual({error, not_found}, flurm_dbd:update_association(9999, #{})).

test_get_user_associations() ->
    Assocs = flurm_dbd:get_user_associations(<<"alice">>),
    ?assert(is_list(Assocs)).

test_get_account_associations() ->
    Assocs = flurm_dbd:get_account_associations(<<"research">>),
    ?assert(is_list(Assocs)).

%% === Archive/purge ===

test_archive_old_jobs() ->
    {ok, Count} = flurm_dbd:archive_old_jobs(90),
    ?assertEqual(10, Count).

test_purge_old_jobs() ->
    {ok, Count} = flurm_dbd:purge_old_jobs(365),
    ?assertEqual(5, Count).

test_get_archived_jobs_all() ->
    Jobs = flurm_dbd:get_archived_jobs(),
    ?assert(is_list(Jobs)).

test_get_archived_jobs_filtered() ->
    Jobs = flurm_dbd:get_archived_jobs(#{user => <<"alice">>}),
    ?assert(is_list(Jobs)).

%% === Statistics ===

test_get_stats() ->
    Stats = flurm_dbd:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(jobs_submitted, Stats)).
