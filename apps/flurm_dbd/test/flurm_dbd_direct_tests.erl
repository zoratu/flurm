%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd module
%%%
%%% These tests call the actual flurm_dbd functions directly to get
%%% code coverage. The flurm_dbd module is a thin wrapper around
%%% flurm_dbd_server, so we mock only the server (external dependency).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

flurm_dbd_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"record_job_submit delegates to server", fun test_record_job_submit/0},
      {"record_job_start delegates to server", fun test_record_job_start/0},
      {"record_job_end delegates to server", fun test_record_job_end/0},
      {"record_job_cancelled delegates to server", fun test_record_job_cancelled/0},
      {"get_job delegates to server", fun test_get_job/0},
      {"get_jobs with no args delegates to server", fun test_get_jobs/0},
      {"get_jobs with filters delegates to server", fun test_get_jobs_with_filters/0},
      {"get_jobs_by_user delegates to server", fun test_get_jobs_by_user/0},
      {"get_jobs_by_account delegates to server", fun test_get_jobs_by_account/0},
      {"get_jobs_in_range delegates to server", fun test_get_jobs_in_range/0},
      {"sacct no args formats jobs", fun test_sacct/0},
      {"sacct with filters formats jobs", fun test_sacct_with_filters/0},
      {"format_sacct formats job list", fun test_format_sacct/0},
      {"get_user_usage delegates to server", fun test_get_user_usage/0},
      {"get_account_usage delegates to server", fun test_get_account_usage/0},
      {"get_cluster_usage delegates to server", fun test_get_cluster_usage/0},
      {"reset_usage delegates to server", fun test_reset_usage/0},
      {"add_association with 3 args delegates to server", fun test_add_association_3/0},
      {"add_association with 4 args delegates to server", fun test_add_association_4/0},
      {"get_associations delegates to server", fun test_get_associations/0},
      {"get_association delegates to server", fun test_get_association/0},
      {"remove_association delegates to server", fun test_remove_association/0},
      {"update_association delegates to server", fun test_update_association/0},
      {"get_user_associations delegates to server", fun test_get_user_associations/0},
      {"get_account_associations delegates to server", fun test_get_account_associations/0},
      {"archive_old_jobs delegates to server", fun test_archive_old_jobs/0},
      {"purge_old_jobs delegates to server", fun test_purge_old_jobs/0},
      {"get_archived_jobs with no args delegates to server", fun test_get_archived_jobs/0},
      {"get_archived_jobs with filters delegates to server", fun test_get_archived_jobs_with_filters/0},
      {"get_stats delegates to server", fun test_get_stats/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking
    catch meck:unload(flurm_dbd_server),
    %% Mock flurm_dbd_server (the external dependency)
    meck:new(flurm_dbd_server, [passthrough, no_link]),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_server),
    ok.

%%====================================================================
%% Job Accounting Tests
%%====================================================================

test_record_job_submit() ->
    JobData = #{job_id => 1, user_id => 1000},
    meck:expect(flurm_dbd_server, record_job_submit, fun(_) -> ok end),
    ?assertEqual(ok, flurm_dbd:record_job_submit(JobData)),
    ?assert(meck:called(flurm_dbd_server, record_job_submit, [JobData])).

test_record_job_start() ->
    JobId = 123,
    Nodes = [<<"node1">>, <<"node2">>],
    meck:expect(flurm_dbd_server, record_job_start, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_dbd:record_job_start(JobId, Nodes)),
    ?assert(meck:called(flurm_dbd_server, record_job_start, [JobId, Nodes])).

test_record_job_end() ->
    JobId = 123,
    ExitCode = 0,
    FinalState = completed,
    meck:expect(flurm_dbd_server, record_job_end, fun(_, _, _) -> ok end),
    ?assertEqual(ok, flurm_dbd:record_job_end(JobId, ExitCode, FinalState)),
    ?assert(meck:called(flurm_dbd_server, record_job_end, [JobId, ExitCode, FinalState])).

test_record_job_cancelled() ->
    JobId = 123,
    Reason = user_request,
    meck:expect(flurm_dbd_server, record_job_cancelled, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_dbd:record_job_cancelled(JobId, Reason)),
    ?assert(meck:called(flurm_dbd_server, record_job_cancelled, [JobId, Reason])).

test_get_job() ->
    JobId = 123,
    Expected = {ok, #{job_id => 123, state => running}},
    meck:expect(flurm_dbd_server, get_job_record, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_job(JobId)),
    ?assert(meck:called(flurm_dbd_server, get_job_record, [JobId])).

test_get_jobs() ->
    Expected = [#{job_id => 1}, #{job_id => 2}],
    meck:expect(flurm_dbd_server, list_job_records, fun() -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_jobs()),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [])).

test_get_jobs_with_filters() ->
    Filters = #{user => <<"testuser">>},
    Expected = [#{job_id => 1}],
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_jobs(Filters)),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [Filters])).

test_get_jobs_by_user() ->
    Username = <<"testuser">>,
    Expected = [#{job_id => 1, user_name => Username}],
    meck:expect(flurm_dbd_server, get_jobs_by_user, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_jobs_by_user(Username)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_by_user, [Username])).

test_get_jobs_by_account() ->
    Account = <<"research">>,
    Expected = [#{job_id => 1, account => Account}],
    meck:expect(flurm_dbd_server, get_jobs_by_account, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_jobs_by_account(Account)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_by_account, [Account])).

test_get_jobs_in_range() ->
    StartTime = 1000000,
    EndTime = 2000000,
    Expected = [#{job_id => 1}],
    meck:expect(flurm_dbd_server, get_jobs_in_range, fun(_, _) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_jobs_in_range(StartTime, EndTime)),
    ?assert(meck:called(flurm_dbd_server, get_jobs_in_range, [StartTime, EndTime])).

%%====================================================================
%% sacct Tests
%%====================================================================

test_sacct() ->
    Jobs = [#{job_id => 1, user_name => <<"user1">>, account => <<"acct1">>,
              partition => <<"batch">>, state => completed, elapsed => 3600, exit_code => 0}],
    meck:expect(flurm_dbd_server, list_job_records, fun() -> Jobs end),
    meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> ["formatted output"] end),
    Result = flurm_dbd:sacct(),
    ?assert(is_list(Result)),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [])),
    ?assert(meck:called(flurm_dbd_server, format_sacct_output, '_')).

test_sacct_with_filters() ->
    Filters = #{user => <<"testuser">>},
    Jobs = [#{job_id => 1, user_name => <<"testuser">>}],
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> Jobs end),
    meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> ["formatted output"] end),
    Result = flurm_dbd:sacct(Filters),
    ?assert(is_list(Result)),
    ?assert(meck:called(flurm_dbd_server, list_job_records, [Filters])),
    ?assert(meck:called(flurm_dbd_server, format_sacct_output, '_')).

test_format_sacct() ->
    Jobs = [#{job_id => 1}],
    Expected = ["formatted"],
    meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:format_sacct(Jobs)),
    ?assert(meck:called(flurm_dbd_server, format_sacct_output, [Jobs])).

%%====================================================================
%% TRES Usage Tests
%%====================================================================

test_get_user_usage() ->
    Username = <<"testuser">>,
    Expected = #{cpu_seconds => 1000, job_count => 5},
    meck:expect(flurm_dbd_server, get_user_usage, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_user_usage(Username)),
    ?assert(meck:called(flurm_dbd_server, get_user_usage, [Username])).

test_get_account_usage() ->
    Account = <<"research">>,
    Expected = #{cpu_seconds => 5000, job_count => 20},
    meck:expect(flurm_dbd_server, get_account_usage, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_account_usage(Account)),
    ?assert(meck:called(flurm_dbd_server, get_account_usage, [Account])).

test_get_cluster_usage() ->
    Expected = #{cpu_seconds => 100000, job_count => 500},
    meck:expect(flurm_dbd_server, get_cluster_usage, fun() -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_cluster_usage()),
    ?assert(meck:called(flurm_dbd_server, get_cluster_usage, [])).

test_reset_usage() ->
    meck:expect(flurm_dbd_server, reset_usage, fun() -> ok end),
    ?assertEqual(ok, flurm_dbd:reset_usage()),
    ?assert(meck:called(flurm_dbd_server, reset_usage, [])).

%%====================================================================
%% Association Management Tests
%%====================================================================

test_add_association_3() ->
    Cluster = <<"flurm">>,
    Account = <<"research">>,
    User = <<"testuser">>,
    Expected = {ok, 1},
    meck:expect(flurm_dbd_server, add_association, fun(_, _, _) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:add_association(Cluster, Account, User)),
    ?assert(meck:called(flurm_dbd_server, add_association, [Cluster, Account, User])).

test_add_association_4() ->
    Cluster = <<"flurm">>,
    Account = <<"research">>,
    User = <<"testuser">>,
    Opts = #{shares => 10, max_jobs => 100},
    Expected = {ok, 2},
    meck:expect(flurm_dbd_server, add_association, fun(_, _, _, _) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:add_association(Cluster, Account, User, Opts)),
    ?assert(meck:called(flurm_dbd_server, add_association, [Cluster, Account, User, Opts])).

test_get_associations() ->
    Cluster = <<"flurm">>,
    Expected = [#{id => 1, cluster => Cluster, account => <<"acct1">>, user => <<"user1">>}],
    meck:expect(flurm_dbd_server, get_associations, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_associations(Cluster)),
    ?assert(meck:called(flurm_dbd_server, get_associations, [Cluster])).

test_get_association() ->
    AssocId = 1,
    Expected = {ok, #{id => 1, cluster => <<"flurm">>}},
    meck:expect(flurm_dbd_server, get_association, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_association(AssocId)),
    ?assert(meck:called(flurm_dbd_server, get_association, [AssocId])).

test_remove_association() ->
    AssocId = 1,
    meck:expect(flurm_dbd_server, remove_association, fun(_) -> ok end),
    ?assertEqual(ok, flurm_dbd:remove_association(AssocId)),
    ?assert(meck:called(flurm_dbd_server, remove_association, [AssocId])).

test_update_association() ->
    AssocId = 1,
    Updates = #{shares => 20},
    meck:expect(flurm_dbd_server, update_association, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_dbd:update_association(AssocId, Updates)),
    ?assert(meck:called(flurm_dbd_server, update_association, [AssocId, Updates])).

test_get_user_associations() ->
    Username = <<"testuser">>,
    Expected = [#{id => 1, user => Username}],
    meck:expect(flurm_dbd_server, get_user_associations, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_user_associations(Username)),
    ?assert(meck:called(flurm_dbd_server, get_user_associations, [Username])).

test_get_account_associations() ->
    Account = <<"research">>,
    Expected = [#{id => 1, account => Account}],
    meck:expect(flurm_dbd_server, get_account_associations, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_account_associations(Account)),
    ?assert(meck:called(flurm_dbd_server, get_account_associations, [Account])).

%%====================================================================
%% Archive/Purge Tests
%%====================================================================

test_archive_old_jobs() ->
    Days = 30,
    Expected = {ok, 100},
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:archive_old_jobs(Days)),
    ?assert(meck:called(flurm_dbd_server, archive_old_jobs, [Days])).

test_purge_old_jobs() ->
    Days = 90,
    Expected = {ok, 50},
    meck:expect(flurm_dbd_server, purge_old_jobs, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:purge_old_jobs(Days)),
    ?assert(meck:called(flurm_dbd_server, purge_old_jobs, [Days])).

test_get_archived_jobs() ->
    Expected = [#{job_id => 1, state => completed}],
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_archived_jobs()),
    ?assert(meck:called(flurm_dbd_server, get_archived_jobs, [])).

test_get_archived_jobs_with_filters() ->
    Filters = #{user => <<"olduser">>},
    Expected = [#{job_id => 1}],
    meck:expect(flurm_dbd_server, get_archived_jobs, fun(_) -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_archived_jobs(Filters)),
    ?assert(meck:called(flurm_dbd_server, get_archived_jobs, [Filters])).

%%====================================================================
%% Statistics Tests
%%====================================================================

test_get_stats() ->
    Expected = #{jobs_submitted => 1000, jobs_completed => 900},
    meck:expect(flurm_dbd_server, get_stats, fun() -> Expected end),
    ?assertEqual(Expected, flurm_dbd:get_stats()),
    ?assert(meck:called(flurm_dbd_server, get_stats, [])).
