%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_dbd module exports and API structure
%%%
%%% This module is a thin wrapper around flurm_dbd_server, providing
%%% a clean API for interacting with the accounting database.
%%% Since all functions delegate directly to flurm_dbd_server,
%%% there are no internal functions to test via ifdef(TEST) exports.
%%%
%%% These tests verify the module structure and export definitions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Module Export Tests
%%====================================================================

module_exists_test_() ->
    {"flurm_dbd module exists and loads",
     ?_assertMatch({module, flurm_dbd}, code:ensure_loaded(flurm_dbd))}.

%% Test that all expected Job Accounting API functions are exported
job_accounting_exports_test_() ->
    [{"record_job_submit/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_submit, 1))},
     {"record_job_start/2 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_start, 2))},
     {"record_job_end/3 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_end, 3))},
     {"record_job_cancelled/2 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_cancelled, 2))},
     {"get_job/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_job, 1))},
     {"get_jobs/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs, 0))},
     {"get_jobs/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs, 1))},
     {"get_jobs_by_user/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_by_user, 1))},
     {"get_jobs_by_account/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_by_account, 1))},
     {"get_jobs_in_range/2 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_in_range, 2))},
     {"sacct/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, sacct, 0))},
     {"sacct/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, sacct, 1))},
     {"format_sacct/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, format_sacct, 1))}].

%% Test that all expected TRES tracking API functions are exported
tres_tracking_exports_test_() ->
    [{"get_user_usage/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_user_usage, 1))},
     {"get_account_usage/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_account_usage, 1))},
     {"get_cluster_usage/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_cluster_usage, 0))},
     {"reset_usage/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, reset_usage, 0))}].

%% Test that all expected Association management API functions are exported
association_exports_test_() ->
    [{"add_association/3 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, add_association, 3))},
     {"add_association/4 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, add_association, 4))},
     {"get_associations/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_associations, 1))},
     {"get_association/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_association, 1))},
     {"remove_association/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, remove_association, 1))},
     {"update_association/2 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, update_association, 2))},
     {"get_user_associations/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_user_associations, 1))},
     {"get_account_associations/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_account_associations, 1))}].

%% Test that all expected Archive/purge API functions are exported
archive_exports_test_() ->
    [{"archive_old_jobs/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, archive_old_jobs, 1))},
     {"purge_old_jobs/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, purge_old_jobs, 1))},
     {"get_archived_jobs/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_archived_jobs, 0))},
     {"get_archived_jobs/1 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_archived_jobs, 1))}].

%% Test that statistics API function is exported
stats_exports_test_() ->
    [{"get_stats/0 is exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_stats, 0))}].

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [{"module_info/0 returns valid info",
      ?_assertMatch([_|_], flurm_dbd:module_info())},
     {"module_info exports returns list",
      ?_assertMatch([_|_], flurm_dbd:module_info(exports))}].

%% Count expected exports (excluding module_info functions)
export_count_test_() ->
    {"Expected number of exported functions",
     fun() ->
         Exports = flurm_dbd:module_info(exports),
         %% Filter out module_info/0 and module_info/1
         UserExports = [E || E = {F, _} <- Exports, F =/= module_info],
         %% 13 job accounting + 4 TRES + 8 association + 4 archive + 1 stats = 30
         ExpectedCount = 30,
         ?assertEqual(ExpectedCount, length(UserExports))
     end}.
