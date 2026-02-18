%%%-------------------------------------------------------------------
%%% @doc Delegation tests for flurm_dbd module
%%%
%%% This module is a thin delegation layer to flurm_dbd_server.
%%% These tests verify:
%%% 1. All exported functions exist with correct arity
%%% 2. Functions properly delegate to flurm_dbd_server
%%% 3. Return values are passed through correctly
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_delegation_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Export Tests - Verify all functions exist with correct arity
%%====================================================================

exports_test_() ->
    [
     %% Job Accounting API
     {"record_job_submit/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_submit, 1))},
     {"record_job_start/2 exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_start, 2))},
     {"record_job_end/3 exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_end, 3))},
     {"record_job_cancelled/2 exported",
      ?_assert(erlang:function_exported(flurm_dbd, record_job_cancelled, 2))},
     {"get_job/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_job, 1))},
     {"get_jobs/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs, 0))},
     {"get_jobs/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs, 1))},
     {"get_jobs_by_user/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_by_user, 1))},
     {"get_jobs_by_account/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_by_account, 1))},
     {"get_jobs_in_range/2 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_jobs_in_range, 2))},
     {"sacct/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, sacct, 0))},
     {"sacct/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, sacct, 1))},
     {"format_sacct/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, format_sacct, 1))},

     %% TRES Tracking API
     {"get_user_usage/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_user_usage, 1))},
     {"get_account_usage/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_account_usage, 1))},
     {"get_cluster_usage/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_cluster_usage, 0))},
     {"reset_usage/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, reset_usage, 0))},

     %% Association Management API
     {"add_association/3 exported",
      ?_assert(erlang:function_exported(flurm_dbd, add_association, 3))},
     {"add_association/4 exported",
      ?_assert(erlang:function_exported(flurm_dbd, add_association, 4))},
     {"get_associations/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_associations, 1))},
     {"get_association/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_association, 1))},
     {"remove_association/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, remove_association, 1))},
     {"update_association/2 exported",
      ?_assert(erlang:function_exported(flurm_dbd, update_association, 2))},
     {"get_user_associations/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_user_associations, 1))},
     {"get_account_associations/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_account_associations, 1))},

     %% Archive/Purge API
     {"archive_old_jobs/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, archive_old_jobs, 1))},
     {"purge_old_jobs/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, purge_old_jobs, 1))},
     {"get_archived_jobs/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_archived_jobs, 0))},
     {"get_archived_jobs/1 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_archived_jobs, 1))},

     %% Statistics API
     {"get_stats/0 exported",
      ?_assert(erlang:function_exported(flurm_dbd, get_stats, 0))}
    ].

%%====================================================================
%% Delegation Tests - Verify functions delegate to flurm_dbd_server
%%====================================================================

delegation_test_() ->
    {setup,
     fun setup_meck/0,
     fun cleanup_meck/1,
     fun(_) ->
         [
          %% Job Accounting Delegation Tests
          {"record_job_submit delegates to server",
           fun() ->
               JobData = #{job_id => 1, user_id => <<"test">>},
               meck:expect(flurm_dbd_server, record_job_submit, fun(_) -> ok end),
               ?assertEqual(ok, flurm_dbd:record_job_submit(JobData)),
               ?assert(meck:called(flurm_dbd_server, record_job_submit, [JobData]))
           end},

          {"record_job_start delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, record_job_start, fun(_, _) -> ok end),
               ?assertEqual(ok, flurm_dbd:record_job_start(123, [<<"node1">>])),
               ?assert(meck:called(flurm_dbd_server, record_job_start, [123, [<<"node1">>]]))
           end},

          {"record_job_end delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, record_job_end, fun(_, _, _) -> ok end),
               ?assertEqual(ok, flurm_dbd:record_job_end(123, 0, completed)),
               ?assert(meck:called(flurm_dbd_server, record_job_end, [123, 0, completed]))
           end},

          {"record_job_cancelled delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, record_job_cancelled, fun(_, _) -> ok end),
               ?assertEqual(ok, flurm_dbd:record_job_cancelled(123, user_request)),
               ?assert(meck:called(flurm_dbd_server, record_job_cancelled, [123, user_request]))
           end},

          {"get_job delegates to server",
           fun() ->
               ExpectedJob = #{job_id => 123, state => completed},
               meck:expect(flurm_dbd_server, get_job_record, fun(_) -> {ok, ExpectedJob} end),
               ?assertEqual({ok, ExpectedJob}, flurm_dbd:get_job(123)),
               ?assert(meck:called(flurm_dbd_server, get_job_record, [123]))
           end},

          {"get_jobs/0 delegates to server",
           fun() ->
               Jobs = [#{job_id => 1}, #{job_id => 2}],
               meck:expect(flurm_dbd_server, list_job_records, fun() -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_jobs()),
               ?assert(meck:called(flurm_dbd_server, list_job_records, []))
           end},

          {"get_jobs/1 delegates to server",
           fun() ->
               Filters = #{user => <<"alice">>},
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, list_job_records, fun(_) -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_jobs(Filters)),
               ?assert(meck:called(flurm_dbd_server, list_job_records, [Filters]))
           end},

          {"get_jobs_by_user delegates to server",
           fun() ->
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, get_jobs_by_user, fun(_) -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_jobs_by_user(<<"alice">>)),
               ?assert(meck:called(flurm_dbd_server, get_jobs_by_user, [<<"alice">>]))
           end},

          {"get_jobs_by_account delegates to server",
           fun() ->
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, get_jobs_by_account, fun(_) -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_jobs_by_account(<<"physics">>)),
               ?assert(meck:called(flurm_dbd_server, get_jobs_by_account, [<<"physics">>]))
           end},

          {"get_jobs_in_range delegates to server",
           fun() ->
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, get_jobs_in_range, fun(_, _) -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_jobs_in_range(1000, 2000)),
               ?assert(meck:called(flurm_dbd_server, get_jobs_in_range, [1000, 2000]))
           end},

          {"format_sacct delegates to server",
           fun() ->
               Output = <<"JobID|User|State\n">>,
               meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> Output end),
               ?assertEqual(Output, flurm_dbd:format_sacct([#{job_id => 1}])),
               ?assert(meck:called(flurm_dbd_server, format_sacct_output, '_'))
           end},

          %% TRES Tracking Delegation Tests
          {"get_user_usage delegates to server",
           fun() ->
               Usage = #{cpu_seconds => 1000},
               meck:expect(flurm_dbd_server, get_user_usage, fun(_) -> Usage end),
               ?assertEqual(Usage, flurm_dbd:get_user_usage(<<"alice">>)),
               ?assert(meck:called(flurm_dbd_server, get_user_usage, [<<"alice">>]))
           end},

          {"get_account_usage delegates to server",
           fun() ->
               Usage = #{cpu_seconds => 5000},
               meck:expect(flurm_dbd_server, get_account_usage, fun(_) -> Usage end),
               ?assertEqual(Usage, flurm_dbd:get_account_usage(<<"physics">>)),
               ?assert(meck:called(flurm_dbd_server, get_account_usage, [<<"physics">>]))
           end},

          {"get_cluster_usage delegates to server",
           fun() ->
               Usage = #{cpu_seconds => 100000},
               meck:expect(flurm_dbd_server, get_cluster_usage, fun() -> Usage end),
               ?assertEqual(Usage, flurm_dbd:get_cluster_usage()),
               ?assert(meck:called(flurm_dbd_server, get_cluster_usage, []))
           end},

          {"reset_usage delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, reset_usage, fun() -> ok end),
               ?assertEqual(ok, flurm_dbd:reset_usage()),
               ?assert(meck:called(flurm_dbd_server, reset_usage, []))
           end},

          %% Association Management Delegation Tests
          {"add_association/3 delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, add_association, fun(_, _, _) -> {ok, 1} end),
               ?assertEqual({ok, 1}, flurm_dbd:add_association(<<"cluster">>, <<"acct">>, <<"user">>)),
               ?assert(meck:called(flurm_dbd_server, add_association, [<<"cluster">>, <<"acct">>, <<"user">>]))
           end},

          {"add_association/4 delegates to server",
           fun() ->
               Opts = #{shares => 10},
               meck:expect(flurm_dbd_server, add_association, fun(_, _, _, _) -> {ok, 2} end),
               ?assertEqual({ok, 2}, flurm_dbd:add_association(<<"cluster">>, <<"acct">>, <<"user">>, Opts)),
               ?assert(meck:called(flurm_dbd_server, add_association, [<<"cluster">>, <<"acct">>, <<"user">>, Opts]))
           end},

          {"get_associations delegates to server",
           fun() ->
               Assocs = [#{id => 1}],
               meck:expect(flurm_dbd_server, get_associations, fun(_) -> Assocs end),
               ?assertEqual(Assocs, flurm_dbd:get_associations(<<"cluster">>)),
               ?assert(meck:called(flurm_dbd_server, get_associations, [<<"cluster">>]))
           end},

          {"get_association delegates to server",
           fun() ->
               Assoc = #{id => 1, user => <<"alice">>},
               meck:expect(flurm_dbd_server, get_association, fun(_) -> {ok, Assoc} end),
               ?assertEqual({ok, Assoc}, flurm_dbd:get_association(1)),
               ?assert(meck:called(flurm_dbd_server, get_association, [1]))
           end},

          {"remove_association delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, remove_association, fun(_) -> ok end),
               ?assertEqual(ok, flurm_dbd:remove_association(1)),
               ?assert(meck:called(flurm_dbd_server, remove_association, [1]))
           end},

          {"update_association delegates to server",
           fun() ->
               Updates = #{shares => 20},
               meck:expect(flurm_dbd_server, update_association, fun(_, _) -> ok end),
               ?assertEqual(ok, flurm_dbd:update_association(1, Updates)),
               ?assert(meck:called(flurm_dbd_server, update_association, [1, Updates]))
           end},

          {"get_user_associations delegates to server",
           fun() ->
               Assocs = [#{id => 1}],
               meck:expect(flurm_dbd_server, get_user_associations, fun(_) -> Assocs end),
               ?assertEqual(Assocs, flurm_dbd:get_user_associations(<<"alice">>)),
               ?assert(meck:called(flurm_dbd_server, get_user_associations, [<<"alice">>]))
           end},

          {"get_account_associations delegates to server",
           fun() ->
               Assocs = [#{id => 1}],
               meck:expect(flurm_dbd_server, get_account_associations, fun(_) -> Assocs end),
               ?assertEqual(Assocs, flurm_dbd:get_account_associations(<<"physics">>)),
               ?assert(meck:called(flurm_dbd_server, get_account_associations, [<<"physics">>]))
           end},

          %% Archive/Purge Delegation Tests
          {"archive_old_jobs delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, archive_old_jobs, fun(_) -> {ok, 10} end),
               ?assertEqual({ok, 10}, flurm_dbd:archive_old_jobs(30)),
               ?assert(meck:called(flurm_dbd_server, archive_old_jobs, [30]))
           end},

          {"purge_old_jobs delegates to server",
           fun() ->
               meck:expect(flurm_dbd_server, purge_old_jobs, fun(_) -> {ok, 5} end),
               ?assertEqual({ok, 5}, flurm_dbd:purge_old_jobs(90)),
               ?assert(meck:called(flurm_dbd_server, purge_old_jobs, [90]))
           end},

          {"get_archived_jobs/0 delegates to server",
           fun() ->
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_archived_jobs()),
               ?assert(meck:called(flurm_dbd_server, get_archived_jobs, []))
           end},

          {"get_archived_jobs/1 delegates to server",
           fun() ->
               Filters = #{user => <<"alice">>},
               Jobs = [#{job_id => 1}],
               meck:expect(flurm_dbd_server, get_archived_jobs, fun(_) -> Jobs end),
               ?assertEqual(Jobs, flurm_dbd:get_archived_jobs(Filters)),
               ?assert(meck:called(flurm_dbd_server, get_archived_jobs, [Filters]))
           end},

          %% Statistics Delegation Tests
          {"get_stats delegates to server",
           fun() ->
               Stats = #{jobs_submitted => 100},
               meck:expect(flurm_dbd_server, get_stats, fun() -> Stats end),
               ?assertEqual(Stats, flurm_dbd:get_stats()),
               ?assert(meck:called(flurm_dbd_server, get_stats, []))
           end}
         ]
     end}.

%%====================================================================
%% sacct Composite Function Tests
%%====================================================================

sacct_composite_test_() ->
    {setup,
     fun setup_meck/0,
     fun cleanup_meck/1,
     fun(_) ->
         [
          {"sacct/0 calls get_jobs then format_sacct",
           fun() ->
               Jobs = [#{job_id => 1}, #{job_id => 2}],
               Output = <<"formatted output">>,
               meck:expect(flurm_dbd_server, list_job_records, fun() -> Jobs end),
               meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> Output end),
               ?assertEqual(Output, flurm_dbd:sacct()),
               ?assert(meck:called(flurm_dbd_server, list_job_records, [])),
               ?assert(meck:called(flurm_dbd_server, format_sacct_output, [Jobs]))
           end},

          {"sacct/1 calls get_jobs with filters then format_sacct",
           fun() ->
               Filters = #{user => <<"alice">>},
               Jobs = [#{job_id => 1}],
               Output = <<"filtered output">>,
               meck:expect(flurm_dbd_server, list_job_records, fun(_) -> Jobs end),
               meck:expect(flurm_dbd_server, format_sacct_output, fun(_) -> Output end),
               ?assertEqual(Output, flurm_dbd:sacct(Filters)),
               ?assert(meck:called(flurm_dbd_server, list_job_records, [Filters])),
               ?assert(meck:called(flurm_dbd_server, format_sacct_output, [Jobs]))
           end}
         ]
     end}.

%%====================================================================
%% Error Propagation Tests
%%====================================================================

error_propagation_test_() ->
    {setup,
     fun setup_meck/0,
     fun cleanup_meck/1,
     fun(_) ->
         [
          {"get_job returns error from server",
           fun() ->
               meck:expect(flurm_dbd_server, get_job_record, fun(_) -> {error, not_found} end),
               ?assertEqual({error, not_found}, flurm_dbd:get_job(999))
           end},

          {"get_association returns error from server",
           fun() ->
               meck:expect(flurm_dbd_server, get_association, fun(_) -> {error, not_found} end),
               ?assertEqual({error, not_found}, flurm_dbd:get_association(999))
           end},

          {"remove_association returns error from server",
           fun() ->
               meck:expect(flurm_dbd_server, remove_association, fun(_) -> {error, not_found} end),
               ?assertEqual({error, not_found}, flurm_dbd:remove_association(999))
           end},

          {"update_association returns error from server",
           fun() ->
               meck:expect(flurm_dbd_server, update_association, fun(_, _) -> {error, not_found} end),
               ?assertEqual({error, not_found}, flurm_dbd:update_association(999, #{}))
           end},

          {"add_association returns error from server",
           fun() ->
               meck:expect(flurm_dbd_server, add_association, fun(_, _, _) -> {error, already_exists} end),
               ?assertEqual({error, already_exists}, flurm_dbd:add_association(<<"c">>, <<"a">>, <<"u">>))
           end}
         ]
     end}.

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [{"module_info/0 returns valid info",
      ?_assertMatch([_|_], flurm_dbd:module_info())},
     {"module_info/1 exports returns list",
      ?_assertMatch([_|_], flurm_dbd:module_info(exports))}].

export_count_test_() ->
    {"Expected number of exported functions (30 API functions)",
     fun() ->
         Exports = flurm_dbd:module_info(exports),
         %% Filter out module_info/0 and module_info/1
         UserExports = [E || E = {F, _} <- Exports, F =/= module_info],
         %% 13 job accounting + 4 TRES + 8 association + 4 archive + 1 stats = 30
         ExpectedCount = 30,
         ?assertEqual(ExpectedCount, length(UserExports))
     end}.

%%====================================================================
%% Setup/Cleanup Helpers
%%====================================================================

setup_meck() ->
    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [passthrough]),
    ok.

cleanup_meck(_) ->
    meck:unload(flurm_dbd_server),
    ok.
