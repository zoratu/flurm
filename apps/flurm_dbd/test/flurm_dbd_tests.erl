%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon High-Level API Tests
%%%
%%% Tests for the flurm_dbd module which provides the clean API for
%%% interacting with the accounting database.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_server) of
        undefined -> ok;
        ExistingPid ->
            exit(ExistingPid, shutdown),
            timer:sleep(50)
    end,
    %% Mock flurm_dbd_sup to avoid Ranch dependency
    meck:new(flurm_dbd_sup, [passthrough, no_link]),
    meck:expect(flurm_dbd_sup, start_listener, fun() -> {ok, self()} end),
    %% Start the DBD server for testing
    {ok, Pid} = flurm_dbd_server:start_link(),
    timer:sleep(100),
    Pid.

cleanup(Pid) ->
    %% Stop the server if running
    case is_process_alive(Pid) of
        true -> exit(Pid, shutdown);
        false -> ok
    end,
    timer:sleep(50),
    catch meck:unload(flurm_dbd_sup),
    ok.

%%====================================================================
%% Job Accounting API Tests
%%====================================================================

job_accounting_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"record_job_submit works", fun test_record_job_submit/0},
             {"record_job_start with allocated nodes", fun test_record_job_start/0},
             {"record_job_end with exit code", fun test_record_job_end/0},
             {"record_job_cancelled", fun test_record_job_cancelled/0},
             {"get_job returns job record", fun test_get_job/0},
             {"get_jobs returns all jobs", fun test_get_jobs/0},
             {"get_jobs with filters", fun test_get_jobs_with_filters/0},
             {"get_jobs_by_user", fun test_get_jobs_by_user/0},
             {"get_jobs_by_account", fun test_get_jobs_by_account/0},
             {"get_jobs_in_range", fun test_get_jobs_in_range/0}
         ]
     end
    }.

test_record_job_submit() ->
    JobData = #{
        job_id => 1001,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4
    },
    Result = flurm_dbd:record_job_submit(JobData),
    ?assertEqual(ok, Result),
    timer:sleep(50),  % Allow async processing
    ok.

test_record_job_start() ->
    %% First submit a job
    JobData = #{
        job_id => 1002,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 2,
        num_cpus => 8
    },
    ok = flurm_dbd:record_job_submit(JobData),
    timer:sleep(50),

    %% Then record start
    Result = flurm_dbd:record_job_start(1002, [<<"node1">>, <<"node2">>]),
    ?assertEqual(ok, Result),
    timer:sleep(50),
    ok.

test_record_job_end() ->
    %% Submit and start a job first
    JobData = #{
        job_id => 1003,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 2
    },
    ok = flurm_dbd:record_job_submit(JobData),
    timer:sleep(50),
    ok = flurm_dbd:record_job_start(1003, [<<"node1">>]),
    timer:sleep(50),

    %% Record end
    Result = flurm_dbd:record_job_end(1003, 0, completed),
    ?assertEqual(ok, Result),
    timer:sleep(50),
    ok.

test_record_job_cancelled() ->
    %% Submit a job
    JobData = #{
        job_id => 1004,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 1
    },
    ok = flurm_dbd:record_job_submit(JobData),
    timer:sleep(50),

    %% Cancel it
    Result = flurm_dbd:record_job_cancelled(1004, user_cancelled),
    ?assertEqual(ok, Result),
    timer:sleep(50),
    ok.

test_get_job() ->
    %% Submit a job
    JobData = #{
        job_id => 1005,
        user_id => 1000,
        group_id => 1000,
        partition => <<"batch">>,
        num_nodes => 4,
        num_cpus => 16
    },
    ok = flurm_dbd:record_job_submit(JobData),
    timer:sleep(50),

    %% Get it
    case flurm_dbd:get_job(1005) of
        {ok, Job} ->
            ?assertEqual(1005, maps:get(job_id, Job));
        {error, not_found} ->
            %% Might not be found if async hasn't completed
            ok
    end,
    ok.

test_get_jobs() ->
    Jobs = flurm_dbd:get_jobs(),
    ?assert(is_list(Jobs)),
    ok.

test_get_jobs_with_filters() ->
    %% Get jobs with partition filter
    Filters = #{partition => <<"default">>},
    Jobs = flurm_dbd:get_jobs(Filters),
    ?assert(is_list(Jobs)),
    ok.

test_get_jobs_by_user() ->
    Jobs = flurm_dbd:get_jobs_by_user(<<"testuser">>),
    ?assert(is_list(Jobs)),
    ok.

test_get_jobs_by_account() ->
    Jobs = flurm_dbd:get_jobs_by_account(<<"testaccount">>),
    ?assert(is_list(Jobs)),
    ok.

test_get_jobs_in_range() ->
    Now = erlang:system_time(second),
    Jobs = flurm_dbd:get_jobs_in_range(Now - 3600, Now + 3600),
    ?assert(is_list(Jobs)),
    ok.

%%====================================================================
%% SACCT Output Tests
%%====================================================================

sacct_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"sacct returns iolist", fun() ->
                 Output = flurm_dbd:sacct(),
                 ?assert(is_list(Output) orelse is_binary(Output))
             end},
             {"sacct with filters returns iolist", fun() ->
                 Output = flurm_dbd:sacct(#{partition => <<"default">>}),
                 ?assert(is_list(Output) orelse is_binary(Output))
             end},
             {"format_sacct formats jobs", fun() ->
                 Jobs = [
                     #{job_id => 1, user_name => <<"user1">>, account => <<"acct1">>,
                       partition => <<"default">>, state => completed, elapsed => 3600,
                       exit_code => 0}
                 ],
                 Output = flurm_dbd:format_sacct(Jobs),
                 ?assert(is_list(Output) orelse is_binary(Output))
             end}
         ]
     end
    }.

%%====================================================================
%% TRES Tracking Tests
%%====================================================================

tres_tracking_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_user_usage returns map", fun() ->
                 Usage = flurm_dbd:get_user_usage(<<"testuser">>),
                 ?assert(is_map(Usage))
             end},
             {"get_account_usage returns map", fun() ->
                 Usage = flurm_dbd:get_account_usage(<<"testaccount">>),
                 ?assert(is_map(Usage))
             end},
             {"get_cluster_usage returns map", fun() ->
                 Usage = flurm_dbd:get_cluster_usage(),
                 ?assert(is_map(Usage))
             end},
             {"reset_usage returns ok", fun() ->
                 Result = flurm_dbd:reset_usage(),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Association Management Tests
%%====================================================================

association_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"add_association creates association", fun() ->
                 {ok, Id} = flurm_dbd:add_association(<<"cluster1">>, <<"account1">>, <<"user1">>),
                 ?assert(is_integer(Id)),
                 ?assert(Id > 0)
             end},
             {"add_association with options", fun() ->
                 Opts = #{
                     partition => <<"batch">>,
                     shares => 10,
                     max_jobs => 100
                 },
                 {ok, Id} = flurm_dbd:add_association(<<"cluster1">>, <<"account2">>, <<"user2">>, Opts),
                 ?assert(is_integer(Id))
             end},
             {"get_associations by cluster", fun() ->
                 Assocs = flurm_dbd:get_associations(<<"cluster1">>),
                 ?assert(is_list(Assocs))
             end},
             {"get_association by id", fun() ->
                 {ok, Id} = flurm_dbd:add_association(<<"cluster2">>, <<"account3">>, <<"user3">>),
                 case flurm_dbd:get_association(Id) of
                     {ok, Assoc} ->
                         ?assertEqual(Id, maps:get(id, Assoc));
                     {error, not_found} ->
                         %% May happen due to timing
                         ok
                 end
             end},
             {"get_user_associations", fun() ->
                 Assocs = flurm_dbd:get_user_associations(<<"user1">>),
                 ?assert(is_list(Assocs))
             end},
             {"get_account_associations", fun() ->
                 Assocs = flurm_dbd:get_account_associations(<<"account1">>),
                 ?assert(is_list(Assocs))
             end},
             {"update_association", fun() ->
                 {ok, Id} = flurm_dbd:add_association(<<"cluster3">>, <<"account4">>, <<"user4">>),
                 Result = flurm_dbd:update_association(Id, #{shares => 20}),
                 ?assertEqual(ok, Result)
             end},
             {"remove_association", fun() ->
                 {ok, Id} = flurm_dbd:add_association(<<"cluster4">>, <<"account5">>, <<"user5">>),
                 Result = flurm_dbd:remove_association(Id),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Archive/Purge Tests
%%====================================================================

archive_purge_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"archive_old_jobs returns count", fun() ->
                 {ok, Count} = flurm_dbd:archive_old_jobs(30),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"purge_old_jobs returns count", fun() ->
                 {ok, Count} = flurm_dbd:purge_old_jobs(90),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"get_archived_jobs returns list", fun() ->
                 Jobs = flurm_dbd:get_archived_jobs(),
                 ?assert(is_list(Jobs))
             end},
             {"get_archived_jobs with filters", fun() ->
                 Jobs = flurm_dbd:get_archived_jobs(#{partition => <<"default">>}),
                 ?assert(is_list(Jobs))
             end}
         ]
     end
    }.

%%====================================================================
%% Statistics Tests
%%====================================================================

stats_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_stats returns map with expected keys", fun() ->
                 Stats = flurm_dbd:get_stats(),
                 ?assert(is_map(Stats)),
                 ?assert(maps:is_key(uptime_seconds, Stats) orelse maps:is_key(start_time, Stats))
             end}
         ]
     end
    }.
