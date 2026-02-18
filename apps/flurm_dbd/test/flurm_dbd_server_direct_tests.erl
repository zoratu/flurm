%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_server module
%%%
%%% These tests start the actual flurm_dbd_server gen_server and call
%%% its functions directly to achieve code coverage. External dependencies
%%% like lager and flurm_dbd_sup are mocked.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

%% Tests that require running server
flurm_dbd_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link starts server", fun test_start_link/0},
      {"record_job_start cast", fun test_record_job_start_cast/0},
      {"record_job_end cast", fun test_record_job_end_cast/0},
      {"record_job_step cast", fun test_record_job_step_cast/0},
      {"record_job_submit cast", fun test_record_job_submit/0},
      {"record_job_start event cast", fun test_record_job_start_event/0},
      {"record_job_end event cast", fun test_record_job_end_event/0},
      {"record_job_cancelled cast", fun test_record_job_cancelled/0},
      {"get_job_record call", fun test_get_job_record/0},
      {"list_job_records call", fun test_list_job_records/0},
      {"list_job_records with filters", fun test_list_job_records_filters/0},
      {"get_user_usage call", fun test_get_user_usage/0},
      {"get_account_usage call", fun test_get_account_usage/0},
      {"get_cluster_usage call", fun test_get_cluster_usage/0},
      {"reset_usage call", fun test_reset_usage/0},
      {"archive_old_records call", fun test_archive_old_records/0},
      {"purge_old_records call", fun test_purge_old_records/0},
      {"get_stats call", fun test_get_stats/0},
      {"calculate_user_tres_usage call", fun test_calculate_user_tres_usage/0},
      {"calculate_account_tres_usage call", fun test_calculate_account_tres_usage/0},
      {"calculate_cluster_tres_usage call", fun test_calculate_cluster_tres_usage/0},
      {"get_jobs_by_user call", fun test_get_jobs_by_user/0},
      {"get_jobs_by_account call", fun test_get_jobs_by_account/0},
      {"get_jobs_in_range call", fun test_get_jobs_in_range/0},
      {"format_sacct_output call", fun test_format_sacct_output/0},
      {"add_association call", fun test_add_association/0},
      {"add_association with opts call", fun test_add_association_with_opts/0},
      {"get_associations call", fun test_get_associations/0},
      {"get_association call", fun test_get_association/0},
      {"remove_association call", fun test_remove_association/0},
      {"update_association call", fun test_update_association/0},
      {"get_user_associations call", fun test_get_user_associations/0},
      {"get_account_associations call", fun test_get_account_associations/0},
      {"archive_old_jobs call", fun test_archive_old_jobs/0},
      {"purge_old_jobs call", fun test_purge_old_jobs/0},
      {"get_archived_jobs call", fun test_get_archived_jobs/0},
      {"get_archived_jobs with filters call", fun test_get_archived_jobs_filters/0},
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast is handled", fun test_unknown_cast/0},
      {"handle_info start_listener", fun test_handle_info_start_listener/0},
      {"handle_info other", fun test_handle_info_other/0},
      {"terminate callback", fun test_terminate/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking these modules
    catch meck:unload(lager),
    catch meck:unload(flurm_dbd_sup),

    %% Mock lager for logging
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock flurm_dbd_sup to prevent listener startup
    catch meck:unload(flurm_dbd_sup),
    meck:new(flurm_dbd_sup, [passthrough, no_link]),
    meck:expect(flurm_dbd_sup, start_listener, fun() -> {ok, self()} end),
    ok.

cleanup(_) ->
    %% Stop server if running
    case whereis(flurm_dbd_server) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, normal, 5000)
    end,
    catch meck:unload(lager),
    catch meck:unload(flurm_dbd_sup),
    ok.

%%====================================================================
%% Server Lifecycle Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

%%====================================================================
%% Job Recording Tests (Cast Operations)
%%====================================================================

test_record_job_start_cast() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    JobInfo = #{job_id => 1, name => <<"test_job">>, user_name => <<"testuser">>,
                partition => <<"batch">>, state => running},
    ?assertEqual(ok, flurm_dbd_server:record_job_start(JobInfo)),
    _ = sys:get_state(flurm_dbd_server), % Allow cast to process
    gen_server:stop(Pid).

test_record_job_end_cast() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% First create a job
    JobInfo = #{job_id => 2, name => <<"test_job">>, state => running},
    flurm_dbd_server:record_job_start(JobInfo),
    _ = sys:get_state(flurm_dbd_server),
    %% Then end it
    EndInfo = #{job_id => 2, exit_code => 0, state => completed},
    ?assertEqual(ok, flurm_dbd_server:record_job_end(EndInfo)),
    _ = sys:get_state(flurm_dbd_server),
    gen_server:stop(Pid).

test_record_job_step_cast() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    StepInfo = #{job_id => 1, step_id => 0, name => <<"step0">>, state => running},
    ?assertEqual(ok, flurm_dbd_server:record_job_step(StepInfo)),
    _ = sys:get_state(flurm_dbd_server),
    gen_server:stop(Pid).

test_record_job_submit() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    JobData = #{job_id => 10, user_id => 1000, group_id => 1000,
                partition => <<"batch">>, num_nodes => 2, num_cpus => 4},
    ?assertEqual(ok, flurm_dbd_server:record_job_submit(JobData)),
    _ = sys:get_state(flurm_dbd_server),
    %% Verify job was recorded
    {ok, Job} = flurm_dbd_server:get_job_record(10),
    ?assertEqual(10, maps:get(job_id, Job)),
    ?assertEqual(pending, maps:get(state, Job)),
    gen_server:stop(Pid).

test_record_job_start_event() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% First submit a job
    JobData = #{job_id => 11, user_id => 1000},
    flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    %% Then start it
    ?assertEqual(ok, flurm_dbd_server:record_job_start(11, [<<"node1">>, <<"node2">>])),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(11),
    ?assertEqual(running, maps:get(state, Job)),
    ?assertEqual(2, maps:get(num_nodes, Job)),
    gen_server:stop(Pid).

test_record_job_end_event() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Create job through submit and start
    JobData = #{job_id => 12, user_id => 1000},
    flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    flurm_dbd_server:record_job_start(12, [<<"node1">>]),
    _ = sys:get_state(flurm_dbd_server),
    %% End job
    ?assertEqual(ok, flurm_dbd_server:record_job_end(12, 0, completed)),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(12),
    ?assertEqual(completed, maps:get(state, Job)),
    ?assertEqual(0, maps:get(exit_code, Job)),
    gen_server:stop(Pid).

test_record_job_cancelled() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Create a job first
    JobData = #{job_id => 13, user_id => 1000},
    flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    %% Cancel it
    ?assertEqual(ok, flurm_dbd_server:record_job_cancelled(13, user_request)),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(13),
    ?assertEqual(cancelled, maps:get(state, Job)),
    gen_server:stop(Pid).

%%====================================================================
%% Job Query Tests (Call Operations)
%%====================================================================

test_get_job_record() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Test not found
    ?assertEqual({error, not_found}, flurm_dbd_server:get_job_record(999)),
    %% Create and retrieve
    JobData = #{job_id => 20, user_id => 1000},
    flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(20),
    ?assertEqual(20, maps:get(job_id, Job)),
    gen_server:stop(Pid).

test_list_job_records() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Initially empty
    ?assertEqual([], flurm_dbd_server:list_job_records()),
    %% Add some jobs
    flurm_dbd_server:record_job_submit(#{job_id => 21, user_id => 1000}),
    flurm_dbd_server:record_job_submit(#{job_id => 22, user_id => 1001}),
    _ = sys:get_state(flurm_dbd_server),
    Jobs = flurm_dbd_server:list_job_records(),
    ?assertEqual(2, length(Jobs)),
    gen_server:stop(Pid).

test_list_job_records_filters() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Add jobs with different users
    flurm_dbd_server:record_job_start(#{job_id => 31, user_name => <<"alice">>, account => <<"acct1">>, state => running}),
    flurm_dbd_server:record_job_start(#{job_id => 32, user_name => <<"bob">>, account => <<"acct1">>, state => completed}),
    _ = sys:get_state(flurm_dbd_server),
    %% Filter by user
    AliceJobs = flurm_dbd_server:list_job_records(#{user => <<"alice">>}),
    ?assertEqual(1, length(AliceJobs)),
    %% Filter by account
    AcctJobs = flurm_dbd_server:list_job_records(#{account => <<"acct1">>}),
    ?assertEqual(2, length(AcctJobs)),
    %% Filter by state
    RunningJobs = flurm_dbd_server:list_job_records(#{state => running}),
    ?assertEqual(1, length(RunningJobs)),
    gen_server:stop(Pid).

%%====================================================================
%% Usage Tracking Tests
%%====================================================================

test_get_user_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:get_user_usage(<<"testuser">>),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    ?assert(maps:is_key(job_count, Usage)),
    gen_server:stop(Pid).

test_get_account_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:get_account_usage(<<"research">>),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    gen_server:stop(Pid).

test_get_cluster_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:get_cluster_usage(),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    gen_server:stop(Pid).

test_reset_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    ?assertEqual(ok, flurm_dbd_server:reset_usage()),
    gen_server:stop(Pid).

%%====================================================================
%% Archive/Purge Tests (old records)
%%====================================================================

test_archive_old_records() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, Count} = flurm_dbd_server:archive_old_records(30),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    gen_server:stop(Pid).

test_purge_old_records() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, Count} = flurm_dbd_server:purge_old_records(90),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    gen_server:stop(Pid).

%%====================================================================
%% Statistics Tests
%%====================================================================

test_get_stats() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Stats = flurm_dbd_server:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(jobs_submitted, Stats)),
    ?assert(maps:is_key(jobs_completed, Stats)),
    ?assert(maps:is_key(uptime_seconds, Stats)),
    gen_server:stop(Pid).

%%====================================================================
%% TRES Usage Calculation Tests
%%====================================================================

test_calculate_user_tres_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:calculate_user_tres_usage(<<"testuser">>),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    ?assert(maps:is_key(mem_seconds, Usage)),
    ?assert(maps:is_key(gpu_seconds, Usage)),
    gen_server:stop(Pid).

test_calculate_account_tres_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:calculate_account_tres_usage(<<"research">>),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    gen_server:stop(Pid).

test_calculate_cluster_tres_usage() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Usage = flurm_dbd_server:calculate_cluster_tres_usage(),
    ?assert(is_map(Usage)),
    ?assert(maps:is_key(cpu_seconds, Usage)),
    gen_server:stop(Pid).

%%====================================================================
%% sacct-style Query Tests
%%====================================================================

test_get_jobs_by_user() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Add jobs for different users
    flurm_dbd_server:record_job_start(#{job_id => 40, user_name => <<"alice">>}),
    flurm_dbd_server:record_job_start(#{job_id => 41, user_name => <<"bob">>}),
    _ = sys:get_state(flurm_dbd_server),
    AliceJobs = flurm_dbd_server:get_jobs_by_user(<<"alice">>),
    ?assertEqual(1, length(AliceJobs)),
    ?assertEqual(<<"alice">>, maps:get(user_name, hd(AliceJobs))),
    gen_server:stop(Pid).

test_get_jobs_by_account() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    flurm_dbd_server:record_job_start(#{job_id => 42, account => <<"physics">>}),
    flurm_dbd_server:record_job_start(#{job_id => 43, account => <<"chem">>}),
    _ = sys:get_state(flurm_dbd_server),
    PhysicsJobs = flurm_dbd_server:get_jobs_by_account(<<"physics">>),
    ?assertEqual(1, length(PhysicsJobs)),
    gen_server:stop(Pid).

test_get_jobs_in_range() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_start(#{job_id => 44, start_time => Now}),
    _ = sys:get_state(flurm_dbd_server),
    Jobs = flurm_dbd_server:get_jobs_in_range(Now - 100, Now + 100),
    ?assert(length(Jobs) >= 1),
    gen_server:stop(Pid).

test_format_sacct_output() ->
    Jobs = [
        #{job_id => 1, user_name => <<"user1">>, account => <<"acct1">>,
          partition => <<"batch">>, state => completed, elapsed => 3600, exit_code => 0}
    ],
    Output = flurm_dbd_server:format_sacct_output(Jobs),
    ?assert(is_list(Output)),
    OutputStr = lists:flatten(Output),
    ?assert(string:find(OutputStr, "JobID") =/= nomatch),
    ?assert(string:find(OutputStr, "User") =/= nomatch).

%%====================================================================
%% Association Management Tests
%%====================================================================

test_add_association() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, AssocId} = flurm_dbd_server:add_association(<<"flurm">>, <<"research">>, <<"testuser">>),
    ?assert(is_integer(AssocId)),
    ?assert(AssocId > 0),
    gen_server:stop(Pid).

test_add_association_with_opts() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Opts = #{shares => 10, max_jobs => 100, max_wall => 1440,
             partition => <<"gpu">>, qos => [<<"normal">>, <<"high">>]},
    {ok, AssocId} = flurm_dbd_server:add_association(<<"flurm">>, <<"research">>, <<"gpuuser">>, Opts),
    ?assert(AssocId > 0),
    {ok, Assoc} = flurm_dbd_server:get_association(AssocId),
    ?assertEqual(10, maps:get(shares, Assoc)),
    ?assertEqual(100, maps:get(max_jobs, Assoc)),
    gen_server:stop(Pid).

test_get_associations() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    flurm_dbd_server:add_association(<<"flurm">>, <<"acct1">>, <<"user1">>),
    flurm_dbd_server:add_association(<<"flurm">>, <<"acct2">>, <<"user2">>),
    flurm_dbd_server:add_association(<<"other">>, <<"acct3">>, <<"user3">>),
    FlurmAssocs = flurm_dbd_server:get_associations(<<"flurm">>),
    ?assertEqual(2, length(FlurmAssocs)),
    gen_server:stop(Pid).

test_get_association() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, AssocId} = flurm_dbd_server:add_association(<<"flurm">>, <<"research">>, <<"alice">>),
    {ok, Assoc} = flurm_dbd_server:get_association(AssocId),
    ?assertEqual(<<"alice">>, maps:get(user, Assoc)),
    %% Test not found
    ?assertEqual({error, not_found}, flurm_dbd_server:get_association(99999)),
    gen_server:stop(Pid).

test_remove_association() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, AssocId} = flurm_dbd_server:add_association(<<"flurm">>, <<"temp">>, <<"tempuser">>),
    ?assertEqual(ok, flurm_dbd_server:remove_association(AssocId)),
    ?assertEqual({error, not_found}, flurm_dbd_server:get_association(AssocId)),
    %% Removing non-existent
    ?assertEqual({error, not_found}, flurm_dbd_server:remove_association(99999)),
    gen_server:stop(Pid).

test_update_association() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, AssocId} = flurm_dbd_server:add_association(<<"flurm">>, <<"research">>, <<"bob">>),
    Updates = #{shares => 20, max_jobs => 50},
    ?assertEqual(ok, flurm_dbd_server:update_association(AssocId, Updates)),
    {ok, Updated} = flurm_dbd_server:get_association(AssocId),
    ?assertEqual(20, maps:get(shares, Updated)),
    ?assertEqual(50, maps:get(max_jobs, Updated)),
    %% Update non-existent
    ?assertEqual({error, not_found}, flurm_dbd_server:update_association(99999, Updates)),
    gen_server:stop(Pid).

test_get_user_associations() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    flurm_dbd_server:add_association(<<"flurm">>, <<"acct1">>, <<"charlie">>),
    flurm_dbd_server:add_association(<<"flurm">>, <<"acct2">>, <<"charlie">>),
    flurm_dbd_server:add_association(<<"flurm">>, <<"acct1">>, <<"dave">>),
    CharlieAssocs = flurm_dbd_server:get_user_associations(<<"charlie">>),
    ?assertEqual(2, length(CharlieAssocs)),
    gen_server:stop(Pid).

test_get_account_associations() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    flurm_dbd_server:add_association(<<"flurm">>, <<"physics">>, <<"user1">>),
    flurm_dbd_server:add_association(<<"flurm">>, <<"physics">>, <<"user2">>),
    flurm_dbd_server:add_association(<<"flurm">>, <<"chem">>, <<"user3">>),
    PhysicsAssocs = flurm_dbd_server:get_account_associations(<<"physics">>),
    ?assertEqual(2, length(PhysicsAssocs)),
    gen_server:stop(Pid).

%%====================================================================
%% Archive/Purge Jobs Tests
%%====================================================================

test_archive_old_jobs() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Add old completed job
    OldTime = erlang:system_time(second) - (40 * 86400), % 40 days ago
    flurm_dbd_server:record_job_start(#{job_id => 50, state => completed,
                                        end_time => OldTime}),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Count} = flurm_dbd_server:archive_old_jobs(30),
    ?assert(is_integer(Count)),
    gen_server:stop(Pid).

test_purge_old_jobs() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    {ok, Count} = flurm_dbd_server:purge_old_jobs(90),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    gen_server:stop(Pid).

test_get_archived_jobs() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Archived = flurm_dbd_server:get_archived_jobs(),
    ?assert(is_list(Archived)),
    gen_server:stop(Pid).

test_get_archived_jobs_filters() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Archived = flurm_dbd_server:get_archived_jobs(#{user => <<"olduser">>}),
    ?assert(is_list(Archived)),
    gen_server:stop(Pid).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_unknown_call() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Result = gen_server:call(Pid, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result),
    gen_server:stop(Pid).

test_unknown_cast() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Should not crash
    gen_server:cast(Pid, {unknown_cast, bar}),
    _ = sys:get_state(flurm_dbd_server),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_handle_info_start_listener() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% The init already sends start_listener, but we can send another
    Pid ! start_listener,
    _ = sys:get_state(flurm_dbd_server),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_handle_info_other() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    Pid ! {some, random, message},
    _ = sys:get_state(flurm_dbd_server),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_terminate() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Calling stop triggers terminate/2
    gen_server:stop(Pid, normal, 5000),
    _ = sys:get_state(flurm_dbd_server),
    ?assertEqual(undefined, whereis(flurm_dbd_server)).

%%====================================================================
%% Additional Job Recording Edge Cases
%%====================================================================

job_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"record_job_end for new job", fun test_record_job_end_new_job/0},
      {"record_job_start_event for new job", fun test_record_job_start_event_new_job/0},
      {"record_job_end_event for unknown job", fun test_record_job_end_event_unknown/0},
      {"record_job_cancelled for unknown job", fun test_record_job_cancelled_unknown/0},
      {"record_job_submit with timestamp tuple", fun test_record_job_submit_timestamp_tuple/0}
     ]}.

test_record_job_end_new_job() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% End a job that was never started - should create new record
    EndInfo = #{job_id => 100, exit_code => 1, state => failed},
    flurm_dbd_server:record_job_end(EndInfo),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(100),
    ?assertEqual(100, maps:get(job_id, Job)),
    gen_server:stop(Pid).

test_record_job_start_event_new_job() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Start event for job that was never submitted
    flurm_dbd_server:record_job_start(101, [<<"node1">>]),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(101),
    ?assertEqual(running, maps:get(state, Job)),
    gen_server:stop(Pid).

test_record_job_end_event_unknown() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% End event for unknown job - should log warning but not crash
    flurm_dbd_server:record_job_end(999, 0, completed),
    _ = sys:get_state(flurm_dbd_server),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_record_job_cancelled_unknown() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Cancel unknown job - should log warning but not crash
    flurm_dbd_server:record_job_cancelled(999, timeout),
    _ = sys:get_state(flurm_dbd_server),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_record_job_submit_timestamp_tuple() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Submit with timestamp as {MegaSecs, Secs, MicroSecs} tuple
    Now = os:timestamp(),
    JobData = #{job_id => 102, user_id => 1000, submit_time => Now},
    flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    {ok, Job} = flurm_dbd_server:get_job_record(102),
    ?assert(maps:get(submit_time, Job) > 0),
    gen_server:stop(Pid).

%%====================================================================
%% Listener Error Handling Test
%%====================================================================

listener_error_test_() ->
    {foreach,
     fun() ->
         catch meck:unload(lager),
         catch meck:unload(flurm_dbd_sup),

         meck:new(lager, [no_link, non_strict]),
         meck:expect(lager, info, fun(_) -> ok end),
         meck:expect(lager, info, fun(_, _) -> ok end),
         meck:expect(lager, debug, fun(_, _) -> ok end),
         meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
         %% Mock flurm_dbd_sup to fail listener start
         catch meck:unload(flurm_dbd_sup),
         meck:new(flurm_dbd_sup, [passthrough, no_link]),
         meck:expect(flurm_dbd_sup, start_listener, fun() -> {error, eaddrinuse} end),
         ok
     end,
     fun(_) ->
         case whereis(flurm_dbd_server) of
             undefined -> ok;
             Pid -> catch gen_server:stop(Pid, normal, 5000)
         end,
         catch meck:unload(lager),
         catch meck:unload(flurm_dbd_sup),
         ok
     end,
     [
      {"handle listener start failure", fun test_listener_start_failure/0}
     ]}.

test_listener_start_failure() ->
    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Sync to wait for handle_info start_listener to process
    _ = sys:get_state(Pid),
    %% Server should still be running even if listener failed
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).
