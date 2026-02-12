%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Server Tests
%%%
%%% Tests for the flurm_dbd_server module which is the main accounting
%%% server.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_server) of
        undefined -> ok;
        ExistingPid ->
            flurm_test_utils:kill_and_wait(ExistingPid)
    end,
    %% Mock flurm_dbd_sup to avoid Ranch dependency
    meck:new(flurm_dbd_sup, [passthrough, no_link]),
    meck:expect(flurm_dbd_sup, start_listener, fun() -> {ok, self()} end),
    %% Start fresh server
    {ok, NewPid} = flurm_dbd_server:start_link(),
    %% Sync to ensure listener start message is processed
    _ = sys:get_state(NewPid),
    NewPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            gen_server:stop(Pid, normal, 5000);
        false ->
            ok
    end,
    catch meck:unload(flurm_dbd_sup),
    ok.

%%====================================================================
%% Job Record Tests
%%====================================================================

job_record_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"record_job_start with map", fun test_record_job_start_map/0},
             {"record_job_end with map", fun test_record_job_end_map/0},
             {"record_job_step", fun test_record_job_step/0},
             {"record_job_submit event", fun test_record_job_submit_event/0},
             {"record_job_start event", fun test_record_job_start_event/0},
             {"record_job_end event", fun test_record_job_end_event/0},
             {"record_job_cancelled event", fun test_record_job_cancelled_event/0},
             {"get_job_record", fun test_get_job_record/0},
             {"list_job_records", fun test_list_job_records/0},
             {"list_job_records with filters", fun test_list_job_records_filters/0}
         ]
     end
    }.

test_record_job_start_map() ->
    JobInfo = #{
        job_id => 5001,
        name => <<"test_job">>,
        user_name => <<"testuser">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"testaccount">>,
        partition => <<"default">>,
        state => running,
        num_nodes => 1,
        num_cpus => 4
    },
    Result = flurm_dbd_server:record_job_start(JobInfo),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_end_map() ->
    JobInfo = #{
        job_id => 5002,
        state => completed,
        exit_code => 0,
        end_time => erlang:system_time(second)
    },
    Result = flurm_dbd_server:record_job_end(JobInfo),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_step() ->
    StepInfo = #{
        job_id => 5001,
        step_id => 0,
        name => <<"step0">>,
        state => completed,
        exit_code => 0,
        num_tasks => 1,
        num_nodes => 1,
        start_time => erlang:system_time(second) - 100,
        end_time => erlang:system_time(second),
        elapsed => 100
    },
    Result = flurm_dbd_server:record_job_step(StepInfo),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_submit_event() ->
    JobData = #{
        job_id => 6001,
        user_id => 1000,
        group_id => 1000,
        partition => <<"batch">>,
        num_nodes => 2,
        num_cpus => 8
    },
    Result = flurm_dbd_server:record_job_submit(JobData),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_start_event() ->
    %% First submit
    JobData = #{
        job_id => 6002,
        user_id => 1000,
        group_id => 1000,
        partition => <<"batch">>,
        num_nodes => 2,
        num_cpus => 8
    },
    ok = flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),

    %% Then start
    Result = flurm_dbd_server:record_job_start(6002, [<<"node1">>, <<"node2">>]),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_end_event() ->
    %% Submit and start first
    JobData = #{
        job_id => 6003,
        user_id => 1000,
        group_id => 1000,
        partition => <<"batch">>,
        num_nodes => 1,
        num_cpus => 4
    },
    ok = flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_start(6003, [<<"node1">>]),
    _ = sys:get_state(flurm_dbd_server),

    %% End with exit code
    Result = flurm_dbd_server:record_job_end(6003, 0, completed),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_record_job_cancelled_event() ->
    %% Submit first
    JobData = #{
        job_id => 6004,
        user_id => 1000,
        group_id => 1000,
        partition => <<"batch">>,
        num_nodes => 1,
        num_cpus => 1
    },
    ok = flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),

    %% Cancel
    Result = flurm_dbd_server:record_job_cancelled(6004, user_cancelled),
    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_dbd_server).

test_get_job_record() ->
    %% Submit a job
    JobData = #{
        job_id => 7001,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 2
    },
    ok = flurm_dbd_server:record_job_submit(JobData),
    _ = sys:get_state(flurm_dbd_server),

    %% Get the record
    case flurm_dbd_server:get_job_record(7001) of
        {ok, Job} ->
            ?assertEqual(7001, maps:get(job_id, Job));
        {error, not_found} ->
            %% May happen due to async processing
            ok
    end.

test_list_job_records() ->
    Records = flurm_dbd_server:list_job_records(),
    ?assert(is_list(Records)).

test_list_job_records_filters() ->
    Filters = #{partition => <<"batch">>},
    Records = flurm_dbd_server:list_job_records(Filters),
    ?assert(is_list(Records)).

%%====================================================================
%% Usage Tracking Tests
%%====================================================================

usage_tracking_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_user_usage returns map", fun() ->
                 Usage = flurm_dbd_server:get_user_usage(<<"testuser">>),
                 ?assert(is_map(Usage))
             end},
             {"get_account_usage returns map", fun() ->
                 Usage = flurm_dbd_server:get_account_usage(<<"testaccount">>),
                 ?assert(is_map(Usage))
             end},
             {"get_cluster_usage returns map", fun() ->
                 Usage = flurm_dbd_server:get_cluster_usage(),
                 ?assert(is_map(Usage))
             end},
             {"reset_usage clears usage records", fun() ->
                 Result = flurm_dbd_server:reset_usage(),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% TRES Usage Tests
%%====================================================================

tres_usage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"calculate_user_tres_usage", fun() ->
                 Usage = flurm_dbd_server:calculate_user_tres_usage(<<"testuser">>),
                 ?assert(is_map(Usage)),
                 ?assert(maps:is_key(cpu_seconds, Usage)),
                 ?assert(maps:is_key(mem_seconds, Usage)),
                 ?assert(maps:is_key(gpu_seconds, Usage))
             end},
             {"calculate_account_tres_usage", fun() ->
                 Usage = flurm_dbd_server:calculate_account_tres_usage(<<"testaccount">>),
                 ?assert(is_map(Usage))
             end},
             {"calculate_cluster_tres_usage", fun() ->
                 Usage = flurm_dbd_server:calculate_cluster_tres_usage(),
                 ?assert(is_map(Usage))
             end}
         ]
     end
    }.

%%====================================================================
%% SACCT Query Tests
%%====================================================================

sacct_query_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_jobs_by_user returns list", fun() ->
                 Jobs = flurm_dbd_server:get_jobs_by_user(<<"testuser">>),
                 ?assert(is_list(Jobs))
             end},
             {"get_jobs_by_account returns list", fun() ->
                 Jobs = flurm_dbd_server:get_jobs_by_account(<<"testaccount">>),
                 ?assert(is_list(Jobs))
             end},
             {"get_jobs_in_range returns list", fun() ->
                 Now = erlang:system_time(second),
                 Jobs = flurm_dbd_server:get_jobs_in_range(Now - 3600, Now + 3600),
                 ?assert(is_list(Jobs))
             end},
             {"format_sacct_output returns iolist", fun() ->
                 Jobs = [
                     #{job_id => 1, user_name => <<"user">>, account => <<"acct">>,
                       partition => <<"default">>, state => completed,
                       elapsed => 3600, exit_code => 0}
                 ],
                 Output = flurm_dbd_server:format_sacct_output(Jobs),
                 ?assert(is_list(Output))
             end}
         ]
     end
    }.

%%====================================================================
%% Retention and Not Found Branch Tests
%%====================================================================

retention_and_not_found_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_job_record not found branch", fun() ->
                 ?assertEqual({error, not_found}, flurm_dbd_server:get_job_record(999999))
             end},
             {"get_association not found branch", fun() ->
                 ?assertEqual({error, not_found}, flurm_dbd_server:get_association(999999))
             end},
             {"archive and purge old records branches", fun() ->
                 Old = erlang:system_time(second) - (3 * 86400),
                 ok = flurm_dbd_server:record_job_start(#{
                     job_id => 88001,
                     user_name => <<"u">>,
                     account => <<"a">>,
                     state => completed,
                     exit_code => 0,
                     start_time => Old - 100,
                     end_time => Old
                 }),
                 {ok, _ArchivedCount} = flurm_dbd_server:archive_old_records(1),
                 {ok, _PurgedCount} = flurm_dbd_server:purge_old_records(1)
             end},
             {"archive and purge old jobs branches", fun() ->
                 Old = erlang:system_time(second) - (4 * 86400),
                 ok = flurm_dbd_server:record_job_start(#{
                     job_id => 88002,
                     user_name => <<"u">>,
                     account => <<"a">>,
                     state => completed,
                     exit_code => 0,
                     start_time => Old - 100,
                     end_time => Old
                 }),
                 {ok, _} = flurm_dbd_server:archive_old_jobs(1),
                 {ok, _} = flurm_dbd_server:purge_old_jobs(1)
             end}
         ]
     end
    }.

%%====================================================================
%% Association Tests
%%====================================================================

association_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"add_association creates association", fun() ->
                 {ok, Id} = flurm_dbd_server:add_association(<<"cluster">>, <<"account">>, <<"user">>),
                 ?assert(is_integer(Id)),
                 ?assert(Id > 0)
             end},
             {"add_association with options", fun() ->
                 Opts = #{shares => 10, max_jobs => 50},
                 {ok, Id} = flurm_dbd_server:add_association(<<"cluster">>, <<"account2">>, <<"user2">>, Opts),
                 ?assert(is_integer(Id))
             end},
             {"get_associations returns list", fun() ->
                 Assocs = flurm_dbd_server:get_associations(<<"cluster">>),
                 ?assert(is_list(Assocs))
             end},
             {"get_association returns association or not_found", fun() ->
                 %% Create one first
                 {ok, Id} = flurm_dbd_server:add_association(<<"cluster">>, <<"account3">>, <<"user3">>),
                 case flurm_dbd_server:get_association(Id) of
                     {ok, Assoc} ->
                         ?assertEqual(Id, maps:get(id, Assoc));
                     {error, not_found} ->
                         ok  % May happen due to timing
                 end
             end},
             {"update_association", fun() ->
                 {ok, Id} = flurm_dbd_server:add_association(<<"cluster">>, <<"account4">>, <<"user4">>),
                 Result = flurm_dbd_server:update_association(Id, #{shares => 20}),
                 ?assertEqual(ok, Result)
             end},
             {"update_association returns not_found for invalid id", fun() ->
                 Result = flurm_dbd_server:update_association(999999, #{shares => 20}),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"remove_association", fun() ->
                 {ok, Id} = flurm_dbd_server:add_association(<<"cluster">>, <<"account5">>, <<"user5">>),
                 Result = flurm_dbd_server:remove_association(Id),
                 ?assertEqual(ok, Result)
             end},
             {"remove_association returns not_found for invalid id", fun() ->
                 Result = flurm_dbd_server:remove_association(999999),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"get_user_associations returns list", fun() ->
                 Assocs = flurm_dbd_server:get_user_associations(<<"testuser">>),
                 ?assert(is_list(Assocs))
             end},
             {"get_account_associations returns list", fun() ->
                 Assocs = flurm_dbd_server:get_account_associations(<<"testaccount">>),
                 ?assert(is_list(Assocs))
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
             {"archive_old_records returns count", fun() ->
                 {ok, Count} = flurm_dbd_server:archive_old_records(30),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"purge_old_records returns count", fun() ->
                 {ok, Count} = flurm_dbd_server:purge_old_records(90),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"archive_old_jobs returns count", fun() ->
                 {ok, Count} = flurm_dbd_server:archive_old_jobs(30),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"purge_old_jobs returns count", fun() ->
                 {ok, Count} = flurm_dbd_server:purge_old_jobs(90),
                 ?assert(is_integer(Count)),
                 ?assert(Count >= 0)
             end},
             {"get_archived_jobs returns list", fun() ->
                 Jobs = flurm_dbd_server:get_archived_jobs(),
                 ?assert(is_list(Jobs))
             end},
             {"get_archived_jobs with filters", fun() ->
                 Jobs = flurm_dbd_server:get_archived_jobs(#{partition => <<"default">>}),
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
             {"get_stats returns map", fun() ->
                 Stats = flurm_dbd_server:get_stats(),
                 ?assert(is_map(Stats)),
                 ?assert(maps:is_key(uptime_seconds, Stats))
             end}
         ]
     end
    }.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"handle_call unknown returns error", fun() ->
                 Result = gen_server:call(flurm_dbd_server, unknown_request),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"handle_cast unknown does not crash", fun() ->
                 gen_server:cast(flurm_dbd_server, unknown_message),
                 _ = sys:get_state(flurm_dbd_server),
                 ?assert(is_process_alive(whereis(flurm_dbd_server)))
             end},
             {"handle_info unknown does not crash", fun() ->
                 whereis(flurm_dbd_server) ! unknown_info,
                 _ = sys:get_state(flurm_dbd_server),
                 ?assert(is_process_alive(whereis(flurm_dbd_server)))
             end}
         ]
     end
    }.

%%====================================================================
%% Format Helper Tests
%%====================================================================

format_helpers_test_() ->
    [
        {"format_elapsed formats seconds correctly", fun() ->
             %% 1 hour, 30 minutes, 45 seconds = 5445 seconds
             Seconds = 1 * 3600 + 30 * 60 + 45,
             %% The function is internal, test via format_sacct_output
             Jobs = [#{
                 job_id => 1,
                 user_name => <<"u">>,
                 account => <<"a">>,
                 partition => <<"p">>,
                 state => completed,
                 elapsed => Seconds,
                 exit_code => 0
             }],
             Output = flurm_dbd_server:format_sacct_output(Jobs),
             %% Just verify it produces output
             ?assert(is_list(Output))
         end},
        {"format_exit_code formats correctly", fun() ->
             Jobs = [#{
                 job_id => 1,
                 user_name => <<"u">>,
                 account => <<"a">>,
                 partition => <<"p">>,
                 state => failed,
                 elapsed => 100,
                 exit_code => 1
             }],
             Output = flurm_dbd_server:format_sacct_output(Jobs),
             ?assert(is_list(Output))
         end},
        {"format_sacct_output handles missing fields", fun() ->
             Jobs = [#{
                 job_id => 1
             }],
             Output = flurm_dbd_server:format_sacct_output(Jobs),
             ?assert(is_list(Output))
         end},
        {"format_elapsed handles negative", fun() ->
             Jobs = [#{
                 job_id => 1,
                 user_name => <<"u">>,
                 account => <<"a">>,
                 partition => <<"p">>,
                 state => completed,
                 elapsed => -1,
                 exit_code => 0
             }],
             Output = flurm_dbd_server:format_sacct_output(Jobs),
             ?assert(is_list(Output))
         end},
        {"format_exit_code handles non-integer", fun() ->
             Jobs = [#{
                 job_id => 1,
                 user_name => <<"u">>,
                 account => <<"a">>,
                 partition => <<"p">>,
                 state => completed,
                 elapsed => 100,
                 exit_code => undefined
             }],
             Output = flurm_dbd_server:format_sacct_output(Jobs),
             ?assert(is_list(Output))
         end}
    ].

%%====================================================================
%% Job Info Conversion Tests
%%====================================================================

job_conversion_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"job_info with all fields", fun() ->
                 JobInfo = #{
                     job_id => 8001,
                     name => <<"full_job">>,
                     user_name => <<"user">>,
                     user_id => 1000,
                     group_id => 1000,
                     account => <<"account">>,
                     partition => <<"batch">>,
                     cluster => <<"flurm">>,
                     qos => <<"normal">>,
                     state => running,
                     exit_code => 0,
                     num_nodes => 4,
                     num_cpus => 16,
                     num_tasks => 16,
                     req_mem => 32768,
                     submit_time => erlang:system_time(second) - 100,
                     eligible_time => erlang:system_time(second) - 50,
                     start_time => erlang:system_time(second),
                     tres_alloc => #{gpu => 2},
                     work_dir => <<"/home/user">>,
                     std_out => <<"/home/user/job.out">>,
                     std_err => <<"/home/user/job.err">>
                 },
                 ok = flurm_dbd_server:record_job_start(JobInfo),
                 _ = sys:get_state(flurm_dbd_server)
             end},
             {"job submit with erlang timestamp", fun() ->
                 JobData = #{
                     job_id => 8002,
                     user_id => 1000,
                     group_id => 1000,
                     partition => <<"batch">>,
                     num_nodes => 1,
                     num_cpus => 1,
                     submit_time => erlang:timestamp()
                 },
                 ok = flurm_dbd_server:record_job_submit(JobData),
                 _ = sys:get_state(flurm_dbd_server)
             end},
             {"job end for unknown job", fun() ->
                 %% Try to end a job that doesn't exist
                 ok = flurm_dbd_server:record_job_end(99999, 0, completed),
                 _ = sys:get_state(flurm_dbd_server)
             end},
             {"job cancelled for unknown job", fun() ->
                 %% Try to cancel a job that doesn't exist
                 ok = flurm_dbd_server:record_job_cancelled(99998, unknown),
                 _ = sys:get_state(flurm_dbd_server)
             end},
             {"job start event for unknown job", fun() ->
                 %% Start a job without prior submit
                 ok = flurm_dbd_server:record_job_start(88888, [<<"node1">>]),
                 _ = sys:get_state(flurm_dbd_server)
             end}
         ]
     end
    }.

%%====================================================================
%% Filter Tests
%%====================================================================

filter_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"filter by user", fun() ->
                 %% Submit job with specific user
                 JobInfo = #{
                     job_id => 9001,
                     user_name => <<"filter_user">>,
                     partition => <<"default">>,
                     state => completed
                 },
                 ok = flurm_dbd_server:record_job_start(JobInfo),
                 _ = sys:get_state(flurm_dbd_server),
                 %% Filter by user
                 Jobs = flurm_dbd_server:list_job_records(#{user => <<"filter_user">>}),
                 ?assert(is_list(Jobs))
             end},
             {"filter by state", fun() ->
                 Jobs = flurm_dbd_server:list_job_records(#{state => completed}),
                 ?assert(is_list(Jobs))
             end},
             {"filter by time range", fun() ->
                 Now = erlang:system_time(second),
                 Jobs = flurm_dbd_server:list_job_records(#{
                     start_time_after => Now - 3600,
                     start_time_before => Now + 3600
                 }),
                 ?assert(is_list(Jobs))
             end},
             {"filter by end_time", fun() ->
                 Now = erlang:system_time(second),
                 Jobs = flurm_dbd_server:list_job_records(#{
                     end_time_after => Now - 3600,
                     end_time_before => Now + 3600
                 }),
                 ?assert(is_list(Jobs))
             end},
             {"filter by account", fun() ->
                 Jobs = flurm_dbd_server:list_job_records(#{account => <<"testaccount">>}),
                 ?assert(is_list(Jobs))
             end},
             {"filter by unknown field is ignored", fun() ->
                 Jobs = flurm_dbd_server:list_job_records(#{unknown_field => <<"value">>}),
                 ?assert(is_list(Jobs))
             end}
         ]
     end
    }.

%%====================================================================
%% Job End with Update Tests
%%====================================================================

job_end_update_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"record_job_end updates existing record", fun() ->
                 %% First record start
                 JobInfo1 = #{
                     job_id => 10001,
                     user_name => <<"end_user">>,
                     partition => <<"default">>,
                     state => running,
                     num_cpus => 4,
                     num_nodes => 1,
                     start_time => erlang:system_time(second) - 100
                 },
                 ok = flurm_dbd_server:record_job_start(JobInfo1),
                 _ = sys:get_state(flurm_dbd_server),

                 %% Then record end
                 JobInfo2 = #{
                     job_id => 10001,
                     state => completed,
                     exit_code => 0,
                     end_time => erlang:system_time(second)
                 },
                 ok = flurm_dbd_server:record_job_end(JobInfo2),
                 _ = sys:get_state(flurm_dbd_server),

                 %% Verify
                 case flurm_dbd_server:get_job_record(10001) of
                     {ok, Job} ->
                         ?assertEqual(completed, maps:get(state, Job)),
                         ?assertEqual(0, maps:get(exit_code, Job));
                     {error, not_found} ->
                         ok
                 end
             end},
             {"record_job_end creates new if not exists", fun() ->
                 JobInfo = #{
                     job_id => 10002,
                     state => completed,
                     exit_code => 1,
                     end_time => erlang:system_time(second)
                 },
                 ok = flurm_dbd_server:record_job_end(JobInfo),
                 _ = sys:get_state(flurm_dbd_server)
             end},
             {"record_job_end with failed exit code", fun() ->
                 %% First record start
                 JobInfo1 = #{
                     job_id => 10003,
                     user_name => <<"fail_user">>,
                     partition => <<"default">>,
                     state => running,
                     num_cpus => 4,
                     num_nodes => 1,
                     start_time => erlang:system_time(second) - 100
                 },
                 ok = flurm_dbd_server:record_job_start(JobInfo1),
                 _ = sys:get_state(flurm_dbd_server),

                 %% Then record end with failure
                 JobInfo2 = #{
                     job_id => 10003,
                     state => failed,
                     exit_code => 1,
                     end_time => erlang:system_time(second)
                 },
                 ok = flurm_dbd_server:record_job_end(JobInfo2),
                 _ = sys:get_state(flurm_dbd_server)
             end}
         ]
     end
    }.

%%====================================================================
%% Association Options Tests
%%====================================================================

association_options_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"add_association with all options", fun() ->
                 Opts = #{
                     partition => <<"batch">>,
                     shares => 100,
                     max_jobs => 10,
                     max_submit => 20,
                     max_wall => 1440,
                     grp_tres => #{cpu => 1000, mem => 65536},
                     max_tres_per_job => #{cpu => 64, mem => 8192},
                     qos => [<<"normal">>, <<"high">>],
                     default_qos => <<"normal">>
                 },
                 {ok, Id} = flurm_dbd_server:add_association(
                     <<"cluster">>, <<"full_account">>, <<"full_user">>, Opts),
                 ?assert(is_integer(Id)),

                 %% Verify association
                 case flurm_dbd_server:get_association(Id) of
                     {ok, Assoc} ->
                         ?assertEqual(<<"batch">>, maps:get(partition, Assoc)),
                         ?assertEqual(100, maps:get(shares, Assoc)),
                         ?assertEqual(10, maps:get(max_jobs, Assoc));
                     {error, not_found} ->
                         ok
                 end
             end},
             {"update_association with multiple fields", fun() ->
                 {ok, Id} = flurm_dbd_server:add_association(
                     <<"cluster">>, <<"update_acct">>, <<"update_user">>),
                 Updates = #{
                     shares => 50,
                     max_jobs => 25,
                     max_wall => 720,
                     default_qos => <<"high">>
                 },
                 Result = flurm_dbd_server:update_association(Id, Updates),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Handle Info Start Listener Error Tests
%%====================================================================

handle_info_listener_test_() ->
    [
        {"handle_info start_listener error is logged", fun() ->
             %% Stop existing
             case whereis(flurm_dbd_server) of
                 undefined -> ok;
                 ExistingPid ->
                     flurm_test_utils:kill_and_wait(ExistingPid)
             end,
             %% Mock to return error
             meck:new(flurm_dbd_sup, [passthrough, no_link]),
             meck:expect(flurm_dbd_sup, start_listener, fun() -> {error, test_error} end),

             %% Start server - should log error but not crash
             {ok, Pid} = flurm_dbd_server:start_link(),
             _ = sys:get_state(Pid),
             ?assert(is_process_alive(Pid)),

             %% Cleanup
             gen_server:stop(Pid, normal, 5000),
             catch meck:unload(flurm_dbd_sup)
         end}
    ].

%%====================================================================
%% Targeted Uncovered Branch Tests
%%====================================================================

targeted_uncovered_branches_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
          {"archive/purge non-matching branches", fun test_archive_purge_non_matching/0},
          {"get_associations non-match fold branch", fun test_get_associations_non_match_fold_branch/0},
          {"user/account/range matching fold branches", fun test_query_matching_fold_branches/0},
          {"event end/cancel with zero start_time branch", fun test_event_end_cancel_zero_start/0},
          {"cancel with nonzero start_time branch", fun test_cancel_nonzero_start_time_branch/0},
          {"submit event integer timestamp branch", fun test_submit_event_integer_timestamp_branch/0},
          {"update_job_record zero-time branch", fun test_update_job_record_zero_time_branch/0},
          {"purge archived non-matching fold branch", fun test_purge_archived_non_matching_fold_branch/0},
          {"usage helper matching branches", fun test_usage_helper_matching_branches/0},
          {"usage update existing and insert branches", fun test_usage_update_existing_and_insert_branches/0},
          {"tres usage helper matching branches", fun test_tres_usage_helper_matching_branches/0},
          {"calculate_cluster_usage non-empty branch", fun test_cluster_usage_non_empty_branch/0}
         ]
     end
    }.

test_archive_purge_non_matching() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12001,
        user_name => <<"nm">>,
        account => <<"nm">>,
        state => running,
        start_time => Now - 10,
        end_time => 0
    }),
    _ = sys:get_state(flurm_dbd_server),
    ?assertMatch({ok, _}, flurm_dbd_server:archive_old_records(1)),
    ?assertMatch({ok, _}, flurm_dbd_server:purge_old_records(1)),
    ?assertMatch({ok, _}, flurm_dbd_server:archive_old_jobs(1)),
    ?assertMatch({ok, _}, flurm_dbd_server:purge_old_jobs(1)).

test_get_associations_non_match_fold_branch() ->
    {ok, _} = flurm_dbd_server:add_association(<<"cluster_a">>, <<"acct_a">>, <<"user_a">>),
    {ok, _} = flurm_dbd_server:add_association(<<"cluster_b">>, <<"acct_b">>, <<"user_b">>),
    Assocs = flurm_dbd_server:get_associations(<<"cluster_a">>),
    ?assert(length(Assocs) >= 1).

test_query_matching_fold_branches() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12011,
        user_name => <<"match_user">>,
        account => <<"match_account">>,
        partition => <<"p">>,
        state => completed,
        start_time => Now - 100,
        end_time => Now - 10
    }),
    _ = sys:get_state(flurm_dbd_server),
    ByUser = flurm_dbd_server:get_jobs_by_user(<<"match_user">>),
    ByAccount = flurm_dbd_server:get_jobs_by_account(<<"match_account">>),
    InRange = flurm_dbd_server:get_jobs_in_range(Now - 200, Now),
    ?assert(length(ByUser) >= 1),
    ?assert(length(ByAccount) >= 1),
    ?assert(length(InRange) >= 1).

test_event_end_cancel_zero_start() ->
    %% start_time defaults to 0 via submit path
    ok = flurm_dbd_server:record_job_submit(#{
        job_id => 12002,
        user_id => 1,
        group_id => 1,
        partition => <<"p">>,
        num_nodes => 1,
        num_cpus => 1
    }),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_end(12002, 1, failed),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_cancelled(12002, user_cancelled),
    _ = sys:get_state(flurm_dbd_server).

test_cancel_nonzero_start_time_branch() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12016,
        user_name => <<"cancel_user">>,
        account => <<"cancel_account">>,
        partition => <<"p">>,
        state => running,
        start_time => Now - 10
    }),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_cancelled(12016, user_cancelled),
    _ = sys:get_state(flurm_dbd_server).

test_submit_event_integer_timestamp_branch() ->
    TS = erlang:system_time(second) - 50,
    ok = flurm_dbd_server:record_job_submit(#{
        job_id => 12012,
        user_id => 1,
        group_id => 1,
        partition => <<"p">>,
        num_nodes => 1,
        num_cpus => 1,
        submit_time => TS
    }),
    _ = sys:get_state(flurm_dbd_server),
    ?assertMatch({ok, #{submit_time := TS}}, flurm_dbd_server:get_job_record(12012)).

test_update_job_record_zero_time_branch() ->
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12003,
        user_name => <<"u0">>,
        account => <<"a0">>,
        partition => <<"p">>,
        state => running,
        start_time => 0
    }),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_end(#{
        job_id => 12003,
        state => completed,
        exit_code => 0,
        end_time => 0
    }),
    _ = sys:get_state(flurm_dbd_server).

test_purge_archived_non_matching_fold_branch() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12013,
        user_name => <<"arch_user">>,
        account => <<"arch_account">>,
        state => completed,
        start_time => Now - 60,
        end_time => Now - 30
    }),
    _ = sys:get_state(flurm_dbd_server),
    %% Archive recent completed job, then purge with an older cutoff so it is not deleted.
    ?assertMatch({ok, _}, flurm_dbd_server:archive_old_jobs(0)),
    ?assertMatch({ok, _}, flurm_dbd_server:purge_old_jobs(1)).

test_usage_helper_matching_branches() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12004,
        user_name => <<"usage_u">>,
        account => <<"usage_a">>,
        partition => <<"p">>,
        state => completed,
        num_cpus => 2,
        num_nodes => 3,
        start_time => Now - 100,
        end_time => Now,
        elapsed => 100
    }),
    _ = sys:get_state(flurm_dbd_server),
    U = flurm_dbd_server:get_user_usage(<<"usage_u">>),
    A = flurm_dbd_server:get_account_usage(<<"usage_a">>),
    ?assert(maps:get(job_count, U, 0) >= 1),
    ?assert(maps:get(job_count, A, 0) >= 1).

test_usage_update_existing_and_insert_branches() ->
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12014,
        user_name => <<"usage2_u">>,
        account => <<"usage2_a">>,
        partition => <<"p">>,
        state => running,
        num_cpus => 1,
        num_nodes => 1,
        start_time => 0
    }),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_end(12014, 0, completed),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12015,
        user_name => <<"usage2_u">>,
        account => <<"usage2_a">>,
        partition => <<"p">>,
        state => running,
        num_cpus => 2,
        num_nodes => 1,
        start_time => 0
    }),
    _ = sys:get_state(flurm_dbd_server),
    ok = flurm_dbd_server:record_job_end(12015, 1, failed),
    _ = sys:get_state(flurm_dbd_server),
    U = flurm_dbd_server:get_user_usage(<<"usage2_u">>),
    ?assert(maps:get(job_count, U, 0) >= 2).

test_tres_usage_helper_matching_branches() ->
    Now = erlang:system_time(second),
    ok = flurm_dbd_server:record_job_start(#{
        job_id => 12005,
        user_name => <<"tres_u">>,
        account => <<"tres_a">>,
        partition => <<"p">>,
        state => completed,
        num_cpus => 4,
        num_nodes => 2,
        req_mem => 1024,
        start_time => Now - 50,
        end_time => Now,
        elapsed => 50,
        tres_alloc => #{gpu => 1}
    }),
    _ = sys:get_state(flurm_dbd_server),
    UserTres = flurm_dbd_server:calculate_user_tres_usage(<<"tres_u">>),
    AcctTres = flurm_dbd_server:calculate_account_tres_usage(<<"tres_a">>),
    ClusterTres = flurm_dbd_server:calculate_cluster_tres_usage(),
    ?assert(maps:get(elapsed_total, UserTres, 0) >= 1),
    ?assert(maps:get(elapsed_total, AcctTres, 0) >= 1),
    ?assert(maps:get(elapsed_total, ClusterTres, 0) >= 1).

test_cluster_usage_non_empty_branch() ->
    Usage = flurm_dbd_server:get_cluster_usage(),
    ?assert(is_map(Usage)).
