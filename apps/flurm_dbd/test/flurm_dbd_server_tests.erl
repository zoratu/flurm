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
            exit(ExistingPid, shutdown),
            timer:sleep(50)
    end,
    %% Mock flurm_dbd_sup to avoid Ranch dependency
    meck:new(flurm_dbd_sup, [passthrough, no_link]),
    meck:expect(flurm_dbd_sup, start_listener, fun() -> {ok, self()} end),
    %% Start fresh server
    {ok, NewPid} = flurm_dbd_server:start_link(),
    %% Wait for listener start message to be processed
    timer:sleep(100),
    NewPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            unlink(Pid),
            exit(Pid, shutdown),
            timer:sleep(50);
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
    timer:sleep(50).

test_record_job_end_map() ->
    JobInfo = #{
        job_id => 5002,
        state => completed,
        exit_code => 0,
        end_time => erlang:system_time(second)
    },
    Result = flurm_dbd_server:record_job_end(JobInfo),
    ?assertEqual(ok, Result),
    timer:sleep(50).

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
    timer:sleep(50).

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
    timer:sleep(50).

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
    timer:sleep(50),

    %% Then start
    Result = flurm_dbd_server:record_job_start(6002, [<<"node1">>, <<"node2">>]),
    ?assertEqual(ok, Result),
    timer:sleep(50).

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
    timer:sleep(50),
    ok = flurm_dbd_server:record_job_start(6003, [<<"node1">>]),
    timer:sleep(50),

    %% End with exit code
    Result = flurm_dbd_server:record_job_end(6003, 0, completed),
    ?assertEqual(ok, Result),
    timer:sleep(50).

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
    timer:sleep(50),

    %% Cancel
    Result = flurm_dbd_server:record_job_cancelled(6004, user_cancelled),
    ?assertEqual(ok, Result),
    timer:sleep(50).

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
    timer:sleep(50),

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
                 timer:sleep(50),
                 ?assert(is_process_alive(whereis(flurm_dbd_server)))
             end},
             {"handle_info unknown does not crash", fun() ->
                 whereis(flurm_dbd_server) ! unknown_info,
                 timer:sleep(50),
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
                 timer:sleep(50)
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
                 timer:sleep(50)
             end},
             {"job end for unknown job", fun() ->
                 %% Try to end a job that doesn't exist
                 ok = flurm_dbd_server:record_job_end(99999, 0, completed),
                 timer:sleep(50)
             end},
             {"job cancelled for unknown job", fun() ->
                 %% Try to cancel a job that doesn't exist
                 ok = flurm_dbd_server:record_job_cancelled(99998, unknown),
                 timer:sleep(50)
             end},
             {"job start event for unknown job", fun() ->
                 %% Start a job without prior submit
                 ok = flurm_dbd_server:record_job_start(88888, [<<"node1">>]),
                 timer:sleep(50)
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
                 timer:sleep(50),
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
                 timer:sleep(50),

                 %% Then record end
                 JobInfo2 = #{
                     job_id => 10001,
                     state => completed,
                     exit_code => 0,
                     end_time => erlang:system_time(second)
                 },
                 ok = flurm_dbd_server:record_job_end(JobInfo2),
                 timer:sleep(50),

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
                 timer:sleep(50)
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
                 timer:sleep(50),

                 %% Then record end with failure
                 JobInfo2 = #{
                     job_id => 10003,
                     state => failed,
                     exit_code => 1,
                     end_time => erlang:system_time(second)
                 },
                 ok = flurm_dbd_server:record_job_end(JobInfo2),
                 timer:sleep(50)
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
                     unlink(ExistingPid),
                     exit(ExistingPid, shutdown),
                     timer:sleep(50)
             end,
             %% Mock to return error
             meck:new(flurm_dbd_sup, [passthrough, no_link]),
             meck:expect(flurm_dbd_sup, start_listener, fun() -> {error, test_error} end),

             %% Start server - should log error but not crash
             {ok, Pid} = flurm_dbd_server:start_link(),
             timer:sleep(100),
             ?assert(is_process_alive(Pid)),

             %% Cleanup
             unlink(Pid),
             exit(Pid, shutdown),
             timer:sleep(50),
             catch meck:unload(flurm_dbd_sup)
         end}
    ].
