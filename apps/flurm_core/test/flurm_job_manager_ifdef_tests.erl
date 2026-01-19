%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_manager internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure internal functions for job creation,
%%% update application, and limit check spec building without
%%% requiring a running gen_server.
%%%
%%% Note: flurm_job_manager is in flurm_controller app, but tests are
%%% in flurm_core/test for organization purposes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% create_job/2 Tests
%%====================================================================

create_job_test_() ->
    {"create_job/2 tests", [
        {"creates job with all specified fields",
         fun() ->
             JobId = 100,
             JobSpec = #{
                 name => <<"test_job">>,
                 user => <<"testuser">>,
                 partition => <<"compute">>,
                 script => <<"#!/bin/bash\necho hello">>,
                 num_nodes => 4,
                 num_cpus => 16,
                 memory_mb => 8192,
                 time_limit => 7200,
                 priority => 500,
                 work_dir => <<"/home/testuser/jobs">>,
                 std_out => <<"/home/testuser/jobs/output.log">>,
                 std_err => <<"/home/testuser/jobs/error.log">>,
                 account => <<"research">>,
                 qos => <<"high">>
             },
             Job = flurm_job_manager:create_job(JobId, JobSpec),

             ?assertEqual(100, Job#job.id),
             ?assertEqual(<<"test_job">>, Job#job.name),
             ?assertEqual(<<"testuser">>, Job#job.user),
             ?assertEqual(<<"compute">>, Job#job.partition),
             ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
             ?assertEqual(4, Job#job.num_nodes),
             ?assertEqual(16, Job#job.num_cpus),
             ?assertEqual(8192, Job#job.memory_mb),
             ?assertEqual(7200, Job#job.time_limit),
             ?assertEqual(500, Job#job.priority),
             ?assertEqual(<<"/home/testuser/jobs">>, Job#job.work_dir),
             ?assertEqual(<<"/home/testuser/jobs/output.log">>, Job#job.std_out),
             ?assertEqual(<<"/home/testuser/jobs/error.log">>, Job#job.std_err),
             ?assertEqual(<<"research">>, Job#job.account),
             ?assertEqual(<<"high">>, Job#job.qos),
             ?assertEqual(pending, Job#job.state),
             ?assertEqual([], Job#job.allocated_nodes),
             ?assertEqual(undefined, Job#job.start_time),
             ?assertEqual(undefined, Job#job.end_time),
             ?assertEqual(undefined, Job#job.exit_code)
         end},

        {"creates job with default values",
         fun() ->
             JobId = 200,
             JobSpec = #{},
             Job = flurm_job_manager:create_job(JobId, JobSpec),

             ?assertEqual(200, Job#job.id),
             ?assertEqual(<<"unnamed">>, Job#job.name),
             ?assertEqual(<<"unknown">>, Job#job.user),
             ?assertEqual(<<"default">>, Job#job.partition),
             ?assertEqual(<<>>, Job#job.script),
             ?assertEqual(1, Job#job.num_nodes),
             ?assertEqual(1, Job#job.num_cpus),
             ?assertEqual(1024, Job#job.memory_mb),
             ?assertEqual(3600, Job#job.time_limit),
             ?assertEqual(100, Job#job.priority),
             ?assertEqual(<<"/tmp">>, Job#job.work_dir),
             ?assertEqual(<<>>, Job#job.std_err),
             ?assertEqual(<<>>, Job#job.account),
             ?assertEqual(<<"normal">>, Job#job.qos),
             ?assertEqual(pending, Job#job.state)
         end},

        {"generates default stdout path from work_dir and job_id",
         fun() ->
             JobId = 12345,
             JobSpec = #{work_dir => <<"/home/user/work">>},
             Job = flurm_job_manager:create_job(JobId, JobSpec),

             %% Default stdout should be work_dir/slurm-<jobid>.out
             ?assertEqual(<<"/home/user/work/slurm-12345.out">>, Job#job.std_out)
         end},

        {"uses provided stdout over default",
         fun() ->
             JobId = 100,
             JobSpec = #{
                 work_dir => <<"/home/user/work">>,
                 std_out => <<"/custom/path/output.txt">>
             },
             Job = flurm_job_manager:create_job(JobId, JobSpec),

             ?assertEqual(<<"/custom/path/output.txt">>, Job#job.std_out)
         end},

        {"sets submit_time to current time",
         fun() ->
             Before = erlang:system_time(second),
             Job = flurm_job_manager:create_job(1, #{}),
             After = erlang:system_time(second),

             ?assert(Job#job.submit_time >= Before),
             ?assert(Job#job.submit_time =< After)
         end},

        {"handles licenses in job spec",
         fun() ->
             JobSpec = #{licenses => [{<<"matlab">>, 1}, {<<"simulink">>, 2}]},
             Job = flurm_job_manager:create_job(1, JobSpec),
             ?assertEqual([{<<"matlab">>, 1}, {<<"simulink">>, 2}], Job#job.licenses)
         end},

        {"defaults licenses to empty list",
         fun() ->
             Job = flurm_job_manager:create_job(1, #{}),
             ?assertEqual([], Job#job.licenses)
         end}
    ]}.

%%====================================================================
%% apply_job_updates/2 Tests
%%====================================================================

make_test_job() ->
    make_test_job(#{}).

make_test_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test">>,
        user => <<"user">>,
        partition => <<"default">>,
        state => pending,
        script => <<>>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        submit_time => 1704067200,
        start_time => undefined,
        end_time => undefined,
        allocated_nodes => [],
        exit_code => undefined,
        work_dir => <<"/tmp">>,
        std_out => <<>>,
        std_err => <<>>,
        account => <<>>,
        qos => <<"normal">>,
        licenses => []
    },
    Merged = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Merged),
        name = maps:get(name, Merged),
        user = maps:get(user, Merged),
        partition = maps:get(partition, Merged),
        state = maps:get(state, Merged),
        script = maps:get(script, Merged),
        num_nodes = maps:get(num_nodes, Merged),
        num_cpus = maps:get(num_cpus, Merged),
        memory_mb = maps:get(memory_mb, Merged),
        time_limit = maps:get(time_limit, Merged),
        priority = maps:get(priority, Merged),
        submit_time = maps:get(submit_time, Merged),
        start_time = maps:get(start_time, Merged),
        end_time = maps:get(end_time, Merged),
        allocated_nodes = maps:get(allocated_nodes, Merged),
        exit_code = maps:get(exit_code, Merged),
        work_dir = maps:get(work_dir, Merged),
        std_out = maps:get(std_out, Merged),
        std_err = maps:get(std_err, Merged),
        account = maps:get(account, Merged),
        qos = maps:get(qos, Merged),
        licenses = maps:get(licenses, Merged)
    }.

apply_job_updates_test_() ->
    {"apply_job_updates/2 tests", [
        {"updates state field",
         fun() ->
             Job = make_test_job(),
             Updated = flurm_job_manager:apply_job_updates(Job, #{state => running}),
             ?assertEqual(running, Updated#job.state)
         end},

        {"updates allocated_nodes field",
         fun() ->
             Job = make_test_job(),
             Nodes = [<<"node1">>, <<"node2">>],
             Updated = flurm_job_manager:apply_job_updates(Job, #{allocated_nodes => Nodes}),
             ?assertEqual(Nodes, Updated#job.allocated_nodes)
         end},

        {"updates start_time field",
         fun() ->
             Job = make_test_job(),
             Now = erlang:system_time(second),
             Updated = flurm_job_manager:apply_job_updates(Job, #{start_time => Now}),
             ?assertEqual(Now, Updated#job.start_time)
         end},

        {"updates end_time field",
         fun() ->
             Job = make_test_job(),
             Now = erlang:system_time(second),
             Updated = flurm_job_manager:apply_job_updates(Job, #{end_time => Now}),
             ?assertEqual(Now, Updated#job.end_time)
         end},

        {"updates exit_code field",
         fun() ->
             Job = make_test_job(),
             Updated = flurm_job_manager:apply_job_updates(Job, #{exit_code => 0}),
             ?assertEqual(0, Updated#job.exit_code)
         end},

        {"updates priority field",
         fun() ->
             Job = make_test_job(),
             Updated = flurm_job_manager:apply_job_updates(Job, #{priority => 500}),
             ?assertEqual(500, Updated#job.priority)
         end},

        {"updates time_limit field",
         fun() ->
             Job = make_test_job(),
             Updated = flurm_job_manager:apply_job_updates(Job, #{time_limit => 7200}),
             ?assertEqual(7200, Updated#job.time_limit)
         end},

        {"updates multiple fields at once",
         fun() ->
             Job = make_test_job(),
             Now = erlang:system_time(second),
             Updates = #{
                 state => running,
                 allocated_nodes => [<<"node1">>],
                 start_time => Now
             },
             Updated = flurm_job_manager:apply_job_updates(Job, Updates),
             ?assertEqual(running, Updated#job.state),
             ?assertEqual([<<"node1">>], Updated#job.allocated_nodes),
             ?assertEqual(Now, Updated#job.start_time)
         end},

        {"ignores unknown fields",
         fun() ->
             Job = make_test_job(#{name => <<"original">>}),
             Updates = #{
                 state => running,
                 unknown_field => <<"ignored">>,
                 another_unknown => 12345
             },
             Updated = flurm_job_manager:apply_job_updates(Job, Updates),
             ?assertEqual(running, Updated#job.state),
             ?assertEqual(<<"original">>, Updated#job.name)
         end},

        {"empty updates returns job unchanged",
         fun() ->
             Job = make_test_job(#{state => pending, priority => 100}),
             Updated = flurm_job_manager:apply_job_updates(Job, #{}),
             ?assertEqual(pending, Updated#job.state),
             ?assertEqual(100, Updated#job.priority)
         end},

        {"preserves unupdated fields",
         fun() ->
             Job = make_test_job(#{
                 id => 999,
                 name => <<"preserved">>,
                 user => <<"testuser">>
             }),
             Updated = flurm_job_manager:apply_job_updates(Job, #{state => completed}),
             ?assertEqual(999, Updated#job.id),
             ?assertEqual(<<"preserved">>, Updated#job.name),
             ?assertEqual(<<"testuser">>, Updated#job.user)
         end}
    ]}.

%%====================================================================
%% build_limit_check_spec/1 Tests
%%====================================================================

build_limit_check_spec_test_() ->
    {"build_limit_check_spec/1 tests", [
        {"builds spec with all fields from job spec",
         fun() ->
             JobSpec = #{
                 user => <<"testuser">>,
                 account => <<"research">>,
                 partition => <<"gpu">>,
                 num_nodes => 4,
                 num_cpus => 32,
                 memory_mb => 16384,
                 time_limit => 86400
             },
             LimitSpec = flurm_job_manager:build_limit_check_spec(JobSpec),

             ?assertEqual(<<"testuser">>, maps:get(user, LimitSpec)),
             ?assertEqual(<<"research">>, maps:get(account, LimitSpec)),
             ?assertEqual(<<"gpu">>, maps:get(partition, LimitSpec)),
             ?assertEqual(4, maps:get(num_nodes, LimitSpec)),
             ?assertEqual(32, maps:get(num_cpus, LimitSpec)),
             ?assertEqual(16384, maps:get(memory_mb, LimitSpec)),
             ?assertEqual(86400, maps:get(time_limit, LimitSpec))
         end},

        {"uses default values for missing fields",
         fun() ->
             JobSpec = #{},
             LimitSpec = flurm_job_manager:build_limit_check_spec(JobSpec),

             ?assertEqual(<<"unknown">>, maps:get(user, LimitSpec)),
             ?assertEqual(<<>>, maps:get(account, LimitSpec)),
             ?assertEqual(<<"default">>, maps:get(partition, LimitSpec)),
             ?assertEqual(1, maps:get(num_nodes, LimitSpec)),
             ?assertEqual(1, maps:get(num_cpus, LimitSpec)),
             ?assertEqual(1024, maps:get(memory_mb, LimitSpec)),
             ?assertEqual(3600, maps:get(time_limit, LimitSpec))
         end},

        {"uses user_id fallback for user field",
         fun() ->
             JobSpec = #{user_id => <<"user_from_id">>},
             LimitSpec = flurm_job_manager:build_limit_check_spec(JobSpec),

             ?assertEqual(<<"user_from_id">>, maps:get(user, LimitSpec))
         end},

        {"prefers user over user_id",
         fun() ->
             JobSpec = #{
                 user => <<"explicit_user">>,
                 user_id => <<"user_from_id">>
             },
             LimitSpec = flurm_job_manager:build_limit_check_spec(JobSpec),

             ?assertEqual(<<"explicit_user">>, maps:get(user, LimitSpec))
         end},

        {"returns map with all required keys",
         fun() ->
             LimitSpec = flurm_job_manager:build_limit_check_spec(#{}),
             RequiredKeys = [user, account, partition, num_nodes, num_cpus, memory_mb, time_limit],

             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, LimitSpec),
                         io_lib:format("Missing key: ~p", [Key]))
             end, RequiredKeys)
         end}
    ]}.

%%====================================================================
%% Job Lifecycle State Transition Tests
%%====================================================================

job_state_transitions_test_() ->
    {"Job state transition tests", [
        {"pending to running transition",
         fun() ->
             Job = make_test_job(#{state => pending}),
             Now = erlang:system_time(second),
             Updated = flurm_job_manager:apply_job_updates(Job, #{
                 state => running,
                 start_time => Now,
                 allocated_nodes => [<<"node1">>]
             }),
             ?assertEqual(running, Updated#job.state),
             ?assertEqual(Now, Updated#job.start_time),
             ?assertEqual([<<"node1">>], Updated#job.allocated_nodes)
         end},

        {"running to completed transition",
         fun() ->
             StartTime = erlang:system_time(second) - 3600,
             Job = make_test_job(#{
                 state => running,
                 start_time => StartTime,
                 allocated_nodes => [<<"node1">>]
             }),
             EndTime = erlang:system_time(second),
             Updated = flurm_job_manager:apply_job_updates(Job, #{
                 state => completed,
                 end_time => EndTime,
                 exit_code => 0
             }),
             ?assertEqual(completed, Updated#job.state),
             ?assertEqual(EndTime, Updated#job.end_time),
             ?assertEqual(0, Updated#job.exit_code)
         end},

        {"running to failed transition",
         fun() ->
             Job = make_test_job(#{state => running}),
             Updated = flurm_job_manager:apply_job_updates(Job, #{
                 state => failed,
                 end_time => erlang:system_time(second),
                 exit_code => 1
             }),
             ?assertEqual(failed, Updated#job.state),
             ?assertEqual(1, Updated#job.exit_code)
         end},

        {"pending to cancelled transition",
         fun() ->
             Job = make_test_job(#{state => pending}),
             Updated = flurm_job_manager:apply_job_updates(Job, #{
                 state => cancelled,
                 end_time => erlang:system_time(second)
             }),
             ?assertEqual(cancelled, Updated#job.state)
         end},

        {"pending to held transition",
         fun() ->
             Job = make_test_job(#{state => pending}),
             Updated = flurm_job_manager:apply_job_updates(Job, #{state => held}),
             ?assertEqual(held, Updated#job.state)
         end}
    ]}.
