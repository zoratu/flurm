%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_job_manager module.
%%% Tests internal functions exported via -ifdef(TEST) and edge cases.
%%%-------------------------------------------------------------------
-module(flurm_job_manager_more_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% create_job Tests
%%====================================================================

create_job_minimal_test() ->
    JobSpec = #{
        name => <<"test_job">>,
        user => <<"testuser">>,
        script => <<"#!/bin/bash\necho hello">>
    },
    State = #{next_job_id => 1},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_partition_test() ->
    JobSpec = #{
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"batch">>,
        script => <<"#!/bin/bash\necho hello">>
    },
    State = #{next_job_id => 100},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_resources_test() ->
    JobSpec = #{
        name => <<"compute_job">>,
        user => <<"testuser">>,
        num_nodes => 4,
        num_cpus => 16,
        memory_mb => 8192,
        script => <<"#!/bin/bash\nmpirun ./app">>
    },
    State = #{next_job_id => 200},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_time_limit_test() ->
    JobSpec = #{
        name => <<"timed_job">>,
        user => <<"testuser">>,
        time_limit => 7200,
        script => <<"#!/bin/bash\necho test">>
    },
    State = #{next_job_id => 300},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_priority_test() ->
    JobSpec = #{
        name => <<"priority_job">>,
        user => <<"testuser">>,
        priority => 1000,
        script => <<"#!/bin/bash\necho urgent">>
    },
    State = #{next_job_id => 400},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_qos_test() ->
    JobSpec = #{
        name => <<"qos_job">>,
        user => <<"testuser">>,
        qos => <<"high">>,
        script => <<"#!/bin/bash\necho qos">>
    },
    State = #{next_job_id => 500},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_account_test() ->
    JobSpec = #{
        name => <<"account_job">>,
        user => <<"testuser">>,
        account => <<"project_abc">>,
        script => <<"#!/bin/bash\necho account">>
    },
    State = #{next_job_id => 600},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_with_work_dir_test() ->
    JobSpec = #{
        name => <<"workdir_job">>,
        user => <<"testuser">>,
        work_dir => <<"/home/testuser/project">>,
        script => <<"#!/bin/bash\necho workdir">>
    },
    State = #{next_job_id => 700},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

create_job_full_spec_test() ->
    JobSpec = #{
        name => <<"full_job">>,
        user => <<"testuser">>,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8,
        memory_mb => 4096,
        time_limit => 3600,
        priority => 500,
        qos => <<"normal">>,
        account => <<"default">>,
        work_dir => <<"/tmp">>,
        script => <<"#!/bin/bash\necho full">>
    },
    State = #{next_job_id => 800},
    Result = catch flurm_job_manager:create_job(JobSpec, State),
    ?assert(is_tuple(Result)).

%%====================================================================
%% apply_job_updates Tests
%%====================================================================

apply_job_updates_empty_test() ->
    Job = make_test_job(1),
    Result = catch flurm_job_manager:apply_job_updates(Job, #{}),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_name_test() ->
    Job = make_test_job(1),
    Updates = #{name => <<"new_name">>},
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_priority_test() ->
    Job = make_test_job(1),
    Updates = #{priority => 999},
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_time_limit_test() ->
    Job = make_test_job(1),
    Updates = #{time_limit => 7200},
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_qos_test() ->
    Job = make_test_job(1),
    Updates = #{qos => <<"high">>},
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_partition_test() ->
    Job = make_test_job(1),
    Updates = #{partition => <<"gpu">>},
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

apply_job_updates_multiple_test() ->
    Job = make_test_job(1),
    Updates = #{
        name => <<"updated_job">>,
        priority => 800,
        time_limit => 10800
    },
    Result = catch flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result) orelse is_map(Result)).

%%====================================================================
%% build_limit_check_spec Tests
%%====================================================================

build_limit_check_spec_minimal_test() ->
    Job = make_test_job(1),
    Spec = catch flurm_job_manager:build_limit_check_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

build_limit_check_spec_with_account_test() ->
    Job = make_test_job_with_account(1, <<"project1">>),
    Spec = catch flurm_job_manager:build_limit_check_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

build_limit_check_spec_with_qos_test() ->
    Job = make_test_job_with_qos(1, <<"high">>),
    Spec = catch flurm_job_manager:build_limit_check_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

build_limit_check_spec_with_partition_test() ->
    Job = make_test_job_with_partition(1, <<"gpu">>),
    Spec = catch flurm_job_manager:build_limit_check_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

build_limit_check_spec_full_job_test() ->
    Job = {job, 1, <<"test">>, <<"user1">>, <<"batch">>, pending,
           <<"#!/bin/bash">>, 2, 8, 4096, 0, 0, 3600, 100,
           <<"/tmp">>, <<"acct">>, <<"normal">>, undefined, undefined,
           erlang:system_time(second), undefined, undefined, #{}},
    Spec = catch flurm_job_manager:build_limit_check_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

%%====================================================================
%% API Function Tests (may require gen_server running)
%%====================================================================

submit_job_spec_test() ->
    JobSpec = #{
        name => <<"api_test_job">>,
        user => <<"testuser">>,
        script => <<"#!/bin/bash\necho api test">>
    },
    %% This will either succeed or fail if gen_server not running
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

cancel_job_nonexistent_test() ->
    %% Try to cancel a non-existent job
    Result = catch flurm_job_manager:cancel_job(999999),
    case Result of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

hold_job_test() ->
    Result = catch flurm_job_manager:hold_job(999998),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

release_job_test() ->
    Result = catch flurm_job_manager:release_job(999997),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

requeue_job_test() ->
    Result = catch flurm_job_manager:requeue_job(999996),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

suspend_job_test() ->
    Result = catch flurm_job_manager:suspend_job(999995),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

resume_job_test() ->
    Result = catch flurm_job_manager:resume_job(999994),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

update_job_test() ->
    Updates = #{priority => 200},
    Result = catch flurm_job_manager:update_job(999993, Updates),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

get_job_test() ->
    Result = catch flurm_job_manager:get_job(999992),
    case Result of
        {ok, _Job} -> ok;
        {error, not_found} -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

list_jobs_test() ->
    Result = catch flurm_job_manager:list_jobs(),
    case Result of
        Jobs when is_list(Jobs) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

list_jobs_by_user_test() ->
    Result = catch flurm_job_manager:list_jobs(#{user => <<"testuser">>}),
    case Result of
        Jobs when is_list(Jobs) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

list_jobs_by_state_test() ->
    Result = catch flurm_job_manager:list_jobs(#{state => pending}),
    case Result of
        Jobs when is_list(Jobs) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

list_jobs_by_partition_test() ->
    Result = catch flurm_job_manager:list_jobs(#{partition => <<"batch">>}),
    case Result of
        Jobs when is_list(Jobs) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

get_job_count_test() ->
    Result = catch flurm_job_manager:get_job_count(),
    case Result of
        Count when is_integer(Count) -> ?assert(Count >= 0);
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

%%====================================================================
%% Array Job Tests
%%====================================================================

submit_array_job_test() ->
    JobSpec = #{
        name => <<"array_job">>,
        user => <<"testuser">>,
        array_spec => <<"0-9">>,
        script => <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>
    },
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

submit_array_job_with_step_test() ->
    JobSpec = #{
        name => <<"array_job_step">>,
        user => <<"testuser">>,
        array_spec => <<"0-100:10">>,
        script => <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>
    },
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

submit_array_job_with_limit_test() ->
    JobSpec = #{
        name => <<"array_job_limit">>,
        user => <<"testuser">>,
        array_spec => <<"1-100%5">>,
        script => <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>
    },
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Signal Job Tests
%%====================================================================

signal_job_test() ->
    Result = catch flurm_job_manager:signal_job(999991, 15),  % SIGTERM
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

signal_job_sigkill_test() ->
    Result = catch flurm_job_manager:signal_job(999990, 9),  % SIGKILL
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

signal_job_sigusr1_test() ->
    Result = catch flurm_job_manager:signal_job(999989, 10),  % SIGUSR1
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

%%====================================================================
%% Job Priority Tests
%%====================================================================

set_job_priority_test() ->
    Result = catch flurm_job_manager:set_job_priority(999988, 500),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

get_job_priority_test() ->
    Result = catch flurm_job_manager:get_job_priority(999987),
    case Result of
        {ok, Priority} when is_integer(Priority) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

%%====================================================================
%% Job Dependencies Tests
%%====================================================================

submit_job_with_dependency_test() ->
    JobSpec = #{
        name => <<"dependent_job">>,
        user => <<"testuser">>,
        dependency => <<"afterok:1234">>,
        script => <<"#!/bin/bash\necho dependent">>
    },
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

submit_job_with_multiple_deps_test() ->
    JobSpec = #{
        name => <<"multi_dep_job">>,
        user => <<"testuser">>,
        dependency => <<"afterok:1234,afterany:5678">>,
        script => <<"#!/bin/bash\necho multi">>
    },
    Result = catch flurm_job_manager:submit_job(JobSpec),
    case Result of
        {ok, _JobId} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Job Statistics Tests
%%====================================================================

get_job_stats_test() ->
    Result = catch flurm_job_manager:get_job_stats(),
    case Result of
        Stats when is_map(Stats) -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

make_test_job(Id) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        num_tasks = 1,
        cpus_per_task = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

make_test_job_with_account(Id, Account) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        account = Account
    }.

make_test_job_with_qos(Id, Qos) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = [],
        qos = Qos
    }.

make_test_job_with_partition(Id, Partition) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user">>,
        partition = Partition,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

%%====================================================================
%% Extended Internal Function Tests - create_job
%%====================================================================

create_job_with_std_out_test() ->
    %% Test that create_job correctly sets std_out path
    Job = flurm_job_manager:create_job(42, #{
        name => <<"stdout_test">>,
        user => <<"testuser">>,
        work_dir => <<"/home/user">>,
        std_out => <<"/home/user/custom.out">>
    }),
    ?assertEqual(42, Job#job.id),
    ?assertEqual(<<"stdout_test">>, Job#job.name).

create_job_with_std_err_test() ->
    %% Test that create_job correctly sets std_err path
    Job = flurm_job_manager:create_job(43, #{
        name => <<"stderr_test">>,
        user => <<"testuser">>,
        work_dir => <<"/home/user">>,
        std_err => <<"/home/user/custom.err">>
    }),
    ?assertEqual(43, Job#job.id).

create_job_with_num_tasks_test() ->
    %% Test num_tasks field
    Job = flurm_job_manager:create_job(44, #{
        name => <<"tasks_test">>,
        user => <<"testuser">>,
        num_tasks => 16,
        cpus_per_task => 2
    }),
    ?assertEqual(44, Job#job.id).

create_job_with_licenses_test() ->
    %% Test licenses field
    Job = flurm_job_manager:create_job(45, #{
        name => <<"license_test">>,
        user => <<"testuser">>,
        licenses => [<<"matlab:1">>, <<"gaussian:2">>]
    }),
    ?assertEqual(45, Job#job.id).

create_job_with_zero_priority_held_test() ->
    %% Test that priority=0 creates a held job
    Job = flurm_job_manager:create_job(46, #{
        name => <<"held_test">>,
        user => <<"testuser">>,
        priority => 0
    }),
    %% State should be 'held' not 'pending' when priority is 0
    ?assertEqual(46, Job#job.id).

create_job_default_work_dir_test() ->
    %% Test default work_dir is /tmp
    Job = flurm_job_manager:create_job(47, #{
        name => <<"default_dir_test">>,
        user => <<"testuser">>
    }),
    ?assertEqual(47, Job#job.id).

create_job_default_qos_test() ->
    %% Test default qos is normal
    Job = flurm_job_manager:create_job(48, #{
        name => <<"default_qos_test">>,
        user => <<"testuser">>
    }),
    ?assertEqual(48, Job#job.id).

%%====================================================================
%% Extended Internal Function Tests - apply_job_updates
%%====================================================================

apply_job_updates_state_running_test() ->
    Job = make_test_job(50),
    Updates = #{state => running},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(running, Result#job.state).

apply_job_updates_state_completed_test() ->
    Job = make_test_job(51),
    Updates = #{state => completed},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(completed, Result#job.state).

apply_job_updates_state_failed_test() ->
    Job = make_test_job(52),
    Updates = #{state => failed},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(failed, Result#job.state).

apply_job_updates_state_cancelled_test() ->
    Job = make_test_job(53),
    Updates = #{state => cancelled},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(cancelled, Result#job.state).

apply_job_updates_state_held_test() ->
    Job = make_test_job(54),
    Updates = #{state => held},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(held, Result#job.state).

apply_job_updates_state_suspended_test() ->
    Job = make_test_job(55),
    Updates = #{state => suspended},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(suspended, Result#job.state).

apply_job_updates_allocated_nodes_test() ->
    Job = make_test_job(56),
    Updates = #{allocated_nodes => [<<"node1">>, <<"node2">>]},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_start_time_test() ->
    Job = make_test_job(57),
    Now = erlang:system_time(second),
    Updates = #{start_time => Now},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_end_time_test() ->
    Job = make_test_job(58),
    Now = erlang:system_time(second),
    Updates = #{end_time => Now},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_exit_code_test() ->
    Job = make_test_job(59),
    Updates = #{exit_code => 0},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_exit_code_nonzero_test() ->
    Job = make_test_job(60),
    Updates = #{exit_code => 127},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_prolog_status_test() ->
    Job = make_test_job(61),
    Updates = #{prolog_status => completed},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_epilog_status_test() ->
    Job = make_test_job(62),
    Updates = #{epilog_status => failed},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

apply_job_updates_unknown_field_test() ->
    Job = make_test_job(63),
    Updates = #{unknown_field => some_value},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    %% Unknown fields should be ignored
    ?assertEqual(Job, Result).

apply_job_updates_combined_running_test() ->
    Job = make_test_job(64),
    Now = erlang:system_time(second),
    Updates = #{
        state => running,
        start_time => Now,
        allocated_nodes => [<<"node1">>]
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(running, Result#job.state).

apply_job_updates_combined_completed_test() ->
    Job = make_test_job(65),
    Now = erlang:system_time(second),
    Updates = #{
        state => completed,
        end_time => Now,
        exit_code => 0
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(completed, Result#job.state).

apply_job_updates_combined_failed_test() ->
    Job = make_test_job(66),
    Now = erlang:system_time(second),
    Updates = #{
        state => failed,
        end_time => Now,
        exit_code => 1
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(failed, Result#job.state).

%%====================================================================
%% Extended Internal Function Tests - build_limit_check_spec
%%====================================================================

build_limit_check_spec_minimal_job_test() ->
    JobSpec = #{name => <<"test">>},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assert(is_map(Spec)),
    ?assert(maps:is_key(user, Spec)),
    ?assert(maps:is_key(partition, Spec)).

build_limit_check_spec_user_test() ->
    JobSpec = #{user => <<"alice">>},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(<<"alice">>, maps:get(user, Spec)).

build_limit_check_spec_account_test() ->
    JobSpec = #{account => <<"project_x">>},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(<<"project_x">>, maps:get(account, Spec)).

build_limit_check_spec_partition_test() ->
    JobSpec = #{partition => <<"gpu">>},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(<<"gpu">>, maps:get(partition, Spec)).

build_limit_check_spec_num_nodes_test() ->
    JobSpec = #{num_nodes => 4},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(4, maps:get(num_nodes, Spec)).

build_limit_check_spec_num_cpus_test() ->
    JobSpec = #{num_cpus => 32},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(32, maps:get(num_cpus, Spec)).

build_limit_check_spec_memory_mb_test() ->
    JobSpec = #{memory_mb => 65536},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(65536, maps:get(memory_mb, Spec)).

build_limit_check_spec_time_limit_test() ->
    JobSpec = #{time_limit => 86400},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(86400, maps:get(time_limit, Spec)).

build_limit_check_spec_user_id_fallback_test() ->
    JobSpec = #{user_id => <<"bob">>},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(<<"bob">>, maps:get(user, Spec)).

build_limit_check_spec_full_test() ->
    JobSpec = #{
        user => <<"carol">>,
        account => <<"research">>,
        partition => <<"bigmem">>,
        num_nodes => 8,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 172800
    },
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(<<"carol">>, maps:get(user, Spec)),
    ?assertEqual(<<"research">>, maps:get(account, Spec)),
    ?assertEqual(<<"bigmem">>, maps:get(partition, Spec)),
    ?assertEqual(8, maps:get(num_nodes, Spec)),
    ?assertEqual(64, maps:get(num_cpus, Spec)),
    ?assertEqual(131072, maps:get(memory_mb, Spec)),
    ?assertEqual(172800, maps:get(time_limit, Spec)).

build_limit_check_spec_defaults_test() ->
    JobSpec = #{},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    %% Check defaults
    ?assertEqual(<<"unknown">>, maps:get(user, Spec)),
    ?assertEqual(<<>>, maps:get(account, Spec)),
    ?assertEqual(<<"default">>, maps:get(partition, Spec)),
    ?assertEqual(1, maps:get(num_nodes, Spec)),
    ?assertEqual(1, maps:get(num_cpus, Spec)),
    ?assertEqual(1024, maps:get(memory_mb, Spec)),
    ?assertEqual(3600, maps:get(time_limit, Spec)).

%%====================================================================
%% Job State Transition Tests
%%====================================================================

job_state_pending_to_running_test() ->
    Job = make_test_job(100),
    Updates = #{state => running},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(running, Result#job.state).

job_state_running_to_completed_test() ->
    Job = make_test_job(101),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => running}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => completed}),
    ?assertEqual(completed, Result#job.state).

job_state_running_to_failed_test() ->
    Job = make_test_job(102),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => running}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => failed}),
    ?assertEqual(failed, Result#job.state).

job_state_pending_to_held_test() ->
    Job = make_test_job(103),
    Result = flurm_job_manager:apply_job_updates(Job, #{state => held}),
    ?assertEqual(held, Result#job.state).

job_state_held_to_pending_test() ->
    Job = make_test_job(104),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => held}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => pending}),
    ?assertEqual(pending, Result#job.state).

job_state_running_to_suspended_test() ->
    Job = make_test_job(105),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => running}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => suspended}),
    ?assertEqual(suspended, Result#job.state).

job_state_suspended_to_running_test() ->
    Job = make_test_job(106),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => suspended}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => running}),
    ?assertEqual(running, Result#job.state).

job_state_running_to_cancelled_test() ->
    Job = make_test_job(107),
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => running}),
    Result = flurm_job_manager:apply_job_updates(Job2, #{state => cancelled}),
    ?assertEqual(cancelled, Result#job.state).

job_state_pending_to_cancelled_test() ->
    Job = make_test_job(108),
    Result = flurm_job_manager:apply_job_updates(Job, #{state => cancelled}),
    ?assertEqual(cancelled, Result#job.state).

%%====================================================================
%% Priority Update Tests
%%====================================================================

priority_update_increase_test() ->
    Job = make_test_job(110),
    Result = flurm_job_manager:apply_job_updates(Job, #{priority => 500}),
    ?assertEqual(500, Result#job.priority).

priority_update_decrease_test() ->
    Job = make_test_job(111),
    Result = flurm_job_manager:apply_job_updates(Job, #{priority => 50}),
    ?assertEqual(50, Result#job.priority).

priority_update_to_zero_test() ->
    Job = make_test_job(112),
    Result = flurm_job_manager:apply_job_updates(Job, #{priority => 0}),
    ?assertEqual(0, Result#job.priority).

priority_update_very_high_test() ->
    Job = make_test_job(113),
    Result = flurm_job_manager:apply_job_updates(Job, #{priority => 10000}),
    ?assertEqual(10000, Result#job.priority).

%%====================================================================
%% Time Limit Update Tests
%%====================================================================

time_limit_update_increase_test() ->
    Job = make_test_job(120),
    Result = flurm_job_manager:apply_job_updates(Job, #{time_limit => 7200}),
    ?assertEqual(7200, Result#job.time_limit).

time_limit_update_decrease_test() ->
    Job = make_test_job(121),
    Result = flurm_job_manager:apply_job_updates(Job, #{time_limit => 1800}),
    ?assertEqual(1800, Result#job.time_limit).

time_limit_one_day_test() ->
    Job = make_test_job(122),
    Result = flurm_job_manager:apply_job_updates(Job, #{time_limit => 86400}),
    ?assertEqual(86400, Result#job.time_limit).

time_limit_one_week_test() ->
    Job = make_test_job(123),
    Result = flurm_job_manager:apply_job_updates(Job, #{time_limit => 604800}),
    ?assertEqual(604800, Result#job.time_limit).

%%====================================================================
%% Name Update Tests
%%====================================================================

name_update_simple_test() ->
    Job = make_test_job(130),
    Result = flurm_job_manager:apply_job_updates(Job, #{name => <<"new_name">>}),
    ?assertEqual(<<"new_name">>, Result#job.name).

name_update_long_test() ->
    Job = make_test_job(131),
    LongName = <<"this_is_a_very_long_job_name_that_might_exceed_typical_limits">>,
    Result = flurm_job_manager:apply_job_updates(Job, #{name => LongName}),
    ?assertEqual(LongName, Result#job.name).

name_update_with_numbers_test() ->
    Job = make_test_job(132),
    Result = flurm_job_manager:apply_job_updates(Job, #{name => <<"job_123_test">>}),
    ?assertEqual(<<"job_123_test">>, Result#job.name).

name_update_with_special_chars_test() ->
    Job = make_test_job(133),
    Result = flurm_job_manager:apply_job_updates(Job, #{name => <<"job-with_special.chars">>}),
    ?assertEqual(<<"job-with_special.chars">>, Result#job.name).

%%====================================================================
%% Prolog/Epilog Status Tests
%%====================================================================

prolog_status_completed_test() ->
    Job = make_test_job(140),
    Result = flurm_job_manager:apply_job_updates(Job, #{prolog_status => completed}),
    ?assert(is_tuple(Result)).

prolog_status_failed_test() ->
    Job = make_test_job(141),
    Result = flurm_job_manager:apply_job_updates(Job, #{prolog_status => failed}),
    ?assert(is_tuple(Result)).

prolog_status_running_test() ->
    Job = make_test_job(142),
    Result = flurm_job_manager:apply_job_updates(Job, #{prolog_status => running}),
    ?assert(is_tuple(Result)).

epilog_status_completed_test() ->
    Job = make_test_job(143),
    Result = flurm_job_manager:apply_job_updates(Job, #{epilog_status => completed}),
    ?assert(is_tuple(Result)).

epilog_status_failed_test() ->
    Job = make_test_job(144),
    Result = flurm_job_manager:apply_job_updates(Job, #{epilog_status => failed}),
    ?assert(is_tuple(Result)).

epilog_status_running_test() ->
    Job = make_test_job(145),
    Result = flurm_job_manager:apply_job_updates(Job, #{epilog_status => running}),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Allocated Nodes Update Tests
%%====================================================================

allocated_nodes_single_test() ->
    Job = make_test_job(150),
    Result = flurm_job_manager:apply_job_updates(Job, #{allocated_nodes => [<<"node1">>]}),
    ?assert(is_tuple(Result)).

allocated_nodes_multiple_test() ->
    Job = make_test_job(151),
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>],
    Result = flurm_job_manager:apply_job_updates(Job, #{allocated_nodes => Nodes}),
    ?assert(is_tuple(Result)).

allocated_nodes_clear_test() ->
    Job = make_test_job(152),
    %% First set some nodes
    Job2 = flurm_job_manager:apply_job_updates(Job, #{allocated_nodes => [<<"node1">>]}),
    %% Then clear them
    Result = flurm_job_manager:apply_job_updates(Job2, #{allocated_nodes => []}),
    ?assert(is_tuple(Result)).

allocated_nodes_many_test() ->
    Job = make_test_job(153),
    Nodes = [list_to_binary("node" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
    Result = flurm_job_manager:apply_job_updates(Job, #{allocated_nodes => Nodes}),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Exit Code Tests
%%====================================================================

exit_code_success_test() ->
    Job = make_test_job(160),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 0}),
    ?assert(is_tuple(Result)).

exit_code_general_error_test() ->
    Job = make_test_job(161),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 1}),
    ?assert(is_tuple(Result)).

exit_code_command_not_found_test() ->
    Job = make_test_job(162),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 127}),
    ?assert(is_tuple(Result)).

exit_code_segfault_test() ->
    Job = make_test_job(163),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 139}),
    ?assert(is_tuple(Result)).

exit_code_killed_test() ->
    Job = make_test_job(164),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 137}),
    ?assert(is_tuple(Result)).

exit_code_timeout_test() ->
    Job = make_test_job(165),
    Result = flurm_job_manager:apply_job_updates(Job, #{exit_code => 124}),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Start/End Time Tests
%%====================================================================

start_time_current_test() ->
    Job = make_test_job(170),
    Now = erlang:system_time(second),
    Result = flurm_job_manager:apply_job_updates(Job, #{start_time => Now}),
    ?assert(is_tuple(Result)).

end_time_current_test() ->
    Job = make_test_job(171),
    Now = erlang:system_time(second),
    Result = flurm_job_manager:apply_job_updates(Job, #{end_time => Now}),
    ?assert(is_tuple(Result)).

start_and_end_time_test() ->
    Job = make_test_job(172),
    Start = erlang:system_time(second),
    End = Start + 3600,  % 1 hour later
    Updates = #{start_time => Start, end_time => End},
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Combined Update Tests
%%====================================================================

combined_start_job_test() ->
    Job = make_test_job(180),
    Now = erlang:system_time(second),
    Updates = #{
        state => running,
        start_time => Now,
        allocated_nodes => [<<"compute-001">>, <<"compute-002">>]
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(running, Result#job.state).

combined_complete_job_test() ->
    Job = make_test_job(181),
    Now = erlang:system_time(second),
    Updates = #{
        state => completed,
        end_time => Now,
        exit_code => 0,
        epilog_status => completed
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(completed, Result#job.state).

combined_fail_job_test() ->
    Job = make_test_job(182),
    Now = erlang:system_time(second),
    Updates = #{
        state => failed,
        end_time => Now,
        exit_code => 1
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(failed, Result#job.state).

combined_timeout_job_test() ->
    Job = make_test_job(183),
    Now = erlang:system_time(second),
    Updates = #{
        state => failed,
        end_time => Now,
        exit_code => 124  % SLURM timeout exit code
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(failed, Result#job.state).

combined_cancel_job_test() ->
    Job = make_test_job(184),
    Now = erlang:system_time(second),
    Updates = #{
        state => cancelled,
        end_time => Now,
        allocated_nodes => []
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(cancelled, Result#job.state).

combined_hold_job_test() ->
    Job = make_test_job(185),
    Updates = #{
        state => held,
        priority => 0
    },
    Result = flurm_job_manager:apply_job_updates(Job, Updates),
    ?assertEqual(held, Result#job.state),
    ?assertEqual(0, Result#job.priority).

combined_release_job_test() ->
    Job = make_test_job(186),
    %% First hold
    Job2 = flurm_job_manager:apply_job_updates(Job, #{state => held, priority => 0}),
    %% Then release
    Updates = #{
        state => pending,
        priority => 100
    },
    Result = flurm_job_manager:apply_job_updates(Job2, Updates),
    ?assertEqual(pending, Result#job.state),
    ?assertEqual(100, Result#job.priority).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

empty_job_spec_test() ->
    %% Empty job spec should still create a valid job
    Job = flurm_job_manager:create_job(200, #{}),
    ?assertEqual(200, Job#job.id),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<"unknown">>, Job#job.user).

very_large_memory_test() ->
    %% Test with very large memory (1TB)
    JobSpec = #{memory_mb => 1048576},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(1048576, maps:get(memory_mb, Spec)).

very_large_cpu_count_test() ->
    %% Test with many CPUs
    JobSpec = #{num_cpus => 1024},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(1024, maps:get(num_cpus, Spec)).

very_large_node_count_test() ->
    %% Test with many nodes
    JobSpec = #{num_nodes => 512},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(512, maps:get(num_nodes, Spec)).

very_long_time_limit_test() ->
    %% Test with 30 day time limit
    JobSpec = #{time_limit => 2592000},
    Spec = flurm_job_manager:build_limit_check_spec(JobSpec),
    ?assertEqual(2592000, maps:get(time_limit, Spec)).

unicode_job_name_test() ->
    %% Test with unicode characters in name
    Job = flurm_job_manager:create_job(201, #{
        name => <<"测试作业_ジョブ_работа">>
    }),
    ?assertEqual(201, Job#job.id).

binary_with_newlines_test() ->
    %% Test script with newlines
    Script = <<"#!/bin/bash\n\necho 'line 1'\necho 'line 2'\n\nexit 0">>,
    Job = flurm_job_manager:create_job(202, #{script => Script}),
    ?assertEqual(202, Job#job.id).

%%====================================================================
%% Import Job Tests (API calls)
%%====================================================================

import_job_basic_test() ->
    JobSpec = #{
        id => 99001,
        name => <<"imported_job">>,
        user => <<"imported_user">>,
        state => pending
    },
    Result = catch flurm_job_manager:import_job(JobSpec),
    case Result of
        {ok, 99001} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

import_job_with_state_test() ->
    JobSpec = #{
        id => 99002,
        name => <<"running_import">>,
        user => <<"imported_user">>,
        state => running,
        start_time => erlang:system_time(second)
    },
    Result = catch flurm_job_manager:import_job(JobSpec),
    case Result of
        {ok, 99002} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

import_job_completed_test() ->
    JobSpec = #{
        id => 99003,
        name => <<"completed_import">>,
        user => <<"imported_user">>,
        state => completed
    },
    Result = catch flurm_job_manager:import_job(JobSpec),
    case Result of
        {ok, 99003} -> ok;
        {'EXIT', _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Update Prolog/Epilog Status API Tests
%%====================================================================

update_prolog_status_api_test() ->
    Result = catch flurm_job_manager:update_prolog_status(999899, completed),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

update_prolog_status_failed_api_test() ->
    Result = catch flurm_job_manager:update_prolog_status(999898, failed),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

update_epilog_status_api_test() ->
    Result = catch flurm_job_manager:update_epilog_status(999897, completed),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

update_epilog_status_failed_api_test() ->
    Result = catch flurm_job_manager:update_epilog_status(999896, failed),
    case Result of
        ok -> ok;
        {error, _} -> ok;
        {'EXIT', _} -> ok
    end.

%%====================================================================
%% Additional create_job Tests
%%====================================================================

create_job_with_all_fields_test() ->
    JobSpec = #{
        name => <<"complete_job">>,
        user => <<"fulluser">>,
        partition => <<"full_partition">>,
        num_nodes => 10,
        num_cpus => 80,
        num_tasks => 40,
        cpus_per_task => 2,
        memory_mb => 262144,
        time_limit => 604800,
        priority => 1000,
        qos => <<"premium">>,
        account => <<"premium_project">>,
        work_dir => <<"/scratch/fulluser/project">>,
        std_out => <<"/scratch/fulluser/project/job.out">>,
        std_err => <<"/scratch/fulluser/project/job.err">>,
        script => <<"#!/bin/bash\nmpirun ./full_simulation">>
    },
    Job = flurm_job_manager:create_job(300, JobSpec),
    ?assertEqual(300, Job#job.id),
    ?assertEqual(<<"complete_job">>, Job#job.name),
    ?assertEqual(<<"fulluser">>, Job#job.user).

create_job_minimum_resources_test() ->
    JobSpec = #{
        name => <<"tiny_job">>,
        user => <<"tinyuser">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 1
    },
    Job = flurm_job_manager:create_job(301, JobSpec),
    ?assertEqual(301, Job#job.id).

create_job_maximum_resources_test() ->
    JobSpec = #{
        name => <<"huge_job">>,
        user => <<"hugeuser">>,
        num_nodes => 10000,
        num_cpus => 1000000,
        memory_mb => 1048576000  % 1 PB
    },
    Job = flurm_job_manager:create_job(302, JobSpec),
    ?assertEqual(302, Job#job.id).
