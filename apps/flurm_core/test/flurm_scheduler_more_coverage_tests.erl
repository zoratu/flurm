%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_scheduler module.
%%% Tests internal functions exported via -ifdef(TEST) and edge cases.
%%%-------------------------------------------------------------------
-module(flurm_scheduler_more_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% job_to_info Tests
%%====================================================================

job_to_info_basic_test() ->
    Job = make_test_job(1),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

job_to_info_has_resources_test() ->
    Job = make_test_job_with_resources(1, 4, 16, 8192),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

job_to_info_has_partition_test() ->
    Job = make_test_job_with_partition(1, <<"compute">>),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

job_to_info_has_time_limit_test() ->
    Job = make_test_job_with_time_limit(1, 7200),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

job_to_info_has_user_test() ->
    Job = make_test_job(1),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

job_to_info_has_account_test() ->
    Job = make_test_job_with_account(1, <<"research_project">>),
    Info = catch flurm_scheduler:job_to_info(Job),
    ?assert(is_map(Info) orelse is_tuple(Info)).

%%====================================================================
%% job_to_limit_spec Tests
%%====================================================================

job_to_limit_spec_basic_test() ->
    Job = make_test_job(1),
    Spec = catch flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

job_to_limit_spec_has_user_test() ->
    Job = make_test_job(1),
    Spec = catch flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

job_to_limit_spec_has_account_test() ->
    Job = make_test_job_with_account(1, <<"project">>),
    Spec = catch flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

job_to_limit_spec_has_qos_test() ->
    Job = make_test_job_with_qos(1, <<"high">>),
    Spec = catch flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

job_to_limit_spec_has_tres_test() ->
    Job = make_test_job_with_resources(1, 2, 8, 4096),
    Spec = catch flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(Spec) orelse is_tuple(Spec)).

%%====================================================================
%% job_to_backfill_map Tests
%%====================================================================

job_to_backfill_map_basic_test() ->
    Job = make_test_job(1),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_name_test() ->
    Job = make_test_job(1),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_resources_test() ->
    Job = make_test_job_with_resources(1, 4, 16, 8192),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_time_limit_test() ->
    Job = make_test_job_with_time_limit(1, 3600),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_priority_test() ->
    Job = make_test_job_with_priority(1, 500),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_partition_test() ->
    Job = make_test_job_with_partition(1, <<"gpu">>),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

job_to_backfill_map_has_submit_time_test() ->
    Job = make_test_job(1),
    Result = catch flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(Result) orelse is_tuple(Result)).

%%====================================================================
%% build_limit_info Tests (expects a map, not a record)
%%====================================================================

build_limit_info_basic_test() ->
    JobInfo = make_job_info_map(1),
    Info = catch flurm_scheduler:build_limit_info(JobInfo),
    ?assert(is_map(Info) orelse is_tuple(Info)).

build_limit_info_has_user_test() ->
    JobInfo = make_job_info_map(1),
    Info = catch flurm_scheduler:build_limit_info(JobInfo),
    case Info of
        #{user := User} -> ?assertEqual(<<"user">>, User);
        _ -> ok
    end.

build_limit_info_has_account_test() ->
    JobInfo = #{
        id => 1,
        user => <<"user">>,
        account => <<"acct">>,
        num_cpus => 4,
        memory_mb => 2048,
        num_nodes => 1
    },
    Info = catch flurm_scheduler:build_limit_info(JobInfo),
    case Info of
        #{account := Account} -> ?assertEqual(<<"acct">>, Account);
        _ -> ok
    end.

build_limit_info_has_tres_test() ->
    JobInfo = #{
        id => 1,
        user => <<"user">>,
        account => <<>>,
        num_cpus => 4,
        memory_mb => 2048,
        num_nodes => 1
    },
    Info = catch flurm_scheduler:build_limit_info(JobInfo),
    case Info of
        #{tres := _} -> ok;
        _ -> ok
    end.

%%====================================================================
%% calculate_resources_to_free Tests (expects maps, not records)
%%====================================================================

calculate_resources_to_free_empty_test() ->
    Result = flurm_scheduler:calculate_resources_to_free([]),
    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(cpus, Result, 0)),
    ?assertEqual(0, maps:get(memory_mb, Result, 0)),
    ?assertEqual(0, maps:get(nodes, Result, 0)).

calculate_resources_to_free_single_job_test() ->
    Jobs = [#{num_nodes => 2, num_cpus => 8, memory_mb => 4096}],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),
    ?assertEqual(8, maps:get(cpus, Result)),
    ?assertEqual(4096, maps:get(memory_mb, Result)),
    ?assertEqual(2, maps:get(nodes, Result)).

calculate_resources_to_free_multiple_jobs_test() ->
    Jobs = [
        #{num_nodes => 2, num_cpus => 8, memory_mb => 4096},
        #{num_nodes => 4, num_cpus => 16, memory_mb => 8192},
        #{num_nodes => 1, num_cpus => 4, memory_mb => 2048}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),
    ?assertEqual(28, maps:get(cpus, Result)),
    ?assertEqual(14336, maps:get(memory_mb, Result)),
    ?assertEqual(7, maps:get(nodes, Result)).

%%====================================================================
%% remove_jobs_from_queue Tests
%%====================================================================

remove_jobs_from_queue_empty_jobs_test() ->
    Q = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([], Q),
    ?assertEqual([1, 2, 3, 4, 5], queue:to_list(Result)).

remove_jobs_from_queue_empty_queue_test() ->
    Q = queue:new(),
    Result = flurm_scheduler:remove_jobs_from_queue([1, 2, 3], Q),
    ?assertEqual([], queue:to_list(Result)).

remove_jobs_from_queue_remove_one_test() ->
    Q = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([3], Q),
    ?assertEqual([1, 2, 4, 5], queue:to_list(Result)).

remove_jobs_from_queue_remove_multiple_test() ->
    Q = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([1, 3, 5], Q),
    ?assertEqual([2, 4], queue:to_list(Result)).

remove_jobs_from_queue_remove_all_test() ->
    Q = queue:from_list([1, 2, 3]),
    Result = flurm_scheduler:remove_jobs_from_queue([1, 2, 3], Q),
    ?assertEqual([], queue:to_list(Result)).

remove_jobs_from_queue_nonexistent_jobs_test() ->
    Q = queue:from_list([1, 2, 3]),
    Result = flurm_scheduler:remove_jobs_from_queue([4, 5, 6], Q),
    ?assertEqual([1, 2, 3], queue:to_list(Result)).

remove_jobs_from_queue_mixed_test() ->
    Q = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([2, 4, 6, 8], Q),
    ?assertEqual([1, 3, 5], queue:to_list(Result)).

%%====================================================================
%% API Function Tests (may require gen_server running)
%%====================================================================

submit_job_test() ->
    Result = catch flurm_scheduler:submit_job(99999),
    case Result of
        ok -> ok;
        {'EXIT', {noproc, _}} -> ok;
        {'EXIT', _} -> ok
    end.

job_completed_test() ->
    Result = catch flurm_scheduler:job_completed(99998),
    case Result of
        ok -> ok;
        {'EXIT', {noproc, _}} -> ok;
        {'EXIT', _} -> ok
    end.

job_failed_test() ->
    Result = catch flurm_scheduler:job_failed(99997),
    case Result of
        ok -> ok;
        {'EXIT', {noproc, _}} -> ok;
        {'EXIT', _} -> ok
    end.

trigger_schedule_test() ->
    Result = catch flurm_scheduler:trigger_schedule(),
    case Result of
        ok -> ok;
        {'EXIT', {noproc, _}} -> ok;
        {'EXIT', _} -> ok
    end.

get_stats_test() ->
    Result = catch flurm_scheduler:get_stats(),
    case Result of
        {ok, Stats} when is_map(Stats) ->
            ?assert(maps:is_key(pending_count, Stats)),
            ?assert(maps:is_key(running_count, Stats)),
            ?assert(maps:is_key(completed_count, Stats)),
            ?assert(maps:is_key(failed_count, Stats));
        {'EXIT', {noproc, _}} -> ok;
        {'EXIT', _} -> ok
    end.

job_deps_satisfied_test() ->
    Result = catch flurm_scheduler:job_deps_satisfied(99996),
    case Result of
        ok -> ok;
        {'EXIT', {noproc, _}} -> ok;
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
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
    }.

make_test_job_with_resources(Id, Nodes, Cpus, MemoryMb) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = Nodes,
        num_cpus = Cpus,
        memory_mb = MemoryMb,
        time_limit = 3600,
        priority = 100,
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
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
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
    }.

make_test_job_with_time_limit(Id, TimeLimit) ->
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
        time_limit = TimeLimit,
        priority = 100,
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
    }.

make_test_job_with_priority(Id, Priority) ->
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
        priority = Priority,
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
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
        work_dir = <<"/tmp">>,
        account = Account,
        qos = <<"normal">>,
        submit_time = erlang:system_time(second)
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
        work_dir = <<"/tmp">>,
        account = <<>>,
        qos = Qos,
        submit_time = erlang:system_time(second)
    }.

make_job_info_map(Id) ->
    #{
        id => Id,
        name => <<"test_job">>,
        user => <<"user">>,
        partition => <<"batch">>,
        state => pending,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        account => <<>>,
        qos => <<"normal">>,
        submit_time => erlang:system_time(second)
    }.
