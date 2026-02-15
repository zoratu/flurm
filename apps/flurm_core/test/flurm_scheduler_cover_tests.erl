%%%-------------------------------------------------------------------
%%% @doc FLURM Scheduler Coverage Tests
%%%
%%% Comprehensive EUnit tests specifically designed to maximize code
%%% coverage for the flurm_scheduler module's TEST-exported internal
%%% functions. Tests all 6 exported helper functions with various inputs.
%%%
%%% TEST-exported functions covered:
%%% - job_to_info/1 - converts job record to map
%%% - job_to_limit_spec/1 - converts job to limit spec map
%%% - job_to_backfill_map/1 - converts job to backfill map
%%% - build_limit_info/1 - builds limit info from map
%%% - calculate_resources_to_free/1 - aggregates resources from job list
%%% - remove_jobs_from_queue/2 - filters jobs from queue
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Helper to create a minimal job record for testing
make_job() ->
    make_job(#{}).

make_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 1,
        num_cpus => 4,
        num_tasks => 1,
        cpus_per_task => 1,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        submit_time => erlang:system_time(second),
        start_time => undefined,
        end_time => undefined,
        allocated_nodes => [],
        exit_code => undefined,
        work_dir => <<"/tmp">>,
        std_out => <<>>,
        std_err => <<>>,
        account => <<"default_account">>,
        qos => <<"normal">>,
        reservation => <<>>,
        licenses => [],
        gres => <<>>,
        gres_per_node => <<>>,
        gres_per_task => <<>>,
        gpu_type => <<>>,
        gpu_memory_mb => 0,
        gpu_exclusive => true,
        prolog => <<>>,
        epilog => <<>>,
        prolog_status => pending,
        epilog_status => pending,
        array_job_id => 0,
        array_task_id => undefined
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
        num_tasks = maps:get(num_tasks, Merged),
        cpus_per_task = maps:get(cpus_per_task, Merged),
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
        reservation = maps:get(reservation, Merged),
        licenses = maps:get(licenses, Merged),
        gres = maps:get(gres, Merged),
        gres_per_node = maps:get(gres_per_node, Merged),
        gres_per_task = maps:get(gres_per_task, Merged),
        gpu_type = maps:get(gpu_type, Merged),
        gpu_memory_mb = maps:get(gpu_memory_mb, Merged),
        gpu_exclusive = maps:get(gpu_exclusive, Merged),
        prolog = maps:get(prolog, Merged),
        epilog = maps:get(epilog, Merged),
        prolog_status = maps:get(prolog_status, Merged),
        epilog_status = maps:get(epilog_status, Merged),
        array_job_id = maps:get(array_job_id, Merged),
        array_task_id = maps:get(array_task_id, Merged)
    }.

%%====================================================================
%% job_to_info/1 Tests
%%====================================================================

job_to_info_test_() ->
    {"job_to_info/1 tests",
     [
        {"converts minimal job to info map", fun test_job_to_info_minimal/0},
        {"converts job with all fields populated", fun test_job_to_info_full/0},
        {"converts job with GRES fields", fun test_job_to_info_gres/0},
        {"converts job with allocated nodes", fun test_job_to_info_allocated/0},
        {"converts job with licenses", fun test_job_to_info_licenses/0},
        {"converts running job", fun test_job_to_info_running/0},
        {"converts job with custom work_dir", fun test_job_to_info_workdir/0},
        {"converts job with output paths", fun test_job_to_info_output_paths/0}
     ]}.

test_job_to_info_minimal() ->
    Job = make_job(),
    Result = flurm_scheduler:job_to_info(Job),

    ?assert(is_map(Result)),
    ?assertEqual(1, maps:get(job_id, Result)),
    ?assertEqual(<<"test_job">>, maps:get(name, Result)),
    ?assertEqual(pending, maps:get(state, Result)),
    ?assertEqual(<<"default">>, maps:get(partition, Result)),
    ?assertEqual(1, maps:get(num_nodes, Result)),
    ?assertEqual(4, maps:get(num_cpus, Result)),
    ?assertEqual(1024, maps:get(memory_mb, Result)),
    ?assertEqual(3600, maps:get(time_limit, Result)),
    ok.

test_job_to_info_full() ->
    Job = make_job(#{
        id => 12345,
        name => <<"full_job">>,
        user => <<"admin">>,
        partition => <<"gpu">>,
        state => running,
        num_nodes => 4,
        num_cpus => 32,
        memory_mb => 65536,
        time_limit => 86400,
        account => <<"research">>,
        work_dir => <<"/scratch/admin/job12345">>,
        std_out => <<"/scratch/admin/job12345/out.log">>,
        std_err => <<"/scratch/admin/job12345/err.log">>
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(12345, maps:get(job_id, Result)),
    ?assertEqual(<<"full_job">>, maps:get(name, Result)),
    ?assertEqual(<<"admin">>, maps:get(user, Result)),
    ?assertEqual(<<"gpu">>, maps:get(partition, Result)),
    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual(4, maps:get(num_nodes, Result)),
    ?assertEqual(32, maps:get(num_cpus, Result)),
    ?assertEqual(65536, maps:get(memory_mb, Result)),
    ?assertEqual(86400, maps:get(time_limit, Result)),
    ?assertEqual(<<"research">>, maps:get(account, Result)),
    ?assertEqual(<<"/scratch/admin/job12345">>, maps:get(work_dir, Result)),
    ?assertEqual(<<"/scratch/admin/job12345/out.log">>, maps:get(std_out, Result)),
    ?assertEqual(<<"/scratch/admin/job12345/err.log">>, maps:get(std_err, Result)),
    ok.

test_job_to_info_gres() ->
    Job = make_job(#{
        gres => <<"gpu:4">>,
        gres_per_node => <<"gpu:1">>,
        gres_per_task => <<"gpu:1">>,
        gpu_type => <<"a100">>,
        gpu_memory_mb => 40960,
        gpu_exclusive => false
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(<<"gpu:4">>, maps:get(gres, Result)),
    ?assertEqual(<<"gpu:1">>, maps:get(gres_per_node, Result)),
    ?assertEqual(<<"gpu:1">>, maps:get(gres_per_task, Result)),
    ?assertEqual(<<"a100">>, maps:get(gpu_type, Result)),
    ?assertEqual(40960, maps:get(gpu_memory_mb, Result)),
    ?assertEqual(false, maps:get(gpu_exclusive, Result)),
    ok.

test_job_to_info_allocated() ->
    Job = make_job(#{
        state => running,
        allocated_nodes => [<<"node1">>, <<"node2">>, <<"node3">>]
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], maps:get(allocated_nodes, Result)),
    ok.

test_job_to_info_licenses() ->
    Job = make_job(#{
        licenses => [{<<"matlab">>, 2}, {<<"ansys">>, 1}]
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual([{<<"matlab">>, 2}, {<<"ansys">>, 1}], maps:get(licenses, Result)),
    ok.

test_job_to_info_running() ->
    SubmitTime = erlang:system_time(second) - 3600,
    StartTime = erlang:system_time(second) - 1800,
    Job = make_job(#{
        state => running,
        submit_time => SubmitTime,
        start_time => StartTime,
        allocated_nodes => [<<"compute-01">>]
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual([<<"compute-01">>], maps:get(allocated_nodes, Result)),
    ok.

test_job_to_info_workdir() ->
    Job = make_job(#{
        work_dir => <<"/home/user/project/run1">>
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(<<"/home/user/project/run1">>, maps:get(work_dir, Result)),
    ok.

test_job_to_info_output_paths() ->
    Job = make_job(#{
        std_out => <<"/logs/job-%j.out">>,
        std_err => <<"/logs/job-%j.err">>
    }),
    Result = flurm_scheduler:job_to_info(Job),

    ?assertEqual(<<"/logs/job-%j.out">>, maps:get(std_out, Result)),
    ?assertEqual(<<"/logs/job-%j.err">>, maps:get(std_err, Result)),
    ok.

%%====================================================================
%% job_to_limit_spec/1 Tests
%%====================================================================

job_to_limit_spec_test_() ->
    {"job_to_limit_spec/1 tests",
     [
        {"converts job to limit spec", fun test_job_to_limit_spec_basic/0},
        {"extracts user and account", fun test_job_to_limit_spec_user_account/0},
        {"extracts partition", fun test_job_to_limit_spec_partition/0},
        {"extracts resource requirements", fun test_job_to_limit_spec_resources/0},
        {"handles large resource requests", fun test_job_to_limit_spec_large/0},
        {"handles minimal resources", fun test_job_to_limit_spec_minimal/0}
     ]}.

test_job_to_limit_spec_basic() ->
    Job = make_job(),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assert(is_map(Result)),
    ?assert(maps:is_key(user, Result)),
    ?assert(maps:is_key(account, Result)),
    ?assert(maps:is_key(partition, Result)),
    ?assert(maps:is_key(num_nodes, Result)),
    ?assert(maps:is_key(num_cpus, Result)),
    ?assert(maps:is_key(memory_mb, Result)),
    ?assert(maps:is_key(time_limit, Result)),
    ok.

test_job_to_limit_spec_user_account() ->
    Job = make_job(#{
        user => <<"researcher">>,
        account => <<"physics">>
    }),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assertEqual(<<"researcher">>, maps:get(user, Result)),
    ?assertEqual(<<"physics">>, maps:get(account, Result)),
    ok.

test_job_to_limit_spec_partition() ->
    Job = make_job(#{
        partition => <<"high-mem">>
    }),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assertEqual(<<"high-mem">>, maps:get(partition, Result)),
    ok.

test_job_to_limit_spec_resources() ->
    Job = make_job(#{
        num_nodes => 8,
        num_cpus => 64,
        memory_mb => 262144,
        time_limit => 172800
    }),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assertEqual(8, maps:get(num_nodes, Result)),
    ?assertEqual(64, maps:get(num_cpus, Result)),
    ?assertEqual(262144, maps:get(memory_mb, Result)),
    ?assertEqual(172800, maps:get(time_limit, Result)),
    ok.

test_job_to_limit_spec_large() ->
    Job = make_job(#{
        num_nodes => 1024,
        num_cpus => 65536,
        memory_mb => 16777216,
        time_limit => 604800
    }),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assertEqual(1024, maps:get(num_nodes, Result)),
    ?assertEqual(65536, maps:get(num_cpus, Result)),
    ?assertEqual(16777216, maps:get(memory_mb, Result)),
    ?assertEqual(604800, maps:get(time_limit, Result)),
    ok.

test_job_to_limit_spec_minimal() ->
    Job = make_job(#{
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 64,
        time_limit => 60
    }),
    Result = flurm_scheduler:job_to_limit_spec(Job),

    ?assertEqual(1, maps:get(num_nodes, Result)),
    ?assertEqual(1, maps:get(num_cpus, Result)),
    ?assertEqual(64, maps:get(memory_mb, Result)),
    ?assertEqual(60, maps:get(time_limit, Result)),
    ok.

%%====================================================================
%% job_to_backfill_map/1 Tests
%%====================================================================

job_to_backfill_map_test_() ->
    {"job_to_backfill_map/1 tests",
     [
        {"converts job to backfill map", fun test_job_to_backfill_basic/0},
        {"includes job_id and name", fun test_job_to_backfill_identity/0},
        {"includes scheduling fields", fun test_job_to_backfill_scheduling/0},
        {"includes resource fields", fun test_job_to_backfill_resources/0},
        {"includes accounting fields", fun test_job_to_backfill_accounting/0},
        {"handles pending job", fun test_job_to_backfill_pending/0},
        {"handles high priority job", fun test_job_to_backfill_priority/0}
     ]}.

test_job_to_backfill_basic() ->
    Job = make_job(),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assert(is_map(Result)),
    ?assert(maps:is_key(job_id, Result)),
    ?assert(maps:is_key(name, Result)),
    ?assert(maps:is_key(user, Result)),
    ?assert(maps:is_key(partition, Result)),
    ?assert(maps:is_key(state, Result)),
    ok.

test_job_to_backfill_identity() ->
    Job = make_job(#{
        id => 54321,
        name => <<"backfill_test">>
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(54321, maps:get(job_id, Result)),
    ?assertEqual(<<"backfill_test">>, maps:get(name, Result)),
    ok.

test_job_to_backfill_scheduling() ->
    SubmitTime = erlang:system_time(second),
    Job = make_job(#{
        partition => <<"batch">>,
        state => pending,
        priority => 500,
        submit_time => SubmitTime,
        time_limit => 7200
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(<<"batch">>, maps:get(partition, Result)),
    ?assertEqual(pending, maps:get(state, Result)),
    ?assertEqual(500, maps:get(priority, Result)),
    ?assertEqual(SubmitTime, maps:get(submit_time, Result)),
    ?assertEqual(7200, maps:get(time_limit, Result)),
    ok.

test_job_to_backfill_resources() ->
    Job = make_job(#{
        num_nodes => 16,
        num_cpus => 128,
        memory_mb => 524288
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(16, maps:get(num_nodes, Result)),
    ?assertEqual(128, maps:get(num_cpus, Result)),
    ?assertEqual(524288, maps:get(memory_mb, Result)),
    ok.

test_job_to_backfill_accounting() ->
    Job = make_job(#{
        user => <<"scientist">>,
        account => <<"chemistry">>,
        qos => <<"high">>
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(<<"scientist">>, maps:get(user, Result)),
    ?assertEqual(<<"chemistry">>, maps:get(account, Result)),
    ?assertEqual(<<"high">>, maps:get(qos, Result)),
    ok.

test_job_to_backfill_pending() ->
    Job = make_job(#{
        state => pending,
        allocated_nodes => []
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(pending, maps:get(state, Result)),
    ok.

test_job_to_backfill_priority() ->
    Job = make_job(#{
        priority => 9999
    }),
    Result = flurm_scheduler:job_to_backfill_map(Job),

    ?assertEqual(9999, maps:get(priority, Result)),
    ok.

%%====================================================================
%% build_limit_info/1 Tests
%%====================================================================

build_limit_info_test_() ->
    {"build_limit_info/1 tests",
     [
        {"builds info from complete map", fun test_build_limit_info_complete/0},
        {"uses defaults for missing user", fun test_build_limit_info_default_user/0},
        {"uses defaults for missing account", fun test_build_limit_info_default_account/0},
        {"uses defaults for missing cpus", fun test_build_limit_info_default_cpus/0},
        {"uses defaults for missing memory", fun test_build_limit_info_default_mem/0},
        {"uses defaults for missing nodes", fun test_build_limit_info_default_nodes/0},
        {"handles empty map", fun test_build_limit_info_empty/0},
        {"builds TRES correctly", fun test_build_limit_info_tres/0}
     ]}.

test_build_limit_info_complete() ->
    JobInfo = #{
        user => <<"fulluser">>,
        account => <<"fullaccount">>,
        num_cpus => 16,
        memory_mb => 32768,
        num_nodes => 4
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    ?assert(is_map(Result)),
    ?assertEqual(<<"fulluser">>, maps:get(user, Result)),
    ?assertEqual(<<"fullaccount">>, maps:get(account, Result)),

    TRES = maps:get(tres, Result),
    ?assertEqual(16, maps:get(cpu, TRES)),
    ?assertEqual(32768, maps:get(mem, TRES)),
    ?assertEqual(4, maps:get(node, TRES)),
    ok.

test_build_limit_info_default_user() ->
    JobInfo = #{
        account => <<"someaccount">>,
        num_cpus => 8
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    ?assertEqual(<<"unknown">>, maps:get(user, Result)),
    ok.

test_build_limit_info_default_account() ->
    JobInfo = #{
        user => <<"someuser">>,
        num_cpus => 8
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    ?assertEqual(<<>>, maps:get(account, Result)),
    ok.

test_build_limit_info_default_cpus() ->
    JobInfo = #{
        user => <<"user">>,
        memory_mb => 1024
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    TRES = maps:get(tres, Result),
    ?assertEqual(1, maps:get(cpu, TRES)),
    ok.

test_build_limit_info_default_mem() ->
    JobInfo = #{
        user => <<"user">>,
        num_cpus => 4
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    TRES = maps:get(tres, Result),
    ?assertEqual(0, maps:get(mem, TRES)),
    ok.

test_build_limit_info_default_nodes() ->
    JobInfo = #{
        user => <<"user">>,
        num_cpus => 4
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    TRES = maps:get(tres, Result),
    ?assertEqual(1, maps:get(node, TRES)),
    ok.

test_build_limit_info_empty() ->
    Result = flurm_scheduler:build_limit_info(#{}),

    ?assertEqual(<<"unknown">>, maps:get(user, Result)),
    ?assertEqual(<<>>, maps:get(account, Result)),

    TRES = maps:get(tres, Result),
    ?assertEqual(1, maps:get(cpu, TRES)),
    ?assertEqual(0, maps:get(mem, TRES)),
    ?assertEqual(1, maps:get(node, TRES)),
    ok.

test_build_limit_info_tres() ->
    JobInfo = #{
        user => <<"tresuser">>,
        account => <<"tresaccount">>,
        num_cpus => 256,
        memory_mb => 1048576,
        num_nodes => 64
    },
    Result = flurm_scheduler:build_limit_info(JobInfo),

    TRES = maps:get(tres, Result),
    ?assert(is_map(TRES)),
    ?assertEqual(256, maps:get(cpu, TRES)),
    ?assertEqual(1048576, maps:get(mem, TRES)),
    ?assertEqual(64, maps:get(node, TRES)),
    ok.

%%====================================================================
%% calculate_resources_to_free/1 Tests
%%====================================================================

calculate_resources_to_free_test_() ->
    {"calculate_resources_to_free/1 tests",
     [
        {"handles empty list", fun test_calc_resources_empty/0},
        {"handles single job", fun test_calc_resources_single/0},
        {"handles multiple jobs", fun test_calc_resources_multiple/0},
        {"accumulates nodes correctly", fun test_calc_resources_nodes/0},
        {"accumulates cpus correctly", fun test_calc_resources_cpus/0},
        {"accumulates memory correctly", fun test_calc_resources_memory/0},
        {"handles missing fields with defaults", fun test_calc_resources_defaults/0},
        {"handles large scale jobs", fun test_calc_resources_large/0}
     ]}.

test_calc_resources_empty() ->
    Result = flurm_scheduler:calculate_resources_to_free([]),

    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(nodes, Result)),
    ?assertEqual(0, maps:get(cpus, Result)),
    ?assertEqual(0, maps:get(memory_mb, Result)),
    ok.

test_calc_resources_single() ->
    Jobs = [#{
        num_nodes => 2,
        num_cpus => 16,
        memory_mb => 8192
    }],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(2, maps:get(nodes, Result)),
    ?assertEqual(16, maps:get(cpus, Result)),
    ?assertEqual(8192, maps:get(memory_mb, Result)),
    ok.

test_calc_resources_multiple() ->
    Jobs = [
        #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
        #{num_nodes => 2, num_cpus => 8, memory_mb => 2048},
        #{num_nodes => 4, num_cpus => 16, memory_mb => 4096}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(7, maps:get(nodes, Result)),   % 1+2+4
    ?assertEqual(28, maps:get(cpus, Result)),   % 4+8+16
    ?assertEqual(7168, maps:get(memory_mb, Result)),  % 1024+2048+4096
    ok.

test_calc_resources_nodes() ->
    Jobs = [
        #{num_nodes => 10, num_cpus => 1, memory_mb => 100},
        #{num_nodes => 20, num_cpus => 1, memory_mb => 100},
        #{num_nodes => 30, num_cpus => 1, memory_mb => 100}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(60, maps:get(nodes, Result)),
    ok.

test_calc_resources_cpus() ->
    Jobs = [
        #{num_nodes => 1, num_cpus => 100, memory_mb => 100},
        #{num_nodes => 1, num_cpus => 200, memory_mb => 100},
        #{num_nodes => 1, num_cpus => 300, memory_mb => 100}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(600, maps:get(cpus, Result)),
    ok.

test_calc_resources_memory() ->
    Jobs = [
        #{num_nodes => 1, num_cpus => 1, memory_mb => 10000},
        #{num_nodes => 1, num_cpus => 1, memory_mb => 20000},
        #{num_nodes => 1, num_cpus => 1, memory_mb => 30000}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(60000, maps:get(memory_mb, Result)),
    ok.

test_calc_resources_defaults() ->
    %% Jobs without fields should use defaults (1 for nodes/cpus, 1024 for memory)
    Jobs = [
        #{},  % All defaults
        #{num_nodes => 2},  % Only nodes specified
        #{num_cpus => 8}    % Only cpus specified
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    %% Defaults: num_nodes=1, num_cpus=1, memory_mb=1024
    ?assertEqual(4, maps:get(nodes, Result)),    % 1+2+1
    ?assertEqual(10, maps:get(cpus, Result)),    % 1+1+8
    ?assertEqual(3072, maps:get(memory_mb, Result)),  % 1024+1024+1024
    ok.

test_calc_resources_large() ->
    %% Large scale cluster jobs
    Jobs = [
        #{num_nodes => 512, num_cpus => 16384, memory_mb => 4194304},
        #{num_nodes => 256, num_cpus => 8192, memory_mb => 2097152}
    ],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(768, maps:get(nodes, Result)),
    ?assertEqual(24576, maps:get(cpus, Result)),
    ?assertEqual(6291456, maps:get(memory_mb, Result)),
    ok.

%%====================================================================
%% remove_jobs_from_queue/2 Tests
%%====================================================================

remove_jobs_from_queue_test_() ->
    {"remove_jobs_from_queue/2 tests",
     [
        {"handles empty jobs list", fun test_remove_jobs_empty_list/0},
        {"handles empty queue", fun test_remove_jobs_empty_queue/0},
        {"removes single job", fun test_remove_jobs_single/0},
        {"removes multiple jobs", fun test_remove_jobs_multiple/0},
        {"handles non-existing jobs", fun test_remove_jobs_nonexistent/0},
        {"preserves order", fun test_remove_jobs_order/0},
        {"handles mixed existing and non-existing", fun test_remove_jobs_mixed/0},
        {"removes all jobs", fun test_remove_jobs_all/0},
        {"handles duplicates in removal list", fun test_remove_jobs_duplicates/0}
     ]}.

test_remove_jobs_empty_list() ->
    Queue = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([], Queue),

    ?assertEqual([1, 2, 3, 4, 5], queue:to_list(Result)),
    ok.

test_remove_jobs_empty_queue() ->
    Queue = queue:new(),
    Result = flurm_scheduler:remove_jobs_from_queue([1, 2, 3], Queue),

    ?assertEqual([], queue:to_list(Result)),
    ok.

test_remove_jobs_single() ->
    Queue = queue:from_list([1, 2, 3, 4, 5]),
    Result = flurm_scheduler:remove_jobs_from_queue([3], Queue),

    ?assertEqual([1, 2, 4, 5], queue:to_list(Result)),
    ok.

test_remove_jobs_multiple() ->
    Queue = queue:from_list([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    Result = flurm_scheduler:remove_jobs_from_queue([2, 4, 6, 8, 10], Queue),

    ?assertEqual([1, 3, 5, 7, 9], queue:to_list(Result)),
    ok.

test_remove_jobs_nonexistent() ->
    Queue = queue:from_list([1, 2, 3]),
    Result = flurm_scheduler:remove_jobs_from_queue([100, 200, 300], Queue),

    ?assertEqual([1, 2, 3], queue:to_list(Result)),
    ok.

test_remove_jobs_order() ->
    Queue = queue:from_list([10, 20, 30, 40, 50]),
    Result = flurm_scheduler:remove_jobs_from_queue([30], Queue),

    ResultList = queue:to_list(Result),
    ?assertEqual([10, 20, 40, 50], ResultList),

    %% Verify order is preserved
    ?assertEqual(10, lists:nth(1, ResultList)),
    ?assertEqual(20, lists:nth(2, ResultList)),
    ?assertEqual(40, lists:nth(3, ResultList)),
    ?assertEqual(50, lists:nth(4, ResultList)),
    ok.

test_remove_jobs_mixed() ->
    Queue = queue:from_list([1, 2, 3, 4, 5]),
    %% Remove 2 and 4 (exist) and 100 (doesn't exist)
    Result = flurm_scheduler:remove_jobs_from_queue([2, 4, 100], Queue),

    ?assertEqual([1, 3, 5], queue:to_list(Result)),
    ok.

test_remove_jobs_all() ->
    Queue = queue:from_list([1, 2, 3]),
    Result = flurm_scheduler:remove_jobs_from_queue([1, 2, 3], Queue),

    ?assertEqual([], queue:to_list(Result)),
    ?assert(queue:is_empty(Result)),
    ok.

test_remove_jobs_duplicates() ->
    Queue = queue:from_list([1, 2, 3, 4, 5]),
    %% Removal list has duplicates
    Result = flurm_scheduler:remove_jobs_from_queue([2, 2, 4, 4], Queue),

    ?assertEqual([1, 3, 5], queue:to_list(Result)),
    ok.

%%====================================================================
%% Integration Tests - Combining Functions
%%====================================================================

integration_test_() ->
    {"Integration tests combining multiple functions",
     [
        {"job conversion chain", fun test_integration_job_chain/0},
        {"queue manipulation with job info", fun test_integration_queue_jobs/0}
     ]}.

test_integration_job_chain() ->
    %% Create a job and convert it through all conversion functions
    Job = make_job(#{
        id => 999,
        name => <<"integration_job">>,
        user => <<"integration_user">>,
        account => <<"integration_acct">>,
        partition => <<"compute">>,
        num_nodes => 8,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 14400,
        priority => 200
    }),

    %% Convert to info map
    InfoMap = flurm_scheduler:job_to_info(Job),
    ?assert(is_map(InfoMap)),
    ?assertEqual(999, maps:get(job_id, InfoMap)),

    %% Convert to limit spec
    LimitSpec = flurm_scheduler:job_to_limit_spec(Job),
    ?assert(is_map(LimitSpec)),
    ?assertEqual(<<"integration_user">>, maps:get(user, LimitSpec)),

    %% Convert to backfill map
    BackfillMap = flurm_scheduler:job_to_backfill_map(Job),
    ?assert(is_map(BackfillMap)),
    ?assertEqual(999, maps:get(job_id, BackfillMap)),

    %% Build limit info from the info map
    LimitInfo = flurm_scheduler:build_limit_info(InfoMap),
    ?assert(is_map(LimitInfo)),
    ?assertEqual(<<"integration_user">>, maps:get(user, LimitInfo)),

    TRES = maps:get(tres, LimitInfo),
    ?assertEqual(64, maps:get(cpu, TRES)),
    ok.

test_integration_queue_jobs() ->
    %% Create a queue of jobs
    Queue = queue:from_list([101, 102, 103, 104, 105]),

    %% Create job maps for resource calculation
    JobMaps = [
        #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
        #{num_nodes => 2, num_cpus => 8, memory_mb => 2048}
    ],

    %% Calculate resources
    Resources = flurm_scheduler:calculate_resources_to_free(JobMaps),
    ?assertEqual(3, maps:get(nodes, Resources)),
    ?assertEqual(12, maps:get(cpus, Resources)),

    %% Remove some jobs from queue
    NewQueue = flurm_scheduler:remove_jobs_from_queue([102, 104], Queue),
    ?assertEqual([101, 103, 105], queue:to_list(NewQueue)),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {"Edge case tests",
     [
        {"job with very long name", fun test_edge_long_name/0},
        {"job with unicode characters", fun test_edge_unicode/0},
        {"job with extreme priority", fun test_edge_extreme_priority/0},
        {"queue with many jobs", fun test_edge_large_queue/0},
        {"many jobs to calculate resources", fun test_edge_many_jobs/0}
     ]}.

test_edge_long_name() ->
    LongName = list_to_binary(lists:duplicate(1000, $a)),
    Job = make_job(#{name => LongName}),

    InfoMap = flurm_scheduler:job_to_info(Job),
    ?assertEqual(LongName, maps:get(name, InfoMap)),

    BackfillMap = flurm_scheduler:job_to_backfill_map(Job),
    ?assertEqual(LongName, maps:get(name, BackfillMap)),
    ok.

test_edge_unicode() ->
    UnicodeName = <<"job_\x{03B1}\x{03B2}\x{03B3}"/utf8>>,
    Job = make_job(#{name => UnicodeName}),

    InfoMap = flurm_scheduler:job_to_info(Job),
    ?assertEqual(UnicodeName, maps:get(name, InfoMap)),
    ok.

test_edge_extreme_priority() ->
    %% Test with maximum priority
    Job = make_job(#{priority => 10000}),
    BackfillMap = flurm_scheduler:job_to_backfill_map(Job),
    ?assertEqual(10000, maps:get(priority, BackfillMap)),

    %% Test with minimum priority
    Job2 = make_job(#{priority => 0}),
    BackfillMap2 = flurm_scheduler:job_to_backfill_map(Job2),
    ?assertEqual(0, maps:get(priority, BackfillMap2)),
    ok.

test_edge_large_queue() ->
    %% Create a queue with many jobs
    JobIds = lists:seq(1, 1000),
    Queue = queue:from_list(JobIds),

    %% Remove every 10th job
    ToRemove = [I || I <- JobIds, I rem 10 =:= 0],
    Result = flurm_scheduler:remove_jobs_from_queue(ToRemove, Queue),

    Expected = [I || I <- JobIds, I rem 10 =/= 0],
    ?assertEqual(Expected, queue:to_list(Result)),
    ?assertEqual(900, length(queue:to_list(Result))),
    ok.

test_edge_many_jobs() ->
    %% Calculate resources for many jobs
    Jobs = [#{num_nodes => 1, num_cpus => 4, memory_mb => 1024} || _ <- lists:seq(1, 100)],
    Result = flurm_scheduler:calculate_resources_to_free(Jobs),

    ?assertEqual(100, maps:get(nodes, Result)),
    ?assertEqual(400, maps:get(cpus, Result)),
    ?assertEqual(102400, maps:get(memory_mb, Result)),
    ok.

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    {"Stress tests",
     [
        {"rapid job conversions", fun test_stress_conversions/0},
        {"rapid queue operations", fun test_stress_queue_ops/0}
     ]}.

test_stress_conversions() ->
    %% Rapidly convert many jobs
    Jobs = [make_job(#{id => I}) || I <- lists:seq(1, 100)],

    lists:foreach(fun(Job) ->
        _ = flurm_scheduler:job_to_info(Job),
        _ = flurm_scheduler:job_to_limit_spec(Job),
        _ = flurm_scheduler:job_to_backfill_map(Job)
    end, Jobs),
    ok.

test_stress_queue_ops() ->
    %% Rapidly manipulate queues
    lists:foreach(fun(N) ->
        Queue = queue:from_list(lists:seq(1, N * 10)),
        ToRemove = lists:seq(1, N),
        _ = flurm_scheduler:remove_jobs_from_queue(ToRemove, Queue)
    end, lists:seq(1, 50)),
    ok.
