%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_scheduler internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure calculation functions that are exported
%%% only when compiled with -DTEST.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

sample_job() ->
    #job{
        id = 1001,
        name = <<"test_job">>,
        user = <<"testuser">>,
        account = <<"testaccount">>,
        partition = <<"compute">>,
        state = pending,
        num_nodes = 2,
        num_cpus = 4,
        memory_mb = 2048,
        time_limit = 3600,
        script = <<"/bin/bash -c 'echo hello'">>,
        allocated_nodes = [],
        work_dir = <<"/home/testuser">>,
        std_out = <<"/home/testuser/job.out">>,
        std_err = <<"/home/testuser/job.err">>,
        licenses = [{<<"matlab">>, 2}],
        gres = <<"gpu:1">>,
        gres_per_node = 1,
        gres_per_task = 0,
        gpu_type = <<"a100">>,
        gpu_memory_mb = 40000,
        gpu_exclusive = true,
        submit_time = erlang:system_time(second),
        priority = 1000,
        qos = <<"normal">>
    }.

%%====================================================================
%% job_to_info/1 Tests
%%====================================================================

job_to_info_test_() ->
    {"job_to_info/1 converts job record to map",
     [
      {"converts basic fields",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual(1001, maps:get(job_id, Info)),
           ?assertEqual(<<"test_job">>, maps:get(name, Info)),
           ?assertEqual(pending, maps:get(state, Info)),
           ?assertEqual(<<"compute">>, maps:get(partition, Info))
       end},

      {"converts resource requirements",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual(2, maps:get(num_nodes, Info)),
           ?assertEqual(4, maps:get(num_cpus, Info)),
           ?assertEqual(2048, maps:get(memory_mb, Info)),
           ?assertEqual(3600, maps:get(time_limit, Info))
       end},

      {"converts user info",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual(<<"testuser">>, maps:get(user, Info)),
           ?assertEqual(<<"testaccount">>, maps:get(account, Info))
       end},

      {"converts GRES fields",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual(<<"gpu:1">>, maps:get(gres, Info)),
           ?assertEqual(1, maps:get(gres_per_node, Info)),
           ?assertEqual(<<"a100">>, maps:get(gpu_type, Info)),
           ?assertEqual(40000, maps:get(gpu_memory_mb, Info)),
           ?assertEqual(true, maps:get(gpu_exclusive, Info))
       end},

      {"converts paths and script",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual(<<"/home/testuser">>, maps:get(work_dir, Info)),
           ?assertEqual(<<"/home/testuser/job.out">>, maps:get(std_out, Info)),
           ?assertEqual(<<"/home/testuser/job.err">>, maps:get(std_err, Info)),
           ?assertEqual(<<"/bin/bash -c 'echo hello'">>, maps:get(script, Info))
       end},

      {"converts licenses",
       fun() ->
           Job = sample_job(),
           Info = flurm_scheduler:job_to_info(Job),
           ?assertEqual([{<<"matlab">>, 2}], maps:get(licenses, Info))
       end},

      {"handles job with no GRES",
       fun() ->
           Job = sample_job(),
           JobNoGres = Job#job{gres = <<>>, gpu_type = undefined},
           Info = flurm_scheduler:job_to_info(JobNoGres),
           ?assertEqual(<<>>, maps:get(gres, Info)),
           ?assertEqual(undefined, maps:get(gpu_type, Info))
       end}
     ]}.

%%====================================================================
%% job_to_limit_spec/1 Tests
%%====================================================================

job_to_limit_spec_test_() ->
    {"job_to_limit_spec/1 creates limit check specification",
     [
      {"includes user and account",
       fun() ->
           Job = sample_job(),
           Spec = flurm_scheduler:job_to_limit_spec(Job),
           ?assertEqual(<<"testuser">>, maps:get(user, Spec)),
           ?assertEqual(<<"testaccount">>, maps:get(account, Spec))
       end},

      {"includes partition",
       fun() ->
           Job = sample_job(),
           Spec = flurm_scheduler:job_to_limit_spec(Job),
           ?assertEqual(<<"compute">>, maps:get(partition, Spec))
       end},

      {"includes resource requirements",
       fun() ->
           Job = sample_job(),
           Spec = flurm_scheduler:job_to_limit_spec(Job),
           ?assertEqual(2, maps:get(num_nodes, Spec)),
           ?assertEqual(4, maps:get(num_cpus, Spec)),
           ?assertEqual(2048, maps:get(memory_mb, Spec)),
           ?assertEqual(3600, maps:get(time_limit, Spec))
       end},

      {"handles minimal job",
       fun() ->
           Job = #job{
               id = 1,
               user = <<"minuser">>,
               account = <<>>,
               partition = <<"default">>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 256,
               time_limit = 60
           },
           Spec = flurm_scheduler:job_to_limit_spec(Job),
           ?assertEqual(<<"minuser">>, maps:get(user, Spec)),
           ?assertEqual(1, maps:get(num_nodes, Spec))
       end}
     ]}.

%%====================================================================
%% job_to_backfill_map/1 Tests
%%====================================================================

job_to_backfill_map_test_() ->
    {"job_to_backfill_map/1 creates backfill processing map",
     [
      {"includes job identification",
       fun() ->
           Job = sample_job(),
           Map = flurm_scheduler:job_to_backfill_map(Job),
           ?assertEqual(1001, maps:get(job_id, Map)),
           ?assertEqual(<<"test_job">>, maps:get(name, Map)),
           ?assertEqual(<<"testuser">>, maps:get(user, Map))
       end},

      {"includes scheduling info",
       fun() ->
           Job = sample_job(),
           Map = flurm_scheduler:job_to_backfill_map(Job),
           ?assertEqual(pending, maps:get(state, Map)),
           ?assertEqual(1000, maps:get(priority, Map)),
           ?assertEqual(<<"normal">>, maps:get(qos, Map))
       end},

      {"includes resource requirements",
       fun() ->
           Job = sample_job(),
           Map = flurm_scheduler:job_to_backfill_map(Job),
           ?assertEqual(2, maps:get(num_nodes, Map)),
           ?assertEqual(4, maps:get(num_cpus, Map)),
           ?assertEqual(2048, maps:get(memory_mb, Map)),
           ?assertEqual(3600, maps:get(time_limit, Map))
       end},

      {"includes account info",
       fun() ->
           Job = sample_job(),
           Map = flurm_scheduler:job_to_backfill_map(Job),
           ?assertEqual(<<"testaccount">>, maps:get(account, Map)),
           ?assertEqual(<<"compute">>, maps:get(partition, Map))
       end}
     ]}.

%%====================================================================
%% build_limit_info/1 Tests
%%====================================================================

build_limit_info_test_() ->
    {"build_limit_info/1 creates limit enforcement info",
     [
      {"includes user and account",
       fun() ->
           Info = #{
               user => <<"testuser">>,
               account => <<"testaccount">>,
               num_cpus => 4,
               memory_mb => 2048,
               num_nodes => 2
           },
           LimitInfo = flurm_scheduler:build_limit_info(Info),
           ?assertEqual(<<"testuser">>, maps:get(user, LimitInfo)),
           ?assertEqual(<<"testaccount">>, maps:get(account, LimitInfo))
       end},

      {"includes TRES (trackable resources)",
       fun() ->
           Info = #{
               user => <<"testuser">>,
               account => <<>>,
               num_cpus => 8,
               memory_mb => 4096,
               num_nodes => 4
           },
           LimitInfo = flurm_scheduler:build_limit_info(Info),
           Tres = maps:get(tres, LimitInfo),
           ?assertEqual(8, maps:get(cpu, Tres)),
           ?assertEqual(4096, maps:get(mem, Tres)),
           ?assertEqual(4, maps:get(node, Tres))
       end},

      {"handles missing fields with defaults",
       fun() ->
           Info = #{},
           LimitInfo = flurm_scheduler:build_limit_info(Info),
           ?assertEqual(<<"unknown">>, maps:get(user, LimitInfo)),
           ?assertEqual(<<>>, maps:get(account, LimitInfo)),
           Tres = maps:get(tres, LimitInfo),
           ?assertEqual(1, maps:get(cpu, Tres)),
           ?assertEqual(0, maps:get(mem, Tres)),
           ?assertEqual(1, maps:get(node, Tres))
       end}
     ]}.

%%====================================================================
%% calculate_resources_to_free/1 Tests
%%====================================================================

calculate_resources_to_free_test_() ->
    {"calculate_resources_to_free/1 sums resources from jobs",
     [
      {"handles empty list",
       fun() ->
           Result = flurm_scheduler:calculate_resources_to_free([]),
           ?assertEqual(0, maps:get(nodes, Result)),
           ?assertEqual(0, maps:get(cpus, Result)),
           ?assertEqual(0, maps:get(memory_mb, Result))
       end},

      {"calculates single job resources",
       fun() ->
           Jobs = [#{num_nodes => 2, num_cpus => 8, memory_mb => 4096}],
           Result = flurm_scheduler:calculate_resources_to_free(Jobs),
           ?assertEqual(2, maps:get(nodes, Result)),
           ?assertEqual(8, maps:get(cpus, Result)),
           ?assertEqual(4096, maps:get(memory_mb, Result))
       end},

      {"sums multiple job resources",
       fun() ->
           Jobs = [
               #{num_nodes => 2, num_cpus => 8, memory_mb => 4096},
               #{num_nodes => 4, num_cpus => 16, memory_mb => 8192},
               #{num_nodes => 1, num_cpus => 4, memory_mb => 2048}
           ],
           Result = flurm_scheduler:calculate_resources_to_free(Jobs),
           ?assertEqual(7, maps:get(nodes, Result)),
           ?assertEqual(28, maps:get(cpus, Result)),
           ?assertEqual(14336, maps:get(memory_mb, Result))
       end},

      {"uses defaults for missing fields",
       fun() ->
           Jobs = [#{}, #{num_nodes => 2}],
           Result = flurm_scheduler:calculate_resources_to_free(Jobs),
           ?assertEqual(3, maps:get(nodes, Result)),  % 1 default + 2
           ?assertEqual(2, maps:get(cpus, Result)),   % 1 default + 1 default
           ?assertEqual(2048, maps:get(memory_mb, Result))  % 1024 default + 1024 default
       end}
     ]}.

%%====================================================================
%% remove_jobs_from_queue/2 Tests
%%====================================================================

remove_jobs_from_queue_test_() ->
    {"remove_jobs_from_queue/2 filters jobs from queue",
     [
      {"handles empty removal list",
       fun() ->
           Queue = queue:from_list([1, 2, 3, 4, 5]),
           Result = flurm_scheduler:remove_jobs_from_queue([], Queue),
           ?assertEqual([1, 2, 3, 4, 5], queue:to_list(Result))
       end},

      {"removes single job",
       fun() ->
           Queue = queue:from_list([1, 2, 3, 4, 5]),
           Result = flurm_scheduler:remove_jobs_from_queue([3], Queue),
           ?assertEqual([1, 2, 4, 5], queue:to_list(Result))
       end},

      {"removes multiple jobs",
       fun() ->
           Queue = queue:from_list([1, 2, 3, 4, 5, 6, 7]),
           Result = flurm_scheduler:remove_jobs_from_queue([2, 4, 6], Queue),
           ?assertEqual([1, 3, 5, 7], queue:to_list(Result))
       end},

      {"handles job not in queue",
       fun() ->
           Queue = queue:from_list([1, 2, 3]),
           Result = flurm_scheduler:remove_jobs_from_queue([99], Queue),
           ?assertEqual([1, 2, 3], queue:to_list(Result))
       end},

      {"handles empty queue",
       fun() ->
           Queue = queue:new(),
           Result = flurm_scheduler:remove_jobs_from_queue([1, 2], Queue),
           ?assertEqual([], queue:to_list(Result))
       end},

      {"removes all jobs",
       fun() ->
           Queue = queue:from_list([1, 2, 3]),
           Result = flurm_scheduler:remove_jobs_from_queue([1, 2, 3], Queue),
           ?assertEqual([], queue:to_list(Result))
       end}
     ]}.
