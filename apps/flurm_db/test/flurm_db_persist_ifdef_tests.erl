%%%-------------------------------------------------------------------
%%% @doc Tests for internal functions exported via -ifdef(TEST)
%%% in flurm_db_persist module.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_persist_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% ra_job_to_job/1 Tests
%%====================================================================

ra_job_to_job_test_() ->
    {"ra_job_to_job/1 converts #ra_job to #job correctly",
     [
      {"converts basic fields correctly",
       fun() ->
           RaJob = #ra_job{
               id = 42,
               name = <<"test_job">>,
               user = <<"alice">>,
               group = <<"users">>,
               partition = <<"default">>,
               state = running,
               script = <<"#!/bin/bash\necho hello">>,
               num_nodes = 2,
               num_cpus = 4,
               memory_mb = 1024,
               time_limit = 3600,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [<<"node1">>, <<"node2">>],
               exit_code = undefined
           },
           Job = flurm_db_persist:ra_job_to_job(RaJob),
           ?assertEqual(42, Job#job.id),
           ?assertEqual(<<"test_job">>, Job#job.name),
           ?assertEqual(<<"alice">>, Job#job.user),
           ?assertEqual(<<"default">>, Job#job.partition),
           ?assertEqual(running, Job#job.state),
           ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
           ?assertEqual(2, Job#job.num_nodes),
           ?assertEqual(4, Job#job.num_cpus),
           ?assertEqual(1024, Job#job.memory_mb),
           ?assertEqual(3600, Job#job.time_limit),
           ?assertEqual(100, Job#job.priority),
           ?assertEqual(1700000000, Job#job.submit_time),
           ?assertEqual(1700000100, Job#job.start_time),
           ?assertEqual(undefined, Job#job.end_time),
           ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
           ?assertEqual(undefined, Job#job.exit_code)
       end},

      {"sets default account and qos",
       fun() ->
           RaJob = #ra_job{
               id = 1,
               name = <<"job">>,
               user = <<"bob">>,
               group = <<"devs">>,
               partition = <<"compute">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 50,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Job = flurm_db_persist:ra_job_to_job(RaJob),
           ?assertEqual(<<>>, Job#job.account),
           ?assertEqual(<<"normal">>, Job#job.qos)
       end},

      {"handles completed job with exit code",
       fun() ->
           RaJob = #ra_job{
               id = 99,
               name = <<"finished_job">>,
               user = <<"charlie">>,
               group = <<"users">>,
               partition = <<"batch">>,
               state = completed,
               script = <<"exit 0">>,
               num_nodes = 1,
               num_cpus = 8,
               memory_mb = 4096,
               time_limit = 7200,
               priority = 200,
               submit_time = 1700000000,
               start_time = 1700000500,
               end_time = 1700001000,
               allocated_nodes = [<<"worker01">>],
               exit_code = 0
           },
           Job = flurm_db_persist:ra_job_to_job(RaJob),
           ?assertEqual(completed, Job#job.state),
           ?assertEqual(1700001000, Job#job.end_time),
           ?assertEqual(0, Job#job.exit_code)
       end},

      {"handles failed job with non-zero exit code",
       fun() ->
           RaJob = #ra_job{
               id = 100,
               name = <<"failed_job">>,
               user = <<"dave">>,
               group = <<"users">>,
               partition = <<"debug">>,
               state = failed,
               script = <<"exit 1">>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 256,
               time_limit = 60,
               priority = 10,
               submit_time = 1700000000,
               start_time = 1700000010,
               end_time = 1700000020,
               allocated_nodes = [<<"node1">>],
               exit_code = 127
           },
           Job = flurm_db_persist:ra_job_to_job(RaJob),
           ?assertEqual(failed, Job#job.state),
           ?assertEqual(127, Job#job.exit_code)
       end}
     ]}.

%%====================================================================
%% apply_job_updates/2 Tests
%%====================================================================

apply_job_updates_test_() ->
    {"apply_job_updates/2 updates job records correctly",
     [
      {"updates state to running and sets start_time",
       fun() ->
           Job = #job{
               id = 1,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{state => running},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(running, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.start_time)
       end},

      {"updates state to completed and sets end_time",
       fun() ->
           Job = #job{
               id = 2,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [<<"node1">>],
               exit_code = undefined
           },
           Updates = #{state => completed},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(completed, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.end_time)
       end},

      {"updates state to failed and sets end_time",
       fun() ->
           Job = #job{
               id = 3,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [<<"node1">>],
               exit_code = undefined
           },
           Updates = #{state => failed},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(failed, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.end_time)
       end},

      {"updates state to cancelled and sets end_time",
       fun() ->
           Job = #job{
               id = 4,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{state => cancelled},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(cancelled, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.end_time)
       end},

      {"updates state to timeout and sets end_time",
       fun() ->
           Job = #job{
               id = 5,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [<<"node1">>],
               exit_code = undefined
           },
           Updates = #{state => timeout},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(timeout, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.end_time)
       end},

      {"updates state to node_fail and sets end_time",
       fun() ->
           Job = #job{
               id = 6,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [<<"node1">>],
               exit_code = undefined
           },
           Updates = #{state => node_fail},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(node_fail, Updated#job.state),
           ?assertNotEqual(undefined, Updated#job.end_time)
       end},

      {"does not reset start_time when already running",
       fun() ->
           OriginalStartTime = 1700000100,
           Job = #job{
               id = 7,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = OriginalStartTime,
               end_time = undefined,
               allocated_nodes = [<<"node1">>],
               exit_code = undefined
           },
           Updates = #{state => running},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(running, Updated#job.state),
           ?assertEqual(OriginalStartTime, Updated#job.start_time)
       end},

      {"updates allocated_nodes",
       fun() ->
           Job = #job{
               id = 8,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 2,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{allocated_nodes => [<<"node1">>, <<"node2">>]},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual([<<"node1">>, <<"node2">>], Updated#job.allocated_nodes)
       end},

      {"updates start_time explicitly",
       fun() ->
           Job = #job{
               id = 9,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           NewStartTime = 1700000500,
           Updates = #{start_time => NewStartTime},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(NewStartTime, Updated#job.start_time)
       end},

      {"updates end_time explicitly",
       fun() ->
           Job = #job{
               id = 10,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = running,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           NewEndTime = 1700001000,
           Updates = #{end_time => NewEndTime},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(NewEndTime, Updated#job.end_time)
       end},

      {"updates exit_code",
       fun() ->
           Job = #job{
               id = 11,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = completed,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = 1700000100,
               end_time = 1700001000,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{exit_code => 0},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(0, Updated#job.exit_code)
       end},

      {"ignores unknown fields",
       fun() ->
           Job = #job{
               id = 12,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{unknown_field => <<"ignored">>},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(Job, Updated)
       end},

      {"applies multiple updates at once",
       fun() ->
           Job = #job{
               id = 13,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{
               state => running,
               allocated_nodes => [<<"node1">>]
           },
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(running, Updated#job.state),
           ?assertEqual([<<"node1">>], Updated#job.allocated_nodes),
           ?assertNotEqual(undefined, Updated#job.start_time)
       end},

      {"handles empty updates map",
       fun() ->
           Job = #job{
               id = 14,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(Job, Updated)
       end},

      {"updates configuring state without setting times",
       fun() ->
           Job = #job{
               id = 15,
               name = <<"test">>,
               user = <<"user">>,
               partition = <<"default">>,
               state = pending,
               script = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               memory_mb = 512,
               time_limit = 60,
               priority = 100,
               submit_time = 1700000000,
               start_time = undefined,
               end_time = undefined,
               allocated_nodes = [],
               exit_code = undefined
           },
           Updates = #{state => configuring},
           Updated = flurm_db_persist:apply_job_updates(Job, Updates),
           ?assertEqual(configuring, Updated#job.state),
           ?assertEqual(undefined, Updated#job.start_time),
           ?assertEqual(undefined, Updated#job.end_time)
       end}
     ]}.
