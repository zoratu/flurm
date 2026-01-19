%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_backfill internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure calculation functions that are exported
%%% only when compiled with -DTEST.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

sample_timeline() ->
    Now = erlang:system_time(second),
    #{
        timestamp => Now,
        free_nodes => [
            {<<"node01">>, 32, 65536},
            {<<"node02">>, 16, 32768},
            {<<"node03">>, 8, 16384}
        ],
        busy_nodes => [
            {<<"node04">>, 0, 0},
            {<<"node05">>, 0, 0}
        ],
        node_end_times => [
            {<<"node04">>, Now + 3600},
            {<<"node05">>, Now + 7200}
        ],
        total_nodes => 5
    }.

sample_job_map(JobId, Priority, NumNodes, NumCpus, MemoryMb, TimeLimit) ->
    #{
        job_id => JobId,
        pid => undefined,
        priority => Priority,
        num_nodes => NumNodes,
        num_cpus => NumCpus,
        memory_mb => MemoryMb,
        time_limit => TimeLimit
    }.

sample_job_record() ->
    #job{
        id = 1001,
        name = <<"backfill_test">>,
        user = <<"testuser">>,
        account = <<"testaccount">>,
        partition = <<"compute">>,
        state = pending,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 1800,
        priority = 500,
        submit_time = erlang:system_time(second) - 300,
        start_time = undefined,
        qos = <<"normal">>
    }.

%%====================================================================
%% timestamp_to_seconds/1 Tests
%%====================================================================

timestamp_to_seconds_test_() ->
    {"timestamp_to_seconds/1 converts various formats",
     [
      {"converts erlang:now() format (MegaSecs, Secs, MicroSecs)",
       fun() ->
           Timestamp = {1700, 000000, 123456},
           Result = flurm_backfill:timestamp_to_seconds(Timestamp),
           ?assertEqual(1700000000, Result)
       end},

      {"handles integer seconds directly",
       fun() ->
           Timestamp = 1700000000,
           Result = flurm_backfill:timestamp_to_seconds(Timestamp),
           ?assertEqual(1700000000, Result)
       end},

      {"handles invalid format with current time",
       fun() ->
           Before = erlang:system_time(second),
           Result = flurm_backfill:timestamp_to_seconds(invalid),
           After = erlang:system_time(second),
           ?assert(Result >= Before),
           ?assert(Result =< After)
       end},

      {"handles zero timestamp",
       fun() ->
           Result = flurm_backfill:timestamp_to_seconds({0, 0, 0}),
           ?assertEqual(0, Result)
       end},

      {"handles large integer timestamp",
       fun() ->
           Timestamp = 2000000000,
           Result = flurm_backfill:timestamp_to_seconds(Timestamp),
           ?assertEqual(2000000000, Result)
       end}
     ]}.

%%====================================================================
%% find_nth_end_time/3 Tests
%%====================================================================

find_nth_end_time_test_() ->
    {"find_nth_end_time/3 finds when Nth node becomes free",
     [
      {"returns default for empty list",
       fun() ->
           Default = 9999999,
           Result = flurm_backfill:find_nth_end_time([], 1, Default),
           ?assertEqual(Default, Result)
       end},

      {"finds first end time",
       fun() ->
           EndTimes = [{<<"node1">>, 1000}, {<<"node2">>, 2000}, {<<"node3">>, 3000}],
           Result = flurm_backfill:find_nth_end_time(EndTimes, 1, 9999),
           ?assertEqual(1000, Result)
       end},

      {"finds second end time",
       fun() ->
           EndTimes = [{<<"node1">>, 1000}, {<<"node2">>, 2000}, {<<"node3">>, 3000}],
           Result = flurm_backfill:find_nth_end_time(EndTimes, 2, 9999),
           ?assertEqual(2000, Result)
       end},

      {"finds third end time",
       fun() ->
           EndTimes = [{<<"node1">>, 1000}, {<<"node2">>, 2000}, {<<"node3">>, 3000}],
           Result = flurm_backfill:find_nth_end_time(EndTimes, 3, 9999),
           ?assertEqual(3000, Result)
       end},

      {"returns default when N exceeds list length",
       fun() ->
           EndTimes = [{<<"node1">>, 1000}, {<<"node2">>, 2000}],
           Result = flurm_backfill:find_nth_end_time(EndTimes, 5, 9999),
           ?assertEqual(9999, Result)
       end},

      {"handles single element list",
       fun() ->
           EndTimes = [{<<"node1">>, 5000}],
           Result = flurm_backfill:find_nth_end_time(EndTimes, 1, 9999),
           ?assertEqual(5000, Result)
       end}
     ]}.

%%====================================================================
%% find_backfill_nodes/5 Tests
%%====================================================================

find_backfill_nodes_test_() ->
    {"find_backfill_nodes/5 finds nodes for backfill jobs",
     [
      {"finds nodes meeting requirements",
       fun() ->
           Timeline = sample_timeline(),
           Result = flurm_backfill:find_backfill_nodes(1, 8, 16384, 3600, Timeline),
           case Result of
               {true, Nodes} ->
                   ?assertEqual(1, length(Nodes)),
                   ?assert(lists:member(hd(Nodes), [<<"node01">>, <<"node02">>, <<"node03">>]));
               false ->
                   ?assert(false)  % Should have found nodes
           end
       end},

      {"returns false when insufficient nodes",
       fun() ->
           Timeline = sample_timeline(),
           % Request 10 nodes when only 3 are free
           Result = flurm_backfill:find_backfill_nodes(10, 4, 8192, 3600, Timeline),
           ?assertEqual(false, Result)
       end},

      {"returns false when CPU requirements too high",
       fun() ->
           Timeline = sample_timeline(),
           % Request 64 CPUs when max available is 32
           Result = flurm_backfill:find_backfill_nodes(1, 64, 8192, 3600, Timeline),
           ?assertEqual(false, Result)
       end},

      {"returns false when memory requirements too high",
       fun() ->
           Timeline = sample_timeline(),
           % Request 100GB when max available is 64GB
           Result = flurm_backfill:find_backfill_nodes(1, 4, 102400, 3600, Timeline),
           ?assertEqual(false, Result)
       end},

      {"finds multiple nodes",
       fun() ->
           Timeline = sample_timeline(),
           Result = flurm_backfill:find_backfill_nodes(2, 8, 16384, 3600, Timeline),
           case Result of
               {true, Nodes} ->
                   ?assertEqual(2, length(Nodes));
               false ->
                   ?assert(false)
           end
       end},

      {"handles empty free nodes list",
       fun() ->
           Timeline = #{free_nodes => [], node_end_times => [], timestamp => 0},
           Result = flurm_backfill:find_backfill_nodes(1, 4, 8192, 3600, Timeline),
           ?assertEqual(false, Result)
       end}
     ]}.

%%====================================================================
%% reserve_nodes_in_timeline/3 Tests
%%====================================================================

reserve_nodes_in_timeline_test_() ->
    {"reserve_nodes_in_timeline/3 updates timeline with reservation",
     [
      {"removes reserved nodes from free list",
       fun() ->
           Timeline = sample_timeline(),
           Job = #{time_limit => 3600},
           Result = flurm_backfill:reserve_nodes_in_timeline([<<"node01">>], Job, Timeline),
           FreeNodes = maps:get(free_nodes, Result),
           FreeNames = [N || {N, _, _} <- FreeNodes],
           ?assertNot(lists:member(<<"node01">>, FreeNames))
       end},

      {"adds to node end times",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node01">>, 32, 65536}],
               node_end_times => []
           },
           Job = #{time_limit => 1800},
           Result = flurm_backfill:reserve_nodes_in_timeline([<<"node01">>], Job, Timeline),
           EndTimes = maps:get(node_end_times, Result),
           ?assertEqual(1, length(EndTimes)),
           {NodeName, EndTime} = hd(EndTimes),
           ?assertEqual(<<"node01">>, NodeName),
           ?assertEqual(Now + 1800, EndTime)
       end},

      {"handles multiple node reservation",
       fun() ->
           Timeline = sample_timeline(),
           Job = #{time_limit => 7200},
           Result = flurm_backfill:reserve_nodes_in_timeline(
               [<<"node01">>, <<"node02">>], Job, Timeline),
           FreeNodes = maps:get(free_nodes, Result),
           FreeNames = [N || {N, _, _} <- FreeNodes],
           ?assertNot(lists:member(<<"node01">>, FreeNames)),
           ?assertNot(lists:member(<<"node02">>, FreeNames)),
           ?assert(lists:member(<<"node03">>, FreeNames))
       end},

      {"preserves existing end times",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node01">>, 32, 65536}],
               node_end_times => [{<<"node02">>, Now + 3600}]
           },
           Job = #{time_limit => 1800},
           Result = flurm_backfill:reserve_nodes_in_timeline([<<"node01">>], Job, Timeline),
           EndTimes = maps:get(node_end_times, Result),
           ?assertEqual(2, length(EndTimes))
       end}
     ]}.

%%====================================================================
%% job_record_to_map/1 Tests
%%====================================================================

job_record_to_map_test_() ->
    {"job_record_to_map/1 converts job record to backfill map",
     [
      {"converts job identification fields",
       fun() ->
           Job = sample_job_record(),
           Map = flurm_backfill:job_record_to_map(Job),
           ?assertEqual(1001, maps:get(job_id, Map)),
           ?assertEqual(<<"backfill_test">>, maps:get(name, Map)),
           ?assertEqual(<<"testuser">>, maps:get(user, Map)),
           ?assertEqual(<<"testaccount">>, maps:get(account, Map))
       end},

      {"converts resource requirements",
       fun() ->
           Job = sample_job_record(),
           Map = flurm_backfill:job_record_to_map(Job),
           ?assertEqual(2, maps:get(num_nodes, Map)),
           ?assertEqual(8, maps:get(num_cpus, Map)),
           ?assertEqual(4096, maps:get(memory_mb, Map)),
           ?assertEqual(1800, maps:get(time_limit, Map))
       end},

      {"converts scheduling fields",
       fun() ->
           Job = sample_job_record(),
           Map = flurm_backfill:job_record_to_map(Job),
           ?assertEqual(pending, maps:get(state, Map)),
           ?assertEqual(500, maps:get(priority, Map)),
           ?assertEqual(<<"normal">>, maps:get(qos, Map)),
           ?assertEqual(<<"compute">>, maps:get(partition, Map))
       end},

      {"sets pid to undefined",
       fun() ->
           Job = sample_job_record(),
           Map = flurm_backfill:job_record_to_map(Job),
           ?assertEqual(undefined, maps:get(pid, Map))
       end},

      {"converts time fields",
       fun() ->
           Job = sample_job_record(),
           Map = flurm_backfill:job_record_to_map(Job),
           ?assert(is_integer(maps:get(submit_time, Map))),
           ?assertEqual(undefined, maps:get(start_time, Map))
       end}
     ]}.

%%====================================================================
%% find_shadow_time/5 Tests
%%====================================================================

find_shadow_time_test_() ->
    {"find_shadow_time/5 calculates when blocker can start",
     [
      {"returns current time when enough free nodes with non-empty EndTimes",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [
                   {<<"node01">>, 32, 65536},
                   {<<"node02">>, 32, 65536}
               ],
               node_end_times => []
           },
           % Need 2 nodes with 8 CPUs and 4096 MB each
           % Pass non-empty EndTimes to trigger the second clause
           EndTimes = [{<<"dummy">>, Now + 10000}],
           Result = flurm_backfill:find_shadow_time(EndTimes, 2, 8, 4096, Timeline),
           % Result should be Now because we have enough free nodes
           ?assertEqual(Now, Result)
       end},

      {"calculates wait time when nodes busy",
       fun() ->
           Now = erlang:system_time(second),
           EndTimes = [
               {<<"node01">>, Now + 1000},
               {<<"node02">>, Now + 2000},
               {<<"node03">>, Now + 3000}
           ],
           Timeline = #{
               timestamp => Now,
               free_nodes => [],
               node_end_times => EndTimes
           },
           % Need 2 nodes - should wait until second node is free
           Result = flurm_backfill:find_shadow_time(EndTimes, 2, 8, 4096, Timeline),
           ?assertEqual(Now + 2000, Result)
       end},

      {"returns Now + BACKFILL_WINDOW for empty end times",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node01">>, 8, 8192}],
               node_end_times => []
           },
           % Empty EndTimes triggers first clause which returns Now + 86400
           Result = flurm_backfill:find_shadow_time([], 2, 8, 4096, Timeline),
           % Should return Now + BACKFILL_WINDOW (24 hours)
           ?assertEqual(Now + 86400, Result)
       end},

      {"waits for Nth node when not enough free nodes",
       fun() ->
           Now = erlang:system_time(second),
           EndTimes = [
               {<<"node01">>, Now + 1000},
               {<<"node02">>, Now + 2000}
           ],
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node03">>, 8, 8192}],  % Only 1 free node
               node_end_times => EndTimes
           },
           % Need 3 nodes: 1 free + need 2 more from busy
           Result = flurm_backfill:find_shadow_time(EndTimes, 3, 8, 4096, Timeline),
           % Should wait until second busy node becomes free
           ?assertEqual(Now + 2000, Result)
       end}
     ]}.

%%====================================================================
%% find_fitting_jobs/5 Tests
%%====================================================================

find_fitting_jobs_test_() ->
    {"find_fitting_jobs/5 finds jobs that fit in backfill slots",
     [
      {"returns empty list for empty candidates",
       fun() ->
           Timeline = sample_timeline(),
           ShadowTime = erlang:system_time(second) + 7200,
           Result = flurm_backfill:find_fitting_jobs([], Timeline, ShadowTime, [], 10),
           ?assertEqual([], Result)
       end},

      {"returns empty list when max jobs is 0",
       fun() ->
           Timeline = sample_timeline(),
           ShadowTime = erlang:system_time(second) + 7200,
           Candidates = [sample_job_map(1, 100, 1, 4, 4096, 1800)],
           Result = flurm_backfill:find_fitting_jobs(Candidates, Timeline, ShadowTime, [], 0),
           ?assertEqual([], Result)
       end},

      {"finds jobs that complete before shadow time",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node01">>, 32, 65536}],
               node_end_times => []
           },
           ShadowTime = Now + 7200,  % 2 hours from now
           % Job with 1 hour time limit should fit
           Candidates = [sample_job_map(100, 500, 1, 8, 8192, 3600)],
           Result = flurm_backfill:find_fitting_jobs(Candidates, Timeline, ShadowTime, [], 10),
           ?assertEqual(1, length(Result))
       end},

      {"rejects jobs that won't complete before shadow time",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [{<<"node01">>, 32, 65536}],
               node_end_times => []
           },
           ShadowTime = Now + 1800,  % 30 minutes from now
           % Job with 1 hour time limit won't fit
           Candidates = [sample_job_map(100, 500, 1, 8, 8192, 3600)],
           Result = flurm_backfill:find_fitting_jobs(Candidates, Timeline, ShadowTime, [], 10),
           ?assertEqual([], Result)
       end},

      {"respects max jobs limit",
       fun() ->
           Now = erlang:system_time(second),
           Timeline = #{
               timestamp => Now,
               free_nodes => [
                   {<<"node01">>, 32, 65536},
                   {<<"node02">>, 32, 65536},
                   {<<"node03">>, 32, 65536}
               ],
               node_end_times => []
           },
           ShadowTime = Now + 7200,
           Candidates = [
               sample_job_map(100, 500, 1, 8, 8192, 1800),
               sample_job_map(101, 400, 1, 8, 8192, 1800),
               sample_job_map(102, 300, 1, 8, 8192, 1800)
           ],
           Result = flurm_backfill:find_fitting_jobs(Candidates, Timeline, ShadowTime, [], 2),
           ?assertEqual(2, length(Result))
       end}
     ]}.
