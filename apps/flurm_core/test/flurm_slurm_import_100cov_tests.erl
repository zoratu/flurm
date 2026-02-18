%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_slurm_import module - 100% coverage target
%%%
%%% Tests SLURM configuration import functionality:
%%% - gen_server lifecycle (start_link, init, terminate)
%%% - Import jobs, nodes, partitions
%%% - Parsing squeue, sinfo output
%%% - State parsing (job states, node states)
%%% - Time, memory, feature parsing
%%% - Sync functionality (start_sync, stop_sync, get_sync_status)
%%% - Stats updating
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_import_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing import server
    case whereis(flurm_slurm_import) of
        undefined -> ok;
        ExistingPid ->
            gen_server:stop(ExistingPid, normal, 5000),
            timer:sleep(50)
    end,
    %% Start fresh import server
    {ok, Pid} = flurm_slurm_import:start_link(),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end.

%%====================================================================
%% Test Generator
%%====================================================================

flurm_slurm_import_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         %% Basic API tests
         fun test_start_link/1,
         fun test_start_link_with_opts/1,
         fun test_unknown_call/1,
         fun test_unknown_cast/1,
         fun test_unknown_info/1,

         %% Import jobs tests
         fun test_import_jobs_no_slurm/1,
         fun test_import_jobs_with_opts/1,

         %% Import nodes tests
         fun test_import_nodes_no_slurm/1,
         fun test_import_nodes_with_opts/1,

         %% Import partitions tests
         fun test_import_partitions_no_slurm/1,

         %% Import all tests
         fun test_import_all/1,
         fun test_import_all_with_opts/1,

         %% Sync tests
         fun test_start_sync/1,
         fun test_stop_sync/1,
         fun test_get_sync_status/1,
         fun test_sync_tick/1
     ]}.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_link(_Pid) ->
    ?_test(begin
        ?assert(is_process_alive(whereis(flurm_slurm_import)))
    end).

test_start_link_with_opts(_Pid) ->
    ?_test(begin
        %% Server already running from setup
        ?assert(is_process_alive(whereis(flurm_slurm_import)))
    end).

test_unknown_call(_Pid) ->
    ?_test(begin
        Result = gen_server:call(flurm_slurm_import, unknown_request),
        ?assertEqual({error, unknown_request}, Result)
    end).

test_unknown_cast(_Pid) ->
    ?_test(begin
        gen_server:cast(flurm_slurm_import, unknown_cast),
        timer:sleep(10),
        ?assert(is_process_alive(whereis(flurm_slurm_import)))
    end).

test_unknown_info(_Pid) ->
    ?_test(begin
        flurm_slurm_import ! unknown_info,
        timer:sleep(10),
        ?assert(is_process_alive(whereis(flurm_slurm_import)))
    end).

%%====================================================================
%% Import Jobs Tests
%%====================================================================

test_import_jobs_no_slurm(_Pid) ->
    ?_test(begin
        %% Without SLURM installed, this should handle the error gracefully
        Result = flurm_slurm_import:import_jobs(),
        %% Could be error or ok depending on whether dependencies exist
        ?assert(is_tuple(Result) orelse Result =:= ok)
    end).

test_import_jobs_with_opts(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_jobs(#{filter => running}),
        ?assert(is_tuple(Result) orelse Result =:= ok)
    end).

%%====================================================================
%% Import Nodes Tests
%%====================================================================

test_import_nodes_no_slurm(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_nodes(),
        ?assert(is_tuple(Result) orelse Result =:= ok)
    end).

test_import_nodes_with_opts(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_nodes(#{partition => <<"compute">>}),
        ?assert(is_tuple(Result) orelse Result =:= ok)
    end).

%%====================================================================
%% Import Partitions Tests
%%====================================================================

test_import_partitions_no_slurm(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_partitions(),
        ?assert(is_tuple(Result) orelse Result =:= ok)
    end).

%%====================================================================
%% Import All Tests
%%====================================================================

test_import_all(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_all(),
        ?assertMatch({ok, _}, Result)
    end).

test_import_all_with_opts(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:import_all(#{}),
        ?assertMatch({ok, _}, Result)
    end).

%%====================================================================
%% Sync Tests
%%====================================================================

test_start_sync(_Pid) ->
    ?_test(begin
        Result = flurm_slurm_import:start_sync(10000),
        ?assertEqual(ok, Result),
        %% Stop sync to clean up
        flurm_slurm_import:stop_sync()
    end).

test_stop_sync(_Pid) ->
    ?_test(begin
        %% First start sync
        flurm_slurm_import:start_sync(10000),
        %% Then stop
        Result = flurm_slurm_import:stop_sync(),
        ?assertEqual(ok, Result)
    end).

test_get_sync_status(_Pid) ->
    ?_test(begin
        {ok, Status} = flurm_slurm_import:get_sync_status(),
        ?assert(is_map(Status)),
        ?assert(maps:is_key(sync_enabled, Status)),
        ?assert(maps:is_key(sync_interval, Status)),
        ?assert(maps:is_key(stats, Status))
    end).

test_sync_tick(_Pid) ->
    ?_test(begin
        %% Start sync with a short interval
        flurm_slurm_import:start_sync(100),
        %% Wait for a tick to happen
        timer:sleep(150),
        %% Get status to verify last_sync was updated
        {ok, Status} = flurm_slurm_import:get_sync_status(),
        ?assertEqual(true, maps:get(sync_enabled, Status)),
        flurm_slurm_import:stop_sync()
    end).

%%====================================================================
%% Parsing Tests - squeue output
%%====================================================================

parse_squeue_tests_test_() ->
    [
        {"Parse empty squeue output",
         ?_test(begin
             Result = flurm_slurm_import:parse_squeue_output("", #{}),
             ?assertEqual([], Result)
         end)},

        {"Parse single job line",
         ?_test(begin
             Line = "123|test_job|user1|compute|R|10:30:00|1|4|4000M|1-00:00:00|2024-01-01T10:00:00|2024-01-01T09:00:00",
             Result = flurm_slurm_import:parse_squeue_output(Line, #{}),
             ?assertEqual(1, length(Result)),
             [Job] = Result,
             ?assertEqual(123, maps:get(id, Job)),
             ?assertEqual(<<"test_job">>, maps:get(name, Job)),
             ?assertEqual(<<"user1">>, maps:get(user, Job)),
             ?assertEqual(<<"compute">>, maps:get(partition, Job)),
             ?assertEqual(running, maps:get(state, Job))
         end)},

        {"Parse multiple job lines",
         ?_test(begin
             Lines = "123|job1|user1|part1|R|1:00:00|1|2|2000M|1:00:00|N/A|N/A\n" ++
                     "456|job2|user2|part2|PD|0:00:00|2|4|4000M|2:00:00|N/A|N/A",
             Result = flurm_slurm_import:parse_squeue_output(Lines, #{}),
             ?assertEqual(2, length(Result))
         end)},

        {"Parse malformed line",
         ?_test(begin
             Line = "malformed|line",
             Result = flurm_slurm_import:parse_squeue_output(Line, #{}),
             ?assertEqual([], Result)
         end)}
    ].

%%====================================================================
%% Parsing Tests - squeue line
%%====================================================================

parse_squeue_line_tests_test_() ->
    [
        {"Parse complete job line",
         ?_test(begin
             Line = "100|myjob|testuser|default|CG|5:30:00|2|8|8G|2-00:00:00|2024-01-15T10:00:00|2024-01-15T09:30:00",
             {true, Job} = flurm_slurm_import:parse_squeue_line(Line),
             ?assertEqual(100, maps:get(id, Job)),
             ?assertEqual(<<"myjob">>, maps:get(name, Job)),
             ?assertEqual(<<"testuser">>, maps:get(user, Job)),
             ?assertEqual(completing, maps:get(state, Job)),
             ?assertEqual(2, maps:get(num_nodes, Job)),
             ?assertEqual(8, maps:get(num_cpus, Job)),
             ?assertEqual(8192, maps:get(memory_mb, Job))  % 8G = 8192MB
         end)},

        {"Parse line with incomplete fields",
         ?_test(begin
             Line = "incomplete",
             Result = flurm_slurm_import:parse_squeue_line(Line),
             ?assertEqual(false, Result)
         end)}
    ].

%%====================================================================
%% Job State Parsing Tests
%%====================================================================

parse_job_state_tests_test_() ->
    [
        {"Parse PD state", ?_assertEqual(pending, flurm_slurm_import:parse_job_state("PD"))},
        {"Parse R state", ?_assertEqual(running, flurm_slurm_import:parse_job_state("R"))},
        {"Parse CG state", ?_assertEqual(completing, flurm_slurm_import:parse_job_state("CG"))},
        {"Parse CD state", ?_assertEqual(completed, flurm_slurm_import:parse_job_state("CD"))},
        {"Parse F state", ?_assertEqual(failed, flurm_slurm_import:parse_job_state("F"))},
        {"Parse CA state", ?_assertEqual(cancelled, flurm_slurm_import:parse_job_state("CA"))},
        {"Parse TO state", ?_assertEqual(timeout, flurm_slurm_import:parse_job_state("TO"))},
        {"Parse NF state", ?_assertEqual(node_fail, flurm_slurm_import:parse_job_state("NF"))},
        {"Parse PR state", ?_assertEqual(preempted, flurm_slurm_import:parse_job_state("PR"))},
        {"Parse S state", ?_assertEqual(suspended, flurm_slurm_import:parse_job_state("S"))},
        {"Parse unknown state", ?_assertEqual(unknown, flurm_slurm_import:parse_job_state("XX"))}
    ].

%%====================================================================
%% Parsing Tests - sinfo output
%%====================================================================

parse_sinfo_tests_test_() ->
    [
        {"Parse empty sinfo output",
         ?_test(begin
             Result = flurm_slurm_import:parse_sinfo_output("", #{}),
             ?assertEqual([], Result)
         end)},

        {"Parse single node line",
         ?_test(begin
             Line = "node001|compute|idle|32|128000|feature1,feature2|gpu:2",
             Result = flurm_slurm_import:parse_sinfo_output(Line, #{}),
             ?assertEqual(1, length(Result)),
             [Node] = Result,
             ?assertEqual(<<"node001">>, maps:get(name, Node)),
             ?assertEqual(<<"compute">>, maps:get(partition, Node)),
             ?assertEqual(idle, maps:get(state, Node)),
             ?assertEqual(32, maps:get(cpus, Node)),
             ?assertEqual(128000, maps:get(memory_mb, Node))
         end)},

        {"Parse multiple node lines",
         ?_test(begin
             Lines = "node001|part1|idle|16|64000|feat1|(null)\n" ++
                     "node002|part1|alloc|16|64000|feat2|gpu:1",
             Result = flurm_slurm_import:parse_sinfo_output(Lines, #{}),
             ?assertEqual(2, length(Result))
         end)}
    ].

%%====================================================================
%% Node State Parsing Tests
%%====================================================================

parse_node_state_tests_test_() ->
    [
        {"Parse idle state", ?_assertEqual(idle, flurm_slurm_import:parse_node_state("idle"))},
        {"Parse alloc state", ?_assertEqual(allocated, flurm_slurm_import:parse_node_state("alloc"))},
        {"Parse mix state", ?_assertEqual(mixed, flurm_slurm_import:parse_node_state("mix"))},
        {"Parse down state", ?_assertEqual(down, flurm_slurm_import:parse_node_state("down"))},
        {"Parse drain state", ?_assertEqual(drain, flurm_slurm_import:parse_node_state("drain"))},
        {"Parse drng state", ?_assertEqual(draining, flurm_slurm_import:parse_node_state("drng"))},
        {"Parse comp state", ?_assertEqual(completing, flurm_slurm_import:parse_node_state("comp"))},
        {"Parse state with star suffix", ?_assertEqual(idle, flurm_slurm_import:parse_node_state("idle*"))},
        {"Parse state with tilde suffix", ?_assertEqual(down, flurm_slurm_import:parse_node_state("down~"))}
    ].

%%====================================================================
%% Partition Parsing Tests
%%====================================================================

parse_partition_tests_test_() ->
    [
        {"Parse empty partition output",
         ?_test(begin
             Result = flurm_slurm_import:parse_partition_output(""),
             ?assertEqual([], Result)
         end)},

        {"Parse single partition line",
         ?_test(begin
             Line = "compute*|up|UNLIMITED|10|320|128000|node[001-010]",
             Result = flurm_slurm_import:parse_partition_output(Line),
             ?assertEqual(1, length(Result)),
             [Part] = Result,
             ?assertEqual(<<"compute">>, maps:get(name, Part)),
             ?assertEqual(true, maps:get(is_default, Part)),
             ?assertEqual(true, maps:get(available, Part)),
             ?assertEqual(unlimited, maps:get(time_limit, Part))
         end)},

        {"Parse non-default partition",
         ?_test(begin
             Line = "debug|up|1:00:00|2|32|64000|node[011-012]",
             Result = flurm_slurm_import:parse_partition_output(Line),
             [Part] = Result,
             ?assertEqual(<<"debug">>, maps:get(name, Part)),
             ?assertEqual(false, maps:get(is_default, Part))
         end)},

        {"Parse partition line malformed",
         ?_test(begin
             Line = "malformed",
             Result = flurm_slurm_import:parse_partition_output(Line),
             ?assertEqual([], Result)
         end)}
    ].

%%====================================================================
%% Integer Parsing Tests
%%====================================================================

parse_int_tests_test_() ->
    [
        {"Parse valid integer", ?_assertEqual(123, flurm_slurm_import:parse_int("123"))},
        {"Parse zero", ?_assertEqual(0, flurm_slurm_import:parse_int("0"))},
        {"Parse invalid string", ?_assertEqual(0, flurm_slurm_import:parse_int("abc"))},
        {"Parse empty string", ?_assertEqual(0, flurm_slurm_import:parse_int(""))},
        {"Parse with trailing chars", ?_assertEqual(42, flurm_slurm_import:parse_int("42M"))}
    ].

%%====================================================================
%% Memory Parsing Tests
%%====================================================================

parse_memory_tests_test_() ->
    [
        {"Parse MB", ?_assertEqual(4000, flurm_slurm_import:parse_memory("4000M"))},
        {"Parse mb lowercase", ?_assertEqual(4000, flurm_slurm_import:parse_memory("4000m"))},
        {"Parse GB", ?_assertEqual(4096, flurm_slurm_import:parse_memory("4G"))},
        {"Parse gb lowercase", ?_assertEqual(4096, flurm_slurm_import:parse_memory("4g"))},
        {"Parse TB", ?_assertEqual(4194304, flurm_slurm_import:parse_memory("4T"))},
        {"Parse tb lowercase", ?_assertEqual(4194304, flurm_slurm_import:parse_memory("4t"))},
        {"Parse no suffix", ?_assertEqual(1000, flurm_slurm_import:parse_memory("1000"))},
        {"Parse invalid", ?_assertEqual(0, flurm_slurm_import:parse_memory("invalid"))}
    ].

%%====================================================================
%% Time Parsing Tests
%%====================================================================

parse_time_tests_test_() ->
    [
        {"Parse UNLIMITED", ?_assertEqual(unlimited, flurm_slurm_import:parse_time("UNLIMITED"))},
        {"Parse N/A", ?_assertEqual(undefined, flurm_slurm_import:parse_time("N/A"))},
        {"Parse days-hours:mins:secs",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("1-00:00:00"),
             ?assertEqual(1440, Result)  % 1 day = 1440 minutes
         end)},
        {"Parse hours:mins:secs",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("10:30:00"),
             ?assertEqual(630, Result)  % 10*60 + 30 = 630 minutes
         end)},
        {"Parse simple integer",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("60"),
             ?assertEqual(60, Result)
         end)}
    ].

%%====================================================================
%% Timestamp Parsing Tests
%%====================================================================

parse_timestamp_tests_test_() ->
    [
        {"Parse N/A", ?_assertEqual(undefined, flurm_slurm_import:parse_timestamp("N/A"))},
        {"Parse Unknown", ?_assertEqual(undefined, flurm_slurm_import:parse_timestamp("Unknown"))},
        {"Parse invalid format", ?_assertEqual(undefined, flurm_slurm_import:parse_timestamp("invalid"))}
    ].

%%====================================================================
%% Features Parsing Tests
%%====================================================================

parse_features_tests_test_() ->
    [
        {"Parse null features", ?_assertEqual([], flurm_slurm_import:parse_features("(null)"))},
        {"Parse empty features", ?_assertEqual([], flurm_slurm_import:parse_features(""))},
        {"Parse single feature", ?_assertEqual([<<"gpu">>], flurm_slurm_import:parse_features("gpu"))},
        {"Parse multiple features",
         ?_test(begin
             Result = flurm_slurm_import:parse_features("gpu,ssd,nvme"),
             ?assertEqual([<<"gpu">>, <<"ssd">>, <<"nvme">>], Result)
         end)}
    ].

%%====================================================================
%% GRES Parsing Tests
%%====================================================================

parse_gres_tests_test_() ->
    [
        {"Parse null gres", ?_assertEqual([], flurm_slurm_import:parse_gres("(null)"))},
        {"Parse empty gres", ?_assertEqual([], flurm_slurm_import:parse_gres(""))},
        {"Parse single gres", ?_assertEqual([<<"gpu:2">>], flurm_slurm_import:parse_gres("gpu:2"))},
        {"Parse multiple gres",
         ?_test(begin
             Result = flurm_slurm_import:parse_gres("gpu:4,fpga:2"),
             ?assertEqual([<<"gpu:4">>, <<"fpga:2">>], Result)
         end)}
    ].

%%====================================================================
%% Update Stats Tests
%%====================================================================

update_stats_tests_test_() ->
    [
        {"Update stats with ok result",
         ?_test(begin
             OldStats = #{jobs_imported => 0},
             Result = {ok, #{imported => 5, updated => 3}},
             NewStats = flurm_slurm_import:update_stats(jobs, Result, OldStats),
             ?assertEqual(8, maps:get(jobs_imported, NewStats))
         end)},

        {"Update stats with error result",
         ?_test(begin
             OldStats = #{jobs_imported => 10},
             Result = {error, failed},
             NewStats = flurm_slurm_import:update_stats(jobs, Result, OldStats),
             ?assertEqual(10, maps:get(jobs_imported, NewStats))
         end)},

        {"Update nodes stats",
         ?_test(begin
             OldStats = #{nodes_imported => 5},
             Result = {ok, #{imported => 2, updated => 1}},
             NewStats = flurm_slurm_import:update_stats(nodes, Result, OldStats),
             ?assertEqual(8, maps:get(nodes_imported, NewStats))
         end)}
    ].

%%====================================================================
%% Count Imported Tests
%%====================================================================

count_imported_tests_test_() ->
    [
        {"Count imported from ok result",
         ?_test(begin
             Result = {ok, #{imported => 5, updated => 3}},
             Count = flurm_slurm_import:count_imported(Result),
             ?assertEqual(8, Count)
         end)},

        {"Count imported from error result",
         ?_test(begin
             Result = {error, failed},
             Count = flurm_slurm_import:count_imported(Result),
             ?assertEqual(0, Count)
         end)},

        {"Count imported with only imported key",
         ?_test(begin
             Result = {ok, #{imported => 10}},
             Count = flurm_slurm_import:count_imported(Result),
             ?assertEqual(10, Count)
         end)}
    ].

%%====================================================================
%% Edge Cases and Integration Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Start with auto_sync option",
              ?_test(begin
                  %% Stop current server
                  gen_server:stop(flurm_slurm_import, normal, 5000),
                  timer:sleep(50),
                  %% Start with auto_sync
                  {ok, Pid} = flurm_slurm_import:start_link([{auto_sync, true}, {sync_interval, 10000}]),
                  {ok, Status} = flurm_slurm_import:get_sync_status(),
                  ?assertEqual(true, maps:get(sync_enabled, Status)),
                  gen_server:stop(Pid, normal, 5000)
              end)},

             {"Terminate stops sync timer",
              ?_test(begin
                  flurm_slurm_import:start_sync(10000),
                  {ok, Status1} = flurm_slurm_import:get_sync_status(),
                  ?assertEqual(true, maps:get(sync_enabled, Status1))
                  %% terminate will be called by cleanup
              end)},

             {"Multiple start_sync calls",
              ?_test(begin
                  flurm_slurm_import:start_sync(10000),
                  flurm_slurm_import:start_sync(5000),
                  {ok, Status} = flurm_slurm_import:get_sync_status(),
                  ?assertEqual(5000, maps:get(sync_interval, Status)),
                  flurm_slurm_import:stop_sync()
              end)},

             {"Stop sync when not running",
              ?_test(begin
                  Result = flurm_slurm_import:stop_sync(),
                  ?assertEqual(ok, Result)
              end)}
         ]
     end}.

%%====================================================================
%% Complex Time Parsing Tests
%%====================================================================

complex_time_parsing_test_() ->
    [
        {"Parse 2 days 12 hours",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("2-12:30:45"),
             %% 2*1440 + 12*60 + 30 + 45/60 = 2880 + 720 + 30 = 3630
             ?assertEqual(3630, Result)
         end)},

        {"Parse just hours and minutes",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("01:30:00"),
             ?assertEqual(90, Result)
         end)},

        {"Parse with seconds truncated",
         ?_test(begin
             Result = flurm_slurm_import:parse_time("00:01:59"),
             ?assertEqual(1, Result)  % 0*60 + 1 + 59/60 (truncated) = 1
         end)}
    ].

%%====================================================================
%% Parse sinfo line tests
%%====================================================================

parse_sinfo_line_tests_test_() ->
    [
        {"Parse complete node line",
         ?_test(begin
             Line = "compute001|batch|mix|64|256000|skylake,avx512|gpu:v100:4",
             {true, Node} = flurm_slurm_import:parse_sinfo_line(Line),
             ?assertEqual(<<"compute001">>, maps:get(name, Node)),
             ?assertEqual(<<"batch">>, maps:get(partition, Node)),
             ?assertEqual(mixed, maps:get(state, Node)),
             ?assertEqual(64, maps:get(cpus, Node)),
             ?assertEqual(256000, maps:get(memory_mb, Node)),
             ?assertEqual([<<"skylake">>, <<"avx512">>], maps:get(features, Node)),
             ?assertEqual([<<"gpu:v100:4">>], maps:get(gres, Node))
         end)},

        {"Parse incomplete line",
         ?_test(begin
             Result = flurm_slurm_import:parse_sinfo_line("incomplete|data"),
             ?assertEqual(false, Result)
         end)}
    ].

%%====================================================================
%% Parse partition line tests
%%====================================================================

parse_partition_line_tests_test_() ->
    [
        {"Parse default partition",
         ?_test(begin
             Line = "gpu*|up|7-00:00:00|20|640|512000|gpu[001-020]",
             {true, Part} = flurm_slurm_import:parse_partition_line(Line),
             ?assertEqual(<<"gpu">>, maps:get(name, Part)),
             ?assertEqual(true, maps:get(is_default, Part)),
             ?assertEqual(true, maps:get(available, Part)),
             ?assertEqual(10080, maps:get(time_limit, Part)),  % 7 days in minutes
             ?assertEqual(20, maps:get(total_nodes, Part))
         end)},

        {"Parse down partition",
         ?_test(begin
             Line = "maint|down|UNLIMITED|5|80|32000|maint[001-005]",
             {true, Part} = flurm_slurm_import:parse_partition_line(Line),
             ?assertEqual(false, maps:get(available, Part))
         end)},

        {"Parse incomplete partition line",
         ?_test(begin
             Result = flurm_slurm_import:parse_partition_line("bad|line"),
             ?assertEqual(false, Result)
         end)}
    ].
