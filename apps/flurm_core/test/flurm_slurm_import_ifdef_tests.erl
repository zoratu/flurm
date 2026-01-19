%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_slurm_import internal functions
%%%
%%% These tests exercise the internal parser functions exported via
%%% -ifdef(TEST) to achieve coverage of SLURM output parsing logic.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_import_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Job State Parsing Tests
%%====================================================================

parse_job_state_pending_test() ->
    ?assertEqual(pending, flurm_slurm_import:parse_job_state("PD")).

parse_job_state_running_test() ->
    ?assertEqual(running, flurm_slurm_import:parse_job_state("R")).

parse_job_state_completing_test() ->
    ?assertEqual(completing, flurm_slurm_import:parse_job_state("CG")).

parse_job_state_completed_test() ->
    ?assertEqual(completed, flurm_slurm_import:parse_job_state("CD")).

parse_job_state_failed_test() ->
    ?assertEqual(failed, flurm_slurm_import:parse_job_state("F")).

parse_job_state_cancelled_test() ->
    ?assertEqual(cancelled, flurm_slurm_import:parse_job_state("CA")).

parse_job_state_timeout_test() ->
    ?assertEqual(timeout, flurm_slurm_import:parse_job_state("TO")).

parse_job_state_node_fail_test() ->
    ?assertEqual(node_fail, flurm_slurm_import:parse_job_state("NF")).

parse_job_state_preempted_test() ->
    ?assertEqual(preempted, flurm_slurm_import:parse_job_state("PR")).

parse_job_state_suspended_test() ->
    ?assertEqual(suspended, flurm_slurm_import:parse_job_state("S")).

parse_job_state_unknown_test() ->
    ?assertEqual(unknown, flurm_slurm_import:parse_job_state("XX")).

%%====================================================================
%% Node State Parsing Tests
%%====================================================================

parse_node_state_idle_test() ->
    ?assertEqual(idle, flurm_slurm_import:parse_node_state("idle")).

parse_node_state_allocated_test() ->
    ?assertEqual(allocated, flurm_slurm_import:parse_node_state("alloc")).

parse_node_state_mixed_test() ->
    ?assertEqual(mixed, flurm_slurm_import:parse_node_state("mix")).

parse_node_state_down_test() ->
    ?assertEqual(down, flurm_slurm_import:parse_node_state("down")).

parse_node_state_drain_test() ->
    ?assertEqual(drain, flurm_slurm_import:parse_node_state("drain")).

parse_node_state_draining_test() ->
    ?assertEqual(draining, flurm_slurm_import:parse_node_state("drng")).

parse_node_state_completing_test() ->
    ?assertEqual(completing, flurm_slurm_import:parse_node_state("comp")).

parse_node_state_with_suffix_star_test() ->
    %% States with * suffix (e.g., "idle*" for responsive)
    ?assertEqual(idle, flurm_slurm_import:parse_node_state("idle*")).

parse_node_state_with_suffix_tilde_test() ->
    %% States with ~ suffix (e.g., "down~" for powered down)
    ?assertEqual(down, flurm_slurm_import:parse_node_state("down~")).

%%====================================================================
%% Integer Parsing Tests
%%====================================================================

parse_int_valid_test() ->
    ?assertEqual(42, flurm_slurm_import:parse_int("42")).

parse_int_zero_test() ->
    ?assertEqual(0, flurm_slurm_import:parse_int("0")).

parse_int_large_test() ->
    ?assertEqual(1000000, flurm_slurm_import:parse_int("1000000")).

parse_int_invalid_test() ->
    ?assertEqual(0, flurm_slurm_import:parse_int("invalid")).

parse_int_empty_test() ->
    ?assertEqual(0, flurm_slurm_import:parse_int("")).

parse_int_with_trailing_test() ->
    %% string:to_integer handles trailing non-digits
    ?assertEqual(123, flurm_slurm_import:parse_int("123abc")).

%%====================================================================
%% Memory Parsing Tests
%%====================================================================

parse_memory_megabytes_test() ->
    ?assertEqual(4000, flurm_slurm_import:parse_memory("4000M")).

parse_memory_megabytes_lowercase_test() ->
    ?assertEqual(4000, flurm_slurm_import:parse_memory("4000m")).

parse_memory_gigabytes_test() ->
    %% 4G = 4 * 1024 MB = 4096 MB
    ?assertEqual(4096, flurm_slurm_import:parse_memory("4G")).

parse_memory_gigabytes_lowercase_test() ->
    ?assertEqual(4096, flurm_slurm_import:parse_memory("4g")).

parse_memory_terabytes_test() ->
    %% 1T = 1 * 1024 * 1024 MB = 1048576 MB
    ?assertEqual(1048576, flurm_slurm_import:parse_memory("1T")).

parse_memory_terabytes_lowercase_test() ->
    ?assertEqual(1048576, flurm_slurm_import:parse_memory("1t")).

parse_memory_no_suffix_test() ->
    ?assertEqual(8000, flurm_slurm_import:parse_memory("8000")).

parse_memory_invalid_test() ->
    ?assertEqual(0, flurm_slurm_import:parse_memory("invalid")).

%%====================================================================
%% Time Parsing Tests
%%====================================================================

parse_time_unlimited_test() ->
    ?assertEqual(unlimited, flurm_slurm_import:parse_time("UNLIMITED")).

parse_time_na_test() ->
    ?assertEqual(undefined, flurm_slurm_import:parse_time("N/A")).

parse_time_days_hours_mins_secs_test() ->
    %% "1-02:30:45" = 1 day + 2 hours + 30 mins + 45 secs
    %% = 1440 + 120 + 30 + 0 (45/60 rounded) = 1590 minutes
    Result = flurm_slurm_import:parse_time("1-02:30:45"),
    ?assertEqual(1590, Result).

parse_time_hours_mins_secs_test() ->
    %% "10:30:00" = 10 hours + 30 mins = 630 minutes
    Result = flurm_slurm_import:parse_time("10:30:00"),
    ?assertEqual(630, Result).

parse_time_just_minutes_test() ->
    %% Plain number interpreted as minutes
    Result = flurm_slurm_import:parse_time("60"),
    ?assertEqual(60, Result).

parse_time_with_seconds_rounding_test() ->
    %% "00:01:30" = 0 hours + 1 min + 30 secs = 1 + 0 (30/60) = 1 minute
    Result = flurm_slurm_import:parse_time("00:01:30"),
    ?assertEqual(1, Result).

%%====================================================================
%% Timestamp Parsing Tests
%%====================================================================

parse_timestamp_na_test() ->
    ?assertEqual(undefined, flurm_slurm_import:parse_timestamp("N/A")).

parse_timestamp_unknown_test() ->
    ?assertEqual(undefined, flurm_slurm_import:parse_timestamp("Unknown")).

parse_timestamp_invalid_test() ->
    %% Invalid format either returns undefined or throws an exception
    try
        Result = flurm_slurm_import:parse_timestamp("not-a-timestamp"),
        ?assertEqual(undefined, Result)
    catch
        _:_ ->
            %% Exception on invalid format is also acceptable
            ok
    end.

%%====================================================================
%% Features Parsing Tests
%%====================================================================

parse_features_null_test() ->
    ?assertEqual([], flurm_slurm_import:parse_features("(null)")).

parse_features_empty_test() ->
    ?assertEqual([], flurm_slurm_import:parse_features("")).

parse_features_single_test() ->
    ?assertEqual([<<"gpu">>], flurm_slurm_import:parse_features("gpu")).

parse_features_multiple_test() ->
    Result = flurm_slurm_import:parse_features("gpu,nvme,high_mem"),
    ?assertEqual([<<"gpu">>, <<"nvme">>, <<"high_mem">>], Result).

%%====================================================================
%% GRES Parsing Tests
%%====================================================================

parse_gres_null_test() ->
    ?assertEqual([], flurm_slurm_import:parse_gres("(null)")).

parse_gres_empty_test() ->
    ?assertEqual([], flurm_slurm_import:parse_gres("")).

parse_gres_single_test() ->
    ?assertEqual([<<"gpu:1">>], flurm_slurm_import:parse_gres("gpu:1")).

parse_gres_multiple_test() ->
    Result = flurm_slurm_import:parse_gres("gpu:2,nvme:4"),
    ?assertEqual([<<"gpu:2">>, <<"nvme:4">>], Result).

%%====================================================================
%% squeue Output Parsing Tests
%%====================================================================

parse_squeue_output_empty_test() ->
    Result = flurm_slurm_import:parse_squeue_output("", #{}),
    ?assertEqual([], Result).

parse_squeue_output_single_job_test() ->
    %% Format: JobId|Name|User|Partition|State|Time|Nodes|CPUs|Mem|TimeLimit|Start|Submit
    Line = "12345|test_job|user1|batch|R|10:30:00|1|4|4000M|1-00:00:00|N/A|N/A",
    Result = flurm_slurm_import:parse_squeue_output(Line, #{}),
    ?assertEqual(1, length(Result)),
    [Job] = Result,
    ?assertEqual(12345, maps:get(id, Job)),
    ?assertEqual(<<"test_job">>, maps:get(name, Job)),
    ?assertEqual(<<"user1">>, maps:get(user, Job)),
    ?assertEqual(running, maps:get(state, Job)).

parse_squeue_output_multiple_jobs_test() ->
    Lines = "12345|job1|user1|batch|R|10:30:00|1|4|4000M|1-00:00:00|N/A|N/A\n"
            "12346|job2|user2|gpu|PD|00:00:00|2|8|8000M|2-00:00:00|N/A|N/A",
    Result = flurm_slurm_import:parse_squeue_output(Lines, #{}),
    ?assertEqual(2, length(Result)).

parse_squeue_line_valid_test() ->
    Line = "100|myjob|alice|compute|PD|00:00:00|4|16|16000M|UNLIMITED|N/A|N/A",
    Result = flurm_slurm_import:parse_squeue_line(Line),
    ?assertMatch({true, _}, Result),
    {true, Job} = Result,
    ?assertEqual(100, maps:get(id, Job)),
    ?assertEqual(<<"myjob">>, maps:get(name, Job)),
    ?assertEqual(<<"alice">>, maps:get(user, Job)),
    ?assertEqual(pending, maps:get(state, Job)),
    ?assertEqual(4, maps:get(num_nodes, Job)),
    ?assertEqual(16, maps:get(num_cpus, Job)).

parse_squeue_line_invalid_test() ->
    Line = "invalid|line|format",
    Result = flurm_slurm_import:parse_squeue_line(Line),
    ?assertEqual(false, Result).

%%====================================================================
%% sinfo Output Parsing Tests
%%====================================================================

parse_sinfo_output_empty_test() ->
    Result = flurm_slurm_import:parse_sinfo_output("", #{}),
    ?assertEqual([], Result).

parse_sinfo_output_single_node_test() ->
    %% Format: Name|Partition|State|CPUs|Memory|Features|GRES
    Line = "node001|batch|idle|32|128000|(null)|(null)",
    Result = flurm_slurm_import:parse_sinfo_output(Line, #{}),
    ?assertEqual(1, length(Result)),
    [Node] = Result,
    ?assertEqual(<<"node001">>, maps:get(name, Node)),
    ?assertEqual(idle, maps:get(state, Node)),
    ?assertEqual(32, maps:get(cpus, Node)).

parse_sinfo_line_valid_test() ->
    Line = "compute-01|gpu|mix|64|256000|gpu,nvme|gpu:4",
    Result = flurm_slurm_import:parse_sinfo_line(Line),
    ?assertMatch({true, _}, Result),
    {true, Node} = Result,
    ?assertEqual(<<"compute-01">>, maps:get(name, Node)),
    ?assertEqual(<<"gpu">>, maps:get(partition, Node)),
    ?assertEqual(mixed, maps:get(state, Node)),
    ?assertEqual(64, maps:get(cpus, Node)),
    ?assertEqual([<<"gpu">>, <<"nvme">>], maps:get(features, Node)),
    ?assertEqual([<<"gpu:4">>], maps:get(gres, Node)).

parse_sinfo_line_invalid_test() ->
    Line = "invalid",
    Result = flurm_slurm_import:parse_sinfo_line(Line),
    ?assertEqual(false, Result).

%%====================================================================
%% Partition Output Parsing Tests
%%====================================================================

parse_partition_output_empty_test() ->
    Result = flurm_slurm_import:parse_partition_output(""),
    ?assertEqual([], Result).

parse_partition_output_single_test() ->
    %% Format: Name|Avail|TimeLimit|Nodes|CPUs|Memory|NodeList
    Line = "batch*|up|UNLIMITED|10|320|1280000|node[001-010]",
    Result = flurm_slurm_import:parse_partition_output(Line),
    ?assertEqual(1, length(Result)),
    [Part] = Result,
    ?assertEqual(<<"batch">>, maps:get(name, Part)),
    ?assertEqual(true, maps:get(is_default, Part)),
    ?assertEqual(true, maps:get(available, Part)).

parse_partition_line_valid_test() ->
    Line = "compute|up|1-00:00:00|20|640|2560000|node[001-020]",
    Result = flurm_slurm_import:parse_partition_line(Line),
    ?assertMatch({true, _}, Result),
    {true, Part} = Result,
    ?assertEqual(<<"compute">>, maps:get(name, Part)),
    ?assertEqual(false, maps:get(is_default, Part)),
    ?assertEqual(true, maps:get(available, Part)),
    ?assertEqual(20, maps:get(total_nodes, Part)).

parse_partition_line_down_test() ->
    Line = "maint|down|UNLIMITED|5|160|640000|node[100-104]",
    Result = flurm_slurm_import:parse_partition_line(Line),
    {true, Part} = Result,
    ?assertEqual(false, maps:get(available, Part)).

parse_partition_line_invalid_test() ->
    Line = "invalid",
    Result = flurm_slurm_import:parse_partition_line(Line),
    ?assertEqual(false, Result).

%%====================================================================
%% Stats Update Tests
%%====================================================================

update_stats_jobs_success_test() ->
    Stats = #{jobs_imported => 10, nodes_imported => 5},
    Result = {ok, #{imported => 3, updated => 2}},
    NewStats = flurm_slurm_import:update_stats(jobs, Result, Stats),
    ?assertEqual(15, maps:get(jobs_imported, NewStats)).

update_stats_nodes_success_test() ->
    Stats = #{jobs_imported => 10, nodes_imported => 5},
    Result = {ok, #{imported => 7, updated => 3}},
    NewStats = flurm_slurm_import:update_stats(nodes, Result, Stats),
    ?assertEqual(15, maps:get(nodes_imported, NewStats)).

update_stats_error_test() ->
    Stats = #{jobs_imported => 10},
    Result = {error, some_error},
    NewStats = flurm_slurm_import:update_stats(jobs, Result, Stats),
    %% Stats should be unchanged on error
    ?assertEqual(Stats, NewStats).

%%====================================================================
%% Count Imported Tests
%%====================================================================

count_imported_success_test() ->
    Result = {ok, #{imported => 5, updated => 3}},
    ?assertEqual(8, flurm_slurm_import:count_imported(Result)).

count_imported_error_test() ->
    Result = {error, reason},
    ?assertEqual(0, flurm_slurm_import:count_imported(Result)).

count_imported_other_test() ->
    ?assertEqual(0, flurm_slurm_import:count_imported(unexpected)).
