%%%-------------------------------------------------------------------
%%% @doc Coverage-oriented EUnit tests for flurm_backfill module.
%%%
%%% Pure function calls only -- no mocking. Every test calls a real
%%% exported (or TEST-exported) function with controlled inputs to
%%% exercise code paths that existing test files do not reach.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Helpers
%%====================================================================

now_sec() ->
    erlang:system_time(second).

%% Build a timeline map with all required keys.
timeline(FreeNodes, EndTimes) ->
    Now = now_sec(),
    #{
        timestamp   => Now,
        free_nodes  => FreeNodes,
        busy_nodes  => [],
        node_end_times => EndTimes,
        total_nodes => length(FreeNodes) + length(EndTimes)
    }.

timeline(Now, FreeNodes, EndTimes) ->
    #{
        timestamp   => Now,
        free_nodes  => FreeNodes,
        busy_nodes  => [],
        node_end_times => EndTimes,
        total_nodes => length(FreeNodes) + length(EndTimes)
    }.

candidate(Id, Nodes, Cpus, Mem, Time, Priority) ->
    #{
        job_id    => Id,
        pid       => undefined,
        num_nodes => Nodes,
        num_cpus  => Cpus,
        memory_mb => Mem,
        time_limit => Time,
        priority  => Priority
    }.

%%====================================================================
%% timestamp_to_seconds/1 -- cover every clause
%%====================================================================

%% Erlang now() tuple: {Mega, Sec, Micro}
ts_now_format_test() ->
    ?assertEqual(1700000000, flurm_backfill:timestamp_to_seconds({1700, 0, 999999})).

%% Zero tuple
ts_zero_tuple_test() ->
    ?assertEqual(0, flurm_backfill:timestamp_to_seconds({0, 0, 0})).

%% Large mega+sec
ts_large_tuple_test() ->
    ?assertEqual(1234567890, flurm_backfill:timestamp_to_seconds({1234, 567890, 0})).

%% Integer passthrough
ts_integer_test() ->
    ?assertEqual(42, flurm_backfill:timestamp_to_seconds(42)).

ts_zero_integer_test() ->
    ?assertEqual(0, flurm_backfill:timestamp_to_seconds(0)).

%% Negative integer (still integer clause)
ts_negative_integer_test() ->
    ?assertEqual(-100, flurm_backfill:timestamp_to_seconds(-100)).

%% Fallback clause: atom
ts_atom_fallback_test() ->
    Before = now_sec(),
    Result = flurm_backfill:timestamp_to_seconds(undefined),
    After  = now_sec(),
    ?assert(Result >= Before),
    ?assert(Result =< After).

%% Fallback clause: binary
ts_binary_fallback_test() ->
    Before = now_sec(),
    Result = flurm_backfill:timestamp_to_seconds(<<"not_a_time">>),
    After  = now_sec(),
    ?assert(Result >= Before),
    ?assert(Result =< After).

%% Fallback clause: list
ts_list_fallback_test() ->
    Before = now_sec(),
    Result = flurm_backfill:timestamp_to_seconds([1, 2, 3]),
    After  = now_sec(),
    ?assert(Result >= Before),
    ?assert(Result =< After).

%% Fallback clause: float
ts_float_fallback_test() ->
    Before = now_sec(),
    Result = flurm_backfill:timestamp_to_seconds(3.14),
    After  = now_sec(),
    ?assert(Result >= Before),
    ?assert(Result =< After).

%%====================================================================
%% find_nth_end_time/3 -- all clauses
%%====================================================================

%% Empty list returns default
nth_empty_test() ->
    ?assertEqual(9999, flurm_backfill:find_nth_end_time([], 3, 9999)).

%% N = 1, first element
nth_first_test() ->
    EndTimes = [{<<"a">>, 100}, {<<"b">>, 200}],
    ?assertEqual(100, flurm_backfill:find_nth_end_time(EndTimes, 1, 9999)).

%% N = 2, second element
nth_second_test() ->
    EndTimes = [{<<"a">>, 100}, {<<"b">>, 200}, {<<"c">>, 300}],
    ?assertEqual(200, flurm_backfill:find_nth_end_time(EndTimes, 2, 9999)).

%% N > length => default
nth_exceed_test() ->
    EndTimes = [{<<"a">>, 100}],
    ?assertEqual(5555, flurm_backfill:find_nth_end_time(EndTimes, 10, 5555)).

%% N = exact length of list (last element)
nth_exact_length_test() ->
    EndTimes = [{<<"a">>, 10}, {<<"b">>, 20}, {<<"c">>, 30}],
    ?assertEqual(30, flurm_backfill:find_nth_end_time(EndTimes, 3, 9999)).

%% Single element, N = 1
nth_single_test() ->
    ?assertEqual(777, flurm_backfill:find_nth_end_time([{<<"x">>, 777}], 1, 0)).

%%====================================================================
%% find_backfill_nodes/5 -- all branches
%%====================================================================

%% Empty free_nodes list
bf_nodes_empty_test() ->
    T = timeline([], []),
    ?assertEqual(false, flurm_backfill:find_backfill_nodes(1, 1, 1, 60, T)).

%% Single suitable node
bf_nodes_single_suitable_test() ->
    T = timeline([{<<"n1">>, 8, 4096}], []),
    ?assertMatch({true, [<<"n1">>]},
                 flurm_backfill:find_backfill_nodes(1, 4, 2048, 60, T)).

%% Node insufficient CPUs -- filtered out
bf_nodes_cpu_filter_test() ->
    T = timeline([{<<"n1">>, 2, 4096}], []),
    ?assertEqual(false, flurm_backfill:find_backfill_nodes(1, 4, 2048, 60, T)).

%% Node insufficient memory -- filtered out
bf_nodes_mem_filter_test() ->
    T = timeline([{<<"n1">>, 8, 512}], []),
    ?assertEqual(false, flurm_backfill:find_backfill_nodes(1, 4, 2048, 60, T)).

%% Need 2 nodes but only 1 suitable
bf_nodes_not_enough_test() ->
    T = timeline([{<<"n1">>, 8, 4096}], []),
    ?assertEqual(false, flurm_backfill:find_backfill_nodes(2, 4, 2048, 60, T)).

%% Need 2 nodes, have 3 suitable, picks first 2
bf_nodes_select_first_test() ->
    T = timeline([{<<"a">>, 8, 4096}, {<<"b">>, 8, 4096}, {<<"c">>, 8, 4096}], []),
    {true, Nodes} = flurm_backfill:find_backfill_nodes(2, 4, 2048, 60, T),
    ?assertEqual([<<"a">>, <<"b">>], Nodes).

%% Mix of suitable and unsuitable nodes
bf_nodes_mixed_test() ->
    FreeNodes = [
        {<<"bad_cpu">>, 1, 8192},
        {<<"bad_mem">>, 16, 64},
        {<<"good1">>,  8, 4096},
        {<<"good2">>,  8, 4096}
    ],
    T = timeline(FreeNodes, []),
    {true, Nodes} = flurm_backfill:find_backfill_nodes(2, 4, 2048, 60, T),
    ?assertEqual([<<"good1">>, <<"good2">>], Nodes).

%%====================================================================
%% reserve_nodes_in_timeline/3 -- verify timeline mutation
%%====================================================================

%% Reserve one node, check it leaves free_nodes and appears in end_times
reserve_single_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}, {<<"n2">>, 8, 4096}], []),
    Job = #{time_limit => 600},
    NewT = flurm_backfill:reserve_nodes_in_timeline([<<"n1">>], Job, T),
    FreeNames = [N || {N, _, _} <- maps:get(free_nodes, NewT)],
    ?assertNot(lists:member(<<"n1">>, FreeNames)),
    ?assert(lists:member(<<"n2">>, FreeNames)),
    EndTimes = maps:get(node_end_times, NewT),
    ?assertEqual([{<<"n1">>, Now + 600}], EndTimes).

%% Reserve all free nodes
reserve_all_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"x">>, 4, 2048}], []),
    Job = #{time_limit => 1800},
    NewT = flurm_backfill:reserve_nodes_in_timeline([<<"x">>], Job, T),
    ?assertEqual([], maps:get(free_nodes, NewT)),
    ?assertEqual([{<<"x">>, Now + 1800}], maps:get(node_end_times, NewT)).

%% Reserve preserves existing end_times
reserve_preserves_existing_test() ->
    Now = now_sec(),
    ExistingEnd = [{<<"old">>, Now + 500}],
    T = timeline(Now, [{<<"n1">>, 8, 4096}], ExistingEnd),
    Job = #{time_limit => 300},
    NewT = flurm_backfill:reserve_nodes_in_timeline([<<"n1">>], Job, T),
    EndTimes = maps:get(node_end_times, NewT),
    ?assertEqual(2, length(EndTimes)),
    ?assert(lists:member({<<"old">>, Now + 500}, EndTimes)),
    ?assert(lists:member({<<"n1">>, Now + 300}, EndTimes)).

%% Reserve uses default time_limit when key is missing
reserve_default_time_limit_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 4, 2048}], []),
    Job = #{},  %% no time_limit key => maps:get default 3600
    NewT = flurm_backfill:reserve_nodes_in_timeline([<<"n1">>], Job, T),
    [{<<"n1">>, EndTime}] = maps:get(node_end_times, NewT),
    ?assertEqual(Now + 3600, EndTime).

%%====================================================================
%% job_record_to_map/1 -- verify all fields
%%====================================================================

job_record_basic_test() ->
    Job = #job{
        id = 42,
        name = <<"myjob">>,
        user = <<"alice">>,
        partition = <<"gpu">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 3,
        num_cpus = 12,
        memory_mb = 8192,
        time_limit = 7200,
        priority = 500,
        submit_time = 1000000,
        start_time = undefined,
        allocated_nodes = [],
        account = <<"research">>,
        qos = <<"high">>
    },
    M = flurm_backfill:job_record_to_map(Job),
    ?assertEqual(42, maps:get(job_id, M)),
    ?assertEqual(<<"myjob">>, maps:get(name, M)),
    ?assertEqual(<<"alice">>, maps:get(user, M)),
    ?assertEqual(<<"gpu">>, maps:get(partition, M)),
    ?assertEqual(pending, maps:get(state, M)),
    ?assertEqual(3, maps:get(num_nodes, M)),
    ?assertEqual(12, maps:get(num_cpus, M)),
    ?assertEqual(8192, maps:get(memory_mb, M)),
    ?assertEqual(7200, maps:get(time_limit, M)),
    ?assertEqual(500, maps:get(priority, M)),
    ?assertEqual(1000000, maps:get(submit_time, M)),
    ?assertEqual(undefined, maps:get(start_time, M)),
    ?assertEqual(<<"research">>, maps:get(account, M)),
    ?assertEqual(<<"high">>, maps:get(qos, M)),
    ?assertEqual(undefined, maps:get(pid, M)).

%% Running job with start_time set
job_record_running_test() ->
    Job = #job{
        id = 99,
        name = <<"runner">>,
        user = <<"bob">>,
        partition = <<"default">>,
        state = running,
        script = <<"#!/bin/bash\necho hi">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 2048,
        time_limit = 3600,
        priority = 100,
        submit_time = 1000,
        start_time = 2000,
        allocated_nodes = [<<"node1">>],
        account = <<"ops">>,
        qos = <<"normal">>
    },
    M = flurm_backfill:job_record_to_map(Job),
    ?assertEqual(99, maps:get(job_id, M)),
    ?assertEqual(running, maps:get(state, M)),
    ?assertEqual(2000, maps:get(start_time, M)).

%% Job with default account / qos
job_record_defaults_test() ->
    Job = #job{
        id = 7,
        name = <<"minimal">>,
        user = <<"charlie">>,
        partition = <<"test">>,
        state = pending,
        script = <<"true">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 512,
        time_limit = 60,
        priority = 0,
        submit_time = 0,
        allocated_nodes = []
    },
    M = flurm_backfill:job_record_to_map(Job),
    ?assertEqual(<<>>, maps:get(account, M)),
    ?assertEqual(<<"normal">>, maps:get(qos, M)).

%%====================================================================
%% find_shadow_time/5 -- cover all three branches
%%====================================================================

%% Branch 1: enough free suitable nodes => returns Now
shadow_enough_free_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 16, 8192}, {<<"n2">>, 16, 8192}], []),
    EndTimes = [{<<"busy">>, Now + 5000}],
    Result = flurm_backfill:find_shadow_time(EndTimes, 2, 8, 4096, T),
    ?assertEqual(Now, Result).

%% Branch 2: not enough free, EndTimes empty => Now + 86400
shadow_empty_endtimes_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 4, 2048}], []),
    Result = flurm_backfill:find_shadow_time([], 3, 4, 2048, T),
    ?assertEqual(Now + 86400, Result).

%% Branch 3: not enough free, wait for Nth node
shadow_wait_for_nth_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"free1">>, 16, 8192}], []),
    EndTimes = [
        {<<"b1">>, Now + 100},
        {<<"b2">>, Now + 200},
        {<<"b3">>, Now + 300}
    ],
    %% Need 3 nodes total, 1 free => need 2 more from EndTimes
    Result = flurm_backfill:find_shadow_time(EndTimes, 3, 8, 4096, T),
    ?assertEqual(Now + 200, Result).

%% Free nodes exist but are not suitable (CPU too low), exercise CurrentSuitable = 0
shadow_no_suitable_free_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"tiny">>, 1, 512}], []),
    EndTimes = [{<<"b1">>, Now + 1000}],
    %% Need 1 node with 8 CPUs; free node has 1 CPU => not suitable
    Result = flurm_backfill:find_shadow_time(EndTimes, 1, 8, 4096, T),
    ?assertEqual(Now + 1000, Result).

%% Free nodes exist but insufficient memory
shadow_no_suitable_free_mem_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"low_mem">>, 32, 256}], []),
    EndTimes = [{<<"b1">>, Now + 2000}],
    Result = flurm_backfill:find_shadow_time(EndTimes, 1, 4, 1024, T),
    ?assertEqual(Now + 2000, Result).

%%====================================================================
%% find_fitting_jobs/5 -- cover Acc path and MaxJobs = 0
%%====================================================================

%% Empty candidate list returns reversed Acc
fitting_empty_returns_acc_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Shadow = Now + 7200,
    Acc = [{99, self(), [<<"prev">>]}],
    Result = flurm_backfill:find_fitting_jobs([], T, Shadow, Acc, 10),
    ?assertEqual(lists:reverse(Acc), Result).

%% MaxJobs = 0 returns reversed Acc immediately
fitting_maxjobs_zero_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Shadow = Now + 7200,
    Acc = [{50, self(), [<<"old">>]}],
    Jobs = [candidate(1, 1, 4, 2048, 600, 100)],
    Result = flurm_backfill:find_fitting_jobs(Jobs, T, Shadow, Acc, 0),
    ?assertEqual(lists:reverse(Acc), Result).

%% Candidate fits => added to Acc, timeline updated, continues
fitting_one_fits_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Shadow = Now + 7200,
    Jobs = [candidate(10, 1, 4, 2048, 600, 100)],
    Result = flurm_backfill:find_fitting_jobs(Jobs, T, Shadow, [], 5),
    ?assertEqual(1, length(Result)),
    [{10, _, [<<"n1">>]}] = Result.

%% Candidate does not fit (exceeds shadow) => skipped
fitting_one_skipped_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Shadow = Now + 100,  %% very near
    Jobs = [candidate(10, 1, 4, 2048, 3600, 100)],  %% needs 3600s > 100s
    Result = flurm_backfill:find_fitting_jobs(Jobs, T, Shadow, [], 5),
    ?assertEqual([], Result).

%% Multiple candidates: one fits, one doesn't
fitting_mixed_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}, {<<"n2">>, 8, 4096}], []),
    Shadow = Now + 1000,
    Jobs = [
        candidate(1, 1, 4, 2048, 500, 200),   %% fits
        candidate(2, 1, 4, 2048, 5000, 100)    %% too long
    ],
    Result = flurm_backfill:find_fitting_jobs(Jobs, T, Shadow, [], 10),
    ?assertEqual(1, length(Result)),
    [{1, _, _}] = Result.

%% Two candidates both fit; timeline is updated between them so
%% node is consumed by first job
fitting_timeline_consumed_test() ->
    Now = now_sec(),
    %% Only one free node
    T = timeline(Now, [{<<"only">>, 8, 4096}], []),
    Shadow = Now + 7200,
    Jobs = [
        candidate(1, 1, 4, 2048, 600, 300),
        candidate(2, 1, 4, 2048, 600, 200)
    ],
    Result = flurm_backfill:find_fitting_jobs(Jobs, T, Shadow, [], 10),
    %% First job takes the only node; second cannot be placed
    ?assertEqual(1, length(Result)),
    [{1, _, [<<"only">>]}] = Result.

%%====================================================================
%% can_backfill/3 -- boundary and edge cases
%%====================================================================

%% Job ends exactly at shadow time => allowed (=<)
can_backfill_exact_boundary_test() ->
    Now = now_sec(),
    TimeLimit = 500,
    Shadow = Now + TimeLimit,
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Job = candidate(1, 1, 4, 2048, TimeLimit, 100),
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job, T, Shadow)).

%% Job ends 1 second after shadow => rejected
can_backfill_one_over_test() ->
    Now = now_sec(),
    TimeLimit = 501,
    Shadow = Now + 500,
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Job = candidate(1, 1, 4, 2048, TimeLimit, 100),
    ?assertEqual(false, flurm_backfill:can_backfill(Job, T, Shadow)).

%% Empty timeline (no free_nodes key) => false
can_backfill_empty_timeline_test() ->
    Now = now_sec(),
    Shadow = Now + 99999,
    T = #{},
    Job = candidate(1, 1, 1, 1, 60, 100),
    ?assertEqual(false, flurm_backfill:can_backfill(Job, T, Shadow)).

%% Job uses default values from maps:get
can_backfill_minimal_job_test() ->
    Now = now_sec(),
    Shadow = Now + 99999,
    T = timeline(Now, [{<<"n1">>, 8, 4096}], []),
    Job = #{job_id => 1},  %% defaults: 1 node, 1 cpu, 1024 mem, 3600 time
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job, T, Shadow)).

%%====================================================================
%% calculate_shadow_time/2 -- integration through public API
%%====================================================================

%% Blocker with defaults, enough free nodes
calc_shadow_defaults_enough_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 8, 4096}],
                 [{<<"busy">>, Now + 5000}]),
    Blocker = #{},  %% defaults: 1 node, 1 cpu, 1024 mem
    Result = flurm_backfill:calculate_shadow_time(Blocker, T),
    ?assertEqual(Now, Result).

%% Blocker needs more than available free
calc_shadow_needs_wait_test() ->
    Now = now_sec(),
    T = timeline(Now, [{<<"n1">>, 16, 8192}],
                 [{<<"b1">>, Now + 1000}, {<<"b2">>, Now + 2000}]),
    Blocker = #{num_nodes => 3, num_cpus => 8, memory_mb => 4096},
    Result = flurm_backfill:calculate_shadow_time(Blocker, T),
    %% 1 free + need 2 from busy => second busy at Now+2000
    ?assertEqual(Now + 2000, Result).

%% Blocker needs nodes but end_times unsorted => still correct
calc_shadow_unsorted_endtimes_test() ->
    Now = now_sec(),
    T = timeline(Now, [],
                 [{<<"z">>, Now + 5000}, {<<"a">>, Now + 1000}, {<<"m">>, Now + 3000}]),
    Blocker = #{num_nodes => 2, num_cpus => 1, memory_mb => 512},
    Result = flurm_backfill:calculate_shadow_time(Blocker, T),
    %% After sorting: a@1000, m@3000, z@5000. Need 2 => second = m@3000
    ?assertEqual(Now + 3000, Result).

%%====================================================================
%% run_backfill_cycle/2 -- cover early-exit clauses
%%====================================================================

cycle_undefined_blocker_test() ->
    ?assertEqual([], flurm_backfill:run_backfill_cycle(undefined, [#{job_id => 1}])).

cycle_empty_candidates_test() ->
    ?assertEqual([], flurm_backfill:run_backfill_cycle(#{job_id => 1}, [])).

cycle_disabled_test() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    Result = flurm_backfill:run_backfill_cycle(
        #{job_id => 1, num_nodes => 1},
        [candidate(2, 1, 4, 2048, 600, 100)]),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    ?assertEqual([], Result).

%%====================================================================
%% is_backfill_enabled/0 -- all scheduler_type branches
%%====================================================================

enabled_backfill_type_test() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

enabled_fifo_backfill_type_test() ->
    application:set_env(flurm_core, scheduler_type, fifo_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

enabled_priority_backfill_type_test() ->
    application:set_env(flurm_core, scheduler_type, priority_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

enabled_explicit_true_test() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, true),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled).

enabled_default_test() ->
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

%%====================================================================
%% get_resource_timeline/1 -- no registry available
%%====================================================================

timeline_no_registry_test() ->
    T = flurm_backfill:get_resource_timeline(#{}),
    ?assert(is_map(T)),
    ?assert(is_integer(maps:get(timestamp, T))),
    ?assertEqual([], maps:get(free_nodes, T)),
    ?assertEqual([], maps:get(busy_nodes, T)),
    ?assertEqual(0, maps:get(total_nodes, T)).

%%====================================================================
%% get_backfill_candidates/1 -- no job manager
%%====================================================================

candidates_empty_test() ->
    ?assertEqual([], flurm_backfill:get_backfill_candidates([])).

candidates_no_manager_test() ->
    %% Job manager not running => filtermap returns false for all
    ?assertEqual([], flurm_backfill:get_backfill_candidates([1, 2, 3])).
