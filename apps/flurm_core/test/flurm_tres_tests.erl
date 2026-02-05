%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_tres module
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_tres_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Definitions
%%====================================================================

add_test_() ->
    [
        {"add empty maps", fun() ->
            Result = flurm_tres:add(#{}, #{}),
            ?assertEqual(#{}, Result)
        end},
        {"add to empty map", fun() ->
            Result = flurm_tres:add(#{}, #{cpu_seconds => 100}),
            ?assertEqual(#{cpu_seconds => 100}, Result)
        end},
        {"add from empty map", fun() ->
            Result = flurm_tres:add(#{cpu_seconds => 100}, #{}),
            ?assertEqual(#{cpu_seconds => 100}, Result)
        end},
        {"add same keys", fun() ->
            Map1 = #{cpu_seconds => 100, mem_seconds => 200},
            Map2 = #{cpu_seconds => 50, mem_seconds => 100},
            Result = flurm_tres:add(Map1, Map2),
            ?assertEqual(#{cpu_seconds => 150, mem_seconds => 300}, Result)
        end},
        {"add different keys", fun() ->
            Map1 = #{cpu_seconds => 100},
            Map2 = #{gpu_seconds => 50},
            Result = flurm_tres:add(Map1, Map2),
            ?assertEqual(#{cpu_seconds => 100, gpu_seconds => 50}, Result)
        end},
        {"add overlapping keys", fun() ->
            Map1 = #{cpu_seconds => 100, mem_seconds => 200},
            Map2 = #{mem_seconds => 100, gpu_seconds => 50},
            Result = flurm_tres:add(Map1, Map2),
            ?assertEqual(#{cpu_seconds => 100, mem_seconds => 300, gpu_seconds => 50}, Result)
        end}
    ].

subtract_test_() ->
    [
        {"subtract empty maps", fun() ->
            Result = flurm_tres:subtract(#{}, #{}),
            ?assertEqual(#{}, Result)
        end},
        {"subtract from map", fun() ->
            Map1 = #{cpu_seconds => 100, mem_seconds => 200},
            Map2 = #{cpu_seconds => 30, mem_seconds => 50},
            Result = flurm_tres:subtract(Map1, Map2),
            ?assertEqual(#{cpu_seconds => 70, mem_seconds => 150}, Result)
        end},
        {"subtract floors at zero", fun() ->
            Map1 = #{cpu_seconds => 100},
            Map2 = #{cpu_seconds => 150},
            Result = flurm_tres:subtract(Map1, Map2),
            ?assertEqual(#{cpu_seconds => 0}, Result)
        end}
    ].

zero_test_() ->
    [
        {"zero returns map with standard keys", fun() ->
            Result = flurm_tres:zero(),
            ?assert(is_map(Result)),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(mem_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(gpu_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(job_time, Result))
        end}
    ].

from_job_test_() ->
    [
        {"from_job with map", fun() ->
            JobInfo = #{
                elapsed => 3600,
                num_cpus => 4,
                req_mem => 8192,
                num_nodes => 2,
                tres_alloc => #{gpu => 1}
            },
            Result = flurm_tres:from_job(JobInfo),
            ?assertEqual(3600 * 4, maps:get(cpu_seconds, Result)),
            ?assertEqual(3600 * 8192, maps:get(mem_seconds, Result)),
            ?assertEqual(3600 * 2, maps:get(node_seconds, Result)),
            ?assertEqual(3600 * 1, maps:get(gpu_seconds, Result)),
            ?assertEqual(1, maps:get(job_count, Result)),
            ?assertEqual(3600, maps:get(job_time, Result))
        end},
        {"from_job with explicit params", fun() ->
            Result = flurm_tres:from_job(3600, 4, 8192, 2),
            ?assertEqual(3600 * 4, maps:get(cpu_seconds, Result)),
            ?assertEqual(3600 * 8192, maps:get(mem_seconds, Result)),
            ?assertEqual(3600 * 2, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(gpu_seconds, Result)),
            ?assertEqual(1, maps:get(job_count, Result))
        end},
        {"from_job handles missing fields", fun() ->
            JobInfo = #{elapsed => 100},
            Result = flurm_tres:from_job(JobInfo),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(mem_seconds, Result)),
            ?assertEqual(100, maps:get(node_seconds, Result)),  % num_nodes defaults to 1
            ?assertEqual(100, maps:get(job_time, Result))
        end}
    ].

format_test_() ->
    [
        {"format empty map", fun() ->
            Result = flurm_tres:format(#{}),
            ?assertEqual(<<>>, Result)
        end},
        {"format single key", fun() ->
            Result = flurm_tres:format(#{cpu_seconds => 100}),
            ?assertEqual(<<"cpu=100">>, Result)
        end},
        {"format multiple keys", fun() ->
            Result = flurm_tres:format(#{cpu_seconds => 100, mem_seconds => 200}),
            %% Order may vary, so check both possibilities
            ?assert(Result =:= <<"cpu=100,mem=200">> orelse Result =:= <<"mem=200,cpu=100">>)
        end},
        {"format skips zero values", fun() ->
            Result = flurm_tres:format(#{cpu_seconds => 100, mem_seconds => 0}),
            ?assertEqual(<<"cpu=100">>, Result)
        end},
        {"format skips job_count and job_time", fun() ->
            Result = flurm_tres:format(#{cpu_seconds => 100, job_count => 5, job_time => 3600}),
            ?assertEqual(<<"cpu=100">>, Result)
        end}
    ].

parse_test_() ->
    [
        {"parse empty string", fun() ->
            Result = flurm_tres:parse(<<>>),
            ?assert(is_map(Result))
        end},
        {"parse single key", fun() ->
            Result = flurm_tres:parse("cpu=100"),
            ?assertEqual(100, maps:get(cpu_seconds, Result))
        end},
        {"parse binary", fun() ->
            Result = flurm_tres:parse(<<"cpu=100,mem=200">>),
            ?assertEqual(100, maps:get(cpu_seconds, Result)),
            ?assertEqual(200, maps:get(mem_seconds, Result))
        end},
        {"parse gres/gpu", fun() ->
            Result = flurm_tres:parse("gres/gpu=4"),
            ?assertEqual(4, maps:get(gpu_seconds, Result))
        end},
        {"parse gpu shorthand", fun() ->
            Result = flurm_tres:parse("gpu=2"),
            ?assertEqual(2, maps:get(gpu_seconds, Result))
        end}
    ].

multiply_test_() ->
    [
        {"multiply by integer", fun() ->
            Input = #{cpu_seconds => 100, mem_seconds => 50},
            Result = flurm_tres:multiply(Input, 2),
            ?assertEqual(#{cpu_seconds => 200, mem_seconds => 100}, Result)
        end},
        {"multiply by float", fun() ->
            Input = #{cpu_seconds => 100},
            Result = flurm_tres:multiply(Input, 0.5),
            ?assertEqual(#{cpu_seconds => 50}, Result)
        end}
    ].

is_empty_test_() ->
    [
        {"empty map is empty", fun() ->
            ?assert(flurm_tres:is_empty(#{}))
        end},
        {"zero map is empty", fun() ->
            ?assert(flurm_tres:is_empty(flurm_tres:zero()))
        end},
        {"non-zero map is not empty", fun() ->
            ?assertNot(flurm_tres:is_empty(#{cpu_seconds => 1}))
        end}
    ].

exceeds_test_() ->
    [
        {"no limits exceeded", fun() ->
            Current = #{cpu_seconds => 100, mem_seconds => 200},
            Limits = #{cpu_seconds => 500, mem_seconds => 500},
            Result = flurm_tres:exceeds(Current, Limits),
            ?assertEqual([], Result)
        end},
        {"cpu limit exceeded", fun() ->
            Current = #{cpu_seconds => 600},
            Limits = #{cpu_seconds => 500},
            Result = flurm_tres:exceeds(Current, Limits),
            ?assertEqual([{cpu_seconds, 600, 500}], Result)
        end},
        {"zero limit is unlimited", fun() ->
            Current = #{cpu_seconds => 1000000},
            Limits = #{cpu_seconds => 0},
            Result = flurm_tres:exceeds(Current, Limits),
            ?assertEqual([], Result)
        end}
    ].

keys_test_() ->
    [
        {"keys returns standard list", fun() ->
            Keys = flurm_tres:keys(),
            ?assert(is_list(Keys)),
            ?assert(lists:member(cpu_seconds, Keys)),
            ?assert(lists:member(mem_seconds, Keys)),
            ?assert(lists:member(gpu_seconds, Keys))
        end}
    ].
