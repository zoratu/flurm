%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_array internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure internal functions for array spec parsing
%%% and task ID generation without requiring a running gen_server.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Record Definitions (matching flurm_job_array internal records)
%%====================================================================

-record(array_spec, {
    start_idx :: non_neg_integer() | undefined,
    end_idx :: non_neg_integer() | undefined,
    step :: pos_integer(),
    indices :: [non_neg_integer()] | undefined,
    max_concurrent :: pos_integer() | unlimited
}).

-record(array_task, {
    id :: {pos_integer(), non_neg_integer()},
    array_job_id :: pos_integer(),
    task_id :: non_neg_integer(),
    job_id :: pos_integer() | undefined,
    state :: atom(),
    exit_code :: integer() | undefined,
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    node :: binary() | undefined
}).

%%====================================================================
%% do_parse_array_spec/1 Tests
%%====================================================================

do_parse_array_spec_test_() ->
    {"do_parse_array_spec/1 tests", [
        {"parses simple range",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("0-10"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(10, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent)
         end},

        {"parses range with step",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("0-100:2"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(100, Spec#array_spec.end_idx),
             ?assertEqual(2, Spec#array_spec.step),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent)
         end},

        {"parses range with max concurrent",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("0-50%10"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(50, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step),
             ?assertEqual(10, Spec#array_spec.max_concurrent)
         end},

        {"parses range with step and max concurrent",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("0-100:5%20"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(100, Spec#array_spec.end_idx),
             ?assertEqual(5, Spec#array_spec.step),
             ?assertEqual(20, Spec#array_spec.max_concurrent)
         end},

        {"parses comma-separated list",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("1,3,5,7,9"),
             ?assertEqual([1, 3, 5, 7, 9], Spec#array_spec.indices)
         end},

        {"parses comma-separated list with max concurrent",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("1,2,3,4,5%2"),
             ?assertEqual([1, 2, 3, 4, 5], Spec#array_spec.indices),
             ?assertEqual(2, Spec#array_spec.max_concurrent)
         end},

        {"parses mixed list with ranges",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("1-5,10,15-20"),
             ?assertEqual([1, 2, 3, 4, 5, 10, 15, 16, 17, 18, 19, 20],
                          Spec#array_spec.indices)
         end},

        {"parses single value",
         fun() ->
             Spec = flurm_job_array:do_parse_array_spec("5"),
             ?assertEqual(5, Spec#array_spec.start_idx),
             ?assertEqual(5, Spec#array_spec.end_idx)
         end}
    ]}.

%%====================================================================
%% parse_range_spec/2 Tests
%%====================================================================

parse_range_spec_test_() ->
    {"parse_range_spec/2 tests", [
        {"parses simple range",
         fun() ->
             Spec = flurm_job_array:parse_range_spec("0-10", unlimited),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(10, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent)
         end},

        {"parses range with step",
         fun() ->
             Spec = flurm_job_array:parse_range_spec("1-100:10", unlimited),
             ?assertEqual(1, Spec#array_spec.start_idx),
             ?assertEqual(100, Spec#array_spec.end_idx),
             ?assertEqual(10, Spec#array_spec.step)
         end},

        {"parses range with max concurrent",
         fun() ->
             Spec = flurm_job_array:parse_range_spec("0-50", 5),
             ?assertEqual(5, Spec#array_spec.max_concurrent)
         end},

        {"parses single index as range",
         fun() ->
             Spec = flurm_job_array:parse_range_spec("42", unlimited),
             ?assertEqual(42, Spec#array_spec.start_idx),
             ?assertEqual(42, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step)
         end}
    ]}.

%%====================================================================
%% parse_list_spec/2 Tests
%%====================================================================

parse_list_spec_test_() ->
    {"parse_list_spec/2 tests", [
        {"parses simple comma-separated list",
         fun() ->
             Spec = flurm_job_array:parse_list_spec("1,2,3", unlimited),
             ?assertEqual([1, 2, 3], Spec#array_spec.indices),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent)
         end},

        {"parses list with duplicates (deduplicated and sorted)",
         fun() ->
             Spec = flurm_job_array:parse_list_spec("5,3,1,3,5", unlimited),
             ?assertEqual([1, 3, 5], Spec#array_spec.indices)
         end},

        {"parses list with embedded ranges",
         fun() ->
             Spec = flurm_job_array:parse_list_spec("1-3,10,20-22", unlimited),
             ?assertEqual([1, 2, 3, 10, 20, 21, 22], Spec#array_spec.indices)
         end},

        {"respects max concurrent",
         fun() ->
             Spec = flurm_job_array:parse_list_spec("1,2,3,4,5", 3),
             ?assertEqual(3, Spec#array_spec.max_concurrent)
         end}
    ]}.

%%====================================================================
%% parse_list_element/1 Tests
%%====================================================================

parse_list_element_test_() ->
    {"parse_list_element/1 tests", [
        {"parses single number",
         fun() ->
             ?assertEqual([42], flurm_job_array:parse_list_element("42"))
         end},

        {"parses range",
         fun() ->
             ?assertEqual([1, 2, 3, 4, 5],
                          flurm_job_array:parse_list_element("1-5"))
         end},

        {"parses range with whitespace",
         fun() ->
             ?assertEqual([10, 11, 12],
                          flurm_job_array:parse_list_element(" 10-12 "))
         end}
    ]}.

%%====================================================================
%% generate_task_ids/1 Tests
%%====================================================================

generate_task_ids_test_() ->
    {"generate_task_ids/1 tests", [
        {"generates IDs from simple range",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 5,
                 step = 1,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual([0, 1, 2, 3, 4, 5],
                          flurm_job_array:generate_task_ids(Spec))
         end},

        {"generates IDs from range with step",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 10,
                 step = 2,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual([0, 2, 4, 6, 8, 10],
                          flurm_job_array:generate_task_ids(Spec))
         end},

        {"generates IDs from explicit list",
         fun() ->
             Spec = #array_spec{
                 start_idx = undefined,
                 end_idx = undefined,
                 step = 1,
                 indices = [1, 5, 10, 15],
                 max_concurrent = unlimited
             },
             ?assertEqual([1, 5, 10, 15],
                          flurm_job_array:generate_task_ids(Spec))
         end},

        {"generates single ID",
         fun() ->
             Spec = #array_spec{
                 start_idx = 42,
                 end_idx = 42,
                 step = 1,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual([42], flurm_job_array:generate_task_ids(Spec))
         end},

        {"handles step larger than range",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 5,
                 step = 10,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual([0], flurm_job_array:generate_task_ids(Spec))
         end}
    ]}.

%%====================================================================
%% apply_task_updates/2 Tests
%%====================================================================

make_test_task() ->
    make_test_task(#{}).

make_test_task(Overrides) ->
    Defaults = #{
        id => {1, 0},
        array_job_id => 1,
        task_id => 0,
        job_id => undefined,
        state => pending,
        exit_code => undefined,
        start_time => undefined,
        end_time => undefined,
        node => undefined
    },
    Merged = maps:merge(Defaults, Overrides),
    #array_task{
        id = maps:get(id, Merged),
        array_job_id = maps:get(array_job_id, Merged),
        task_id = maps:get(task_id, Merged),
        job_id = maps:get(job_id, Merged),
        state = maps:get(state, Merged),
        exit_code = maps:get(exit_code, Merged),
        start_time = maps:get(start_time, Merged),
        end_time = maps:get(end_time, Merged),
        node = maps:get(node, Merged)
    }.

apply_task_updates_test_() ->
    {"apply_task_updates/2 tests", [
        {"updates state",
         fun() ->
             Task = make_test_task(),
             Updated = flurm_job_array:apply_task_updates(Task, #{state => running}),
             ?assertEqual(running, Updated#array_task.state)
         end},

        {"updates job_id",
         fun() ->
             Task = make_test_task(),
             Updated = flurm_job_array:apply_task_updates(Task, #{job_id => 12345}),
             ?assertEqual(12345, Updated#array_task.job_id)
         end},

        {"updates exit_code",
         fun() ->
             Task = make_test_task(),
             Updated = flurm_job_array:apply_task_updates(Task, #{exit_code => 0}),
             ?assertEqual(0, Updated#array_task.exit_code)
         end},

        {"updates start_time",
         fun() ->
             Task = make_test_task(),
             Now = erlang:system_time(second),
             Updated = flurm_job_array:apply_task_updates(Task, #{start_time => Now}),
             ?assertEqual(Now, Updated#array_task.start_time)
         end},

        {"updates end_time",
         fun() ->
             Task = make_test_task(),
             Now = erlang:system_time(second),
             Updated = flurm_job_array:apply_task_updates(Task, #{end_time => Now}),
             ?assertEqual(Now, Updated#array_task.end_time)
         end},

        {"updates node",
         fun() ->
             Task = make_test_task(),
             Updated = flurm_job_array:apply_task_updates(Task, #{node => <<"node1">>}),
             ?assertEqual(<<"node1">>, Updated#array_task.node)
         end},

        {"updates multiple fields",
         fun() ->
             Task = make_test_task(),
             Now = erlang:system_time(second),
             Updates = #{
                 state => running,
                 job_id => 999,
                 start_time => Now,
                 node => <<"compute1">>
             },
             Updated = flurm_job_array:apply_task_updates(Task, Updates),
             ?assertEqual(running, Updated#array_task.state),
             ?assertEqual(999, Updated#array_task.job_id),
             ?assertEqual(Now, Updated#array_task.start_time),
             ?assertEqual(<<"compute1">>, Updated#array_task.node)
         end},

        {"preserves unupdated fields",
         fun() ->
             Task = make_test_task(#{
                 id => {5, 3},
                 array_job_id => 5,
                 task_id => 3
             }),
             Updated = flurm_job_array:apply_task_updates(Task, #{state => running}),
             ?assertEqual({5, 3}, Updated#array_task.id),
             ?assertEqual(5, Updated#array_task.array_job_id),
             ?assertEqual(3, Updated#array_task.task_id)
         end},

        {"empty updates returns same values",
         fun() ->
             Task = make_test_task(#{state => pending, job_id => 100}),
             Updated = flurm_job_array:apply_task_updates(Task, #{}),
             ?assertEqual(pending, Updated#array_task.state),
             ?assertEqual(100, Updated#array_task.job_id)
         end}
    ]}.

%%====================================================================
%% API Function Tests (parse_array_spec, format_array_spec)
%%====================================================================

parse_array_spec_test_() ->
    {"parse_array_spec/1 tests", [
        {"parses binary spec",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-10">>),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(10, Spec#array_spec.end_idx)
         end},

        {"parses string spec",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec("0-10"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(10, Spec#array_spec.end_idx)
         end}
    ]}.

format_array_spec_test_() ->
    {"format_array_spec/1 tests", [
        {"formats simple range",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 10,
                 step = 1,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual(<<"0-10">>, flurm_job_array:format_array_spec(Spec))
         end},

        {"formats range with step",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 100,
                 step = 5,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual(<<"0-100:5">>, flurm_job_array:format_array_spec(Spec))
         end},

        {"formats range with max concurrent",
         fun() ->
             Spec = #array_spec{
                 start_idx = 0,
                 end_idx = 50,
                 step = 1,
                 indices = undefined,
                 max_concurrent = 10
             },
             ?assertEqual(<<"0-50%10">>, flurm_job_array:format_array_spec(Spec))
         end},

        {"formats single index",
         fun() ->
             Spec = #array_spec{
                 start_idx = 42,
                 end_idx = 42,
                 step = 1,
                 indices = undefined,
                 max_concurrent = unlimited
             },
             ?assertEqual(<<"42">>, flurm_job_array:format_array_spec(Spec))
         end},

        {"formats index list",
         fun() ->
             Spec = #array_spec{
                 start_idx = undefined,
                 end_idx = undefined,
                 step = 1,
                 indices = [1, 3, 5],
                 max_concurrent = unlimited
             },
             ?assertEqual(<<"1,3,5">>, flurm_job_array:format_array_spec(Spec))
         end},

        {"formats index list with max concurrent",
         fun() ->
             Spec = #array_spec{
                 start_idx = undefined,
                 end_idx = undefined,
                 step = 1,
                 indices = [1, 2, 3],
                 max_concurrent = 2
             },
             ?assertEqual(<<"1,2,3%2">>, flurm_job_array:format_array_spec(Spec))
         end}
    ]}.

%%====================================================================
%% Roundtrip Tests
%%====================================================================

parse_format_roundtrip_test_() ->
    {"parse/format roundtrip tests", [
        {"simple range roundtrip",
         fun() ->
             Original = "0-10",
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-10">>, Formatted)
         end},

        {"range with step roundtrip",
         fun() ->
             Original = "0-100:5",
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-100:5">>, Formatted)
         end},

        {"range with max concurrent roundtrip",
         fun() ->
             Original = "0-50%10",
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-50%10">>, Formatted)
         end}
    ]}.

%%====================================================================
%% expand_array/1 Tests
%%====================================================================

expand_array_test_() ->
    {"expand_array/1 tests", [
        {"expands simple range",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-2">>),
             ?assertEqual(3, length(Tasks)),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([0, 1, 2], TaskIds)
         end},

        {"expands range with step",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-10:5">>),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([0, 5, 10], TaskIds)
         end},

        {"includes task metadata",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-4">>),
             [Task | _] = Tasks,
             ?assertEqual(5, maps:get(task_count, Task)),
             ?assertEqual(0, maps:get(task_min, Task)),
             ?assertEqual(4, maps:get(task_max, Task)),
             ?assertEqual(unlimited, maps:get(max_concurrent, Task))
         end},

        {"includes max concurrent in metadata",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-9%5">>),
             [Task | _] = Tasks,
             ?assertEqual(5, maps:get(max_concurrent, Task))
         end}
    ]}.
