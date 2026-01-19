%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_accounting internal functions
%%%
%%% Tests the internal pure functions exposed via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_accounting_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Suppress compile-time error detection for intentional badarg test
-compile(nowarn_fail_call).

%%====================================================================
%% Test: job_data_to_map/1
%%====================================================================

job_data_to_map_basic_test() ->
    JobData = #job_data{
        job_id = 123,
        user_id = 1000,
        group_id = 1000,
        partition = <<"batch">>,
        num_nodes = 2,
        num_cpus = 4,
        time_limit = 3600,
        submit_time = 1700000000,
        priority = 100
    },
    Result = flurm_accounting:job_data_to_map(JobData),
    ?assertEqual(123, maps:get(job_id, Result)),
    ?assertEqual(1000, maps:get(user_id, Result)),
    ?assertEqual(1000, maps:get(group_id, Result)),
    ?assertEqual(<<"batch">>, maps:get(partition, Result)),
    ?assertEqual(2, maps:get(num_nodes, Result)),
    ?assertEqual(4, maps:get(num_cpus, Result)),
    ?assertEqual(3600, maps:get(time_limit, Result)),
    ?assertEqual(1700000000, maps:get(submit_time, Result)),
    ?assertEqual(100, maps:get(priority, Result)).

job_data_to_map_undefined_submit_time_test() ->
    JobData = #job_data{
        job_id = 456,
        user_id = 1001,
        group_id = 1001,
        partition = <<"debug">>,
        num_nodes = 1,
        num_cpus = 1,
        time_limit = 60,
        submit_time = undefined,
        priority = 50
    },
    Result = flurm_accounting:job_data_to_map(JobData),
    SubmitTime = maps:get(submit_time, Result),
    %% Should be current time when undefined
    ?assert(is_integer(SubmitTime)),
    Now = erlang:system_time(second),
    ?assert(SubmitTime =< Now),
    ?assert(SubmitTime > Now - 10).

job_data_to_map_tuple_submit_time_test() ->
    %% Test erlang:now() style tuple
    JobData = #job_data{
        job_id = 789,
        user_id = 1002,
        group_id = 1002,
        partition = <<"gpu">>,
        num_nodes = 4,
        num_cpus = 16,
        time_limit = 7200,
        submit_time = {1700, 0, 123456},  % {MegaSecs, Secs, MicroSecs}
        priority = 200
    },
    Result = flurm_accounting:job_data_to_map(JobData),
    %% 1700 * 1000000 + 0 = 1700000000
    ?assertEqual(1700000000, maps:get(submit_time, Result)).

job_data_to_map_all_fields_test() ->
    JobData = #job_data{
        job_id = 999,
        user_id = 0,
        group_id = 0,
        partition = <<>>,
        num_nodes = 0,
        num_cpus = 0,
        time_limit = 0,
        submit_time = 0,
        priority = 0
    },
    Result = flurm_accounting:job_data_to_map(JobData),
    ?assertEqual(9, maps:size(Result)),
    ?assertEqual(999, maps:get(job_id, Result)),
    ?assertEqual(0, maps:get(user_id, Result)),
    ?assertEqual(0, maps:get(group_id, Result)),
    ?assertEqual(<<>>, maps:get(partition, Result)),
    ?assertEqual(0, maps:get(num_nodes, Result)),
    ?assertEqual(0, maps:get(num_cpus, Result)),
    ?assertEqual(0, maps:get(time_limit, Result)),
    ?assertEqual(0, maps:get(submit_time, Result)),
    ?assertEqual(0, maps:get(priority, Result)).

%%====================================================================
%% Test: safe_call/3
%%====================================================================

safe_call_success_test() ->
    %% Function that succeeds
    Fun = fun() -> ok end,
    Result = flurm_accounting:safe_call(Fun, "test_op", 1),
    ?assertEqual(ok, Result).

safe_call_catches_throw_test() ->
    %% Function that throws
    Fun = fun() -> throw(test_throw) end,
    Result = flurm_accounting:safe_call(Fun, "test_op", 2),
    ?assertEqual(ok, Result).

safe_call_catches_error_test() ->
    %% Function that raises error
    Fun = fun() -> error(test_error) end,
    Result = flurm_accounting:safe_call(Fun, "test_op", 3),
    ?assertEqual(ok, Result).

safe_call_catches_exit_test() ->
    %% Function that exits
    Fun = fun() -> exit(test_exit) end,
    Result = flurm_accounting:safe_call(Fun, "test_op", 4),
    ?assertEqual(ok, Result).

safe_call_catches_badarg_test() ->
    %% Function that causes badarg - use process dictionary to avoid compile-time detection
    put(bad_string, "not_a_number"),
    Fun = fun() ->
        Str = get(bad_string),
        list_to_integer(Str)
    end,
    Result = flurm_accounting:safe_call(Fun, "test_op", 5),
    erase(bad_string),
    ?assertEqual(ok, Result).

safe_call_function_return_value_test() ->
    %% Verify the function is actually called
    Self = self(),
    Fun = fun() -> Self ! called, ok end,
    flurm_accounting:safe_call(Fun, "test_op", 6),
    receive
        called -> ok
    after 100 ->
        ?assert(false)
    end.
