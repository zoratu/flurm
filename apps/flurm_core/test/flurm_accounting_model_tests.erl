%%%-------------------------------------------------------------------
%%% @doc Deterministic accounting lifecycle model tests.
%%%
%%% Replays fixed submit/start/end/cancel traces with injected dbd failures
%%% and verifies wrapper invariants:
%%% - accounting APIs never crash callers
%%% - successful events are recorded with stable ordering
%%%-------------------------------------------------------------------
-module(flurm_accounting_model_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-define(FAIL_KEY, {?MODULE, fail_ops}).
-define(CALL_KEY, {?MODULE, calls}).

accounting_model_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"deterministic accounting trace", fun deterministic_accounting_trace_case/0},
      {"deterministic failure-injection trace", fun deterministic_failure_trace_case/0}
     ]}.

deterministic_accounting_trace_case() ->
    JobData = job_data(1001),
    Ops = [
        {submit, JobData},
        {start, 1001, [<<"node1">>]},
        {finish, 1001, 0, completed},
        {cancel, 1001, user_cancelled}
    ],
    replay(Ops),
    Calls = lists:reverse(persistent_term:get(?CALL_KEY)),
    ?assertEqual(
        [{record_job_submit, 1001},
         {record_job_start, 1001},
         {record_job_end, 1001},
         {record_job_cancelled, 1001}],
        Calls
    ).

deterministic_failure_trace_case() ->
    JobData = job_data(1002),
    set_failures([record_job_start, record_job_end]),
    Ops = [
        {submit, JobData},
        {start, 1002, [<<"node2">>]},
        {finish, 1002, 1, failed},
        {cancel, 1002, preempted}
    ],
    replay(Ops),
    Calls = lists:reverse(persistent_term:get(?CALL_KEY)),
    %% Failed operations are swallowed by flurm_accounting:safe_call/3
    ?assertEqual(
        [{record_job_submit, 1002},
         {record_job_cancelled, 1002}],
        Calls
    ).

replay(Ops) ->
    lists:foreach(fun apply_op/1, Ops).

apply_op({submit, JobData}) ->
    ?assertEqual(ok, flurm_accounting:record_job_submit(JobData));
apply_op({start, JobId, Nodes}) ->
    ?assertEqual(ok, flurm_accounting:record_job_start(JobId, Nodes));
apply_op({finish, JobId, ExitCode, FinalState}) ->
    ?assertEqual(ok, flurm_accounting:record_job_end(JobId, ExitCode, FinalState));
apply_op({cancel, JobId, Reason}) ->
    ?assertEqual(ok, flurm_accounting:record_job_cancelled(JobId, Reason)).

job_data(JobId) ->
    #job_data{
        job_id = JobId,
        user_id = 1000,
        group_id = 1000,
        partition = <<"debug">>,
        num_nodes = 1,
        num_cpus = 1,
        time_limit = 300,
        submit_time = erlang:system_time(second),
        priority = 100
    }.

set_failures(Ops) ->
    persistent_term:put(?FAIL_KEY, Ops).

setup() ->
    persistent_term:put(?FAIL_KEY, []),
    persistent_term:put(?CALL_KEY, []),

    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [non_strict]),
    meck:expect(flurm_dbd_server, record_job_submit, fun(JobMap) ->
        maybe_fail(record_job_submit, maps:get(job_id, JobMap))
    end),
    meck:expect(flurm_dbd_server, record_job_start, fun(JobId, _Nodes) ->
        maybe_fail(record_job_start, JobId)
    end),
    meck:expect(flurm_dbd_server, record_job_end, fun(JobId, _ExitCode, _FinalState) ->
        maybe_fail(record_job_end, JobId)
    end),
    meck:expect(flurm_dbd_server, record_job_cancelled, fun(JobId, _Reason) ->
        maybe_fail(record_job_cancelled, JobId)
    end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_server),
    persistent_term:erase(?FAIL_KEY),
    persistent_term:erase(?CALL_KEY),
    ok.

maybe_fail(Op, JobId) ->
    FailOps = persistent_term:get(?FAIL_KEY, []),
    case lists:member(Op, FailOps) of
        true -> erlang:error({injected_failure, Op, JobId});
        false ->
            Calls0 = persistent_term:get(?CALL_KEY, []),
            persistent_term:put(?CALL_KEY, [{Op, JobId} | Calls0]),
            ok
    end.
