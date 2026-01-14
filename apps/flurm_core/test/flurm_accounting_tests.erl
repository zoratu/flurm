%%%-------------------------------------------------------------------
%%% @doc FLURM Accounting Helper Tests
%%%
%%% Tests for the flurm_accounting module which provides safe wrappers
%%% around accounting calls to flurm_dbd_server.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_accounting_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

accounting_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Record job submit - no crash", fun test_record_job_submit/0},
        {"Record job start - no crash", fun test_record_job_start/0},
        {"Record job end - no crash", fun test_record_job_end/0},
        {"Record job cancelled - no crash", fun test_record_job_cancelled/0},
        {"Safe call handles exceptions", fun test_safe_call_exceptions/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_data() ->
    make_job_data(#{}).

make_job_data(Overrides) ->
    Now = erlang:timestamp(),
    #job_data{
        job_id = maps:get(job_id, Overrides, 1),
        user_id = maps:get(user_id, Overrides, 1000),
        group_id = maps:get(group_id, Overrides, 1000),
        partition = maps:get(partition, Overrides, <<"default">>),
        num_nodes = maps:get(num_nodes, Overrides, 1),
        num_cpus = maps:get(num_cpus, Overrides, 4),
        time_limit = maps:get(time_limit, Overrides, 3600),
        script = maps:get(script, Overrides, <<"#!/bin/bash\necho hello">>),
        priority = maps:get(priority, Overrides, 100),
        submit_time = maps:get(submit_time, Overrides, Now),
        state_version = ?JOB_STATE_VERSION
    }.

%%====================================================================
%% Tests
%%====================================================================

test_record_job_submit() ->
    %% flurm_dbd_server is not running, but this should not crash
    JobData = make_job_data(),
    Result = flurm_accounting:record_job_submit(JobData),
    ?assertEqual(ok, Result),
    ok.

test_record_job_start() ->
    %% flurm_dbd_server is not running, but this should not crash
    Result = flurm_accounting:record_job_start(1, [<<"node1">>, <<"node2">>]),
    ?assertEqual(ok, Result),
    ok.

test_record_job_end() ->
    %% flurm_dbd_server is not running, but this should not crash
    Result = flurm_accounting:record_job_end(1, 0, completed),
    ?assertEqual(ok, Result),

    %% Test with failure exit code
    Result2 = flurm_accounting:record_job_end(2, 1, failed),
    ?assertEqual(ok, Result2),

    %% Test with timeout
    Result3 = flurm_accounting:record_job_end(3, -1, timeout),
    ?assertEqual(ok, Result3),
    ok.

test_record_job_cancelled() ->
    %% flurm_dbd_server is not running, but this should not crash
    Result = flurm_accounting:record_job_cancelled(1, user_request),
    ?assertEqual(ok, Result),

    Result2 = flurm_accounting:record_job_cancelled(2, admin_cancel),
    ?assertEqual(ok, Result2),
    ok.

test_safe_call_exceptions() ->
    %% The safe_call function should handle any exception type

    %% Test with different job data variations
    JobData1 = make_job_data(#{job_id => 100}),
    ?assertEqual(ok, flurm_accounting:record_job_submit(JobData1)),

    %% Test with undefined submit_time (should be handled)
    JobData2 = make_job_data(#{job_id => 101, submit_time => undefined}),
    ?assertEqual(ok, flurm_accounting:record_job_submit(JobData2)),

    %% Test with erlang:now() format submit_time
    JobData3 = make_job_data(#{job_id => 102, submit_time => {1000, 0, 0}}),
    ?assertEqual(ok, flurm_accounting:record_job_submit(JobData3)),
    ok.

%%====================================================================
%% Additional Tests
%%====================================================================

%% Test concurrent accounting calls
concurrent_accounting_test() ->
    Self = self(),
    NumCalls = 20,

    %% Spawn multiple processes making accounting calls
    Pids = [spawn(fun() ->
        JobData = make_job_data(#{job_id => I}),
        R1 = flurm_accounting:record_job_submit(JobData),
        R2 = flurm_accounting:record_job_start(I, [<<"node1">>]),
        R3 = flurm_accounting:record_job_end(I, 0, completed),
        Self ! {done, I, {R1, R2, R3}}
    end) || I <- lists:seq(1, NumCalls)],

    %% Wait for all to complete
    Results = [receive {done, _I, R} -> R end || _ <- Pids],

    %% All should return ok
    lists:foreach(fun({R1, R2, R3}) ->
        ?assertEqual(ok, R1),
        ?assertEqual(ok, R2),
        ?assertEqual(ok, R3)
    end, Results),
    ok.

%% Test with various exit codes
various_exit_codes_test() ->
    ExitCodes = [0, 1, 2, 127, 128, 137, 139, 255, -1, -15],
    lists:foreach(fun(Code) ->
        State = case Code of
            0 -> completed;
            _ -> failed
        end,
        ?assertEqual(ok, flurm_accounting:record_job_end(1000 + Code, Code, State))
    end, ExitCodes),
    ok.

%% Test with various cancellation reasons
various_cancel_reasons_test() ->
    Reasons = [user_request, admin_cancel, timeout, preempted,
               node_failure, dependency_failed, resource_limit],
    lists:foreach(fun(Reason) ->
        ?assertEqual(ok, flurm_accounting:record_job_cancelled(2000, Reason))
    end, Reasons),
    ok.
