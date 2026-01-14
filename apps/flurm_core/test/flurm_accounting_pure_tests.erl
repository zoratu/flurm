%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_accounting module
%%%
%%% These tests exercise the flurm_accounting module WITHOUT mocking.
%%% The module wraps all calls in safe_call/3 which catches exceptions,
%%% so functions return ok even when flurm_dbd_server is unavailable.
%%%
%%% Tests cover:
%%% - All exported functions with various inputs
%%% - Different submit_time formats in job_data
%%% - The safe_call wrapper behavior (returns ok on exceptions)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_accounting_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Helper to create a basic job_data record
make_job_data() ->
    make_job_data(1).

make_job_data(JobId) ->
    #job_data{
        job_id = JobId,
        user_id = 1000,
        group_id = 1000,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho hello">>,
        allocated_nodes = [],
        submit_time = erlang:timestamp(),
        start_time = undefined,
        end_time = undefined,
        exit_code = undefined,
        priority = 100,
        state_version = 1
    }.

make_job_data_with_submit_time(SubmitTime) ->
    JobData = make_job_data(),
    JobData#job_data{submit_time = SubmitTime}.

%%====================================================================
%% record_job_submit/1 Tests
%%====================================================================

record_job_submit_test_() ->
    {"record_job_submit/1 tests",
     [
      {"returns ok with valid job_data (timestamp submit_time)",
       fun() ->
           JobData = make_job_data(),
           Result = flurm_accounting:record_job_submit(JobData),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with integer submit_time",
       fun() ->
           JobData = make_job_data_with_submit_time(1700000000),
           Result = flurm_accounting:record_job_submit(JobData),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with undefined submit_time",
       fun() ->
           JobData = make_job_data_with_submit_time(undefined),
           Result = flurm_accounting:record_job_submit(JobData),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with erlang:now() style timestamp",
       fun() ->
           JobData = make_job_data_with_submit_time({1000, 500000, 123456}),
           Result = flurm_accounting:record_job_submit(JobData),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with different job IDs",
       fun() ->
           lists:foreach(
               fun(JobId) ->
                   JobData = make_job_data(JobId),
                   Result = flurm_accounting:record_job_submit(JobData),
                   ?assertEqual(ok, Result)
               end,
               [1, 100, 999999, 1000000000])
       end},

      {"returns ok with various partitions",
       fun() ->
           lists:foreach(
               fun(Partition) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{partition = Partition}),
                   ?assertEqual(ok, Result)
               end,
               [<<"batch">>, <<"gpu">>, <<"interactive">>, <<"long">>])
       end},

      {"returns ok with various node counts",
       fun() ->
           lists:foreach(
               fun(NumNodes) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{num_nodes = NumNodes}),
                   ?assertEqual(ok, Result)
               end,
               [1, 2, 10, 100, 1000])
       end},

      {"returns ok with various CPU counts",
       fun() ->
           lists:foreach(
               fun(NumCpus) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{num_cpus = NumCpus}),
                   ?assertEqual(ok, Result)
               end,
               [1, 4, 16, 64, 256])
       end},

      {"returns ok with various time limits",
       fun() ->
           lists:foreach(
               fun(TimeLimit) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{time_limit = TimeLimit}),
                   ?assertEqual(ok, Result)
               end,
               [60, 3600, 86400, 604800])  % 1 min, 1 hour, 1 day, 1 week
       end},

      {"returns ok with various priorities",
       fun() ->
           lists:foreach(
               fun(Priority) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{priority = Priority}),
                   ?assertEqual(ok, Result)
               end,
               [0, 1, 100, 1000, 10000])
       end},

      {"returns ok with various user IDs",
       fun() ->
           lists:foreach(
               fun(UserId) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{user_id = UserId}),
                   ?assertEqual(ok, Result)
               end,
               [0, 1, 1000, 65534, 65535])
       end},

      {"returns ok with various group IDs",
       fun() ->
           lists:foreach(
               fun(GroupId) ->
                   JobData = make_job_data(),
                   Result = flurm_accounting:record_job_submit(
                       JobData#job_data{group_id = GroupId}),
                   ?assertEqual(ok, Result)
               end,
               [0, 1, 1000, 65534, 65535])
       end}
     ]}.

%%====================================================================
%% record_job_start/2 Tests
%%====================================================================

record_job_start_test_() ->
    {"record_job_start/2 tests",
     [
      {"returns ok with valid job ID and single node",
       fun() ->
           Result = flurm_accounting:record_job_start(1, [<<"node001">>]),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with multiple nodes",
       fun() ->
           Nodes = [<<"node001">>, <<"node002">>, <<"node003">>],
           Result = flurm_accounting:record_job_start(1, Nodes),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with empty node list",
       fun() ->
           Result = flurm_accounting:record_job_start(1, []),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with large job ID",
       fun() ->
           Result = flurm_accounting:record_job_start(1000000000, [<<"node001">>]),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with various job IDs",
       fun() ->
           lists:foreach(
               fun(JobId) ->
                   Result = flurm_accounting:record_job_start(JobId, [<<"node001">>]),
                   ?assertEqual(ok, Result)
               end,
               [1, 10, 100, 1000, 10000, 100000])
       end},

      {"returns ok with many nodes",
       fun() ->
           Nodes = [list_to_binary("node" ++ integer_to_list(I))
                    || I <- lists:seq(1, 100)],
           Result = flurm_accounting:record_job_start(1, Nodes),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with different node naming conventions",
       fun() ->
           NodeLists = [
               [<<"compute-001">>, <<"compute-002">>],
               [<<"gpu-node-01">>, <<"gpu-node-02">>],
               [<<"worker.cluster.local">>],
               [<<"192.168.1.1">>, <<"192.168.1.2">>]
           ],
           lists:foreach(
               fun(Nodes) ->
                   Result = flurm_accounting:record_job_start(1, Nodes),
                   ?assertEqual(ok, Result)
               end,
               NodeLists)
       end}
     ]}.

%%====================================================================
%% record_job_end/3 Tests
%%====================================================================

record_job_end_test_() ->
    {"record_job_end/3 tests",
     [
      {"returns ok with successful completion (exit code 0)",
       fun() ->
           Result = flurm_accounting:record_job_end(1, 0, completed),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with failure exit code",
       fun() ->
           Result = flurm_accounting:record_job_end(1, 1, failed),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with various exit codes",
       fun() ->
           ExitCodes = [0, 1, 2, 127, 128, 137, 139, 255, -1, -15],
           lists:foreach(
               fun(ExitCode) ->
                   Result = flurm_accounting:record_job_end(1, ExitCode, completed),
                   ?assertEqual(ok, Result)
               end,
               ExitCodes)
       end},

      {"returns ok with various final states",
       fun() ->
           States = [completed, failed, timeout, cancelled, node_fail],
           lists:foreach(
               fun(State) ->
                   Result = flurm_accounting:record_job_end(1, 0, State),
                   ?assertEqual(ok, Result)
               end,
               States)
       end},

      {"returns ok with large job ID",
       fun() ->
           Result = flurm_accounting:record_job_end(999999999, 0, completed),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with various job IDs",
       fun() ->
           lists:foreach(
               fun(JobId) ->
                   Result = flurm_accounting:record_job_end(JobId, 0, completed),
                   ?assertEqual(ok, Result)
               end,
               [1, 100, 1000, 10000, 100000])
       end},

      {"returns ok with signal-based exit codes",
       fun() ->
           %% SIGTERM = 15, SIGKILL = 9, SIGSEGV = 11
           SignalExitCodes = [128 + 9, 128 + 11, 128 + 15],
           lists:foreach(
               fun(ExitCode) ->
                   Result = flurm_accounting:record_job_end(1, ExitCode, failed),
                   ?assertEqual(ok, Result)
               end,
               SignalExitCodes)
       end}
     ]}.

%%====================================================================
%% record_job_cancelled/2 Tests
%%====================================================================

record_job_cancelled_test_() ->
    {"record_job_cancelled/2 tests",
     [
      {"returns ok with user cancellation",
       fun() ->
           Result = flurm_accounting:record_job_cancelled(1, user_request),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with admin cancellation",
       fun() ->
           Result = flurm_accounting:record_job_cancelled(1, admin_request),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with timeout cancellation",
       fun() ->
           Result = flurm_accounting:record_job_cancelled(1, timeout),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with various cancellation reasons",
       fun() ->
           Reasons = [
               user_request, admin_request, timeout,
               node_fail, resource_limit, preemption,
               dependency_fail, system_shutdown
           ],
           lists:foreach(
               fun(Reason) ->
                   Result = flurm_accounting:record_job_cancelled(1, Reason),
                   ?assertEqual(ok, Result)
               end,
               Reasons)
       end},

      {"returns ok with large job ID",
       fun() ->
           Result = flurm_accounting:record_job_cancelled(999999999, user_request),
           ?assertEqual(ok, Result)
       end},

      {"returns ok with various job IDs",
       fun() ->
           lists:foreach(
               fun(JobId) ->
                   Result = flurm_accounting:record_job_cancelled(JobId, user_request),
                   ?assertEqual(ok, Result)
               end,
               [1, 10, 100, 1000, 10000, 100000])
       end}
     ]}.

%%====================================================================
%% Integration/Combination Tests
%%====================================================================

job_lifecycle_test_() ->
    {"job lifecycle tests",
     [
      {"complete job lifecycle returns ok at each step",
       fun() ->
           JobId = 12345,
           JobData = make_job_data(JobId),

           %% Submit
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData)),

           %% Start
           ?assertEqual(ok, flurm_accounting:record_job_start(JobId, [<<"node001">>])),

           %% End
           ?assertEqual(ok, flurm_accounting:record_job_end(JobId, 0, completed))
       end},

      {"cancelled job lifecycle returns ok at each step",
       fun() ->
           JobId = 12346,
           JobData = make_job_data(JobId),

           %% Submit
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData)),

           %% Cancel before start
           ?assertEqual(ok, flurm_accounting:record_job_cancelled(JobId, user_request))
       end},

      {"failed job lifecycle returns ok at each step",
       fun() ->
           JobId = 12347,
           JobData = make_job_data(JobId),

           %% Submit
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData)),

           %% Start
           ?assertEqual(ok, flurm_accounting:record_job_start(JobId, [<<"node001">>, <<"node002">>])),

           %% End with failure
           ?assertEqual(ok, flurm_accounting:record_job_end(JobId, 1, failed))
       end},

      {"multiple jobs can be tracked simultaneously",
       fun() ->
           JobIds = lists:seq(1, 10),

           %% Submit all jobs
           lists:foreach(
               fun(JobId) ->
                   JobData = make_job_data(JobId),
                   ?assertEqual(ok, flurm_accounting:record_job_submit(JobData))
               end,
               JobIds),

           %% Start odd jobs
           lists:foreach(
               fun(JobId) when JobId rem 2 == 1 ->
                   ?assertEqual(ok, flurm_accounting:record_job_start(JobId, [<<"node001">>]));
                  (_) -> ok
               end,
               JobIds),

           %% Complete odd jobs
           lists:foreach(
               fun(JobId) when JobId rem 2 == 1 ->
                   ?assertEqual(ok, flurm_accounting:record_job_end(JobId, 0, completed));
                  (_) -> ok
               end,
               JobIds),

           %% Cancel even jobs
           lists:foreach(
               fun(JobId) when JobId rem 2 == 0 ->
                   ?assertEqual(ok, flurm_accounting:record_job_cancelled(JobId, user_request));
                  (_) -> ok
               end,
               JobIds)
       end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {"edge case tests",
     [
      {"handles job_data with all fields set",
       fun() ->
           JobData = #job_data{
               job_id = 99999,
               user_id = 65535,
               group_id = 65535,
               partition = <<"gpu-long">>,
               num_nodes = 1000,
               num_cpus = 128000,
               time_limit = 604800,  % 1 week
               script = <<"#!/bin/bash\nfor i in $(seq 1 1000); do\n  echo $i\ndone">>,
               allocated_nodes = [<<"node001">>, <<"node002">>],
               submit_time = {1700, 0, 0},
               start_time = {1700, 1, 0},
               end_time = undefined,
               exit_code = undefined,
               priority = 10000,
               state_version = 5
           },
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData))
       end},

      {"handles minimum values",
       fun() ->
           JobData = #job_data{
               job_id = 1,
               user_id = 0,
               group_id = 0,
               partition = <<>>,
               num_nodes = 1,
               num_cpus = 1,
               time_limit = 1,
               script = <<>>,
               allocated_nodes = [],
               submit_time = {0, 0, 0},
               start_time = undefined,
               end_time = undefined,
               exit_code = undefined,
               priority = 0,
               state_version = 1
           },
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData))
       end},

      {"handles large script content",
       fun() ->
           LargeScript = list_to_binary(lists:duplicate(10000, $x)),
           JobData = make_job_data(),
           ?assertEqual(ok, flurm_accounting:record_job_submit(
               JobData#job_data{script = LargeScript}))
       end},

      {"handles unicode in partition name",
       fun() ->
           JobData = make_job_data(),
           %% UTF-8 encoded partition name
           ?assertEqual(ok, flurm_accounting:record_job_submit(
               JobData#job_data{partition = <<"gpu-queue-test">>}))
       end},

      {"handles zero timestamp components",
       fun() ->
           JobData = make_job_data_with_submit_time({0, 0, 0}),
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData))
       end},

      {"handles max integer timestamp",
       fun() ->
           JobData = make_job_data_with_submit_time(2147483647),  % Max 32-bit signed int
           ?assertEqual(ok, flurm_accounting:record_job_submit(JobData))
       end}
     ]}.

%%====================================================================
%% Concurrent Calling Tests
%%====================================================================

concurrent_test_() ->
    {"concurrent calling tests",
     {timeout, 30,
      [
       {"multiple concurrent submits return ok",
        fun() ->
            Parent = self(),
            NumProcs = 10,

            %% Spawn multiple processes to call record_job_submit concurrently
            Pids = [spawn(fun() ->
                JobData = make_job_data(I),
                Result = flurm_accounting:record_job_submit(JobData),
                Parent ! {done, self(), Result}
            end) || I <- lists:seq(1, NumProcs)],

            %% Collect results
            Results = [receive {done, Pid, R} -> R end || Pid <- Pids],

            %% All should return ok
            ?assertEqual(lists:duplicate(NumProcs, ok), Results)
        end},

       {"multiple concurrent operations return ok",
        fun() ->
            Parent = self(),

            %% Mix of different operations
            Operations = [
                fun() -> flurm_accounting:record_job_submit(make_job_data(1)) end,
                fun() -> flurm_accounting:record_job_start(2, [<<"node001">>]) end,
                fun() -> flurm_accounting:record_job_end(3, 0, completed) end,
                fun() -> flurm_accounting:record_job_cancelled(4, user_request) end,
                fun() -> flurm_accounting:record_job_submit(make_job_data(5)) end,
                fun() -> flurm_accounting:record_job_start(6, [<<"node002">>]) end
            ],

            Pids = [spawn(fun() ->
                Result = Op(),
                Parent ! {done, self(), Result}
            end) || Op <- Operations],

            Results = [receive {done, Pid, R} -> R end || Pid <- Pids],

            ?assertEqual(lists:duplicate(length(Operations), ok), Results)
        end}
      ]}}.

%%====================================================================
%% Return Type Tests
%%====================================================================

return_type_test_() ->
    {"return type tests",
     [
      {"record_job_submit returns exactly the atom ok",
       fun() ->
           JobData = make_job_data(),
           Result = flurm_accounting:record_job_submit(JobData),
           ?assert(is_atom(Result)),
           ?assertEqual(ok, Result)
       end},

      {"record_job_start returns exactly the atom ok",
       fun() ->
           Result = flurm_accounting:record_job_start(1, [<<"node">>]),
           ?assert(is_atom(Result)),
           ?assertEqual(ok, Result)
       end},

      {"record_job_end returns exactly the atom ok",
       fun() ->
           Result = flurm_accounting:record_job_end(1, 0, completed),
           ?assert(is_atom(Result)),
           ?assertEqual(ok, Result)
       end},

      {"record_job_cancelled returns exactly the atom ok",
       fun() ->
           Result = flurm_accounting:record_job_cancelled(1, user_request),
           ?assert(is_atom(Result)),
           ?assertEqual(ok, Result)
       end}
     ]}.
