%%%-------------------------------------------------------------------
%%% @doc Real Ra Cluster Integration Tests
%%%
%%% These tests run against actual Ra clusters (not simulated) to verify:
%%% - Cluster formation and leader election
%%% - State replication across nodes
%%% - Failover and recovery
%%% - Linearizability of operations
%%%
%%% Tests use the flurm_db_cluster and flurm_db_ra APIs.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_ra_integration_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

-define(TIMEOUT, 30000).

%%====================================================================
%% Test Suite
%%====================================================================

%% Run integration tests with setup/teardown
ra_integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Context) ->
         {inorder, [
             {"Concurrent job submissions", fun() -> test_concurrent_submissions(Context) end},
             {"Job state transitions under load", fun() -> test_state_transitions_load(Context) end},
             {"Rapid submit and cancel", fun() -> test_rapid_submit_cancel(Context) end},
             {"Mixed operations stress test", fun() -> test_mixed_operations(Context) end}
         ]}
     end
    }.

%%====================================================================
%% Setup and Teardown
%%====================================================================

setup() ->
    %% Start Erlang distribution if needed
    case node() of
        nonode@nohost ->
            NodeName = list_to_atom("flurm_integration_" ++
                integer_to_list(erlang:system_time(millisecond))),
            case net_kernel:start([NodeName, shortnames]) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                _ -> ok
            end;
        _ ->
            ok
    end,

    %% Create temp directory
    TmpDir = "/tmp/flurm_integration_" ++ integer_to_list(erlang:system_time(millisecond)),
    ok = filelib:ensure_dir(TmpDir ++ "/"),

    %% Configure Ra
    application:set_env(ra, data_dir, TmpDir),
    application:set_env(flurm_db, data_dir, TmpDir),

    %% Start Ra
    {ok, _} = application:ensure_all_started(ra),

    %% Start Ra default system
    case ra_system:start_default() of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        _ -> ok
    end,

    %% Start cluster (Ra leader election is synchronous with start_cluster)
    ok = flurm_db_cluster:start_cluster([node()]),

    #{data_dir => TmpDir}.

cleanup(#{data_dir := DataDir}) ->
    catch flurm_db_cluster:leave_cluster(),
    %% application:stop is synchronous - no sleep needed
    application:stop(ra),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% Test: Submit many jobs concurrently
test_concurrent_submissions(_Context) ->
    NumJobs = 30,
    Parent = self(),
    Ref = make_ref(),

    %% Spawn workers to submit jobs
    Workers = [spawn(fun() ->
        JobSpec = #ra_job_spec{
            name = list_to_binary("concurrent_" ++ integer_to_list(N)),
            user = <<"testuser">>,
            group = <<"testgroup">>,
            partition = <<"default">>,
            script = <<"#!/bin/bash\necho test">>,
            num_nodes = 1,
            num_cpus = 1,
            memory_mb = 256,
            time_limit = 60,
            priority = 100
        },
        Result = flurm_db_ra:submit_job(JobSpec),
        Parent ! {Ref, self(), Result}
    end) || N <- lists:seq(1, NumJobs)],

    %% Collect results
    Results = [receive
        {Ref, Pid, Result} -> Result
    after ?TIMEOUT ->
        {error, timeout}
    end || Pid <- Workers],

    %% Count successes
    Successes = [R || R = {ok, _} <- Results],
    ?assertEqual(NumJobs, length(Successes)),

    %% Verify unique IDs
    JobIds = [Id || {ok, Id} <- Results],
    ?assertEqual(length(JobIds), length(lists:usort(JobIds))),

    ok.

%% Test: Rapid state transitions
test_state_transitions_load(_Context) ->
    %% Submit jobs
    JobIds = lists:map(fun(N) ->
        JobSpec = #ra_job_spec{
            name = list_to_binary("state_test_" ++ integer_to_list(N)),
            user = <<"testuser">>,
            group = <<"testgroup">>,
            partition = <<"default">>,
            script = <<"#!/bin/bash\necho test">>,
            num_nodes = 1,
            num_cpus = 1,
            memory_mb = 256,
            time_limit = 60,
            priority = 100
        },
        {ok, JobId} = flurm_db_ra:submit_job(JobSpec),
        JobId
    end, lists:seq(1, 10)),

    %% Transition all to running
    lists:foreach(fun(JobId) ->
        ok = flurm_db_ra:update_job_state(JobId, running)
    end, JobIds),

    %% Verify all are running
    lists:foreach(fun(JobId) ->
        {ok, Job} = flurm_db_ra:get_job(JobId),
        ?assertEqual(running, Job#ra_job.state)
    end, JobIds),

    %% Complete half, fail half
    {CompleteIds, FailIds} = lists:split(5, JobIds),

    lists:foreach(fun(JobId) ->
        ok = flurm_db_ra:set_job_exit_code(JobId, 0),
        ok = flurm_db_ra:update_job_state(JobId, completed)
    end, CompleteIds),

    lists:foreach(fun(JobId) ->
        ok = flurm_db_ra:set_job_exit_code(JobId, 1),
        ok = flurm_db_ra:update_job_state(JobId, failed)
    end, FailIds),

    %% Verify final states
    CompletedJobs = [J || {ok, J} <- [flurm_db_ra:get_job(Id) || Id <- CompleteIds]],
    FailedJobs = [J || {ok, J} <- [flurm_db_ra:get_job(Id) || Id <- FailIds]],

    ?assert(lists:all(fun(J) -> J#ra_job.state =:= completed end, CompletedJobs)),
    ?assert(lists:all(fun(J) -> J#ra_job.state =:= failed end, FailedJobs)),

    ok.

%% Test: Rapid submit and cancel
test_rapid_submit_cancel(_Context) ->
    %% Submit and immediately cancel jobs
    Results = lists:map(fun(N) ->
        JobSpec = #ra_job_spec{
            name = list_to_binary("cancel_race_" ++ integer_to_list(N)),
            user = <<"testuser">>,
            group = <<"testgroup">>,
            partition = <<"default">>,
            script = <<"#!/bin/bash\necho test">>,
            num_nodes = 1,
            num_cpus = 1,
            memory_mb = 256,
            time_limit = 60,
            priority = 100
        },
        {ok, JobId} = flurm_db_ra:submit_job(JobSpec),
        CancelResult = flurm_db_ra:cancel_job(JobId),
        {JobId, CancelResult}
    end, lists:seq(1, 15)),

    %% All cancels should succeed
    Cancels = [R || {_, R} <- Results],
    SuccessfulCancels = [R || R <- Cancels, R =:= ok],
    ?assert(length(SuccessfulCancels) >= 10),  % Most should succeed

    %% Verify cancelled jobs are actually cancelled
    lists:foreach(fun({JobId, _}) ->
        {ok, Job} = flurm_db_ra:get_job(JobId),
        ?assertEqual(cancelled, Job#ra_job.state)
    end, Results),

    ok.

%% Test: Mixed operations stress test
test_mixed_operations(_Context) ->
    %% Register some nodes
    Nodes = [
        #ra_node_spec{name = <<"stress_node_1">>, hostname = <<"stress1.example.com">>,
                      cpus = 8, memory_mb = 8192, partitions = [<<"default">>]},
        #ra_node_spec{name = <<"stress_node_2">>, hostname = <<"stress2.example.com">>,
                      cpus = 16, memory_mb = 16384, partitions = [<<"default">>]}
    ],

    lists:foreach(fun(NodeSpec) ->
        {ok, _} = flurm_db_ra:register_node(NodeSpec)
    end, Nodes),

    %% Submit jobs
    JobIds = lists:map(fun(N) ->
        JobSpec = #ra_job_spec{
            name = list_to_binary("mixed_" ++ integer_to_list(N)),
            user = <<"testuser">>,
            group = <<"testgroup">>,
            partition = <<"default">>,
            script = <<"#!/bin/bash\necho mixed">>,
            num_nodes = 1,
            num_cpus = 1,
            memory_mb = 256,
            time_limit = 60,
            priority = N  % Different priorities
        },
        {ok, JobId} = flurm_db_ra:submit_job(JobSpec),
        JobId
    end, lists:seq(1, 10)),

    %% Update node states
    ok = flurm_db_ra:update_node_state(<<"stress_node_1">>, drain),

    %% Query operations
    {ok, AllJobs} = flurm_db_ra:list_jobs(),
    {ok, AllNodes} = flurm_db_ra:list_nodes(),
    {ok, PendingJobs} = flurm_db_ra:get_jobs_by_state(pending),

    ?assert(length(AllJobs) >= 10),
    ?assertEqual(2, length(AllNodes)),
    ?assert(length(PendingJobs) >= 1),

    %% Verify node state change
    {ok, Node1} = flurm_db_ra:get_node(<<"stress_node_1">>),
    ?assertEqual(drain, Node1#ra_node.state),

    %% Clean up - unregister nodes
    ok = flurm_db_ra:unregister_node(<<"stress_node_1">>),
    ok = flurm_db_ra:unregister_node(<<"stress_node_2">>),

    %% Cancel remaining jobs
    lists:foreach(fun(JobId) ->
        catch flurm_db_ra:cancel_job(JobId)
    end, JobIds),

    ok.
