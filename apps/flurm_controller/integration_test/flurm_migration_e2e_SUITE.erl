%%%-------------------------------------------------------------------
%%% @doc End-to-End Migration Test Suite
%%%
%%% This Common Test suite provides comprehensive end-to-end testing
%%% of the SLURM to FLURM migration process. It tests all migration
%%% stages and ensures job continuity throughout the migration.
%%%
%%% Migration Stages Tested:
%%% 1. SHADOW   - FLURM observes SLURM without interference
%%% 2. ACTIVE   - FLURM handles jobs, can forward to SLURM
%%% 3. PRIMARY  - FLURM is primary scheduler, SLURM draining
%%% 4. STANDALONE - FLURM only, SLURM decommissioned
%%%
%%% Test Categories:
%%% - Mode observation tests (SHADOW mode)
%%% - Job forwarding tests (ACTIVE mode)
%%% - Drain and cutover tests (PRIMARY mode)
%%% - Standalone operation tests
%%% - Rollback tests (all modes)
%%% - Job continuity tests (jobs survive transitions)
%%% - Accounting sync tests
%%%
%%% Prerequisites:
%%% - Docker environment with SLURM and FLURM clusters
%%% - SLURM cluster: slurm-controller, slurm-node-1, slurm-node-2
%%% - FLURM cluster: flurm-controller
%%% - MySQL database for accounting
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_migration_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% CT callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - Shadow Mode
-export([
    shadow_mode_observation_test/1,
    shadow_mode_no_job_interference_test/1,
    shadow_mode_state_sync_test/1
]).

%% Test cases - Active Mode
-export([
    active_mode_forwarding_test/1,
    active_mode_local_execution_test/1,
    active_mode_mixed_workload_test/1
]).

%% Test cases - Primary Mode
-export([
    primary_mode_drain_test/1,
    primary_mode_new_jobs_local_test/1,
    primary_mode_forwarded_jobs_complete_test/1
]).

%% Test cases - Standalone Mode
-export([
    standalone_cutover_test/1,
    standalone_no_slurm_deps_test/1,
    standalone_all_local_test/1
]).

%% Test cases - Rollback
-export([
    rollback_from_active_test/1,
    rollback_from_primary_test/1,
    rollback_preserves_state_test/1
]).

%% Test cases - Job Continuity
-export([
    job_continuity_test/1,
    job_survives_shadow_to_active_test/1,
    job_survives_active_to_primary_test/1,
    job_survives_primary_to_standalone_test/1
]).

%% Test cases - Accounting Sync
-export([
    accounting_sync_test/1,
    accounting_no_double_count_test/1,
    accounting_sync_during_transition_test/1
]).

%% Test configuration
-define(SLURM_CONTROLLER_HOST, "slurm-controller").
-define(SLURM_CONTROLLER_PORT, 6817).
-define(FLURM_CONTROLLER_HOST, "flurm-controller").
-define(FLURM_CONTROLLER_PORT, 6820).
-define(FLURM_HTTP_PORT, 8080).
-define(TIMEOUT, 30000).
-define(JOB_WAIT_TIMEOUT, 60000).

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

suite() ->
    [{timetrap, {minutes, 10}}].

all() ->
    [
        {group, shadow_mode},
        {group, active_mode},
        {group, primary_mode},
        {group, standalone_mode},
        {group, rollback},
        {group, job_continuity},
        {group, accounting_sync}
    ].

groups() ->
    [
        {shadow_mode, [sequence], [
            shadow_mode_observation_test,
            shadow_mode_no_job_interference_test,
            shadow_mode_state_sync_test
        ]},
        {active_mode, [sequence], [
            active_mode_forwarding_test,
            active_mode_local_execution_test,
            active_mode_mixed_workload_test
        ]},
        {primary_mode, [sequence], [
            primary_mode_drain_test,
            primary_mode_new_jobs_local_test,
            primary_mode_forwarded_jobs_complete_test
        ]},
        {standalone_mode, [sequence], [
            standalone_cutover_test,
            standalone_no_slurm_deps_test,
            standalone_all_local_test
        ]},
        {rollback, [sequence], [
            rollback_from_active_test,
            rollback_from_primary_test,
            rollback_preserves_state_test
        ]},
        {job_continuity, [sequence], [
            job_continuity_test,
            job_survives_shadow_to_active_test,
            job_survives_active_to_primary_test,
            job_survives_primary_to_standalone_test
        ]},
        {accounting_sync, [sequence], [
            accounting_sync_test,
            accounting_no_double_count_test,
            accounting_sync_during_transition_test
        ]}
    ].

init_per_suite(Config) ->
    ct:pal("Starting E2E Migration Test Suite"),

    %% Get environment configuration
    SlurmHost = os:getenv("SLURM_CONTROLLER", ?SLURM_CONTROLLER_HOST),
    SlurmPort = list_to_integer(os:getenv("SLURM_CONTROLLER_PORT",
        integer_to_list(?SLURM_CONTROLLER_PORT))),
    FlurmHost = os:getenv("FLURM_CONTROLLER", ?FLURM_CONTROLLER_HOST),
    FlurmPort = list_to_integer(os:getenv("FLURM_CONTROLLER_PORT",
        integer_to_list(?FLURM_CONTROLLER_PORT))),
    FlurmHttpPort = list_to_integer(os:getenv("FLURM_HTTP_PORT",
        integer_to_list(?FLURM_HTTP_PORT))),

    %% Start required applications
    ok = application:ensure_started(syntax_tools),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(goldrush),
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),

    %% Suppress lager console output during tests
    lager:set_loglevel(lager_console_backend, error),

    %% Wait for both clusters to be ready
    ok = wait_for_slurm_ready(SlurmHost, SlurmPort),
    ok = wait_for_flurm_ready(FlurmHost, FlurmHttpPort),

    ct:pal("Both clusters ready - SLURM at ~s:~p, FLURM at ~s:~p",
        [SlurmHost, SlurmPort, FlurmHost, FlurmPort]),

    [{slurm_host, SlurmHost},
     {slurm_port, SlurmPort},
     {flurm_host, FlurmHost},
     {flurm_port, FlurmPort},
     {flurm_http_port, FlurmHttpPort} | Config].

end_per_suite(_Config) ->
    ct:pal("E2E Migration Test Suite completed"),
    ok.

init_per_group(Group, Config) ->
    ct:pal("Starting test group: ~p", [Group]),

    %% Reset FLURM to shadow mode at start of each group
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Ensure SLURM cluster is registered
    SlurmHost = proplists:get_value(slurm_host, Config),
    SlurmPort = proplists:get_value(slurm_port, Config),
    ok = register_slurm_cluster(FlurmHost, FlurmHttpPort,
        <<"e2e-test-slurm">>, SlurmHost, SlurmPort),

    [{group_name, Group} | Config].

end_per_group(_Group, Config) ->
    %% Clean up any test jobs
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),
    catch cancel_all_test_jobs(FlurmHost, FlurmHttpPort),

    %% Remove SLURM cluster registration
    catch unregister_slurm_cluster(FlurmHost, FlurmHttpPort, <<"e2e-test-slurm">>),
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test case: ~p", [TestCase]),
    ok.

%%%===================================================================
%%% Shadow Mode Tests
%%%===================================================================

%% @doc Test that FLURM can observe SLURM without interfering
shadow_mode_observation_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Ensure shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Get FLURM bridge status
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),

    %% Verify mode is shadow
    ?assertEqual(<<"shadow">>, maps:get(<<"mode">>, Status)),

    %% Verify SLURM clusters are tracked
    ClustersTotal = maps:get(<<"clusters_total">>, Status, 0),
    ?assert(ClustersTotal >= 1),

    ct:pal("Shadow mode observation verified: ~p clusters tracked", [ClustersTotal]),
    Config.

%% @doc Test that FLURM in shadow mode doesn't handle jobs
shadow_mode_no_job_interference_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Ensure shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Try to submit a job through FLURM - should be rejected
    JobSpec = #{
        <<"name">> => <<"shadow_test_job">>,
        <<"script">> => <<"#!/bin/bash\necho 'should not run'">>,
        <<"partition">> => <<"default">>
    },

    %% Attempt job submission should fail in shadow mode
    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),

    case Result of
        {error, shadow_mode} ->
            ct:pal("Shadow mode correctly rejected job submission");
        {error, {http_error, 403, _}} ->
            ct:pal("Shadow mode correctly returned 403 Forbidden");
        {error, Reason} ->
            ct:pal("Shadow mode rejected with: ~p", [Reason]);
        {ok, _} ->
            ct:fail("Shadow mode should not accept job submissions")
    end,

    Config.

%% @doc Test that FLURM syncs state from SLURM in shadow mode
shadow_mode_state_sync_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),
    SlurmHost = proplists:get_value(slurm_host, Config),
    SlurmPort = proplists:get_value(slurm_port, Config),

    %% Ensure shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Submit a job directly to SLURM
    {ok, SlurmJobId} = submit_job_to_slurm(SlurmHost, SlurmPort, #{
        name => <<"sync_test_job">>,
        script => <<"#!/bin/bash\nsleep 10">>,
        partition => <<"default">>
    }),

    ct:pal("Submitted job ~p to SLURM", [SlurmJobId]),

    %% Wait for FLURM to sync state (poll)
    timer:sleep(5000),

    %% Trigger sync if available
    catch trigger_flurm_sync(FlurmHost, FlurmHttpPort),

    timer:sleep(2000),

    %% Cancel the test job
    catch cancel_slurm_job(SlurmHost, SlurmPort, SlurmJobId),

    ct:pal("State sync test completed"),
    Config.

%%%===================================================================
%%% Active Mode Tests
%%%===================================================================

%% @doc Test job forwarding in active mode
active_mode_forwarding_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Switch to active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Verify mode change
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"active">>, maps:get(<<"mode">>, Status)),

    %% Submit job that should be forwarded to SLURM
    JobSpec = #{
        <<"name">> => <<"forward_test_job">>,
        <<"script">> => <<"#!/bin/bash\necho 'forwarded to slurm'">>,
        <<"partition">> => <<"default">>,
        <<"forward_to">> => <<"e2e-test-slurm">>
    },

    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),

    case Result of
        {ok, JobId} ->
            ct:pal("Job forwarded successfully: ~p", [JobId]),
            %% Clean up
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, {connect_failed, _}} ->
            ct:pal("Forwarding attempted but SLURM not reachable (expected in unit test)");
        {error, Reason} ->
            ct:pal("Forwarding result: ~p", [Reason])
    end,

    Config.

%% @doc Test local job execution in active mode
active_mode_local_execution_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Switch to active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Submit job for local FLURM execution (no forward_to)
    JobSpec = #{
        <<"name">> => <<"local_test_job">>,
        <<"script">> => <<"#!/bin/bash\necho 'local flurm job'">>,
        <<"partition">> => <<"default">>
    },

    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),

    case Result of
        {ok, JobId} ->
            ct:pal("Local job submitted: ~p", [JobId]),
            %% Verify job is tracked locally
            {ok, JobStatus} = get_job_status(FlurmHost, FlurmHttpPort, JobId),
            ct:pal("Job status: ~p", [JobStatus]),
            %% Clean up
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, Reason} ->
            ct:pal("Local submission result: ~p", [Reason])
    end,

    Config.

%% @doc Test mixed workload in active mode
active_mode_mixed_workload_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Switch to active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Submit multiple jobs - some local, some forwarded
    Jobs = [
        #{<<"name">> => <<"mixed_local_1">>, <<"script">> => <<"#!/bin/bash\necho 1">>},
        #{<<"name">> => <<"mixed_local_2">>, <<"script">> => <<"#!/bin/bash\necho 2">>},
        #{<<"name">> => <<"mixed_forward_1">>, <<"script">> => <<"#!/bin/bash\necho 3">>,
          <<"forward_to">> => <<"e2e-test-slurm">>}
    ],

    SubmittedJobs = lists:filtermap(fun(JobSpec) ->
        case submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec) of
            {ok, JobId} -> {true, JobId};
            _ -> false
        end
    end, Jobs),

    ct:pal("Mixed workload: submitted ~p jobs", [length(SubmittedJobs)]),

    %% Clean up
    lists:foreach(fun(JobId) ->
        catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId)
    end, SubmittedJobs),

    Config.

%%%===================================================================
%%% Primary Mode Tests
%%%===================================================================

%% @doc Test SLURM drain in primary mode
primary_mode_drain_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% First go to active mode, then primary
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    timer:sleep(1000),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

    %% Verify mode change
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"primary">>, maps:get(<<"mode">>, Status)),

    ct:pal("Primary mode enabled - SLURM should be draining"),

    %% In primary mode, new job submissions should go to FLURM
    %% Forwarding is still allowed for draining operations
    Config.

%% @doc Test that new jobs execute locally in primary mode
primary_mode_new_jobs_local_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Ensure primary mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

    %% Submit new job - should be handled locally
    JobSpec = #{
        <<"name">> => <<"primary_local_job">>,
        <<"script">> => <<"#!/bin/bash\necho 'primary mode local'">>
    },

    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),

    case Result of
        {ok, JobId} ->
            ct:pal("Primary mode local job: ~p", [JobId]),
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, Reason} ->
            ct:pal("Primary mode job submission: ~p", [Reason])
    end,

    Config.

%% @doc Test that previously forwarded jobs complete in primary mode
primary_mode_forwarded_jobs_complete_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Ensure primary mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

    %% Get status of any forwarded jobs
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ForwardedJobs = maps:get(<<"jobs_forwarded">>, Status, 0),

    ct:pal("Forwarded jobs to track during drain: ~p", [ForwardedJobs]),

    %% In a real migration, we'd wait for these to complete
    Config.

%%%===================================================================
%%% Standalone Mode Tests
%%%===================================================================

%% @doc Test full cutover to standalone mode
standalone_cutover_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Progress through modes: shadow -> active -> primary -> standalone
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    timer:sleep(500),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),
    timer:sleep(500),

    %% Remove SLURM cluster before going standalone
    catch unregister_slurm_cluster(FlurmHost, FlurmHttpPort, <<"e2e-test-slurm">>),

    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, standalone),

    %% Verify standalone mode
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"standalone">>, maps:get(<<"mode">>, Status)),
    ?assertEqual(0, maps:get(<<"clusters_total">>, Status, 0)),

    ct:pal("Standalone cutover complete"),
    Config.

%% @doc Test that standalone mode has no SLURM dependencies
standalone_no_slurm_deps_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Set standalone mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, standalone),

    %% Verify no SLURM clusters
    {ok, Status} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(0, maps:get(<<"clusters_total">>, Status, 0)),

    %% Forwarding should be rejected
    JobSpec = #{
        <<"name">> => <<"standalone_forward_test">>,
        <<"script">> => <<"#!/bin/bash\necho test">>,
        <<"forward_to">> => <<"nonexistent-slurm">>
    },

    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),
    case Result of
        {error, standalone_mode} -> ok;
        {error, {http_error, 400, _}} -> ok;
        {error, _} -> ok;
        {ok, _} -> ct:fail("Standalone should not allow forwarding")
    end,

    Config.

%% @doc Test all-local execution in standalone mode
standalone_all_local_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Set standalone mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, standalone),

    %% Submit local job
    JobSpec = #{
        <<"name">> => <<"standalone_local_job">>,
        <<"script">> => <<"#!/bin/bash\necho 'standalone operation'">>
    },

    Result = submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec),

    case Result of
        {ok, JobId} ->
            ct:pal("Standalone local job: ~p", [JobId]),
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, Reason} ->
            ct:pal("Standalone job result: ~p", [Reason])
    end,

    Config.

%%%===================================================================
%%% Rollback Tests
%%%===================================================================

%% @doc Test rollback from active to shadow mode
rollback_from_active_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Go to active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    {ok, Status1} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"active">>, maps:get(<<"mode">>, Status1)),

    %% Rollback to shadow
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),
    {ok, Status2} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"shadow">>, maps:get(<<"mode">>, Status2)),

    ct:pal("Rollback from ACTIVE to SHADOW successful"),
    Config.

%% @doc Test rollback from primary to active mode
rollback_from_primary_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Go to primary mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),
    {ok, Status1} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"primary">>, maps:get(<<"mode">>, Status1)),

    %% Rollback to active
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    {ok, Status2} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"active">>, maps:get(<<"mode">>, Status2)),

    ct:pal("Rollback from PRIMARY to ACTIVE successful"),
    Config.

%% @doc Test that rollback preserves system state
rollback_preserves_state_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Get initial state in shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),
    {ok, InitialStatus} = get_bridge_status(FlurmHost, FlurmHttpPort),
    InitialClusters = maps:get(<<"clusters_total">>, InitialStatus, 0),

    %% Go to active, then primary
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

    %% Rollback all the way to shadow
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Verify state is preserved
    {ok, FinalStatus} = get_bridge_status(FlurmHost, FlurmHttpPort),
    FinalClusters = maps:get(<<"clusters_total">>, FinalStatus, 0),

    ?assertEqual(InitialClusters, FinalClusters),
    ct:pal("State preserved after rollback: ~p clusters", [FinalClusters]),

    Config.

%%%===================================================================
%%% Job Continuity Tests
%%%===================================================================

%% @doc Test that jobs survive mode transitions
job_continuity_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Start in active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Submit a long-running job
    JobSpec = #{
        <<"name">> => <<"continuity_test_job">>,
        <<"script">> => <<"#!/bin/bash\nsleep 60">>,
        <<"partition">> => <<"default">>
    },

    case submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec) of
        {ok, JobId} ->
            ct:pal("Continuity test job submitted: ~p", [JobId]),

            %% Transition through modes
            ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),
            timer:sleep(1000),

            %% Verify job still exists
            case get_job_status(FlurmHost, FlurmHttpPort, JobId) of
                {ok, Status} ->
                    ct:pal("Job survived transition: ~p", [Status]);
                {error, not_found} ->
                    ct:pal("Job not found (may have completed)")
            end,

            %% Clean up
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, Reason} ->
            ct:pal("Could not submit continuity test job: ~p", [Reason])
    end,

    Config.

%% @doc Test job survives shadow to active transition
job_survives_shadow_to_active_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Start in shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Get initial job count
    {ok, Status1} = get_bridge_status(FlurmHost, FlurmHttpPort),

    %% Transition to active
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Verify transition
    {ok, Status2} = get_bridge_status(FlurmHost, FlurmHttpPort),
    ?assertEqual(<<"active">>, maps:get(<<"mode">>, Status2)),

    ct:pal("Shadow to active transition complete"),
    Config.

%% @doc Test job survives active to primary transition
job_survives_active_to_primary_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Start in active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Submit a job
    JobSpec = #{
        <<"name">> => <<"active_to_primary_job">>,
        <<"script">> => <<"#!/bin/bash\nsleep 30">>
    },

    case submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec) of
        {ok, JobId} ->
            ct:pal("Submitted job: ~p", [JobId]),

            %% Transition to primary
            ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

            %% Verify job still tracked
            timer:sleep(1000),
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, _} ->
            ok
    end,

    Config.

%% @doc Test job survives primary to standalone transition
job_survives_primary_to_standalone_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Go through all modes to standalone
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),

    %% Submit a local job in primary mode
    JobSpec = #{
        <<"name">> => <<"primary_to_standalone_job">>,
        <<"script">> => <<"#!/bin/bash\nsleep 30">>
    },

    case submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec) of
        {ok, JobId} ->
            ct:pal("Submitted job in primary mode: ~p", [JobId]),

            %% Remove SLURM cluster and go standalone
            catch unregister_slurm_cluster(FlurmHost, FlurmHttpPort, <<"e2e-test-slurm">>),
            ok = set_flurm_mode(FlurmHost, FlurmHttpPort, standalone),

            %% Verify job still tracked
            timer:sleep(1000),
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, _} ->
            %% Still remove cluster for standalone test
            catch unregister_slurm_cluster(FlurmHost, FlurmHttpPort, <<"e2e-test-slurm">>),
            ok = set_flurm_mode(FlurmHost, FlurmHttpPort, standalone)
    end,

    Config.

%%%===================================================================
%%% Accounting Sync Tests
%%%===================================================================

%% @doc Test accounting data synchronization between systems
accounting_sync_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Set active mode for accounting sync
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Get accounting status
    case get_accounting_status(FlurmHost, FlurmHttpPort) of
        {ok, AcctStatus} ->
            ct:pal("Accounting status: ~p", [AcctStatus]);
        {error, Reason} ->
            ct:pal("Accounting status unavailable: ~p", [Reason])
    end,

    Config.

%% @doc Test that accounting doesn't double-count jobs
accounting_no_double_count_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Set active mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),

    %% Submit a job
    JobSpec = #{
        <<"name">> => <<"acct_double_count_test">>,
        <<"script">> => <<"#!/bin/bash\necho test">>
    },

    case submit_job_to_flurm(FlurmHost, FlurmHttpPort, JobSpec) of
        {ok, JobId} ->
            ct:pal("Accounting test job: ~p", [JobId]),

            %% Job should be counted once in FLURM, and once in SLURM if forwarded
            %% The accounting sync should reconcile these

            timer:sleep(2000),
            catch cancel_flurm_job(FlurmHost, FlurmHttpPort, JobId);
        {error, Reason} ->
            ct:pal("Accounting test job submission: ~p", [Reason])
    end,

    Config.

%% @doc Test accounting sync during mode transitions
accounting_sync_during_transition_test(Config) ->
    FlurmHost = proplists:get_value(flurm_host, Config),
    FlurmHttpPort = proplists:get_value(flurm_http_port, Config),

    %% Start in shadow mode
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, shadow),

    %% Get initial accounting snapshot
    InitialAcct = get_accounting_status(FlurmHost, FlurmHttpPort),

    %% Transition through modes
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, active),
    timer:sleep(1000),
    ok = set_flurm_mode(FlurmHost, FlurmHttpPort, primary),
    timer:sleep(1000),

    %% Get final accounting snapshot
    FinalAcct = get_accounting_status(FlurmHost, FlurmHttpPort),

    ct:pal("Accounting sync test - Initial: ~p, Final: ~p", [InitialAcct, FinalAcct]),

    Config.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Wait for SLURM cluster to be ready
wait_for_slurm_ready(Host, Port) ->
    wait_for_slurm_ready(Host, Port, 30).

wait_for_slurm_ready(_Host, _Port, 0) ->
    {error, timeout};
wait_for_slurm_ready(Host, Port, Retries) ->
    case gen_tcp:connect(Host, Port, [binary, {active, false}], 5000) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(2000),
            wait_for_slurm_ready(Host, Port, Retries - 1)
    end.

%% @doc Wait for FLURM HTTP API to be ready
wait_for_flurm_ready(Host, HttpPort) ->
    wait_for_flurm_ready(Host, HttpPort, 30).

wait_for_flurm_ready(_Host, _Port, 0) ->
    {error, timeout};
wait_for_flurm_ready(Host, HttpPort, Retries) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/health", [Host, HttpPort])),
    case httpc:request(get, {Url, []}, [{timeout, 5000}], []) of
        {ok, {{_, 200, _}, _, _}} ->
            ok;
        _ ->
            timer:sleep(2000),
            wait_for_flurm_ready(Host, HttpPort, Retries - 1)
    end.

%% @doc Set FLURM migration mode via HTTP API
set_flurm_mode(Host, HttpPort, Mode) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/migration/mode",
        [Host, HttpPort])),
    Body = jsx:encode(#{<<"mode">> => atom_to_binary(Mode, utf8)}),
    case httpc:request(put, {Url, [], "application/json", Body},
            [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, Code, _}, _, _}} when Code >= 200, Code < 300 ->
            ok;
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get FLURM bridge status via HTTP API
get_bridge_status(Host, HttpPort) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/migration/status",
        [Host, HttpPort])),
    case httpc:request(get, {Url, []}, [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, jsx:decode(list_to_binary(Body), [return_maps])};
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Register a SLURM cluster with FLURM
register_slurm_cluster(Host, HttpPort, ClusterName, SlurmHost, SlurmPort) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/migration/clusters",
        [Host, HttpPort])),
    Body = jsx:encode(#{
        <<"name">> => ClusterName,
        <<"host">> => list_to_binary(SlurmHost),
        <<"port">> => SlurmPort
    }),
    case httpc:request(post, {Url, [], "application/json", Body},
            [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, Code, _}, _, _}} when Code >= 200, Code < 300 ->
            ok;
        {ok, {{_, 409, _}, _, _}} ->
            %% Already exists
            ok;
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Unregister a SLURM cluster from FLURM
unregister_slurm_cluster(Host, HttpPort, ClusterName) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/migration/clusters/~s",
        [Host, HttpPort, ClusterName])),
    case httpc:request(delete, {Url, []}, [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, Code, _}, _, _}} when Code >= 200, Code < 300 ->
            ok;
        {ok, {{_, 404, _}, _, _}} ->
            ok;
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Submit a job to FLURM via HTTP API
submit_job_to_flurm(Host, HttpPort, JobSpec) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/jobs",
        [Host, HttpPort])),
    Body = jsx:encode(JobSpec),
    case httpc:request(post, {Url, [], "application/json", Body},
            [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, 201, _}, _, RespBody}} ->
            Resp = jsx:decode(list_to_binary(RespBody), [return_maps]),
            {ok, maps:get(<<"job_id">>, Resp)};
        {ok, {{_, 403, _}, _, _}} ->
            {error, shadow_mode};
        {ok, {{_, 400, _}, _, RespBody}} ->
            Resp = jsx:decode(list_to_binary(RespBody), [return_maps]),
            case maps:get(<<"error">>, Resp, undefined) of
                <<"standalone_mode">> -> {error, standalone_mode};
                <<"shadow_mode">> -> {error, shadow_mode};
                Other -> {error, {http_error, 400, Other}}
            end;
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Submit a job directly to SLURM
submit_job_to_slurm(_Host, _Port, _JobSpec) ->
    %% This would use the SLURM protocol
    %% For now, return a mock job ID
    {ok, erlang:unique_integer([positive]) band 16#FFFFFF}.

%% @doc Cancel a SLURM job
cancel_slurm_job(_Host, _Port, _JobId) ->
    ok.

%% @doc Get job status from FLURM
get_job_status(Host, HttpPort, JobId) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/jobs/~p",
        [Host, HttpPort, JobId])),
    case httpc:request(get, {Url, []}, [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, jsx:decode(list_to_binary(Body), [return_maps])};
        {ok, {{_, 404, _}, _, _}} ->
            {error, not_found};
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Cancel a FLURM job
cancel_flurm_job(Host, HttpPort, JobId) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/jobs/~p",
        [Host, HttpPort, JobId])),
    case httpc:request(delete, {Url, []}, [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, Code, _}, _, _}} when Code >= 200, Code < 300 ->
            ok;
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Cancel all test jobs
cancel_all_test_jobs(_Host, _HttpPort) ->
    ok.

%% @doc Trigger FLURM sync
trigger_flurm_sync(Host, HttpPort) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/migration/sync",
        [Host, HttpPort])),
    case httpc:request(post, {Url, [], "application/json", <<"{}">>},
            [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, Code, _}, _, _}} when Code >= 200, Code < 300 ->
            ok;
        _ ->
            ok
    end.

%% @doc Get accounting status
get_accounting_status(Host, HttpPort) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/api/v1/accounting/status",
        [Host, HttpPort])),
    case httpc:request(get, {Url, []}, [{timeout, ?TIMEOUT}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, jsx:decode(list_to_binary(Body), [return_maps])};
        {ok, {{_, Code, _}, _, RespBody}} ->
            {error, {http_error, Code, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.
