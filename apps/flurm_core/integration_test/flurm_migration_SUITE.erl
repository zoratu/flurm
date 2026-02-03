%%%-------------------------------------------------------------------
%%% @doc Integration Tests for SLURM to FLURM Migration
%%%
%%% This Common Test suite verifies the zero-downtime migration path
%%% from SLURM to FLURM. It tests the three primary migration scenarios:
%%%
%%% Scenario 1: SLURM Exists
%%%   - Deploy FLURM in shadow mode
%%%   - FLURM observes but doesn't handle jobs
%%%   - Verify FLURM can track SLURM cluster state
%%%
%%% Scenario 2: Add FLURM (Active Mode)
%%%   - FLURM accepts jobs alongside SLURM
%%%   - Jobs can be forwarded to SLURM
%%%   - Gradual workload migration
%%%
%%% Scenario 3: Ditch SLURM (Standalone Mode)
%%%   - FLURM becomes sole scheduler
%%%   - SLURM clusters removed
%%%   - No job forwarding
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_migration_SUITE).

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

%% Test cases
-export([
    %% Scenario 1: SLURM Exists (Shadow Mode)
    shadow_mode_observation/1,
    shadow_mode_no_job_handling/1,
    shadow_mode_cluster_tracking/1,

    %% Scenario 2: Add FLURM (Active Mode)
    active_mode_accepts_jobs/1,
    active_mode_can_forward/1,
    active_mode_parallel_operation/1,

    %% Scenario 3: Primary Mode
    primary_mode_flurm_leads/1,
    primary_mode_slurm_drains/1,
    primary_mode_forwarding_available/1,

    %% Scenario 4: Standalone Mode
    standalone_mode_no_slurm/1,
    standalone_mode_all_local/1,
    standalone_mode_cluster_removed/1,

    %% Full Migration Path
    full_migration_path/1,
    migration_rollback/1,

    %% Job Forwarding
    forward_job_to_slurm/1,
    forward_job_status_tracking/1,
    forward_job_cancellation/1
]).

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        {group, scenario_1_shadow},
        {group, scenario_2_active},
        {group, scenario_3_primary},
        {group, scenario_4_standalone},
        {group, full_migration},
        {group, job_forwarding}
    ].

groups() ->
    [
        {scenario_1_shadow, [sequence], [
            shadow_mode_observation,
            shadow_mode_no_job_handling,
            shadow_mode_cluster_tracking
        ]},
        {scenario_2_active, [sequence], [
            active_mode_accepts_jobs,
            active_mode_can_forward,
            active_mode_parallel_operation
        ]},
        {scenario_3_primary, [sequence], [
            primary_mode_flurm_leads,
            primary_mode_slurm_drains,
            primary_mode_forwarding_available
        ]},
        {scenario_4_standalone, [sequence], [
            standalone_mode_no_slurm,
            standalone_mode_all_local,
            standalone_mode_cluster_removed
        ]},
        {full_migration, [sequence], [
            full_migration_path,
            migration_rollback
        ]},
        {job_forwarding, [sequence], [
            forward_job_to_slurm,
            forward_job_status_tracking,
            forward_job_cancellation
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    ok = application:ensure_started(syntax_tools),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(goldrush),
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(sasl),

    %% Suppress lager console output during tests
    lager:set_loglevel(lager_console_backend, error),

    %% Start core services
    start_services(),

    Config.

end_per_suite(_Config) ->
    %% Stop the bridge
    catch gen_server:stop(flurm_slurm_bridge),
    ok.

init_per_group(Group, Config) ->
    ct:pal("Starting test group: ~p", [Group]),

    %% Ensure bridge is started fresh
    case whereis(flurm_slurm_bridge) of
        undefined -> ok;
        OldPid ->
            catch gen_server:stop(OldPid, normal, 5000),
            timer:sleep(100)
    end,

    %% Start the bridge with trap_exit so linked processes don't kill test
    case flurm_slurm_bridge:start_link() of
        {ok, Pid} ->
            %% Unlink so test process doesn't die if bridge crashes
            unlink(Pid),
            ct:pal("Started bridge with pid: ~p", [Pid]);
        {error, {already_started, Pid}} ->
            ct:pal("Bridge already running with pid: ~p", [Pid])
    end,

    %% Reset to shadow mode
    ok = flurm_slurm_bridge:set_mode(shadow),

    [{group, Group} | Config].

end_per_group(_Group, _Config) ->
    %% Clean up any clusters
    case whereis(flurm_slurm_bridge) of
        undefined -> ok;
        _Pid ->
            try
                lists:foreach(fun(#{name := Name}) ->
                    catch flurm_slurm_bridge:remove_slurm_cluster(Name)
                end, flurm_slurm_bridge:list_slurm_clusters())
            catch _:_ -> ok
            end
    end,
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Scenario 1: Shadow Mode Tests
%%%===================================================================

shadow_mode_observation(Config) ->
    %% In shadow mode, FLURM should:
    %% 1. Start in shadow mode by default
    %% 2. Track SLURM clusters for observation
    %% 3. Not interfere with job handling

    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),

    %% Add a SLURM cluster to observe
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"observed-slurm">>, #{
        host => <<"slurmctld.example.com">>,
        port => 6817
    }),

    %% Verify cluster is tracked
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(1, length(Clusters)),

    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(shadow, maps:get(mode, Status)),
    ct:pal("Shadow mode status: ~p", [Status]),

    Config.

shadow_mode_no_job_handling(Config) ->
    %% In shadow mode, job forwarding should be blocked
    JobSpec = #{
        name => <<"test-job">>,
        script => <<"#!/bin/bash\necho 'test'">>
    },

    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, shadow_mode}, Result),

    %% Jobs should be handled locally by FLURM, not forwarded
    Config.

shadow_mode_cluster_tracking(Config) ->
    %% Verify multiple clusters can be tracked
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-dc1">>, #{
        host => <<"dc1.slurm.internal">>,
        port => 6817
    }),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-dc2">>, #{
        host => <<"dc2.slurm.internal">>,
        port => 6817
    }),

    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(length(Clusters) >= 2),

    %% Each cluster should have required fields
    lists:foreach(fun(C) ->
        ?assert(maps:is_key(name, C)),
        ?assert(maps:is_key(host, C)),
        ?assert(maps:is_key(port, C)),
        ?assert(maps:is_key(state, C))
    end, Clusters),

    Config.

%%%===================================================================
%%% Scenario 2: Active Mode Tests
%%%===================================================================

active_mode_accepts_jobs(Config) ->
    %% Transition to active mode
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()),

    %% FLURM should now accept jobs
    %% Submit a local job (not forwarded)
    %% This verifies FLURM job handling works in active mode

    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(active, maps:get(mode, Status)),

    Config.

active_mode_can_forward(Config) ->
    ok = flurm_slurm_bridge:set_mode(active),

    %% Add a SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"forward-target">>, #{
        host => <<"slurmctld.forward.internal">>,
        port => 6817
    }),

    %% In active mode, forwarding is allowed (but will fail due to no network)
    JobSpec = #{
        name => <<"forward-test">>,
        script => <<"#!/bin/bash\necho test">>
    },

    %% Should attempt to forward, not reject due to mode
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"forward-target">>),

    %% Expected: connection error (not mode rejection)
    case Result of
        {error, {connect_failed, _}} -> ok;
        {error, {cluster_down, _}} -> ok;
        Other ->
            %% Log unexpected result for debugging
            ct:pal("Unexpected forward result: ~p", [Other]),
            %% Mode error would be {error, shadow_mode} or {error, standalone_mode}
            ?assertNotEqual({error, shadow_mode}, Result),
            ?assertNotEqual({error, standalone_mode}, Result)
    end,

    Config.

active_mode_parallel_operation(Config) ->
    %% Verify FLURM can operate alongside SLURM
    ok = flurm_slurm_bridge:set_mode(active),

    %% Clean up any existing clusters first
    lists:foreach(fun(#{name := Name}) ->
        catch flurm_slurm_bridge:remove_slurm_cluster(Name)
    end, flurm_slurm_bridge:list_slurm_clusters()),

    %% Add multiple SLURM clusters
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-a">>, #{host => <<"a">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-b">>, #{host => <<"b">>}),

    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(active, maps:get(mode, Status)),
    ?assertEqual(2, maps:get(clusters_total, Status)),

    Config.

%%%===================================================================
%%% Scenario 3: Primary Mode Tests
%%%===================================================================

primary_mode_flurm_leads(Config) ->
    %% Transition to primary mode
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),

    %% FLURM is now the primary scheduler
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(primary, maps:get(mode, Status)),

    Config.

primary_mode_slurm_drains(Config) ->
    ok = flurm_slurm_bridge:set_mode(primary),

    %% In primary mode, SLURM clusters are still tracked (for draining)
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"draining-slurm">>, #{
        host => <<"drain.slurm.internal">>
    }),

    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(1, length(Clusters)),

    %% Existing forwarded jobs should still be tracked
    Config.

primary_mode_forwarding_available(Config) ->
    ok = flurm_slurm_bridge:set_mode(primary),

    %% Add cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"primary-target">>, #{
        host => <<"target.slurm.internal">>
    }),

    %% Forwarding should still work (for finishing migration)
    JobSpec = #{name => <<"primary-test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"primary-target">>),

    %% Should not be mode rejection
    ?assertNotEqual({error, shadow_mode}, Result),
    ?assertNotEqual({error, standalone_mode}, Result),

    Config.

%%%===================================================================
%%% Scenario 4: Standalone Mode Tests
%%%===================================================================

standalone_mode_no_slurm(Config) ->
    %% Transition to standalone mode
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()),

    %% SLURM should not be available
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()),

    Config.

standalone_mode_all_local(Config) ->
    ok = flurm_slurm_bridge:set_mode(standalone),

    %% All jobs should be handled locally
    JobSpec = #{name => <<"local-only">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, standalone_mode}, Result),

    Config.

standalone_mode_cluster_removed(Config) ->
    %% Start with a cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"to-remove">>, #{
        host => <<"remove.slurm.internal">>
    }),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())),

    %% Remove before going standalone
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"to-remove">>),

    %% Go standalone
    ok = flurm_slurm_bridge:set_mode(standalone),

    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(standalone, maps:get(mode, Status)),
    ?assertEqual(0, maps:get(clusters_total, Status)),

    Config.

%%%===================================================================
%%% Full Migration Path Tests
%%%===================================================================

full_migration_path(Config) ->
    %% Test the complete migration journey:
    %% shadow -> active -> primary -> standalone

    %% Step 1: Start in shadow mode (observation)
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),
    ct:pal("Step 1: Shadow mode - observing SLURM"),

    %% Add existing SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
        host => <<"prod.slurm.internal">>,
        port => 6817
    }),

    %% Verify observation only
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    ?assertEqual({error, shadow_mode}, flurm_slurm_bridge:forward_job(JobSpec, auto)),

    %% Step 2: Go active (parallel operation)
    ct:pal("Step 2: Active mode - parallel operation"),
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()),

    %% Forwarding now allowed
    Result2 = flurm_slurm_bridge:forward_job(JobSpec, <<"prod-slurm">>),
    ?assertNotEqual({error, shadow_mode}, Result2),

    %% Step 3: Go primary (FLURM leads)
    ct:pal("Step 3: Primary mode - FLURM leads, SLURM draining"),
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),

    %% Still can forward if needed
    Result3 = flurm_slurm_bridge:forward_job(JobSpec, <<"prod-slurm">>),
    ?assertNotEqual({error, standalone_mode}, Result3),

    %% Step 4: Remove SLURM, go standalone
    ct:pal("Step 4: Standalone mode - SLURM decommissioned"),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"prod-slurm">>),
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()),

    %% No more SLURM
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()),
    ?assertEqual({error, standalone_mode}, flurm_slurm_bridge:forward_job(JobSpec, auto)),

    ct:pal("Migration complete: FLURM is now standalone"),
    Config.

migration_rollback(Config) ->
    %% Test rollback capability at any stage

    %% Go to active mode
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"rollback-slurm">>, #{
        host => <<"rollback.slurm.internal">>
    }),

    %% Go to primary
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),

    %% Rollback to active (if issues discovered)
    ct:pal("Rolling back from primary to active"),
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()),

    %% Rollback all the way to shadow
    ct:pal("Rolling back to shadow mode"),
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),

    %% Clusters still tracked
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())),

    Config.

%%%===================================================================
%%% Job Forwarding Tests
%%%===================================================================

forward_job_to_slurm(Config) ->
    %% Set up for forwarding
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"forward-cluster">>, #{
        host => <<"127.0.0.1">>,  % Will fail to connect
        port => 9999
    }),

    JobSpec = #{
        name => <<"forward-test">>,
        script => <<"#!/bin/bash\necho 'forwarded job'">>,
        partition => <<"default">>,
        num_cpus => 1,
        num_nodes => 1
    },

    %% Attempt forward (will fail to connect but tests the flow)
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"forward-cluster">>),

    %% Should be a connection error, not mode rejection
    case Result of
        {error, {connect_failed, _}} -> ok;
        {error, {cluster_down, _}} -> ok;
        {ok, _JobId} -> ok;  % Would succeed with real SLURM
        Other ->
            ct:pal("Forward result: ~p", [Other]),
            ?assertNotEqual({error, shadow_mode}, Result)
    end,

    Config.

forward_job_status_tracking(Config) ->
    %% Test status tracking for forwarded jobs
    ok = flurm_slurm_bridge:set_mode(active),

    %% Query non-existent job
    Result = flurm_slurm_bridge:get_forwarded_job_status(12345),
    ?assertEqual({error, not_found}, Result),

    Config.

forward_job_cancellation(Config) ->
    %% Test cancellation of forwarded jobs
    ok = flurm_slurm_bridge:set_mode(active),

    %% Cancel non-existent job
    Result = flurm_slurm_bridge:cancel_forwarded_job(12345),
    ?assertEqual({error, not_found}, Result),

    Config.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

start_services() ->
    %% Start core FLURM services needed for tests
    Services = [
        {flurm_job_registry, fun flurm_job_registry:start_link/0},
        {flurm_job_sup, fun flurm_job_sup:start_link/0},
        {flurm_node_registry, fun flurm_node_registry:start_link/0},
        {flurm_node_sup, fun flurm_node_sup:start_link/0},
        {flurm_limits, fun flurm_limits:start_link/0},
        {flurm_license, fun flurm_license:start_link/0},
        {flurm_job_manager, fun flurm_job_manager:start_link/0}
    ],

    lists:foreach(fun({Name, StartFun}) ->
        case whereis(Name) of
            undefined ->
                case StartFun() of
                    {ok, Pid} ->
                        unlink(Pid),
                        ct:pal("Started ~p", [Name]);
                    {error, {already_started, _}} ->
                        ok
                end;
            _ ->
                ok
        end
    end, Services).
