%%%-------------------------------------------------------------------
%%% @doc Unit Tests for SLURM Bridge Module
%%%
%%% Tests the zero-downtime migration bridge that enables FLURM
%%% to interoperate with existing SLURM clusters.
%%%
%%% Migration Scenarios Tested:
%%% 1. Shadow Mode - FLURM observes, no job handling
%%% 2. Active Mode - FLURM accepts jobs, can forward to SLURM
%%% 3. Primary Mode - FLURM primary, SLURM draining
%%% 4. Standalone Mode - SLURM removed, FLURM only
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_bridge_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

%% Setup/teardown for bridge tests
bridge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Mode management tests
        {"Default mode is shadow", fun test_default_mode/0},
        {"Set mode to active", fun test_set_mode_active/0},
        {"Set mode to primary", fun test_set_mode_primary/0},
        {"Set mode to standalone", fun test_set_mode_standalone/0},
        {"Mode transitions: shadow -> active -> primary -> standalone",
         fun test_mode_progression/0},

        %% Cluster management tests
        {"Add SLURM cluster", fun test_add_cluster/0},
        {"Add multiple clusters", fun test_add_multiple_clusters/0},
        {"Remove cluster", fun test_remove_cluster/0},
        {"Remove non-existent cluster", fun test_remove_unknown_cluster/0},
        {"List clusters empty", fun test_list_clusters_empty/0},
        {"List clusters with entries", fun test_list_clusters_with_entries/0},

        %% Status tests
        {"Get bridge status", fun test_get_status/0},
        {"Bridge available in shadow mode", fun test_available_shadow_mode/0},
        {"Bridge not available in standalone mode", fun test_not_available_standalone/0},

        %% Job forwarding mode restrictions
        {"Cannot forward in shadow mode", fun test_no_forward_shadow/0},
        {"Cannot forward in standalone mode", fun test_no_forward_standalone/0},
        {"Forward allowed in active mode (no cluster)", fun test_forward_active_no_cluster/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Start the bridge
    case whereis(flurm_slurm_bridge) of
        undefined ->
            {ok, Pid} = flurm_slurm_bridge:start_link(),
            Pid;
        Pid ->
            %% Reset to default state
            flurm_slurm_bridge:set_mode(shadow),
            %% Remove any clusters
            lists:foreach(fun(#{name := Name}) ->
                catch flurm_slurm_bridge:remove_slurm_cluster(Name)
            end, flurm_slurm_bridge:list_slurm_clusters()),
            Pid
    end.

cleanup(_Pid) ->
    %% Reset mode
    catch flurm_slurm_bridge:set_mode(shadow),
    %% Remove any clusters
    lists:foreach(fun(#{name := Name}) ->
        catch flurm_slurm_bridge:remove_slurm_cluster(Name)
    end, flurm_slurm_bridge:list_slurm_clusters()),
    ok.

%%%===================================================================
%%% Mode Management Tests
%%%===================================================================

test_default_mode() ->
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_set_mode_active() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

test_set_mode_primary() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()).

test_set_mode_standalone() ->
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()).

test_mode_progression() ->
    %% Simulate the migration path: shadow -> active -> primary -> standalone

    %% Start in shadow (observation mode)
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),

    %% Move to active (accepting jobs, can forward)
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()),

    %% Move to primary (FLURM is primary, SLURM draining)
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),

    %% Move to standalone (SLURM removed)
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()).

%%%===================================================================
%%% Cluster Management Tests
%%%===================================================================

test_add_cluster() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-prod">>, #{
        host => <<"slurm.example.com">>,
        port => 6817
    }),
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(1, length(Clusters)),
    [Cluster] = Clusters,
    ?assertEqual(<<"slurm-prod">>, maps:get(name, Cluster)),
    ?assertEqual(<<"slurm.example.com">>, maps:get(host, Cluster)),
    ?assertEqual(6817, maps:get(port, Cluster)).

test_add_multiple_clusters() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-a">>, #{
        host => <<"slurm-a.example.com">>,
        port => 6817
    }),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-b">>, #{
        host => <<"slurm-b.example.com">>,
        port => 6818
    }),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"slurm-c">>, #{
        host => <<"slurm-c.example.com">>
        %% Default port
    }),
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(3, length(Clusters)),
    Names = [maps:get(name, C) || C <- Clusters],
    ?assert(lists:member(<<"slurm-a">>, Names)),
    ?assert(lists:member(<<"slurm-b">>, Names)),
    ?assert(lists:member(<<"slurm-c">>, Names)).

test_remove_cluster() ->
    %% Add then remove
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"to-remove">>, #{
        host => <<"temp.example.com">>
    }),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())),

    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"to-remove">>),
    ?assertEqual(0, length(flurm_slurm_bridge:list_slurm_clusters())).

test_remove_unknown_cluster() ->
    Result = flurm_slurm_bridge:remove_slurm_cluster(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_clusters_empty() ->
    ?assertEqual([], flurm_slurm_bridge:list_slurm_clusters()).

test_list_clusters_with_entries() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test-1">>, #{host => <<"h1">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test-2">>, #{host => <<"h2">>}),
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(2, length(Clusters)),

    %% Verify cluster fields
    [C1 | _] = Clusters,
    ?assert(maps:is_key(name, C1)),
    ?assert(maps:is_key(host, C1)),
    ?assert(maps:is_key(port, C1)),
    ?assert(maps:is_key(state, C1)),
    ?assert(maps:is_key(last_check, C1)),
    ?assert(maps:is_key(consecutive_failures, C1)).

%%%===================================================================
%%% Status Tests
%%%===================================================================

test_get_status() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assert(is_map(Status)),
    ?assertEqual(shadow, maps:get(mode, Status)),
    ?assertEqual(0, maps:get(clusters_total, Status)),
    ?assertEqual(0, maps:get(clusters_up, Status)),
    ?assertEqual(0, maps:get(jobs_forwarded, Status)),
    ?assertEqual(0, maps:get(jobs_pending, Status)),
    ?assertEqual(0, maps:get(jobs_running, Status)).

test_available_shadow_mode() ->
    %% In shadow mode with no clusters, not available
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()),

    %% Add a cluster (state will be unknown initially)
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    %% Still not available until health check marks it up
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()).

test_not_available_standalone() ->
    %% In standalone mode, always not available (no SLURM integration)
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()),

    %% Even with clusters configured
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()).

%%%===================================================================
%%% Job Forwarding Tests
%%%===================================================================

test_no_forward_shadow() ->
    %% In shadow mode, job forwarding is not allowed
    JobSpec = #{name => <<"test-job">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, shadow_mode}, Result).

test_no_forward_standalone() ->
    %% In standalone mode, job forwarding is not allowed
    ok = flurm_slurm_bridge:set_mode(standalone),
    JobSpec = #{name => <<"test-job">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, standalone_mode}, Result).

test_forward_active_no_cluster() ->
    %% In active mode without any clusters, should fail with no_clusters
    ok = flurm_slurm_bridge:set_mode(active),
    JobSpec = #{name => <<"test-job">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, no_clusters_available}, Result).

%%%===================================================================
%%% Migration Scenario Tests
%%%===================================================================

%% These tests verify the expected behavior at each migration stage

scenario_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Scenario 1: SLURM exists (shadow mode)", fun test_scenario_slurm_exists/0},
        {"Scenario 2: Add FLURM alongside (active mode)", fun test_scenario_add_flurm/0},
        {"Scenario 3: Transition to FLURM primary", fun test_scenario_flurm_primary/0},
        {"Scenario 4: Ditch SLURM (standalone mode)", fun test_scenario_ditch_slurm/0}
     ]}.

test_scenario_slurm_exists() ->
    %% Scenario 1: SLURM exists, FLURM deployed in shadow mode
    %%
    %% At this stage:
    %% - SLURM is the primary scheduler
    %% - FLURM observes but doesn't handle jobs
    %% - Job forwarding is disabled

    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),

    %% Add reference to existing SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
        host => <<"slurmctld.prod.internal">>,
        port => 6817
    }),

    %% Verify SLURM cluster is tracked
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(1, length(Clusters)),

    %% Verify job forwarding is blocked in shadow mode
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    ?assertEqual({error, shadow_mode}, flurm_slurm_bridge:forward_job(JobSpec, auto)),

    %% Status shows shadow mode
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(shadow, maps:get(mode, Status)).

test_scenario_add_flurm() ->
    %% Scenario 2: FLURM is added alongside SLURM (active mode)
    %%
    %% At this stage:
    %% - Both FLURM and SLURM can handle jobs
    %% - FLURM can forward jobs to SLURM
    %% - Gradual migration of workloads

    %% Add SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
        host => <<"slurmctld.prod.internal">>,
        port => 6817
    }),

    %% Transition to active mode
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()),

    %% In active mode, job forwarding is allowed (but will fail due to network)
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"prod-slurm">>),

    %% Will fail to connect (no actual SLURM), but mode allows it
    ?assertMatch({error, {unknown_cluster, _}} ,
        flurm_slurm_bridge:forward_job(JobSpec, <<"unknown">>)),

    %% Status shows active mode
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(active, maps:get(mode, Status)).

test_scenario_flurm_primary() ->
    %% Scenario 3: FLURM becomes primary, SLURM draining
    %%
    %% At this stage:
    %% - FLURM is the primary scheduler
    %% - SLURM is still running but draining workloads
    %% - New jobs go to FLURM, SLURM finishes existing jobs

    %% Add SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
        host => <<"slurmctld.prod.internal">>,
        port => 6817
    }),

    %% Transition to primary mode
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),

    %% In primary mode, forwarding is still possible (for compatibility)
    %% but the expectation is that new jobs are handled by FLURM
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(primary, maps:get(mode, Status)),
    ?assertEqual(1, maps:get(clusters_total, Status)).

test_scenario_ditch_slurm() ->
    %% Scenario 4: SLURM is removed, FLURM standalone
    %%
    %% At this stage:
    %% - SLURM is decommissioned
    %% - FLURM handles all jobs independently
    %% - No job forwarding needed

    %% Start with a SLURM cluster
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"prod-slurm">>, #{
        host => <<"slurmctld.prod.internal">>,
        port => 6817
    }),

    %% Remove the cluster before going standalone
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"prod-slurm">>),

    %% Transition to standalone mode
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()),

    %% Job forwarding is blocked in standalone mode
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    ?assertEqual({error, standalone_mode}, flurm_slurm_bridge:forward_job(JobSpec, auto)),

    %% SLURM is not available
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()),

    %% Status shows standalone with no clusters
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(standalone, maps:get(mode, Status)),
    ?assertEqual(0, maps:get(clusters_total, Status)).

%%%===================================================================
%%% Job Status Tests
%%%===================================================================

job_status_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Get status of unknown job", fun test_unknown_job_status/0},
        {"Cancel unknown job", fun test_cancel_unknown_job/0}
     ]}.

test_unknown_job_status() ->
    Result = flurm_slurm_bridge:get_forwarded_job_status(999999),
    ?assertEqual({error, not_found}, Result).

test_cancel_unknown_job() ->
    Result = flurm_slurm_bridge:cancel_forwarded_job(999999),
    ?assertEqual({error, not_found}, Result).
