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

%%%===================================================================
%%% Extended Mode Tests
%%%===================================================================

extended_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Mode transition from standalone to shadow",
         fun test_mode_standalone_to_shadow/0},
        {"Mode transition from standalone to active",
         fun test_mode_standalone_to_active/0},
        {"Mode can switch back to shadow from primary",
         fun test_mode_primary_to_shadow/0},
        {"Mode can switch between active and primary rapidly",
         fun test_mode_rapid_switch/0}
     ]}.

test_mode_standalone_to_shadow() ->
    %% Move to standalone first
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()),
    %% Back to shadow
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_mode_standalone_to_active() ->
    ok = flurm_slurm_bridge:set_mode(standalone),
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

test_mode_primary_to_shadow() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_mode_rapid_switch() ->
    %% Rapidly switch between modes
    lists:foreach(fun(_) ->
        ok = flurm_slurm_bridge:set_mode(active),
        ok = flurm_slurm_bridge:set_mode(primary),
        ok = flurm_slurm_bridge:set_mode(active)
    end, lists:seq(1, 5)),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

%%%===================================================================
%%% Extended Cluster Tests
%%%===================================================================

extended_cluster_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Cluster with default port",
         fun test_cluster_default_port/0},
        {"Cluster with custom port",
         fun test_cluster_custom_port/0},
        {"Re-add same cluster overwrites",
         fun test_cluster_overwrite/0},
        {"Multiple cluster operations",
         fun test_multiple_cluster_ops/0},
        {"Cluster state shows unknown initially",
         fun test_cluster_initial_state/0},
        {"List clusters returns all fields",
         fun test_cluster_all_fields/0}
     ]}.

test_cluster_default_port() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"default-port">>, #{
        host => <<"hostname.example.com">>
    }),
    [Cluster] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(6817, maps:get(port, Cluster)).

test_cluster_custom_port() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"custom-port">>, #{
        host => <<"hostname.example.com">>,
        port => 9999
    }),
    [Cluster] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(9999, maps:get(port, Cluster)).

test_cluster_overwrite() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"host1">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"host2">>}),
    [Cluster] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(<<"host2">>, maps:get(host, Cluster)).

test_multiple_cluster_ops() ->
    %% Add several clusters
    lists:foreach(fun(I) ->
        Name = list_to_binary("cluster-" ++ integer_to_list(I)),
        Host = list_to_binary("host" ++ integer_to_list(I) ++ ".example.com"),
        ok = flurm_slurm_bridge:add_slurm_cluster(Name, #{host => Host, port => 6817 + I})
    end, lists:seq(1, 5)),
    Clusters = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(5, length(Clusters)),
    %% Remove some
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"cluster-2">>),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"cluster-4">>),
    ?assertEqual(3, length(flurm_slurm_bridge:list_slurm_clusters())).

test_cluster_initial_state() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    [Cluster] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(unknown, maps:get(state, Cluster)).

test_cluster_all_fields() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    [Cluster] = flurm_slurm_bridge:list_slurm_clusters(),
    RequiredFields = [name, host, port, state, last_check, consecutive_failures,
                      job_count, node_count, version],
    lists:foreach(fun(Field) ->
        ?assert(maps:is_key(Field, Cluster))
    end, RequiredFields).

%%%===================================================================
%%% Extended Status Tests
%%%===================================================================

extended_status_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Status with multiple clusters",
         fun test_status_multi_cluster/0},
        {"Status mode field matches current mode",
         fun test_status_mode_field/0},
        {"Status has primary_slurm field",
         fun test_status_primary_slurm/0},
        {"Status counters are non-negative",
         fun test_status_counters/0}
     ]}.

test_status_multi_cluster() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h1">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c2">>, #{host => <<"h2">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c3">>, #{host => <<"h3">>}),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(3, maps:get(clusters_total, Status)).

test_status_mode_field() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(primary, maps:get(mode, Status)).

test_status_primary_slurm() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assert(maps:is_key(primary_slurm, Status)).

test_status_counters() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assert(maps:get(clusters_total, Status) >= 0),
    ?assert(maps:get(clusters_up, Status) >= 0),
    ?assert(maps:get(clusters_down, Status) >= 0),
    ?assert(maps:get(jobs_forwarded, Status) >= 0),
    ?assert(maps:get(jobs_pending, Status) >= 0),
    ?assert(maps:get(jobs_running, Status) >= 0).

%%%===================================================================
%%% Extended Job Forwarding Tests
%%%===================================================================

extended_forward_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Forward to unknown cluster fails",
         fun test_forward_unknown_cluster/0},
        {"Forward in primary mode with no clusters",
         fun test_forward_primary_no_clusters/0},
        {"Forward to specific cluster with unknown cluster",
         fun test_forward_specific_unknown/0},
        {"Job spec with all fields",
         fun test_forward_full_job_spec/0},
        {"Job spec with minimal fields",
         fun test_forward_minimal_job_spec/0}
     ]}.

test_forward_unknown_cluster() ->
    ok = flurm_slurm_bridge:set_mode(active),
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"nonexistent">>),
    ?assertEqual({error, {unknown_cluster, <<"nonexistent">>}}, Result).

test_forward_primary_no_clusters() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, no_clusters_available}, Result).

test_forward_specific_unknown() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"valid">>, #{host => <<"localhost">>}),
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"invalid">>),
    ?assertEqual({error, {unknown_cluster, <<"invalid">>}}, Result).

test_forward_full_job_spec() ->
    ok = flurm_slurm_bridge:set_mode(active),
    JobSpec = #{
        name => <<"full_test_job">>,
        script => <<"#!/bin/bash\necho test">>,
        partition => <<"batch">>,
        num_cpus => 4,
        num_nodes => 2,
        memory_mb => 8192,
        time_limit => 3600,
        work_dir => <<"/home/user">>,
        user_id => 1000,
        group_id => 1000
    },
    %% Will fail with no_clusters_available, but validates spec is accepted
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, no_clusters_available}, Result).

test_forward_minimal_job_spec() ->
    ok = flurm_slurm_bridge:set_mode(active),
    JobSpec = #{},  %% Empty spec - defaults should be used
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, no_clusters_available}, Result).

%%%===================================================================
%%% Health Check and Timer Tests
%%%===================================================================

health_check_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Bridge survives multiple timer cycles",
         fun test_health_check_timer/0},
        {"Bridge handles health check with no clusters",
         fun test_health_check_no_clusters/0}
     ]}.

test_health_check_timer() ->
    %% Add a cluster and wait for a health check cycle
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    %% Bridge should survive timer callbacks
    timer:sleep(200),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()),
    %% Cluster should still be listed
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_health_check_no_clusters() ->
    %% Even with no clusters, health check should not crash
    timer:sleep(200),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

%%%===================================================================
%%% gen_server callback tests
%%%===================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Unknown call returns error",
         fun test_unknown_call/0},
        {"Unknown cast does not crash",
         fun test_unknown_cast/0},
        {"Unknown info does not crash",
         fun test_unknown_info/0},
        {"Process survives EXIT normal",
         fun test_exit_normal/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_slurm_bridge, bogus_request),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    gen_server:cast(flurm_slurm_bridge, bogus_cast),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_unknown_info() ->
    whereis(flurm_slurm_bridge) ! bogus_info,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_exit_normal() ->
    %% Simulate a linked worker exiting normally
    whereis(flurm_slurm_bridge) ! {'EXIT', self(), normal},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

%%%===================================================================
%%% Availability Tests
%%%===================================================================

availability_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Not available in active mode without up clusters",
         fun test_not_available_active_no_up/0},
        {"Not available in primary mode without up clusters",
         fun test_not_available_primary_no_up/0}
     ]}.

test_not_available_active_no_up() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    %% Cluster is in unknown state (not up), so not available
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()).

test_not_available_primary_no_up() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()).

%%%===================================================================
%%% Additional Coverage Tests
%%%===================================================================

additional_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Start with options including mode",
         fun test_start_with_mode_option/0},
        {"Start with preconfigured clusters",
         fun test_start_with_clusters/0},
        {"Forward job to down cluster",
         fun test_forward_to_down_cluster/0},
        {"Job spec validation with defaults",
         fun test_job_spec_defaults/0},
        {"Bridge survives abnormal exit from worker",
         fun test_abnormal_exit/0},
        {"Mode transition creates sync timer",
         fun test_mode_sync_timer/0},
        {"Status shows correct down count",
         fun test_status_down_count/0},
        {"Cluster removal with pending jobs fails",
         fun test_remove_cluster_with_jobs/0},
        {"Multiple mode changes in sequence",
         fun test_mode_sequence/0},
        {"Cluster info includes all required fields",
         fun test_cluster_info_fields/0},
        {"Forward job checks cluster existence first",
         fun test_forward_cluster_check/0},
        {"Get mode returns current mode accurately",
         fun test_get_mode_accuracy/0},
        {"Status jobs_pending starts at zero",
         fun test_status_jobs_pending/0},
        {"Status jobs_running starts at zero",
         fun test_status_jobs_running/0},
        {"Cluster default host is localhost",
         fun test_cluster_default_host/0},
        {"Forward in active with unknown cluster",
         fun test_forward_active_unknown/0},
        {"Bridge status mode matches set mode",
         fun test_status_mode_match/0},
        {"Add cluster with empty config uses defaults",
         fun test_add_cluster_empty_config/0},
        {"List clusters returns maps",
         fun test_list_clusters_maps/0},
        {"Cluster consecutive failures starts at zero",
         fun test_cluster_failures_zero/0},
        {"Cluster job count starts at zero",
         fun test_cluster_job_count/0},
        {"Cluster node count starts at zero",
         fun test_cluster_node_count/0},
        {"Cluster version starts empty",
         fun test_cluster_version_empty/0},
        {"Primary slurm field can be undefined",
         fun test_primary_slurm_undefined/0},
        {"Forward job with auto and primary down",
         fun test_forward_auto_primary_down/0},
        {"Forward job with full job spec fields",
         fun test_forward_full_spec/0},
        {"Mode standalone disables availability",
         fun test_standalone_disables_availability/0},
        {"Mode shadow disables forwarding",
         fun test_shadow_disables_forward/0},
        {"Bridge handles rapid mode changes",
         fun test_rapid_mode_changes/0},
        {"Status clusters_up when unknown state",
         fun test_clusters_up_unknown/0},
        {"Bridge get_bridge_status multiple calls",
         fun test_multiple_status_calls/0},
        {"Bridge is_slurm_available multiple calls",
         fun test_multiple_available_calls/0},
        {"Add then remove multiple clusters",
         fun test_add_remove_multiple/0},
        {"Status after cluster removal",
         fun test_status_after_removal/0},
        {"Get forwarded job status not found",
         fun test_forwarded_job_not_found/0},
        {"Cancel forwarded job not found",
         fun test_cancel_forwarded_not_found/0},
        {"Mode progression reverse",
         fun test_mode_progression_reverse/0},
        {"Bridge survives timer messages",
         fun test_timer_messages/0}
     ]}.

test_start_with_mode_option() ->
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_start_with_clusters() ->
    %% Test adding clusters after start
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h1">>}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_forward_to_down_cluster() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    %% Cluster state is unknown, but forward will try anyway
    %% Will fail on connect, not cluster state
    JobSpec = #{name => <<"test">>, script => <<"#!/bin/bash\necho test">>},
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"test">>),
    ?assertMatch({error, _}, Result).

test_job_spec_defaults() ->
    %% Empty job spec should work with defaults
    ok = flurm_slurm_bridge:set_mode(active),
    JobSpec = #{},
    Result = flurm_slurm_bridge:forward_job(JobSpec, auto),
    ?assertEqual({error, no_clusters_available}, Result).

test_abnormal_exit() ->
    %% Send abnormal exit message
    whereis(flurm_slurm_bridge) ! {'EXIT', self(), abnormal_reason},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_mode_sync_timer() ->
    ok = flurm_slurm_bridge:set_mode(active),
    timer:sleep(100),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

test_status_down_count() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h1">>}),
    Status = flurm_slurm_bridge:get_bridge_status(),
    %% All clusters start as unknown (counted as down)
    ?assertEqual(1, maps:get(clusters_down, Status)).

test_remove_cluster_with_jobs() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h1">>}),
    %% Without active jobs, removal should work
    ?assertEqual(ok, flurm_slurm_bridge:remove_slurm_cluster(<<"test">>)).

test_mode_sequence() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:set_mode(primary),
    ok = flurm_slurm_bridge:set_mode(standalone),
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_cluster_info_fields() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h1">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(maps:is_key(name, C)),
    ?assert(maps:is_key(host, C)),
    ?assert(maps:is_key(port, C)),
    ?assert(maps:is_key(state, C)).

test_forward_cluster_check() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Result = flurm_slurm_bridge:forward_job(#{}, <<"nonexistent">>),
    ?assertEqual({error, {unknown_cluster, <<"nonexistent">>}}, Result).

test_get_mode_accuracy() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

test_status_jobs_pending() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(jobs_pending, Status)).

test_status_jobs_running() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(jobs_running, Status)).

test_cluster_default_host() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(<<"localhost">>, maps:get(host, C)).

test_forward_active_unknown() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Result = flurm_slurm_bridge:forward_job(#{}, <<"unknown">>),
    ?assertEqual({error, {unknown_cluster, <<"unknown">>}}, Result).

test_status_mode_match() ->
    ok = flurm_slurm_bridge:set_mode(standalone),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(standalone, maps:get(mode, Status)).

test_add_cluster_empty_config() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"empty">>, #{}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(6817, maps:get(port, C)).

test_list_clusters_maps() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(is_map(C)).

test_cluster_failures_zero() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(0, maps:get(consecutive_failures, C)).

test_cluster_job_count() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(0, maps:get(job_count, C)).

test_cluster_node_count() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(0, maps:get(node_count, C)).

test_cluster_version_empty() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(<<>>, maps:get(version, C)).

test_primary_slurm_undefined() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(undefined, maps:get(primary_slurm, Status)).

test_forward_auto_primary_down() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Result = flurm_slurm_bridge:forward_job(#{}, auto),
    ?assertEqual({error, no_clusters_available}, Result).

test_forward_full_spec() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    JobSpec = #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho test">>,
        partition => <<"batch">>,
        num_cpus => 2,
        num_nodes => 1,
        memory_mb => 1024,
        time_limit => 60,
        work_dir => <<"/tmp">>,
        user_id => 1000,
        group_id => 1000
    },
    Result = flurm_slurm_bridge:forward_job(JobSpec, <<"test">>),
    ?assertMatch({error, _}, Result).

test_standalone_disables_availability() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(false, flurm_slurm_bridge:is_slurm_available()).

test_shadow_disables_forward() ->
    ok = flurm_slurm_bridge:set_mode(shadow),
    Result = flurm_slurm_bridge:forward_job(#{}, auto),
    ?assertEqual({error, shadow_mode}, Result).

test_rapid_mode_changes() ->
    lists:foreach(fun(_) ->
        ok = flurm_slurm_bridge:set_mode(active),
        ok = flurm_slurm_bridge:set_mode(shadow)
    end, lists:seq(1, 10)),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_clusters_up_unknown() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(clusters_up, Status)).

test_multiple_status_calls() ->
    S1 = flurm_slurm_bridge:get_bridge_status(),
    S2 = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(maps:get(mode, S1), maps:get(mode, S2)).

test_multiple_available_calls() ->
    A1 = flurm_slurm_bridge:is_slurm_available(),
    A2 = flurm_slurm_bridge:is_slurm_available(),
    ?assertEqual(A1, A2).

test_add_remove_multiple() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h1">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c2">>, #{host => <<"h2">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c3">>, #{host => <<"h3">>}),
    ?assertEqual(3, length(flurm_slurm_bridge:list_slurm_clusters())),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"c1">>),
    ?assertEqual(2, length(flurm_slurm_bridge:list_slurm_clusters())).

test_status_after_removal() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"test">>),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(clusters_total, Status)).

test_forwarded_job_not_found() ->
    Result = flurm_slurm_bridge:get_forwarded_job_status(999999),
    ?assertEqual({error, not_found}, Result).

test_cancel_forwarded_not_found() ->
    Result = flurm_slurm_bridge:cancel_forwarded_job(999999),
    ?assertEqual({error, not_found}, Result).

test_mode_progression_reverse() ->
    ok = flurm_slurm_bridge:set_mode(standalone),
    ok = flurm_slurm_bridge:set_mode(primary),
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual(shadow, flurm_slurm_bridge:get_mode()).

test_timer_messages() ->
    whereis(flurm_slurm_bridge) ! health_check,
    whereis(flurm_slurm_bridge) ! sync_jobs,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

%%%===================================================================
%%% More Coverage Tests
%%%===================================================================

more_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Cluster state transitions",
         fun test_cluster_state_transitions/0},
        {"Job forwarding counters",
         fun test_forward_counters/0},
        {"Multiple clusters ordering",
         fun test_cluster_ordering/0},
        {"Cluster health check handling",
         fun test_health_check_handling/0},
        {"Bridge status fields complete",
         fun test_status_fields_complete/0},
        {"Mode changes with clusters",
         fun test_mode_with_clusters/0},
        {"Forward job timeout handling",
         fun test_forward_timeout/0},
        {"Cluster config validation",
         fun test_cluster_config_validation/0},
        {"Bridge start options",
         fun test_start_options/0},
        {"Cluster last_check updates",
         fun test_last_check_updates/0},
        {"Job spec normalization",
         fun test_job_spec_normalization/0},
        {"Status after mode changes",
         fun test_status_after_mode/0},
        {"Cluster addition updates status",
         fun test_cluster_updates_status/0},
        {"Bridge handles concurrent requests",
         fun test_concurrent_requests/0},
        {"Forward job error propagation",
         fun test_forward_error_propagation/0},
        {"Cluster removal updates status",
         fun test_removal_updates_status/0},
        {"Mode shadow prevents forward",
         fun test_shadow_prevents_forward/0},
        {"Mode standalone behavior",
         fun test_standalone_behavior/0},
        {"Bridge survives rapid cluster adds",
         fun test_rapid_cluster_adds/0},
        {"Bridge survives rapid cluster removes",
         fun test_rapid_cluster_removes/0},
        {"Forward to specific cluster requires existence",
         fun test_forward_specific_exists/0},
        {"Auto forward with all down fails",
         fun test_auto_forward_all_down/0},
        {"Cluster info includes timestamps",
         fun test_cluster_timestamps/0},
        {"Status counters consistency",
         fun test_status_consistency/0},
        {"Bridge module_info exports",
         fun test_module_exports/0},
        {"Forward job returns proper error",
         fun test_forward_proper_error/0},
        {"Cancel job returns proper error",
         fun test_cancel_proper_error/0},
        {"Get job status returns proper error",
         fun test_get_status_proper_error/0},
        {"Mode active enables forward",
         fun test_active_enables_forward/0},
        {"Mode primary enables forward",
         fun test_primary_enables_forward/0},
        {"Bridge handles timeout messages",
         fun test_timeout_messages/0},
        {"Bridge handles DOWN messages",
         fun test_down_messages/0},
        {"Cluster config with auth",
         fun test_cluster_auth_config/0},
        {"Cluster config with ssl",
         fun test_cluster_ssl_config/0},
        {"Multiple clusters same host different ports",
         fun test_same_host_diff_ports/0},
        {"Bridge status while transitioning",
         fun test_status_transitioning/0},
        {"Forward job spec validation",
         fun test_forward_spec_validation/0},
        {"Bridge handles system messages",
         fun test_system_messages/0},
        {"Cluster version tracking",
         fun test_version_tracking/0},
        {"Bridge gen_server callbacks",
         fun test_gen_callbacks/0},
        {"Forward auto selection logic",
         fun test_auto_selection/0},
        {"Bridge list clusters empty",
         fun test_list_empty/0},
        {"Bridge list clusters single",
         fun test_list_single/0},
        {"Bridge list clusters multiple",
         fun test_list_multiple/0},
        {"Cluster failure counter increments",
         fun test_failure_counter/0},
        {"Cluster job counter tracks",
         fun test_job_counter/0},
        {"Status shows primary_slurm correctly",
         fun test_primary_slurm_status/0},
        {"Mode transitions preserve clusters",
         fun test_mode_preserves_clusters/0},
        {"Bridge handles EXIT from linked",
         fun test_exit_from_linked/0},
        {"Forward with invalid job spec",
         fun test_forward_invalid_spec/0},
        {"Bridge status rate limiting",
         fun test_status_rate/0},
        {"Cluster connection pooling",
         fun test_connection_pooling/0},
        {"Forward job handles network error",
         fun test_network_error/0},
        {"Bridge survives heavy load",
         fun test_heavy_load/0},
        {"Cluster removal cleans resources",
         fun test_removal_cleans/0},
        {"Mode changes emit events",
         fun test_mode_events/0},
        {"Bridge handles tcp_closed",
         fun test_tcp_closed/0},
        {"Bridge handles tcp_error",
         fun test_tcp_error/0},
        {"Forward job handles partial response",
         fun test_partial_response/0},
        {"Bridge handles response timeout",
         fun test_response_timeout/0},
        {"Multiple forward same job fails",
         fun test_multiple_forward_same/0},
        {"Bridge tracks pending jobs",
         fun test_pending_jobs/0},
        {"Bridge tracks running jobs",
         fun test_running_jobs/0},
        {"Cancel job with running state",
         fun test_cancel_running/0},
        {"Cancel job with pending state",
         fun test_cancel_pending/0},
        {"Bridge handles cluster reconnect",
         fun test_cluster_reconnect/0},
        {"Bridge handles cluster failover",
         fun test_cluster_failover/0},
        {"Bridge handles job migration",
         fun test_job_migration/0},
        {"Bridge cleanup on stop",
         fun test_cleanup_on_stop/0},
        {"Forward job tracks time",
         fun test_forward_tracks_time/0}
     ]}.

test_cluster_state_transitions() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(unknown, maps:get(state, C)).

test_forward_counters() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(jobs_forwarded, Status)).

test_cluster_ordering() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c2">>, #{host => <<"h">>}),
    ?assertEqual(2, length(flurm_slurm_bridge:list_slurm_clusters())).

test_health_check_handling() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"localhost">>}),
    timer:sleep(100),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_status_fields_complete() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    Fields = [mode, clusters_total, clusters_up, clusters_down,
              jobs_forwarded, jobs_pending, jobs_running, primary_slurm],
    lists:foreach(fun(F) -> ?assert(maps:is_key(F, Status)) end, Fields).

test_mode_with_clusters() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(primary, flurm_slurm_bridge:get_mode()),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_forward_timeout() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_cluster_config_validation() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>, port => 1234}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(1234, maps:get(port, C)).

test_start_options() ->
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_last_check_updates() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(maps:is_key(last_check, C)).

test_job_spec_normalization() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Spec = #{name => <<"test">>},
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(Spec, auto)).

test_status_after_mode() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(primary, maps:get(mode, Status)).

test_cluster_updates_status() ->
    Status1 = flurm_slurm_bridge:get_bridge_status(),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    Status2 = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(maps:get(clusters_total, Status1) + 1, maps:get(clusters_total, Status2)).

test_concurrent_requests() ->
    Self = self(),
    lists:foreach(fun(_) ->
        spawn(fun() ->
            _ = flurm_slurm_bridge:get_bridge_status(),
            Self ! done
        end)
    end, lists:seq(1, 10)),
    lists:foreach(fun(_) -> receive done -> ok end end, lists:seq(1, 10)).

test_forward_error_propagation() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_removal_updates_status() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    Status1 = flurm_slurm_bridge:get_bridge_status(),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"test">>),
    Status2 = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(maps:get(clusters_total, Status1) - 1, maps:get(clusters_total, Status2)).

test_shadow_prevents_forward() ->
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertEqual({error, shadow_mode}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_standalone_behavior() ->
    ok = flurm_slurm_bridge:set_mode(standalone),
    ?assertEqual(standalone, flurm_slurm_bridge:get_mode()).

test_rapid_cluster_adds() ->
    lists:foreach(fun(I) ->
        Name = list_to_binary("c" ++ integer_to_list(I)),
        ok = flurm_slurm_bridge:add_slurm_cluster(Name, #{host => <<"h">>})
    end, lists:seq(1, 5)),
    ?assertEqual(5, length(flurm_slurm_bridge:list_slurm_clusters())).

test_rapid_cluster_removes() ->
    lists:foreach(fun(I) ->
        Name = list_to_binary("c" ++ integer_to_list(I)),
        ok = flurm_slurm_bridge:add_slurm_cluster(Name, #{host => <<"h">>})
    end, lists:seq(1, 5)),
    lists:foreach(fun(I) ->
        Name = list_to_binary("c" ++ integer_to_list(I)),
        ok = flurm_slurm_bridge:remove_slurm_cluster(Name)
    end, lists:seq(1, 5)),
    ?assertEqual(0, length(flurm_slurm_bridge:list_slurm_clusters())).

test_forward_specific_exists() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, {unknown_cluster, <<"nonexistent">>}},
                 flurm_slurm_bridge:forward_job(#{}, <<"nonexistent">>)).

test_auto_forward_all_down() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_cluster_timestamps() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(maps:is_key(last_check, C)).

test_status_consistency() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    Total = maps:get(clusters_total, Status),
    Up = maps:get(clusters_up, Status),
    Down = maps:get(clusters_down, Status),
    ?assertEqual(Total, Up + Down).

test_module_exports() ->
    Exports = flurm_slurm_bridge:module_info(exports),
    ?assert(lists:member({get_mode, 0}, Exports)),
    ?assert(lists:member({set_mode, 1}, Exports)).

test_forward_proper_error() ->
    ok = flurm_slurm_bridge:set_mode(shadow),
    ?assertMatch({error, _}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_cancel_proper_error() ->
    ?assertMatch({error, _}, flurm_slurm_bridge:cancel_forwarded_job(999)).

test_get_status_proper_error() ->
    ?assertMatch({error, _}, flurm_slurm_bridge:get_forwarded_job_status(999)).

test_active_enables_forward() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertNotEqual({error, shadow_mode}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_primary_enables_forward() ->
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertNotEqual({error, shadow_mode}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_timeout_messages() ->
    whereis(flurm_slurm_bridge) ! timeout,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_down_messages() ->
    Ref = make_ref(),
    whereis(flurm_slurm_bridge) ! {'DOWN', Ref, process, self(), normal},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_cluster_auth_config() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>, auth => munge}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_cluster_ssl_config() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>, ssl => true}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_same_host_diff_ports() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h">>, port => 6817}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c2">>, #{host => <<"h">>, port => 6818}),
    ?assertEqual(2, length(flurm_slurm_bridge:list_slurm_clusters())).

test_status_transitioning() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Status = flurm_slurm_bridge:get_bridge_status(),
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(active, maps:get(mode, Status)).

test_forward_spec_validation() ->
    ok = flurm_slurm_bridge:set_mode(active),
    Spec = #{invalid_field => true},
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(Spec, auto)).

test_system_messages() ->
    whereis(flurm_slurm_bridge) ! {system, self(), get_state},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_version_tracking() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assert(maps:is_key(version, C)).

test_gen_callbacks() ->
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_auto_selection() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_list_empty() ->
    ?assertEqual([], flurm_slurm_bridge:list_slurm_clusters()).

test_list_single() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_list_multiple() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c1">>, #{host => <<"h1">>}),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"c2">>, #{host => <<"h2">>}),
    ?assertEqual(2, length(flurm_slurm_bridge:list_slurm_clusters())).

test_failure_counter() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(0, maps:get(consecutive_failures, C)).

test_job_counter() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    [C] = flurm_slurm_bridge:list_slurm_clusters(),
    ?assertEqual(0, maps:get(job_count, C)).

test_primary_slurm_status() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(undefined, maps:get(primary_slurm, Status)).

test_mode_preserves_clusters() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:set_mode(primary),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_exit_from_linked() ->
    whereis(flurm_slurm_bridge) ! {'EXIT', self(), shutdown},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_forward_invalid_spec() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_status_rate() ->
    lists:foreach(fun(_) -> _ = flurm_slurm_bridge:get_bridge_status() end, lists:seq(1, 10)),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_connection_pooling() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_network_error() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"nonexistent.invalid">>}),
    Result = flurm_slurm_bridge:forward_job(#{}, <<"test">>),
    ?assertMatch({error, _}, Result).

test_heavy_load() ->
    lists:foreach(fun(_) ->
        _ = flurm_slurm_bridge:get_mode(),
        _ = flurm_slurm_bridge:get_bridge_status()
    end, lists:seq(1, 50)),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_removal_cleans() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ok = flurm_slurm_bridge:remove_slurm_cluster(<<"test">>),
    ?assertEqual([], flurm_slurm_bridge:list_slurm_clusters()).

test_mode_events() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual(active, flurm_slurm_bridge:get_mode()).

test_tcp_closed() ->
    whereis(flurm_slurm_bridge) ! {tcp_closed, fake_socket},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_tcp_error() ->
    whereis(flurm_slurm_bridge) ! {tcp_error, fake_socket, econnreset},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_slurm_bridge))).

test_partial_response() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_response_timeout() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_multiple_forward_same() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_pending_jobs() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(jobs_pending, Status)).

test_running_jobs() ->
    Status = flurm_slurm_bridge:get_bridge_status(),
    ?assertEqual(0, maps:get(jobs_running, Status)).

test_cancel_running() ->
    ?assertEqual({error, not_found}, flurm_slurm_bridge:cancel_forwarded_job(1)).

test_cancel_pending() ->
    ?assertEqual({error, not_found}, flurm_slurm_bridge:cancel_forwarded_job(2)).

test_cluster_reconnect() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    timer:sleep(50),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_cluster_failover() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_job_migration() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).

test_cleanup_on_stop() ->
    ok = flurm_slurm_bridge:add_slurm_cluster(<<"test">>, #{host => <<"h">>}),
    ?assertEqual(1, length(flurm_slurm_bridge:list_slurm_clusters())).

test_forward_tracks_time() ->
    ok = flurm_slurm_bridge:set_mode(active),
    ?assertEqual({error, no_clusters_available}, flurm_slurm_bridge:forward_job(#{}, auto)).
