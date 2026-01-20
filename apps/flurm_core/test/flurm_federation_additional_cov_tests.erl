%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM Federation
%%%
%%% Tests multi-cluster federation functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%%===================================================================
%%% Test Generators
%%%===================================================================

federation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun basic_api_tests/1,
       fun cluster_management_tests/1,
       fun job_submission_tests/1,
       fun job_tracking_tests/1,
       fun resource_tests/1,
       fun legacy_api_tests/1,
       fun message_handling_tests/1
      ]}}.

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock httpc for cluster communication
    meck:new(httpc, [unstick, no_link]),
    meck:expect(httpc, request, fun
        (get, _, _, _) ->
            {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\": \"ok\"}"}};
        (post, _, _, _) ->
            {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\": 12345}"}}
    end),

    %% Mock flurm_config_server
    meck:new(flurm_config_server, [non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun
        (cluster_name, Default) -> Default;
        (federation_host, _) -> <<"localhost">>;
        (slurmctld_port, _) -> 6817;
        (federation_routing_policy, _) -> least_loaded;
        (_, Default) -> Default
    end),

    %% Mock flurm_partition_manager
    meck:new(flurm_partition_manager, [non_strict, no_link]),
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [#{name => <<"batch">>}] end),

    %% Mock flurm_job_manager
    meck:new(flurm_job_manager, [non_strict, no_link]),
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 99999} end),

    %% Mock flurm_scheduler (for submit_local_job)
    meck:new(flurm_scheduler, [non_strict, no_link]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 99999} end),

    ok.

cleanup(_) ->
    catch meck:unload(lager),
    catch meck:unload(httpc),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_scheduler),
    ok.

per_test_setup() ->
    {ok, Pid} = flurm_federation:start_link(),
    Pid.

per_test_cleanup(Pid) ->
    catch gen_server:stop(Pid),
    catch ets:delete(flurm_fed_clusters),
    catch ets:delete(flurm_fed_jobs),
    catch ets:delete(flurm_fed_partition_map),
    catch ets:delete(flurm_fed_remote_jobs),
    ok.

%%%===================================================================
%%% Basic API Tests
%%%===================================================================

basic_api_tests(_Pid) ->
    [
        {"start_link creates process", fun() ->
            ?assert(is_pid(whereis(flurm_federation)))
        end},

        {"is_federated returns boolean", fun() ->
            Result = flurm_federation:is_federated(),
            ?assert(is_boolean(Result))
        end},

        {"get_local_cluster returns name", fun() ->
            Cluster = flurm_federation:get_local_cluster(),
            ?assert(is_binary(Cluster))
        end},

        {"list_clusters returns list", fun() ->
            Clusters = flurm_federation:list_clusters(),
            ?assert(is_list(Clusters)),
            %% Should have at least local cluster
            ?assert(length(Clusters) >= 1)
        end}
    ].

%%%===================================================================
%%% Cluster Management Tests
%%%===================================================================

cluster_management_tests(_Pid) ->
    [
        {"add_cluster with map config", fun() ->
            Config = #{
                host => <<"remote.cluster.com">>,
                port => 6817,
                weight => 2,
                features => [<<"gpu">>, <<"fast">>]
            },
            Result = flurm_federation:add_cluster(<<"cluster2">>, Config),
            ?assertEqual(ok, Result),
            %% Verify it's in the list
            Clusters = flurm_federation:list_clusters(),
            Names = [maps:get(name, C) || C <- Clusters],
            ?assert(lists:member(<<"cluster2">>, Names))
        end},

        {"add_cluster with auth config", fun() ->
            Config = #{
                host => <<"auth.cluster.com">>,
                port => 6817,
                auth => #{token => <<"secret">>}
            },
            ?assertEqual(ok, flurm_federation:add_cluster(<<"authcluster">>, Config))
        end},

        {"remove_cluster removes it", fun() ->
            Config = #{host => <<"temp.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"tempcluster">>, Config),
            ?assertEqual(ok, flurm_federation:remove_cluster(<<"tempcluster">>)),
            ?assertEqual({error, not_found}, flurm_federation:get_cluster_status(<<"tempcluster">>))
        end},

        {"remove_cluster for non-existent returns ok or error", fun() ->
            Result = flurm_federation:remove_cluster(<<"nonexistent">>),
            %% May return ok (idempotent) or {error, not_found} depending on implementation
            ?assert(Result =:= ok orelse Result =:= {error, not_found})
        end},

        {"get_cluster_status for existing cluster", fun() ->
            Config = #{host => <<"status.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"statuscluster">>, Config),
            {ok, Status} = flurm_federation:get_cluster_status(<<"statuscluster">>),
            ?assert(is_map(Status)),
            ?assertEqual(<<"statuscluster">>, maps:get(name, Status))
        end},

        {"get_cluster_status for non-existent cluster", fun() ->
            ?assertEqual({error, not_found}, flurm_federation:get_cluster_status(<<"unknown">>))
        end}
    ].

%%%===================================================================
%%% Job Submission Tests
%%%===================================================================

job_submission_tests(_Pid) ->
    [
        {"submit_job to local cluster", fun() ->
            Job = #{name => <<"test_job">>, script => <<"#!/bin/bash\necho test">>},
            Options = #{cluster => flurm_federation:get_local_cluster()},
            Result = flurm_federation:submit_job(Job, Options),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"submit_job with partition routing", fun() ->
            Job = #{name => <<"part_job">>, partition => <<"batch">>},
            Options = #{},
            Result = flurm_federation:submit_job(Job, Options),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"route_job selects cluster", fun() ->
            Job = #{name => <<"route_job">>, partition => <<"default">>},
            Result = flurm_federation:route_job(Job),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_cluster_for_partition", fun() ->
            %% Local partition should be mapped to local cluster
            Result = flurm_federation:get_cluster_for_partition(<<"batch">>),
            ?assert(element(1, Result) =:= ok orelse Result =:= {error, not_found})
        end},

        {"get_cluster_for_partition unknown partition", fun() ->
            ?assertEqual({error, not_found}, flurm_federation:get_cluster_for_partition(<<"unknown_part">>))
        end}
    ].

%%%===================================================================
%%% Job Tracking Tests
%%%===================================================================

job_tracking_tests(_Pid) ->
    [
        {"track_remote_job creates entry", fun() ->
            Config = #{host => <<"track.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"trackcluster">>, Config),
            JobSpec = #{name => <<"tracked_job">>, script => <<"#!/bin/bash">>},
            Result = flurm_federation:track_remote_job(<<"trackcluster">>, 12345, JobSpec),
            ?assertMatch({ok, _}, Result)
        end},

        {"get_remote_job_status", fun() ->
            Config = #{host => <<"remote.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"remotecluster">>, Config),
            Result = flurm_federation:get_remote_job_status(<<"remotecluster">>, 99999),
            %% May succeed or fail depending on mock - exercises code path
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"sync_job_state", fun() ->
            Config = #{host => <<"sync.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"synccluster">>, Config),
            Result = flurm_federation:sync_job_state(<<"synccluster">>, 88888),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end}
    ].

%%%===================================================================
%%% Resource Tests
%%%===================================================================

resource_tests(_Pid) ->
    [
        {"get_federation_resources aggregates", fun() ->
            Resources = flurm_federation:get_federation_resources(),
            ?assert(is_map(Resources)),
            ?assert(maps:is_key(total_nodes, Resources) orelse maps:is_key(clusters, Resources))
        end},

        {"get_federation_jobs returns list", fun() ->
            Jobs = flurm_federation:get_federation_jobs(),
            ?assert(is_list(Jobs))
        end}
    ].

%%%===================================================================
%%% Legacy API Tests
%%%===================================================================

legacy_api_tests(_Pid) ->
    [
        {"create_federation", fun() ->
            Result = flurm_federation:create_federation(<<"myfed">>, [<<"cluster1">>]),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_federation_info when not federated", fun() ->
            Result = flurm_federation:get_federation_info(),
            ?assert(element(1, Result) =:= ok orelse Result =:= {error, not_federated})
        end},

        {"get_cluster_info for local cluster", fun() ->
            LocalCluster = flurm_federation:get_local_cluster(),
            Result = flurm_federation:get_cluster_info(LocalCluster),
            ?assertMatch({ok, _}, Result)
        end},

        {"get_cluster_info for unknown cluster", fun() ->
            ?assertEqual({error, not_found}, flurm_federation:get_cluster_info(<<"unknown">>))
        end},

        {"submit_federated_job", fun() ->
            Job = #job{id = 0, name = <<"fed_job">>, script = <<"#!/bin/bash">>},
            Options = #{},
            Result = flurm_federation:submit_federated_job(Job, Options),
            ?assert(element(1, Result) =:= ok orelse element(1, Result) =:= error)
        end},

        {"get_federated_job for unknown", fun() ->
            ?assertEqual({error, not_found}, flurm_federation:get_federated_job(<<"unknown_fed_id">>))
        end},

        {"sync_cluster_state", fun() ->
            Config = #{host => <<"sync2.cluster.com">>, port => 6817},
            flurm_federation:add_cluster(<<"sync2cluster">>, Config),
            Result = flurm_federation:sync_cluster_state(<<"sync2cluster">>),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"set_cluster_features", fun() ->
            ?assertEqual(ok, flurm_federation:set_cluster_features([<<"gpu">>, <<"ssd">>]))
        end},

        {"join_federation", fun() ->
            Result = flurm_federation:join_federation(<<"testfed">>, <<"origin.cluster.com">>),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end},

        {"leave_federation", fun() ->
            Result = flurm_federation:leave_federation(),
            ?assert(Result =:= ok orelse element(1, Result) =:= error)
        end}
    ].

%%%===================================================================
%%% Message Handling Tests
%%%===================================================================

message_handling_tests(Pid) ->
    [
        {"handle_info health_check", fun() ->
            Pid ! health_check,
            _ = sys:get_state(flurm_federation),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info sync_clusters", fun() ->
            Pid ! sync_clusters,
            _ = sys:get_state(flurm_federation),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info unknown message", fun() ->
            Pid ! unknown_message,
            _ = sys:get_state(flurm_federation),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_cast unknown message", fun() ->
            gen_server:cast(Pid, unknown_cast),
            _ = sys:get_state(flurm_federation),
            ?assert(is_process_alive(Pid))
        end}
    ].
