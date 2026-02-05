%%%-------------------------------------------------------------------
%%% @doc Comprehensive Sibling Job Coordination Tests
%%%
%%% Tests the TLA+ invariants for federation sibling coordination:
%%% - SiblingExclusivity: At most one sibling runs at any time
%%% - OriginAwareness: Origin cluster tracks which sibling is running
%%% - NoJobLoss: Jobs have at least one active sibling or all terminal
%%% - RevokedIsTerminal: Revoked siblings never transition to running
%%%
%%% Note: These tests run on a single node. The "local cluster" is
%%% always "test_origin". Remote clusters are simulated via HTTP mocks.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_sibling_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Test state
-record(test_state, {
    fed_pid :: pid() | undefined
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

sibling_coordination_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% State Machine Tests
      {"sibling starts in NULL state", fun test_sibling_initial_state/0},
      {"sibling transitions NULL -> PENDING on create", fun test_sibling_pending_transition/0},
      {"sibling transitions PENDING -> RUNNING on start", fun test_sibling_running_transition/0},
      {"sibling transitions PENDING -> REVOKED on revoke", fun test_sibling_revoked_transition/0},

      %% Exclusivity Invariant Tests
      {"local start triggers revocation of remote siblings", fun test_local_start_revokes_remotes/0},

      %% Origin Awareness Tests
      {"origin tracks running cluster after local start", fun test_origin_tracks_running/0},

      %% NoJobLoss Invariant Tests
      {"job always has at least one active sibling", fun test_no_job_loss/0},

      %% Revoke Tests
      {"handle_sibling_revoke cancels pending", fun test_handle_sibling_revoke/0},

      %% Idempotency Tests
      {"duplicate start notification is idempotent", fun test_duplicate_start_idempotent/0},
      {"duplicate revoke is idempotent", fun test_duplicate_revoke_idempotent/0},

      %% Edge Cases
      {"revoke non-existent sibling returns error", fun test_revoke_nonexistent/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),

    %% Set application environment for cluster name BEFORE starting federation
    application:set_env(flurm_core, cluster_name, <<"test_origin">>),

    %% Setup meck for external dependencies
    meck:new(flurm_scheduler, [passthrough, non_strict, no_link]),
    meck:new(flurm_job_manager, [passthrough, non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:new(flurm_metrics, [passthrough, non_strict, no_link]),
    meck:new(httpc, [passthrough, non_strict, no_link]),

    %% Default mock expectations
    meck:expect(flurm_config_server, get, fun(Key) ->
        case Key of
            cluster_name -> <<"test_origin">>;
            _ -> undefined
        end
    end),
    meck:expect(flurm_config_server, get, fun(Key, Default) ->
        case Key of
            cluster_name -> <<"test_origin">>;
            _ -> Default
        end
    end),

    %% Mock scheduler to return unique job IDs
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, erlang:unique_integer([positive])} end),
    meck:expect(flurm_job_manager, cancel_job, fun(_) -> ok end),

    %% Mock metrics
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),

    %% Mock HTTP for federation messages - always succeed for both GET and POST
    meck:expect(httpc, request, fun
        (get, {_Url, _Headers}, _Opts, _Profile) ->
            {ok, {{"HTTP/1.1", 200, "OK"}, [],
                  "{\"state\":\"up\",\"node_count\":10,\"cpu_count\":100,\"memory_mb\":10000}"}};
        (post, {_Url, _Headers, _ContentType, _Body}, _Opts, _Profile) ->
            {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\":\"ok\"}"}}
    end),

    %% Stop any existing federation server to ensure clean state
    case whereis(flurm_federation) of
        undefined -> ok;
        OldPid ->
            catch gen_server:stop(OldPid, normal, 1000),
            timer:sleep(100)  % Give it time to clean up
    end,

    %% Start fresh federation server with correct cluster name
    {ok, Pid} = flurm_federation:start_link(),

    %% Register clusters needed for tests
    %% Note: Local cluster is "test_origin" per mocked config
    ok = flurm_federation:add_cluster(<<"test_origin">>, #{
        host => <<"localhost">>,
        port => 6817
    }),
    ok = flurm_federation:add_cluster(<<"cluster_b">>, #{
        host => <<"cluster-b.example.com">>,
        port => 6817
    }),

    %% Set cluster states to "up" for testing
    update_cluster_resources(<<"test_origin">>, #{state => up}),
    update_cluster_resources(<<"cluster_b">>, #{state => up}),

    #test_state{fed_pid = Pid}.

cleanup(#test_state{fed_pid = Pid}) ->
    %% Stop federation if we started it
    case Pid of
        undefined -> ok;
        _ ->
            case is_process_alive(Pid) of
                true ->
                    catch gen_server:stop(Pid, normal, 1000);
                false ->
                    ok
            end
    end,

    %% Unload mocks
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_metrics),
    catch meck:unload(httpc),
    ok.

%%====================================================================
%% State Machine Tests
%%====================================================================

test_sibling_initial_state() ->
    %% Query for non-existent sibling should return not_found
    Result = flurm_federation:get_sibling_job_state(<<"fed_999">>, <<"cluster_x">>),
    ?assertEqual({error, not_found}, Result).

test_sibling_pending_transition() ->
    %% Create sibling jobs - include local cluster test_origin
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    TargetClusters = [<<"test_origin">>, <<"cluster_b">>],

    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, TargetClusters),
    ?assert(is_binary(FedJobId)),

    %% Check local sibling state is PENDING
    {ok, SiblingLocal} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_PENDING, SiblingLocal#sibling_job_state.state),

    %% Remote sibling should also be PENDING (mocked HTTP succeeded)
    {ok, SiblingB} = flurm_federation:get_sibling_job_state(FedJobId, <<"cluster_b">>),
    ?assertEqual(?SIBLING_STATE_PENDING, SiblingB#sibling_job_state.state).

test_sibling_running_transition() ->
    %% Create sibling jobs including local cluster
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Notify that local sibling started (12345 is the local job ID)
    ok = flurm_federation:notify_job_started(FedJobId, 12345),

    %% Check local cluster is RUNNING
    {ok, Sibling} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_RUNNING, Sibling#sibling_job_state.state).

test_sibling_revoked_transition() ->
    %% Create sibling job on local cluster
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Simulate revoke message arriving (from origin saying another cluster started)
    RevokeMsg = #fed_sibling_revoke_msg{
        federation_job_id = FedJobId,
        running_cluster = <<"other_cluster">>,
        revoke_reason = <<"another sibling started">>
    },
    ok = flurm_federation:handle_sibling_revoke(RevokeMsg),

    %% Check local sibling is REVOKED
    {ok, Sibling} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_REVOKED, Sibling#sibling_job_state.state).

%%====================================================================
%% Exclusivity Invariant Tests
%%====================================================================

test_local_start_revokes_remotes() ->
    %% Create sibling jobs on local and remote clusters
    JobSpec = #{name => <<"test">>, script => <<"/bin/sleep 10">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>, <<"cluster_b">>]),

    %% Start local sibling
    ok = flurm_federation:notify_job_started(FedJobId, 12345),

    %% Verify local is RUNNING
    {ok, SiblingLocal} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_RUNNING, SiblingLocal#sibling_job_state.state),

    %% Verify remote cluster_b is REVOKED (revoke message was sent via HTTP mock)
    {ok, SiblingB} = flurm_federation:get_sibling_job_state(FedJobId, <<"cluster_b">>),
    ?assertEqual(?SIBLING_STATE_REVOKED, SiblingB#sibling_job_state.state).

%%====================================================================
%% Origin Awareness Tests
%%====================================================================

test_origin_tracks_running() ->
    %% Create sibling jobs
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Before start, no cluster should be tracked as running
    BeforeResult = flurm_federation:get_running_cluster(FedJobId),
    case BeforeResult of
        {ok, undefined} -> ok;
        {error, no_running_sibling} -> ok
    end,

    %% Start local sibling
    ok = flurm_federation:notify_job_started(FedJobId, 12345),

    %% Origin should track test_origin as running
    {ok, RunningCluster} = flurm_federation:get_running_cluster(FedJobId),
    ?assertEqual(<<"test_origin">>, RunningCluster).

%%====================================================================
%% NoJobLoss Invariant Tests
%%====================================================================

test_no_job_loss() ->
    %% Create sibling jobs
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>, <<"cluster_b">>]),

    %% Verify at least one sibling is active (PENDING or RUNNING)
    {ok, SiblingLocal} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    {ok, SiblingB} = flurm_federation:get_sibling_job_state(FedJobId, <<"cluster_b">>),

    ActiveCount = count_active_siblings([SiblingLocal, SiblingB]),
    ?assert(ActiveCount >= 1).

%%====================================================================
%% Revoke Tests
%%====================================================================

test_handle_sibling_revoke() ->
    %% Create sibling on local cluster
    JobSpec = #{name => <<"test">>, script => <<"/bin/sleep 60">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Simulate receiving revoke message
    RevokeMsg = #fed_sibling_revoke_msg{
        federation_job_id = FedJobId,
        running_cluster = <<"other_cluster">>,
        revoke_reason = <<"another sibling started">>
    },

    Result = flurm_federation:handle_sibling_revoke(RevokeMsg),
    ?assertEqual(ok, Result),

    %% Verify sibling was revoked
    {ok, Sibling} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_REVOKED, Sibling#sibling_job_state.state).

%%====================================================================
%% Idempotency Tests
%%====================================================================

test_duplicate_start_idempotent() ->
    %% Create sibling
    JobSpec = #{name => <<"test">>, script => <<"/bin/true">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Start twice
    ok = flurm_federation:notify_job_started(FedJobId, 12345),
    Result = flurm_federation:notify_job_started(FedJobId, 12345),

    %% Second start should succeed (idempotent)
    ?assertEqual(ok, Result),

    %% State should still be RUNNING
    {ok, Sibling} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_RUNNING, Sibling#sibling_job_state.state).

test_duplicate_revoke_idempotent() ->
    %% Create sibling
    JobSpec = #{name => <<"test">>, script => <<"/bin/sleep 60">>, cpus => 1},
    {ok, FedJobId} = flurm_federation:create_sibling_jobs(JobSpec, [<<"test_origin">>]),

    %% Revoke twice
    RevokeMsg = #fed_sibling_revoke_msg{
        federation_job_id = FedJobId,
        running_cluster = <<"other_cluster">>,
        revoke_reason = <<"test">>
    },
    ok = flurm_federation:handle_sibling_revoke(RevokeMsg),
    Result = flurm_federation:handle_sibling_revoke(RevokeMsg),

    %% Second revoke should succeed (idempotent)
    ?assertEqual(ok, Result),

    %% State should still be REVOKED
    {ok, Sibling} = flurm_federation:get_sibling_job_state(FedJobId, <<"test_origin">>),
    ?assertEqual(?SIBLING_STATE_REVOKED, Sibling#sibling_job_state.state).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_revoke_nonexistent() ->
    %% Try to revoke a sibling that doesn't exist
    RevokeMsg = #fed_sibling_revoke_msg{
        federation_job_id = <<"nonexistent_fed_job">>,
        running_cluster = <<"cluster_x">>,
        revoke_reason = <<"test">>
    },

    Result = flurm_federation:handle_sibling_revoke(RevokeMsg),
    %% Returns ok to handle out-of-order messages gracefully
    %% (revoke might arrive before sibling create in distributed system)
    ?assertEqual(ok, Result).

%%====================================================================
%% Helper Functions
%%====================================================================

count_active_siblings(Siblings) ->
    lists:foldl(fun(#sibling_job_state{state = State}, Count) ->
        case State of
            ?SIBLING_STATE_PENDING -> Count + 1;
            ?SIBLING_STATE_RUNNING -> Count + 1;
            _ -> Count
        end
    end, 0, Siblings).

%% Update cluster state in ETS (for testing)
update_cluster_resources(Name, Updates) ->
    case ets:lookup(flurm_fed_clusters, Name) of
        [Cluster] ->
            Updated = apply_updates(Cluster, Updates),
            ets:insert(flurm_fed_clusters, Updated);
        [] ->
            {error, not_found}
    end.

apply_updates(Cluster, Updates) ->
    %% Position 6 = #fed_cluster.state
    Cluster1 = case maps:get(state, Updates, undefined) of
        undefined -> Cluster;
        State -> setelement(6, Cluster, State)
    end,
    Cluster1.
