%%%-------------------------------------------------------------------
%%% @doc Federation Message Handling Tests
%%%
%%% Tests the handle_federation_message/2 and update_settings/1
%%% functions of flurm_federation. Exercises the internal
%%% do_handle_federation_message/3 dispatch for each federation
%%% message type (2070-2074) and verifies update_settings/1 applies
%%% routing_policy, sync_interval, and health_check_interval without
%%% crashing.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_message_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

federation_message_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun handle_unknown_message_type/1,
       fun handle_decode_failure/1,
       fun handle_fed_job_submit/1,
       fun handle_fed_job_started_unknown/1,
       fun handle_fed_job_started_known/1,
       fun handle_fed_sibling_revoke_unknown/1,
       fun handle_fed_job_completed_unknown/1,
       fun handle_fed_job_completed_known/1,
       fun handle_fed_job_failed_known/1,
       fun update_settings_routing_policy/1,
       fun update_settings_sync_interval/1,
       fun update_settings_empty_map/1
      ]}}.

%%%===================================================================
%%% Setup / Cleanup
%%%===================================================================

setup() ->
    %% Mock lager
    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(lager),
    catch meck:unload(httpc),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_protocol_codec),

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
    meck:new(flurm_config_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_config_server, get, fun
        (cluster_name, Default) -> Default;
        (federation_host, _) -> <<"localhost">>;
        (slurmctld_port, _) -> 6817;
        (federation_routing_policy, _) -> least_loaded;
        (_, Default) -> Default
    end),

    %% Mock flurm_partition_manager
    meck:new(flurm_partition_manager, [passthrough, non_strict, no_link]),
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [#{name => <<"batch">>}] end),

    %% Mock flurm_job_manager
    meck:new(flurm_job_manager, [passthrough, non_strict, no_link]),
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 99999} end),

    %% Mock flurm_scheduler (for submit_local_job / submit_job)
    meck:new(flurm_scheduler, [passthrough, non_strict, no_link]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 42} end),
    meck:expect(flurm_scheduler, cancel_job, fun(_) -> ok end),

    %% Mock flurm_metrics
    meck:new(flurm_metrics, [non_strict, no_link]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),

    %% Mock flurm_protocol_codec with passthrough
    meck:new(flurm_protocol_codec, [passthrough, no_link]),
    install_decode_body_mock(),

    ok.

%% Install the standard decode_body mock for federation message types.
%% Called in setup and in per_test_setup to restore after tests that override it.
install_decode_body_mock() ->
    meck:expect(flurm_protocol_codec, decode_body, fun
        (?MSG_FED_JOB_SUBMIT, _Bin) ->
            {ok, #fed_job_submit_msg{
                federation_job_id = <<"fed-mock-submit">>,
                origin_cluster = <<"remote_origin">>,
                target_cluster = <<"default">>,
                job_spec = #{name => <<"test_job">>, script => <<"#!/bin/bash\necho hi">>},
                submit_time = 1000
            }};
        (?MSG_FED_JOB_STARTED, _Bin) ->
            {ok, #fed_job_started_msg{
                federation_job_id = <<"fed-test-123">>,
                running_cluster = <<"cluster2">>,
                local_job_id = 100,
                start_time = 2000
            }};
        (?MSG_FED_SIBLING_REVOKE, _Bin) ->
            {ok, #fed_sibling_revoke_msg{
                federation_job_id = <<"fed-test-123">>,
                running_cluster = <<"cluster2">>,
                revoke_reason = <<"sibling_started">>
            }};
        (?MSG_FED_JOB_COMPLETED, _Bin) ->
            {ok, #fed_job_completed_msg{
                federation_job_id = <<"fed-test-123">>,
                running_cluster = <<"cluster2">>,
                local_job_id = 100,
                end_time = 3000,
                exit_code = 0
            }};
        (?MSG_FED_JOB_FAILED, _Bin) ->
            {ok, #fed_job_failed_msg{
                federation_job_id = <<"fed-test-123">>,
                running_cluster = <<"cluster2">>,
                local_job_id = 100,
                end_time = 3000,
                exit_code = 1,
                error_msg = <<"OOM">>
            }};
        (MsgType, Bin) ->
            meck:passthrough([MsgType, Bin])
    end).

cleanup(_) ->
    catch meck:unload(lager),
    catch meck:unload(httpc),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_protocol_codec),
    ok.

per_test_setup() ->
    %% Restore the decode_body mock in case a previous test overrode it
    install_decode_body_mock(),
    {ok, Pid} = flurm_federation:start_link(),
    Pid.

per_test_cleanup(Pid) ->
    catch gen_server:stop(Pid),
    catch ets:delete(flurm_fed_clusters),
    catch ets:delete(flurm_fed_jobs),
    catch ets:delete(flurm_fed_partition_map),
    catch ets:delete(flurm_fed_remote_jobs),
    catch ets:delete(flurm_fed_sibling_jobs),
    ok.

%%%===================================================================
%%% Helper: insert a federation job tracker into ETS
%%%===================================================================

insert_test_tracker() ->
    %% Use <<"default">> directly since that's what get_local_cluster_name()
    %% returns via application:get_env(flurm_core, cluster_name, <<"default">>)
    LocalCluster = <<"default">>,
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-test-123">>,
        origin_cluster = LocalCluster,
        origin_job_id = 42,
        running_cluster = undefined,
        sibling_states = #{
            <<"cluster2">> => #sibling_job_state{
                federation_job_id = <<"fed-test-123">>,
                sibling_cluster = <<"cluster2">>,
                origin_cluster = LocalCluster,
                local_job_id = 100,
                state = ?SIBLING_STATE_PENDING
            }
        },
        submit_time = 1000,
        job_spec = #{}
    },
    ets:insert(flurm_fed_sibling_jobs, Tracker),
    Tracker.

%%%===================================================================
%%% Tests: handle_federation_message/2
%%%===================================================================

%% 1. Unknown message type
handle_unknown_message_type(_Pid) ->
    [
        {"unknown federation message type returns error", fun() ->
            %% Temporarily add an expectation for 9999
            meck:expect(flurm_protocol_codec, decode_body, fun
                (9999, _Bin) -> {ok, ignored};
                (MsgType, Bin) -> meck:passthrough([MsgType, Bin])
            end),
            Result = flurm_federation:handle_federation_message(9999, <<>>),
            ?assertMatch({error, {unknown_message_type, 9999}}, Result)
        end}
    ].

%% 2. Decode failure
handle_decode_failure(_Pid) ->
    [
        {"decode failure returns decode_failed error", fun() ->
            meck:expect(flurm_protocol_codec, decode_body, fun
                (?MSG_FED_JOB_SUBMIT, _Bin) ->
                    {error, bad_binary};
                (MsgType, Bin) ->
                    meck:passthrough([MsgType, Bin])
            end),
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_SUBMIT, <<"garbage">>),
            ?assertMatch({error, {decode_failed, bad_binary}}, Result)
        end}
    ].

%% 3. MSG_FED_JOB_SUBMIT -- successful path
handle_fed_job_submit(_Pid) ->
    [
        {"MSG_FED_JOB_SUBMIT creates local sibling job", fun() ->
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_SUBMIT, <<"payload">>),
            ?assertEqual(ok, Result)
        end}
    ].

%% 4. MSG_FED_JOB_STARTED with unknown federation job
handle_fed_job_started_unknown(_Pid) ->
    [
        {"MSG_FED_JOB_STARTED with unknown job returns error", fun() ->
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_STARTED, <<"payload">>),
            ?assertMatch({error, federation_job_not_found}, Result)
        end}
    ].

%% 5. MSG_FED_JOB_STARTED with known federation job
handle_fed_job_started_known(_Pid) ->
    [
        {"MSG_FED_JOB_STARTED with known job updates tracker", fun() ->
            insert_test_tracker(),
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_STARTED, <<"payload">>),
            ?assertEqual(ok, Result),
            [UpdatedTracker] = ets:lookup(flurm_fed_sibling_jobs, <<"fed-test-123">>),
            ?assertEqual(<<"cluster2">>, UpdatedTracker#fed_job_tracker.running_cluster)
        end}
    ].

%% 6. MSG_FED_SIBLING_REVOKE with unknown job
handle_fed_sibling_revoke_unknown(_Pid) ->
    [
        {"MSG_FED_SIBLING_REVOKE with unknown job returns ok", fun() ->
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_SIBLING_REVOKE, <<"payload">>),
            ?assertEqual(ok, Result)
        end}
    ].

%% 7. MSG_FED_JOB_COMPLETED with unknown job -- tolerant, returns ok
handle_fed_job_completed_unknown(_Pid) ->
    [
        {"MSG_FED_JOB_COMPLETED with unknown job returns ok", fun() ->
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_COMPLETED, <<"payload">>),
            ?assertEqual(ok, Result)
        end}
    ].

%% 8. MSG_FED_JOB_COMPLETED with known job -- updates state
handle_fed_job_completed_known(_Pid) ->
    [
        {"MSG_FED_JOB_COMPLETED with known job updates state", fun() ->
            insert_test_tracker(),
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_COMPLETED, <<"payload">>),
            ?assertEqual(ok, Result),
            [UpdatedTracker] = ets:lookup(flurm_fed_sibling_jobs, <<"fed-test-123">>),
            #{<<"cluster2">> := SibState} = UpdatedTracker#fed_job_tracker.sibling_states,
            ?assertEqual(?SIBLING_STATE_COMPLETED, SibState#sibling_job_state.state),
            ?assertEqual(3000, SibState#sibling_job_state.end_time),
            ?assertEqual(0, SibState#sibling_job_state.exit_code)
        end}
    ].

%% 9. MSG_FED_JOB_FAILED with known job -- updates state
handle_fed_job_failed_known(_Pid) ->
    [
        {"MSG_FED_JOB_FAILED with known job updates state", fun() ->
            insert_test_tracker(),
            Result = flurm_federation:handle_federation_message(
                         ?MSG_FED_JOB_FAILED, <<"payload">>),
            ?assertEqual(ok, Result),
            [UpdatedTracker] = ets:lookup(flurm_fed_sibling_jobs, <<"fed-test-123">>),
            #{<<"cluster2">> := SibState} = UpdatedTracker#fed_job_tracker.sibling_states,
            ?assertEqual(?SIBLING_STATE_FAILED, SibState#sibling_job_state.state),
            ?assertEqual(3000, SibState#sibling_job_state.end_time),
            ?assertEqual(1, SibState#sibling_job_state.exit_code)
        end}
    ].

%%%===================================================================
%%% Tests: update_settings/1
%%%===================================================================

%% 10. update_settings with routing_policy
update_settings_routing_policy(_Pid) ->
    [
        {"update_settings with routing_policy does not crash", fun() ->
            Result = flurm_federation:update_settings(#{routing_policy => round_robin}),
            ?assertEqual(ok, Result)
        end}
    ].

%% 11. update_settings with sync_interval
update_settings_sync_interval(_Pid) ->
    [
        {"update_settings with sync_interval does not crash", fun() ->
            Result = flurm_federation:update_settings(#{sync_interval => 5000}),
            ?assertEqual(ok, Result)
        end}
    ].

%% 12. update_settings with empty map
update_settings_empty_map(_Pid) ->
    [
        {"update_settings with empty map does not crash", fun() ->
            Result = flurm_federation:update_settings(#{}),
            ?assertEqual(ok, Result)
        end}
    ].
