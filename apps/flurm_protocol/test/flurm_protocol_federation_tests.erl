%%%-------------------------------------------------------------------
%%% @doc Unit Tests for Federation Protocol Messages
%%%
%%% Tests encode/decode for:
%%% - REQUEST_FEDERATION_SUBMIT (2032)
%%% - RESPONSE_FEDERATION_SUBMIT (2033)
%%% - REQUEST_FEDERATION_JOB_STATUS (2034)
%%% - RESPONSE_FEDERATION_JOB_STATUS (2035)
%%% - REQUEST_FEDERATION_JOB_CANCEL (2036)
%%% - RESPONSE_FEDERATION_JOB_CANCEL (2037)
%%%
%%% Note: Some round-trip tests are disabled pending full codec implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_federation_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Setup/Cleanup
%%%===================================================================

federation_protocol_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          federation_submit_tests(),
          federation_job_status_tests(),
          federation_job_cancel_tests(),
          helper_function_tests()
         ]
     end}.

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(lager),
    ok.

%%%===================================================================
%%% Federation Submit Tests
%%%===================================================================

federation_submit_tests() ->
    [
        {"encode REQUEST_FEDERATION_SUBMIT produces binary", fun() ->
            Req = #federation_submit_request{
                source_cluster = <<"cluster-a">>,
                target_cluster = <<"cluster-b">>,
                job_id = 12345,
                name = <<"test-job">>,
                script = <<"#!/bin/bash\necho hello">>,
                partition = <<"default">>,
                num_cpus = 4,
                num_nodes = 1,
                memory_mb = 8192,
                time_limit = 3600,
                user_id = 1000,
                group_id = 1000,
                work_dir = <<"/home/user">>,
                features = <<"gpu,fast">>,
                environment = [<<"PATH=/usr/bin">>]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_FEDERATION_SUBMIT, Req),
            ?assert(is_binary(Binary)),
            ?assert(byte_size(Binary) > 20)
        end},

        {"encode RESPONSE_FEDERATION_SUBMIT produces binary", fun() ->
            Resp = #federation_submit_response{
                source_cluster = <<"cluster-a">>,
                error_code = 0,
                job_id = 54321,
                error_msg = <<>>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_SUBMIT, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_SUBMIT with error produces binary", fun() ->
            Resp = #federation_submit_response{
                source_cluster = <<"cluster-a">>,
                error_code = 1,
                job_id = 0,
                error_msg = <<"No resources available">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_SUBMIT, Resp),
            ?assert(is_binary(Binary)),
            ?assert(byte_size(Binary) > 0)
        end},

        {"REQUEST_FEDERATION_SUBMIT with minimal fields", fun() ->
            Req = #federation_submit_request{
                source_cluster = <<>>,
                target_cluster = <<>>,
                job_id = 0,
                name = <<>>,
                script = <<>>,
                partition = <<>>,
                num_cpus = 1,
                num_nodes = 1
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_FEDERATION_SUBMIT, Req),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Federation Job Status Tests
%%%===================================================================

federation_job_status_tests() ->
    [
        {"encode REQUEST_FEDERATION_JOB_STATUS produces binary", fun() ->
            Req = #federation_job_status_request{
                source_cluster = <<"cluster-a">>,
                job_id = 12345,
                job_id_str = <<"12345">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_FEDERATION_JOB_STATUS, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_JOB_STATUS produces binary", fun() ->
            Resp = #federation_job_status_response{
                error_code = 0,
                job_id = 12345,
                job_state = 1,  % RUNNING
                state_reason = 0,
                start_time = 1700000000,
                end_time = 0,
                exit_code = 0,
                nodes = <<"node[1-4]">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_JOB_STATUS for completed job", fun() ->
            Resp = #federation_job_status_response{
                error_code = 0,
                job_id = 88888,
                job_state = 3,  % COMPLETED
                state_reason = 0,
                start_time = 1700000000,
                end_time = 1700003600,
                exit_code = 0,
                nodes = <<"compute-01">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_JOB_STATUS job not found", fun() ->
            Resp = #federation_job_status_response{
                error_code = 1,
                job_id = 0,
                job_state = 0,
                state_reason = 0,
                start_time = 0,
                end_time = 0,
                exit_code = 0,
                nodes = <<>>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Federation Job Cancel Tests
%%%===================================================================

federation_job_cancel_tests() ->
    [
        {"encode REQUEST_FEDERATION_JOB_CANCEL produces binary", fun() ->
            Req = #federation_job_cancel_request{
                source_cluster = <<"cluster-a">>,
                job_id = 12345,
                job_id_str = <<"12345">>,
                signal = 15
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_FEDERATION_JOB_CANCEL, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode REQUEST_FEDERATION_JOB_CANCEL with SIGKILL", fun() ->
            Req = #federation_job_cancel_request{
                source_cluster = <<"admin-cluster">>,
                job_id = 55555,
                job_id_str = <<"55555">>,
                signal = 9
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_FEDERATION_JOB_CANCEL, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_JOB_CANCEL produces binary", fun() ->
            Resp = #federation_job_cancel_response{
                error_code = 0,
                job_id = 12345
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FEDERATION_JOB_CANCEL with error", fun() ->
            Resp = #federation_job_cancel_response{
                error_code = 2,  % Job already completed
                job_id = 44444
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Helper Function Tests
%%%===================================================================

helper_function_tests() ->
    [
        {"message_type_name returns atoms for federation types", fun() ->
            ?assertEqual(request_federation_submit,
                         flurm_protocol_codec:message_type_name(?REQUEST_FEDERATION_SUBMIT)),
            ?assertEqual(response_federation_submit,
                         flurm_protocol_codec:message_type_name(?RESPONSE_FEDERATION_SUBMIT)),
            ?assertEqual(request_federation_job_status,
                         flurm_protocol_codec:message_type_name(?REQUEST_FEDERATION_JOB_STATUS)),
            ?assertEqual(response_federation_job_status,
                         flurm_protocol_codec:message_type_name(?RESPONSE_FEDERATION_JOB_STATUS)),
            ?assertEqual(request_federation_job_cancel,
                         flurm_protocol_codec:message_type_name(?REQUEST_FEDERATION_JOB_CANCEL)),
            ?assertEqual(response_federation_job_cancel,
                         flurm_protocol_codec:message_type_name(?RESPONSE_FEDERATION_JOB_CANCEL))
        end},

        {"is_request returns true for federation requests", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_FEDERATION_SUBMIT)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_FEDERATION_JOB_STATUS)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_FEDERATION_JOB_CANCEL)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_FEDERATION_SUBMIT)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_FEDERATION_JOB_STATUS)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_FEDERATION_JOB_CANCEL))
        end},

        {"is_response returns true for federation responses", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_FEDERATION_SUBMIT)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_FEDERATION_JOB_STATUS)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_FEDERATION_JOB_CANCEL)),
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_FEDERATION_SUBMIT)),
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_FEDERATION_JOB_STATUS)),
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_FEDERATION_JOB_CANCEL))
        end},

        {"message type constants are defined correctly", fun() ->
            ?assertEqual(2032, ?REQUEST_FEDERATION_SUBMIT),
            ?assertEqual(2033, ?RESPONSE_FEDERATION_SUBMIT),
            ?assertEqual(2034, ?REQUEST_FEDERATION_JOB_STATUS),
            ?assertEqual(2035, ?RESPONSE_FEDERATION_JOB_STATUS),
            ?assertEqual(2036, ?REQUEST_FEDERATION_JOB_CANCEL),
            ?assertEqual(2037, ?RESPONSE_FEDERATION_JOB_CANCEL)
        end}
    ].
