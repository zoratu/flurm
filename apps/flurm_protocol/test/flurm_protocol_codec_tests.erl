%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for FLURM Protocol Codec
%%%
%%% Tests for:
%%% - All message encoding/decoding functions
%%% - Error handling paths
%%% - Edge cases
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Setup/Cleanup
%%%===================================================================

%% Module-level setup that runs once for all tests
codec_test_() ->
    {setup,
     fun module_setup/0,
     fun module_cleanup/1,
     fun(_) ->
         [
          encode_decode_tests(),
          node_info_tests(),
          job_info_response_tests(),
          kill_job_tests(),
          batch_job_request_tests(),
          job_step_tests(),
          info_query_tests(),
          reconfigure_tests(),
          resource_allocation_tests(),
          message_type_helper_tests(),
          decode_error_tests(),
          protocol_resource_tests(),
          decode_body_tests(),
          encode_body_tests(),
          with_extra_tests(),
          response_tests(),
          edge_case_tests()
         ]
     end}.

module_setup() ->
    %% Mock lager since it uses parse transform and may not be available
    %% Use non_strict since lager functions don't actually exist (they're transformed)
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

module_cleanup(_) ->
    meck:unload(lager),
    ok.

%%%===================================================================
%%% Basic encode/decode Tests
%%%===================================================================

encode_decode_tests() ->
    [
        {"encode REQUEST_PING", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
            ?assert(is_binary(Binary)),
            ?assert(byte_size(Binary) >= 14)  % 4 byte length + 10 byte header
        end},

        {"decode REQUEST_PING roundtrip", fun() ->
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertMatch(#ping_request{}, Msg#slurm_msg.body)
        end},

        {"encode RESPONSE_SLURM_RC", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
            ?assert(is_binary(Binary))
        end},

        {"decode RESPONSE_SLURM_RC roundtrip", fun() ->
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertEqual(-1, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end},

        {"encode REQUEST_JOB_INFO", fun() ->
            Req = #job_info_request{show_flags = 1, job_id = 123, user_id = 1000},
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
            ?assert(is_binary(Binary))
        end},

        {"decode REQUEST_JOB_INFO roundtrip", fun() ->
            Req = #job_info_request{show_flags = 2, job_id = 456, user_id = 500},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_JOB_INFO, Msg#slurm_msg.header#slurm_header.msg_type),
            Body = Msg#slurm_msg.body,
            ?assertEqual(2, Body#job_info_request.show_flags),
            ?assertEqual(456, Body#job_info_request.job_id),
            ?assertEqual(500, Body#job_info_request.user_id)
        end},

        {"encode REQUEST_NODE_REGISTRATION_STATUS", fun() ->
            Req = #node_registration_request{status_only = true},
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Req),
            ?assert(is_binary(Binary))
        end},

        {"decode REQUEST_NODE_REGISTRATION_STATUS roundtrip", fun() ->
            lists:foreach(fun(StatusOnly) ->
                Req = #node_registration_request{status_only = StatusOnly},
                {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Req),
                {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
                ?assertEqual(StatusOnly, (Msg#slurm_msg.body)#node_registration_request.status_only)
            end, [true, false])
        end},

        {"encode REQUEST_CANCEL_JOB", fun() ->
            Req = #cancel_job_request{job_id = 999, step_id = 0, signal = 15, flags = 0},
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
            ?assert(is_binary(Binary))
        end},

        {"decode REQUEST_CANCEL_JOB roundtrip", fun() ->
            Req = #cancel_job_request{job_id = 12345, job_id_str = <<"12345">>, step_id = 1, signal = 9, flags = 0},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            Body = Msg#slurm_msg.body,
            ?assertEqual(12345, Body#cancel_job_request.job_id),
            ?assertEqual(9, Body#cancel_job_request.signal)
        end},

        {"encode RESPONSE_SUBMIT_BATCH_JOB", fun() ->
            Resp = #batch_job_response{job_id = 10000, step_id = 0, error_code = 0},
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
            ?assert(is_binary(Binary))
        end},

        {"decode RESPONSE_SUBMIT_BATCH_JOB roundtrip", fun() ->
            Resp = #batch_job_response{job_id = 88888, step_id = 0, error_code = 0, job_submit_user_msg = <<"OK">>},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            Body = Msg#slurm_msg.body,
            ?assertEqual(88888, Body#batch_job_response.job_id),
            ?assertEqual(0, Body#batch_job_response.error_code),
            ?assertEqual(<<"OK">>, Body#batch_job_response.job_submit_user_msg)
        end}
    ].

%%%===================================================================
%%% Node and Partition Info Tests
%%%===================================================================

node_info_tests() ->
    [
        {"encode REQUEST_NODE_INFO", fun() ->
            Req = #node_info_request{show_flags = 0, node_name = <<"node01">>},
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_NODE_INFO, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_NODE_INFO", fun() ->
            Node = #node_info{
                name = <<"node01">>,
                node_hostname = <<"node01.local">>,
                node_addr = <<"192.168.1.1">>,
                port = 6818,
                node_state = 0,
                cpus = 64,
                boards = 1,
                sockets = 2,
                cores = 16,
                threads = 2,
                real_memory = 131072,
                tmp_disk = 102400,
                weight = 1,
                arch = <<"x86_64">>,
                features = <<"gpu,nvme">>,
                features_act = <<"gpu,nvme">>,
                os = <<"Linux">>,
                version = <<"22.05">>
            },
            Resp = #node_info_response{
                last_update = erlang:system_time(second),
                node_count = 1,
                nodes = [Node]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_NODE_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode REQUEST_PARTITION_INFO", fun() ->
            Req = #partition_info_request{show_flags = 0, partition_name = <<"batch">>},
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PARTITION_INFO, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_PARTITION_INFO", fun() ->
            Part = #partition_info{
                name = <<"batch">>,
                max_time = 1440,
                default_time = 60,
                max_nodes = 100,
                min_nodes = 1,
                total_nodes = 50,
                total_cpus = 3200,
                priority_job_factor = 1,
                priority_tier = 1,
                state_up = 1,
                nodes = <<"node[01-50]">>
            },
            Resp = #partition_info_response{
                last_update = erlang:system_time(second),
                partition_count = 1,
                partitions = [Part]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_PARTITION_INFO, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Job Info Response Tests
%%%===================================================================

job_info_response_tests() ->
    [
        {"encode RESPONSE_JOB_INFO with empty jobs", fun() ->
            Resp = #job_info_response{
                last_update = erlang:system_time(second),
                job_count = 0,
                jobs = []
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_JOB_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_JOB_INFO with one job", fun() ->
            Job = #job_info{
                job_id = 1234,
                user_id = 1000,
                group_id = 1000,
                name = <<"test_job">>,
                partition = <<"batch">>,
                account = <<"users">>,
                job_state = 1,
                num_nodes = 1,
                num_cpus = 4,
                num_tasks = 4,
                time_limit = 60,
                start_time = erlang:system_time(second),
                submit_time = erlang:system_time(second) - 3600
            },
            Resp = #job_info_response{
                last_update = erlang:system_time(second),
                job_count = 1,
                jobs = [Job]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_JOB_INFO, Resp),
            ?assert(is_binary(Binary)),
            ?assert(byte_size(Binary) > 100)
        end}
    ].

%%%===================================================================
%%% Kill Job Request Tests
%%%===================================================================

kill_job_tests() ->
    [
        {"encode REQUEST_KILL_JOB", fun() ->
            Req = #kill_job_request{
                job_id = 5678,
                job_id_str = <<"5678">>,
                step_id = -1,
                signal = 9,
                flags = 0
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_KILL_JOB, Req),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Batch Job Request Tests
%%%===================================================================

batch_job_request_tests() ->
    [
        {"encode REQUEST_SUBMIT_BATCH_JOB", fun() ->
            Req = #batch_job_request{
                name = <<"test_batch">>,
                script = <<"#!/bin/bash\necho hello">>,
                partition = <<"batch">>,
                min_nodes = 1,
                max_nodes = 1,
                min_cpus = 4,
                num_tasks = 4,
                cpus_per_task = 1,
                time_limit = 60,
                priority = 0,
                user_id = 1000,
                group_id = 1000,
                work_dir = <<"/home/user">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode job_submit_req legacy format", fun() ->
            Req = #job_submit_req{
                name = <<"legacy_job">>,
                script = <<"#!/bin/bash\necho test">>,
                partition = <<"default">>,
                num_nodes = 2,
                num_cpus = 8,
                memory_mb = 4096,
                time_limit = 120,
                working_dir = <<"/tmp">>,
                env = #{path => <<"/usr/bin">>}
            },
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, Req),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Job Step Tests
%%%===================================================================

job_step_tests() ->
    [
        {"encode RESPONSE_JOB_STEP_CREATE", fun() ->
            Resp = #job_step_create_response{
                job_step_id = 0,
                error_code = 0,
                error_msg = <<>>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_JOB_STEP_CREATE, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_JOB_STEP_INFO", fun() ->
            Step = #job_step_info{
                job_id = 1234,
                step_id = 0,
                step_name = <<"batch">>,
                partition = <<"batch">>,
                user_id = 1000,
                state = 1,
                num_tasks = 4,
                num_cpus = 4,
                time_limit = 60,
                start_time = erlang:system_time(second),
                run_time = 30,
                nodes = <<"node01">>,
                node_cnt = 1,
                exit_code = 0
            },
            Resp = #job_step_info_response{
                last_update = erlang:system_time(second),
                step_count = 1,
                steps = [Step]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_JOB_STEP_INFO, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Info/Query Messages Tests
%%%===================================================================

info_query_tests() ->
    [
        {"encode RESPONSE_RESERVATION_INFO", fun() ->
            Resv = #reservation_info{
                name = <<"maint">>,
                accounts = <<"admin">>,
                node_cnt = 10,
                node_list = <<"node[01-10]">>,
                start_time = erlang:system_time(second),
                end_time = erlang:system_time(second) + 3600
            },
            Resp = #reservation_info_response{
                last_update = erlang:system_time(second),
                reservation_count = 1,
                reservations = [Resv]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_RESERVATION_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_LICENSE_INFO", fun() ->
            Lic = #license_info{
                name = <<"matlab">>,
                total = 100,
                in_use = 25,
                available = 75,
                reserved = 0,
                remote = 0
            },
            Resp = #license_info_response{
                last_update = erlang:system_time(second),
                license_count = 1,
                licenses = [Lic]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_LICENSE_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_TOPO_INFO", fun() ->
            Topo = #topo_info{
                level = 0,
                link_speed = 100000,
                name = <<"sw0">>,
                nodes = <<"node[01-10]">>,
                switches = <<>>
            },
            Resp = #topo_info_response{
                topo_count = 1,
                topos = [Topo]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_TOPO_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_FRONT_END_INFO", fun() ->
            FE = #front_end_info{
                name = <<"login01">>,
                node_state = 0,
                boot_time = erlang:system_time(second) - 86400,
                slurmd_start_time = erlang:system_time(second) - 86400,
                version = <<"22.05">>
            },
            Resp = #front_end_info_response{
                last_update = erlang:system_time(second),
                front_end_count = 1,
                front_ends = [FE]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_FRONT_END_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_BURST_BUFFER_INFO", fun() ->
            Pool = #burst_buffer_pool{
                granularity = 1024,
                name = <<"default">>,
                total_space = 1048576,
                unfree_space = 0,
                used_space = 0
            },
            BB = #burst_buffer_info{
                name = <<"cray_aries">>,
                pool_cnt = 1,
                pools = [Pool],
                total_space = 1048576,
                granularity = 1024
            },
            Resp = #burst_buffer_info_response{
                last_update = erlang:system_time(second),
                burst_buffer_count = 1,
                burst_buffers = [BB]
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_BURST_BUFFER_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_BUILD_INFO", fun() ->
            Resp = #build_info_response{
                cluster_name = <<"test_cluster">>,
                control_machine = <<"controller">>,
                slurmctld_host = <<"controller">>,
                slurmctld_port = 6817,
                slurmd_port = 6818,
                version = <<"22.05.0">>,
                auth_type = <<"auth/munge">>,
                select_type = <<"select/cons_tres">>,
                scheduler_type = <<"sched/backfill">>,
                priority_type = <<"priority/multifactor">>,
                accounting_storage_type = <<"accounting_storage/slurmdbd">>,
                job_comp_type = <<"jobcomp/none">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_BUILD_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_CONFIG_INFO", fun() ->
            Resp = #config_info_response{
                last_update = erlang:system_time(second),
                config = #{
                    cluster_name => <<"test">>,
                    max_job_count => 10000,
                    slurmctld_port => 6817
                }
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_CONFIG_INFO, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_STATS_INFO", fun() ->
            Resp = #stats_info_response{
                parts_packed = 0,
                req_time = erlang:system_time(second),
                req_time_start = erlang:system_time(second) - 3600,
                server_thread_count = 10,
                agent_queue_size = 0,
                jobs_submitted = 1000,
                jobs_started = 950,
                jobs_completed = 900,
                jobs_pending = 50,
                jobs_running = 50
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_STATS_INFO, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Reconfigure Tests
%%%===================================================================

reconfigure_tests() ->
    [
        {"encode reconfigure_response", fun() ->
            Resp = #reconfigure_response{
                return_code = 0,
                message = <<"Configuration reloaded">>,
                changed_keys = [max_job_count, slurmctld_timeout],
                version = 22050
            },
            {ok, Binary} = flurm_protocol_codec:encode_reconfigure_response(Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode reconfigure_response with non-record", fun() ->
            Result = flurm_protocol_codec:encode_reconfigure_response(not_a_record),
            ?assertMatch({ok, _}, Result)
        end}
    ].

%%%===================================================================
%%% Resource Allocation Tests
%%%===================================================================

resource_allocation_tests() ->
    [
        {"encode RESPONSE_RESOURCE_ALLOCATION with error", fun() ->
            Resp = #resource_allocation_response{
                job_id = 0,
                error_code = -1
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
            ?assert(is_binary(Binary))
        end},

        {"encode RESPONSE_RESOURCE_ALLOCATION success", fun() ->
            Resp = #resource_allocation_response{
                job_id = 5678,
                node_list = <<"node01">>,
                num_nodes = 1,
                partition = <<"batch">>,
                error_code = 0,
                job_submit_user_msg = <<"Allocated">>
            },
            {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% Message Type Helper Tests
%%%===================================================================

message_type_helper_tests() ->
    [
        {"message_type_name for REQUEST_PING", fun() ->
            ?assertEqual(request_ping, flurm_protocol_codec:message_type_name(?REQUEST_PING))
        end},

        {"message_type_name for REQUEST_JOB_INFO", fun() ->
            ?assertEqual(request_job_info, flurm_protocol_codec:message_type_name(?REQUEST_JOB_INFO))
        end},

        {"message_type_name for RESPONSE_SLURM_RC", fun() ->
            ?assertEqual(response_slurm_rc, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC))
        end},

        {"message_type_name for REQUEST_SUBMIT_BATCH_JOB", fun() ->
            ?assertEqual(request_submit_batch_job, flurm_protocol_codec:message_type_name(?REQUEST_SUBMIT_BATCH_JOB))
        end},

        {"message_type_name for unknown", fun() ->
            ?assertMatch({unknown, 99999}, flurm_protocol_codec:message_type_name(99999))
        end},

        {"is_request for requests", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_PING)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_CANCEL_JOB)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_SUBMIT_BATCH_JOB)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_NODE_INFO)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_PARTITION_INFO))
        end},

        {"is_request for responses returns false", fun() ->
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_JOB_INFO)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SUBMIT_BATCH_JOB))
        end},

        {"is_response for responses", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_INFO)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_SUBMIT_BATCH_JOB)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_NODE_INFO)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_PARTITION_INFO))
        end},

        {"is_response for requests returns false", fun() ->
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_PING)),
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_JOB_INFO))
        end}
    ].

%%%===================================================================
%%% Decode Error Tests
%%%===================================================================

decode_error_tests() ->
    [
        {"decode empty binary", fun() ->
            Result = flurm_protocol_codec:decode(<<>>),
            ?assertMatch({error, {incomplete_length_prefix, 0}}, Result)
        end},

        {"decode incomplete length prefix", fun() ->
            Result = flurm_protocol_codec:decode(<<1, 2, 3>>),
            ?assertMatch({error, {incomplete_length_prefix, 3}}, Result)
        end},

        {"decode with length < header size", fun() ->
            Result = flurm_protocol_codec:decode(<<5:32/big, 1, 2, 3, 4, 5>>),
            ?assertMatch({error, {invalid_message_length, 5}}, Result)
        end},

        {"decode incomplete message", fun() ->
            Result = flurm_protocol_codec:decode(<<50:32/big, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>),
            ?assertMatch({error, {incomplete_message, 50, _}}, Result)
        end}
    ].

%%%===================================================================
%%% Protocol Resource Extraction Tests
%%%===================================================================

protocol_resource_tests() ->
    [
        {"extract_resources_from_protocol with small binary", fun() ->
            Binary = <<1, 2, 3, 4, 5>>,
            {MinNodes, MinCpus} = flurm_protocol_codec:extract_resources_from_protocol(Binary),
            ?assertEqual(0, MinNodes),
            ?assertEqual(0, MinCpus)
        end},

        {"extract_full_job_desc with small binary", fun() ->
            Binary = <<1, 2, 3, 4, 5>>,
            Result = flurm_protocol_codec:extract_full_job_desc(Binary),
            ?assertMatch({error, binary_too_small}, Result)
        end},

        {"extract_full_job_desc with valid header", fun() ->
            %% Build a binary with job_id, user_id, group_id at expected locations
            Binary = <<12345:32/big, 1000:32/big, 1000:32/big, 0:800/big>>,  % Pad to 100 bytes
            Result = flurm_protocol_codec:extract_full_job_desc(Binary),
            ?assertMatch({ok, _}, Result),
            {ok, JobDesc} = Result,
            ?assertEqual(1000, maps:get(user_id, JobDesc)),
            ?assertEqual(1000, maps:get(group_id, JobDesc))
        end}
    ].

%%%===================================================================
%%% Decode Body Tests
%%%===================================================================

decode_body_tests() ->
    [
        {"decode_body for REQUEST_PING", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),
            ?assertMatch(#ping_request{}, Body)
        end},

        {"decode_body for RESPONSE_SLURM_RC", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<0:32/big-signed>>),
            ?assertEqual(0, Body#slurm_rc_response.return_code)
        end},

        {"decode_body for REQUEST_JOB_INFO", fun() ->
            Binary = <<1:32/big, 12345:32/big, 1000:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, Binary),
            ?assertEqual(1, Body#job_info_request.show_flags),
            ?assertEqual(12345, Body#job_info_request.job_id),
            ?assertEqual(1000, Body#job_info_request.user_id)
        end},

        {"decode_body for REQUEST_CANCEL_JOB with data", fun() ->
            Body = <<12345:32/big, 0:32/big, 9:32/big, 0:32/big, 6:32/big, "12345", 0>>,
            {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, Body),
            ?assertEqual(12345, Req#cancel_job_request.job_id),
            ?assertEqual(0, Req#cancel_job_request.step_id),
            ?assertEqual(9, Req#cancel_job_request.signal)
        end},

        {"decode_body for REQUEST_CANCEL_JOB empty", fun() ->
            {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, <<>>),
            ?assertMatch(#cancel_job_request{}, Req)
        end},

        {"decode_body for unknown message type", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(99999, <<"raw data">>),
            ?assertEqual(<<"raw data">>, Body)
        end}
    ].

%%%===================================================================
%%% Encode Body Tests
%%%===================================================================

encode_body_tests() ->
    [
        {"encode_body for REQUEST_PING", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_PING, #ping_request{}),
            ?assertEqual(<<>>, Binary)
        end},

        {"encode_body for RESPONSE_SLURM_RC", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
            <<ReturnCode:32/big-signed>> = Binary,
            ?assertEqual(42, ReturnCode)
        end},

        {"encode_body for REQUEST_JOB_INFO", fun() ->
            Req = #job_info_request{show_flags = 1, job_id = 123, user_id = 1000},
            {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_JOB_INFO, Req),
            ?assert(is_binary(Binary))
        end},

        {"encode_body for REQUEST_NODE_REGISTRATION_STATUS", fun() ->
            Req = #node_registration_request{status_only = true},
            {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
            ?assert(is_binary(Binary))
        end}
    ].

%%%===================================================================
%%% With Extra Data Tests
%%%===================================================================

with_extra_tests() ->
    [
        {"encode_with_extra REQUEST_PING", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
            ?assert(is_binary(Binary)),
            ?assert(byte_size(Binary) >= 14)
        end},

        {"encode_with_extra with custom hostname", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}, <<"myhost">>),
            ?assert(is_binary(Binary))
        end},

        {"decode_with_extra roundtrip", fun() ->
            {ok, Encoded} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
            {ok, Msg, _Extra, <<>>} = flurm_protocol_codec:decode_with_extra(Encoded),
            ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type)
        end}
    ].

%%%===================================================================
%%% Response Encoding/Decoding Tests
%%%===================================================================

response_tests() ->
    [
        {"encode_response RESPONSE_SLURM_RC", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
            ?assert(is_binary(Binary))
        end},

        {"decode_response roundtrip", fun() ->
            {ok, Encoded} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode_response(Encoded),
            ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertEqual(42, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end}
    ].

%%%===================================================================
%%% Edge Cases and Error Handling
%%%===================================================================

edge_case_tests() ->
    [
        {"encode with default record values", fun() ->
            {ok, _} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{}),
            {ok, _} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, #job_info_request{}),
            {ok, _} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, #cancel_job_request{}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, #batch_job_response{}),
            {ok, _} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, #node_registration_request{})
        end},

        {"encode responses with empty lists", fun() ->
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_JOB_INFO, #job_info_response{jobs = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_NODE_INFO, #node_info_response{nodes = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_PARTITION_INFO, #partition_info_response{partitions = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_JOB_STEP_INFO, #job_step_info_response{steps = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_RESERVATION_INFO, #reservation_info_response{reservations = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_LICENSE_INFO, #license_info_response{licenses = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_TOPO_INFO, #topo_info_response{topos = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_FRONT_END_INFO, #front_end_info_response{front_ends = []}),
            {ok, _} = flurm_protocol_codec:encode(?RESPONSE_BURST_BUFFER_INFO, #burst_buffer_info_response{burst_buffers = []})
        end},

        {"large job_id values", fun() ->
            Req = #cancel_job_request{job_id = 16#FFFFFFFF - 1},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(16#FFFFFFFF - 1, (Msg#slurm_msg.body)#cancel_job_request.job_id)
        end},

        {"negative return codes", fun() ->
            Resp = #slurm_rc_response{return_code = -2147483648},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(-2147483648, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end},

        {"encode binary body passes through", fun() ->
            {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, <<"raw binary">>),
            ?assert(is_binary(Binary))
        end}
    ].
