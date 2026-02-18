%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_codec_step module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_codec_step_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_codec_step_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode REQUEST_JOB_STEP_CREATE tests
      {"decode REQUEST_JOB_STEP_CREATE full binary",
       fun decode_job_step_create_full/0},
      {"decode REQUEST_JOB_STEP_CREATE minimal binary",
       fun decode_job_step_create_minimal/0},
      {"decode REQUEST_JOB_STEP_CREATE just job_id",
       fun decode_job_step_create_job_id_only/0},
      {"decode REQUEST_JOB_STEP_CREATE invalid",
       fun decode_job_step_create_invalid/0},
      {"decode REQUEST_JOB_STEP_CREATE with name",
       fun decode_job_step_create_with_name/0},

      %% Decode REQUEST_JOB_STEP_INFO tests
      {"decode REQUEST_JOB_STEP_INFO full",
       fun decode_job_step_info_full/0},
      {"decode REQUEST_JOB_STEP_INFO two fields",
       fun decode_job_step_info_two_fields/0},
      {"decode REQUEST_JOB_STEP_INFO one field",
       fun decode_job_step_info_one_field/0},
      {"decode REQUEST_JOB_STEP_INFO empty",
       fun decode_job_step_info_empty/0},
      {"decode REQUEST_JOB_STEP_INFO invalid",
       fun decode_job_step_info_invalid/0},

      %% Decode REQUEST_COMPLETE_PROLOG tests
      {"decode REQUEST_COMPLETE_PROLOG full",
       fun decode_complete_prolog_full/0},
      {"decode REQUEST_COMPLETE_PROLOG two fields",
       fun decode_complete_prolog_two_fields/0},
      {"decode REQUEST_COMPLETE_PROLOG one field",
       fun decode_complete_prolog_one_field/0},
      {"decode REQUEST_COMPLETE_PROLOG empty",
       fun decode_complete_prolog_empty/0},
      {"decode REQUEST_COMPLETE_PROLOG invalid",
       fun decode_complete_prolog_invalid/0},

      %% Decode MESSAGE_EPILOG_COMPLETE tests
      {"decode MESSAGE_EPILOG_COMPLETE full",
       fun decode_epilog_complete_full/0},
      {"decode MESSAGE_EPILOG_COMPLETE two fields",
       fun decode_epilog_complete_two_fields/0},
      {"decode MESSAGE_EPILOG_COMPLETE one field",
       fun decode_epilog_complete_one_field/0},
      {"decode MESSAGE_EPILOG_COMPLETE empty",
       fun decode_epilog_complete_empty/0},
      {"decode MESSAGE_EPILOG_COMPLETE invalid",
       fun decode_epilog_complete_invalid/0},

      %% Decode MESSAGE_TASK_EXIT tests
      {"decode MESSAGE_TASK_EXIT full",
       fun decode_task_exit_full/0},
      {"decode MESSAGE_TASK_EXIT minimal",
       fun decode_task_exit_minimal/0},

      %% Decode REQUEST_LAUNCH_TASKS tests
      {"decode REQUEST_LAUNCH_TASKS minimal",
       fun decode_launch_tasks_minimal/0},
      {"decode REQUEST_LAUNCH_TASKS with env",
       fun decode_launch_tasks_with_env/0},

      %% Decode unsupported tests
      {"decode unsupported message type",
       fun decode_unsupported/0},

      %% Encode RESPONSE_JOB_STEP_CREATE tests
      {"encode RESPONSE_JOB_STEP_CREATE full record",
       fun encode_job_step_create_response_full/0},
      {"encode RESPONSE_JOB_STEP_CREATE minimal",
       fun encode_job_step_create_response_minimal/0},
      {"encode RESPONSE_JOB_STEP_CREATE non-record",
       fun encode_job_step_create_response_non_record/0},

      %% Encode RESPONSE_JOB_STEP_INFO tests
      {"encode RESPONSE_JOB_STEP_INFO full",
       fun encode_job_step_info_response_full/0},
      {"encode RESPONSE_JOB_STEP_INFO empty steps",
       fun encode_job_step_info_response_empty/0},
      {"encode RESPONSE_JOB_STEP_INFO non-record",
       fun encode_job_step_info_response_non_record/0},
      {"encode single job step info",
       fun encode_single_job_step_info/0},
      {"encode single job step info non-record",
       fun encode_single_job_step_info_non_record/0},

      %% Encode RESPONSE_LAUNCH_TASKS tests
      {"encode RESPONSE_LAUNCH_TASKS full record",
       fun encode_launch_tasks_response_full/0},
      {"encode RESPONSE_LAUNCH_TASKS map",
       fun encode_launch_tasks_response_map/0},
      {"encode RESPONSE_LAUNCH_TASKS non-record",
       fun encode_launch_tasks_response_non_record/0},

      %% Encode RESPONSE_REATTACH_TASKS tests
      {"encode RESPONSE_REATTACH_TASKS full record",
       fun encode_reattach_tasks_response_full/0},
      {"encode RESPONSE_REATTACH_TASKS map",
       fun encode_reattach_tasks_response_map/0},
      {"encode RESPONSE_REATTACH_TASKS non-record",
       fun encode_reattach_tasks_response_non_record/0},

      %% Encode MESSAGE_TASK_EXIT tests
      {"encode MESSAGE_TASK_EXIT map",
       fun encode_task_exit_msg_map/0},
      {"encode MESSAGE_TASK_EXIT non-map",
       fun encode_task_exit_msg_non_map/0},

      %% Encode unsupported tests
      {"encode unsupported message type",
       fun encode_unsupported/0},

      %% Step layout tests
      {"encode step layout",
       fun encode_step_layout_test/0},

      %% Edge cases
      {"normalize step id NO_VAL",
       fun normalize_step_id_no_val/0},
      {"normalize step id regular",
       fun normalize_step_id_regular/0},

      %% Roundtrip tests
      {"roundtrip job step info",
       fun roundtrip_job_step_info/0},

      %% Large data tests
      {"encode large task list",
       fun encode_large_task_list/0},
      {"encode large environment",
       fun encode_large_environment/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Decode REQUEST_JOB_STEP_CREATE Tests
%%%===================================================================

decode_job_step_create_full() ->
    %% Full binary: JobId, StepId, UserId, MinNodes, MaxNodes, NumTasks,
    %%              CpusPerTask, TimeLimit, Flags, Immediate, Name
    Name = <<"test_step">>,
    NameLen = byte_size(Name),
    Binary = <<123:32/big, 0:32/big, 1000:32/big, 1:32/big, 10:32/big,
               8:32/big, 4:32/big, 3600:32/big, 0:32/big, 0:32/big,
               NameLen:32/big, Name/binary>>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertMatch({ok, #job_step_create_request{job_id = 123, user_id = 1000}}, Result).

decode_job_step_create_minimal() ->
    %% Just JobId and StepId with rest
    Binary = <<456:32/big, 1:32/big, "extra">>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertMatch({ok, #job_step_create_request{job_id = 456}}, Result).

decode_job_step_create_job_id_only() ->
    Binary = <<789:32/big>>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertMatch({ok, #job_step_create_request{job_id = 789}}, Result).

decode_job_step_create_invalid() ->
    Binary = <<1, 2, 3>>,  % Less than 4 bytes
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertMatch({error, _}, Result).

decode_job_step_create_with_name() ->
    Name = <<"my_step_name">>,
    NameLen = byte_size(Name),
    Binary = <<100:32/big, 5:32/big, 0:32/big, 1:32/big, 1:32/big,
               1:32/big, 1:32/big, 0:32/big, 0:32/big, 0:32/big,
               NameLen:32/big, Name/binary>>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertMatch({ok, #job_step_create_request{name = <<"my_step_name">>}}, Result).

%%%===================================================================
%%% Decode REQUEST_JOB_STEP_INFO Tests
%%%===================================================================

decode_job_step_info_full() ->
    Binary = <<16#0001:32/big, 123:32/big, 0:32/big, "extra">>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertMatch({ok, #job_step_info_request{show_flags = 1, job_id = 123, step_id = 0}}, Result).

decode_job_step_info_two_fields() ->
    Binary = <<0:32/big, 456:32/big>>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertMatch({ok, #job_step_info_request{show_flags = 0, job_id = 456}}, Result).

decode_job_step_info_one_field() ->
    Binary = <<255:32/big>>,
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertMatch({ok, #job_step_info_request{show_flags = 255}}, Result).

decode_job_step_info_empty() ->
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, <<>>),
    ?assertMatch({ok, #job_step_info_request{}}, Result).

decode_job_step_info_invalid() ->
    Binary = <<1, 2, 3>>,  % Invalid length
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_COMPLETE_PROLOG Tests
%%%===================================================================

decode_complete_prolog_full() ->
    NodeName = <<"node001">>,
    NameLen = byte_size(NodeName),
    Binary = <<100:32/big, 0:32/signed-big, NameLen:32/big, NodeName/binary>>,
    Result = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertMatch({ok, #complete_prolog_request{job_id = 100, prolog_rc = 0, node_name = <<"node001">>}}, Result).

decode_complete_prolog_two_fields() ->
    Binary = <<200:32/big, (-1):32/signed-big>>,
    Result = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertMatch({ok, #complete_prolog_request{job_id = 200, prolog_rc = -1}}, Result).

decode_complete_prolog_one_field() ->
    Binary = <<300:32/big>>,
    Result = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertMatch({ok, #complete_prolog_request{job_id = 300}}, Result).

decode_complete_prolog_empty() ->
    Result = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, <<>>),
    ?assertMatch({ok, #complete_prolog_request{}}, Result).

decode_complete_prolog_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode MESSAGE_EPILOG_COMPLETE Tests
%%%===================================================================

decode_epilog_complete_full() ->
    NodeName = <<"compute01">>,
    NameLen = byte_size(NodeName),
    Binary = <<500:32/big, 0:32/signed-big, NameLen:32/big, NodeName/binary>>,
    Result = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
    ?assertMatch({ok, #epilog_complete_msg{job_id = 500, epilog_rc = 0, node_name = <<"compute01">>}}, Result).

decode_epilog_complete_two_fields() ->
    Binary = <<600:32/big, 1:32/signed-big>>,
    Result = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
    ?assertMatch({ok, #epilog_complete_msg{job_id = 600, epilog_rc = 1}}, Result).

decode_epilog_complete_one_field() ->
    Binary = <<700:32/big>>,
    Result = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
    ?assertMatch({ok, #epilog_complete_msg{job_id = 700}}, Result).

decode_epilog_complete_empty() ->
    Result = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, <<>>),
    ?assertMatch({ok, #epilog_complete_msg{}}, Result).

decode_epilog_complete_invalid() ->
    Binary = <<1, 2>>,
    Result = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode MESSAGE_TASK_EXIT Tests
%%%===================================================================

decode_task_exit_full() ->
    %% return_code, num_tasks, array_count, task_ids[], job_id, step_id, step_het_comp
    Binary = <<0:32/signed-big, 2:32/big, 2:32/big, 0:32/big, 1:32/big,
               123:32/big, 0:32/big, 16#FFFFFFFE:32/big>>,
    Result = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
    ?assertMatch({ok, #task_exit_msg{job_id = 123, task_ids = [0, 1], return_code = 0}}, Result).

decode_task_exit_minimal() ->
    Result = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, <<>>),
    ?assertMatch({ok, #task_exit_msg{}}, Result).

%%%===================================================================
%%% Decode REQUEST_LAUNCH_TASKS Tests
%%%===================================================================

decode_launch_tasks_minimal() ->
    %% Minimal launch tasks binary - needs at least step_id, uid, gid, user_name
    Binary = <<123:32/big, 0:32/big, 16#FFFFFFFE:32/big,  % job_id, step_id, step_het_comp
               1000:32/big, 1000:32/big,  % uid, gid
               4:32/big, "root",  % user_name
               0:32/big,  % ngids
               16#FFFFFFFE:32/big, 0:32/big, 16#FFFFFFFE:32/big,  % het_job fields
               1:32/big,  % ntasks
               16#FFFFFFFE:32/big,  % het_job_ntasks
               0:32/big, 0:32/big, 0:32/big,  % more het fields
               16#FFFFFFFF:32/big,  % NULL het_job_node_list
               0:32/big,  % mpi_plugin_id
               1:32/big, 0:16/big, 0:16/big, 0:16/big, 0:16/big,  % task counts
               0:64/big, 0:64/big,  % mem limits
               1:32/big,  % nnodes
               1:16/big,  % cpus_per_task
               16#FFFFFFFF:32/big,  % tres_per_task (NULL)
               0:16/big, 1:32/big, 0:16/big, 0:16/big, 0:16/big  % more fields
             >>,
    Result = flurm_codec_step:decode_body(?REQUEST_LAUNCH_TASKS, Binary),
    ?assertMatch({ok, #launch_tasks_request{job_id = 123}}, Result).

decode_launch_tasks_with_env() ->
    %% Create a binary with SLURM_ env marker for find_env_vars_position
    Padding = binary:copy(<<0>>, 100),
    EnvMarker = <<"SLURM_JOB_ID=123">>,
    EnvLen = byte_size(EnvMarker),
    %% Format: count(32), strlen(32), string
    EnvSection = <<1:32/big, EnvLen:32/big, EnvMarker/binary>>,
    Binary = <<123:32/big, 0:32/big, 16#FFFFFFFE:32/big,
               1000:32/big, 1000:32/big,
               4:32/big, "root",
               0:32/big,
               Padding/binary,
               EnvSection/binary>>,
    %% This will likely return the minimal fallback but tests the env parsing path
    Result = flurm_codec_step:decode_body(?REQUEST_LAUNCH_TASKS, Binary),
    ?assertMatch({ok, #launch_tasks_request{job_id = 123}}, Result).

%%%===================================================================
%%% Decode Unsupported Tests
%%%===================================================================

decode_unsupported() ->
    Result = flurm_codec_step:decode_body(99999, <<1, 2, 3>>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Encode RESPONSE_JOB_STEP_CREATE Tests
%%%===================================================================

encode_job_step_create_response_full() ->
    Resp = #job_step_create_response{
        job_step_id = 5,
        job_id = 123,
        user_id = 1000,
        group_id = 1000,
        user_name = <<"testuser">>,
        node_list = <<"node[001-010]">>,
        num_tasks = 8,
        error_code = 0,
        error_msg = <<>>
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

encode_job_step_create_response_minimal() ->
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 100,
        user_id = 0,
        group_id = 0,
        user_name = <<>>,
        node_list = <<>>,
        num_tasks = 1
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)).

encode_job_step_create_response_non_record() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_JOB_STEP_INFO Tests
%%%===================================================================

encode_job_step_info_response_full() ->
    Steps = [
        #job_step_info{
            job_id = 123,
            step_id = 0,
            step_name = <<"batch">>,
            partition = <<"default">>,
            user_id = 1000,
            state = 1,
            num_tasks = 4,
            num_cpus = 8,
            time_limit = 3600,
            start_time = 1234567890,
            run_time = 100,
            nodes = <<"node[001-004]">>,
            node_cnt = 4,
            tres_alloc_str = <<"cpu=8,mem=16G">>,
            exit_code = 0
        },
        #job_step_info{
            job_id = 123,
            step_id = 1,
            step_name = <<"interactive">>,
            partition = <<"default">>,
            user_id = 1000,
            state = 1,
            num_tasks = 1,
            num_cpus = 2
        }
    ],
    Resp = #job_step_info_response{
        last_update = 1234567890,
        step_count = 2,
        steps = Steps
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

encode_job_step_info_response_empty() ->
    Resp = #job_step_info_response{
        last_update = 1234567890,
        step_count = 0,
        steps = []
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_job_step_info_response_non_record() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, not_a_record),
    ?assert(is_binary(Binary)).

encode_single_job_step_info() ->
    Step = #job_step_info{
        job_id = 456,
        step_id = 2,
        step_name = <<"test">>,
        partition = <<"gpu">>,
        user_id = 1001,
        state = 0,
        num_tasks = 16,
        num_cpus = 64,
        time_limit = 7200,
        start_time = 1234567900,
        run_time = 500,
        nodes = <<"gpu[01-04]">>,
        node_cnt = 4,
        tres_alloc_str = <<"cpu=64,gres/gpu=4">>,
        exit_code = 0
    },
    Resp = #job_step_info_response{
        last_update = 1234567890,
        step_count = 1,
        steps = [Step]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_job_step_info_non_record() ->
    Resp = #job_step_info_response{
        last_update = 1234567890,
        step_count = 1,
        steps = [not_a_record]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_LAUNCH_TASKS Tests
%%%===================================================================

encode_launch_tasks_response_full() ->
    Resp = #launch_tasks_response{
        job_id = 123,
        step_id = 0,
        step_het_comp = 16#FFFFFFFE,
        return_code = 0,
        node_name = <<"compute01">>,
        count_of_pids = 4,
        local_pids = [1001, 1002, 1003, 1004],
        gtids = [0, 1, 2, 3]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 40).

encode_launch_tasks_response_map() ->
    Map = #{
        return_code => 0,
        job_id => 456,
        step_id => 1,
        step_het_comp => 0,
        node_name => <<"node01">>,
        local_pids => [2001, 2002],
        gtids => [0, 1]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Map),
    ?assert(is_binary(Binary)).

encode_launch_tasks_response_non_record() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_REATTACH_TASKS Tests
%%%===================================================================

encode_reattach_tasks_response_full() ->
    Resp = #reattach_tasks_response{
        return_code = 0,
        node_name = <<"compute01">>,
        count_of_pids = 2,
        local_pids = [1001, 1002],
        gtids = [0, 1],
        executable_names = [<<"/bin/bash">>, <<"/bin/bash">>]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 30).

encode_reattach_tasks_response_map() ->
    Map = #{
        return_code => 0,
        node_name => <<"node01">>,
        local_pids => [3001],
        gtids => [0],
        executable_names => [<<"/usr/bin/python">>]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Map),
    ?assert(is_binary(Binary)).

encode_reattach_tasks_response_non_record() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MESSAGE_TASK_EXIT Tests
%%%===================================================================

encode_task_exit_msg_map() ->
    Map = #{
        job_id => 123,
        step_id => 0,
        step_het_comp => 16#FFFFFFFE,
        task_ids => [0, 1, 2, 3],
        return_codes => [0, 0, 1, 0]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT, Map),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 20).

encode_task_exit_msg_non_map() ->
    {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT, not_a_map),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode Unsupported Tests
%%%===================================================================

encode_unsupported() ->
    Result = flurm_codec_step:encode_body(99999, some_body),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Step Layout Tests
%%%===================================================================

encode_step_layout_test() ->
    %% Test via job step create response
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 100,
        user_id = 1000,
        group_id = 1000,
        user_name = <<"user">>,
        node_list = <<"node[01-10]">>,
        num_tasks = 10
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

%%%===================================================================
%%% Normalize Step ID Tests
%%%===================================================================

normalize_step_id_no_val() ->
    %% Test by decoding with NO_VAL step ID
    Binary = <<0:32/big, 16#FFFFFFFE:32/big>>,
    {ok, Result} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertEqual(0, Result#job_step_info_request.step_id).

normalize_step_id_regular() ->
    Binary = <<0:32/big, 5:32/big>>,
    {ok, Result} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertEqual(5, Result#job_step_info_request.step_id).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_job_step_info() ->
    %% Encode a step info response and verify structure
    Steps = [
        #job_step_info{
            job_id = 999,
            step_id = 0,
            step_name = <<"roundtrip_test">>,
            partition = <<"test">>,
            user_id = 5000,
            state = 1,
            num_tasks = 1,
            num_cpus = 1
        }
    ],
    Resp = #job_step_info_response{
        last_update = erlang:system_time(second),
        step_count = 1,
        steps = Steps
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)),
    %% Verify last_update is at the start (packed as time)
    <<_:64/big, StepCount:32/big, _/binary>> = Binary,
    ?assertEqual(1, StepCount).

%%%===================================================================
%%% Large Data Tests
%%%===================================================================

encode_large_task_list() ->
    %% Create a launch tasks response with many tasks
    NumTasks = 100,
    Pids = lists:seq(1001, 1000 + NumTasks),
    Gtids = lists:seq(0, NumTasks - 1),
    Resp = #launch_tasks_response{
        job_id = 12345,
        step_id = 0,
        step_het_comp = 16#FFFFFFFE,
        return_code = 0,
        node_name = <<"bignode">>,
        count_of_pids = NumTasks,
        local_pids = Pids,
        gtids = Gtids
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 800).  % Should be substantial

encode_large_environment() ->
    %% Create a step info with many steps
    Steps = [
        #job_step_info{
            job_id = 1000 + N,
            step_id = N,
            step_name = list_to_binary("step" ++ integer_to_list(N)),
            partition = <<"default">>,
            user_id = 1000,
            num_tasks = N + 1,
            num_cpus = (N + 1) * 2
        }
    || N <- lists:seq(0, 50)],
    Resp = #job_step_info_response{
        last_update = erlang:system_time(second),
        step_count = 51,
        steps = Steps
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 1000).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test with unicode in names
unicode_step_name_test_() ->
    {"encode step info with unicode name",
     fun() ->
         Step = #job_step_info{
             job_id = 100,
             step_id = 0,
             step_name = <<"step_", 195, 169, "test">>,  % e-acute
             partition = <<"default">>
         },
         Resp = #job_step_info_response{
             last_update = 0,
             step_count = 1,
             steps = [Step]
         },
         {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test with empty lists
empty_pid_list_test_() ->
    {"encode launch tasks response with empty pid list",
     fun() ->
         Resp = #launch_tasks_response{
             job_id = 100,
             step_id = 0,
             step_het_comp = 0,
             return_code = 1,  % Error case
             node_name = <<"node01">>,
             count_of_pids = 0,
             local_pids = [],
             gtids = []
         },
         {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test with negative return codes
negative_return_code_test_() ->
    {"encode task exit with negative return code",
     fun() ->
         Map = #{
             job_id => 100,
             step_id => 0,
             task_ids => [0],
             return_codes => [-1]
         },
         {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT, Map),
         ?assert(is_binary(Binary))
     end}.

%% Test prolog with negative return code
prolog_negative_rc_test_() ->
    {"decode complete prolog with negative rc",
     fun() ->
         Binary = <<100:32/big, (-127):32/signed-big>>,
         {ok, Result} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
         ?assertEqual(-127, Result#complete_prolog_request.prolog_rc)
     end}.

%% Test maximum values
max_values_test_() ->
    {"encode step info with maximum values",
     fun() ->
         Step = #job_step_info{
             job_id = 16#FFFFFFFF,
             step_id = 16#FFFFFFFF,
             step_name = <<"max">>,
             user_id = 16#FFFFFFFF,
             num_tasks = 16#FFFFFFFF,
             num_cpus = 16#FFFFFFFF,
             time_limit = 16#FFFFFFFF
         },
         Resp = #job_step_info_response{
             last_update = 16#FFFFFFFFFFFFFFFF,
             step_count = 1,
             steps = [Step]
         },
         {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test all job step states
step_state_test_() ->
    States = [0, 1, 2, 3, 4, 5, 6, 7],  % Various step states
    [
        {lists:flatten(io_lib:format("encode step with state ~p", [State])),
         fun() ->
             Step = #job_step_info{
                 job_id = 100,
                 step_id = 0,
                 state = State
             },
             Resp = #job_step_info_response{
                 last_update = 0,
                 step_count = 1,
                 steps = [Step]
             },
             {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
             ?assert(is_binary(Binary))
         end}
    || State <- States].
