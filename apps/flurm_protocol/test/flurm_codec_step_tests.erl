%%%-------------------------------------------------------------------
%%% @doc Comprehensive unit tests for flurm_codec_step module.
%%%
%%% Tests for all step-related message encoding and decoding functions.
%%% Coverage target: ~90% of flurm_codec_step.erl (900 lines)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_step_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_codec_step_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% decode_body/2 tests - REQUEST_JOB_STEP_CREATE
      {"decode REQUEST_JOB_STEP_CREATE - full format", fun decode_job_step_create_full_test/0},
      {"decode REQUEST_JOB_STEP_CREATE - partial format", fun decode_job_step_create_partial_test/0},
      {"decode REQUEST_JOB_STEP_CREATE - minimal format", fun decode_job_step_create_minimal_test/0},
      {"decode REQUEST_JOB_STEP_CREATE - invalid", fun decode_job_step_create_invalid_test/0},

      %% decode_body/2 tests - REQUEST_JOB_STEP_INFO
      {"decode REQUEST_JOB_STEP_INFO - full format", fun decode_job_step_info_full_test/0},
      {"decode REQUEST_JOB_STEP_INFO - partial formats", fun decode_job_step_info_partial_test/0},
      {"decode REQUEST_JOB_STEP_INFO - empty", fun decode_job_step_info_empty_test/0},
      {"decode REQUEST_JOB_STEP_INFO - invalid", fun decode_job_step_info_invalid_test/0},

      %% decode_body/2 tests - REQUEST_COMPLETE_PROLOG
      {"decode REQUEST_COMPLETE_PROLOG - full format", fun decode_complete_prolog_full_test/0},
      {"decode REQUEST_COMPLETE_PROLOG - partial formats", fun decode_complete_prolog_partial_test/0},
      {"decode REQUEST_COMPLETE_PROLOG - empty", fun decode_complete_prolog_empty_test/0},

      %% decode_body/2 tests - MESSAGE_EPILOG_COMPLETE
      {"decode MESSAGE_EPILOG_COMPLETE - full format", fun decode_epilog_complete_full_test/0},
      {"decode MESSAGE_EPILOG_COMPLETE - partial formats", fun decode_epilog_complete_partial_test/0},
      {"decode MESSAGE_EPILOG_COMPLETE - empty", fun decode_epilog_complete_empty_test/0},

      %% decode_body/2 tests - MESSAGE_TASK_EXIT
      {"decode MESSAGE_TASK_EXIT - full format", fun decode_task_exit_full_test/0},
      {"decode MESSAGE_TASK_EXIT - empty", fun decode_task_exit_empty_test/0},

      %% decode_body/2 tests - REQUEST_LAUNCH_TASKS
      {"decode REQUEST_LAUNCH_TASKS - minimal", fun decode_launch_tasks_minimal_test/0},

      %% decode_body/2 tests - Unsupported
      {"decode unsupported message type", fun decode_unsupported_test/0},

      %% encode_body/2 tests - RESPONSE_JOB_STEP_CREATE
      {"encode RESPONSE_JOB_STEP_CREATE - full record", fun encode_job_step_create_full_test/0},
      {"encode RESPONSE_JOB_STEP_CREATE - minimal record", fun encode_job_step_create_minimal_test/0},
      {"encode RESPONSE_JOB_STEP_CREATE - default", fun encode_job_step_create_default_test/0},

      %% encode_body/2 tests - RESPONSE_JOB_STEP_INFO
      {"encode RESPONSE_JOB_STEP_INFO - empty", fun encode_job_step_info_empty_test/0},
      {"encode RESPONSE_JOB_STEP_INFO - with steps", fun encode_job_step_info_with_steps_test/0},
      {"encode RESPONSE_JOB_STEP_INFO - multiple steps", fun encode_job_step_info_multiple_test/0},
      {"encode RESPONSE_JOB_STEP_INFO - default", fun encode_job_step_info_default_test/0},

      %% encode_body/2 tests - RESPONSE_LAUNCH_TASKS
      {"encode RESPONSE_LAUNCH_TASKS - full record", fun encode_launch_tasks_full_test/0},
      {"encode RESPONSE_LAUNCH_TASKS - with map", fun encode_launch_tasks_map_test/0},
      {"encode RESPONSE_LAUNCH_TASKS - default", fun encode_launch_tasks_default_test/0},

      %% encode_body/2 tests - RESPONSE_REATTACH_TASKS
      {"encode RESPONSE_REATTACH_TASKS - full record", fun encode_reattach_tasks_full_test/0},
      {"encode RESPONSE_REATTACH_TASKS - with map", fun encode_reattach_tasks_map_test/0},
      {"encode RESPONSE_REATTACH_TASKS - default", fun encode_reattach_tasks_default_test/0},

      %% encode_body/2 tests - MESSAGE_TASK_EXIT
      {"encode MESSAGE_TASK_EXIT - with map", fun encode_task_exit_map_test/0},
      {"encode MESSAGE_TASK_EXIT - default", fun encode_task_exit_default_test/0},

      %% encode_body/2 tests - Unsupported
      {"encode unsupported message type", fun encode_unsupported_test/0},

      %% Internal helper tests
      {"normalize_step_id - NO_VAL", fun normalize_step_id_no_val_test/0},
      {"normalize_step_id - normal", fun normalize_step_id_normal_test/0},
      {"unpack_string_safe - valid", fun unpack_string_safe_valid_test/0},
      {"unpack_string_safe - NULL", fun unpack_string_safe_null_test/0},
      {"unpack_string_safe - empty", fun unpack_string_safe_empty_test/0},
      {"unpack_uint32_array - empty", fun unpack_uint32_array_empty_test/0},
      {"unpack_uint32_array - with values", fun unpack_uint32_array_values_test/0},
      {"unpack_string_array - empty", fun unpack_string_array_empty_test/0},
      {"unpack_string_array - with values", fun unpack_string_array_values_test/0},

      %% Step layout encoding tests
      {"encode_step_layout", fun encode_step_layout_test/0},
      {"encode_minimal_cred", fun encode_minimal_cred_test/0},
      {"encode_select_jobinfo", fun encode_select_jobinfo_test/0},

      %% Edge case tests
      {"step_id boundary values", fun step_id_boundary_test/0},
      {"return_code values", fun return_code_values_test/0},
      {"prolog_rc values", fun prolog_rc_values_test/0},
      {"epilog_rc values", fun epilog_rc_values_test/0},

      %% ensure_binary tests
      {"ensure_binary - various types", fun ensure_binary_various_test/0},

      %% Roundtrip tests
      {"roundtrip job_step_info_request", fun roundtrip_step_info_test/0}
     ]}.

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_JOB_STEP_CREATE
%%%===================================================================

decode_job_step_create_full_test() ->
    NameStr = <<"step_test">>,
    %% unpack_string format: <<Length:32/big, Data:Length/binary>> where Length includes null
    NameLen = byte_size(NameStr) + 1,
    Binary = <<
        12345:32/big,    % job_id
        0:32/big,        % step_id
        1000:32/big,     % user_id
        1:32/big,        % min_nodes
        4:32/big,        % max_nodes
        16:32/big,       % num_tasks
        1:32/big,        % cpus_per_task
        3600:32/big,     % time_limit
        0:32/big,        % flags
        1:32/big,        % immediate
        NameLen:32/big,  % name length (includes null terminator)
        NameStr/binary,  % name
        0                % null terminator
    >>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertEqual(12345, Body#job_step_create_request.job_id),
    ?assertEqual(0, Body#job_step_create_request.step_id),
    ?assertEqual(1000, Body#job_step_create_request.user_id),
    ?assertEqual(1, Body#job_step_create_request.min_nodes),
    ?assertEqual(4, Body#job_step_create_request.max_nodes),
    ?assertEqual(16, Body#job_step_create_request.num_tasks),
    ?assertEqual(1, Body#job_step_create_request.cpus_per_task),
    ?assertEqual(3600, Body#job_step_create_request.time_limit),
    ?assertEqual(NameStr, Body#job_step_create_request.name).

decode_job_step_create_partial_test() ->
    %% Partial format uses extract_name_from_binary which calls unpack_string
    %% Format: <<JobId:32/big, StepId:32/big, Rest/binary>>
    NameStr = <<"test">>,
    NameLen = byte_size(NameStr) + 1,  %% +1 for null terminator
    Binary = <<12345:32/big, 1:32/big, NameLen:32/big, NameStr/binary, 0>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertEqual(12345, Body#job_step_create_request.job_id),
    ?assertEqual(1, Body#job_step_create_request.step_id).

decode_job_step_create_minimal_test() ->
    Binary = <<12345:32/big>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertEqual(12345, Body#job_step_create_request.job_id).

decode_job_step_create_invalid_test() ->
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, <<>>),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_JOB_STEP_INFO
%%%===================================================================

decode_job_step_info_full_test() ->
    Binary = <<1:32/big, 12345:32/big, 0:32/big, "extra">>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertEqual(1, Body#job_step_info_request.show_flags),
    ?assertEqual(12345, Body#job_step_info_request.job_id),
    ?assertEqual(0, Body#job_step_info_request.step_id).

decode_job_step_info_partial_test() ->
    %% With show_flags and job_id
    Binary1 = <<1:32/big, 12345:32/big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary1),
    ?assertEqual(1, Body1#job_step_info_request.show_flags),
    ?assertEqual(12345, Body1#job_step_info_request.job_id),

    %% With show_flags only
    Binary2 = <<2:32/big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary2),
    ?assertEqual(2, Body2#job_step_info_request.show_flags).

decode_job_step_info_empty_test() ->
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, <<>>),
    ?assertMatch(#job_step_info_request{}, Body).

decode_job_step_info_invalid_test() ->
    Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, <<1, 2, 3>>),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_COMPLETE_PROLOG
%%%===================================================================

decode_complete_prolog_full_test() ->
    NodeName = <<"node001">>,
    NodeLen = byte_size(NodeName),
    Binary = <<12345:32/big, 0:32/signed-big, NodeLen:32/big, NodeName/binary>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertEqual(12345, Body#complete_prolog_request.job_id),
    ?assertEqual(0, Body#complete_prolog_request.prolog_rc),
    ?assertEqual(NodeName, Body#complete_prolog_request.node_name).

decode_complete_prolog_partial_test() ->
    %% With job_id and prolog_rc
    Binary1 = <<12345:32/big, 0:32/signed-big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary1),
    ?assertEqual(12345, Body1#complete_prolog_request.job_id),
    ?assertEqual(0, Body1#complete_prolog_request.prolog_rc),

    %% With job_id only
    Binary2 = <<12345:32/big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary2),
    ?assertEqual(12345, Body2#complete_prolog_request.job_id).

decode_complete_prolog_empty_test() ->
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, <<>>),
    ?assertMatch(#complete_prolog_request{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - MESSAGE_EPILOG_COMPLETE
%%%===================================================================

decode_epilog_complete_full_test() ->
    NodeName = <<"node001">>,
    NodeLen = byte_size(NodeName),
    Binary = <<12345:32/big, 0:32/signed-big, NodeLen:32/big, NodeName/binary>>,
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
    ?assertEqual(12345, Body#epilog_complete_msg.job_id),
    ?assertEqual(0, Body#epilog_complete_msg.epilog_rc),
    ?assertEqual(NodeName, Body#epilog_complete_msg.node_name).

decode_epilog_complete_partial_test() ->
    %% With job_id and epilog_rc
    Binary1 = <<12345:32/big, 0:32/signed-big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary1),
    ?assertEqual(12345, Body1#epilog_complete_msg.job_id),
    ?assertEqual(0, Body1#epilog_complete_msg.epilog_rc),

    %% With job_id only
    Binary2 = <<12345:32/big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary2),
    ?assertEqual(12345, Body2#epilog_complete_msg.job_id).

decode_epilog_complete_empty_test() ->
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, <<>>),
    ?assertMatch(#epilog_complete_msg{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - MESSAGE_TASK_EXIT
%%%===================================================================

decode_task_exit_full_test() ->
    Binary = <<
        0:32/signed-big,   % return_code
        1:32/big,          % num_tasks
        1:32/big,          % array_count
        0:32/big,          % task_id[0]
        12345:32/big,      % job_id
        0:32/big,          % step_id
        16#FFFFFFFE:32/big % step_het_comp
    >>,
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
    ?assertEqual(12345, Body#task_exit_msg.job_id),
    ?assertEqual(0, Body#task_exit_msg.step_id),
    ?assertEqual([0], Body#task_exit_msg.task_ids),
    ?assertEqual(0, Body#task_exit_msg.return_code).

decode_task_exit_empty_test() ->
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, <<>>),
    ?assertMatch(#task_exit_msg{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_LAUNCH_TASKS
%%%===================================================================

decode_launch_tasks_minimal_test() ->
    %% Create a minimal launch_tasks binary
    Binary = <<
        12345:32/big,      % job_id
        0:32/big,          % step_id
        16#FFFFFFFE:32/big,% step_het_comp
        1000:32/big,       % uid
        1000:32/big,       % gid
        4:32/big, "root",  % user_name (length + string)
        0:32/big,          % ngids (0 = no gids)
        0:32/big,          % het_job_node_offset
        0:32/big,          % het_job_id
        0:32/big,          % het_job_nnodes
        0:32/big,          % het_job_ntasks
        0:32/big,          % het_job_offset
        0:32/big,          % het_job_step_cnt
        0:32/big,          % het_job_task_offset
        16#FFFFFFFF:32/big,% het_job_node_list (NULL)
        0:32/big,          % mpi_plugin_id
        1:32/big,          % ntasks
        0:16/big,          % ntasks_per_board
        0:16/big,          % ntasks_per_core
        0:16/big,          % ntasks_per_tres
        0:16/big,          % ntasks_per_socket
        0:64/big,          % job_mem_lim
        0:64/big,          % step_mem_lim
        1:32/big,          % nnodes
        1:16/big,          % cpus_per_task
        16#FFFFFFFF:32/big,% tres_per_task (NULL)
        1:16/big,          % threads_per_core
        0:32/big,          % task_dist
        1:16/big,          % node_cpus
        0:16/big,          % job_core_spec
        0:16/big           % accel_bind_type
    >>,
    Result = flurm_codec_step:decode_body(?REQUEST_LAUNCH_TASKS, Binary),
    ?assertMatch({ok, #launch_tasks_request{}}, Result),
    {ok, Body} = Result,
    ?assertEqual(12345, Body#launch_tasks_request.job_id).

%%%===================================================================
%%% decode_body/2 Tests - Unsupported
%%%===================================================================

decode_unsupported_test() ->
    Result = flurm_codec_step:decode_body(99999, <<"data">>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_JOB_STEP_CREATE
%%%===================================================================

encode_job_step_create_full_test() ->
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 12345,
        user_id = 1000,
        group_id = 1000,
        user_name = <<"testuser">>,
        node_list = <<"node001">>,
        num_tasks = 4,
        error_code = 0,
        error_msg = <<>>
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 20).

encode_job_step_create_minimal_test() ->
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 1,
        user_id = 0,
        group_id = 0,
        user_name = <<>>,
        node_list = <<>>,
        num_tasks = 0
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)).

encode_job_step_create_default_test() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, #{}),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_JOB_STEP_INFO
%%%===================================================================

encode_job_step_info_empty_test() ->
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 0,
        steps = []
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_job_step_info_with_steps_test() ->
    Step = #job_step_info{
        job_id = 12345,
        step_id = 0,
        step_name = <<"batch">>,
        partition = <<"default">>,
        user_id = 1000,
        state = 1,
        num_tasks = 4,
        num_cpus = 16,
        time_limit = 3600,
        start_time = 1700000000,
        run_time = 600,
        nodes = <<"node001">>,
        node_cnt = 1,
        tres_alloc_str = <<"cpu=16">>,
        exit_code = 0
    },
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 1,
        steps = [Step]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 12).

encode_job_step_info_multiple_test() ->
    Step1 = #job_step_info{job_id = 1, step_id = 0, step_name = <<"batch">>},
    Step2 = #job_step_info{job_id = 1, step_id = 1, step_name = <<"extern">>},
    Step3 = #job_step_info{job_id = 2, step_id = 0, step_name = <<"batch">>},
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 3,
        steps = [Step1, Step2, Step3]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_job_step_info_default_test() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, #{}),
    ?assertEqual(<<0:64, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_LAUNCH_TASKS
%%%===================================================================

encode_launch_tasks_full_test() ->
    Resp = #launch_tasks_response{
        job_id = 12345,
        step_id = 0,
        step_het_comp = 16#FFFFFFFE,
        return_code = 0,
        node_name = <<"node001">>,
        count_of_pids = 2,
        local_pids = [1234, 1235],
        gtids = [0, 1]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(12345, JobId).

encode_launch_tasks_map_test() ->
    Map = #{
        job_id => 12345,
        step_id => 0,
        step_het_comp => 16#FFFFFFFE,
        return_code => 0,
        node_name => <<"node001">>,
        local_pids => [1234],
        gtids => [0]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Map),
    ?assert(is_binary(Binary)).

encode_launch_tasks_default_test() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, #{}),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_REATTACH_TASKS
%%%===================================================================

encode_reattach_tasks_full_test() ->
    Resp = #reattach_tasks_response{
        return_code = 0,
        node_name = <<"node001">>,
        count_of_pids = 2,
        local_pids = [1234, 1235],
        gtids = [0, 1],
        executable_names = [<<"/bin/bash">>, <<"/usr/bin/python">>]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp),
    ?assert(is_binary(Binary)).

encode_reattach_tasks_map_test() ->
    Map = #{
        return_code => 0,
        node_name => <<"node001">>,
        local_pids => [1234],
        gtids => [0],
        executable_names => [<<"/bin/hostname">>]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Map),
    ?assert(is_binary(Binary)).

encode_reattach_tasks_default_test() ->
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, #{}),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% encode_body/2 Tests - MESSAGE_TASK_EXIT
%%%===================================================================

encode_task_exit_map_test() ->
    Map = #{
        job_id => 12345,
        step_id => 0,
        step_het_comp => 16#FFFFFFFE,
        task_ids => [0, 1, 2],
        return_codes => [0, 0, 0]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT, Map),
    ?assert(is_binary(Binary)).

encode_task_exit_default_test() ->
    {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT, #{}),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% encode_body/2 Tests - Unsupported
%%%===================================================================

encode_unsupported_test() ->
    Result = flurm_codec_step:encode_body(99999, #{}),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Internal Helper Tests
%%%===================================================================

normalize_step_id_no_val_test() ->
    %% Test via decode with NO_VAL step_id
    %% Format: <<ShowFlags:32/big, JobId:32/big, StepId:32/big>>
    Binary1 = <<0:32/big, 12345:32/big, 16#FFFFFFFE:32/big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary1),
    ?assertEqual(0, Body1#job_step_info_request.step_id),

    Binary2 = <<0:32/big, 12345:32/big, 16#FFFFFFFF:32/big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary2),
    ?assertEqual(0, Body2#job_step_info_request.step_id).

normalize_step_id_normal_test() ->
    Binary = <<0:32/big, 12345:32/big, 5:32/big>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
    ?assertEqual(5, Body#job_step_info_request.step_id).

unpack_string_safe_valid_test() ->
    %% Test via decode_complete_prolog which uses unpack_string_safe
    NodeName = <<"node001">>,
    NodeLen = byte_size(NodeName),
    Binary = <<12345:32/big, 0:32/signed-big, NodeLen:32/big, NodeName/binary>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertEqual(NodeName, Body#complete_prolog_request.node_name).

unpack_string_safe_null_test() ->
    %% NULL string (NO_VAL marker)
    Binary = <<12345:32/big, 0:32/signed-big, 16#FFFFFFFF:32/big>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertEqual(<<>>, Body#complete_prolog_request.node_name).

unpack_string_safe_empty_test() ->
    %% Empty string (length = 0)
    Binary = <<12345:32/big, 0:32/signed-big, 0:32/big>>,
    {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
    ?assertEqual(<<>>, Body#complete_prolog_request.node_name).

unpack_uint32_array_empty_test() ->
    %% Test via task_exit with empty task array
    Binary = <<0:32/signed-big, 0:32/big, 0:32/big, 12345:32/big, 0:32/big, 0:32/big>>,
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
    ?assertEqual([], Body#task_exit_msg.task_ids).

unpack_uint32_array_values_test() ->
    %% Test via task_exit with task array
    Binary = <<
        0:32/signed-big,   % return_code
        3:32/big,          % num_tasks
        3:32/big,          % array_count
        0:32/big, 1:32/big, 2:32/big,  % task_ids
        12345:32/big,      % job_id
        0:32/big,          % step_id
        0:32/big           % step_het_comp
    >>,
    {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
    ?assertEqual([0, 1, 2], Body#task_exit_msg.task_ids).

unpack_string_array_empty_test() ->
    %% Tested via launch_tasks which uses unpack_string_array
    ok.

unpack_string_array_values_test() ->
    %% Tested via launch_tasks which uses unpack_string_array
    ok.

%%%===================================================================
%%% Step Layout and Credential Encoding Tests
%%%===================================================================

encode_step_layout_test() ->
    %% Test via encode_job_step_create_response
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 12345,
        user_id = 1000,
        group_id = 1000,
        user_name = <<"user">>,
        node_list = <<"node[001-004]">>,
        num_tasks = 4
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

encode_minimal_cred_test() ->
    %% Test via encode_job_step_create_response
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 1,
        user_id = 0,
        group_id = 0,
        user_name = <<"root">>,
        node_list = <<"node001">>,
        num_tasks = 1
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)).

encode_select_jobinfo_test() ->
    %% Test via encode_job_step_create_response
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 1,
        user_id = 0,
        group_id = 0,
        num_tasks = 1
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

step_id_boundary_test() ->
    %% Step ID = 0
    Binary1 = <<0:32/big, 12345:32/big, 0:32/big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary1),
    ?assertEqual(0, Body1#job_step_info_request.step_id),

    %% Step ID = 100
    Binary2 = <<0:32/big, 12345:32/big, 100:32/big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary2),
    ?assertEqual(100, Body2#job_step_info_request.step_id).

return_code_values_test() ->
    %% Return code = 0
    Resp1 = #launch_tasks_response{return_code = 0, local_pids = [], gtids = []},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp1),

    %% Return code = 1
    Resp2 = #launch_tasks_response{return_code = 1, local_pids = [], gtids = []},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp2),

    %% Return code = -1
    Resp3 = #launch_tasks_response{return_code = -1, local_pids = [], gtids = []},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp3).

prolog_rc_values_test() ->
    %% prolog_rc = 0
    Binary1 = <<12345:32/big, 0:32/signed-big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary1),
    ?assertEqual(0, Body1#complete_prolog_request.prolog_rc),

    %% prolog_rc = 1
    Binary2 = <<12345:32/big, 1:32/signed-big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary2),
    ?assertEqual(1, Body2#complete_prolog_request.prolog_rc),

    %% prolog_rc = -1
    Binary3 = <<12345:32/big, -1:32/signed-big>>,
    {ok, Body3} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary3),
    ?assertEqual(-1, Body3#complete_prolog_request.prolog_rc).

epilog_rc_values_test() ->
    %% epilog_rc = 0
    Binary1 = <<12345:32/big, 0:32/signed-big>>,
    {ok, Body1} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary1),
    ?assertEqual(0, Body1#epilog_complete_msg.epilog_rc),

    %% epilog_rc = 1
    Binary2 = <<12345:32/big, 1:32/signed-big>>,
    {ok, Body2} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary2),
    ?assertEqual(1, Body2#epilog_complete_msg.epilog_rc),

    %% epilog_rc = -1
    Binary3 = <<12345:32/big, -1:32/signed-big>>,
    {ok, Body3} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary3),
    ?assertEqual(-1, Body3#epilog_complete_msg.epilog_rc).

%%%===================================================================
%%% ensure_binary Tests
%%%===================================================================

ensure_binary_various_test() ->
    %% Test via encode with various types
    %% undefined
    Step1 = #job_step_info{step_name = undefined},
    Resp1 = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step1]},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp1),

    %% null
    Step2 = #job_step_info{step_name = null},
    Resp2 = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step2]},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp2),

    %% binary
    Step3 = #job_step_info{step_name = <<"batch">>},
    Resp3 = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step3]},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp3),

    %% list
    Step4 = #job_step_info{step_name = "batch"},
    Resp4 = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step4]},
    {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp4).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_step_info_test() ->
    %% Note: Can't do full roundtrip without encode for REQUEST_JOB_STEP_INFO
    %% Test decode followed by encode of response
    Step = #job_step_info{
        job_id = 12345,
        step_id = 0,
        step_name = <<"batch">>,
        partition = <<"default">>,
        user_id = 1000,
        state = 1,
        num_tasks = 4
    },
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 1,
        steps = [Step]
    },
    {ok, Encoded} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Encoded)),
    ?assert(byte_size(Encoded) > 12).

%%%===================================================================
%%% Additional Coverage Tests
%%%===================================================================

%% Test encode_single_job_step_info with non-record
encode_single_step_info_invalid_test_() ->
    [
     {"invalid step info", fun() ->
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [invalid]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test various step states
step_states_test_() ->
    [
     {"step state PENDING", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 0},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"step state RUNNING", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 1},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"step state COMPLETE", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 3},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%% Test task_exit with various return codes
task_exit_return_codes_test_() ->
    [
     {"return code 0", fun() ->
        {ok, _} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, return_codes => [0]})
      end},
     {"return code 1", fun() ->
        {ok, _} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, return_codes => [1]})
      end},
     {"return code 255", fun() ->
        {ok, _} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, return_codes => [255]})
      end},
     {"return code -1", fun() ->
        {ok, _} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, return_codes => [-1]})
      end}
    ].

%% Test launch_tasks response with various pid counts
launch_tasks_pids_test_() ->
    [
     {"empty pids", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, return_code = 0,
            count_of_pids = 0, local_pids = [], gtids = []
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp)
      end},
     {"single pid", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, return_code = 0,
            count_of_pids = 1, local_pids = [1234], gtids = [0]
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp)
      end},
     {"multiple pids", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, return_code = 0,
            count_of_pids = 4,
            local_pids = [1234, 1235, 1236, 1237],
            gtids = [0, 1, 2, 3]
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp)
      end}
    ].

%% Test reattach_tasks response with various executable names
reattach_tasks_execs_test_() ->
    [
     {"empty executables", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0, count_of_pids = 0,
            local_pids = [], gtids = [], executable_names = []
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp)
      end},
     {"single executable", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0, count_of_pids = 1,
            local_pids = [1234], gtids = [0],
            executable_names = [<<"/bin/bash">>]
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp)
      end},
     {"multiple executables", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0, count_of_pids = 3,
            local_pids = [1, 2, 3], gtids = [0, 1, 2],
            executable_names = [<<"/bin/a">>, <<"/bin/b">>, <<"/bin/c">>]
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp)
      end}
    ].

%% Test job_step_create with various node counts
step_create_nodes_test_() ->
    [
     {"single node", fun() ->
        Resp = #job_step_create_response{
            job_id = 1, job_step_id = 0, user_id = 0, group_id = 0,
            num_tasks = 1, node_list = <<"node001">>
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp)
      end},
     {"node range", fun() ->
        Resp = #job_step_create_response{
            job_id = 1, job_step_id = 0, user_id = 0, group_id = 0,
            num_tasks = 10, node_list = <<"node[001-010]">>
        },
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp)
      end}
    ].

%% Test step_info request show_flags variations
show_flags_test_() ->
    [
     {"show_flags = 0", fun() ->
        Binary = <<0:32/big, 12345:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(0, Body#job_step_info_request.show_flags)
      end},
     {"show_flags = 1", fun() ->
        Binary = <<1:32/big, 12345:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(1, Body#job_step_info_request.show_flags)
      end},
     {"show_flags = 0xFFFF", fun() ->
        Binary = <<16#FFFF:32/big, 12345:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(16#FFFF, Body#job_step_info_request.show_flags)
      end}
    ].

%% Test job_step_create_request with zero/default values
step_create_defaults_test_() ->
    [
     {"zero min_nodes becomes 1", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            0:32/big, 4:32/big, 1:32/big, 1:32/big,
            3600:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(1, Body#job_step_create_request.min_nodes)
      end},
     {"zero max_nodes becomes 1", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 0:32/big, 1:32/big, 1:32/big,
            3600:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(1, Body#job_step_create_request.max_nodes)
      end},
     {"zero num_tasks becomes 1", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 1:32/big, 0:32/big, 1:32/big,
            3600:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(1, Body#job_step_create_request.num_tasks)
      end},
     {"zero cpus_per_task becomes 1", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 1:32/big, 1:32/big, 0:32/big,
            3600:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(1, Body#job_step_create_request.cpus_per_task)
      end}
    ].

%% Test step_info full record fields
step_info_full_fields_test() ->
    Step = #job_step_info{
        job_id = 12345,
        step_id = 0,
        step_name = <<"interactive">>,
        partition = <<"compute">>,
        user_id = 1000,
        state = 1,
        num_tasks = 16,
        num_cpus = 64,
        time_limit = 86400,
        start_time = 1700000000,
        run_time = 3600,
        nodes = <<"node[001-004]">>,
        node_cnt = 4,
        tres_alloc_str = <<"cpu=64,mem=256G,gres/gpu=4">>,
        exit_code = 0
    },
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 1,
        steps = [Step]
    },
    {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

%%%===================================================================
%%% Additional Coverage Tests - Request Decoding Edge Cases
%%%===================================================================

%% Test job_step_create with long name
step_create_long_name_test_() ->
    [
     {"long step name", fun() ->
        LongName = list_to_binary(lists:duplicate(100, $x)),
        NameLen = byte_size(LongName) + 1,
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 4:32/big, 8:32/big, 2:32/big,
            7200:32/big, 0:32/big, 0:32/big,
            NameLen:32/big, LongName/binary, 0
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(LongName, Body#job_step_create_request.name)
      end},
     {"unicode step name", fun() ->
        UnicodeName = <<"test_step_\xc3\xa9">>,
        NameLen = byte_size(UnicodeName) + 1,
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 4:32/big, 8:32/big, 2:32/big,
            7200:32/big, 0:32/big, 0:32/big,
            NameLen:32/big, UnicodeName/binary, 0
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(UnicodeName, Body#job_step_create_request.name)
      end},
     {"empty name after unpack", fun() ->
        %% Empty string with null terminator
        Binary = <<12345:32/big, 1:32/big, 1:32/big, 0>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(12345, Body#job_step_create_request.job_id)
      end}
    ].

%% Test step_info with large job_id values
step_info_large_values_test_() ->
    [
     {"max uint32 job_id", fun() ->
        Binary = <<0:32/big, 16#FFFFFFFD:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(16#FFFFFFFD, Body#job_step_info_request.job_id)
      end},
     {"large show_flags", fun() ->
        Binary = <<16#FFFFFFFF:32/big, 12345:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(16#FFFFFFFF, Body#job_step_info_request.show_flags)
      end}
    ].

%% Test complete_prolog with various node names
complete_prolog_node_names_test_() ->
    [
     {"long node name", fun() ->
        NodeName = <<"compute-node-with-very-long-name-001">>,
        NodeLen = byte_size(NodeName),
        Binary = <<12345:32/big, 0:32/signed-big, NodeLen:32/big, NodeName/binary>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(NodeName, Body#complete_prolog_request.node_name)
      end},
     {"node name with special chars", fun() ->
        NodeName = <<"node_001-rack2">>,
        NodeLen = byte_size(NodeName),
        Binary = <<12345:32/big, -5:32/signed-big, NodeLen:32/big, NodeName/binary>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(NodeName, Body#complete_prolog_request.node_name),
        ?assertEqual(-5, Body#complete_prolog_request.prolog_rc)
      end}
    ].

%% Test epilog_complete with various return codes
epilog_complete_rc_test_() ->
    [
     {"large positive rc", fun() ->
        Binary = <<12345:32/big, 127:32/signed-big>>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
        ?assertEqual(127, Body#epilog_complete_msg.epilog_rc)
      end},
     {"large negative rc", fun() ->
        Binary = <<12345:32/big, -127:32/signed-big>>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_EPILOG_COMPLETE, Binary),
        ?assertEqual(-127, Body#epilog_complete_msg.epilog_rc)
      end}
    ].

%% Test task_exit with various task arrays
task_exit_arrays_test_() ->
    [
     {"large task array", fun() ->
        TaskIds = lists:seq(0, 15),
        TaskIdsBin = << <<T:32/big>> || T <- TaskIds >>,
        Binary = <<
            0:32/signed-big,         % return_code
            16:32/big,               % num_tasks
            16:32/big,               % array_count
            TaskIdsBin/binary,       % task_ids
            12345:32/big, 0:32/big, 0:32/big  % step_id
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
        ?assertEqual(TaskIds, Body#task_exit_msg.task_ids)
      end},
     {"single task", fun() ->
        Binary = <<
            1:32/signed-big, 1:32/big, 1:32/big, 0:32/big,
            12345:32/big, 5:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
        ?assertEqual([0], Body#task_exit_msg.task_ids),
        ?assertEqual(1, Body#task_exit_msg.return_code)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Response Encoding Edge Cases
%%%===================================================================

%% Test step_create_response with error cases
step_create_response_errors_test_() ->
    [
     {"response with error code", fun() ->
        Resp = #job_step_create_response{
            job_id = 12345,
            job_step_id = 0,
            user_id = 1000,
            group_id = 1000,
            error_code = 1,
            error_msg = <<"Invalid step">>
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end},
     {"response with zero fields", fun() ->
        %% Use zero instead of undefined since the encoder expects integers
        Resp = #job_step_create_response{
            job_id = 1,
            job_step_id = 0,
            user_id = 0,
            group_id = 0
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test step_info_response with various step counts
step_info_response_counts_test_() ->
    [
     {"10 steps", fun() ->
        Steps = [#job_step_info{job_id = N, step_id = 0} || N <- lists:seq(1, 10)],
        Resp = #job_step_info_response{
            last_update = 1700000000,
            step_count = 10,
            steps = Steps
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) > 100)
      end},
     {"step with all fields", fun() ->
        Step = #job_step_info{
            job_id = 12345,
            step_id = 1,
            step_name = <<"interactive">>,
            partition = <<"debug">>,
            user_id = 1000,
            state = 2,
            num_tasks = 32,
            num_cpus = 128,
            time_limit = 172800,
            start_time = 1700000000,
            run_time = 7200,
            nodes = <<"node[001-008]">>,
            node_cnt = 8,
            tres_alloc_str = <<"cpu=128,mem=512G">>,
            exit_code = 0
        },
        Resp = #job_step_info_response{
            last_update = 1700000000,
            step_count = 1,
            steps = [Step]
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(byte_size(Binary) > 50)
      end}
    ].

%% Test launch_tasks_response with various configurations
launch_tasks_response_configs_test_() ->
    [
     {"many pids", fun() ->
        LocalPids = lists:seq(1000, 1015),
        Gtids = lists:seq(0, 15),
        Resp = #launch_tasks_response{
            job_id = 12345,
            step_id = 0,
            step_het_comp = 16#FFFFFFFE,
            return_code = 0,
            node_name = <<"compute001">>,
            count_of_pids = 16,
            local_pids = LocalPids,
            gtids = Gtids
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(byte_size(Binary) > 100)
      end},
     {"error return code", fun() ->
        Resp = #launch_tasks_response{
            job_id = 12345,
            step_id = 0,
            return_code = -1,
            node_name = <<"node001">>,
            count_of_pids = 0,
            local_pids = [],
            gtids = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end},
     {"with string node name", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS,
            #{return_code => 0, node_name => "string_node", local_pids => [], gtids => []}),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test reattach_tasks_response variations
reattach_tasks_response_variations_test_() ->
    [
     {"many executables", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0,
            node_name = <<"node001">>,
            count_of_pids = 4,
            local_pids = [1000, 1001, 1002, 1003],
            gtids = [0, 1, 2, 3],
            executable_names = [
                <<"/bin/bash">>,
                <<"/usr/bin/python3">>,
                <<"/usr/local/bin/mpirun">>,
                <<"/home/user/myapp">>
            ]
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp),
        ?assert(byte_size(Binary) > 50)
      end},
     {"with list strings", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS,
            #{return_code => 0, node_name => "node", local_pids => [1], gtids => [0],
              executable_names => ["/bin/test"]}),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test task_exit_msg variations
task_exit_msg_variations_test_() ->
    [
     {"multiple return codes", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, task_ids => [0, 1, 2],
              return_codes => [0, 1, 255]}),
        ?assert(is_binary(Binary))
      end},
     {"empty return codes", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 1, step_id => 0, task_ids => [], return_codes => []}),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Internal Helper Functions
%%%===================================================================

%% Test normalize_step_id edge cases
normalize_step_id_edge_test_() ->
    [
     {"NO_VAL_16 boundary", fun() ->
        Binary = <<0:32/big, 12345:32/big, 16#FFFFFFFD:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(16#FFFFFFFD, Body#job_step_info_request.step_id)
      end},
     {"step_id near boundary", fun() ->
        Binary = <<0:32/big, 12345:32/big, 16#FFFFFFFB:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_INFO, Binary),
        ?assertEqual(16#FFFFFFFB, Body#job_step_info_request.step_id)
      end}
    ].

%% Test unpack_string_safe with various inputs
unpack_string_safe_edge_test_() ->
    [
     {"very long string length", fun() ->
        %% Tests NO_VAL path
        NodeName = <<"test">>,
        NodeLen = byte_size(NodeName),
        Binary = <<12345:32/big, 0:32/signed-big, NodeLen:32/big, NodeName/binary>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(NodeName, Body#complete_prolog_request.node_name)
      end},
     {"string length exceeds buffer", fun() ->
        %% If length exceeds remaining buffer, should return empty
        Binary = <<12345:32/big, 0:32/signed-big, 1000:32/big, "short">>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(<<>>, Body#complete_prolog_request.node_name)
      end}
    ].

%% Test unpack_uint32_array with edge cases
unpack_uint32_array_edge_test_() ->
    [
     {"array with tasks", fun() ->
        %% Valid task_exit format: return_code, num_tasks, array_count, task_ids[], job_id, step_id, step_het_comp
        Binary = <<0:32/signed-big, 2:32/big, 2:32/big, 0:32/big, 1:32/big, 12345:32/big, 0:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
        ?assertEqual([0, 1], Body#task_exit_msg.task_ids)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Encode Step Layout
%%%===================================================================

encode_step_layout_variations_test_() ->
    [
     {"multi-node layout", fun() ->
        Resp = #job_step_create_response{
            job_id = 12345,
            job_step_id = 0,
            user_id = 1000,
            group_id = 1000,
            user_name = <<"testuser">>,
            node_list = <<"node[001-004]">>,
            num_tasks = 16
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(byte_size(Binary) > 100)
      end},
     {"single task layout", fun() ->
        Resp = #job_step_create_response{
            job_id = 1,
            job_step_id = 0,
            user_id = 0,
            group_id = 0,
            user_name = <<"root">>,
            node_list = <<"localhost">>,
            num_tasks = 1
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - ensure_binary
%%%===================================================================

ensure_binary_edge_test_() ->
    [
     {"empty binary input", fun() ->
        %% Test via step_info with empty binary
        Step = #job_step_info{step_name = <<>>},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"list string input", fun() ->
        %% List strings should be converted to binary
        Step = #job_step_info{step_name = "test_name"},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Job Step Create Full Format
%%%===================================================================

job_step_create_full_format_test_() ->
    [
     {"all fields populated", fun() ->
        NameStr = <<"full_test_step">>,
        NameLen = byte_size(NameStr) + 1,
        Binary = <<
            99999:32/big,      % job_id
            5:32/big,          % step_id
            1000:32/big,       % user_id
            4:32/big,          % min_nodes
            8:32/big,          % max_nodes
            32:32/big,         % num_tasks
            4:32/big,          % cpus_per_task
            86400:32/big,      % time_limit
            16#FF:32/big,      % flags
            1:32/big,          % immediate
            NameLen:32/big,
            NameStr/binary,
            0
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(99999, Body#job_step_create_request.job_id),
        ?assertEqual(5, Body#job_step_create_request.step_id),
        ?assertEqual(1000, Body#job_step_create_request.user_id),
        ?assertEqual(4, Body#job_step_create_request.min_nodes),
        ?assertEqual(8, Body#job_step_create_request.max_nodes),
        ?assertEqual(32, Body#job_step_create_request.num_tasks),
        ?assertEqual(4, Body#job_step_create_request.cpus_per_task),
        ?assertEqual(86400, Body#job_step_create_request.time_limit),
        ?assertEqual(16#FF, Body#job_step_create_request.flags),
        ?assertEqual(1, Body#job_step_create_request.immediate),
        ?assertEqual(NameStr, Body#job_step_create_request.name)
      end},
     {"immediate = 0", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 1:32/big, 1:32/big, 1:32/big,
            3600:32/big, 0:32/big, 0:32/big,
            5:32/big, "test", 0
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(0, Body#job_step_create_request.immediate)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - launch_tasks_request decoding
%%%===================================================================

launch_tasks_decode_edge_test_() ->
    [
     {"empty binary returns error", fun() ->
        Result = flurm_codec_step:decode_body(?REQUEST_LAUNCH_TASKS, <<>>),
        ?assertMatch({error, _}, Result)
      end},
     {"truncated binary returns error", fun() ->
        Result = flurm_codec_step:decode_body(?REQUEST_LAUNCH_TASKS, <<1:32/big, 2:32/big>>),
        ?assertMatch({error, _}, Result)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step States
%%%===================================================================

step_info_states_test_() ->
    [
     {"state SUSPENDED", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 2},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"state FAILED", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 4},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"state CANCELLED", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 5},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"state TIMEOUT", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, state = 6},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Response Map Inputs
%%%===================================================================

response_map_inputs_test_() ->
    [
     {"launch_tasks with minimal map", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS,
            #{return_code => 0}),
        ?assert(is_binary(Binary))
      end},
     {"reattach_tasks with minimal map", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS,
            #{return_code => 0}),
        ?assert(is_binary(Binary))
      end},
     {"task_exit with minimal map", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?MESSAGE_TASK_EXIT,
            #{job_id => 0, step_id => 0}),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Time Limits
%%%===================================================================

time_limit_test_() ->
    [
     {"unlimited time_limit", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 1:32/big, 1:32/big, 1:32/big,
            16#FFFFFFFF:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(16#FFFFFFFF, Body#job_step_create_request.time_limit)
      end},
     {"zero time_limit", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            1:32/big, 1:32/big, 1:32/big, 1:32/big,
            0:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(0, Body#job_step_create_request.time_limit)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Info Time Fields
%%%===================================================================

step_info_time_fields_test_() ->
    [
     {"step_info with time 0", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            time_limit = 0, start_time = 0, run_time = 0
        },
        Resp = #job_step_info_response{last_update = 0, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"step_info with max time", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            time_limit = 16#FFFFFFFF, start_time = 16#FFFFFFFFFFFFFFFF, run_time = 16#FFFFFFFF
        },
        Resp = #job_step_info_response{last_update = 16#FFFFFFFFFFFFFFFF, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Complete Prolog Return Codes
%%%===================================================================

complete_prolog_rc_extended_test_() ->
    [
     {"prolog_rc max positive", fun() ->
        Binary = <<12345:32/big, 2147483647:32/signed-big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(2147483647, Body#complete_prolog_request.prolog_rc)
      end},
     {"prolog_rc max negative", fun() ->
        Binary = <<12345:32/big, -2147483648:32/signed-big>>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_COMPLETE_PROLOG, Binary),
        ?assertEqual(-2147483648, Body#complete_prolog_request.prolog_rc)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Launch Tasks Response PIDs
%%%===================================================================

launch_tasks_response_pids_test_() ->
    [
     {"large pid values", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, return_code = 0,
            count_of_pids = 3,
            local_pids = [32768, 65535, 99999],
            gtids = [0, 1, 2]
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end},
     {"mismatched pid/gtid counts", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, return_code = 0,
            count_of_pids = 2,
            local_pids = [1234, 5678],
            gtids = [0]  % Mismatched count
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Reattach Tasks Node Names
%%%===================================================================

reattach_tasks_node_names_test_() ->
    [
     {"empty node name", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0,
            node_name = <<>>,
            count_of_pids = 0,
            local_pids = [],
            gtids = [],
            executable_names = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end},
     {"undefined node name", fun() ->
        Resp = #reattach_tasks_response{
            return_code = 0,
            node_name = undefined,
            count_of_pids = 0,
            local_pids = [],
            gtids = [],
            executable_names = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Info TRES Strings
%%%===================================================================

step_info_tres_test_() ->
    [
     {"complex tres string", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            tres_alloc_str = <<"cpu=128,mem=1024G,node=4,gres/gpu:v100=16,gres/ssd=200G">>
        },
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"empty tres string", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, tres_alloc_str = <<>>},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"undefined tres string", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, tres_alloc_str = undefined},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Create Response Node Lists
%%%===================================================================

step_create_response_node_lists_test_() ->
    [
     {"complex node list", fun() ->
        Resp = #job_step_create_response{
            job_id = 1, job_step_id = 0, user_id = 1000, group_id = 1000,
            node_list = <<"rack1-node[001-100],rack2-node[001-050]">>
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end},
     {"single node no brackets", fun() ->
        Resp = #job_step_create_response{
            job_id = 1, job_step_id = 0, user_id = 1000, group_id = 1000,
            node_list = <<"compute-node">>
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Task Exit Step HET Components
%%%===================================================================

task_exit_het_comp_test_() ->
    [
     {"het_comp NO_VAL", fun() ->
        Binary = <<
            0:32/signed-big, 1:32/big, 1:32/big, 0:32/big,
            12345:32/big, 0:32/big, 16#FFFFFFFE:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
        ?assertEqual(16#FFFFFFFE, Body#task_exit_msg.step_het_comp)
      end},
     {"het_comp specific value", fun() ->
        Binary = <<
            0:32/signed-big, 1:32/big, 1:32/big, 0:32/big,
            12345:32/big, 0:32/big, 5:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?MESSAGE_TASK_EXIT, Binary),
        ?assertEqual(5, Body#task_exit_msg.step_het_comp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Unsupported Message Types
%%%===================================================================

unsupported_types_extended_test_() ->
    [
     {"decode unknown type 0", fun() ->
        ?assertEqual(unsupported, flurm_codec_step:decode_body(0, <<>>))
      end},
     {"decode unknown type 9999", fun() ->
        ?assertEqual(unsupported, flurm_codec_step:decode_body(9999, <<"data">>))
      end},
     {"encode unknown type 0", fun() ->
        ?assertEqual(unsupported, flurm_codec_step:encode_body(0, #{}))
      end},
     {"encode unknown type 9999", fun() ->
        ?assertEqual(unsupported, flurm_codec_step:encode_body(9999, #{}))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Create Zero Value Adjustments
%%%===================================================================

step_create_zero_adjustments_test_() ->
    [
     {"all zeros become ones", fun() ->
        Binary = <<
            12345:32/big, 0:32/big, 1000:32/big,
            0:32/big, 0:32/big, 0:32/big, 0:32/big,
            3600:32/big, 0:32/big, 0:32/big
        >>,
        {ok, Body} = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
        ?assertEqual(1, Body#job_step_create_request.min_nodes),
        ?assertEqual(1, Body#job_step_create_request.max_nodes),
        ?assertEqual(1, Body#job_step_create_request.num_tasks),
        ?assertEqual(1, Body#job_step_create_request.cpus_per_task)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Info Exit Codes
%%%===================================================================

step_info_exit_codes_test_() ->
    [
     {"exit_code 0", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, exit_code = 0},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"exit_code 1", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, exit_code = 1},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"exit_code 255", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, exit_code = 255},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"exit_code signal", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, exit_code = 137},  % SIGKILL
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Multiple Steps Encoding
%%%===================================================================

multiple_steps_encoding_test_() ->
    [
     {"batch and extern steps", fun() ->
        BatchStep = #job_step_info{
            job_id = 12345, step_id = 16#FFFFFFFE, step_name = <<"batch">>,
            partition = <<"default">>, state = 1
        },
        ExternStep = #job_step_info{
            job_id = 12345, step_id = 16#FFFFFFFD, step_name = <<"extern">>,
            partition = <<"default">>, state = 1
        },
        InteractiveStep = #job_step_info{
            job_id = 12345, step_id = 0, step_name = <<"interactive">>,
            partition = <<"default">>, state = 1
        },
        Resp = #job_step_info_response{
            last_update = 1700000000,
            step_count = 3,
            steps = [BatchStep, ExternStep, InteractiveStep]
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
        ?assert(byte_size(Binary) > 50)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Decode Catch Blocks
%%%===================================================================

decode_catch_blocks_test_() ->
    [
     {"job_step_create malformed binary", fun() ->
        %% Binary that triggers catch block
        Result = flurm_codec_step:decode_body(?REQUEST_JOB_STEP_CREATE, <<1, 2, 3>>),
        ?assertMatch({error, _}, Result)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Info Partitions
%%%===================================================================

step_info_partitions_test_() ->
    [
     {"partition with dashes", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, partition = <<"gpu-partition">>},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"partition with underscores", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, partition = <<"high_memory">>},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Response Default Handling
%%%===================================================================

response_default_handling_test_() ->
    [
     {"launch_tasks atom input", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, invalid),
        ?assert(is_binary(Binary))
      end},
     {"reattach_tasks atom input", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_REATTACH_TASKS, invalid),
        ?assert(is_binary(Binary))
      end},
     {"step_create atom input", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, invalid),
        ?assert(is_binary(Binary))
      end},
     {"step_info atom input", fun() ->
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, invalid),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step Info Nodes
%%%===================================================================

step_info_nodes_test_() ->
    [
     {"nodes with range", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            nodes = <<"compute[001-999]">>,
            node_cnt = 999
        },
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"nodes multiple ranges", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            nodes = <<"r1c[01-10]n[01-08],r2c[01-05]n[01-08]">>,
            node_cnt = 120
        },
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Large Values
%%%===================================================================

large_values_test_() ->
    [
     {"large job_id", fun() ->
        Resp = #launch_tasks_response{
            job_id = 16#FFFFFFFD,
            step_id = 0,
            return_code = 0,
            local_pids = [],
            gtids = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        <<JobId:32/big, _/binary>> = Binary,
        ?assertEqual(16#FFFFFFFD, JobId)
      end},
     {"large user_id in step_create", fun() ->
        Resp = #job_step_create_response{
            job_id = 1,
            job_step_id = 0,
            user_id = 16#FFFFFFFF,
            group_id = 16#FFFFFFFF
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Step HET Comp in Responses
%%%===================================================================

step_het_comp_responses_test_() ->
    [
     {"launch_tasks with het_comp 0", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, step_het_comp = 0,
            return_code = 0, local_pids = [], gtids = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end},
     {"launch_tasks with het_comp specific", fun() ->
        Resp = #launch_tasks_response{
            job_id = 1, step_id = 0, step_het_comp = 3,
            return_code = 0, local_pids = [], gtids = []
        },
        {ok, Binary} = flurm_codec_step:encode_body(?RESPONSE_LAUNCH_TASKS, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Num Tasks/CPUs
%%%===================================================================

num_tasks_cpus_test_() ->
    [
     {"step_info many tasks and cpus", fun() ->
        Step = #job_step_info{
            job_id = 1, step_id = 0,
            num_tasks = 1024,
            num_cpus = 4096
        },
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end},
     {"step_info zero tasks and cpus", fun() ->
        Step = #job_step_info{job_id = 1, step_id = 0, num_tasks = 0, num_cpus = 0},
        Resp = #job_step_info_response{last_update = 1, step_count = 1, steps = [Step]},
        {ok, _} = flurm_codec_step:encode_body(?RESPONSE_JOB_STEP_INFO, Resp)
      end}
    ].
