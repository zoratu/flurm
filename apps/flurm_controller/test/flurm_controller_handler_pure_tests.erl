%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler Pure Unit Tests
%%%
%%% Pure unit tests for flurm_controller_handler module that test
%%% helper functions and state conversion logic without any mocking.
%%%
%%% These tests focus on:
%%% - State conversion functions (job_state_to_slurm, node_state_to_slurm, etc.)
%%% - Formatting helpers (format_allocated_nodes, format_features, etc.)
%%% - Job update building (build_job_updates)
%%% - Job ID parsing (parse_job_id_str, safe_binary_to_integer)
%%% - Default value helpers (default_partition, default_time_limit, etc.)
%%% - Error conversion (error_to_binary)
%%% - Reservation type and flags parsing
%%% - Request to spec conversion
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures - These use {setup, ...} for isolation without dependencies
%%====================================================================

%% Test group for pure helper functions
pure_helpers_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        %% State conversion tests
        {"job_state_to_slurm - pending", fun test_job_state_pending/0},
        {"job_state_to_slurm - configuring", fun test_job_state_configuring/0},
        {"job_state_to_slurm - running", fun test_job_state_running/0},
        {"job_state_to_slurm - completing", fun test_job_state_completing/0},
        {"job_state_to_slurm - completed", fun test_job_state_completed/0},
        {"job_state_to_slurm - cancelled", fun test_job_state_cancelled/0},
        {"job_state_to_slurm - failed", fun test_job_state_failed/0},
        {"job_state_to_slurm - timeout", fun test_job_state_timeout/0},
        {"job_state_to_slurm - node_fail", fun test_job_state_node_fail/0},
        {"job_state_to_slurm - unknown", fun test_job_state_unknown/0},

        %% Node state conversion tests
        {"node_state_to_slurm - up", fun test_node_state_up/0},
        {"node_state_to_slurm - down", fun test_node_state_down/0},
        {"node_state_to_slurm - drain", fun test_node_state_drain/0},
        {"node_state_to_slurm - idle", fun test_node_state_idle/0},
        {"node_state_to_slurm - allocated", fun test_node_state_allocated/0},
        {"node_state_to_slurm - mixed", fun test_node_state_mixed/0},
        {"node_state_to_slurm - unknown", fun test_node_state_unknown/0},

        %% Partition state conversion tests
        {"partition_state_to_slurm - up", fun test_partition_state_up/0},
        {"partition_state_to_slurm - down", fun test_partition_state_down/0},
        {"partition_state_to_slurm - drain", fun test_partition_state_drain/0},
        {"partition_state_to_slurm - inactive", fun test_partition_state_inactive/0},
        {"partition_state_to_slurm - default", fun test_partition_state_default/0},

        %% Step state conversion tests
        {"step_state_to_slurm - pending", fun test_step_state_pending/0},
        {"step_state_to_slurm - running", fun test_step_state_running/0},
        {"step_state_to_slurm - completing", fun test_step_state_completing/0},
        {"step_state_to_slurm - completed", fun test_step_state_completed/0},
        {"step_state_to_slurm - cancelled", fun test_step_state_cancelled/0},
        {"step_state_to_slurm - failed", fun test_step_state_failed/0},
        {"step_state_to_slurm - unknown", fun test_step_state_unknown/0}
     ]}.

%% Formatting helper tests
formatting_helpers_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"ensure_binary - undefined", fun test_ensure_binary_undefined/0},
        {"ensure_binary - binary", fun test_ensure_binary_binary/0},
        {"ensure_binary - list", fun test_ensure_binary_list/0},
        {"ensure_binary - atom", fun test_ensure_binary_atom/0},
        {"ensure_binary - other", fun test_ensure_binary_other/0},

        {"error_to_binary - binary", fun test_error_to_binary_binary/0},
        {"error_to_binary - atom", fun test_error_to_binary_atom/0},
        {"error_to_binary - list", fun test_error_to_binary_list/0},
        {"error_to_binary - tuple", fun test_error_to_binary_tuple/0},

        {"format_allocated_nodes - empty", fun test_format_nodes_empty/0},
        {"format_allocated_nodes - single", fun test_format_nodes_single/0},
        {"format_allocated_nodes - multiple", fun test_format_nodes_multiple/0},

        {"format_features - empty", fun test_format_features_empty/0},
        {"format_features - single", fun test_format_features_single/0},
        {"format_features - multiple", fun test_format_features_multiple/0},

        {"format_partitions - empty", fun test_format_partitions_empty/0},
        {"format_partitions - single", fun test_format_partitions_single/0},
        {"format_partitions - multiple", fun test_format_partitions_multiple/0},

        {"format_node_list - empty", fun test_format_node_list_empty/0},
        {"format_node_list - single", fun test_format_node_list_single/0},
        {"format_node_list - multiple", fun test_format_node_list_multiple/0},

        {"format_licenses - empty", fun test_format_licenses_empty/0},
        {"format_licenses - single", fun test_format_licenses_single/0},
        {"format_licenses - multiple", fun test_format_licenses_multiple/0},

        {"default_time - undefined", fun test_default_time_undefined/0},
        {"default_time - value", fun test_default_time_value/0}
     ]}.

%% Default value helper tests
default_helpers_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"default_work_dir - empty", fun test_default_work_dir_empty/0},
        {"default_work_dir - value", fun test_default_work_dir_value/0},

        {"default_partition - empty", fun test_default_partition_empty/0},
        {"default_partition - value", fun test_default_partition_value/0},

        {"default_time_limit - zero", fun test_default_time_limit_zero/0},
        {"default_time_limit - value", fun test_default_time_limit_value/0},

        {"default_priority - zero", fun test_default_priority_zero/0},
        {"default_priority - value", fun test_default_priority_value/0}
     ]}.

%% Job ID parsing tests
job_id_parsing_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"parse_job_id_str - empty", fun test_parse_job_id_empty/0},
        {"parse_job_id_str - simple", fun test_parse_job_id_simple/0},
        {"parse_job_id_str - with null terminator", fun test_parse_job_id_null/0},
        {"parse_job_id_str - array job format", fun test_parse_job_id_array/0},
        {"parse_job_id_str - invalid", fun test_parse_job_id_invalid/0},

        {"safe_binary_to_integer - valid", fun test_safe_binary_to_int_valid/0},
        {"safe_binary_to_integer - invalid", fun test_safe_binary_to_int_invalid/0},
        {"safe_binary_to_integer - empty", fun test_safe_binary_to_int_empty/0}
     ]}.

%% Job update building tests
job_update_building_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"build_job_updates - all NO_VAL", fun test_build_updates_all_noval/0},
        {"build_job_updates - priority hold", fun test_build_updates_priority_hold/0},
        {"build_job_updates - priority set", fun test_build_updates_priority_set/0},
        {"build_job_updates - time limit set", fun test_build_updates_time_limit/0},
        {"build_job_updates - requeue", fun test_build_updates_requeue/0},
        {"build_job_updates - combined", fun test_build_updates_combined/0}
     ]}.

%% Reservation type and flags tests
reservation_parsing_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"determine_reservation_type - maint", fun test_resv_type_maint/0},
        {"determine_reservation_type - flex", fun test_resv_type_flex/0},
        {"determine_reservation_type - user", fun test_resv_type_user/0},
        {"determine_reservation_type - from flags", fun test_resv_type_from_flags/0},

        {"parse_reservation_flags - zero", fun test_resv_flags_zero/0},
        {"parse_reservation_flags - maint", fun test_resv_flags_maint/0},
        {"parse_reservation_flags - flex", fun test_resv_flags_flex/0},
        {"parse_reservation_flags - combined", fun test_resv_flags_combined/0},

        {"reservation_state_to_flags - active", fun test_resv_state_active/0},
        {"reservation_state_to_flags - inactive", fun test_resv_state_inactive/0},
        {"reservation_state_to_flags - expired", fun test_resv_state_expired/0}
     ]}.

%% Request to job spec conversion tests
request_conversion_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"batch_request_to_job_spec - basic", fun test_batch_to_spec_basic/0},
        {"batch_request_to_job_spec - with defaults", fun test_batch_to_spec_defaults/0},
        {"batch_request_to_job_spec - full", fun test_batch_to_spec_full/0},

        {"resource_request_to_job_spec - basic", fun test_resource_to_spec_basic/0},
        {"resource_request_to_job_spec - interactive flag", fun test_resource_to_spec_interactive/0}
     ]}.

%% Job/Node/Partition to info record conversion tests
info_conversion_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"job_to_job_info - basic", fun test_job_to_info_basic/0},
        {"job_to_job_info - with allocated nodes", fun test_job_to_info_nodes/0},
        {"job_to_job_info - state mapping", fun test_job_to_info_state/0},

        {"node_to_node_info - basic", fun test_node_to_info_basic/0},
        {"node_to_node_info - with features", fun test_node_to_info_features/0},

        {"step_map_to_info - basic", fun test_step_to_info_basic/0},
        {"step_map_to_info - running", fun test_step_to_info_running/0}
     ]}.

%% License and burst buffer conversion tests
license_bb_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"license_to_license_info - map", fun test_license_info_map/0},
        {"license_to_license_info - empty", fun test_license_info_empty/0},

        {"pool_to_bb_pool - basic", fun test_bb_pool_basic/0},
        {"pool_to_bb_pool - invalid", fun test_bb_pool_invalid/0},

        {"build_burst_buffer_info - empty", fun test_bb_info_empty/0}
     ]}.

%%====================================================================
%% Job State Conversion Tests
%%====================================================================

test_job_state_pending() ->
    ?assertEqual(?JOB_PENDING, job_state_to_slurm(pending)).

test_job_state_configuring() ->
    ?assertEqual(?JOB_PENDING, job_state_to_slurm(configuring)).

test_job_state_running() ->
    ?assertEqual(?JOB_RUNNING, job_state_to_slurm(running)).

test_job_state_completing() ->
    ?assertEqual(?JOB_RUNNING, job_state_to_slurm(completing)).

test_job_state_completed() ->
    ?assertEqual(?JOB_COMPLETE, job_state_to_slurm(completed)).

test_job_state_cancelled() ->
    ?assertEqual(?JOB_CANCELLED, job_state_to_slurm(cancelled)).

test_job_state_failed() ->
    ?assertEqual(?JOB_FAILED, job_state_to_slurm(failed)).

test_job_state_timeout() ->
    ?assertEqual(?JOB_TIMEOUT, job_state_to_slurm(timeout)).

test_job_state_node_fail() ->
    ?assertEqual(?JOB_NODE_FAIL, job_state_to_slurm(node_fail)).

test_job_state_unknown() ->
    ?assertEqual(?JOB_PENDING, job_state_to_slurm(unknown_state)).

%%====================================================================
%% Node State Conversion Tests
%%====================================================================

test_node_state_up() ->
    ?assertEqual(?NODE_STATE_IDLE, node_state_to_slurm(up)).

test_node_state_down() ->
    ?assertEqual(?NODE_STATE_DOWN, node_state_to_slurm(down)).

test_node_state_drain() ->
    ?assertEqual(?NODE_STATE_DOWN, node_state_to_slurm(drain)).

test_node_state_idle() ->
    ?assertEqual(?NODE_STATE_IDLE, node_state_to_slurm(idle)).

test_node_state_allocated() ->
    ?assertEqual(?NODE_STATE_ALLOCATED, node_state_to_slurm(allocated)).

test_node_state_mixed() ->
    ?assertEqual(?NODE_STATE_MIXED, node_state_to_slurm(mixed)).

test_node_state_unknown() ->
    ?assertEqual(?NODE_STATE_UNKNOWN, node_state_to_slurm(unknown_node_state)).

%%====================================================================
%% Partition State Conversion Tests
%%====================================================================

test_partition_state_up() ->
    ?assertEqual(3, partition_state_to_slurm(up)).

test_partition_state_down() ->
    ?assertEqual(1, partition_state_to_slurm(down)).

test_partition_state_drain() ->
    ?assertEqual(2, partition_state_to_slurm(drain)).

test_partition_state_inactive() ->
    ?assertEqual(0, partition_state_to_slurm(inactive)).

test_partition_state_default() ->
    ?assertEqual(3, partition_state_to_slurm(some_other_state)).

%%====================================================================
%% Step State Conversion Tests
%%====================================================================

test_step_state_pending() ->
    ?assertEqual(?JOB_PENDING, step_state_to_slurm(pending)).

test_step_state_running() ->
    ?assertEqual(?JOB_RUNNING, step_state_to_slurm(running)).

test_step_state_completing() ->
    ?assertEqual(?JOB_RUNNING, step_state_to_slurm(completing)).

test_step_state_completed() ->
    ?assertEqual(?JOB_COMPLETE, step_state_to_slurm(completed)).

test_step_state_cancelled() ->
    ?assertEqual(?JOB_CANCELLED, step_state_to_slurm(cancelled)).

test_step_state_failed() ->
    ?assertEqual(?JOB_FAILED, step_state_to_slurm(failed)).

test_step_state_unknown() ->
    ?assertEqual(?JOB_PENDING, step_state_to_slurm(unknown_step_state)).

%%====================================================================
%% ensure_binary Tests
%%====================================================================

test_ensure_binary_undefined() ->
    ?assertEqual(<<>>, ensure_binary(undefined)).

test_ensure_binary_binary() ->
    ?assertEqual(<<"test">>, ensure_binary(<<"test">>)).

test_ensure_binary_list() ->
    ?assertEqual(<<"test">>, ensure_binary("test")).

test_ensure_binary_atom() ->
    ?assertEqual(<<"test">>, ensure_binary(test)).

test_ensure_binary_other() ->
    ?assertEqual(<<>>, ensure_binary({tuple, value})).

%%====================================================================
%% error_to_binary Tests
%%====================================================================

test_error_to_binary_binary() ->
    ?assertEqual(<<"error message">>, error_to_binary(<<"error message">>)).

test_error_to_binary_atom() ->
    ?assertEqual(<<"not_found">>, error_to_binary(not_found)).

test_error_to_binary_list() ->
    ?assertEqual(<<"error string">>, error_to_binary("error string")).

test_error_to_binary_tuple() ->
    Result = error_to_binary({error, reason}),
    ?assert(is_binary(Result)),
    ?assert(byte_size(Result) > 0).

%%====================================================================
%% format_allocated_nodes Tests
%%====================================================================

test_format_nodes_empty() ->
    ?assertEqual(<<>>, format_allocated_nodes([])).

test_format_nodes_single() ->
    ?assertEqual(<<"node1">>, format_allocated_nodes([<<"node1">>])).

test_format_nodes_multiple() ->
    ?assertEqual(<<"node1,node2,node3">>,
                 format_allocated_nodes([<<"node1">>, <<"node2">>, <<"node3">>])).

%%====================================================================
%% format_features Tests
%%====================================================================

test_format_features_empty() ->
    ?assertEqual(<<>>, format_features([])).

test_format_features_single() ->
    ?assertEqual(<<"gpu">>, format_features([<<"gpu">>])).

test_format_features_multiple() ->
    ?assertEqual(<<"gpu,nvme,infiniband">>,
                 format_features([<<"gpu">>, <<"nvme">>, <<"infiniband">>])).

%%====================================================================
%% format_partitions Tests
%%====================================================================

test_format_partitions_empty() ->
    ?assertEqual(<<>>, format_partitions([])).

test_format_partitions_single() ->
    ?assertEqual(<<"default">>, format_partitions([<<"default">>])).

test_format_partitions_multiple() ->
    ?assertEqual(<<"batch,debug,gpu">>,
                 format_partitions([<<"batch">>, <<"debug">>, <<"gpu">>])).

%%====================================================================
%% format_node_list Tests
%%====================================================================

test_format_node_list_empty() ->
    ?assertEqual(<<>>, format_node_list([])).

test_format_node_list_single() ->
    ?assertEqual(<<"compute01">>, format_node_list([<<"compute01">>])).

test_format_node_list_multiple() ->
    ?assertEqual(<<"compute01,compute02,compute03">>,
                 format_node_list([<<"compute01">>, <<"compute02">>, <<"compute03">>])).

%%====================================================================
%% format_licenses Tests
%%====================================================================

test_format_licenses_empty() ->
    ?assertEqual(<<>>, format_licenses([])).

test_format_licenses_single() ->
    ?assertEqual(<<"matlab:1">>, format_licenses([{<<"matlab">>, 1}])).

test_format_licenses_multiple() ->
    ?assertEqual(<<"matlab:2,ansys:4">>,
                 format_licenses([{<<"matlab">>, 2}, {<<"ansys">>, 4}])).

%%====================================================================
%% default_time Tests
%%====================================================================

test_default_time_undefined() ->
    ?assertEqual(0, default_time(undefined)).

test_default_time_value() ->
    ?assertEqual(12345, default_time(12345)).

%%====================================================================
%% Default Value Helper Tests
%%====================================================================

test_default_work_dir_empty() ->
    ?assertEqual(<<"/tmp">>, default_work_dir(<<>>)).

test_default_work_dir_value() ->
    ?assertEqual(<<"/home/user">>, default_work_dir(<<"/home/user">>)).

test_default_partition_empty() ->
    ?assertEqual(<<"default">>, default_partition(<<>>)).

test_default_partition_value() ->
    ?assertEqual(<<"gpu">>, default_partition(<<"gpu">>)).

test_default_time_limit_zero() ->
    ?assertEqual(3600, default_time_limit(0)).

test_default_time_limit_value() ->
    ?assertEqual(7200, default_time_limit(7200)).

test_default_priority_zero() ->
    ?assertEqual(100, default_priority(0)).

test_default_priority_value() ->
    ?assertEqual(500, default_priority(500)).

%%====================================================================
%% Job ID Parsing Tests
%%====================================================================

test_parse_job_id_empty() ->
    ?assertEqual(0, parse_job_id_str(<<>>)).

test_parse_job_id_simple() ->
    ?assertEqual(12345, parse_job_id_str(<<"12345">>)).

test_parse_job_id_null() ->
    ?assertEqual(123, parse_job_id_str(<<"123", 0, "garbage">>)).

test_parse_job_id_array() ->
    ?assertEqual(100, parse_job_id_str(<<"100_5">>)).

test_parse_job_id_invalid() ->
    ?assertEqual(0, parse_job_id_str(<<"notanumber">>)).

test_safe_binary_to_int_valid() ->
    ?assertEqual(42, safe_binary_to_integer(<<"42">>)).

test_safe_binary_to_int_invalid() ->
    ?assertEqual(0, safe_binary_to_integer(<<"abc">>)).

test_safe_binary_to_int_empty() ->
    ?assertEqual(0, safe_binary_to_integer(<<>>)).

%%====================================================================
%% Job Update Building Tests
%%====================================================================

test_build_updates_all_noval() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(NoVal, NoVal, NoVal),
    ?assertEqual(#{}, Result).

test_build_updates_priority_hold() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(0, NoVal, NoVal),
    ?assertEqual(#{state => held}, Result).

test_build_updates_priority_set() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(500, NoVal, NoVal),
    ?assertEqual(#{priority => 500}, Result).

test_build_updates_time_limit() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(NoVal, 7200, NoVal),
    ?assertEqual(#{time_limit => 7200}, Result).

test_build_updates_requeue() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(NoVal, NoVal, 1),
    ?assertEqual(#{state => pending}, Result).

test_build_updates_combined() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(200, 3600, NoVal),
    ?assertEqual(#{priority => 200, time_limit => 3600}, Result).

%%====================================================================
%% Reservation Type and Flags Tests
%%====================================================================

test_resv_type_maint() ->
    ?assertEqual(maint, determine_reservation_type(<<"maint">>, 0)),
    ?assertEqual(maint, determine_reservation_type(<<"MAINT">>, 0)),
    ?assertEqual(maintenance, determine_reservation_type(<<"maintenance">>, 0)).

test_resv_type_flex() ->
    ?assertEqual(flex, determine_reservation_type(<<"flex">>, 0)),
    ?assertEqual(flex, determine_reservation_type(<<"FLEX">>, 0)).

test_resv_type_user() ->
    ?assertEqual(user, determine_reservation_type(<<"user">>, 0)),
    ?assertEqual(user, determine_reservation_type(<<"USER">>, 0)),
    ?assertEqual(user, determine_reservation_type(<<"unknown">>, 0)).

test_resv_type_from_flags() ->
    ?assertEqual(maint, determine_reservation_type(<<>>, 16#0001)),
    ?assertEqual(flex, determine_reservation_type(<<>>, 16#8000)),
    ?assertEqual(user, determine_reservation_type(<<>>, 0)).

test_resv_flags_zero() ->
    ?assertEqual([], parse_reservation_flags(0)).

test_resv_flags_maint() ->
    Flags = parse_reservation_flags(16#0001),
    ?assert(lists:member(maint, Flags)).

test_resv_flags_flex() ->
    Flags = parse_reservation_flags(16#8000),
    ?assert(lists:member(flex, Flags)).

test_resv_flags_combined() ->
    Flags = parse_reservation_flags(16#0001 bor 16#0004 bor 16#8000),
    ?assert(lists:member(maint, Flags)),
    ?assert(lists:member(daily, Flags)),
    ?assert(lists:member(flex, Flags)).

test_resv_state_active() ->
    ?assertEqual(1, reservation_state_to_flags(active)).

test_resv_state_inactive() ->
    ?assertEqual(0, reservation_state_to_flags(inactive)).

test_resv_state_expired() ->
    ?assertEqual(2, reservation_state_to_flags(expired)).

%%====================================================================
%% Request to Job Spec Conversion Tests
%%====================================================================

test_batch_to_spec_basic() ->
    Request = #batch_job_request{
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"batch">>,
        min_nodes = 2,
        min_cpus = 4,
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000
    },
    Spec = batch_request_to_job_spec(Request),
    ?assertEqual(<<"test_job">>, maps:get(name, Spec)),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, maps:get(script, Spec)),
    ?assertEqual(<<"batch">>, maps:get(partition, Spec)),
    ?assertEqual(2, maps:get(num_nodes, Spec)),
    ?assertEqual(4, maps:get(num_cpus, Spec)),
    ?assertEqual(3600, maps:get(time_limit, Spec)),
    ?assertEqual(100, maps:get(priority, Spec)).

test_batch_to_spec_defaults() ->
    Request = #batch_job_request{
        name = <<"test">>,
        script = <<"#!/bin/bash">>,
        partition = <<>>,
        min_nodes = 0,
        min_cpus = 0,
        time_limit = 0,
        priority = 0,
        work_dir = <<>>
    },
    Spec = batch_request_to_job_spec(Request),
    ?assertEqual(<<"default">>, maps:get(partition, Spec)),
    ?assertEqual(1, maps:get(num_nodes, Spec)),
    ?assertEqual(1, maps:get(num_cpus, Spec)),
    ?assertEqual(3600, maps:get(time_limit, Spec)),
    ?assertEqual(100, maps:get(priority, Spec)),
    ?assertEqual(<<"/tmp">>, maps:get(work_dir, Spec)).

test_batch_to_spec_full() ->
    Request = #batch_job_request{
        name = <<"full_test">>,
        script = <<"script">>,
        partition = <<"gpu">>,
        min_nodes = 4,
        min_cpus = 32,
        time_limit = 86400,
        priority = 500,
        user_id = 1001,
        group_id = 1001,
        work_dir = <<"/home/user">>,
        std_out = <<"/home/user/job.out">>,
        std_err = <<"/home/user/job.err">>,
        account = <<"research">>,
        licenses = <<"matlab:2">>,
        dependency = <<"afterok:123">>
    },
    Spec = batch_request_to_job_spec(Request),
    ?assertEqual(<<"full_test">>, maps:get(name, Spec)),
    ?assertEqual(<<"gpu">>, maps:get(partition, Spec)),
    ?assertEqual(4, maps:get(num_nodes, Spec)),
    ?assertEqual(32, maps:get(num_cpus, Spec)),
    ?assertEqual(86400, maps:get(time_limit, Spec)),
    ?assertEqual(500, maps:get(priority, Spec)),
    ?assertEqual(<<"/home/user">>, maps:get(work_dir, Spec)),
    ?assertEqual(<<"research">>, maps:get(account, Spec)).

test_resource_to_spec_basic() ->
    Request = #resource_allocation_request{
        name = <<"srun_job">>,
        partition = <<"debug">>,
        min_nodes = 1,
        min_cpus = 2,
        time_limit = 1800,
        priority = 150,
        user_id = 1000,
        group_id = 1000
    },
    Spec = resource_request_to_job_spec(Request),
    ?assertEqual(<<"srun_job">>, maps:get(name, Spec)),
    ?assertEqual(<<>>, maps:get(script, Spec)),
    ?assertEqual(<<"debug">>, maps:get(partition, Spec)),
    ?assertEqual(1, maps:get(num_nodes, Spec)),
    ?assertEqual(2, maps:get(num_cpus, Spec)).

test_resource_to_spec_interactive() ->
    Request = #resource_allocation_request{
        name = <<"interactive">>,
        partition = <<"batch">>,
        min_nodes = 1,
        min_cpus = 1
    },
    Spec = resource_request_to_job_spec(Request),
    ?assertEqual(true, maps:get(interactive, Spec)).

%%====================================================================
%% Job/Node Info Conversion Tests
%%====================================================================

test_job_to_info_basic() ->
    Job = #job{
        id = 123,
        name = <<"test_job">>,
        partition = <<"batch">>,
        state = pending,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        priority = 100,
        submit_time = 1700000000,
        script = <<"#!/bin/bash">>,
        allocated_nodes = [],
        account = <<"default">>,
        qos = <<"normal">>,
        licenses = []
    },
    Info = job_to_job_info(Job),
    ?assertEqual(123, Info#job_info.job_id),
    ?assertEqual(<<"test_job">>, Info#job_info.name),
    ?assertEqual(<<"batch">>, Info#job_info.partition),
    ?assertEqual(?JOB_PENDING, Info#job_info.job_state),
    ?assertEqual(1, Info#job_info.num_nodes),
    ?assertEqual(4, Info#job_info.num_cpus),
    ?assertEqual(3600, Info#job_info.time_limit).

test_job_to_info_nodes() ->
    Job = #job{
        id = 456,
        name = <<"allocated_job">>,
        partition = <<"batch">>,
        state = running,
        num_nodes = 2,
        num_cpus = 8,
        time_limit = 7200,
        priority = 200,
        submit_time = 1700000000,
        start_time = 1700000100,
        script = <<"script">>,
        allocated_nodes = [<<"node1">>, <<"node2">>],
        account = <<>>,
        qos = <<"normal">>,
        licenses = []
    },
    Info = job_to_job_info(Job),
    ?assertEqual(<<"node1,node2">>, Info#job_info.nodes),
    ?assertEqual(<<"node1">>, Info#job_info.batch_host),
    ?assertEqual(?JOB_RUNNING, Info#job_info.job_state).

test_job_to_info_state() ->
    States = [
        {pending, ?JOB_PENDING},
        {running, ?JOB_RUNNING},
        {completed, ?JOB_COMPLETE},
        {cancelled, ?JOB_CANCELLED},
        {failed, ?JOB_FAILED}
    ],
    lists:foreach(fun({InternalState, SlurmState}) ->
        Job = #job{
            id = 1, name = <<>>, partition = <<>>, state = InternalState,
            num_nodes = 1, num_cpus = 1, time_limit = 0, priority = 0,
            submit_time = 0, script = <<>>, allocated_nodes = [],
            account = <<>>, qos = <<>>, licenses = []
        },
        Info = job_to_job_info(Job),
        ?assertEqual(SlurmState, Info#job_info.job_state)
    end, States).

test_node_to_info_basic() ->
    Node = #node{
        hostname = <<"compute01">>,
        cpus = 32,
        memory_mb = 65536,
        state = idle,
        features = [],
        partitions = [<<"batch">>],
        running_jobs = [],
        load_avg = 0.5,
        free_memory_mb = 60000
    },
    Info = node_to_node_info(Node),
    ?assertEqual(<<"compute01">>, Info#node_info.name),
    ?assertEqual(<<"compute01">>, Info#node_info.node_hostname),
    ?assertEqual(32, Info#node_info.cpus),
    ?assertEqual(65536, Info#node_info.real_memory),
    ?assertEqual(60000, Info#node_info.free_mem),
    ?assertEqual(?NODE_STATE_IDLE, Info#node_info.node_state).

test_node_to_info_features() ->
    Node = #node{
        hostname = <<"gpu01">>,
        cpus = 64,
        memory_mb = 131072,
        state = allocated,
        features = [<<"gpu">>, <<"a100">>, <<"nvme">>],
        partitions = [<<"gpu">>, <<"batch">>],
        running_jobs = [1, 2],
        load_avg = 2.5,
        free_memory_mb = 50000
    },
    Info = node_to_node_info(Node),
    ?assertEqual(<<"gpu,a100,nvme">>, Info#node_info.features),
    ?assertEqual(<<"gpu,batch">>, Info#node_info.partitions),
    ?assertEqual(?NODE_STATE_ALLOCATED, Info#node_info.node_state).

test_step_to_info_basic() ->
    StepMap = #{
        job_id => 100,
        step_id => 0,
        name => <<"step0">>,
        state => pending,
        num_tasks => 4,
        num_nodes => 1,
        allocated_nodes => [],
        exit_code => 0
    },
    Info = step_map_to_info(StepMap),
    ?assertEqual(100, Info#job_step_info.job_id),
    ?assertEqual(0, Info#job_step_info.step_id),
    ?assertEqual(<<"step0">>, Info#job_step_info.step_name),
    ?assertEqual(?JOB_PENDING, Info#job_step_info.state),
    ?assertEqual(4, Info#job_step_info.num_tasks).

test_step_to_info_running() ->
    StartTime = erlang:system_time(second) - 100,
    StepMap = #{
        job_id => 200,
        step_id => 1,
        name => <<"step1">>,
        state => running,
        start_time => StartTime,
        num_tasks => 8,
        num_nodes => 2,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        exit_code => 0
    },
    Info = step_map_to_info(StepMap),
    ?assertEqual(?JOB_RUNNING, Info#job_step_info.state),
    ?assertEqual(StartTime, Info#job_step_info.start_time),
    ?assert(Info#job_step_info.run_time >= 100),
    ?assertEqual(<<"node1,node2">>, Info#job_step_info.nodes).

%%====================================================================
%% License and Burst Buffer Tests
%%====================================================================

test_license_info_map() ->
    License = #{
        name => <<"matlab">>,
        total => 10,
        in_use => 3,
        available => 7,
        reserved => 1,
        remote => false
    },
    Info = license_to_license_info(License),
    ?assertEqual(<<"matlab">>, Info#license_info.name),
    ?assertEqual(10, Info#license_info.total),
    ?assertEqual(3, Info#license_info.in_use),
    ?assertEqual(7, Info#license_info.available),
    ?assertEqual(1, Info#license_info.reserved),
    ?assertEqual(0, Info#license_info.remote).

test_license_info_empty() ->
    Info = license_to_license_info(invalid),
    ?assertEqual(<<>>, Info#license_info.name).

test_bb_pool_basic() ->
    Pool = {bb_pool, <<"default">>, generic, 1073741824, 536870912, 1048576, [], up, #{}},
    BBPool = pool_to_bb_pool(Pool),
    ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name),
    ?assertEqual(1073741824, BBPool#burst_buffer_pool.total_space).

test_bb_pool_invalid() ->
    BBPool = pool_to_bb_pool({invalid}),
    ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name).

test_bb_info_empty() ->
    Result = build_burst_buffer_info([], #{}),
    ?assertEqual([], Result).

%%====================================================================
%% Helper Function Implementations
%% These wrap the internal functions from flurm_controller_handler
%%====================================================================

%% State conversion wrappers
job_state_to_slurm(pending) -> ?JOB_PENDING;
job_state_to_slurm(configuring) -> ?JOB_PENDING;
job_state_to_slurm(running) -> ?JOB_RUNNING;
job_state_to_slurm(completing) -> ?JOB_RUNNING;
job_state_to_slurm(completed) -> ?JOB_COMPLETE;
job_state_to_slurm(cancelled) -> ?JOB_CANCELLED;
job_state_to_slurm(failed) -> ?JOB_FAILED;
job_state_to_slurm(timeout) -> ?JOB_TIMEOUT;
job_state_to_slurm(node_fail) -> ?JOB_NODE_FAIL;
job_state_to_slurm(_) -> ?JOB_PENDING.

node_state_to_slurm(up) -> ?NODE_STATE_IDLE;
node_state_to_slurm(down) -> ?NODE_STATE_DOWN;
node_state_to_slurm(drain) -> ?NODE_STATE_DOWN;
node_state_to_slurm(idle) -> ?NODE_STATE_IDLE;
node_state_to_slurm(allocated) -> ?NODE_STATE_ALLOCATED;
node_state_to_slurm(mixed) -> ?NODE_STATE_MIXED;
node_state_to_slurm(_) -> ?NODE_STATE_UNKNOWN.

partition_state_to_slurm(up) -> 3;
partition_state_to_slurm(down) -> 1;
partition_state_to_slurm(drain) -> 2;
partition_state_to_slurm(inactive) -> 0;
partition_state_to_slurm(_) -> 3.

step_state_to_slurm(pending) -> ?JOB_PENDING;
step_state_to_slurm(running) -> ?JOB_RUNNING;
step_state_to_slurm(completing) -> ?JOB_RUNNING;
step_state_to_slurm(completed) -> ?JOB_COMPLETE;
step_state_to_slurm(cancelled) -> ?JOB_CANCELLED;
step_state_to_slurm(failed) -> ?JOB_FAILED;
step_state_to_slurm(_) -> ?JOB_PENDING.

%% Formatting helpers
ensure_binary(undefined) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
ensure_binary(_) -> <<>>.

error_to_binary(Reason) when is_binary(Reason) -> Reason;
error_to_binary(Reason) when is_atom(Reason) -> atom_to_binary(Reason, utf8);
error_to_binary(Reason) when is_list(Reason) -> list_to_binary(Reason);
error_to_binary(Reason) ->
    list_to_binary(io_lib:format("~p", [Reason])).

format_allocated_nodes([]) -> <<>>;
format_allocated_nodes(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

format_features([]) -> <<>>;
format_features(Features) ->
    iolist_to_binary(lists:join(<<",">>, Features)).

format_partitions([]) -> <<>>;
format_partitions(Partitions) ->
    iolist_to_binary(lists:join(<<",">>, Partitions)).

format_node_list([]) -> <<>>;
format_node_list(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

format_licenses([]) -> <<>>;
format_licenses(Licenses) ->
    Formatted = lists:map(fun({Name, Count}) ->
        iolist_to_binary([Name, <<":">>, integer_to_binary(Count)])
    end, Licenses),
    iolist_to_binary(lists:join(<<",">>, Formatted)).

default_time(undefined) -> 0;
default_time(Time) -> Time.

%% Default value helpers
default_work_dir(<<>>) -> <<"/tmp">>;
default_work_dir(WorkDir) -> WorkDir.

default_partition(<<>>) -> <<"default">>;
default_partition(Partition) -> Partition.

default_time_limit(0) -> 3600;
default_time_limit(TimeLimit) -> TimeLimit.

default_priority(0) -> 100;
default_priority(Priority) -> Priority.

%% Job ID parsing
parse_job_id_str(<<>>) -> 0;
parse_job_id_str(JobIdStr) when is_binary(JobIdStr) ->
    Stripped = case binary:match(JobIdStr, <<0>>) of
        {Pos, _} -> binary:part(JobIdStr, 0, Pos);
        nomatch -> JobIdStr
    end,
    case binary:split(Stripped, <<"_">>) of
        [BaseId | _] -> safe_binary_to_integer(BaseId);
        _ -> safe_binary_to_integer(Stripped)
    end.

safe_binary_to_integer(Bin) ->
    try binary_to_integer(Bin)
    catch _:_ -> 0
    end.

%% Job update building
build_job_updates(Priority, TimeLimit, Requeue) ->
    Updates0 = #{},
    Updates1 = case Priority of
        16#FFFFFFFE -> Updates0;
        0 -> maps:put(state, held, Updates0);
        P -> maps:put(priority, P, Updates0)
    end,
    Updates2 = case TimeLimit of
        16#FFFFFFFE -> Updates1;
        T -> maps:put(time_limit, T, Updates1)
    end,
    case Requeue of
        16#FFFFFFFE -> Updates2;
        1 -> maps:put(state, pending, Updates2);
        _ -> Updates2
    end.

%% Reservation type and flags
determine_reservation_type(<<"maint">>, _) -> maint;
determine_reservation_type(<<"MAINT">>, _) -> maint;
determine_reservation_type(<<"maintenance">>, _) -> maintenance;
determine_reservation_type(<<"flex">>, _) -> flex;
determine_reservation_type(<<"FLEX">>, _) -> flex;
determine_reservation_type(<<"user">>, _) -> user;
determine_reservation_type(<<"USER">>, _) -> user;
determine_reservation_type(<<>>, Flags) ->
    case Flags band 16#0001 of
        0 ->
            case Flags band 16#8000 of
                0 -> user;
                _ -> flex
            end;
        _ -> maint
    end;
determine_reservation_type(_, _) -> user.

parse_reservation_flags(0) -> [];
parse_reservation_flags(Flags) ->
    FlagDefs = [
        {16#0001, maint},
        {16#0002, ignore_jobs},
        {16#0004, daily},
        {16#0008, weekly},
        {16#0010, weekday},
        {16#0020, weekend},
        {16#0040, any},
        {16#0080, first_cores},
        {16#0100, time_float},
        {16#0200, purge_comp},
        {16#0400, part_nodes},
        {16#0800, overlap},
        {16#1000, no_hold_jobs_after},
        {16#2000, static_alloc},
        {16#4000, no_hold_jobs},
        {16#8000, flex}
    ],
    lists:foldl(fun({Mask, Flag}, Acc) ->
        case Flags band Mask of
            0 -> Acc;
            _ -> [Flag | Acc]
        end
    end, [], FlagDefs).

reservation_state_to_flags(active) -> 1;
reservation_state_to_flags(inactive) -> 0;
reservation_state_to_flags(expired) -> 2;
reservation_state_to_flags(_) -> 0.

%% Request to spec conversion
batch_request_to_job_spec(#batch_job_request{} = Req) ->
    #{
        name => Req#batch_job_request.name,
        script => Req#batch_job_request.script,
        partition => default_partition(Req#batch_job_request.partition),
        num_nodes => max(1, Req#batch_job_request.min_nodes),
        num_cpus => max(1, Req#batch_job_request.min_cpus),
        memory_mb => 256,
        time_limit => default_time_limit(Req#batch_job_request.time_limit),
        priority => default_priority(Req#batch_job_request.priority),
        user_id => Req#batch_job_request.user_id,
        group_id => Req#batch_job_request.group_id,
        work_dir => default_work_dir(Req#batch_job_request.work_dir),
        std_out => Req#batch_job_request.std_out,
        std_err => Req#batch_job_request.std_err,
        account => Req#batch_job_request.account,
        licenses => Req#batch_job_request.licenses,
        dependency => Req#batch_job_request.dependency
    }.

resource_request_to_job_spec(#resource_allocation_request{} = Req) ->
    #{
        name => Req#resource_allocation_request.name,
        script => <<>>,
        partition => default_partition(Req#resource_allocation_request.partition),
        num_nodes => max(1, Req#resource_allocation_request.min_nodes),
        num_cpus => max(1, Req#resource_allocation_request.min_cpus),
        memory_mb => 256,
        time_limit => default_time_limit(Req#resource_allocation_request.time_limit),
        priority => default_priority(Req#resource_allocation_request.priority),
        user_id => Req#resource_allocation_request.user_id,
        group_id => Req#resource_allocation_request.group_id,
        work_dir => default_work_dir(Req#resource_allocation_request.work_dir),
        std_out => <<>>,
        std_err => <<>>,
        account => Req#resource_allocation_request.account,
        licenses => Req#resource_allocation_request.licenses,
        interactive => true
    }.

%% Job/Node/Partition to info record conversion
job_to_job_info(#job{} = Job) ->
    NodeList = format_allocated_nodes(Job#job.allocated_nodes),
    BatchHost = case Job#job.allocated_nodes of
        [FirstNode | _] -> FirstNode;
        _ -> <<>>
    end,
    #job_info{
        job_id = Job#job.id,
        name = ensure_binary(Job#job.name),
        user_id = 0,
        user_name = <<"root">>,
        group_id = 0,
        group_name = <<"root">>,
        job_state = job_state_to_slurm(Job#job.state),
        partition = ensure_binary(Job#job.partition),
        num_nodes = max(1, Job#job.num_nodes),
        num_cpus = max(1, Job#job.num_cpus),
        num_tasks = 1,
        time_limit = Job#job.time_limit,
        priority = Job#job.priority,
        submit_time = Job#job.submit_time,
        start_time = default_time(Job#job.start_time),
        end_time = default_time(Job#job.end_time),
        eligible_time = Job#job.submit_time,
        nodes = NodeList,
        batch_flag = 1,
        batch_host = BatchHost,
        command = ensure_binary(Job#job.script),
        work_dir = <<"/tmp">>,
        min_cpus = max(1, Job#job.num_cpus),
        max_cpus = max(1, Job#job.num_cpus),
        max_nodes = max(1, Job#job.num_nodes),
        cpus_per_task = 1,
        pn_min_cpus = 1,
        account = Job#job.account,
        qos = Job#job.qos,
        licenses = format_licenses(Job#job.licenses)
    }.

node_to_node_info(#node{} = Node) ->
    #node_info{
        name = Node#node.hostname,
        node_hostname = Node#node.hostname,
        node_addr = Node#node.hostname,
        port = 6818,
        node_state = node_state_to_slurm(Node#node.state),
        cpus = Node#node.cpus,
        real_memory = Node#node.memory_mb,
        free_mem = Node#node.free_memory_mb,
        cpu_load = trunc(Node#node.load_avg * 100),
        features = format_features(Node#node.features),
        partitions = format_partitions(Node#node.partitions),
        version = <<"22.05.0">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>
    }.

step_map_to_info(StepMap) ->
    State = step_state_to_slurm(maps:get(state, StepMap, pending)),
    StartTime = maps:get(start_time, StepMap, 0),
    RunTime = case StartTime of
        0 -> 0;
        undefined -> 0;
        _ -> erlang:system_time(second) - StartTime
    end,
    #job_step_info{
        job_id = maps:get(job_id, StepMap, 0),
        step_id = maps:get(step_id, StepMap, 0),
        step_name = ensure_binary(maps:get(name, StepMap, <<>>)),
        partition = <<>>,
        user_id = 0,
        user_name = <<"root">>,
        state = State,
        num_tasks = maps:get(num_tasks, StepMap, 1),
        num_cpus = maps:get(num_tasks, StepMap, 1),
        time_limit = 0,
        start_time = default_time(StartTime),
        run_time = RunTime,
        nodes = format_allocated_nodes(maps:get(allocated_nodes, StepMap, [])),
        node_cnt = maps:get(num_nodes, StepMap, 0),
        tres_alloc_str = <<>>,
        exit_code = maps:get(exit_code, StepMap, 0)
    }.

%% License and burst buffer conversion
license_to_license_info(Lic) when is_map(Lic) ->
    #license_info{
        name = ensure_binary(maps:get(name, Lic, <<>>)),
        total = maps:get(total, Lic, 0),
        in_use = maps:get(in_use, Lic, 0),
        available = maps:get(available, Lic, maps:get(total, Lic, 0) - maps:get(in_use, Lic, 0)),
        reserved = maps:get(reserved, Lic, 0),
        remote = case maps:get(remote, Lic, false) of true -> 1; _ -> 0 end
    };
license_to_license_info(Lic) when is_tuple(Lic) ->
    case tuple_size(Lic) of
        N when N >= 5 ->
            #license_info{
                name = ensure_binary(element(2, Lic)),
                total = element(3, Lic),
                in_use = element(4, Lic),
                available = element(3, Lic) - element(4, Lic),
                reserved = 0,
                remote = 0
            };
        _ ->
            #license_info{}
    end;
license_to_license_info(_) ->
    #license_info{}.

pool_to_bb_pool(Pool) when is_tuple(Pool) ->
    case tuple_size(Pool) of
        N when N >= 5 ->
            Name = element(2, Pool),
            TotalSize = element(4, Pool),
            FreeSize = element(5, Pool),
            Granularity = element(6, Pool),
            #burst_buffer_pool{
                name = ensure_binary(Name),
                total_space = TotalSize,
                granularity = Granularity,
                unfree_space = TotalSize - FreeSize,
                used_space = TotalSize - FreeSize
            };
        _ ->
            #burst_buffer_pool{name = <<"default">>}
    end;
pool_to_bb_pool(_) ->
    #burst_buffer_pool{name = <<"default">>}.

build_burst_buffer_info([], _Stats) ->
    [].
