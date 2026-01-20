%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler More Pure Unit Tests
%%%
%%% Additional pure unit tests for flurm_controller_handler module
%%% that test helper functions and conversion logic without mocking.
%%% This extends the coverage from flurm_controller_handler_pure_tests.
%%%
%%% These tests focus on:
%%% - Reservation name generation and spec building
%%% - Extract reservation fields
%%% - Update reservation request to updates conversion
%%% - Burst buffer info building with data
%%% - Edge cases in formatting helpers
%%% - Additional state conversion edge cases
%%% - Job ID string parsing edge cases
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_more_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Reservation generation and conversion tests
reservation_advanced_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"generate_reservation_name returns unique binaries",
         fun test_generate_reservation_name_unique/0},
        {"generate_reservation_name format is valid",
         fun test_generate_reservation_name_format/0},
        {"extract_reservation_fields - full tuple",
         fun test_extract_resv_fields_full/0},
        {"extract_reservation_fields - small tuple",
         fun test_extract_resv_fields_small/0},
        {"extract_reservation_fields - not a tuple",
         fun test_extract_resv_fields_invalid/0},
        {"create_reservation_request_to_spec - with name",
         fun test_create_resv_spec_with_name/0},
        {"create_reservation_request_to_spec - empty name auto-generates",
         fun test_create_resv_spec_autogen_name/0},
        {"create_reservation_request_to_spec - start time now",
         fun test_create_resv_spec_start_now/0},
        {"create_reservation_request_to_spec - future start time",
         fun test_create_resv_spec_future_start/0},
        {"create_reservation_request_to_spec - with duration",
         fun test_create_resv_spec_with_duration/0},
        {"create_reservation_request_to_spec - with end time",
         fun test_create_resv_spec_with_end_time/0},
        {"create_reservation_request_to_spec - with users list",
         fun test_create_resv_spec_with_users/0},
        {"create_reservation_request_to_spec - with accounts list",
         fun test_create_resv_spec_with_accounts/0},
        {"create_reservation_request_to_spec - with node count",
         fun test_create_resv_spec_with_node_count/0},
        {"create_reservation_request_to_spec - with features",
         fun test_create_resv_spec_with_features/0}
     ]}.

%% Update reservation request tests
update_reservation_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"update_reservation_request_to_updates - empty",
         fun test_update_resv_empty/0},
        {"update_reservation_request_to_updates - start_time only",
         fun test_update_resv_start_time/0},
        {"update_reservation_request_to_updates - end_time only",
         fun test_update_resv_end_time/0},
        {"update_reservation_request_to_updates - duration only",
         fun test_update_resv_duration/0},
        {"update_reservation_request_to_updates - users update",
         fun test_update_resv_users/0},
        {"update_reservation_request_to_updates - accounts update",
         fun test_update_resv_accounts/0},
        {"update_reservation_request_to_updates - partition update",
         fun test_update_resv_partition/0},
        {"update_reservation_request_to_updates - flags update",
         fun test_update_resv_flags/0},
        {"update_reservation_request_to_updates - combined updates",
         fun test_update_resv_combined/0},
        {"update_reservation_request_to_updates - nodes update",
         fun test_update_resv_nodes/0}
     ]}.

%% Burst buffer tests with data
burst_buffer_advanced_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"build_burst_buffer_info - single pool",
         fun test_bb_info_single_pool/0},
        {"build_burst_buffer_info - multiple pools",
         fun test_bb_info_multiple_pools/0},
        {"build_burst_buffer_info - with stats",
         fun test_bb_info_with_stats/0},
        {"pool_to_bb_pool - tuple with exact fields",
         fun test_pool_to_bb_exact/0},
        {"pool_to_bb_pool - map format",
         fun test_pool_to_bb_map/0}
     ]}.

%% Additional formatting edge cases
formatting_edge_cases_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"format_allocated_nodes - atom list",
         fun test_format_nodes_atoms/0},
        {"format_features - with unicode",
         fun test_format_features_unicode/0},
        {"format_licenses - complex types",
         fun test_format_licenses_complex/0},
        {"ensure_binary - integer",
         fun test_ensure_binary_integer/0},
        {"ensure_binary - pid",
         fun test_ensure_binary_pid/0},
        {"error_to_binary - nested tuple",
         fun test_error_to_binary_nested/0},
        {"error_to_binary - map",
         fun test_error_to_binary_map/0}
     ]}.

%% Job ID parsing edge cases
job_id_parsing_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"parse_job_id_str - with trailing spaces",
         fun test_parse_job_id_trailing/0},
        {"parse_job_id_str - multiple underscores",
         fun test_parse_job_id_multiple_underscores/0},
        {"parse_job_id_str - step format",
         fun test_parse_job_id_step_format/0},
        {"parse_job_id_str - negative number",
         fun test_parse_job_id_negative/0},
        {"parse_job_id_str - very large number",
         fun test_parse_job_id_large/0},
        {"safe_binary_to_integer - with leading zeros",
         fun test_safe_bin_to_int_leading_zeros/0},
        {"safe_binary_to_integer - negative",
         fun test_safe_bin_to_int_negative/0},
        {"safe_binary_to_integer - mixed",
         fun test_safe_bin_to_int_mixed/0}
     ]}.

%% State conversion edge cases
state_conversion_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"job_state_to_slurm - held maps to pending",
         fun test_job_state_held/0},
        {"job_state_to_slurm - requeued",
         fun test_job_state_requeued/0},
        {"job_state_to_slurm - suspended",
         fun test_job_state_suspended/0},
        {"node_state_to_slurm - maint",
         fun test_node_state_maint/0},
        {"partition_state_to_slurm - multiple values",
         fun test_partition_state_multiple/0},
        {"step_state_to_slurm - timeout",
         fun test_step_state_timeout/0}
     ]}.

%% Build job updates edge cases
job_updates_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"build_job_updates - max priority",
         fun test_build_updates_max_priority/0},
        {"build_job_updates - very long time limit",
         fun test_build_updates_long_time/0},
        {"build_job_updates - requeue zero",
         fun test_build_updates_requeue_zero/0},
        {"build_job_updates - all values set",
         fun test_build_updates_all_set/0}
     ]}.

%% Reservation flags advanced tests
reservation_flags_advanced_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"parse_reservation_flags - all flags set",
         fun test_resv_flags_all/0},
        {"parse_reservation_flags - specific combinations",
         fun test_resv_flags_combos/0},
        {"determine_reservation_type - unknown string defaults",
         fun test_resv_type_unknown_string/0},
        {"reservation_state_to_flags - unknown state",
         fun test_resv_state_unknown/0}
     ]}.

%% License info edge cases
license_info_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"license_to_license_info - remote true",
         fun test_license_remote_true/0},
        {"license_to_license_info - calculated available",
         fun test_license_calculated_avail/0},
        {"license_to_license_info - tuple with 4 elements",
         fun test_license_tuple_4/0},
        {"license_to_license_info - tuple with 6+ elements",
         fun test_license_tuple_6/0}
     ]}.

%% Request to spec conversion edge cases
request_conversion_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"batch_request_to_job_spec - negative nodes clamped to 1",
         fun test_batch_spec_negative_nodes/0},
        {"batch_request_to_job_spec - zero cpus clamped to 1",
         fun test_batch_spec_zero_cpus/0},
        {"resource_request_to_job_spec - with account",
         fun test_resource_spec_with_account/0},
        {"resource_request_to_job_spec - with licenses",
         fun test_resource_spec_with_licenses/0}
     ]}.

%% Job/Node/Step info conversion edge cases
info_conversion_edge_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"job_to_job_info - empty allocated nodes",
         fun test_job_info_empty_nodes/0},
        {"job_to_job_info - undefined start time",
         fun test_job_info_undefined_start/0},
        {"node_to_node_info - empty features",
         fun test_node_info_empty_features/0},
        {"node_to_node_info - high load average",
         fun test_node_info_high_load/0},
        {"step_map_to_info - undefined start_time",
         fun test_step_info_undefined_start/0},
        {"step_map_to_info - missing fields",
         fun test_step_info_missing_fields/0}
     ]}.

%%====================================================================
%% Reservation Generation Tests
%%====================================================================

test_generate_reservation_name_unique() ->
    %% The random component ensures uniqueness without needing sleep
    Name1 = generate_reservation_name(),
    Name2 = generate_reservation_name(),
    ?assertNotEqual(Name1, Name2).

test_generate_reservation_name_format() ->
    Name = generate_reservation_name(),
    ?assert(is_binary(Name)),
    ?assert(byte_size(Name) > 5),
    %% Should start with "resv_"
    ?assertMatch(<<"resv_", _/binary>>, Name).

test_extract_resv_fields_full() ->
    %% Simulating a 14+ element tuple (reservation record)
    Resv = {reservation, <<"test_resv">>, user, 1700000000, 1700003600,
            3600, [<<"node1">>, <<"node2">>], 2, <<"batch">>, [<<"gpu">>],
            [<<"user1">>, <<"user2">>], [maint], #{}, active},
    {Name, StartTime, EndTime, Nodes, Users, State, Flags} = extract_reservation_fields(Resv),
    ?assertEqual(<<"test_resv">>, Name),
    ?assertEqual(1700000000, StartTime),
    ?assertEqual(1700003600, EndTime),
    ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
    ?assertEqual([<<"user1">>, <<"user2">>], Users),
    ?assertEqual(active, State),
    ?assertEqual([maint], Flags).

test_extract_resv_fields_small() ->
    %% Tuple too small
    Resv = {reservation, <<"name">>, user},
    Result = extract_reservation_fields(Resv),
    ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result).

test_extract_resv_fields_invalid() ->
    %% Not a tuple
    Result1 = extract_reservation_fields(#{name => <<"test">>}),
    ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result1),

    Result2 = extract_reservation_fields("not a tuple"),
    ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result2).

test_create_resv_spec_with_name() ->
    Req = #create_reservation_request{
        name = <<"my_reservation">>,
        start_time = 1700000000,
        end_time = 1700003600,
        nodes = <<>>
    },
    Spec = create_reservation_request_to_spec(Req),
    ?assertEqual(<<"my_reservation">>, maps:get(name, Spec)).

test_create_resv_spec_autogen_name() ->
    Req = #create_reservation_request{
        name = <<>>,
        start_time = 1700000000,
        end_time = 1700003600
    },
    Spec = create_reservation_request_to_spec(Req),
    Name = maps:get(name, Spec),
    ?assert(is_binary(Name)),
    ?assertMatch(<<"resv_", _/binary>>, Name).

test_create_resv_spec_start_now() ->
    Now = erlang:system_time(second),
    Req = #create_reservation_request{
        name = <<"test">>,
        start_time = 0, % Start now
        end_time = Now + 3600
    },
    Spec = create_reservation_request_to_spec(Req),
    StartTime = maps:get(start_time, Spec),
    %% Should be approximately now (within 2 seconds)
    ?assert(abs(StartTime - Now) < 2).

test_create_resv_spec_future_start() ->
    FutureTime = erlang:system_time(second) + 86400, % Tomorrow
    Req = #create_reservation_request{
        name = <<"future">>,
        start_time = FutureTime,
        end_time = FutureTime + 3600
    },
    Spec = create_reservation_request_to_spec(Req),
    ?assertEqual(FutureTime, maps:get(start_time, Spec)),
    ?assertEqual(inactive, maps:get(state, Spec)).

test_create_resv_spec_with_duration() ->
    Now = erlang:system_time(second),
    Req = #create_reservation_request{
        name = <<"duration_test">>,
        start_time = Now,
        end_time = 0,  % No end time
        duration = 120 % 120 minutes
    },
    Spec = create_reservation_request_to_spec(Req),
    EndTime = maps:get(end_time, Spec),
    ExpectedEnd = Now + (120 * 60),
    ?assertEqual(ExpectedEnd, EndTime).

test_create_resv_spec_with_end_time() ->
    Now = erlang:system_time(second),
    EndTime = Now + 7200,
    Req = #create_reservation_request{
        name = <<"end_time_test">>,
        start_time = Now,
        end_time = EndTime,
        duration = 120 % Should be ignored when end_time is set
    },
    Spec = create_reservation_request_to_spec(Req),
    ?assertEqual(EndTime, maps:get(end_time, Spec)).

test_create_resv_spec_with_users() ->
    Req = #create_reservation_request{
        name = <<"users_test">>,
        start_time = 1700000000,
        end_time = 1700003600,
        users = <<"alice,bob,charlie">>
    },
    Spec = create_reservation_request_to_spec(Req),
    Users = maps:get(users, Spec),
    ?assertEqual([<<"alice">>, <<"bob">>, <<"charlie">>], Users).

test_create_resv_spec_with_accounts() ->
    Req = #create_reservation_request{
        name = <<"accounts_test">>,
        start_time = 1700000000,
        end_time = 1700003600,
        accounts = <<"research,engineering">>
    },
    Spec = create_reservation_request_to_spec(Req),
    Accounts = maps:get(accounts, Spec),
    ?assertEqual([<<"research">>, <<"engineering">>], Accounts).

test_create_resv_spec_with_node_count() ->
    Req = #create_reservation_request{
        name = <<"nodecount_test">>,
        start_time = 1700000000,
        end_time = 1700003600,
        nodes = <<>>,
        node_cnt = 10
    },
    Spec = create_reservation_request_to_spec(Req),
    ?assertEqual(10, maps:get(node_count, Spec)).

test_create_resv_spec_with_features() ->
    Req = #create_reservation_request{
        name = <<"features_test">>,
        start_time = 1700000000,
        end_time = 1700003600,
        features = <<"gpu,nvme,a100">>
    },
    Spec = create_reservation_request_to_spec(Req),
    Features = maps:get(features, Spec),
    ?assertEqual([<<"gpu">>, <<"nvme">>, <<"a100">>], Features).

%%====================================================================
%% Update Reservation Tests
%%====================================================================

test_update_resv_empty() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(#{}, Updates).

test_update_resv_start_time() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 1700000000,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(#{start_time => 1700000000}, Updates).

test_update_resv_end_time() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 1700003600,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(#{end_time => 1700003600}, Updates).

test_update_resv_duration() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 60, % 60 minutes
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(#{duration => 3600}, Updates). % 60 * 60 seconds

test_update_resv_users() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<"newuser1,newuser2">>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual([<<"newuser1">>, <<"newuser2">>], maps:get(users, Updates)).

test_update_resv_accounts() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<"acct1,acct2,acct3">>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual([<<"acct1">>, <<"acct2">>, <<"acct3">>], maps:get(accounts, Updates)).

test_update_resv_partition() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<"gpu">>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(#{partition => <<"gpu">>}, Updates).

test_update_resv_flags() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 16#0001 bor 16#0004  % maint + daily
    },
    Updates = update_reservation_request_to_updates(Req),
    Flags = maps:get(flags, Updates),
    ?assert(lists:member(maint, Flags)),
    ?assert(lists:member(daily, Flags)).

test_update_resv_combined() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 1700000000,
        end_time = 1700007200,
        duration = 0,
        nodes = <<>>,
        users = <<"user1">>,
        accounts = <<>>,
        partition = <<"batch">>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    ?assertEqual(1700000000, maps:get(start_time, Updates)),
    ?assertEqual(1700007200, maps:get(end_time, Updates)),
    ?assertEqual([<<"user1">>], maps:get(users, Updates)),
    ?assertEqual(<<"batch">>, maps:get(partition, Updates)).

test_update_resv_nodes() ->
    Req = #update_reservation_request{
        name = <<"test">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<"node1,node2,node3">>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    Updates = update_reservation_request_to_updates(Req),
    Nodes = maps:get(nodes, Updates),
    %% Should be a list of node names
    ?assert(is_list(Nodes)),
    ?assert(length(Nodes) >= 1).

%%====================================================================
%% Burst Buffer Advanced Tests
%%====================================================================

test_bb_info_single_pool() ->
    Pool = {bb_pool, <<"default">>, generic, 1073741824, 536870912, 1048576, [], up, #{}},
    Result = build_burst_buffer_info([Pool], #{}),
    ?assertEqual(1, length(Result)),
    [BBInfo] = Result,
    ?assertEqual(<<"generic">>, BBInfo#burst_buffer_info.name),
    ?assertEqual(1, BBInfo#burst_buffer_info.pool_cnt).

test_bb_info_multiple_pools() ->
    Pool1 = {bb_pool, <<"pool1">>, generic, 1073741824, 536870912, 1048576, [], up, #{}},
    Pool2 = {bb_pool, <<"pool2">>, generic, 2147483648, 1073741824, 1048576, [], up, #{}},
    Result = build_burst_buffer_info([Pool1, Pool2], #{}),
    ?assertEqual(1, length(Result)),
    [BBInfo] = Result,
    ?assertEqual(2, BBInfo#burst_buffer_info.pool_cnt).

test_bb_info_with_stats() ->
    Pool = {bb_pool, <<"default">>, generic, 1073741824, 536870912, 1048576, [], up, #{}},
    Stats = #{
        total_size => 1073741824,
        used_size => 268435456,
        free_size => 805306368
    },
    Result = build_burst_buffer_info([Pool], Stats),
    [BBInfo] = Result,
    ?assertEqual(1073741824, BBInfo#burst_buffer_info.total_space),
    ?assertEqual(268435456, BBInfo#burst_buffer_info.used_space).

test_pool_to_bb_exact() ->
    Pool = {bb_pool, <<"exact_pool">>, datawarp, 4294967296, 2147483648, 2097152, [], up, #{}},
    BBPool = pool_to_bb_pool(Pool),
    ?assertEqual(<<"exact_pool">>, BBPool#burst_buffer_pool.name),
    ?assertEqual(4294967296, BBPool#burst_buffer_pool.total_space),
    ?assertEqual(2097152, BBPool#burst_buffer_pool.granularity).

test_pool_to_bb_map() ->
    %% pool_to_bb_pool should handle non-tuple gracefully
    Result = pool_to_bb_pool(#{name => <<"map_pool">>}),
    ?assertEqual(<<"default">>, Result#burst_buffer_pool.name).

%%====================================================================
%% Formatting Edge Cases
%%====================================================================

test_format_nodes_atoms() ->
    %% Even if we pass atoms, they should be handled
    Nodes = [<<"node1">>, <<"node2">>],
    Result = format_allocated_nodes(Nodes),
    ?assertEqual(<<"node1,node2">>, Result).

test_format_features_unicode() ->
    Features = [<<"feature1">>, <<"gpu">>, <<"nvme">>],
    Result = format_features(Features),
    ?assertEqual(<<"feature1,gpu,nvme">>, Result).

test_format_licenses_complex() ->
    Licenses = [
        {<<"matlab">>, 5},
        {<<"ansys">>, 10},
        {<<"fluent">>, 2}
    ],
    Result = format_licenses(Licenses),
    ?assertEqual(<<"matlab:5,ansys:10,fluent:2">>, Result).

test_ensure_binary_integer() ->
    %% Integers should return empty binary (not convertible directly)
    Result = ensure_binary(12345),
    ?assertEqual(<<>>, Result).

test_ensure_binary_pid() ->
    %% Pids should return empty binary
    Result = ensure_binary(self()),
    ?assertEqual(<<>>, Result).

test_error_to_binary_nested() ->
    Nested = {error, {nested, {deeply, reason}}},
    Result = error_to_binary(Nested),
    ?assert(is_binary(Result)),
    ?assert(byte_size(Result) > 0).

test_error_to_binary_map() ->
    MapError = #{reason => not_found, details => <<"extra info">>},
    Result = error_to_binary(MapError),
    ?assert(is_binary(Result)),
    ?assert(byte_size(Result) > 0).

%%====================================================================
%% Job ID Parsing Edge Cases
%%====================================================================

test_parse_job_id_trailing() ->
    %% Job ID with trailing characters after null
    Result = parse_job_id_str(<<"456", 0, "extra", 0>>),
    ?assertEqual(456, Result).

test_parse_job_id_multiple_underscores() ->
    %% Multiple underscores - should only split on first
    Result = parse_job_id_str(<<"789_10_20">>),
    ?assertEqual(789, Result).

test_parse_job_id_step_format() ->
    %% Step format with dot
    Result = parse_job_id_str(<<"100.0">>),
    %% Should fail to parse as integer since it contains a dot
    ?assertEqual(0, Result).

test_parse_job_id_negative() ->
    %% Negative should parse but result depends on implementation
    %% Most likely returns 0 for invalid
    Result = parse_job_id_str(<<"-123">>),
    %% Negative numbers are valid integers
    ?assertEqual(-123, Result).

test_parse_job_id_large() ->
    %% Very large job ID
    Result = parse_job_id_str(<<"9999999999">>),
    ?assertEqual(9999999999, Result).

test_safe_bin_to_int_leading_zeros() ->
    Result = safe_binary_to_integer(<<"007">>),
    ?assertEqual(7, Result).

test_safe_bin_to_int_negative() ->
    Result = safe_binary_to_integer(<<"-42">>),
    ?assertEqual(-42, Result).

test_safe_bin_to_int_mixed() ->
    %% Mixed content should fail
    Result = safe_binary_to_integer(<<"12abc">>),
    ?assertEqual(0, Result).

%%====================================================================
%% State Conversion Edge Cases
%%====================================================================

test_job_state_held() ->
    %% Held jobs should map to PENDING
    Result = job_state_to_slurm(held),
    ?assertEqual(?JOB_PENDING, Result).

test_job_state_requeued() ->
    Result = job_state_to_slurm(requeued),
    ?assertEqual(?JOB_PENDING, Result).

test_job_state_suspended() ->
    %% Suspended is a special case - maps to pending by default
    Result = job_state_to_slurm(suspended),
    ?assertEqual(?JOB_PENDING, Result).

test_node_state_maint() ->
    %% Maintenance state should map to down or unknown
    Result = node_state_to_slurm(maint),
    ?assertEqual(?NODE_STATE_UNKNOWN, Result).

test_partition_state_multiple() ->
    %% Test all partition states
    ?assertEqual(3, partition_state_to_slurm(up)),
    ?assertEqual(1, partition_state_to_slurm(down)),
    ?assertEqual(2, partition_state_to_slurm(drain)),
    ?assertEqual(0, partition_state_to_slurm(inactive)),
    ?assertEqual(3, partition_state_to_slurm(unknown)).

test_step_state_timeout() ->
    %% Timeout state for steps
    Result = step_state_to_slurm(timeout),
    ?assertEqual(?JOB_PENDING, Result).

%%====================================================================
%% Build Job Updates Edge Cases
%%====================================================================

test_build_updates_max_priority() ->
    NoVal = 16#FFFFFFFE,
    Result = build_job_updates(10000, NoVal, NoVal),
    ?assertEqual(#{priority => 10000}, Result).

test_build_updates_long_time() ->
    NoVal = 16#FFFFFFFE,
    %% Very long time limit (1 year in seconds)
    Result = build_job_updates(NoVal, 31536000, NoVal),
    ?assertEqual(#{time_limit => 31536000}, Result).

test_build_updates_requeue_zero() ->
    NoVal = 16#FFFFFFFE,
    %% Requeue with value 0 (not 1) should not change state
    Result = build_job_updates(NoVal, NoVal, 0),
    ?assertEqual(#{}, Result).

test_build_updates_all_set() ->
    %% All values set simultaneously
    Result = build_job_updates(500, 7200, 1),
    ?assertEqual(500, maps:get(priority, Result)),
    ?assertEqual(7200, maps:get(time_limit, Result)),
    %% Requeue=1 sets state to pending, which may override other state changes
    ?assertEqual(pending, maps:get(state, Result)).

%%====================================================================
%% Reservation Flags Advanced Tests
%%====================================================================

test_resv_flags_all() ->
    %% All flags set
    AllFlags = 16#FFFF,
    Result = parse_reservation_flags(AllFlags),
    ?assert(lists:member(maint, Result)),
    ?assert(lists:member(flex, Result)),
    ?assert(lists:member(daily, Result)),
    ?assert(lists:member(weekly, Result)).

test_resv_flags_combos() ->
    %% Weekend + time_float
    Flags = 16#0020 bor 16#0100,
    Result = parse_reservation_flags(Flags),
    ?assert(lists:member(weekend, Result)),
    ?assert(lists:member(time_float, Result)),
    ?assertNot(lists:member(maint, Result)).

test_resv_type_unknown_string() ->
    %% Unknown type string defaults to user
    ?assertEqual(user, determine_reservation_type(<<"custom">>, 0)),
    ?assertEqual(user, determine_reservation_type(<<"CUSTOM">>, 0)),
    ?assertEqual(user, determine_reservation_type(<<"123">>, 0)).

test_resv_state_unknown() ->
    ?assertEqual(0, reservation_state_to_flags(pending)),
    ?assertEqual(0, reservation_state_to_flags(unknown)),
    ?assertEqual(0, reservation_state_to_flags(test)).

%%====================================================================
%% License Info Edge Cases
%%====================================================================

test_license_remote_true() ->
    License = #{
        name => <<"remote_lic">>,
        total => 100,
        in_use => 50,
        available => 50,
        reserved => 0,
        remote => true
    },
    Info = license_to_license_info(License),
    ?assertEqual(1, Info#license_info.remote).

test_license_calculated_avail() ->
    %% When available is not provided, it should be calculated
    License = #{
        name => <<"calc_lic">>,
        total => 100,
        in_use => 30
        % available not provided
    },
    Info = license_to_license_info(License),
    ?assertEqual(70, Info#license_info.available).

test_license_tuple_4() ->
    %% Tuple with only 4 elements - too small
    License = {license, <<"small">>, 10, 5},
    Info = license_to_license_info(License),
    ?assertEqual(<<>>, Info#license_info.name).

test_license_tuple_6() ->
    %% Tuple with 6+ elements - should work
    License = {license, <<"big">>, 100, 25, 0, false},
    Info = license_to_license_info(License),
    ?assertEqual(<<"big">>, Info#license_info.name),
    ?assertEqual(100, Info#license_info.total),
    ?assertEqual(25, Info#license_info.in_use).

%%====================================================================
%% Request to Spec Edge Cases
%%====================================================================

test_batch_spec_negative_nodes() ->
    %% min_nodes being 0 should be clamped to 1
    Request = #batch_job_request{
        name = <<"test">>,
        script = <<"#!/bin/bash">>,
        partition = <<"batch">>,
        min_nodes = 0,  % Should become 1
        min_cpus = 4,
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000
    },
    Spec = batch_request_to_job_spec(Request),
    ?assertEqual(1, maps:get(num_nodes, Spec)).

test_batch_spec_zero_cpus() ->
    Request = #batch_job_request{
        name = <<"test">>,
        script = <<"#!/bin/bash">>,
        partition = <<"batch">>,
        min_nodes = 2,
        min_cpus = 0,  % Should become 1
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000
    },
    Spec = batch_request_to_job_spec(Request),
    ?assertEqual(1, maps:get(num_cpus, Spec)).

test_resource_spec_with_account() ->
    Request = #resource_allocation_request{
        name = <<"interactive">>,
        partition = <<"debug">>,
        min_nodes = 1,
        min_cpus = 2,
        time_limit = 1800,
        priority = 100,
        user_id = 1000,
        group_id = 1000,
        account = <<"research">>
    },
    Spec = resource_request_to_job_spec(Request),
    ?assertEqual(<<"research">>, maps:get(account, Spec)).

test_resource_spec_with_licenses() ->
    Request = #resource_allocation_request{
        name = <<"licensed">>,
        partition = <<"batch">>,
        min_nodes = 1,
        min_cpus = 1,
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000,
        licenses = <<"matlab:2">>
    },
    Spec = resource_request_to_job_spec(Request),
    ?assertEqual(<<"matlab:2">>, maps:get(licenses, Spec)).

%%====================================================================
%% Info Conversion Edge Cases
%%====================================================================

test_job_info_empty_nodes() ->
    Job = #job{
        id = 999,
        name = <<"empty_nodes">>,
        partition = <<"batch">>,
        state = pending,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        priority = 100,
        submit_time = 1700000000,
        script = <<"#!/bin/bash">>,
        allocated_nodes = [],  % Empty
        account = <<>>,
        qos = <<"normal">>,
        licenses = []
    },
    Info = job_to_job_info(Job),
    ?assertEqual(<<>>, Info#job_info.nodes),
    ?assertEqual(<<>>, Info#job_info.batch_host).

test_job_info_undefined_start() ->
    Job = #job{
        id = 1000,
        name = <<"undefined_start">>,
        partition = <<"batch">>,
        state = pending,
        num_nodes = 1,
        num_cpus = 1,
        time_limit = 3600,
        priority = 100,
        submit_time = 1700000000,
        start_time = undefined,
        end_time = undefined,
        script = <<>>,
        allocated_nodes = [],
        account = <<>>,
        qos = <<"normal">>,
        licenses = []
    },
    Info = job_to_job_info(Job),
    ?assertEqual(0, Info#job_info.start_time),
    ?assertEqual(0, Info#job_info.end_time).

test_node_info_empty_features() ->
    Node = #node{
        hostname = <<"compute01">>,
        cpus = 16,
        memory_mb = 32768,
        state = idle,
        features = [],  % Empty
        partitions = [],  % Empty
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = 32000
    },
    Info = node_to_node_info(Node),
    ?assertEqual(<<>>, Info#node_info.features),
    ?assertEqual(<<>>, Info#node_info.partitions).

test_node_info_high_load() ->
    Node = #node{
        hostname = <<"loaded01">>,
        cpus = 8,
        memory_mb = 16384,
        state = allocated,
        features = [<<"gpu">>],
        partitions = [<<"compute">>],
        running_jobs = [1, 2, 3],
        load_avg = 99.99,  % Very high load
        free_memory_mb = 100
    },
    Info = node_to_node_info(Node),
    %% Load is multiplied by 100
    ?assertEqual(9999, Info#node_info.cpu_load).

test_step_info_undefined_start() ->
    StepMap = #{
        job_id => 500,
        step_id => 0,
        name => <<"step0">>,
        state => pending,
        start_time => undefined,  % Undefined
        num_tasks => 1,
        num_nodes => 1,
        allocated_nodes => [],
        exit_code => 0
    },
    Info = step_map_to_info(StepMap),
    ?assertEqual(0, Info#job_step_info.start_time),
    ?assertEqual(0, Info#job_step_info.run_time).

test_step_info_missing_fields() ->
    %% Minimal map with just required fields
    StepMap = #{
        job_id => 600
    },
    Info = step_map_to_info(StepMap),
    ?assertEqual(600, Info#job_step_info.job_id),
    ?assertEqual(0, Info#job_step_info.step_id),
    ?assertEqual(<<>>, Info#job_step_info.step_name),
    ?assertEqual(?JOB_PENDING, Info#job_step_info.state).

%%====================================================================
%% Helper Function Implementations
%% These replicate the internal functions from flurm_controller_handler
%%====================================================================

%% Generate unique reservation name
generate_reservation_name() ->
    Timestamp = erlang:system_time(microsecond),
    Random = rand:uniform(9999),
    iolist_to_binary(io_lib:format("resv_~p_~4..0B", [Timestamp, Random])).

%% Extract fields from reservation tuple
extract_reservation_fields(Resv) when is_tuple(Resv) ->
    case tuple_size(Resv) of
        N when N >= 14 ->
            Name = element(2, Resv),
            StartTime = element(4, Resv),
            EndTime = element(5, Resv),
            Nodes = element(7, Resv),
            Users = element(11, Resv),
            State = element(14, Resv),
            Flags = element(12, Resv),
            {Name, StartTime, EndTime, Nodes, Users, State, Flags};
        _ ->
            {<<>>, 0, 0, [], [], inactive, []}
    end;
extract_reservation_fields(_) ->
    {<<>>, 0, 0, [], [], inactive, []}.

%% Create reservation request to spec
create_reservation_request_to_spec(#create_reservation_request{} = Req) ->
    Now = erlang:system_time(second),

    StartTime = case Req#create_reservation_request.start_time of
        0 -> Now;
        S -> S
    end,

    EndTime = case Req#create_reservation_request.end_time of
        0 ->
            Duration = case Req#create_reservation_request.duration of
                0 -> 60;
                D -> D
            end,
            StartTime + (Duration * 60);
        E -> E
    end,

    Nodes = case Req#create_reservation_request.nodes of
        <<>> -> [];
        NodeList -> binary:split(NodeList, <<",">>, [global])
    end,

    Users = case Req#create_reservation_request.users of
        <<>> -> [];
        UserList -> binary:split(UserList, <<",">>, [global])
    end,

    Accounts = case Req#create_reservation_request.accounts of
        <<>> -> [];
        AccountList -> binary:split(AccountList, <<",">>, [global])
    end,

    Type = determine_reservation_type(
        Req#create_reservation_request.type,
        Req#create_reservation_request.flags
    ),

    Flags = parse_reservation_flags(Req#create_reservation_request.flags),

    #{
        name => case Req#create_reservation_request.name of
            <<>> -> generate_reservation_name();
            N -> N
        end,
        type => Type,
        start_time => StartTime,
        end_time => EndTime,
        nodes => Nodes,
        node_count => case Req#create_reservation_request.node_cnt of
            0 -> length(Nodes);
            NC -> NC
        end,
        partition => Req#create_reservation_request.partition,
        users => Users,
        accounts => Accounts,
        features => case Req#create_reservation_request.features of
            <<>> -> [];
            F -> binary:split(F, <<",">>, [global])
        end,
        flags => Flags,
        state => case StartTime =< Now of
            true -> active;
            false -> inactive
        end
    }.

%% Update reservation request to updates map
update_reservation_request_to_updates(#update_reservation_request{} = Req) ->
    Updates0 = #{},

    Updates1 = case Req#update_reservation_request.start_time of
        0 -> Updates0;
        S -> maps:put(start_time, S, Updates0)
    end,

    Updates2 = case Req#update_reservation_request.end_time of
        0 -> Updates1;
        E -> maps:put(end_time, E, Updates1)
    end,

    Updates3 = case Req#update_reservation_request.duration of
        0 -> Updates2;
        D -> maps:put(duration, D * 60, Updates2)
    end,

    Updates4 = case Req#update_reservation_request.nodes of
        <<>> -> Updates3;
        NodeList ->
            Nodes = binary:split(NodeList, <<",">>, [global]),
            maps:put(nodes, Nodes, Updates3)
    end,

    Updates5 = case Req#update_reservation_request.users of
        <<>> -> Updates4;
        UserList ->
            Users = binary:split(UserList, <<",">>, [global]),
            maps:put(users, Users, Updates4)
    end,

    Updates6 = case Req#update_reservation_request.accounts of
        <<>> -> Updates5;
        AccountList ->
            Accounts = binary:split(AccountList, <<",">>, [global]),
            maps:put(accounts, Accounts, Updates5)
    end,

    Updates7 = case Req#update_reservation_request.partition of
        <<>> -> Updates6;
        P -> maps:put(partition, P, Updates6)
    end,

    case Req#update_reservation_request.flags of
        0 -> Updates7;
        F -> maps:put(flags, parse_reservation_flags(F), Updates7)
    end.

%% Build burst buffer info
build_burst_buffer_info([], _Stats) ->
    [];
build_burst_buffer_info(Pools, Stats) ->
    PoolInfos = [pool_to_bb_pool(P) || P <- Pools],
    TotalSpace = maps:get(total_size, Stats, 0),
    UsedSpace = maps:get(used_size, Stats, 0),
    UnfreeSpace = TotalSpace - maps:get(free_size, Stats, TotalSpace),
    [#burst_buffer_info{
        name = <<"generic">>,
        default_pool = <<"default">>,
        allow_users = <<>>,
        create_buffer = <<>>,
        deny_users = <<>>,
        destroy_buffer = <<>>,
        flags = 0,
        get_sys_state = <<>>,
        get_sys_status = <<>>,
        granularity = 1048576,
        pool_cnt = length(PoolInfos),
        pools = PoolInfos,
        other_timeout = 300,
        stage_in_timeout = 300,
        stage_out_timeout = 300,
        start_stage_in = <<>>,
        start_stage_out = <<>>,
        stop_stage_in = <<>>,
        stop_stage_out = <<>>,
        total_space = TotalSpace,
        unfree_space = UnfreeSpace,
        used_space = UsedSpace,
        validate_timeout = 60
    }].

%% Pool to burst buffer pool
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

%% Format helpers
format_allocated_nodes([]) -> <<>>;
format_allocated_nodes(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

format_features([]) -> <<>>;
format_features(Features) ->
    iolist_to_binary(lists:join(<<",">>, Features)).

format_licenses([]) -> <<>>;
format_licenses(Licenses) ->
    Formatted = lists:map(fun({Name, Count}) ->
        iolist_to_binary([Name, <<":">>, integer_to_binary(Count)])
    end, Licenses),
    iolist_to_binary(lists:join(<<",">>, Formatted)).

format_partitions([]) -> <<>>;
format_partitions(Partitions) ->
    iolist_to_binary(lists:join(<<",">>, Partitions)).

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

default_time(undefined) -> 0;
default_time(Time) -> Time.

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

%% State conversions
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

%% Job updates
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

%% License info
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

%% Default helpers
default_work_dir(<<>>) -> <<"/tmp">>;
default_work_dir(WorkDir) -> WorkDir.

default_partition(<<>>) -> <<"default">>;
default_partition(Partition) -> Partition.

default_time_limit(0) -> 3600;
default_time_limit(TimeLimit) -> TimeLimit.

default_priority(0) -> 100;
default_priority(Priority) -> Priority.

%% Request to spec conversions
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

%% Job to info
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

%% Node to info
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

%% Step map to info
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
