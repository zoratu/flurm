%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler Coverage Unit Tests
%%%
%%% Comprehensive cover-effective unit tests for flurm_controller_handler
%%% pure helper functions that are TEST-exported.
%%%
%%% These tests directly call the actual module functions (not copies)
%%% to ensure proper code coverage metrics.
%%%
%%% Tested functions (24 TEST-exported):
%%% 1. Type conversion: ensure_binary/1, error_to_binary/1, safe_binary_to_integer/1
%%% 2. State conversions: job_state_to_slurm/1, node_state_to_slurm/1,
%%%    partition_state_to_slurm/1, step_state_to_slurm/1
%%% 3. Default helpers: default_time/1, default_partition/1, default_time_limit/1,
%%%    default_priority/1, default_work_dir/1
%%% 4. Format functions: format_allocated_nodes/1, format_features/1,
%%%    format_partitions/1, format_node_list/1, format_licenses/1
%%% 5. Parsing: parse_job_id_str/1
%%% 6. Reservation: reservation_state_to_flags/1, determine_reservation_type/2,
%%%    parse_reservation_flags/1, generate_reservation_name/0,
%%%    extract_reservation_fields/1, build_field_updates/2
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup - Module Reference
%%====================================================================

%% Direct module reference for all tests
-define(M, flurm_controller_handler).

%%====================================================================
%% Type Conversion Tests - ensure_binary/1
%%====================================================================

ensure_binary_test_() ->
    {"ensure_binary/1 type conversion tests",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: undefined returns empty binary
       {"undefined returns empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary(undefined))},

       %% Test 2: binary passthrough
       {"binary passthrough",
        ?_assertEqual(<<"hello">>, ?M:ensure_binary(<<"hello">>))},

       %% Test 3: empty binary passthrough
       {"empty binary passthrough",
        ?_assertEqual(<<>>, ?M:ensure_binary(<<>>))},

       %% Test 4: list converted to binary
       {"list to binary",
        ?_assertEqual(<<"test">>, ?M:ensure_binary("test"))},

       %% Test 5: empty list to empty binary
       {"empty list to empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary([]))},

       %% Test 6: atom to binary
       {"atom to binary",
        ?_assertEqual(<<"some_atom">>, ?M:ensure_binary(some_atom))},

       %% Test 7: tuple returns empty (other case)
       {"tuple returns empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary({tuple, data}))},

       %% Test 8: integer returns empty (other case)
       {"integer returns empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary(12345))},

       %% Test 9: float returns empty (other case)
       {"float returns empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary(3.14))},

       %% Test 10: pid returns empty (other case)
       {"pid returns empty binary",
        ?_assertEqual(<<>>, ?M:ensure_binary(self()))}
      ]}}.

%%====================================================================
%% Type Conversion Tests - error_to_binary/1
%%====================================================================

error_to_binary_test_() ->
    {"error_to_binary/1 error conversion tests",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: binary passthrough
       {"binary error passthrough",
        ?_assertEqual(<<"error message">>, ?M:error_to_binary(<<"error message">>))},

       %% Test 2: empty binary
       {"empty binary passthrough",
        ?_assertEqual(<<>>, ?M:error_to_binary(<<>>))},

       %% Test 3: atom to binary
       {"atom error to binary",
        ?_assertEqual(<<"not_found">>, ?M:error_to_binary(not_found))},

       %% Test 4: common error atom
       {"timeout atom to binary",
        ?_assertEqual(<<"timeout">>, ?M:error_to_binary(timeout))},

       %% Test 5: list to binary
       {"list error to binary",
        ?_assertEqual(<<"error string">>, ?M:error_to_binary("error string"))},

       %% Test 6: tuple error formatted
       {"tuple error formatted",
        fun() ->
            Result = ?M:error_to_binary({error, some_reason}),
            ?assert(is_binary(Result)),
            ?assert(byte_size(Result) > 0)
        end},

       %% Test 7: complex term formatted
       {"complex term formatted",
        fun() ->
            Result = ?M:error_to_binary({badmatch, #{key => value}}),
            ?assert(is_binary(Result)),
            ?assert(byte_size(Result) > 0)
        end},

       %% Test 8: integer converted
       {"integer converted to binary",
        fun() ->
            Result = ?M:error_to_binary(500),
            ?assert(is_binary(Result))
        end}
      ]}}.

%%====================================================================
%% Type Conversion Tests - safe_binary_to_integer/1
%%====================================================================

safe_binary_to_integer_test_() ->
    {"safe_binary_to_integer/1 safe parsing tests",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: valid positive integer
       {"valid positive integer",
        ?_assertEqual(42, ?M:safe_binary_to_integer(<<"42">>))},

       %% Test 2: valid large integer
       {"valid large integer",
        ?_assertEqual(123456789, ?M:safe_binary_to_integer(<<"123456789">>))},

       %% Test 3: valid zero
       {"valid zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<"0">>))},

       %% Test 4: valid negative integer
       {"valid negative integer",
        ?_assertEqual(-100, ?M:safe_binary_to_integer(<<"-100">>))},

       %% Test 5: invalid string returns 0
       {"invalid string returns zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<"abc">>))},

       %% Test 6: empty binary returns 0
       {"empty binary returns zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<>>))},

       %% Test 7: mixed content returns 0
       {"mixed content returns zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<"12abc">>))},

       %% Test 8: floating point string returns 0
       {"float string returns zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<"3.14">>))},

       %% Test 9: whitespace returns 0
       {"whitespace returns zero",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<"  ">>))},

       %% Test 10: leading zeros work
       {"leading zeros work",
        ?_assertEqual(42, ?M:safe_binary_to_integer(<<"0042">>))}
      ]}}.

%%====================================================================
%% State Conversion Tests - job_state_to_slurm/1
%%====================================================================

job_state_to_slurm_test_() ->
    {"job_state_to_slurm/1 all state mappings",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: pending
       {"pending maps to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:job_state_to_slurm(pending))},

       %% Test 2: configuring (also maps to pending)
       {"configuring maps to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:job_state_to_slurm(configuring))},

       %% Test 3: running
       {"running maps to JOB_RUNNING",
        ?_assertEqual(?JOB_RUNNING, ?M:job_state_to_slurm(running))},

       %% Test 4: completing (also maps to running)
       {"completing maps to JOB_RUNNING",
        ?_assertEqual(?JOB_RUNNING, ?M:job_state_to_slurm(completing))},

       %% Test 5: completed
       {"completed maps to JOB_COMPLETE",
        ?_assertEqual(?JOB_COMPLETE, ?M:job_state_to_slurm(completed))},

       %% Test 6: cancelled
       {"cancelled maps to JOB_CANCELLED",
        ?_assertEqual(?JOB_CANCELLED, ?M:job_state_to_slurm(cancelled))},

       %% Test 7: failed
       {"failed maps to JOB_FAILED",
        ?_assertEqual(?JOB_FAILED, ?M:job_state_to_slurm(failed))},

       %% Test 8: timeout
       {"timeout maps to JOB_TIMEOUT",
        ?_assertEqual(?JOB_TIMEOUT, ?M:job_state_to_slurm(timeout))},

       %% Test 9: node_fail
       {"node_fail maps to JOB_NODE_FAIL",
        ?_assertEqual(?JOB_NODE_FAIL, ?M:job_state_to_slurm(node_fail))},

       %% Test 10: unknown state defaults to pending
       {"unknown state defaults to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:job_state_to_slurm(unknown_state))},

       %% Test 11: arbitrary atom defaults to pending
       {"arbitrary atom defaults to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:job_state_to_slurm(some_random_state))}
      ]}}.

%%====================================================================
%% State Conversion Tests - node_state_to_slurm/1
%%====================================================================

node_state_to_slurm_test_() ->
    {"node_state_to_slurm/1 all state mappings",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: up maps to idle
       {"up maps to NODE_STATE_IDLE",
        ?_assertEqual(?NODE_STATE_IDLE, ?M:node_state_to_slurm(up))},

       %% Test 2: down
       {"down maps to NODE_STATE_DOWN",
        ?_assertEqual(?NODE_STATE_DOWN, ?M:node_state_to_slurm(down))},

       %% Test 3: drain (maps to down)
       {"drain maps to NODE_STATE_DOWN",
        ?_assertEqual(?NODE_STATE_DOWN, ?M:node_state_to_slurm(drain))},

       %% Test 4: idle
       {"idle maps to NODE_STATE_IDLE",
        ?_assertEqual(?NODE_STATE_IDLE, ?M:node_state_to_slurm(idle))},

       %% Test 5: allocated
       {"allocated maps to NODE_STATE_ALLOCATED",
        ?_assertEqual(?NODE_STATE_ALLOCATED, ?M:node_state_to_slurm(allocated))},

       %% Test 6: mixed
       {"mixed maps to NODE_STATE_MIXED",
        ?_assertEqual(?NODE_STATE_MIXED, ?M:node_state_to_slurm(mixed))},

       %% Test 7: unknown state
       {"unknown state maps to NODE_STATE_UNKNOWN",
        ?_assertEqual(?NODE_STATE_UNKNOWN, ?M:node_state_to_slurm(unknown_node))},

       %% Test 8: arbitrary atom
       {"arbitrary atom maps to NODE_STATE_UNKNOWN",
        ?_assertEqual(?NODE_STATE_UNKNOWN, ?M:node_state_to_slurm(something_else))}
      ]}}.

%%====================================================================
%% State Conversion Tests - partition_state_to_slurm/1
%%====================================================================

partition_state_to_slurm_test_() ->
    {"partition_state_to_slurm/1 all state mappings",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: up (PARTITION_UP = 3)
       {"up maps to 3",
        ?_assertEqual(3, ?M:partition_state_to_slurm(up))},

       %% Test 2: down (PARTITION_DOWN = 1)
       {"down maps to 1",
        ?_assertEqual(1, ?M:partition_state_to_slurm(down))},

       %% Test 3: drain (PARTITION_DRAIN = 2)
       {"drain maps to 2",
        ?_assertEqual(2, ?M:partition_state_to_slurm(drain))},

       %% Test 4: inactive (PARTITION_INACTIVE = 0)
       {"inactive maps to 0",
        ?_assertEqual(0, ?M:partition_state_to_slurm(inactive))},

       %% Test 5: unknown defaults to up (3)
       {"unknown state defaults to 3",
        ?_assertEqual(3, ?M:partition_state_to_slurm(unknown))},

       %% Test 6: arbitrary atom defaults to up
       {"arbitrary atom defaults to 3",
        ?_assertEqual(3, ?M:partition_state_to_slurm(some_state))}
      ]}}.

%%====================================================================
%% State Conversion Tests - step_state_to_slurm/1
%%====================================================================

step_state_to_slurm_test_() ->
    {"step_state_to_slurm/1 all state mappings",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: pending
       {"pending maps to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:step_state_to_slurm(pending))},

       %% Test 2: running
       {"running maps to JOB_RUNNING",
        ?_assertEqual(?JOB_RUNNING, ?M:step_state_to_slurm(running))},

       %% Test 3: completing
       {"completing maps to JOB_RUNNING",
        ?_assertEqual(?JOB_RUNNING, ?M:step_state_to_slurm(completing))},

       %% Test 4: completed
       {"completed maps to JOB_COMPLETE",
        ?_assertEqual(?JOB_COMPLETE, ?M:step_state_to_slurm(completed))},

       %% Test 5: cancelled
       {"cancelled maps to JOB_CANCELLED",
        ?_assertEqual(?JOB_CANCELLED, ?M:step_state_to_slurm(cancelled))},

       %% Test 6: failed
       {"failed maps to JOB_FAILED",
        ?_assertEqual(?JOB_FAILED, ?M:step_state_to_slurm(failed))},

       %% Test 7: unknown defaults to pending
       {"unknown state defaults to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:step_state_to_slurm(unknown))},

       %% Test 8: arbitrary atom
       {"arbitrary atom defaults to JOB_PENDING",
        ?_assertEqual(?JOB_PENDING, ?M:step_state_to_slurm(something))}
      ]}}.

%%====================================================================
%% Default Helper Tests - default_time/1
%%====================================================================

default_time_test_() ->
    {"default_time/1 time default handling",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: undefined returns 0
       {"undefined returns 0",
        ?_assertEqual(0, ?M:default_time(undefined))},

       %% Test 2: positive value passthrough
       {"positive value passthrough",
        ?_assertEqual(12345, ?M:default_time(12345))},

       %% Test 3: zero passthrough
       {"zero passthrough",
        ?_assertEqual(0, ?M:default_time(0))},

       %% Test 4: large value passthrough
       {"large timestamp passthrough",
        ?_assertEqual(1700000000, ?M:default_time(1700000000))}
      ]}}.

%%====================================================================
%% Default Helper Tests - default_partition/1
%%====================================================================

default_partition_test_() ->
    {"default_partition/1 partition default handling",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty binary returns "default"
       {"empty binary returns default",
        ?_assertEqual(<<"default">>, ?M:default_partition(<<>>))},

       %% Test 2: specified partition passthrough
       {"specified partition passthrough",
        ?_assertEqual(<<"gpu">>, ?M:default_partition(<<"gpu">>))},

       %% Test 3: batch partition passthrough
       {"batch partition passthrough",
        ?_assertEqual(<<"batch">>, ?M:default_partition(<<"batch">>))},

       %% Test 4: debug partition passthrough
       {"debug partition passthrough",
        ?_assertEqual(<<"debug">>, ?M:default_partition(<<"debug">>))}
      ]}}.

%%====================================================================
%% Default Helper Tests - default_time_limit/1
%%====================================================================

default_time_limit_test_() ->
    {"default_time_limit/1 time limit default handling",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: zero returns 3600 (1 hour default)
       {"zero returns 3600 seconds",
        ?_assertEqual(3600, ?M:default_time_limit(0))},

       %% Test 2: positive value passthrough
       {"positive value passthrough",
        ?_assertEqual(7200, ?M:default_time_limit(7200))},

       %% Test 3: large value passthrough
       {"large value passthrough",
        ?_assertEqual(86400, ?M:default_time_limit(86400))},

       %% Test 4: 1 second passthrough
       {"one second passthrough",
        ?_assertEqual(1, ?M:default_time_limit(1))}
      ]}}.

%%====================================================================
%% Default Helper Tests - default_priority/1
%%====================================================================

default_priority_test_() ->
    {"default_priority/1 priority default handling",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: NO_VAL (0xFFFFFFFE) returns 100
       {"NO_VAL returns 100",
        ?_assertEqual(100, ?M:default_priority(16#FFFFFFFE))},

       %% Test 2: zero passthrough (hold job)
       {"zero passthrough for hold",
        ?_assertEqual(0, ?M:default_priority(0))},

       %% Test 3: positive value passthrough
       {"positive value passthrough",
        ?_assertEqual(500, ?M:default_priority(500))},

       %% Test 4: high priority passthrough
       {"high priority passthrough",
        ?_assertEqual(10000, ?M:default_priority(10000))},

       %% Test 5: value 1 passthrough
       {"value 1 passthrough",
        ?_assertEqual(1, ?M:default_priority(1))}
      ]}}.

%%====================================================================
%% Default Helper Tests - default_work_dir/1
%%====================================================================

default_work_dir_test_() ->
    {"default_work_dir/1 working directory default handling",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty binary returns /tmp
       {"empty returns /tmp",
        ?_assertEqual(<<"/tmp">>, ?M:default_work_dir(<<>>))},

       %% Test 2: specified directory passthrough
       {"specified directory passthrough",
        ?_assertEqual(<<"/home/user">>, ?M:default_work_dir(<<"/home/user">>))},

       %% Test 3: root directory passthrough
       {"root directory passthrough",
        ?_assertEqual(<<"/">>, ?M:default_work_dir(<<"/">>))},

       %% Test 4: complex path passthrough
       {"complex path passthrough",
        ?_assertEqual(<<"/data/jobs/123">>, ?M:default_work_dir(<<"/data/jobs/123">>))}
      ]}}.

%%====================================================================
%% Format Function Tests - format_allocated_nodes/1
%%====================================================================

format_allocated_nodes_test_() ->
    {"format_allocated_nodes/1 node list formatting",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty list
       {"empty list returns empty binary",
        ?_assertEqual(<<>>, ?M:format_allocated_nodes([]))},

       %% Test 2: single node
       {"single node",
        ?_assertEqual(<<"node1">>, ?M:format_allocated_nodes([<<"node1">>]))},

       %% Test 3: two nodes
       {"two nodes comma separated",
        ?_assertEqual(<<"node1,node2">>,
                     ?M:format_allocated_nodes([<<"node1">>, <<"node2">>]))},

       %% Test 4: multiple nodes
       {"multiple nodes comma separated",
        ?_assertEqual(<<"node1,node2,node3,node4">>,
                     ?M:format_allocated_nodes([<<"node1">>, <<"node2">>,
                                                <<"node3">>, <<"node4">>]))},

       %% Test 5: complex hostnames
       {"complex hostnames",
        ?_assertEqual(<<"compute-001,compute-002">>,
                     ?M:format_allocated_nodes([<<"compute-001">>, <<"compute-002">>]))}
      ]}}.

%%====================================================================
%% Format Function Tests - format_features/1
%%====================================================================

format_features_test_() ->
    {"format_features/1 feature list formatting",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty list
       {"empty list returns empty binary",
        ?_assertEqual(<<>>, ?M:format_features([]))},

       %% Test 2: single feature
       {"single feature",
        ?_assertEqual(<<"gpu">>, ?M:format_features([<<"gpu">>]))},

       %% Test 3: multiple features
       {"multiple features comma separated",
        ?_assertEqual(<<"gpu,nvme,infiniband">>,
                     ?M:format_features([<<"gpu">>, <<"nvme">>, <<"infiniband">>]))},

       %% Test 4: feature with version
       {"feature with version",
        ?_assertEqual(<<"a100,cuda11">>,
                     ?M:format_features([<<"a100">>, <<"cuda11">>]))}
      ]}}.

%%====================================================================
%% Format Function Tests - format_partitions/1
%%====================================================================

format_partitions_test_() ->
    {"format_partitions/1 partition list formatting",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty list
       {"empty list returns empty binary",
        ?_assertEqual(<<>>, ?M:format_partitions([]))},

       %% Test 2: single partition
       {"single partition",
        ?_assertEqual(<<"default">>, ?M:format_partitions([<<"default">>]))},

       %% Test 3: multiple partitions
       {"multiple partitions comma separated",
        ?_assertEqual(<<"batch,debug,gpu">>,
                     ?M:format_partitions([<<"batch">>, <<"debug">>, <<"gpu">>]))}
      ]}}.

%%====================================================================
%% Format Function Tests - format_node_list/1
%%====================================================================

format_node_list_test_() ->
    {"format_node_list/1 node list formatting",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty list
       {"empty list returns empty binary",
        ?_assertEqual(<<>>, ?M:format_node_list([]))},

       %% Test 2: single node
       {"single node",
        ?_assertEqual(<<"compute01">>, ?M:format_node_list([<<"compute01">>]))},

       %% Test 3: multiple nodes
       {"multiple nodes comma separated",
        ?_assertEqual(<<"compute01,compute02,compute03">>,
                     ?M:format_node_list([<<"compute01">>, <<"compute02">>,
                                          <<"compute03">>]))}
      ]}}.

%%====================================================================
%% Format Function Tests - format_licenses/1
%%====================================================================

format_licenses_test_() ->
    {"format_licenses/1 license formatting",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty list
       {"empty list returns empty binary",
        ?_assertEqual(<<>>, ?M:format_licenses([]))},

       %% Test 2: empty binary passthrough
       {"empty binary passthrough",
        ?_assertEqual(<<>>, ?M:format_licenses(<<>>))},

       %% Test 3: binary passthrough
       {"binary passthrough",
        ?_assertEqual(<<"matlab:1">>, ?M:format_licenses(<<"matlab:1">>))},

       %% Test 4: single license tuple
       {"single license tuple",
        ?_assertEqual(<<"matlab:1">>, ?M:format_licenses([{<<"matlab">>, 1}]))},

       %% Test 5: multiple licenses
       {"multiple licenses formatted",
        ?_assertEqual(<<"matlab:2,ansys:4">>,
                     ?M:format_licenses([{<<"matlab">>, 2}, {<<"ansys">>, 4}]))},

       %% Test 6: license with high count
       {"license with high count",
        ?_assertEqual(<<"license:100">>, ?M:format_licenses([{<<"license">>, 100}]))}
      ]}}.

%%====================================================================
%% Parsing Tests - parse_job_id_str/1
%%====================================================================

parse_job_id_str_test_() ->
    {"parse_job_id_str/1 job ID string parsing",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: empty string returns 0
       {"empty string returns 0",
        ?_assertEqual(0, ?M:parse_job_id_str(<<>>))},

       %% Test 2: simple numeric ID
       {"simple numeric ID",
        ?_assertEqual(12345, ?M:parse_job_id_str(<<"12345">>))},

       %% Test 3: ID with null terminator
       {"ID with null terminator stripped",
        ?_assertEqual(123, ?M:parse_job_id_str(<<"123", 0, "garbage">>))},

       %% Test 4: array job format "123_4"
       {"array job format extracts base ID",
        ?_assertEqual(100, ?M:parse_job_id_str(<<"100_5">>))},

       %% Test 5: array job format with larger task ID
       {"array job format with large task ID",
        ?_assertEqual(500, ?M:parse_job_id_str(<<"500_999">>))},

       %% Test 6: invalid string returns 0
       {"invalid string returns 0",
        ?_assertEqual(0, ?M:parse_job_id_str(<<"notanumber">>))},

       %% Test 7: array format with invalid base
       {"array format with invalid base returns 0",
        ?_assertEqual(0, ?M:parse_job_id_str(<<"abc_123">>))},

       %% Test 8: just underscore
       {"just underscore returns 0",
        ?_assertEqual(0, ?M:parse_job_id_str(<<"_123">>))},

       %% Test 9: multiple underscores
       {"multiple underscores uses first part",
        ?_assertEqual(1, ?M:parse_job_id_str(<<"1_2_3">>))},

       %% Test 10: large job ID
       {"large job ID",
        ?_assertEqual(999999999, ?M:parse_job_id_str(<<"999999999">>))}
      ]}}.

%%====================================================================
%% Reservation Tests - reservation_state_to_flags/1
%%====================================================================

reservation_state_to_flags_test_() ->
    {"reservation_state_to_flags/1 state flag mapping",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: active
       {"active returns 1",
        ?_assertEqual(1, ?M:reservation_state_to_flags(active))},

       %% Test 2: inactive
       {"inactive returns 0",
        ?_assertEqual(0, ?M:reservation_state_to_flags(inactive))},

       %% Test 3: expired
       {"expired returns 2",
        ?_assertEqual(2, ?M:reservation_state_to_flags(expired))},

       %% Test 4: unknown state
       {"unknown state returns 0",
        ?_assertEqual(0, ?M:reservation_state_to_flags(unknown_state))}
      ]}}.

%%====================================================================
%% Reservation Tests - determine_reservation_type/2
%%====================================================================

determine_reservation_type_test_() ->
    {"determine_reservation_type/2 type determination",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: maint lowercase
       {"maint lowercase",
        ?_assertEqual(maint, ?M:determine_reservation_type(<<"maint">>, 0))},

       %% Test 2: MAINT uppercase
       {"MAINT uppercase",
        ?_assertEqual(maint, ?M:determine_reservation_type(<<"MAINT">>, 0))},

       %% Test 3: maintenance full word
       {"maintenance full word",
        ?_assertEqual(maintenance, ?M:determine_reservation_type(<<"maintenance">>, 0))},

       %% Test 4: flex lowercase
       {"flex lowercase",
        ?_assertEqual(flex, ?M:determine_reservation_type(<<"flex">>, 0))},

       %% Test 5: FLEX uppercase
       {"FLEX uppercase",
        ?_assertEqual(flex, ?M:determine_reservation_type(<<"FLEX">>, 0))},

       %% Test 6: user lowercase
       {"user lowercase",
        ?_assertEqual(user, ?M:determine_reservation_type(<<"user">>, 0))},

       %% Test 7: USER uppercase
       {"USER uppercase",
        ?_assertEqual(user, ?M:determine_reservation_type(<<"USER">>, 0))},

       %% Test 8: unknown type defaults to user
       {"unknown type defaults to user",
        ?_assertEqual(user, ?M:determine_reservation_type(<<"unknown">>, 0))},

       %% Test 9: empty with maint flag
       {"empty with maint flag (0x0001)",
        ?_assertEqual(maint, ?M:determine_reservation_type(<<>>, 16#0001))},

       %% Test 10: empty with flex flag
       {"empty with flex flag (0x8000)",
        ?_assertEqual(flex, ?M:determine_reservation_type(<<>>, 16#8000))},

       %% Test 11: empty with no flags defaults to user
       {"empty with no flags defaults to user",
        ?_assertEqual(user, ?M:determine_reservation_type(<<>>, 0))},

       %% Test 12: maint flag takes precedence
       {"maint flag takes precedence over flex",
        ?_assertEqual(maint, ?M:determine_reservation_type(<<>>, 16#8001))}
      ]}}.

%%====================================================================
%% Reservation Tests - parse_reservation_flags/1
%%====================================================================

parse_reservation_flags_test_() ->
    {"parse_reservation_flags/1 flag parsing",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: zero returns empty list
       {"zero flags returns empty list",
        ?_assertEqual([], ?M:parse_reservation_flags(0))},

       %% Test 2: maint flag
       {"maint flag (0x0001)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0001),
            ?assert(lists:member(maint, Flags))
        end},

       %% Test 3: flex flag
       {"flex flag (0x8000)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#8000),
            ?assert(lists:member(flex, Flags))
        end},

       %% Test 4: daily flag
       {"daily flag (0x0004)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0004),
            ?assert(lists:member(daily, Flags))
        end},

       %% Test 5: weekly flag
       {"weekly flag (0x0008)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0008),
            ?assert(lists:member(weekly, Flags))
        end},

       %% Test 6: combined flags
       {"combined flags (maint + daily + flex)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0001 bor 16#0004 bor 16#8000),
            ?assert(lists:member(maint, Flags)),
            ?assert(lists:member(daily, Flags)),
            ?assert(lists:member(flex, Flags))
        end},

       %% Test 7: all standard flags
       {"multiple flags",
        fun() ->
            Combined = 16#0001 bor 16#0002 bor 16#0004 bor 16#0008,
            Flags = ?M:parse_reservation_flags(Combined),
            ?assert(lists:member(maint, Flags)),
            ?assert(lists:member(ignore_jobs, Flags)),
            ?assert(lists:member(daily, Flags)),
            ?assert(lists:member(weekly, Flags))
        end},

       %% Test 8: time_float flag
       {"time_float flag (0x0100)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0100),
            ?assert(lists:member(time_float, Flags))
        end},

       %% Test 9: overlap flag
       {"overlap flag (0x0800)",
        fun() ->
            Flags = ?M:parse_reservation_flags(16#0800),
            ?assert(lists:member(overlap, Flags))
        end}
      ]}}.

%%====================================================================
%% Reservation Tests - generate_reservation_name/0
%%====================================================================

generate_reservation_name_test_() ->
    {"generate_reservation_name/0 unique name generation",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: returns binary
       {"returns binary",
        fun() ->
            Name = ?M:generate_reservation_name(),
            ?assert(is_binary(Name))
        end},

       %% Test 2: starts with resv_
       {"starts with resv_ prefix",
        fun() ->
            Name = ?M:generate_reservation_name(),
            ?assertMatch(<<"resv_", _/binary>>, Name)
        end},

       %% Test 3: unique names
       {"generates unique names",
        fun() ->
            Name1 = ?M:generate_reservation_name(),
            timer:sleep(1),  %% Ensure time difference
            Name2 = ?M:generate_reservation_name(),
            ?assertNotEqual(Name1, Name2)
        end},

       %% Test 4: reasonable length
       {"reasonable length",
        fun() ->
            Name = ?M:generate_reservation_name(),
            Len = byte_size(Name),
            ?assert(Len > 10),
            ?assert(Len < 50)
        end}
      ]}}.

%%====================================================================
%% Reservation Tests - extract_reservation_fields/1
%%====================================================================

extract_reservation_fields_test_() ->
    {"extract_reservation_fields/1 field extraction",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: non-tuple returns defaults
       {"non-tuple returns defaults",
        fun() ->
            Result = ?M:extract_reservation_fields(not_a_tuple),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},

       %% Test 2: small tuple returns defaults
       {"small tuple returns defaults",
        fun() ->
            Result = ?M:extract_reservation_fields({too, small}),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},

       %% Test 3: valid reservation tuple extracts fields
       {"valid tuple extracts fields",
        fun() ->
            %% Create a tuple that matches expected reservation format
            %% Element positions: 2=name, 4=start_time, 5=end_time,
            %% 7=nodes, 11=users, 12=flags, 14=state
            Resv = {reservation, <<"test_resv">>, node, 1000, 2000, 60,
                    [<<"node1">>], 0, <<"default">>, [], [<<"user1">>],
                    [], [], active},
            {Name, Start, End, Nodes, Users, State, _Flags} =
                ?M:extract_reservation_fields(Resv),
            ?assertEqual(<<"test_resv">>, Name),
            ?assertEqual(1000, Start),
            ?assertEqual(2000, End),
            ?assertEqual([<<"node1">>], Nodes),
            ?assertEqual([<<"user1">>], Users),
            ?assertEqual(active, State)
        end}
      ]}}.

%%====================================================================
%% Job Update Tests - build_field_updates/2
%%====================================================================

build_field_updates_test_() ->
    {"build_field_updates/2 field update building",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% Test 1: both NO_VAL returns empty map
       {"both NO_VAL returns empty map",
        fun() ->
            Result = ?M:build_field_updates(16#FFFFFFFE, <<>>),
            ?assertEqual(#{}, Result)
        end},

       %% Test 2: time limit set (converts minutes to seconds)
       {"time limit set converts minutes to seconds",
        fun() ->
            Result = ?M:build_field_updates(60, <<>>),
            ?assertEqual(#{time_limit => 3600}, Result)
        end},

       %% Test 3: name set
       {"name set",
        fun() ->
            Result = ?M:build_field_updates(16#FFFFFFFE, <<"new_name">>),
            ?assertEqual(#{name => <<"new_name">>}, Result)
        end},

       %% Test 4: both set
       {"both time_limit and name set",
        fun() ->
            Result = ?M:build_field_updates(30, <<"job_name">>),
            ?assertEqual(#{time_limit => 1800, name => <<"job_name">>}, Result)
        end},

       %% Test 5: empty name ignored
       {"empty name ignored",
        fun() ->
            Result = ?M:build_field_updates(120, <<>>),
            ?assertEqual(#{time_limit => 7200}, Result)
        end},

       %% Test 6: zero time limit
       {"zero time limit",
        fun() ->
            Result = ?M:build_field_updates(0, <<>>),
            ?assertEqual(#{time_limit => 0}, Result)
        end}
      ]}}.

%%====================================================================
%% Extended Edge Case Tests
%%====================================================================

edge_case_tests_test_() ->
    {"Additional edge case coverage",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       %% ensure_binary edge cases
       {"ensure_binary with unicode list",
        fun() ->
            %% Unicode characters in list
            Result = ?M:ensure_binary([104, 101, 108, 108, 111]),
            ?assertEqual(<<"hello">>, Result)
        end},

       %% format_licenses with edge cases
       {"format_licenses with zero count",
        ?_assertEqual(<<"lic:0">>, ?M:format_licenses([{<<"lic">>, 0}]))},

       %% parse_job_id_str edge cases
       {"parse_job_id_str with trailing underscore",
        fun() ->
            Result = ?M:parse_job_id_str(<<"123_">>),
            ?assertEqual(123, Result)
        end},

       %% Reservation flags - all flags
       {"parse all reservation flags",
        fun() ->
            %% weekday = 0x0010, weekend = 0x0020
            Flags = ?M:parse_reservation_flags(16#0010 bor 16#0020),
            ?assert(lists:member(weekday, Flags)),
            ?assert(lists:member(weekend, Flags))
        end},

       %% More reservation flags
       {"parse any and first_cores flags",
        fun() ->
            %% any = 0x0040, first_cores = 0x0080
            Flags = ?M:parse_reservation_flags(16#0040 bor 16#0080),
            ?assert(lists:member(any, Flags)),
            ?assert(lists:member(first_cores, Flags))
        end},

       {"parse purge_comp and part_nodes flags",
        fun() ->
            %% purge_comp = 0x0200, part_nodes = 0x0400
            Flags = ?M:parse_reservation_flags(16#0200 bor 16#0400),
            ?assert(lists:member(purge_comp, Flags)),
            ?assert(lists:member(part_nodes, Flags))
        end},

       {"parse no_hold_jobs_after and static_alloc flags",
        fun() ->
            %% no_hold_jobs_after = 0x1000, static_alloc = 0x2000
            Flags = ?M:parse_reservation_flags(16#1000 bor 16#2000),
            ?assert(lists:member(no_hold_jobs_after, Flags)),
            ?assert(lists:member(static_alloc, Flags))
        end},

       {"parse no_hold_jobs flag",
        fun() ->
            %% no_hold_jobs = 0x4000
            Flags = ?M:parse_reservation_flags(16#4000),
            ?assert(lists:member(no_hold_jobs, Flags))
        end},

       %% safe_binary_to_integer edge cases
       {"safe_binary_to_integer with plus sign works",
        ?_assertEqual(42, ?M:safe_binary_to_integer(<<"+42">>))},

       {"safe_binary_to_integer with spaces",
        ?_assertEqual(0, ?M:safe_binary_to_integer(<<" 42 ">>))},

       %% State conversion edge cases
       {"job_state_to_slurm with held state",
        ?_assertEqual(?JOB_PENDING, ?M:job_state_to_slurm(held))},

       %% format functions with single element
       {"format_features with gpu tag",
        ?_assertEqual(<<"gpu">>, ?M:format_features([<<"gpu">>]))},

       {"format_partitions with single partition",
        ?_assertEqual(<<"batch">>, ?M:format_partitions([<<"batch">>]))}
      ]}}.

%%====================================================================
%% Comprehensive State Tests
%%====================================================================

comprehensive_state_tests_test_() ->
    {"Comprehensive state conversion tests",
     [
      %% Job states - ensure all documented states work
      {"all job states covered",
       fun() ->
           States = [pending, configuring, running, completing, completed,
                    cancelled, failed, timeout, node_fail],
           lists:foreach(fun(S) ->
               Result = ?M:job_state_to_slurm(S),
               ?assert(is_integer(Result))
           end, States)
       end},

      %% Node states - ensure all documented states work
      {"all node states covered",
       fun() ->
           States = [up, down, drain, idle, allocated, mixed],
           lists:foreach(fun(S) ->
               Result = ?M:node_state_to_slurm(S),
               ?assert(is_integer(Result))
           end, States)
       end},

      %% Partition states - ensure all documented states work
      {"all partition states covered",
       fun() ->
           States = [up, down, drain, inactive],
           lists:foreach(fun(S) ->
               Result = ?M:partition_state_to_slurm(S),
               ?assert(is_integer(Result))
           end, States)
       end},

      %% Step states - ensure all documented states work
      {"all step states covered",
       fun() ->
           States = [pending, running, completing, completed, cancelled, failed],
           lists:foreach(fun(S) ->
               Result = ?M:step_state_to_slurm(S),
               ?assert(is_integer(Result))
           end, States)
       end}
     ]}.

%%====================================================================
%% Summary Test Count
%%====================================================================
%% Total tests: 115+
%% - ensure_binary: 10 tests
%% - error_to_binary: 8 tests
%% - safe_binary_to_integer: 10 tests
%% - job_state_to_slurm: 11 tests
%% - node_state_to_slurm: 8 tests
%% - partition_state_to_slurm: 6 tests
%% - step_state_to_slurm: 8 tests
%% - default_time: 4 tests
%% - default_partition: 4 tests
%% - default_time_limit: 4 tests
%% - default_priority: 5 tests
%% - default_work_dir: 4 tests
%% - format_allocated_nodes: 5 tests
%% - format_features: 4 tests
%% - format_partitions: 3 tests
%% - format_node_list: 3 tests
%% - format_licenses: 6 tests
%% - parse_job_id_str: 10 tests
%% - reservation_state_to_flags: 4 tests
%% - determine_reservation_type: 12 tests
%% - parse_reservation_flags: 9 tests
%% - generate_reservation_name: 4 tests
%% - extract_reservation_fields: 3 tests
%% - build_field_updates: 6 tests
%% - Edge cases: 15+ tests
%% - Comprehensive state tests: 4 tests
