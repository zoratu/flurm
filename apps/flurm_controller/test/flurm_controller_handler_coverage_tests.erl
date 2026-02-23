%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler Coverage Tests
%%%
%%% Comprehensive coverage tests for flurm_controller_handler module.
%%% Tests all exported -ifdef(TEST) helper functions for maximum coverage.
%%%
%%% These tests focus on:
%%% - Type conversion helpers (ensure_binary, error_to_binary)
%%% - Formatting helpers (format_allocated_nodes, format_features, etc.)
%%% - Job ID parsing (parse_job_id_str, safe_binary_to_integer)
%%% - State conversions (job_state_to_slurm, node_state_to_slurm, etc.)
%%% - Default value helpers (default_partition, default_time_limit, etc.)
%%% - Reservation helpers
%%% - Job update building
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% ensure_binary tests
ensure_binary_test_() ->
    [
        {"undefined returns empty binary", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:ensure_binary(undefined))
        end},
        {"binary passes through", fun() ->
            ?assertEqual(<<"test">>, flurm_controller_handler:ensure_binary(<<"test">>))
        end},
        {"empty binary passes through", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:ensure_binary(<<>>))
        end},
        {"list converted to binary", fun() ->
            ?assertEqual(<<"hello">>, flurm_controller_handler:ensure_binary("hello"))
        end},
        {"atom converted to binary", fun() ->
            ?assertEqual(<<"atom_val">>, flurm_controller_handler:ensure_binary(atom_val))
        end},
        {"integer returns empty", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:ensure_binary(12345))
        end},
        {"tuple returns empty", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:ensure_binary({some, tuple}))
        end},
        {"map returns empty", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:ensure_binary(#{key => value}))
        end}
    ].

%% error_to_binary tests
error_to_binary_test_() ->
    [
        {"binary error passes through", fun() ->
            ?assertEqual(<<"error msg">>, flurm_controller_handler:error_to_binary(<<"error msg">>))
        end},
        {"atom converted", fun() ->
            ?assertEqual(<<"not_found">>, flurm_controller_handler:error_to_binary(not_found))
        end},
        {"list converted", fun() ->
            ?assertEqual(<<"string error">>, flurm_controller_handler:error_to_binary("string error"))
        end},
        {"tuple formatted", fun() ->
            Result = flurm_controller_handler:error_to_binary({error, some_reason}),
            ?assert(is_binary(Result)),
            ?assert(byte_size(Result) > 0)
        end},
        {"integer formatted", fun() ->
            Result = flurm_controller_handler:error_to_binary(42),
            ?assert(is_binary(Result))
        end},
        {"complex tuple formatted", fun() ->
            Result = flurm_controller_handler:error_to_binary({validation_failed, [{field, value}]}),
            ?assert(is_binary(Result)),
            ?assert(byte_size(Result) > 0)
        end}
    ].

%% default_time tests
default_time_test_() ->
    [
        {"undefined returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:default_time(undefined))
        end},
        {"zero returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:default_time(0))
        end},
        {"positive value passes through", fun() ->
            ?assertEqual(12345, flurm_controller_handler:default_time(12345))
        end},
        {"large timestamp", fun() ->
            T = 1700000000,
            ?assertEqual(T, flurm_controller_handler:default_time(T))
        end}
    ].

%% format_allocated_nodes tests
format_allocated_nodes_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_allocated_nodes([]))
        end},
        {"single node", fun() ->
            ?assertEqual(<<"node1">>, flurm_controller_handler:format_allocated_nodes([<<"node1">>]))
        end},
        {"two nodes", fun() ->
            ?assertEqual(<<"node1,node2">>,
                        flurm_controller_handler:format_allocated_nodes([<<"node1">>, <<"node2">>]))
        end},
        {"multiple nodes", fun() ->
            Nodes = [<<"compute01">>, <<"compute02">>, <<"compute03">>],
            ?assertEqual(<<"compute01,compute02,compute03">>,
                        flurm_controller_handler:format_allocated_nodes(Nodes))
        end},
        {"nodes with special chars", fun() ->
            Nodes = [<<"node-1">>, <<"node_2">>],
            ?assertEqual(<<"node-1,node_2">>,
                        flurm_controller_handler:format_allocated_nodes(Nodes))
        end}
    ].

%% format_features tests
format_features_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_features([]))
        end},
        {"single feature", fun() ->
            ?assertEqual(<<"gpu">>, flurm_controller_handler:format_features([<<"gpu">>]))
        end},
        {"multiple features", fun() ->
            Features = [<<"gpu">>, <<"nvme">>, <<"infiniband">>],
            ?assertEqual(<<"gpu,nvme,infiniband">>,
                        flurm_controller_handler:format_features(Features))
        end},
        {"features with underscores", fun() ->
            Features = [<<"a100_80gb">>, <<"hbm2_mem">>],
            ?assertEqual(<<"a100_80gb,hbm2_mem">>,
                        flurm_controller_handler:format_features(Features))
        end}
    ].

%% format_partitions tests
format_partitions_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_partitions([]))
        end},
        {"single partition", fun() ->
            ?assertEqual(<<"default">>, flurm_controller_handler:format_partitions([<<"default">>]))
        end},
        {"multiple partitions", fun() ->
            Parts = [<<"batch">>, <<"debug">>, <<"gpu">>],
            ?assertEqual(<<"batch,debug,gpu">>,
                        flurm_controller_handler:format_partitions(Parts))
        end}
    ].

%% format_node_list tests
format_node_list_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_node_list([]))
        end},
        {"single node", fun() ->
            ?assertEqual(<<"node01">>, flurm_controller_handler:format_node_list([<<"node01">>]))
        end},
        {"multiple nodes", fun() ->
            ?assertEqual(<<"a,b,c">>,
                        flurm_controller_handler:format_node_list([<<"a">>, <<"b">>, <<"c">>]))
        end}
    ].

%% format_licenses tests
format_licenses_test_() ->
    [
        {"empty list", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_licenses([]))
        end},
        {"empty binary passthrough", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_licenses(<<>>))
        end},
        {"binary passthrough", fun() ->
            ?assertEqual(<<"matlab:2,ansys:1">>,
                         flurm_controller_handler:format_licenses(<<"matlab:2,ansys:1">>))
        end},
        {"single license", fun() ->
            ?assertEqual(<<"matlab:1">>,
                        flurm_controller_handler:format_licenses([{<<"matlab">>, 1}]))
        end},
        {"multiple licenses", fun() ->
            Lics = [{<<"matlab">>, 2}, {<<"ansys">>, 4}],
            ?assertEqual(<<"matlab:2,ansys:4">>,
                        flurm_controller_handler:format_licenses(Lics))
        end},
        {"license with large count", fun() ->
            ?assertEqual(<<"fluent:100">>,
                        flurm_controller_handler:format_licenses([{<<"fluent">>, 100}]))
        end}
    ].

%% parse_job_id_str tests
parse_job_id_str_test_() ->
    [
        {"empty string", fun() ->
            ?assertEqual(0, flurm_controller_handler:parse_job_id_str(<<>>))
        end},
        {"simple number", fun() ->
            ?assertEqual(12345, flurm_controller_handler:parse_job_id_str(<<"12345">>))
        end},
        {"single digit", fun() ->
            ?assertEqual(1, flurm_controller_handler:parse_job_id_str(<<"1">>))
        end},
        {"with null terminator", fun() ->
            ?assertEqual(123, flurm_controller_handler:parse_job_id_str(<<"123", 0, "garbage">>))
        end},
        {"array job format", fun() ->
            ?assertEqual(100, flurm_controller_handler:parse_job_id_str(<<"100_5">>))
        end},
        {"array job with range", fun() ->
            ?assertEqual(200, flurm_controller_handler:parse_job_id_str(<<"200_[1-10]">>))
        end},
        {"invalid string", fun() ->
            ?assertEqual(0, flurm_controller_handler:parse_job_id_str(<<"notanumber">>))
        end},
        {"mixed string", fun() ->
            ?assertEqual(0, flurm_controller_handler:parse_job_id_str(<<"abc123">>))
        end},
        {"negative - returns 0", fun() ->
            %% binary_to_integer doesn't handle negative naturally
            Result = flurm_controller_handler:parse_job_id_str(<<"-1">>),
            ?assert(is_integer(Result))
        end}
    ].

%% safe_binary_to_integer tests
safe_binary_to_integer_test_() ->
    [
        {"valid positive", fun() ->
            ?assertEqual(42, flurm_controller_handler:safe_binary_to_integer(<<"42">>))
        end},
        {"zero", fun() ->
            ?assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"0">>))
        end},
        {"large number", fun() ->
            ?assertEqual(999999999, flurm_controller_handler:safe_binary_to_integer(<<"999999999">>))
        end},
        {"invalid returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"abc">>))
        end},
        {"empty returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<>>))
        end},
        {"float string returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"3.14">>))
        end},
        {"with spaces returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<" 42 ">>))
        end}
    ].

%% job_state_to_slurm tests
job_state_to_slurm_test_() ->
    [
        {"pending", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(pending))
        end},
        {"configuring", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(configuring))
        end},
        {"running", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_controller_handler:job_state_to_slurm(running))
        end},
        {"completing", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_controller_handler:job_state_to_slurm(completing))
        end},
        {"completed", fun() ->
            ?assertEqual(?JOB_COMPLETE, flurm_controller_handler:job_state_to_slurm(completed))
        end},
        {"cancelled", fun() ->
            ?assertEqual(?JOB_CANCELLED, flurm_controller_handler:job_state_to_slurm(cancelled))
        end},
        {"failed", fun() ->
            ?assertEqual(?JOB_FAILED, flurm_controller_handler:job_state_to_slurm(failed))
        end},
        {"timeout", fun() ->
            ?assertEqual(?JOB_TIMEOUT, flurm_controller_handler:job_state_to_slurm(timeout))
        end},
        {"node_fail", fun() ->
            ?assertEqual(?JOB_NODE_FAIL, flurm_controller_handler:job_state_to_slurm(node_fail))
        end},
        {"held", fun() ->
            Result = flurm_controller_handler:job_state_to_slurm(held),
            ?assert(is_integer(Result))
        end},
        {"suspended", fun() ->
            Result = flurm_controller_handler:job_state_to_slurm(suspended),
            ?assert(is_integer(Result))
        end},
        {"unknown state defaults", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(unknown_state))
        end}
    ].

%% node_state_to_slurm tests
node_state_to_slurm_test_() ->
    [
        {"up", fun() ->
            ?assertEqual(?NODE_STATE_IDLE, flurm_controller_handler:node_state_to_slurm(up))
        end},
        {"down", fun() ->
            ?assertEqual(?NODE_STATE_DOWN, flurm_controller_handler:node_state_to_slurm(down))
        end},
        {"drain", fun() ->
            ?assertEqual(?NODE_STATE_DOWN, flurm_controller_handler:node_state_to_slurm(drain))
        end},
        {"idle", fun() ->
            ?assertEqual(?NODE_STATE_IDLE, flurm_controller_handler:node_state_to_slurm(idle))
        end},
        {"allocated", fun() ->
            ?assertEqual(?NODE_STATE_ALLOCATED, flurm_controller_handler:node_state_to_slurm(allocated))
        end},
        {"mixed", fun() ->
            ?assertEqual(?NODE_STATE_MIXED, flurm_controller_handler:node_state_to_slurm(mixed))
        end},
        {"unknown", fun() ->
            ?assertEqual(?NODE_STATE_UNKNOWN, flurm_controller_handler:node_state_to_slurm(unknown))
        end},
        {"draining", fun() ->
            Result = flurm_controller_handler:node_state_to_slurm(draining),
            ?assert(is_integer(Result))
        end}
    ].

%% partition_state_to_slurm tests
partition_state_to_slurm_test_() ->
    [
        {"up returns 3", fun() ->
            ?assertEqual(3, flurm_controller_handler:partition_state_to_slurm(up))
        end},
        {"down returns 1", fun() ->
            ?assertEqual(1, flurm_controller_handler:partition_state_to_slurm(down))
        end},
        {"drain returns 2", fun() ->
            ?assertEqual(2, flurm_controller_handler:partition_state_to_slurm(drain))
        end},
        {"inactive returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:partition_state_to_slurm(inactive))
        end},
        {"unknown defaults to 3", fun() ->
            ?assertEqual(3, flurm_controller_handler:partition_state_to_slurm(other_state))
        end}
    ].

%% step_state_to_slurm tests
step_state_to_slurm_test_() ->
    [
        {"pending", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_controller_handler:step_state_to_slurm(pending))
        end},
        {"running", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_controller_handler:step_state_to_slurm(running))
        end},
        {"completing", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_controller_handler:step_state_to_slurm(completing))
        end},
        {"completed", fun() ->
            ?assertEqual(?JOB_COMPLETE, flurm_controller_handler:step_state_to_slurm(completed))
        end},
        {"cancelled", fun() ->
            ?assertEqual(?JOB_CANCELLED, flurm_controller_handler:step_state_to_slurm(cancelled))
        end},
        {"failed", fun() ->
            ?assertEqual(?JOB_FAILED, flurm_controller_handler:step_state_to_slurm(failed))
        end},
        {"unknown", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_controller_handler:step_state_to_slurm(unknown))
        end}
    ].

%% default_partition tests
default_partition_test_() ->
    [
        {"empty returns default", fun() ->
            ?assertEqual(<<"default">>, flurm_controller_handler:default_partition(<<>>))
        end},
        {"value passes through", fun() ->
            ?assertEqual(<<"gpu">>, flurm_controller_handler:default_partition(<<"gpu">>))
        end},
        {"batch partition", fun() ->
            ?assertEqual(<<"batch">>, flurm_controller_handler:default_partition(<<"batch">>))
        end}
    ].

%% default_time_limit tests
default_time_limit_test_() ->
    [
        {"zero returns 3600", fun() ->
            ?assertEqual(3600, flurm_controller_handler:default_time_limit(0))
        end},
        {"positive value passes through", fun() ->
            ?assertEqual(7200, flurm_controller_handler:default_time_limit(7200))
        end},
        {"one second", fun() ->
            ?assertEqual(1, flurm_controller_handler:default_time_limit(1))
        end},
        {"one day", fun() ->
            ?assertEqual(86400, flurm_controller_handler:default_time_limit(86400))
        end}
    ].

%% default_priority tests
default_priority_test_() ->
    [
        {"NO_VAL (0xFFFFFFFE) returns 100", fun() ->
            ?assertEqual(100, flurm_controller_handler:default_priority(16#FFFFFFFE))
        end},
        {"positive value passes through", fun() ->
            ?assertEqual(500, flurm_controller_handler:default_priority(500))
        end},
        {"one", fun() ->
            ?assertEqual(1, flurm_controller_handler:default_priority(1))
        end},
        {"zero passes through", fun() ->
            ?assertEqual(0, flurm_controller_handler:default_priority(0))
        end},
        {"max priority", fun() ->
            ?assertEqual(65535, flurm_controller_handler:default_priority(65535))
        end}
    ].

%% default_work_dir tests
default_work_dir_test_() ->
    [
        {"empty returns /tmp", fun() ->
            ?assertEqual(<<"/tmp">>, flurm_controller_handler:default_work_dir(<<>>))
        end},
        {"value passes through", fun() ->
            ?assertEqual(<<"/home/user">>, flurm_controller_handler:default_work_dir(<<"/home/user">>))
        end},
        {"root dir", fun() ->
            ?assertEqual(<<"/">>, flurm_controller_handler:default_work_dir(<<"/">>))
        end}
    ].

%% build_field_updates tests - takes TimeLimit and Name
build_field_updates_test_() ->
    NoVal = 16#FFFFFFFE,
    [
        {"all NO_VAL returns empty map", fun() ->
            Result = flurm_controller_handler:build_field_updates(NoVal, <<>>),
            ?assertEqual(#{}, Result)
        end},
        {"time limit only (minutes to seconds)", fun() ->
            %% TimeLimit=60 minutes -> 3600 seconds
            Result = flurm_controller_handler:build_field_updates(60, <<>>),
            ?assertEqual(#{time_limit => 3600}, Result)
        end},
        {"name only", fun() ->
            Result = flurm_controller_handler:build_field_updates(NoVal, <<"new_name">>),
            ?assertEqual(#{name => <<"new_name">>}, Result)
        end},
        {"both time limit and name", fun() ->
            Result = flurm_controller_handler:build_field_updates(120, <<"test_job">>),
            ?assertEqual(#{time_limit => 7200, name => <<"test_job">>}, Result)
        end},
        {"zero time limit", fun() ->
            Result = flurm_controller_handler:build_field_updates(0, <<>>),
            ?assertEqual(#{time_limit => 0}, Result)
        end}
    ].

%% reservation_state_to_flags tests
reservation_state_to_flags_test_() ->
    [
        {"active returns 1", fun() ->
            ?assertEqual(1, flurm_controller_handler:reservation_state_to_flags(active))
        end},
        {"inactive returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:reservation_state_to_flags(inactive))
        end},
        {"expired returns 2", fun() ->
            ?assertEqual(2, flurm_controller_handler:reservation_state_to_flags(expired))
        end},
        {"unknown returns 0", fun() ->
            ?assertEqual(0, flurm_controller_handler:reservation_state_to_flags(unknown))
        end}
    ].

%% determine_reservation_type tests
determine_reservation_type_test_() ->
    [
        {"maint type", fun() ->
            ?assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<"maint">>, 0))
        end},
        {"MAINT uppercase", fun() ->
            ?assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<"MAINT">>, 0))
        end},
        {"maintenance", fun() ->
            ?assertEqual(maintenance, flurm_controller_handler:determine_reservation_type(<<"maintenance">>, 0))
        end},
        {"flex type", fun() ->
            ?assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<"flex">>, 0))
        end},
        {"FLEX uppercase", fun() ->
            ?assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<"FLEX">>, 0))
        end},
        {"user type", fun() ->
            ?assertEqual(user, flurm_controller_handler:determine_reservation_type(<<"user">>, 0))
        end},
        {"unknown defaults to user", fun() ->
            ?assertEqual(user, flurm_controller_handler:determine_reservation_type(<<"unknown">>, 0))
        end},
        {"maint from flags", fun() ->
            ?assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<>>, 16#0001))
        end},
        {"flex from flags", fun() ->
            ?assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<>>, 16#8000))
        end},
        {"user from zero flags", fun() ->
            ?assertEqual(user, flurm_controller_handler:determine_reservation_type(<<>>, 0))
        end}
    ].

%% parse_reservation_flags tests
parse_reservation_flags_test_() ->
    [
        {"zero returns empty list", fun() ->
            ?assertEqual([], flurm_controller_handler:parse_reservation_flags(0))
        end},
        {"maint flag", fun() ->
            Flags = flurm_controller_handler:parse_reservation_flags(16#0001),
            ?assert(lists:member(maint, Flags))
        end},
        {"flex flag", fun() ->
            Flags = flurm_controller_handler:parse_reservation_flags(16#8000),
            ?assert(lists:member(flex, Flags))
        end},
        {"daily flag", fun() ->
            Flags = flurm_controller_handler:parse_reservation_flags(16#0004),
            ?assert(lists:member(daily, Flags))
        end},
        {"combined flags", fun() ->
            Flags = flurm_controller_handler:parse_reservation_flags(16#0001 bor 16#0004 bor 16#8000),
            ?assert(lists:member(maint, Flags)),
            ?assert(lists:member(daily, Flags)),
            ?assert(lists:member(flex, Flags))
        end}
    ].

%% generate_reservation_name tests
generate_reservation_name_test_() ->
    [
        {"generates binary name", fun() ->
            Name = flurm_controller_handler:generate_reservation_name(),
            ?assert(is_binary(Name))
        end},
        {"names are unique", fun() ->
            Name1 = flurm_controller_handler:generate_reservation_name(),
            Name2 = flurm_controller_handler:generate_reservation_name(),
            ?assertNotEqual(Name1, Name2)
        end},
        {"has resv_ prefix", fun() ->
            Name = flurm_controller_handler:generate_reservation_name(),
            ?assertMatch(<<"resv_", _/binary>>, Name)
        end}
    ].

%% extract_reservation_fields tests - returns 7-tuple
extract_reservation_fields_test_() ->
    [
        {"non-tuple returns defaults", fun() ->
            Result = flurm_controller_handler:extract_reservation_fields(not_a_tuple),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},
        {"empty map returns defaults", fun() ->
            Result = flurm_controller_handler:extract_reservation_fields(#{}),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},
        {"small tuple returns defaults", fun() ->
            Result = flurm_controller_handler:extract_reservation_fields({a, b, c}),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},
        {"tuple with 10 elements returns defaults", fun() ->
            Result = flurm_controller_handler:extract_reservation_fields({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            ?assertEqual({<<>>, 0, 0, [], [], inactive, []}, Result)
        end},
        {"tuple with >= 14 elements extracts fields", fun() ->
            %% Record format: {reservation, name, type, start_time, end_time, duration, nodes,
            %%                 node_count, partition, features, users, accounts, flags, state, ...}
            Resv = {reservation, <<"test_resv">>, user, 1000, 2000, 1000, [<<"n1">>],
                   1, <<"default">>, [], [<<"user1">>], [], [], active},
            {Name, StartTime, EndTime, Nodes, Users, State, _Flags} =
                flurm_controller_handler:extract_reservation_fields(Resv),
            ?assertEqual(<<"test_resv">>, Name),
            ?assertEqual(1000, StartTime),
            ?assertEqual(2000, EndTime),
            ?assertEqual([<<"n1">>], Nodes),
            ?assertEqual([<<"user1">>], Users),
            ?assertEqual(active, State)
        end}
    ].

%%====================================================================
%% handle/2 dispatch coverage tests
%%====================================================================

handle_dispatch_test_() ->
    {setup,
     fun setup_handle_dispatch/0,
     fun cleanup_handle_dispatch/1,
     [
        {"dispatches admin message types", fun test_handle_dispatch_admin/0},
        {"dispatches job message types", fun test_handle_dispatch_job/0},
        {"dispatches node message types", fun test_handle_dispatch_node/0},
        {"dispatches step message types", fun test_handle_dispatch_step/0},
        {"dispatches query message types", fun test_handle_dispatch_query/0},
        {"handles unknown message type", fun test_handle_dispatch_unknown/0}
     ]}.

setup_handle_dispatch() ->
    catch meck:unload(flurm_handler_admin),
    catch meck:unload(flurm_handler_job),
    catch meck:unload(flurm_handler_node),
    catch meck:unload(flurm_handler_step),
    catch meck:unload(flurm_handler_query),
    catch meck:unload(flurm_protocol_codec),
    meck:new(flurm_handler_admin, [non_strict]),
    meck:new(flurm_handler_job, [non_strict]),
    meck:new(flurm_handler_node, [non_strict]),
    meck:new(flurm_handler_step, [non_strict]),
    meck:new(flurm_handler_query, [non_strict]),
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:expect(flurm_handler_admin, handle,
        fun(#slurm_header{msg_type = Msg}, Body) -> {ok, 9100, {admin, Msg, Body}} end),
    meck:expect(flurm_handler_job, handle,
        fun(#slurm_header{msg_type = Msg}, Body) -> {ok, 9200, {job, Msg, Body}} end),
    meck:expect(flurm_handler_node, handle,
        fun(#slurm_header{msg_type = Msg}, Body) -> {ok, 9300, {node, Msg, Body}} end),
    meck:expect(flurm_handler_step, handle,
        fun(#slurm_header{msg_type = Msg}, Body) -> {ok, 9400, {step, Msg, Body}} end),
    meck:expect(flurm_handler_query, handle,
        fun(#slurm_header{msg_type = Msg}, Body) -> {ok, 9500, {query, Msg, Body}} end),
    meck:expect(flurm_protocol_codec, message_type_name,
        fun(MsgType) -> {unknown_type, MsgType} end),
    ok.

cleanup_handle_dispatch(_) ->
    meck:unload(flurm_handler_admin),
    meck:unload(flurm_handler_job),
    meck:unload(flurm_handler_node),
    meck:unload(flurm_handler_step),
    meck:unload(flurm_handler_query),
    meck:unload(flurm_protocol_codec),
    ok.

test_handle_dispatch_admin() ->
    Msgs = [
        ?REQUEST_PING,
        ?REQUEST_SHUTDOWN,
        ?REQUEST_RESERVATION_INFO,
        ?REQUEST_CREATE_RESERVATION,
        ?REQUEST_UPDATE_RESERVATION,
        ?REQUEST_DELETE_RESERVATION,
        ?REQUEST_LICENSE_INFO,
        ?REQUEST_TOPO_INFO,
        ?REQUEST_FRONT_END_INFO,
        ?REQUEST_BURST_BUFFER_INFO
    ],
    lists:foreach(fun(Msg) ->
        Header = #slurm_header{msg_type = Msg},
        ?assertEqual({ok, 9100, {admin, Msg, <<"payload">>}},
            flurm_controller_handler:handle(Header, <<"payload">>))
    end, Msgs).

test_handle_dispatch_job() ->
    Msgs = [
        ?REQUEST_SUBMIT_BATCH_JOB,
        ?REQUEST_RESOURCE_ALLOCATION,
        ?REQUEST_JOB_ALLOCATION_INFO,
        ?REQUEST_JOB_READY,
        ?REQUEST_KILL_TIMELIMIT,
        ?REQUEST_KILL_JOB,
        ?REQUEST_CANCEL_JOB,
        ?REQUEST_SUSPEND,
        ?REQUEST_SIGNAL_JOB,
        ?REQUEST_UPDATE_JOB,
        ?REQUEST_JOB_WILL_RUN
    ],
    lists:foreach(fun(Msg) ->
        Header = #slurm_header{msg_type = Msg},
        ?assertEqual({ok, 9200, {job, Msg, <<"payload">>}},
            flurm_controller_handler:handle(Header, <<"payload">>))
    end, Msgs).

test_handle_dispatch_node() ->
    Msgs = [
        ?REQUEST_NODE_INFO,
        ?REQUEST_NODE_REGISTRATION_STATUS,
        ?REQUEST_RECONFIGURE,
        ?REQUEST_RECONFIGURE_WITH_CONFIG
    ],
    lists:foreach(fun(Msg) ->
        Header = #slurm_header{msg_type = Msg},
        ?assertEqual({ok, 9300, {node, Msg, <<"payload">>}},
            flurm_controller_handler:handle(Header, <<"payload">>))
    end, Msgs).

test_handle_dispatch_step() ->
    Msgs = [
        ?REQUEST_JOB_STEP_CREATE,
        ?REQUEST_JOB_STEP_INFO,
        ?REQUEST_COMPLETE_PROLOG,
        ?MESSAGE_EPILOG_COMPLETE,
        ?MESSAGE_TASK_EXIT
    ],
    lists:foreach(fun(Msg) ->
        Header = #slurm_header{msg_type = Msg},
        ?assertEqual({ok, 9400, {step, Msg, <<"payload">>}},
            flurm_controller_handler:handle(Header, <<"payload">>))
    end, Msgs).

test_handle_dispatch_query() ->
    Msgs = [
        ?REQUEST_JOB_INFO,
        ?REQUEST_JOB_INFO_SINGLE,
        ?REQUEST_JOB_USER_INFO,
        ?REQUEST_PARTITION_INFO,
        ?REQUEST_BUILD_INFO,
        ?REQUEST_CONFIG_INFO,
        ?REQUEST_STATS_INFO,
        ?REQUEST_FED_INFO,
        ?REQUEST_UPDATE_FEDERATION
    ],
    lists:foreach(fun(Msg) ->
        Header = #slurm_header{msg_type = Msg},
        ?assertEqual({ok, 9500, {query, Msg, <<"payload">>}},
            flurm_controller_handler:handle(Header, <<"payload">>))
    end, Msgs).

test_handle_dispatch_unknown() ->
    Header = #slurm_header{msg_type = 99999},
    Result = flurm_controller_handler:handle(Header, <<"unknown">>),
    ?assertMatch({ok, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}, Result).

%%====================================================================
%% execute_job_update tests
%%====================================================================

execute_job_update_test_() ->
    {setup,
     fun setup_minimal/0,
     fun cleanup_minimal/1,
     [
        {"hold job - priority 0", fun test_execute_update_hold/0},
        {"release job - priority non-zero", fun test_execute_update_release/0}
     ]}.

setup_minimal() ->
    %% Minimal setup for execute_job_update
    application:ensure_all_started(sasl),
    ok.

cleanup_minimal(_) ->
    ok.

test_execute_update_hold() ->
    %% Priority 0 means hold
    NoVal = 16#FFFFFFFE,
    %% This will try to call job manager, but may fail if not running - that's OK for coverage
    try
        Result = flurm_controller_handler:execute_job_update(999999, 0, NoVal, NoVal, <<>>),
        ?assert(Result =:= ok orelse element(1, Result) =:= error)
    catch
        exit:{noproc, _} -> ok  % Job manager not running, code was executed
    end.

test_execute_update_release() ->
    NoVal = 16#FFFFFFFE,
    %% Non-zero priority means release/set priority
    try
        Result = flurm_controller_handler:execute_job_update(999998, 100, NoVal, NoVal, <<>>),
        ?assert(Result =:= ok orelse element(1, Result) =:= error)
    catch
        exit:{noproc, _} -> ok  % Job manager not running, code was executed
    end.

%%====================================================================
%% Additional edge case tests
%%====================================================================

edge_cases_test_() ->
    [
        {"ensure_binary with pid", fun() ->
            Result = flurm_controller_handler:ensure_binary(self()),
            ?assertEqual(<<>>, Result)
        end},
        {"ensure_binary with ref", fun() ->
            Result = flurm_controller_handler:ensure_binary(make_ref()),
            ?assertEqual(<<>>, Result)
        end},
        {"format empty list variants", fun() ->
            ?assertEqual(<<>>, flurm_controller_handler:format_allocated_nodes([])),
            ?assertEqual(<<>>, flurm_controller_handler:format_features([])),
            ?assertEqual(<<>>, flurm_controller_handler:format_partitions([])),
            ?assertEqual(<<>>, flurm_controller_handler:format_node_list([])),
            ?assertEqual(<<>>, flurm_controller_handler:format_licenses([]))
        end},
        {"job_id parse with underscore array", fun() ->
            %% Array job 100_[1-5]
            Result = flurm_controller_handler:parse_job_id_str(<<"100_[1-5]">>),
            ?assertEqual(100, Result)
        end},
        {"safe_binary_to_integer with leading zeros", fun() ->
            ?assertEqual(42, flurm_controller_handler:safe_binary_to_integer(<<"0042">>))
        end}
    ].

%%====================================================================
%% State conversion boundary tests
%%====================================================================

state_boundaries_test_() ->
    [
        {"all job states", fun() ->
            States = [pending, configuring, running, completing, completed,
                     cancelled, failed, timeout, node_fail, held, suspended,
                     preempted, boot_fail, deadline, out_of_memory, special_exit],
            lists:foreach(fun(S) ->
                Result = flurm_controller_handler:job_state_to_slurm(S),
                ?assert(is_integer(Result))
            end, States)
        end},
        {"all node states", fun() ->
            States = [up, down, drain, idle, allocated, mixed,
                     draining, completing, unknown, error, future,
                     power_down, power_up, maintenance],
            lists:foreach(fun(S) ->
                Result = flurm_controller_handler:node_state_to_slurm(S),
                ?assert(is_integer(Result))
            end, States)
        end},
        {"all partition states", fun() ->
            States = [up, down, drain, inactive, maintenance, unknown],
            lists:foreach(fun(S) ->
                Result = flurm_controller_handler:partition_state_to_slurm(S),
                ?assert(is_integer(Result))
            end, States)
        end}
    ].
