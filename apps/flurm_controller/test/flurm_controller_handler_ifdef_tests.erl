%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Handler Internal Function Tests
%%%
%%% Tests for internal helper functions exported via -ifdef(TEST).
%%% These tests focus on pure functions that don't require complex
%%% system setup.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Type Conversion Tests
%%====================================================================

ensure_binary_test_() ->
    [
        {"undefined returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary(undefined))},
        {"binary passthrough",
         ?_assertEqual(<<"test">>, flurm_controller_handler:ensure_binary(<<"test">>))},
        {"list to binary",
         ?_assertEqual(<<"hello">>, flurm_controller_handler:ensure_binary("hello"))},
        {"atom to binary",
         ?_assertEqual(<<"test_atom">>, flurm_controller_handler:ensure_binary(test_atom))},
        {"integer returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary(123))},
        {"empty binary passthrough",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary(<<>>))},
        {"empty list to binary",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary([]))}
    ].

error_to_binary_test_() ->
    [
        {"binary passthrough",
         ?_assertEqual(<<"error msg">>, flurm_controller_handler:error_to_binary(<<"error msg">>))},
        {"atom to binary",
         ?_assertEqual(<<"not_found">>, flurm_controller_handler:error_to_binary(not_found))},
        {"list to binary",
         ?_assertEqual(<<"error string">>, flurm_controller_handler:error_to_binary("error string"))},
        {"tuple formatted",
         fun() ->
             Result = flurm_controller_handler:error_to_binary({error, reason}),
             ?assert(is_binary(Result)),
             ?assert(byte_size(Result) > 0)
         end}
    ].

default_time_test_() ->
    [
        {"undefined returns 0",
         ?_assertEqual(0, flurm_controller_handler:default_time(undefined))},
        {"time passthrough",
         ?_assertEqual(1234567890, flurm_controller_handler:default_time(1234567890))},
        {"zero passthrough",
         ?_assertEqual(0, flurm_controller_handler:default_time(0))}
    ].

%%====================================================================
%% Formatting Helper Tests
%%====================================================================

format_allocated_nodes_test_() ->
    [
        {"empty list returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:format_allocated_nodes([]))},
        {"single node",
         ?_assertEqual(<<"node1">>, flurm_controller_handler:format_allocated_nodes([<<"node1">>]))},
        {"multiple nodes comma separated",
         ?_assertEqual(<<"node1,node2,node3">>,
                      flurm_controller_handler:format_allocated_nodes([<<"node1">>, <<"node2">>, <<"node3">>]))}
    ].

format_features_test_() ->
    [
        {"empty list returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:format_features([]))},
        {"single feature",
         ?_assertEqual(<<"gpu">>, flurm_controller_handler:format_features([<<"gpu">>]))},
        {"multiple features comma separated",
         ?_assertEqual(<<"gpu,ssd,fast">>,
                      flurm_controller_handler:format_features([<<"gpu">>, <<"ssd">>, <<"fast">>]))}
    ].

format_partitions_test_() ->
    [
        {"empty list returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:format_partitions([]))},
        {"single partition",
         ?_assertEqual(<<"default">>, flurm_controller_handler:format_partitions([<<"default">>]))},
        {"multiple partitions comma separated",
         ?_assertEqual(<<"default,gpu,batch">>,
                      flurm_controller_handler:format_partitions([<<"default">>, <<"gpu">>, <<"batch">>]))}
    ].

format_node_list_test_() ->
    [
        {"empty list returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:format_node_list([]))},
        {"single node",
         ?_assertEqual(<<"compute01">>, flurm_controller_handler:format_node_list([<<"compute01">>]))},
        {"multiple nodes comma separated",
         ?_assertEqual(<<"compute01,compute02,compute03">>,
                      flurm_controller_handler:format_node_list([<<"compute01">>, <<"compute02">>, <<"compute03">>]))}
    ].

format_licenses_test_() ->
    [
        {"empty list returns empty binary",
         ?_assertEqual(<<>>, flurm_controller_handler:format_licenses([]))},
        {"single license",
         ?_assertEqual(<<"matlab:5">>,
                      flurm_controller_handler:format_licenses([{<<"matlab">>, 5}]))},
        {"multiple licenses comma separated",
         ?_assertEqual(<<"matlab:5,ansys:2">>,
                      flurm_controller_handler:format_licenses([{<<"matlab">>, 5}, {<<"ansys">>, 2}]))}
    ].

%%====================================================================
%% Job ID Parsing Tests
%%====================================================================

parse_job_id_str_test_() ->
    [
        {"empty binary returns 0",
         ?_assertEqual(0, flurm_controller_handler:parse_job_id_str(<<>>))},
        {"simple job ID",
         ?_assertEqual(12345, flurm_controller_handler:parse_job_id_str(<<"12345">>))},
        {"array job ID with underscore",
         ?_assertEqual(12345, flurm_controller_handler:parse_job_id_str(<<"12345_0">>))},
        {"array job ID with task range",
         ?_assertEqual(12345, flurm_controller_handler:parse_job_id_str(<<"12345_[0-10]">>))},
        {"job ID with null terminator",
         ?_assertEqual(12345, flurm_controller_handler:parse_job_id_str(<<"12345", 0>>))},
        {"invalid job ID returns 0",
         ?_assertEqual(0, flurm_controller_handler:parse_job_id_str(<<"invalid">>))}
    ].

safe_binary_to_integer_test_() ->
    [
        {"valid integer",
         ?_assertEqual(12345, flurm_controller_handler:safe_binary_to_integer(<<"12345">>))},
        {"zero",
         ?_assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"0">>))},
        {"invalid returns 0",
         ?_assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"abc">>))},
        {"empty returns 0",
         ?_assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<>>))},
        {"negative number",
         ?_assertEqual(-123, flurm_controller_handler:safe_binary_to_integer(<<"-123">>))}
    ].

build_job_updates_test_() ->
    NoVal = 16#FFFFFFFE,
    [
        {"all NO_VAL returns empty map",
         ?_assertEqual(#{}, flurm_controller_handler:build_job_updates(NoVal, NoVal, NoVal))},
        {"priority 0 means hold",
         ?_assertEqual(#{state => held}, flurm_controller_handler:build_job_updates(0, NoVal, NoVal))},
        {"non-zero priority sets priority",
         ?_assertEqual(#{priority => 100}, flurm_controller_handler:build_job_updates(100, NoVal, NoVal))},
        {"time limit sets time_limit",
         ?_assertEqual(#{time_limit => 3600}, flurm_controller_handler:build_job_updates(NoVal, 3600, NoVal))},
        {"requeue=1 sets state to pending",
         ?_assertEqual(#{state => pending}, flurm_controller_handler:build_job_updates(NoVal, NoVal, 1))},
        {"combined updates",
         fun() ->
             Result = flurm_controller_handler:build_job_updates(100, 3600, NoVal),
             ?assertEqual(100, maps:get(priority, Result)),
             ?assertEqual(3600, maps:get(time_limit, Result))
         end}
    ].

%%====================================================================
%% State Conversion Tests
%%====================================================================

job_state_to_slurm_test_() ->
    [
        {"pending state",
         ?_assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(pending))},
        {"configuring state",
         ?_assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(configuring))},
        {"running state",
         ?_assertEqual(?JOB_RUNNING, flurm_controller_handler:job_state_to_slurm(running))},
        {"completing state",
         ?_assertEqual(?JOB_RUNNING, flurm_controller_handler:job_state_to_slurm(completing))},
        {"completed state",
         ?_assertEqual(?JOB_COMPLETE, flurm_controller_handler:job_state_to_slurm(completed))},
        {"cancelled state",
         ?_assertEqual(?JOB_CANCELLED, flurm_controller_handler:job_state_to_slurm(cancelled))},
        {"failed state",
         ?_assertEqual(?JOB_FAILED, flurm_controller_handler:job_state_to_slurm(failed))},
        {"timeout state",
         ?_assertEqual(?JOB_TIMEOUT, flurm_controller_handler:job_state_to_slurm(timeout))},
        {"node_fail state",
         ?_assertEqual(?JOB_NODE_FAIL, flurm_controller_handler:job_state_to_slurm(node_fail))},
        {"unknown state defaults to pending",
         ?_assertEqual(?JOB_PENDING, flurm_controller_handler:job_state_to_slurm(unknown_state))}
    ].

node_state_to_slurm_test_() ->
    [
        {"up state",
         ?_assertEqual(?NODE_STATE_IDLE, flurm_controller_handler:node_state_to_slurm(up))},
        {"down state",
         ?_assertEqual(?NODE_STATE_DOWN, flurm_controller_handler:node_state_to_slurm(down))},
        {"drain state",
         ?_assertEqual(?NODE_STATE_DOWN, flurm_controller_handler:node_state_to_slurm(drain))},
        {"idle state",
         ?_assertEqual(?NODE_STATE_IDLE, flurm_controller_handler:node_state_to_slurm(idle))},
        {"allocated state",
         ?_assertEqual(?NODE_STATE_ALLOCATED, flurm_controller_handler:node_state_to_slurm(allocated))},
        {"mixed state",
         ?_assertEqual(?NODE_STATE_MIXED, flurm_controller_handler:node_state_to_slurm(mixed))},
        {"unknown state defaults to unknown",
         ?_assertEqual(?NODE_STATE_UNKNOWN, flurm_controller_handler:node_state_to_slurm(unknown_state))}
    ].

partition_state_to_slurm_test_() ->
    [
        {"up state returns 3 (PARTITION_UP)",
         ?_assertEqual(3, flurm_controller_handler:partition_state_to_slurm(up))},
        {"down state returns 1 (PARTITION_DOWN)",
         ?_assertEqual(1, flurm_controller_handler:partition_state_to_slurm(down))},
        {"drain state returns 2 (PARTITION_DRAIN)",
         ?_assertEqual(2, flurm_controller_handler:partition_state_to_slurm(drain))},
        {"inactive state returns 0 (PARTITION_INACTIVE)",
         ?_assertEqual(0, flurm_controller_handler:partition_state_to_slurm(inactive))},
        {"unknown state defaults to UP",
         ?_assertEqual(3, flurm_controller_handler:partition_state_to_slurm(unknown_state))}
    ].

step_state_to_slurm_test_() ->
    [
        {"pending state",
         ?_assertEqual(?JOB_PENDING, flurm_controller_handler:step_state_to_slurm(pending))},
        {"running state",
         ?_assertEqual(?JOB_RUNNING, flurm_controller_handler:step_state_to_slurm(running))},
        {"completing state",
         ?_assertEqual(?JOB_RUNNING, flurm_controller_handler:step_state_to_slurm(completing))},
        {"completed state",
         ?_assertEqual(?JOB_COMPLETE, flurm_controller_handler:step_state_to_slurm(completed))},
        {"cancelled state",
         ?_assertEqual(?JOB_CANCELLED, flurm_controller_handler:step_state_to_slurm(cancelled))},
        {"failed state",
         ?_assertEqual(?JOB_FAILED, flurm_controller_handler:step_state_to_slurm(failed))},
        {"unknown state defaults to pending",
         ?_assertEqual(?JOB_PENDING, flurm_controller_handler:step_state_to_slurm(unknown_state))}
    ].

%%====================================================================
%% Default Value Helper Tests
%%====================================================================

default_partition_test_() ->
    [
        {"empty binary returns default",
         ?_assertEqual(<<"default">>, flurm_controller_handler:default_partition(<<>>))},
        {"partition passthrough",
         ?_assertEqual(<<"compute">>, flurm_controller_handler:default_partition(<<"compute">>))}
    ].

default_time_limit_test_() ->
    [
        {"zero returns 3600 (1 hour)",
         ?_assertEqual(3600, flurm_controller_handler:default_time_limit(0))},
        {"time limit passthrough",
         ?_assertEqual(7200, flurm_controller_handler:default_time_limit(7200))}
    ].

default_priority_test_() ->
    [
        {"zero returns 100",
         ?_assertEqual(100, flurm_controller_handler:default_priority(0))},
        {"priority passthrough",
         ?_assertEqual(500, flurm_controller_handler:default_priority(500))}
    ].

default_work_dir_test_() ->
    [
        {"empty binary returns /tmp",
         ?_assertEqual(<<"/tmp">>, flurm_controller_handler:default_work_dir(<<>>))},
        {"work dir passthrough",
         ?_assertEqual(<<"/home/user">>, flurm_controller_handler:default_work_dir(<<"/home/user">>))}
    ].

%%====================================================================
%% Reservation Helper Tests
%%====================================================================

reservation_state_to_flags_test_() ->
    [
        {"active state returns 1",
         ?_assertEqual(1, flurm_controller_handler:reservation_state_to_flags(active))},
        {"inactive state returns 0",
         ?_assertEqual(0, flurm_controller_handler:reservation_state_to_flags(inactive))},
        {"expired state returns 2",
         ?_assertEqual(2, flurm_controller_handler:reservation_state_to_flags(expired))},
        {"unknown state returns 0",
         ?_assertEqual(0, flurm_controller_handler:reservation_state_to_flags(unknown_state))}
    ].

determine_reservation_type_test_() ->
    [
        {"maint type from string",
         ?_assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<"maint">>, 0))},
        {"MAINT type from string (uppercase)",
         ?_assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<"MAINT">>, 0))},
        {"maintenance type from string",
         ?_assertEqual(maintenance, flurm_controller_handler:determine_reservation_type(<<"maintenance">>, 0))},
        {"flex type from string",
         ?_assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<"flex">>, 0))},
        {"FLEX type from string (uppercase)",
         ?_assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<"FLEX">>, 0))},
        {"user type from string",
         ?_assertEqual(user, flurm_controller_handler:determine_reservation_type(<<"user">>, 0))},
        {"USER type from string (uppercase)",
         ?_assertEqual(user, flurm_controller_handler:determine_reservation_type(<<"USER">>, 0))},
        {"maint inferred from flags",
         ?_assertEqual(maint, flurm_controller_handler:determine_reservation_type(<<>>, 16#0001))},
        {"flex inferred from flags",
         ?_assertEqual(flex, flurm_controller_handler:determine_reservation_type(<<>>, 16#8000))},
        {"user default when no type or special flags",
         ?_assertEqual(user, flurm_controller_handler:determine_reservation_type(<<>>, 0))},
        {"unknown type defaults to user",
         ?_assertEqual(user, flurm_controller_handler:determine_reservation_type(<<"unknown">>, 0))}
    ].

parse_reservation_flags_test_() ->
    [
        {"zero flags returns empty list",
         ?_assertEqual([], flurm_controller_handler:parse_reservation_flags(0))},
        {"maint flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0001),
             ?assert(lists:member(maint, Flags))
         end},
        {"flex flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#8000),
             ?assert(lists:member(flex, Flags))
         end},
        {"daily flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0004),
             ?assert(lists:member(daily, Flags))
         end},
        {"weekly flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0008),
             ?assert(lists:member(weekly, Flags))
         end},
        {"multiple flags",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0001 bor 16#0004 bor 16#8000),
             ?assert(lists:member(maint, Flags)),
             ?assert(lists:member(daily, Flags)),
             ?assert(lists:member(flex, Flags))
         end},
        {"ignore_jobs flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0002),
             ?assert(lists:member(ignore_jobs, Flags))
         end},
        {"overlap flag",
         fun() ->
             Flags = flurm_controller_handler:parse_reservation_flags(16#0800),
             ?assert(lists:member(overlap, Flags))
         end}
    ].

generate_reservation_name_test_() ->
    [
        {"generates binary",
         fun() ->
             Name = flurm_controller_handler:generate_reservation_name(),
             ?assert(is_binary(Name))
         end},
        {"starts with resv_ prefix",
         fun() ->
             Name = flurm_controller_handler:generate_reservation_name(),
             ?assertMatch(<<"resv_", _/binary>>, Name)
         end},
        {"generates unique names",
         fun() ->
             %% The random component ensures uniqueness without needing sleep
             Name1 = flurm_controller_handler:generate_reservation_name(),
             Name2 = flurm_controller_handler:generate_reservation_name(),
             ?assertNotEqual(Name1, Name2)
         end}
    ].

extract_reservation_fields_test_() ->
    [
        {"non-tuple returns defaults",
         fun() ->
             {Name, StartTime, EndTime, Nodes, Users, State, Flags} =
                 flurm_controller_handler:extract_reservation_fields(not_a_tuple),
             ?assertEqual(<<>>, Name),
             ?assertEqual(0, StartTime),
             ?assertEqual(0, EndTime),
             ?assertEqual([], Nodes),
             ?assertEqual([], Users),
             ?assertEqual(inactive, State),
             ?assertEqual([], Flags)
         end},
        {"small tuple returns defaults",
         fun() ->
             {Name, StartTime, EndTime, Nodes, Users, State, Flags} =
                 flurm_controller_handler:extract_reservation_fields({small, tuple}),
             ?assertEqual(<<>>, Name),
             ?assertEqual(0, StartTime),
             ?assertEqual(0, EndTime),
             ?assertEqual([], Nodes),
             ?assertEqual([], Users),
             ?assertEqual(inactive, State),
             ?assertEqual([], Flags)
         end},
        {"valid record tuple extracts fields",
         fun() ->
             %% Create a mock reservation tuple with at least 14 elements
             %% Format: {reservation, name, type, start_time, end_time, duration,
             %%          nodes, node_count, partition, features, users, flags, tres, state, ...}
             MockResv = {reservation, <<"test_resv">>, maint, 1000, 2000, 1000,
                        [<<"node1">>], 1, <<"default">>, [], [<<"user1">>], [maint], <<>>, active},
             {Name, StartTime, EndTime, Nodes, Users, State, Flags} =
                 flurm_controller_handler:extract_reservation_fields(MockResv),
             ?assertEqual(<<"test_resv">>, Name),
             ?assertEqual(1000, StartTime),
             ?assertEqual(2000, EndTime),
             ?assertEqual([<<"node1">>], Nodes),
             ?assertEqual([<<"user1">>], Users),
             ?assertEqual(active, State),
             ?assertEqual([maint], Flags)
         end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"ensure_binary with tuple returns empty",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary({some, tuple}))},
        {"ensure_binary with pid returns empty",
         ?_assertEqual(<<>>, flurm_controller_handler:ensure_binary(self()))},
        {"format_allocated_nodes with mixed types",
         fun() ->
             %% This should work with binary nodes
             Result = flurm_controller_handler:format_allocated_nodes([<<"a">>, <<"b">>]),
             ?assertEqual(<<"a,b">>, Result)
         end},
        {"parse_job_id_str with array notation",
         ?_assertEqual(100, flurm_controller_handler:parse_job_id_str(<<"100_[1-5]">>))},
        {"safe_binary_to_integer with float string returns 0",
         ?_assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"3.14">>))},
        {"safe_binary_to_integer with mixed content returns 0",
         ?_assertEqual(0, flurm_controller_handler:safe_binary_to_integer(<<"123abc">>))}
    ].
