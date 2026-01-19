%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_reservation internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_reservation_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Validate Spec Tests
%%====================================================================

validate_spec_missing_end_time_test() ->
    Spec = #{
        start_time => erlang:system_time(second) + 3600,
        nodes => [<<"node1">>]
    },
    ?assertEqual({error, missing_end_time_or_duration}, flurm_reservation:validate_spec(Spec)).

validate_spec_missing_nodes_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        start_time => Now + 3600,
        end_time => Now + 7200
    },
    ?assertEqual({error, missing_node_specification}, flurm_reservation:validate_spec(Spec)).

validate_spec_start_in_past_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        start_time => Now - 100,
        end_time => Now + 3600,
        nodes => [<<"node1">>]
    },
    ?assertEqual({error, start_time_in_past}, flurm_reservation:validate_spec(Spec)).

validate_spec_valid_with_end_time_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        start_time => Now + 3600,
        end_time => Now + 7200,
        nodes => [<<"node1">>]
    },
    ?assertEqual(ok, flurm_reservation:validate_spec(Spec)).

validate_spec_valid_with_duration_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        start_time => Now + 3600,
        duration => 3600,
        nodes => [<<"node1">>]
    },
    ?assertEqual(ok, flurm_reservation:validate_spec(Spec)).

validate_spec_valid_with_node_count_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        start_time => Now + 3600,
        end_time => Now + 7200,
        node_count => 4
    },
    ?assertEqual(ok, flurm_reservation:validate_spec(Spec)).

%%====================================================================
%% Build Reservation Tests
%%====================================================================

build_reservation_basic_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"test_res">>,
        start_time => Now + 3600,
        end_time => Now + 7200,
        nodes => [<<"node1">>, <<"node2">>],
        users => [<<"user1">>],
        created_by => <<"admin">>
    },
    Res = flurm_reservation:build_reservation(Spec),
    ?assertEqual(<<"test_res">>, element(2, Res)),
    ?assertEqual([<<"node1">>, <<"node2">>], element(7, Res)).

build_reservation_with_duration_test() ->
    Now = erlang:system_time(second),
    StartTime = Now + 3600,
    Duration = 1800,
    Spec = #{
        name => <<"test_res">>,
        start_time => StartTime,
        duration => Duration,
        nodes => [<<"node1">>]
    },
    Res = flurm_reservation:build_reservation(Spec),
    %% End time should be start_time + duration
    ?assertEqual(StartTime + Duration, element(5, Res)).

build_reservation_defaults_test() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => <<"test_res">>,
        start_time => Now + 3600,
        end_time => Now + 7200,
        nodes => [<<"node1">>]
    },
    Res = flurm_reservation:build_reservation(Spec),
    %% Check default type is 'user'
    ?assertEqual(user, element(3, Res)),
    %% State should be inactive
    ?assertEqual(inactive, element(15, Res)).

%%====================================================================
%% Times Overlap Tests
%%====================================================================

times_overlap_no_overlap_before_test() ->
    %% Period 1 ends before period 2 starts
    ?assertNot(flurm_reservation:times_overlap(100, 200, 300, 400)).

times_overlap_no_overlap_after_test() ->
    %% Period 1 starts after period 2 ends
    ?assertNot(flurm_reservation:times_overlap(500, 600, 300, 400)).

times_overlap_partial_start_test() ->
    %% Period 1 starts before period 2 but overlaps
    ?assert(flurm_reservation:times_overlap(100, 350, 300, 400)).

times_overlap_partial_end_test() ->
    %% Period 1 starts during period 2
    ?assert(flurm_reservation:times_overlap(350, 500, 300, 400)).

times_overlap_contained_test() ->
    %% Period 1 is contained within period 2
    ?assert(flurm_reservation:times_overlap(320, 380, 300, 400)).

times_overlap_contains_test() ->
    %% Period 1 contains period 2
    ?assert(flurm_reservation:times_overlap(200, 500, 300, 400)).

times_overlap_exact_test() ->
    %% Same time period
    ?assert(flurm_reservation:times_overlap(300, 400, 300, 400)).

times_overlap_adjacent_test() ->
    %% Period 1 ends exactly when period 2 starts - no overlap
    ?assertNot(flurm_reservation:times_overlap(200, 300, 300, 400)).

%%====================================================================
%% Nodes Overlap Tests
%%====================================================================

nodes_overlap_empty_first_test() ->
    ?assertNot(flurm_reservation:nodes_overlap([], [<<"node1">>])).

nodes_overlap_empty_second_test() ->
    ?assertNot(flurm_reservation:nodes_overlap([<<"node1">>], [])).

nodes_overlap_both_empty_test() ->
    ?assertNot(flurm_reservation:nodes_overlap([], [])).

nodes_overlap_no_common_test() ->
    ?assertNot(flurm_reservation:nodes_overlap([<<"node1">>, <<"node2">>],
                                                [<<"node3">>, <<"node4">>])).

nodes_overlap_one_common_test() ->
    ?assert(flurm_reservation:nodes_overlap([<<"node1">>, <<"node2">>],
                                            [<<"node2">>, <<"node3">>])).

nodes_overlap_all_common_test() ->
    ?assert(flurm_reservation:nodes_overlap([<<"node1">>, <<"node2">>],
                                            [<<"node1">>, <<"node2">>])).

%%====================================================================
%% Check Type Access Tests
%%====================================================================

check_type_access_maintenance_test() ->
    %% Maintenance reservations always deny access
    ?assertNot(flurm_reservation:check_type_access(maintenance, [], <<"user1">>,
                                                    <<"acc1">>, [], [])).

check_type_access_maint_ignore_jobs_test() ->
    %% Maint with ignore_jobs flag denies access
    ?assertNot(flurm_reservation:check_type_access(maint, [ignore_jobs], <<"user1">>,
                                                    <<"acc1">>, [<<"user1">>], [])).

check_type_access_maint_no_ignore_test() ->
    %% Maint without ignore_jobs allows if user is listed
    ?assert(flurm_reservation:check_type_access(maint, [], <<"user1">>,
                                                 <<"acc1">>, [<<"user1">>], [])).

check_type_access_flex_empty_lists_test() ->
    %% Flex with empty users/accounts allows anyone
    ?assert(flurm_reservation:check_type_access(flex, [], <<"user1">>,
                                                 <<"acc1">>, [], [])).

check_type_access_flex_user_listed_test() ->
    ?assert(flurm_reservation:check_type_access(flex, [], <<"user1">>,
                                                 <<"acc1">>, [<<"user1">>], [])).

check_type_access_flex_user_not_listed_test() ->
    ?assertNot(flurm_reservation:check_type_access(flex, [], <<"user1">>,
                                                    <<"acc1">>, [<<"user2">>], [])).

check_type_access_user_type_test() ->
    ?assert(flurm_reservation:check_type_access(user, [], <<"user1">>,
                                                 <<"acc1">>, [<<"user1">>], [])),
    ?assertNot(flurm_reservation:check_type_access(user, [], <<"user1">>,
                                                    <<"acc1">>, [<<"user2">>], [])).

%%====================================================================
%% Check User Access Tests
%%====================================================================

check_user_access_empty_lists_test() ->
    %% Empty users and accounts means anyone can access
    ?assert(flurm_reservation:check_user_access(<<"user1">>, <<"acc1">>, [], [])).

check_user_access_user_in_list_test() ->
    ?assert(flurm_reservation:check_user_access(<<"user1">>, <<"acc1">>,
                                                 [<<"user1">>, <<"user2">>], [])).

check_user_access_user_not_in_list_test() ->
    ?assertNot(flurm_reservation:check_user_access(<<"user1">>, <<"acc1">>,
                                                    [<<"user2">>], [<<"other_acc">>])).

check_user_access_account_in_list_test() ->
    ?assert(flurm_reservation:check_user_access(<<"user1">>, <<"acc1">>,
                                                 [], [<<"acc1">>])).

%%====================================================================
%% Find Gap Tests
%%====================================================================

find_gap_no_reservations_test() ->
    Now = erlang:system_time(second),
    {ok, Start} = flurm_reservation:find_gap(Now, [], 3600),
    ?assertEqual(Now, Start).

find_gap_gap_before_first_test() ->
    Now = erlang:system_time(second),
    %% Reservation record: {reservation, name, type, start_time, end_time, ...}
    Res = {reservation, <<"res1">>, user, Now + 10000, Now + 20000, 0, [], 0,
           undefined, [], [], [], [], #{}, inactive, <<"admin">>, Now, undefined, [], false},
    {ok, Start} = flurm_reservation:find_gap(Now, [Res], 3600),
    ?assertEqual(Now, Start).

find_gap_gap_after_reservation_test() ->
    Now = erlang:system_time(second),
    Res = {reservation, <<"res1">>, user, Now, Now + 1000, 0, [], 0,
           undefined, [], [], [], [], #{}, inactive, <<"admin">>, Now, undefined, [], false},
    {ok, Start} = flurm_reservation:find_gap(Now, [Res], 3600),
    ?assertEqual(Now + 1000, Start).

%%====================================================================
%% Generate Reservation Name Tests
%%====================================================================

generate_reservation_name_unique_test() ->
    Name1 = flurm_reservation:generate_reservation_name(),
    Name2 = flurm_reservation:generate_reservation_name(),
    ?assert(is_binary(Name1)),
    ?assert(is_binary(Name2)),
    ?assertNotEqual(Name1, Name2).

generate_reservation_name_prefix_test() ->
    Name = flurm_reservation:generate_reservation_name(),
    ?assertMatch(<<"res_", _/binary>>, Name).

%%====================================================================
%% Node In Partition Tests
%%====================================================================

node_in_partition_no_registry_test() ->
    %% When partition registry is not available, should return false
    ?assertNot(flurm_reservation:node_in_partition(<<"node1">>, <<"partition1">>)).

%%====================================================================
%% User In Accounts Tests
%%====================================================================

user_in_accounts_no_manager_test() ->
    %% When account manager is not available, should return false
    ?assertNot(flurm_reservation:user_in_accounts(<<"user1">>, [<<"acc1">>])).
