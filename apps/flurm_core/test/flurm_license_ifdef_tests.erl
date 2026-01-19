%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_license internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_license_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

-define(LICENSE_TABLE, flurm_licenses).
-define(LICENSE_ALLOC_TABLE, flurm_license_allocations).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Create ETS tables if they don't exist
    case ets:whereis(?LICENSE_TABLE) of
        undefined ->
            ets:new(?LICENSE_TABLE, [
                named_table, public, set,
                {keypos, 2}  % license.name position
            ]);
        _ -> ok
    end,
    case ets:whereis(?LICENSE_ALLOC_TABLE) of
        undefined ->
            ets:new(?LICENSE_ALLOC_TABLE, [
                named_table, public, set,
                {keypos, 2}  % license_alloc.id position
            ]);
        _ -> ok
    end,
    ok.

cleanup(_) ->
    case ets:whereis(?LICENSE_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?LICENSE_TABLE)
    end,
    case ets:whereis(?LICENSE_ALLOC_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?LICENSE_ALLOC_TABLE)
    end,
    ok.

license_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun parse_single_license_name_only/0,
         fun parse_single_license_name_count/0,
         fun parse_single_license_empty_name/0,
         fun parse_single_license_invalid_count/0,
         fun do_add_license_basic/0,
         fun do_add_license_with_options/0,
         fun do_remove_license_success/0,
         fun do_remove_license_in_use/0,
         fun license_to_map_basic/0,
         fun do_reserve_success/0,
         fun do_reserve_insufficient/0,
         fun do_unreserve_basic/0
     ]}.

%%====================================================================
%% Parse Single License Tests
%%====================================================================

parse_single_license_name_only() ->
    Result = flurm_license:parse_single_license(<<"matlab">>),
    ?assertEqual({<<"matlab">>, 1}, Result).

parse_single_license_name_count() ->
    Result = flurm_license:parse_single_license(<<"matlab:5">>),
    ?assertEqual({<<"matlab">>, 5}, Result).

parse_single_license_empty_name() ->
    ?assertThrow({invalid_license, empty_name},
                 flurm_license:parse_single_license(<<>>)).

parse_single_license_invalid_count() ->
    ?assertThrow({invalid_license, {invalid_count, _}},
                 flurm_license:parse_single_license(<<"matlab:abc">>)).

parse_single_license_zero_count_test() ->
    ?assertThrow({invalid_license, {invalid_count, 0}},
                 flurm_license:parse_single_license(<<"matlab:0">>)).

parse_single_license_negative_count_test() ->
    ?assertThrow({invalid_license, {invalid_count, -1}},
                 flurm_license:parse_single_license(<<"matlab:-1">>)).

parse_single_license_with_spaces_test() ->
    Result = flurm_license:parse_single_license(<<"  matlab : 5  ">>),
    ?assertEqual({<<"matlab">>, 5}, Result).

%%====================================================================
%% Parse License Spec Tests (via public API)
%%====================================================================

parse_license_spec_empty_test() ->
    ?assertEqual({ok, []}, flurm_license:parse_license_spec(<<>>)),
    ?assertEqual({ok, []}, flurm_license:parse_license_spec("")).

parse_license_spec_single_test() ->
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab:2">>),
    ?assertEqual([{<<"matlab">>, 2}], Result).

parse_license_spec_multiple_test() ->
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab:2,ansys:1,gaussian">>),
    ?assertEqual([{<<"matlab">>, 2}, {<<"ansys">>, 1}, {<<"gaussian">>, 1}], Result).

parse_license_spec_string_input_test() ->
    {ok, Result} = flurm_license:parse_license_spec("matlab:3"),
    ?assertEqual([{<<"matlab">>, 3}], Result).

parse_license_spec_invalid_test() ->
    {error, _} = flurm_license:parse_license_spec(<<":invalid">>).

%%====================================================================
%% Do Add License Tests
%%====================================================================

do_add_license_basic() ->
    Config = #{total => 10},
    ok = flurm_license:do_add_license(<<"basic_lic">>, Config),
    ?assertMatch([_], ets:lookup(?LICENSE_TABLE, <<"basic_lic">>)).

do_add_license_with_options() ->
    Config = #{
        total => 100,
        remote => true,
        server => <<"license.example.com">>,
        port => 27000
    },
    ok = flurm_license:do_add_license(<<"remote_lic">>, Config),
    [{license, Name, Total, InUse, Reserved, Remote, Server, Port}] =
        ets:lookup(?LICENSE_TABLE, <<"remote_lic">>),
    ?assertEqual(<<"remote_lic">>, Name),
    ?assertEqual(100, Total),
    ?assertEqual(0, InUse),
    ?assertEqual(0, Reserved),
    ?assertEqual(true, Remote),
    ?assertEqual(<<"license.example.com">>, Server),
    ?assertEqual(27000, Port).

do_add_license_defaults_test() ->
    Config = #{},
    ok = flurm_license:do_add_license(<<"default_lic">>, Config),
    [{license, _, Total, InUse, Reserved, Remote, Server, Port}] =
        ets:lookup(?LICENSE_TABLE, <<"default_lic">>),
    ?assertEqual(0, Total),
    ?assertEqual(0, InUse),
    ?assertEqual(0, Reserved),
    ?assertEqual(false, Remote),
    ?assertEqual(undefined, Server),
    ?assertEqual(undefined, Port).

%%====================================================================
%% Do Remove License Tests
%%====================================================================

do_remove_license_success() ->
    flurm_license:do_add_license(<<"remove_me">>, #{total => 5}),
    ok = flurm_license:do_remove_license(<<"remove_me">>),
    ?assertEqual([], ets:lookup(?LICENSE_TABLE, <<"remove_me">>)).

do_remove_license_in_use() ->
    flurm_license:do_add_license(<<"in_use_lic">>, #{total => 5}),
    %% Create an allocation
    Alloc = {license_alloc, {1, <<"in_use_lic">>}, 1, <<"in_use_lic">>, 2, erlang:system_time(second)},
    ets:insert(?LICENSE_ALLOC_TABLE, Alloc),
    Result = flurm_license:do_remove_license(<<"in_use_lic">>),
    ?assertEqual({error, license_in_use}, Result).

do_remove_license_not_found_test() ->
    %% Removing non-existent license should succeed (no-op)
    ok = flurm_license:do_remove_license(<<"nonexistent">>).

%%====================================================================
%% License To Map Tests
%%====================================================================

license_to_map_basic() ->
    License = {license, <<"test">>, 10, 3, 2, false, undefined, undefined},
    Map = flurm_license:license_to_map(License),
    ?assertEqual(<<"test">>, maps:get(name, Map)),
    ?assertEqual(10, maps:get(total, Map)),
    ?assertEqual(3, maps:get(in_use, Map)),
    ?assertEqual(2, maps:get(reserved, Map)),
    ?assertEqual(5, maps:get(available, Map)),  % 10 - 3 - 2
    ?assertEqual(false, maps:get(remote, Map)).

license_to_map_remote_test() ->
    License = {license, <<"remote">>, 50, 10, 5, true, <<"server.com">>, 27000},
    Map = flurm_license:license_to_map(License),
    ?assertEqual(true, maps:get(remote, Map)),
    ?assertEqual(<<"server.com">>, maps:get(server, Map)),
    ?assertEqual(27000, maps:get(port, Map)),
    ?assertEqual(35, maps:get(available, Map)).  % 50 - 10 - 5

license_to_map_negative_available_test() ->
    %% When in_use + reserved > total, available should be 0
    License = {license, <<"overallocated">>, 10, 8, 5, false, undefined, undefined},
    Map = flurm_license:license_to_map(License),
    ?assertEqual(0, maps:get(available, Map)).  % max(0, 10 - 8 - 5)

%%====================================================================
%% Do Reserve Tests
%%====================================================================

do_reserve_success() ->
    License = {license, <<"reservable">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    ok = flurm_license:do_reserve(<<"reservable">>, 5),
    [{license, _, _, _, Reserved, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"reservable">>),
    ?assertEqual(5, Reserved).

do_reserve_insufficient() ->
    License = {license, <<"limited">>, 10, 5, 3, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    %% Only 2 available (10 - 5 - 3), trying to reserve 5
    Result = flurm_license:do_reserve(<<"limited">>, 5),
    ?assertEqual({error, insufficient_licenses}, Result).

do_reserve_not_found_test() ->
    Result = flurm_license:do_reserve(<<"nonexistent">>, 1),
    ?assertEqual({error, not_found}, Result).

do_reserve_exact_available_test() ->
    License = {license, <<"exact">>, 10, 5, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    %% 5 available, reserving exactly 5
    ok = flurm_license:do_reserve(<<"exact">>, 5),
    [{license, _, _, _, Reserved, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"exact">>),
    ?assertEqual(5, Reserved).

%%====================================================================
%% Do Unreserve Tests
%%====================================================================

do_unreserve_basic() ->
    License = {license, <<"unreservable">>, 10, 0, 5, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    flurm_license:do_unreserve(<<"unreservable">>, 3),
    [{license, _, _, _, Reserved, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"unreservable">>),
    ?assertEqual(2, Reserved).

do_unreserve_all_test() ->
    License = {license, <<"unreserve_all">>, 10, 0, 5, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    flurm_license:do_unreserve(<<"unreserve_all">>, 5),
    [{license, _, _, _, Reserved, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"unreserve_all">>),
    ?assertEqual(0, Reserved).

do_unreserve_more_than_reserved_test() ->
    License = {license, <<"over_unreserve">>, 10, 0, 3, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    %% Unreserving more than reserved should clamp to 0
    flurm_license:do_unreserve(<<"over_unreserve">>, 10),
    [{license, _, _, _, Reserved, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"over_unreserve">>),
    ?assertEqual(0, Reserved).

do_unreserve_not_found_test() ->
    %% Unreserving from non-existent license should be no-op
    flurm_license:do_unreserve(<<"nonexistent">>, 5).

%%====================================================================
%% Do Allocate Tests
%%====================================================================

do_allocate_success_test() ->
    License = {license, <<"allocatable">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    Requests = [{<<"allocatable">>, 3}],
    ok = flurm_license:do_allocate(1, Requests),
    [{license, _, _, InUse, _, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"allocatable">>),
    ?assertEqual(3, InUse).

do_allocate_insufficient_test() ->
    License = {license, <<"limited_alloc">>, 5, 3, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    Requests = [{<<"limited_alloc">>, 5}],
    Result = flurm_license:do_allocate(1, Requests),
    ?assertEqual({error, insufficient_licenses}, Result).

do_allocate_multiple_licenses_test() ->
    License1 = {license, <<"lic1">>, 10, 0, 0, false, undefined, undefined},
    License2 = {license, <<"lic2">>, 20, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License1),
    ets:insert(?LICENSE_TABLE, License2),
    Requests = [{<<"lic1">>, 5}, {<<"lic2">>, 10}],
    ok = flurm_license:do_allocate(1, Requests),
    [{license, _, _, InUse1, _, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"lic1">>),
    [{license, _, _, InUse2, _, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"lic2">>),
    ?assertEqual(5, InUse1),
    ?assertEqual(10, InUse2).

%%====================================================================
%% Do Deallocate Tests
%%====================================================================

do_deallocate_success_test() ->
    License = {license, <<"deallocatable">>, 10, 5, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    Alloc = {license_alloc, {1, <<"deallocatable">>}, 1, <<"deallocatable">>, 3, erlang:system_time(second)},
    ets:insert(?LICENSE_ALLOC_TABLE, Alloc),
    flurm_license:do_deallocate(1, [{<<"deallocatable">>, 3}]),
    [{license, _, _, InUse, _, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"deallocatable">>),
    ?assertEqual(2, InUse).

do_deallocate_clamp_zero_test() ->
    License = {license, <<"underflow">>, 10, 2, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    %% Deallocating more than in_use should clamp to 0
    flurm_license:do_deallocate(1, [{<<"underflow">>, 10}]),
    [{license, _, _, InUse, _, _, _, _}] = ets:lookup(?LICENSE_TABLE, <<"underflow">>),
    ?assertEqual(0, InUse).

%%====================================================================
%% Exists Tests
%%====================================================================

exists_true_test() ->
    License = {license, <<"exists_test">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    ?assert(flurm_license:exists(<<"exists_test">>)).

exists_false_test() ->
    ?assertNot(flurm_license:exists(<<"does_not_exist">>)).

%%====================================================================
%% Validate Licenses Tests
%%====================================================================

validate_licenses_empty_test() ->
    ?assertEqual(ok, flurm_license:validate_licenses([])).

validate_licenses_all_exist_test() ->
    License1 = {license, <<"valid1">>, 10, 0, 0, false, undefined, undefined},
    License2 = {license, <<"valid2">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License1),
    ets:insert(?LICENSE_TABLE, License2),
    ?assertEqual(ok, flurm_license:validate_licenses([{<<"valid1">>, 1}, {<<"valid2">>, 2}])).

validate_licenses_unknown_test() ->
    License = {license, <<"known">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    Result = flurm_license:validate_licenses([{<<"known">>, 1}, {<<"unknown">>, 1}]),
    ?assertEqual({error, {unknown_license, <<"unknown">>}}, Result).

%%====================================================================
%% Get Available Tests
%%====================================================================

get_available_test() ->
    License = {license, <<"avail_test">>, 10, 3, 2, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    ?assertEqual(5, flurm_license:get_available(<<"avail_test">>)).

get_available_not_found_test() ->
    ?assertEqual(0, flurm_license:get_available(<<"nonexistent">>)).

%%====================================================================
%% Check Availability Tests
%%====================================================================

check_availability_true_test() ->
    License = {license, <<"check_avail">>, 10, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    ?assert(flurm_license:check_availability([{<<"check_avail">>, 5}])).

check_availability_false_test() ->
    License = {license, <<"check_unavail">>, 10, 8, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License),
    ?assertNot(flurm_license:check_availability([{<<"check_unavail">>, 5}])).

check_availability_multiple_test() ->
    License1 = {license, <<"multi1">>, 10, 0, 0, false, undefined, undefined},
    License2 = {license, <<"multi2">>, 5, 0, 0, false, undefined, undefined},
    ets:insert(?LICENSE_TABLE, License1),
    ets:insert(?LICENSE_TABLE, License2),
    ?assert(flurm_license:check_availability([{<<"multi1">>, 5}, {<<"multi2">>, 3}])),
    ?assertNot(flurm_license:check_availability([{<<"multi1">>, 5}, {<<"multi2">>, 10}])).
