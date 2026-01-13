%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_license module
%%%-------------------------------------------------------------------
-module(flurm_license_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the license server
    case whereis(flurm_license) of
        undefined ->
            {ok, Pid} = flurm_license:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    catch ets:delete(flurm_licenses),
    catch ets:delete(flurm_license_allocations),
    gen_server:stop(flurm_license);
cleanup({existing, _Pid}) ->
    ok.

license_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"add and remove license", fun test_add_remove_license/0},
      {"list and get licenses", fun test_list_get_licenses/0},
      {"allocate and deallocate", fun test_allocate_deallocate/0},
      {"allocate insufficient licenses", fun test_allocate_insufficient/0},
      {"check availability", fun test_check_availability/0},
      {"get available count", fun test_get_available/0},
      {"reserve and unreserve", fun test_reserve_unreserve/0},
      {"reserve insufficient", fun test_reserve_insufficient/0},
      {"exists check", fun test_exists/0},
      {"validate licenses", fun test_validate_licenses/0},
      {"remove license in use", fun test_remove_license_in_use/0},
      {"remote license config", fun test_remote_license_config/0}
     ]}.

%%====================================================================
%% Test Cases - Add/Remove License
%%====================================================================

test_add_remove_license() ->
    %% Add a license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Verify it exists
    ?assertEqual(true, flurm_license:exists(<<"matlab">>)),

    %% Remove the license
    ok = flurm_license:remove_license(<<"matlab">>),

    %% Verify it's gone
    ?assertEqual(false, flurm_license:exists(<<"matlab">>)).

%%====================================================================
%% Test Cases - List/Get
%%====================================================================

test_list_get_licenses() ->
    %% Initially empty or has configured licenses
    InitialList = flurm_license:list(),
    InitialCount = length(InitialList),

    %% Add some licenses
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),
    ok = flurm_license:add_license(<<"ansys">>, #{total => 5}),

    %% List should have 2 more licenses
    AllLicenses = flurm_license:list(),
    ?assertEqual(InitialCount + 2, length(AllLicenses)),

    %% Get specific license
    {ok, Matlab} = flurm_license:get(<<"matlab">>),
    ?assertEqual(<<"matlab">>, maps:get(name, Matlab)),
    ?assertEqual(10, maps:get(total, Matlab)),
    ?assertEqual(0, maps:get(in_use, Matlab)),
    ?assertEqual(10, maps:get(available, Matlab)),

    %% Get non-existent license
    ?assertEqual({error, not_found}, flurm_license:get(<<"nonexistent">>)).

%%====================================================================
%% Test Cases - Allocate/Deallocate
%%====================================================================

test_allocate_deallocate() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Allocate for job 1
    ok = flurm_license:allocate(1, [{<<"matlab">>, 3}]),

    %% Check usage
    {ok, License1} = flurm_license:get(<<"matlab">>),
    ?assertEqual(3, maps:get(in_use, License1)),
    ?assertEqual(7, maps:get(available, License1)),

    %% Allocate more for job 2
    ok = flurm_license:allocate(2, [{<<"matlab">>, 2}]),

    %% Check updated usage
    {ok, License2} = flurm_license:get(<<"matlab">>),
    ?assertEqual(5, maps:get(in_use, License2)),
    ?assertEqual(5, maps:get(available, License2)),

    %% Deallocate job 1
    ok = flurm_license:deallocate(1, [{<<"matlab">>, 3}]),

    %% Check usage decreased
    {ok, License3} = flurm_license:get(<<"matlab">>),
    ?assertEqual(2, maps:get(in_use, License3)),
    ?assertEqual(8, maps:get(available, License3)).

test_allocate_insufficient() ->
    %% Add license with limited count
    ok = flurm_license:add_license(<<"matlab">>, #{total => 5}),

    %% Try to allocate more than available
    Result = flurm_license:allocate(1, [{<<"matlab">>, 10}]),
    ?assertEqual({error, insufficient_licenses}, Result),

    %% License should be unchanged
    {ok, License} = flurm_license:get(<<"matlab">>),
    ?assertEqual(0, maps:get(in_use, License)).

%%====================================================================
%% Test Cases - Availability Checks
%%====================================================================

test_check_availability() ->
    %% Add licenses
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),
    ok = flurm_license:add_license(<<"ansys">>, #{total => 5}),

    %% Check single license availability
    ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 5}])),
    ?assertEqual(true, flurm_license:check_availability([{<<"ansys">>, 3}])),

    %% Check multiple license availability
    ?assertEqual(true, flurm_license:check_availability([
        {<<"matlab">>, 5},
        {<<"ansys">>, 3}
    ])),

    %% Check exceeding availability
    ?assertEqual(false, flurm_license:check_availability([{<<"matlab">>, 15}])),
    ?assertEqual(false, flurm_license:check_availability([
        {<<"matlab">>, 5},
        {<<"ansys">>, 10}
    ])),

    %% Check non-existent license
    ?assertEqual(false, flurm_license:check_availability([{<<"nonexistent">>, 1}])).

test_get_available() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Full availability initially
    ?assertEqual(10, flurm_license:get_available(<<"matlab">>)),

    %% Allocate some
    ok = flurm_license:allocate(1, [{<<"matlab">>, 3}]),
    ?assertEqual(7, flurm_license:get_available(<<"matlab">>)),

    %% Non-existent license returns 0
    ?assertEqual(0, flurm_license:get_available(<<"nonexistent">>)).

%%====================================================================
%% Test Cases - Reserve/Unreserve
%%====================================================================

test_reserve_unreserve() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Reserve some
    ok = flurm_license:reserve(<<"matlab">>, 3),

    %% Available should decrease
    ?assertEqual(7, flurm_license:get_available(<<"matlab">>)),

    %% Check license details
    {ok, License1} = flurm_license:get(<<"matlab">>),
    ?assertEqual(3, maps:get(reserved, License1)),
    ?assertEqual(0, maps:get(in_use, License1)),

    %% Can still allocate from unreserved pool
    ok = flurm_license:allocate(1, [{<<"matlab">>, 5}]),
    ?assertEqual(2, flurm_license:get_available(<<"matlab">>)),

    %% Unreserve
    ok = flurm_license:unreserve(<<"matlab">>, 2),
    ?assertEqual(4, flurm_license:get_available(<<"matlab">>)),

    %% Check updated reserved count
    {ok, License2} = flurm_license:get(<<"matlab">>),
    ?assertEqual(1, maps:get(reserved, License2)).

test_reserve_insufficient() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 5}),

    %% Reserve more than available
    Result = flurm_license:reserve(<<"matlab">>, 10),
    ?assertEqual({error, insufficient_licenses}, Result),

    %% Reserve non-existent license
    Result2 = flurm_license:reserve(<<"nonexistent">>, 1),
    ?assertEqual({error, not_found}, Result2).

%%====================================================================
%% Test Cases - Exists and Validate
%%====================================================================

test_exists() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Check exists
    ?assertEqual(true, flurm_license:exists(<<"matlab">>)),
    ?assertEqual(false, flurm_license:exists(<<"nonexistent">>)).

test_validate_licenses() ->
    %% Add licenses
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),
    ok = flurm_license:add_license(<<"ansys">>, #{total => 5}),

    %% Validate existing licenses
    ?assertEqual(ok, flurm_license:validate_licenses([{<<"matlab">>, 1}])),
    ?assertEqual(ok, flurm_license:validate_licenses([
        {<<"matlab">>, 2},
        {<<"ansys">>, 3}
    ])),

    %% Validate empty list
    ?assertEqual(ok, flurm_license:validate_licenses([])),

    %% Validate with unknown license
    ?assertEqual({error, {unknown_license, <<"unknown">>}},
                 flurm_license:validate_licenses([{<<"unknown">>, 1}])),

    %% Validate mixed (first unknown fails)
    ?assertEqual({error, {unknown_license, <<"unknown">>}},
                 flurm_license:validate_licenses([
                     {<<"matlab">>, 1},
                     {<<"unknown">>, 1}
                 ])).

%%====================================================================
%% Test Cases - Error Cases
%%====================================================================

test_remove_license_in_use() ->
    %% Add license
    ok = flurm_license:add_license(<<"matlab">>, #{total => 10}),

    %% Allocate to a job
    ok = flurm_license:allocate(1, [{<<"matlab">>, 5}]),

    %% Try to remove - should fail
    Result = flurm_license:remove_license(<<"matlab">>),
    ?assertEqual({error, license_in_use}, Result),

    %% License should still exist
    ?assertEqual(true, flurm_license:exists(<<"matlab">>)),

    %% Deallocate and then remove
    ok = flurm_license:deallocate(1, [{<<"matlab">>, 5}]),
    ok = flurm_license:remove_license(<<"matlab">>),
    ?assertEqual(false, flurm_license:exists(<<"matlab">>)).

test_remote_license_config() ->
    %% Add remote license with server info
    ok = flurm_license:add_license(<<"remote_lic">>, #{
        total => 20,
        remote => true,
        server => <<"lic-server.example.com">>,
        port => 27000
    }),

    %% Get and verify
    {ok, License} = flurm_license:get(<<"remote_lic">>),
    ?assertEqual(true, maps:get(remote, License)),
    ?assertEqual(<<"lic-server.example.com">>, maps:get(server, License)),
    ?assertEqual(27000, maps:get(port, License)).

%%====================================================================
%% Parse License Spec Tests (pure functions, no server needed)
%%====================================================================

parse_license_spec_test_() ->
    [
     {"parse empty string", fun test_parse_empty/0},
     {"parse single license", fun test_parse_single/0},
     {"parse single with count", fun test_parse_single_with_count/0},
     {"parse multiple licenses", fun test_parse_multiple/0},
     {"parse with whitespace", fun test_parse_whitespace/0},
     {"parse invalid count", fun test_parse_invalid_count/0},
     {"parse empty name", fun test_parse_empty_name/0},
     {"parse zero count", fun test_parse_zero_count/0}
    ].

test_parse_empty() ->
    ?assertEqual({ok, []}, flurm_license:parse_license_spec(<<>>)),
    ?assertEqual({ok, []}, flurm_license:parse_license_spec("")).

test_parse_single() ->
    %% Single license without count defaults to 1
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab">>),
    ?assertEqual([{<<"matlab">>, 1}], Result).

test_parse_single_with_count() ->
    %% Single license with count
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab:5">>),
    ?assertEqual([{<<"matlab">>, 5}], Result).

test_parse_multiple() ->
    %% Multiple licenses
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab:1,ansys:2">>),
    ?assertEqual([{<<"matlab">>, 1}, {<<"ansys">>, 2}], Result),

    %% Mixed with and without counts
    {ok, Result2} = flurm_license:parse_license_spec(<<"matlab,ansys:3,comsol">>),
    ?assertEqual([{<<"matlab">>, 1}, {<<"ansys">>, 3}, {<<"comsol">>, 1}], Result2).

test_parse_whitespace() ->
    %% Handles whitespace
    {ok, Result} = flurm_license:parse_license_spec(<<"matlab : 5 , ansys : 2">>),
    ?assertEqual([{<<"matlab">>, 5}, {<<"ansys">>, 2}], Result).

test_parse_invalid_count() ->
    %% Invalid count (not a number)
    Result = flurm_license:parse_license_spec(<<"matlab:abc">>),
    ?assertMatch({error, {invalid_license_spec, _}}, Result).

test_parse_empty_name() ->
    %% Empty license name
    Result = flurm_license:parse_license_spec(<<":5">>),
    ?assertMatch({error, {invalid_license_spec, _}}, Result).

test_parse_zero_count() ->
    %% Zero count is invalid
    Result = flurm_license:parse_license_spec(<<"matlab:0">>),
    ?assertMatch({error, {invalid_license_spec, {invalid_count, 0}}}, Result).

%%====================================================================
%% Parse License Spec with String Input
%%====================================================================

parse_string_input_test_() ->
    [
     {"parse string input", fun test_parse_string_input/0}
    ].

test_parse_string_input() ->
    %% String input (not binary)
    {ok, Result} = flurm_license:parse_license_spec("matlab:2,ansys:1"),
    ?assertEqual([{<<"matlab">>, 2}, {<<"ansys">>, 1}], Result).
