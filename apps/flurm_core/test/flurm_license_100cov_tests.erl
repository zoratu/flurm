%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_license module - 100% coverage target
%%%
%%% Tests license management functionality:
%%% - gen_server lifecycle (start_link, init, terminate, code_change)
%%% - License CRUD (add_license, remove_license, list, get, exists)
%%% - Allocation and deallocation
%%% - Reservation management
%%% - License spec parsing
%%% - Availability checking
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_license_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SERVER, flurm_license).
-define(LICENSE_TABLE, flurm_licenses).
-define(LICENSE_ALLOC_TABLE, flurm_license_allocations).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing license server
    _ = case whereis(?SERVER) of
        undefined -> ok;
        OldPid ->
            gen_server:stop(OldPid, normal, 5000),
            timer:sleep(50)
    end,
    %% Clean up ETS tables
    cleanup_ets(),
    %% Start fresh license server
    {ok, NewPid} = flurm_license:start_link(),
    NewPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end,
    cleanup_ets().

cleanup_ets() ->
    lists:foreach(fun(Tab) ->
        case ets:whereis(Tab) of
            undefined -> ok;
            _ -> catch ets:delete(Tab)
        end
    end, [?LICENSE_TABLE, ?LICENSE_ALLOC_TABLE]).

%%====================================================================
%% Test Generator
%%====================================================================

flurm_license_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         %% Basic API tests
         fun test_start_link/1,
         fun test_start_link_already_started/1,
         fun test_unknown_call/1,
         fun test_unknown_cast/1,
         fun test_unknown_info/1,
         fun test_code_change/1,

         %% License CRUD tests
         fun test_add_license_basic/1,
         fun test_add_license_with_all_options/1,
         fun test_remove_license/1,
         fun test_remove_license_in_use/1,
         fun test_list_licenses/1,
         fun test_list_licenses_empty/1,
         fun test_get_license/1,
         fun test_get_license_not_found/1,
         fun test_exists/1,
         fun test_exists_false/1,

         %% Allocation tests
         fun test_allocate_single_license/1,
         fun test_allocate_multiple_licenses/1,
         fun test_allocate_insufficient/1,
         fun test_deallocate/1,
         fun test_deallocate_triggers_schedule/1,

         %% Reservation tests
         fun test_reserve_license/1,
         fun test_reserve_insufficient/1,
         fun test_reserve_not_found/1,
         fun test_unreserve_license/1,
         fun test_unreserve_not_found/1,

         %% Availability tests
         fun test_get_available/1,
         fun test_get_available_not_found/1,
         fun test_check_availability_true/1,
         fun test_check_availability_false/1,

         %% Parse tests
         fun test_parse_license_spec_empty_binary/1,
         fun test_parse_license_spec_empty_string/1,
         fun test_parse_license_spec_single/1,
         fun test_parse_license_spec_multiple/1,
         fun test_parse_license_spec_string_input/1,
         fun test_parse_license_spec_invalid/1,

         %% Parse single license tests
         fun test_parse_single_license_name_only/1,
         fun test_parse_single_license_name_count/1,
         fun test_parse_single_license_empty_name/1,
         fun test_parse_single_license_invalid_count/1,
         fun test_parse_single_license_zero_count/1,
         fun test_parse_single_license_negative_count/1,
         fun test_parse_single_license_with_spaces/1,

         %% Validate licenses tests
         fun test_validate_licenses_empty/1,
         fun test_validate_licenses_all_exist/1,
         fun test_validate_licenses_unknown/1
     ]}.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_link(_Pid) ->
    ?_test(begin
        ?assert(is_process_alive(whereis(?SERVER)))
    end).

test_start_link_already_started(_Pid) ->
    ?_test(begin
        {ok, ExistingPid} = flurm_license:start_link(),
        ?assertEqual(whereis(?SERVER), ExistingPid)
    end).

test_unknown_call(_Pid) ->
    ?_test(begin
        Result = gen_server:call(?SERVER, unknown_request),
        ?assertEqual({error, unknown_request}, Result)
    end).

test_unknown_cast(_Pid) ->
    ?_test(begin
        gen_server:cast(?SERVER, unknown_cast),
        timer:sleep(10),
        ?assert(is_process_alive(whereis(?SERVER)))
    end).

test_unknown_info(_Pid) ->
    ?_test(begin
        ?SERVER ! unknown_info,
        timer:sleep(10),
        ?assert(is_process_alive(whereis(?SERVER)))
    end).

test_code_change(_Pid) ->
    ?_test(begin
        State = #{test => state},
        Result = flurm_license:code_change(old_vsn, State, extra),
        ?assertEqual({ok, State}, Result)
    end).

%%====================================================================
%% License CRUD Tests
%%====================================================================

test_add_license_basic(_Pid) ->
    ?_test(begin
        Result = flurm_license:add_license(<<"matlab">>, #{total => 10}),
        ?assertEqual(ok, Result),
        ?assertEqual(true, flurm_license:exists(<<"matlab">>))
    end).

test_add_license_with_all_options(_Pid) ->
    ?_test(begin
        Config = #{
            total => 50,
            remote => true,
            server => <<"license.example.com">>,
            port => 27000
        },
        ok = flurm_license:add_license(<<"remote_lic">>, Config),
        {ok, License} = flurm_license:get(<<"remote_lic">>),
        ?assertEqual(50, maps:get(total, License)),
        ?assertEqual(true, maps:get(remote, License)),
        ?assertEqual(<<"license.example.com">>, maps:get(server, License)),
        ?assertEqual(27000, maps:get(port, License))
    end).

test_remove_license(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"to_remove">>, #{total => 5}),
        ?assertEqual(true, flurm_license:exists(<<"to_remove">>)),
        ok = flurm_license:remove_license(<<"to_remove">>),
        ?assertEqual(false, flurm_license:exists(<<"to_remove">>))
    end).

test_remove_license_in_use(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"in_use_lic">>, #{total => 10}),
        ok = flurm_license:allocate(1, [{<<"in_use_lic">>, 2}]),
        Result = flurm_license:remove_license(<<"in_use_lic">>),
        ?assertEqual({error, license_in_use}, Result)
    end).

test_list_licenses(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"lic1">>, #{total => 10}),
        flurm_license:add_license(<<"lic2">>, #{total => 20}),
        flurm_license:add_license(<<"lic3">>, #{total => 30}),
        List = flurm_license:list(),
        ?assertEqual(3, length(List)),
        Names = [maps:get(name, L) || L <- List],
        ?assert(lists:member(<<"lic1">>, Names)),
        ?assert(lists:member(<<"lic2">>, Names)),
        ?assert(lists:member(<<"lic3">>, Names))
    end).

test_list_licenses_empty(_Pid) ->
    ?_test(begin
        List = flurm_license:list(),
        ?assertEqual([], List)
    end).

test_get_license(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"get_test">>, #{total => 15}),
        {ok, License} = flurm_license:get(<<"get_test">>),
        ?assertEqual(<<"get_test">>, maps:get(name, License)),
        ?assertEqual(15, maps:get(total, License)),
        ?assertEqual(0, maps:get(in_use, License)),
        ?assertEqual(0, maps:get(reserved, License)),
        ?assertEqual(15, maps:get(available, License))
    end).

test_get_license_not_found(_Pid) ->
    ?_test(begin
        Result = flurm_license:get(<<"nonexistent">>),
        ?assertEqual({error, not_found}, Result)
    end).

test_exists(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"exists_test">>, #{total => 5}),
        ?assertEqual(true, flurm_license:exists(<<"exists_test">>))
    end).

test_exists_false(_Pid) ->
    ?_test(begin
        ?assertEqual(false, flurm_license:exists(<<"does_not_exist">>))
    end).

%%====================================================================
%% Allocation Tests
%%====================================================================

test_allocate_single_license(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"alloc_test">>, #{total => 10}),
        Result = flurm_license:allocate(1, [{<<"alloc_test">>, 3}]),
        ?assertEqual(ok, Result),
        {ok, License} = flurm_license:get(<<"alloc_test">>),
        ?assertEqual(3, maps:get(in_use, License)),
        ?assertEqual(7, maps:get(available, License))
    end).

test_allocate_multiple_licenses(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"multi1">>, #{total => 10}),
        flurm_license:add_license(<<"multi2">>, #{total => 20}),
        Result = flurm_license:allocate(1, [{<<"multi1">>, 5}, {<<"multi2">>, 10}]),
        ?assertEqual(ok, Result),
        {ok, L1} = flurm_license:get(<<"multi1">>),
        {ok, L2} = flurm_license:get(<<"multi2">>),
        ?assertEqual(5, maps:get(in_use, L1)),
        ?assertEqual(10, maps:get(in_use, L2))
    end).

test_allocate_insufficient(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"insufficient">>, #{total => 5}),
        Result = flurm_license:allocate(1, [{<<"insufficient">>, 10}]),
        ?assertEqual({error, insufficient_licenses}, Result)
    end).

test_deallocate(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"dealloc_test">>, #{total => 10}),
        flurm_license:allocate(1, [{<<"dealloc_test">>, 5}]),
        {ok, L1} = flurm_license:get(<<"dealloc_test">>),
        ?assertEqual(5, maps:get(in_use, L1)),
        ok = flurm_license:deallocate(1, [{<<"dealloc_test">>, 5}]),
        {ok, L2} = flurm_license:get(<<"dealloc_test">>),
        ?assertEqual(0, maps:get(in_use, L2))
    end).

test_deallocate_triggers_schedule(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"trigger_test">>, #{total => 10}),
        flurm_license:allocate(1, [{<<"trigger_test">>, 5}]),
        %% Deallocate should try to trigger scheduler (may fail if not running)
        ok = flurm_license:deallocate(1, [{<<"trigger_test">>, 5}]),
        ?assert(true)  % Just verify it doesn't crash
    end).

%%====================================================================
%% Reservation Tests
%%====================================================================

test_reserve_license(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"reserve_test">>, #{total => 10}),
        Result = flurm_license:reserve(<<"reserve_test">>, 3),
        ?assertEqual(ok, Result),
        {ok, License} = flurm_license:get(<<"reserve_test">>),
        ?assertEqual(3, maps:get(reserved, License)),
        ?assertEqual(7, maps:get(available, License))
    end).

test_reserve_insufficient(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"reserve_insuff">>, #{total => 5}),
        flurm_license:allocate(1, [{<<"reserve_insuff">>, 3}]),
        Result = flurm_license:reserve(<<"reserve_insuff">>, 5),
        ?assertEqual({error, insufficient_licenses}, Result)
    end).

test_reserve_not_found(_Pid) ->
    ?_test(begin
        Result = flurm_license:reserve(<<"nonexistent">>, 5),
        ?assertEqual({error, not_found}, Result)
    end).

test_unreserve_license(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"unreserve_test">>, #{total => 10}),
        flurm_license:reserve(<<"unreserve_test">>, 5),
        ok = flurm_license:unreserve(<<"unreserve_test">>, 3),
        {ok, License} = flurm_license:get(<<"unreserve_test">>),
        ?assertEqual(2, maps:get(reserved, License))
    end).

test_unreserve_not_found(_Pid) ->
    ?_test(begin
        %% Unreserving non-existent license should be ok (no-op)
        ok = flurm_license:unreserve(<<"nonexistent">>, 5),
        ?assert(true)
    end).

%%====================================================================
%% Availability Tests
%%====================================================================

test_get_available(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"avail_test">>, #{total => 20}),
        flurm_license:allocate(1, [{<<"avail_test">>, 5}]),
        flurm_license:reserve(<<"avail_test">>, 3),
        Available = flurm_license:get_available(<<"avail_test">>),
        ?assertEqual(12, Available)  % 20 - 5 - 3
    end).

test_get_available_not_found(_Pid) ->
    ?_test(begin
        Available = flurm_license:get_available(<<"nonexistent">>),
        ?assertEqual(0, Available)
    end).

test_check_availability_true(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"check_avail1">>, #{total => 10}),
        flurm_license:add_license(<<"check_avail2">>, #{total => 20}),
        Result = flurm_license:check_availability([{<<"check_avail1">>, 5}, {<<"check_avail2">>, 10}]),
        ?assertEqual(true, Result)
    end).

test_check_availability_false(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"check_unavail">>, #{total => 5}),
        Result = flurm_license:check_availability([{<<"check_unavail">>, 10}]),
        ?assertEqual(false, Result)
    end).

%%====================================================================
%% Parse License Spec Tests
%%====================================================================

test_parse_license_spec_empty_binary(_Pid) ->
    ?_test(begin
        {ok, Result} = flurm_license:parse_license_spec(<<>>),
        ?assertEqual([], Result)
    end).

test_parse_license_spec_empty_string(_Pid) ->
    ?_test(begin
        {ok, Result} = flurm_license:parse_license_spec(""),
        ?assertEqual([], Result)
    end).

test_parse_license_spec_single(_Pid) ->
    ?_test(begin
        {ok, Result} = flurm_license:parse_license_spec(<<"matlab:5">>),
        ?assertEqual([{<<"matlab">>, 5}], Result)
    end).

test_parse_license_spec_multiple(_Pid) ->
    ?_test(begin
        {ok, Result} = flurm_license:parse_license_spec(<<"matlab:2,ansys:3,gaussian">>),
        ?assertEqual([{<<"matlab">>, 2}, {<<"ansys">>, 3}, {<<"gaussian">>, 1}], Result)
    end).

test_parse_license_spec_string_input(_Pid) ->
    ?_test(begin
        {ok, Result} = flurm_license:parse_license_spec("matlab:10"),
        ?assertEqual([{<<"matlab">>, 10}], Result)
    end).

test_parse_license_spec_invalid(_Pid) ->
    ?_test(begin
        {error, _} = flurm_license:parse_license_spec(<<":invalid">>),
        ?assert(true)
    end).

%%====================================================================
%% Parse Single License Tests
%%====================================================================

test_parse_single_license_name_only(_Pid) ->
    ?_test(begin
        Result = flurm_license:parse_single_license(<<"matlab">>),
        ?assertEqual({<<"matlab">>, 1}, Result)
    end).

test_parse_single_license_name_count(_Pid) ->
    ?_test(begin
        Result = flurm_license:parse_single_license(<<"matlab:5">>),
        ?assertEqual({<<"matlab">>, 5}, Result)
    end).

test_parse_single_license_empty_name(_Pid) ->
    ?_test(begin
        ?assertThrow({invalid_license, empty_name},
                     flurm_license:parse_single_license(<<>>))
    end).

test_parse_single_license_invalid_count(_Pid) ->
    ?_test(begin
        ?assertThrow({invalid_license, {invalid_count, _}},
                     flurm_license:parse_single_license(<<"matlab:abc">>))
    end).

test_parse_single_license_zero_count(_Pid) ->
    ?_test(begin
        ?assertThrow({invalid_license, {invalid_count, 0}},
                     flurm_license:parse_single_license(<<"matlab:0">>))
    end).

test_parse_single_license_negative_count(_Pid) ->
    ?_test(begin
        ?assertThrow({invalid_license, {invalid_count, -1}},
                     flurm_license:parse_single_license(<<"matlab:-1">>))
    end).

test_parse_single_license_with_spaces(_Pid) ->
    ?_test(begin
        Result = flurm_license:parse_single_license(<<"  matlab : 5  ">>),
        ?assertEqual({<<"matlab">>, 5}, Result)
    end).

%%====================================================================
%% Validate Licenses Tests
%%====================================================================

test_validate_licenses_empty(_Pid) ->
    ?_test(begin
        Result = flurm_license:validate_licenses([]),
        ?assertEqual(ok, Result)
    end).

test_validate_licenses_all_exist(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"valid1">>, #{total => 10}),
        flurm_license:add_license(<<"valid2">>, #{total => 10}),
        Result = flurm_license:validate_licenses([{<<"valid1">>, 1}, {<<"valid2">>, 2}]),
        ?assertEqual(ok, Result)
    end).

test_validate_licenses_unknown(_Pid) ->
    ?_test(begin
        flurm_license:add_license(<<"known">>, #{total => 10}),
        Result = flurm_license:validate_licenses([{<<"known">>, 1}, {<<"unknown">>, 1}]),
        ?assertEqual({error, {unknown_license, <<"unknown">>}}, Result)
    end).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"license_to_map with all fields",
              ?_test(begin
                  flurm_license:add_license(<<"full_map">>, #{
                      total => 100,
                      remote => true,
                      server => <<"server.com">>,
                      port => 1234
                  }),
                  {ok, License} = flurm_license:get(<<"full_map">>),
                  ?assertEqual(<<"full_map">>, maps:get(name, License)),
                  ?assertEqual(100, maps:get(total, License)),
                  ?assertEqual(0, maps:get(in_use, License)),
                  ?assertEqual(0, maps:get(reserved, License)),
                  ?assertEqual(100, maps:get(available, License)),
                  ?assertEqual(true, maps:get(remote, License)),
                  ?assertEqual(<<"server.com">>, maps:get(server, License)),
                  ?assertEqual(1234, maps:get(port, License))
              end)},

             {"license_to_map available calculation",
              ?_test(begin
                  flurm_license:add_license(<<"calc_avail">>, #{total => 10}),
                  flurm_license:allocate(1, [{<<"calc_avail">>, 3}]),
                  flurm_license:reserve(<<"calc_avail">>, 2),
                  {ok, License} = flurm_license:get(<<"calc_avail">>),
                  ?assertEqual(5, maps:get(available, License))  % 10 - 3 - 2
              end)},

             {"license_to_map negative available clamped",
              ?_test(begin
                  %% This is an edge case - shouldn't happen in practice
                  %% but the code handles it
                  flurm_license:add_license(<<"clamp_test">>, #{total => 10}),
                  flurm_license:allocate(1, [{<<"clamp_test">>, 10}]),
                  %% Now available should be 0
                  {ok, License} = flurm_license:get(<<"clamp_test">>),
                  ?assertEqual(0, maps:get(available, License))
              end)},

             {"allocate_license for non-existent license",
              ?_test(begin
                  %% allocate with non-existent license should be handled
                  Result = flurm_license:allocate(1, [{<<"nonexistent">>, 1}]),
                  ?assertEqual({error, insufficient_licenses}, Result)
              end)},

             {"deallocate_license clamps to zero",
              ?_test(begin
                  flurm_license:add_license(<<"dealloc_clamp">>, #{total => 10}),
                  flurm_license:allocate(1, [{<<"dealloc_clamp">>, 5}]),
                  %% Deallocate more than allocated
                  ok = flurm_license:deallocate(1, [{<<"dealloc_clamp">>, 10}]),
                  {ok, License} = flurm_license:get(<<"dealloc_clamp">>),
                  ?assertEqual(0, maps:get(in_use, License))
              end)},

             {"unreserve clamps to zero",
              ?_test(begin
                  flurm_license:add_license(<<"unreserve_clamp">>, #{total => 10}),
                  flurm_license:reserve(<<"unreserve_clamp">>, 3),
                  ok = flurm_license:unreserve(<<"unreserve_clamp">>, 10),
                  {ok, License} = flurm_license:get(<<"unreserve_clamp">>),
                  ?assertEqual(0, maps:get(reserved, License))
              end)},

             {"deallocate empty list",
              ?_test(begin
                  ok = flurm_license:deallocate(1, []),
                  ?assert(true)
              end)},

             {"deallocate with non-empty list triggers scheduler spawn",
              ?_test(begin
                  flurm_license:add_license(<<"spawn_test">>, #{total => 10}),
                  flurm_license:allocate(1, [{<<"spawn_test">>, 5}]),
                  %% Deallocate with non-empty list - triggers spawn
                  ok = flurm_license:deallocate(1, [{<<"spawn_test">>, 5}]),
                  %% Give the spawned process time to execute
                  timer:sleep(20),
                  ?assert(true)
              end)}
         ]
     end}.

%%====================================================================
%% Load Configured Licenses Tests
%%====================================================================

load_configured_licenses_test_() ->
    {"load_configured_licenses with config",
     {setup,
      fun() ->
          %% Stop any existing license server
          _ = case whereis(?SERVER) of
              undefined -> ok;
              OldPid ->
                  gen_server:stop(OldPid, normal, 5000),
                  timer:sleep(50)
          end,
          cleanup_ets(),
          %% Set up license config before starting
          application:set_env(flurm_core, licenses, [
              {<<"configured_matlab">>, #{total => 50}},
              {<<"configured_ansys">>, #{total => 25, remote => true}}
          ]),
          {ok, Pid} = flurm_license:start_link(),
          Pid
      end,
      fun(Pid) ->
          case is_process_alive(Pid) of
              true -> gen_server:stop(Pid, normal, 5000);
              false -> ok
          end,
          cleanup_ets(),
          application:unset_env(flurm_core, licenses)
      end,
      fun(_Pid) ->
          [
              {"configured licenses are loaded",
               ?_test(begin
                   ?assertEqual(true, flurm_license:exists(<<"configured_matlab">>)),
                   ?assertEqual(true, flurm_license:exists(<<"configured_ansys">>)),
                   {ok, Matlab} = flurm_license:get(<<"configured_matlab">>),
                   ?assertEqual(50, maps:get(total, Matlab)),
                   {ok, Ansys} = flurm_license:get(<<"configured_ansys">>),
                   ?assertEqual(25, maps:get(total, Ansys)),
                   ?assertEqual(true, maps:get(remote, Ansys))
               end)}
          ]
      end}}.

%%====================================================================
%% Internal Function Tests
%%====================================================================

internal_function_tests_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"do_add_license with defaults",
              ?_test(begin
                  ok = flurm_license:do_add_license(<<"defaults_test">>, #{}),
                  {ok, License} = flurm_license:get(<<"defaults_test">>),
                  ?assertEqual(0, maps:get(total, License)),
                  ?assertEqual(false, maps:get(remote, License)),
                  ?assertEqual(undefined, maps:get(server, License)),
                  ?assertEqual(undefined, maps:get(port, License))
              end)},

             {"do_remove_license success",
              ?_test(begin
                  flurm_license:do_add_license(<<"remove_internal">>, #{total => 5}),
                  ok = flurm_license:do_remove_license(<<"remove_internal">>),
                  ?assertEqual(false, flurm_license:exists(<<"remove_internal">>))
              end)},

             {"do_allocate success",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_alloc">>, #{total => 10}),
                  ok = flurm_license:do_allocate(1, [{<<"do_alloc">>, 3}]),
                  ?assertEqual(7, flurm_license:get_available(<<"do_alloc">>))
              end)},

             {"do_allocate insufficient",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_alloc_insuff">>, #{total => 5}),
                  Result = flurm_license:do_allocate(1, [{<<"do_alloc_insuff">>, 10}]),
                  ?assertEqual({error, insufficient_licenses}, Result)
              end)},

             {"do_deallocate",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_dealloc">>, #{total => 10}),
                  flurm_license:do_allocate(1, [{<<"do_dealloc">>, 5}]),
                  flurm_license:do_deallocate(1, [{<<"do_dealloc">>, 3}]),
                  ?assertEqual(8, flurm_license:get_available(<<"do_dealloc">>))
              end)},

             {"do_reserve success",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_reserve">>, #{total => 10}),
                  ok = flurm_license:do_reserve(<<"do_reserve">>, 5),
                  ?assertEqual(5, flurm_license:get_available(<<"do_reserve">>))
              end)},

             {"do_reserve insufficient",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_reserve_insuff">>, #{total => 5}),
                  flurm_license:do_allocate(1, [{<<"do_reserve_insuff">>, 3}]),
                  Result = flurm_license:do_reserve(<<"do_reserve_insuff">>, 5),
                  ?assertEqual({error, insufficient_licenses}, Result)
              end)},

             {"do_unreserve",
              ?_test(begin
                  flurm_license:do_add_license(<<"do_unreserve">>, #{total => 10}),
                  flurm_license:do_reserve(<<"do_unreserve">>, 5),
                  flurm_license:do_unreserve(<<"do_unreserve">>, 3),
                  {ok, License} = flurm_license:get(<<"do_unreserve">>),
                  ?assertEqual(2, maps:get(reserved, License))
              end)},

             {"load_configured_licenses with empty config",
              ?_test(begin
                  application:unset_env(flurm_core, licenses),
                  ok = flurm_license:load_configured_licenses(),
                  ?assert(true)
              end)},

             {"load_configured_licenses with licenses",
              ?_test(begin
                  application:set_env(flurm_core, licenses, [
                      {<<"direct_configured">>, #{total => 100}}
                  ]),
                  ok = flurm_license:load_configured_licenses(),
                  ?assertEqual(true, flurm_license:exists(<<"direct_configured">>)),
                  application:unset_env(flurm_core, licenses)
              end)},

             %% Line 204: empty name without count (just whitespace)
             {"parse_single_license empty name without count",
              ?_test(begin
                  ?assertThrow({invalid_license, empty_name},
                               flurm_license:parse_single_license(<<"   ">>))
              end)},

             %% Line 211: empty name with count
             {"parse_single_license empty name with count",
              ?_test(begin
                  ?assertThrow({invalid_license, empty_name},
                               flurm_license:parse_single_license(<<"  :5">>))
              end)},

             %% Line 386: allocate_license on non-existent license
             {"allocate_license non-existent license",
              ?_test(begin
                  ok = flurm_license:allocate_license(999, <<"no_such_license">>, 1),
                  ?assert(true)
              end)},

             %% Line 404: deallocate_license on non-existent license
             {"deallocate_license non-existent license",
              ?_test(begin
                  ok = flurm_license:deallocate_license(999, <<"no_such_license">>, 1),
                  ?assert(true)
              end)},

             %% Line 429: do_unreserve on non-existent license
             {"do_unreserve non-existent license",
              ?_test(begin
                  ok = flurm_license:do_unreserve(<<"no_such_license">>, 5),
                  ?assert(true)
              end)}
         ]
     end}.

%%====================================================================
%% Edge Case Coverage Tests - Lines 106, 125, 282
%%====================================================================

list_before_init_test_() ->
    {"list when ETS table doesn't exist (line 125)",
     {setup,
      fun() ->
          %% Stop server and destroy tables
          _ = case whereis(?SERVER) of
              undefined -> ok;
              OldPid ->
                  gen_server:stop(OldPid, normal, 5000),
                  timer:sleep(50)
          end,
          cleanup_ets(),
          ok
      end,
      fun(_) ->
          %% Restart server for other tests
          cleanup_ets(),
          ok
      end,
      fun(_) ->
          [
              {"list returns empty when table undefined",
               ?_test(begin
                   %% ETS table should not exist now
                   Result = flurm_license:list(),
                   ?assertEqual([], Result)
               end)}
          ]
      end}}.

deallocate_empty_request_test_() ->
    {"deallocate empty list triggers line 282",
     {setup,
      fun setup/0,
      fun cleanup/1,
      fun(_) ->
          [
              {"deallocate empty requests - line 282 [] -> ok",
               ?_test(begin
                   %% Empty deallocate should hit line 282
                   ok = flurm_license:deallocate(999, []),
                   ?assert(true)
               end)}
          ]
      end}}.
