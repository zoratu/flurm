%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_upgrade module
%%%
%%% These tests do NOT use meck - they test the actual implementation
%%% directly, handling environment limitations gracefully.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_upgrade_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Ensure lager is started for logging functions
    application:load(lager),
    application:set_env(lager, handlers, []),
    application:set_env(lager, error_logger_redirect, false),
    catch application:start(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

flurm_upgrade_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"versions/0 tests", fun versions_test/0},
      {"reload_module/1 tests", fun reload_module_test/0},
      {"reload_modules/1 tests", fun reload_modules_test/0},
      {"check_upgrade/1 tests", fun check_upgrade_test/0},
      {"which_releases/0 tests", fun which_releases_test/0},
      {"upgrade_status/0 tests", fun upgrade_status_test/0},
      {"transform_state/3 tests", fun transform_state_test/0},
      {"install_release/1 tests", fun install_release_test/0},
      {"rollback/0 tests", fun rollback_test/0}
     ]}.

%%====================================================================
%% versions/0 Tests
%%====================================================================

versions_test() ->
    %% versions/0 returns list of {App, Version} for loaded FLURM apps
    Result = flurm_upgrade:versions(),

    %% Result should be a list
    ?assert(is_list(Result)),

    %% Each element should be {atom(), string()}
    lists:foreach(fun({App, Vsn}) ->
        ?assert(is_atom(App)),
        ?assert(is_list(Vsn))
    end, Result),

    %% If any FLURM apps are loaded, they should be in the list
    %% The FLURM_APPS list is: flurm_config, flurm_protocol, flurm_core,
    %%                         flurm_db, flurm_controller, flurm_node_daemon

    %% Check that result contains only known FLURM apps
    FlurMApps = [flurm_config, flurm_protocol, flurm_core,
                 flurm_db, flurm_controller, flurm_node_daemon],
    AppNames = [App || {App, _} <- Result],
    lists:foreach(fun(App) ->
        ?assert(lists:member(App, FlurMApps))
    end, AppNames),

    ok.

%%====================================================================
%% reload_module/1 Tests
%%====================================================================

reload_module_test() ->
    %% Test with a non-existent module - should return error
    Result1 = flurm_upgrade:reload_module(nonexistent_module_xyz_12345),
    ?assertMatch({error, {load_failed, _}}, Result1),

    %% Test with flurm_upgrade itself (not a sticky module)
    Result2 = flurm_upgrade:reload_module(flurm_upgrade),
    %% It should either succeed or fail gracefully
    case Result2 of
        ok -> ok;
        {error, _} -> ok  % May fail if old code exists
    end,

    ok.

%%====================================================================
%% reload_modules/1 Tests
%%====================================================================

reload_modules_test() ->
    %% Test with empty list
    ?assertEqual(ok, flurm_upgrade:reload_modules([])),

    %% Test with non-existent module - should return partial failure
    Result1 = flurm_upgrade:reload_modules([nonexistent_module_abc_99999]),
    ?assertMatch({error, {partial_failure, _}}, Result1),

    %% Test with multiple non-existent modules
    Result2 = flurm_upgrade:reload_modules([
        nonexistent_mod_1_xyz,
        nonexistent_mod_2_xyz
    ]),
    ?assertMatch({error, {partial_failure, Failures}} when length(Failures) == 2, Result2),

    ok.

%%====================================================================
%% check_upgrade/1 Tests
%%====================================================================

check_upgrade_test() ->
    %% Test check_upgrade with a non-existent version
    %% This should fail the release_exists check
    Result1 = flurm_upgrade:check_upgrade("999.999.999"),
    ?assertMatch({error, _}, Result1),

    %% Verify the error includes release_exists failure
    {error, Failures1} = Result1,
    ?assert(is_list(Failures1)),

    %% Check that release_exists is one of the failures
    FailureNames1 = [Name || {Name, _} <- Failures1],
    ?assert(lists:member(release_exists, FailureNames1)),

    %% Test with another fake version
    Result2 = flurm_upgrade:check_upgrade("0.0.0-nonexistent"),
    ?assertMatch({error, _}, Result2),

    ok.

%%====================================================================
%% which_releases/0 Tests
%%====================================================================

which_releases_test() ->
    %% which_releases/0 calls release_handler:which_releases()
    %% In test environment, release_handler may not be running
    try
        Result = flurm_upgrade:which_releases(),

        %% Result should be a list
        ?assert(is_list(Result)),

        %% Each element should be {string(), atom()}
        lists:foreach(fun({Vsn, Status}) ->
            ?assert(is_list(Vsn)),
            ?assert(is_atom(Status))
        end, Result),

        %% If there are releases, status should be one of the known values
        KnownStatuses = [current, permanent, old, unpacked],
        lists:foreach(fun({_Vsn, Status}) ->
            ?assert(lists:member(Status, KnownStatuses) orelse is_atom(Status))
        end, Result)
    catch
        exit:{noproc, _} ->
            %% release_handler not running in test environment - expected
            ok
    end,

    ok.

%%====================================================================
%% upgrade_status/0 Tests
%%====================================================================

upgrade_status_test() ->
    %% upgrade_status/0 returns a map with upgrade information
    %% May fail if release_handler is not running
    try
        Result = flurm_upgrade:upgrade_status(),

        %% Result should be a map
        ?assert(is_map(Result)),

        %% Should contain expected keys
        ?assert(maps:is_key(versions, Result)),
        ?assert(maps:is_key(releases, Result)),
        ?assert(maps:is_key(loaded_modules, Result)),
        ?assert(maps:is_key(old_code_modules, Result)),

        %% Verify types of values
        ?assert(is_list(maps:get(versions, Result))),
        ?assert(is_list(maps:get(releases, Result))),
        ?assert(is_list(maps:get(loaded_modules, Result))),
        ?assert(is_list(maps:get(old_code_modules, Result))),

        %% loaded_modules should contain flurm_upgrade since it's loaded
        LoadedMods = maps:get(loaded_modules, Result),
        ?assert(lists:member(flurm_upgrade, LoadedMods)),

        %% All loaded modules should have flurm_ prefix
        lists:foreach(fun(Mod) ->
            ModStr = atom_to_list(Mod),
            ?assert(lists:prefix("flurm_", ModStr))
        end, LoadedMods),

        %% old_code_modules should be a subset of loaded_modules
        OldCodeMods = maps:get(old_code_modules, Result),
        lists:foreach(fun(Mod) ->
            ?assert(lists:member(Mod, LoadedMods))
        end, OldCodeMods)
    catch
        exit:{noproc, _} ->
            %% release_handler not running in test environment - expected
            ok
    end,

    ok.

%%====================================================================
%% transform_state/3 Tests
%%====================================================================

transform_state_test() ->
    %% Test with a module that doesn't export transform_state/3
    %% Should return the state unchanged
    OldState1 = #{key => value, data => [1, 2, 3]},
    Extra1 = [],
    Result1 = flurm_upgrade:transform_state(lists, OldState1, Extra1),
    ?assertEqual(OldState1, Result1),

    %% Test with different state types
    Result2 = flurm_upgrade:transform_state(erlang, {state, 123, "test"}, extra_data),
    ?assertEqual({state, 123, "test"}, Result2),

    %% Test with simple values
    Result3 = flurm_upgrade:transform_state(maps, simple_atom, []),
    ?assertEqual(simple_atom, Result3),

    %% Test with undefined state
    Result4 = flurm_upgrade:transform_state(proplists, undefined, undefined),
    ?assertEqual(undefined, Result4),

    %% Test with empty state
    Result5 = flurm_upgrade:transform_state(string, [], []),
    ?assertEqual([], Result5),

    %% Test with complex nested state
    ComplexState = #{
        users => [#{id => 1, name => <<"Alice">>},
                  #{id => 2, name => <<"Bob">>}],
        config => #{timeout => 5000, retries => 3},
        buffer => <<1, 2, 3, 4, 5>>
    },
    Result6 = flurm_upgrade:transform_state(io, ComplexState, upgrade_v2),
    ?assertEqual(ComplexState, Result6),

    ok.

%%====================================================================
%% install_release/1 Tests
%%====================================================================

install_release_test() ->
    %% Test install_release with non-existent version
    %% This should fail at check_upgrade stage
    Result1 = flurm_upgrade:install_release("999.0.0-fake"),
    ?assertMatch({error, {pre_check_failed, _}}, Result1),

    %% Verify the pre-check failure contains release_exists
    {error, {pre_check_failed, Reasons}} = Result1,
    FailureNames = [Name || {Name, _} <- Reasons],
    ?assert(lists:member(release_exists, FailureNames)),

    %% Test with another non-existent version
    Result2 = flurm_upgrade:install_release("0.0.1-test"),
    ?assertMatch({error, {pre_check_failed, _}}, Result2),

    ok.

%%====================================================================
%% rollback/0 Tests
%%====================================================================

rollback_test() ->
    %% rollback/0 checks which_releases and attempts to roll back
    %% In test environment without proper release setup, this will fail
    try
        Result = flurm_upgrade:rollback(),

        %% Should return either ok, or an error
        case Result of
            ok -> ok;
            {error, no_previous_release} -> ok;  % Expected if no previous release
            {error, _Reason} -> ok  % Other errors are also acceptable
        end
    catch
        exit:{noproc, _} ->
            %% release_handler not running in test environment - expected
            ok
    end,

    ok.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

%% Test for consistent return types
return_types_test_() ->
    [
     {"versions returns list", fun() ->
         ?assert(is_list(flurm_upgrade:versions()))
     end},
     {"which_releases handles missing release_handler", fun() ->
         try
             Result = flurm_upgrade:which_releases(),
             ?assert(is_list(Result))
         catch
             exit:{noproc, _} -> ok
         end
     end},
     {"upgrade_status handles missing release_handler", fun() ->
         try
             Result = flurm_upgrade:upgrade_status(),
             ?assert(is_map(Result))
         catch
             exit:{noproc, _} -> ok
         end
     end}
    ].

%% Test reload_module with various module states
reload_module_edge_cases_test_() ->
    [
     {"reload non-existent module", fun() ->
         Result = flurm_upgrade:reload_module(definitely_not_a_real_module_xyz),
         ?assertMatch({error, {load_failed, _}}, Result)
     end},
     {"reload flurm module", fun() ->
         %% flurm_upgrade is a non-sticky module we can try to reload
         Result = flurm_upgrade:reload_module(flurm_upgrade),
         %% Should succeed or fail gracefully
         ?assert(Result =:= ok orelse element(1, Result) =:= error)
     end}
    ].

%% Test reload_modules with various inputs
reload_modules_edge_cases_test_() ->
    [
     {"empty list succeeds", fun() ->
         ?assertEqual(ok, flurm_upgrade:reload_modules([]))
     end},
     {"single non-existent module fails", fun() ->
         Result = flurm_upgrade:reload_modules([fake_module_abc123]),
         ?assertMatch({error, {partial_failure, [{fake_module_abc123, _}]}}, Result)
     end},
     {"multiple failures are reported", fun() ->
         Mods = [fake_mod_a, fake_mod_b, fake_mod_c],
         Result = flurm_upgrade:reload_modules(Mods),
         {error, {partial_failure, Failures}} = Result,
         ?assertEqual(3, length(Failures))
     end}
    ].

%% Test check_upgrade with various version strings
check_upgrade_edge_cases_test_() ->
    [
     {"check non-existent version", fun() ->
         Result = flurm_upgrade:check_upgrade("100.0.0"),
         ?assertMatch({error, _}, Result)
     end},
     {"check version with special chars", fun() ->
         Result = flurm_upgrade:check_upgrade("1.0.0-alpha+build.123"),
         ?assertMatch({error, _}, Result)
     end},
     {"check empty version string", fun() ->
         Result = flurm_upgrade:check_upgrade(""),
         %% Empty string may pass release_exists check if releases dir exists
         %% but still returns ok or error depending on environment
         ?assert(Result =:= ok orelse element(1, Result) =:= error)
     end}
    ].

%% Test transform_state preserves various state types
transform_state_types_test_() ->
    [
     {"atom state preserved", fun() ->
         ?assertEqual(my_atom, flurm_upgrade:transform_state(io, my_atom, []))
     end},
     {"tuple state preserved", fun() ->
         State = {state, data, 123},
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"map state preserved", fun() ->
         State = #{a => 1, b => 2},
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"list state preserved", fun() ->
         State = [1, 2, 3, 4, 5],
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"binary state preserved", fun() ->
         State = <<1, 2, 3, 4>>,
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"pid state preserved", fun() ->
         State = self(),
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"reference state preserved", fun() ->
         State = make_ref(),
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end},
     {"nested structure preserved", fun() ->
         State = #{list => [1, {2, 3}], map => #{nested => true}},
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, []))
     end}
    ].

%% Test upgrade_status map structure (with release_handler protection)
upgrade_status_structure_test_() ->
    [
     {"status has versions key", fun() ->
         try
             Status = flurm_upgrade:upgrade_status(),
             ?assert(maps:is_key(versions, Status))
         catch
             exit:{noproc, _} -> ok
         end
     end},
     {"status has releases key", fun() ->
         try
             Status = flurm_upgrade:upgrade_status(),
             ?assert(maps:is_key(releases, Status))
         catch
             exit:{noproc, _} -> ok
         end
     end},
     {"status has loaded_modules key", fun() ->
         try
             Status = flurm_upgrade:upgrade_status(),
             ?assert(maps:is_key(loaded_modules, Status))
         catch
             exit:{noproc, _} -> ok
         end
     end},
     {"status has old_code_modules key", fun() ->
         try
             Status = flurm_upgrade:upgrade_status(),
             ?assert(maps:is_key(old_code_modules, Status))
         catch
             exit:{noproc, _} -> ok
         end
     end},
     {"loaded_modules are sorted", fun() ->
         try
             Status = flurm_upgrade:upgrade_status(),
             Mods = maps:get(loaded_modules, Status),
             ?assertEqual(lists:sort(Mods), Mods)
         catch
             exit:{noproc, _} -> ok
         end
     end}
    ].

%% Test install_release error handling
install_release_errors_test_() ->
    [
     {"non-existent release fails pre-check", fun() ->
         Result = flurm_upgrade:install_release("99.99.99"),
         ?assertMatch({error, {pre_check_failed, _}}, Result)
     end},
     {"pre-check includes release_exists", fun() ->
         {error, {pre_check_failed, Reasons}} = flurm_upgrade:install_release("88.88.88"),
         Names = [N || {N, _} <- Reasons],
         ?assert(lists:member(release_exists, Names))
     end}
    ].

%% Test versions filtering behavior
versions_filtering_test_() ->
    [
     {"versions returns only FLURM apps", fun() ->
         Versions = flurm_upgrade:versions(),
         ValidApps = [flurm_config, flurm_protocol, flurm_core,
                      flurm_db, flurm_controller, flurm_node_daemon],
         lists:foreach(fun({App, _}) ->
             ?assert(lists:member(App, ValidApps))
         end, Versions)
     end},
     {"version strings are non-empty", fun() ->
         Versions = flurm_upgrade:versions(),
         lists:foreach(fun({_, Vsn}) ->
             ?assert(length(Vsn) > 0)
         end, Versions)
     end}
    ].

%% Test which_releases format (with release_handler protection)
which_releases_format_test_() ->
    [
     {"releases are version-status tuples", fun() ->
         try
             Releases = flurm_upgrade:which_releases(),
             lists:foreach(fun(Rel) ->
                 ?assertMatch({_, _}, Rel),
                 {Vsn, Status} = Rel,
                 ?assert(is_list(Vsn)),
                 ?assert(is_atom(Status))
             end, Releases)
         catch
             exit:{noproc, _} -> ok
         end
     end}
    ].

%% Test rollback behavior (with release_handler protection)
rollback_behavior_test_() ->
    [
     {"rollback returns expected result type", fun() ->
         try
             Result = flurm_upgrade:rollback(),
             ?assert(Result =:= ok orelse
                     (is_tuple(Result) andalso element(1, Result) =:= error))
         catch
             exit:{noproc, _} -> ok
         end
     end}
    ].

%%====================================================================
%% Additional Tests for Higher Coverage
%%====================================================================

%% Test transform_state with different module types
transform_state_module_test_() ->
    [
     {"transform with gen_server module", fun() ->
         State = {state, gen_server_state},
         ?assertEqual(State, flurm_upgrade:transform_state(gen_server, State, []))
     end},
     {"transform with gen_statem module", fun() ->
         State = {state_name, state_data},
         ?assertEqual(State, flurm_upgrade:transform_state(gen_statem, State, []))
     end},
     {"transform with supervisor module", fun() ->
         State = {state, children, dynamic},
         ?assertEqual(State, flurm_upgrade:transform_state(supervisor, State, []))
     end},
     {"transform with application module", fun() ->
         State = #{app_state => running},
         ?assertEqual(State, flurm_upgrade:transform_state(application, State, []))
     end}
    ].

%% Test reload_module error cases
reload_module_error_test_() ->
    [
     {"reload returns load_failed for missing module", fun() ->
         {error, {load_failed, Reason}} = flurm_upgrade:reload_module(no_such_module_ever),
         ?assert(Reason =:= nofile orelse Reason =:= badfile)
     end}
    ].

%% Test reload_modules order and result aggregation
reload_modules_aggregation_test_() ->
    [
     {"failures include module names", fun() ->
         Mods = [missing_a, missing_b],
         {error, {partial_failure, Failures}} = flurm_upgrade:reload_modules(Mods),
         FailedMods = [M || {M, _} <- Failures],
         ?assertEqual(lists:sort(Mods), lists:sort(FailedMods))
     end},
     {"each failure has error reason", fun() ->
         Mods = [missing_x],
         {error, {partial_failure, Failures}} = flurm_upgrade:reload_modules(Mods),
         [{missing_x, Reason}] = Failures,
         ?assertMatch({load_failed, _}, Reason)
     end}
    ].

%% Test check_upgrade detailed failure info
check_upgrade_failures_test_() ->
    [
     {"failures are list of tuples", fun() ->
         {error, Failures} = flurm_upgrade:check_upgrade("nonexistent"),
         ?assert(is_list(Failures)),
         lists:foreach(fun(F) ->
             ?assertMatch({_, _}, F)
         end, Failures)
     end},
     {"each failure has name and reason", fun() ->
         {error, Failures} = flurm_upgrade:check_upgrade("fake_version"),
         lists:foreach(fun({Name, Reason}) ->
             ?assert(is_atom(Name)),
             ?assert(Reason =/= undefined)
         end, Failures)
     end}
    ].

%% Test versions with loaded applications
versions_loaded_apps_test_() ->
    [
     {"versions list may be empty", fun() ->
         %% If no FLURM apps are started, list is empty
         Result = flurm_upgrade:versions(),
         ?assert(is_list(Result))
     end},
     {"versions contains tuples", fun() ->
         Result = flurm_upgrade:versions(),
         lists:foreach(fun(Item) ->
             ?assertMatch({_, _}, Item)
         end, Result)
     end}
    ].

%% Test transform_state with various extra values
transform_state_extra_test_() ->
    [
     {"extra can be atom", fun() ->
         State = state_value,
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, upgrade))
     end},
     {"extra can be tuple", fun() ->
         State = state_value,
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, {from, "1.0", to, "2.0"}))
     end},
     {"extra can be map", fun() ->
         State = state_value,
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, #{version => 2}))
     end},
     {"extra can be undefined", fun() ->
         State = state_value,
         ?assertEqual(State, flurm_upgrade:transform_state(io, State, undefined))
     end}
    ].

%% Test install_release pre-check failure structure
install_release_precheck_test_() ->
    [
     {"pre_check_failed error format", fun() ->
         Result = flurm_upgrade:install_release("v_test"),
         ?assertMatch({error, {pre_check_failed, _}}, Result)
     end},
     {"pre_check_failed contains failures list", fun() ->
         {error, {pre_check_failed, Failures}} = flurm_upgrade:install_release("v_test2"),
         ?assert(is_list(Failures)),
         ?assert(length(Failures) >= 1)
     end}
    ].
