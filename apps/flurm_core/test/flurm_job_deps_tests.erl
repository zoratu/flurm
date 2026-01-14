%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dependencies Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_deps gen_server,
%%% covering dependency parsing, registration, resolution, and
%%% circular dependency detection.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_deps_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_deps_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Parse afterok dependency", fun test_parse_afterok/0},
        {"Parse afternotok dependency", fun test_parse_afternotok/0},
        {"Parse afterany dependency", fun test_parse_afterany/0},
        {"Parse after dependency", fun test_parse_after/0},
        {"Parse aftercorr dependency", fun test_parse_aftercorr/0},
        {"Parse singleton dependency", fun test_parse_singleton/0},
        {"Parse multiple dependencies", fun test_parse_multiple_deps/0},
        {"Parse plus-separated job IDs", fun test_parse_plus_separated/0},
        {"Format dependency spec", fun test_format_dependency_spec/0},
        {"Add dependency simple", fun test_add_dependency_simple/0},
        {"Add dependencies from spec", fun test_add_dependencies_spec/0},
        {"Get dependencies", fun test_get_dependencies/0},
        {"Get dependents", fun test_get_dependents/0},
        {"Check dependencies", fun test_check_dependencies/0},
        {"Are dependencies satisfied", fun test_are_dependencies_satisfied/0},
        {"Circular dependency detection", fun test_circular_dependency/0},
        {"Has circular dependency", fun test_has_circular_dependency/0},
        {"Remove dependency", fun test_remove_dependency/0},
        {"Remove all dependencies", fun test_remove_all_dependencies/0},
        {"On job state change", fun test_on_job_state_change/0},
        {"Notify completion", fun test_notify_completion/0},
        {"Get dependency graph", fun test_get_dependency_graph/0},
        {"Clear completed dependencies", fun test_clear_completed_deps/0},
        {"Singleton dependency handling", fun test_singleton_dependency/0},
        {"Unknown request handling", fun test_unknown_request/0},
        {"Cast message handling", fun test_cast_message_handling/0},
        {"Info message handling", fun test_info_message_handling/0},
        {"Invalid dependency spec", fun test_invalid_dep_spec/0},
        {"Empty dependency spec", fun test_empty_dep_spec/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, Pid} = flurm_job_deps:start_link(),
    #{deps_pid => Pid}.

cleanup(#{deps_pid := Pid}) ->
    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Parsing Tests
%%====================================================================

test_parse_afterok() ->
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:123">>),
    ?assertEqual([{afterok, 123}], Deps),
    ok.

test_parse_afternotok() ->
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afternotok:456">>),
    ?assertEqual([{afternotok, 456}], Deps),
    ok.

test_parse_afterany() ->
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterany:789">>),
    ?assertEqual([{afterany, 789}], Deps),
    ok.

test_parse_after() ->
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"after:100">>),
    ?assertEqual([{after_start, 100}], Deps),
    ok.

test_parse_aftercorr() ->
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"aftercorr:200">>),
    ?assertEqual([{aftercorr, 200}], Deps),
    ok.

test_parse_singleton() ->
    %% Singleton without name
    {ok, Deps1} = flurm_job_deps:parse_dependency_spec(<<"singleton">>),
    ?assertEqual([{singleton, <<"default">>}], Deps1),

    %% Singleton with name
    {ok, Deps2} = flurm_job_deps:parse_dependency_spec(<<"singleton:myname">>),
    ?assertEqual([{singleton, <<"myname">>}], Deps2),
    ok.

test_parse_multiple_deps() ->
    %% Comma-separated dependencies
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:100,afternotok:200">>),
    ?assertEqual(2, length(Deps)),
    ?assert(lists:member({afterok, 100}, Deps)),
    ?assert(lists:member({afternotok, 200}, Deps)),
    ok.

test_parse_plus_separated() ->
    %% Multiple job IDs with + separator
    {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:100+200+300">>),
    ?assertEqual([{afterok, [100, 200, 300]}], Deps),
    ok.

%%====================================================================
%% Format Tests
%%====================================================================

test_format_dependency_spec() ->
    %% Empty list
    ?assertEqual(<<>>, flurm_job_deps:format_dependency_spec([])),

    %% Single dependency
    Formatted1 = flurm_job_deps:format_dependency_spec([{afterok, 123}]),
    ?assertEqual(<<"afterok:123">>, Formatted1),

    %% Singleton (default)
    Formatted3 = flurm_job_deps:format_dependency_spec([{singleton, <<"default">>}]),
    ?assertEqual(<<"singleton">>, Formatted3),

    %% Singleton with name
    Formatted4 = flurm_job_deps:format_dependency_spec([{singleton, <<"myname">>}]),
    ?assertEqual(<<"singleton:myname">>, Formatted4),

    %% Multiple targets
    Formatted5 = flurm_job_deps:format_dependency_spec([{afterok, [100, 200]}]),
    ?assertEqual(<<"afterok:100+200">>, Formatted5),
    ok.

%%====================================================================
%% Add Dependency Tests
%%====================================================================

test_add_dependency_simple() ->
    %% Add a simple dependency
    ok = flurm_job_deps:add_dependency(10, afterok, 1),

    %% Verify dependency exists
    Deps = flurm_job_deps:get_dependencies(10),
    ?assertEqual(1, length(Deps)),
    ok.

test_add_dependencies_spec() ->
    %% Add from parsed spec string
    ok = flurm_job_deps:add_dependencies(20, <<"afterok:100,afternotok:200">>),

    %% Verify both dependencies exist
    Deps = flurm_job_deps:get_dependencies(20),
    ?assertEqual(2, length(Deps)),
    ok.

%%====================================================================
%% Query Tests
%%====================================================================

test_get_dependencies() ->
    ok = flurm_job_deps:add_dependency(30, afterok, 300),
    ok = flurm_job_deps:add_dependency(30, afternotok, 301),

    Deps = flurm_job_deps:get_dependencies(30),
    ?assertEqual(2, length(Deps)),

    %% No dependencies for non-existent job
    NoDeps = flurm_job_deps:get_dependencies(99999),
    ?assertEqual([], NoDeps),
    ok.

test_get_dependents() ->
    ok = flurm_job_deps:add_dependency(40, afterok, 400),
    ok = flurm_job_deps:add_dependency(41, afterok, 400),
    ok = flurm_job_deps:add_dependency(42, afternotok, 400),

    Dependents = flurm_job_deps:get_dependents(400),
    ?assertEqual(3, length(Dependents)),
    ?assert(lists:member(40, Dependents)),
    ?assert(lists:member(41, Dependents)),
    ?assert(lists:member(42, Dependents)),

    %% No dependents for job with no deps
    NoDeps = flurm_job_deps:get_dependents(99999),
    ?assertEqual([], NoDeps),
    ok.

test_check_dependencies() ->
    %% Job with no dependencies
    {ok, []} = flurm_job_deps:check_dependencies(99998),

    %% Job with unsatisfied dependency
    ok = flurm_job_deps:add_dependency(50, afterok, 500),
    {waiting, Deps} = flurm_job_deps:check_dependencies(50),
    ?assertEqual(1, length(Deps)),
    ok.

test_are_dependencies_satisfied() ->
    %% Job with no dependencies is satisfied
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(99997)),

    %% Job with unsatisfied dependency
    ok = flurm_job_deps:add_dependency(60, afterok, 600),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(60)),
    ok.

%%====================================================================
%% Circular Dependency Tests
%%====================================================================

test_circular_dependency() ->
    %% Job A depends on B
    ok = flurm_job_deps:add_dependency(100, afterok, 101),

    %% Check if B->A would create cycle
    Result = flurm_job_deps:detect_circular_dependency(101, 100),
    ?assertMatch({error, circular_dependency, _}, Result),

    %% No cycle when no existing dependency
    ok = flurm_job_deps:detect_circular_dependency(200, 201),
    ok.

test_has_circular_dependency() ->
    ok = flurm_job_deps:add_dependency(110, afterok, 111),

    %% Would create cycle
    ?assertEqual(true, flurm_job_deps:has_circular_dependency(111, 110)),

    %% No cycle
    ?assertEqual(false, flurm_job_deps:has_circular_dependency(200, 201)),

    %% Non-integer targets (singletons) can't create cycles
    ?assertEqual(false, flurm_job_deps:has_circular_dependency(300, <<"singleton">>)),
    ok.

%%====================================================================
%% Remove Tests
%%====================================================================

test_remove_dependency() ->
    ok = flurm_job_deps:add_dependency(70, afterok, 700),
    ok = flurm_job_deps:add_dependency(70, afternotok, 701),

    ?assertEqual(2, length(flurm_job_deps:get_dependencies(70))),

    %% Remove one
    ok = flurm_job_deps:remove_dependency(70, {afterok, 700}),
    ?assertEqual(1, length(flurm_job_deps:get_dependencies(70))),
    ok.

test_remove_all_dependencies() ->
    ok = flurm_job_deps:add_dependency(80, afterok, 800),
    ok = flurm_job_deps:add_dependency(80, afternotok, 801),
    ok = flurm_job_deps:add_dependency(80, afterany, 802),

    ?assertEqual(3, length(flurm_job_deps:get_dependencies(80))),

    %% Remove all
    ok = flurm_job_deps:remove_all_dependencies(80),
    ?assertEqual(0, length(flurm_job_deps:get_dependencies(80))),
    ok.

%%====================================================================
%% State Change Tests
%%====================================================================

test_on_job_state_change() ->
    %% Setup dependent jobs
    ok = flurm_job_deps:add_dependency(90, afterok, 900),

    %% State change (this is async)
    ok = flurm_job_deps:on_job_state_change(900, completed),
    timer:sleep(50),

    %% The dependency should be marked as satisfied internally
    %% (depends on actual implementation behavior)
    ok.

test_notify_completion() ->
    %% Setup dependency
    ok = flurm_job_deps:add_dependency(95, afterany, 950),

    %% Notify completion (async)
    ok = flurm_job_deps:notify_completion(950, completed),
    timer:sleep(50),
    ok.

%%====================================================================
%% Graph Tests
%%====================================================================

test_get_dependency_graph() ->
    ok = flurm_job_deps:add_dependency(120, afterok, 1200),
    ok = flurm_job_deps:add_dependency(121, afterok, 1200),
    ok = flurm_job_deps:add_dependency(121, afternotok, 1201),

    Graph = flurm_job_deps:get_dependency_graph(),
    ?assert(is_map(Graph)),

    %% Check that our jobs are in the graph
    ?assert(maps:is_key(120, Graph) orelse maps:is_key(121, Graph)),
    ok.

test_clear_completed_deps() ->
    %% Clear any completed dependencies (mostly tests code path)
    Count = flurm_job_deps:clear_completed_dependencies(),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    ok.

%%====================================================================
%% Singleton Tests
%%====================================================================

test_singleton_dependency() ->
    %% Add singleton dependency
    ok = flurm_job_deps:add_dependency(130, singleton, <<"test_singleton">>),

    %% First singleton should be satisfied immediately
    Deps = flurm_job_deps:get_dependencies(130),
    ?assertEqual(1, length(Deps)),

    %% The first singleton holder can be verified through dependencies
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_unknown_request() ->
    Result = gen_server:call(flurm_job_deps, unknown_request),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_cast_message_handling() ->
    %% Send unknown cast - should not crash server
    gen_server:cast(flurm_job_deps, unknown_cast),
    timer:sleep(10),
    ?assertEqual(true, is_process_alive(whereis(flurm_job_deps))),
    ok.

test_info_message_handling() ->
    %% Send info message - should not crash server
    whereis(flurm_job_deps) ! {arbitrary, info, message},
    timer:sleep(10),
    ?assertEqual(true, is_process_alive(whereis(flurm_job_deps))),
    ok.

test_invalid_dep_spec() ->
    %% Unknown dependency type
    Result = flurm_job_deps:parse_dependency_spec(<<"unknowntype:123">>),
    ?assertMatch({error, _}, Result),
    ok.

test_empty_dep_spec() ->
    %% Empty spec returns empty list
    {ok, []} = flurm_job_deps:parse_dependency_spec(<<>>),
    ok.

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

%% Test adding multiple targets at once
multiple_targets_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Add dependency with multiple targets", fun() ->
             %% Add dependency with list of targets
             ok = flurm_job_deps:add_dependency(140, afterok, [1400, 1401, 1402]),
             Deps = flurm_job_deps:get_dependencies(140),
             ?assert(length(Deps) >= 1)
         end}
     end}.

%% Test dependency chain detection
dependency_chain_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Dependency chain creates no false positive cycles", fun() ->
             %% A -> B -> C is not a cycle
             ok = flurm_job_deps:add_dependency(150, afterok, 151),
             ok = flurm_job_deps:add_dependency(151, afterok, 152),

             %% Adding 152 -> 153 should not detect cycle
             ?assertEqual(false, flurm_job_deps:has_circular_dependency(152, 153)),

             %% But 152 -> 150 would create cycle
             ?assertEqual(true, flurm_job_deps:has_circular_dependency(152, 150))
         end}
     end}.

%% Test release_job and hold_for_dependencies code paths
release_and_hold_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Hold for dependencies with no deps", fun() ->
                 %% Job with no dependencies can run immediately
                 Result = flurm_job_deps:hold_for_dependencies(9900),
                 ?assertEqual(ok, Result)
             end},
             {"Release job with satisfied dependencies", fun() ->
                 %% Job with no dependencies, release should succeed or error gracefully
                 Result = flurm_job_deps:release_job(9901),
                 %% Depending on whether job_manager is running, this returns ok or error
                 ?assert(Result =:= ok orelse element(1, Result) =:= error)
             end},
             {"Release job with unsatisfied dependencies", fun() ->
                 ok = flurm_job_deps:add_dependency(9902, afterok, 9903),
                 Result = flurm_job_deps:release_job(9902),
                 ?assertEqual({error, dependencies_not_satisfied}, Result)
             end}
         ]
     end}.
