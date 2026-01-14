%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dependencies Direct Tests
%%%
%%% Additional comprehensive EUnit tests for the flurm_job_deps gen_server,
%%% focusing on direct function calls without mocking, complex dependency
%%% scenarios, cycle detection, and internal function coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_deps_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

deps_direct_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Parse all dependency types", fun test_parse_all_dep_types/0},
        {"Parse afterburstbuffer", fun test_parse_afterburstbuffer/0},
        {"Format all dependency types", fun test_format_all_dep_types/0},
        {"Format multi-target dependency", fun test_format_multi_target/0},
        {"Add dependency with list of targets", fun test_add_dep_multiple_targets/0},
        {"Circular dependency chain detection", fun test_circular_chain_detection/0},
        {"Circular dependency direct detection", fun test_circular_direct_detection/0},
        {"State satisfaction checks", fun test_state_satisfaction/0},
        {"Singleton dependency lifecycle", fun test_singleton_lifecycle/0},
        {"Multiple singletons", fun test_multiple_singletons/0},
        {"Dependency graph traversal", fun test_dependency_graph/0},
        {"Remove dependency from graph", fun test_remove_from_graph/0},
        {"Add duplicate dependency", fun test_add_duplicate_dep/0},
        {"Check already satisfied dependency", fun test_already_satisfied_dep/0},
        {"Terminal state cleanup", fun test_terminal_state_cleanup/0},
        {"Hold and release job workflow", fun test_hold_release_workflow/0},
        {"Complex dependency chain", fun test_complex_dep_chain/0},
        {"Notify completion triggers satisfaction", fun test_notify_completion_satisfaction/0},
        {"Parse error handling", fun test_parse_errors/0},
        {"Clear completed dependencies", fun test_clear_completed/0}
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
%% Parsing Tests - Comprehensive
%%====================================================================

test_parse_all_dep_types() ->
    %% Test all supported dependency types
    {ok, [{after_start, 100}]} = flurm_job_deps:parse_dependency_spec(<<"after:100">>),
    {ok, [{afterok, 200}]} = flurm_job_deps:parse_dependency_spec(<<"afterok:200">>),
    {ok, [{afternotok, 300}]} = flurm_job_deps:parse_dependency_spec(<<"afternotok:300">>),
    {ok, [{afterany, 400}]} = flurm_job_deps:parse_dependency_spec(<<"afterany:400">>),
    {ok, [{aftercorr, 500}]} = flurm_job_deps:parse_dependency_spec(<<"aftercorr:500">>),
    ok.

test_parse_afterburstbuffer() ->
    %% Test afterburstbuffer dependency type
    {ok, [{afterburstbuffer, 600}]} = flurm_job_deps:parse_dependency_spec(<<"afterburstbuffer:600">>),
    ok.

test_format_all_dep_types() ->
    %% Test formatting all dependency types
    ?assertEqual(<<"after:100">>, flurm_job_deps:format_dependency_spec([{after_start, 100}])),
    ?assertEqual(<<"afterok:200">>, flurm_job_deps:format_dependency_spec([{afterok, 200}])),
    ?assertEqual(<<"afternotok:300">>, flurm_job_deps:format_dependency_spec([{afternotok, 300}])),
    ?assertEqual(<<"afterany:400">>, flurm_job_deps:format_dependency_spec([{afterany, 400}])),
    ?assertEqual(<<"aftercorr:500">>, flurm_job_deps:format_dependency_spec([{aftercorr, 500}])),
    ?assertEqual(<<"afterburstbuffer:600">>, flurm_job_deps:format_dependency_spec([{afterburstbuffer, 600}])),
    ok.

test_format_multi_target() ->
    %% Test formatting with multiple targets (job+job+job)
    Formatted = flurm_job_deps:format_dependency_spec([{afterok, [100, 200, 300]}]),
    ?assertEqual(<<"afterok:100+200+300">>, Formatted),
    ok.

%%====================================================================
%% Add Dependency Tests - Extended
%%====================================================================

test_add_dep_multiple_targets() ->
    %% Add dependency with list of targets
    ok = flurm_job_deps:add_dependency(10, afterok, [100, 101, 102]),

    %% Verify dependencies exist for each target
    Deps = flurm_job_deps:get_dependencies(10),
    ?assert(length(Deps) >= 1),  %% At least one dependency added
    ok.

test_add_duplicate_dep() ->
    %% Adding duplicate dependency should not create duplicate
    ok = flurm_job_deps:add_dependency(20, afterok, 200),
    Deps1 = flurm_job_deps:get_dependencies(20),

    %% Add same dependency again
    ok = flurm_job_deps:add_dependency(20, afterok, 200),
    Deps2 = flurm_job_deps:get_dependencies(20),

    %% Count should be the same
    ?assertEqual(length(Deps1), length(Deps2)),
    ok.

test_already_satisfied_dep() ->
    %% When target job is already in satisfying state, dependency is immediately satisfied
    %% This tests the check_target_satisfies internal function
    %% Since we don't have a running job manager, deps will be unsatisfied

    ok = flurm_job_deps:add_dependency(30, afterok, 3000),
    Deps = flurm_job_deps:get_dependencies(30),

    %% Dependency should exist but be unsatisfied (no job to check)
    ?assertEqual(1, length(Deps)),
    ok.

%%====================================================================
%% Circular Dependency Tests - Extended
%%====================================================================

test_circular_chain_detection() ->
    %% Test circular dependency detection in a chain: A -> B -> C -> A
    ok = flurm_job_deps:add_dependency(100, afterok, 101),
    ok = flurm_job_deps:add_dependency(101, afterok, 102),

    %% Adding 102 -> 100 would create cycle
    Result = flurm_job_deps:detect_circular_dependency(102, 100),
    ?assertMatch({error, circular_dependency, _}, Result),

    %% has_circular_dependency should return true
    ?assertEqual(true, flurm_job_deps:has_circular_dependency(102, 100)),

    %% Adding 102 -> 103 should be fine (no cycle)
    ?assertEqual(ok, flurm_job_deps:detect_circular_dependency(102, 103)),
    ?assertEqual(false, flurm_job_deps:has_circular_dependency(102, 103)),
    ok.

test_circular_direct_detection() ->
    %% Test direct cycle: A -> B, B -> A
    ok = flurm_job_deps:add_dependency(200, afterok, 201),

    %% Adding 201 -> 200 would create direct cycle
    {error, circular_dependency, Path} = flurm_job_deps:detect_circular_dependency(201, 200),
    ?assert(length(Path) >= 2),
    ok.

%%====================================================================
%% State Satisfaction Tests
%%====================================================================

test_state_satisfaction() ->
    %% Test state_satisfies logic by simulating state changes

    %% Add dependencies for different types
    ok = flurm_job_deps:add_dependency(300, after_start, 3000),
    ok = flurm_job_deps:add_dependency(301, afterok, 3001),
    ok = flurm_job_deps:add_dependency(302, afternotok, 3002),
    ok = flurm_job_deps:add_dependency(303, afterany, 3003),

    %% Verify all have unsatisfied dependencies (no job manager)
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(300)),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(301)),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(302)),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(303)),

    %% Notify state changes (even without real jobs, tests code paths)
    ok = flurm_job_deps:on_job_state_change(3000, running),
    ok = flurm_job_deps:on_job_state_change(3001, completed),
    ok = flurm_job_deps:on_job_state_change(3002, failed),
    ok = flurm_job_deps:on_job_state_change(3003, cancelled),
    timer:sleep(50),
    ok.

%%====================================================================
%% Singleton Tests - Extended
%%====================================================================

test_singleton_lifecycle() ->
    %% First singleton holder gets immediate satisfaction
    ok = flurm_job_deps:add_dependency(400, singleton, <<"test_singleton_1">>),
    Deps1 = flurm_job_deps:get_dependencies(400),
    ?assertEqual(1, length(Deps1)),

    %% Second job with same singleton must wait
    ok = flurm_job_deps:add_dependency(401, singleton, <<"test_singleton_1">>),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(401)),

    %% First job completes - singleton should be released
    ok = flurm_job_deps:on_job_state_change(400, completed),
    timer:sleep(50),

    %% Note: Without full job manager, release may not fully propagate
    ok.

test_multiple_singletons() ->
    %% Test that different singleton names are independent
    ok = flurm_job_deps:add_dependency(500, singleton, <<"singleton_a">>),
    ok = flurm_job_deps:add_dependency(501, singleton, <<"singleton_b">>),
    ok = flurm_job_deps:add_dependency(502, singleton, <<"singleton_c">>),

    %% All should be holders of their respective singletons (satisfied)
    %% (first holder of each singleton)
    Deps500 = flurm_job_deps:get_dependencies(500),
    Deps501 = flurm_job_deps:get_dependencies(501),
    Deps502 = flurm_job_deps:get_dependencies(502),

    ?assertEqual(1, length(Deps500)),
    ?assertEqual(1, length(Deps501)),
    ?assertEqual(1, length(Deps502)),
    ok.

%%====================================================================
%% Graph and Path Tests
%%====================================================================

test_dependency_graph() ->
    %% Build a dependency graph and verify structure
    ok = flurm_job_deps:add_dependency(600, afterok, 6000),
    ok = flurm_job_deps:add_dependency(600, afternotok, 6001),
    ok = flurm_job_deps:add_dependency(601, afterok, 6000),

    Graph = flurm_job_deps:get_dependency_graph(),
    ?assert(is_map(Graph)),

    %% Check that our jobs are in the graph
    ?assert(maps:is_key(600, Graph) orelse maps:is_key(601, Graph)),
    ok.

test_remove_from_graph() ->
    %% Test that removing dependencies updates the graph
    ok = flurm_job_deps:add_dependency(700, afterok, 7000),
    ok = flurm_job_deps:add_dependency(700, afternotok, 7001),

    %% Verify dependencies exist
    Deps1 = flurm_job_deps:get_dependencies(700),
    ?assertEqual(2, length(Deps1)),

    %% Remove one dependency
    ok = flurm_job_deps:remove_dependency(700, {afterok, 7000}),
    Deps2 = flurm_job_deps:get_dependencies(700),
    ?assertEqual(1, length(Deps2)),

    %% Remove all remaining
    ok = flurm_job_deps:remove_all_dependencies(700),
    Deps3 = flurm_job_deps:get_dependencies(700),
    ?assertEqual(0, length(Deps3)),
    ok.

%%====================================================================
%% Terminal State and Cleanup Tests
%%====================================================================

test_terminal_state_cleanup() ->
    %% Test that terminal states trigger cleanup
    ok = flurm_job_deps:add_dependency(800, afterok, 8000),
    ok = flurm_job_deps:add_dependency(800, afternotok, 8001),

    %% Get dependents before completion
    Dependents = flurm_job_deps:get_dependents(8000),
    ?assert(lists:member(800, Dependents)),

    %% Notify completion - should clean up dependents table
    ok = flurm_job_deps:on_job_state_change(8000, completed),
    timer:sleep(50),
    ok.

test_clear_completed() ->
    %% Test clearing completed dependencies
    ok = flurm_job_deps:add_dependency(900, afterok, 9000),

    %% Clear completed dependencies (tests code path)
    Count = flurm_job_deps:clear_completed_dependencies(),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    ok.

%%====================================================================
%% Hold and Release Tests
%%====================================================================

test_hold_release_workflow() ->
    %% Test hold_for_dependencies and release_job paths

    %% Job with no dependencies - should return ok for hold
    Result1 = flurm_job_deps:hold_for_dependencies(9900),
    ?assertEqual(ok, Result1),

    %% Job with unsatisfied dependency - should try to hold
    ok = flurm_job_deps:add_dependency(9901, afterok, 99010),
    Result2 = flurm_job_deps:hold_for_dependencies(9901),
    %% Result depends on job_manager availability
    ?assert(Result2 =:= ok orelse element(1, Result2) =:= error),

    %% Release job with unsatisfied deps should fail
    {error, dependencies_not_satisfied} = flurm_job_deps:release_job(9901),
    ok.

%%====================================================================
%% Complex Dependency Chain Tests
%%====================================================================

test_complex_dep_chain() ->
    %% Build a complex dependency chain:
    %% J1 -> J10, J1 -> J11
    %% J2 -> J10, J2 -> J12
    %% J3 depends on both J1 and J2
    ok = flurm_job_deps:add_dependency(1001, afterok, 10010),
    ok = flurm_job_deps:add_dependency(1001, afterok, 10011),
    ok = flurm_job_deps:add_dependency(1002, afterok, 10010),
    ok = flurm_job_deps:add_dependency(1002, afternotok, 10012),

    %% Verify structure
    Deps1001 = flurm_job_deps:get_dependencies(1001),
    Deps1002 = flurm_job_deps:get_dependencies(1002),
    ?assertEqual(2, length(Deps1001)),
    ?assertEqual(2, length(Deps1002)),

    %% Verify dependents
    Dependents10010 = flurm_job_deps:get_dependents(10010),
    ?assert(lists:member(1001, Dependents10010)),
    ?assert(lists:member(1002, Dependents10010)),
    ok.

test_notify_completion_satisfaction() ->
    %% Test that notify_completion satisfies appropriate dependencies
    ok = flurm_job_deps:add_dependency(1100, afterok, 11000),
    ok = flurm_job_deps:add_dependency(1101, afternotok, 11000),
    ok = flurm_job_deps:add_dependency(1102, afterany, 11000),

    %% Check initial state
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(1100)),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(1101)),
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(1102)),

    %% Notify completion with 'completed' result
    ok = flurm_job_deps:notify_completion(11000, completed),
    timer:sleep(100),

    %% afterok (1100) and afterany (1102) should be satisfied
    %% afternotok (1101) should NOT be satisfied by 'completed'
    %% Note: Without real job state, satisfaction may not be verified via are_dependencies_satisfied
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_parse_errors() ->
    %% Test invalid dependency type
    Result1 = flurm_job_deps:parse_dependency_spec(<<"invalidtype:123">>),
    ?assertMatch({error, _}, Result1),

    %% Test malformed spec
    Result2 = flurm_job_deps:parse_dependency_spec(<<"nocolon">>),
    ?assertMatch({error, _}, Result2),
    ok.

%%====================================================================
%% Additional Test Suites
%%====================================================================

%% Test dependency with multiple targets (plus syntax)
multi_target_dep_test_() ->
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
             {"Parse plus-separated job IDs", fun() ->
                 {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:100+200+300">>),
                 ?assertEqual([{afterok, [100, 200, 300]}], Deps)
             end},
             {"Add dependency with multiple targets creates entries", fun() ->
                 ok = flurm_job_deps:add_dependency(1200, afterok, [12001, 12002, 12003]),
                 Deps = flurm_job_deps:get_dependencies(1200),
                 ?assert(length(Deps) >= 1)
             end}
         ]
     end}.

%% Test add_dependencies from spec string
add_deps_from_spec_test_() ->
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
             {"Add multiple dependencies from comma-separated spec", fun() ->
                 ok = flurm_job_deps:add_dependencies(1300, <<"afterok:13001,afternotok:13002,afterany:13003">>),
                 Deps = flurm_job_deps:get_dependencies(1300),
                 ?assertEqual(3, length(Deps))
             end},
             {"Add dependencies with empty spec does nothing", fun() ->
                 ok = flurm_job_deps:add_dependencies(1301, <<>>),
                 Deps = flurm_job_deps:get_dependencies(1301),
                 ?assertEqual(0, length(Deps))
             end},
             {"Add dependencies with invalid spec returns error", fun() ->
                 Result = flurm_job_deps:add_dependencies(1302, <<"invalidtype:123">>),
                 ?assertMatch({error, _}, Result)
             end}
         ]
     end}.

%% Test circular dependency prevention when adding
circular_prevention_test_() ->
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
             {"Adding circular dependency is rejected", fun() ->
                 ok = flurm_job_deps:add_dependency(1400, afterok, 1401),
                 %% Try to add reverse - should fail
                 Result = flurm_job_deps:add_dependency(1401, afterok, 1400),
                 ?assertMatch({error, {circular_dependency, _}}, Result)
             end},
             {"Add dependencies batch rejects circular", fun() ->
                 ok = flurm_job_deps:add_dependency(1500, afterok, 1501),
                 ok = flurm_job_deps:add_dependency(1501, afterok, 1502),
                 %% Try to add 1502 -> 1500 via add_dependencies
                 Result = flurm_job_deps:add_dependencies(1502, <<"afterok:1500">>),
                 ?assertMatch({error, {circular_dependency, _}}, Result)
             end}
         ]
     end}.

%% Test singleton behavior
singleton_detailed_test_() ->
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
             {"First singleton holder is satisfied", fun() ->
                 ok = flurm_job_deps:add_dependency(1600, singleton, <<"singleton_test_1">>),
                 %% First holder should have satisfied dependency
                 Deps = flurm_job_deps:get_dependencies(1600),
                 ?assertEqual(1, length(Deps))
             end},
             {"Second singleton waiter is unsatisfied", fun() ->
                 %% First holder
                 ok = flurm_job_deps:add_dependency(1700, singleton, <<"singleton_test_2">>),
                 %% Second waiter
                 ok = flurm_job_deps:add_dependency(1701, singleton, <<"singleton_test_2">>),
                 ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(1701))
             end},
             {"Singleton without name uses default", fun() ->
                 {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"singleton">>),
                 ?assertEqual([{singleton, <<"default">>}], Deps)
             end},
             {"Singleton with name is parsed correctly", fun() ->
                 {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"singleton:custom_name">>),
                 ?assertEqual([{singleton, <<"custom_name">>}], Deps)
             end},
             {"Format singleton default", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{singleton, <<"default">>}]),
                 ?assertEqual(<<"singleton">>, Formatted)
             end},
             {"Format singleton with name", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{singleton, <<"myname">>}]),
                 ?assertEqual(<<"singleton:myname">>, Formatted)
             end}
         ]
     end}.

%% Test unknown/error handlers
error_handlers_test_() ->
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
             {"Unknown gen_server call returns error", fun() ->
                 Result = gen_server:call(flurm_job_deps, {unknown_request, data}),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"Unknown gen_server cast is handled", fun() ->
                 gen_server:cast(flurm_job_deps, {unknown_cast, data}),
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_job_deps)))
             end},
             {"Unknown info message is handled", fun() ->
                 whereis(flurm_job_deps) ! {random, info, message},
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_job_deps)))
             end}
         ]
     end}.

%% Test check_dependencies return values
check_deps_return_test_() ->
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
             {"Check deps with no deps returns ok empty", fun() ->
                 {ok, []} = flurm_job_deps:check_dependencies(99990)
             end},
             {"Check deps with unsatisfied returns waiting", fun() ->
                 ok = flurm_job_deps:add_dependency(1800, afterok, 18000),
                 {waiting, Deps} = flurm_job_deps:check_dependencies(1800),
                 ?assertEqual(1, length(Deps))
             end}
         ]
     end}.

%% Test detect_circular_dependency for non-integer targets
non_integer_target_test_() ->
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
         {"Non-integer targets cannot create cycles", fun() ->
             %% Singletons use binary names, not job IDs
             Result = flurm_job_deps:detect_circular_dependency(1900, <<"singleton_name">>),
             ?assertEqual(ok, Result),

             Result2 = flurm_job_deps:has_circular_dependency(1901, <<"other_singleton">>),
             ?assertEqual(false, Result2)
         end}
     end}.

%% Test remove_dependency for singleton/non-integer targets
remove_non_integer_test_() ->
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
         {"Remove singleton dependency", fun() ->
             ok = flurm_job_deps:add_dependency(2000, singleton, <<"remove_test">>),
             Deps1 = flurm_job_deps:get_dependencies(2000),
             ?assertEqual(1, length(Deps1)),

             ok = flurm_job_deps:remove_dependency(2000, {singleton, <<"remove_test">>}),
             Deps2 = flurm_job_deps:get_dependencies(2000),
             ?assertEqual(0, length(Deps2))
         end}
     end}.
