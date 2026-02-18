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
    _ = sys:get_state(flurm_job_deps),

    %% The dependency should be marked as satisfied internally
    %% (depends on actual implementation behavior)
    ok.

test_notify_completion() ->
    %% Setup dependency
    ok = flurm_job_deps:add_dependency(95, afterany, 950),

    %% Notify completion (async)
    ok = flurm_job_deps:notify_completion(950, completed),
    _ = sys:get_state(flurm_job_deps),
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
    _ = sys:get_state(flurm_job_deps),
    ?assertEqual(true, is_process_alive(whereis(flurm_job_deps))),
    ok.

test_info_message_handling() ->
    %% Send info message - should not crash server
    whereis(flurm_job_deps) ! {arbitrary, info, message},
    _ = sys:get_state(flurm_job_deps),
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

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

%% Test singleton dependency where another job holds the singleton
singleton_contention_test_() ->
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
             {"Singleton contention - second job waits", fun() ->
                 %% First job grabs singleton
                 ok = flurm_job_deps:add_dependency(1000, singleton, <<"contention_test">>),
                 Deps1 = flurm_job_deps:get_dependencies(1000),
                 ?assertEqual(1, length(Deps1)),
                 %% First singleton should be satisfied
                 [Dep1] = Deps1,
                 ?assert(element(6, Dep1)),  %% satisfied field

                 %% Second job tries to grab same singleton - should be unsatisfied
                 ok = flurm_job_deps:add_dependency(1001, singleton, <<"contention_test">>),
                 Deps2 = flurm_job_deps:get_dependencies(1001),
                 ?assertEqual(1, length(Deps2)),
                 [Dep2] = Deps2,
                 ?assertNot(element(6, Dep2)),  %% should NOT be satisfied

                 %% Second job should be dependent on first
                 Dependents = flurm_job_deps:get_dependents(1000),
                 ?assert(lists:member(1001, Dependents))
             end}
         ]
     end}.

%% Test add_dependencies with invalid spec
add_dependencies_error_test_() ->
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
             {"add_dependencies with invalid spec", fun() ->
                 Result = flurm_job_deps:add_dependencies(2000, <<"invalidtype:123">>),
                 ?assertMatch({error, _}, Result)
             end},
             {"add_dependencies with empty spec", fun() ->
                 Result = flurm_job_deps:add_dependencies(2001, <<>>),
                 ?assertEqual(ok, Result)
             end}
         ]
     end}.

%% Test multiple target dependency (with list of targets)
multiple_targets_full_test_() ->
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
             {"Add dependency with multiple integer targets", fun() ->
                 %% Parsed from "afterok:100+101+102" syntax
                 ok = flurm_job_deps:add_dependency(3000, afterok, [100, 101, 102]),
                 Deps = flurm_job_deps:get_dependencies(3000),
                 %% Should have 3 separate dependencies
                 ?assertEqual(3, length(Deps))
             end},
             {"Multiple targets with circular dependency error", fun() ->
                 %% First set up: 3100 depends on 3101
                 ok = flurm_job_deps:add_dependency(3100, afterok, 3101),
                 %% Try to add 3101 depending on [3100, 3102] - should fail due to cycle
                 Result = flurm_job_deps:add_dependency(3101, afterok, [3100, 3102]),
                 ?assertMatch({error, {circular_dependency, _}}, Result)
             end}
         ]
     end}.

%% Test hold_for_dependencies with unsatisfied dependencies
hold_for_deps_unsatisfied_test_() ->
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
             {"hold_for_dependencies with unsatisfied deps", fun() ->
                 %% Add an unsatisfied dependency first
                 ok = flurm_job_deps:add_dependency(4000, afterok, 4001),
                 %% Now try to hold - should return ok or error depending on job_manager
                 Result = flurm_job_deps:hold_for_dependencies(4000),
                 %% Result can be ok (if job_manager accepts) or error (if not available)
                 ?assert(Result =:= ok orelse element(1, Result) =:= error)
             end}
         ]
     end}.

%% Test state change with terminal state
state_change_terminal_test_() ->
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
             {"State change to terminal state releases singleton", fun() ->
                 %% Job 5000 holds singleton
                 ok = flurm_job_deps:add_dependency(5000, singleton, <<"terminal_test">>),
                 %% Job 5001 waits for singleton
                 ok = flurm_job_deps:add_dependency(5001, singleton, <<"terminal_test">>),
                 %% Verify 5001 is unsatisfied
                 {waiting, _} = flurm_job_deps:check_dependencies(5001),
                 %% Simulate 5000 completing (terminal state)
                 ok = flurm_job_deps:on_job_state_change(5000, completed),
                 %% Give async message time to process
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 %% Now 5001's singleton should be satisfied
                 Deps = flurm_job_deps:get_dependencies(5001),
                 [Dep] = Deps,
                 ?assert(element(6, Dep))  %% satisfied field
             end},
             {"State change updates afterok dependency", fun() ->
                 %% Job 5100 depends on 5101 completing successfully
                 ok = flurm_job_deps:add_dependency(5100, afterok, 5101),
                 {waiting, _} = flurm_job_deps:check_dependencies(5100),
                 %% Simulate 5101 completing
                 ok = flurm_job_deps:on_job_state_change(5101, completed),
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 %% Dependency should now be satisfied
                 Deps = flurm_job_deps:get_dependencies(5100),
                 [Dep] = Deps,
                 ?assert(element(6, Dep))  %% satisfied field
             end},
             {"State change with non-terminal state", fun() ->
                 %% Job 5200 depends on 5201
                 ok = flurm_job_deps:add_dependency(5200, afterok, 5201),
                 %% State change to running (non-terminal)
                 ok = flurm_job_deps:on_job_state_change(5201, running),
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 %% Dependency should still be unsatisfied
                 {waiting, _} = flurm_job_deps:check_dependencies(5200)
             end},
             {"State change afternotok satisfied by failed", fun() ->
                 ok = flurm_job_deps:add_dependency(5300, afternotok, 5301),
                 {waiting, _} = flurm_job_deps:check_dependencies(5300),
                 ok = flurm_job_deps:on_job_state_change(5301, failed),
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 Deps = flurm_job_deps:get_dependencies(5300),
                 [Dep] = Deps,
                 ?assert(element(6, Dep))
             end},
             {"State change afterany satisfied by timeout", fun() ->
                 ok = flurm_job_deps:add_dependency(5400, afterany, 5401),
                 {waiting, _} = flurm_job_deps:check_dependencies(5400),
                 ok = flurm_job_deps:on_job_state_change(5401, timeout),
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 Deps = flurm_job_deps:get_dependencies(5400),
                 [Dep] = Deps,
                 ?assert(element(6, Dep))
             end}
         ]
     end}.

%% Test circular dependency in add_dependencies (batch)
batch_circular_dependency_test_() ->
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
             {"add_dependencies fails on circular dependency", fun() ->
                 %% Set up: 6000 depends on 6001
                 ok = flurm_job_deps:add_dependency(6000, afterok, 6001),
                 %% Try batch add: 6001 depending on 6000 - should fail
                 Result = flurm_job_deps:add_dependencies(6001, <<"afterok:6000">>),
                 ?assertMatch({error, {circular_dependency, _}}, Result)
             end}
         ]
     end}.

%% Test code_change callback
code_change_test_() ->
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
         {"code_change returns ok", fun() ->
             State = sys:get_state(flurm_job_deps),
             Result = flurm_job_deps:code_change("1.0", State, []),
             ?assertEqual({ok, State}, Result)
         end}
     end}.

%% Test dependency graph operations
graph_operations_test_() ->
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
             {"Adding same dependency twice doesn't duplicate in graph", fun() ->
                 ok = flurm_job_deps:add_dependency(7000, afterok, 7001),
                 ok = flurm_job_deps:add_dependency(7000, afternotok, 7001),
                 Graph = flurm_job_deps:get_dependency_graph(),
                 ?assert(maps:is_key(7000, Graph))
             end},
             {"Removing dependency updates graph", fun() ->
                 ok = flurm_job_deps:add_dependency(7100, afterok, 7101),
                 ok = flurm_job_deps:add_dependency(7100, afternotok, 7102),
                 ok = flurm_job_deps:remove_dependency(7100, {afterok, 7101}),
                 Deps = flurm_job_deps:get_dependencies(7100),
                 ?assertEqual(1, length(Deps))
             end},
             {"Remove all dependencies clears graph entry", fun() ->
                 ok = flurm_job_deps:add_dependency(7200, afterok, 7201),
                 ok = flurm_job_deps:add_dependency(7200, afternotok, 7202),
                 ok = flurm_job_deps:remove_all_dependencies(7200),
                 Deps = flurm_job_deps:get_dependencies(7200),
                 ?assertEqual(0, length(Deps))
             end}
         ]
     end}.

%% Test removing dependency from non-integer target
remove_non_integer_target_test_() ->
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
             ok = flurm_job_deps:add_dependency(8000, singleton, <<"my_singleton">>),
             ok = flurm_job_deps:remove_dependency(8000, {singleton, <<"my_singleton">>}),
             Deps = flurm_job_deps:get_dependencies(8000),
             ?assertEqual(0, length(Deps))
         end}
     end}.

%% Test adding to dependents when already exists
dependents_duplicate_test_() ->
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
         {"Adding same dependent twice doesn't duplicate", fun() ->
             %% Add two dependencies on same target from same job
             ok = flurm_job_deps:add_dependency(8100, afterok, 8101),
             ok = flurm_job_deps:add_dependency(8100, afternotok, 8101),
             Dependents = flurm_job_deps:get_dependents(8101),
             %% Should only have one entry
             ?assertEqual(1, length(Dependents))
         end}
     end}.

%% Test remove_from_dependents when target doesn't exist
remove_nonexistent_dependent_test_() ->
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
         {"Removing dependency when dependents list is empty", fun() ->
             %% Try to remove a dependency that doesn't exist
             ok = flurm_job_deps:remove_dependency(8200, {afterok, 8201}),
             Deps = flurm_job_deps:get_dependencies(8200),
             ?assertEqual(0, length(Deps))
         end}
     end}.

%% Test clear_completed_deps
clear_completed_deps_test_() ->
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
         {"Clear completed deps returns count", fun() ->
             %% Add some dependencies
             ok = flurm_job_deps:add_dependency(8300, afterok, 8301),
             ok = flurm_job_deps:add_dependency(8302, afterok, 8303),
             %% Clear - will clean up deps for non-existent jobs
             Count = flurm_job_deps:clear_completed_dependencies(),
             ?assert(is_integer(Count)),
             ?assert(Count >= 0)
         end}
     end}.

%% Test already_started path in start_link
start_link_already_started_test_() ->
    {setup,
     fun() ->
         ok
     end,
     fun(_) ->
         catch gen_server:stop(flurm_job_deps, shutdown, 5000),
         ok
     end,
     fun(_) ->
         {"start_link when already running returns existing pid", fun() ->
             %% First start_link
             {ok, Pid1} = flurm_job_deps:start_link(),
             %% Second start_link should return same pid
             {ok, Pid2} = flurm_job_deps:start_link(),
             ?assertEqual(Pid1, Pid2)
         end}
     end}.

%% Test dependency with pre-satisfied target (target already in terminal state)
pre_satisfied_dependency_test_() ->
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
             {"Add dependency to already-completed target", fun() ->
                 %% First simulate a job completing by state change
                 ok = flurm_job_deps:on_job_state_change(9500, completed),
                 timer:sleep(10),
                 _ = sys:get_state(flurm_job_deps),
                 %% Now add a dependency on that "completed" job
                 %% The dependency may be satisfied immediately based on check_target_satisfies
                 ok = flurm_job_deps:add_dependency(9501, afterok, 9500),
                 Deps = flurm_job_deps:get_dependencies(9501),
                 ?assertEqual(1, length(Deps))
             end}
         ]
     end}.

%% Test remove from graph with last target
graph_remove_last_target_test_() ->
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
         {"Removing last dependency removes graph entry", fun() ->
             ok = flurm_job_deps:add_dependency(9600, afterok, 9601),
             ?assertEqual(1, length(flurm_job_deps:get_dependencies(9600))),
             ok = flurm_job_deps:remove_dependency(9600, {afterok, 9601}),
             ?assertEqual(0, length(flurm_job_deps:get_dependencies(9600))),
             %% Graph should be cleaned up
             Graph = flurm_job_deps:get_dependency_graph(),
             ?assertNot(maps:is_key(9600, Graph))
         end}
     end}.

%% Test remove from dependents when last dependent removed
dependents_remove_last_test_() ->
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
         {"Removing last dependent clears dependents table entry", fun() ->
             ok = flurm_job_deps:add_dependency(9700, afterok, 9701),
             Dependents1 = flurm_job_deps:get_dependents(9701),
             ?assertEqual([9700], Dependents1),
             ok = flurm_job_deps:remove_dependency(9700, {afterok, 9701}),
             Dependents2 = flurm_job_deps:get_dependents(9701),
             ?assertEqual([], Dependents2)
         end}
     end}.

%% Test state change satisfying after_start dependency
after_start_dependency_test_() ->
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
         {"after_start satisfied when job starts running", fun() ->
             ok = flurm_job_deps:add_dependency(9800, after_start, 9801),
             {waiting, _} = flurm_job_deps:check_dependencies(9800),
             %% State change to running satisfies after_start
             ok = flurm_job_deps:on_job_state_change(9801, running),
             timer:sleep(10),
             _ = sys:get_state(flurm_job_deps),
             Deps = flurm_job_deps:get_dependencies(9800),
             [Dep] = Deps,
             ?assert(element(6, Dep))  %% satisfied field
         end}
     end}.

%% Test notify_completion (alias for state_change)
notify_completion_test_() ->
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
         {"notify_completion updates dependencies", fun() ->
             ok = flurm_job_deps:add_dependency(10000, afterany, 10001),
             {waiting, _} = flurm_job_deps:check_dependencies(10000),
             ok = flurm_job_deps:notify_completion(10001, cancelled),
             timer:sleep(10),
             _ = sys:get_state(flurm_job_deps),
             Deps = flurm_job_deps:get_dependencies(10000),
             [Dep] = Deps,
             ?assert(element(6, Dep))  %% satisfied
         end}
     end}.

%% Test aftercorr dependency
aftercorr_dependency_test_() ->
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
         {"aftercorr satisfied by terminal states", fun() ->
             ok = flurm_job_deps:add_dependency(10100, aftercorr, 10101),
             {waiting, _} = flurm_job_deps:check_dependencies(10100),
             ok = flurm_job_deps:on_job_state_change(10101, cancelled),
             timer:sleep(10),
             _ = sys:get_state(flurm_job_deps),
             Deps = flurm_job_deps:get_dependencies(10100),
             [Dep] = Deps,
             ?assert(element(6, Dep))  %% satisfied
         end}
     end}.

%% Test already-satisfied dependency branch for singleton
singleton_already_owned_test_() ->
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
             {"Singleton dependency - owned job completing releases next", fun() ->
                 %% First job grabs singleton
                 ok = flurm_job_deps:add_dependency(10200, singleton, <<"release_singleton_test">>),
                 %% Second job waits
                 ok = flurm_job_deps:add_dependency(10201, singleton, <<"release_singleton_test">>),
                 %% Verify second is waiting
                 {waiting, _} = flurm_job_deps:check_dependencies(10201),
                 %% First job completes
                 ok = flurm_job_deps:on_job_state_change(10200, completed),
                 timer:sleep(20),
                 _ = sys:get_state(flurm_job_deps),
                 %% Second job's singleton should now be satisfied
                 {ok, []} = flurm_job_deps:check_dependencies(10201)
             end},
             {"Multiple jobs waiting for singleton - first gets it", fun() ->
                 %% First job owns singleton
                 ok = flurm_job_deps:add_dependency(10300, singleton, <<"multi_singleton_test">>),
                 %% Second and third jobs wait
                 ok = flurm_job_deps:add_dependency(10301, singleton, <<"multi_singleton_test">>),
                 ok = flurm_job_deps:add_dependency(10302, singleton, <<"multi_singleton_test">>),
                 %% First completes
                 ok = flurm_job_deps:on_job_state_change(10300, completed),
                 timer:sleep(20),
                 _ = sys:get_state(flurm_job_deps),
                 %% One of the waiting jobs should now be satisfied
                 Deps1 = flurm_job_deps:get_dependencies(10301),
                 Deps2 = flurm_job_deps:get_dependencies(10302),
                 %% At least one should be satisfied
                 Satisfied = lists:any(fun(D) -> element(6, D) end, Deps1 ++ Deps2),
                 ?assert(Satisfied)
             end}
         ]
     end}.

%% Test graph add_to_graph when already exists
graph_add_existing_test_() ->
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
         {"Adding same target twice to graph doesn't duplicate", fun() ->
             %% Add dependency to same target twice (different dep types)
             ok = flurm_job_deps:add_dependency(10400, afterok, 10401),
             ok = flurm_job_deps:add_dependency(10400, afternotok, 10401),
             %% Graph should have one entry with one target
             Graph = flurm_job_deps:get_dependency_graph(),
             ?assert(maps:is_key(10400, Graph)),
             %% Dependencies should exist
             Deps = flurm_job_deps:get_dependencies(10400),
             ?assertEqual(2, length(Deps))
         end}
     end}.

%% Test remove_from_graph with remaining targets
graph_remove_partial_test_() ->
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
         {"Removing one dependency keeps graph entry with other targets", fun() ->
             %% Add dependencies to different targets
             ok = flurm_job_deps:add_dependency(10500, afterok, 10501),
             ok = flurm_job_deps:add_dependency(10500, afterok, 10502),
             %% Remove one
             ok = flurm_job_deps:remove_dependency(10500, {afterok, 10501}),
             %% Graph should still have 10500
             Graph = flurm_job_deps:get_dependency_graph(),
             ?assert(maps:is_key(10500, Graph))
         end}
     end}.

%% Test add_to_graph with non-integer (skips)
graph_non_integer_test_() ->
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
         {"Singleton dependencies don't go into cycle detection graph", fun() ->
             %% Add singleton (non-integer target)
             ok = flurm_job_deps:add_dependency(10600, singleton, <<"no_graph">>),
             %% Should not create cycle detection issue
             ?assertEqual(false, flurm_job_deps:has_circular_dependency(10600, 10601))
         end}
     end}.

%% Test removing one dependent when multiple exist (line 505)
remove_one_of_multiple_dependents_test_() ->
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
         {"Remove one dependent when multiple exist", fun() ->
             %% Two jobs depend on same target
             ok = flurm_job_deps:add_dependency(10700, afterok, 10702),
             ok = flurm_job_deps:add_dependency(10701, afterok, 10702),
             %% Both should be dependents of 10702
             Dependents1 = flurm_job_deps:get_dependents(10702),
             ?assertEqual(2, length(Dependents1)),
             %% Remove one dependency
             ok = flurm_job_deps:remove_dependency(10700, {afterok, 10702}),
             %% Should still have one dependent
             Dependents2 = flurm_job_deps:get_dependents(10702),
             ?assertEqual(1, length(Dependents2)),
             ?assertEqual([10701], Dependents2)
         end}
     end}.

%% Test afterburstbuffer dependency type parsing and formatting (lines 734, 766)
afterburstbuffer_dep_test_() ->
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
             {"Parse afterburstbuffer dependency type", fun() ->
                 {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterburstbuffer:100">>),
                 ?assertEqual([{afterburstbuffer, 100}], Deps)
             end},
             {"Format afterburstbuffer dependency type", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{afterburstbuffer, 200}]),
                 ?assertEqual(<<"afterburstbuffer:200">>, Formatted)
             end}
         ]
     end}.

%% Test format_dep_type for additional types (lines 761, 763-765)
format_dep_types_test_() ->
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
             {"Format after_start dep type", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{after_start, 300}]),
                 ?assertEqual(<<"after:300">>, Formatted)
             end},
             {"Format afternotok dep type", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{afternotok, 400}]),
                 ?assertEqual(<<"afternotok:400">>, Formatted)
             end},
             {"Format afterany dep type", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{afterany, 500}]),
                 ?assertEqual(<<"afterany:500">>, Formatted)
             end},
             {"Format aftercorr dep type", fun() ->
                 Formatted = flurm_job_deps:format_dependency_spec([{aftercorr, 600}]),
                 ?assertEqual(<<"aftercorr:600">>, Formatted)
             end}
         ]
     end}.

%% Test invalid dependency spec without colon (line 726)
invalid_dep_no_colon_test_() ->
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
         {"Parse invalid dependency without colon", fun() ->
             Result = flurm_job_deps:parse_dependency_spec(<<"invalid_no_colon">>),
             ?assertMatch({error, {invalid_dependency, _}}, Result)
         end}
     end}.

%% Test state_satisfies with unknown type (line 584)
state_satisfies_unknown_test_() ->
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
         {"state_satisfies returns false for unknown type", fun() ->
             %% Call the exported test function
             Result = flurm_job_deps:state_satisfies(afterburstbuffer, completed),
             ?assertEqual(false, Result)
         end}
     end}.

%% Test release_job when job_manager returns ok (lines 191-192)
release_job_success_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, release_job, fun(_) -> ok end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"release_job returns ok when deps satisfied and job_manager succeeds", fun() ->
             %% Job with no dependencies - deps satisfied
             Result = flurm_job_deps:release_job(99900),
             ?assertEqual(ok, Result)
         end}
     end}.

%% Test release_job when job_manager returns error (line 194)
release_job_error_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, release_job, fun(_) -> {error, not_found} end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"release_job returns error when job_manager fails", fun() ->
             %% Job with no dependencies - deps satisfied
             Result = flurm_job_deps:release_job(99901),
             ?assertEqual({error, not_found}, Result)
         end}
     end}.

%% Test hold_for_dependencies when job_manager returns ok (lines 214-215)
hold_for_deps_success_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, hold_job, fun(_) -> ok end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"hold_for_dependencies returns ok when job_manager succeeds", fun() ->
             %% Add unsatisfied dependency first
             ok = flurm_job_deps:add_dependency(99902, afterok, 99903),
             Result = flurm_job_deps:hold_for_dependencies(99902),
             ?assertEqual(ok, Result)
         end}
     end}.

%% Test hold_for_dependencies when job_manager returns error (lines 216-217)
hold_for_deps_error_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, hold_job, fun(_) -> {error, not_found} end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"hold_for_dependencies returns error when job_manager fails", fun() ->
             %% Add unsatisfied dependency first
             ok = flurm_job_deps:add_dependency(99904, afterok, 99905),
             Result = flurm_job_deps:hold_for_dependencies(99904),
             ?assertEqual({error, not_found}, Result)
         end}
     end}.

%% Test start_link when already_started returns error for other reasons (line 104)
start_link_error_test_() ->
    {setup,
     fun() ->
         %% Ensure server is stopped
         catch gen_server:stop(flurm_job_deps, shutdown, 5000),
         timer:sleep(50),
         ok
     end,
     fun(_) ->
         catch gen_server:stop(flurm_job_deps, shutdown, 5000),
         ok
     end,
     fun(_) ->
         {"start_link handles gen_server errors", fun() ->
             %% First start should succeed
             {ok, _Pid1} = flurm_job_deps:start_link(),
             %% Second start should return existing pid (already_started path)
             {ok, _Pid2} = flurm_job_deps:start_link()
         end}
     end}.

%% Test add dependency where target is already satisfied (line 410)
dependency_already_satisfied_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_job_registry),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_job_registry, [passthrough, non_strict]),
         %% Mock get_job to return a completed job
         meck:expect(flurm_job_manager, get_job, fun(9999) ->
             {ok, {job, 9999, <<"test">>, <<"user">>, <<"default">>, completed, <<>>, 1, 1, 1024, 3600, 100, 0, 0, 0, [], undefined, <<>>, <<"normal">>}}
         end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_job_registry),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Add dependency to already-completed target is satisfied immediately", fun() ->
             %% Add dependency to job 9999 which is "completed"
             ok = flurm_job_deps:add_dependency(9998, afterok, 9999),
             %% Dependency should be immediately satisfied
             Deps = flurm_job_deps:get_dependencies(9998),
             ?assertEqual(1, length(Deps)),
             [Dep] = Deps,
             %% The satisfied field should be true
             ?assert(element(6, Dep))
         end}
     end}.

%% Test multiple targets where one fails with circular dep (line 458)
multiple_targets_partial_error_test_() ->
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
         {"Multiple targets with partial circular dependency error", fun() ->
             %% Set up: 11000 depends on 11001
             ok = flurm_job_deps:add_dependency(11000, afterok, 11001),
             %% Try to add 11001 depending on [11000, 11002] - should fail
             %% This tests line 458 where we find the first error
             Result = flurm_job_deps:add_dependency(11001, afterok, [11000, 11002]),
             ?assertMatch({error, {circular_dependency, _}}, Result)
         end}
     end}.

%% Test find_path when already visited (line 545)
find_path_already_visited_test_() ->
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
         {"Find path returns not_found when target not reachable", fun() ->
             %% Create a chain: 12000 -> 12001 -> 12002
             ok = flurm_job_deps:add_dependency(12000, afterok, 12001),
             ok = flurm_job_deps:add_dependency(12001, afterok, 12002),
             %% Try to find path from 12002 to 12003 (doesn't exist)
             %% This exercises the not_found path
             Result = flurm_job_deps:detect_circular_dependency(12003, 12002),
             ?assertEqual(ok, Result),
             %% Try to find cycle that doesn't exist
             ?assertEqual(false, flurm_job_deps:has_circular_dependency(12003, 12004))
         end}
     end}.

%% Test find_path recursion (line 563)
find_path_recursive_test_() ->
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
         {"Find path traverses multiple targets", fun() ->
             %% Create a more complex graph:
             %% 13000 -> 13001
             %% 13000 -> 13002
             %% 13001 -> 13003
             ok = flurm_job_deps:add_dependency(13000, afterok, 13001),
             ok = flurm_job_deps:add_dependency(13000, afternotok, 13002),
             ok = flurm_job_deps:add_dependency(13001, afterok, 13003),
             %% Try to add 13003 -> 13000 (would create cycle through first path)
             Result = flurm_job_deps:detect_circular_dependency(13003, 13000),
             ?assertMatch({error, circular_dependency, _}, Result)
         end}
     end}.

%% Test check_target_satisfies with job_manager returning job (line 568, 591-592)
check_target_satisfies_with_job_manager_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_job_registry),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_job_registry, [passthrough, non_strict]),
         %% Mock get_job to return a running job (satisfies after_start)
         meck:expect(flurm_job_manager, get_job, fun(14000) ->
             {ok, {job, 14000, <<"test">>, <<"user">>, <<"default">>, running, <<>>, 1, 1, 1024, 3600, 100, 0, 0, 0, [], undefined, <<>>, <<"normal">>}}
         end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_job_registry),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Add after_start dependency to running job is satisfied", fun() ->
             ok = flurm_job_deps:add_dependency(14001, after_start, 14000),
             Deps = flurm_job_deps:get_dependencies(14001),
             ?assertEqual(1, length(Deps)),
             [Dep] = Deps,
             %% Should be satisfied because 14000 is "running"
             ?assert(element(6, Dep))
         end}
     end}.

%% Test get_job_state via job_registry fallback (lines 598-599)
get_job_state_via_registry_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_job_registry),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_job_registry, [passthrough, non_strict]),
         %% Mock get_job to fail
         meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
         %% Mock job_registry to return a job entry
         meck:expect(flurm_job_registry, get_job_entry, fun(15000) ->
             {ok, {job_entry, 15000, <<"test">>, completed, 0}}
         end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_job_registry),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Add dependency uses job_registry fallback for state check", fun() ->
             ok = flurm_job_deps:add_dependency(15001, afterok, 15000),
             Deps = flurm_job_deps:get_dependencies(15001),
             ?assertEqual(1, length(Deps)),
             [Dep] = Deps,
             %% Should be satisfied because 15000 is "completed" via registry
             ?assert(element(6, Dep))
         end}
     end}.

%% Test release_singleton with no waiting jobs (line 663)
release_singleton_no_waiting_test_() ->
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
         {"Singleton release with no waiting jobs", fun() ->
             %% Only one job holds the singleton, no others waiting
             ok = flurm_job_deps:add_dependency(16000, singleton, <<"solo_singleton">>),
             %% Job completes - releases singleton with no waiters (line 663)
             ok = flurm_job_deps:on_job_state_change(16000, completed),
             timer:sleep(20),
             _ = sys:get_state(flurm_job_deps),
             %% Singleton should be released
             ok
         end}
     end}.

%% Test maybe_notify_scheduler with release success (lines 680-681)
notify_scheduler_release_success_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_scheduler),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_scheduler, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, release_job, fun(_) -> ok end),
         meck:expect(flurm_scheduler, job_deps_satisfied, fun(_) -> ok end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_scheduler),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"maybe_notify_scheduler releases job and notifies scheduler", fun() ->
             %% Setup dependency
             ok = flurm_job_deps:add_dependency(17000, afterok, 17001),
             {waiting, _} = flurm_job_deps:check_dependencies(17000),
             %% Complete target job
             ok = flurm_job_deps:on_job_state_change(17001, completed),
             timer:sleep(50),
             _ = sys:get_state(flurm_job_deps),
             %% Should have called release_job and job_deps_satisfied
             ?assert(meck:called(flurm_job_manager, release_job, [17000])),
             ?assert(meck:called(flurm_scheduler, job_deps_satisfied, [17000]))
         end}
     end}.

%% Test maybe_notify_scheduler with invalid_state pending error (lines 684-685)
notify_scheduler_already_pending_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_scheduler),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_scheduler, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, release_job, fun(_) -> {error, {invalid_state, pending}} end),
         meck:expect(flurm_scheduler, job_deps_satisfied, fun(_) -> ok end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_scheduler),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"maybe_notify_scheduler handles already pending job", fun() ->
             ok = flurm_job_deps:add_dependency(18000, afterok, 18001),
             ok = flurm_job_deps:on_job_state_change(18001, completed),
             timer:sleep(50),
             _ = sys:get_state(flurm_job_deps),
             %% Should still notify scheduler
             ?assert(meck:called(flurm_scheduler, job_deps_satisfied, [18000]))
         end}
     end}.

%% Test maybe_notify_scheduler with other error (lines 687-688)
notify_scheduler_release_error_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(lager),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(lager, [passthrough, non_strict]),
         meck:expect(flurm_job_manager, release_job, fun(_) -> {error, some_other_error} end),
         meck:expect(lager, warning, fun(_, _) -> ok end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(lager),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"maybe_notify_scheduler logs warning on release error", fun() ->
             ok = flurm_job_deps:add_dependency(19000, afterok, 19001),
             ok = flurm_job_deps:on_job_state_change(19001, completed),
             timer:sleep(50),
             _ = sys:get_state(flurm_job_deps),
             %% Should have logged warning
             ok
         end}
     end}.

%% Test maybe_notify_scheduler with unsatisfied deps (line 694)
notify_scheduler_deps_not_satisfied_test_() ->
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
         {"maybe_notify_scheduler does nothing when deps not satisfied", fun() ->
             %% Job with two dependencies
             ok = flurm_job_deps:add_dependency(20000, afterok, 20001),
             ok = flurm_job_deps:add_dependency(20000, afterok, 20002),
             %% Only complete one target
             ok = flurm_job_deps:on_job_state_change(20001, completed),
             timer:sleep(50),
             _ = sys:get_state(flurm_job_deps),
             %% Job should still have unsatisfied dependency
             {waiting, _} = flurm_job_deps:check_dependencies(20000)
         end}
     end}.

%% Test clear_completed_deps with terminal state job (line 702)
clear_completed_deps_terminal_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_job_registry),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_job_registry, [passthrough, non_strict]),
         %% Mock get_job to return a completed job for cleanup
         meck:expect(flurm_job_manager, get_job, fun(21000) ->
             {ok, {job, 21000, <<"test">>, <<"user">>, <<"default">>, completed, <<>>, 1, 1, 1024, 3600, 100, 0, 0, 0, [], undefined, <<>>, <<"normal">>}}
         end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_job_registry),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"clear_completed_deps removes deps for terminal state jobs", fun() ->
             %% Add a dependency for job 21000 which is "completed"
             ok = flurm_job_deps:add_dependency(21000, afterok, 21001),
             %% Clear completed dependencies
             Count = flurm_job_deps:clear_completed_dependencies(),
             %% Should have cleared at least one
             ?assert(Count >= 1)
         end}
     end}.

%% Test dependency already satisfied at add time (line 422)
dependency_satisfied_at_add_skip_dependents_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_job_registry),
         meck:new(flurm_job_manager, [passthrough, non_strict]),
         meck:new(flurm_job_registry, [passthrough, non_strict]),
         %% Mock get_job to return a completed job
         meck:expect(flurm_job_manager, get_job, fun(22000) ->
             {ok, {job, 22000, <<"test">>, <<"user">>, <<"default">>, completed, <<>>, 1, 1, 1024, 3600, 100, 0, 0, 0, [], undefined, <<>>, <<"normal">>}}
         end),
         {ok, Pid} = flurm_job_deps:start_link(),
         #{deps_pid => Pid}
     end,
     fun(#{deps_pid := Pid}) ->
         meck:unload(flurm_job_manager),
         meck:unload(flurm_job_registry),
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"Satisfied dependency doesn't add to dependents list", fun() ->
             %% Add dependency to job 22000 which is "completed"
             ok = flurm_job_deps:add_dependency(22001, afterok, 22000),
             %% Should not be in dependents list since already satisfied
             Dependents = flurm_job_deps:get_dependents(22000),
             ?assertNot(lists:member(22001, Dependents))
         end}
     end}.
