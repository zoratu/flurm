%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for flurm_job_deps TEST-exported Pure Functions
%%%
%%% Calls REAL functions directly (no mocking) to maximize rebar3
%%% cover results.  Every TEST-exported helper in flurm_job_deps is
%%% exercised here: parsing, formatting, dependency-type matching,
%%% state satisfaction checks, and terminal-state detection.
%%%
%%% Public pure functions (parse_dependency_spec/1,
%%% format_dependency_spec/1) are also tested.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_deps_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% parse_single_dep/1
%%====================================================================

parse_single_dep_test_() ->
    {"parse_single_dep/1", [
        {"afterok:123",
         fun() ->
             ?assertEqual({afterok, 123},
                          flurm_job_deps:parse_single_dep(<<"afterok:123">>))
         end},

        {"after:456",
         fun() ->
             ?assertEqual({after_start, 456},
                          flurm_job_deps:parse_single_dep(<<"after:456">>))
         end},

        {"afternotok:789",
         fun() ->
             ?assertEqual({afternotok, 789},
                          flurm_job_deps:parse_single_dep(<<"afternotok:789">>))
         end},

        {"afterany:100",
         fun() ->
             ?assertEqual({afterany, 100},
                          flurm_job_deps:parse_single_dep(<<"afterany:100">>))
         end},

        {"aftercorr:200",
         fun() ->
             ?assertEqual({aftercorr, 200},
                          flurm_job_deps:parse_single_dep(<<"aftercorr:200">>))
         end},

        {"afterburstbuffer:300",
         fun() ->
             ?assertEqual({afterburstbuffer, 300},
                          flurm_job_deps:parse_single_dep(<<"afterburstbuffer:300">>))
         end},

        {"singleton (bare keyword)",
         fun() ->
             ?assertEqual({singleton, <<"default">>},
                          flurm_job_deps:parse_single_dep(<<"singleton">>))
         end},

        {"singleton:myname (named singleton)",
         fun() ->
             ?assertEqual({singleton, <<"myname">>},
                          flurm_job_deps:parse_single_dep(<<"singleton:myname">>))
         end},

        {"multiple targets afterok:1+2+3",
         fun() ->
             ?assertEqual({afterok, [1, 2, 3]},
                          flurm_job_deps:parse_single_dep(<<"afterok:1+2+3">>))
         end},

        {"invalid spec throws parse_error",
         fun() ->
             ?assertThrow({parse_error, {invalid_dependency, <<"bad_no_colon">>}},
                          flurm_job_deps:parse_single_dep(<<"bad_no_colon">>))
         end}
    ]}.

%%====================================================================
%% parse_dep_type/1
%%====================================================================

parse_dep_type_test_() ->
    {"parse_dep_type/1", [
        {"after",
         fun() -> ?assertEqual(after_start, flurm_job_deps:parse_dep_type(<<"after">>)) end},

        {"afterok",
         fun() -> ?assertEqual(afterok, flurm_job_deps:parse_dep_type(<<"afterok">>)) end},

        {"afternotok",
         fun() -> ?assertEqual(afternotok, flurm_job_deps:parse_dep_type(<<"afternotok">>)) end},

        {"afterany",
         fun() -> ?assertEqual(afterany, flurm_job_deps:parse_dep_type(<<"afterany">>)) end},

        {"aftercorr",
         fun() -> ?assertEqual(aftercorr, flurm_job_deps:parse_dep_type(<<"aftercorr">>)) end},

        {"afterburstbuffer",
         fun() -> ?assertEqual(afterburstbuffer,
                               flurm_job_deps:parse_dep_type(<<"afterburstbuffer">>)) end},

        {"unknown type throws parse_error",
         fun() ->
             ?assertThrow({parse_error, {unknown_dep_type, <<"bogus">>}},
                          flurm_job_deps:parse_dep_type(<<"bogus">>))
         end}
    ]}.

%%====================================================================
%% parse_target/1
%%====================================================================

parse_target_test_() ->
    {"parse_target/1", [
        {"single integer target",
         fun() -> ?assertEqual(42, flurm_job_deps:parse_target(<<"42">>)) end},

        {"multiple targets with +",
         fun() -> ?assertEqual([1, 2, 3], flurm_job_deps:parse_target(<<"1+2+3">>)) end},

        {"two targets",
         fun() -> ?assertEqual([10, 20], flurm_job_deps:parse_target(<<"10+20">>)) end}
    ]}.

%%====================================================================
%% format_single_dep/1
%%====================================================================

format_single_dep_test_() ->
    {"format_single_dep/1", [
        {"afterok:123",
         fun() ->
             ?assertEqual(<<"afterok:123">>,
                          flurm_job_deps:format_single_dep({afterok, 123}))
         end},

        {"after:456",
         fun() ->
             ?assertEqual(<<"after:456">>,
                          flurm_job_deps:format_single_dep({after_start, 456}))
         end},

        {"afternotok:789",
         fun() ->
             ?assertEqual(<<"afternotok:789">>,
                          flurm_job_deps:format_single_dep({afternotok, 789}))
         end},

        {"afterany:100",
         fun() ->
             ?assertEqual(<<"afterany:100">>,
                          flurm_job_deps:format_single_dep({afterany, 100}))
         end},

        {"aftercorr:200",
         fun() ->
             ?assertEqual(<<"aftercorr:200">>,
                          flurm_job_deps:format_single_dep({aftercorr, 200}))
         end},

        {"afterburstbuffer:300",
         fun() ->
             ?assertEqual(<<"afterburstbuffer:300">>,
                          flurm_job_deps:format_single_dep({afterburstbuffer, 300}))
         end},

        {"singleton (default name)",
         fun() ->
             ?assertEqual(<<"singleton">>,
                          flurm_job_deps:format_single_dep({singleton, <<"default">>}))
         end},

        {"singleton:myname",
         fun() ->
             ?assertEqual(<<"singleton:myname">>,
                          flurm_job_deps:format_single_dep({singleton, <<"myname">>}))
         end},

        {"multiple targets afterok:1+2+3",
         fun() ->
             ?assertEqual(<<"afterok:1+2+3">>,
                          flurm_job_deps:format_single_dep({afterok, [1, 2, 3]}))
         end}
    ]}.

%%====================================================================
%% format_dep_type/1
%%====================================================================

format_dep_type_test_() ->
    {"format_dep_type/1", [
        {"after_start -> after",
         fun() -> ?assertEqual(<<"after">>, flurm_job_deps:format_dep_type(after_start)) end},

        {"afterok -> afterok",
         fun() -> ?assertEqual(<<"afterok">>, flurm_job_deps:format_dep_type(afterok)) end},

        {"afternotok -> afternotok",
         fun() -> ?assertEqual(<<"afternotok">>, flurm_job_deps:format_dep_type(afternotok)) end},

        {"afterany -> afterany",
         fun() -> ?assertEqual(<<"afterany">>, flurm_job_deps:format_dep_type(afterany)) end},

        {"aftercorr -> aftercorr",
         fun() -> ?assertEqual(<<"aftercorr">>, flurm_job_deps:format_dep_type(aftercorr)) end},

        {"afterburstbuffer -> afterburstbuffer",
         fun() -> ?assertEqual(<<"afterburstbuffer">>,
                               flurm_job_deps:format_dep_type(afterburstbuffer)) end}
    ]}.

%%====================================================================
%% state_satisfies/2
%%====================================================================

state_satisfies_test_() ->
    {"state_satisfies/2", [
        %% after_start: satisfied when state is anything except pending
        {"after_start satisfied by running",
         fun() -> ?assert(flurm_job_deps:state_satisfies(after_start, running)) end},

        {"after_start satisfied by completed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(after_start, completed)) end},

        {"after_start NOT satisfied by pending",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(after_start, pending)) end},

        %% afterok: only completed
        {"afterok satisfied by completed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afterok, completed)) end},

        {"afterok NOT satisfied by failed",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afterok, failed)) end},

        {"afterok NOT satisfied by running",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afterok, running)) end},

        {"afterok NOT satisfied by cancelled",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afterok, cancelled)) end},

        %% afternotok: failed, cancelled, or timeout
        {"afternotok satisfied by failed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afternotok, failed)) end},

        {"afternotok satisfied by cancelled",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afternotok, cancelled)) end},

        {"afternotok satisfied by timeout",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afternotok, timeout)) end},

        {"afternotok NOT satisfied by completed",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afternotok, completed)) end},

        {"afternotok NOT satisfied by running",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afternotok, running)) end},

        %% afterany: any terminal state
        {"afterany satisfied by completed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afterany, completed)) end},

        {"afterany satisfied by failed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afterany, failed)) end},

        {"afterany satisfied by cancelled",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afterany, cancelled)) end},

        {"afterany satisfied by timeout",
         fun() -> ?assert(flurm_job_deps:state_satisfies(afterany, timeout)) end},

        {"afterany NOT satisfied by running",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afterany, running)) end},

        {"afterany NOT satisfied by pending",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(afterany, pending)) end},

        %% aftercorr: same as afterany for now
        {"aftercorr satisfied by completed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(aftercorr, completed)) end},

        {"aftercorr satisfied by failed",
         fun() -> ?assert(flurm_job_deps:state_satisfies(aftercorr, failed)) end},

        {"aftercorr NOT satisfied by running",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(aftercorr, running)) end},

        %% catch-all clause returns false for unknown types
        {"unknown dep type returns false",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(singleton, running)) end},

        {"unknown dep type returns false for completed too",
         fun() -> ?assertNot(flurm_job_deps:state_satisfies(bogus_type, completed)) end}
    ]}.

%%====================================================================
%% is_terminal_state/1
%%====================================================================

is_terminal_state_test_() ->
    {"is_terminal_state/1", [
        {"completed is terminal",
         fun() -> ?assert(flurm_job_deps:is_terminal_state(completed)) end},

        {"failed is terminal",
         fun() -> ?assert(flurm_job_deps:is_terminal_state(failed)) end},

        {"cancelled is terminal",
         fun() -> ?assert(flurm_job_deps:is_terminal_state(cancelled)) end},

        {"timeout is terminal",
         fun() -> ?assert(flurm_job_deps:is_terminal_state(timeout)) end},

        {"running is NOT terminal",
         fun() -> ?assertNot(flurm_job_deps:is_terminal_state(running)) end},

        {"pending is NOT terminal",
         fun() -> ?assertNot(flurm_job_deps:is_terminal_state(pending)) end},

        {"held is NOT terminal",
         fun() -> ?assertNot(flurm_job_deps:is_terminal_state(held)) end},

        {"suspended is NOT terminal",
         fun() -> ?assertNot(flurm_job_deps:is_terminal_state(suspended)) end},

        {"configuring is NOT terminal",
         fun() -> ?assertNot(flurm_job_deps:is_terminal_state(configuring)) end}
    ]}.

%%====================================================================
%% find_path/3 (requires ETS tables)
%%====================================================================

find_path_setup() ->
    catch ets:delete(flurm_dep_graph),
    ets:new(flurm_dep_graph, [named_table, public, set]).

find_path_cleanup() ->
    catch ets:delete(flurm_dep_graph).

find_path_test_() ->
    {"find_path/3", [
        {"trivial path: start == end",
         fun() ->
             find_path_setup(),
             ?assertEqual({ok, [42]},
                          flurm_job_deps:find_path(42, 42, sets:new())),
             find_path_cleanup()
         end},

        {"direct path: 1 -> 2",
         fun() ->
             find_path_setup(),
             ets:insert(flurm_dep_graph, {1, [2]}),
             ?assertEqual({ok, [1, 2]},
                          flurm_job_deps:find_path(1, 2, sets:new())),
             find_path_cleanup()
         end},

        {"transitive path: 1 -> 2 -> 3",
         fun() ->
             find_path_setup(),
             ets:insert(flurm_dep_graph, {1, [2]}),
             ets:insert(flurm_dep_graph, {2, [3]}),
             {ok, Path} = flurm_job_deps:find_path(1, 3, sets:new()),
             ?assertEqual([1, 2, 3], Path),
             find_path_cleanup()
         end},

        {"no path returns not_found",
         fun() ->
             find_path_setup(),
             ets:insert(flurm_dep_graph, {1, [2]}),
             ?assertEqual(not_found,
                          flurm_job_deps:find_path(1, 99, sets:new())),
             find_path_cleanup()
         end},

        {"visited set prevents infinite loop",
         fun() ->
             find_path_setup(),
             %% Create a cycle: 1 -> 2 -> 1, but searching 1 -> 99
             ets:insert(flurm_dep_graph, {1, [2]}),
             ets:insert(flurm_dep_graph, {2, [1]}),
             ?assertEqual(not_found,
                          flurm_job_deps:find_path(1, 99, sets:new())),
             find_path_cleanup()
         end},

        {"empty graph returns not_found for different nodes",
         fun() ->
             find_path_setup(),
             ?assertEqual(not_found,
                          flurm_job_deps:find_path(1, 2, sets:new())),
             find_path_cleanup()
         end}
    ]}.

%%====================================================================
%% parse_dependency_spec/1 (public API, pure)
%%====================================================================

parse_dependency_spec_test_() ->
    {"parse_dependency_spec/1", [
        {"empty string",
         fun() ->
             ?assertEqual({ok, []},
                          flurm_job_deps:parse_dependency_spec(<<>>))
         end},

        {"single afterok",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:123">>),
             ?assertEqual([{afterok, 123}], Deps)
         end},

        {"multiple deps comma-separated",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(
                             <<"afterok:123,afterany:456,singleton">>),
             ?assertEqual(3, length(Deps)),
             ?assertEqual({afterok, 123},          lists:nth(1, Deps)),
             ?assertEqual({afterany, 456},         lists:nth(2, Deps)),
             ?assertEqual({singleton, <<"default">>}, lists:nth(3, Deps))
         end},

        {"after:10,afternotok:20",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(
                             <<"after:10,afternotok:20">>),
             ?assertEqual([{after_start, 10}, {afternotok, 20}], Deps)
         end},

        {"invalid dep returns error",
         fun() ->
             {error, _Reason} = flurm_job_deps:parse_dependency_spec(
                                  <<"badtype:123">>),
             ok
         end},

        {"singleton:jobname",
         fun() ->
             {ok, [{singleton, <<"jobname">>}]} =
                 flurm_job_deps:parse_dependency_spec(<<"singleton:jobname">>)
         end},

        {"aftercorr:50",
         fun() ->
             {ok, [{aftercorr, 50}]} =
                 flurm_job_deps:parse_dependency_spec(<<"aftercorr:50">>)
         end},

        {"afterburstbuffer:60",
         fun() ->
             {ok, [{afterburstbuffer, 60}]} =
                 flurm_job_deps:parse_dependency_spec(<<"afterburstbuffer:60">>)
         end},

        {"multi-target: afterok:1+2+3",
         fun() ->
             {ok, [{afterok, [1, 2, 3]}]} =
                 flurm_job_deps:parse_dependency_spec(<<"afterok:1+2+3">>)
         end}
    ]}.

%%====================================================================
%% format_dependency_spec/1 (public API, pure)
%%====================================================================

format_dependency_spec_test_() ->
    {"format_dependency_spec/1", [
        {"empty list",
         fun() ->
             ?assertEqual(<<>>,
                          flurm_job_deps:format_dependency_spec([]))
         end},

        {"single dep",
         fun() ->
             ?assertEqual(<<"afterok:123">>,
                          flurm_job_deps:format_dependency_spec([{afterok, 123}]))
         end},

        {"multiple deps joined by comma",
         fun() ->
             Result = flurm_job_deps:format_dependency_spec(
                        [{afterok, 100}, {afterany, 200}, {singleton, <<"default">>}]),
             ?assertEqual(<<"afterok:100,afterany:200,singleton">>, Result)
         end},

        {"multi-target dep",
         fun() ->
             Result = flurm_job_deps:format_dependency_spec([{afterok, [1, 2, 3]}]),
             ?assertEqual(<<"afterok:1+2+3">>, Result)
         end},

        {"named singleton",
         fun() ->
             Result = flurm_job_deps:format_dependency_spec([{singleton, <<"myjob">>}]),
             ?assertEqual(<<"singleton:myjob">>, Result)
         end}
    ]}.

%%====================================================================
%% Round-trip: parse then format
%%====================================================================

roundtrip_test_() ->
    {"parse -> format round-trip", [
        {"afterok:123",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:123">>),
             ?assertEqual(<<"afterok:123">>,
                          flurm_job_deps:format_dependency_spec(Deps))
         end},

        {"singleton",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"singleton">>),
             ?assertEqual(<<"singleton">>,
                          flurm_job_deps:format_dependency_spec(Deps))
         end},

        {"complex multi-dep spec",
         fun() ->
             Spec = <<"after:1,afterok:2,afternotok:3,afterany:4,aftercorr:5,singleton">>,
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(Spec),
             Formatted = flurm_job_deps:format_dependency_spec(Deps),
             %% Re-parse formatted result and compare
             {ok, Deps2} = flurm_job_deps:parse_dependency_spec(Formatted),
             ?assertEqual(Deps, Deps2)
         end},

        {"empty string round-trip",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<>>),
             ?assertEqual(<<>>, flurm_job_deps:format_dependency_spec(Deps))
         end}
    ]}.

%%====================================================================
%% Edge cases and additional coverage
%%====================================================================

edge_cases_test_() ->
    {"edge cases", [
        {"after_start satisfied by all non-pending states",
         fun() ->
             NonPending = [running, completed, failed, cancelled,
                           timeout, held, configuring, suspended],
             lists:foreach(fun(St) ->
                 ?assert(flurm_job_deps:state_satisfies(after_start, St))
             end, NonPending)
         end},

        {"afterany NOT satisfied by non-terminal states",
         fun() ->
             NonTerminal = [running, pending, held, configuring, suspended],
             lists:foreach(fun(St) ->
                 ?assertNot(flurm_job_deps:state_satisfies(afterany, St))
             end, NonTerminal)
         end},

        {"aftercorr matches same states as afterany",
         fun() ->
             States = [completed, failed, cancelled, timeout,
                       running, pending, held],
             lists:foreach(fun(St) ->
                 ?assertEqual(
                     flurm_job_deps:state_satisfies(afterany, St),
                     flurm_job_deps:state_satisfies(aftercorr, St))
             end, States)
         end},

        {"format then parse preserves multi-target",
         fun() ->
             Dep = {afterok, [10, 20, 30]},
             Bin = flurm_job_deps:format_single_dep(Dep),
             Parsed = flurm_job_deps:parse_single_dep(Bin),
             ?assertEqual(Dep, Parsed)
         end},

        {"all dep types survive format->parse round-trip",
         fun() ->
             Types = [after_start, afterok, afternotok, afterany,
                      aftercorr, afterburstbuffer],
             lists:foreach(fun(T) ->
                 Dep = {T, 999},
                 Bin = flurm_job_deps:format_single_dep(Dep),
                 Parsed = flurm_job_deps:parse_single_dep(Bin),
                 ?assertEqual(Dep, Parsed)
             end, Types)
         end}
    ]}.
