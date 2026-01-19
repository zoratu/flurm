%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_deps internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure internal functions for dependency parsing,
%%% formatting, and state checking without requiring a running gen_server.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_deps_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% parse_single_dep/1 Tests
%%====================================================================

parse_single_dep_test_() ->
    {"parse_single_dep/1 tests", [
        {"parses singleton dependency",
         fun() ->
             ?assertEqual({singleton, <<"default">>},
                          flurm_job_deps:parse_single_dep(<<"singleton">>))
         end},

        {"parses named singleton dependency",
         fun() ->
             ?assertEqual({singleton, <<"myjob">>},
                          flurm_job_deps:parse_single_dep(<<"singleton:myjob">>))
         end},

        {"parses afterok dependency",
         fun() ->
             ?assertEqual({afterok, 123},
                          flurm_job_deps:parse_single_dep(<<"afterok:123">>))
         end},

        {"parses afterany dependency",
         fun() ->
             ?assertEqual({afterany, 456},
                          flurm_job_deps:parse_single_dep(<<"afterany:456">>))
         end},

        {"parses afternotok dependency",
         fun() ->
             ?assertEqual({afternotok, 789},
                          flurm_job_deps:parse_single_dep(<<"afternotok:789">>))
         end},

        {"parses after (after_start) dependency",
         fun() ->
             ?assertEqual({after_start, 100},
                          flurm_job_deps:parse_single_dep(<<"after:100">>))
         end},

        {"parses aftercorr dependency",
         fun() ->
             ?assertEqual({aftercorr, 200},
                          flurm_job_deps:parse_single_dep(<<"aftercorr:200">>))
         end},

        {"parses afterburstbuffer dependency",
         fun() ->
             ?assertEqual({afterburstbuffer, 300},
                          flurm_job_deps:parse_single_dep(<<"afterburstbuffer:300">>))
         end},

        {"parses multiple target dependency (job+job)",
         fun() ->
             ?assertEqual({afterok, [10, 20, 30]},
                          flurm_job_deps:parse_single_dep(<<"afterok:10+20+30">>))
         end},

        {"throws on invalid dependency type",
         fun() ->
             ?assertThrow({parse_error, {unknown_dep_type, <<"invalid">>}},
                          flurm_job_deps:parse_single_dep(<<"invalid:123">>))
         end},

        {"throws on malformed dependency",
         fun() ->
             ?assertThrow({parse_error, {invalid_dependency, <<"nocolon">>}},
                          flurm_job_deps:parse_single_dep(<<"nocolon">>))
         end}
    ]}.

%%====================================================================
%% parse_dep_type/1 Tests
%%====================================================================

parse_dep_type_test_() ->
    {"parse_dep_type/1 tests", [
        {"parses all valid dependency types",
         fun() ->
             ?assertEqual(after_start, flurm_job_deps:parse_dep_type(<<"after">>)),
             ?assertEqual(afterok, flurm_job_deps:parse_dep_type(<<"afterok">>)),
             ?assertEqual(afternotok, flurm_job_deps:parse_dep_type(<<"afternotok">>)),
             ?assertEqual(afterany, flurm_job_deps:parse_dep_type(<<"afterany">>)),
             ?assertEqual(aftercorr, flurm_job_deps:parse_dep_type(<<"aftercorr">>)),
             ?assertEqual(afterburstbuffer, flurm_job_deps:parse_dep_type(<<"afterburstbuffer">>))
         end},

        {"throws on unknown type",
         fun() ->
             ?assertThrow({parse_error, {unknown_dep_type, <<"unknown">>}},
                          flurm_job_deps:parse_dep_type(<<"unknown">>))
         end}
    ]}.

%%====================================================================
%% parse_target/1 Tests
%%====================================================================

parse_target_test_() ->
    {"parse_target/1 tests", [
        {"parses single job ID",
         fun() ->
             ?assertEqual(12345, flurm_job_deps:parse_target(<<"12345">>))
         end},

        {"parses multiple job IDs (job+job syntax)",
         fun() ->
             ?assertEqual([1, 2, 3], flurm_job_deps:parse_target(<<"1+2+3">>))
         end},

        {"parses two job IDs",
         fun() ->
             ?assertEqual([100, 200], flurm_job_deps:parse_target(<<"100+200">>))
         end}
    ]}.

%%====================================================================
%% format_single_dep/1 Tests
%%====================================================================

format_single_dep_test_() ->
    {"format_single_dep/1 tests", [
        {"formats default singleton",
         fun() ->
             ?assertEqual(<<"singleton">>,
                          flurm_job_deps:format_single_dep({singleton, <<"default">>}))
         end},

        {"formats named singleton",
         fun() ->
             ?assertEqual(<<"singleton:myjob">>,
                          flurm_job_deps:format_single_dep({singleton, <<"myjob">>}))
         end},

        {"formats afterok dependency",
         fun() ->
             ?assertEqual(<<"afterok:123">>,
                          flurm_job_deps:format_single_dep({afterok, 123}))
         end},

        {"formats afterany dependency",
         fun() ->
             ?assertEqual(<<"afterany:456">>,
                          flurm_job_deps:format_single_dep({afterany, 456}))
         end},

        {"formats after_start dependency",
         fun() ->
             ?assertEqual(<<"after:100">>,
                          flurm_job_deps:format_single_dep({after_start, 100}))
         end},

        {"formats multiple targets",
         fun() ->
             ?assertEqual(<<"afterok:1+2+3">>,
                          flurm_job_deps:format_single_dep({afterok, [1, 2, 3]}))
         end}
    ]}.

%%====================================================================
%% format_dep_type/1 Tests
%%====================================================================

format_dep_type_test_() ->
    {"format_dep_type/1 tests", [
        {"formats all dependency types",
         fun() ->
             ?assertEqual(<<"after">>, flurm_job_deps:format_dep_type(after_start)),
             ?assertEqual(<<"afterok">>, flurm_job_deps:format_dep_type(afterok)),
             ?assertEqual(<<"afternotok">>, flurm_job_deps:format_dep_type(afternotok)),
             ?assertEqual(<<"afterany">>, flurm_job_deps:format_dep_type(afterany)),
             ?assertEqual(<<"aftercorr">>, flurm_job_deps:format_dep_type(aftercorr)),
             ?assertEqual(<<"afterburstbuffer">>, flurm_job_deps:format_dep_type(afterburstbuffer))
         end}
    ]}.

%%====================================================================
%% state_satisfies/2 Tests
%%====================================================================

state_satisfies_test_() ->
    {"state_satisfies/2 tests", [
        {"after_start is satisfied when not pending",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(after_start, pending)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(after_start, running)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(after_start, completed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(after_start, failed))
         end},

        {"afterok is only satisfied by completed",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterok, pending)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterok, running)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afterok, completed)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterok, failed)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterok, cancelled)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterok, timeout))
         end},

        {"afternotok is satisfied by failed, cancelled, or timeout",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(afternotok, pending)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afternotok, running)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afternotok, completed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afternotok, failed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afternotok, cancelled)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afternotok, timeout))
         end},

        {"afterany is satisfied by any terminal state",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterany, pending)),
             ?assertEqual(false, flurm_job_deps:state_satisfies(afterany, running)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afterany, completed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afterany, failed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afterany, cancelled)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(afterany, timeout))
         end},

        {"aftercorr behaves like afterany",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(aftercorr, pending)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(aftercorr, completed)),
             ?assertEqual(true, flurm_job_deps:state_satisfies(aftercorr, failed))
         end},

        {"unknown dep type returns false",
         fun() ->
             ?assertEqual(false, flurm_job_deps:state_satisfies(unknown_type, completed))
         end}
    ]}.

%%====================================================================
%% is_terminal_state/1 Tests
%%====================================================================

is_terminal_state_test_() ->
    {"is_terminal_state/1 tests", [
        {"completed is terminal",
         fun() ->
             ?assertEqual(true, flurm_job_deps:is_terminal_state(completed))
         end},

        {"failed is terminal",
         fun() ->
             ?assertEqual(true, flurm_job_deps:is_terminal_state(failed))
         end},

        {"cancelled is terminal",
         fun() ->
             ?assertEqual(true, flurm_job_deps:is_terminal_state(cancelled))
         end},

        {"timeout is terminal",
         fun() ->
             ?assertEqual(true, flurm_job_deps:is_terminal_state(timeout))
         end},

        {"pending is not terminal",
         fun() ->
             ?assertEqual(false, flurm_job_deps:is_terminal_state(pending))
         end},

        {"running is not terminal",
         fun() ->
             ?assertEqual(false, flurm_job_deps:is_terminal_state(running))
         end},

        {"configuring is not terminal",
         fun() ->
             ?assertEqual(false, flurm_job_deps:is_terminal_state(configuring))
         end},

        {"held is not terminal",
         fun() ->
             ?assertEqual(false, flurm_job_deps:is_terminal_state(held))
         end}
    ]}.

%%====================================================================
%% Roundtrip Tests (parse + format)
%%====================================================================

parse_format_roundtrip_test_() ->
    {"parse/format roundtrip tests", [
        {"afterok roundtrip",
         fun() ->
             Original = <<"afterok:123">>,
             {Type, Target} = flurm_job_deps:parse_single_dep(Original),
             Formatted = flurm_job_deps:format_single_dep({Type, Target}),
             ?assertEqual(Original, Formatted)
         end},

        {"singleton roundtrip",
         fun() ->
             Original = <<"singleton">>,
             {Type, Target} = flurm_job_deps:parse_single_dep(Original),
             Formatted = flurm_job_deps:format_single_dep({Type, Target}),
             ?assertEqual(Original, Formatted)
         end},

        {"named singleton roundtrip",
         fun() ->
             Original = <<"singleton:workflow1">>,
             {Type, Target} = flurm_job_deps:parse_single_dep(Original),
             Formatted = flurm_job_deps:format_single_dep({Type, Target}),
             ?assertEqual(Original, Formatted)
         end},

        {"multi-target roundtrip",
         fun() ->
             Original = <<"afterany:10+20+30">>,
             {Type, Target} = flurm_job_deps:parse_single_dep(Original),
             Formatted = flurm_job_deps:format_single_dep({Type, Target}),
             ?assertEqual(Original, Formatted)
         end}
    ]}.

%%====================================================================
%% API Function Tests (parse_dependency_spec, format_dependency_spec)
%%====================================================================

parse_dependency_spec_test_() ->
    {"parse_dependency_spec/1 tests", [
        {"parses empty spec",
         fun() ->
             ?assertEqual({ok, []}, flurm_job_deps:parse_dependency_spec(<<>>))
         end},

        {"parses single dependency",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:100">>),
             ?assertEqual([{afterok, 100}], Deps)
         end},

        {"parses multiple dependencies",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:100,afterany:200">>),
             ?assertEqual([{afterok, 100}, {afterany, 200}], Deps)
         end},

        {"parses complex dependency spec",
         fun() ->
             {ok, Deps} = flurm_job_deps:parse_dependency_spec(
                 <<"afterok:10+20,singleton,afternotok:30">>
             ),
             ?assertEqual(3, length(Deps)),
             ?assertEqual({afterok, [10, 20]}, lists:nth(1, Deps)),
             ?assertEqual({singleton, <<"default">>}, lists:nth(2, Deps)),
             ?assertEqual({afternotok, 30}, lists:nth(3, Deps))
         end},

        {"returns error on invalid spec",
         fun() ->
             {error, _Reason} = flurm_job_deps:parse_dependency_spec(<<"invalid:abc">>)
         end}
    ]}.

format_dependency_spec_test_() ->
    {"format_dependency_spec/1 tests", [
        {"formats empty list",
         fun() ->
             ?assertEqual(<<>>, flurm_job_deps:format_dependency_spec([]))
         end},

        {"formats single dependency",
         fun() ->
             ?assertEqual(<<"afterok:123">>,
                          flurm_job_deps:format_dependency_spec([{afterok, 123}]))
         end},

        {"formats multiple dependencies",
         fun() ->
             Deps = [{afterok, 100}, {afterany, 200}],
             ?assertEqual(<<"afterok:100,afterany:200">>,
                          flurm_job_deps:format_dependency_spec(Deps))
         end}
    ]}.
