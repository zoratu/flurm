%%%-------------------------------------------------------------------
%%% @doc TLA+ Model Checker Integration Tests for FLURM
%%%
%%% This module runs the TLC model checker on FLURM TLA+ specifications
%%% and integrates the results with Erlang's EUnit test framework.
%%%
%%% These tests execute the actual TLC model checker and parse the output
%%% to report invariant violations as test failures.
%%%
%%% Prerequisites:
%%%   - Java 11+ installed and in PATH
%%%   - tla2tools.jar available in the tla/ directory
%%%
%%% The tests are marked as long-running and may be skipped in CI by
%%% setting the environment variable SKIP_TLA_TESTS=1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_tla_integration_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Macros and Configuration
%%====================================================================

-define(TLA_DIR, "tla").
-define(TLA2TOOLS, "tla2tools.jar").
-define(STATES_DIR, "states").
-define(TIMEOUT_MS, 300000).  % 5 minute timeout per spec
-define(JAVA_MEMORY, "4g").
-define(WORKERS, "auto").

%% All TLA+ specifications to check
-define(ALL_SPECS, [
    "FlurmFederation",
    "FlurmAccounting",
    "FlurmMigration",
    "FlurmConsensus",
    "FlurmFailover",
    "FlurmJobLifecycle",
    "FlurmScheduler"
]).

%%====================================================================
%% Test Setup and Teardown
%%====================================================================

%% @doc Check if TLA+ tests should be skipped
should_skip_tla_tests() ->
    case os:getenv("SKIP_TLA_TESTS") of
        "1" -> true;
        "true" -> true;
        _ -> false
    end.

%% @doc Get the project root directory
get_project_root() ->
    %% Start from the current working directory and find project root
    case file:get_cwd() of
        {ok, Cwd} ->
            find_project_root(Cwd);
        {error, _} ->
            undefined
    end.

%% @doc Find the project root by looking for tla/ directory
find_project_root(Dir) ->
    TlaDir = filename:join(Dir, "tla"),
    case filelib:is_dir(TlaDir) of
        true -> Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> undefined;  % Reached filesystem root
                false -> find_project_root(Parent)
            end
    end.

%% @doc Check if Java is available
java_available() ->
    case os:find_executable("java") of
        false -> false;
        _ ->
            case os:cmd("java -version 2>&1") of
                [] -> false;
                _ -> true
            end
    end.

%% @doc Check if TLA tools are available
tla_tools_available() ->
    case get_project_root() of
        undefined -> false;
        Root ->
            TlaToolsPath = filename:join([Root, ?TLA_DIR, ?TLA2TOOLS]),
            filelib:is_file(TlaToolsPath)
    end.

%%====================================================================
%% Main Test Suite
%%====================================================================

%% @doc Run all TLA+ model checking tests
tla_model_checking_test_() ->
    case should_skip_tla_tests() of
        true ->
            {"TLA+ tests skipped (SKIP_TLA_TESTS=1)", []};
        false ->
            case {java_available(), tla_tools_available()} of
                {false, _} ->
                    {"TLA+ tests skipped (Java not available)", []};
                {_, false} ->
                    {"TLA+ tests skipped (tla2tools.jar not found)", []};
                {true, true} ->
                    {"TLA+ Model Checking", {setup,
                        fun setup/0,
                        fun cleanup/1,
                        {timeout, ?TIMEOUT_MS * length(?ALL_SPECS) div 1000,
                            generate_spec_tests()
                        }
                    }}
            end
    end.

%% @doc Setup function for tests
setup() ->
    Root = get_project_root(),
    StatesDir = filename:join([Root, ?TLA_DIR, ?STATES_DIR]),
    ok = filelib:ensure_dir(filename:join(StatesDir, "dummy")),
    #{root => Root, states_dir => StatesDir}.

%% @doc Cleanup function for tests
cleanup(_State) ->
    ok.

%% @doc Generate test cases for each specification
generate_spec_tests() ->
    [generate_spec_test(Spec) || Spec <- ?ALL_SPECS].

%% @doc Generate a test case for a single specification
generate_spec_test(Spec) ->
    {Spec ++ " invariant checking", {timeout, ?TIMEOUT_MS div 1000,
        fun() -> run_spec_test(Spec) end
    }}.

%%====================================================================
%% Spec Test Runner
%%====================================================================

%% @doc Run TLC on a single specification and check results
run_spec_test(Spec) ->
    Root = get_project_root(),
    TlaDir = filename:join(Root, ?TLA_DIR),
    TlaFile = filename:join(TlaDir, Spec ++ ".tla"),
    CfgFile = filename:join(TlaDir, Spec ++ ".cfg"),
    TlaTools = filename:join(TlaDir, ?TLA2TOOLS),
    StatesDir = filename:join(TlaDir, ?STATES_DIR),
    OutputFile = filename:join(StatesDir, Spec ++ "_output.txt"),

    %% Verify files exist
    ?assert(filelib:is_file(TlaFile)),
    ?assert(filelib:is_file(CfgFile)),
    ?assert(filelib:is_file(TlaTools)),

    %% Build the TLC command
    Cmd = build_tlc_command(TlaTools, CfgFile, TlaFile),

    %% Run TLC
    {Output, ExitCode} = run_tlc(Cmd, TlaDir),

    %% Save output to file
    file:write_file(OutputFile, Output),

    %% Parse and verify results
    Result = parse_tlc_output(Output),

    %% Report results
    case Result of
        {ok, States, _Time} ->
            io:format("~s: Model checking completed (~p states)~n", [Spec, States]),
            ?assertEqual(0, ExitCode);
        {error, {invariant_violated, Invariant, Details}} ->
            io:format("~s: INVARIANT VIOLATED: ~s~n~s~n", [Spec, Invariant, Details]),
            ?assert(false, "Invariant " ++ Invariant ++ " was violated");
        {error, {property_violated, Property, Details}} ->
            io:format("~s: PROPERTY VIOLATED: ~s~n~s~n", [Spec, Property, Details]),
            ?assert(false, "Property " ++ Property ++ " was violated");
        {error, {deadlock, Details}} ->
            io:format("~s: DEADLOCK FOUND~n~s~n", [Spec, Details]),
            ?assert(false, "Deadlock found in specification");
        {error, {tlc_error, ErrorMsg}} ->
            io:format("~s: TLC ERROR: ~s~n", [Spec, ErrorMsg]),
            ?assert(false, "TLC error: " ++ ErrorMsg);
        {error, {unknown_error, Details}} ->
            io:format("~s: UNKNOWN ERROR~n~s~n", [Spec, Details]),
            ?assertNot(ExitCode =/= 0, "TLC failed with unknown error")
    end.

%% @doc Build the TLC command string
build_tlc_command(TlaTools, CfgFile, TlaFile) ->
    lists:flatten(io_lib:format(
        "java -XX:+UseParallelGC -Xmx~s -cp ~s tlc2.TLC "
        "-config ~s -workers ~s -deadlock ~s 2>&1",
        [?JAVA_MEMORY, TlaTools, CfgFile, ?WORKERS, TlaFile]
    )).

%% @doc Run TLC and capture output
run_tlc(Cmd, WorkDir) ->
    %% Use os:cmd with a timeout wrapper
    Port = open_port({spawn, Cmd}, [
        {cd, WorkDir},
        stream,
        exit_status,
        stderr_to_stdout,
        binary
    ]),
    collect_port_output(Port, [], ?TIMEOUT_MS).

%% @doc Collect output from a port with timeout
collect_port_output(Port, Acc, Timeout) ->
    receive
        {Port, {data, Data}} ->
            collect_port_output(Port, [Data | Acc], Timeout);
        {Port, {exit_status, Status}} ->
            Output = iolist_to_binary(lists:reverse(Acc)),
            {binary_to_list(Output), Status}
    after Timeout ->
        port_close(Port),
        Output = iolist_to_binary(lists:reverse(Acc)),
        {binary_to_list(Output), -1}
    end.

%%====================================================================
%% TLC Output Parser
%%====================================================================

%% @doc Parse TLC output and return structured result
-spec parse_tlc_output(string()) ->
    {ok, non_neg_integer(), non_neg_integer()} |
    {error, {atom(), string()} | {atom(), string(), string()}}.
parse_tlc_output(Output) ->
    Lines = string:split(Output, "\n", all),
    parse_tlc_lines(Lines, #{}).

%% @doc Parse TLC output lines
parse_tlc_lines([], State) ->
    case maps:get(completed, State, false) of
        true ->
            States = maps:get(states, State, 0),
            Time = maps:get(time, State, 0),
            {ok, States, Time};
        false ->
            case maps:get(error, State, undefined) of
                undefined ->
                    {error, {unknown_error, "TLC did not complete normally"}};
                Error ->
                    Error
            end
    end;
parse_tlc_lines([Line | Rest], State) ->
    NewState = parse_tlc_line(Line, State),
    parse_tlc_lines(Rest, NewState).

%% @doc Parse a single TLC output line
parse_tlc_line(Line, State) ->
    %% Check for model checking completion
    State1 = case string:find(Line, "Model checking completed") of
        nomatch -> State;
        _ -> State#{completed => true}
    end,

    %% Check for state count
    State2 = case re:run(Line, "([0-9,]+) distinct states found", [{capture, [1], list}]) of
        {match, [StatesStr]} ->
            States = list_to_integer(string:replace(StatesStr, ",", "", all)),
            State1#{states => States};
        nomatch -> State1
    end,

    %% Check for invariant violation
    State3 = case string:find(Line, "is violated") of
        nomatch -> State2;
        _ ->
            case re:run(Line, "Invariant ([^ ]+) is violated", [{capture, [1], list}]) of
                {match, [InvName]} ->
                    State2#{error => {error, {invariant_violated, InvName, Line}}};
                nomatch ->
                    case re:run(Line, "Property ([^ ]+)", [{capture, [1], list}]) of
                        {match, [PropName]} ->
                            State2#{error => {error, {property_violated, PropName, Line}}};
                        nomatch ->
                            State2#{error => {error, {invariant_violated, "unknown", Line}}}
                    end
            end
    end,

    %% Check for deadlock
    State4 = case string:find(Line, "Deadlock reached") of
        nomatch -> State3;
        _ -> State3#{error => {error, {deadlock, Line}}}
    end,

    %% Check for TLC errors
    case string:find(Line, "Error:") of
        nomatch -> State4;
        _ ->
            case maps:is_key(error, State4) of
                true -> State4;  % Keep first error
                false -> State4#{error => {error, {tlc_error, Line}}}
            end
    end.

%%====================================================================
%% Individual Spec Tests (for running specific specs)
%%====================================================================

%% @doc Run FlurmFederation model checking
federation_test_() ->
    create_single_spec_test("FlurmFederation").

%% @doc Run FlurmAccounting model checking
accounting_test_() ->
    create_single_spec_test("FlurmAccounting").

%% @doc Run FlurmMigration model checking
migration_test_() ->
    create_single_spec_test("FlurmMigration").

%% @doc Run FlurmConsensus model checking
consensus_test_() ->
    create_single_spec_test("FlurmConsensus").

%% @doc Run FlurmFailover model checking
failover_test_() ->
    create_single_spec_test("FlurmFailover").

%% @doc Run FlurmJobLifecycle model checking
job_lifecycle_test_() ->
    create_single_spec_test("FlurmJobLifecycle").

%% @doc Run FlurmScheduler model checking
scheduler_test_() ->
    create_single_spec_test("FlurmScheduler").

%% @doc Create a single spec test with guards
create_single_spec_test(Spec) ->
    case should_skip_tla_tests() of
        true ->
            {Spec ++ " skipped", []};
        false ->
            case {java_available(), tla_tools_available()} of
                {false, _} ->
                    {Spec ++ " skipped (no Java)", []};
                {_, false} ->
                    {Spec ++ " skipped (no tla2tools.jar)", []};
                {true, true} ->
                    {Spec, {timeout, ?TIMEOUT_MS div 1000,
                        fun() -> run_spec_test(Spec) end
                    }}
            end
    end.

%%====================================================================
%% Utility Functions
%%====================================================================

%% @doc Run TLC with JSON output and return parsed results
-spec run_tlc_json(string()) -> {ok, map()} | {error, term()}.
run_tlc_json(Spec) ->
    Root = get_project_root(),
    TlaDir = filename:join(Root, ?TLA_DIR),
    StatesDir = filename:join(TlaDir, ?STATES_DIR),
    JsonFile = filename:join(StatesDir, Spec ++ "_result.json"),

    %% Run the run_tlc.sh script with JSON output
    Cmd = lists:flatten(io_lib:format(
        "cd ~s && ./run_tlc.sh --json --quiet ~s",
        [TlaDir, Spec]
    )),

    _Output = os:cmd(Cmd),

    %% Parse JSON result
    case file:read_file(JsonFile) of
        {ok, Json} ->
            %% Simple JSON parsing (for CI integration)
            parse_simple_json(Json);
        {error, Reason} ->
            {error, {no_json_output, Reason}}
    end.

%% @doc Simple JSON parser for TLC results
parse_simple_json(Json) ->
    %% Very basic extraction of key fields
    Status = extract_json_field(Json, <<"status">>),
    States = extract_json_field(Json, <<"states">>),
    Elapsed = extract_json_field(Json, <<"elapsed_seconds">>),

    case Status of
        <<"PASS">> ->
            {ok, #{status => pass, states => States, elapsed => Elapsed}};
        <<"FAIL">> ->
            {ok, #{status => fail, states => States, elapsed => Elapsed}};
        <<"ERROR">> ->
            {ok, #{status => error, states => States, elapsed => Elapsed}};
        _ ->
            {error, {invalid_status, Status}}
    end.

%% @doc Extract a field value from JSON binary (simple implementation)
extract_json_field(Json, Field) ->
    Pattern = <<"\"", Field/binary, "\": \"">>,
    case binary:split(Json, Pattern) of
        [_, Rest] ->
            case binary:split(Rest, <<"\"">>) of
                [Value, _] -> Value;
                _ -> undefined
            end;
        _ ->
            %% Try numeric field
            NumPattern = <<"\"", Field/binary, "\": ">>,
            case binary:split(Json, NumPattern) of
                [_, Rest2] ->
                    case re:run(Rest2, "^([0-9]+)", [{capture, [1], binary}]) of
                        {match, [Num]} -> binary_to_integer(Num);
                        nomatch -> undefined
                    end;
                _ ->
                    undefined
            end
    end.

%% @doc Get summary of all TLA+ model checking results
-spec get_all_results() -> {ok, [map()]} | {error, term()}.
get_all_results() ->
    Results = [run_tlc_json(Spec) || Spec <- ?ALL_SPECS],
    {ok, [R || {ok, R} <- Results]}.

%% @doc Check if all specifications pass
-spec all_specs_pass() -> boolean().
all_specs_pass() ->
    case get_all_results() of
        {ok, Results} ->
            lists:all(fun(#{status := S}) -> S =:= pass end, Results);
        _ ->
            false
    end.
