%%%-------------------------------------------------------------------
%%% @doc Tests for FLURM slurm.conf parser
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_slurm_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

slurm_parser_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"Parse simple key=value", fun test_simple_keyvalue/0},
         {"Parse boolean values", fun test_boolean_values/0},
         {"Parse numeric values", fun test_numeric_values/0},
         {"Parse memory values with suffixes", fun test_memory_values/0},
         {"Parse time values", fun test_time_values/0},
         {"Parse comments", fun test_comments/0},
         {"Parse node definition", fun test_node_definition/0},
         {"Parse partition definition", fun test_partition_definition/0},
         {"Parse complete config", fun test_complete_config/0},
         {"Expand simple hostlist", fun test_expand_simple/0},
         {"Expand hostlist range", fun test_expand_range/0},
         {"Expand hostlist comma", fun test_expand_comma/0},
         {"Expand hostlist complex", fun test_expand_complex/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Parse Tests
%%====================================================================

test_simple_keyvalue() ->
    {ok, Config} = flurm_config_slurm:parse_string(<<"ClusterName=mycluster">>),
    ?assertEqual(<<"mycluster">>, maps:get(clustername, Config)).

test_boolean_values() ->
    {ok, Config1} = flurm_config_slurm:parse_string(<<"ReturnToService=YES">>),
    ?assertEqual(true, maps:get(returntoservice, Config1)),

    {ok, Config2} = flurm_config_slurm:parse_string(<<"ReturnToService=NO">>),
    ?assertEqual(false, maps:get(returntoservice, Config2)),

    {ok, Config3} = flurm_config_slurm:parse_string(<<"SomeFlag=true">>),
    ?assertEqual(<<"true">>, maps:get(someflag, Config3)),

    {ok, Config4} = flurm_config_slurm:parse_string(<<"EnableFlag=TRUE">>),
    ?assertEqual(true, maps:get(enableflag, Config4)).

test_numeric_values() ->
    {ok, Config1} = flurm_config_slurm:parse_string(<<"SlurmctldPort=6817">>),
    ?assertEqual(6817, maps:get(slurmctldport, Config1)),

    {ok, Config2} = flurm_config_slurm:parse_string(<<"MaxJobCount=10000">>),
    ?assertEqual(10000, maps:get(maxjobcount, Config2)).

test_memory_values() ->
    {ok, Config1} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=1024">>),
    ?assertEqual(1024, maps:get(defmempercpu, Config1)),

    {ok, Config2} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=1K">>),
    ?assertEqual(1024, maps:get(defmempercpu, Config2)),

    {ok, Config3} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=1M">>),
    ?assertEqual(1024 * 1024, maps:get(defmempercpu, Config3)),

    {ok, Config4} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=1G">>),
    ?assertEqual(1024 * 1024 * 1024, maps:get(defmempercpu, Config4)),

    {ok, Config5} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=1T">>),
    ?assertEqual(1024 * 1024 * 1024 * 1024, maps:get(defmempercpu, Config5)).

test_time_values() ->
    {ok, Config1} = flurm_config_slurm:parse_string(<<"MaxTime=1:30:00">>),
    ?assertEqual(5400, maps:get(maxtime, Config1)),  % 1.5 hours in seconds

    {ok, Config2} = flurm_config_slurm:parse_string(<<"MaxTime=1-00:00:00">>),
    ?assertEqual(86400, maps:get(maxtime, Config2)),  % 1 day in seconds

    {ok, Config3} = flurm_config_slurm:parse_string(<<"MaxTime=INFINITE">>),
    ?assertEqual(infinity, maps:get(maxtime, Config3)),

    {ok, Config4} = flurm_config_slurm:parse_string(<<"MaxTime=30:00">>),
    ?assertEqual(1800, maps:get(maxtime, Config4)).  % 30 minutes

test_comments() ->
    Input = <<"# This is a comment\nClusterName=test\n# Another comment">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    ?assertEqual(<<"test">>, maps:get(clustername, Config)),
    ?assertEqual(1, maps:size(Config)).

test_node_definition() ->
    Input = <<"NodeName=node001 CPUs=64 RealMemory=128000 State=IDLE">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    Nodes = maps:get(nodes, Config),
    ?assertEqual(1, length(Nodes)),
    [Node] = Nodes,
    ?assertEqual(<<"node001">>, maps:get(nodename, Node)),
    ?assertEqual(64, maps:get(cpus, Node)),
    ?assertEqual(128000, maps:get(realmemory, Node)).

test_partition_definition() ->
    Input = <<"PartitionName=batch Nodes=node[001-010] Default=YES MaxTime=INFINITE">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    Partitions = maps:get(partitions, Config),
    ?assertEqual(1, length(Partitions)),
    [Part] = Partitions,
    ?assertEqual(<<"batch">>, maps:get(partitionname, Part)),
    ?assertEqual(true, maps:get(default, Part)),
    ?assertEqual(infinity, maps:get(maxtime, Part)).

test_complete_config() ->
    Input = <<"
# SLURM Configuration File
ClusterName=mycluster
SlurmctldPort=6817
SlurmdPort=6818

# Node definitions
NodeName=compute[001-010] CPUs=64 RealMemory=256000 State=IDLE
NodeName=gpu[01-04] CPUs=32 RealMemory=512000 Gres=gpu:4

# Partition definitions
PartitionName=batch Nodes=compute[001-010] Default=YES MaxTime=1-00:00:00
PartitionName=gpu Nodes=gpu[01-04] MaxTime=12:00:00
">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),

    ?assertEqual(<<"mycluster">>, maps:get(clustername, Config)),
    ?assertEqual(6817, maps:get(slurmctldport, Config)),
    ?assertEqual(6818, maps:get(slurmdport, Config)),

    Nodes = maps:get(nodes, Config),
    ?assertEqual(2, length(Nodes)),

    Partitions = maps:get(partitions, Config),
    ?assertEqual(2, length(Partitions)).

%%====================================================================
%% Hostlist Expansion Tests
%%====================================================================

test_expand_simple() ->
    Result = flurm_config_slurm:expand_hostlist(<<"node001">>),
    ?assertEqual([<<"node001">>], Result).

test_expand_range() ->
    Result = flurm_config_slurm:expand_hostlist(<<"node[001-003]">>),
    ?assertEqual([<<"node001">>, <<"node002">>, <<"node003">>], Result).

test_expand_comma() ->
    Result = flurm_config_slurm:expand_hostlist(<<"node[001,003,005]">>),
    ?assertEqual([<<"node001">>, <<"node003">>, <<"node005">>], Result).

test_expand_complex() ->
    Result = flurm_config_slurm:expand_hostlist(<<"node[001-003,005,010-012]">>),
    Expected = [<<"node001">>, <<"node002">>, <<"node003">>,
                <<"node005">>,
                <<"node010">>, <<"node011">>, <<"node012">>],
    ?assertEqual(Expected, Result).

%%====================================================================
%% Edge Cases
%%====================================================================

empty_lines_test() ->
    Input = <<"ClusterName=test\n\n\nPort=6817\n">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    ?assertEqual(<<"test">>, maps:get(clustername, Config)),
    ?assertEqual(6817, maps:get(port, Config)).

whitespace_handling_test() ->
    Input = <<"  ClusterName = test  ">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    ?assertEqual(<<"test">>, maps:get(clustername, Config)).

case_insensitive_keys_test() ->
    {ok, Config1} = flurm_config_slurm:parse_string(<<"ClusterName=test">>),
    {ok, Config2} = flurm_config_slurm:parse_string(<<"CLUSTERNAME=test">>),
    {ok, Config3} = flurm_config_slurm:parse_string(<<"clustername=test">>),
    ?assertEqual(maps:get(clustername, Config1), maps:get(clustername, Config2)),
    ?assertEqual(maps:get(clustername, Config2), maps:get(clustername, Config3)).

parse_line_test() ->
    ?assertEqual({key_value, clustername, <<"test">>},
                 flurm_config_slurm:parse_line(<<"ClusterName=test">>)),
    ?assertEqual(comment, flurm_config_slurm:parse_line(<<"# comment">>)),
    ?assertEqual(empty, flurm_config_slurm:parse_line(<<"">>)).

%%====================================================================
%% File Parsing Tests
%%====================================================================

parse_file_test_() ->
    {foreach,
     fun() ->
         file:make_dir("/tmp/flurm_slurm_test"),
         ok
     end,
     fun(_) ->
         file:del_dir_r("/tmp/flurm_slurm_test"),
         ok
     end,
     [
      {"Parse file - success", fun test_parse_file_success/0},
      {"Parse file - error (not found)", fun test_parse_file_not_found/0},
      {"Parse string as list (not binary)", fun test_parse_string_as_list/0}
     ]}.

test_parse_file_success() ->
    TestFile = "/tmp/flurm_slurm_test/test.conf",
    Content = "ClusterName=filetest\nSlurmctldPort=6817\n",
    ok = file:write_file(TestFile, Content),
    Result = flurm_config_slurm:parse_file(TestFile),
    ?assertMatch({ok, _}, Result),
    {ok, Config} = Result,
    ?assertEqual(<<"filetest">>, maps:get(clustername, Config)),
    ?assertEqual(6817, maps:get(slurmctldport, Config)).

test_parse_file_not_found() ->
    Result = flurm_config_slurm:parse_file("/tmp/flurm_slurm_test/nonexistent.conf"),
    ?assertMatch({error, {file_error, _, enoent}}, Result).

test_parse_string_as_list() ->
    %% parse_string should accept list (string) input
    Result = flurm_config_slurm:parse_string("ClusterName=listtest"),
    ?assertMatch({ok, _}, Result),
    {ok, Config} = Result,
    ?assertEqual(<<"listtest">>, maps:get(clustername, Config)).

%%====================================================================
%% Additional Value Parsing Tests
%%====================================================================

additional_values_test_() ->
    [
     {"Parse UNLIMITED value", fun test_unlimited_value/0},
     {"Parse Yes/No variants", fun test_yes_no_variants/0},
     {"Parse FALSE boolean", fun test_false_boolean/0},
     {"Parse invalid line (no equals)", fun test_invalid_line/0}
    ].

test_unlimited_value() ->
    {ok, Config} = flurm_config_slurm:parse_string(<<"MaxJobCount=UNLIMITED">>),
    ?assertEqual(unlimited, maps:get(maxjobcount, Config)).

test_yes_no_variants() ->
    %% Test lowercase yes/no
    {ok, Config1} = flurm_config_slurm:parse_string(<<"Flag=yes">>),
    ?assertEqual(true, maps:get(flag, Config1)),

    {ok, Config2} = flurm_config_slurm:parse_string(<<"Flag=no">>),
    ?assertEqual(false, maps:get(flag, Config2)),

    %% Test mixed case Yes/No
    {ok, Config3} = flurm_config_slurm:parse_string(<<"Flag=Yes">>),
    ?assertEqual(true, maps:get(flag, Config3)),

    {ok, Config4} = flurm_config_slurm:parse_string(<<"Flag=No">>),
    ?assertEqual(false, maps:get(flag, Config4)).

test_false_boolean() ->
    {ok, Config} = flurm_config_slurm:parse_string(<<"Flag=FALSE">>),
    ?assertEqual(false, maps:get(flag, Config)).

test_invalid_line() ->
    %% Lines without = should return error
    Result = flurm_config_slurm:parse_line(<<"InvalidLineWithoutEquals">>),
    ?assertMatch({error, {invalid_line, _}}, Result).

%%====================================================================
%% Line Continuation Tests
%%====================================================================

line_continuation_test_() ->
    [
     {"Simple line continuation", fun test_simple_continuation/0},
     {"Multi-line continuation", fun test_multi_line_continuation/0}
    ].

test_simple_continuation() ->
    Input = <<"ClusterName=\\\nmytest">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    ?assertEqual(<<"mytest">>, maps:get(clustername, Config)).

test_multi_line_continuation() ->
    Input = <<"Key1=value1\\\ncontinued">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    ?assertEqual(<<"value1continued">>, maps:get(key1, Config)).

%%====================================================================
%% Hostlist Expansion Edge Cases
%%====================================================================

hostlist_edge_cases_test_() ->
    [
     {"Expand hostlist with list input (not binary)", fun test_expand_list_input/0},
     {"Expand hostlist without brackets", fun test_expand_no_brackets/0},
     {"Expand hostlist with suffix", fun test_expand_with_suffix/0}
    ].

test_expand_list_input() ->
    %% expand_hostlist should accept list (string) input
    Result = flurm_config_slurm:expand_hostlist("node001"),
    ?assertEqual([<<"node001">>], Result).

test_expand_no_brackets() ->
    Result = flurm_config_slurm:expand_hostlist("simplehost"),
    ?assertEqual([<<"simplehost">>], Result).

test_expand_with_suffix() ->
    Result = flurm_config_slurm:expand_hostlist(<<"rack[1-3]-node01">>),
    ?assertEqual([<<"rack1-node01">>, <<"rack2-node01">>, <<"rack3-node01">>], Result).

%%====================================================================
%% Time Parsing Edge Cases
%%====================================================================

time_parsing_test_() ->
    [
     {"Parse time with days and hours", fun test_time_days_hours/0},
     {"Parse simple hours:minutes:seconds", fun test_time_hms/0},
     {"String that looks like time but isn't", fun test_not_a_time/0}
    ].

test_time_days_hours() ->
    %% 2 days, 3 hours, 30 minutes, 15 seconds
    {ok, Config} = flurm_config_slurm:parse_string(<<"MaxTime=2-03:30:15">>),
    Expected = 2 * 86400 + 3 * 3600 + 30 * 60 + 15,
    ?assertEqual(Expected, maps:get(maxtime, Config)).

test_time_hms() ->
    %% 12 hours, 30 minutes, 45 seconds
    {ok, Config} = flurm_config_slurm:parse_string(<<"MaxTime=12:30:45">>),
    Expected = 12 * 3600 + 30 * 60 + 45,
    ?assertEqual(Expected, maps:get(maxtime, Config)).

test_not_a_time() ->
    %% Something that looks like time but has invalid components
    {ok, Config} = flurm_config_slurm:parse_string(<<"Key=abc:def:ghi">>),
    %% Should be stored as binary since it's not a valid time
    ?assertEqual(<<"abc:def:ghi">>, maps:get(key, Config)).

%%====================================================================
%% Node/Partition Definition Edge Cases
%%====================================================================

definition_edge_cases_test_() ->
    [
     {"Node definition with empty part", fun test_node_empty_part/0},
     {"Partition definition parsing", fun test_partition_parsing/0}
    ].

test_node_empty_part() ->
    %% Node definition where some parts might be invalid
    Input = <<"NodeName=node001 CPUs=64  RealMemory=128000">>,  %% double space
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    Nodes = maps:get(nodes, Config),
    ?assertEqual(1, length(Nodes)).

test_partition_parsing() ->
    Input = <<"PartitionName=test State=UP Priority=1">>,
    {ok, Config} = flurm_config_slurm:parse_string(Input),
    Partitions = maps:get(partitions, Config),
    ?assertEqual(1, length(Partitions)),
    [Part] = Partitions,
    ?assertEqual(<<"test">>, maps:get(partitionname, Part)),
    ?assertEqual(1, maps:get(priority, Part)).
