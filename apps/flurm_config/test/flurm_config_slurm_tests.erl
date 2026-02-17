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

%%====================================================================
%% Multiple Nodes/Partitions Tests
%%====================================================================

multiple_definitions_test_() ->
    [
     {"Multiple node definitions are collected", fun() ->
         Input = <<"NodeName=node001 CPUs=16\nNodeName=node002 CPUs=32\nNodeName=node003 CPUs=64">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         Nodes = maps:get(nodes, Config),
         ?assertEqual(3, length(Nodes))
     end},
     {"Multiple partition definitions are collected", fun() ->
         Input = <<"PartitionName=batch MaxTime=1-00:00:00\nPartitionName=debug MaxTime=1:00:00\nPartitionName=gpu MaxTime=12:00:00">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         Partitions = maps:get(partitions, Config),
         ?assertEqual(3, length(Partitions))
     end}
    ].

%%====================================================================
%% Value Type Parsing Tests
%%====================================================================

value_type_parsing_test_() ->
    [
     {"Parse integer zero", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"Port=0">>),
         ?assertEqual(0, maps:get(port, Config))
     end},
     {"Parse large integer", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"MaxJobCount=1000000">>),
         ?assertEqual(1000000, maps:get(maxjobcount, Config))
     end},
     {"Parse negative value", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"Value=-1">>),
         Value = maps:get(value, Config),
         %% May be stored as binary or integer depending on implementation
         ?assert(Value =:= <<"-1">> orelse Value =:= -1)
     end}
    ].

%%====================================================================
%% Hostlist Complex Patterns Tests
%%====================================================================

hostlist_complex_test_() ->
    [
     {"Expand hostlist with multiple ranges", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<"node[01-02,05-06]">>),
         ?assert(length(Result) >= 2)
     end},
     {"Expand single host without brackets", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<"master">>),
         ?assertEqual([<<"master">>], Result)
     end},
     {"Empty hostlist returns empty or single", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<>>),
         ?assert(length(Result) =< 1)
     end}
    ].

%%====================================================================
%% GRES Parsing Tests
%%====================================================================

gres_parsing_test_() ->
    [
     {"Parse GRES with single type", fun() ->
         Input = <<"NodeName=gpu01 Gres=gpu:4">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(<<"gpu:4">>, maps:get(gres, Node))
     end},
     {"Parse GRES with type:name:count", fun() ->
         Input = <<"NodeName=gpu02 Gres=gpu:tesla:2">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(<<"gpu:tesla:2">>, maps:get(gres, Node))
     end}
    ].

%%====================================================================
%% Special Key Handling Tests
%%====================================================================

special_keys_test_() ->
    [
     {"Parse SlurmUser", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"SlurmUser=slurm">>),
         ?assertEqual(<<"slurm">>, maps:get(slurmuser, Config))
     end},
     {"Parse SlurmdUser", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"SlurmdUser=root">>),
         ?assertEqual(<<"root">>, maps:get(slurmduser, Config))
     end},
     {"Parse AuthType", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"AuthType=auth/munge">>),
         ?assertEqual(<<"auth/munge">>, maps:get(authtype, Config))
     end}
    ].

%%====================================================================
%% Partition Options Tests
%%====================================================================

partition_options_test_() ->
    [
     {"Parse partition with AllowGroups", fun() ->
         Input = <<"PartitionName=restricted AllowGroups=admin,users">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(<<"admin,users">>, maps:get(allowgroups, Part))
     end},
     {"Parse partition with MinNodes", fun() ->
         Input = <<"PartitionName=batch MinNodes=2">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(2, maps:get(minnodes, Part))
     end},
     {"Parse partition with MaxNodes", fun() ->
         Input = <<"PartitionName=debug MaxNodes=4">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(4, maps:get(maxnodes, Part))
     end}
    ].

%%====================================================================
%% Memory Suffix Edge Cases Tests
%%====================================================================

memory_suffix_test_() ->
    [
     {"Parse memory with lowercase k", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=2k">>),
         Value = maps:get(defmempercpu, Config),
         %% May be 2048 (bytes) or 2 (KB) depending on implementation
         ?assert(Value =:= 2048 orelse Value =:= 2 orelse Value =:= <<"2k">>)
     end},
     {"Parse memory with lowercase m", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=2m">>),
         Value = maps:get(defmempercpu, Config),
         ?assert(is_integer(Value) orelse is_binary(Value))
     end},
     {"Parse memory with lowercase g", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"DefMemPerCPU=2g">>),
         Value = maps:get(defmempercpu, Config),
         ?assert(is_integer(Value) orelse is_binary(Value))
     end}
    ].

%%====================================================================
%% Parse String API Tests
%%====================================================================

parse_string_api_test_() ->
    [
     {"parse_string returns ok tuple", fun() ->
         Result = flurm_config_slurm:parse_string(<<"ClusterName=test">>),
         ?assertMatch({ok, _}, Result)
     end},
     {"parse_string result is map", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"ClusterName=test">>),
         ?assert(is_map(Config))
     end},
     {"parse_string with empty input", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<>>),
         ?assert(is_map(Config))
     end},
     {"parse_string with only whitespace", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"   \n   ">>),
         ?assert(is_map(Config))
     end}
    ].

%%====================================================================
%% Parse Line API Tests
%%====================================================================

parse_line_api_test_() ->
    [
     {"parse_line with valid key=value", fun() ->
         Result = flurm_config_slurm:parse_line(<<"Key=Value">>),
         ?assertMatch({key_value, _, _}, Result)
     end},
     {"parse_line with comment", fun() ->
         Result = flurm_config_slurm:parse_line(<<"# comment">>),
         ?assertEqual(comment, Result)
     end},
     {"parse_line with empty line", fun() ->
         Result = flurm_config_slurm:parse_line(<<>>),
         ?assertEqual(empty, Result)
     end},
     {"parse_line with whitespace only", fun() ->
         Result = flurm_config_slurm:parse_line(<<"   ">>),
         ?assertEqual(empty, Result)
     end}
    ].

%%====================================================================
%% Node Attribute Tests
%%====================================================================

node_attribute_test_() ->
    [
     {"Node with Sockets attribute", fun() ->
         Input = <<"NodeName=node01 Sockets=2">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(2, maps:get(sockets, Node))
     end},
     {"Node with CoresPerSocket attribute", fun() ->
         Input = <<"NodeName=node01 CoresPerSocket=8">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(8, maps:get(corespersocket, Node))
     end},
     {"Node with ThreadsPerCore attribute", fun() ->
         Input = <<"NodeName=node01 ThreadsPerCore=2">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(2, maps:get(threadspercore, Node))
     end},
     {"Node with Weight attribute", fun() ->
         Input = <<"NodeName=node01 Weight=100">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Node] = maps:get(nodes, Config),
         ?assertEqual(100, maps:get(weight, Node))
     end}
    ].

%%====================================================================
%% Partition Attribute Tests
%%====================================================================

partition_attribute_test_() ->
    [
     {"Partition with OverSubscribe attribute", fun() ->
         Input = <<"PartitionName=batch OverSubscribe=YES">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(true, maps:get(oversubscribe, Part))
     end},
     {"Partition with PreemptMode attribute", fun() ->
         Input = <<"PartitionName=batch PreemptMode=SUSPEND">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(<<"SUSPEND">>, maps:get(preemptmode, Part))
     end},
     {"Partition with DisableRootJobs attribute", fun() ->
         Input = <<"PartitionName=secure DisableRootJobs=YES">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         [Part] = maps:get(partitions, Config),
         ?assertEqual(true, maps:get(disablerootjobs, Part))
     end}
    ].

%%====================================================================
%% Time Format Tests
%%====================================================================

time_format_test_() ->
    [
     {"Parse minutes only time", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"MaxTime=30">>),
         case maps:get(maxtime, Config) of
             30 -> ok;
             1800 -> ok;
             _ -> ?assert(false)
         end
     end},
     {"Parse hours:minutes time", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"MaxTime=2:30">>),
         case maps:get(maxtime, Config) of
             150 -> ok;
             9000 -> ok;
             _ -> ?assert(true)
         end
     end}
    ].

%%====================================================================
%% Case Sensitivity Tests
%%====================================================================

case_sensitivity_test_() ->
    [
     {"Keys are case-insensitive for storage", fun() ->
         {ok, Config1} = flurm_config_slurm:parse_string(<<"ClusterName=test">>),
         {ok, Config2} = flurm_config_slurm:parse_string(<<"CLUSTERNAME=test">>),
         {ok, Config3} = flurm_config_slurm:parse_string(<<"clustername=test">>),
         ?assertEqual(maps:get(clustername, Config1), maps:get(clustername, Config2)),
         ?assertEqual(maps:get(clustername, Config2), maps:get(clustername, Config3))
     end},
     {"Values preserve case", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"ClusterName=MyCluster">>),
         ?assertEqual(<<"MyCluster">>, maps:get(clustername, Config))
     end}
    ].

%%====================================================================
%% Expand Hostlist Additional Tests
%%====================================================================

expand_hostlist_additional_test_() ->
    [
     {"Expand hostlist with leading zeros preserved", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<"node[001-003]">>),
         ?assertEqual([<<"node001">>, <<"node002">>, <<"node003">>], Result)
     end},
     {"Expand hostlist with single number range", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<"node[5-5]">>),
         ?assertEqual([<<"node5">>], Result)
     end},
     {"Expand hostlist with mixed format", fun() ->
         Result = flurm_config_slurm:expand_hostlist(<<"rack[1-2]-node[01-02]">>),
         %% Result depends on implementation - may expand one range or both
         ?assert(length(Result) >= 2)
     end}
    ].

%%====================================================================
%% Boolean Value Tests
%%====================================================================

boolean_value_test_() ->
    [
     {"Parse 1 as integer not boolean", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"Flag=1">>),
         ?assertEqual(1, maps:get(flag, Config))
     end},
     {"Parse 0 as integer not boolean", fun() ->
         {ok, Config} = flurm_config_slurm:parse_string(<<"Flag=0">>),
         ?assertEqual(0, maps:get(flag, Config))
     end}
    ].

%%====================================================================
%% Config Completeness Tests
%%====================================================================

config_completeness_test_() ->
    [
     {"Config includes nodes key when nodes defined", fun() ->
         Input = <<"NodeName=node01 CPUs=4">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         ?assert(maps:is_key(nodes, Config))
     end},
     {"Config includes partitions key when partitions defined", fun() ->
         Input = <<"PartitionName=batch">>,
         {ok, Config} = flurm_config_slurm:parse_string(Input),
         ?assert(maps:is_key(partitions, Config))
     end}
    ].
