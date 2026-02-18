%%%-------------------------------------------------------------------
%%% @doc FLURM Config Slurm Parser 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_config_slurm module covering:
%%% - File parsing
%%% - String parsing
%%% - Line parsing
%%% - Value parsing
%%% - Hostlist expansion
%%% - Node/partition definitions
%%% - Memory and time value parsing
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_slurm_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_config.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(file),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Parse File Tests
%%====================================================================

parse_file_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"parse_file success",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun("/test/slurm.conf") ->
                 {ok, <<"ClusterName=test\nSlurmctldPort=6817\n">>}
             end),

             {ok, Config} = flurm_config_slurm:parse_file("/test/slurm.conf"),
             ?assertEqual(<<"test">>, maps:get(clustername, Config)),
             ?assertEqual(6817, maps:get(slurmctldport, Config))
         end},
        {"parse_file with comments",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) ->
                 {ok, <<"# This is a comment\nClusterName=test\n# Another comment\n">>}
             end),

             {ok, Config} = flurm_config_slurm:parse_file("/test/conf"),
             ?assertEqual(<<"test">>, maps:get(clustername, Config))
         end},
        {"parse_file with empty lines",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) ->
                 {ok, <<"ClusterName=test\n\n\nSlurmdPort=6818\n\n">>}
             end),

             {ok, Config} = flurm_config_slurm:parse_file("/test/conf"),
             ?assertEqual(<<"test">>, maps:get(clustername, Config)),
             ?assertEqual(6818, maps:get(slurmdport, Config))
         end},
        {"parse_file error",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) -> {error, enoent} end),

             Result = flurm_config_slurm:parse_file("/nonexistent"),
             ?assertMatch({error, {file_error, _, enoent}}, Result)
         end},
        {"parse_file with line continuation",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) ->
                 {ok, <<"ClusterName=my\\\nvery\\\nlong\\\nname\n">>}
             end),

             {ok, Config} = flurm_config_slurm:parse_file("/test/conf"),
             %% Line continuation joins lines
             ?assert(maps:is_key(clustername, Config))
         end}
     ]}.

%%====================================================================
%% Parse String Tests
%%====================================================================

parse_string_test_() ->
    {"parse_string tests",
     [
        {"parse_string with binary",
         fun() ->
             {ok, Config} = flurm_config_slurm:parse_string(<<"ClusterName=test">>),
             ?assertEqual(<<"test">>, maps:get(clustername, Config))
         end},
        {"parse_string with list",
         fun() ->
             {ok, Config} = flurm_config_slurm:parse_string("SlurmdPort=6818"),
             ?assertEqual(6818, maps:get(slurmdport, Config))
         end},
        {"parse_string with multiple lines",
         fun() ->
             Input = <<"ClusterName=prod\nSlurmctldPort=6817\nSlurmdPort=6818">>,
             {ok, Config} = flurm_config_slurm:parse_string(Input),
             ?assertEqual(<<"prod">>, maps:get(clustername, Config)),
             ?assertEqual(6817, maps:get(slurmctldport, Config)),
             ?assertEqual(6818, maps:get(slurmdport, Config))
         end}
     ]}.

%%====================================================================
%% Parse Line Tests
%%====================================================================

parse_line_test_() ->
    {"parse_line tests",
     [
        {"parse_line with key=value",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"ClusterName=mytest">>),
             ?assertEqual({key_value, clustername, <<"mytest">>}, Result)
         end},
        {"parse_line with comment",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"# This is a comment">>),
             ?assertEqual(comment, Result)
         end},
        {"parse_line with empty string",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"">>),
             ?assertEqual(empty, Result)
         end},
        {"parse_line with whitespace only",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"   ">>),
             ?assertEqual(empty, Result)
         end},
        {"parse_line with invalid line",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"invalid line without equals">>),
             ?assertMatch({error, {invalid_line, _}}, Result)
         end}
     ]}.

%%====================================================================
%% Parse Trimmed Line Tests
%%====================================================================

parse_trimmed_line_test_() ->
    {"parse_trimmed_line tests",
     [
        {"parse_trimmed_line empty",
         fun() ->
             Result = flurm_config_slurm:parse_trimmed_line([]),
             ?assertEqual(empty, Result)
         end},
        {"parse_trimmed_line comment",
         fun() ->
             Result = flurm_config_slurm:parse_trimmed_line("# comment"),
             ?assertEqual(comment, Result)
         end},
        {"parse_trimmed_line key=value",
         fun() ->
             Result = flurm_config_slurm:parse_trimmed_line("Key=Value"),
             ?assertEqual({key_value, key, <<"Value">>}, Result)
         end}
     ]}.

%%====================================================================
%% Normalize Key Tests
%%====================================================================

normalize_key_test_() ->
    {"normalize_key tests",
     [
        {"normalize_key lowercase",
         fun() ->
             ?assertEqual(clustername, flurm_config_slurm:normalize_key("ClusterName")),
             ?assertEqual(slurmctldport, flurm_config_slurm:normalize_key("SlurmctldPort")),
             ?assertEqual(nodename, flurm_config_slurm:normalize_key("NodeName"))
         end},
        {"normalize_key already lowercase",
         fun() ->
             ?assertEqual(test, flurm_config_slurm:normalize_key("test")),
             ?assertEqual(value, flurm_config_slurm:normalize_key("value"))
         end},
        {"normalize_key with numbers",
         fun() ->
             ?assertEqual('key123', flurm_config_slurm:normalize_key("Key123"))
         end}
     ]}.

%%====================================================================
%% Parse Value Tests
%%====================================================================

parse_value_test_() ->
    {"parse_value tests",
     [
        {"parse_value boolean YES",
         fun() ->
             ?assertEqual(true, flurm_config_slurm:parse_value("YES")),
             ?assertEqual(true, flurm_config_slurm:parse_value("Yes")),
             ?assertEqual(true, flurm_config_slurm:parse_value("yes"))
         end},
        {"parse_value boolean NO",
         fun() ->
             ?assertEqual(false, flurm_config_slurm:parse_value("NO")),
             ?assertEqual(false, flurm_config_slurm:parse_value("No")),
             ?assertEqual(false, flurm_config_slurm:parse_value("no"))
         end},
        {"parse_value boolean TRUE/FALSE",
         fun() ->
             ?assertEqual(true, flurm_config_slurm:parse_value("TRUE")),
             ?assertEqual(false, flurm_config_slurm:parse_value("FALSE"))
         end},
        {"parse_value INFINITE",
         fun() ->
             ?assertEqual(infinity, flurm_config_slurm:parse_value("INFINITE"))
         end},
        {"parse_value UNLIMITED",
         fun() ->
             ?assertEqual(unlimited, flurm_config_slurm:parse_value("UNLIMITED"))
         end},
        {"parse_value integer",
         fun() ->
             ?assertEqual(42, flurm_config_slurm:parse_value("42")),
             ?assertEqual(0, flurm_config_slurm:parse_value("0")),
             ?assertEqual(12345, flurm_config_slurm:parse_value("12345"))
         end},
        {"parse_value string",
         fun() ->
             ?assertEqual(<<"hello">>, flurm_config_slurm:parse_value("hello")),
             ?assertEqual(<<"/path/to/file">>, flurm_config_slurm:parse_value("/path/to/file"))
         end}
     ]}.

%%====================================================================
%% Parse Memory Value Tests
%%====================================================================

parse_memory_value_test_() ->
    {"parse_memory_value tests",
     [
        {"parse_memory_value plain number",
         fun() ->
             ?assertEqual({ok, 1000}, flurm_config_slurm:parse_memory_value("1000"))
         end},
        {"parse_memory_value with K suffix",
         fun() ->
             ?assertEqual({ok, 1024}, flurm_config_slurm:parse_memory_value("1K")),
             ?assertEqual({ok, 2048}, flurm_config_slurm:parse_memory_value("2K"))
         end},
        {"parse_memory_value with M suffix",
         fun() ->
             ?assertEqual({ok, 1024 * 1024}, flurm_config_slurm:parse_memory_value("1M")),
             ?assertEqual({ok, 512 * 1024 * 1024}, flurm_config_slurm:parse_memory_value("512M"))
         end},
        {"parse_memory_value with G suffix",
         fun() ->
             ?assertEqual({ok, 1024 * 1024 * 1024}, flurm_config_slurm:parse_memory_value("1G")),
             ?assertEqual({ok, 128 * 1024 * 1024 * 1024}, flurm_config_slurm:parse_memory_value("128G"))
         end},
        {"parse_memory_value with T suffix",
         fun() ->
             ?assertEqual({ok, 1024 * 1024 * 1024 * 1024}, flurm_config_slurm:parse_memory_value("1T"))
         end},
        {"parse_memory_value invalid",
         fun() ->
             ?assertEqual(error, flurm_config_slurm:parse_memory_value("invalid")),
             ?assertEqual(error, flurm_config_slurm:parse_memory_value("1.5G")),
             ?assertEqual(error, flurm_config_slurm:parse_memory_value("G"))
         end}
     ]}.

%%====================================================================
%% Parse Time Value Tests
%%====================================================================

parse_time_value_test_() ->
    {"parse_time_value tests",
     [
        {"parse_time_value days format",
         fun() ->
             ?assertEqual({ok, 86400}, flurm_config_slurm:parse_time_value("1-00:00:00")),
             ?assertEqual({ok, 172800}, flurm_config_slurm:parse_time_value("2-00:00:00")),
             ?assertEqual({ok, 86400 + 3600}, flurm_config_slurm:parse_time_value("1-01:00:00"))
         end},
        {"parse_time_value hours format",
         fun() ->
             ?assertEqual({ok, 3600}, flurm_config_slurm:parse_time_value("1:00:00")),
             ?assertEqual({ok, 7200}, flurm_config_slurm:parse_time_value("2:00:00")),
             ?assertEqual({ok, 3661}, flurm_config_slurm:parse_time_value("1:01:01"))
         end},
        {"parse_time_value minutes format",
         fun() ->
             ?assertEqual({ok, 60}, flurm_config_slurm:parse_time_value("1:00")),
             ?assertEqual({ok, 90}, flurm_config_slurm:parse_time_value("1:30")),
             ?assertEqual({ok, 3599}, flurm_config_slurm:parse_time_value("59:59"))
         end},
        {"parse_time_value invalid",
         fun() ->
             ?assertEqual(error, flurm_config_slurm:parse_time_value("invalid")),
             ?assertEqual(error, flurm_config_slurm:parse_time_value("1")),
             ?assertEqual(error, flurm_config_slurm:parse_time_value("a:b:c"))
         end}
     ]}.

%%====================================================================
%% Parse HMS Tests
%%====================================================================

parse_hms_test_() ->
    {"parse_hms tests",
     [
        {"parse_hms hours:minutes:seconds",
         fun() ->
             ?assertEqual({ok, 3661}, flurm_config_slurm:parse_hms("1:01:01")),
             ?assertEqual({ok, 0}, flurm_config_slurm:parse_hms("0:00:00")),
             ?assertEqual({ok, 86399}, flurm_config_slurm:parse_hms("23:59:59"))
         end},
        {"parse_hms minutes:seconds",
         fun() ->
             ?assertEqual({ok, 61}, flurm_config_slurm:parse_hms("1:01")),
             ?assertEqual({ok, 0}, flurm_config_slurm:parse_hms("0:00")),
             ?assertEqual({ok, 3599}, flurm_config_slurm:parse_hms("59:59"))
         end},
        {"parse_hms invalid",
         fun() ->
             ?assertEqual(error, flurm_config_slurm:parse_hms("invalid")),
             ?assertEqual(error, flurm_config_slurm:parse_hms("1")),
             ?assertEqual(error, flurm_config_slurm:parse_hms("a:b")),
             ?assertEqual(error, flurm_config_slurm:parse_hms("1:2:3:4"))
         end}
     ]}.

%%====================================================================
%% Expand Hostlist Tests
%%====================================================================

expand_hostlist_test_() ->
    {"expand_hostlist tests",
     [
        {"expand_hostlist single host",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("node1"),
             ?assertEqual([<<"node1">>], Result)
         end},
        {"expand_hostlist binary single host",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist(<<"compute001">>),
             ?assertEqual([<<"compute001">>], Result)
         end},
        {"expand_hostlist simple range",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("node[1-3]"),
             ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], Result)
         end},
        {"expand_hostlist padded range",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("node[001-003]"),
             ?assertEqual([<<"node001">>, <<"node002">>, <<"node003">>], Result)
         end},
        {"expand_hostlist comma separated",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("node[1,3,5]"),
             ?assertEqual([<<"node1">>, <<"node3">>, <<"node5">>], Result)
         end},
        {"expand_hostlist mixed ranges and values",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("node[1-2,5]"),
             ?assertEqual([<<"node1">>, <<"node2">>, <<"node5">>], Result)
         end},
        {"expand_hostlist with prefix and suffix",
         fun() ->
             Result = flurm_config_slurm:expand_hostlist("rack1-node[01-02]-ib"),
             ?assertEqual([<<"rack1-node01-ib">>, <<"rack1-node02-ib">>], Result)
         end}
     ]}.

%%====================================================================
%% Expand Range Tests
%%====================================================================

expand_range_test_() ->
    {"expand_range tests",
     [
        {"expand_range simple range",
         fun() ->
             Result = flurm_config_slurm:expand_range("node", "1-3", ""),
             ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], Result)
         end},
        {"expand_range single value",
         fun() ->
             Result = flurm_config_slurm:expand_range("host", "5", ".local"),
             ?assertEqual([<<"host5.local">>], Result)
         end},
        {"expand_range padded",
         fun() ->
             Result = flurm_config_slurm:expand_range("n", "01-03", ""),
             ?assertEqual([<<"n01">>, <<"n02">>, <<"n03">>], Result)
         end}
     ]}.

%%====================================================================
%% Format Host Tests
%%====================================================================

format_host_test_() ->
    {"format_host tests",
     [
        {"format_host no padding needed",
         fun() ->
             Result = flurm_config_slurm:format_host("node", 5, 1, ""),
             ?assertEqual(<<"node5">>, Result)
         end},
        {"format_host with padding",
         fun() ->
             Result = flurm_config_slurm:format_host("node", 5, 3, ""),
             ?assertEqual(<<"node005">>, Result)
         end},
        {"format_host with suffix",
         fun() ->
             Result = flurm_config_slurm:format_host("host", 1, 2, "-ib"),
             ?assertEqual(<<"host01-ib">>, Result)
         end}
     ]}.

%%====================================================================
%% Finalize Config Tests
%%====================================================================

finalize_config_test_() ->
    {"finalize_config tests",
     [
        {"finalize_config reverses nodes",
         fun() ->
             Config = #{nodes => [node3, node2, node1]},
             Result = flurm_config_slurm:finalize_config(Config),
             ?assertEqual([node1, node2, node3], maps:get(nodes, Result))
         end},
        {"finalize_config reverses partitions",
         fun() ->
             Config = #{partitions => [part3, part2, part1]},
             Result = flurm_config_slurm:finalize_config(Config),
             ?assertEqual([part1, part2, part3], maps:get(partitions, Result))
         end},
        {"finalize_config handles missing nodes",
         fun() ->
             Config = #{other_key => value},
             Result = flurm_config_slurm:finalize_config(Config),
             ?assertEqual(value, maps:get(other_key, Result)),
             ?assertNot(maps:is_key(nodes, Result))
         end},
        {"finalize_config handles missing partitions",
         fun() ->
             Config = #{nodes => [n1]},
             Result = flurm_config_slurm:finalize_config(Config),
             ?assertEqual([n1], maps:get(nodes, Result)),
             ?assertNot(maps:is_key(partitions, Result))
         end}
     ]}.

%%====================================================================
%% Node Definition Tests
%%====================================================================

node_definition_test_() ->
    {"node definition parsing tests",
     [
        {"parse NodeName line",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"NodeName=node[1-10] CPUs=8 RealMemory=32000">>),
             ?assertMatch({node_def, _}, Result),
             {node_def, NodeDef} = Result,
             ?assertEqual(<<"node[1-10]">>, maps:get(nodename, NodeDef)),
             ?assertEqual(8, maps:get(cpus, NodeDef)),
             ?assertEqual(32000, maps:get(realmemory, NodeDef))
         end},
        {"parse NodeName with state",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"NodeName=node1 State=IDLE CPUs=4">>),
             {node_def, NodeDef} = Result,
             ?assertEqual(<<"IDLE">>, maps:get(state, NodeDef))
         end}
     ]}.

%%====================================================================
%% Partition Definition Tests
%%====================================================================

partition_definition_test_() ->
    {"partition definition parsing tests",
     [
        {"parse PartitionName line",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"PartitionName=batch Nodes=node[1-50] Default=YES">>),
             ?assertMatch({partition_def, _}, Result),
             {partition_def, PartDef} = Result,
             ?assertEqual(<<"batch">>, maps:get(partitionname, PartDef)),
             ?assertEqual(<<"node[1-50]">>, maps:get(nodes, PartDef)),
             ?assertEqual(true, maps:get(default, PartDef))
         end},
        {"parse PartitionName with MaxTime",
         fun() ->
             Result = flurm_config_slurm:parse_line(<<"PartitionName=debug Nodes=node1 MaxTime=INFINITE">>),
             {partition_def, PartDef} = Result,
             ?assertEqual(infinity, maps:get(maxtime, PartDef))
         end}
     ]}.

%%====================================================================
%% Full Config Parsing Tests
%%====================================================================

full_config_test_() ->
    {"full config parsing tests",
     [
        {"parse complete slurm.conf",
         fun() ->
             ConfigContent = <<"
ClusterName=testcluster
SlurmctldPort=6817
SlurmdPort=6818

# Node definitions
NodeName=node[001-010] CPUs=8 RealMemory=32000 State=IDLE
NodeName=gpu[1-2] CPUs=16 RealMemory=64000 Gres=gpu:4

# Partition definitions
PartitionName=batch Nodes=node[001-010] Default=YES MaxTime=7-00:00:00
PartitionName=gpu Nodes=gpu[1-2] MaxTime=1-00:00:00
">>,
             {ok, Config} = flurm_config_slurm:parse_string(ConfigContent),

             ?assertEqual(<<"testcluster">>, maps:get(clustername, Config)),
             ?assertEqual(6817, maps:get(slurmctldport, Config)),
             ?assertEqual(6818, maps:get(slurmdport, Config)),

             Nodes = maps:get(nodes, Config),
             ?assertEqual(2, length(Nodes)),

             Partitions = maps:get(partitions, Config),
             ?assertEqual(2, length(Partitions))
         end},
        {"parse config with memory suffixes",
         fun() ->
             ConfigContent = <<"
NodeName=big CPUs=64 RealMemory=512G
NodeName=small CPUs=4 RealMemory=16G
">>,
             {ok, Config} = flurm_config_slurm:parse_string(ConfigContent),
             Nodes = maps:get(nodes, Config),
             ?assertEqual(2, length(Nodes))
         end}
     ]}.
