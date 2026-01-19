%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_slurm internal functions exposed via -ifdef(TEST)
%%%-------------------------------------------------------------------
-module(flurm_config_slurm_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% parse_trimmed_line/1 tests
%%====================================================================

parse_trimmed_line_test_() ->
    [
        {"parses empty line",
         ?_assertEqual(empty, flurm_config_slurm:parse_trimmed_line([]))},
        {"parses comment line",
         ?_assertEqual(comment, flurm_config_slurm:parse_trimmed_line("# This is a comment"))},
        {"parses simple key=value",
         ?_assertEqual({key_value, clustername, <<"mycluster">>},
                       flurm_config_slurm:parse_trimmed_line("ClusterName=mycluster"))},
        {"parses key=value with port number",
         ?_assertEqual({key_value, slurmctldport, 6817},
                       flurm_config_slurm:parse_trimmed_line("SlurmctldPort=6817"))},
        {"parses key=value with boolean YES",
         ?_assertEqual({key_value, default, true},
                       flurm_config_slurm:parse_trimmed_line("Default=YES"))},
        {"returns error for invalid line",
         ?_assertMatch({error, {invalid_line, _}},
                       flurm_config_slurm:parse_trimmed_line("invalid line no equals"))}
    ].

%%====================================================================
%% normalize_key/1 tests
%%====================================================================

normalize_key_test_() ->
    [
        {"normalizes uppercase to lowercase atom",
         ?_assertEqual(clustername, flurm_config_slurm:normalize_key("ClusterName"))},
        {"normalizes mixed case",
         ?_assertEqual(slurmctldport, flurm_config_slurm:normalize_key("SlurmctldPort"))},
        {"preserves lowercase",
         ?_assertEqual(port, flurm_config_slurm:normalize_key("port"))},
        {"handles all uppercase",
         ?_assertEqual(cpus, flurm_config_slurm:normalize_key("CPUS"))}
    ].

%%====================================================================
%% parse_value/1 tests
%%====================================================================

parse_value_test_() ->
    [
        {"parses YES to true",
         ?_assertEqual(true, flurm_config_slurm:parse_value("YES"))},
        {"parses Yes to true",
         ?_assertEqual(true, flurm_config_slurm:parse_value("Yes"))},
        {"parses yes to true",
         ?_assertEqual(true, flurm_config_slurm:parse_value("yes"))},
        {"parses NO to false",
         ?_assertEqual(false, flurm_config_slurm:parse_value("NO"))},
        {"parses No to false",
         ?_assertEqual(false, flurm_config_slurm:parse_value("No"))},
        {"parses no to false",
         ?_assertEqual(false, flurm_config_slurm:parse_value("no"))},
        {"parses TRUE to true",
         ?_assertEqual(true, flurm_config_slurm:parse_value("TRUE"))},
        {"parses FALSE to false",
         ?_assertEqual(false, flurm_config_slurm:parse_value("FALSE"))},
        {"parses INFINITE to infinity",
         ?_assertEqual(infinity, flurm_config_slurm:parse_value("INFINITE"))},
        {"parses UNLIMITED to unlimited",
         ?_assertEqual(unlimited, flurm_config_slurm:parse_value("UNLIMITED"))},
        {"parses integer",
         ?_assertEqual(64, flurm_config_slurm:parse_value("64"))},
        {"parses string as binary",
         ?_assertEqual(<<"mycluster">>, flurm_config_slurm:parse_value("mycluster"))}
    ].

%%====================================================================
%% parse_memory_value/1 tests
%%====================================================================

parse_memory_value_test_() ->
    [
        {"parses plain number",
         ?_assertEqual({ok, 1024}, flurm_config_slurm:parse_memory_value("1024"))},
        {"parses kilobytes",
         ?_assertEqual({ok, 1024}, flurm_config_slurm:parse_memory_value("1K"))},
        {"parses megabytes",
         ?_assertEqual({ok, 1048576}, flurm_config_slurm:parse_memory_value("1M"))},
        {"parses gigabytes",
         ?_assertEqual({ok, 1073741824}, flurm_config_slurm:parse_memory_value("1G"))},
        {"parses terabytes",
         ?_assertEqual({ok, 1099511627776}, flurm_config_slurm:parse_memory_value("1T"))},
        {"parses 128G",
         ?_assertEqual({ok, 137438953472}, flurm_config_slurm:parse_memory_value("128G"))},
        {"returns error for invalid format",
         ?_assertEqual(error, flurm_config_slurm:parse_memory_value("invalid"))},
        {"returns error for negative number",
         ?_assertEqual(error, flurm_config_slurm:parse_memory_value("-100"))},
        {"parses zero",
         ?_assertEqual({ok, 0}, flurm_config_slurm:parse_memory_value("0"))}
    ].

%%====================================================================
%% parse_time_value/1 tests
%%====================================================================

parse_time_value_test_() ->
    [
        {"parses days-hours:minutes:seconds format",
         ?_assertEqual({ok, 86400}, flurm_config_slurm:parse_time_value("1-00:00:00"))},
        {"parses 2 days",
         ?_assertEqual({ok, 172800}, flurm_config_slurm:parse_time_value("2-00:00:00"))},
        {"parses 1 day 1 hour",
         ?_assertEqual({ok, 90000}, flurm_config_slurm:parse_time_value("1-01:00:00"))},
        {"parses hours:minutes:seconds without days",
         ?_assertEqual({ok, 3600}, flurm_config_slurm:parse_time_value("01:00:00"))},
        {"parses 2 hours",
         ?_assertEqual({ok, 7200}, flurm_config_slurm:parse_time_value("02:00:00"))},
        {"parses minutes:seconds only",
         ?_assertEqual({ok, 150}, flurm_config_slurm:parse_time_value("02:30"))},
        {"parses complex time",
         ?_assertEqual({ok, 90061}, flurm_config_slurm:parse_time_value("1-01:01:01"))},
        {"returns error for invalid format",
         ?_assertEqual(error, flurm_config_slurm:parse_time_value("invalid"))}
    ].

%%====================================================================
%% parse_hms/1 tests
%%====================================================================

parse_hms_test_() ->
    [
        {"parses hours:minutes:seconds",
         ?_assertEqual({ok, 3661}, flurm_config_slurm:parse_hms("01:01:01"))},
        {"parses minutes:seconds",
         ?_assertEqual({ok, 61}, flurm_config_slurm:parse_hms("01:01"))},
        {"parses zero time",
         ?_assertEqual({ok, 0}, flurm_config_slurm:parse_hms("00:00:00"))},
        {"parses large hours",
         ?_assertEqual({ok, 36000}, flurm_config_slurm:parse_hms("10:00:00"))},
        {"returns error for invalid",
         ?_assertEqual(error, flurm_config_slurm:parse_hms("invalid"))},
        {"returns error for single value",
         ?_assertEqual(error, flurm_config_slurm:parse_hms("30"))}
    ].

%%====================================================================
%% expand_range/3 tests
%%====================================================================

expand_range_test_() ->
    [
        {"expands simple range",
         ?_assertEqual([<<"node001">>, <<"node002">>, <<"node003">>],
                       flurm_config_slurm:expand_range("node", "001-003", ""))},
        {"expands single number",
         ?_assertEqual([<<"node005">>],
                       flurm_config_slurm:expand_range("node", "005", ""))},
        {"expands with suffix",
         ?_assertEqual([<<"node01-eth0">>, <<"node02-eth0">>],
                       flurm_config_slurm:expand_range("node", "01-02", "-eth0"))},
        {"expands range without padding",
         ?_assertEqual([<<"n1">>, <<"n2">>, <<"n3">>],
                       flurm_config_slurm:expand_range("n", "1-3", ""))},
        {"preserves zero padding width",
         ?_assertEqual([<<"host0001">>, <<"host0002">>],
                       flurm_config_slurm:expand_range("host", "0001-0002", ""))}
    ].

%%====================================================================
%% format_host/4 tests
%%====================================================================

format_host_test_() ->
    [
        {"formats host with padding",
         ?_assertEqual(<<"node001">>,
                       flurm_config_slurm:format_host("node", 1, 3, ""))},
        {"formats host without padding needed",
         ?_assertEqual(<<"node123">>,
                       flurm_config_slurm:format_host("node", 123, 3, ""))},
        {"formats host with suffix",
         ?_assertEqual(<<"host01-ib">>,
                       flurm_config_slurm:format_host("host", 1, 2, "-ib"))},
        {"formats host with larger width",
         ?_assertEqual(<<"n00005">>,
                       flurm_config_slurm:format_host("n", 5, 5, ""))},
        {"formats host with no prefix",
         ?_assertEqual(<<"001">>,
                       flurm_config_slurm:format_host("", 1, 3, ""))}
    ].

%%====================================================================
%% finalize_config/1 tests
%%====================================================================

finalize_config_test_() ->
    [
        {"reverses nodes list",
         ?_assertEqual(#{nodes => [a, b, c]},
                       flurm_config_slurm:finalize_config(#{nodes => [c, b, a]}))},
        {"reverses partitions list",
         ?_assertEqual(#{partitions => [x, y, z]},
                       flurm_config_slurm:finalize_config(#{partitions => [z, y, x]}))},
        {"handles config without nodes or partitions",
         ?_assertEqual(#{clustername => <<"test">>},
                       flurm_config_slurm:finalize_config(#{clustername => <<"test">>}))},
        {"handles empty config",
         ?_assertEqual(#{}, flurm_config_slurm:finalize_config(#{}))},
        {"reverses both nodes and partitions",
         ?_assertEqual(#{nodes => [a, b], partitions => [x, y]},
                       flurm_config_slurm:finalize_config(#{nodes => [b, a], partitions => [y, x]}))}
    ].
