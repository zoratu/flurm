%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_server internal functions exposed via -ifdef(TEST)
%%%-------------------------------------------------------------------
-module(flurm_config_server_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% find_changes/2 tests
%%====================================================================

find_changes_test_() ->
    [
        {"detects added key",
         ?_assertEqual([{new_key, undefined, value}],
                       flurm_config_server:find_changes(#{}, #{new_key => value}))},
        {"detects removed key",
         ?_assertEqual([{old_key, value, undefined}],
                       flurm_config_server:find_changes(#{old_key => value}, #{}))},
        {"detects changed value",
         ?_assertEqual([{key, old_val, new_val}],
                       flurm_config_server:find_changes(#{key => old_val}, #{key => new_val}))},
        {"returns empty list for identical configs",
         ?_assertEqual([],
                       flurm_config_server:find_changes(#{a => 1, b => 2}, #{a => 1, b => 2}))},
        {"detects multiple changes",
         fun() ->
             OldConfig = #{a => 1, b => 2},
             NewConfig = #{a => 10, c => 3},
             Changes = flurm_config_server:find_changes(OldConfig, NewConfig),
             ?assertEqual(3, length(Changes)),
             ?assert(lists:member({a, 1, 10}, Changes)),
             ?assert(lists:member({b, 2, undefined}, Changes)),
             ?assert(lists:member({c, undefined, 3}, Changes))
         end},
        {"handles empty old config",
         ?_assertEqual([{x, undefined, 1}],
                       flurm_config_server:find_changes(#{}, #{x => 1}))},
        {"handles empty new config",
         ?_assertEqual([{y, 2, undefined}],
                       flurm_config_server:find_changes(#{y => 2}, #{}))}
    ].

%%====================================================================
%% validate_port_numbers/1 tests
%%====================================================================

validate_port_numbers_test_() ->
    [
        {"accepts valid port",
         ?_assertEqual(ok, flurm_config_server:validate_port_numbers(#{slurmctldport => 6817}))},
        {"accepts multiple valid ports",
         ?_assertEqual(ok, flurm_config_server:validate_port_numbers(
             #{slurmctldport => 6817, slurmdport => 6818, slurmdbdport => 6819}))},
        {"accepts config without ports",
         ?_assertEqual(ok, flurm_config_server:validate_port_numbers(#{clustername => <<"test">>}))},
        {"accepts minimum valid port",
         ?_assertEqual(ok, flurm_config_server:validate_port_numbers(#{slurmctldport => 1}))},
        {"accepts maximum valid port",
         ?_assertEqual(ok, flurm_config_server:validate_port_numbers(#{slurmctldport => 65535}))},
        {"rejects zero port",
         ?_assertMatch({error, {invalid_ports, [slurmctldport]}},
                       flurm_config_server:validate_port_numbers(#{slurmctldport => 0}))},
        {"rejects negative port",
         ?_assertMatch({error, {invalid_ports, [slurmdport]}},
                       flurm_config_server:validate_port_numbers(#{slurmdport => -1}))},
        {"rejects port over 65535",
         ?_assertMatch({error, {invalid_ports, [slurmdbdport]}},
                       flurm_config_server:validate_port_numbers(#{slurmdbdport => 65536}))},
        {"rejects non-integer port",
         ?_assertMatch({error, {invalid_ports, [slurmctldport]}},
                       flurm_config_server:validate_port_numbers(#{slurmctldport => <<"6817">>}))}
    ].

%%====================================================================
%% validate_node_definitions/1 tests
%%====================================================================

validate_node_definitions_test_() ->
    [
        {"accepts config without nodes",
         ?_assertEqual(ok, flurm_config_server:validate_node_definitions(#{}))},
        {"accepts empty nodes list",
         ?_assertEqual(ok, flurm_config_server:validate_node_definitions(#{nodes => []}))},
        {"accepts valid node definition",
         ?_assertEqual(ok, flurm_config_server:validate_node_definitions(
             #{nodes => [#{nodename => <<"node001">>}]}))},
        {"accepts multiple valid nodes",
         ?_assertEqual(ok, flurm_config_server:validate_node_definitions(
             #{nodes => [#{nodename => <<"node001">>}, #{nodename => <<"node002">>}]}))},
        {"rejects node without nodename",
         ?_assertMatch({error, {invalid_nodes, [#{cpus := 64}]}},
                       flurm_config_server:validate_node_definitions(
                           #{nodes => [#{cpus => 64}]}))},
        {"rejects mixed valid and invalid nodes",
         ?_assertMatch({error, {invalid_nodes, [#{}]}},
                       flurm_config_server:validate_node_definitions(
                           #{nodes => [#{nodename => <<"node001">>}, #{}]}))}
    ].

%%====================================================================
%% validate_required_keys/1 tests
%%====================================================================

validate_required_keys_test_() ->
    [
        {"accepts any config (no required keys currently)",
         ?_assertEqual(ok, flurm_config_server:validate_required_keys(#{}))},
        {"accepts config with values",
         ?_assertEqual(ok, flurm_config_server:validate_required_keys(#{a => 1, b => 2}))}
    ].

%%====================================================================
%% run_validators/2 tests
%%====================================================================

run_validators_test_() ->
    AlwaysOk = fun(_) -> ok end,
    AlwaysError = fun(_) -> {error, failed} end,
    [
        {"returns ok for empty validator list",
         ?_assertEqual(ok, flurm_config_server:run_validators([], #{}))},
        {"returns ok when all validators pass",
         ?_assertEqual(ok, flurm_config_server:run_validators([AlwaysOk, AlwaysOk], #{}))},
        {"returns error on first failure",
         ?_assertEqual({error, failed},
                       flurm_config_server:run_validators([AlwaysOk, AlwaysError], #{}))},
        {"stops at first error",
         ?_assertEqual({error, failed},
                       flurm_config_server:run_validators([AlwaysError, AlwaysOk], #{}))}
    ].

%%====================================================================
%% find_node/2 tests
%%====================================================================

find_node_test_() ->
    Nodes = [
        #{nodename => <<"node[001-003]">>, cpus => 64},
        #{nodename => <<"compute[01-02]">>, cpus => 128}
    ],
    [
        {"finds node in hostlist pattern",
         ?_assertEqual(#{nodename => <<"node[001-003]">>, cpus => 64},
                       flurm_config_server:find_node(<<"node001">>, Nodes))},
        {"finds another node in same pattern",
         ?_assertEqual(#{nodename => <<"node[001-003]">>, cpus => 64},
                       flurm_config_server:find_node(<<"node003">>, Nodes))},
        {"finds node in different pattern",
         ?_assertEqual(#{nodename => <<"compute[01-02]">>, cpus => 128},
                       flurm_config_server:find_node(<<"compute01">>, Nodes))},
        {"returns undefined for non-existent node",
         ?_assertEqual(undefined,
                       flurm_config_server:find_node(<<"nonexistent">>, Nodes))},
        {"returns undefined for empty list",
         ?_assertEqual(undefined,
                       flurm_config_server:find_node(<<"node001">>, []))}
    ].

%%====================================================================
%% find_partition/2 tests
%%====================================================================

find_partition_test_() ->
    Partitions = [
        #{partitionname => <<"batch">>, default => true},
        #{partitionname => <<"debug">>, maxtime => 3600}
    ],
    [
        {"finds partition by name",
         ?_assertEqual(#{partitionname => <<"batch">>, default => true},
                       flurm_config_server:find_partition(<<"batch">>, Partitions))},
        {"finds second partition",
         ?_assertEqual(#{partitionname => <<"debug">>, maxtime => 3600},
                       flurm_config_server:find_partition(<<"debug">>, Partitions))},
        {"returns undefined for non-existent partition",
         ?_assertEqual(undefined,
                       flurm_config_server:find_partition(<<"nonexistent">>, Partitions))},
        {"returns undefined for empty list",
         ?_assertEqual(undefined,
                       flurm_config_server:find_partition(<<"batch">>, []))}
    ].
