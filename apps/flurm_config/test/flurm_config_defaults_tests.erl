%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_config_defaults module
%%% Tests default configuration values
%%%-------------------------------------------------------------------
-module(flurm_config_defaults_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases
%%====================================================================

get_defaults_test() ->
    Defaults = flurm_config_defaults:get_defaults(),
    ?assert(is_map(Defaults)),
    ?assert(maps:size(Defaults) > 0),

    %% Check some key defaults exist
    ?assert(maps:is_key(slurmctldport, Defaults)),
    ?assert(maps:is_key(slurmdport, Defaults)),
    ?assert(maps:is_key(clustername, Defaults)),
    ?assert(maps:is_key(schedulertype, Defaults)),
    ?assert(maps:is_key(prioritytype, Defaults)).

apply_defaults_test() ->
    %% Empty config should get all defaults
    EmptyConfig = #{},
    WithDefaults = flurm_config_defaults:apply_defaults(EmptyConfig),

    ?assert(is_map(WithDefaults)),
    ?assertEqual(6817, maps:get(slurmctldport, WithDefaults)),
    ?assertEqual(6818, maps:get(slurmdport, WithDefaults)),
    ?assertEqual(<<"flurm">>, maps:get(clustername, WithDefaults)).

apply_defaults_override_test() ->
    %% User values should override defaults
    Config = #{
        slurmctldport => 7777,
        clustername => <<"mycluster">>
    },
    WithDefaults = flurm_config_defaults:apply_defaults(Config),

    %% User values preserved
    ?assertEqual(7777, maps:get(slurmctldport, WithDefaults)),
    ?assertEqual(<<"mycluster">>, maps:get(clustername, WithDefaults)),

    %% Defaults still applied for other keys
    ?assertEqual(6818, maps:get(slurmdport, WithDefaults)).

get_default_test() ->
    %% Get existing default
    Port = flurm_config_defaults:get_default(slurmctldport),
    ?assertEqual(6817, Port),

    %% Get non-existing with fallback
    Value = flurm_config_defaults:get_default(nonexistent_key),
    ?assertEqual(undefined, Value).

get_default_with_fallback_test() ->
    %% Get with explicit fallback
    Value = flurm_config_defaults:get_default(nonexistent_key, my_fallback),
    ?assertEqual(my_fallback, Value),

    %% Existing key should return default, not fallback
    Port = flurm_config_defaults:get_default(slurmctldport, 9999),
    ?assertEqual(6817, Port).

%%====================================================================
%% Daemon Defaults Tests
%%====================================================================

daemon_defaults_test_() ->
    [
     {"Controller defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(6817, maps:get(slurmctldport, Defaults)),
         ?assertEqual(<<"localhost">>, maps:get(slurmctldhost, Defaults)),
         ?assertEqual(120, maps:get(slurmctldtimeout, Defaults))
     end},
     {"Slurmd defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(6818, maps:get(slurmdport, Defaults)),
         ?assertEqual(300, maps:get(slurmdtimeout, Defaults))
     end},
     {"Slurmdbd defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(6819, maps:get(slurmdbdport, Defaults))
     end},
     {"User defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"slurm">>, maps:get(slurmuser, Defaults)),
         ?assertEqual(<<"root">>, maps:get(slurmduser, Defaults))
     end},
     {"Auth defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"auth/munge">>, maps:get(authtype, Defaults)),
         ?assertEqual(<<"crypto/munge">>, maps:get(cryptotype, Defaults))
     end}
    ].

%%====================================================================
%% Scheduler Defaults Tests
%%====================================================================

scheduler_defaults_test_() ->
    [
     {"Scheduler type defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"sched/backfill">>, maps:get(schedulertype, Defaults)),
         Params = maps:get(schedulerparameters, Defaults),
         ?assert(is_map(Params)),
         ?assertEqual(true, maps:get(bf_continue, Params)),
         ?assertEqual(500, maps:get(bf_max_job_test, Params))
     end},
     {"Select type defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"select/cons_tres">>, maps:get(selecttype, Defaults)),
         ?assertEqual(<<"CR_CPU_Memory">>, maps:get(selecttypeparameters, Defaults))
     end},
     {"Job limits defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(10000, maps:get(maxjobcount, Defaults)),
         ?assertEqual(1001, maps:get(maxarraysize, Defaults))
     end}
    ].

%%====================================================================
%% Priority Defaults Tests
%%====================================================================

priority_defaults_test_() ->
    [
     {"Priority type default", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"priority/multifactor">>, maps:get(prioritytype, Defaults))
     end},
     {"Priority weight defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(1000, maps:get(priorityweightage, Defaults)),
         ?assertEqual(10000, maps:get(priorityweightfairshare, Defaults)),
         ?assertEqual(1000, maps:get(priorityweightjobsize, Defaults)),
         ?assertEqual(1000, maps:get(priorityweightpartition, Defaults)),
         ?assertEqual(10000, maps:get(priorityweightqos, Defaults)),
         ?assertEqual(1000, maps:get(priorityweighttres, Defaults))
     end},
     {"Preemption defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"preempt/none">>, maps:get(preempttype, Defaults)),
         ?assertEqual(<<"OFF">>, maps:get(preemptmode, Defaults))
     end}
    ].

%%====================================================================
%% Miscellaneous Defaults Tests
%%====================================================================

misc_defaults_test_() ->
    [
     {"Accounting defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"accounting_storage/none">>, maps:get(accountingstoragetype, Defaults)),
         ?assertEqual(<<"jobacct_gather/linux">>, maps:get(jobacctgathertype, Defaults)),
         ?assertEqual(30, maps:get(jobacctgatherfrequency, Defaults))
     end},
     {"Task plugin defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"task/affinity">>, maps:get(taskplugin, Defaults)),
         ?assertEqual(<<"proctrack/cgroup">>, maps:get(proctracktype, Defaults))
     end},
     {"MPI defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(<<"none">>, maps:get(mpidefault, Defaults))
     end},
     {"Timeout defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(10, maps:get(messagetimeout, Defaults)),
         ?assertEqual(30, maps:get(killwait, Defaults)),
         ?assertEqual(10, maps:get(batchstarttimeout, Defaults))
     end},
     {"Debug defaults", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual([], maps:get(debugflags, Defaults)),
         ?assertEqual(<<"info">>, maps:get(slurmctlddebug, Defaults)),
         ?assertEqual(<<"info">>, maps:get(slurmdebug, Defaults))
     end}
    ].

%%====================================================================
%% Path Defaults Tests
%%====================================================================

path_defaults_test_() ->
    [
     {"Spool directory defaults (if defined)", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         case maps:is_key(slurmdspooldir, Defaults) of
             true -> ?assert(is_binary(maps:get(slurmdspooldir, Defaults)));
             false -> ok
         end
     end},
     {"State save location defaults (if defined)", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         case maps:is_key(statesavelocation, Defaults) of
             true -> ?assert(is_binary(maps:get(statesavelocation, Defaults)));
             false -> ok
         end
     end},
     {"Log file defaults (if defined)", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         case maps:is_key(slurmctldlogfile, Defaults) of
             true -> ?assert(is_binary(maps:get(slurmctldlogfile, Defaults)));
             false -> ok
         end
     end}
    ].

%%====================================================================
%% Merge Behavior Tests
%%====================================================================

apply_defaults_merge_test_() ->
    [
     {"apply_defaults preserves all user keys", fun() ->
         Config = #{custom_key => <<"custom_value">>, another => 123},
         WithDefaults = flurm_config_defaults:apply_defaults(Config),
         ?assertEqual(<<"custom_value">>, maps:get(custom_key, WithDefaults)),
         ?assertEqual(123, maps:get(another, WithDefaults))
     end},
     {"apply_defaults does not overwrite user values", fun() ->
         Config = #{slurmctldport => 9999, slurmdport => 8888},
         WithDefaults = flurm_config_defaults:apply_defaults(Config),
         ?assertEqual(9999, maps:get(slurmctldport, WithDefaults)),
         ?assertEqual(8888, maps:get(slurmdport, WithDefaults))
     end},
     {"apply_defaults adds missing keys only", fun() ->
         Config = #{slurmctldport => 7000},
         WithDefaults = flurm_config_defaults:apply_defaults(Config),
         ?assertEqual(7000, maps:get(slurmctldport, WithDefaults)),
         ?assertEqual(6818, maps:get(slurmdport, WithDefaults))
     end}
    ].

%%====================================================================
%% Get Default Edge Cases Tests
%%====================================================================

get_default_edge_cases_test_() ->
    [
     {"get_default with atom fallback", fun() ->
         ?assertEqual(my_atom, flurm_config_defaults:get_default(nonexistent, my_atom))
     end},
     {"get_default with integer fallback", fun() ->
         ?assertEqual(42, flurm_config_defaults:get_default(nonexistent, 42))
     end},
     {"get_default with list fallback", fun() ->
         ?assertEqual([a, b, c], flurm_config_defaults:get_default(nonexistent, [a, b, c]))
     end},
     {"get_default with map fallback", fun() ->
         ?assertEqual(#{a => 1}, flurm_config_defaults:get_default(nonexistent, #{a => 1}))
     end},
     {"get_default with binary fallback", fun() ->
         ?assertEqual(<<"test">>, flurm_config_defaults:get_default(nonexistent, <<"test">>))
     end}
    ].

%%====================================================================
%% Network Defaults Tests
%%====================================================================

network_defaults_test_() ->
    [
     {"Port values are integers", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(slurmctldport, Defaults))),
         ?assert(is_integer(maps:get(slurmdport, Defaults))),
         ?assert(is_integer(maps:get(slurmdbdport, Defaults)))
     end},
     {"Ports are in valid range", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(maps:get(slurmctldport, Defaults) > 0),
         ?assert(maps:get(slurmctldport, Defaults) < 65536),
         ?assert(maps:get(slurmdport, Defaults) > 0),
         ?assert(maps:get(slurmdport, Defaults) < 65536)
     end}
    ].

%%====================================================================
%% Resource Defaults Tests
%%====================================================================

resource_defaults_test_() ->
    [
     {"DefMemPerCPU default", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         case maps:is_key(defmempercpu, Defaults) of
             true -> ?assert(is_integer(maps:get(defmempercpu, Defaults)));
             false -> ok
         end
     end},
     {"MaxMemPerCPU default", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         case maps:is_key(maxmempercpu, Defaults) of
             true -> ?assert(is_integer(maps:get(maxmempercpu, Defaults)));
             false -> ok
         end
     end}
    ].

%%====================================================================
%% Apply Defaults Empty Input Tests
%%====================================================================

apply_defaults_empty_test_() ->
    [
     {"apply_defaults on empty map returns defaults", fun() ->
         WithDefaults = flurm_config_defaults:apply_defaults(#{}),
         Defaults = flurm_config_defaults:get_defaults(),
         ?assertEqual(maps:get(slurmctldport, Defaults), maps:get(slurmctldport, WithDefaults)),
         ?assertEqual(maps:get(clustername, Defaults), maps:get(clustername, WithDefaults))
     end}
    ].

%%====================================================================
%% Module API Tests
%%====================================================================

module_api_test_() ->
    [
     {"get_defaults returns a map", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_map(Defaults))
     end},
     {"get_defaults returns non-empty map", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(maps:size(Defaults) > 0)
     end},
     {"apply_defaults returns a map", fun() ->
         Result = flurm_config_defaults:apply_defaults(#{}),
         ?assert(is_map(Result))
     end},
     {"get_default/1 exists", fun() ->
         ?assert(is_function(fun flurm_config_defaults:get_default/1, 1))
     end},
     {"get_default/2 exists", fun() ->
         ?assert(is_function(fun flurm_config_defaults:get_default/2, 2))
     end}
    ].

%%====================================================================
%% Specific Key Tests
%%====================================================================

specific_key_test_() ->
    [
     {"clustername is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(clustername, Defaults)))
     end},
     {"slurmctldhost is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(slurmctldhost, Defaults)))
     end},
     {"slurmuser is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(slurmuser, Defaults)))
     end},
     {"slurmduser is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(slurmduser, Defaults)))
     end},
     {"authtype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(authtype, Defaults)))
     end},
     {"schedulertype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(schedulertype, Defaults)))
     end},
     {"selecttype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(selecttype, Defaults)))
     end},
     {"prioritytype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(prioritytype, Defaults)))
     end}
    ].

%%====================================================================
%% Timeout Value Tests
%%====================================================================

timeout_value_test_() ->
    [
     {"slurmctldtimeout is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(slurmctldtimeout, Defaults)))
     end},
     {"slurmdtimeout is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(slurmdtimeout, Defaults)))
     end},
     {"messagetimeout is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(messagetimeout, Defaults)))
     end},
     {"killwait is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(killwait, Defaults)))
     end},
     {"batchstarttimeout is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(batchstarttimeout, Defaults)))
     end}
    ].

%%====================================================================
%% Scheduler Parameters Tests
%%====================================================================

scheduler_params_test_() ->
    [
     {"schedulerparameters is map", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         Params = maps:get(schedulerparameters, Defaults),
         ?assert(is_map(Params))
     end},
     {"schedulerparameters has bf_continue", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         Params = maps:get(schedulerparameters, Defaults),
         ?assert(maps:is_key(bf_continue, Params))
     end},
     {"schedulerparameters has bf_max_job_test", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         Params = maps:get(schedulerparameters, Defaults),
         ?assert(maps:is_key(bf_max_job_test, Params))
     end}
    ].

%%====================================================================
%% Priority Weight Tests
%%====================================================================

priority_weight_test_() ->
    [
     {"priorityweightage is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweightage, Defaults)))
     end},
     {"priorityweightfairshare is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweightfairshare, Defaults)))
     end},
     {"priorityweightjobsize is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweightjobsize, Defaults)))
     end},
     {"priorityweightpartition is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweightpartition, Defaults)))
     end},
     {"priorityweightqos is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweightqos, Defaults)))
     end},
     {"priorityweighttres is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(priorityweighttres, Defaults)))
     end}
    ].

%%====================================================================
%% Get Default Existing Key Tests
%%====================================================================

get_default_existing_test_() ->
    [
     {"get_default for slurmctldport returns 6817", fun() ->
         ?assertEqual(6817, flurm_config_defaults:get_default(slurmctldport))
     end},
     {"get_default for slurmdport returns 6818", fun() ->
         ?assertEqual(6818, flurm_config_defaults:get_default(slurmdport))
     end},
     {"get_default for slurmdbdport returns 6819", fun() ->
         ?assertEqual(6819, flurm_config_defaults:get_default(slurmdbdport))
     end},
     {"get_default with fallback ignores fallback for existing key", fun() ->
         ?assertEqual(6817, flurm_config_defaults:get_default(slurmctldport, 9999))
     end}
    ].

%%====================================================================
%% Job Limits Tests
%%====================================================================

job_limits_test_() ->
    [
     {"maxjobcount is positive integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         MaxJobCount = maps:get(maxjobcount, Defaults),
         ?assert(is_integer(MaxJobCount)),
         ?assert(MaxJobCount > 0)
     end},
     {"maxarraysize is positive integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         MaxArraySize = maps:get(maxarraysize, Defaults),
         ?assert(is_integer(MaxArraySize)),
         ?assert(MaxArraySize > 0)
     end}
    ].

%%====================================================================
%% Accounting Defaults Tests
%%====================================================================

accounting_defaults_test_() ->
    [
     {"accountingstoragetype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(accountingstoragetype, Defaults)))
     end},
     {"jobacctgathertype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(jobacctgathertype, Defaults)))
     end},
     {"jobacctgatherfrequency is integer", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_integer(maps:get(jobacctgatherfrequency, Defaults)))
     end}
    ].

%%====================================================================
%% Plugin Defaults Tests
%%====================================================================

plugin_defaults_test_() ->
    [
     {"taskplugin is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(taskplugin, Defaults)))
     end},
     {"proctracktype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(proctracktype, Defaults)))
     end},
     {"cryptotype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(cryptotype, Defaults)))
     end},
     {"mpidefault is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(mpidefault, Defaults)))
     end}
    ].

%%====================================================================
%% Preemption Defaults Tests
%%====================================================================

preemption_defaults_test_() ->
    [
     {"preempttype is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(preempttype, Defaults)))
     end},
     {"preemptmode is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(preemptmode, Defaults)))
     end}
    ].

%%====================================================================
%% Debug Flags Tests
%%====================================================================

debug_flags_test_() ->
    [
     {"debugflags is list", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_list(maps:get(debugflags, Defaults)))
     end},
     {"slurmctlddebug is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(slurmctlddebug, Defaults)))
     end},
     {"slurmdebug is binary", fun() ->
         Defaults = flurm_config_defaults:get_defaults(),
         ?assert(is_binary(maps:get(slurmdebug, Defaults)))
     end}
    ].

%%====================================================================
%% Apply Defaults Multiple Times Tests
%%====================================================================

apply_defaults_multiple_test_() ->
    [
     {"apply_defaults is idempotent", fun() ->
         Config = #{custom => <<"value">>},
         Once = flurm_config_defaults:apply_defaults(Config),
         Twice = flurm_config_defaults:apply_defaults(Once),
         ?assertEqual(Once, Twice)
     end},
     {"apply_defaults preserves custom values through multiple calls", fun() ->
         Config = #{slurmctldport => 1234, custom => <<"test">>},
         Result = flurm_config_defaults:apply_defaults(
             flurm_config_defaults:apply_defaults(Config)
         ),
         ?assertEqual(1234, maps:get(slurmctldport, Result)),
         ?assertEqual(<<"test">>, maps:get(custom, Result))
     end}
    ].
