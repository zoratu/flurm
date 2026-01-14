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
