%%%-------------------------------------------------------------------
%%% @doc FLURM Config Defaults 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_config_defaults module covering:
%%% - get_defaults/0
%%% - apply_defaults/1
%%% - get_default/1
%%% - get_default/2
%%% - All internal default categories
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_defaults_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Get Defaults Tests
%%====================================================================

get_defaults_test_() ->
    {"get_defaults tests",
     [
        {"get_defaults returns a map",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             ?assert(is_map(Defaults))
         end},
        {"get_defaults contains daemon settings",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             ?assert(maps:is_key(slurmctldport, Defaults)),
             ?assertEqual(6817, maps:get(slurmctldport, Defaults)),
             ?assert(maps:is_key(slurmdport, Defaults)),
             ?assertEqual(6818, maps:get(slurmdport, Defaults)),
             ?assert(maps:is_key(slurmdbdport, Defaults)),
             ?assertEqual(6819, maps:get(slurmdbdport, Defaults))
         end},
        {"get_defaults contains scheduler settings",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             ?assertEqual(<<"sched/backfill">>, maps:get(schedulertype, Defaults)),
             ?assertEqual(30, maps:get(schedulerintervalsec, Defaults)),
             ?assertEqual(10000, maps:get(maxjobcount, Defaults))
         end},
        {"get_defaults contains priority settings",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             ?assertEqual(<<"priority/multifactor">>, maps:get(prioritytype, Defaults)),
             ?assertEqual(1000, maps:get(priorityweightage, Defaults)),
             ?assertEqual(10000, maps:get(priorityweightfairshare, Defaults))
         end},
        {"get_defaults contains misc settings",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             ?assertEqual(<<"accounting_storage/none">>, maps:get(accountingstoragetype, Defaults)),
             ?assertEqual(<<"switch/none">>, maps:get(switchtype, Defaults)),
             ?assertEqual(<<"task/affinity">>, maps:get(taskplugin, Defaults))
         end}
     ]}.

%%====================================================================
%% Apply Defaults Tests
%%====================================================================

apply_defaults_test_() ->
    {"apply_defaults tests",
     [
        {"apply_defaults to empty config",
         fun() ->
             Result = flurm_config_defaults:apply_defaults(#{}),
             ?assert(is_map(Result)),
             %% Should have all default values
             ?assertEqual(6817, maps:get(slurmctldport, Result)),
             ?assertEqual(<<"flurm">>, maps:get(clustername, Result))
         end},
        {"apply_defaults preserves existing values",
         fun() ->
             Config = #{slurmctldport => 9999, custom_key => custom_value},
             Result = flurm_config_defaults:apply_defaults(Config),

             %% Custom values should be preserved
             ?assertEqual(9999, maps:get(slurmctldport, Result)),
             ?assertEqual(custom_value, maps:get(custom_key, Result)),
             %% Other defaults should be applied
             ?assertEqual(6818, maps:get(slurmdport, Result))
         end},
        {"apply_defaults does not override any existing keys",
         fun() ->
             %% Set all commonly used keys to custom values
             Config = #{
                 slurmctldport => 1111,
                 slurmdport => 2222,
                 slurmdbdport => 3333,
                 clustername => <<"mytest">>,
                 schedulertype => <<"custom/sched">>
             },
             Result = flurm_config_defaults:apply_defaults(Config),

             ?assertEqual(1111, maps:get(slurmctldport, Result)),
             ?assertEqual(2222, maps:get(slurmdport, Result)),
             ?assertEqual(3333, maps:get(slurmdbdport, Result)),
             ?assertEqual(<<"mytest">>, maps:get(clustername, Result)),
             ?assertEqual(<<"custom/sched">>, maps:get(schedulertype, Result))
         end}
     ]}.

%%====================================================================
%% Get Default Tests
%%====================================================================

get_default_test_() ->
    {"get_default tests",
     [
        {"get_default/1 for existing key",
         fun() ->
             ?assertEqual(6817, flurm_config_defaults:get_default(slurmctldport)),
             ?assertEqual(6818, flurm_config_defaults:get_default(slurmdport)),
             ?assertEqual(<<"flurm">>, flurm_config_defaults:get_default(clustername))
         end},
        {"get_default/1 for non-existing key returns undefined",
         fun() ->
             ?assertEqual(undefined, flurm_config_defaults:get_default(nonexistent_key)),
             ?assertEqual(undefined, flurm_config_defaults:get_default(random_key_xyz))
         end},
        {"get_default/2 for existing key ignores fallback",
         fun() ->
             ?assertEqual(6817, flurm_config_defaults:get_default(slurmctldport, 9999)),
             ?assertEqual(<<"flurm">>, flurm_config_defaults:get_default(clustername, <<"other">>))
         end},
        {"get_default/2 for non-existing key returns fallback",
         fun() ->
             ?assertEqual(my_fallback, flurm_config_defaults:get_default(nonexistent, my_fallback)),
             ?assertEqual(12345, flurm_config_defaults:get_default(unknown, 12345)),
             ?assertEqual([], flurm_config_defaults:get_default(missing, []))
         end}
     ]}.

%%====================================================================
%% Daemon Defaults Tests
%%====================================================================

daemon_defaults_test_() ->
    {"daemon_defaults tests",
     [
        {"daemon_defaults contains controller settings",
         fun() ->
             Defaults = flurm_config_defaults:daemon_defaults(),
             ?assertEqual(6817, maps:get(slurmctldport, Defaults)),
             ?assertEqual(<<"localhost">>, maps:get(slurmctldhost, Defaults)),
             ?assertEqual(<<"/var/run/flurm/slurmctld.pid">>, maps:get(slurmctldpidfile, Defaults)),
             ?assertEqual(<<"/var/log/flurm/slurmctld.log">>, maps:get(slurmctldlogfile, Defaults)),
             ?assertEqual(<<"/var/spool/flurm">>, maps:get(statesavelocation, Defaults)),
             ?assertEqual(120, maps:get(slurmctldtimeout, Defaults))
         end},
        {"daemon_defaults contains slurmd settings",
         fun() ->
             Defaults = flurm_config_defaults:daemon_defaults(),
             ?assertEqual(6818, maps:get(slurmdport, Defaults)),
             ?assertEqual(<<"/var/run/flurm/slurmd.pid">>, maps:get(slurmdpidfile, Defaults)),
             ?assertEqual(<<"/var/log/flurm/slurmd.log">>, maps:get(slurmdlogfile, Defaults)),
             ?assertEqual(<<"/var/spool/flurm/d">>, maps:get(slurmdspooldir, Defaults)),
             ?assertEqual(300, maps:get(slurmdtimeout, Defaults))
         end},
        {"daemon_defaults contains authentication settings",
         fun() ->
             Defaults = flurm_config_defaults:daemon_defaults(),
             ?assertEqual(<<"auth/munge">>, maps:get(authtype, Defaults)),
             ?assertEqual(<<"crypto/munge">>, maps:get(cryptotype, Defaults)),
             ?assertEqual(<<"slurm">>, maps:get(slurmuser, Defaults)),
             ?assertEqual(<<"root">>, maps:get(slurmduser, Defaults))
         end},
        {"daemon_defaults contains return to service",
         fun() ->
             Defaults = flurm_config_defaults:daemon_defaults(),
             ?assertEqual(1, maps:get(returntoservice, Defaults))
         end}
     ]}.

%%====================================================================
%% Scheduler Defaults Tests
%%====================================================================

scheduler_defaults_test_() ->
    {"scheduler_defaults tests",
     [
        {"scheduler_defaults contains scheduler type",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             ?assertEqual(<<"sched/backfill">>, maps:get(schedulertype, Defaults))
         end},
        {"scheduler_defaults contains scheduler parameters",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             Params = maps:get(schedulerparameters, Defaults),
             ?assert(is_map(Params)),
             ?assertEqual(true, maps:get(bf_continue, Params)),
             ?assertEqual(500, maps:get(bf_max_job_test, Params)),
             ?assertEqual(86400, maps:get(bf_window, Params))
         end},
        {"scheduler_defaults contains intervals",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             ?assertEqual(30, maps:get(schedulerintervalsec, Defaults)),
             ?assertEqual(300, maps:get(minjobage, Defaults))
         end},
        {"scheduler_defaults contains job limits",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             ?assertEqual(10000, maps:get(maxjobcount, Defaults)),
             ?assertEqual(1001, maps:get(maxarraysize, Defaults))
         end},
        {"scheduler_defaults contains resource selection",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             ?assertEqual(<<"select/cons_tres">>, maps:get(selecttype, Defaults)),
             ?assertEqual(<<"CR_CPU_Memory">>, maps:get(selecttypeparameters, Defaults))
         end},
        {"scheduler_defaults contains kill settings",
         fun() ->
             Defaults = flurm_config_defaults:scheduler_defaults(),
             ?assertEqual(30, maps:get(killwait, Defaults)),
             ?assertEqual(false, maps:get(killonbadexit, Defaults))
         end}
     ]}.

%%====================================================================
%% Priority Defaults Tests
%%====================================================================

priority_defaults_test_() ->
    {"priority_defaults tests",
     [
        {"priority_defaults contains priority type",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(<<"priority/multifactor">>, maps:get(prioritytype, Defaults))
         end},
        {"priority_defaults contains all weights",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(1000, maps:get(priorityweightage, Defaults)),
             ?assertEqual(10000, maps:get(priorityweightfairshare, Defaults)),
             ?assertEqual(1000, maps:get(priorityweightjobsize, Defaults)),
             ?assertEqual(1000, maps:get(priorityweightpartition, Defaults)),
             ?assertEqual(10000, maps:get(priorityweightqos, Defaults)),
             ?assertEqual(1000, maps:get(priorityweighttres, Defaults))
         end},
        {"priority_defaults contains decay settings",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(<<"7-0">>, maps:get(prioritydecayhalflife, Defaults)),
             ?assertEqual(<<"7-0">>, maps:get(prioritymaxage, Defaults))
         end},
        {"priority_defaults contains preemption settings",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(<<"preempt/none">>, maps:get(preempttype, Defaults)),
             ?assertEqual(<<"OFF">>, maps:get(preemptmode, Defaults)),
             ?assertEqual(<<"0">>, maps:get(preemptexempttime, Defaults))
         end},
        {"priority_defaults contains fair share settings",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(1, maps:get(fairshareddampeningfactor, Defaults)),
             ?assertEqual(1.0, maps:get(usagefactor, Defaults))
         end},
        {"priority_defaults contains priority flags",
         fun() ->
             Defaults = flurm_config_defaults:priority_defaults(),
             ?assertEqual(false, maps:get(priorityfavorsmall, Defaults)),
             ?assertEqual(5, maps:get(prioritycalcperiod, Defaults))
         end}
     ]}.

%%====================================================================
%% Misc Defaults Tests
%%====================================================================

misc_defaults_test_() ->
    {"misc_defaults tests",
     [
        {"misc_defaults contains accounting settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(<<"accounting_storage/none">>, maps:get(accountingstoragetype, Defaults)),
             ?assertEqual(undefined, maps:get(accountingstoragehost, Defaults)),
             ?assertEqual(6819, maps:get(accountingstorageport, Defaults)),
             ?assertEqual(<<"jobacct_gather/linux">>, maps:get(jobacctgathertype, Defaults)),
             ?assertEqual(30, maps:get(jobacctgatherfrequency, Defaults))
         end},
        {"misc_defaults contains plugin settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(<<"switch/none">>, maps:get(switchtype, Defaults)),
             ?assertEqual(<<"task/affinity">>, maps:get(taskplugin, Defaults)),
             ?assertEqual(<<"proctrack/cgroup">>, maps:get(proctracktype, Defaults)),
             ?assertEqual(<<"none">>, maps:get(mpidefault, Defaults)),
             ?assertEqual(<<"topology/none">>, maps:get(topologyplugin, Defaults))
         end},
        {"misc_defaults contains logging settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual([], maps:get(debugflags, Defaults)),
             ?assertEqual(<<"info">>, maps:get(slurmctlddebug, Defaults)),
             ?assertEqual(<<"info">>, maps:get(slurmdebug, Defaults))
         end},
        {"misc_defaults contains timeout settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(10, maps:get(messagetimeout, Defaults)),
             ?assertEqual(10, maps:get(batchstarttimeout, Defaults)),
             ?assertEqual(0, maps:get(completewait, Defaults)),
             ?assertEqual(2000, maps:get(epilogmsgtimeout, Defaults))
         end},
        {"misc_defaults contains health check settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(0, maps:get(healthcheckinterval, Defaults)),
             ?assertEqual(undefined, maps:get(healthcheckprogram, Defaults))
         end},
        {"misc_defaults contains requeue and step settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(true, maps:get(jobrequeue, Defaults)),
             ?assertEqual(40000, maps:get(maxstepcnt, Defaults))
         end},
        {"misc_defaults contains resource limit settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(<<"ALL">>, maps:get(propagateresourcelimits, Defaults)),
             ?assertEqual([], maps:get(prologflags, Defaults)),
             ?assertEqual(0, maps:get(vsizefactor, Defaults))
         end},
        {"misc_defaults contains TCP settings",
         fun() ->
             Defaults = flurm_config_defaults:misc_defaults(),
             ?assertEqual(0, maps:get(tcpbuffersize, Defaults))
         end}
     ]}.

%%====================================================================
%% Complete Coverage Tests
%%====================================================================

complete_coverage_test_() ->
    {"complete coverage tests",
     [
        {"all defaults are merged correctly",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),

             %% Check that defaults from all categories are present
             %% Daemon
             ?assert(maps:is_key(slurmctldport, Defaults)),
             %% Scheduler
             ?assert(maps:is_key(schedulertype, Defaults)),
             %% Priority
             ?assert(maps:is_key(prioritytype, Defaults)),
             %% Misc
             ?assert(maps:is_key(accountingstoragetype, Defaults))
         end},
        {"defaults count is reasonable",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             Size = maps:size(Defaults),
             %% Should have a substantial number of defaults
             ?assert(Size > 50)
         end},
        {"no nil or undefined values in required defaults",
         fun() ->
             Defaults = flurm_config_defaults:get_defaults(),
             %% These keys should always have concrete values
             RequiredKeys = [slurmctldport, slurmdport, clustername, schedulertype],
             lists:foreach(fun(Key) ->
                 Value = maps:get(Key, Defaults),
                 ?assertNotEqual(undefined, Value),
                 ?assertNotEqual(nil, Value)
             end, RequiredKeys)
         end}
     ]}.
