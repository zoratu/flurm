%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_defaults internal functions exposed via -ifdef(TEST)
%%%-------------------------------------------------------------------
-module(flurm_config_defaults_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% daemon_defaults/0 tests
%%====================================================================

daemon_defaults_test_() ->
    [
        {"returns map",
         ?_assert(is_map(flurm_config_defaults:daemon_defaults()))},
        {"contains slurmctldport",
         ?_assertEqual(6817, maps:get(slurmctldport, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmdport",
         ?_assertEqual(6818, maps:get(slurmdport, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmdbdport",
         ?_assertEqual(6819, maps:get(slurmdbdport, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmctldhost",
         ?_assertEqual(<<"localhost">>, maps:get(slurmctldhost, flurm_config_defaults:daemon_defaults()))},
        {"contains clustername",
         ?_assertEqual(<<"flurm">>, maps:get(clustername, flurm_config_defaults:daemon_defaults()))},
        {"contains authtype",
         ?_assertEqual(<<"auth/munge">>, maps:get(authtype, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmuser",
         ?_assertEqual(<<"slurm">>, maps:get(slurmuser, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmduser",
         ?_assertEqual(<<"root">>, maps:get(slurmduser, flurm_config_defaults:daemon_defaults()))},
        {"contains returntoservice",
         ?_assertEqual(1, maps:get(returntoservice, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmctldtimeout",
         ?_assertEqual(120, maps:get(slurmctldtimeout, flurm_config_defaults:daemon_defaults()))},
        {"contains slurmdtimeout",
         ?_assertEqual(300, maps:get(slurmdtimeout, flurm_config_defaults:daemon_defaults()))}
    ].

%%====================================================================
%% scheduler_defaults/0 tests
%%====================================================================

scheduler_defaults_test_() ->
    [
        {"returns map",
         ?_assert(is_map(flurm_config_defaults:scheduler_defaults()))},
        {"contains schedulertype",
         ?_assertEqual(<<"sched/backfill">>, maps:get(schedulertype, flurm_config_defaults:scheduler_defaults()))},
        {"contains schedulerparameters as map",
         fun() ->
             Params = maps:get(schedulerparameters, flurm_config_defaults:scheduler_defaults()),
             ?assert(is_map(Params)),
             ?assertEqual(true, maps:get(bf_continue, Params)),
             ?assertEqual(500, maps:get(bf_max_job_test, Params)),
             ?assertEqual(86400, maps:get(bf_window, Params))
         end},
        {"contains schedulerintervalsec",
         ?_assertEqual(30, maps:get(schedulerintervalsec, flurm_config_defaults:scheduler_defaults()))},
        {"contains maxjobcount",
         ?_assertEqual(10000, maps:get(maxjobcount, flurm_config_defaults:scheduler_defaults()))},
        {"contains maxarraysize",
         ?_assertEqual(1001, maps:get(maxarraysize, flurm_config_defaults:scheduler_defaults()))},
        {"contains selecttype",
         ?_assertEqual(<<"select/cons_tres">>, maps:get(selecttype, flurm_config_defaults:scheduler_defaults()))},
        {"contains treewidth",
         ?_assertEqual(50, maps:get(treewidth, flurm_config_defaults:scheduler_defaults()))},
        {"contains killwait",
         ?_assertEqual(30, maps:get(killwait, flurm_config_defaults:scheduler_defaults()))},
        {"contains killonbadexit",
         ?_assertEqual(false, maps:get(killonbadexit, flurm_config_defaults:scheduler_defaults()))},
        {"contains minjobage",
         ?_assertEqual(300, maps:get(minjobage, flurm_config_defaults:scheduler_defaults()))}
    ].

%%====================================================================
%% priority_defaults/0 tests
%%====================================================================

priority_defaults_test_() ->
    [
        {"returns map",
         ?_assert(is_map(flurm_config_defaults:priority_defaults()))},
        {"contains prioritytype",
         ?_assertEqual(<<"priority/multifactor">>, maps:get(prioritytype, flurm_config_defaults:priority_defaults()))},
        {"contains priorityweightage",
         ?_assertEqual(1000, maps:get(priorityweightage, flurm_config_defaults:priority_defaults()))},
        {"contains priorityweightfairshare",
         ?_assertEqual(10000, maps:get(priorityweightfairshare, flurm_config_defaults:priority_defaults()))},
        {"contains priorityweightjobsize",
         ?_assertEqual(1000, maps:get(priorityweightjobsize, flurm_config_defaults:priority_defaults()))},
        {"contains priorityweightpartition",
         ?_assertEqual(1000, maps:get(priorityweightpartition, flurm_config_defaults:priority_defaults()))},
        {"contains priorityweightqos",
         ?_assertEqual(10000, maps:get(priorityweightqos, flurm_config_defaults:priority_defaults()))},
        {"contains prioritydecayhalflife",
         ?_assertEqual(<<"7-0">>, maps:get(prioritydecayhalflife, flurm_config_defaults:priority_defaults()))},
        {"contains prioritymaxage",
         ?_assertEqual(<<"7-0">>, maps:get(prioritymaxage, flurm_config_defaults:priority_defaults()))},
        {"contains priorityfavorsmall",
         ?_assertEqual(false, maps:get(priorityfavorsmall, flurm_config_defaults:priority_defaults()))},
        {"contains preempttype",
         ?_assertEqual(<<"preempt/none">>, maps:get(preempttype, flurm_config_defaults:priority_defaults()))},
        {"contains preemptmode",
         ?_assertEqual(<<"OFF">>, maps:get(preemptmode, flurm_config_defaults:priority_defaults()))},
        {"contains usagefactor",
         ?_assertEqual(1.0, maps:get(usagefactor, flurm_config_defaults:priority_defaults()))}
    ].

%%====================================================================
%% misc_defaults/0 tests
%%====================================================================

misc_defaults_test_() ->
    [
        {"returns map",
         ?_assert(is_map(flurm_config_defaults:misc_defaults()))},
        {"contains accountingstoragetype",
         ?_assertEqual(<<"accounting_storage/none">>, maps:get(accountingstoragetype, flurm_config_defaults:misc_defaults()))},
        {"contains jobacctgathertype",
         ?_assertEqual(<<"jobacct_gather/linux">>, maps:get(jobacctgathertype, flurm_config_defaults:misc_defaults()))},
        {"contains jobacctgatherfrequency",
         ?_assertEqual(30, maps:get(jobacctgatherfrequency, flurm_config_defaults:misc_defaults()))},
        {"contains switchtype",
         ?_assertEqual(<<"switch/none">>, maps:get(switchtype, flurm_config_defaults:misc_defaults()))},
        {"contains taskplugin",
         ?_assertEqual(<<"task/affinity">>, maps:get(taskplugin, flurm_config_defaults:misc_defaults()))},
        {"contains proctracktype",
         ?_assertEqual(<<"proctrack/cgroup">>, maps:get(proctracktype, flurm_config_defaults:misc_defaults()))},
        {"contains mpidefault",
         ?_assertEqual(<<"none">>, maps:get(mpidefault, flurm_config_defaults:misc_defaults()))},
        {"contains topologyplugin",
         ?_assertEqual(<<"topology/none">>, maps:get(topologyplugin, flurm_config_defaults:misc_defaults()))},
        {"contains debugflags as empty list",
         ?_assertEqual([], maps:get(debugflags, flurm_config_defaults:misc_defaults()))},
        {"contains messagetimeout",
         ?_assertEqual(10, maps:get(messagetimeout, flurm_config_defaults:misc_defaults()))},
        {"contains jobrequeue",
         ?_assertEqual(true, maps:get(jobrequeue, flurm_config_defaults:misc_defaults()))},
        {"contains maxstepcnt",
         ?_assertEqual(40000, maps:get(maxstepcnt, flurm_config_defaults:misc_defaults()))},
        {"contains healthcheckinterval",
         ?_assertEqual(0, maps:get(healthcheckinterval, flurm_config_defaults:misc_defaults()))},
        {"contains healthcheckprogram",
         ?_assertEqual(undefined, maps:get(healthcheckprogram, flurm_config_defaults:misc_defaults()))}
    ].

%%====================================================================
%% Integration tests - verify defaults categories merge correctly
%%====================================================================

defaults_integration_test_() ->
    [
        {"all category defaults are included in get_defaults",
         fun() ->
             AllDefaults = flurm_config_defaults:get_defaults(),
             DaemonKeys = maps:keys(flurm_config_defaults:daemon_defaults()),
             SchedulerKeys = maps:keys(flurm_config_defaults:scheduler_defaults()),
             PriorityKeys = maps:keys(flurm_config_defaults:priority_defaults()),
             MiscKeys = maps:keys(flurm_config_defaults:misc_defaults()),

             %% Check all keys are present
             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, AllDefaults),
                         io_lib:format("Missing daemon key: ~p", [Key]))
             end, DaemonKeys),
             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, AllDefaults),
                         io_lib:format("Missing scheduler key: ~p", [Key]))
             end, SchedulerKeys),
             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, AllDefaults),
                         io_lib:format("Missing priority key: ~p", [Key]))
             end, PriorityKeys),
             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, AllDefaults),
                         io_lib:format("Missing misc key: ~p", [Key]))
             end, MiscKeys)
         end},
        {"no duplicate keys between categories (would cause merge issues)",
         fun() ->
             DaemonKeys = maps:keys(flurm_config_defaults:daemon_defaults()),
             SchedulerKeys = maps:keys(flurm_config_defaults:scheduler_defaults()),
             PriorityKeys = maps:keys(flurm_config_defaults:priority_defaults()),
             MiscKeys = maps:keys(flurm_config_defaults:misc_defaults()),

             AllKeysList = DaemonKeys ++ SchedulerKeys ++ PriorityKeys ++ MiscKeys,
             UniqueKeys = lists:usort(AllKeysList),
             ?assertEqual(length(UniqueKeys), length(AllKeysList),
                         "Duplicate keys found between default categories")
         end}
    ].
