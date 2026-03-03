%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_diagnostics edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_diagnostics_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

diagnostics_tail_cov_test_() ->
    [
     {"health_check warning branch (high memory)",
      fun health_check_warning_branch_test/0},
     {"health_check critical branch (critical memory)",
      fun health_check_critical_branch_test/0},
     {"health_check queue warning branch",
      fun health_check_large_queue_warning_branch_test/0},
     {"health_check high process count warning branch",
      fun health_check_high_process_count_warning_branch_test/0},
     {"deterministic process_info undefined branches",
      fun process_info_undefined_branches_test/0},
     {"ets_report table_gone catch branch",
      fun ets_report_table_gone_branch_test/0}
    ].

health_check_warning_branch_test() ->
    Old = os:getenv("FLURM_MEMORY_LIMIT"),
    Mem = erlang:memory(total),
    %% Set limit so memory lands in warning range (>80%, <95%).
    Limit = trunc(Mem / 0.85),
    os:putenv("FLURM_MEMORY_LIMIT", integer_to_list(Limit)),
    Result = flurm_diagnostics:health_check(),
    restore_mem_limit(Old),
    ?assertMatch({warning, _}, Result),
    {warning, Warnings} = Result,
    ?assert(lists:any(fun({high_memory, _, _}) -> true;
                         (_) -> false
                      end, Warnings)).

health_check_critical_branch_test() ->
    Old = os:getenv("FLURM_MEMORY_LIMIT"),
    os:putenv("FLURM_MEMORY_LIMIT", "1"),
    Result = flurm_diagnostics:health_check(),
    restore_mem_limit(Old),
    ?assertMatch({critical, _}, Result),
    {critical, Criticals} = Result,
    ?assert(lists:any(fun({critical_memory, _, _}) -> true;
                         (_) -> false
                      end, Criticals)).

health_check_large_queue_warning_branch_test() ->
    Old = os:getenv("FLURM_MEMORY_LIMIT"),
    %% Keep memory checks out of warning/critical territory.
    os:putenv("FLURM_MEMORY_LIMIT", "1099511627776"), %% 1TB
    Blocker = spawn(fun queue_blocker/0),
    lists:foreach(fun(_) -> Blocker ! msg end, lists:seq(1, 11000)),
    timer:sleep(20),
    Result = flurm_diagnostics:health_check(),
    Blocker ! stop,
    restore_mem_limit(Old),
    ?assertMatch({warning, _}, Result),
    {warning, Warnings} = Result,
    ?assert(lists:any(fun({large_message_queues, N}) when is_integer(N), N > 0 -> true;
                         (_) -> false
                      end, Warnings)).

health_check_high_process_count_warning_branch_test() ->
    OldMemLimit = os:getenv("FLURM_MEMORY_LIMIT"),
    OldSysFun = application:get_env(flurm_core, diagnostics_system_info_fun),
    OldProcFun = application:get_env(flurm_core, diagnostics_processes_fun),
    os:putenv("FLURM_MEMORY_LIMIT", "1099511627776"), %% 1TB
    application:set_env(flurm_core, diagnostics_system_info_fun,
        fun(process_count) -> 9;
           (process_limit) -> 10;
           (Key) -> erlang:system_info(Key)
        end),
    application:set_env(flurm_core, diagnostics_processes_fun, fun() -> [] end),
    try
        Result = flurm_diagnostics:health_check(),
        ?assertMatch({warning, _}, Result),
        {warning, Warnings} = Result,
        ?assert(lists:any(fun({high_process_count, 9, 10}) -> true;
                             (_) -> false
                          end, Warnings))
    after
        restore_mem_limit(OldMemLimit),
        restore_app_env(diagnostics_system_info_fun, OldSysFun),
        restore_app_env(diagnostics_processes_fun, OldProcFun)
    end.

process_info_undefined_branches_test() ->
    OldProcFun = application:get_env(flurm_core, diagnostics_processes_fun),
    OldInfoFun = application:get_env(flurm_core, diagnostics_process_info_fun),
    application:set_env(flurm_core, diagnostics_processes_fun,
                        fun() -> [missing, small, large] end),
    application:set_env(flurm_core, diagnostics_process_info_fun,
                        fun
                            (missing, _) ->
                                undefined;
                            (small, [message_queue_len, registered_name, current_function]) ->
                                [{message_queue_len, 5},
                                 {registered_name, small_proc},
                                 {current_function, {m, f, 0}}];
                            (large, [message_queue_len, registered_name, current_function]) ->
                                [{message_queue_len, 150},
                                 {registered_name, large_proc},
                                 {current_function, {m, f, 0}}];
                            (small, [binary, registered_name, memory]) ->
                                [{binary, [{ref_small, 2 * 1024 * 1024, 1}]},
                                 {registered_name, small_proc},
                                 {memory, 111}];
                            (large, [binary, registered_name, memory]) ->
                                [{binary, [{ref_large, 3 * 1024 * 1024, 1}]},
                                 {registered_name, large_proc},
                                 {memory, 222}];
                            (small, [memory, registered_name, current_function, message_queue_len]) ->
                                [{memory, 1000},
                                 {registered_name, small_proc},
                                 {current_function, {m, f, 0}},
                                 {message_queue_len, 5}];
                            (large, [memory, registered_name, current_function, message_queue_len]) ->
                                [{memory, 2000},
                                 {registered_name, large_proc},
                                 {current_function, {m, f, 0}},
                                 {message_queue_len, 150}]
                        end),
    try
        QueueReport = flurm_diagnostics:message_queue_report(),
        ?assertEqual(1, length(QueueReport)),
        ?assertEqual(large_proc, maps:get(name, hd(QueueReport))),

        BinaryReport = flurm_diagnostics:binary_leak_check(),
        ?assertEqual(2, length(BinaryReport)),
        ?assertEqual(large_proc, maps:get(name, hd(BinaryReport))),

        TopMem = flurm_diagnostics:top_memory_processes(5),
        ?assertEqual(2, length(TopMem)),

        TopQueues = flurm_diagnostics:top_message_queue_processes(5),
        ?assertEqual(2, length(TopQueues))
    after
        restore_app_env(diagnostics_processes_fun, OldProcFun),
        restore_app_env(diagnostics_process_info_fun, OldInfoFun)
    end.

ets_report_table_gone_branch_test() ->
    ChurnPid = spawn(fun ets_churn_loop/0),
    Found = find_table_gone(300),
    exit(ChurnPid, kill),
    ?assertEqual(true, Found).

restore_mem_limit(false) ->
    os:unsetenv("FLURM_MEMORY_LIMIT");
restore_mem_limit(Value) when is_list(Value) ->
    os:putenv("FLURM_MEMORY_LIMIT", Value).

restore_app_env(Key, undefined) ->
    application:unset_env(flurm_core, Key);
restore_app_env(Key, {ok, Value}) ->
    application:set_env(flurm_core, Key, Value).

queue_blocker() ->
    receive
        stop -> ok
    end.

find_table_gone(0) ->
    false;
find_table_gone(N) ->
    Report = flurm_diagnostics:ets_report(),
    case lists:any(fun(E) ->
        maps:get(error, E, none) =:= table_gone
    end, Report) of
        true -> true;
        false ->
            timer:sleep(2),
            find_table_gone(N - 1)
    end.

ets_churn_loop() ->
    _ = spawn(fun() ->
        Tab = ets:new(churn, [set, public]),
        ets:insert(Tab, {k, v}),
        timer:sleep(1)
    end),
    ets_churn_loop().
