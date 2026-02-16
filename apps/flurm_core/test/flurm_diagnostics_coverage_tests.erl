%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_diagnostics module
%%% Tests for runtime diagnostics and leak detection
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: These tests call the actual diagnostic functions which
%% examine the running Erlang VM.

%%====================================================================
%% memory_report Tests
%%====================================================================

memory_report_test() ->
    Report = flurm_diagnostics:memory_report(),
    ?assert(is_map(Report)),
    %% Check for expected keys
    ?assert(maps:is_key(total, Report)),
    ?assert(maps:is_key(processes, Report)),
    ?assert(maps:is_key(processes_used, Report)),
    ?assert(maps:is_key(system, Report)),
    ?assert(maps:is_key(atom, Report)),
    ?assert(maps:is_key(atom_used, Report)),
    ?assert(maps:is_key(binary, Report)),
    ?assert(maps:is_key(ets, Report)),
    ?assert(maps:is_key(code, Report)),
    %% All values should be non-negative integers
    maps:foreach(fun(_Key, Value) ->
        ?assert(is_integer(Value)),
        ?assert(Value >= 0)
    end, Report).

memory_report_values_reasonable_test() ->
    Report = flurm_diagnostics:memory_report(),
    Total = maps:get(total, Report),
    Processes = maps:get(processes, Report),
    ProcessesUsed = maps:get(processes_used, Report),

    %% Total should be at least a few MB
    ?assert(Total > 1000000),
    %% ProcessesUsed should be <= Processes
    ?assert(ProcessesUsed =< Processes),
    %% Total should be >= system + processes
    System = maps:get(system, Report),
    ?assert(Total >= System + Processes).

%%====================================================================
%% process_report Tests
%%====================================================================

process_report_test() ->
    Report = flurm_diagnostics:process_report(),
    ?assert(is_map(Report)),
    ?assert(maps:is_key(count, Report)),
    ?assert(maps:is_key(limit, Report)),
    ?assert(maps:is_key(utilization, Report)),
    ?assert(maps:is_key(registered, Report)).

process_report_values_test() ->
    Report = flurm_diagnostics:process_report(),
    Count = maps:get(count, Report),
    Limit = maps:get(limit, Report),
    Utilization = maps:get(utilization, Report),
    Registered = maps:get(registered, Report),

    %% Count should be positive
    ?assert(is_integer(Count)),
    ?assert(Count > 0),
    %% Limit should be higher than count
    ?assert(is_integer(Limit)),
    ?assert(Limit >= Count),
    %% Utilization should be a percentage
    ?assert(is_float(Utilization)),
    ?assert(Utilization >= 0.0),
    ?assert(Utilization =< 100.0),
    %% Registered should be non-negative
    ?assert(is_integer(Registered)),
    ?assert(Registered >= 0).

%%====================================================================
%% ets_report Tests
%%====================================================================

ets_report_test() ->
    Report = flurm_diagnostics:ets_report(),
    ?assert(is_list(Report)),
    %% Should have at least some ETS tables (e.g., from ETS internal tables)
    ?assert(length(Report) > 0).

ets_report_structure_test() ->
    Report = flurm_diagnostics:ets_report(),
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry)),
        case maps:is_key(error, Entry) of
            true ->
                %% Table was deleted between ets:all() and ets:info()
                ?assertEqual(table_gone, maps:get(error, Entry));
            false ->
                ?assert(maps:is_key(name, Entry)),
                ?assert(maps:is_key(size, Entry)),
                ?assert(maps:is_key(memory, Entry)),
                ?assert(maps:is_key(type, Entry)),
                ?assert(maps:is_key(owner, Entry))
        end
    end, Report).

%%====================================================================
%% message_queue_report Tests
%%====================================================================

message_queue_report_test() ->
    Report = flurm_diagnostics:message_queue_report(),
    ?assert(is_list(Report)),
    %% Under normal conditions, no process should have > 100 messages
    %% so the report may be empty
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry)),
        ?assert(maps:is_key(pid, Entry)),
        ?assert(maps:is_key(queue_len, Entry)),
        QueueLen = maps:get(queue_len, Entry),
        ?assert(QueueLen > 100)  % Only reported if > 100
    end, Report).

message_queue_report_sorted_test() ->
    Report = flurm_diagnostics:message_queue_report(),
    case Report of
        [] -> ok;
        [_] -> ok;
        [First | Rest] ->
            %% Check descending order
            lists:foldl(fun(Current, Prev) ->
                ?assert(maps:get(queue_len, Current) =< maps:get(queue_len, Prev)),
                Current
            end, First, Rest)
    end.

%%====================================================================
%% binary_leak_check Tests
%%====================================================================

binary_leak_check_test() ->
    Report = flurm_diagnostics:binary_leak_check(),
    ?assert(is_list(Report)),
    %% Under normal conditions, no process should hold > 1MB of binaries
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry)),
        ?assert(maps:is_key(pid, Entry)),
        ?assert(maps:is_key(binary_size, Entry)),
        ?assert(maps:is_key(binary_count, Entry)),
        ?assert(maps:is_key(heap_size, Entry)),
        BinarySize = maps:get(binary_size, Entry),
        ?assert(BinarySize > 1024 * 1024)  % Only reported if > 1MB
    end, Report).

binary_leak_check_sorted_test() ->
    Report = flurm_diagnostics:binary_leak_check(),
    case Report of
        [] -> ok;
        [_] -> ok;
        [First | Rest] ->
            lists:foldl(fun(Current, Prev) ->
                ?assert(maps:get(binary_size, Current) =< maps:get(binary_size, Prev)),
                Current
            end, First, Rest)
    end.

%%====================================================================
%% top_memory_processes Tests
%%====================================================================

top_memory_processes_default_test() ->
    Report = flurm_diagnostics:top_memory_processes(),
    ?assert(is_list(Report)),
    %% Default is 20 processes
    ?assert(length(Report) =< 20).

top_memory_processes_custom_test() ->
    Report = flurm_diagnostics:top_memory_processes(5),
    ?assert(is_list(Report)),
    ?assert(length(Report) =< 5).

top_memory_processes_large_test() ->
    Report = flurm_diagnostics:top_memory_processes(100),
    ?assert(is_list(Report)),
    ?assert(length(Report) =< 100).

top_memory_processes_structure_test() ->
    Report = flurm_diagnostics:top_memory_processes(10),
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry)),
        ?assert(maps:is_key(pid, Entry)),
        ?assert(maps:is_key(memory, Entry)),
        ?assert(maps:is_key(current_function, Entry)),
        ?assert(maps:is_key(message_queue_len, Entry)),
        Memory = maps:get(memory, Entry),
        ?assert(is_integer(Memory)),
        ?assert(Memory >= 0)
    end, Report).

top_memory_processes_sorted_test() ->
    Report = flurm_diagnostics:top_memory_processes(10),
    case Report of
        [] -> ok;
        [_] -> ok;
        [First | Rest] ->
            lists:foldl(fun(Current, Prev) ->
                ?assert(maps:get(memory, Current) =< maps:get(memory, Prev)),
                Current
            end, First, Rest)
    end.

%%====================================================================
%% top_message_queue_processes Tests
%%====================================================================

top_message_queue_processes_default_test() ->
    Report = flurm_diagnostics:top_message_queue_processes(),
    ?assert(is_list(Report)),
    ?assert(length(Report) =< 20).

top_message_queue_processes_custom_test() ->
    Report = flurm_diagnostics:top_message_queue_processes(5),
    ?assert(is_list(Report)),
    ?assert(length(Report) =< 5).

top_message_queue_processes_structure_test() ->
    Report = flurm_diagnostics:top_message_queue_processes(10),
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry)),
        ?assert(maps:is_key(pid, Entry)),
        ?assert(maps:is_key(message_queue_len, Entry)),
        ?assert(maps:is_key(current_function, Entry)),
        QueueLen = maps:get(message_queue_len, Entry),
        ?assert(is_integer(QueueLen)),
        ?assert(QueueLen >= 0)
    end, Report).

%%====================================================================
%% full_report Tests
%%====================================================================

full_report_test() ->
    Report = flurm_diagnostics:full_report(),
    ?assert(is_map(Report)),
    %% Check for expected keys
    ?assert(maps:is_key(timestamp, Report)),
    ?assert(maps:is_key(node, Report)),
    ?assert(maps:is_key(memory, Report)),
    ?assert(maps:is_key(processes, Report)),
    ?assert(maps:is_key(top_memory_processes, Report)),
    ?assert(maps:is_key(message_queues, Report)),
    ?assert(maps:is_key(binary_leaks, Report)),
    ?assert(maps:is_key(ets_tables, Report)),
    ?assert(maps:is_key(schedulers, Report)),
    ?assert(maps:is_key(uptime_seconds, Report)).

full_report_timestamp_test() ->
    Report = flurm_diagnostics:full_report(),
    Timestamp = maps:get(timestamp, Report),
    ?assert(is_integer(Timestamp)),
    ?assert(Timestamp > 0).

full_report_node_test() ->
    Report = flurm_diagnostics:full_report(),
    Node = maps:get(node, Report),
    ?assert(is_atom(Node)),
    ?assertEqual(node(), Node).

full_report_schedulers_test() ->
    Report = flurm_diagnostics:full_report(),
    Schedulers = maps:get(schedulers, Report),
    ?assert(is_integer(Schedulers)),
    ?assert(Schedulers > 0).

full_report_uptime_test() ->
    Report = flurm_diagnostics:full_report(),
    Uptime = maps:get(uptime_seconds, Report),
    ?assert(is_integer(Uptime)),
    ?assert(Uptime >= 0).

%%====================================================================
%% health_check Tests
%%====================================================================

health_check_test() ->
    Result = flurm_diagnostics:health_check(),
    case Result of
        ok -> ok;
        {warning, Warnings} ->
            ?assert(is_list(Warnings)),
            ?assert(length(Warnings) > 0);
        {critical, Criticals} ->
            ?assert(is_list(Criticals)),
            ?assert(length(Criticals) > 0)
    end.

health_check_returns_atom_or_tuple_test() ->
    Result = flurm_diagnostics:health_check(),
    Valid = case Result of
        ok -> true;
        {warning, _} -> true;
        {critical, _} -> true;
        _ -> false
    end,
    ?assert(Valid).

%%====================================================================
%% Leak Detector Tests (gen_server)
%%====================================================================

start_stop_leak_detector_test_() ->
    {setup,
     fun() ->
         %% Stop any existing leak detector
         flurm_diagnostics:stop_leak_detector(),
         timer:sleep(50)
     end,
     fun(_) ->
         flurm_diagnostics:stop_leak_detector()
     end,
     fun(_) ->
         [
             {"start default interval", fun() ->
                 {ok, Pid} = flurm_diagnostics:start_leak_detector(),
                 ?assert(is_pid(Pid)),
                 ?assert(is_process_alive(Pid)),
                 flurm_diagnostics:stop_leak_detector()
             end},
             {"start custom interval", fun() ->
                 {ok, Pid} = flurm_diagnostics:start_leak_detector(1000),
                 ?assert(is_pid(Pid)),
                 flurm_diagnostics:stop_leak_detector()
             end}
         ]
     end}.

get_leak_history_not_running_test() ->
    %% Make sure leak detector is stopped
    flurm_diagnostics:stop_leak_detector(),
    timer:sleep(50),
    Result = flurm_diagnostics:get_leak_history(),
    ?assertEqual({error, not_running}, Result).

get_leak_history_running_test_() ->
    {setup,
     fun() ->
         flurm_diagnostics:stop_leak_detector(),
         timer:sleep(50),
         {ok, _} = flurm_diagnostics:start_leak_detector(60000),
         ok
     end,
     fun(_) ->
         flurm_diagnostics:stop_leak_detector()
     end,
     fun(_) ->
         [
             {"get history when running", fun() ->
                 Result = flurm_diagnostics:get_leak_history(),
                 ?assertMatch({ok, _, _}, Result),
                 {ok, History, Alerts} = Result,
                 ?assert(is_list(History)),
                 ?assert(is_list(Alerts))
             end}
         ]
     end}.

stop_leak_detector_not_running_test() ->
    %% Stopping when not running should be ok
    flurm_diagnostics:stop_leak_detector(),
    timer:sleep(50),
    Result = flurm_diagnostics:stop_leak_detector(),
    ?assertEqual(ok, Result).

%%====================================================================
%% Edge Cases
%%====================================================================

top_memory_processes_zero_test() ->
    Report = flurm_diagnostics:top_memory_processes(0),
    ?assert(is_list(Report)),
    ?assertEqual([], Report).

top_message_queue_processes_one_test() ->
    Report = flurm_diagnostics:top_message_queue_processes(1),
    ?assert(is_list(Report)),
    ?assert(length(Report) =< 1).

%% Test that functions don't crash when called rapidly
rapid_calls_test() ->
    lists:foreach(fun(_) ->
        _ = flurm_diagnostics:memory_report(),
        _ = flurm_diagnostics:process_report()
    end, lists:seq(1, 10)).

%% Test concurrent calls
concurrent_calls_test() ->
    Parent = self(),
    Pids = [spawn(fun() ->
        Report = flurm_diagnostics:full_report(),
        Parent ! {done, self(), is_map(Report)}
    end) || _ <- lists:seq(1, 5)],

    Results = [receive {done, Pid, Result} -> Result end || Pid <- Pids],
    ?assert(lists:all(fun(R) -> R =:= true end, Results)).
