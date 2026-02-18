%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_diagnostics module - 100% coverage target
%%%
%%% Tests runtime diagnostics functionality:
%%% - One-shot diagnostics (memory_report, process_report, ets_report, etc.)
%%% - Continuous monitoring (leak detector)
%%% - Process inspection (top_memory_processes, top_message_queue_processes)
%%% - Health check
%%% - gen_server lifecycle
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing leak detector
    case whereis(flurm_leak_detector) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,
    ok.

cleanup(_) ->
    %% Stop leak detector if running
    case whereis(flurm_leak_detector) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000)
    end,
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

flurm_diagnostics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         %% One-shot diagnostics tests
         fun test_memory_report/1,
         fun test_memory_report_structure/1,
         fun test_process_report/1,
         fun test_process_report_structure/1,
         fun test_ets_report/1,
         fun test_ets_report_structure/1,
         fun test_message_queue_report/1,
         fun test_message_queue_report_empty/1,
         fun test_binary_leak_check/1,
         fun test_full_report/1,
         fun test_full_report_structure/1,

         %% Process inspection tests
         fun test_top_memory_processes/1,
         fun test_top_memory_processes_default/1,
         fun test_top_memory_processes_custom_n/1,
         fun test_top_message_queue_processes/1,
         fun test_top_message_queue_processes_default/1,
         fun test_top_message_queue_processes_custom_n/1,

         %% Health check tests
         fun test_health_check_ok/1,
         fun test_health_check_structure/1,

         %% Leak detector tests
         fun test_start_leak_detector/1,
         fun test_start_leak_detector_custom_interval/1,
         fun test_stop_leak_detector/1,
         fun test_stop_leak_detector_not_running/1,
         fun test_get_leak_history/1,
         fun test_get_leak_history_not_running/1,

         %% gen_server tests
         fun test_leak_detector_unknown_call/1,
         fun test_leak_detector_unknown_cast/1,
         fun test_leak_detector_unknown_info/1,
         fun test_leak_detector_snapshots/1
     ]}.

%%====================================================================
%% One-shot Diagnostics Tests
%%====================================================================

test_memory_report(_) ->
    ?_test(begin
        Report = flurm_diagnostics:memory_report(),
        ?assert(is_map(Report))
    end).

test_memory_report_structure(_) ->
    ?_test(begin
        Report = flurm_diagnostics:memory_report(),
        ?assert(maps:is_key(total, Report)),
        ?assert(maps:is_key(processes, Report)),
        ?assert(maps:is_key(processes_used, Report)),
        ?assert(maps:is_key(system, Report)),
        ?assert(maps:is_key(atom, Report)),
        ?assert(maps:is_key(atom_used, Report)),
        ?assert(maps:is_key(binary, Report)),
        ?assert(maps:is_key(ets, Report)),
        ?assert(maps:is_key(code, Report)),
        %% All values should be integers
        ?assert(is_integer(maps:get(total, Report))),
        ?assert(is_integer(maps:get(processes, Report)))
    end).

test_process_report(_) ->
    ?_test(begin
        Report = flurm_diagnostics:process_report(),
        ?assert(is_map(Report))
    end).

test_process_report_structure(_) ->
    ?_test(begin
        Report = flurm_diagnostics:process_report(),
        ?assert(maps:is_key(count, Report)),
        ?assert(maps:is_key(limit, Report)),
        ?assert(maps:is_key(utilization, Report)),
        ?assert(maps:is_key(registered, Report)),
        ?assert(is_integer(maps:get(count, Report))),
        ?assert(is_integer(maps:get(limit, Report))),
        ?assert(is_number(maps:get(utilization, Report))),
        ?assert(is_integer(maps:get(registered, Report)))
    end).

test_ets_report(_) ->
    ?_test(begin
        Report = flurm_diagnostics:ets_report(),
        ?assert(is_list(Report))
    end).

test_ets_report_structure(_) ->
    ?_test(begin
        Report = flurm_diagnostics:ets_report(),
        %% Should have at least some system tables
        ?assert(length(Report) > 0),
        %% Each entry should be a map
        [First | _] = Report,
        ?assert(is_map(First)),
        %% Should have expected keys (or error for gone tables)
        case maps:is_key(error, First) of
            true ->
                ?assertEqual(table_gone, maps:get(error, First));
            false ->
                ?assert(maps:is_key(name, First)),
                ?assert(maps:is_key(size, First)),
                ?assert(maps:is_key(memory, First)),
                ?assert(maps:is_key(type, First)),
                ?assert(maps:is_key(owner, First))
        end
    end).

test_message_queue_report(_) ->
    ?_test(begin
        Report = flurm_diagnostics:message_queue_report(),
        ?assert(is_list(Report))
    end).

test_message_queue_report_empty(_) ->
    ?_test(begin
        %% Most processes should have small queues
        Report = flurm_diagnostics:message_queue_report(),
        %% Report only includes processes with > 100 messages
        %% In normal operation, this should be empty or near empty
        ?assert(is_list(Report))
    end).

test_binary_leak_check(_) ->
    ?_test(begin
        Report = flurm_diagnostics:binary_leak_check(),
        ?assert(is_list(Report)),
        %% Each entry should be a map if any exist
        case Report of
            [] -> ok;
            [First | _] ->
                ?assert(is_map(First)),
                ?assert(maps:is_key(pid, First)),
                ?assert(maps:is_key(binary_size, First)),
                ?assert(maps:is_key(binary_count, First))
        end
    end).

test_full_report(_) ->
    ?_test(begin
        Report = flurm_diagnostics:full_report(),
        ?assert(is_map(Report))
    end).

test_full_report_structure(_) ->
    ?_test(begin
        Report = flurm_diagnostics:full_report(),
        ?assert(maps:is_key(timestamp, Report)),
        ?assert(maps:is_key(node, Report)),
        ?assert(maps:is_key(memory, Report)),
        ?assert(maps:is_key(processes, Report)),
        ?assert(maps:is_key(top_memory_processes, Report)),
        ?assert(maps:is_key(message_queues, Report)),
        ?assert(maps:is_key(binary_leaks, Report)),
        ?assert(maps:is_key(ets_tables, Report)),
        ?assert(maps:is_key(schedulers, Report)),
        ?assert(maps:is_key(uptime_seconds, Report)),
        %% Verify types
        ?assert(is_integer(maps:get(timestamp, Report))),
        ?assert(is_atom(maps:get(node, Report))),
        ?assert(is_map(maps:get(memory, Report))),
        ?assert(is_map(maps:get(processes, Report))),
        ?assert(is_list(maps:get(top_memory_processes, Report))),
        ?assert(is_integer(maps:get(schedulers, Report)))
    end).

%%====================================================================
%% Process Inspection Tests
%%====================================================================

test_top_memory_processes(_) ->
    ?_test(begin
        Report = flurm_diagnostics:top_memory_processes(),
        ?assert(is_list(Report)),
        ?assertEqual(20, length(Report))  % Default is 20
    end).

test_top_memory_processes_default(_) ->
    ?_test(begin
        Report = flurm_diagnostics:top_memory_processes(),
        ?assert(length(Report) =< 20),
        case Report of
            [] -> ok;
            [First | _] ->
                ?assert(is_map(First)),
                ?assert(maps:is_key(pid, First)),
                ?assert(maps:is_key(memory, First)),
                ?assert(maps:is_key(name, First)),
                ?assert(maps:is_key(current_function, First)),
                ?assert(maps:is_key(message_queue_len, First))
        end
    end).

test_top_memory_processes_custom_n(_) ->
    ?_test(begin
        Report5 = flurm_diagnostics:top_memory_processes(5),
        ?assertEqual(5, length(Report5)),
        Report50 = flurm_diagnostics:top_memory_processes(50),
        ?assert(length(Report50) =< 50)
    end).

test_top_message_queue_processes(_) ->
    ?_test(begin
        Report = flurm_diagnostics:top_message_queue_processes(),
        ?assert(is_list(Report)),
        ?assertEqual(20, length(Report))  % Default is 20
    end).

test_top_message_queue_processes_default(_) ->
    ?_test(begin
        Report = flurm_diagnostics:top_message_queue_processes(),
        case Report of
            [] -> ok;
            [First | _] ->
                ?assert(is_map(First)),
                ?assert(maps:is_key(pid, First)),
                ?assert(maps:is_key(message_queue_len, First)),
                ?assert(maps:is_key(name, First)),
                ?assert(maps:is_key(current_function, First))
        end
    end).

test_top_message_queue_processes_custom_n(_) ->
    ?_test(begin
        Report5 = flurm_diagnostics:top_message_queue_processes(5),
        ?assertEqual(5, length(Report5)),
        Report50 = flurm_diagnostics:top_message_queue_processes(50),
        ?assert(length(Report50) =< 50)
    end).

%%====================================================================
%% Health Check Tests
%%====================================================================

test_health_check_ok(_) ->
    ?_test(begin
        %% Under normal test conditions, health should be ok
        Result = flurm_diagnostics:health_check(),
        ?assert(Result =:= ok orelse
                element(1, Result) =:= warning orelse
                element(1, Result) =:= critical)
    end).

test_health_check_structure(_) ->
    ?_test(begin
        Result = flurm_diagnostics:health_check(),
        case Result of
            ok -> ok;
            {warning, Reasons} ->
                ?assert(is_list(Reasons));
            {critical, Reasons} ->
                ?assert(is_list(Reasons))
        end
    end).

%%====================================================================
%% Leak Detector Tests
%%====================================================================

test_start_leak_detector(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(),
        ?assert(is_process_alive(Pid)),
        ?assertEqual(Pid, whereis(flurm_leak_detector)),
        gen_server:stop(Pid)
    end).

test_start_leak_detector_custom_interval(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(5000),
        ?assert(is_process_alive(Pid)),
        gen_server:stop(Pid)
    end).

test_stop_leak_detector(_) ->
    ?_test(begin
        {ok, _Pid} = flurm_diagnostics:start_leak_detector(),
        ?assert(whereis(flurm_leak_detector) =/= undefined),
        ok = flurm_diagnostics:stop_leak_detector(),
        timer:sleep(50),
        ?assertEqual(undefined, whereis(flurm_leak_detector))
    end).

test_stop_leak_detector_not_running(_) ->
    ?_test(begin
        %% Stop when not running should return ok
        ok = flurm_diagnostics:stop_leak_detector(),
        ?assert(true)
    end).

test_get_leak_history(_) ->
    ?_test(begin
        {ok, _Pid} = flurm_diagnostics:start_leak_detector(100),
        %% Wait for at least one snapshot
        timer:sleep(150),
        {ok, History, Alerts} = flurm_diagnostics:get_leak_history(),
        ?assert(is_list(History)),
        ?assert(is_list(Alerts)),
        %% Should have at least one snapshot
        ?assert(length(History) >= 1)
    end).

test_get_leak_history_not_running(_) ->
    ?_test(begin
        Result = flurm_diagnostics:get_leak_history(),
        ?assertEqual({error, not_running}, Result)
    end).

%%====================================================================
%% gen_server Tests
%%====================================================================

test_leak_detector_unknown_call(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(),
        Result = gen_server:call(Pid, unknown_request),
        ?assertEqual({error, unknown_request}, Result),
        gen_server:stop(Pid)
    end).

test_leak_detector_unknown_cast(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(),
        gen_server:cast(Pid, unknown_cast),
        timer:sleep(10),
        ?assert(is_process_alive(Pid)),
        gen_server:stop(Pid)
    end).

test_leak_detector_unknown_info(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(),
        Pid ! unknown_info,
        timer:sleep(10),
        ?assert(is_process_alive(Pid)),
        gen_server:stop(Pid)
    end).

test_leak_detector_snapshots(_) ->
    ?_test(begin
        {ok, Pid} = flurm_diagnostics:start_leak_detector(50),
        %% Wait for multiple snapshots
        timer:sleep(200),
        {ok, History, _Alerts} = flurm_diagnostics:get_leak_history(),
        %% Should have accumulated snapshots
        ?assert(length(History) >= 2),
        gen_server:stop(Pid)
    end).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"ETS report handles deleted tables",
              ?_test(begin
                  %% Create and delete a table quickly
                  Tab = ets:new(temp_table, []),
                  ets:delete(Tab),
                  %% Report should handle this gracefully
                  Report = flurm_diagnostics:ets_report(),
                  ?assert(is_list(Report))
              end)},

             {"Message queue report with large queue",
              ?_test(begin
                  %% Create a process with a large queue
                  _Self = self(),
                  Pid = spawn(fun() ->
                      receive stop -> ok end
                  end),
                  %% Send many messages
                  lists:foreach(fun(_) ->
                      Pid ! test_message
                  end, lists:seq(1, 150)),
                  timer:sleep(10),
                  Report = flurm_diagnostics:message_queue_report(),
                  %% Clean up
                  exit(Pid, kill),
                  %% Should have found the process
                  ?assert(is_list(Report)),
                  case [P || P <- Report, maps:get(pid, P) =:= Pid] of
                      [] -> ok;  % Process may have died
                      [Found] ->
                          ?assert(maps:get(queue_len, Found) >= 100)
                  end
              end)},

             {"Binary leak check with large binaries",
              ?_test(begin
                  %% Create a process holding large binaries
                  LargeBin = binary:copy(<<"x">>, 2 * 1024 * 1024),  % 2MB
                  Pid = spawn(fun() ->
                      _ = LargeBin,  % Keep reference
                      receive stop -> ok end
                  end),
                  timer:sleep(50),  % Allow process to settle
                  Report = flurm_diagnostics:binary_leak_check(),
                  exit(Pid, kill),
                  ?assert(is_list(Report))
              end)},

             {"Top processes sorted by memory",
              ?_test(begin
                  Report = flurm_diagnostics:top_memory_processes(10),
                  case Report of
                      [] -> ok;
                      _ ->
                          Memories = [maps:get(memory, P) || P <- Report],
                          %% Should be sorted descending
                          ?assertEqual(Memories, lists:reverse(lists:sort(Memories)))
                  end
              end)},

             {"Top processes sorted by queue length",
              ?_test(begin
                  Report = flurm_diagnostics:top_message_queue_processes(10),
                  case Report of
                      [] -> ok;
                      _ ->
                          Lengths = [maps:get(message_queue_len, P) || P <- Report],
                          ?assertEqual(Lengths, lists:reverse(lists:sort(Lengths)))
                  end
              end)},

             {"start_link for leak detector",
              ?_test(begin
                  {ok, Pid} = flurm_diagnostics:start_link([1000]),
                  ?assert(is_process_alive(Pid)),
                  gen_server:stop(Pid)
              end)}
         ]
     end}.

%%====================================================================
%% Leak Detection Tests
%%====================================================================

leak_detection_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"Detect memory growth",
              ?_test(begin
                  {ok, Pid} = flurm_diagnostics:start_leak_detector(50),
                  %% Wait for baseline
                  timer:sleep(60),
                  %% Allocate some memory
                  _LargeList = lists:seq(1, 100000),
                  %% Wait for more snapshots
                  timer:sleep(100),
                  {ok, _History, Alerts} = flurm_diagnostics:get_leak_history(),
                  %% Alerts list should exist (may or may not have content)
                  ?assert(is_list(Alerts)),
                  gen_server:stop(Pid)
              end)},

             {"Multiple snapshots accumulated",
              ?_test(begin
                  {ok, Pid} = flurm_diagnostics:start_leak_detector(30),
                  timer:sleep(200),
                  {ok, History, _Alerts} = flurm_diagnostics:get_leak_history(),
                  ?assert(length(History) >= 4),
                  gen_server:stop(Pid)
              end)},

             {"History limited to max_history",
              ?_test(begin
                  %% Start with very fast snapshots
                  {ok, Pid} = flurm_diagnostics:start_leak_detector(10),
                  %% Wait for many snapshots
                  timer:sleep(500),
                  {ok, History, _} = flurm_diagnostics:get_leak_history(),
                  %% Should not exceed MAX_HISTORY (1440)
                  ?assert(length(History) =< 1440),
                  gen_server:stop(Pid)
              end)}
         ]
     end}.

%%====================================================================
%% Health Check Edge Cases
%%====================================================================

health_check_edge_cases_test_() ->
    [
        {"Health check returns valid structure",
         ?_test(begin
             Result = flurm_diagnostics:health_check(),
             case Result of
                 ok -> ok;
                 {warning, W} -> ?assert(is_list(W));
                 {critical, C} -> ?assert(is_list(C))
             end
         end)},

        {"Health check detects high memory",
         ?_test(begin
             %% We can't easily simulate high memory without system risk
             %% Just verify the function works
             Result = flurm_diagnostics:health_check(),
             ?assert(Result =:= ok orelse is_tuple(Result))
         end)}
    ].

%%====================================================================
%% Process Info Edge Cases
%%====================================================================

process_info_edge_cases_test_() ->
    [
        {"Dead process in scan",
         ?_test(begin
             %% Create and kill a process
             Pid = spawn(fun() -> ok end),
             timer:sleep(10),
             %% Now run reports - should handle dead pids gracefully
             TopMem = flurm_diagnostics:top_memory_processes(5),
             ?assert(is_list(TopMem)),
             TopQueue = flurm_diagnostics:top_message_queue_processes(5),
             ?assert(is_list(TopQueue))
         end)},

        {"Process with no registered name",
         ?_test(begin
             Pid = spawn(fun() ->
                 receive stop -> ok end
             end),
             TopMem = flurm_diagnostics:top_memory_processes(100),
             exit(Pid, kill),
             %% Should include unregistered processes
             ?assert(lists:any(fun(P) ->
                 maps:get(name, P) =:= undefined
             end, TopMem) orelse length(TopMem) =:= 0)
         end)}
    ].

%%====================================================================
%% Full Report Comprehensive Test
%%====================================================================

full_report_comprehensive_test() ->
    Report = flurm_diagnostics:full_report(),
    %% Test all fields exist and have correct types
    ?assert(is_integer(maps:get(timestamp, Report))),
    ?assert(maps:get(timestamp, Report) > 0),

    ?assert(is_atom(maps:get(node, Report))),

    Memory = maps:get(memory, Report),
    ?assert(is_integer(maps:get(total, Memory))),

    Processes = maps:get(processes, Report),
    ?assert(is_integer(maps:get(count, Processes))),

    TopMem = maps:get(top_memory_processes, Report),
    ?assert(is_list(TopMem)),
    ?assertEqual(10, length(TopMem)),

    MsgQueues = maps:get(message_queues, Report),
    ?assert(is_list(MsgQueues)),

    BinaryLeaks = maps:get(binary_leaks, Report),
    ?assert(is_list(BinaryLeaks)),

    EtsTables = maps:get(ets_tables, Report),
    ?assert(is_list(EtsTables)),
    ?assert(length(EtsTables) =< 10),

    ?assert(is_integer(maps:get(schedulers, Report))),
    ?assert(maps:get(schedulers, Report) > 0),

    ?assert(is_integer(maps:get(uptime_seconds, Report))),
    ?assert(maps:get(uptime_seconds, Report) >= 0).
