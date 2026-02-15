%%%-------------------------------------------------------------------
%%% @doc FLURM Diagnostics Coverage Tests
%%%
%%% Cover-effective EUnit tests for the flurm_diagnostics module.
%%% Tests pure functions that don't require mocking.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Pure Functions Tests
%%====================================================================

memory_report_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"memory_report returns map", fun test_memory_report/0},
        {"memory_report keys valid", fun test_memory_report_keys/0}
     ]}.

test_memory_report() ->
    Report = flurm_diagnostics:memory_report(),
    ?assert(is_map(Report)),
    ?assert(maps:size(Report) > 0).

test_memory_report_keys() ->
    Report = flurm_diagnostics:memory_report(),
    ExpectedKeys = [total, processes, system, binary, ets, code],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Report))
    end, ExpectedKeys).

%%====================================================================
%% Process Report Tests
%%====================================================================

process_report_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"process_report returns map", fun test_process_report/0},
        {"process_report has count", fun test_process_report_count/0}
     ]}.

test_process_report() ->
    Report = flurm_diagnostics:process_report(),
    ?assert(is_map(Report)).

test_process_report_count() ->
    Report = flurm_diagnostics:process_report(),
    ?assert(maps:is_key(count, Report)),
    Count = maps:get(count, Report),
    ?assert(is_integer(Count) andalso Count > 0).

%%====================================================================
%% ETS Report Tests
%%====================================================================

ets_report_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"ets_report returns list", fun test_ets_report/0},
        {"ets_report entries are maps", fun test_ets_report_entries/0}
     ]}.

test_ets_report() ->
    Report = flurm_diagnostics:ets_report(),
    ?assert(is_list(Report)).

test_ets_report_entries() ->
    Report = flurm_diagnostics:ets_report(),
    lists:foreach(fun(Entry) ->
        ?assert(is_map(Entry))
    end, Report).

%%====================================================================
%% Top Processes Tests
%%====================================================================

top_processes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"top_memory_processes returns list", fun test_top_memory/0},
        {"top_memory_processes respects N", fun test_top_memory_n/0},
        {"top_message_queue_processes returns list", fun test_top_queue/0}
     ]}.

test_top_memory() ->
    Result = flurm_diagnostics:top_memory_processes(),
    ?assert(is_list(Result)),
    ?assert(length(Result) =< 20).

test_top_memory_n() ->
    Result = flurm_diagnostics:top_memory_processes(5),
    ?assert(is_list(Result)),
    ?assert(length(Result) =< 5).

test_top_queue() ->
    Result = flurm_diagnostics:top_message_queue_processes(),
    ?assert(is_list(Result)).

%%====================================================================
%% Full Report Tests
%%====================================================================

full_report_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"full_report returns map", fun test_full_report/0},
        {"full_report has all sections", fun test_full_report_sections/0}
     ]}.

test_full_report() ->
    Report = flurm_diagnostics:full_report(),
    ?assert(is_map(Report)).

test_full_report_sections() ->
    Report = flurm_diagnostics:full_report(),
    ExpectedKeys = [memory, processes, ets_tables, top_memory_processes, message_queues, timestamp],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Report))
    end, ExpectedKeys).

%%====================================================================
%% Health Check Tests
%%====================================================================

health_check_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"health_check returns valid result", fun test_health_check/0}
     ]}.

test_health_check() ->
    Result = flurm_diagnostics:health_check(),
    ?assert(Result =:= ok orelse
            (is_tuple(Result) andalso
             (element(1, Result) =:= warning orelse element(1, Result) =:= critical))).

%%====================================================================
%% Message Queue Report Tests
%%====================================================================

message_queue_report_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"message_queue_report returns list", fun test_message_queue_report/0}
     ]}.

test_message_queue_report() ->
    Result = flurm_diagnostics:message_queue_report(),
    ?assert(is_list(Result)).

%%====================================================================
%% Binary Leak Check Tests
%%====================================================================

binary_leak_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"binary_leak_check returns list", fun test_binary_leak_check/0}
     ]}.

test_binary_leak_check() ->
    Result = flurm_diagnostics:binary_leak_check(),
    ?assert(is_list(Result)).

%%====================================================================
%% Leak Detector Tests
%%====================================================================

leak_detector_test_() ->
    {foreach,
     fun() ->
         setup(),
         %% Stop any running detector
         case whereis(flurm_leak_detector) of
             undefined -> ok;
             Pid -> catch gen_server:stop(Pid, normal, 1000)
         end
     end,
     fun cleanup/1,
     [
        {"start_leak_detector works", fun test_start_leak_detector/0},
        {"stop_leak_detector works", fun test_stop_leak_detector/0},
        {"get_leak_history when not running", fun test_get_history_not_running/0}
     ]}.

test_start_leak_detector() ->
    case flurm_diagnostics:start_leak_detector(60000) of
        {ok, Pid} ->
            ?assert(is_pid(Pid)),
            gen_server:stop(Pid, normal, 1000);
        {error, {already_started, _}} ->
            ok
    end.

test_stop_leak_detector() ->
    Result = flurm_diagnostics:stop_leak_detector(),
    ?assertEqual(ok, Result).

test_get_history_not_running() ->
    Result = flurm_diagnostics:get_leak_history(),
    ?assertEqual({error, not_running}, Result).
