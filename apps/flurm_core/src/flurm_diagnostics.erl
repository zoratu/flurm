%%%-------------------------------------------------------------------
%%% @doc FLURM Runtime Diagnostics
%%%
%%% Provides tools for detecting memory leaks, process leaks, and
%%% other long-running bugs. Can be used interactively or as part
%%% of automated health checks.
%%%
%%% Usage:
%%%   flurm_diagnostics:memory_report().
%%%   flurm_diagnostics:process_report().
%%%   flurm_diagnostics:start_leak_detector(60000). % Check every 60s
%%%   flurm_diagnostics:ets_report().
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics).

-behaviour(gen_server).

-export([
    %% One-shot diagnostics
    memory_report/0,
    process_report/0,
    ets_report/0,
    message_queue_report/0,
    binary_leak_check/0,
    full_report/0,

    %% Continuous monitoring
    start_leak_detector/0,
    start_leak_detector/1,
    stop_leak_detector/0,
    get_leak_history/0,

    %% Process inspection
    top_memory_processes/0,
    top_memory_processes/1,
    top_message_queue_processes/0,
    top_message_queue_processes/1,

    %% Health check for automation
    health_check/0
]).

%% gen_server exports
-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    interval :: pos_integer(),
    history :: [snapshot()],
    max_history :: pos_integer(),
    alerts :: [alert()]
}).

-record(snapshot, {
    timestamp :: integer(),
    total_memory :: non_neg_integer(),
    process_count :: non_neg_integer(),
    ets_memory :: non_neg_integer(),
    binary_memory :: non_neg_integer(),
    atom_count :: non_neg_integer(),
    port_count :: non_neg_integer()
}).

-type snapshot() :: #snapshot{}.

-record(alert, {
    timestamp :: integer(),
    type :: memory_growth | process_leak | binary_leak | message_queue,
    details :: term()
}).

-type alert() :: #alert{}.

-define(DEFAULT_INTERVAL, 60000).  % 1 minute
-define(MAX_HISTORY, 1440).        % 24 hours at 1 min intervals
-define(MEMORY_GROWTH_THRESHOLD, 1.5).  % 50% growth triggers alert
-define(PROCESS_GROWTH_THRESHOLD, 1.2). % 20% process growth
-define(MESSAGE_QUEUE_THRESHOLD, 10000). % Messages in queue

%%%===================================================================
%%% One-shot Diagnostics
%%%===================================================================

%% @doc Get a comprehensive memory report
-spec memory_report() -> map().
memory_report() ->
    Memory = erlang:memory(),
    #{
        total => proplists:get_value(total, Memory),
        processes => proplists:get_value(processes, Memory),
        processes_used => proplists:get_value(processes_used, Memory),
        system => proplists:get_value(system, Memory),
        atom => proplists:get_value(atom, Memory),
        atom_used => proplists:get_value(atom_used, Memory),
        binary => proplists:get_value(binary, Memory),
        ets => proplists:get_value(ets, Memory),
        code => proplists:get_value(code, Memory)
    }.

%% @doc Get process statistics
-spec process_report() -> map().
process_report() ->
    ProcessCount = erlang:system_info(process_count),
    ProcessLimit = erlang:system_info(process_limit),
    #{
        count => ProcessCount,
        limit => ProcessLimit,
        utilization => ProcessCount / ProcessLimit * 100,
        registered => length(registered())
    }.

%% @doc Get ETS table statistics
-spec ets_report() -> [map()].
ets_report() ->
    Tables = ets:all(),
    lists:map(fun(Tab) ->
        try
            Info = ets:info(Tab),
            #{
                name => proplists:get_value(name, Info),
                size => proplists:get_value(size, Info),
                memory => proplists:get_value(memory, Info) * erlang:system_info(wordsize),
                type => proplists:get_value(type, Info),
                owner => proplists:get_value(owner, Info)
            }
        catch
            _:_ -> #{name => Tab, error => table_gone}
        end
    end, Tables).

%% @doc Find processes with large message queues
-spec message_queue_report() -> [map()].
message_queue_report() ->
    Processes = erlang:processes(),
    WithQueues = lists:filtermap(fun(Pid) ->
        case erlang:process_info(Pid, [message_queue_len, registered_name, current_function]) of
            undefined -> false;
            Info ->
                QueueLen = proplists:get_value(message_queue_len, Info, 0),
                case QueueLen > 100 of  % Only report if > 100 messages
                    true ->
                        {true, #{
                            pid => Pid,
                            queue_len => QueueLen,
                            name => proplists:get_value(registered_name, Info, undefined),
                            current_function => proplists:get_value(current_function, Info)
                        }};
                    false ->
                        false
                end
        end
    end, Processes),
    lists:sort(fun(A, B) ->
        maps:get(queue_len, A) > maps:get(queue_len, B)
    end, WithQueues).

%% @doc Check for binary memory leaks (large binaries held by processes)
-spec binary_leak_check() -> [map()].
binary_leak_check() ->
    Processes = erlang:processes(),
    WithBinaries = lists:filtermap(fun(Pid) ->
        case erlang:process_info(Pid, [binary, registered_name, memory]) of
            undefined -> false;
            Info ->
                Binaries = proplists:get_value(binary, Info, []),
                BinarySize = lists:sum([Size || {_, Size, _} <- Binaries]),
                case BinarySize > 1024 * 1024 of  % > 1MB of binaries
                    true ->
                        {true, #{
                            pid => Pid,
                            binary_size => BinarySize,
                            binary_count => length(Binaries),
                            name => proplists:get_value(registered_name, Info, undefined),
                            heap_size => proplists:get_value(memory, Info, 0)
                        }};
                    false ->
                        false
                end
        end
    end, Processes),
    lists:sort(fun(A, B) ->
        maps:get(binary_size, A) > maps:get(binary_size, B)
    end, WithBinaries).

%% @doc Get top N processes by memory usage
-spec top_memory_processes() -> [map()].
top_memory_processes() ->
    top_memory_processes(20).

-spec top_memory_processes(pos_integer()) -> [map()].
top_memory_processes(N) ->
    Processes = erlang:processes(),
    WithMemory = lists:filtermap(fun(Pid) ->
        case erlang:process_info(Pid, [memory, registered_name, current_function, message_queue_len]) of
            undefined -> false;
            Info ->
                {true, #{
                    pid => Pid,
                    memory => proplists:get_value(memory, Info, 0),
                    name => proplists:get_value(registered_name, Info, undefined),
                    current_function => proplists:get_value(current_function, Info),
                    message_queue_len => proplists:get_value(message_queue_len, Info, 0)
                }}
        end
    end, Processes),
    Sorted = lists:sort(fun(A, B) ->
        maps:get(memory, A) > maps:get(memory, B)
    end, WithMemory),
    lists:sublist(Sorted, N).

%% @doc Get top N processes by message queue length
-spec top_message_queue_processes() -> [map()].
top_message_queue_processes() ->
    top_message_queue_processes(20).

-spec top_message_queue_processes(pos_integer()) -> [map()].
top_message_queue_processes(N) ->
    Processes = erlang:processes(),
    WithQueues = lists:filtermap(fun(Pid) ->
        case erlang:process_info(Pid, [message_queue_len, registered_name, current_function]) of
            undefined -> false;
            Info ->
                {true, #{
                    pid => Pid,
                    message_queue_len => proplists:get_value(message_queue_len, Info, 0),
                    name => proplists:get_value(registered_name, Info, undefined),
                    current_function => proplists:get_value(current_function, Info)
                }}
        end
    end, Processes),
    Sorted = lists:sort(fun(A, B) ->
        maps:get(message_queue_len, A) > maps:get(message_queue_len, B)
    end, WithQueues),
    lists:sublist(Sorted, N).

%% @doc Generate a full diagnostic report
-spec full_report() -> map().
full_report() ->
    #{
        timestamp => erlang:system_time(millisecond),
        node => node(),
        memory => memory_report(),
        processes => process_report(),
        top_memory_processes => top_memory_processes(10),
        message_queues => message_queue_report(),
        binary_leaks => binary_leak_check(),
        ets_tables => lists:sublist(
            lists:sort(fun(A, B) ->
                maps:get(memory, A, 0) > maps:get(memory, B, 0)
            end, ets_report()),
            10
        ),
        schedulers => erlang:system_info(schedulers_online),
        uptime_seconds => element(1, erlang:statistics(wall_clock)) div 1000
    }.

%% @doc Health check returning ok | {warning, Reasons} | {critical, Reasons}
-spec health_check() -> ok | {warning, [term()]} | {critical, [term()]}.
health_check() ->
    Warnings = [],
    Criticals = [],

    %% Check memory
    Memory = erlang:memory(total),
    MemLimit = case os:getenv("FLURM_MEMORY_LIMIT") of
        false -> 8 * 1024 * 1024 * 1024;  % 8GB default
        Limit -> list_to_integer(Limit)
    end,

    MemWarnings = case Memory > MemLimit * 0.8 of
        true -> [{high_memory, Memory, MemLimit}];
        false -> []
    end,
    MemCriticals = case Memory > MemLimit * 0.95 of
        true -> [{critical_memory, Memory, MemLimit}];
        false -> []
    end,

    %% Check process count
    ProcCount = erlang:system_info(process_count),
    ProcLimit = erlang:system_info(process_limit),
    ProcWarnings = case ProcCount > ProcLimit * 0.8 of
        true -> [{high_process_count, ProcCount, ProcLimit}];
        false -> []
    end,

    %% Check message queues
    LargeQueues = [P || P <- message_queue_report(),
                        maps:get(queue_len, P) > ?MESSAGE_QUEUE_THRESHOLD],
    QueueWarnings = case LargeQueues of
        [] -> [];
        _ -> [{large_message_queues, length(LargeQueues)}]
    end,

    AllWarnings = Warnings ++ MemWarnings ++ ProcWarnings ++ QueueWarnings,
    AllCriticals = Criticals ++ MemCriticals,

    case {AllCriticals, AllWarnings} of
        {[], []} -> ok;
        {[], W} -> {warning, W};
        {C, _} -> {critical, C}
    end.

%%%===================================================================
%%% Continuous Monitoring (Leak Detector)
%%%===================================================================

%% @doc Start the leak detector with default interval (1 minute)
-spec start_leak_detector() -> {ok, pid()} | {error, term()}.
start_leak_detector() ->
    start_leak_detector(?DEFAULT_INTERVAL).

%% @doc Start the leak detector with custom interval (milliseconds)
-spec start_leak_detector(pos_integer()) -> {ok, pid()} | {error, term()}.
start_leak_detector(Interval) ->
    gen_server:start_link({local, flurm_leak_detector}, ?MODULE, [Interval], []).

%% @doc Stop the leak detector
-spec stop_leak_detector() -> ok.
stop_leak_detector() ->
    case whereis(flurm_leak_detector) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end.

%% @doc Get the history of snapshots and alerts
-spec get_leak_history() -> {ok, [snapshot()], [alert()]} | {error, not_running}.
get_leak_history() ->
    case whereis(flurm_leak_detector) of
        undefined -> {error, not_running};
        _ -> gen_server:call(flurm_leak_detector, get_history)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

start_link(Args) ->
    gen_server:start_link({local, flurm_leak_detector}, ?MODULE, Args, []).

init([Interval]) ->
    lager:info("Leak detector started with interval ~p ms", [Interval]),
    erlang:send_after(Interval, self(), take_snapshot),
    {ok, #state{
        interval = Interval,
        history = [],
        max_history = ?MAX_HISTORY,
        alerts = []
    }}.

handle_call(get_history, _From, State) ->
    {reply, {ok, State#state.history, State#state.alerts}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(take_snapshot, State) ->
    %% Take a new snapshot
    Snapshot = take_snapshot(),

    %% Check for leaks by comparing with history
    NewAlerts = check_for_leaks(Snapshot, State#state.history),

    %% Log any new alerts
    lists:foreach(fun(Alert) ->
        lager:warning("Leak detector alert: ~p - ~p",
                     [Alert#alert.type, Alert#alert.details])
    end, NewAlerts),

    %% Update history (keep max_history entries)
    NewHistory = lists:sublist([Snapshot | State#state.history], State#state.max_history),
    AllAlerts = lists:sublist(NewAlerts ++ State#state.alerts, 1000),

    %% Schedule next snapshot
    erlang:send_after(State#state.interval, self(), take_snapshot),

    {noreply, State#state{history = NewHistory, alerts = AllAlerts}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

take_snapshot() ->
    Memory = erlang:memory(),
    #snapshot{
        timestamp = erlang:system_time(millisecond),
        total_memory = proplists:get_value(total, Memory),
        process_count = erlang:system_info(process_count),
        ets_memory = proplists:get_value(ets, Memory),
        binary_memory = proplists:get_value(binary, Memory),
        atom_count = erlang:system_info(atom_count),
        port_count = erlang:system_info(port_count)
    }.

check_for_leaks(_Current, []) ->
    %% No history yet
    [];
check_for_leaks(Current, History) ->
    %% Compare with oldest snapshot to detect trends
    Oldest = lists:last(History),
    Alerts = [],

    %% Check memory growth
    MemGrowth = Current#snapshot.total_memory / max(1, Oldest#snapshot.total_memory),
    MemAlerts = case MemGrowth > ?MEMORY_GROWTH_THRESHOLD of
        true ->
            [#alert{
                timestamp = Current#snapshot.timestamp,
                type = memory_growth,
                details = #{
                    growth_factor => MemGrowth,
                    current => Current#snapshot.total_memory,
                    baseline => Oldest#snapshot.total_memory
                }
            }];
        false -> []
    end,

    %% Check process growth
    ProcGrowth = Current#snapshot.process_count / max(1, Oldest#snapshot.process_count),
    ProcAlerts = case ProcGrowth > ?PROCESS_GROWTH_THRESHOLD of
        true ->
            [#alert{
                timestamp = Current#snapshot.timestamp,
                type = process_leak,
                details = #{
                    growth_factor => ProcGrowth,
                    current => Current#snapshot.process_count,
                    baseline => Oldest#snapshot.process_count
                }
            }];
        false -> []
    end,

    %% Check binary memory growth (common source of leaks)
    BinGrowth = Current#snapshot.binary_memory / max(1, Oldest#snapshot.binary_memory),
    BinAlerts = case BinGrowth > ?MEMORY_GROWTH_THRESHOLD of
        true ->
            [#alert{
                timestamp = Current#snapshot.timestamp,
                type = binary_leak,
                details = #{
                    growth_factor => BinGrowth,
                    current => Current#snapshot.binary_memory,
                    baseline => Oldest#snapshot.binary_memory
                }
            }];
        false -> []
    end,

    Alerts ++ MemAlerts ++ ProcAlerts ++ BinAlerts.
