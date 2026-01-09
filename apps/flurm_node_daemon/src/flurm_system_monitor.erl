%%%-------------------------------------------------------------------
%%% @doc FLURM System Monitor
%%%
%%% Collects system metrics from the compute node including CPU load,
%%% memory usage, and running processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([get_metrics/0, get_hostname/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COLLECT_INTERVAL, 5000). % 5 seconds

-record(state, {
    hostname :: binary(),
    cpus :: pos_integer(),
    total_memory_mb :: pos_integer(),
    load_avg :: float(),
    free_memory_mb :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Get current system metrics
-spec get_metrics() -> map().
get_metrics() ->
    gen_server:call(?MODULE, get_metrics).

%% @doc Get the hostname
-spec get_hostname() -> binary().
get_hostname() ->
    gen_server:call(?MODULE, get_hostname).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("System Monitor started"),

    %% Get static system info
    Hostname = list_to_binary(net_adm:localhost()),
    Cpus = erlang:system_info(logical_processors),
    TotalMemoryMB = get_total_memory_mb(),

    %% Start periodic collection
    erlang:send_after(?COLLECT_INTERVAL, self(), collect),

    {ok, #state{
        hostname = Hostname,
        cpus = Cpus,
        total_memory_mb = TotalMemoryMB,
        load_avg = 0.0,
        free_memory_mb = TotalMemoryMB
    }}.

handle_call(get_metrics, _From, State) ->
    Metrics = #{
        hostname => State#state.hostname,
        cpus => State#state.cpus,
        total_memory_mb => State#state.total_memory_mb,
        load_avg => State#state.load_avg,
        free_memory_mb => State#state.free_memory_mb
    },
    {reply, Metrics, State};

handle_call(get_hostname, _From, State) ->
    {reply, State#state.hostname, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(collect, State) ->
    LoadAvg = get_load_average(),
    FreeMemoryMB = get_free_memory_mb(),

    erlang:send_after(?COLLECT_INTERVAL, self(), collect),

    {noreply, State#state{
        load_avg = LoadAvg,
        free_memory_mb = FreeMemoryMB
    }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

get_total_memory_mb() ->
    case erlang:system_info(allocated_areas) of
        Areas when is_list(Areas) ->
            %% Rough estimate from Erlang VM
            erlang:memory(total) div (1024 * 1024);
        _ ->
            1024 % Default to 1GB
    end.

get_free_memory_mb() ->
    %% Use Erlang memory as a rough proxy
    %% In a real implementation, we would read from /proc/meminfo on Linux
    TotalErlangMem = erlang:memory(total),
    ProcessMem = erlang:memory(processes),
    (TotalErlangMem - ProcessMem) div (1024 * 1024).

get_load_average() ->
    %% Placeholder - in a real implementation, read from /proc/loadavg
    %% For now, use scheduler utilization as a proxy
    case erlang:statistics(scheduler_wall_time) of
        undefined ->
            0.0;
        _ ->
            %% Simple approximation
            RunQueue = erlang:statistics(run_queue),
            float(RunQueue)
    end.
