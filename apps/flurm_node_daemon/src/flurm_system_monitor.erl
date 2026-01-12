%%%-------------------------------------------------------------------
%%% @doc FLURM System Monitor
%%%
%%% Collects system metrics from the compute node including CPU load,
%%% memory usage, disk space, GPUs, and running processes.
%%%
%%% On Linux, reads from /proc filesystem for accurate metrics.
%%% Falls back to Erlang VM metrics on other platforms.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_system_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([get_metrics/0, get_hostname/0, get_gpus/0, get_disk_usage/0]).
-export([get_gpu_allocation/0, allocate_gpus/2, release_gpus/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COLLECT_INTERVAL, 5000). % 5 seconds

-record(state, {
    hostname :: binary(),
    cpus :: pos_integer(),
    total_memory_mb :: pos_integer(),
    load_avg :: float(),
    load_avg_5 :: float(),
    load_avg_15 :: float(),
    free_memory_mb :: non_neg_integer(),
    cached_memory_mb :: non_neg_integer(),
    available_memory_mb :: non_neg_integer(),
    gpus :: list(),
    disk_usage :: map(),
    platform :: linux | darwin | other,
    %% GPU allocation tracking: #{GpuIndex => JobId}
    gpu_allocation :: #{non_neg_integer() => pos_integer()}
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

%% @doc Get GPU information
-spec get_gpus() -> list().
get_gpus() ->
    gen_server:call(?MODULE, get_gpus).

%% @doc Get disk usage information
-spec get_disk_usage() -> map().
get_disk_usage() ->
    gen_server:call(?MODULE, get_disk_usage).

%% @doc Get current GPU allocation (which GPUs are allocated to which jobs)
-spec get_gpu_allocation() -> #{non_neg_integer() => pos_integer()}.
get_gpu_allocation() ->
    gen_server:call(?MODULE, get_gpu_allocation).

%% @doc Allocate GPUs for a job
%% Returns list of allocated GPU indices or {error, not_enough_gpus}
-spec allocate_gpus(pos_integer(), non_neg_integer()) -> {ok, [non_neg_integer()]} | {error, term()}.
allocate_gpus(JobId, NumGpus) ->
    gen_server:call(?MODULE, {allocate_gpus, JobId, NumGpus}).

%% @doc Release GPUs allocated to a job
-spec release_gpus(pos_integer()) -> ok.
release_gpus(JobId) ->
    gen_server:cast(?MODULE, {release_gpus, JobId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("System Monitor starting"),

    Platform = detect_platform(),

    %% Get static system info
    Hostname = get_system_hostname(),
    Cpus = get_cpu_count(),
    TotalMemoryMB = get_total_memory(Platform),
    GPUs = detect_gpus(),

    lager:info("Detected: ~s, ~p CPUs, ~p MB RAM, ~p GPUs",
               [Hostname, Cpus, TotalMemoryMB, length(GPUs)]),

    %% Start periodic collection
    erlang:send_after(?COLLECT_INTERVAL, self(), collect),

    {ok, #state{
        hostname = Hostname,
        cpus = Cpus,
        total_memory_mb = TotalMemoryMB,
        load_avg = 0.0,
        load_avg_5 = 0.0,
        load_avg_15 = 0.0,
        free_memory_mb = TotalMemoryMB,
        cached_memory_mb = 0,
        available_memory_mb = TotalMemoryMB,
        gpus = GPUs,
        disk_usage = #{},
        platform = Platform,
        gpu_allocation = #{}
    }}.

handle_call(get_metrics, _From, State) ->
    Metrics = #{
        hostname => State#state.hostname,
        cpus => State#state.cpus,
        total_memory_mb => State#state.total_memory_mb,
        free_memory_mb => State#state.free_memory_mb,
        available_memory_mb => State#state.available_memory_mb,
        cached_memory_mb => State#state.cached_memory_mb,
        load_avg => State#state.load_avg,
        load_avg_5 => State#state.load_avg_5,
        load_avg_15 => State#state.load_avg_15,
        gpu_count => length(State#state.gpus)
    },
    {reply, Metrics, State};

handle_call(get_hostname, _From, State) ->
    {reply, State#state.hostname, State};

handle_call(get_gpus, _From, State) ->
    {reply, State#state.gpus, State};

handle_call(get_disk_usage, _From, State) ->
    {reply, State#state.disk_usage, State};

handle_call(get_gpu_allocation, _From, State) ->
    {reply, State#state.gpu_allocation, State};

handle_call({allocate_gpus, JobId, NumGpus}, _From, State) ->
    case NumGpus of
        0 ->
            {reply, {ok, []}, State};
        _ ->
            %% Find available (unallocated) GPUs
            AllGpuIndices = [maps:get(index, G) || G <- State#state.gpus],
            AllocatedIndices = maps:keys(State#state.gpu_allocation),
            AvailableIndices = AllGpuIndices -- AllocatedIndices,
            case length(AvailableIndices) >= NumGpus of
                true ->
                    %% Allocate the requested number of GPUs
                    {ToAllocate, _Rest} = lists:split(NumGpus, AvailableIndices),
                    %% Update allocation map
                    NewAllocation = lists:foldl(
                        fun(GpuIdx, Acc) -> maps:put(GpuIdx, JobId, Acc) end,
                        State#state.gpu_allocation,
                        ToAllocate
                    ),
                    lager:info("Allocated GPUs ~p to job ~p", [ToAllocate, JobId]),
                    {reply, {ok, ToAllocate}, State#state{gpu_allocation = NewAllocation}};
                false ->
                    lager:warning("Not enough GPUs for job ~p: requested ~p, available ~p",
                                 [JobId, NumGpus, length(AvailableIndices)]),
                    {reply, {error, not_enough_gpus}, State}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({release_gpus, JobId}, State) ->
    %% Remove all GPU allocations for this job
    NewAllocation = maps:filter(
        fun(_GpuIdx, AllocJobId) -> AllocJobId =/= JobId end,
        State#state.gpu_allocation
    ),
    ReleasedCount = maps:size(State#state.gpu_allocation) - maps:size(NewAllocation),
    case ReleasedCount > 0 of
        true ->
            lager:info("Released ~p GPUs from job ~p", [ReleasedCount, JobId]);
        false ->
            ok
    end,
    {noreply, State#state{gpu_allocation = NewAllocation}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(collect, #state{platform = Platform} = State) ->
    {LoadAvg, LoadAvg5, LoadAvg15} = get_load_average(Platform),
    {FreeMem, CachedMem, AvailMem} = get_memory_info(Platform),
    DiskUsage = get_disk_info(),

    erlang:send_after(?COLLECT_INTERVAL, self(), collect),

    {noreply, State#state{
        load_avg = LoadAvg,
        load_avg_5 = LoadAvg5,
        load_avg_15 = LoadAvg15,
        free_memory_mb = FreeMem,
        cached_memory_mb = CachedMem,
        available_memory_mb = AvailMem,
        disk_usage = DiskUsage
    }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions - Platform Detection
%%====================================================================

detect_platform() ->
    case os:type() of
        {unix, linux} -> linux;
        {unix, darwin} -> darwin;
        _ -> other
    end.

get_system_hostname() ->
    case inet:gethostname() of
        {ok, Hostname} ->
            list_to_binary(Hostname);
        _ ->
            list_to_binary(net_adm:localhost())
    end.

get_cpu_count() ->
    case erlang:system_info(logical_processors_available) of
        unknown -> erlang:system_info(logical_processors);
        N -> N
    end.

%%====================================================================
%% Internal functions - Memory
%%====================================================================

get_total_memory(linux) ->
    case file:read_file("/proc/meminfo") of
        {ok, Content} ->
            parse_meminfo_field(Content, <<"MemTotal:">>) div 1024;
        _ ->
            erlang_memory_fallback()
    end;
get_total_memory(darwin) ->
    %% Use sysctl on macOS
    case os:cmd("sysctl -n hw.memsize 2>/dev/null") of
        Result when is_list(Result) ->
            try
                list_to_integer(string:trim(Result)) div (1024 * 1024)
            catch
                _:_ -> erlang_memory_fallback()
            end;
        _ ->
            erlang_memory_fallback()
    end;
get_total_memory(_) ->
    erlang_memory_fallback().

get_memory_info(linux) ->
    case file:read_file("/proc/meminfo") of
        {ok, Content} ->
            FreeMem = parse_meminfo_field(Content, <<"MemFree:">>) div 1024,
            Cached = parse_meminfo_field(Content, <<"Cached:">>) div 1024,
            Buffers = parse_meminfo_field(Content, <<"Buffers:">>) div 1024,
            %% Available memory (kernel 3.14+) or estimate
            AvailMem = case parse_meminfo_field(Content, <<"MemAvailable:">>) of
                0 -> FreeMem + Cached + Buffers;  % Estimate if not available
                N -> N div 1024
            end,
            {FreeMem, Cached + Buffers, AvailMem};
        _ ->
            {0, 0, 0}
    end;
get_memory_info(darwin) ->
    %% Use vm_stat on macOS
    case os:cmd("vm_stat 2>/dev/null") of
        Result when is_list(Result) ->
            PageSize = 4096,  % Usually 4KB on macOS
            Lines = string:split(Result, "\n", all),
            Free = parse_vm_stat_line(Lines, "Pages free") * PageSize div (1024 * 1024),
            Inactive = parse_vm_stat_line(Lines, "Pages inactive") * PageSize div (1024 * 1024),
            {Free, Inactive, Free + Inactive};
        _ ->
            {0, 0, 0}
    end;
get_memory_info(_) ->
    {0, 0, 0}.

parse_meminfo_field(Content, Field) ->
    case binary:match(Content, Field) of
        {Start, Len} ->
            %% Find the value after the field name
            RestStart = Start + Len,
            Rest = binary:part(Content, RestStart, byte_size(Content) - RestStart),
            %% Extract the number (skip whitespace, read digits)
            case re:run(Rest, <<"\\s*(\\d+)">>, [{capture, [1], binary}]) of
                {match, [NumBin]} ->
                    binary_to_integer(NumBin);
                _ ->
                    0
            end;
        nomatch ->
            0
    end.

parse_vm_stat_line(Lines, Field) ->
    case [L || L <- Lines, string:find(L, Field) =/= nomatch] of
        [Line | _] ->
            case re:run(Line, "(\\d+)", [{capture, [1], list}]) of
                {match, [NumStr]} -> list_to_integer(NumStr);
                _ -> 0
            end;
        [] ->
            0
    end.

erlang_memory_fallback() ->
    %% Very rough estimate from Erlang VM
    erlang:memory(total) div (1024 * 1024) * 10.  % Multiply by 10 as estimate

%%====================================================================
%% Internal functions - Load Average
%%====================================================================

get_load_average(linux) ->
    case file:read_file("/proc/loadavg") of
        {ok, Content} ->
            case binary:split(Content, <<" ">>, [global]) of
                [L1, L5, L15 | _] ->
                    {binary_to_float_safe(L1),
                     binary_to_float_safe(L5),
                     binary_to_float_safe(L15)};
                _ ->
                    {0.0, 0.0, 0.0}
            end;
        _ ->
            {0.0, 0.0, 0.0}
    end;
get_load_average(darwin) ->
    case os:cmd("sysctl -n vm.loadavg 2>/dev/null") of
        Result when is_list(Result) ->
            %% Result is like "{ 1.23 1.45 1.67 }"
            case re:run(Result, "([\\d.]+)\\s+([\\d.]+)\\s+([\\d.]+)",
                       [{capture, [1,2,3], list}]) of
                {match, [L1, L5, L15]} ->
                    {list_to_float_safe(L1),
                     list_to_float_safe(L5),
                     list_to_float_safe(L15)};
                _ ->
                    {0.0, 0.0, 0.0}
            end;
        _ ->
            {0.0, 0.0, 0.0}
    end;
get_load_average(_) ->
    %% Fallback using Erlang run queue
    RunQueue = erlang:statistics(run_queue),
    Load = float(RunQueue),
    {Load, Load, Load}.

binary_to_float_safe(Bin) ->
    Str = binary_to_list(Bin),
    list_to_float_safe(Str).

list_to_float_safe(Str) ->
    Trimmed = string:trim(Str),
    try
        list_to_float(Trimmed)
    catch
        error:badarg ->
            try
                float(list_to_integer(Trimmed))
            catch
                _:_ -> 0.0
            end
    end.

%%====================================================================
%% Internal functions - GPU Detection
%%====================================================================

detect_gpus() ->
    NvidiaGPUs = detect_nvidia_gpus(),
    AMDGPUs = detect_amd_gpus(),
    NvidiaGPUs ++ AMDGPUs.

detect_nvidia_gpus() ->
    case os:find_executable("nvidia-smi") of
        false ->
            [];
        _ ->
            %% Query NVIDIA GPUs using nvidia-smi
            Cmd = "nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader,nounits 2>/dev/null",
            case os:cmd(Cmd) of
                [] ->
                    [];
                Result ->
                    Lines = string:split(Result, "\n", all),
                    lists:filtermap(fun parse_nvidia_gpu/1, Lines)
            end
    end.

parse_nvidia_gpu(Line) ->
    case string:split(string:trim(Line), ", ", all) of
        [IdxStr, Name, MemStr] ->
            try
                Idx = list_to_integer(string:trim(IdxStr)),
                MemMB = list_to_integer(string:trim(MemStr)),
                {true, #{
                    index => Idx,
                    name => list_to_binary(string:trim(Name)),
                    type => nvidia,
                    memory_mb => MemMB
                }}
            catch
                _:_ -> false
            end;
        _ ->
            false
    end.

detect_amd_gpus() ->
    %% Check for AMD GPUs via /sys filesystem
    case filelib:is_dir("/sys/class/drm") of
        true ->
            case filelib:wildcard("/sys/class/drm/card*/device/vendor") of
                [] -> [];
                VendorFiles ->
                    lists:filtermap(fun check_amd_vendor/1, VendorFiles)
            end;
        false ->
            []
    end.

check_amd_vendor(VendorFile) ->
    case file:read_file(VendorFile) of
        {ok, <<"0x1002", _/binary>>} ->  % AMD vendor ID
            %% Extract card number from path
            case re:run(VendorFile, "card(\\d+)", [{capture, [1], list}]) of
                {match, [NumStr]} ->
                    {true, #{
                        index => list_to_integer(NumStr),
                        name => <<"AMD GPU">>,
                        type => amd,
                        memory_mb => 0  % Would need rocm-smi for this
                    }};
                _ ->
                    false
            end;
        _ ->
            false
    end.

%%====================================================================
%% Internal functions - Disk Usage
%%====================================================================

get_disk_info() ->
    %% Get disk usage for common mount points
    MountPoints = ["/", "/tmp", "/home", "/scratch"],
    maps:from_list(lists:filtermap(fun get_mount_usage/1, MountPoints)).

get_mount_usage(MountPoint) ->
    case os:cmd("df -m " ++ MountPoint ++ " 2>/dev/null | tail -1") of
        [] ->
            false;
        Result ->
            case string:split(string:trim(Result), " ", all) of
                Parts when length(Parts) >= 4 ->
                    %% df output: Filesystem Size Used Avail Use% Mounted
                    NonEmpty = [P || P <- Parts, P =/= []],
                    case NonEmpty of
                        [_FS, TotalStr, UsedStr, AvailStr | _] ->
                            try
                                Total = list_to_integer(TotalStr),
                                Used = list_to_integer(UsedStr),
                                Avail = list_to_integer(AvailStr),
                                {true, {list_to_binary(MountPoint), #{
                                    total_mb => Total,
                                    used_mb => Used,
                                    available_mb => Avail,
                                    percent_used => (Used * 100) div max(Total, 1)
                                }}}
                            catch
                                _:_ -> false
                            end;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end
    end.
