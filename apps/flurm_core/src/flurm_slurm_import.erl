%%%-------------------------------------------------------------------
%%% @doc FLURM SLURM State Importer
%%%
%%% Imports job and node state from a running SLURM installation to
%%% enable zero-downtime migration from SLURM to FLURM.
%%%
%%% Import methods:
%%% - CLI output parsing (squeue, sinfo) - Safe, non-invasive
%%% - State file reading - Faster but requires file access
%%% - Database import - For slurmdbd data
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_import).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    start_link/1,
    import_jobs/0,
    import_jobs/1,
    import_nodes/0,
    import_nodes/1,
    import_partitions/0,
    import_all/0,
    import_all/1,
    start_sync/1,
    stop_sync/0,
    get_sync_status/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    sync_enabled = false :: boolean(),
    sync_interval = 5000 :: pos_integer(),
    sync_timer :: reference() | undefined,
    last_sync :: calendar:datetime() | undefined,
    stats = #{} :: map()
}).

-define(DEFAULT_SYNC_INTERVAL, 5000).

%% Test exports for coverage
-ifdef(TEST).
-export([
    parse_squeue_output/2,
    parse_squeue_line/1,
    parse_job_state/1,
    parse_sinfo_output/2,
    parse_sinfo_line/1,
    parse_node_state/1,
    parse_partition_output/1,
    parse_partition_line/1,
    parse_int/1,
    parse_memory/1,
    parse_time/1,
    parse_timestamp/1,
    parse_features/1,
    parse_gres/1,
    update_stats/3,
    count_imported/1
]).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @doc Import jobs from SLURM using squeue
import_jobs() ->
    import_jobs(#{}).

import_jobs(Opts) ->
    gen_server:call(?MODULE, {import_jobs, Opts}, 60000).

%% @doc Import nodes from SLURM using sinfo
import_nodes() ->
    import_nodes(#{}).

import_nodes(Opts) ->
    gen_server:call(?MODULE, {import_nodes, Opts}, 60000).

%% @doc Import partitions from SLURM using sinfo
import_partitions() ->
    gen_server:call(?MODULE, import_partitions, 60000).

%% @doc Import everything
import_all() ->
    import_all(#{}).

import_all(Opts) ->
    gen_server:call(?MODULE, {import_all, Opts}, 120000).

%% @doc Start continuous sync from SLURM
start_sync(Interval) ->
    gen_server:call(?MODULE, {start_sync, Interval}).

%% @doc Stop continuous sync
stop_sync() ->
    gen_server:call(?MODULE, stop_sync).

%% @doc Get sync status
get_sync_status() ->
    gen_server:call(?MODULE, get_sync_status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    SyncInterval = proplists:get_value(sync_interval, Opts, ?DEFAULT_SYNC_INTERVAL),
    AutoSync = proplists:get_value(auto_sync, Opts, false),

    State = #state{
        sync_interval = SyncInterval,
        stats = #{jobs_imported => 0, nodes_imported => 0, partitions_imported => 0}
    },

    State2 = case AutoSync of
        true -> start_sync_timer(State);
        false -> State
    end,

    {ok, State2}.

handle_call({import_jobs, Opts}, _From, State) ->
    Result = do_import_jobs(Opts),
    NewStats = update_stats(jobs, Result, State#state.stats),
    {reply, Result, State#state{stats = NewStats}};

handle_call({import_nodes, Opts}, _From, State) ->
    Result = do_import_nodes(Opts),
    NewStats = update_stats(nodes, Result, State#state.stats),
    {reply, Result, State#state{stats = NewStats}};

handle_call(import_partitions, _From, State) ->
    Result = do_import_partitions(),
    NewStats = update_stats(partitions, Result, State#state.stats),
    {reply, Result, State#state{stats = NewStats}};

handle_call({import_all, Opts}, _From, State) ->
    PartResult = do_import_partitions(),
    NodeResult = do_import_nodes(Opts),
    JobResult = do_import_jobs(Opts),

    Result = #{
        partitions => PartResult,
        nodes => NodeResult,
        jobs => JobResult
    },

    OldStats = State#state.stats,
    NewStats = OldStats#{
        partitions_imported => count_imported(PartResult),
        nodes_imported => count_imported(NodeResult),
        jobs_imported => count_imported(JobResult),
        last_full_import => calendar:universal_time()
    },

    {reply, {ok, Result}, State#state{stats = NewStats, last_sync = calendar:universal_time()}};

handle_call({start_sync, Interval}, _From, State) ->
    State2 = stop_sync_timer(State),
    State3 = State2#state{sync_interval = Interval},
    State4 = start_sync_timer(State3),
    {reply, ok, State4};

handle_call(stop_sync, _From, State) ->
    State2 = stop_sync_timer(State),
    {reply, ok, State2};

handle_call(get_sync_status, _From, State) ->
    Status = #{
        sync_enabled => State#state.sync_enabled,
        sync_interval => State#state.sync_interval,
        last_sync => State#state.last_sync,
        stats => State#state.stats
    },
    {reply, {ok, Status}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync_tick, State) ->
    %% Perform sync
    _ = do_import_jobs(#{}),
    _ = do_import_nodes(#{}),

    State2 = State#state{last_sync = calendar:universal_time()},
    State3 = start_sync_timer(State2),
    {noreply, State3};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = stop_sync_timer(State),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_sync_timer(#state{sync_interval = Interval} = State) ->
    Timer = erlang:send_after(Interval, self(), sync_tick),
    State#state{sync_enabled = true, sync_timer = Timer}.

stop_sync_timer(#state{sync_timer = undefined} = State) ->
    State#state{sync_enabled = false};
stop_sync_timer(#state{sync_timer = Timer} = State) ->
    erlang:cancel_timer(Timer),
    State#state{sync_enabled = false, sync_timer = undefined}.

%% Import jobs from squeue output
do_import_jobs(Opts) ->
    Format = "%i|%j|%u|%P|%t|%M|%D|%C|%m|%l|%S|%V",
    Cmd = io_lib:format("squeue -h -o '~s' 2>/dev/null", [Format]),

    case run_command(Cmd) of
        {ok, Output} ->
            Jobs = parse_squeue_output(Output, Opts),
            import_jobs_to_flurm(Jobs);
        {error, Reason} ->
            {error, {squeue_failed, Reason}}
    end.

parse_squeue_output(Output, _Opts) ->
    Lines = string:tokens(Output, "\n"),
    lists:filtermap(fun parse_squeue_line/1, Lines).

parse_squeue_line(Line) ->
    case string:tokens(Line, "|") of
        [JobId, Name, User, Partition, State, Time, Nodes, CPUs, Mem, TimeLimit, Start, Submit] ->
            Job = #{
                id => list_to_integer(JobId),
                name => list_to_binary(Name),
                user => list_to_binary(User),
                partition => list_to_binary(Partition),
                state => parse_job_state(State),
                run_time => parse_time(Time),
                num_nodes => parse_int(Nodes),
                num_cpus => parse_int(CPUs),
                memory_mb => parse_memory(Mem),
                time_limit => parse_time(TimeLimit),
                start_time => parse_timestamp(Start),
                submit_time => parse_timestamp(Submit)
            },
            {true, Job};
        _ ->
            false
    end.

parse_job_state("PD") -> pending;
parse_job_state("R") -> running;
parse_job_state("CG") -> completing;
parse_job_state("CD") -> completed;
parse_job_state("F") -> failed;
parse_job_state("CA") -> cancelled;
parse_job_state("TO") -> timeout;
parse_job_state("NF") -> node_fail;
parse_job_state("PR") -> preempted;
parse_job_state("S") -> suspended;
parse_job_state(_) -> unknown.

%% Import nodes from sinfo output
do_import_nodes(Opts) ->
    Format = "%n|%P|%t|%c|%m|%f|%G",
    Cmd = io_lib:format("sinfo -h -N -o '~s' 2>/dev/null", [Format]),

    case run_command(Cmd) of
        {ok, Output} ->
            Nodes = parse_sinfo_output(Output, Opts),
            import_nodes_to_flurm(Nodes);
        {error, Reason} ->
            {error, {sinfo_failed, Reason}}
    end.

parse_sinfo_output(Output, _Opts) ->
    Lines = string:tokens(Output, "\n"),
    lists:filtermap(fun parse_sinfo_line/1, Lines).

parse_sinfo_line(Line) ->
    case string:tokens(Line, "|") of
        [Name, Partition, State, CPUs, Memory, Features, GRES] ->
            Node = #{
                name => list_to_binary(Name),
                partition => list_to_binary(Partition),
                state => parse_node_state(State),
                cpus => parse_int(CPUs),
                memory_mb => parse_int(Memory),
                features => parse_features(Features),
                gres => parse_gres(GRES)
            },
            {true, Node};
        _ ->
            false
    end.

parse_node_state("idle") -> idle;
parse_node_state("alloc") -> allocated;
parse_node_state("mix") -> mixed;
parse_node_state("down") -> down;
parse_node_state("drain") -> drain;
parse_node_state("drng") -> draining;
parse_node_state("comp") -> completing;
parse_node_state(State) ->
    %% Handle states with suffixes like "idle*" or "down*"
    Base = lists:takewhile(fun(C) -> C =/= $* andalso C =/= $~ end, State),
    parse_node_state(Base).

%% Import partitions from sinfo
do_import_partitions() ->
    Format = "%P|%a|%l|%D|%c|%m|%F",
    Cmd = io_lib:format("sinfo -h -s -o '~s' 2>/dev/null", [Format]),

    case run_command(Cmd) of
        {ok, Output} ->
            Partitions = parse_partition_output(Output),
            import_partitions_to_flurm(Partitions);
        {error, Reason} ->
            {error, {sinfo_failed, Reason}}
    end.

parse_partition_output(Output) ->
    Lines = string:tokens(Output, "\n"),
    lists:filtermap(fun parse_partition_line/1, Lines).

parse_partition_line(Line) ->
    case string:tokens(Line, "|") of
        [Name, Avail, TimeLimit, Nodes, CPUs, Memory, NodeList] ->
            %% Remove trailing '*' for default partition
            CleanName = lists:takewhile(fun(C) -> C =/= $* end, Name),
            IsDefault = lists:last(Name) =:= $*,
            Partition = #{
                name => list_to_binary(CleanName),
                is_default => IsDefault,
                available => Avail =:= "up",
                time_limit => parse_time(TimeLimit),
                total_nodes => parse_int(Nodes),
                total_cpus => parse_int(CPUs),
                memory_per_node => parse_int(Memory),
                node_list => list_to_binary(NodeList)
            },
            {true, Partition};
        _ ->
            false
    end.

%% Import parsed jobs into FLURM
import_jobs_to_flurm(Jobs) ->
    Results = lists:map(fun(Job) ->
        JobId = maps:get(id, Job),
        case flurm_job_manager:get_job(JobId) of
            {ok, _} ->
                %% Job exists, update state if needed
                {updated, JobId};
            {error, not_found} ->
                %% Create new job
                case flurm_job_manager:import_job(Job) of
                    {ok, _} -> {imported, JobId};
                    {error, Reason} -> {failed, JobId, Reason}
                end
        end
    end, Jobs),

    Imported = length([X || {imported, X} <- Results]),
    Updated = length([X || {updated, X} <- Results]),
    Failed = length([X || {failed, X, _} <- Results]),

    {ok, #{imported => Imported, updated => Updated, failed => Failed}}.

%% Import parsed nodes into FLURM
import_nodes_to_flurm(Nodes) ->
    Results = lists:map(fun(Node) ->
        NodeName = maps:get(name, Node),
        case flurm_node_manager:get_node(NodeName) of
            {ok, _} ->
                %% Node exists, update state
                flurm_node_manager:update_node(NodeName, Node),
                {updated, NodeName};
            {error, not_found} ->
                %% Register new node
                case flurm_node_manager:register_node(Node) of
                    ok -> {imported, NodeName};
                    {error, Reason} -> {failed, NodeName, Reason}
                end
        end
    end, Nodes),

    Imported = length([X || {imported, X} <- Results]),
    Updated = length([X || {updated, X} <- Results]),
    Failed = length([X || {failed, X, _} <- Results]),

    {ok, #{imported => Imported, updated => Updated, failed => Failed}}.

%% Import partitions into FLURM
import_partitions_to_flurm(Partitions) ->
    Results = lists:map(fun(Part) ->
        PartName = maps:get(name, Part),
        case flurm_partition_manager:get_partition(PartName) of
            {ok, _} ->
                {updated, PartName};
            {error, not_found} ->
                case flurm_partition_manager:create_partition(Part) of
                    ok -> {imported, PartName};
                    {error, Reason} -> {failed, PartName, Reason}
                end
        end
    end, Partitions),

    Imported = length([X || {imported, X} <- Results]),
    Updated = length([X || {updated, X} <- Results]),
    Failed = length([X || {failed, X, _} <- Results]),

    {ok, #{imported => Imported, updated => Updated, failed => Failed}}.

%% Helper functions
run_command(Cmd) ->
    try
        Output = os:cmd(lists:flatten(Cmd)),
        {ok, Output}
    catch
        _:Reason ->
            {error, Reason}
    end.

parse_int(Str) ->
    case string:to_integer(Str) of
        {Int, _} when is_integer(Int) -> Int;
        _ -> 0
    end.

parse_memory(Str) ->
    %% Parse memory like "4000M" or "4G"
    case re:run(Str, "^([0-9]+)([MmGgTt])?", [{capture, all_but_first, list}]) of
        {match, [Num, "G"]} -> list_to_integer(Num) * 1024;
        {match, [Num, "g"]} -> list_to_integer(Num) * 1024;
        {match, [Num, "T"]} -> list_to_integer(Num) * 1024 * 1024;
        {match, [Num, "t"]} -> list_to_integer(Num) * 1024 * 1024;
        {match, [Num, _]} -> list_to_integer(Num);
        {match, [Num]} -> list_to_integer(Num);
        _ -> 0
    end.

parse_time(Str) ->
    %% Parse time like "1-00:00:00" or "10:30:00" or "UNLIMITED"
    case Str of
        "UNLIMITED" -> unlimited;
        "N/A" -> undefined;
        _ ->
            %% Try to parse as minutes
            case re:run(Str, "^([0-9]+)-([0-9]+):([0-9]+):([0-9]+)$", [{capture, all_but_first, list}]) of
                {match, [Days, Hours, Mins, Secs]} ->
                    list_to_integer(Days) * 1440 +
                    list_to_integer(Hours) * 60 +
                    list_to_integer(Mins) +
                    list_to_integer(Secs) div 60;
                _ ->
                    case re:run(Str, "^([0-9]+):([0-9]+):([0-9]+)$", [{capture, all_but_first, list}]) of
                        {match, [Hours, Mins, Secs]} ->
                            list_to_integer(Hours) * 60 +
                            list_to_integer(Mins) +
                            list_to_integer(Secs) div 60;
                        _ ->
                            parse_int(Str)
                    end
            end
    end.

parse_timestamp(Str) ->
    %% Parse SLURM timestamp format
    case Str of
        "N/A" -> undefined;
        "Unknown" -> undefined;
        _ ->
            %% Try ISO format first
            case calendar:rfc3339_to_system_time(Str, [{unit, second}]) of
                Time when is_integer(Time) -> Time;
                _ -> undefined
            end
    end.

parse_features(Str) ->
    case Str of
        "(null)" -> [];
        "" -> [];
        _ -> [list_to_binary(F) || F <- string:tokens(Str, ",")]
    end.

parse_gres(Str) ->
    case Str of
        "(null)" -> [];
        "" -> [];
        _ -> [list_to_binary(G) || G <- string:tokens(Str, ",")]
    end.

update_stats(Type, {ok, Result}, Stats) ->
    Key = list_to_atom(atom_to_list(Type) ++ "_imported"),
    Count = maps:get(imported, Result, 0) + maps:get(updated, Result, 0),
    Stats#{Key => maps:get(Key, Stats, 0) + Count};
update_stats(_Type, _Error, Stats) ->
    Stats.

count_imported({ok, #{imported := I, updated := U}}) -> I + U;
count_imported(_) -> 0.
