%%%-------------------------------------------------------------------
%%% @doc FLURM Database Supervisor
%%%
%%% Top-level supervisor for the FLURM database application.
%%% Responsible for starting and supervising the Ra cluster and
%%% related processes.
%%%
%%% The supervisor starts:
%%% 1. Ra application (if not already started)
%%% 2. The Ra cluster for FLURM DB
%%% 3. A process group for event subscribers
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup).
-behaviour(supervisor).

-include("flurm_db.hrl").

%% API
-export([
    start_link/0,
    start_link/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the supervisor with default configuration.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the supervisor with custom configuration.
%% Options:
%%   cluster_nodes - List of nodes to form the cluster with
%%   join_node - Existing node to join (alternative to cluster_nodes)
-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) when is_map(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init(Config) ->
    %% Create ETS tables for fallback storage (before starting children)
    %% These are owned by the supervisor process and will persist
    %% even when flurm_db_ra_starter terminates
    init_ets_tables(),

    %% Set restart strategy
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    %% Child specifications
    Children = [
        %% Ra cluster starter - initializes the Ra cluster
        #{
            id => flurm_db_ra_starter,
            start => {flurm_db_ra_starter, start_link, [Config]},
            restart => transient,
            shutdown => 5000,
            type => worker,
            modules => [flurm_db_ra_starter]
        }
    ],

    {ok, {SupFlags, Children}}.

%% @private
%% Initialize ETS tables for fallback storage.
%% Called from the supervisor so tables persist when ra_starter terminates.
%% Also initializes DETS for disk persistence and loads existing data.
init_ets_tables() ->
    %% Ensure data directory exists
    DataDir = get_data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Create ETS tables
    Tables = [flurm_db_jobs_ets, flurm_db_nodes_ets, flurm_db_partitions_ets,
              flurm_db_job_counter_ets],
    lists:foreach(fun(Table) ->
        case ets:whereis(Table) of
            undefined ->
                ets:new(Table, [named_table, public, set, {read_concurrency, true}]);
            _ ->
                ok
        end
    end, Tables),

    %% Open DETS tables for disk persistence
    DetsOpts = [{type, set}, {auto_save, 30000}],  % Auto-save every 30 seconds
    JobsDetsFile = filename:join(DataDir, "flurm_jobs.dets"),
    CounterDetsFile = filename:join(DataDir, "flurm_counter.dets"),

    case dets:open_file(flurm_db_jobs_dets, [{file, JobsDetsFile} | DetsOpts]) of
        {ok, _} ->
            lager:info("Opened DETS jobs table: ~s", [JobsDetsFile]);
        {error, Reason1} ->
            lager:warning("Failed to open DETS jobs table: ~p", [Reason1])
    end,

    case dets:open_file(flurm_db_counter_dets, [{file, CounterDetsFile} | DetsOpts]) of
        {ok, _} ->
            lager:info("Opened DETS counter table: ~s", [CounterDetsFile]);
        {error, Reason2} ->
            lager:warning("Failed to open DETS counter table: ~p", [Reason2])
    end,

    %% Load existing jobs from DETS into ETS
    load_jobs_from_dets(),

    %% Initialize job counter
    init_job_counter(),

    ok.

%% Get data directory for persistence
get_data_dir() ->
    case application:get_env(flurm_db, data_dir) of
        {ok, Dir} -> Dir;
        undefined ->
            %% Default to /var/lib/flurm or ./data
            case filelib:is_dir("/var/lib/flurm") of
                true -> "/var/lib/flurm";
                false ->
                    {ok, Cwd} = file:get_cwd(),
                    filename:join(Cwd, "data")
            end
    end.

%% Load jobs from DETS into ETS on startup
load_jobs_from_dets() ->
    case dets:info(flurm_db_jobs_dets) of
        undefined ->
            lager:info("DETS jobs table not available, starting fresh");
        _ ->
            Jobs = dets:select(flurm_db_jobs_dets, [{'$1', [], ['$1']}]),
            lists:foreach(fun({_Id, _Job} = Entry) ->
                ets:insert(flurm_db_jobs_ets, Entry)
            end, Jobs),
            lager:info("Loaded ~p jobs from DETS into ETS", [length(Jobs)])
    end.

%% Initialize job counter from DETS or existing jobs
init_job_counter() ->
    Counter = case dets:lookup(flurm_db_counter_dets, counter) of
        [{counter, C}] ->
            lager:info("Restored job counter from DETS: ~p", [C]),
            C;
        [] ->
            %% Calculate from existing jobs
            case ets:tab2list(flurm_db_jobs_ets) of
                [] -> 0;
                Jobs ->
                    MaxId = lists:max([Id || {Id, _} <- Jobs]),
                    lager:info("Calculated job counter from jobs: ~p", [MaxId]),
                    MaxId
            end
    end,
    ets:insert(flurm_db_job_counter_ets, {counter, Counter}),
    dets:insert(flurm_db_counter_dets, {counter, Counter}).
