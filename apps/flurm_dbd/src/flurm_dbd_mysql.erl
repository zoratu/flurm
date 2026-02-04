%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - slurmdbd MySQL Bridge
%%%
%%% MIGRATION-ONLY BRIDGE MODULE
%%% ============================
%%% This module provides a MySQL connector for zero-downtime migration
%%% from SLURM to FLURM. It allows FLURM to:
%%%
%%% 1. Push completed job records to an existing slurmdbd MySQL database
%%% 2. Read historical job data from slurmdbd for reporting continuity
%%% 3. Verify schema compatibility with slurmdbd database versions
%%%
%%% This is NOT intended for long-term use. Once migration is complete,
%%% organizations should transition fully to FLURM's native storage.
%%%
%%% REQUIREMENTS:
%%% - mysql-otp dependency (optional, only needed for migration)
%%% - Network access to slurmdbd MySQL database
%%% - Read/write credentials for slurmdbd job_table
%%%
%%% SCHEMA MAPPING (FLURM -> slurmdbd job_table):
%%% - job_id       -> id_job
%%% - job_name     -> job_name
%%% - user_name    -> user (via id_user lookup)
%%% - account      -> account (via id_assoc lookup)
%%% - partition    -> partition
%%% - state        -> state
%%% - exit_code    -> exit_code
%%% - num_nodes    -> nodes_alloc
%%% - num_cpus     -> cpus_req
%%% - submit_time  -> time_submit
%%% - start_time   -> time_start
%%% - end_time     -> time_end
%%% - elapsed      -> (calculated from time_end - time_start)
%%% - tres_alloc   -> tres_alloc (TRES string format)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1]).
-export([
    connect/1,
    disconnect/0,
    is_connected/0,
    sync_job_record/1,
    sync_job_records/1,
    read_historical_jobs/2,
    read_historical_jobs/3,
    check_schema_compatibility/0,
    get_connection_status/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports
-ifdef(TEST).
-export([
    map_job_to_slurmdbd/1,
    map_slurmdbd_to_job/1,
    state_to_slurmdbd/1,
    slurmdbd_to_state/1,
    format_tres_string/1,
    parse_tres_string/1,
    build_insert_query/1,
    build_select_query/2
]).
-endif.

-define(SERVER, ?MODULE).

%% slurmdbd job_table schema version we support
-define(MIN_SCHEMA_VERSION, 7).
-define(MAX_SCHEMA_VERSION, 10).

%% Connection timeout
-define(CONNECT_TIMEOUT, 10000).
-define(QUERY_TIMEOUT, 30000).

%% Retry configuration
-define(MAX_RETRIES, 3).
-define(RETRY_DELAY, 1000).

-record(state, {
    connection :: pid() | undefined,
    config :: map(),
    connected :: boolean(),
    schema_version :: non_neg_integer() | undefined,
    last_error :: term(),
    stats :: map()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the MySQL connector with default config from application env
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Config = get_mysql_config(),
    start_link(Config).

%% @doc Start the MySQL connector with explicit config
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Connect to slurmdbd MySQL database
%% Config must include: host, port, user, password, database
%% Optional: ssl (boolean), ssl_opts (list)
-spec connect(map()) -> ok | {error, term()}.
connect(Config) ->
    gen_server:call(?SERVER, {connect, Config}, ?CONNECT_TIMEOUT).

%% @doc Disconnect from slurmdbd MySQL database
-spec disconnect() -> ok.
disconnect() ->
    gen_server:call(?SERVER, disconnect).

%% @doc Check if connected to MySQL
-spec is_connected() -> boolean().
is_connected() ->
    gen_server:call(?SERVER, is_connected).

%% @doc Push a completed job record to slurmdbd MySQL
%% This is used during migration to keep slurmdbd in sync
%% JobRecord is a map with FLURM job fields
-spec sync_job_record(map()) -> ok | {error, term()}.
sync_job_record(JobRecord) ->
    gen_server:call(?SERVER, {sync_job_record, JobRecord}, ?QUERY_TIMEOUT).

%% @doc Push multiple job records in a batch
-spec sync_job_records([map()]) -> {ok, non_neg_integer()} | {error, term()}.
sync_job_records(JobRecords) ->
    gen_server:call(?SERVER, {sync_job_records, JobRecords}, ?QUERY_TIMEOUT * 2).

%% @doc Read historical jobs from slurmdbd MySQL
%% StartTime and EndTime are Unix timestamps
-spec read_historical_jobs(non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
read_historical_jobs(StartTime, EndTime) ->
    read_historical_jobs(StartTime, EndTime, #{}).

%% @doc Read historical jobs with additional filters
%% Filters can include: user, account, partition, state, limit
-spec read_historical_jobs(non_neg_integer(), non_neg_integer(), map()) ->
    {ok, [map()]} | {error, term()}.
read_historical_jobs(StartTime, EndTime, Filters) ->
    gen_server:call(?SERVER, {read_historical_jobs, StartTime, EndTime, Filters}, ?QUERY_TIMEOUT).

%% @doc Check if slurmdbd schema version is compatible
-spec check_schema_compatibility() -> {ok, non_neg_integer()} | {error, term()}.
check_schema_compatibility() ->
    gen_server:call(?SERVER, check_schema_compatibility, ?QUERY_TIMEOUT).

%% @doc Get connection status and statistics
-spec get_connection_status() -> map().
get_connection_status() ->
    gen_server:call(?SERVER, get_connection_status).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    lager:info("FLURM DBD MySQL Bridge starting (MIGRATION-ONLY module)"),
    State = #state{
        connection = undefined,
        config = Config,
        connected = false,
        schema_version = undefined,
        last_error = undefined,
        stats = #{
            jobs_synced => 0,
            jobs_read => 0,
            sync_errors => 0,
            read_errors => 0,
            connect_attempts => 0,
            last_sync_time => undefined,
            last_read_time => undefined
        }
    },
    %% Auto-connect if config is provided and auto_connect is true
    case maps:get(auto_connect, Config, false) of
        true ->
            self() ! auto_connect;
        false ->
            ok
    end,
    {ok, State}.

handle_call({connect, Config}, _From, State) ->
    NewConfig = maps:merge(State#state.config, Config),
    case do_connect(NewConfig) of
        {ok, Conn} ->
            Stats = maps:update_with(connect_attempts, fun(V) -> V + 1 end, 1, State#state.stats),
            lager:info("Connected to slurmdbd MySQL at ~s:~p",
                      [maps:get(host, NewConfig, "localhost"),
                       maps:get(port, NewConfig, 3306)]),
            {reply, ok, State#state{
                connection = Conn,
                config = NewConfig,
                connected = true,
                last_error = undefined,
                stats = Stats
            }};
        {error, Reason} = Error ->
            Stats = maps:update_with(connect_attempts, fun(V) -> V + 1 end, 1, State#state.stats),
            lager:error("Failed to connect to slurmdbd MySQL: ~p", [Reason]),
            {reply, Error, State#state{
                connected = false,
                last_error = Reason,
                stats = Stats
            }}
    end;

handle_call(disconnect, _From, #state{connection = Conn} = State) ->
    case Conn of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            catch mysql:stop(Pid)
    end,
    lager:info("Disconnected from slurmdbd MySQL"),
    {reply, ok, State#state{connection = undefined, connected = false}};

handle_call(is_connected, _From, #state{connected = Connected} = State) ->
    {reply, Connected, State};

handle_call({sync_job_record, _JobRecord}, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call({sync_job_record, JobRecord}, _From, #state{connection = Conn} = State) ->
    case do_sync_job(Conn, JobRecord, State#state.config) of
        ok ->
            Stats = State#state.stats,
            NewStats = Stats#{
                jobs_synced => maps:get(jobs_synced, Stats, 0) + 1,
                last_sync_time => erlang:system_time(second)
            },
            {reply, ok, State#state{stats = NewStats}};
        {error, Reason} = Error ->
            Stats = State#state.stats,
            NewStats = Stats#{
                sync_errors => maps:get(sync_errors, Stats, 0) + 1
            },
            lager:warning("Failed to sync job ~p to slurmdbd: ~p",
                         [maps:get(job_id, JobRecord, unknown), Reason]),
            {reply, Error, State#state{stats = NewStats, last_error = Reason}}
    end;

handle_call({sync_job_records, _JobRecords}, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call({sync_job_records, JobRecords}, _From, #state{connection = Conn} = State) ->
    case do_sync_jobs_batch(Conn, JobRecords, State#state.config) of
        {ok, Count} ->
            Stats = State#state.stats,
            NewStats = Stats#{
                jobs_synced => maps:get(jobs_synced, Stats, 0) + Count,
                last_sync_time => erlang:system_time(second)
            },
            {reply, {ok, Count}, State#state{stats = NewStats}};
        {error, Reason} = Error ->
            Stats = State#state.stats,
            NewStats = Stats#{
                sync_errors => maps:get(sync_errors, Stats, 0) + 1
            },
            lager:warning("Failed to batch sync ~p jobs to slurmdbd: ~p",
                         [length(JobRecords), Reason]),
            {reply, Error, State#state{stats = NewStats, last_error = Reason}}
    end;

handle_call({read_historical_jobs, _StartTime, _EndTime, _Filters}, _From,
            #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call({read_historical_jobs, StartTime, EndTime, Filters}, _From,
            #state{connection = Conn} = State) ->
    case do_read_jobs(Conn, StartTime, EndTime, Filters, State#state.config) of
        {ok, Jobs} ->
            Stats = State#state.stats,
            NewStats = Stats#{
                jobs_read => maps:get(jobs_read, Stats, 0) + length(Jobs),
                last_read_time => erlang:system_time(second)
            },
            {reply, {ok, Jobs}, State#state{stats = NewStats}};
        {error, Reason} = Error ->
            Stats = State#state.stats,
            NewStats = Stats#{
                read_errors => maps:get(read_errors, Stats, 0) + 1
            },
            lager:warning("Failed to read historical jobs from slurmdbd: ~p", [Reason]),
            {reply, Error, State#state{stats = NewStats, last_error = Reason}}
    end;

handle_call(check_schema_compatibility, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call(check_schema_compatibility, _From, #state{connection = Conn} = State) ->
    case do_check_schema(Conn) of
        {ok, Version} when Version >= ?MIN_SCHEMA_VERSION, Version =< ?MAX_SCHEMA_VERSION ->
            lager:info("slurmdbd schema version ~p is compatible", [Version]),
            {reply, {ok, Version}, State#state{schema_version = Version}};
        {ok, Version} ->
            lager:warning("slurmdbd schema version ~p may not be fully compatible "
                         "(supported: ~p-~p)", [Version, ?MIN_SCHEMA_VERSION, ?MAX_SCHEMA_VERSION]),
            {reply, {ok, Version}, State#state{schema_version = Version}};
        {error, Reason} = Error ->
            lager:error("Failed to check slurmdbd schema: ~p", [Reason]),
            {reply, Error, State#state{last_error = Reason}}
    end;

handle_call(get_connection_status, _From, State) ->
    Status = #{
        connected => State#state.connected,
        schema_version => State#state.schema_version,
        last_error => State#state.last_error,
        stats => State#state.stats,
        config => maps:without([password], State#state.config)  % Hide password
    },
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(auto_connect, State) ->
    case maps:size(State#state.config) > 0 of
        true ->
            case do_connect(State#state.config) of
                {ok, Conn} ->
                    lager:info("Auto-connected to slurmdbd MySQL"),
                    {noreply, State#state{connection = Conn, connected = true}};
                {error, Reason} ->
                    lager:warning("Auto-connect to slurmdbd MySQL failed: ~p", [Reason]),
                    %% Retry after delay
                    erlang:send_after(5000, self(), auto_connect),
                    {noreply, State#state{last_error = Reason}}
            end;
        false ->
            {noreply, State}
    end;

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{connection = Pid} = State) ->
    lager:warning("slurmdbd MySQL connection lost: ~p", [Reason]),
    %% Try to reconnect
    self() ! auto_connect,
    {noreply, State#state{connection = undefined, connected = false, last_error = Reason}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{connection = Conn}) ->
    case Conn of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            catch mysql:stop(Pid)
    end,
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get MySQL config from application environment
get_mysql_config() ->
    Host = application:get_env(flurm_dbd, mysql_host, "localhost"),
    Port = application:get_env(flurm_dbd, mysql_port, 3306),
    User = application:get_env(flurm_dbd, mysql_user, "slurm"),
    Password = application:get_env(flurm_dbd, mysql_password, ""),
    Database = application:get_env(flurm_dbd, mysql_database, "slurm_acct_db"),
    #{
        host => Host,
        port => Port,
        user => User,
        password => Password,
        database => Database,
        auto_connect => false
    }.

%% @private Connect to MySQL
do_connect(Config) ->
    Host = maps:get(host, Config, "localhost"),
    Port = maps:get(port, Config, 3306),
    User = maps:get(user, Config, "slurm"),
    Password = maps:get(password, Config, ""),
    Database = maps:get(database, Config, "slurm_acct_db"),

    %% Build connection options
    Opts = [
        {host, Host},
        {port, Port},
        {user, User},
        {password, Password},
        {database, Database},
        {connect_timeout, ?CONNECT_TIMEOUT},
        {query_timeout, ?QUERY_TIMEOUT}
    ],

    %% Add SSL options if configured
    SslOpts = case maps:get(ssl, Config, false) of
        true ->
            [{ssl, maps:get(ssl_opts, Config, [])}];
        false ->
            []
    end,

    case mysql:start_link(Opts ++ SslOpts) of
        {ok, Pid} ->
            %% Monitor the connection
            erlang:monitor(process, Pid),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Sync a single job to slurmdbd
do_sync_job(Conn, JobRecord, Config) ->
    ClusterName = maps:get(cluster_name, Config, <<"flurm">>),
    TableName = io_lib:format("~s_job_table", [ClusterName]),

    %% Map FLURM job record to slurmdbd columns
    SlurmdBdRecord = map_job_to_slurmdbd(JobRecord),

    %% Build INSERT ... ON DUPLICATE KEY UPDATE query
    Query = build_insert_query(TableName),
    Params = build_insert_params(SlurmdBdRecord),

    case mysql:query(Conn, Query, Params) of
        ok -> ok;
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @private Sync multiple jobs in a batch
do_sync_jobs_batch(Conn, JobRecords, Config) ->
    %% Use transaction for batch insert
    case mysql:transaction(Conn, fun() ->
        lists:foreach(fun(Job) ->
            case do_sync_job(Conn, Job, Config) of
                ok -> ok;
                {error, Reason} -> throw({sync_error, Reason})
            end
        end, JobRecords)
    end) of
        {atomic, _} -> {ok, length(JobRecords)};
        {aborted, {sync_error, Reason}} -> {error, Reason};
        {aborted, Reason} -> {error, Reason}
    end.

%% @private Read historical jobs from slurmdbd
do_read_jobs(Conn, StartTime, EndTime, Filters, Config) ->
    ClusterName = maps:get(cluster_name, Config, <<"flurm">>),
    TableName = io_lib:format("~s_job_table", [ClusterName]),

    {Query, Params} = build_select_query(TableName, #{
        start_time => StartTime,
        end_time => EndTime,
        filters => Filters
    }),

    case mysql:query(Conn, Query, Params) of
        {ok, Columns, Rows} ->
            Jobs = [map_slurmdbd_to_job(row_to_map(Columns, Row)) || Row <- Rows],
            {ok, Jobs};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Check slurmdbd schema version
do_check_schema(Conn) ->
    Query = "SELECT version FROM table_defs_table WHERE table_name = 'job_table' LIMIT 1",
    case mysql:query(Conn, Query) of
        {ok, [<<"version">>], [[Version]]} when is_integer(Version) ->
            {ok, Version};
        {ok, _, [[Version]]} when is_binary(Version) ->
            {ok, binary_to_integer(Version)};
        {ok, _, []} ->
            {error, schema_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Map FLURM job record to slurmdbd job_table format
%% This handles the schema differences between FLURM and slurmdbd
map_job_to_slurmdbd(Job) ->
    #{
        id_job => maps:get(job_id, Job, 0),
        job_name => maps:get(job_name, Job, <<>>),
        id_user => maps:get(user_id, Job, 0),
        id_group => maps:get(group_id, Job, 0),
        account => maps:get(account, Job, <<>>),
        partition => maps:get(partition, Job, <<>>),
        state => state_to_slurmdbd(maps:get(state, Job, pending)),
        exit_code => maps:get(exit_code, Job, 0),
        nodes_alloc => maps:get(num_nodes, Job, 0),
        cpus_req => maps:get(num_cpus, Job, 0),
        time_submit => maps:get(submit_time, Job, 0),
        time_eligible => maps:get(eligible_time, Job, 0),
        time_start => maps:get(start_time, Job, 0),
        time_end => maps:get(end_time, Job, 0),
        timelimit => maps:get(time_limit, Job, 0),
        tres_alloc => format_tres_string(maps:get(tres_alloc, Job, #{})),
        tres_req => format_tres_string(maps:get(tres_req, Job, #{})),
        work_dir => maps:get(work_dir, Job, <<>>),
        std_out => maps:get(std_out, Job, <<>>),
        std_err => maps:get(std_err, Job, <<>>)
    }.

%% @doc Map slurmdbd job_table row to FLURM job record format
map_slurmdbd_to_job(Row) ->
    StartTime = maps:get(time_start, Row, 0),
    EndTime = maps:get(time_end, Row, 0),
    Elapsed = case EndTime > 0 andalso StartTime > 0 of
        true -> EndTime - StartTime;
        false -> 0
    end,
    #{
        job_id => maps:get(id_job, Row, 0),
        job_name => maps:get(job_name, Row, <<>>),
        user_id => maps:get(id_user, Row, 0),
        group_id => maps:get(id_group, Row, 0),
        account => maps:get(account, Row, <<>>),
        partition => maps:get(partition, Row, <<>>),
        state => slurmdbd_to_state(maps:get(state, Row, 0)),
        exit_code => maps:get(exit_code, Row, 0),
        num_nodes => maps:get(nodes_alloc, Row, 0),
        num_cpus => maps:get(cpus_req, Row, 0),
        submit_time => maps:get(time_submit, Row, 0),
        eligible_time => maps:get(time_eligible, Row, 0),
        start_time => StartTime,
        end_time => EndTime,
        elapsed => Elapsed,
        tres_alloc => parse_tres_string(maps:get(tres_alloc, Row, <<>>)),
        tres_req => parse_tres_string(maps:get(tres_req, Row, <<>>)),
        work_dir => maps:get(work_dir, Row, <<>>),
        std_out => maps:get(std_out, Row, <<>>),
        std_err => maps:get(std_err, Row, <<>>)
    }.

%% @doc Convert FLURM job state to slurmdbd state integer
%% Based on SLURM's job_state_enum from slurm.h
state_to_slurmdbd(pending) -> 0;        % JOB_PENDING
state_to_slurmdbd(running) -> 1;        % JOB_RUNNING
state_to_slurmdbd(suspended) -> 2;      % JOB_SUSPENDED
state_to_slurmdbd(completed) -> 3;      % JOB_COMPLETE
state_to_slurmdbd(cancelled) -> 4;      % JOB_CANCELLED
state_to_slurmdbd(failed) -> 5;         % JOB_FAILED
state_to_slurmdbd(timeout) -> 6;        % JOB_TIMEOUT
state_to_slurmdbd(node_fail) -> 7;      % JOB_NODE_FAIL
state_to_slurmdbd(preempted) -> 8;      % JOB_PREEMPTED
state_to_slurmdbd(boot_fail) -> 9;      % JOB_BOOT_FAIL
state_to_slurmdbd(deadline) -> 10;      % JOB_DEADLINE
state_to_slurmdbd(oom) -> 11;           % JOB_OOM
state_to_slurmdbd(_) -> 0.              % Default to pending

%% @doc Convert slurmdbd state integer to FLURM job state atom
slurmdbd_to_state(0) -> pending;
slurmdbd_to_state(1) -> running;
slurmdbd_to_state(2) -> suspended;
slurmdbd_to_state(3) -> completed;
slurmdbd_to_state(4) -> cancelled;
slurmdbd_to_state(5) -> failed;
slurmdbd_to_state(6) -> timeout;
slurmdbd_to_state(7) -> node_fail;
slurmdbd_to_state(8) -> preempted;
slurmdbd_to_state(9) -> boot_fail;
slurmdbd_to_state(10) -> deadline;
slurmdbd_to_state(11) -> oom;
slurmdbd_to_state(_) -> unknown.

%% @doc Format TRES map to slurmdbd TRES string format
%% SLURM uses format: "1=4,2=8192,4=2" (type_id=count pairs)
%% Type IDs: 1=cpu, 2=mem, 3=energy, 4=node, 5=billing, 6=fs/disk, 7=vmem, 8=pages, 1001+=GRES
format_tres_string(TresMap) when is_map(TresMap), map_size(TresMap) =:= 0 ->
    <<>>;
format_tres_string(TresMap) when is_map(TresMap) ->
    Parts = maps:fold(fun(Key, Value, Acc) ->
        TypeId = tres_key_to_id(Key),
        case TypeId of
            undefined -> Acc;
            Id -> [io_lib:format("~B=~B", [Id, Value]) | Acc]
        end
    end, [], TresMap),
    case Parts of
        [] -> <<>>;
        _ -> iolist_to_binary(lists:join(",", lists:reverse(Parts)))
    end;
format_tres_string(_) ->
    <<>>.

%% @doc Parse slurmdbd TRES string to FLURM TRES map
parse_tres_string(<<>>) ->
    #{};
parse_tres_string(TresString) when is_binary(TresString) ->
    Parts = binary:split(TresString, <<",">>, [global]),
    lists:foldl(fun(Part, Acc) ->
        case binary:split(Part, <<"=">>) of
            [IdBin, ValueBin] ->
                try
                    Id = binary_to_integer(IdBin),
                    Value = binary_to_integer(ValueBin),
                    Key = tres_id_to_key(Id),
                    case Key of
                        undefined -> Acc;
                        K -> Acc#{K => Value}
                    end
                catch
                    _:_ -> Acc
                end;
            _ ->
                Acc
        end
    end, #{}, Parts);
parse_tres_string(_) ->
    #{}.

%% @private Map TRES key atom to slurmdbd type ID
tres_key_to_id(cpu) -> 1;
tres_key_to_id(mem) -> 2;
tres_key_to_id(energy) -> 3;
tres_key_to_id(node) -> 4;
tres_key_to_id(billing) -> 5;
tres_key_to_id(fs_disk) -> 6;
tres_key_to_id(vmem) -> 7;
tres_key_to_id(pages) -> 8;
tres_key_to_id(gpu) -> 1001;  % GRES/gpu
tres_key_to_id(_) -> undefined.

%% @private Map slurmdbd type ID to TRES key atom
tres_id_to_key(1) -> cpu;
tres_id_to_key(2) -> mem;
tres_id_to_key(3) -> energy;
tres_id_to_key(4) -> node;
tres_id_to_key(5) -> billing;
tres_id_to_key(6) -> fs_disk;
tres_id_to_key(7) -> vmem;
tres_id_to_key(8) -> pages;
tres_id_to_key(Id) when Id >= 1001 -> gpu;  % Simplified: all GRES -> gpu
tres_id_to_key(_) -> undefined.

%% @doc Build INSERT ... ON DUPLICATE KEY UPDATE query for job_table
build_insert_query(TableName) ->
    iolist_to_binary([
        "INSERT INTO ", TableName, " (",
        "id_job, job_name, id_user, id_group, account, `partition`, state, exit_code, ",
        "nodes_alloc, cpus_req, time_submit, time_eligible, time_start, time_end, ",
        "timelimit, tres_alloc, tres_req, work_dir, std_out, std_err",
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ",
        "ON DUPLICATE KEY UPDATE ",
        "job_name = VALUES(job_name), state = VALUES(state), exit_code = VALUES(exit_code), ",
        "nodes_alloc = VALUES(nodes_alloc), time_start = VALUES(time_start), ",
        "time_end = VALUES(time_end), tres_alloc = VALUES(tres_alloc)"
    ]).

%% @private Build INSERT parameters from slurmdbd record map
build_insert_params(Record) ->
    [
        maps:get(id_job, Record, 0),
        maps:get(job_name, Record, <<>>),
        maps:get(id_user, Record, 0),
        maps:get(id_group, Record, 0),
        maps:get(account, Record, <<>>),
        maps:get(partition, Record, <<>>),
        maps:get(state, Record, 0),
        maps:get(exit_code, Record, 0),
        maps:get(nodes_alloc, Record, 0),
        maps:get(cpus_req, Record, 0),
        maps:get(time_submit, Record, 0),
        maps:get(time_eligible, Record, 0),
        maps:get(time_start, Record, 0),
        maps:get(time_end, Record, 0),
        maps:get(timelimit, Record, 0),
        maps:get(tres_alloc, Record, <<>>),
        maps:get(tres_req, Record, <<>>),
        maps:get(work_dir, Record, <<>>),
        maps:get(std_out, Record, <<>>),
        maps:get(std_err, Record, <<>>)
    ].

%% @doc Build SELECT query for reading historical jobs
build_select_query(TableName, Options) ->
    StartTime = maps:get(start_time, Options, 0),
    EndTime = maps:get(end_time, Options, erlang:system_time(second)),
    Filters = maps:get(filters, Options, #{}),

    BaseQuery = iolist_to_binary([
        "SELECT id_job, job_name, id_user, id_group, account, `partition`, state, ",
        "exit_code, nodes_alloc, cpus_req, time_submit, time_eligible, time_start, ",
        "time_end, timelimit, tres_alloc, tres_req, work_dir, std_out, std_err ",
        "FROM ", TableName, " WHERE time_submit >= ? AND time_submit <= ?"
    ]),

    BaseParams = [StartTime, EndTime],

    %% Add optional filters
    {FilterQuery, FilterParams} = build_filters(Filters),

    %% Add ORDER BY and LIMIT
    Limit = maps:get(limit, Filters, 1000),
    FinalQuery = iolist_to_binary([
        BaseQuery, FilterQuery, " ORDER BY time_submit DESC LIMIT ", integer_to_binary(Limit)
    ]),

    {FinalQuery, BaseParams ++ FilterParams}.

%% @private Build WHERE clause additions from filters
build_filters(Filters) ->
    {QueryParts, Params} = maps:fold(fun
        (user, User, {Q, P}) when is_binary(User) ->
            {[" AND id_user = ?" | Q], [User | P]};
        (account, Account, {Q, P}) when is_binary(Account) ->
            {[" AND account = ?" | Q], [Account | P]};
        (partition, Partition, {Q, P}) when is_binary(Partition) ->
            {[" AND `partition` = ?" | Q], [Partition | P]};
        (state, State, {Q, P}) when is_atom(State) ->
            {[" AND state = ?" | Q], [state_to_slurmdbd(State) | P]};
        (_, _, Acc) ->
            Acc
    end, {[], []}, Filters),
    {iolist_to_binary(lists:reverse(QueryParts)), lists:reverse(Params)}.

%% @private Convert MySQL row to map using column names
row_to_map(Columns, Row) ->
    lists:foldl(fun({Col, Val}, Acc) ->
        Key = column_to_atom(Col),
        Acc#{Key => Val}
    end, #{}, lists:zip(Columns, tuple_to_list(Row))).

%% @private Convert column name binary to atom
column_to_atom(<<"id_job">>) -> id_job;
column_to_atom(<<"job_name">>) -> job_name;
column_to_atom(<<"id_user">>) -> id_user;
column_to_atom(<<"id_group">>) -> id_group;
column_to_atom(<<"account">>) -> account;
column_to_atom(<<"partition">>) -> partition;
column_to_atom(<<"state">>) -> state;
column_to_atom(<<"exit_code">>) -> exit_code;
column_to_atom(<<"nodes_alloc">>) -> nodes_alloc;
column_to_atom(<<"cpus_req">>) -> cpus_req;
column_to_atom(<<"time_submit">>) -> time_submit;
column_to_atom(<<"time_eligible">>) -> time_eligible;
column_to_atom(<<"time_start">>) -> time_start;
column_to_atom(<<"time_end">>) -> time_end;
column_to_atom(<<"timelimit">>) -> timelimit;
column_to_atom(<<"tres_alloc">>) -> tres_alloc;
column_to_atom(<<"tres_req">>) -> tres_req;
column_to_atom(<<"work_dir">>) -> work_dir;
column_to_atom(<<"std_out">>) -> std_out;
column_to_atom(<<"std_err">>) -> std_err;
column_to_atom(Other) -> binary_to_atom(Other, utf8).
