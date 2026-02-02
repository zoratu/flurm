%%%-------------------------------------------------------------------
%%% @doc SLURM Bridge for Zero-Downtime Migration
%%%
%%% This module enables FLURM to interoperate with existing SLURM
%%% clusters for seamless migration scenarios:
%%%
%%% Migration Phases:
%%% 1. Shadow Mode - FLURM observes SLURM, no job handling
%%% 2. Active Mode - FLURM accepts jobs, can forward to SLURM
%%% 3. Primary Mode - FLURM is primary, SLURM draining
%%% 4. Standalone Mode - SLURM removed, FLURM only
%%%
%%% Job Forwarding:
%%% - Forward FLURM jobs to SLURM for execution
%%% - Track forwarded jobs for status queries
%%% - Handle completion notifications
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_bridge).
-behaviour(gen_server).

-export([
    start_link/0,
    start_link/1,
    %% Configuration
    set_mode/1,
    get_mode/0,
    add_slurm_cluster/2,
    remove_slurm_cluster/1,
    list_slurm_clusters/0,
    %% Job Operations
    forward_job/2,
    get_forwarded_job_status/1,
    cancel_forwarded_job/1,
    %% Status
    get_bridge_status/0,
    is_slurm_available/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-define(SERVER, ?MODULE).
-define(SLURM_CLUSTERS_TABLE, flurm_slurm_clusters).
-define(FORWARDED_JOBS_TABLE, flurm_forwarded_jobs).
-define(DEFAULT_SLURM_PORT, 6817).
-define(HEALTH_CHECK_INTERVAL, 10000).  % 10 seconds
-define(JOB_SYNC_INTERVAL, 5000).       % 5 seconds

%% Migration modes
-type migration_mode() :: shadow | active | primary | standalone.

%% SLURM cluster record
-record(slurm_cluster, {
    name :: binary(),
    host :: binary(),
    port = ?DEFAULT_SLURM_PORT :: non_neg_integer(),
    state = unknown :: up | down | draining | unknown,
    last_check = 0 :: non_neg_integer(),
    consecutive_failures = 0 :: non_neg_integer(),
    job_count = 0 :: non_neg_integer(),
    node_count = 0 :: non_neg_integer(),
    version = <<>> :: binary()
}).

%% Forwarded job record
-record(forwarded_job, {
    flurm_job_id :: non_neg_integer(),
    slurm_job_id :: non_neg_integer(),
    slurm_cluster :: binary(),
    submit_time :: non_neg_integer(),
    state = pending :: pending | running | completed | failed | cancelled,
    last_sync = 0 :: non_neg_integer(),
    job_spec :: map()
}).

%% Server state
-record(state, {
    mode = shadow :: migration_mode(),
    health_timer :: reference() | undefined,
    sync_timer :: reference() | undefined,
    primary_slurm :: binary() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the SLURM bridge with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start the SLURM bridge with options.
-spec start_link(proplists:proplist()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Set the migration mode.
%%
%% Modes:
%% - shadow: Read-only observation of SLURM
%% - active: Accept jobs, can forward to SLURM
%% - primary: FLURM is primary, SLURM draining
%% - standalone: No SLURM integration
%%
-spec set_mode(migration_mode()) -> ok | {error, term()}.
set_mode(Mode) when Mode =:= shadow; Mode =:= active;
                    Mode =:= primary; Mode =:= standalone ->
    gen_server:call(?SERVER, {set_mode, Mode}).

%% @doc Get the current migration mode.
-spec get_mode() -> migration_mode().
get_mode() ->
    gen_server:call(?SERVER, get_mode).

%% @doc Add a SLURM cluster to the bridge.
-spec add_slurm_cluster(binary(), map()) -> ok | {error, term()}.
add_slurm_cluster(Name, Config) ->
    gen_server:call(?SERVER, {add_cluster, Name, Config}).

%% @doc Remove a SLURM cluster from the bridge.
-spec remove_slurm_cluster(binary()) -> ok | {error, term()}.
remove_slurm_cluster(Name) ->
    gen_server:call(?SERVER, {remove_cluster, Name}).

%% @doc List all configured SLURM clusters.
-spec list_slurm_clusters() -> [map()].
list_slurm_clusters() ->
    gen_server:call(?SERVER, list_clusters).

%% @doc Forward a job to a SLURM cluster.
%%
%% Returns the SLURM job ID on success.
%%
-spec forward_job(map(), binary() | auto) ->
    {ok, non_neg_integer()} | {error, term()}.
forward_job(JobSpec, ClusterName) ->
    gen_server:call(?SERVER, {forward_job, JobSpec, ClusterName}, 30000).

%% @doc Get the status of a forwarded job.
-spec get_forwarded_job_status(non_neg_integer()) ->
    {ok, map()} | {error, not_found}.
get_forwarded_job_status(FlurmJobId) ->
    gen_server:call(?SERVER, {get_job_status, FlurmJobId}).

%% @doc Cancel a forwarded job.
-spec cancel_forwarded_job(non_neg_integer()) ->
    ok | {error, term()}.
cancel_forwarded_job(FlurmJobId) ->
    gen_server:call(?SERVER, {cancel_job, FlurmJobId}).

%% @doc Get overall bridge status.
-spec get_bridge_status() -> map().
get_bridge_status() ->
    gen_server:call(?SERVER, get_status).

%% @doc Check if any SLURM cluster is available.
-spec is_slurm_available() -> boolean().
is_slurm_available() ->
    gen_server:call(?SERVER, is_available).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    process_flag(trap_exit, true),

    %% Create ETS tables
    ets:new(?SLURM_CLUSTERS_TABLE, [
        named_table, set, public,
        {keypos, #slurm_cluster.name}
    ]),
    ets:new(?FORWARDED_JOBS_TABLE, [
        named_table, set, public,
        {keypos, #forwarded_job.flurm_job_id}
    ]),

    %% Get initial mode from config
    Mode = proplists:get_value(mode, Opts, shadow),
    PrimarySlurm = proplists:get_value(primary_slurm, Opts, undefined),

    %% Add any pre-configured clusters
    Clusters = proplists:get_value(slurm_clusters, Opts, []),
    lists:foreach(fun({Name, Config}) ->
        do_add_cluster(Name, Config)
    end, Clusters),

    %% Start health check timer
    HealthTimer = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),

    %% Start job sync timer if not in standalone mode
    SyncTimer = case Mode of
        standalone -> undefined;
        _ -> erlang:send_after(?JOB_SYNC_INTERVAL, self(), sync_jobs)
    end,

    lager:info("SLURM bridge started in ~p mode", [Mode]),

    {ok, #state{
        mode = Mode,
        health_timer = HealthTimer,
        sync_timer = SyncTimer,
        primary_slurm = PrimarySlurm
    }}.

handle_call({set_mode, NewMode}, _From, State) ->
    OldMode = State#state.mode,
    lager:info("SLURM bridge mode changing: ~p -> ~p", [OldMode, NewMode]),

    %% Handle mode transition
    NewState = case {OldMode, NewMode} of
        {_, standalone} ->
            %% Entering standalone - stop sync timer
            cancel_timer(State#state.sync_timer),
            State#state{mode = NewMode, sync_timer = undefined};
        {standalone, _} ->
            %% Leaving standalone - start sync timer
            Timer = erlang:send_after(?JOB_SYNC_INTERVAL, self(), sync_jobs),
            State#state{mode = NewMode, sync_timer = Timer};
        _ ->
            State#state{mode = NewMode}
    end,
    {reply, ok, NewState};

handle_call(get_mode, _From, State) ->
    {reply, State#state.mode, State};

handle_call({add_cluster, Name, Config}, _From, State) ->
    Result = do_add_cluster(Name, Config),
    {reply, Result, State};

handle_call({remove_cluster, Name}, _From, State) ->
    Result = do_remove_cluster(Name),
    {reply, Result, State};

handle_call(list_clusters, _From, State) ->
    Clusters = ets:tab2list(?SLURM_CLUSTERS_TABLE),
    Result = [format_cluster(C) || C <- Clusters],
    {reply, Result, State};

handle_call({forward_job, JobSpec, ClusterName}, _From, State) ->
    case State#state.mode of
        shadow ->
            {reply, {error, shadow_mode}, State};
        standalone ->
            {reply, {error, standalone_mode}, State};
        _ ->
            Result = do_forward_job(JobSpec, ClusterName, State),
            {reply, Result, State}
    end;

handle_call({get_job_status, FlurmJobId}, _From, State) ->
    Result = do_get_job_status(FlurmJobId),
    {reply, Result, State};

handle_call({cancel_job, FlurmJobId}, _From, State) ->
    Result = do_cancel_job(FlurmJobId),
    {reply, Result, State};

handle_call(get_status, _From, State) ->
    Clusters = ets:tab2list(?SLURM_CLUSTERS_TABLE),
    Jobs = ets:tab2list(?FORWARDED_JOBS_TABLE),

    UpClusters = length([C || C <- Clusters, C#slurm_cluster.state =:= up]),
    PendingJobs = length([J || J <- Jobs, J#forwarded_job.state =:= pending]),
    RunningJobs = length([J || J <- Jobs, J#forwarded_job.state =:= running]),

    Status = #{
        mode => State#state.mode,
        clusters_total => length(Clusters),
        clusters_up => UpClusters,
        clusters_down => length(Clusters) - UpClusters,
        jobs_forwarded => length(Jobs),
        jobs_pending => PendingJobs,
        jobs_running => RunningJobs,
        primary_slurm => State#state.primary_slurm
    },
    {reply, Status, State};

handle_call(is_available, _From, State) ->
    case State#state.mode of
        standalone ->
            {reply, false, State};
        _ ->
            Clusters = ets:tab2list(?SLURM_CLUSTERS_TABLE),
            Available = lists:any(fun(C) -> C#slurm_cluster.state =:= up end, Clusters),
            {reply, Available, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(health_check, State) ->
    %% Check health of all SLURM clusters
    spawn_link(fun() -> check_all_cluster_health() end),
    Timer = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),
    {noreply, State#state{health_timer = Timer}};

handle_info(sync_jobs, State) ->
    case State#state.mode of
        standalone ->
            {noreply, State};
        _ ->
            %% Sync status of forwarded jobs
            spawn_link(fun() -> sync_forwarded_jobs() end),
            Timer = erlang:send_after(?JOB_SYNC_INTERVAL, self(), sync_jobs),
            {noreply, State#state{sync_timer = Timer}}
    end;

handle_info({cluster_health, ClusterName, Status}, State) ->
    update_cluster_health(ClusterName, Status),
    {noreply, State};

handle_info({job_status_update, FlurmJobId, Status}, State) ->
    update_job_status(FlurmJobId, Status),
    {noreply, State};

handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};

handle_info({'EXIT', _Pid, Reason}, State) ->
    lager:warning("SLURM bridge worker exited: ~p", [Reason]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    cancel_timer(State#state.health_timer),
    cancel_timer(State#state.sync_timer),
    catch ets:delete(?SLURM_CLUSTERS_TABLE),
    catch ets:delete(?FORWARDED_JOBS_TABLE),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_add_cluster(Name, Config) ->
    Host = maps:get(host, Config, <<"localhost">>),
    Port = maps:get(port, Config, ?DEFAULT_SLURM_PORT),

    Cluster = #slurm_cluster{
        name = Name,
        host = Host,
        port = Port,
        state = unknown,
        last_check = 0
    },

    ets:insert(?SLURM_CLUSTERS_TABLE, Cluster),
    lager:info("Added SLURM cluster: ~s at ~s:~p", [Name, Host, Port]),
    ok.

do_remove_cluster(Name) ->
    case ets:lookup(?SLURM_CLUSTERS_TABLE, Name) of
        [] ->
            {error, not_found};
        [_Cluster] ->
            %% Check for active forwarded jobs
            Jobs = ets:match_object(?FORWARDED_JOBS_TABLE,
                #forwarded_job{slurm_cluster = Name, _ = '_'}),
            ActiveJobs = [J || J <- Jobs,
                J#forwarded_job.state =:= pending orelse
                J#forwarded_job.state =:= running],
            case ActiveJobs of
                [] ->
                    ets:delete(?SLURM_CLUSTERS_TABLE, Name),
                    lager:info("Removed SLURM cluster: ~s", [Name]),
                    ok;
                _ ->
                    {error, {active_jobs, length(ActiveJobs)}}
            end
    end.

do_forward_job(JobSpec, auto, State) ->
    %% Auto-select cluster - prefer primary, then any up cluster
    case select_cluster(State#state.primary_slurm) of
        {ok, ClusterName} ->
            do_forward_job(JobSpec, ClusterName, State);
        {error, _} = Err ->
            Err
    end;
do_forward_job(JobSpec, ClusterName, _State) ->
    case ets:lookup(?SLURM_CLUSTERS_TABLE, ClusterName) of
        [] ->
            {error, {unknown_cluster, ClusterName}};
        [#slurm_cluster{state = down}] ->
            {error, {cluster_down, ClusterName}};
        [Cluster] ->
            submit_to_slurm(Cluster, JobSpec)
    end.

select_cluster(undefined) ->
    %% No primary, find any up cluster
    Clusters = ets:tab2list(?SLURM_CLUSTERS_TABLE),
    UpClusters = [C || C <- Clusters, C#slurm_cluster.state =:= up],
    case UpClusters of
        [] -> {error, no_clusters_available};
        [First | _] -> {ok, First#slurm_cluster.name}
    end;
select_cluster(PrimaryName) ->
    case ets:lookup(?SLURM_CLUSTERS_TABLE, PrimaryName) of
        [#slurm_cluster{state = up}] ->
            {ok, PrimaryName};
        _ ->
            %% Primary down, fall back to any available
            select_cluster(undefined)
    end.

submit_to_slurm(#slurm_cluster{name = Name, host = Host, port = Port}, JobSpec) ->
    lager:info("Forwarding job to SLURM cluster ~s at ~s:~p", [Name, Host, Port]),

    %% Build batch job request
    Request = build_batch_request(JobSpec),

    %% Connect and submit
    case connect_to_slurm(Host, Port) of
        {ok, Socket} ->
            try
                %% Encode and send request
                {ok, Encoded} = flurm_protocol_codec:encode(
                    ?REQUEST_SUBMIT_BATCH_JOB, Request),
                ok = gen_tcp:send(Socket, Encoded),

                %% Receive response
                case receive_slurm_response(Socket) of
                    {ok, #batch_job_response{job_id = SlurmJobId}} ->
                        %% Generate FLURM job ID and track
                        FlurmJobId = generate_flurm_job_id(),
                        track_forwarded_job(FlurmJobId, SlurmJobId, Name, JobSpec),
                        lager:info("Job forwarded: FLURM ~p -> SLURM ~p on ~s",
                            [FlurmJobId, SlurmJobId, Name]),
                        {ok, FlurmJobId};
                    {ok, #slurm_rc_response{return_code = RC}} when RC =/= 0 ->
                        {error, {slurm_error, RC}};
                    {error, Reason} ->
                        {error, Reason}
                end
            after
                gen_tcp:close(Socket)
            end;
        {error, Reason} ->
            lager:error("Failed to connect to SLURM ~s: ~p", [Name, Reason]),
            {error, {connect_failed, Reason}}
    end.

build_batch_request(JobSpec) ->
    #batch_job_request{
        name = maps:get(name, JobSpec, <<"flurm_job">>),
        script = maps:get(script, JobSpec, <<"#!/bin/bash\necho hello">>),
        partition = maps:get(partition, JobSpec, <<>>),
        min_cpus = maps:get(num_cpus, JobSpec, 1),
        min_nodes = maps:get(num_nodes, JobSpec, 1),
        min_mem_per_node = maps:get(memory_mb, JobSpec, 0),
        time_limit = maps:get(time_limit, JobSpec, 0),
        work_dir = maps:get(work_dir, JobSpec, <<"/tmp">>),
        user_id = maps:get(user_id, JobSpec, 0),
        group_id = maps:get(group_id, JobSpec, 0)
    }.

connect_to_slurm(Host, Port) ->
    HostStr = binary_to_list(Host),
    gen_tcp:connect(HostStr, Port, [
        binary, {packet, raw}, {active, false},
        {send_timeout, 5000}
    ], 5000).

receive_slurm_response(Socket) ->
    case gen_tcp:recv(Socket, 0, 10000) of
        {ok, Data} ->
            case flurm_protocol_codec:decode(Data) of
                {ok, #slurm_msg{body = Body}, _Rest} ->
                    {ok, Body};
                {error, Reason} ->
                    {error, {decode_error, Reason}}
            end;
        {error, Reason} ->
            {error, {recv_error, Reason}}
    end.

generate_flurm_job_id() ->
    %% Use cluster ID 0 for locally-tracked forwarded jobs
    %% The actual SLURM job ID is tracked separately
    erlang:unique_integer([positive, monotonic]) band 16#3FFFFFF.

track_forwarded_job(FlurmJobId, SlurmJobId, ClusterName, JobSpec) ->
    Job = #forwarded_job{
        flurm_job_id = FlurmJobId,
        slurm_job_id = SlurmJobId,
        slurm_cluster = ClusterName,
        submit_time = erlang:system_time(second),
        state = pending,
        job_spec = JobSpec
    },
    ets:insert(?FORWARDED_JOBS_TABLE, Job).

do_get_job_status(FlurmJobId) ->
    case ets:lookup(?FORWARDED_JOBS_TABLE, FlurmJobId) of
        [] ->
            {error, not_found};
        [Job] ->
            {ok, #{
                flurm_job_id => Job#forwarded_job.flurm_job_id,
                slurm_job_id => Job#forwarded_job.slurm_job_id,
                slurm_cluster => Job#forwarded_job.slurm_cluster,
                state => Job#forwarded_job.state,
                submit_time => Job#forwarded_job.submit_time,
                last_sync => Job#forwarded_job.last_sync
            }}
    end.

do_cancel_job(FlurmJobId) ->
    case ets:lookup(?FORWARDED_JOBS_TABLE, FlurmJobId) of
        [] ->
            {error, not_found};
        [Job] ->
            %% Send cancel to SLURM
            case cancel_slurm_job(Job) of
                ok ->
                    ets:update_element(?FORWARDED_JOBS_TABLE, FlurmJobId,
                        {#forwarded_job.state, cancelled}),
                    ok;
                {error, _} = Err ->
                    Err
            end
    end.

cancel_slurm_job(#forwarded_job{slurm_job_id = SlurmJobId, slurm_cluster = ClusterName}) ->
    case ets:lookup(?SLURM_CLUSTERS_TABLE, ClusterName) of
        [#slurm_cluster{host = Host, port = Port}] ->
            case connect_to_slurm(Host, Port) of
                {ok, Socket} ->
                    try
                        Request = #kill_job_request{
                            job_id = SlurmJobId,
                            job_id_str = integer_to_binary(SlurmJobId)
                        },
                        {ok, Encoded} = flurm_protocol_codec:encode(
                            ?REQUEST_KILL_JOB, Request),
                        ok = gen_tcp:send(Socket, Encoded),

                        case receive_slurm_response(Socket) of
                            {ok, #slurm_rc_response{return_code = 0}} -> ok;
                            {ok, #slurm_rc_response{return_code = RC}} ->
                                {error, {slurm_error, RC}};
                            {error, _} = Err -> Err
                        end
                    after
                        gen_tcp:close(Socket)
                    end;
                {error, _} = Err ->
                    Err
            end;
        [] ->
            {error, cluster_not_found}
    end.

check_all_cluster_health() ->
    Clusters = ets:tab2list(?SLURM_CLUSTERS_TABLE),
    lists:foreach(fun(Cluster) ->
        check_cluster_health(Cluster)
    end, Clusters).

check_cluster_health(#slurm_cluster{name = Name, host = Host, port = Port}) ->
    Status = case connect_to_slurm(Host, Port) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            up;
        {error, _} ->
            down
    end,
    ?SERVER ! {cluster_health, Name, Status}.

update_cluster_health(ClusterName, Status) ->
    Now = erlang:system_time(second),
    case ets:lookup(?SLURM_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            NewCluster = case Status of
                up ->
                    Cluster#slurm_cluster{
                        state = up,
                        last_check = Now,
                        consecutive_failures = 0
                    };
                down ->
                    Failures = Cluster#slurm_cluster.consecutive_failures + 1,
                    Cluster#slurm_cluster{
                        state = down,
                        last_check = Now,
                        consecutive_failures = Failures
                    }
            end,
            ets:insert(?SLURM_CLUSTERS_TABLE, NewCluster);
        [] ->
            ok
    end.

sync_forwarded_jobs() ->
    Jobs = ets:tab2list(?FORWARDED_JOBS_TABLE),
    ActiveJobs = [J || J <- Jobs,
        J#forwarded_job.state =:= pending orelse
        J#forwarded_job.state =:= running],
    lists:foreach(fun(Job) ->
        sync_job_status(Job)
    end, ActiveJobs).

sync_job_status(#forwarded_job{flurm_job_id = FlurmJobId,
                               slurm_job_id = SlurmJobId,
                               slurm_cluster = ClusterName}) ->
    case ets:lookup(?SLURM_CLUSTERS_TABLE, ClusterName) of
        [#slurm_cluster{state = up, host = Host, port = Port}] ->
            case query_slurm_job_status(Host, Port, SlurmJobId) of
                {ok, Status} ->
                    ?SERVER ! {job_status_update, FlurmJobId, Status};
                {error, _} ->
                    ok  % Will retry next sync
            end;
        _ ->
            ok  % Cluster down, skip
    end.

query_slurm_job_status(Host, Port, SlurmJobId) ->
    case connect_to_slurm(Host, Port) of
        {ok, Socket} ->
            try
                Request = #job_info_request{
                    job_id = SlurmJobId,
                    show_flags = 0
                },
                {ok, Encoded} = flurm_protocol_codec:encode(
                    ?REQUEST_JOB_INFO, Request),
                ok = gen_tcp:send(Socket, Encoded),

                case receive_slurm_response(Socket) of
                    {ok, #job_info_response{jobs = [JobInfo | _]}} ->
                        State = map_slurm_state(JobInfo#job_info.job_state),
                        {ok, State};
                    {ok, #job_info_response{jobs = []}} ->
                        {ok, completed};  % Job no longer exists
                    {error, _} = Err ->
                        Err
                end
            after
                gen_tcp:close(Socket)
            end;
        {error, _} = Err ->
            Err
    end.

map_slurm_state(State) when is_integer(State) ->
    %% Map SLURM job state constants
    case State of
        0 -> pending;    % JOB_PENDING
        1 -> running;    % JOB_RUNNING
        2 -> pending;    % JOB_SUSPENDED (treat as pending)
        3 -> completed;  % JOB_COMPLETE
        4 -> cancelled;  % JOB_CANCELLED
        5 -> failed;     % JOB_FAILED
        6 -> failed;     % JOB_TIMEOUT
        7 -> failed;     % JOB_NODE_FAIL
        _ -> pending
    end.

update_job_status(FlurmJobId, NewState) ->
    Now = erlang:system_time(second),
    case ets:lookup(?FORWARDED_JOBS_TABLE, FlurmJobId) of
        [Job] ->
            UpdatedJob = Job#forwarded_job{
                state = NewState,
                last_sync = Now
            },
            ets:insert(?FORWARDED_JOBS_TABLE, UpdatedJob),

            %% Log state changes
            case Job#forwarded_job.state =/= NewState of
                true ->
                    lager:info("Forwarded job ~p state: ~p -> ~p",
                        [FlurmJobId, Job#forwarded_job.state, NewState]);
                false ->
                    ok
            end;
        [] ->
            ok
    end.

format_cluster(#slurm_cluster{} = C) ->
    #{
        name => C#slurm_cluster.name,
        host => C#slurm_cluster.host,
        port => C#slurm_cluster.port,
        state => C#slurm_cluster.state,
        last_check => C#slurm_cluster.last_check,
        consecutive_failures => C#slurm_cluster.consecutive_failures,
        job_count => C#slurm_cluster.job_count,
        node_count => C#slurm_cluster.node_count,
        version => C#slurm_cluster.version
    }.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).
