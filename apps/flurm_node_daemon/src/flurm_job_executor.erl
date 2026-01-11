%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor
%%%
%%% Executes a single job on the compute node. Manages the job
%%% lifecycle including:
%%% - Resource setup (cgroups on Linux)
%%% - Environment configuration
%%% - Script execution
%%% - Output capture
%%% - Completion reporting
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor).

-behaviour(gen_server).

-export([start_link/1]).
-export([get_status/1, cancel/1, get_output/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(MAX_OUTPUT_SIZE, 1024 * 1024).  % 1MB max output

-record(state, {
    job_id :: pos_integer(),
    script :: binary(),
    working_dir :: binary(),
    environment :: map(),
    num_cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    time_limit :: pos_integer() | undefined,  % seconds, undefined = no limit
    port :: port() | undefined,
    status :: pending | running | completed | failed | cancelled | timeout,
    exit_code :: integer() | undefined,
    output :: binary(),
    start_time :: integer() | undefined,
    end_time :: integer() | undefined,
    cgroup_path :: string() | undefined,
    script_path :: string() | undefined,
    timeout_ref :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

start_link(JobSpec) ->
    gen_server:start_link(?MODULE, JobSpec, []).

%% @doc Get the current status of the job
-spec get_status(pid()) -> map().
get_status(Pid) ->
    gen_server:call(Pid, get_status).

%% @doc Cancel the job
-spec cancel(pid()) -> ok.
cancel(Pid) ->
    gen_server:cast(Pid, cancel).

%% @doc Get job output
-spec get_output(pid()) -> binary().
get_output(Pid) ->
    gen_server:call(Pid, get_output).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(JobSpec) ->
    JobId = maps:get(job_id, JobSpec),
    Script = maps:get(script, JobSpec, <<>>),
    WorkingDir = maps:get(working_dir, JobSpec, <<"/tmp">>),
    Environment = maps:get(environment, JobSpec, #{}),
    NumCpus = maps:get(num_cpus, JobSpec, 1),
    MemoryMB = maps:get(memory_mb, JobSpec, 1024),
    TimeLimit = maps:get(time_limit, JobSpec, undefined),

    lager:info("Job executor ~p started for job ~p (CPUs: ~p, Memory: ~pMB, TimeLimit: ~p)",
               [self(), JobId, NumCpus, MemoryMB, TimeLimit]),

    %% Start execution asynchronously
    self() ! setup_and_execute,

    {ok, #state{
        job_id = JobId,
        script = Script,
        working_dir = WorkingDir,
        environment = Environment,
        num_cpus = NumCpus,
        memory_mb = MemoryMB,
        time_limit = TimeLimit,
        status = pending,
        output = <<>>
    }}.

handle_call(get_status, _From, State) ->
    Status = #{
        job_id => State#state.job_id,
        status => State#state.status,
        exit_code => State#state.exit_code,
        start_time => State#state.start_time,
        end_time => State#state.end_time,
        output_size => byte_size(State#state.output)
    },
    {reply, Status, State};

handle_call(get_output, _From, State) ->
    {reply, State#state.output, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(cancel, #state{port = Port, job_id = JobId} = State) ->
    lager:info("Cancelling job ~p", [JobId]),
    case Port of
        undefined ->
            ok;
        _ ->
            %% Send SIGTERM first, then SIGKILL after timeout
            catch port_close(Port)
    end,
    %% Cancel timeout timer if set
    cancel_timeout(State#state.timeout_ref),
    cleanup_job(State),
    report_completion(cancelled, -15, State),
    {stop, normal, State#state{status = cancelled, end_time = now_ms(), timeout_ref = undefined}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(setup_and_execute, #state{job_id = JobId} = State) ->
    %% Setup cgroup for resource isolation (Linux only)
    CgroupPath = setup_cgroup(JobId, State#state.num_cpus, State#state.memory_mb),

    %% Create the script file
    ScriptPath = create_script_file(JobId, State#state.script),

    %% Create environment
    EnvList = build_environment(State),

    %% Execute the script using a port
    PortOpts = [
        {cd, binary_to_list(State#state.working_dir)},
        {env, EnvList},
        exit_status,
        use_stdio,
        binary,
        stderr_to_stdout
    ],

    try
        %% Wrap execution in cgroup if available
        {Executable, Args} = case CgroupPath of
            undefined ->
                {"/bin/bash", [ScriptPath]};
            _ ->
                %% Use cgexec to run in cgroup
                case os:find_executable("cgexec") of
                    false ->
                        {"/bin/bash", [ScriptPath]};
                    CgExec ->
                        {CgExec, ["-g", "memory,cpu:flurm_" ++ integer_to_list(JobId),
                                  "/bin/bash", ScriptPath]}
                end
        end,

        Port = open_port({spawn_executable, Executable}, [{args, Args} | PortOpts]),
        StartTime = now_ms(),

        %% Set timeout timer if time_limit is specified
        TimeoutRef = case State#state.time_limit of
            undefined ->
                undefined;
            TimeLimitSecs when is_integer(TimeLimitSecs), TimeLimitSecs > 0 ->
                %% Convert seconds to milliseconds
                lager:info("Job ~p will timeout in ~p seconds", [JobId, TimeLimitSecs]),
                erlang:send_after(TimeLimitSecs * 1000, self(), job_timeout);
            _ ->
                undefined
        end,

        lager:info("Job ~p started execution (pid: port)", [JobId]),

        {noreply, State#state{
            port = Port,
            status = running,
            start_time = StartTime,
            cgroup_path = CgroupPath,
            script_path = ScriptPath,
            timeout_ref = TimeoutRef
        }}
    catch
        _:Error ->
            lager:error("Failed to execute job ~p: ~p", [JobId, Error]),
            cleanup_job(State#state{script_path = ScriptPath, cgroup_path = CgroupPath}),
            report_completion(failed, -1, State),
            {stop, normal, State#state{status = failed, exit_code = -1}}
    end;

handle_info({Port, {data, Data}}, #state{port = Port, output = Output} = State) ->
    %% Accumulate output, but cap at max size
    NewOutput = case byte_size(Output) + byte_size(Data) > ?MAX_OUTPUT_SIZE of
        true ->
            %% Truncate to avoid memory issues
            TruncatedData = binary:part(Data, 0, min(byte_size(Data),
                                        ?MAX_OUTPUT_SIZE - byte_size(Output))),
            <<Output/binary, TruncatedData/binary>>;
        false ->
            <<Output/binary, Data/binary>>
    end,
    {noreply, State#state{output = NewOutput}};

handle_info({Port, {exit_status, ExitCode}}, #state{port = Port, job_id = JobId} = State) ->
    EndTime = now_ms(),
    Status = case ExitCode of
        0 -> completed;
        _ -> failed
    end,

    Duration = case State#state.start_time of
        undefined -> 0;
        ST -> EndTime - ST
    end,

    lager:info("Job ~p finished with status ~p (exit code: ~p, duration: ~pms)",
               [JobId, Status, ExitCode, Duration]),

    %% Cancel timeout timer if it was set
    cancel_timeout(State#state.timeout_ref),

    %% Cleanup resources
    cleanup_job(State),

    %% Report completion to controller
    report_completion(Status, ExitCode, State),

    {stop, normal, State#state{
        status = Status,
        exit_code = ExitCode,
        end_time = EndTime,
        port = undefined,
        timeout_ref = undefined
    }};

handle_info(job_timeout, #state{port = Port, job_id = JobId} = State) ->
    lager:warning("Job ~p exceeded time limit (~p seconds), terminating",
                  [JobId, State#state.time_limit]),

    %% Kill the port/process
    case Port of
        undefined ->
            ok;
        _ ->
            catch port_close(Port)
    end,

    EndTime = now_ms(),
    Duration = case State#state.start_time of
        undefined -> 0;
        ST -> EndTime - ST
    end,

    lager:info("Job ~p timed out after ~pms", [JobId, Duration]),

    %% Cleanup resources
    cleanup_job(State),

    %% Report timeout as a special failure
    report_completion(timeout, -14, State),  % -14 like SIGALRM

    {stop, normal, State#state{
        status = timeout,
        exit_code = -14,
        end_time = EndTime,
        port = undefined,
        timeout_ref = undefined
    }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{job_id = JobId} = State) ->
    lager:debug("Job ~p executor terminating: ~p", [JobId, Reason]),
    cleanup_job(State),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

create_script_file(JobId, Script) ->
    Filename = "/tmp/flurm_job_" ++ integer_to_list(JobId) ++ ".sh",
    ok = file:write_file(Filename, Script),
    ok = file:change_mode(Filename, 8#755),
    Filename.

build_environment(#state{job_id = JobId, environment = Env, num_cpus = Cpus, memory_mb = Mem}) ->
    %% Build environment list for port
    BaseEnv = [
        {"FLURM_JOB_ID", integer_to_list(JobId)},
        {"FLURM_JOB_CPUS", integer_to_list(Cpus)},
        {"FLURM_JOB_MEMORY_MB", integer_to_list(Mem)},
        {"SLURM_JOB_ID", integer_to_list(JobId)},  % SLURM compatibility
        {"SLURM_CPUS_ON_NODE", integer_to_list(Cpus)},
        {"SLURM_MEM_PER_NODE", integer_to_list(Mem)}
    ],
    %% Add user-specified environment
    UserEnv = [{binary_to_list(K), binary_to_list(V)} ||
               {K, V} <- maps:to_list(Env), is_binary(K), is_binary(V)],
    BaseEnv ++ UserEnv.

%%====================================================================
%% Cgroup functions (Linux resource isolation)
%%====================================================================

setup_cgroup(JobId, NumCpus, MemoryMB) ->
    case os:type() of
        {unix, linux} ->
            CgroupName = "flurm_" ++ integer_to_list(JobId),
            %% Try cgroups v2 first, fall back to v1
            case setup_cgroup_v2(CgroupName, NumCpus, MemoryMB) of
                {ok, Path} ->
                    Path;
                {error, _} ->
                    case setup_cgroup_v1(CgroupName, NumCpus, MemoryMB) of
                        {ok, Path} -> Path;
                        {error, _} -> undefined
                    end
            end;
        _ ->
            undefined
    end.

setup_cgroup_v2(CgroupName, NumCpus, MemoryMB) ->
    %% Cgroups v2 unified hierarchy
    CgroupPath = "/sys/fs/cgroup/" ++ CgroupName,
    case filelib:is_dir("/sys/fs/cgroup/cgroup.controllers") of
        true ->
            try
                ok = filelib:ensure_dir(CgroupPath ++ "/"),
                file:make_dir(CgroupPath),
                %% Set memory limit
                MemBytes = MemoryMB * 1024 * 1024,
                file:write_file(CgroupPath ++ "/memory.max",
                               integer_to_list(MemBytes)),
                %% Set CPU limit (100000 = 100% of one CPU)
                CpuMax = NumCpus * 100000,
                file:write_file(CgroupPath ++ "/cpu.max",
                               io_lib:format("~p 100000", [CpuMax])),
                {ok, CgroupPath}
            catch
                _:_ -> {error, cgroup_setup_failed}
            end;
        false ->
            {error, cgroup_v2_not_available}
    end.

setup_cgroup_v1(CgroupName, NumCpus, MemoryMB) ->
    %% Cgroups v1 separate hierarchies
    MemCgroupPath = "/sys/fs/cgroup/memory/" ++ CgroupName,
    CpuCgroupPath = "/sys/fs/cgroup/cpu/" ++ CgroupName,

    case filelib:is_dir("/sys/fs/cgroup/memory") of
        true ->
            try
                %% Create memory cgroup
                file:make_dir(MemCgroupPath),
                MemBytes = MemoryMB * 1024 * 1024,
                file:write_file(MemCgroupPath ++ "/memory.limit_in_bytes",
                               integer_to_list(MemBytes)),

                %% Create CPU cgroup
                file:make_dir(CpuCgroupPath),
                %% CPU shares: 1024 per CPU
                CpuShares = NumCpus * 1024,
                file:write_file(CpuCgroupPath ++ "/cpu.shares",
                               integer_to_list(CpuShares)),

                {ok, MemCgroupPath}
            catch
                _:_ -> {error, cgroup_setup_failed}
            end;
        false ->
            {error, cgroup_v1_not_available}
    end.

cleanup_cgroup(undefined) ->
    ok;
cleanup_cgroup(CgroupPath) ->
    %% Remove cgroup directory (must be empty of processes first)
    try
        %% Kill any remaining processes in cgroup
        case file:read_file(CgroupPath ++ "/cgroup.procs") of
            {ok, Pids} ->
                [os:cmd("kill -9 " ++ binary_to_list(P)) ||
                 P <- binary:split(Pids, <<"\n">>, [global]),
                 P =/= <<>>];
            _ ->
                ok
        end,
        %% Remove the cgroup directory
        timer:sleep(100),  % Give processes time to die
        file:del_dir(CgroupPath)
    catch
        _:_ -> ok
    end.

%%====================================================================
%% Cleanup and reporting
%%====================================================================

cleanup_job(#state{script_path = ScriptPath, cgroup_path = CgroupPath}) ->
    %% Remove script file
    case ScriptPath of
        undefined -> ok;
        _ -> file:delete(ScriptPath)
    end,
    %% Remove cgroup
    cleanup_cgroup(CgroupPath),
    ok.

report_completion(Status, ExitCode, #state{job_id = JobId, output = Output}) ->
    %% Report to controller connector
    case Status of
        completed ->
            flurm_controller_connector:report_job_complete(JobId, ExitCode, Output);
        cancelled ->
            flurm_controller_connector:report_job_failed(JobId, cancelled, Output);
        timeout ->
            flurm_controller_connector:report_job_failed(JobId, timeout, Output);
        failed ->
            flurm_controller_connector:report_job_failed(JobId, {exit_code, ExitCode}, Output)
    end.

%% Cancel timeout timer if set
cancel_timeout(undefined) ->
    ok;
cancel_timeout(Ref) ->
    erlang:cancel_timer(Ref),
    %% Flush any pending timeout message
    receive
        job_timeout -> ok
    after 0 ->
        ok
    end.

now_ms() ->
    erlang:system_time(millisecond).
