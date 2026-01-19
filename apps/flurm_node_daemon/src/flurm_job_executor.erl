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
%% Power monitoring API (used by node daemon for heartbeat)
-export([get_current_power/0, get_rapl_power/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([
    create_script_file/2,
    build_environment/1,
    setup_cgroup/3,
    setup_cgroup_v2/3,
    setup_cgroup_v1/3,
    cleanup_cgroup/1,
    cleanup_job/1,
    write_output_files/4,
    report_completion/3,
    report_completion/4,
    cancel_timeout/1,
    now_ms/0,
    execute_prolog/2,
    execute_epilog/3,
    execute_script/4,
    read_current_energy/0,
    read_rapl_energy/0,
    sum_rapl_energies/3
]).
-endif.

%% Suppress warnings for power monitoring internal functions
%% sum_rapl_powers is a helper called by get_rapl_power
-compile([{nowarn_unused_function, [{sum_rapl_powers, 3}]}]).

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
    timeout_ref :: reference() | undefined,
    std_out :: binary() | undefined,   % Output file path
    std_err :: binary() | undefined,   % Stderr file path (empty = merge with stdout)
    gpus :: list(),                    % Allocated GPU indices
    energy_start :: non_neg_integer(), % Energy reading at job start (microjoules)
    prolog_status :: ok | {error, term()} | undefined,
    epilog_status :: ok | {error, term()} | undefined
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
    StdOut = maps:get(std_out, JobSpec, undefined),
    StdErr = maps:get(std_err, JobSpec, undefined),
    GPUs = maps:get(gpus, JobSpec, []),

    lager:info("Job executor ~p started for job ~p (CPUs: ~p, Memory: ~pMB, TimeLimit: ~p, GPUs: ~p, StdOut: ~p)",
               [self(), JobId, NumCpus, MemoryMB, TimeLimit, GPUs, StdOut]),

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
        output = <<>>,
        std_out = StdOut,
        std_err = StdErr,
        gpus = GPUs,
        energy_start = 0,
        prolog_status = undefined,
        epilog_status = undefined
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

    %% Create environment (includes GPU allocation)
    EnvList = build_environment(State),

    %% Read initial energy consumption for accounting
    EnergyStart = read_current_energy(),

    %% Execute prolog script if configured
    PrologStatus = execute_prolog(JobId, EnvList),
    case PrologStatus of
        {error, PrologReason} ->
            lager:error("Prolog failed for job ~p: ~p", [JobId, PrologReason]),
            cleanup_job(State#state{script_path = ScriptPath, cgroup_path = CgroupPath}),
            report_completion(failed, -1, State),
            {stop, normal, State#state{status = failed, exit_code = -1, prolog_status = PrologStatus}};
        ok ->
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
                    timeout_ref = TimeoutRef,
                    energy_start = EnergyStart,
                    prolog_status = ok
                }}
            catch
                _:Error ->
                    lager:error("Failed to execute job ~p: ~p", [JobId, Error]),
                    cleanup_job(State#state{script_path = ScriptPath, cgroup_path = CgroupPath}),
                    report_completion(failed, -1, State),
                    {stop, normal, State#state{status = failed, exit_code = -1}}
            end
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

    %% Calculate energy consumed during job
    EnergyEnd = read_current_energy(),
    EnergyUsed = case {State#state.energy_start, EnergyEnd} of
        {Start, End} when Start > 0, End > Start -> End - Start;
        _ -> 0
    end,

    lager:info("Job ~p finished with status ~p (exit code: ~p, duration: ~pms, energy: ~p uJ)",
               [JobId, Status, ExitCode, Duration, EnergyUsed]),

    %% Cancel timeout timer if it was set
    cancel_timeout(State#state.timeout_ref),

    %% Execute epilog script
    EnvList = build_environment(State),
    EpilogStatus = execute_epilog(JobId, ExitCode, EnvList),
    case EpilogStatus of
        {error, EpilogReason} ->
            lager:warning("Epilog failed for job ~p: ~p", [JobId, EpilogReason]);
        ok ->
            ok
    end,

    %% Cleanup resources
    cleanup_job(State),

    %% Report completion to controller with energy data
    report_completion(Status, ExitCode, State#state{epilog_status = EpilogStatus}, EnergyUsed),

    {stop, normal, State#state{
        status = Status,
        exit_code = ExitCode,
        end_time = EndTime,
        port = undefined,
        timeout_ref = undefined,
        epilog_status = EpilogStatus
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

    %% Calculate energy consumed during job
    EnergyEnd = read_current_energy(),
    EnergyUsed = case {State#state.energy_start, EnergyEnd} of
        {Start, End} when Start > 0, End > Start -> End - Start;
        _ -> 0
    end,

    lager:info("Job ~p timed out after ~pms (energy: ~p uJ)", [JobId, Duration, EnergyUsed]),

    %% Execute epilog script even on timeout
    EnvList = build_environment(State),
    EpilogStatus = execute_epilog(JobId, -14, EnvList),

    %% Cleanup resources
    cleanup_job(State),

    %% Report timeout as a special failure with energy data
    report_completion(timeout, -14, State#state{epilog_status = EpilogStatus}, EnergyUsed),

    {stop, normal, State#state{
        status = timeout,
        exit_code = -14,
        end_time = EndTime,
        port = undefined,
        timeout_ref = undefined,
        epilog_status = EpilogStatus
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

build_environment(#state{job_id = JobId, environment = Env, num_cpus = Cpus,
                         memory_mb = Mem, gpus = GPUs}) ->
    %% Build environment list for port
    BaseEnv = [
        {"FLURM_JOB_ID", integer_to_list(JobId)},
        {"FLURM_JOB_CPUS", integer_to_list(Cpus)},
        {"FLURM_JOB_MEMORY_MB", integer_to_list(Mem)},
        {"SLURM_JOB_ID", integer_to_list(JobId)},  % SLURM compatibility
        {"SLURM_CPUS_ON_NODE", integer_to_list(Cpus)},
        {"SLURM_MEM_PER_NODE", integer_to_list(Mem)}
    ],
    %% Add GPU environment variables if GPUs allocated
    GpuEnv = case GPUs of
        [] -> [];
        _ ->
            GpuList = string:join([integer_to_list(G) || G <- GPUs], ","),
            [
                {"FLURM_GPUS", GpuList},
                {"CUDA_VISIBLE_DEVICES", GpuList},
                {"GPU_DEVICE_ORDINAL", GpuList},   % AMD ROCm compatibility
                {"SLURM_JOB_GPUS", GpuList}        % SLURM compatibility
            ]
    end,
    %% Add user-specified environment
    UserEnv = [{binary_to_list(K), binary_to_list(V)} ||
               {K, V} <- maps:to_list(Env), is_binary(K), is_binary(V)],
    BaseEnv ++ GpuEnv ++ UserEnv.

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

cleanup_job(#state{script_path = ScriptPath, cgroup_path = CgroupPath,
                   output = Output, std_out = StdOut, std_err = StdErr,
                   job_id = JobId}) ->
    lager:info("cleanup_job called for job ~p, output size: ~p, std_out: ~p",
               [JobId, byte_size(Output), StdOut]),
    %% Write output to file(s)
    try
        write_output_files(JobId, Output, StdOut, StdErr)
    catch
        Error:Reason:Stack ->
            lager:error("write_output_files failed: ~p:~p~n~p", [Error, Reason, Stack])
    end,
    %% Remove script file
    case ScriptPath of
        undefined -> ok;
        _ -> file:delete(ScriptPath)
    end,
    %% Remove cgroup
    cleanup_cgroup(CgroupPath),
    ok;
cleanup_job(State) ->
    lager:error("cleanup_job pattern match failed, state: ~p", [State]).

%% Write job output to file(s)
write_output_files(JobId, Output, StdOut, _StdErr) ->
    OutPath = case StdOut of
        undefined -> "/tmp/slurm-" ++ integer_to_list(JobId) ++ ".out";
        <<>> -> "/tmp/slurm-" ++ integer_to_list(JobId) ++ ".out";
        Path when is_binary(Path) -> binary_to_list(Path)
    end,
    %% Ensure parent directory exists
    case filelib:ensure_dir(OutPath) of
        ok ->
            case file:write_file(OutPath, Output) of
                ok ->
                    lager:info("Job ~p output written to ~s (~p bytes)",
                              [JobId, OutPath, byte_size(Output)]);
                {error, Reason} ->
                    lager:error("Failed to write job ~p output to ~s: ~p",
                               [JobId, OutPath, Reason])
            end;
        {error, DirReason} ->
            lager:error("Failed to create output directory for ~s: ~p",
                       [OutPath, DirReason])
    end.

report_completion(Status, ExitCode, State) ->
    %% Default to 0 energy for backward compatibility
    report_completion(Status, ExitCode, State, 0).

report_completion(Status, ExitCode, #state{job_id = JobId, output = Output}, EnergyUsed) ->
    %% Report to controller connector with energy data
    case Status of
        completed ->
            flurm_controller_connector:report_job_complete(JobId, ExitCode, Output, EnergyUsed);
        cancelled ->
            flurm_controller_connector:report_job_failed(JobId, cancelled, Output, EnergyUsed);
        timeout ->
            flurm_controller_connector:report_job_failed(JobId, timeout, Output, EnergyUsed);
        failed ->
            flurm_controller_connector:report_job_failed(JobId, {exit_code, ExitCode}, Output, EnergyUsed)
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

%%====================================================================
%% Prolog/Epilog Script Execution
%%====================================================================

%% @doc Execute prolog script before job starts
%% Returns ok on success, {error, Reason} on failure
-spec execute_prolog(pos_integer(), list()) -> ok | {error, term()}.
execute_prolog(JobId, EnvList) ->
    case application:get_env(flurm_node_daemon, prolog_path) of
        {ok, PrologPath} when is_list(PrologPath), PrologPath =/= "" ->
            execute_script(PrologPath, JobId, EnvList, "prolog");
        _ ->
            ok  % No prolog configured
    end.

%% @doc Execute epilog script after job ends
%% Returns ok on success, {error, Reason} on failure
-spec execute_epilog(pos_integer(), integer(), list()) -> ok | {error, term()}.
execute_epilog(JobId, ExitCode, EnvList) ->
    case application:get_env(flurm_node_daemon, epilog_path) of
        {ok, EpilogPath} when is_list(EpilogPath), EpilogPath =/= "" ->
            %% Add exit code to environment for epilog
            EpilogEnv = [{"FLURM_JOB_EXIT_CODE", integer_to_list(ExitCode)} | EnvList],
            execute_script(EpilogPath, JobId, EpilogEnv, "epilog");
        _ ->
            ok  % No epilog configured
    end.

%% @doc Execute a prolog/epilog script
-spec execute_script(string(), pos_integer(), list(), string()) -> ok | {error, term()}.
execute_script(ScriptPath, JobId, EnvList, ScriptType) ->
    case filelib:is_regular(ScriptPath) of
        true ->
            lager:info("Executing ~s script ~s for job ~p", [ScriptType, ScriptPath, JobId]),
            %% Run script with timeout (60 seconds max for prolog/epilog)
            PortOpts = [
                {env, EnvList},
                exit_status,
                use_stdio,
                binary,
                stderr_to_stdout
            ],
            try
                Port = open_port({spawn_executable, ScriptPath}, [{args, []} | PortOpts]),
                receive
                    {Port, {exit_status, 0}} ->
                        lager:info("~s script completed successfully for job ~p", [ScriptType, JobId]),
                        ok;
                    {Port, {exit_status, Code}} ->
                        lager:error("~s script failed for job ~p with exit code ~p",
                                   [ScriptType, JobId, Code]),
                        {error, {script_failed, Code}};
                    {Port, {data, _Data}} ->
                        %% Collect output but we don't need it
                        receive
                            {Port, {exit_status, Code}} ->
                                case Code of
                                    0 -> ok;
                                    _ -> {error, {script_failed, Code}}
                                end
                        after 60000 ->
                            catch port_close(Port),
                            {error, timeout}
                        end
                after 60000 ->
                    catch port_close(Port),
                    lager:error("~s script timed out for job ~p", [ScriptType, JobId]),
                    {error, timeout}
                end
            catch
                _:Error ->
                    lager:error("Failed to execute ~s script for job ~p: ~p",
                               [ScriptType, JobId, Error]),
                    {error, Error}
            end;
        false ->
            lager:warning("~s script not found: ~s", [ScriptType, ScriptPath]),
            {error, {script_not_found, ScriptPath}}
    end.

%%====================================================================
%% Energy Accounting (Linux RAPL interface)
%%====================================================================

%% @doc Read current energy consumption from system
%% Returns cumulative energy in microjoules, or 0 if not available
-spec read_current_energy() -> non_neg_integer().
read_current_energy() ->
    case os:type() of
        {unix, linux} ->
            read_rapl_energy();
        _ ->
            0
    end.

%% @doc Read energy from Intel RAPL (Running Average Power Limit) interface
-spec read_rapl_energy() -> non_neg_integer().
read_rapl_energy() ->
    %% Try to read from powercap RAPL interface
    RaplBasePath = "/sys/class/powercap",
    case filelib:is_dir(RaplBasePath) of
        true ->
            %% Find all intel-rapl:X domains and sum their energy
            case file:list_dir(RaplBasePath) of
                {ok, Entries} ->
                    RaplDomains = [E || E <- Entries,
                                        lists:prefix("intel-rapl:", E)],
                    sum_rapl_energies(RaplBasePath, RaplDomains, 0);
                _ ->
                    0
            end;
        false ->
            0
    end.

%% @doc Sum energy readings from multiple RAPL domains
-spec sum_rapl_energies(string(), [string()], non_neg_integer()) -> non_neg_integer().
sum_rapl_energies(_BasePath, [], Acc) ->
    Acc;
sum_rapl_energies(BasePath, [Domain | Rest], Acc) ->
    EnergyFile = filename:join([BasePath, Domain, "energy_uj"]),
    Energy = case file:read_file(EnergyFile) of
        {ok, Content} ->
            try
                binary_to_integer(string:trim(Content))
            catch
                _:_ -> 0
            end;
        _ ->
            0
    end,
    sum_rapl_energies(BasePath, Rest, Acc + Energy).

%% @doc Get current power consumption in watts (for heartbeat)
-spec get_current_power() -> float().
get_current_power() ->
    case os:type() of
        {unix, linux} ->
            get_rapl_power();
        _ ->
            0.0
    end.

%% @doc Read instantaneous power from RAPL (if available)
-spec get_rapl_power() -> float().
get_rapl_power() ->
    %% Some systems expose power_uw directly
    RaplBasePath = "/sys/class/powercap",
    case filelib:is_dir(RaplBasePath) of
        true ->
            case file:list_dir(RaplBasePath) of
                {ok, Entries} ->
                    RaplDomains = [E || E <- Entries,
                                        lists:prefix("intel-rapl:", E)],
                    sum_rapl_powers(RaplBasePath, RaplDomains, 0.0);
                _ ->
                    0.0
            end;
        false ->
            0.0
    end.

%% @doc Sum power readings from multiple RAPL domains
-spec sum_rapl_powers(string(), [string()], float()) -> float().
sum_rapl_powers(_BasePath, [], Acc) ->
    Acc;
sum_rapl_powers(BasePath, [Domain | Rest], Acc) ->
    PowerFile = filename:join([BasePath, Domain, "power_uw"]),
    Power = case file:read_file(PowerFile) of
        {ok, Content} ->
            try
                %% Convert microwatts to watts
                binary_to_integer(string:trim(Content)) / 1000000.0
            catch
                _:_ -> 0.0
            end;
        _ ->
            0.0
    end,
    sum_rapl_powers(BasePath, Rest, Acc + Power).
