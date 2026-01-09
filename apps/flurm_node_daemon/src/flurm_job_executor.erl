%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor
%%%
%%% Executes a single job on the compute node. Manages the job
%%% lifecycle including setup, execution, and cleanup.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor).

-behaviour(gen_server).

-export([start_link/1]).
-export([get_status/1, cancel/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    job_id :: pos_integer(),
    script :: binary(),
    working_dir :: binary(),
    port :: port() | undefined,
    status :: running | completed | failed | cancelled,
    exit_code :: integer() | undefined,
    output :: binary()
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

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(JobSpec) ->
    JobId = maps:get(job_id, JobSpec),
    Script = maps:get(script, JobSpec, <<>>),
    WorkingDir = maps:get(working_dir, JobSpec, <<"/tmp">>),

    lager:info("Job executor started for job ~p", [JobId]),

    %% Start execution asynchronously
    self() ! execute,

    {ok, #state{
        job_id = JobId,
        script = Script,
        working_dir = WorkingDir,
        status = running,
        output = <<>>
    }}.

handle_call(get_status, _From, State) ->
    Status = #{
        job_id => State#state.job_id,
        status => State#state.status,
        exit_code => State#state.exit_code
    },
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(cancel, #state{port = Port} = State) when Port =/= undefined ->
    lager:info("Cancelling job ~p", [State#state.job_id]),
    port_close(Port),
    {stop, normal, State#state{status = cancelled}};

handle_cast(cancel, State) ->
    {stop, normal, State#state{status = cancelled}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(execute, #state{script = Script, working_dir = WorkingDir} = State) ->
    %% Create a temporary script file
    ScriptPath = create_script_file(State#state.job_id, Script),

    %% Execute the script using a port
    PortOpts = [
        {cd, binary_to_list(WorkingDir)},
        exit_status,
        use_stdio,
        binary,
        stderr_to_stdout
    ],

    try
        Port = open_port({spawn_executable, "/bin/bash"}, [{args, [ScriptPath]} | PortOpts]),
        lager:info("Job ~p started execution", [State#state.job_id]),
        {noreply, State#state{port = Port}}
    catch
        _:Error ->
            lager:error("Failed to execute job ~p: ~p", [State#state.job_id, Error]),
            {stop, normal, State#state{status = failed, exit_code = -1}}
    end;

handle_info({Port, {data, Data}}, #state{port = Port, output = Output} = State) ->
    %% Accumulate output
    {noreply, State#state{output = <<Output/binary, Data/binary>>}};

handle_info({Port, {exit_status, ExitCode}}, #state{port = Port, job_id = JobId} = State) ->
    Status = case ExitCode of
        0 -> completed;
        _ -> failed
    end,
    lager:info("Job ~p finished with status ~p (exit code: ~p)", [JobId, Status, ExitCode]),

    %% Report completion to controller
    report_job_completion(JobId, Status, ExitCode),

    {stop, normal, State#state{status = Status, exit_code = ExitCode}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port}) when Port =/= undefined ->
    catch port_close(Port),
    ok;
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

create_script_file(JobId, Script) ->
    Filename = io_lib:format("/tmp/flurm_job_~p.sh", [JobId]),
    ok = file:write_file(Filename, Script),
    ok = file:change_mode(Filename, 8#755),
    Filename.

report_job_completion(JobId, Status, ExitCode) ->
    %% TODO: Send completion message to controller via flurm_controller_connector
    lager:info("Job ~p completion: ~p (exit code: ~p)", [JobId, Status, ExitCode]).
