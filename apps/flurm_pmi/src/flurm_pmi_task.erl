%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Task Integration
%%%
%%% Provides PMI support for task launches. When a job step requires
%%% MPI support:
%%%
%%% 1. Starts a PMI listener for the job step
%%% 2. Adds PMI environment variables to the task environment
%%% 3. MPI processes connect and exchange addresses via PMI
%%%
%%% Environment variables set:
%%%   PMI_FD           - Not used (we use socket path instead)
%%%   PMI_RANK         - This process's rank (0-indexed)
%%%   PMI_SIZE         - Total number of processes
%%%   PMI_JOBID        - Job identifier
%%%   PMI_SPAWNED      - "0" (not spawned)
%%%   PMI_APPNUM       - Application number (0)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_task).

-export([
    setup_pmi/4,
    cleanup_pmi/2,
    get_pmi_env/4
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Set up PMI for a job step. Starts the PMI listener if not already running.
%% Returns {ok, ListenerPid, SocketPath} on success.
-spec setup_pmi(pos_integer(), non_neg_integer(), pos_integer(), binary()) ->
    {ok, pid(), string()} | {error, term()}.
setup_pmi(JobId, StepId, Size, _NodeName) ->
    %% Check if PMI listener already running for this step
    ListenerName = list_to_atom(lists:flatten(io_lib:format("flurm_pmi_listener_~p_~p", [JobId, StepId]))),
    case whereis(ListenerName) of
        undefined ->
            %% Start new PMI listener
            case flurm_pmi_listener:start_link(JobId, StepId, Size) of
                {ok, Pid} ->
                    SocketPath = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
                    lager:info("PMI listener started for job ~p step ~p at ~s",
                              [JobId, StepId, SocketPath]),
                    {ok, Pid, SocketPath};
                {error, Reason} ->
                    lager:error("Failed to start PMI listener: ~p", [Reason]),
                    {error, Reason}
            end;
        Pid ->
            %% Listener already running
            SocketPath = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
            {ok, Pid, SocketPath}
    end.

%% @doc Clean up PMI for a job step.
-spec cleanup_pmi(pos_integer(), non_neg_integer()) -> ok.
cleanup_pmi(JobId, StepId) ->
    flurm_pmi_listener:stop(JobId, StepId),
    flurm_pmi_manager:finalize_job(JobId, StepId),
    ok.

%% @doc Get PMI environment variables for a task.
%% These are added to the task's environment before execution.
-spec get_pmi_env(pos_integer(), non_neg_integer(), non_neg_integer(), pos_integer()) ->
    [{binary(), binary()}].
get_pmi_env(JobId, StepId, Rank, Size) ->
    SocketPath = lists:flatten(flurm_pmi_listener:get_socket_path(JobId, StepId)),
    [
        %% PMI-1 standard variables
        {<<"PMI_RANK">>, integer_to_binary(Rank)},
        {<<"PMI_SIZE">>, integer_to_binary(Size)},
        {<<"PMI_JOBID">>, iolist_to_binary([integer_to_binary(JobId), <<"_">>,
                                            integer_to_binary(StepId)])},
        {<<"PMI_SPAWNED">>, <<"0">>},
        {<<"PMI_APPNUM">>, <<"0">>},

        %% Socket path for PMI connection
        %% MPI implementations check for this to connect to PMI server
        {<<"PMI_SOCK_PATH">>, list_to_binary(SocketPath)},

        %% SLURM compatibility
        {<<"SLURM_PMI_FD">>, <<"undefined">>},  % Not using pre-connected FD
        {<<"SLURM_STEP_NODELIST">>, <<"localhost">>},

        %% Additional MPI environment hints
        {<<"MPICH_INTERFACE_HOSTNAME">>, get_hostname()},
        {<<"OMPI_MCA_btl">>, <<"tcp,self">>}  % OpenMPI hint
    ].

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec get_hostname() -> binary().
get_hostname() ->
    case inet:gethostname() of
        {ok, Name} -> list_to_binary(Name);
        _ -> <<"localhost">>
    end.
