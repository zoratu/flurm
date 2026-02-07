%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Top-Level Supervisor
%%%
%%% This supervisor manages all the child processes of the FLURM
%%% node daemon, including the controller connector, job executor,
%%% system monitor, and the srun connection listener.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SRUN_LISTENER_NAME, flurm_srun_listener).
-define(DEFAULT_SRUN_PORT, 6818).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    %% Start Ranch listener for srun connections on port 6818
    Port = application:get_env(flurm_node_daemon, srun_port, ?DEFAULT_SRUN_PORT),
    start_srun_listener(Port),

    Children = [
        %% System Monitor - collects node metrics
        #{
            id => flurm_system_monitor,
            start => {flurm_system_monitor, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_system_monitor]
        },
        %% Controller Connector - maintains connection to controller
        #{
            id => flurm_controller_connector,
            start => {flurm_controller_connector, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_controller_connector]
        },
        %% Job Executor Supervisor - supervises job execution
        #{
            id => flurm_job_executor_sup,
            start => {flurm_job_executor_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [flurm_job_executor_sup]
        }
    ],

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Start the Ranch listener for srun connections.
%% This allows srun to connect directly to the node daemon to launch tasks.
%% Gracefully handles the case when Ranch isn't available (e.g., during tests).
-spec start_srun_listener(non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_srun_listener(Port) ->
    lager:info("Starting srun listener on port ~p", [Port]),
    %% Check if ranch_sup is running before trying to start listener
    case whereis(ranch_sup) of
        undefined ->
            lager:warning("Ranch supervisor not running, skipping srun listener"),
            {error, ranch_not_running};
        _ ->
            TransportOpts = #{
                socket_opts => [
                    {port, Port},
                    {reuseaddr, true}
                ],
                num_acceptors => 10
            },
            ProtocolOpts = #{},
            try ranch:start_listener(
                ?SRUN_LISTENER_NAME,
                ranch_tcp,
                TransportOpts,
                flurm_srun_acceptor,
                ProtocolOpts
            ) of
                {ok, Pid} ->
                    lager:info("srun listener started on port ~p (pid=~p)", [Port, Pid]),
                    {ok, Pid};
                {error, {already_started, Pid}} ->
                    lager:info("srun listener already running on port ~p (pid=~p)", [Port, Pid]),
                    {ok, Pid};
                {error, Reason} ->
                    lager:error("Failed to start srun listener on port ~p: ~p", [Port, Reason]),
                    {error, Reason}
            catch
                exit:{noproc, _} ->
                    lager:warning("Ranch not available, skipping srun listener"),
                    {error, ranch_not_running};
                Class:Error ->
                    lager:error("Unexpected error starting srun listener: ~p:~p", [Class, Error]),
                    {error, {Class, Error}}
            end
    end.
