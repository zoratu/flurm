%%%-------------------------------------------------------------------
%%% @doc FLURM Job Supervisor
%%%
%%% A simple_one_for_one supervisor for dynamically spawning job
%%% processes. Each job runs as a separate gen_statem process under
%%% this supervisor.
%%%
%%% The supervisor uses a temporary restart strategy - if a job
%%% process crashes, it is not automatically restarted. This follows
%%% the let-it-crash philosophy for jobs: a crashed job should be
%%% resubmitted explicitly rather than silently restarted.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_sup).
-behaviour(supervisor).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    start_job/1,
    stop_job/1,
    which_jobs/0,
    count_jobs/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the job supervisor.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new job process with the given job specification.
-spec start_job(#job_spec{}) -> {ok, pid()} | {error, term()}.
start_job(#job_spec{} = JobSpec) ->
    supervisor:start_child(?SERVER, [JobSpec]).

%% @doc Stop a job process.
-spec stop_job(pid()) -> ok | {error, term()}.
stop_job(Pid) when is_pid(Pid) ->
    supervisor:terminate_child(?SERVER, Pid).

%% @doc Get a list of all running job processes.
-spec which_jobs() -> [pid()].
which_jobs() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?SERVER),
            is_pid(Pid)].

%% @doc Count the number of running job processes.
-spec count_jobs() -> non_neg_integer().
count_jobs() ->
    length(which_jobs()).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,    %% Don't restart jobs automatically
        period => 1
    },
    ChildSpec = #{
        id => flurm_job,
        start => {flurm_job, start_link, []},
        restart => temporary,   %% Jobs are not restarted if they crash
        shutdown => 5000,       %% Give jobs 5 seconds to clean up
        type => worker,
        modules => [flurm_job]
    },
    {ok, {SupFlags, [ChildSpec]}}.
