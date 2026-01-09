%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor Supervisor
%%%
%%% Supervises job execution processes. Uses a simple_one_for_one
%%% strategy to dynamically spawn job executors.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_job/1, stop_job/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new job executor
-spec start_job(map()) -> {ok, pid()} | {error, term()}.
start_job(JobSpec) ->
    supervisor:start_child(?SERVER, [JobSpec]).

%% @doc Stop a job executor
-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    supervisor:terminate_child(?SERVER, Pid).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    Children = [
        #{
            id => flurm_job_executor,
            start => {flurm_job_executor, start_link, []},
            restart => temporary,
            shutdown => 30000,
            type => worker,
            modules => [flurm_job_executor]
        }
    ],

    {ok, {SupFlags, Children}}.
