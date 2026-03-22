%%%-------------------------------------------------------------------
%%% @doc FLURM Plugin System
%%%
%%% Provides hooks for extending FLURM's job lifecycle. Plugins are
%%% Erlang modules implementing the flurm_plugin behaviour.
%%%
%%% Plugin hooks:
%%%   job_submit    - Called when a job is submitted (can modify or reject)
%%%   job_prolog    - Called before a job starts on a node
%%%   job_epilog    - Called after a job completes on a node
%%%   job_launch    - Called to customize how a job is launched
%%%   cluster_alloc - Called to set up multi-node jobs (distribute files, etc.)
%%%
%%% Plugins are registered via application env:
%%%   {flurm_core, [{plugins, [my_plugin_module]}]}
%%%
%%% Or at runtime:
%%%   flurm_plugin:register(my_plugin_module)
%%%
%%% Example plugin for distributed model checking:
%%%   -module(my_checker_plugin).
%%%   -behaviour(flurm_plugin).
%%%   -export([job_submit/1, cluster_alloc/2, job_launch/2, job_epilog/2]).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_plugin).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    register/1,
    unregister/1,
    list/0,
    %% Hook invocation
    on_job_submit/1,
    on_job_prolog/2,
    on_job_epilog/2,
    on_job_launch/2,
    on_cluster_alloc/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% Behaviour definition
%%%===================================================================

%% Called when a job is submitted. Can modify the job spec or reject it.
%% Return {ok, ModifiedJobSpec} to accept, {error, Reason} to reject.
-callback job_submit(JobSpec :: map()) ->
    {ok, map()} | {error, term()}.

%% Called before a job starts executing on a node.
%% Can set up the execution environment.
-callback job_prolog(JobId :: integer(), JobInfo :: map()) ->
    ok | {error, term()}.

%% Called after a job finishes on a node.
%% Can clean up, collect results, etc.
-callback job_epilog(JobId :: integer(), JobResult :: map()) ->
    ok.

%% Called to customize how a job is launched on a node.
%% Return {ok, Command, Args, Env} to override the default launch.
-callback job_launch(JobId :: integer(), JobInfo :: map()) ->
    default | {ok, Command :: string(), Args :: [string()], Env :: [{string(), string()}]}.

%% Called after nodes are allocated for a multi-node job.
%% Can distribute files, set up peer lists, etc.
%% Nodes is a list of #{hostname, ip, cpus, ...} maps.
-callback cluster_alloc(JobId :: integer(), Nodes :: [map()]) ->
    {ok, ModifiedNodes :: [map()]} | {error, term()}.

%% All callbacks are optional
-optional_callbacks([job_submit/1, job_prolog/2, job_epilog/2,
                     job_launch/2, cluster_alloc/2]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register(module()) -> ok.
register(Module) ->
    gen_server:call(?SERVER, {register, Module}).

-spec unregister(module()) -> ok.
unregister(Module) ->
    gen_server:call(?SERVER, {unregister, Module}).

-spec list() -> [module()].
list() ->
    gen_server:call(?SERVER, list).

%% Hook invocations - run all registered plugins in order

-spec on_job_submit(map()) -> {ok, map()} | {error, term()}.
on_job_submit(JobSpec) ->
    run_hook(job_submit, [JobSpec], {ok, JobSpec}).

-spec on_job_prolog(integer(), map()) -> ok | {error, term()}.
on_job_prolog(JobId, JobInfo) ->
    run_hook(job_prolog, [JobId, JobInfo], ok).

-spec on_job_epilog(integer(), map()) -> ok.
on_job_epilog(JobId, JobResult) ->
    run_hook(job_epilog, [JobId, JobResult], ok),
    ok.

-spec on_job_launch(integer(), map()) -> default | {ok, string(), [string()], [{string(), string()}]}.
on_job_launch(JobId, JobInfo) ->
    run_hook(job_launch, [JobId, JobInfo], default).

-spec on_cluster_alloc(integer(), [map()]) -> {ok, [map()]} | {error, term()}.
on_cluster_alloc(JobId, Nodes) ->
    run_hook(cluster_alloc, [JobId, Nodes], {ok, Nodes}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% Load plugins from config
    Configured = application:get_env(flurm_core, plugins, []),
    Plugins = lists:foldl(fun(Mod, Acc) ->
        case code:ensure_loaded(Mod) of
            {module, Mod} ->
                lager:info("[plugin] Loaded: ~p", [Mod]),
                [Mod | Acc];
            {error, Reason} ->
                lager:warning("[plugin] Failed to load ~p: ~p", [Mod, Reason]),
                Acc
        end
    end, [], Configured),
    lager:info("[plugin] ~p plugins registered", [length(Plugins)]),
    {ok, #{plugins => lists:reverse(Plugins)}}.

handle_call({register, Module}, _From, #{plugins := Plugins} = State) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            case lists:member(Module, Plugins) of
                true ->
                    {reply, ok, State};
                false ->
                    lager:info("[plugin] Registered: ~p", [Module]),
                    {reply, ok, State#{plugins => Plugins ++ [Module]}}
            end;
        {error, Reason} ->
            {reply, {error, {load_failed, Reason}}, State}
    end;

handle_call({unregister, Module}, _From, #{plugins := Plugins} = State) ->
    NewPlugins = lists:delete(Module, Plugins),
    lager:info("[plugin] Unregistered: ~p", [Module]),
    {reply, ok, State#{plugins => NewPlugins}};

handle_call(list, _From, #{plugins := Plugins} = State) ->
    {reply, Plugins, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal - Hook runner
%%%===================================================================

run_hook(Hook, Args, Default) ->
    case catch gen_server:call(?SERVER, list, 1000) of
        Plugins when is_list(Plugins) ->
            run_plugins(Hook, Args, Default, Plugins);
        _ ->
            Default
    end.

run_plugins(_Hook, _Args, Result, []) ->
    Result;
run_plugins(Hook, Args, Current, [Plugin | Rest]) ->
    case erlang:function_exported(Plugin, Hook, length(Args)) of
        true ->
            try apply(Plugin, Hook, Args) of
                {ok, NewValue} when Hook =:= job_submit ->
                    %% Chain: pass modified spec to next plugin
                    run_plugins(Hook, [NewValue], {ok, NewValue}, Rest);
                {ok, NewNodes} when Hook =:= cluster_alloc ->
                    run_plugins(Hook, [hd(Args), NewNodes], {ok, NewNodes}, Rest);
                {error, _} = Err ->
                    Err;  % Stop chain on error
                ok ->
                    run_plugins(Hook, Args, ok, Rest);
                default ->
                    run_plugins(Hook, Args, Current, Rest);
                Other ->
                    run_plugins(Hook, Args, Other, Rest)
            catch
                Class:Reason:Stack ->
                    lager:error("[plugin] ~p:~p crashed: ~p:~p~n~p",
                               [Plugin, Hook, Class, Reason, Stack]),
                    run_plugins(Hook, Args, Current, Rest)
            end;
        false ->
            %% Plugin doesn't implement this hook, skip
            run_plugins(Hook, Args, Current, Rest)
    end.
