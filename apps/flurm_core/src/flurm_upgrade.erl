%%%-------------------------------------------------------------------
%%% @doc FLURM Hot Upgrade Manager
%%%
%%% Manages hot code upgrades for FLURM releases.
%%% Supports both in-place module upgrades and full release upgrades.
%%%
%%% Usage:
%%%   %% Check current versions
%%%   flurm_upgrade:versions() -> [{app, vsn}]
%%%
%%%   %% Upgrade a single module
%%%   flurm_upgrade:reload_module(flurm_scheduler) -> ok | {error, Reason}
%%%
%%%   %% Install a release upgrade
%%%   flurm_upgrade:install_release("0.2.0") -> ok | {error, Reason}
%%%
%%%   %% Verify upgrade safety
%%%   flurm_upgrade:check_upgrade("0.2.0") -> ok | {error, Reasons}
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_upgrade).

-export([
    versions/0,
    reload_module/1,
    reload_modules/1,
    install_release/1,
    check_upgrade/1,
    rollback/0,
    which_releases/0,
    upgrade_status/0
]).

%% Internal exports for code_change support
-export([
    transform_state/3
]).

-define(FLURM_APPS, [
    flurm_config,
    flurm_protocol,
    flurm_core,
    flurm_db,
    flurm_controller,
    flurm_node_daemon
]).

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Get current versions of all FLURM applications
-spec versions() -> [{atom(), string()}].
versions() ->
    lists:filtermap(fun(App) ->
        case application:get_key(App, vsn) of
            {ok, Vsn} -> {true, {App, Vsn}};
            undefined -> false
        end
    end, ?FLURM_APPS).

%% @doc Reload a single module (hot code loading)
-spec reload_module(module()) -> ok | {error, term()}.
reload_module(Module) ->
    lager:info("Hot reloading module: ~p", [Module]),
    case code:soft_purge(Module) of
        true ->
            case code:load_file(Module) of
                {module, Module} ->
                    lager:info("Module ~p reloaded successfully", [Module]),
                    ok;
                {error, Reason} ->
                    lager:error("Failed to load module ~p: ~p", [Module, Reason]),
                    {error, {load_failed, Reason}}
            end;
        false ->
            %% Module has processes with old code - need to handle
            lager:warning("Module ~p has old code in use, attempting upgrade", [Module]),
            case upgrade_module_with_state(Module) of
                ok -> ok;
                Error -> Error
            end
    end.

%% @doc Reload multiple modules in order
-spec reload_modules([module()]) -> ok | {error, term()}.
reload_modules(Modules) ->
    Results = lists:map(fun(M) -> {M, reload_module(M)} end, Modules),
    Failures = [{M, E} || {M, {error, E}} <- Results],
    case Failures of
        [] -> ok;
        _ -> {error, {partial_failure, Failures}}
    end.

%% @doc Install a release upgrade (full OTP release upgrade)
-spec install_release(string()) -> ok | {error, term()}.
install_release(Vsn) ->
    lager:info("Installing release upgrade to version ~s", [Vsn]),

    %% Pre-upgrade checks
    case check_upgrade(Vsn) of
        ok ->
            do_install_release(Vsn);
        {error, Reasons} ->
            lager:error("Pre-upgrade check failed: ~p", [Reasons]),
            {error, {pre_check_failed, Reasons}}
    end.

%% @doc Check if upgrade to given version is safe
-spec check_upgrade(string()) -> ok | {error, [term()]}.
check_upgrade(Vsn) ->
    Checks = [
        {release_exists, fun() -> check_release_exists(Vsn) end},
        {no_pending_jobs, fun() -> check_no_pending_jobs() end},
        {cluster_healthy, fun() -> check_cluster_healthy() end},
        {disk_space, fun() -> check_disk_space() end}
    ],

    Failures = lists:filtermap(fun({Name, Check}) ->
        case catch Check() of
            ok -> false;
            {error, Reason} -> {true, {Name, Reason}};
            {'EXIT', Reason} -> {true, {Name, {crashed, Reason}}}
        end
    end, Checks),

    case Failures of
        [] -> ok;
        _ -> {error, Failures}
    end.

%% @doc Rollback to previous release
-spec rollback() -> ok | {error, term()}.
rollback() ->
    case which_releases() of
        [{_Current, current}, {Previous, old} | _] ->
            lager:info("Rolling back to previous release: ~s", [Previous]),
            case release_handler:install_release(Previous) of
                {ok, _, _} ->
                    lager:info("Rollback successful, making permanent"),
                    release_handler:make_permanent(Previous);
                {error, Reason} ->
                    lager:error("Rollback failed: ~p", [Reason]),
                    {error, Reason}
            end;
        _ ->
            {error, no_previous_release}
    end.

%% @doc List installed releases
-spec which_releases() -> [{string(), atom()}].
which_releases() ->
    Releases = release_handler:which_releases(),
    [{Vsn, Status} || {_Name, Vsn, _Apps, Status} <- Releases].

%% @doc Get detailed upgrade status
-spec upgrade_status() -> map().
upgrade_status() ->
    #{
        versions => versions(),
        releases => which_releases(),
        loaded_modules => get_loaded_modules(),
        old_code_modules => get_old_code_modules()
    }.

%% @doc Helper for state transformation during code_change
%% Called by gen_server/gen_statem code_change callbacks
-spec transform_state(atom(), term(), term()) -> term().
transform_state(Module, OldState, Extra) ->
    %% Default: no transformation needed
    %% Modules can override by implementing transform_state/3
    case erlang:function_exported(Module, transform_state, 3) of
        true ->
            Module:transform_state(OldState, Extra);
        false ->
            OldState
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

upgrade_module_with_state(Module) ->
    %% Find processes using this module
    Processes = find_processes_using_module(Module),
    lager:info("Found ~p processes using module ~p", [length(Processes), Module]),

    case Processes of
        [] ->
            %% No processes - force purge and reload
            code:purge(Module),
            case code:load_file(Module) of
                {module, Module} -> ok;
                {error, Reason} -> {error, {load_failed, Reason}}
            end;
        _ ->
            %% Trigger sys:suspend -> code_change -> sys:resume for each process
            Results = lists:map(fun(Pid) ->
                upgrade_process(Pid, Module)
            end, Processes),

            case lists:all(fun(R) -> R == ok end, Results) of
                true ->
                    %% Now safe to purge
                    code:purge(Module),
                    ok;
                false ->
                    {error, {process_upgrade_failed, Results}}
            end
    end.

find_processes_using_module(Module) ->
    [Pid || Pid <- processes(),
            lists:member(Module, get_process_modules(Pid))].

get_process_modules(Pid) ->
    case process_info(Pid, [current_function, initial_call, dictionary]) of
        undefined -> [];
        Info ->
            CF = case proplists:get_value(current_function, Info) of
                {M, _, _} -> [M];
                _ -> []
            end,
            IC = case proplists:get_value(initial_call, Info) of
                {M2, _, _} -> [M2];
                _ -> []
            end,
            %% Check $initial_call in process dict for gen_*
            Dict = proplists:get_value(dictionary, Info, []),
            GI = case proplists:get_value('$initial_call', Dict) of
                {M3, _, _} -> [M3];
                _ -> []
            end,
            lists:usort(CF ++ IC ++ GI)
    end.

upgrade_process(Pid, Module) ->
    try
        %% Use sys module for gen_server/gen_statem
        sys:suspend(Pid, 5000),
        try
            %% Get current state (status info not used, just verifying process is responding)
            {status, _, _, _Info} = sys:get_status(Pid),
            OldVsn = get_module_version(Module),

            %% Load new code
            code:load_file(Module),

            %% Trigger code_change
            sys:change_code(Pid, Module, OldVsn, []),
            ok
        after
            sys:resume(Pid, 5000)
        end
    catch
        _:Reason ->
            lager:warning("Failed to upgrade process ~p: ~p", [Pid, Reason]),
            {error, Reason}
    end.

get_module_version(Module) ->
    case code:get_object_code(Module) of
        {Module, Bin, _} ->
            case beam_lib:version(Bin) of
                {ok, {Module, [Vsn | _]}} -> Vsn;
                _ -> undefined
            end;
        error ->
            undefined
    end.

do_install_release(Vsn) ->
    %% Unpack the release if needed
    case release_handler:unpack_release(Vsn) of
        {ok, _} ->
            lager:info("Release ~s unpacked", [Vsn]);
        {error, {already_installed, _}} ->
            lager:info("Release ~s already unpacked", [Vsn]);
        {error, UnpackErr} ->
            lager:error("Failed to unpack release ~s: ~p", [Vsn, UnpackErr]),
            throw({error, {unpack_failed, UnpackErr}})
    end,

    %% Install the release
    case release_handler:install_release(Vsn) of
        {ok, OldVsn, _} ->
            lager:info("Release ~s installed (was ~s)", [Vsn, OldVsn]),

            %% Make permanent after successful install
            case release_handler:make_permanent(Vsn) of
                ok ->
                    lager:info("Release ~s is now permanent", [Vsn]),
                    ok;
                {error, PermErr} ->
                    lager:error("Failed to make release permanent: ~p", [PermErr]),
                    {error, {make_permanent_failed, PermErr}}
            end;
        {error, InstallErr} ->
            lager:error("Failed to install release ~s: ~p", [Vsn, InstallErr]),
            {error, {install_failed, InstallErr}}
    end.

check_release_exists(Vsn) ->
    RelDir = code:lib_dir() ++ "/../releases/" ++ Vsn,
    case filelib:is_dir(RelDir) of
        true -> ok;
        false ->
            %% Check for tar file
            TarFile = code:lib_dir() ++ "/../releases/" ++ Vsn ++ ".tar.gz",
            case filelib:is_file(TarFile) of
                true -> ok;
                false -> {error, release_not_found}
            end
    end.

check_no_pending_jobs() ->
    %% In production, would check for jobs in critical states
    %% For now, just return ok
    ok.

check_cluster_healthy() ->
    %% Check Ra cluster health if enabled
    case erlang:whereis(flurm_controller_cluster) of
        undefined -> ok;  % Not using cluster mode
        _ ->
            case catch flurm_controller_cluster:is_healthy() of
                true -> ok;
                false -> {error, cluster_unhealthy};
                {'EXIT', _} -> ok  % Cluster module not running
            end
    end.

check_disk_space() ->
    %% Basic disk space check
    case file:get_cwd() of
        {ok, Dir} ->
            case disk_free(Dir) of
                {ok, Free} when Free > 100 * 1024 * 1024 -> ok;  % Need 100MB
                {ok, _} -> {error, insufficient_disk_space};
                {error, _} -> ok  % Can't check, assume ok
            end;
        _ -> ok
    end.

disk_free(Path) ->
    case os:type() of
        {unix, _} ->
            Cmd = "df -k " ++ Path ++ " | tail -1 | awk '{print $4}'",
            case os:cmd(Cmd) of
                [] -> {error, no_output};
                Output ->
                    case string:to_integer(string:trim(Output)) of
                        {Int, _} when is_integer(Int) -> {ok, Int * 1024};
                        _ -> {error, parse_error}
                    end
            end;
        _ ->
            {error, unsupported_os}
    end.

get_loaded_modules() ->
    FlurMods = [M || {M, _} <- code:all_loaded(),
                     is_flurm_module(M)],
    lists:sort(FlurMods).

get_old_code_modules() ->
    [M || M <- get_loaded_modules(),
          code:is_loaded(M) =/= false,
          erlang:check_old_code(M)].

is_flurm_module(M) ->
    Prefix = atom_to_list(M),
    lists:prefix("flurm_", Prefix).
