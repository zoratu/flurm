%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Application
%%%
%%% This module implements the OTP application behaviour for the
%%% FLURM node daemon (flurmnd). It runs on each compute node and
%%% is responsible for executing jobs and reporting status to the
%%% controller.
%%%
%%% Features:
%%% - State persistence across restarts
%%% - Orphaned job detection and recovery
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app).

-behaviour(application).

-export([start/2, stop/1, prep_stop/1]).

-ifdef(TEST).
-export([
    save_current_state/0,
    handle_restored_state/1,
    cleanup_orphaned_resources/0,
    cleanup_orphaned_cgroups/0,
    cleanup_cgroup/1,
    cleanup_orphaned_scripts/0
]).
-endif.

%%====================================================================
%% Application callbacks
%%====================================================================

start(_StartType, _StartArgs) ->
    lager:info("Starting FLURM Node Daemon (flurmnd)"),

    %% Get configuration
    {ok, ControllerHost} = application:get_env(flurm_node_daemon, controller_host),
    {ok, ControllerPort} = application:get_env(flurm_node_daemon, controller_port),

    lager:info("Node daemon will connect to controller at ~s:~p",
               [ControllerHost, ControllerPort]),

    %% Check for previous state (orphaned jobs)
    case flurm_state_persistence:load_state() of
        {ok, SavedState} ->
            handle_restored_state(SavedState);
        {error, _} ->
            ok
    end,

    %% Start the supervisor
    flurm_node_daemon_sup:start_link().

%% @doc Called before stop to save state
prep_stop(State) ->
    lager:info("Preparing to stop FLURM Node Daemon, saving state..."),
    save_current_state(),
    State.

stop(_State) ->
    lager:info("Stopping FLURM Node Daemon"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Save current daemon state to disk
save_current_state() ->
    try
        %% Get current running jobs
        ConnState = flurm_controller_connector:get_state(),
        GPUAlloc = flurm_system_monitor:get_gpu_allocation(),

        State = #{
            running_jobs => maps:get(running_jobs, ConnState, 0),
            gpu_allocation => GPUAlloc,
            draining => maps:get(draining, ConnState, false),
            drain_reason => maps:get(drain_reason, ConnState, undefined)
        },

        case flurm_state_persistence:save_state(State) of
            ok ->
                lager:info("Node state saved successfully");
            {error, Reason} ->
                lager:error("Failed to save node state: ~p", [Reason])
        end
    catch
        Error:Reason2 ->
            lager:error("Error saving node state: ~p:~p", [Error, Reason2])
    end.

%% @doc Handle restored state from previous run
handle_restored_state(SavedState) ->
    SavedAt = maps:get(saved_at, SavedState, 0),
    Now = erlang:system_time(millisecond),
    Age = Now - SavedAt,

    lager:info("Found saved state from ~p ms ago", [Age]),

    %% Check for orphaned jobs
    RunningJobs = maps:get(running_jobs, SavedState, 0),
    case RunningJobs of
        N when is_integer(N), N > 0 ->
            lager:warning("Detected ~p potentially orphaned jobs from previous run", [N]),
            %% In a real implementation, we would:
            %% 1. Check if the job processes still exist (they won't after restart)
            %% 2. Report these jobs as failed to the controller
            %% 3. Clean up any leftover resources (cgroups, temp files)
            cleanup_orphaned_resources();
        _ ->
            ok
    end,

    %% Clear the saved state now that we've processed it
    flurm_state_persistence:clear_state(),
    ok.

%% @doc Clean up resources from orphaned jobs
cleanup_orphaned_resources() ->
    lager:info("Cleaning up orphaned job resources..."),

    %% Clean up orphaned cgroups
    cleanup_orphaned_cgroups(),

    %% Clean up orphaned script files
    cleanup_orphaned_scripts(),

    ok.

%% Clean up any leftover cgroups from previous jobs
cleanup_orphaned_cgroups() ->
    case os:type() of
        {unix, linux} ->
            %% Try cgroups v2 first
            case filelib:is_dir("/sys/fs/cgroup") of
                true ->
                    Entries = case file:list_dir("/sys/fs/cgroup") of
                        {ok, E} -> E;
                        _ -> []
                    end,
                    FlumrCgroups = [Entry || Entry <- Entries,
                                             lists:prefix("flurm_", Entry)],
                    lists:foreach(fun(Cg) ->
                        Path = "/sys/fs/cgroup/" ++ Cg,
                        lager:info("Removing orphaned cgroup: ~s", [Path]),
                        cleanup_cgroup(Path)
                    end, FlumrCgroups);
                false ->
                    ok
            end,
            %% Try cgroups v1
            lists:foreach(fun(Controller) ->
                CtrlPath = "/sys/fs/cgroup/" ++ Controller,
                case filelib:is_dir(CtrlPath) of
                    true ->
                        CtrlEntries = case file:list_dir(CtrlPath) of
                            {ok, CE} -> CE;
                            _ -> []
                        end,
                        CtrlFlumrCgroups = [CEntry || CEntry <- CtrlEntries,
                                             lists:prefix("flurm_", CEntry)],
                        lists:foreach(fun(Cg) ->
                            CgPath = CtrlPath ++ "/" ++ Cg,
                            lager:info("Removing orphaned cgroup: ~s", [CgPath]),
                            cleanup_cgroup(CgPath)
                        end, CtrlFlumrCgroups);
                    false ->
                        ok
                end
            end, ["memory", "cpu"]);
        _ ->
            ok
    end.

cleanup_cgroup(Path) ->
    %% Kill any remaining processes in cgroup
    ProcsFile = Path ++ "/cgroup.procs",
    case file:read_file(ProcsFile) of
        {ok, Pids} ->
            [os:cmd("kill -9 " ++ binary_to_list(P)) ||
             P <- binary:split(Pids, <<"\n">>, [global]),
             P =/= <<>>],
            timer:sleep(100);
        _ ->
            ok
    end,
    %% Remove the cgroup directory
    file:del_dir(Path).

%% Clean up orphaned job script files
cleanup_orphaned_scripts() ->
    case file:list_dir("/tmp") of
        {ok, Files} ->
            JobScripts = [F || F <- Files,
                               lists:prefix("flurm_job_", F),
                               lists:suffix(".sh", F)],
            lists:foreach(fun(Script) ->
                Path = "/tmp/" ++ Script,
                lager:info("Removing orphaned script: ~s", [Path]),
                file:delete(Path)
            end, JobScripts);
        _ ->
            ok
    end.
