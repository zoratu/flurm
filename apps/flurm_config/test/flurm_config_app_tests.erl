%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_app and flurm_config_sup modules
%%% Tests application start/stop and supervisor initialization
%%%-------------------------------------------------------------------
-module(flurm_config_app_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any running config server
    case whereis(flurm_config_server) of
        undefined -> ok;
        Pid -> flurm_test_utils:kill_and_wait(Pid)
    end,
    ok.

cleanup(_) ->
    %% Make sure everything is stopped
    case whereis(flurm_config_server) of
        undefined -> ok;
        Pid1 -> flurm_test_utils:kill_and_wait(Pid1)
    end,
    case whereis(flurm_config_sup) of
        undefined -> ok;
        Pid2 -> flurm_test_utils:kill_and_wait(Pid2)
    end,
    ok.

%%====================================================================
%% Application Tests
%%====================================================================

app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Application start returns supervisor pid", fun test_app_start/0},
      {"Application stop returns ok", fun test_app_stop/0}
     ]}.

test_app_start() ->
    %% Start the application module
    Result = flurm_config_app:start(normal, []),
    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Cleanup
    flurm_test_utils:kill_and_wait(Pid).

test_app_stop() ->
    %% Start first
    {ok, Pid} = flurm_config_app:start(normal, []),
    %% Stop should return ok
    Result = flurm_config_app:stop(some_state),
    ?assertEqual(ok, Result),
    %% Cleanup
    flurm_test_utils:kill_and_wait(Pid).

%%====================================================================
%% Supervisor Tests
%%====================================================================

sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Supervisor start_link returns pid", fun test_sup_start_link/0},
      {"Supervisor init returns correct spec", fun test_sup_init/0},
      {"Supervisor starts config_server child", fun test_sup_starts_server/0}
     ]}.

test_sup_start_link() ->
    %% Start the supervisor
    Result = flurm_config_sup:start_link(),
    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Cleanup
    flurm_test_utils:kill_and_wait(Pid).

test_sup_init() ->
    %% Call init directly to check the specification
    {ok, {SupFlags, Children}} = flurm_config_sup:init([]),

    %% Check supervisor flags
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)),

    %% Check children specification
    ?assertEqual(1, length(Children)),
    [Child] = Children,
    ?assertEqual(flurm_config_server, maps:get(id, Child)),
    ?assertEqual({flurm_config_server, start_link, []}, maps:get(start, Child)),
    ?assertEqual(permanent, maps:get(restart, Child)),
    ?assertEqual(5000, maps:get(shutdown, Child)),
    ?assertEqual(worker, maps:get(type, Child)),
    ?assertEqual([flurm_config_server], maps:get(modules, Child)).

test_sup_starts_server() ->
    %% Start the supervisor
    {ok, SupPid} = flurm_config_sup:start_link(),

    %% Config server should be registered - wait for it
    ServerPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(ServerPid)),
    ?assert(is_process_alive(ServerPid)),

    %% Cleanup
    flurm_test_utils:kill_and_wait(SupPid).
