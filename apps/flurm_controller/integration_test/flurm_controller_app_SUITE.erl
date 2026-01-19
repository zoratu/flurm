-module(flurm_controller_app_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([app_starts_test/1, sup_starts_children_test/1, app_stops_cleanly_test/1]).

all() -> [{group, startup}].

groups() ->
    [{startup, [sequence], [app_starts_test, sup_starts_children_test, app_stops_cleanly_test]}].

init_per_suite(Config) ->
    %% Ensure apps are stopped before we start
    application:stop(flurm_controller),
    application:stop(flurm_db),
    application:stop(flurm_core),
    application:stop(flurm_config),
    %% Clean up any leftover data
    os:cmd("rm -rf /tmp/flurm_test_data"),
    Config.

end_per_suite(_Config) ->
    application:stop(flurm_controller),
    application:stop(flurm_db),
    application:stop(flurm_core),
    application:stop(flurm_config),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

app_starts_test(_Config) ->
    %% Start dependencies
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(flurm_config),
    {ok, _} = application:ensure_all_started(flurm_core),
    {ok, _} = application:ensure_all_started(flurm_db),
    %% Start controller
    {ok, _} = application:ensure_all_started(flurm_controller),
    %% Verify it's running
    Apps = application:which_applications(),
    ?assert(lists:keymember(flurm_controller, 1, Apps)).

sup_starts_children_test(_Config) ->
    %% Verify supervisor is alive
    Pid = whereis(flurm_controller_sup),
    ?assertNotEqual(undefined, Pid),
    ?assert(is_process_alive(Pid)),
    %% Verify it has children
    Children = supervisor:which_children(flurm_controller_sup),
    ?assert(length(Children) > 0).

app_stops_cleanly_test(_Config) ->
    %% Stop the application
    ok = application:stop(flurm_controller),
    %% Verify supervisor is gone
    ?assertEqual(undefined, whereis(flurm_controller_sup)),
    %% Restart for other tests
    {ok, _} = application:ensure_all_started(flurm_controller).
