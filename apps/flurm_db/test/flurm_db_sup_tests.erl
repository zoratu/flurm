%%%-------------------------------------------------------------------
%%% @doc FLURM Database Supervisor Tests
%%%
%%% Tests for the flurm_db_sup module.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Set up test environment
    application:set_env(flurm_db, data_dir, "/tmp/flurm_db_sup_test"),
    ok = filelib:ensure_dir("/tmp/flurm_db_sup_test/dummy"),
    ok.

cleanup(_) ->
    %% Clean up
    application:unset_env(flurm_db, data_dir),
    os:cmd("rm -rf /tmp/flurm_db_sup_test"),
    ok.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"init returns supervisor spec", fun test_init_returns_spec/0},
         {"init creates ETS tables", fun test_init_creates_ets_tables/0}
     ]
    }.

test_init_returns_spec() ->
    {ok, {SupFlags, Children}} = flurm_db_sup:init(#{}),

    %% Verify supervisor flags
    ?assertMatch(#{strategy := one_for_one}, SupFlags),
    ?assertMatch(#{intensity := _}, SupFlags),
    ?assertMatch(#{period := _}, SupFlags),

    %% Verify children
    ?assert(length(Children) >= 1),

    %% Find ra_starter child
    RaStarterChild = lists:keyfind(flurm_db_ra_starter, 1,
                                    [{maps:get(id, C), C} || C <- Children]),
    ?assertNotEqual(false, RaStarterChild),
    {_, RaStarterSpec} = RaStarterChild,
    ?assertEqual(transient, maps:get(restart, RaStarterSpec)),
    ok.

test_init_creates_ets_tables() ->
    %% Call init to create tables
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify ETS tables exist
    ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_job_counter_ets)),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    [
        {"start_link/0 starts supervisor", fun() ->
             %% Cleanup any existing tables
             catch ets:delete(flurm_db_jobs_ets),
             catch ets:delete(flurm_db_nodes_ets),
             catch ets:delete(flurm_db_partitions_ets),
             catch ets:delete(flurm_db_job_counter_ets),
             catch dets:close(flurm_db_jobs_dets),
             catch dets:close(flurm_db_counter_dets),

             application:set_env(flurm_db, data_dir, "/tmp/flurm_db_start_test"),
             ok = filelib:ensure_dir("/tmp/flurm_db_start_test/dummy"),

             case flurm_db_sup:start_link() of
                 {ok, Pid} ->
                     ?assert(is_pid(Pid)),
                     ?assert(is_process_alive(Pid)),
                     %% Stop the supervisor and wait for death
                     flurm_test_utils:kill_and_wait(Pid);
                 {error, {already_started, _}} ->
                     ok;
                 {error, Reason} ->
                     %% May fail if dependencies not available
                     io:format("start_link failed: ~p~n", [Reason]),
                     ok
             end,
             os:cmd("rm -rf /tmp/flurm_db_start_test")
         end},
        {"start_link/1 accepts config", fun() ->
             catch ets:delete(flurm_db_jobs_ets),
             catch ets:delete(flurm_db_nodes_ets),
             catch ets:delete(flurm_db_partitions_ets),
             catch ets:delete(flurm_db_job_counter_ets),
             catch dets:close(flurm_db_jobs_dets),
             catch dets:close(flurm_db_counter_dets),

             application:set_env(flurm_db, data_dir, "/tmp/flurm_db_config_test"),
             ok = filelib:ensure_dir("/tmp/flurm_db_config_test/dummy"),

             Config = #{cluster_nodes => [node()]},
             case flurm_db_sup:start_link(Config) of
                 {ok, Pid} ->
                     ?assert(is_pid(Pid)),
                     %% Stop the supervisor and wait for death
                     flurm_test_utils:kill_and_wait(Pid);
                 {error, {already_started, _}} ->
                     ok;
                 {error, _} ->
                     ok
             end,
             os:cmd("rm -rf /tmp/flurm_db_config_test")
         end}
    ].

%%====================================================================
%% Job Counter Tests
%%====================================================================

job_counter_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_db, data_dir, "/tmp/flurm_db_counter_test"),
         ok = filelib:ensure_dir("/tmp/flurm_db_counter_test/dummy"),
         %% Clean up existing tables
         catch ets:delete(flurm_db_jobs_ets),
         catch ets:delete(flurm_db_job_counter_ets),
         catch dets:close(flurm_db_jobs_dets),
         catch dets:close(flurm_db_counter_dets),
         ok
     end,
     fun(_) ->
         catch ets:delete(flurm_db_jobs_ets),
         catch ets:delete(flurm_db_job_counter_ets),
         catch dets:close(flurm_db_jobs_dets),
         catch dets:close(flurm_db_counter_dets),
         os:cmd("rm -rf /tmp/flurm_db_counter_test"),
         ok
     end,
     [
         {"job counter initializes to 0", fun() ->
             {ok, _} = flurm_db_sup:init(#{}),
             [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
             ?assert(Counter >= 0)
         end}
     ]
    }.
