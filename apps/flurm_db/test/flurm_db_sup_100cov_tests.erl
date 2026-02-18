%%%-------------------------------------------------------------------
%%% @doc FLURM DB Supervisor 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_db_sup module covering:
%%% - Supervisor startup
%%% - Child specification
%%% - ETS table initialization
%%% - DETS persistence
%%% - Data directory handling
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(filelib),
    catch meck:unload(file),
    catch meck:unload(dets),
    catch meck:unload(lager),
    catch meck:unload(application),
    %% Clean up any existing tables
    catch ets:delete(flurm_db_jobs_ets),
    catch ets:delete(flurm_db_nodes_ets),
    catch ets:delete(flurm_db_partitions_ets),
    catch ets:delete(flurm_db_job_counter_ets),
    catch dets:close(flurm_db_jobs_dets),
    catch dets:close(flurm_db_counter_dets),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    catch ets:delete(flurm_db_jobs_ets),
    catch ets:delete(flurm_db_nodes_ets),
    catch ets:delete(flurm_db_partitions_ets),
    catch ets:delete(flurm_db_job_counter_ets),
    catch dets:close(flurm_db_jobs_dets),
    catch dets:close(flurm_db_counter_dets),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"start_link/0 calls start_link/1 with empty map",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> undefined end),
             meck:expect(filelib, is_dir, fun("/var/lib/flurm") -> false end),
             meck:expect(file, get_cwd, fun() -> {ok, "/tmp"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, Pid} = flurm_db_sup:start_link(),
             ?assert(is_pid(Pid)),
             %% Stop the supervisor
             exit(Pid, shutdown),
             timer:sleep(50)
         end},
        {"start_link/1 with custom config",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> undefined end),
             meck:expect(filelib, is_dir, fun("/var/lib/flurm") -> false end),
             meck:expect(file, get_cwd, fun() -> {ok, "/tmp"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Config = #{cluster_nodes => [node()]},
             {ok, Pid} = flurm_db_sup:start_link(Config),
             ?assert(is_pid(Pid)),
             exit(Pid, shutdown),
             timer:sleep(50)
         end}
     ]}.

%%====================================================================
%% Get Data Dir Tests
%%====================================================================

get_data_dir_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_data_dir from application env",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/custom/data"} end),

             Result = flurm_db_sup:get_data_dir(),
             ?assertEqual("/custom/data", Result)
         end},
        {"get_data_dir fallback to /var/lib/flurm",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> undefined end),
             meck:expect(filelib, is_dir, fun("/var/lib/flurm") -> true end),

             Result = flurm_db_sup:get_data_dir(),
             ?assertEqual("/var/lib/flurm", Result)
         end},
        {"get_data_dir fallback to cwd/data",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> undefined end),
             meck:expect(filelib, is_dir, fun("/var/lib/flurm") -> false end),
             meck:expect(file, get_cwd, fun() -> {ok, "/home/user"} end),

             Result = flurm_db_sup:get_data_dir(),
             ?assertEqual("/home/user/data", Result)
         end}
     ]}.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init creates ETS tables",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, {{Strategy, Intensity, Period}, Children}} = flurm_db_sup:init(#{}),

             ?assertEqual(one_for_one, Strategy),
             ?assertEqual(5, Intensity),
             ?assertEqual(10, Period),
             ?assertEqual(1, length(Children)),

             %% Verify ETS tables were created
             ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs_ets)),
             ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes_ets)),
             ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions_ets)),
             ?assertNotEqual(undefined, ets:whereis(flurm_db_job_counter_ets))
         end},
        {"init with existing ETS tables",
         fun() ->
             %% Create tables first
             ets:new(flurm_db_jobs_ets, [named_table, public, set]),
             ets:new(flurm_db_nodes_ets, [named_table, public, set]),
             ets:new(flurm_db_partitions_ets, [named_table, public, set]),
             ets:new(flurm_db_job_counter_ets, [named_table, public, set]),

             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             %% Should not crash with existing tables
             {ok, _} = flurm_db_sup:init(#{})
         end},
        {"init opens DETS tables",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(flurm_db_jobs_dets, _) -> {ok, flurm_db_jobs_dets};
                                             (flurm_db_counter_dets, _) -> {ok, flurm_db_counter_dets} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, _} = flurm_db_sup:init(#{}),

             ?assert(meck:called(dets, open_file, [flurm_db_jobs_dets, '_'])),
             ?assert(meck:called(dets, open_file, [flurm_db_counter_dets, '_']))
         end},
        {"init handles DETS open failure for jobs",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(flurm_db_jobs_dets, _) -> {error, enoent};
                                             (flurm_db_counter_dets, _) -> {ok, flurm_db_counter_dets} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             %% Should not crash
             {ok, _} = flurm_db_sup:init(#{})
         end},
        {"init handles DETS open failure for counter",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(flurm_db_jobs_dets, _) -> {ok, flurm_db_jobs_dets};
                                             (flurm_db_counter_dets, _) -> {error, enoent} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             %% Should not crash
             {ok, _} = flurm_db_sup:init(#{})
         end},
        {"init loads jobs from DETS",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             TestJob = {1, {job, 1, <<"test">>, <<"user">>, undefined, pending, undefined, 1, 1, 1, 1, 1024, 3600, 100, undefined, undefined, undefined, [], undefined, 0, undefined, <<>>, <<"normal">>}},

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(flurm_db_jobs_dets) -> [{size, 1}];
                                        (_) -> undefined end),
             meck:expect(dets, select, fun(flurm_db_jobs_dets, _) -> [TestJob] end),
             meck:expect(dets, lookup, fun(_, counter) -> [{counter, 5}] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, _} = flurm_db_sup:init(#{}),

             %% Verify job was loaded into ETS
             ?assertEqual([TestJob], ets:lookup(flurm_db_jobs_ets, 1))
         end},
        {"init restores counter from DETS",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(flurm_db_counter_dets, counter) -> [{counter, 42}] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, _} = flurm_db_sup:init(#{}),

             %% Verify counter was restored
             [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
             ?assertEqual(42, Counter)
         end},
        {"init calculates counter from jobs when no DETS counter",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(flurm_db_jobs_dets) -> [{size, 2}];
                                        (_) -> undefined end),
             meck:expect(dets, select, fun(_, _) -> [{5, job1}, {10, job2}] end),
             meck:expect(dets, lookup, fun(flurm_db_counter_dets, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, _} = flurm_db_sup:init(#{}),

             %% Counter should be max job ID (10)
             [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
             ?assertEqual(10, Counter)
         end},
        {"init with empty jobs sets counter to 0",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(flurm_db_counter_dets, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, _} = flurm_db_sup:init(#{}),

             [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
             ?assertEqual(0, Counter)
         end}
     ]}.

%%====================================================================
%% Child Spec Tests
%%====================================================================

child_spec_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"child spec includes ra_starter",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(dets, [passthrough]),
             meck:new(lager, [passthrough]),
             meck:new(application, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir) -> {ok, "/tmp/test"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(dets, open_file, fun(_, _) -> {ok, test_table} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(dets, lookup, fun(_, counter) -> [] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             {ok, {_SupFlags, Children}} = flurm_db_sup:init(#{}),

             [ChildSpec] = Children,
             ?assertEqual(flurm_db_ra_starter, maps:get(id, ChildSpec)),
             ?assertEqual(transient, maps:get(restart, ChildSpec)),
             ?assertEqual(worker, maps:get(type, ChildSpec)),
             ?assertEqual([flurm_db_ra_starter], maps:get(modules, ChildSpec))
         end}
     ]}.
