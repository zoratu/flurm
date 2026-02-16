%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_ra module.
%%%
%%% Tests the Ra state machine for distributed accounting.
%%% Tests pure functions directly without requiring Ra cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Pure function tests (TEST exports)
%%====================================================================

pure_functions_test_() ->
    {setup,
     fun setup_lager/0,
     fun cleanup_lager/1,
     [
      %% make_job_record
      {"make_job_record full", fun test_make_job_record_full/0},
      {"make_job_record minimal", fun test_make_job_record_minimal/0},
      {"make_job_record defaults", fun test_make_job_record_defaults/0},

      %% calculate_tres_from_job
      {"calculate_tres_from_job", fun test_calculate_tres/0},

      %% aggregate_tres
      {"aggregate_tres two maps", fun test_aggregate_tres/0},
      {"aggregate_tres empty maps", fun test_aggregate_tres_empty/0},

      %% init
      {"init returns empty state", fun test_init/0},

      %% apply - record_job
      {"apply record_job new job", fun test_apply_record_job_new/0},
      {"apply record_job duplicate", fun test_apply_record_job_dup/0},
      {"apply record_job_completion alias", fun test_apply_record_job_completion/0},

      %% apply - update_tres
      {"apply update_tres new", fun test_apply_update_tres_new/0},
      {"apply update_tres aggregate", fun test_apply_update_tres_agg/0},
      {"apply update_tres user", fun test_apply_update_tres_user/0},
      {"apply update_tres account", fun test_apply_update_tres_account/0},

      %% apply - query_jobs
      {"apply query_jobs no filter", fun test_apply_query_jobs/0},
      {"apply query_jobs with filter", fun test_apply_query_jobs_filter/0},

      %% apply - query_tres
      {"apply query_tres found", fun test_apply_query_tres_found/0},
      {"apply query_tres not found", fun test_apply_query_tres_not_found/0},

      %% apply - unknown
      {"apply unknown command", fun test_apply_unknown/0},

      %% state_enter
      {"state_enter leader", fun test_state_enter_leader/0},
      {"state_enter follower", fun test_state_enter_follower/0},
      {"state_enter other", fun test_state_enter_other/0},

      %% snapshot_module
      {"snapshot_module returns module", fun test_snapshot_module/0}
     ]}.

setup_lager() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, add, fun(M1, M2) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(X) -> X + V end, V, Acc)
        end, M1, M2)
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0, node_seconds => 0, job_count => 0, job_time => 0}
    end),
    meck:expect(flurm_tres, from_job, fun(Job) ->
        Elapsed = maps:get(elapsed, Job, 0),
        #{
            cpu_seconds => Elapsed * maps:get(num_cpus, Job, 1),
            mem_seconds => Elapsed * maps:get(req_mem, Job, 0),
            node_seconds => Elapsed * maps:get(num_nodes, Job, 1),
            gpu_seconds => 0,
            job_count => 1,
            job_time => Elapsed
        }
    end),
    ok.

cleanup_lager(_) ->
    catch meck:unload(flurm_tres),
    catch meck:unload(lager),
    ok.

%% === make_job_record ===

test_make_job_record_full() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 42, job_name => <<"test">>, user_name => <<"alice">>,
        user_id => 1000, group_id => 100, account => <<"research">>,
        partition => <<"batch">>, cluster => <<"cluster1">>,
        qos => <<"high">>, state => completed, exit_code => 0,
        num_nodes => 4, num_cpus => 16, num_tasks => 4, req_mem => 8192,
        submit_time => Now - 1000, start_time => Now - 900, end_time => Now,
        elapsed => 900, tres_alloc => #{cpu => 16}, tres_req => #{cpu => 16}
    },
    Record = flurm_dbd_ra:make_job_record(Job),
    ?assert(is_tuple(Record)).

test_make_job_record_minimal() ->
    Job = #{job_id => 1},
    Record = flurm_dbd_ra:make_job_record(Job),
    ?assert(is_tuple(Record)).

test_make_job_record_defaults() ->
    Job = #{job_id => 123},
    Record = flurm_dbd_ra:make_job_record(Job),
    ?assert(is_tuple(Record)).

%% === calculate_tres_from_job ===

test_calculate_tres() ->
    Job = #{job_id => 1, elapsed => 100, num_cpus => 4, req_mem => 1024, num_nodes => 2},
    Record = flurm_dbd_ra:make_job_record(Job),
    Tres = flurm_dbd_ra:calculate_tres_from_job(Record),
    ?assert(is_map(Tres)),
    ?assert(maps:get(cpu_seconds, Tres) >= 0).

%% === aggregate_tres ===

test_aggregate_tres() ->
    Tres1 = #{cpu_seconds => 100, mem_seconds => 200},
    Tres2 = #{cpu_seconds => 50, mem_seconds => 100},
    Result = flurm_dbd_ra:aggregate_tres(Tres1, Tres2),
    ?assertEqual(150, maps:get(cpu_seconds, Result)),
    ?assertEqual(300, maps:get(mem_seconds, Result)).

test_aggregate_tres_empty() ->
    Result = flurm_dbd_ra:aggregate_tres(#{}, #{}),
    ?assertEqual(#{}, Result).

%% === init ===

test_init() ->
    State = flurm_dbd_ra:init(#{}),
    ?assert(is_tuple(State)).

%% === apply - record_job ===

test_apply_record_job_new() ->
    State = flurm_dbd_ra:init(#{}),
    Job = #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>, elapsed => 100},
    {NewState, Result, Effects} = flurm_dbd_ra:apply(#{}, {record_job, Job}, State),
    ?assertEqual({ok, 1}, Result),
    ?assert(is_list(Effects)),
    ?assertNot(State =:= NewState).

test_apply_record_job_dup() ->
    State = flurm_dbd_ra:init(#{}),
    Job = #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>},
    {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, Job}, State),
    {State2, Result, _} = flurm_dbd_ra:apply(#{}, {record_job, Job}, State1),
    ?assertEqual({error, already_recorded}, Result),
    ?assertEqual(State1, State2).

test_apply_record_job_completion() ->
    State = flurm_dbd_ra:init(#{}),
    Job = #{job_id => 2, user_name => <<"bob">>, account => <<"acct">>},
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {record_job_completion, Job}, State),
    ?assertEqual({ok, 2}, Result).

%% === apply - update_tres ===

test_apply_update_tres_new() ->
    State = flurm_dbd_ra:init(#{}),
    Update = #{entity_type => user, entity_id => <<"alice">>, cpu_seconds => 100},
    {NewState, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State),
    ?assertEqual(ok, Result),
    ?assertNot(State =:= NewState).

test_apply_update_tres_agg() ->
    State = flurm_dbd_ra:init(#{}),
    Update1 = #{entity_type => user, entity_id => <<"alice">>, cpu_seconds => 100},
    Update2 = #{entity_type => user, entity_id => <<"alice">>, cpu_seconds => 50},
    {State1, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update1}, State),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update2}, State1),
    ?assertEqual(ok, Result).

test_apply_update_tres_user() ->
    State = flurm_dbd_ra:init(#{}),
    Update = #{entity_type => user, entity_id => <<"bob">>, cpu_seconds => 200},
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State),
    ?assertEqual(ok, Result).

test_apply_update_tres_account() ->
    State = flurm_dbd_ra:init(#{}),
    Update = #{entity_type => account, entity_id => <<"research">>, cpu_seconds => 300},
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State),
    ?assertEqual(ok, Result).

%% === apply - query_jobs ===

test_apply_query_jobs() ->
    State = flurm_dbd_ra:init(#{}),
    Job = #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>},
    {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, Job}, State),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, #{}}, State1),
    ?assertMatch({ok, _}, Result),
    {ok, Jobs} = Result,
    ?assertEqual(1, length(Jobs)).

test_apply_query_jobs_filter() ->
    State = flurm_dbd_ra:init(#{}),
    Job1 = #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>},
    Job2 = #{job_id => 2, user_name => <<"bob">>, account => <<"acct">>},
    {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, Job1}, State),
    {State2, _, _} = flurm_dbd_ra:apply(#{}, {record_job, Job2}, State1),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, #{user_name => <<"alice">>}}, State2),
    ?assertMatch({ok, _}, Result),
    {ok, Jobs} = Result,
    ?assertEqual(1, length(Jobs)).

%% === apply - query_tres ===

test_apply_query_tres_found() ->
    State = flurm_dbd_ra:init(#{}),
    Period = list_to_binary(io_lib:format("~4..0B-~2..0B", [2026, 2])),
    Update = #{entity_type => user, entity_id => <<"alice">>, period => Period, cpu_seconds => 100},
    {State1, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"alice">>, Period}}, State1),
    ?assertMatch({ok, _}, Result).

test_apply_query_tres_not_found() ->
    State = flurm_dbd_ra:init(#{}),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"nobody">>, <<"2020-01">>}}, State),
    ?assertEqual({error, not_found}, Result).

%% === apply - unknown ===

test_apply_unknown() ->
    State = flurm_dbd_ra:init(#{}),
    {_, Result, _} = flurm_dbd_ra:apply(#{}, {unknown_command, whatever}, State),
    ?assertMatch({error, {unknown_command, _}}, Result).

%% === state_enter ===

test_state_enter_leader() ->
    State = flurm_dbd_ra:init(#{}),
    Effects = flurm_dbd_ra:state_enter(leader, State),
    ?assert(is_list(Effects)),
    ?assert(length(Effects) > 0).

test_state_enter_follower() ->
    State = flurm_dbd_ra:init(#{}),
    Effects = flurm_dbd_ra:state_enter(follower, State),
    ?assert(is_list(Effects)).

test_state_enter_other() ->
    State = flurm_dbd_ra:init(#{}),
    Effects = flurm_dbd_ra:state_enter(candidate, State),
    ?assertEqual([], Effects).

%% === snapshot_module ===

test_snapshot_module() ->
    ?assertEqual(ra_machine_simple, flurm_dbd_ra:snapshot_module()).
