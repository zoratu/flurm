%%%-------------------------------------------------------------------
%%% @doc Focused coverage tests for flurm_dbd_ra API wrapper paths.
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_api_wrappers_tests).

-include_lib("eunit/include/eunit.hrl").

-record(tres_usage, {
    entity_type,
    entity_id,
    period,
    cpu_seconds = 0,
    mem_seconds = 0,
    gpu_seconds = 0,
    node_seconds = 0,
    job_count = 0,
    job_time = 0
}).

ra_api_wrappers_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_cluster wrapper branches", fun test_start_cluster_branches/0},
      {"ra_command wrapper branches", fun test_ra_command_wrappers/0},
      {"apply update_usage record branch", fun test_apply_update_usage_record/0},
      {"local query wrappers", fun test_local_query_wrappers/0},
      {"consistent query wrappers", fun test_consistent_query_wrappers/0}
     ]}.

setup() ->
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(ra),
    catch meck:unload(flurm_tres),
    catch meck:unload(flurm_dbd_ra_effects),

    meck:new(ra, [non_strict, no_link]),

    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, add, fun(M1, M2) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(Old) -> Old + V end, V, Acc)
        end, M1, M2)
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{
            cpu_seconds => 0,
            mem_seconds => 0,
            gpu_seconds => 0,
            node_seconds => 0,
            job_count => 0,
            job_time => 0
        }
    end),
    meck:expect(flurm_tres, from_job, fun(JobMap) ->
        Elapsed = maps:get(elapsed, JobMap, 0),
        NumCpus = maps:get(num_cpus, JobMap, 1),
        ReqMem = maps:get(req_mem, JobMap, 0),
        NumNodes = maps:get(num_nodes, JobMap, 1),
        TresAlloc = maps:get(tres_alloc, JobMap, #{}),
        #{
            cpu_seconds => Elapsed * NumCpus,
            mem_seconds => Elapsed * ReqMem,
            gpu_seconds => Elapsed * maps:get(gpu, TresAlloc, 0),
            node_seconds => Elapsed * NumNodes,
            job_count => 1,
            job_time => Elapsed
        }
    end),

    meck:new(flurm_dbd_ra_effects, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra_effects, job_recorded, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_leader, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_follower, fun(_) -> ok end),

    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_ra_effects),
    catch meck:unload(flurm_tres),
    catch meck:unload(ra),
    ok.

test_start_cluster_branches() ->
    meck:expect(ra, start_cluster, fun(_Scope, _Name, _MachineConfig, Servers) ->
        {ok, [hd(Servers)], []}
    end),
    ?assertEqual(ok, flurm_dbd_ra:start_cluster([node()])),

    meck:expect(ra, start_cluster, fun(_Scope, _Name, _MachineConfig, _Servers) ->
        {ok, [], [failed_server]}
    end),
    ?assertMatch({error, {no_servers_started, _}}, flurm_dbd_ra:start_cluster([node()])),

    meck:expect(ra, start_cluster, fun(_Scope, _Name, _MachineConfig, _Servers) ->
        {error, no_quorum}
    end),
    ?assertEqual({error, no_quorum}, flurm_dbd_ra:start_cluster([node()])).

test_ra_command_wrappers() ->
    meck:expect(ra, process_command, fun(_Server, Cmd, _Timeout) ->
        case Cmd of
            {record_job, _} -> {ok, {ok, 42}, node()};
            {update_tres, _} -> {ok, ok, node()};
            {update_usage, _} -> {ok, ok, node()};
            {query_jobs, _} -> {ok, {ok, []}, node()};
            {query_tres, _} -> {ok, {ok, #{cpu_seconds => 7}}, node()}
        end
    end),

    ?assertEqual({ok, 42}, flurm_dbd_ra:record_job(#{job_id => 42})),
    ?assertEqual({ok, 42}, flurm_dbd_ra:record_job_completion(#{job_id => 42})),
    ?assertEqual(ok, flurm_dbd_ra:update_tres(#{entity_type => user, entity_id => <<"u">>})),
    ?assertEqual(ok, flurm_dbd_ra:update_usage(#{entity_type => user, entity_id => <<"u">>})),
    ?assertEqual({ok, []}, flurm_dbd_ra:query_jobs(#{})),
    ?assertEqual({ok, #{cpu_seconds => 7}},
                 flurm_dbd_ra:query_tres({user, <<"u">>, <<"2026-02">>})),

    meck:expect(ra, process_command, fun(_Server, _Cmd, _Timeout) ->
        {timeout, node()}
    end),
    ?assertEqual({error, timeout}, flurm_dbd_ra:query_jobs(#{})),

    meck:expect(ra, process_command, fun(_Server, _Cmd, _Timeout) ->
        {error, raft_down}
    end),
    ?assertEqual({error, raft_down}, flurm_dbd_ra:query_tres({user, <<"u">>, <<"2026-02">>})).

test_apply_update_usage_record() ->
    State0 = flurm_dbd_ra:init(#{}),
    Usage = #tres_usage{
        entity_type = user,
        entity_id = <<"u">>,
        period = <<"2026-02">>,
        cpu_seconds = 5,
        mem_seconds = 10,
        gpu_seconds = 2,
        node_seconds = 3,
        job_count = 1,
        job_time = 4
    },

    {State1, ok, []} = flurm_dbd_ra:apply(#{}, {update_usage, Usage}, State0),
    {State2, ok, []} = flurm_dbd_ra:apply(#{}, {update_usage, Usage}, State1),
    {State2, {ok, AggUsage}, []} =
        flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"u">>, <<"2026-02">>}}, State2),
    ?assertEqual(10, maps:get(cpu_seconds, AggUsage)),
    ?assertEqual(2, maps:get(job_count, AggUsage)),

    MadeUsage = flurm_dbd_ra:make_usage_record(#{
        key => {user, <<"u">>, <<"2026-02">>},
        period => <<"2026-02">>,
        cpu_seconds => 1
    }),
    ?assertEqual(usage_record, element(1, MadeUsage)),

    SumTres = flurm_dbd_ra:aggregate_tres(
        #{cpu_seconds => 1, mem_seconds => 2},
        #{cpu_seconds => 3, mem_seconds => 4}
    ),
    ?assertEqual(4, maps:get(cpu_seconds, SumTres)),
    ?assertEqual(6, maps:get(mem_seconds, SumTres)),

    StateWithJob = build_state(),
    RecordedSet = flurm_dbd_ra:job_recorded_set(StateWithJob),
    ?assertEqual(true, sets:is_element(1001, RecordedSet)).

test_local_query_wrappers() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        {ok, {1, QueryFun(build_state())}, node()}
    end),

    ?assertMatch(#{job_id := 1001}, flurm_dbd_ra:get_job_record(1001)),
    ?assertMatch({ok, [_|_]}, flurm_dbd_ra:list_job_records()),
    ?assertMatch({ok, [_|_]}, flurm_dbd_ra:list_job_records(#{user_name => <<"alice">>})),
    ?assert(is_map(flurm_dbd_ra:get_user_usage(<<"alice">>))),
    ?assert(is_map(flurm_dbd_ra:get_account_usage(<<"acct">>))),
    ?assert(is_map(flurm_dbd_ra:get_tres_usage(user, <<"alice">>, <<"2026-02">>))),
    ?assertEqual(true, flurm_dbd_ra:is_job_recorded(1001)),
    ?assertMatch({ok, _}, flurm_dbd_ra:get_user_totals(<<"alice">>)),
    ?assertMatch({ok, _}, flurm_dbd_ra:get_account_totals(<<"acct">>)),

    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        {ok, {1, QueryFun(flurm_dbd_ra:init(#{}))}, node()}
    end),

    ?assertEqual({error, not_found}, flurm_dbd_ra:get_job_record(9999)),
    ?assertEqual({error, not_found}, flurm_dbd_ra:get_tres_usage(user, <<"nobody">>, <<"2026-02">>)),
    ?assertEqual(false, flurm_dbd_ra:is_job_recorded(9999)),
    ?assertEqual({error, not_found}, flurm_dbd_ra:get_user_totals(<<"nobody">>)),
    ?assertEqual({error, not_found}, flurm_dbd_ra:get_account_totals(<<"nobody">>)),

    meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
        {timeout, node()}
    end),

    ?assertEqual({error, timeout}, flurm_dbd_ra:get_job_record(1001)),
    ?assertEqual(false, flurm_dbd_ra:is_job_recorded(1001)),

    meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, local_query_failed}
    end),

    ?assertEqual({error, local_query_failed},
                 flurm_dbd_ra:get_tres_usage(user, <<"alice">>, <<"2026-02">>)),
    ?assertEqual({error, local_query_failed}, flurm_dbd_ra:get_user_totals(<<"alice">>)),
    ?assertEqual({error, local_query_failed}, flurm_dbd_ra:get_account_totals(<<"acct">>)).

test_consistent_query_wrappers() ->
    meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
        {ok, {1, QueryFun(build_state())}, node()}
    end),

    ?assertMatch({ok, #{job_id := 1001}}, flurm_dbd_ra:consistent_get_job_record(1001)),
    ?assertMatch({ok, [_|_]}, flurm_dbd_ra:consistent_list_job_records()),

    meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
        {ok, {1, QueryFun(flurm_dbd_ra:init(#{}))}, node()}
    end),
    ?assertEqual({error, not_found}, flurm_dbd_ra:consistent_get_job_record(9999)),

    meck:expect(ra, consistent_query, fun(_Server, _QueryFun, _Timeout) ->
        {timeout, node()}
    end),
    ?assertEqual({error, timeout}, flurm_dbd_ra:consistent_list_job_records()),

    meck:expect(ra, consistent_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, no_leader}
    end),
    ?assertEqual({error, no_leader}, flurm_dbd_ra:consistent_get_job_record(1001)).

build_state() ->
    State0 = flurm_dbd_ra:init(#{}),
    JobInfo = #{
        job_id => 1001,
        user_name => <<"alice">>,
        account => <<"acct">>,
        elapsed => 10,
        num_cpus => 2,
        req_mem => 3,
        num_nodes => 1,
        tres_alloc => #{gpu => 1}
    },
    {State1, {ok, 1001}, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State0),
    UsageUpdate = #{
        entity_type => user,
        entity_id => <<"alice">>,
        period => <<"2026-02">>,
        cpu_seconds => 5,
        job_count => 1
    },
    {State2, ok, _} = flurm_dbd_ra:apply(#{}, {update_tres, UsageUpdate}, State1),
    State2.
