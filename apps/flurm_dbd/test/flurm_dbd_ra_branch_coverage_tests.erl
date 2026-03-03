%%%-------------------------------------------------------------------
%%% @doc Branch-coverage tests for flurm_dbd_ra uncovered paths.
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_branch_coverage_tests).

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

ra_branch_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"apply update_usage record new+existing branches", fun test_apply_update_usage_record_branches/0},
      {"get_job_record success branch", fun test_get_job_record_success_branch/0},
      {"get_tres_usage success+other branches", fun test_get_tres_usage_branches/0},
      {"is_job_recorded true branch", fun test_is_job_recorded_true_branch/0},
      {"get_user_totals branches", fun test_get_user_totals_branches/0},
      {"get_account_totals branches", fun test_get_account_totals_branches/0},
      {"consistent_get_job_record found+error branches", fun test_consistent_get_job_record_branches/0},
      {"ra_command error branch", fun test_ra_command_error_branch/0},
      {"internal helper branches", fun test_internal_helper_branches/0}
     ]}.

setup() ->
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(lager),
    catch meck:unload(ra),
    catch meck:unload(flurm_dbd_ra_effects),
    catch meck:unload(flurm_tres),

    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),

    meck:new(ra, [non_strict, no_link]),

    meck:new(flurm_dbd_ra_effects, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra_effects, job_recorded, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_leader, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_follower, fun(_) -> ok end),

    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, add, fun(A, B) when is_map(A), is_map(B) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(Old) -> Old + V end, V, Acc)
        end, A, B)
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
          node_seconds => 0, job_count => 0, job_time => 0}
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
    ok.

cleanup(_) ->
    catch meck:unload(flurm_tres),
    catch meck:unload(flurm_dbd_ra_effects),
    catch meck:unload(ra),
    catch meck:unload(lager),
    ok.

test_apply_update_usage_record_branches() ->
    State0 = flurm_dbd_ra:init(#{}),
    Usage = #tres_usage{
        entity_type = user,
        entity_id = <<"user_rec">>,
        period = <<"2026-02">>,
        cpu_seconds = 10,
        mem_seconds = 20,
        gpu_seconds = 3,
        node_seconds = 4,
        job_count = 1,
        job_time = 5
    },
    {State1, ok, []} = flurm_dbd_ra:apply(#{}, {update_usage, Usage}, State0),
    {State2, ok, []} = flurm_dbd_ra:apply(#{}, {update_usage, Usage}, State1),

    {State2, {ok, UsageMap}, []} =
        flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"user_rec">>, <<"2026-02">>}}, State2),
    ?assertEqual(20, maps:get(cpu_seconds, UsageMap)),
    ?assertEqual(40, maps:get(mem_seconds, UsageMap)),
    ?assertEqual(2, maps:get(job_count, UsageMap)).

test_get_job_record_success_branch() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State = state_with_job(101),
        {ok, {1, QueryFun(State)}, node()}
    end),
    Result = flurm_dbd_ra:get_job_record(101),
    ?assert(is_map(Result)),
    ?assertEqual(101, maps:get(job_id, Result)).

test_get_tres_usage_branches() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State0 = flurm_dbd_ra:init(#{}),
        Update = #{
            entity_type => user,
            entity_id => <<"u1">>,
            period => <<"2026-02">>,
            cpu_seconds => 7,
            job_count => 1
        },
        {State1, ok, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State0),
        {ok, {1, QueryFun(State1)}, node()}
    end),
    Usage = flurm_dbd_ra:get_tres_usage(user, <<"u1">>, <<"2026-02">>),
    ?assert(is_map(Usage)),
    ?assertEqual(7, maps:get(cpu_seconds, Usage)),

    meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, some_reason}
    end),
    ?assertEqual({error, some_reason},
                 flurm_dbd_ra:get_tres_usage(user, <<"u1">>, <<"2026-02">>)).

test_is_job_recorded_true_branch() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State = state_with_job(77),
        {ok, {1, QueryFun(State)}, node()}
    end),
    ?assertEqual(true, flurm_dbd_ra:is_job_recorded(77)).

test_get_user_totals_branches() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State0 = flurm_dbd_ra:init(#{}),
        Update = #{
            entity_type => user,
            entity_id => <<"alice">>,
            period => <<"2026-02">>,
            cpu_seconds => 11,
            job_count => 1
        },
        {State1, ok, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State0),
        {ok, {1, QueryFun(State1)}, node()}
    end),
    {ok, Totals} = flurm_dbd_ra:get_user_totals(<<"alice">>),
    ?assertEqual(11, maps:get(cpu_seconds, Totals)),

    meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, totals_error}
    end),
    ?assertEqual({error, totals_error}, flurm_dbd_ra:get_user_totals(<<"alice">>)).

test_get_account_totals_branches() ->
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State0 = flurm_dbd_ra:init(#{}),
        Update = #{
            entity_type => account,
            entity_id => <<"acct">>,
            period => <<"2026-02">>,
            cpu_seconds => 13,
            job_count => 1
        },
        {State1, ok, _} = flurm_dbd_ra:apply(#{}, {update_tres, Update}, State0),
        {ok, {1, QueryFun(State1)}, node()}
    end),
    {ok, Totals} = flurm_dbd_ra:get_account_totals(<<"acct">>),
    ?assertEqual(13, maps:get(cpu_seconds, Totals)),

    meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, totals_error}
    end),
    ?assertEqual({error, totals_error}, flurm_dbd_ra:get_account_totals(<<"acct">>)).

test_consistent_get_job_record_branches() ->
    meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
        State = state_with_job(202),
        {ok, {1, QueryFun(State)}, node()}
    end),
    {ok, JobMap} = flurm_dbd_ra:consistent_get_job_record(202),
    ?assertEqual(202, maps:get(job_id, JobMap)),

    meck:expect(ra, consistent_query, fun(_Server, _QueryFun, _Timeout) ->
        {error, consistent_failed}
    end),
    ?assertEqual({error, consistent_failed}, flurm_dbd_ra:consistent_get_job_record(202)).

test_ra_command_error_branch() ->
    meck:expect(ra, process_command, fun(_Server, _Cmd, _Timeout) ->
        {error, raft_failed}
    end),
    ?assertEqual({error, raft_failed},
                 flurm_dbd_ra:update_tres(#{entity_type => user, entity_id => <<"u">>})).

test_internal_helper_branches() ->
    UsageMap = #{
        key => {user, <<"helper">>, <<"2026-02">>},
        period => <<"2026-02">>,
        cpu_seconds => 1,
        mem_seconds => 2,
        gpu_seconds => 3,
        node_seconds => 4,
        job_count => 5,
        job_time => 6
    },
    UsageRecord = flurm_dbd_ra:make_usage_record(UsageMap),
    ?assertEqual(usage_record, element(1, UsageRecord)),

    StateWithJob = state_with_job(303),
    Set = flurm_dbd_ra:job_recorded_set(StateWithJob),
    ?assertEqual(true, sets:is_element(303, Set)),

    %% Empty entity_id should not update running totals.
    State0 = flurm_dbd_ra:init(#{}),
    EmptyUpdate = #{
        entity_type => user,
        entity_id => <<>>,
        period => <<"2026-02">>,
        cpu_seconds => 99,
        job_count => 1
    },
    {State1, ok, _} = flurm_dbd_ra:apply(#{}, {update_tres, EmptyUpdate}, State0),
    UserTotals = element(5, State1),
    ?assertEqual(#{}, UserTotals).

state_with_job(JobId) ->
    State0 = flurm_dbd_ra:init(#{}),
    JobInfo = make_job_info(JobId),
    {State1, {ok, JobId}, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State0),
    State1.

make_job_info(JobId) ->
    Now = erlang:system_time(second),
    #{
        job_id => JobId,
        job_name => <<"job">>,
        user_name => <<"testuser">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"testaccount">>,
        partition => <<"default">>,
        cluster => <<"flurm">>,
        qos => <<"normal">>,
        state => completed,
        exit_code => 0,
        num_nodes => 1,
        num_cpus => 2,
        num_tasks => 2,
        req_mem => 1024,
        submit_time => Now - 300,
        start_time => Now - 200,
        end_time => Now - 100,
        elapsed => 100,
        tres_alloc => #{gpu => 1},
        tres_req => #{}
    }.
