%%%-------------------------------------------------------------------
%%% @doc FLURM DBD Ra 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_dbd_ra module covering all
%%% Ra machine callbacks, API functions, and internal helper functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    %% Mock Ra
    meck:new(ra, [non_strict, no_link]),
    meck:expect(ra, process_command, fun(_Server, Cmd, _Timeout) ->
        %% Simulate Ra command processing
        case Cmd of
            {record_job, _} -> {ok, {ok, 1}, node()};
            {update_tres, _} -> {ok, ok, node()};
            {update_usage, _} -> {ok, ok, node()};
            {query_jobs, _} -> {ok, {ok, []}, node()};
            {query_tres, _} -> {ok, {error, not_found}, node()};
            _ -> {ok, {error, {unknown_command, Cmd}}, node()}
        end
    end),
    meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
        State = flurm_dbd_ra:init(#{}),
        Result = QueryFun(State),
        {ok, {1, Result}, node()}
    end),
    meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
        State = flurm_dbd_ra:init(#{}),
        Result = QueryFun(State),
        {ok, {1, Result}, node()}
    end),
    meck:expect(ra, start_cluster, fun(_, _Name, _Config, Servers) ->
        {ok, Servers, []}
    end),
    %% Mock flurm_dbd_ra_effects
    meck:new(flurm_dbd_ra_effects, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra_effects, job_recorded, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_leader, fun(_) -> ok end),
    meck:expect(flurm_dbd_ra_effects, became_follower, fun(_) -> ok end),
    %% Mock flurm_tres
    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, add, fun(A, B) when is_map(A), is_map(B) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(OldV) -> OldV + V end, V, Acc)
        end, A, B)
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0, node_seconds => 0, job_count => 0, job_time => 0}
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
    catch meck:unload(lager),
    catch meck:unload(ra),
    catch meck:unload(flurm_dbd_ra_effects),
    catch meck:unload(flurm_tres),
    ok.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"init returns initial state", fun() ->
             State = flurm_dbd_ra:init(#{}),
             ?assert(is_tuple(State)),
             ?assertEqual(ra_acct_state, element(1, State))
         end},
         {"init with config", fun() ->
             State = flurm_dbd_ra:init(#{some_config => value}),
             ?assert(is_tuple(State))
         end},
         {"initial state has empty job_records", fun() ->
             State = flurm_dbd_ra:init(#{}),
             %% Access job_records field (2nd element)
             JobRecords = element(2, State),
             ?assertEqual(#{}, JobRecords)
         end},
         {"initial state has empty usage_records", fun() ->
             State = flurm_dbd_ra:init(#{}),
             %% Access usage_records field (4th element)
             UsageRecords = element(4, State),
             ?assertEqual(#{}, UsageRecords)
         end},
         {"initial state has version 1", fun() ->
             State = flurm_dbd_ra:init(#{}),
             %% Access version field (7th element)
             Version = element(7, State),
             ?assertEqual(1, Version)
         end}
     ]
    }.

%%====================================================================
%% Apply - Record Job Tests
%%====================================================================

apply_record_job_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"apply record_job creates job record", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {NewState, Result, Effects} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 ?assertMatch({ok, 1}, Result),
                 ?assert(is_tuple(NewState)),
                 ?assert(length(Effects) > 0)
             end},
             {"apply record_job rejects duplicate", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 %% Try to record same job again
                 {State2, Result, Effects} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State1),
                 ?assertEqual({error, already_recorded}, Result),
                 ?assertEqual([], Effects),
                 ?assertEqual(State1, State2)
             end},
             {"apply record_job updates user_totals", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {NewState, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 %% Access user_totals (5th element)
                 UserTotals = element(5, NewState),
                 ?assert(maps:is_key(<<"testuser">>, UserTotals))
             end},
             {"apply record_job updates account_totals", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {NewState, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 %% Access account_totals (6th element)
                 AccountTotals = element(6, NewState),
                 ?assert(maps:is_key(<<"testaccount">>, AccountTotals))
             end},
             {"apply record_job updates usage_records", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {NewState, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 %% Access usage_records (4th element)
                 UsageRecords = element(4, NewState),
                 ?assert(map_size(UsageRecords) > 0)
             end},
             {"apply record_job_completion is alias", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(2),
                 {NewState, Result, Effects} = flurm_dbd_ra:apply(#{}, {record_job_completion, JobInfo}, State),
                 ?assertMatch({ok, 2}, Result),
                 ?assert(is_tuple(NewState)),
                 ?assert(length(Effects) > 0)
             end}
         ]
     end
    }.

%%====================================================================
%% Apply - Update TRES Tests
%%====================================================================

apply_update_tres_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"apply update_tres creates new usage", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate = #{
                     entity_type => user,
                     entity_id => <<"testuser">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 1000,
                     mem_seconds => 2000
                 },
                 {NewState, Result, Effects} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate}, State),
                 ?assertEqual(ok, Result),
                 ?assertEqual([], Effects),
                 UsageRecords = element(4, NewState),
                 ?assert(map_size(UsageRecords) > 0)
             end},
             {"apply update_tres aggregates with existing", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate1 = #{
                     entity_type => user,
                     entity_id => <<"testuser">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 1000
                 },
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate1}, State),
                 TresUpdate2 = #{
                     entity_type => user,
                     entity_id => <<"testuser">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 500
                 },
                 {NewState, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate2}, State1),
                 UsageRecords = element(4, NewState),
                 Key = {user, <<"testuser">>, <<"2026-02">>},
                 Usage = maps:get(Key, UsageRecords),
                 %% CPU seconds should be aggregated
                 ?assertEqual(1500, element(4, Usage))  %% cpu_seconds is 4th element
             end},
             {"apply update_tres for account", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate = #{
                     entity_type => account,
                     entity_id => <<"testaccount">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 1000
                 },
                 {NewState, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate}, State),
                 ?assertEqual(ok, Result),
                 %% Account totals should be updated
                 AccountTotals = element(6, NewState),
                 ?assert(maps:is_key(<<"testaccount">>, AccountTotals))
             end},
             {"apply update_tres for cluster", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate = #{
                     entity_type => cluster,
                     entity_id => <<"flurm">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 1000
                 },
                 {NewState, Result, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate}, State),
                 ?assertEqual(ok, Result),
                 UsageRecords = element(4, NewState),
                 ?assert(map_size(UsageRecords) > 0)
             end},
             {"apply update_tres uses current period as default", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate = #{
                     entity_type => user,
                     entity_id => <<"testuser">>,
                     cpu_seconds => 1000
                 },
                 {NewState, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate}, State),
                 UsageRecords = element(4, NewState),
                 ?assert(map_size(UsageRecords) > 0)
             end}
         ]
     end
    }.

%%====================================================================
%% Apply - Query Jobs Tests
%%====================================================================

apply_query_jobs_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"apply query_jobs with empty state", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 {State, Result, Effects} = flurm_dbd_ra:apply(#{}, {query_jobs, #{}}, State),
                 ?assertEqual({ok, []}, Result),
                 ?assertEqual([], Effects)
             end},
             {"apply query_jobs returns matching jobs", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = make_job_info(1),
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 {State1, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, #{}}, State1),
                 ?assertMatch({ok, [_]}, Result)
             end},
             {"apply query_jobs filters by user_name", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo1 = (make_job_info(1))#{user_name => <<"user1">>},
                 JobInfo2 = (make_job_info(2))#{user_name => <<"user2">>},
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo1}, State),
                 {State2, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo2}, State1),
                 Filters = #{user_name => <<"user1">>},
                 {State2, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, Filters}, State2),
                 ?assertMatch({ok, [_]}, Result),
                 {ok, [Job]} = Result,
                 ?assertEqual(<<"user1">>, maps:get(user_name, Job))
             end},
             {"apply query_jobs filters by account", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo1 = (make_job_info(1))#{account => <<"acct1">>},
                 JobInfo2 = (make_job_info(2))#{account => <<"acct2">>},
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo1}, State),
                 {State2, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo2}, State1),
                 Filters = #{account => <<"acct2">>},
                 {State2, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, Filters}, State2),
                 ?assertMatch({ok, [_]}, Result),
                 {ok, [Job]} = Result,
                 ?assertEqual(<<"acct2">>, maps:get(account, Job))
             end},
             {"apply query_jobs filters by partition", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo = (make_job_info(1))#{partition => <<"gpu">>},
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo}, State),
                 Filters = #{partition => <<"gpu">>},
                 {State1, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, Filters}, State1),
                 ?assertMatch({ok, [_]}, Result)
             end},
             {"apply query_jobs filters by state", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 JobInfo1 = (make_job_info(1))#{state => completed},
                 JobInfo2 = (make_job_info(2))#{state => failed},
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo1}, State),
                 {State2, _, _} = flurm_dbd_ra:apply(#{}, {record_job, JobInfo2}, State1),
                 Filters = #{state => failed},
                 {State2, Result, _} = flurm_dbd_ra:apply(#{}, {query_jobs, Filters}, State2),
                 ?assertMatch({ok, [_]}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Apply - Query TRES Tests
%%====================================================================

apply_query_tres_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"apply query_tres not found", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 {State, Result, Effects} = flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"unknown">>, <<"2026-02">>}}, State),
                 ?assertEqual({error, not_found}, Result),
                 ?assertEqual([], Effects)
             end},
             {"apply query_tres finds existing", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 TresUpdate = #{
                     entity_type => user,
                     entity_id => <<"testuser">>,
                     period => <<"2026-02">>,
                     cpu_seconds => 1000
                 },
                 {State1, _, _} = flurm_dbd_ra:apply(#{}, {update_tres, TresUpdate}, State),
                 {State1, Result, _} = flurm_dbd_ra:apply(#{}, {query_tres, {user, <<"testuser">>, <<"2026-02">>}}, State1),
                 ?assertMatch({ok, _}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Apply - Unknown Command Tests
%%====================================================================

apply_unknown_command_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"apply unknown command returns error", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 {State, Result, Effects} = flurm_dbd_ra:apply(#{}, {unknown_command, #{}}, State),
                 ?assertMatch({error, {unknown_command, _}}, Result),
                 ?assertEqual([], Effects)
             end}
         ]
     end
    }.

%%====================================================================
%% State Enter Tests
%%====================================================================

state_enter_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"state_enter leader triggers effect", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 Effects = flurm_dbd_ra:state_enter(leader, State),
                 ?assertMatch([{mod_call, flurm_dbd_ra_effects, became_leader, [_]}], Effects)
             end},
             {"state_enter follower triggers effect", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 Effects = flurm_dbd_ra:state_enter(follower, State),
                 ?assertMatch([{mod_call, flurm_dbd_ra_effects, became_follower, [_]}], Effects)
             end},
             {"state_enter candidate returns empty", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 Effects = flurm_dbd_ra:state_enter(candidate, State),
                 ?assertEqual([], Effects)
             end},
             {"state_enter pre_vote returns empty", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 Effects = flurm_dbd_ra:state_enter(pre_vote, State),
                 ?assertEqual([], Effects)
             end},
             {"state_enter eol returns empty", fun() ->
                 State = flurm_dbd_ra:init(#{}),
                 Effects = flurm_dbd_ra:state_enter(eol, State),
                 ?assertEqual([], Effects)
             end}
         ]
     end
    }.

%%====================================================================
%% Snapshot Module Tests
%%====================================================================

snapshot_module_test_() ->
    [
        {"snapshot_module returns ra_machine_simple", fun() ->
             Result = flurm_dbd_ra:snapshot_module(),
             ?assertEqual(ra_machine_simple, Result)
         end}
    ].

%%====================================================================
%% Start Cluster Tests
%%====================================================================

start_cluster_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"start_cluster succeeds", fun() ->
                 meck:expect(ra, start_cluster, fun(_, _Name, _Config, Servers) ->
                     {ok, Servers, []}
                 end),
                 Result = flurm_dbd_ra:start_cluster([node()]),
                 ?assertEqual(ok, Result)
             end},
             {"start_cluster fails when no servers started", fun() ->
                 meck:expect(ra, start_cluster, fun(_, _Name, _Config, _Servers) ->
                     {ok, [], [node()]}
                 end),
                 Result = flurm_dbd_ra:start_cluster([node()]),
                 ?assertMatch({error, {no_servers_started, _}}, Result)
             end},
             {"start_cluster returns error on failure", fun() ->
                 meck:expect(ra, start_cluster, fun(_, _Name, _Config, _Servers) ->
                     {error, some_reason}
                 end),
                 Result = flurm_dbd_ra:start_cluster([node()]),
                 ?assertEqual({error, some_reason}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% API Command Tests
%%====================================================================

api_record_job_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"record_job API succeeds", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {record_job, _}, _Timeout) ->
                     {ok, {ok, 123}, node()}
                 end),
                 Result = flurm_dbd_ra:record_job(make_job_info(123)),
                 ?assertEqual({ok, 123}, Result)
             end},
             {"record_job API returns already_recorded", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {record_job, _}, _Timeout) ->
                     {ok, {error, already_recorded}, node()}
                 end),
                 Result = flurm_dbd_ra:record_job(make_job_info(123)),
                 ?assertEqual({error, already_recorded}, Result)
             end},
             {"record_job API handles timeout", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {record_job, _}, _Timeout) ->
                     {timeout, node()}
                 end),
                 Result = flurm_dbd_ra:record_job(make_job_info(123)),
                 ?assertEqual({error, timeout}, Result)
             end},
             {"record_job_completion API is alias", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {record_job, _}, _Timeout) ->
                     {ok, {ok, 456}, node()}
                 end),
                 Result = flurm_dbd_ra:record_job_completion(make_job_info(456)),
                 ?assertEqual({ok, 456}, Result)
             end}
         ]
     end
    }.

api_update_tres_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"update_tres API succeeds", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {update_tres, _}, _Timeout) ->
                     {ok, ok, node()}
                 end),
                 Result = flurm_dbd_ra:update_tres(#{entity_type => user, entity_id => <<"test">>}),
                 ?assertEqual(ok, Result)
             end},
             {"update_usage API is alias", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {update_usage, _}, _Timeout) ->
                     {ok, ok, node()}
                 end),
                 Result = flurm_dbd_ra:update_usage(#{entity_type => user, entity_id => <<"test">>}),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

api_query_jobs_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"query_jobs API succeeds", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {query_jobs, _}, _Timeout) ->
                     {ok, {ok, []}, node()}
                 end),
                 Result = flurm_dbd_ra:query_jobs(#{}),
                 ?assertEqual({ok, []}, Result)
             end},
             {"query_tres API succeeds", fun() ->
                 meck:expect(ra, process_command, fun(_Server, {query_tres, _}, _Timeout) ->
                     {ok, {ok, #{cpu_seconds => 1000}}, node()}
                 end),
                 Result = flurm_dbd_ra:query_tres({user, <<"test">>, <<"2026-02">>}),
                 ?assertEqual({ok, #{cpu_seconds => 1000}}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% API Local Query Tests
%%====================================================================

api_get_job_record_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"get_job_record not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_job_record(999),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"get_job_record handles timeout", fun() ->
                 meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
                     {timeout, node()}
                 end),
                 Result = flurm_dbd_ra:get_job_record(1),
                 ?assertEqual({error, timeout}, Result)
             end},
             {"get_job_record handles error", fun() ->
                 meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
                     {error, some_reason}
                 end),
                 Result = flurm_dbd_ra:get_job_record(1),
                 ?assertEqual({error, some_reason}, Result)
             end}
         ]
     end
    }.

api_list_job_records_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"list_job_records returns empty list", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:list_job_records(),
                 ?assertEqual({ok, []}, Result)
             end},
             {"list_job_records/1 with filters", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:list_job_records(#{user_name => <<"test">>}),
                 ?assertEqual({ok, []}, Result)
             end}
         ]
     end
    }.

api_get_usage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"get_user_usage not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_user_usage(<<"unknown">>),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"get_account_usage not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_account_usage(<<"unknown">>),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"get_tres_usage not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_tres_usage(user, <<"unknown">>, <<"2026-02">>),
                 ?assertEqual({error, not_found}, Result)
             end}
         ]
     end
    }.

api_is_job_recorded_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"is_job_recorded returns false for unknown", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:is_job_recorded(999),
                 ?assertEqual(false, Result)
             end},
             {"is_job_recorded handles error", fun() ->
                 meck:expect(ra, local_query, fun(_Server, _QueryFun, _Timeout) ->
                     {error, some_error}
                 end),
                 Result = flurm_dbd_ra:is_job_recorded(999),
                 ?assertEqual(false, Result)
             end}
         ]
     end
    }.

api_get_totals_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"get_user_totals not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_user_totals(<<"unknown">>),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"get_account_totals not found", fun() ->
                 meck:expect(ra, local_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:get_account_totals(<<"unknown">>),
                 ?assertEqual({error, not_found}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% API Consistent Query Tests
%%====================================================================

api_consistent_query_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"consistent_get_job_record not found", fun() ->
                 meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:consistent_get_job_record(999),
                 ?assertEqual({error, not_found}, Result)
             end},
             {"consistent_get_job_record handles timeout", fun() ->
                 meck:expect(ra, consistent_query, fun(_Server, _QueryFun, _Timeout) ->
                     {timeout, node()}
                 end),
                 Result = flurm_dbd_ra:consistent_get_job_record(1),
                 ?assertEqual({error, timeout}, Result)
             end},
             {"consistent_list_job_records returns empty", fun() ->
                 meck:expect(ra, consistent_query, fun(_Server, QueryFun, _Timeout) ->
                     State = flurm_dbd_ra:init(#{}),
                     {ok, {1, QueryFun(State)}, node()}
                 end),
                 Result = flurm_dbd_ra:consistent_list_job_records(),
                 ?assertEqual({ok, []}, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Internal Helper Function Tests
%%====================================================================

-ifdef(TEST).

make_job_record_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"make_job_record creates record from map", fun() ->
                 JobInfo = make_job_info(123),
                 Record = flurm_dbd_ra:make_job_record(JobInfo),
                 ?assert(is_tuple(Record)),
                 ?assertEqual(job_record, element(1, Record)),
                 ?assertEqual(123, element(2, Record))  %% job_id
             end},
             {"make_job_record uses defaults", fun() ->
                 JobInfo = #{job_id => 456},
                 Record = flurm_dbd_ra:make_job_record(JobInfo),
                 ?assertEqual(456, element(2, Record)),
                 ?assertEqual(<<>>, element(3, Record))  %% job_name defaults to <<>>
             end}
         ]
     end
    }.

calculate_tres_from_job_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"calculate_tres_from_job computes correctly", fun() ->
                 JobInfo = make_job_info(1),
                 Record = flurm_dbd_ra:make_job_record(JobInfo),
                 Tres = flurm_dbd_ra:calculate_tres_from_job(Record),
                 ?assert(is_map(Tres)),
                 ?assert(maps:is_key(cpu_seconds, Tres))
             end}
         ]
     end
    }.

-endif.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_info(JobId) ->
    Now = erlang:system_time(second),
    #{
        job_id => JobId,
        job_name => <<"test_job">>,
        user_name => <<"testuser">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"testaccount">>,
        partition => <<"default">>,
        cluster => <<"flurm">>,
        qos => <<"normal">>,
        state => completed,
        exit_code => 0,
        num_nodes => 2,
        num_cpus => 8,
        num_tasks => 8,
        req_mem => 4096,
        submit_time => Now - 300,
        start_time => Now - 200,
        end_time => Now - 100,
        elapsed => 100,
        tres_alloc => #{cpu => 8, mem => 4096, gpu => 1},
        tres_req => #{cpu => 8, mem => 4096}
    }.
