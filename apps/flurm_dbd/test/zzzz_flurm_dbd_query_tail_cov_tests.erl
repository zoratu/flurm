%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_dbd_query edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_dbd_query_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

query_tail_cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"query_jobs_paginated error passthrough", fun query_jobs_paginated_error_branch_test/0},
      {"query_tres_usage uses ra path", fun query_tres_usage_ra_branch_test/0},
      {"query_jobs uses ra path", fun query_jobs_ra_branch_test/0},
      {"format_tres_string covers mem/node/gpu seconds", fun format_tres_string_remaining_parts_test/0},
      {"aggregate_user_tres skips error periods", fun aggregate_user_tres_skips_errors_test/0}
     ]}.

setup() ->
    maybe_unregister_ra(),
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(flurm_dbd_ra),

    meck:new(flurm_dbd_server, [no_link, non_strict]),
    meck:new(flurm_dbd_ra, [no_link, non_strict]),

    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) -> [] end),
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_User) -> #{} end),
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(_Account) -> #{} end),
    meck:expect(flurm_dbd_server, calculate_cluster_tres_usage, fun() -> #{} end),

    meck:expect(flurm_dbd_ra, query_jobs, fun(_Filters) -> {ok, []} end),
    meck:expect(flurm_dbd_ra, get_tres_usage, fun(_Type, _Id, _Period) -> {ok, #{}} end),
    ok.

cleanup(_) ->
    maybe_unregister_ra(),
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(flurm_dbd_server),
    ok.

query_jobs_paginated_error_branch_test() ->
    meck:expect(flurm_dbd_server, list_job_records,
                fun(_Filters) -> erlang:error(server_down) end),
    ?assertEqual({error, server_down},
                 flurm_dbd_query:query_jobs_paginated(#{}, #{})).

query_tres_usage_ra_branch_test() ->
    Pid = start_ra_placeholder(),
    meck:expect(flurm_dbd_ra, get_tres_usage,
                fun(user, <<"u">>, <<"2026-01">>) -> {ok, #{cpu_seconds => 1}};
                   (_Type, _Id, _Period) -> {error, unexpected}
                end),
    ?assertEqual({ok, #{cpu_seconds => 1}},
                 flurm_dbd_query:query_tres_usage(user, <<"u">>, <<"2026-01">>)),
    stop_ra_placeholder(Pid).

query_jobs_ra_branch_test() ->
    Pid = start_ra_placeholder(),
    meck:expect(flurm_dbd_ra, query_jobs,
                fun(#{user_name := <<"alice">>}) -> {ok, [#{job_id => 42}]};
                   (_Filters) -> {ok, []}
                end),
    ?assertEqual({ok, [#{job_id => 42}]},
                 flurm_dbd_query:query_jobs(#{user_name => <<"alice">>})),
    stop_ra_placeholder(Pid).

format_tres_string_remaining_parts_test() ->
    Tres = #{
        mem_seconds => 2048,
        node_seconds => 2,
        gpu_seconds => 3
    },
    Bin = flurm_dbd_query:format_tres_string(Tres),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"mem=">>)),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"node=2">>)),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"gres/gpu=3">>)).

aggregate_user_tres_skips_errors_test() ->
    Pid = start_ra_placeholder(),
    meck:expect(flurm_dbd_ra, get_tres_usage,
                fun(user, <<"user1">>, <<"bad">>) -> {error, missing};
                   (user, <<"user1">>, <<"good">>) -> {ok, #{cpu_seconds => 7}};
                   (_Type, _Id, _Period) -> {error, unexpected}
                end),
    Aggregated = flurm_dbd_query:aggregate_user_tres(<<"user1">>, [<<"bad">>, <<"good">>]),
    ?assertEqual(7, maps:get(cpu_seconds, Aggregated)),
    stop_ra_placeholder(Pid).

start_ra_placeholder() ->
    maybe_unregister_ra(),
    Pid = spawn(fun ra_placeholder_loop/0),
    true = register(flurm_dbd_ra, Pid),
    Pid.

stop_ra_placeholder(Pid) ->
    catch unregister(flurm_dbd_ra),
    Pid ! stop,
    ok.

ra_placeholder_loop() ->
    receive
        stop -> ok
    end.

maybe_unregister_ra() ->
    case whereis(flurm_dbd_ra) of
        undefined ->
            ok;
        Pid ->
            catch unregister(flurm_dbd_ra),
            exit(Pid, kill),
            timer:sleep(5),
            ok
    end.

