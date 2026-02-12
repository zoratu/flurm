%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd_query (436 lines).
%%%
%%% Tests all internal formatting functions directly and mocks
%%% flurm_dbd_server / flurm_dbd_ra for query-path functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_query_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Format / pure function tests (no mocking needed beyond lager)
%%====================================================================

format_test_() ->
    {setup,
     fun setup_lager/0,
     fun cleanup_lager/1,
     [
      {"format_tres_string empty map",              fun test_tres_string_empty/0},
      {"format_tres_string with cpu",               fun test_tres_string_cpu/0},
      {"format_tres_string with cpu_seconds",       fun test_tres_string_cpu_seconds/0},
      {"format_tres_string with mem",               fun test_tres_string_mem/0},
      {"format_tres_string with mem_seconds",       fun test_tres_string_mem_seconds/0},
      {"format_tres_string with node",              fun test_tres_string_node/0},
      {"format_tres_string with node_seconds",      fun test_tres_string_node_seconds/0},
      {"format_tres_string with gpu",               fun test_tres_string_gpu/0},
      {"format_tres_string with gpu_seconds",       fun test_tres_string_gpu_seconds/0},
      {"format_tres_string with billing",           fun test_tres_string_billing/0},
      {"format_tres_string with energy",            fun test_tres_string_energy/0},
      {"format_tres_string zero values",            fun test_tres_string_zero_values/0},
      {"format_tres_string unknown key",            fun test_tres_string_unknown_key/0},
      {"format_memory bytes",                       fun test_format_memory_bytes/0},
      {"format_memory KB",                          fun test_format_memory_kb/0},
      {"format_memory MB",                          fun test_format_memory_mb/0},
      {"format_memory GB",                          fun test_format_memory_gb/0},
      {"format_memory TB",                          fun test_format_memory_tb/0},
      {"format_sacct_output",                       fun test_format_sacct_output/0},
      {"format_sacct_row",                          fun test_format_sacct_row/0},
      {"format_sacct_row defaults",                 fun test_format_sacct_row_defaults/0},
      {"format_elapsed 0",                          fun test_format_elapsed_0/0},
      {"format_elapsed 59s",                        fun test_format_elapsed_59/0},
      {"format_elapsed 3600",                       fun test_format_elapsed_3600/0},
      {"format_elapsed 86400+",                     fun test_format_elapsed_day/0},
      {"format_elapsed invalid",                    fun test_format_elapsed_invalid/0},
      {"format_exit_code 0",                        fun test_format_exit_code_0/0},
      {"format_exit_code 127",                      fun test_format_exit_code_127/0},
      {"format_exit_code non-int",                  fun test_format_exit_code_non_int/0}
     ]}.

setup_lager() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),
    ok.

cleanup_lager(_) ->
    meck:unload(lager),
    ok.

test_tres_string_empty() ->
    ?assertEqual(<<>>, flurm_dbd_query:format_tres_string(#{})).

test_tres_string_cpu() ->
    R = flurm_dbd_query:format_tres_string(#{cpu => 4}),
    ?assert(is_binary(R)),
    ?assertMatch({match, _}, re:run(R, <<"cpu=4">>)).

test_tres_string_cpu_seconds() ->
    R = flurm_dbd_query:format_tres_string(#{cpu_seconds => 100}),
    ?assertMatch({match, _}, re:run(R, <<"cpu=100">>)).

test_tres_string_mem() ->
    R = flurm_dbd_query:format_tres_string(#{mem => 500}),
    ?assertMatch({match, _}, re:run(R, <<"mem=">>)).

test_tres_string_mem_seconds() ->
    R = flurm_dbd_query:format_tres_string(#{mem_seconds => 500}),
    ?assertMatch({match, _}, re:run(R, <<"mem=">>)).

test_tres_string_node() ->
    R = flurm_dbd_query:format_tres_string(#{node => 2}),
    ?assertMatch({match, _}, re:run(R, <<"node=2">>)).

test_tres_string_node_seconds() ->
    R = flurm_dbd_query:format_tres_string(#{node_seconds => 20}),
    ?assertMatch({match, _}, re:run(R, <<"node=20">>)).

test_tres_string_gpu() ->
    R = flurm_dbd_query:format_tres_string(#{gpu => 2}),
    ?assertMatch({match, _}, re:run(R, <<"gres/gpu=2">>)).

test_tres_string_gpu_seconds() ->
    R = flurm_dbd_query:format_tres_string(#{gpu_seconds => 10}),
    ?assertMatch({match, _}, re:run(R, <<"gres/gpu=10">>)).

test_tres_string_billing() ->
    R = flurm_dbd_query:format_tres_string(#{billing => 8}),
    ?assertMatch({match, _}, re:run(R, <<"billing=8">>)).

test_tres_string_energy() ->
    R = flurm_dbd_query:format_tres_string(#{energy => 1000}),
    ?assertMatch({match, _}, re:run(R, <<"energy=1000">>)).

test_tres_string_zero_values() ->
    %% Zero values should be filtered out
    ?assertEqual(<<>>, flurm_dbd_query:format_tres_string(#{cpu => 0, mem => 0})).

test_tres_string_unknown_key() ->
    %% Unknown keys should be filtered out
    ?assertEqual(<<>>, flurm_dbd_query:format_tres_string(#{unknown => 5})).

test_format_memory_bytes() ->
    %% Less than 1024 => K suffix
    R = flurm_dbd_query:format_tres_string(#{mem => 500}),
    ?assertMatch({match, _}, re:run(R, <<"mem=500K">>)).

test_format_memory_kb() ->
    %% 1024 <= v < 1048576 => M suffix
    R = flurm_dbd_query:format_tres_string(#{mem => 2048}),
    ?assertMatch({match, _}, re:run(R, <<"mem=.*M">>)).

test_format_memory_mb() ->
    %% 1048576 <= v < 1073741824 => G suffix
    R = flurm_dbd_query:format_tres_string(#{mem => 2097152}),
    ?assertMatch({match, _}, re:run(R, <<"mem=.*G">>)).

test_format_memory_gb() ->
    R = flurm_dbd_query:format_tres_string(#{mem => 1048576}),
    ?assertMatch({match, _}, re:run(R, <<"mem=.*G">>)).

test_format_memory_tb() ->
    %% >= 1073741824 => T suffix
    R = flurm_dbd_query:format_tres_string(#{mem => 2147483648}),
    ?assertMatch({match, _}, re:run(R, <<"mem=.*T">>)).

test_format_sacct_output() ->
    Jobs = [
        #{job_id => 1, user_name => <<"u">>, account => <<"a">>,
          partition => <<"p">>, state => completed, elapsed => 60,
          exit_code => 0, tres_alloc => #{}}
    ],
    Result = flurm_dbd_query:format_sacct_output(Jobs),
    Flat = lists:flatten(Result),
    ?assert(length(Flat) > 0),
    ?assertMatch({match, _}, re:run(Flat, "JobID")).

test_format_sacct_row() ->
    Job = #{job_id => 42, user_name => <<"alice">>, account => <<"acct">>,
            partition => <<"batch">>, state => running, elapsed => 120,
            exit_code => 0, tres_alloc => #{cpu => 4}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "42")).

test_format_sacct_row_defaults() ->
    %% Test with minimal map (all defaults)
    Job = #{},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assert(length(Row) > 0).

test_format_elapsed_0() ->
    Job = #{job_id => 1, elapsed => 0, exit_code => 0, state => completed,
            user_name => <<>>, account => <<>>, partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "00:00:00")).

test_format_elapsed_59() ->
    %% Indirectly tested via sacct_output
    Job = #{job_id => 1, elapsed => 59, exit_code => 0, state => completed,
            user_name => <<>>, account => <<>>, partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "00:00:59")).

test_format_elapsed_3600() ->
    Job = #{job_id => 1, elapsed => 3600, exit_code => 0, state => completed,
            user_name => <<>>, account => <<>>, partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "01:00:00")).

test_format_elapsed_day() ->
    Job = #{job_id => 1, elapsed => 90061, exit_code => 0, state => completed,
            user_name => <<>>, account => <<>>, partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "25:01:01")).

test_format_elapsed_invalid() ->
    Job = #{job_id => 1, elapsed => not_a_number, exit_code => 0,
            state => completed, user_name => <<>>, account => <<>>,
            partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "00:00:00")).

test_format_exit_code_0() ->
    Job = #{job_id => 1, elapsed => 0, exit_code => 0,
            state => completed, user_name => <<>>, account => <<>>,
            partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "0:0")).

test_format_exit_code_127() ->
    Job = #{job_id => 1, elapsed => 0, exit_code => 127,
            state => failed, user_name => <<>>, account => <<>>,
            partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "127:0")).

test_format_exit_code_non_int() ->
    Job = #{job_id => 1, elapsed => 0, exit_code => not_an_int,
            state => completed, user_name => <<>>, account => <<>>,
            partition => <<>>, tres_alloc => #{}},
    Row = lists:flatten(flurm_dbd_query:format_sacct_output([Job])),
    ?assertMatch({match, _}, re:run(Row, "0:0")).

%%====================================================================
%% Query function tests (mock flurm_dbd_server and flurm_dbd_ra)
%%====================================================================

query_test_() ->
    {foreach,
     fun setup_query/0,
     fun cleanup_query/1,
     [
      {"query_jobs via server fallback",       fun test_query_jobs_server/0},
      {"query_jobs via Ra",                    fun test_query_jobs_ra/0},
      {"query_jobs_by_user",                   fun test_query_jobs_by_user/0},
      {"query_jobs_by_account",                fun test_query_jobs_by_account/0},
      {"query_jobs_in_range",                  fun test_query_jobs_in_range/0},
      {"jobs_by_user/3",                       fun test_jobs_by_user_3/0},
      {"jobs_by_user/4 with pagination",       fun test_jobs_by_user_4/0},
      {"jobs_by_account/3",                    fun test_jobs_by_account_3/0},
      {"jobs_by_account/4 with pagination",    fun test_jobs_by_account_4/0},
      {"query_jobs_paginated sort asc",        fun test_paginated_sort_asc/0},
      {"query_jobs_paginated sort desc",       fun test_paginated_sort_desc/0},
      {"query_jobs_paginated offset/limit",    fun test_paginated_offset_limit/0},
      {"query_jobs_paginated error pass-through", fun test_paginated_error/0},
      {"tres_usage_by_user",                   fun test_tres_usage_by_user/0},
      {"tres_usage_by_account",                fun test_tres_usage_by_account/0},
      {"query_tres_usage cluster",             fun test_query_tres_cluster/0},
      {"query_tres_usage via Ra",              fun test_query_tres_ra/0},
      {"query_tres_usage error",               fun test_query_tres_error/0},
      {"aggregate_user_tres",                  fun test_aggregate_user_tres/0},
      {"aggregate_account_tres",               fun test_aggregate_account_tres/0},
      {"aggregate with errors",                fun test_aggregate_with_errors/0}
     ]}.

setup_query() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    Now = erlang:system_time(second),
    Jobs = [
        #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>,
          submit_time => Now - 100, start_time => Now - 90,
          end_time => Now - 50, state => completed, elapsed => 40},
        #{job_id => 2, user_name => <<"bob">>, account => <<"acct">>,
          submit_time => Now - 200, start_time => Now - 190,
          end_time => Now - 150, state => completed, elapsed => 40},
        #{job_id => 3, user_name => <<"alice">>, account => <<"other">>,
          submit_time => Now - 300, start_time => Now - 290,
          end_time => Now - 250, state => failed, elapsed => 40}
    ],

    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> Jobs end),
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_) ->
        #{cpu_seconds => 100, mem_seconds => 50, gpu_seconds => 0,
          node_seconds => 10, job_count => 2, job_time => 80}
    end),
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(_) ->
        #{cpu_seconds => 200, mem_seconds => 100}
    end),
    meck:expect(flurm_dbd_server, calculate_cluster_tres_usage, fun() ->
        #{cpu_seconds => 500}
    end),

    %% Default: no Ra process registered
    ok.

cleanup_query(_) ->
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(lager),
    %% Clean up flurm_dbd_ra mock if it was loaded
    catch meck:unload(flurm_dbd_ra),
    ok.

test_query_jobs_server() ->
    %% Ra not registered, should fall back to server
    {ok, Jobs} = flurm_dbd_query:query_jobs(#{}),
    ?assert(length(Jobs) >= 0).

test_query_jobs_ra() ->
    %% Register a fake Ra process and mock its query
    meck:new(flurm_dbd_ra, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra, query_jobs, fun(_) -> {ok, [#{job_id => 99}]} end),
    register(flurm_dbd_ra, self()),
    try
        {ok, Jobs} = flurm_dbd_query:query_jobs(#{}),
        ?assertEqual(1, length(Jobs)),
        ?assertEqual(99, maps:get(job_id, hd(Jobs)))
    after
        unregister(flurm_dbd_ra),
        meck:unload(flurm_dbd_ra)
    end.

test_query_jobs_by_user() ->
    {ok, Jobs} = flurm_dbd_query:query_jobs_by_user(<<"alice">>, 0, erlang:system_time(second) + 86400),
    ?assert(is_list(Jobs)).

test_query_jobs_by_account() ->
    {ok, Jobs} = flurm_dbd_query:query_jobs_by_account(<<"acct">>, 0, erlang:system_time(second) + 86400),
    ?assert(is_list(Jobs)).

test_query_jobs_in_range() ->
    Now = erlang:system_time(second),
    {ok, Jobs} = flurm_dbd_query:query_jobs_in_range(Now - 1000, Now + 1000),
    ?assert(is_list(Jobs)).

test_jobs_by_user_3() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_user(<<"alice">>, 0, erlang:system_time(second) + 86400),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)).

test_jobs_by_user_4() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_user(
        <<"alice">>, 0, erlang:system_time(second) + 86400,
        #{limit => 1, offset => 0, sort_by => submit_time, sort_order => desc}),
    ?assert(length(Jobs) =< 1),
    ?assert(is_map(Pagination)),
    ?assert(maps:is_key(total, Pagination)),
    ?assert(maps:is_key(has_more, Pagination)).

test_jobs_by_account_3() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_account(<<"acct">>, 0, erlang:system_time(second) + 86400),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)).

test_jobs_by_account_4() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_account(
        <<"acct">>, 0, erlang:system_time(second) + 86400,
        #{limit => 10, offset => 0}),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)).

test_paginated_sort_asc() ->
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(
        #{}, #{sort_by => job_id, sort_order => asc}),
    case length(Jobs) >= 2 of
        true ->
            [First, Second | _] = Jobs,
            ?assert(maps:get(job_id, First, 0) =< maps:get(job_id, Second, 0));
        false -> ok
    end.

test_paginated_sort_desc() ->
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(
        #{}, #{sort_by => job_id, sort_order => desc}),
    case length(Jobs) >= 2 of
        true ->
            [First, Second | _] = Jobs,
            ?assert(maps:get(job_id, First, 0) >= maps:get(job_id, Second, 0));
        false -> ok
    end.

test_paginated_offset_limit() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(
        #{}, #{limit => 1, offset => 1}),
    ?assert(length(Jobs) =< 1),
    ?assertEqual(1, maps:get(offset, Pagination)).

test_paginated_error() ->
    %% Make server throw
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> error(boom) end),
    Result = flurm_dbd_query:query_jobs_paginated(#{}, #{}),
    ?assertMatch({error, _}, Result).

test_tres_usage_by_user() ->
    {ok, Usage} = flurm_dbd_query:tres_usage_by_user(<<"alice">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

test_tres_usage_by_account() ->
    {ok, Usage} = flurm_dbd_query:tres_usage_by_account(<<"acct">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

test_query_tres_cluster() ->
    {ok, Usage} = flurm_dbd_query:query_tres_usage(cluster, <<"flurm">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

test_query_tres_ra() ->
    meck:new(flurm_dbd_ra, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra, get_tres_usage, fun(_, _, _) -> {ok, #{cpu_seconds => 99}} end),
    register(flurm_dbd_ra, self()),
    try
        {ok, Usage} = flurm_dbd_query:query_tres_usage(user, <<"alice">>, <<"2026-02">>),
        ?assertEqual(99, maps:get(cpu_seconds, Usage))
    after
        unregister(flurm_dbd_ra),
        meck:unload(flurm_dbd_ra)
    end.

test_query_tres_error() ->
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_) -> error(boom) end),
    Result = flurm_dbd_query:query_tres_usage(user, <<"alice">>, <<"2026-02">>),
    ?assertMatch({error, _}, Result).

test_aggregate_user_tres() ->
    Result = flurm_dbd_query:aggregate_user_tres(<<"alice">>, [<<"2026-01">>, <<"2026-02">>]),
    ?assert(is_map(Result)),
    ?assert(maps:get(cpu_seconds, Result, 0) >= 0).

test_aggregate_account_tres() ->
    Result = flurm_dbd_query:aggregate_account_tres(<<"acct">>, [<<"2026-01">>]),
    ?assert(is_map(Result)).

test_aggregate_with_errors() ->
    %% Make one period fail
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(<<"alice">>) ->
        error(boom)
    end),
    Result = flurm_dbd_query:aggregate_user_tres(<<"alice">>, [<<"2026-01">>]),
    ?assert(is_map(Result)).
