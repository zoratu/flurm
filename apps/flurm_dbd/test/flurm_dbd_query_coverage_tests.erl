%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_query module.
%%%
%%% Tests the query engine for accounting data.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_query_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

query_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% jobs_by_user
      {"jobs_by_user basic", fun test_jobs_by_user/0},
      {"jobs_by_user with options", fun test_jobs_by_user_opts/0},

      %% jobs_by_account
      {"jobs_by_account basic", fun test_jobs_by_account/0},
      {"jobs_by_account with options", fun test_jobs_by_account_opts/0},

      %% tres_usage_by_user
      {"tres_usage_by_user", fun test_tres_usage_by_user/0},

      %% tres_usage_by_account
      {"tres_usage_by_account", fun test_tres_usage_by_account/0},

      %% query_jobs_paginated
      {"query_jobs_paginated basic", fun test_query_paginated/0},
      {"query_jobs_paginated with limit", fun test_query_paginated_limit/0},
      {"query_jobs_paginated with offset", fun test_query_paginated_offset/0},
      {"query_jobs_paginated sort asc", fun test_query_paginated_sort_asc/0},
      {"query_jobs_paginated sort desc", fun test_query_paginated_sort_desc/0},
      {"query_jobs_paginated has_more", fun test_query_paginated_has_more/0},

      %% query_jobs_by_user (backward compat)
      {"query_jobs_by_user", fun test_query_jobs_by_user/0},

      %% query_jobs_by_account (backward compat)
      {"query_jobs_by_account", fun test_query_jobs_by_account/0},

      %% query_tres_usage
      {"query_tres_usage user", fun test_query_tres_user/0},
      {"query_tres_usage account", fun test_query_tres_account/0},
      {"query_tres_usage cluster", fun test_query_tres_cluster/0},

      %% format_tres_string
      {"format_tres_string empty", fun test_format_tres_empty/0},
      {"format_tres_string cpu", fun test_format_tres_cpu/0},
      {"format_tres_string mem bytes", fun test_format_tres_mem_bytes/0},
      {"format_tres_string mem kb", fun test_format_tres_mem_kb/0},
      {"format_tres_string mem mb", fun test_format_tres_mem_mb/0},
      {"format_tres_string mem gb", fun test_format_tres_mem_gb/0},
      {"format_tres_string mem tb", fun test_format_tres_mem_tb/0},
      {"format_tres_string node", fun test_format_tres_node/0},
      {"format_tres_string gpu", fun test_format_tres_gpu/0},
      {"format_tres_string billing", fun test_format_tres_billing/0},
      {"format_tres_string energy", fun test_format_tres_energy/0},
      {"format_tres_string mixed", fun test_format_tres_mixed/0},
      {"format_tres_string zero values", fun test_format_tres_zeros/0},

      %% query_jobs
      {"query_jobs basic", fun test_query_jobs/0},
      {"query_jobs with filters", fun test_query_jobs_filters/0},

      %% query_jobs_in_range
      {"query_jobs_in_range", fun test_query_jobs_in_range/0},

      %% aggregate_user_tres
      {"aggregate_user_tres", fun test_aggregate_user_tres/0},

      %% aggregate_account_tres
      {"aggregate_account_tres", fun test_aggregate_account_tres/0},

      %% format_sacct_output
      {"format_sacct_output empty", fun test_format_sacct_empty/0},
      {"format_sacct_output jobs", fun test_format_sacct_jobs/0}
     ]}.

setup() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_ra),
    meck:new(flurm_dbd_ra, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra, query_jobs, fun(_) -> {ok, sample_jobs()} end),
    meck:expect(flurm_dbd_ra, get_tres_usage, fun(_, _, _) -> {ok, sample_tres()} end),

    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, list_job_records, fun() -> sample_jobs() end),
    meck:expect(flurm_dbd_server, list_job_records, fun(_) -> sample_jobs() end),
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_) -> sample_tres() end),
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(_) -> sample_tres() end),
    meck:expect(flurm_dbd_server, calculate_cluster_tres_usage, fun() -> sample_tres() end),

    %% Ensure flurm_dbd_ra is "running" (registered)
    register(flurm_dbd_ra, self()),
    ok.

cleanup(_) ->
    catch unregister(flurm_dbd_ra),
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(lager),
    ok.

sample_jobs() ->
    Now = erlang:system_time(second),
    [
        #{job_id => 1, user_name => <<"alice">>, account => <<"research">>,
          partition => <<"batch">>, state => completed, submit_time => Now - 1000,
          start_time => Now - 900, end_time => Now - 100, elapsed => 800,
          exit_code => 0, tres_alloc => #{cpu => 4}},
        #{job_id => 2, user_name => <<"bob">>, account => <<"research">>,
          partition => <<"gpu">>, state => running, submit_time => Now - 500,
          start_time => Now - 400, end_time => Now, elapsed => 400,
          exit_code => 0, tres_alloc => #{cpu => 8, gpu => 1}},
        #{job_id => 3, user_name => <<"alice">>, account => <<"physics">>,
          partition => <<"batch">>, state => failed, submit_time => Now - 200,
          start_time => Now - 150, end_time => Now - 50, elapsed => 100,
          exit_code => 1, tres_alloc => #{cpu => 2}}
    ].

sample_tres() ->
    #{
        cpu_seconds => 3600,
        mem_seconds => 1024000,
        gpu_seconds => 100,
        node_seconds => 400,
        job_count => 5,
        job_time => 1800
    }.

%% === jobs_by_user ===

test_jobs_by_user() ->
    Now = erlang:system_time(second),
    {ok, Jobs, _Pagination} = flurm_dbd_query:jobs_by_user(<<"alice">>, Now - 10000, Now),
    ?assert(is_list(Jobs)).

test_jobs_by_user_opts() ->
    Now = erlang:system_time(second),
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_user(<<"alice">>, Now - 10000, Now, #{limit => 10}),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)),
    ?assert(maps:is_key(total, Pagination)).

%% === jobs_by_account ===

test_jobs_by_account() ->
    Now = erlang:system_time(second),
    {ok, Jobs, _Pagination} = flurm_dbd_query:jobs_by_account(<<"research">>, Now - 10000, Now),
    ?assert(is_list(Jobs)).

test_jobs_by_account_opts() ->
    Now = erlang:system_time(second),
    {ok, Jobs, Pagination} = flurm_dbd_query:jobs_by_account(<<"research">>, Now - 10000, Now, #{offset => 0}),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)).

%% === tres_usage_by_user ===

test_tres_usage_by_user() ->
    {ok, Usage} = flurm_dbd_query:tres_usage_by_user(<<"alice">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

%% === tres_usage_by_account ===

test_tres_usage_by_account() ->
    {ok, Usage} = flurm_dbd_query:tres_usage_by_account(<<"research">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

%% === query_jobs_paginated ===

test_query_paginated() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(#{}, #{}),
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)),
    ?assert(maps:is_key(total, Pagination)),
    ?assert(maps:is_key(returned, Pagination)).

test_query_paginated_limit() ->
    {ok, Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(#{}, #{limit => 2}),
    ?assert(length(Jobs) =< 2),
    ?assertEqual(2, maps:get(limit, Pagination)).

test_query_paginated_offset() ->
    {ok, _Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(#{}, #{offset => 1}),
    ?assertEqual(1, maps:get(offset, Pagination)).

test_query_paginated_sort_asc() ->
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(#{}, #{sort_by => job_id, sort_order => asc}),
    case length(Jobs) >= 2 of
        true ->
            [First, Second | _] = Jobs,
            ?assert(maps:get(job_id, First) =< maps:get(job_id, Second));
        false ->
            ok
    end.

test_query_paginated_sort_desc() ->
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(#{}, #{sort_by => job_id, sort_order => desc}),
    case length(Jobs) >= 2 of
        true ->
            [First, Second | _] = Jobs,
            ?assert(maps:get(job_id, First) >= maps:get(job_id, Second));
        false ->
            ok
    end.

test_query_paginated_has_more() ->
    {ok, _Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(#{}, #{limit => 1}),
    ?assert(is_boolean(maps:get(has_more, Pagination))).

%% === query_jobs_by_user ===

test_query_jobs_by_user() ->
    Now = erlang:system_time(second),
    {ok, Jobs} = flurm_dbd_query:query_jobs_by_user(<<"alice">>, Now - 10000, Now),
    ?assert(is_list(Jobs)).

%% === query_jobs_by_account ===

test_query_jobs_by_account() ->
    Now = erlang:system_time(second),
    {ok, Jobs} = flurm_dbd_query:query_jobs_by_account(<<"research">>, Now - 10000, Now),
    ?assert(is_list(Jobs)).

%% === query_tres_usage ===

test_query_tres_user() ->
    {ok, Usage} = flurm_dbd_query:query_tres_usage(user, <<"alice">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

test_query_tres_account() ->
    {ok, Usage} = flurm_dbd_query:query_tres_usage(account, <<"research">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

test_query_tres_cluster() ->
    meck:expect(flurm_dbd_ra, get_tres_usage, fun(cluster, _, _) -> {ok, sample_tres()} end),
    {ok, Usage} = flurm_dbd_query:query_tres_usage(cluster, <<"cluster1">>, <<"2026-02">>),
    ?assert(is_map(Usage)).

%% === format_tres_string ===

test_format_tres_empty() ->
    ?assertEqual(<<>>, flurm_dbd_query:format_tres_string(#{})).

test_format_tres_cpu() ->
    R = flurm_dbd_query:format_tres_string(#{cpu => 4}),
    ?assertEqual(<<"cpu=4">>, R).

test_format_tres_cpu_seconds() ->
    R = flurm_dbd_query:format_tres_string(#{cpu_seconds => 100}),
    ?assertEqual(<<"cpu=100">>, R).

test_format_tres_mem_bytes() ->
    R = flurm_dbd_query:format_tres_string(#{mem => 512}),
    ?assert(is_binary(R)),
    ?assertMatch({match, _}, re:run(R, "mem")).

test_format_tres_mem_kb() ->
    R = flurm_dbd_query:format_tres_string(#{mem => 2048}),
    ?assertMatch({match, _}, re:run(R, "mem")).

test_format_tres_mem_mb() ->
    R = flurm_dbd_query:format_tres_string(#{mem => 2097152}),
    ?assertMatch({match, _}, re:run(R, "mem")).

test_format_tres_mem_gb() ->
    R = flurm_dbd_query:format_tres_string(#{mem_seconds => 1073741824}),
    ?assertMatch({match, _}, re:run(R, "mem")).

test_format_tres_mem_tb() ->
    R = flurm_dbd_query:format_tres_string(#{mem_seconds => 2199023255552}),
    ?assertMatch({match, _}, re:run(R, "mem")).

test_format_tres_node() ->
    R = flurm_dbd_query:format_tres_string(#{node => 4}),
    ?assertEqual(<<"node=4">>, R).

test_format_tres_gpu() ->
    R = flurm_dbd_query:format_tres_string(#{gpu => 2}),
    ?assertEqual(<<"gres/gpu=2">>, R).

test_format_tres_billing() ->
    R = flurm_dbd_query:format_tres_string(#{billing => 100}),
    ?assertEqual(<<"billing=100">>, R).

test_format_tres_energy() ->
    R = flurm_dbd_query:format_tres_string(#{energy => 500}),
    ?assertEqual(<<"energy=500">>, R).

test_format_tres_mixed() ->
    R = flurm_dbd_query:format_tres_string(#{cpu => 4, node => 2, gpu => 1}),
    ?assert(is_binary(R)),
    ?assertMatch({match, _}, re:run(R, "cpu=4")),
    ?assertMatch({match, _}, re:run(R, "node=2")),
    ?assertMatch({match, _}, re:run(R, "gres/gpu=1")).

test_format_tres_zeros() ->
    R = flurm_dbd_query:format_tres_string(#{cpu => 0, node => 0}),
    ?assertEqual(<<>>, R).

%% === query_jobs ===

test_query_jobs() ->
    {ok, Jobs} = flurm_dbd_query:query_jobs(#{}),
    ?assert(is_list(Jobs)).

test_query_jobs_filters() ->
    {ok, Jobs} = flurm_dbd_query:query_jobs(#{user_name => <<"alice">>}),
    ?assert(is_list(Jobs)).

%% === query_jobs_in_range ===

test_query_jobs_in_range() ->
    Now = erlang:system_time(second),
    {ok, Jobs} = flurm_dbd_query:query_jobs_in_range(Now - 10000, Now),
    ?assert(is_list(Jobs)).

%% === aggregate_user_tres ===

test_aggregate_user_tres() ->
    Periods = [<<"2026-01">>, <<"2026-02">>],
    Result = flurm_dbd_query:aggregate_user_tres(<<"alice">>, Periods),
    ?assert(is_map(Result)),
    ?assert(maps:get(cpu_seconds, Result) >= 0).

%% === aggregate_account_tres ===

test_aggregate_account_tres() ->
    Periods = [<<"2026-01">>, <<"2026-02">>],
    Result = flurm_dbd_query:aggregate_account_tres(<<"research">>, Periods),
    ?assert(is_map(Result)).

%% === format_sacct_output ===

test_format_sacct_empty() ->
    Output = flurm_dbd_query:format_sacct_output([]),
    ?assert(is_list(Output)).

test_format_sacct_jobs() ->
    Jobs = sample_jobs(),
    Output = flurm_dbd_query:format_sacct_output(Jobs),
    ?assert(is_list(Output)),
    Flat = lists:flatten(Output),
    ?assertMatch({match, _}, re:run(Flat, "JobID")).
