%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - Query Engine Tests
%%%
%%% Unit tests for the flurm_dbd_query module which provides query
%%% functionality for accounting data.
%%%
%%% Tests cover:
%%% - Query building functions (jobs_by_user, jobs_by_account)
%%% - Time range filtering
%%% - User/account filtering
%%% - TRES aggregation queries
%%% - Result formatting (sacct output, TRES strings)
%%% - Pagination support
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_query_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Mock flurm_dbd_ra (not running in tests)
    case whereis(flurm_dbd_ra) of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
    %% Mock flurm_dbd_server
    meck:new(flurm_dbd_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        sample_jobs()
    end),
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_User) ->
        sample_tres_usage()
    end),
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(_Account) ->
        sample_tres_usage()
    end),
    meck:expect(flurm_dbd_server, calculate_cluster_tres_usage, fun() ->
        sample_tres_usage()
    end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_server),
    ok.

%%====================================================================
%% Jobs by User Tests
%%====================================================================

jobs_by_user_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"jobs_by_user returns jobs for user", fun test_jobs_by_user_basic/0},
             {"jobs_by_user with time range filtering", fun test_jobs_by_user_time_range/0},
             {"jobs_by_user with empty result", fun test_jobs_by_user_empty/0},
             {"jobs_by_user/4 with pagination", fun test_jobs_by_user_paginated/0}
         ]
     end
    }.

test_jobs_by_user_basic() ->
    Now = erlang:system_time(second),
    StartTime = Now - 86400,  % 24 hours ago
    EndTime = Now,

    Result = flurm_dbd_query:jobs_by_user(<<"testuser">>, StartTime, EndTime),
    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, Pagination} = Result,
    ?assert(is_list(Jobs)),
    ?assert(is_map(Pagination)),
    ?assert(maps:is_key(total, Pagination)).

test_jobs_by_user_time_range() ->
    %% Test time range that should filter out some jobs
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [
            #{job_id => 1, user_name => <<"testuser">>, start_time => 1000, end_time => 2000},
            #{job_id => 2, user_name => <<"testuser">>, start_time => 5000, end_time => 6000},
            #{job_id => 3, user_name => <<"testuser">>, start_time => 10000, end_time => 11000}
        ]
    end),

    %% Query for jobs between 4000 and 7000 - should only get job 2
    Result = flurm_dbd_query:jobs_by_user(<<"testuser">>, 4000, 7000),
    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, _} = Result,
    ?assertEqual(1, length(Jobs)),
    [Job] = Jobs,
    ?assertEqual(2, maps:get(job_id, Job)).

test_jobs_by_user_empty() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) -> [] end),

    Now = erlang:system_time(second),
    Result = flurm_dbd_query:jobs_by_user(<<"nonexistent">>, Now - 3600, Now),
    ?assertMatch({ok, [], _}, Result),
    {ok, Jobs, Pagination} = Result,
    ?assertEqual([], Jobs),
    ?assertEqual(0, maps:get(total, Pagination)).

test_jobs_by_user_paginated() ->
    %% Create 10 jobs
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [#{job_id => N, user_name => <<"testuser">>, start_time => N * 1000, end_time => N * 1000 + 500, submit_time => N * 1000}
         || N <- lists:seq(1, 10)]
    end),

    Now = erlang:system_time(second),
    Options = #{limit => 3, offset => 2, sort_by => submit_time, sort_order => asc},
    Result = flurm_dbd_query:jobs_by_user(<<"testuser">>, 0, Now + 100000, Options),

    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, Pagination} = Result,

    %% Should get 3 jobs starting from offset 2
    ?assertEqual(3, length(Jobs)),
    ?assertEqual(10, maps:get(total, Pagination)),
    ?assertEqual(3, maps:get(limit, Pagination)),
    ?assertEqual(2, maps:get(offset, Pagination)),
    ?assertEqual(true, maps:get(has_more, Pagination)).

%%====================================================================
%% Jobs by Account Tests
%%====================================================================

jobs_by_account_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"jobs_by_account returns jobs for account", fun test_jobs_by_account_basic/0},
             {"jobs_by_account with time range", fun test_jobs_by_account_time_range/0},
             {"jobs_by_account/4 with pagination", fun test_jobs_by_account_paginated/0}
         ]
     end
    }.

test_jobs_by_account_basic() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [
            #{job_id => 1, account => <<"research">>, start_time => 1000, end_time => 2000},
            #{job_id => 2, account => <<"research">>, start_time => 3000, end_time => 4000}
        ]
    end),

    Now = erlang:system_time(second),
    Result = flurm_dbd_query:jobs_by_account(<<"research">>, 0, Now + 10000),
    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, _} = Result,
    ?assertEqual(2, length(Jobs)).

test_jobs_by_account_time_range() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [
            #{job_id => 1, account => <<"research">>, start_time => 1000, end_time => 2000},
            #{job_id => 2, account => <<"research">>, start_time => 5000, end_time => 6000}
        ]
    end),

    %% Query for jobs between 4000 and 7000
    Result = flurm_dbd_query:jobs_by_account(<<"research">>, 4000, 7000),
    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, _} = Result,
    ?assertEqual(1, length(Jobs)).

test_jobs_by_account_paginated() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [#{job_id => N, account => <<"research">>, start_time => N * 100, end_time => N * 100 + 50, submit_time => N}
         || N <- lists:seq(1, 20)]
    end),

    Now = erlang:system_time(second),
    Options = #{limit => 5, offset => 0},
    Result = flurm_dbd_query:jobs_by_account(<<"research">>, 0, Now + 10000, Options),

    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, Pagination} = Result,
    ?assertEqual(5, length(Jobs)),
    ?assertEqual(20, maps:get(total, Pagination)),
    ?assertEqual(true, maps:get(has_more, Pagination)).

%%====================================================================
%% TRES Usage Tests
%%====================================================================

tres_usage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"tres_usage_by_user returns usage map", fun test_tres_usage_by_user/0},
             {"tres_usage_by_account returns usage map", fun test_tres_usage_by_account/0},
             {"query_tres_usage for cluster", fun test_tres_usage_cluster/0}
         ]
     end
    }.

test_tres_usage_by_user() ->
    ExpectedUsage = #{cpu_seconds => 3600, mem_seconds => 8192000, gpu_seconds => 0},
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(User) ->
        ?assertEqual(<<"alice">>, User),
        ExpectedUsage
    end),

    Result = flurm_dbd_query:tres_usage_by_user(<<"alice">>, <<"2026-02">>),
    ?assertMatch({ok, _}, Result),
    {ok, Usage} = Result,
    ?assertEqual(3600, maps:get(cpu_seconds, Usage)).

test_tres_usage_by_account() ->
    ExpectedUsage = #{cpu_seconds => 7200, mem_seconds => 16384000},
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(Account) ->
        ?assertEqual(<<"physics">>, Account),
        ExpectedUsage
    end),

    Result = flurm_dbd_query:tres_usage_by_account(<<"physics">>, <<"2026-01">>),
    ?assertMatch({ok, _}, Result),
    {ok, Usage} = Result,
    ?assertEqual(7200, maps:get(cpu_seconds, Usage)).

test_tres_usage_cluster() ->
    ExpectedUsage = #{cpu_seconds => 100000, mem_seconds => 50000000, node_seconds => 1000},
    meck:expect(flurm_dbd_server, calculate_cluster_tres_usage, fun() -> ExpectedUsage end),

    Result = flurm_dbd_query:query_tres_usage(cluster, <<"cluster1">>, <<"2026-02">>),
    ?assertMatch({ok, _}, Result),
    {ok, Usage} = Result,
    ?assertEqual(100000, maps:get(cpu_seconds, Usage)).

%%====================================================================
%% TRES Aggregation Tests
%%====================================================================

tres_aggregation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"aggregate_user_tres sums across periods", fun test_aggregate_user_tres/0},
             {"aggregate_account_tres sums across periods", fun test_aggregate_account_tres/0}
         ]
     end
    }.

test_aggregate_user_tres() ->
    %% Mock to return different usage per period
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_User) ->
        #{cpu_seconds => 1000, mem_seconds => 2000, gpu_seconds => 100}
    end),

    Periods = [<<"2026-01">>, <<"2026-02">>, <<"2026-03">>],
    Result = flurm_dbd_query:aggregate_user_tres(<<"alice">>, Periods),

    ?assert(is_map(Result)),
    %% Should be sum of 3 periods (1000 * 3 = 3000)
    ?assertEqual(3000, maps:get(cpu_seconds, Result)).

test_aggregate_account_tres() ->
    meck:expect(flurm_dbd_server, calculate_account_tres_usage, fun(_Account) ->
        #{cpu_seconds => 500, mem_seconds => 1000}
    end),

    Periods = [<<"2025-12">>, <<"2026-01">>],
    Result = flurm_dbd_query:aggregate_account_tres(<<"research">>, Periods),

    ?assert(is_map(Result)),
    ?assertEqual(1000, maps:get(cpu_seconds, Result)).

%%====================================================================
%% Format TRES String Tests
%%====================================================================

format_tres_string_test_() ->
    [
        {"empty map returns empty binary", fun() ->
            ?assertEqual(<<>>, flurm_dbd_query:format_tres_string(#{}))
        end},
        {"format cpu seconds", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{cpu_seconds => 3600}),
            ?assertEqual(<<"cpu=3600">>, iolist_to_binary(Result))
        end},
        {"format cpu alias", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{cpu => 4}),
            ?assertEqual(<<"cpu=4">>, iolist_to_binary(Result))
        end},
        {"format memory in KB", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{mem => 512}),
            Binary = iolist_to_binary(Result),
            ?assert(binary:match(Binary, <<"mem=">>) =/= nomatch)
        end},
        {"format memory in MB", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{mem => 2048}),
            Binary = iolist_to_binary(Result),
            ?assert(binary:match(Binary, <<"M">>) =/= nomatch)
        end},
        {"format memory in GB", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{mem => 2097152}),  % 2GB in KB
            Binary = iolist_to_binary(Result),
            ?assert(binary:match(Binary, <<"G">>) =/= nomatch)
        end},
        {"format memory in TB", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{mem => 2147483648}),  % 2TB in KB
            Binary = iolist_to_binary(Result),
            ?assert(binary:match(Binary, <<"T">>) =/= nomatch)
        end},
        {"format gpu", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{gpu => 2}),
            Binary = iolist_to_binary(Result),
            ?assertEqual(<<"gres/gpu=2">>, Binary)
        end},
        {"format node", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{node => 4}),
            Binary = iolist_to_binary(Result),
            ?assertEqual(<<"node=4">>, Binary)
        end},
        {"format billing", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{billing => 1000}),
            Binary = iolist_to_binary(Result),
            ?assertEqual(<<"billing=1000">>, Binary)
        end},
        {"format energy", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{energy => 500}),
            Binary = iolist_to_binary(Result),
            ?assertEqual(<<"energy=500">>, Binary)
        end},
        {"format multiple TRES", fun() ->
            TresMap = #{cpu => 8, node => 2, gpu => 1},
            Result = flurm_dbd_query:format_tres_string(TresMap),
            Binary = iolist_to_binary(Result),
            %% Should contain all three
            ?assert(binary:match(Binary, <<"cpu=8">>) =/= nomatch),
            ?assert(binary:match(Binary, <<"node=2">>) =/= nomatch),
            ?assert(binary:match(Binary, <<"gres/gpu=1">>) =/= nomatch)
        end},
        {"zero values are skipped", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{cpu => 0, gpu => 0}),
            ?assertEqual(<<>>, iolist_to_binary(Result))
        end},
        {"unknown keys are skipped", fun() ->
            Result = flurm_dbd_query:format_tres_string(#{unknown_key => 100, cpu => 4}),
            Binary = iolist_to_binary(Result),
            ?assertEqual(<<"cpu=4">>, Binary)
        end}
    ].

%%====================================================================
%% Format SACCT Output Tests
%%====================================================================

format_sacct_output_test_() ->
    [
        {"empty job list returns header only", fun() ->
            Result = flurm_dbd_query:format_sacct_output([]),
            Output = iolist_to_binary(Result),
            ?assert(binary:match(Output, <<"JobID">>) =/= nomatch),
            ?assert(binary:match(Output, <<"User">>) =/= nomatch),
            ?assert(binary:match(Output, <<"Account">>) =/= nomatch)
        end},
        {"format single job", fun() ->
            Jobs = [#{
                job_id => 1001,
                user_name => <<"alice">>,
                account => <<"research">>,
                partition => <<"batch">>,
                state => completed,
                elapsed => 3661,  % 1h 1m 1s
                exit_code => 0,
                tres_alloc => #{cpu => 4}
            }],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),

            ?assert(binary:match(Output, <<"1001">>) =/= nomatch),
            ?assert(binary:match(Output, <<"alice">>) =/= nomatch),
            ?assert(binary:match(Output, <<"research">>) =/= nomatch),
            ?assert(binary:match(Output, <<"batch">>) =/= nomatch),
            ?assert(binary:match(Output, <<"completed">>) =/= nomatch),
            ?assert(binary:match(Output, <<"01:01:01">>) =/= nomatch)
        end},
        {"format multiple jobs", fun() ->
            Jobs = [
                #{job_id => 1001, user_name => <<"alice">>, account => <<"research">>,
                  partition => <<"batch">>, state => completed, elapsed => 100, exit_code => 0, tres_alloc => #{}},
                #{job_id => 1002, user_name => <<"bob">>, account => <<"engineering">>,
                  partition => <<"debug">>, state => running, elapsed => 50, exit_code => 0, tres_alloc => #{}}
            ],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),

            ?assert(binary:match(Output, <<"1001">>) =/= nomatch),
            ?assert(binary:match(Output, <<"1002">>) =/= nomatch),
            ?assert(binary:match(Output, <<"alice">>) =/= nomatch),
            ?assert(binary:match(Output, <<"bob">>) =/= nomatch)
        end},
        {"format elapsed time correctly", fun() ->
            Jobs = [#{job_id => 1, user_name => <<>>, account => <<>>, partition => <<>>,
                      state => completed, elapsed => 7384, exit_code => 0, tres_alloc => #{}}],  % 2h 3m 4s
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),
            ?assert(binary:match(Output, <<"02:03:04">>) =/= nomatch)
        end},
        {"format zero elapsed", fun() ->
            Jobs = [#{job_id => 1, user_name => <<>>, account => <<>>, partition => <<>>,
                      state => pending, elapsed => 0, exit_code => 0, tres_alloc => #{}}],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),
            ?assert(binary:match(Output, <<"00:00:00">>) =/= nomatch)
        end},
        {"format exit code", fun() ->
            Jobs = [#{job_id => 1, user_name => <<>>, account => <<>>, partition => <<>>,
                      state => failed, elapsed => 10, exit_code => 1, tres_alloc => #{}}],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),
            ?assert(binary:match(Output, <<"1:0">>) =/= nomatch)
        end}
    ].

%%====================================================================
%% Query Jobs Tests
%%====================================================================

query_jobs_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"query_jobs with user_name filter", fun test_query_jobs_user_filter/0},
             {"query_jobs with account filter", fun test_query_jobs_account_filter/0},
             {"query_jobs with time range", fun test_query_jobs_time_range/0},
             {"query_jobs_in_range", fun test_query_jobs_in_range/0}
         ]
     end
    }.

test_query_jobs_user_filter() ->
    Filters = #{user_name => <<"testuser">>},
    Result = flurm_dbd_query:query_jobs(Filters),
    ?assertMatch({ok, _}, Result).

test_query_jobs_account_filter() ->
    Filters = #{account => <<"research">>},
    Result = flurm_dbd_query:query_jobs(Filters),
    ?assertMatch({ok, _}, Result).

test_query_jobs_time_range() ->
    Now = erlang:system_time(second),
    Filters = #{start_time => Now - 3600, end_time => Now},
    Result = flurm_dbd_query:query_jobs(Filters),
    ?assertMatch({ok, _}, Result).

test_query_jobs_in_range() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_Filters) ->
        [
            #{job_id => 1, start_time => 5000, end_time => 6000},
            #{job_id => 2, start_time => 7000, end_time => 8000}
        ]
    end),

    Result = flurm_dbd_query:query_jobs_in_range(4000, 9000),
    ?assertMatch({ok, _}, Result),
    {ok, Jobs} = Result,
    ?assertEqual(2, length(Jobs)).

%%====================================================================
%% Backward Compatibility Tests
%%====================================================================

backward_compat_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"query_jobs_by_user (old API)", fun test_old_api_jobs_by_user/0},
             {"query_jobs_by_account (old API)", fun test_old_api_jobs_by_account/0}
         ]
     end
    }.

test_old_api_jobs_by_user() ->
    Now = erlang:system_time(second),
    Result = flurm_dbd_query:query_jobs_by_user(<<"testuser">>, Now - 3600, Now),
    ?assertMatch({ok, _}, Result).

test_old_api_jobs_by_account() ->
    Now = erlang:system_time(second),
    Result = flurm_dbd_query:query_jobs_by_account(<<"research">>, Now - 3600, Now),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% Pagination Tests
%%====================================================================

pagination_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"pagination with limit only", fun test_pagination_limit/0},
             {"pagination with offset", fun test_pagination_offset/0},
             {"pagination with sort ascending", fun test_pagination_sort_asc/0},
             {"pagination with sort descending", fun test_pagination_sort_desc/0},
             {"pagination has_more flag", fun test_pagination_has_more/0},
             {"pagination returned count", fun test_pagination_returned_count/0}
         ]
     end
    }.

test_pagination_limit() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [#{job_id => N, start_time => N * 100, end_time => N * 100 + 50, submit_time => N}
         || N <- lists:seq(1, 100)]
    end),

    Filters = #{start_time => 0, end_time => 20000},
    Options = #{limit => 10},
    Result = flurm_dbd_query:query_jobs_paginated(Filters, Options),

    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, Pagination} = Result,
    ?assertEqual(10, length(Jobs)),
    ?assertEqual(10, maps:get(limit, Pagination)).

test_pagination_offset() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [#{job_id => N, start_time => N * 100, end_time => N * 100 + 50, submit_time => N}
         || N <- lists:seq(1, 20)]
    end),

    Filters = #{start_time => 0, end_time => 5000},
    Options = #{limit => 5, offset => 10},
    Result = flurm_dbd_query:query_jobs_paginated(Filters, Options),

    ?assertMatch({ok, _, _}, Result),
    {ok, Jobs, Pagination} = Result,
    ?assertEqual(5, length(Jobs)),
    ?assertEqual(10, maps:get(offset, Pagination)).

test_pagination_sort_asc() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [
            #{job_id => 3, submit_time => 3000, start_time => 3000, end_time => 3500},
            #{job_id => 1, submit_time => 1000, start_time => 1000, end_time => 1500},
            #{job_id => 2, submit_time => 2000, start_time => 2000, end_time => 2500}
        ]
    end),

    Filters = #{start_time => 0, end_time => 10000},
    Options = #{sort_by => submit_time, sort_order => asc},
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(Filters, Options),

    JobIds = [maps:get(job_id, J) || J <- Jobs],
    ?assertEqual([1, 2, 3], JobIds).

test_pagination_sort_desc() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [
            #{job_id => 1, submit_time => 1000, start_time => 1000, end_time => 1500},
            #{job_id => 3, submit_time => 3000, start_time => 3000, end_time => 3500},
            #{job_id => 2, submit_time => 2000, start_time => 2000, end_time => 2500}
        ]
    end),

    Filters = #{start_time => 0, end_time => 10000},
    Options = #{sort_by => submit_time, sort_order => desc},
    {ok, Jobs, _} = flurm_dbd_query:query_jobs_paginated(Filters, Options),

    JobIds = [maps:get(job_id, J) || J <- Jobs],
    ?assertEqual([3, 2, 1], JobIds).

test_pagination_has_more() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [#{job_id => N, start_time => N * 100, end_time => N * 100 + 50}
         || N <- lists:seq(1, 10)]
    end),

    Filters = #{start_time => 0, end_time => 2000},

    %% With limit that leaves more
    {ok, _, Pagination1} = flurm_dbd_query:query_jobs_paginated(Filters, #{limit => 3}),
    ?assertEqual(true, maps:get(has_more, Pagination1)),

    %% With limit that gets all
    {ok, _, Pagination2} = flurm_dbd_query:query_jobs_paginated(Filters, #{limit => 100}),
    ?assertEqual(false, maps:get(has_more, Pagination2)).

test_pagination_returned_count() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        [#{job_id => N, start_time => N * 100, end_time => N * 100 + 50}
         || N <- lists:seq(1, 5)]
    end),

    Filters = #{start_time => 0, end_time => 1000},
    Options = #{limit => 10},  % More than available
    {ok, Jobs, Pagination} = flurm_dbd_query:query_jobs_paginated(Filters, Options),

    ?assertEqual(5, length(Jobs)),
    ?assertEqual(5, maps:get(returned, Pagination)),
    ?assertEqual(5, maps:get(total, Pagination)).

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"handle query error gracefully", fun test_query_error/0},
             {"handle tres query error", fun test_tres_query_error/0}
         ]
     end
    }.

test_query_error() ->
    meck:expect(flurm_dbd_server, list_job_records, fun(_) ->
        throw(database_error)
    end),

    Result = flurm_dbd_query:query_jobs(#{}),
    ?assertMatch({error, _}, Result).

test_tres_query_error() ->
    meck:expect(flurm_dbd_server, calculate_user_tres_usage, fun(_) ->
        throw(calculation_error)
    end),

    Result = flurm_dbd_query:tres_usage_by_user(<<"alice">>, <<"2026-02">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"format_tres_string with negative values", fun() ->
            %% Negative values should be skipped (guard check)
            Result = flurm_dbd_query:format_tres_string(#{cpu => -1}),
            ?assertEqual(<<>>, iolist_to_binary(Result))
        end},
        {"format elapsed with invalid input", fun() ->
            Jobs = [#{job_id => 1, user_name => <<>>, account => <<>>, partition => <<>>,
                      state => completed, elapsed => invalid, exit_code => 0, tres_alloc => #{}}],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),
            %% Should fall back to 00:00:00
            ?assert(binary:match(Output, <<"00:00:00">>) =/= nomatch)
        end},
        {"format exit_code with invalid input", fun() ->
            Jobs = [#{job_id => 1, user_name => <<>>, account => <<>>, partition => <<>>,
                      state => completed, elapsed => 0, exit_code => invalid, tres_alloc => #{}}],
            Result = flurm_dbd_query:format_sacct_output(Jobs),
            Output = iolist_to_binary(Result),
            %% Should fall back to 0:0
            ?assert(binary:match(Output, <<"0:0">>) =/= nomatch)
        end}
    ].

%%====================================================================
%% Helper Functions
%%====================================================================

sample_jobs() ->
    Now = erlang:system_time(second),
    [
        #{
            job_id => 1001,
            job_name => <<"test_job_1">>,
            user_name => <<"testuser">>,
            account => <<"research">>,
            partition => <<"batch">>,
            state => completed,
            exit_code => 0,
            start_time => Now - 3600,
            end_time => Now - 3000,
            submit_time => Now - 3700,
            elapsed => 600,
            tres_alloc => #{cpu => 4, mem => 8192}
        },
        #{
            job_id => 1002,
            job_name => <<"test_job_2">>,
            user_name => <<"testuser">>,
            account => <<"research">>,
            partition => <<"debug">>,
            state => running,
            exit_code => 0,
            start_time => Now - 1000,
            end_time => Now,
            submit_time => Now - 1200,
            elapsed => 1000,
            tres_alloc => #{cpu => 8, mem => 16384, gpu => 1}
        }
    ].

sample_tres_usage() ->
    #{
        cpu_seconds => 14400,
        mem_seconds => 28672000,
        gpu_seconds => 3600,
        node_seconds => 7200,
        job_count => 5,
        job_time => 14400
    }.
