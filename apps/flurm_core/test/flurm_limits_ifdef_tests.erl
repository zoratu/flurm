%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_limits internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_limits_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Min Nonzero Tests
%%====================================================================

min_nonzero_both_zero_test() ->
    ?assertEqual(0, flurm_limits:min_nonzero(0, 0)).

min_nonzero_first_zero_test() ->
    ?assertEqual(10, flurm_limits:min_nonzero(0, 10)).

min_nonzero_second_zero_test() ->
    ?assertEqual(10, flurm_limits:min_nonzero(10, 0)).

min_nonzero_both_nonzero_test() ->
    ?assertEqual(5, flurm_limits:min_nonzero(5, 10)),
    ?assertEqual(5, flurm_limits:min_nonzero(10, 5)).

min_nonzero_equal_test() ->
    ?assertEqual(7, flurm_limits:min_nonzero(7, 7)).

%%====================================================================
%% Merge TRES Limits Tests
%%====================================================================

merge_tres_limits_empty_test() ->
    ?assertEqual(#{}, flurm_limits:merge_tres_limits(#{}, #{})).

merge_tres_limits_first_empty_test() ->
    Map2 = #{cpu => 100, mem => 1024},
    ?assertEqual(Map2, flurm_limits:merge_tres_limits(#{}, Map2)).

merge_tres_limits_second_empty_test() ->
    Map1 = #{cpu => 100, mem => 1024},
    ?assertEqual(Map1, flurm_limits:merge_tres_limits(Map1, #{})).

merge_tres_limits_no_overlap_test() ->
    Map1 = #{cpu => 100},
    Map2 = #{mem => 1024},
    Result = flurm_limits:merge_tres_limits(Map1, Map2),
    ?assertEqual(#{cpu => 100, mem => 1024}, Result).

merge_tres_limits_overlap_min_test() ->
    Map1 = #{cpu => 50, mem => 2048},
    Map2 = #{cpu => 100, mem => 1024},
    Result = flurm_limits:merge_tres_limits(Map1, Map2),
    ?assertEqual(#{cpu => 50, mem => 1024}, Result).

merge_tres_limits_zero_in_first_test() ->
    Map1 = #{cpu => 0, mem => 1024},
    Map2 = #{cpu => 100, mem => 2048},
    Result = flurm_limits:merge_tres_limits(Map1, Map2),
    %% 0 means use the other value
    ?assertEqual(#{cpu => 100, mem => 1024}, Result).

%%====================================================================
%% Add TRES Tests
%%====================================================================

add_tres_empty_test() ->
    ?assertEqual(#{}, flurm_limits:add_tres(#{}, #{})).

add_tres_first_empty_test() ->
    Map2 = #{cpu => 4, mem => 1024},
    ?assertEqual(Map2, flurm_limits:add_tres(#{}, Map2)).

add_tres_second_empty_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    ?assertEqual(Map1, flurm_limits:add_tres(Map1, #{})).

add_tres_overlap_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    Map2 = #{cpu => 2, mem => 512},
    Result = flurm_limits:add_tres(Map1, Map2),
    ?assertEqual(#{cpu => 6, mem => 1536}, Result).

add_tres_new_keys_test() ->
    Map1 = #{cpu => 4},
    Map2 = #{mem => 1024},
    Result = flurm_limits:add_tres(Map1, Map2),
    ?assertEqual(#{cpu => 4, mem => 1024}, Result).

%%====================================================================
%% Subtract TRES Tests
%%====================================================================

subtract_tres_empty_test() ->
    ?assertEqual(#{}, flurm_limits:subtract_tres(#{}, #{})).

subtract_tres_second_empty_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    ?assertEqual(Map1, flurm_limits:subtract_tres(Map1, #{})).

subtract_tres_overlap_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    Map2 = #{cpu => 2, mem => 512},
    Result = flurm_limits:subtract_tres(Map1, Map2),
    ?assertEqual(#{cpu => 2, mem => 512}, Result).

subtract_tres_underflow_test() ->
    Map1 = #{cpu => 4, mem => 1024},
    Map2 = #{cpu => 10, mem => 2048},
    Result = flurm_limits:subtract_tres(Map1, Map2),
    %% Should clamp to 0
    ?assertEqual(#{cpu => 0, mem => 0}, Result).

subtract_tres_key_not_in_first_test() ->
    Map1 = #{cpu => 4},
    Map2 = #{cpu => 2, mem => 1024},
    Result = flurm_limits:subtract_tres(Map1, Map2),
    ?assertEqual(#{cpu => 2, mem => 0}, Result).

%%====================================================================
%% Add TRES Minutes Tests
%%====================================================================

add_tres_mins_empty_test() ->
    Result = flurm_limits:add_tres_mins(#{}, #{}, 0),
    ?assertEqual(#{}, Result).

add_tres_mins_basic_test() ->
    TRES = #{cpu => 4},
    WallTimeSeconds = 60,  % 1 minute
    Result = flurm_limits:add_tres_mins(#{}, TRES, WallTimeSeconds),
    ?assertEqual(#{cpu => 4.0}, Result).

add_tres_mins_accumulate_test() ->
    ExistingMins = #{cpu => 100.0},
    TRES = #{cpu => 4},
    WallTimeSeconds = 600,  % 10 minutes
    Result = flurm_limits:add_tres_mins(ExistingMins, TRES, WallTimeSeconds),
    %% 100 + (4 * 10) = 140
    ?assertEqual(#{cpu => 140.0}, Result).

add_tres_mins_multiple_tres_test() ->
    TRES = #{cpu => 4, mem => 1024},
    WallTimeSeconds = 120,  % 2 minutes
    Result = flurm_limits:add_tres_mins(#{}, TRES, WallTimeSeconds),
    %% cpu: 4 * 2 = 8, mem: 1024 * 2 = 2048
    ?assertEqual(#{cpu => 8.0, mem => 2048.0}, Result).

%%====================================================================
%% Get Limit Field Tests
%%====================================================================

get_limit_field_max_wall_test() ->
    %% limit_spec record positions:
    %% {limit_spec, key, max_jobs, max_submit, max_wall, max_nodes_per_job,
    %%  max_cpus_per_job, max_mem_per_job, max_tres, grp_jobs, grp_submit, grp_tres}
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 86400, 0, 0, 0, #{}, 0, 0, #{}},
    ?assertEqual(86400, flurm_limits:get_limit_field(max_wall, LimitSpec)).

get_limit_field_max_nodes_per_job_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 100, 0, 0, #{}, 0, 0, #{}},
    ?assertEqual(100, flurm_limits:get_limit_field(max_nodes_per_job, LimitSpec)).

get_limit_field_max_cpus_per_job_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 1000, 0, #{}, 0, 0, #{}},
    ?assertEqual(1000, flurm_limits:get_limit_field(max_cpus_per_job, LimitSpec)).

get_limit_field_max_mem_per_job_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 102400, #{}, 0, 0, #{}},
    ?assertEqual(102400, flurm_limits:get_limit_field(max_mem_per_job, LimitSpec)).

%%====================================================================
%% Check Max Jobs Tests
%%====================================================================

check_max_jobs_unlimited_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Usage = {usage, {user, <<"test">>}, 100, 0, #{}, #{}},
    ?assertEqual(ok, flurm_limits:check_max_jobs(LimitSpec, Usage)).

check_max_jobs_under_limit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 10, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Usage = {usage, {user, <<"test">>}, 5, 0, #{}, #{}},
    ?assertEqual(ok, flurm_limits:check_max_jobs(LimitSpec, Usage)).

check_max_jobs_at_limit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 10, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Usage = {usage, {user, <<"test">>}, 10, 0, #{}, #{}},
    ?assertEqual({error, {max_jobs_exceeded, 10, 10}},
                 flurm_limits:check_max_jobs(LimitSpec, Usage)).

check_max_jobs_over_limit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 10, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Usage = {usage, {user, <<"test">>}, 15, 0, #{}, #{}},
    ?assertEqual({error, {max_jobs_exceeded, 15, 10}},
                 flurm_limits:check_max_jobs(LimitSpec, Usage)).

%%====================================================================
%% Check Grp Jobs Tests
%%====================================================================

check_grp_jobs_unlimited_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Usage = {usage, {account, <<"test">>}, 100, 0, #{}, #{}},
    ?assertEqual(ok, flurm_limits:check_grp_jobs(LimitSpec, Usage)).

check_grp_jobs_under_limit_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 50, 0, #{}},
    Usage = {usage, {account, <<"test">>}, 25, 0, #{}, #{}},
    ?assertEqual(ok, flurm_limits:check_grp_jobs(LimitSpec, Usage)).

check_grp_jobs_at_limit_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 50, 0, #{}},
    Usage = {usage, {account, <<"test">>}, 50, 0, #{}, #{}},
    ?assertEqual({error, {grp_jobs_exceeded, 50, 50}},
                 flurm_limits:check_grp_jobs(LimitSpec, Usage)).

%%====================================================================
%% Check Resources Tests
%%====================================================================

check_resources_empty_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    ?assertEqual(ok, flurm_limits:check_resources([], LimitSpec)).

check_resources_under_limit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 86400, 100, 1000, 102400, #{}, 0, 0, #{}},
    Checks = [
        {max_wall, time_limit, 3600},
        {max_nodes_per_job, num_nodes, 10},
        {max_cpus_per_job, num_cpus, 100}
    ],
    ?assertEqual(ok, flurm_limits:check_resources(Checks, LimitSpec)).

check_resources_over_limit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 86400, 100, 1000, 102400, #{}, 0, 0, #{}},
    Checks = [
        {max_wall, time_limit, 100000}  % Over 86400
    ],
    ?assertEqual({error, {limit_exceeded, time_limit, 100000, 86400}},
                 flurm_limits:check_resources(Checks, LimitSpec)).

check_resources_zero_limit_allows_any_test() ->
    %% Zero limit means unlimited
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Checks = [
        {max_wall, time_limit, 999999},
        {max_nodes_per_job, num_nodes, 10000}
    ],
    ?assertEqual(ok, flurm_limits:check_resources(Checks, LimitSpec)).

%%====================================================================
%% Set Limit Field Tests
%%====================================================================

set_limit_field_max_jobs_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_jobs, 50, LimitSpec),
    ?assertEqual(50, element(3, Updated)).

set_limit_field_max_submit_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_submit, 100, LimitSpec),
    ?assertEqual(100, element(4, Updated)).

set_limit_field_max_wall_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_wall, 86400, LimitSpec),
    ?assertEqual(86400, element(5, Updated)).

set_limit_field_max_nodes_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_nodes_per_job, 100, LimitSpec),
    ?assertEqual(100, element(6, Updated)).

set_limit_field_max_cpus_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_cpus_per_job, 1000, LimitSpec),
    ?assertEqual(1000, element(7, Updated)).

set_limit_field_max_mem_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(max_mem_per_job, 102400, LimitSpec),
    ?assertEqual(102400, element(8, Updated)).

set_limit_field_max_tres_test() ->
    LimitSpec = {limit_spec, {user, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    TRESMap = #{cpu => 100, gpu => 4},
    Updated = flurm_limits:set_limit_field(max_tres, TRESMap, LimitSpec),
    ?assertEqual(TRESMap, element(9, Updated)).

set_limit_field_grp_jobs_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(grp_jobs, 200, LimitSpec),
    ?assertEqual(200, element(10, Updated)).

set_limit_field_grp_submit_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    Updated = flurm_limits:set_limit_field(grp_submit, 500, LimitSpec),
    ?assertEqual(500, element(11, Updated)).

set_limit_field_grp_tres_test() ->
    LimitSpec = {limit_spec, {account, <<"test">>}, 0, 0, 0, 0, 0, 0, #{}, 0, 0, #{}},
    TRESMap = #{cpu => 1000, mem => 1024000},
    Updated = flurm_limits:set_limit_field(grp_tres, TRESMap, LimitSpec),
    ?assertEqual(TRESMap, element(12, Updated)).
