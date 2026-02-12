%%%-------------------------------------------------------------------
%%% @doc Coverage-oriented EUnit tests for flurm_limits module.
%%%
%%% Pure function calls only -- no mocking. Every test calls a real
%%% exported (or TEST-exported) function with controlled inputs to
%%% exercise code paths that existing test files do not fully cover.
%%%
%%% Where ETS tables are required, setup/cleanup fixtures create and
%%% destroy them so tests remain isolated.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_limits_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Records are defined inside flurm_limits.erl; redefine here for test
%% construction. Field order and defaults match the source exactly.
-record(limit_spec, {
    key :: term(),
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer(),
    max_nodes_per_job = 0 :: non_neg_integer(),
    max_cpus_per_job = 0 :: non_neg_integer(),
    max_mem_per_job = 0 :: non_neg_integer(),
    max_tres = #{} :: map(),
    grp_jobs = 0 :: non_neg_integer(),
    grp_submit = 0 :: non_neg_integer(),
    grp_tres = #{} :: map()
}).

-record(usage, {
    key :: term(),
    running_jobs = 0 :: non_neg_integer(),
    pending_jobs = 0 :: non_neg_integer(),
    tres_used = #{} :: map(),
    tres_mins = #{} :: map()
}).

%%====================================================================
%% min_nonzero/2 -- all three clauses
%%====================================================================

%% Both zero => zero
min_nz_both_zero_test() ->
    ?assertEqual(0, flurm_limits:min_nonzero(0, 0)).

%% First zero => second
min_nz_first_zero_test() ->
    ?assertEqual(42, flurm_limits:min_nonzero(0, 42)).

%% Second zero => first
min_nz_second_zero_test() ->
    ?assertEqual(42, flurm_limits:min_nonzero(42, 0)).

%% Both nonzero, A < B
min_nz_a_smaller_test() ->
    ?assertEqual(3, flurm_limits:min_nonzero(3, 10)).

%% Both nonzero, B < A
min_nz_b_smaller_test() ->
    ?assertEqual(3, flurm_limits:min_nonzero(10, 3)).

%% Both equal
min_nz_equal_test() ->
    ?assertEqual(5, flurm_limits:min_nonzero(5, 5)).

%% Large values
min_nz_large_test() ->
    ?assertEqual(999999, flurm_limits:min_nonzero(999999, 1000000)).

%%====================================================================
%% merge_tres_limits/2 -- all branches
%%====================================================================

%% Both empty
merge_tres_empty_test() ->
    ?assertEqual(#{}, flurm_limits:merge_tres_limits(#{}, #{})).

%% First empty, second has values
merge_tres_first_empty_test() ->
    M = #{cpu => 100, mem => 2048},
    ?assertEqual(M, flurm_limits:merge_tres_limits(#{}, M)).

%% Second empty, first has values
merge_tres_second_empty_test() ->
    M = #{cpu => 100},
    ?assertEqual(M, flurm_limits:merge_tres_limits(M, #{})).

%% No overlap -- merge
merge_tres_no_overlap_test() ->
    A = #{cpu => 50},
    B = #{gpu => 4},
    R = flurm_limits:merge_tres_limits(A, B),
    ?assertEqual(50, maps:get(cpu, R)),
    ?assertEqual(4, maps:get(gpu, R)).

%% Overlap, min wins
merge_tres_overlap_min_test() ->
    A = #{cpu => 30, mem => 2048},
    B = #{cpu => 100, mem => 1024},
    R = flurm_limits:merge_tres_limits(A, B),
    ?assertEqual(30, maps:get(cpu, R)),
    ?assertEqual(1024, maps:get(mem, R)).

%% Overlap with zero in accumulator (0 means unlimited in Acc)
merge_tres_zero_in_acc_test() ->
    A = #{cpu => 50},
    B = #{cpu => 0},
    R = flurm_limits:merge_tres_limits(A, B),
    %% When Acc has 0 for cpu, branch puts V (50) directly
    ?assertEqual(50, maps:get(cpu, R)).

%% Overlap with zero in Map1 (folded value is 0)
merge_tres_zero_in_map1_test() ->
    A = #{cpu => 0},
    B = #{cpu => 100},
    R = flurm_limits:merge_tres_limits(A, B),
    %% Acc has cpu=100 (from B), V is 0 from A => Existing=100, min(0,100)=0
    ?assertEqual(0, maps:get(cpu, R)).

%%====================================================================
%% add_tres/2
%%====================================================================

add_tres_empty_test() ->
    ?assertEqual(#{}, flurm_limits:add_tres(#{}, #{})).

add_tres_disjoint_test() ->
    R = flurm_limits:add_tres(#{a => 1}, #{b => 2}),
    ?assertEqual(#{a => 1, b => 2}, R).

add_tres_overlap_test() ->
    R = flurm_limits:add_tres(#{cpu => 4, mem => 100}, #{cpu => 8, mem => 200}),
    ?assertEqual(#{cpu => 12, mem => 300}, R).

add_tres_second_empty_test() ->
    M = #{x => 5},
    ?assertEqual(M, flurm_limits:add_tres(M, #{})).

%%====================================================================
%% subtract_tres/2
%%====================================================================

sub_tres_empty_test() ->
    ?assertEqual(#{}, flurm_limits:subtract_tres(#{}, #{})).

sub_tres_normal_test() ->
    R = flurm_limits:subtract_tres(#{cpu => 10, mem => 500}, #{cpu => 3, mem => 200}),
    ?assertEqual(#{cpu => 7, mem => 300}, R).

sub_tres_clamp_zero_test() ->
    R = flurm_limits:subtract_tres(#{cpu => 2}, #{cpu => 10}),
    ?assertEqual(#{cpu => 0}, R).

%% Key in Map2 not in Map1 => starts at 0, subtracts => max(0, 0-V) = 0
sub_tres_missing_key_test() ->
    R = flurm_limits:subtract_tres(#{}, #{cpu => 5}),
    ?assertEqual(#{cpu => 0}, R).

sub_tres_second_empty_test() ->
    M = #{gpu => 4},
    ?assertEqual(M, flurm_limits:subtract_tres(M, #{})).

%%====================================================================
%% add_tres_mins/3
%%====================================================================

add_tres_mins_empty_test() ->
    ?assertEqual(#{}, flurm_limits:add_tres_mins(#{}, #{}, 0)).

add_tres_mins_basic_test() ->
    %% 4 CPUs * (120s / 60) = 4 * 2 = 8.0
    R = flurm_limits:add_tres_mins(#{}, #{cpu => 4}, 120),
    ?assertEqual(#{cpu => 8.0}, R).

add_tres_mins_accumulate_test() ->
    %% Existing 100 + (2 * 5min) = 110
    R = flurm_limits:add_tres_mins(#{cpu => 100.0}, #{cpu => 2}, 300),
    ?assertEqual(#{cpu => 110.0}, R).

add_tres_mins_multi_tres_test() ->
    R = flurm_limits:add_tres_mins(#{}, #{cpu => 4, gpu => 2}, 60),
    ?assertEqual(#{cpu => 4.0, gpu => 2.0}, R).

add_tres_mins_zero_wall_test() ->
    R = flurm_limits:add_tres_mins(#{}, #{cpu => 4}, 0),
    ?assertEqual(#{cpu => 0.0}, R).

%%====================================================================
%% merge_limits/1 -- fold over list of limit_spec records
%%====================================================================

%% Empty list => all-zero effective spec
merge_limits_empty_test() ->
    Result = flurm_limits:merge_limits([]),
    ?assertEqual(effective, Result#limit_spec.key),
    ?assertEqual(0, Result#limit_spec.max_jobs),
    ?assertEqual(0, Result#limit_spec.max_submit),
    ?assertEqual(0, Result#limit_spec.max_wall),
    ?assertEqual(0, Result#limit_spec.max_nodes_per_job),
    ?assertEqual(0, Result#limit_spec.max_cpus_per_job),
    ?assertEqual(0, Result#limit_spec.max_mem_per_job),
    ?assertEqual(#{}, Result#limit_spec.max_tres),
    ?assertEqual(0, Result#limit_spec.grp_jobs),
    ?assertEqual(0, Result#limit_spec.grp_submit),
    ?assertEqual(#{}, Result#limit_spec.grp_tres).

%% Single element
merge_limits_single_test() ->
    L = #limit_spec{key = a, max_jobs = 10, max_wall = 3600},
    Result = flurm_limits:merge_limits([L]),
    ?assertEqual(effective, Result#limit_spec.key),
    ?assertEqual(10, Result#limit_spec.max_jobs),
    ?assertEqual(3600, Result#limit_spec.max_wall).

%% Two elements, most restrictive wins
merge_limits_two_test() ->
    L1 = #limit_spec{key = a, max_jobs = 50, max_wall = 7200,
                     max_nodes_per_job = 10, max_cpus_per_job = 0},
    L2 = #limit_spec{key = b, max_jobs = 20, max_wall = 0,
                     max_nodes_per_job = 5, max_cpus_per_job = 100},
    Result = flurm_limits:merge_limits([L1, L2]),
    ?assertEqual(20, Result#limit_spec.max_jobs),
    ?assertEqual(7200, Result#limit_spec.max_wall),   %% 0 treated as unlimited
    ?assertEqual(5, Result#limit_spec.max_nodes_per_job),
    ?assertEqual(100, Result#limit_spec.max_cpus_per_job). %% 0 treated as unlimited

%% Three elements with TRES
merge_limits_tres_test() ->
    L1 = #limit_spec{key = a, max_tres = #{cpu => 100}},
    L2 = #limit_spec{key = b, max_tres = #{cpu => 50, gpu => 8}},
    L3 = #limit_spec{key = c, max_tres = #{gpu => 4}},
    Result = flurm_limits:merge_limits([L1, L2, L3]),
    ?assertEqual(50, maps:get(cpu, Result#limit_spec.max_tres)),
    ?assertEqual(4, maps:get(gpu, Result#limit_spec.max_tres)).

%% grp_jobs and grp_submit
merge_limits_grp_test() ->
    L1 = #limit_spec{key = a, grp_jobs = 100, grp_submit = 500},
    L2 = #limit_spec{key = b, grp_jobs = 50,  grp_submit = 0},
    Result = flurm_limits:merge_limits([L1, L2]),
    ?assertEqual(50, Result#limit_spec.grp_jobs),
    ?assertEqual(500, Result#limit_spec.grp_submit).

%%====================================================================
%% check_max_jobs/2
%%====================================================================

check_max_jobs_unlimited_test() ->
    L = #limit_spec{key = x, max_jobs = 0},
    U = #usage{key = x, running_jobs = 9999},
    ?assertEqual(ok, flurm_limits:check_max_jobs(L, U)).

check_max_jobs_under_test() ->
    L = #limit_spec{key = x, max_jobs = 10},
    U = #usage{key = x, running_jobs = 5},
    ?assertEqual(ok, flurm_limits:check_max_jobs(L, U)).

check_max_jobs_at_test() ->
    L = #limit_spec{key = x, max_jobs = 10},
    U = #usage{key = x, running_jobs = 10},
    ?assertEqual({error, {max_jobs_exceeded, 10, 10}},
                 flurm_limits:check_max_jobs(L, U)).

check_max_jobs_over_test() ->
    L = #limit_spec{key = x, max_jobs = 10},
    U = #usage{key = x, running_jobs = 15},
    ?assertEqual({error, {max_jobs_exceeded, 15, 10}},
                 flurm_limits:check_max_jobs(L, U)).

%%====================================================================
%% check_grp_jobs/2
%%====================================================================

check_grp_jobs_unlimited_test() ->
    L = #limit_spec{key = x, grp_jobs = 0},
    U = #usage{key = x, running_jobs = 9999},
    ?assertEqual(ok, flurm_limits:check_grp_jobs(L, U)).

check_grp_jobs_under_test() ->
    L = #limit_spec{key = x, grp_jobs = 50},
    U = #usage{key = x, running_jobs = 25},
    ?assertEqual(ok, flurm_limits:check_grp_jobs(L, U)).

check_grp_jobs_at_test() ->
    L = #limit_spec{key = x, grp_jobs = 50},
    U = #usage{key = x, running_jobs = 50},
    ?assertEqual({error, {grp_jobs_exceeded, 50, 50}},
                 flurm_limits:check_grp_jobs(L, U)).

check_grp_jobs_over_test() ->
    L = #limit_spec{key = x, grp_jobs = 50},
    U = #usage{key = x, running_jobs = 100},
    ?assertEqual({error, {grp_jobs_exceeded, 100, 50}},
                 flurm_limits:check_grp_jobs(L, U)).

%%====================================================================
%% get_limit_field/2
%%====================================================================

get_field_max_wall_test() ->
    L = #limit_spec{key = x, max_wall = 86400},
    ?assertEqual(86400, flurm_limits:get_limit_field(max_wall, L)).

get_field_max_nodes_test() ->
    L = #limit_spec{key = x, max_nodes_per_job = 64},
    ?assertEqual(64, flurm_limits:get_limit_field(max_nodes_per_job, L)).

get_field_max_cpus_test() ->
    L = #limit_spec{key = x, max_cpus_per_job = 1024},
    ?assertEqual(1024, flurm_limits:get_limit_field(max_cpus_per_job, L)).

get_field_max_mem_test() ->
    L = #limit_spec{key = x, max_mem_per_job = 102400},
    ?assertEqual(102400, flurm_limits:get_limit_field(max_mem_per_job, L)).

%% Zero value (unlimited)
get_field_zero_test() ->
    L = #limit_spec{key = x},
    ?assertEqual(0, flurm_limits:get_limit_field(max_wall, L)).

%%====================================================================
%% set_limit_field/3 -- every field clause
%%====================================================================

set_field_max_jobs_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_jobs, 10, L),
    ?assertEqual(10, R#limit_spec.max_jobs).

set_field_max_submit_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_submit, 20, L),
    ?assertEqual(20, R#limit_spec.max_submit).

set_field_max_wall_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_wall, 86400, L),
    ?assertEqual(86400, R#limit_spec.max_wall).

set_field_max_nodes_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_nodes_per_job, 64, L),
    ?assertEqual(64, R#limit_spec.max_nodes_per_job).

set_field_max_cpus_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_cpus_per_job, 1024, L),
    ?assertEqual(1024, R#limit_spec.max_cpus_per_job).

set_field_max_mem_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(max_mem_per_job, 102400, L),
    ?assertEqual(102400, R#limit_spec.max_mem_per_job).

set_field_max_tres_test() ->
    L = #limit_spec{key = x},
    T = #{cpu => 100, gpu => 4},
    R = flurm_limits:set_limit_field(max_tres, T, L),
    ?assertEqual(T, R#limit_spec.max_tres).

set_field_grp_jobs_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(grp_jobs, 200, L),
    ?assertEqual(200, R#limit_spec.grp_jobs).

set_field_grp_submit_test() ->
    L = #limit_spec{key = x},
    R = flurm_limits:set_limit_field(grp_submit, 500, L),
    ?assertEqual(500, R#limit_spec.grp_submit).

set_field_grp_tres_test() ->
    L = #limit_spec{key = x},
    T = #{cpu => 1000},
    R = flurm_limits:set_limit_field(grp_tres, T, L),
    ?assertEqual(T, R#limit_spec.grp_tres).

%%====================================================================
%% check_job_resources/2 -- cover the dispatch through check_resources
%%====================================================================

check_job_resources_all_ok_test() ->
    L = #limit_spec{key = x, max_wall = 86400, max_nodes_per_job = 100,
                    max_cpus_per_job = 1000, max_mem_per_job = 102400},
    JobSpec = #{time_limit => 3600, num_nodes => 4, num_cpus => 32, memory_mb => 8192},
    ?assertEqual(ok, flurm_limits:check_job_resources(L, JobSpec)).

check_job_resources_wall_exceeded_test() ->
    L = #limit_spec{key = x, max_wall = 3600},
    JobSpec = #{time_limit => 7200},
    ?assertEqual({error, {limit_exceeded, time_limit, 7200, 3600}},
                 flurm_limits:check_job_resources(L, JobSpec)).

check_job_resources_nodes_exceeded_test() ->
    L = #limit_spec{key = x, max_nodes_per_job = 4},
    JobSpec = #{num_nodes => 10},
    ?assertEqual({error, {limit_exceeded, num_nodes, 10, 4}},
                 flurm_limits:check_job_resources(L, JobSpec)).

check_job_resources_cpus_exceeded_test() ->
    L = #limit_spec{key = x, max_cpus_per_job = 8},
    JobSpec = #{num_cpus => 16},
    ?assertEqual({error, {limit_exceeded, num_cpus, 16, 8}},
                 flurm_limits:check_job_resources(L, JobSpec)).

check_job_resources_mem_exceeded_test() ->
    L = #limit_spec{key = x, max_mem_per_job = 4096},
    JobSpec = #{memory_mb => 8192},
    ?assertEqual({error, {limit_exceeded, memory_mb, 8192, 4096}},
                 flurm_limits:check_job_resources(L, JobSpec)).

%% All limits zero => unlimited, everything passes
check_job_resources_unlimited_test() ->
    L = #limit_spec{key = x},
    JobSpec = #{time_limit => 999999, num_nodes => 1000,
                num_cpus => 100000, memory_mb => 99999999},
    ?assertEqual(ok, flurm_limits:check_job_resources(L, JobSpec)).

%% JobSpec with missing keys uses defaults
check_job_resources_defaults_test() ->
    L = #limit_spec{key = x, max_wall = 3600, max_nodes_per_job = 10,
                    max_cpus_per_job = 100, max_mem_per_job = 8192},
    JobSpec = #{},  %% defaults: time_limit=0, num_nodes=1, num_cpus=1, memory_mb=0
    ?assertEqual(ok, flurm_limits:check_job_resources(L, JobSpec)).

%%====================================================================
%% check_resources/2 -- empty and multi-item
%%====================================================================

check_resources_empty_test() ->
    L = #limit_spec{key = x},
    ?assertEqual(ok, flurm_limits:check_resources([], L)).

check_resources_first_fails_test() ->
    L = #limit_spec{key = x, max_wall = 100},
    Checks = [{max_wall, time_limit, 200}, {max_nodes_per_job, num_nodes, 1}],
    ?assertEqual({error, {limit_exceeded, time_limit, 200, 100}},
                 flurm_limits:check_resources(Checks, L)).

check_resources_second_fails_test() ->
    L = #limit_spec{key = x, max_wall = 86400, max_nodes_per_job = 2},
    Checks = [{max_wall, time_limit, 100}, {max_nodes_per_job, num_nodes, 5}],
    ?assertEqual({error, {limit_exceeded, num_nodes, 5, 2}},
                 flurm_limits:check_resources(Checks, L)).

check_resources_all_pass_test() ->
    L = #limit_spec{key = x, max_wall = 86400, max_nodes_per_job = 100,
                    max_cpus_per_job = 1000, max_mem_per_job = 102400},
    Checks = [
        {max_wall, time_limit, 3600},
        {max_nodes_per_job, num_nodes, 10},
        {max_cpus_per_job, num_cpus, 100},
        {max_mem_per_job, memory_mb, 8192}
    ],
    ?assertEqual(ok, flurm_limits:check_resources(Checks, L)).

%%====================================================================
%% set_limit_field/3 roundtrip with get_limit_field/2
%%====================================================================

roundtrip_wall_test() ->
    L0 = #limit_spec{key = test},
    L1 = flurm_limits:set_limit_field(max_wall, 7200, L0),
    ?assertEqual(7200, flurm_limits:get_limit_field(max_wall, L1)).

roundtrip_nodes_test() ->
    L0 = #limit_spec{key = test},
    L1 = flurm_limits:set_limit_field(max_nodes_per_job, 32, L0),
    ?assertEqual(32, flurm_limits:get_limit_field(max_nodes_per_job, L1)).

roundtrip_cpus_test() ->
    L0 = #limit_spec{key = test},
    L1 = flurm_limits:set_limit_field(max_cpus_per_job, 256, L0),
    ?assertEqual(256, flurm_limits:get_limit_field(max_cpus_per_job, L1)).

roundtrip_mem_test() ->
    L0 = #limit_spec{key = test},
    L1 = flurm_limits:set_limit_field(max_mem_per_job, 65536, L0),
    ?assertEqual(65536, flurm_limits:get_limit_field(max_mem_per_job, L1)).

%%====================================================================
%% ETS-dependent tests (init_default_limits, get_or_create_usage,
%%                       compute_effective_limits, update_usage)
%%====================================================================

-define(USER_LIMITS_TABLE, flurm_user_limits).
-define(ACCOUNT_LIMITS_TABLE, flurm_account_limits).
-define(PARTITION_LIMITS_TABLE, flurm_partition_limits).
-define(USAGE_TABLE, flurm_usage).

ets_cleanup() ->
    catch ets:delete(?USER_LIMITS_TABLE),
    catch ets:delete(?ACCOUNT_LIMITS_TABLE),
    catch ets:delete(?PARTITION_LIMITS_TABLE),
    catch ets:delete(?USAGE_TABLE),
    ok.

ets_setup() ->
    ets_cleanup(),
    %% init/1 creates tables and populates defaults
    {ok, _State} = flurm_limits:init([]),
    ok.

%% init_default_limits/0 creates the default partition entry
init_default_limits_test_() ->
    {setup,
     fun ets_setup/0,
     fun(_) -> ets_cleanup() end,
     fun() ->
         [DefaultLimit] = ets:lookup(?PARTITION_LIMITS_TABLE, {partition, <<"default">>}),
         ?assertEqual(1000, DefaultLimit#limit_spec.max_jobs),
         ?assertEqual(5000, DefaultLimit#limit_spec.max_submit),
         ?assertEqual(604800, DefaultLimit#limit_spec.max_wall),
         ?assertEqual(100, DefaultLimit#limit_spec.max_nodes_per_job),
         ?assertEqual(10000, DefaultLimit#limit_spec.max_cpus_per_job),
         ?assertEqual(1024000, DefaultLimit#limit_spec.max_mem_per_job)
     end}.

%% get_or_create_usage/1 creates a fresh record when missing
get_or_create_usage_new_test_() ->
    {setup,
     fun ets_setup/0,
     fun(_) -> ets_cleanup() end,
     fun() ->
         U = flurm_limits:get_or_create_usage({user, <<"fresh_user">>}),
         ?assertEqual({user, <<"fresh_user">>}, U#usage.key),
         ?assertEqual(0, U#usage.running_jobs),
         ?assertEqual(0, U#usage.pending_jobs),
         ?assertEqual(#{}, U#usage.tres_used),
         ?assertEqual(#{}, U#usage.tres_mins)
     end}.

%% get_or_create_usage/1 returns existing record
get_or_create_usage_existing_test_() ->
    {setup,
     fun ets_setup/0,
     fun(_) -> ets_cleanup() end,
     fun() ->
         ets:insert(?USAGE_TABLE, #usage{key = {user, <<"existing">>},
                                         running_jobs = 7,
                                         pending_jobs = 3}),
         U = flurm_limits:get_or_create_usage({user, <<"existing">>}),
         ?assertEqual(7, U#usage.running_jobs),
         ?assertEqual(3, U#usage.pending_jobs)
     end}.

%% update_usage/2 applies function and writes back
update_usage_test_() ->
    {setup,
     fun ets_setup/0,
     fun(_) -> ets_cleanup() end,
     fun() ->
         ets:insert(?USAGE_TABLE, #usage{key = {user, <<"upd">>},
                                         running_jobs = 2,
                                         tres_used = #{cpu => 4}}),
         flurm_limits:update_usage({user, <<"upd">>}, fun(U) ->
             U#usage{running_jobs = U#usage.running_jobs + 1,
                     tres_used = flurm_limits:add_tres(U#usage.tres_used, #{cpu => 4})}
         end),
         [Updated] = ets:lookup(?USAGE_TABLE, {user, <<"upd">>}),
         ?assertEqual(3, Updated#usage.running_jobs),
         ?assertEqual(8, maps:get(cpu, Updated#usage.tres_used))
     end}.

%% compute_effective_limits/3 merges partition, account, user limits
compute_effective_limits_test_() ->
    {setup,
     fun() ->
         ets_setup(),
         %% Partition limit
         ets:insert(?PARTITION_LIMITS_TABLE,
                    #limit_spec{key = {partition, <<"batch">>},
                                max_wall = 86400, max_nodes_per_job = 50}),
         %% Account limit (more restrictive on nodes)
         ets:insert(?ACCOUNT_LIMITS_TABLE,
                    #limit_spec{key = {account, <<"eng">>},
                                max_nodes_per_job = 20, grp_jobs = 200}),
         %% User limit (more restrictive on wall)
         ets:insert(?USER_LIMITS_TABLE,
                    #limit_spec{key = {user, <<"alice">>},
                                max_wall = 3600, max_jobs = 10}),
         ok
     end,
     fun(_) -> ets_cleanup() end,
     fun() ->
         Eff = flurm_limits:compute_effective_limits(
                   <<"alice">>, <<"eng">>, <<"batch">>),
         ?assertEqual(effective, Eff#limit_spec.key),
         ?assertEqual(3600, Eff#limit_spec.max_wall),
         ?assertEqual(20, Eff#limit_spec.max_nodes_per_job),
         ?assertEqual(10, Eff#limit_spec.max_jobs),
         ?assertEqual(200, Eff#limit_spec.grp_jobs)
     end}.

%% compute_effective_limits/3 with unknown entities falls back to defaults
compute_effective_unknown_test_() ->
    {setup,
     fun ets_setup/0,
     fun(_) -> ets_cleanup() end,
     fun() ->
         Eff = flurm_limits:compute_effective_limits(
                   <<"nobody">>, <<"noacct">>, <<"nopart">>),
         ?assertEqual(effective, Eff#limit_spec.key),
         %% All fields should be 0 (unlimited) since no limits defined
         ?assertEqual(0, Eff#limit_spec.max_jobs),
         ?assertEqual(0, Eff#limit_spec.max_wall)
     end}.

%% compute_effective_limits/3 with specific user+account+partition entry
compute_effective_specific_test_() ->
    {setup,
     fun() ->
         ets_setup(),
         %% General user limit: 100 jobs
         ets:insert(?USER_LIMITS_TABLE,
                    #limit_spec{key = {user, <<"bob">>}, max_jobs = 100}),
         %% Specific user+acct+part limit: 5 jobs
         ets:insert(?USER_LIMITS_TABLE,
                    #limit_spec{key = {user, <<"bob">>, <<"team">>, <<"gpu">>},
                                max_jobs = 5}),
         ok
     end,
     fun(_) -> ets_cleanup() end,
     fun() ->
         Eff = flurm_limits:compute_effective_limits(
                   <<"bob">>, <<"team">>, <<"gpu">>),
         ?assertEqual(5, Eff#limit_spec.max_jobs)
     end}.
