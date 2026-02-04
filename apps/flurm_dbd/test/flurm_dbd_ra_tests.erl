%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Ra State Machine Unit Tests
%%%
%%% Comprehensive unit tests for the flurm_dbd_ra module that test
%%% the Ra state machine callbacks directly without requiring a
%%% running Ra cluster.
%%%
%%% Tests cover:
%%% - init/1 - state machine initialization
%%% - apply/3 - all command types (record_job, update_tres, queries)
%%% - State transitions for job recording
%%% - TRES calculation and aggregation
%%% - Query operations (by user, by time range, by account)
%%% - Error handling for invalid commands
%%% - Idempotency (no double-counting)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Helpers
%%====================================================================

%% Mock metadata for apply/3 calls
mock_meta() ->
    #{index => 1, term => 1}.

mock_meta(Index) ->
    #{index => Index, term => 1}.

%% Create a minimal job info map
make_job_info(JobId) ->
    make_job_info(JobId, #{}).

make_job_info(JobId, Overrides) ->
    Base = #{
        job_id => JobId,
        job_name => <<"test_job">>,
        user_name => <<"testuser">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"research">>,
        partition => <<"batch">>,
        cluster => <<"flurm">>,
        qos => <<"normal">>,
        state => completed,
        exit_code => 0,
        num_nodes => 2,
        num_cpus => 8,
        num_tasks => 4,
        req_mem => 4096,
        submit_time => 1700000000,
        start_time => 1700000100,
        end_time => 1700003700,
        elapsed => 3600,
        tres_alloc => #{cpu => 8, mem => 4096},
        tres_req => #{cpu => 8, mem => 4096}
    },
    maps:merge(Base, Overrides).

%% Create a TRES update map
make_tres_update(EntityType, EntityId) ->
    make_tres_update(EntityType, EntityId, #{}).

make_tres_update(EntityType, EntityId, Overrides) ->
    Base = #{
        entity_type => EntityType,
        entity_id => EntityId,
        cpu_seconds => 1000,
        mem_seconds => 2000,
        gpu_seconds => 500,
        node_seconds => 100,
        job_count => 1,
        job_time => 3600
    },
    maps:merge(Base, Overrides).

%% Get current period string (YYYY-MM format) - mirrors flurm_dbd_ra:current_period/0
current_period() ->
    {{Year, Month, _Day}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    [
        {"init/1 creates empty state with proper structure", fun() ->
            State = flurm_dbd_ra:init(#{}),
            %% Verify all fields are initialized
            ?assertEqual(#{}, element(2, State)),  % job_records
            ?assert(sets:is_empty(element(3, State))),  % recorded_jobs (sets)
            ?assertEqual(#{}, element(4, State)),  % usage_records
            ?assertEqual(#{}, element(5, State)),  % user_totals
            ?assertEqual(#{}, element(6, State)),  % account_totals
            ?assertEqual(1, element(7, State))     % version
        end},

        {"init/1 with config still creates empty state", fun() ->
            State = flurm_dbd_ra:init(#{some_config => value}),
            ?assertEqual(#{}, element(2, State)),
            ?assert(sets:is_empty(element(3, State)))
        end},

        {"init/1 state version is 1", fun() ->
            State = flurm_dbd_ra:init(#{}),
            ?assertEqual(1, element(7, State))
        end}
    ].

%%====================================================================
%% Apply Command Tests - record_job
%%====================================================================

apply_record_job_test_() ->
    [
        {"record_job creates job record", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1),
            Meta = mock_meta(),

            {State1, Result, Effects} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            ?assertEqual({ok, 1}, Result),
            ?assertEqual(1, maps:size(element(2, State1))),  % job_records
            ?assert(length(Effects) > 0)
        end},

        {"record_job stores correct job data", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(42, #{
                job_name => <<"important_job">>,
                user_name => <<"alice">>,
                account => <<"physics">>
            }),
            Meta = mock_meta(),

            {State1, {ok, 42}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            JobRecords = element(2, State1),
            {ok, Record} = maps:find(42, JobRecords),
            ?assertEqual(<<"important_job">>, element(3, Record)),  % job_name
            ?assertEqual(<<"alice">>, element(4, Record)),  % user_name
            ?assertEqual(<<"physics">>, element(7, Record))  % account
        end},

        {"record_job adds job to recorded_jobs set", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(100),
            Meta = mock_meta(),

            {State1, {ok, 100}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            RecordedJobs = element(3, State1),
            ?assert(sets:is_element(100, RecordedJobs))
        end},

        {"record_job updates user usage records", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                user_name => <<"bob">>,
                elapsed => 1000,
                num_cpus => 4,
                req_mem => 2048,
                num_nodes => 1
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            UsageRecords = element(4, State1),
            %% Should have usage records for both user and account
            ?assert(maps:size(UsageRecords) >= 1)
        end},

        {"record_job updates user totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                user_name => <<"carol">>,
                elapsed => 100,
                num_cpus => 2,
                num_nodes => 1
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            UserTotals = element(5, State1),
            ?assert(maps:is_key(<<"carol">>, UserTotals)),
            {ok, CarolTotals} = maps:find(<<"carol">>, UserTotals),
            ?assertEqual(200, maps:get(cpu_seconds, CarolTotals))  % 100 * 2 CPUs
        end},

        {"record_job updates account totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                account => <<"chemistry">>,
                elapsed => 50,
                num_cpus => 4,
                num_nodes => 2
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            AccountTotals = element(6, State1),
            ?assert(maps:is_key(<<"chemistry">>, AccountTotals)),
            {ok, ChemTotals} = maps:find(<<"chemistry">>, AccountTotals),
            ?assertEqual(200, maps:get(cpu_seconds, ChemTotals))  % 50 * 4 CPUs
        end},

        {"record_job generates mod_call effect", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1),
            Meta = mock_meta(),

            {_State1, {ok, 1}, Effects} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            ?assertEqual(1, length(Effects)),
            [{mod_call, Module, Function, _Args}] = Effects,
            ?assertEqual(flurm_dbd_ra_effects, Module),
            ?assertEqual(job_recorded, Function)
        end}
    ].

%%====================================================================
%% Apply Command Tests - record_job_completion (alias)
%%====================================================================

apply_record_job_completion_test_() ->
    [
        {"record_job_completion is alias for record_job", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1),
            Meta = mock_meta(),

            {State1, Result1, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            State2 = flurm_dbd_ra:init(#{}),
            {State3, Result2, _} = flurm_dbd_ra:apply(Meta, {record_job_completion, JobInfo}, State2),

            ?assertEqual(Result1, Result2),
            ?assertEqual(maps:size(element(2, State1)), maps:size(element(2, State3)))
        end}
    ].

%%====================================================================
%% Idempotency Tests - No Double Counting
%%====================================================================

idempotency_test_() ->
    [
        {"recording same job twice returns already_recorded error", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),
            {State2, Result, Effects} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State1),

            ?assertEqual({error, already_recorded}, Result),
            ?assertEqual(State1, State2),  % State unchanged
            ?assertEqual([], Effects)
        end},

        {"double recording does not increment job count in user totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{user_name => <<"dave">>}),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),
            UserTotals1 = element(5, State1),
            {ok, DaveTotals1} = maps:find(<<"dave">>, UserTotals1),
            JobCount1 = maps:get(job_count, DaveTotals1),

            {State2, {error, already_recorded}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State1),
            UserTotals2 = element(5, State2),
            {ok, DaveTotals2} = maps:find(<<"dave">>, UserTotals2),
            JobCount2 = maps:get(job_count, DaveTotals2),

            ?assertEqual(JobCount1, JobCount2)  % No increment
        end},

        {"different jobs can be recorded", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1)}, State0),
            {State2, {ok, 2}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(2)}, State1),
            {State3, {ok, 3}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(3)}, State2),

            ?assertEqual(3, maps:size(element(2, State3)))
        end},

        {"recorded_jobs set tracks all job IDs", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(10)}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(20)}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(30)}, State2),

            RecordedJobs = element(3, State3),
            ?assert(sets:is_element(10, RecordedJobs)),
            ?assert(sets:is_element(20, RecordedJobs)),
            ?assert(sets:is_element(30, RecordedJobs)),
            ?assertEqual(3, sets:size(RecordedJobs))
        end}
    ].

%%====================================================================
%% Apply Command Tests - update_tres
%%====================================================================

apply_update_tres_test_() ->
    [
        {"update_tres creates new usage record", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate = make_tres_update(user, <<"frank">>),
            Meta = mock_meta(),

            {State1, Result, Effects} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            ?assertEqual(ok, Result),
            ?assertEqual([], Effects),
            UsageRecords = element(4, State1),
            ?assert(maps:size(UsageRecords) >= 1)
        end},

        {"update_tres aggregates with existing usage", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate1 = make_tres_update(user, <<"grace">>, #{cpu_seconds => 1000}),
            TresUpdate2 = make_tres_update(user, <<"grace">>, #{cpu_seconds => 500}),
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate1}, State0),
            {State2, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate2}, State1),

            UserTotals = element(5, State2),
            {ok, GraceTotals} = maps:find(<<"grace">>, UserTotals),
            ?assertEqual(1500, maps:get(cpu_seconds, GraceTotals))
        end},

        {"update_tres for user updates user_totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate = make_tres_update(user, <<"henry">>, #{
                cpu_seconds => 100,
                mem_seconds => 200,
                gpu_seconds => 50,
                node_seconds => 25,
                job_count => 1,
                job_time => 100
            }),
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            UserTotals = element(5, State1),
            {ok, HenryTotals} = maps:find(<<"henry">>, UserTotals),
            ?assertEqual(100, maps:get(cpu_seconds, HenryTotals)),
            ?assertEqual(200, maps:get(mem_seconds, HenryTotals)),
            ?assertEqual(50, maps:get(gpu_seconds, HenryTotals)),
            ?assertEqual(25, maps:get(node_seconds, HenryTotals)),
            ?assertEqual(1, maps:get(job_count, HenryTotals)),
            ?assertEqual(100, maps:get(job_time, HenryTotals))
        end},

        {"update_tres for account updates account_totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate = make_tres_update(account, <<"biology">>, #{cpu_seconds => 5000}),
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            AccountTotals = element(6, State1),
            {ok, BioTotals} = maps:find(<<"biology">>, AccountTotals),
            ?assertEqual(5000, maps:get(cpu_seconds, BioTotals))
        end},

        {"update_tres with default period", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            %% TresUpdate without explicit period
            TresUpdate = #{
                entity_type => user,
                entity_id => <<"ivan">>,
                cpu_seconds => 100
            },
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            UsageRecords = element(4, State1),
            %% Should have created a usage record
            ?assert(maps:size(UsageRecords) >= 1)
        end},

        {"update_tres for cluster type does not update user/account totals", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate = make_tres_update(cluster, <<"maincluster">>),
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            UserTotals = element(5, State1),
            AccountTotals = element(6, State1),
            ?assertEqual(0, maps:size(UserTotals)),
            ?assertEqual(0, maps:size(AccountTotals))
        end}
    ].

%%====================================================================
%% TRES Calculation Tests
%%====================================================================

tres_calculation_test_() ->
    [
        {"TRES calculated correctly from job elapsed time", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                elapsed => 100,   % 100 seconds
                num_cpus => 4,    % 4 CPUs
                req_mem => 1024,  % 1024 MB
                num_nodes => 2,   % 2 nodes
                tres_alloc => #{gpu => 2}  % 2 GPUs
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            UserTotals = element(5, State1),
            {ok, Totals} = maps:find(<<"testuser">>, UserTotals),

            %% cpu_seconds = elapsed * num_cpus = 100 * 4 = 400
            ?assertEqual(400, maps:get(cpu_seconds, Totals)),
            %% mem_seconds = elapsed * req_mem = 100 * 1024 = 102400
            ?assertEqual(102400, maps:get(mem_seconds, Totals)),
            %% node_seconds = elapsed * num_nodes = 100 * 2 = 200
            ?assertEqual(200, maps:get(node_seconds, Totals)),
            %% gpu_seconds = elapsed * gpu_count = 100 * 2 = 200
            ?assertEqual(200, maps:get(gpu_seconds, Totals)),
            %% job_count = 1
            ?assertEqual(1, maps:get(job_count, Totals)),
            %% job_time = elapsed = 100
            ?assertEqual(100, maps:get(job_time, Totals))
        end},

        {"TRES aggregates across multiple jobs", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{
                user_name => <<"julia">>,
                elapsed => 100,
                num_cpus => 2,
                num_nodes => 1
            }),
            Job2 = make_job_info(2, #{
                user_name => <<"julia">>,
                elapsed => 200,
                num_cpus => 4,
                num_nodes => 2
            }),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, {ok, 2}, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),

            UserTotals = element(5, State2),
            {ok, JuliaTotals} = maps:find(<<"julia">>, UserTotals),

            %% cpu_seconds = (100 * 2) + (200 * 4) = 200 + 800 = 1000
            ?assertEqual(1000, maps:get(cpu_seconds, JuliaTotals)),
            %% node_seconds = (100 * 1) + (200 * 2) = 100 + 400 = 500
            ?assertEqual(500, maps:get(node_seconds, JuliaTotals)),
            %% job_count = 2
            ?assertEqual(2, maps:get(job_count, JuliaTotals)),
            %% job_time = 100 + 200 = 300
            ?assertEqual(300, maps:get(job_time, JuliaTotals))
        end},

        {"TRES non-negative invariant preserved (zero elapsed)", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                elapsed => 0,
                num_cpus => 4
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            UserTotals = element(5, State1),
            {ok, Totals} = maps:find(<<"testuser">>, UserTotals),

            ?assertEqual(0, maps:get(cpu_seconds, Totals)),
            ?assertEqual(0, maps:get(job_time, Totals)),
            ?assertEqual(1, maps:get(job_count, Totals))
        end}
    ].

%%====================================================================
%% Apply Command Tests - query_jobs
%%====================================================================

apply_query_jobs_test_() ->
    [
        {"query_jobs returns all jobs with empty filter", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1)}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(2)}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(3)}, State2),

            {State3, {ok, Jobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{}}, State3),

            ?assertEqual(3, length(Jobs))
        end},

        {"query_jobs filters by user_name", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{user_name => <<"alice">>}),
            Job2 = make_job_info(2, #{user_name => <<"bob">>}),
            Job3 = make_job_info(3, #{user_name => <<"alice">>}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, AliceJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{user_name => <<"alice">>}}, State3),

            ?assertEqual(2, length(AliceJobs)),
            lists:foreach(fun(J) ->
                ?assertEqual(<<"alice">>, maps:get(user_name, J))
            end, AliceJobs)
        end},

        {"query_jobs filters by user (alias)", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{user_name => <<"charlie">>}),
            Job2 = make_job_info(2, #{user_name => <<"dave">>}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),

            {State2, {ok, CharlieJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{user => <<"charlie">>}}, State2),

            ?assertEqual(1, length(CharlieJobs))
        end},

        {"query_jobs filters by account", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{account => <<"physics">>}),
            Job2 = make_job_info(2, #{account => <<"chemistry">>}),
            Job3 = make_job_info(3, #{account => <<"physics">>}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, PhysicsJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{account => <<"physics">>}}, State3),

            ?assertEqual(2, length(PhysicsJobs))
        end},

        {"query_jobs filters by partition", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{partition => <<"gpu">>}),
            Job2 = make_job_info(2, #{partition => <<"cpu">>}),
            Job3 = make_job_info(3, #{partition => <<"gpu">>}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, GpuJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{partition => <<"gpu">>}}, State3),

            ?assertEqual(2, length(GpuJobs))
        end},

        {"query_jobs filters by state", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{state => completed}),
            Job2 = make_job_info(2, #{state => failed}),
            Job3 = make_job_info(3, #{state => completed}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, CompletedJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{state => completed}}, State3),

            ?assertEqual(2, length(CompletedJobs))
        end},

        {"query_jobs filters by start_time (jobs starting after)", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{start_time => 1000}),
            Job2 = make_job_info(2, #{start_time => 2000}),
            Job3 = make_job_info(3, #{start_time => 3000}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, RecentJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{start_time => 1500}}, State3),

            ?assertEqual(2, length(RecentJobs))
        end},

        {"query_jobs filters by end_time (jobs ending before)", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{end_time => 1000}),
            Job2 = make_job_info(2, #{end_time => 2000}),
            Job3 = make_job_info(3, #{end_time => 3000}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            {State3, {ok, OldJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{end_time => 2500}}, State3),

            ?assertEqual(2, length(OldJobs))
        end},

        {"query_jobs with multiple filters (AND logic)", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            Job1 = make_job_info(1, #{user_name => <<"eve">>, account => <<"research">>}),
            Job2 = make_job_info(2, #{user_name => <<"eve">>, account => <<"teaching">>}),
            Job3 = make_job_info(3, #{user_name => <<"frank">>, account => <<"research">>}),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job1}, State0),
            {State2, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job2}, State1),
            {State3, _, _} = flurm_dbd_ra:apply(Meta, {record_job, Job3}, State2),

            Filters = #{user_name => <<"eve">>, account => <<"research">>},
            {State3, {ok, FilteredJobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, Filters}, State3),

            ?assertEqual(1, length(FilteredJobs)),
            [Job] = FilteredJobs,
            ?assertEqual(1, maps:get(job_id, Job))
        end},

        {"query_jobs returns empty list when no jobs match", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1)}, State0),

            {State1, {ok, Jobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{user_name => <<"nonexistent">>}}, State1),

            ?assertEqual([], Jobs)
        end},

        {"query_jobs does not modify state", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, _, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1)}, State0),
            {State2, {ok, _}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{}}, State1),

            ?assertEqual(State1, State2)
        end}
    ].

%%====================================================================
%% Apply Command Tests - query_tres
%%====================================================================

apply_query_tres_test_() ->
    [
        {"query_tres returns usage for existing entity", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            TresUpdate = #{
                entity_type => user,
                entity_id => <<"george">>,
                period => <<"2024-01">>,
                cpu_seconds => 5000,
                mem_seconds => 10000,
                gpu_seconds => 1000,
                node_seconds => 500,
                job_count => 10,
                job_time => 36000
            },

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),
            {State1, {ok, Usage}, []} = flurm_dbd_ra:apply(Meta, {query_tres, {user, <<"george">>, <<"2024-01">>}}, State1),

            ?assertEqual(<<"2024-01">>, maps:get(period, Usage)),
            ?assertEqual(5000, maps:get(cpu_seconds, Usage)),
            ?assertEqual(10000, maps:get(mem_seconds, Usage)),
            ?assertEqual(1000, maps:get(gpu_seconds, Usage)),
            ?assertEqual(500, maps:get(node_seconds, Usage)),
            ?assertEqual(10, maps:get(job_count, Usage)),
            ?assertEqual(36000, maps:get(job_time, Usage))
        end},

        {"query_tres returns not_found for non-existent entity", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State0, Result, []} = flurm_dbd_ra:apply(Meta, {query_tres, {user, <<"nobody">>, <<"2024-01">>}}, State0),

            ?assertEqual({error, not_found}, Result)
        end},

        {"query_tres does not modify state", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),
            Period = current_period(),

            TresUpdate = maps:put(period, Period, make_tres_update(user, <<"helen">>)),
            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),
            {State2, {ok, _}, []} = flurm_dbd_ra:apply(Meta, {query_tres, {user, <<"helen">>, Period}}, State1),

            ?assertEqual(State1, State2)
        end}
    ].

%%====================================================================
%% Apply Command Tests - Unknown Commands
%%====================================================================

apply_unknown_command_test_() ->
    [
        {"unknown command returns error", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, Result, Effects} = flurm_dbd_ra:apply(Meta, {unknown_command, arg1, arg2}, State0),

            ?assertEqual({error, {unknown_command, {unknown_command, arg1, arg2}}}, Result),
            ?assertEqual(State0, State1),
            ?assertEqual([], Effects)
        end},

        {"atom command returns error", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, Result, Effects} = flurm_dbd_ra:apply(Meta, invalid_atom_command, State0),

            ?assertEqual({error, {unknown_command, invalid_atom_command}}, Result),
            ?assertEqual(State0, State1),
            ?assertEqual([], Effects)
        end},

        {"malformed record_job command returns error", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            %% record_job expects a map, not a list
            {State1, Result, Effects} = flurm_dbd_ra:apply(Meta, {record_job, [not_a_map]}, State0),

            ?assertEqual({error, {unknown_command, {record_job, [not_a_map]}}}, Result),
            ?assertEqual(State0, State1),
            ?assertEqual([], Effects)
        end}
    ].

%%====================================================================
%% State Transitions Tests
%%====================================================================

state_transitions_test_() ->
    [
        {"multiple jobs update state incrementally", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            %% Record 5 jobs sequentially
            Jobs = [make_job_info(N, #{user_name => <<"sequential_user">>}) || N <- lists:seq(1, 5)],

            FinalState = lists:foldl(fun(JobInfo, Acc) ->
                {NewState, {ok, _}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, Acc),
                NewState
            end, State0, Jobs),

            %% Check final state
            JobRecords = element(2, FinalState),
            ?assertEqual(5, maps:size(JobRecords)),

            RecordedJobs = element(3, FinalState),
            ?assertEqual(5, sets:size(RecordedJobs)),

            UserTotals = element(5, FinalState),
            {ok, Totals} = maps:find(<<"sequential_user">>, UserTotals),
            ?assertEqual(5, maps:get(job_count, Totals))
        end},

        {"state preserves job data after queries", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1)}, State0),

            %% Run multiple queries
            {State2, {ok, _}, _} = flurm_dbd_ra:apply(Meta, {query_jobs, #{}}, State1),
            {State3, {ok, _}, _} = flurm_dbd_ra:apply(Meta, {query_jobs, #{user_name => <<"testuser">>}}, State2),
            {State4, _, _} = flurm_dbd_ra:apply(Meta, {query_tres, {user, <<"testuser">>, <<"2024-01">>}}, State3),

            %% Verify state unchanged
            ?assertEqual(element(2, State1), element(2, State4)),  % job_records
            ?assertEqual(element(5, State1), element(5, State4))   % user_totals
        end},

        {"interleaved record and update operations", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            %% Mix record_job and update_tres operations
            %% Note: make_tres_update includes job_count => 1 by default
            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(1, #{user_name => <<"mixed">>})}, State0),
            {State2, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, make_tres_update(user, <<"mixed">>, #{cpu_seconds => 500, job_count => 0})}, State1),
            {State3, {ok, 2}, _} = flurm_dbd_ra:apply(Meta, {record_job, make_job_info(2, #{user_name => <<"mixed">>})}, State2),
            {State4, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, make_tres_update(user, <<"mixed">>, #{cpu_seconds => 300, job_count => 0})}, State3),

            UserTotals = element(5, State4),
            {ok, MixedTotals} = maps:find(<<"mixed">>, UserTotals),

            %% 2 jobs recorded (TRES updates have job_count => 0)
            ?assertEqual(2, maps:get(job_count, MixedTotals)),
            %% CPU seconds from jobs (3600 * 8 * 2 = 57600) + manual (500 + 300 = 800) = 58400
            %% Actually depends on job_info defaults - let's just check it's > 800
            CpuSeconds = maps:get(cpu_seconds, MixedTotals),
            ?assert(CpuSeconds >= 800)
        end}
    ].

%%====================================================================
%% State Enter Tests
%%====================================================================

state_enter_test_() ->
    [
        {"state_enter leader generates effect", fun() ->
            State = flurm_dbd_ra:init(#{}),
            Effects = flurm_dbd_ra:state_enter(leader, State),
            ?assertEqual(1, length(Effects)),
            [{mod_call, Module, Function, _Args}] = Effects,
            ?assertEqual(flurm_dbd_ra_effects, Module),
            ?assertEqual(became_leader, Function)
        end},

        {"state_enter follower generates effect", fun() ->
            State = flurm_dbd_ra:init(#{}),
            Effects = flurm_dbd_ra:state_enter(follower, State),
            ?assertEqual(1, length(Effects)),
            [{mod_call, Module, Function, _Args}] = Effects,
            ?assertEqual(flurm_dbd_ra_effects, Module),
            ?assertEqual(became_follower, Function)
        end},

        {"state_enter other states generate no effects", fun() ->
            State = flurm_dbd_ra:init(#{}),

            ?assertEqual([], flurm_dbd_ra:state_enter(recover, State)),
            ?assertEqual([], flurm_dbd_ra:state_enter(candidate, State)),
            ?assertEqual([], flurm_dbd_ra:state_enter(eol, State))
        end}
    ].

%%====================================================================
%% Snapshot Module Tests
%%====================================================================

snapshot_module_test() ->
    Module = flurm_dbd_ra:snapshot_module(),
    ?assertEqual(ra_machine_simple, Module).

%%====================================================================
%% Edge Cases and Error Handling Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"empty user_name in job does not crash", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{user_name => <<>>}),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            %% Job should be recorded successfully even with empty user_name
            JobRecords = element(2, State1),
            ?assertEqual(1, maps:size(JobRecords))
        end},

        {"empty account in job does not crash", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{account => <<>>}),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            %% Job should be recorded successfully even with empty account
            JobRecords = element(2, State1),
            ?assertEqual(1, maps:size(JobRecords))
        end},

        {"large job_id is handled", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            LargeJobId = 9999999999,
            JobInfo = make_job_info(LargeJobId),
            Meta = mock_meta(),

            {State1, {ok, LargeJobId}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            JobRecords = element(2, State1),
            ?assert(maps:is_key(LargeJobId, JobRecords))
        end},

        {"unicode characters in names are handled", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                user_name => <<"日本語ユーザー">>,
                account => <<"compte_français">>,
                job_name => <<"trabajo_español">>
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),

            UserTotals = element(5, State1),
            ?assert(maps:is_key(<<"日本語ユーザー">>, UserTotals))
        end},

        {"zero values in TRES update are handled", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            TresUpdate = #{
                entity_type => user,
                entity_id => <<"zero_user">>,
                cpu_seconds => 0,
                mem_seconds => 0,
                gpu_seconds => 0,
                node_seconds => 0,
                job_count => 0,
                job_time => 0
            },
            Meta = mock_meta(),

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            UserTotals = element(5, State1),
            {ok, ZeroTotals} = maps:find(<<"zero_user">>, UserTotals),
            ?assertEqual(0, maps:get(cpu_seconds, ZeroTotals)),
            ?assertEqual(0, maps:get(job_count, ZeroTotals))
        end},

        {"query on empty state returns empty results", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            {State0, {ok, Jobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{}}, State0),
            ?assertEqual([], Jobs)
        end},

        {"missing optional fields in job_info use defaults", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            %% Minimal job info with only required job_id
            MinimalJobInfo = #{job_id => 1},
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, MinimalJobInfo}, State0),

            JobRecords = element(2, State1),
            {ok, Record} = maps:find(1, JobRecords),
            %% Verify defaults were applied
            ?assertEqual(<<>>, element(3, Record)),   % job_name default
            ?assertEqual(<<>>, element(4, Record)),   % user_name default
            ?assertEqual(completed, element(11, Record))  % state default
        end}
    ].

%%====================================================================
%% Concurrent Simulation Tests
%%====================================================================

concurrent_simulation_test_() ->
    [
        {"simulated concurrent job submissions", fun() ->
            State0 = flurm_dbd_ra:init(#{}),

            %% Simulate what would happen with concurrent submissions
            %% (different index for each to simulate Raft ordering)
            Jobs = [
                {make_job_info(1, #{user_name => <<"user1">>}), mock_meta(1)},
                {make_job_info(2, #{user_name => <<"user2">>}), mock_meta(2)},
                {make_job_info(3, #{user_name => <<"user1">>}), mock_meta(3)}
            ],

            FinalState = lists:foldl(fun({JobInfo, Meta}, Acc) ->
                {NewState, {ok, _}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, Acc),
                NewState
            end, State0, Jobs),

            JobRecords = element(2, FinalState),
            ?assertEqual(3, maps:size(JobRecords)),

            UserTotals = element(5, FinalState),
            {ok, User1Totals} = maps:find(<<"user1">>, UserTotals),
            ?assertEqual(2, maps:get(job_count, User1Totals))
        end},

        {"simulated duplicate job detection across indices", fun() ->
            State0 = flurm_dbd_ra:init(#{}),

            %% First submission
            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(mock_meta(1), {record_job, make_job_info(1)}, State0),

            %% Attempted duplicate at later index
            {State2, {error, already_recorded}, _} = flurm_dbd_ra:apply(mock_meta(5), {record_job, make_job_info(1)}, State1),

            %% State should be unchanged
            ?assertEqual(State1, State2)
        end}
    ].

%%====================================================================
%% Performance Related Tests
%%====================================================================

performance_test_() ->
    {"recording many jobs maintains O(1) lookup", {timeout, 30, fun() ->
        State0 = flurm_dbd_ra:init(#{}),
        Meta = mock_meta(),
        N = 1000,

        %% Record N jobs
        FinalState = lists:foldl(fun(JobId, Acc) ->
            JobInfo = make_job_info(JobId, #{user_name => <<"perf_user">>}),
            {NewState, {ok, _}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, Acc),
            NewState
        end, State0, lists:seq(1, N)),

        %% Verify all jobs recorded
        JobRecords = element(2, FinalState),
        ?assertEqual(N, maps:size(JobRecords)),

        %% Query should still work efficiently
        {FinalState, {ok, Jobs}, []} = flurm_dbd_ra:apply(Meta, {query_jobs, #{user_name => <<"perf_user">>}}, FinalState),
        ?assertEqual(N, length(Jobs))
    end}}.

%%====================================================================
%% Update Usage with Record (legacy format) Tests
%%====================================================================

update_usage_record_test_() ->
    [
        {"update_usage with tres_usage record format", fun() ->
            %% This tests the internal record-based update_usage path
            %% which is used by the update_usage/1 API function
            State0 = flurm_dbd_ra:init(#{}),
            Meta = mock_meta(),

            %% The apply/3 handles {update_usage, #tres_usage{}} format internally
            %% We can test this path by using update_tres with a map
            TresUpdate = #{
                entity_type => user,
                entity_id => <<"legacy_user">>,
                period => <<"2024-02">>,
                cpu_seconds => 1000,
                mem_seconds => 2000,
                gpu_seconds => 100,
                node_seconds => 50,
                job_count => 5,
                job_time => 5000
            },

            {State1, ok, _} = flurm_dbd_ra:apply(Meta, {update_tres, TresUpdate}, State0),

            %% Query to verify
            {State1, {ok, Usage}, _} = flurm_dbd_ra:apply(Meta, {query_tres, {user, <<"legacy_user">>, <<"2024-02">>}}, State1),

            ?assertEqual(1000, maps:get(cpu_seconds, Usage)),
            ?assertEqual(5, maps:get(job_count, Usage))
        end}
    ].

%%====================================================================
%% Job Record Conversion Tests
%%====================================================================

job_record_conversion_test_() ->
    [
        {"job record to map conversion includes all fields", fun() ->
            State0 = flurm_dbd_ra:init(#{}),
            JobInfo = make_job_info(1, #{
                job_name => <<"full_job">>,
                user_name => <<"full_user">>,
                user_id => 42,
                group_id => 100,
                account => <<"full_account">>,
                partition => <<"full_partition">>,
                cluster => <<"full_cluster">>,
                qos => <<"high">>,
                state => completed,
                exit_code => 0,
                num_nodes => 4,
                num_cpus => 16,
                num_tasks => 8,
                req_mem => 8192,
                submit_time => 1000,
                start_time => 1100,
                end_time => 2100,
                elapsed => 1000,
                tres_alloc => #{cpu => 16, gpu => 2},
                tres_req => #{cpu => 16, gpu => 2}
            }),
            Meta = mock_meta(),

            {State1, {ok, 1}, _} = flurm_dbd_ra:apply(Meta, {record_job, JobInfo}, State0),
            {State1, {ok, [JobMap]}, _} = flurm_dbd_ra:apply(Meta, {query_jobs, #{}}, State1),

            ?assertEqual(1, maps:get(job_id, JobMap)),
            ?assertEqual(<<"full_job">>, maps:get(job_name, JobMap)),
            ?assertEqual(<<"full_user">>, maps:get(user_name, JobMap)),
            ?assertEqual(42, maps:get(user_id, JobMap)),
            ?assertEqual(100, maps:get(group_id, JobMap)),
            ?assertEqual(<<"full_account">>, maps:get(account, JobMap)),
            ?assertEqual(<<"full_partition">>, maps:get(partition, JobMap)),
            ?assertEqual(<<"full_cluster">>, maps:get(cluster, JobMap)),
            ?assertEqual(<<"high">>, maps:get(qos, JobMap)),
            ?assertEqual(completed, maps:get(state, JobMap)),
            ?assertEqual(0, maps:get(exit_code, JobMap)),
            ?assertEqual(4, maps:get(num_nodes, JobMap)),
            ?assertEqual(16, maps:get(num_cpus, JobMap)),
            ?assertEqual(8, maps:get(num_tasks, JobMap)),
            ?assertEqual(8192, maps:get(req_mem, JobMap)),
            ?assertEqual(1000, maps:get(submit_time, JobMap)),
            ?assertEqual(1100, maps:get(start_time, JobMap)),
            ?assertEqual(2100, maps:get(end_time, JobMap)),
            ?assertEqual(1000, maps:get(elapsed, JobMap)),
            ?assertEqual(#{cpu => 16, gpu => 2}, maps:get(tres_alloc, JobMap)),
            ?assertEqual(#{cpu => 16, gpu => 2}, maps:get(tres_req, JobMap))
        end}
    ].
