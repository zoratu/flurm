%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_dbd_server
%%%
%%% Tests gen_server callbacks directly without mocking.
%%% NO MECK USAGE - all tests are pure unit tests.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup fixture for tests requiring initialized state
setup() ->
    %% Suppress lager output during tests
    application:load(lager),
    application:set_env(lager, handlers, []),
    application:set_env(lager, crash_log, false),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"init creates all required ETS tables",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            %% Verify state record fields are set
            ?assert(is_reference(element(2, State))),  % job_records
            ?assert(is_reference(element(3, State))),  % step_records
            ?assert(is_reference(element(4, State))),  % usage_records
            ?assert(is_reference(element(5, State))),  % usage_by_user
            ?assert(is_reference(element(6, State))),  % usage_by_account
            ?assert(is_reference(element(7, State))),  % associations
            ?assert(is_reference(element(8, State))),  % archive
            ?assertEqual(1, element(9, State)),        % next_assoc_id
            ?assert(is_map(element(10, State))),       % stats
            %% Clean up
            cleanup_state(State)
        end},

       {"init sets correct initial stats",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            Stats = element(10, State),
            ?assertEqual(0, maps:get(jobs_submitted, Stats)),
            ?assertEqual(0, maps:get(jobs_started, Stats)),
            ?assertEqual(0, maps:get(jobs_completed, Stats)),
            ?assertEqual(0, maps:get(jobs_failed, Stats)),
            ?assertEqual(0, maps:get(jobs_cancelled, Stats)),
            ?assertEqual(0, maps:get(steps_recorded, Stats)),
            ?assertEqual(0, maps:get(associations_count, Stats)),
            ?assertEqual(0, maps:get(archived_jobs, Stats)),
            ?assert(is_integer(maps:get(start_time, Stats))),
            cleanup_state(State)
        end},

       {"init sends start_listener message to self",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            %% Check that start_listener message was sent
            receive
                start_listener -> ok
            after 100 ->
                ?assert(false)
            end,
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Job Records
%%====================================================================

handle_call_job_records_test_() ->
    {"handle_call job record tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"get_job_record returns not_found for missing job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_job_record, 999}, {self(), make_ref()}, State),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State)
        end},

       {"list_job_records returns empty list initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                list_job_records, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"list_job_records with filters returns empty list initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            Filters = #{user => <<"testuser">>},
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {list_job_records, Filters}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Usage
%%====================================================================

handle_call_usage_test_() ->
    {"handle_call usage tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"get_user_usage returns zeros for unknown user",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_user_usage, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end},

       {"get_account_usage returns zeros for unknown account",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_account_usage, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end},

       {"get_cluster_usage returns zeros initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                get_cluster_usage, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end},

       {"reset_usage clears usage records",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                reset_usage, {self(), make_ref()}, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - TRES Usage
%%====================================================================

handle_call_tres_usage_test_() ->
    {"handle_call TRES usage tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"calculate_user_tres_usage returns zeros for unknown user",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {calculate_user_tres_usage, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(mem_seconds, Result)),
            ?assertEqual(0, maps:get(gpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end},

       {"calculate_account_tres_usage returns zeros for unknown account",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {calculate_account_tres_usage, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(mem_seconds, Result)),
            ?assertEqual(0, maps:get(gpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end},

       {"calculate_cluster_tres_usage returns zeros initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                calculate_cluster_tres_usage, {self(), make_ref()}, State),
            ?assertEqual(0, maps:get(cpu_seconds, Result)),
            ?assertEqual(0, maps:get(mem_seconds, Result)),
            ?assertEqual(0, maps:get(gpu_seconds, Result)),
            ?assertEqual(0, maps:get(node_seconds, Result)),
            ?assertEqual(0, maps:get(job_count, Result)),
            ?assertEqual(0, maps:get(elapsed_total, Result)),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Archive/Purge
%%====================================================================

handle_call_archive_purge_test_() ->
    {"handle_call archive/purge tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"archive_old_records returns count of zero initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {archive_old_records, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       {"purge_old_records returns count of zero initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {purge_old_records, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       {"archive_old_jobs returns count of zero initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       {"purge_old_jobs returns count of zero initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {purge_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       {"get_archived_jobs returns empty list initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                get_archived_jobs, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"get_archived_jobs with filters returns empty list initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_archived_jobs, #{user => <<"test">>}}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Stats
%%====================================================================

handle_call_stats_test_() ->
    {"handle_call stats tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"get_stats returns proper stats map",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                get_stats, {self(), make_ref()}, State),
            ?assert(is_map(Result)),
            ?assertEqual(0, maps:get(jobs_submitted, Result)),
            ?assertEqual(0, maps:get(jobs_started, Result)),
            ?assertEqual(0, maps:get(jobs_completed, Result)),
            ?assertEqual(0, maps:get(jobs_failed, Result)),
            ?assertEqual(0, maps:get(jobs_cancelled, Result)),
            ?assertEqual(0, maps:get(steps_recorded, Result)),
            ?assertEqual(0, maps:get(total_job_records, Result)),
            ?assertEqual(0, maps:get(total_step_records, Result)),
            ?assertEqual(0, maps:get(total_associations, Result)),
            ?assertEqual(0, maps:get(total_archived, Result)),
            ?assert(is_integer(maps:get(uptime_seconds, Result))),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - sacct-style Queries
%%====================================================================

handle_call_sacct_queries_test_() ->
    {"handle_call sacct-style query tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"get_jobs_by_user returns empty list for unknown user",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_jobs_by_user, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"get_jobs_by_account returns empty list for unknown account",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_jobs_by_account, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"get_jobs_in_range returns empty list initially",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            Now = erlang:system_time(second),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_jobs_in_range, Now - 3600, Now}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Association Management
%%====================================================================

handle_call_associations_test_() ->
    {"handle_call association management tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"add_association creates new association",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            ?assertEqual({ok, 1}, Result),
            %% Verify next_assoc_id was incremented
            ?assertEqual(2, element(9, NewState)),
            cleanup_state(NewState)
        end},

       {"add_association with options",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            Opts = #{
                partition => <<"gpu">>,
                shares => 10,
                max_jobs => 100,
                max_submit => 200,
                max_wall => 86400,
                grp_tres => #{cpu => 100},
                max_tres_per_job => #{cpu => 10},
                qos => [<<"normal">>, <<"high">>],
                default_qos => <<"normal">>
            },
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, Opts},
                {self(), make_ref()}, State),
            ?assertEqual({ok, 1}, Result),
            cleanup_state(NewState)
        end},

       {"get_associations returns associations for cluster",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add an association first
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            %% Get associations
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_associations, <<"cluster1">>}, {self(), make_ref()}, State2),
            ?assertEqual(1, length(Result)),
            [Assoc] = Result,
            ?assertEqual(<<"cluster1">>, maps:get(cluster, Assoc)),
            ?assertEqual(<<"account1">>, maps:get(account, Assoc)),
            ?assertEqual(<<"user1">>, maps:get(user, Assoc)),
            cleanup_state(State2)
        end},

       {"get_associations returns empty list for unknown cluster",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_associations, <<"unknown">>}, {self(), make_ref()}, State),
            ?assertEqual([], Result),
            cleanup_state(State)
        end},

       {"get_association returns association by id",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add an association first
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            %% Get association by id
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_association, 1}, {self(), make_ref()}, State2),
            ?assertMatch({ok, _}, Result),
            {ok, Assoc} = Result,
            ?assertEqual(1, maps:get(id, Assoc)),
            cleanup_state(State2)
        end},

       {"get_association returns not_found for unknown id",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_association, 999}, {self(), make_ref()}, State),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State)
        end},

       {"remove_association deletes association",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add an association first
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            %% Remove it
            {reply, Result, State3} = flurm_dbd_server:handle_call(
                {remove_association, 1}, {self(), make_ref()}, State2),
            ?assertEqual(ok, Result),
            %% Verify it's gone
            {reply, GetResult, _} = flurm_dbd_server:handle_call(
                {get_association, 1}, {self(), make_ref()}, State3),
            ?assertEqual({error, not_found}, GetResult),
            cleanup_state(State3)
        end},

       {"remove_association returns not_found for unknown id",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {remove_association, 999}, {self(), make_ref()}, State),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State)
        end},

       {"update_association updates association settings",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add an association first
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            %% Update it
            Updates = #{shares => 20, max_jobs => 50},
            {reply, Result, State3} = flurm_dbd_server:handle_call(
                {update_association, 1, Updates}, {self(), make_ref()}, State2),
            ?assertEqual(ok, Result),
            %% Verify updates
            {reply, {ok, Assoc}, _} = flurm_dbd_server:handle_call(
                {get_association, 1}, {self(), make_ref()}, State3),
            ?assertEqual(20, maps:get(shares, Assoc)),
            ?assertEqual(50, maps:get(max_jobs, Assoc)),
            cleanup_state(State3)
        end},

       {"update_association returns not_found for unknown id",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {update_association, 999, #{shares => 10}}, {self(), make_ref()}, State),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(State)
        end},

       {"get_user_associations returns associations for user",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add associations
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            {reply, {ok, 2}, State3} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account2">>, <<"user1">>, #{}},
                {self(), make_ref()}, State2),
            %% Get user associations
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_user_associations, <<"user1">>}, {self(), make_ref()}, State3),
            ?assertEqual(2, length(Result)),
            cleanup_state(State3)
        end},

       {"get_account_associations returns associations for account",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Add associations
            {reply, {ok, 1}, State2} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user1">>, #{}},
                {self(), make_ref()}, State),
            {reply, {ok, 2}, State3} = flurm_dbd_server:handle_call(
                {add_association, <<"cluster1">>, <<"account1">>, <<"user2">>, #{}},
                {self(), make_ref()}, State2),
            %% Get account associations
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {get_account_associations, <<"account1">>}, {self(), make_ref()}, State3),
            ?assertEqual(2, length(Result)),
            cleanup_state(State3)
        end}
      ]}}.

%%====================================================================
%% handle_call/3 Tests - Unknown Request
%%====================================================================

handle_call_unknown_test_() ->
    {"handle_call unknown request tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"unknown request returns error",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {unknown_request, data}, {self(), make_ref()}, State),
            ?assertEqual({error, unknown_request}, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"record_job_start inserts job record",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            JobInfo = #{
                job_id => 1001,
                name => <<"test_job">>,
                user_name => <<"testuser">>,
                user_id => 1000,
                group_id => 1000,
                account => <<"testaccount">>,
                partition => <<"normal">>,
                state => running,
                num_nodes => 2,
                num_cpus => 8
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_start, JobInfo}, State),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(jobs_started, Stats)),
            %% Verify job record exists
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1001}, {self(), make_ref()}, NewState),
            ?assertEqual(1001, maps:get(job_id, Record)),
            ?assertEqual(<<"test_job">>, maps:get(job_name, Record)),
            cleanup_state(NewState)
        end},

       {"record_job_end updates existing job record",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% First record job start
            JobInfo = #{
                job_id => 1002,
                name => <<"test_job">>,
                user_name => <<"testuser">>,
                state => running,
                num_cpus => 4
            },
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_start, JobInfo}, State),
            %% Then record job end
            EndInfo = #{
                job_id => 1002,
                state => completed,
                exit_code => 0,
                end_time => erlang:system_time(second)
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_end, EndInfo}, State2),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(jobs_completed, Stats)),
            cleanup_state(NewState)
        end},

       {"record_job_end with exit code updates failed count",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% First record job start
            JobInfo = #{
                job_id => 1003,
                name => <<"test_job">>,
                state => running
            },
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_start, JobInfo}, State),
            %% Then record job end with failure
            EndInfo = #{
                job_id => 1003,
                state => failed,
                exit_code => 1
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_end, EndInfo}, State2),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(jobs_failed, Stats)),
            cleanup_state(NewState)
        end},

       {"record_job_end creates record if not exists",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% Record job end without prior start
            EndInfo = #{
                job_id => 1004,
                name => <<"orphan_job">>,
                state => completed,
                exit_code => 0
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_end, EndInfo}, State),
            %% Verify job record was created
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1004}, {self(), make_ref()}, NewState),
            ?assertEqual(1004, maps:get(job_id, Record)),
            cleanup_state(NewState)
        end},

       {"record_job_step inserts step record",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            StepInfo = #{
                job_id => 1005,
                step_id => 0,
                name => <<"step0">>,
                state => running,
                num_tasks => 4
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_step, StepInfo}, State),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(steps_recorded, Stats)),
            cleanup_state(NewState)
        end},

       {"record_job_submit creates initial job record",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            JobData = #{
                job_id => 1006,
                user_id => 1000,
                group_id => 1000,
                partition => <<"normal">>,
                num_nodes => 1,
                num_cpus => 4
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(jobs_submitted, Stats)),
            %% Verify job record exists
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1006}, {self(), make_ref()}, NewState),
            ?assertEqual(1006, maps:get(job_id, Record)),
            ?assertEqual(pending, maps:get(state, Record)),
            cleanup_state(NewState)
        end},

       {"record_job_submit handles timestamp tuple format",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            JobData = #{
                job_id => 1007,
                submit_time => {1000, 500, 0}  % MegaSecs, Secs, MicroSecs format
            },
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),
            %% Verify job record exists
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1007}, {self(), make_ref()}, NewState),
            ?assertEqual(1000000500, maps:get(submit_time, Record)),
            cleanup_state(NewState)
        end},

       {"record_job_start_event updates existing job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% First submit job
            JobData = #{job_id => 1008},
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),
            %% Then start it
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_start_event, 1008, [<<"node1">>, <<"node2">>]}, State2),
            %% Verify job record updated
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1008}, {self(), make_ref()}, NewState),
            ?assertEqual(running, maps:get(state, Record)),
            ?assertEqual(2, maps:get(num_nodes, Record)),
            ?assert(maps:get(start_time, Record) > 0),
            cleanup_state(NewState)
        end},

       {"record_job_start_event creates record if not exists",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_start_event, 1009, [<<"node1">>]}, State),
            %% Verify job record was created
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1009}, {self(), make_ref()}, NewState),
            ?assertEqual(1009, maps:get(job_id, Record)),
            ?assertEqual(running, maps:get(state, Record)),
            cleanup_state(NewState)
        end},

       {"record_job_end_event updates existing job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% First submit and start job
            JobData = #{job_id => 1010},
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast(
                {record_job_start_event, 1010, [<<"node1">>]}, State2),
            %% Then end it
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_end_event, 1010, 0, completed}, State3),
            %% Verify job record updated
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1010}, {self(), make_ref()}, NewState),
            ?assertEqual(completed, maps:get(state, Record)),
            ?assertEqual(0, maps:get(exit_code, Record)),
            ?assert(maps:get(end_time, Record) > 0),
            cleanup_state(NewState)
        end},

       {"record_job_end_event ignores unknown job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_end_event, 9999, 0, completed}, State),
            %% Should not create a record
            {reply, Result, _} = flurm_dbd_server:handle_call(
                {get_job_record, 9999}, {self(), make_ref()}, NewState),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(NewState)
        end},

       {"record_job_cancelled updates existing job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            %% First submit job
            JobData = #{job_id => 1011},
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),
            %% Then cancel it
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_cancelled, 1011, user_request}, State2),
            %% Verify job record updated
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 1011}, {self(), make_ref()}, NewState),
            ?assertEqual(cancelled, maps:get(state, Record)),
            ?assertEqual(-1, maps:get(exit_code, Record)),
            %% Verify stats updated
            Stats = element(10, NewState),
            ?assertEqual(1, maps:get(jobs_cancelled, Stats)),
            cleanup_state(NewState)
        end},

       {"record_job_cancelled ignores unknown job",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {record_job_cancelled, 9998, user_request}, State),
            %% Should not create a record
            {reply, Result, _} = flurm_dbd_server:handle_call(
                {get_job_record, 9998}, {self(), make_ref()}, NewState),
            ?assertEqual({error, not_found}, Result),
            cleanup_state(NewState)
        end},

       {"unknown cast is ignored",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {noreply, NewState} = flurm_dbd_server:handle_cast(
                {unknown_cast, data}, State),
            ?assertEqual(State, NewState),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"unknown info message is ignored",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            {noreply, NewState} = flurm_dbd_server:handle_info(
                unknown_message, State),
            ?assertEqual(State, NewState),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"terminate returns ok",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),
            Result = flurm_dbd_server:terminate(normal, State),
            ?assertEqual(ok, Result),
            cleanup_state(State)
        end}
      ]}}.

%%====================================================================
%% format_sacct_output/1 Tests
%%====================================================================

format_sacct_output_test_() ->
    {"format_sacct_output tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"format_sacct_output with empty list",
        fun() ->
            Result = flurm_dbd_server:format_sacct_output([]),
            ?assert(is_list(Result)),
            Flat = lists:flatten(Result),
            ?assert(string:find(Flat, "JobID") =/= nomatch),
            ?assert(string:find(Flat, "User") =/= nomatch),
            ?assert(string:find(Flat, "Account") =/= nomatch)
        end},

       {"format_sacct_output with job records",
        fun() ->
            Jobs = [
                #{
                    job_id => 1,
                    user_name => <<"alice">>,
                    account => <<"research">>,
                    partition => <<"normal">>,
                    state => completed,
                    elapsed => 3661,
                    exit_code => 0
                },
                #{
                    job_id => 2,
                    user_name => <<"bob">>,
                    account => <<"dev">>,
                    partition => <<"gpu">>,
                    state => failed,
                    elapsed => 120,
                    exit_code => 1
                }
            ],
            Result = flurm_dbd_server:format_sacct_output(Jobs),
            Flat = lists:flatten(Result),
            ?assert(string:find(Flat, "alice") =/= nomatch),
            ?assert(string:find(Flat, "bob") =/= nomatch),
            ?assert(string:find(Flat, "research") =/= nomatch),
            ?assert(string:find(Flat, "dev") =/= nomatch),
            ?assert(string:find(Flat, "01:01:01") =/= nomatch), % 3661 seconds
            ?assert(string:find(Flat, "00:02:00") =/= nomatch)  % 120 seconds
        end},

       {"format_sacct_output handles missing fields",
        fun() ->
            Jobs = [#{}],
            Result = flurm_dbd_server:format_sacct_output(Jobs),
            ?assert(is_list(Result))
        end}
      ]}}.

%%====================================================================
%% Integration Tests - Full Job Lifecycle
%%====================================================================

job_lifecycle_test_() ->
    {"job lifecycle integration tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"full job lifecycle: submit -> start -> end",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Submit job
            JobData = #{
                job_id => 2001,
                user_id => 1000,
                partition => <<"normal">>,
                num_cpus => 4
            },
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),

            %% Verify pending state
            {reply, {ok, Record1}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 2001}, {self(), make_ref()}, State2),
            ?assertEqual(pending, maps:get(state, Record1)),

            %% Start job
            {noreply, State3} = flurm_dbd_server:handle_cast(
                {record_job_start_event, 2001, [<<"node1">>, <<"node2">>]}, State2),

            %% Verify running state
            {reply, {ok, Record2}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 2001}, {self(), make_ref()}, State3),
            ?assertEqual(running, maps:get(state, Record2)),
            ?assertEqual(2, maps:get(num_nodes, Record2)),

            %% End job
            {noreply, State4} = flurm_dbd_server:handle_cast(
                {record_job_end_event, 2001, 0, completed}, State3),

            %% Verify completed state
            {reply, {ok, Record3}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 2001}, {self(), make_ref()}, State4),
            ?assertEqual(completed, maps:get(state, Record3)),
            ?assertEqual(0, maps:get(exit_code, Record3)),
            ?assert(maps:get(elapsed, Record3) >= 0),

            %% Verify stats
            Stats = element(10, State4),
            ?assertEqual(1, maps:get(jobs_submitted, Stats)),
            ?assertEqual(1, maps:get(jobs_started, Stats)),
            ?assertEqual(1, maps:get(jobs_completed, Stats)),

            cleanup_state(State4)
        end},

       {"job lifecycle with cancellation",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Submit job
            JobData = #{job_id => 2002},
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_submit, JobData}, State),

            %% Cancel job
            {noreply, State3} = flurm_dbd_server:handle_cast(
                {record_job_cancelled, 2002, user_request}, State2),

            %% Verify cancelled state
            {reply, {ok, Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 2002}, {self(), make_ref()}, State3),
            ?assertEqual(cancelled, maps:get(state, Record)),

            %% Verify stats
            Stats = element(10, State3),
            ?assertEqual(1, maps:get(jobs_cancelled, Stats)),

            cleanup_state(State3)
        end}
      ]}}.

%%====================================================================
%% Query Tests with Data
%%====================================================================

query_with_data_test_() ->
    {"query tests with populated data",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"get_jobs_by_user returns matching jobs",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add jobs for different users
            Job1 = #{job_id => 3001, user_name => <<"alice">>, account => <<"research">>},
            Job2 = #{job_id => 3002, user_name => <<"alice">>, account => <<"research">>},
            Job3 = #{job_id => 3003, user_name => <<"bob">>, account => <<"dev">>},

            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast({record_job_start, Job2}, State2),
            {noreply, State4} = flurm_dbd_server:handle_cast({record_job_start, Job3}, State3),

            %% Query alice's jobs
            {reply, AliceJobs, _} = flurm_dbd_server:handle_call(
                {get_jobs_by_user, <<"alice">>}, {self(), make_ref()}, State4),
            ?assertEqual(2, length(AliceJobs)),

            %% Query bob's jobs
            {reply, BobJobs, _} = flurm_dbd_server:handle_call(
                {get_jobs_by_user, <<"bob">>}, {self(), make_ref()}, State4),
            ?assertEqual(1, length(BobJobs)),

            cleanup_state(State4)
        end},

       {"get_jobs_by_account returns matching jobs",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add jobs for different accounts
            Job1 = #{job_id => 3101, user_name => <<"alice">>, account => <<"research">>},
            Job2 = #{job_id => 3102, user_name => <<"bob">>, account => <<"research">>},
            Job3 = #{job_id => 3103, user_name => <<"charlie">>, account => <<"dev">>},

            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast({record_job_start, Job2}, State2),
            {noreply, State4} = flurm_dbd_server:handle_cast({record_job_start, Job3}, State3),

            %% Query research account jobs
            {reply, ResearchJobs, _} = flurm_dbd_server:handle_call(
                {get_jobs_by_account, <<"research">>}, {self(), make_ref()}, State4),
            ?assertEqual(2, length(ResearchJobs)),

            cleanup_state(State4)
        end},

       {"list_job_records with filters",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add jobs with different states and partitions
            Job1 = #{job_id => 3201, user_name => <<"alice">>, partition => <<"normal">>, state => completed},
            Job2 = #{job_id => 3202, user_name => <<"alice">>, partition => <<"gpu">>, state => running},
            Job3 = #{job_id => 3203, user_name => <<"bob">>, partition => <<"normal">>, state => completed},

            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast({record_job_start, Job2}, State2),
            {noreply, State4} = flurm_dbd_server:handle_cast({record_job_start, Job3}, State3),

            %% Filter by user
            {reply, AliceJobs, _} = flurm_dbd_server:handle_call(
                {list_job_records, #{user => <<"alice">>}}, {self(), make_ref()}, State4),
            ?assertEqual(2, length(AliceJobs)),

            %% Filter by partition
            {reply, NormalJobs, _} = flurm_dbd_server:handle_call(
                {list_job_records, #{partition => <<"normal">>}}, {self(), make_ref()}, State4),
            ?assertEqual(2, length(NormalJobs)),

            %% Filter by state
            {reply, CompletedJobs, _} = flurm_dbd_server:handle_call(
                {list_job_records, #{state => completed}}, {self(), make_ref()}, State4),
            ?assertEqual(2, length(CompletedJobs)),

            cleanup_state(State4)
        end}
      ]}}.

%%====================================================================
%% Usage Calculation Tests with Data
%%====================================================================

usage_calculation_test_() ->
    {"usage calculation tests with data",
     {setup, fun setup/0, fun cleanup/1,
      [
       {"user usage calculation with completed jobs",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add completed job with elapsed time
            Job1 = #{
                job_id => 4001,
                user_name => <<"alice">>,
                account => <<"research">>,
                num_cpus => 4,
                num_nodes => 2,
                elapsed => 100
            },
            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),

            %% Calculate user usage
            {reply, Usage, _} = flurm_dbd_server:handle_call(
                {get_user_usage, <<"alice">>}, {self(), make_ref()}, State2),

            ?assertEqual(400, maps:get(cpu_seconds, Usage)),  % 4 cpus * 100 sec
            ?assertEqual(200, maps:get(node_seconds, Usage)), % 2 nodes * 100 sec
            ?assertEqual(1, maps:get(job_count, Usage)),
            ?assertEqual(100, maps:get(elapsed_total, Usage)),

            cleanup_state(State2)
        end},

       {"account usage calculation with multiple jobs",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add multiple jobs for same account
            Job1 = #{
                job_id => 4101,
                user_name => <<"alice">>,
                account => <<"research">>,
                num_cpus => 4,
                num_nodes => 1,
                elapsed => 100
            },
            Job2 = #{
                job_id => 4102,
                user_name => <<"bob">>,
                account => <<"research">>,
                num_cpus => 8,
                num_nodes => 2,
                elapsed => 50
            },

            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast({record_job_start, Job2}, State2),

            %% Calculate account usage
            {reply, Usage, _} = flurm_dbd_server:handle_call(
                {get_account_usage, <<"research">>}, {self(), make_ref()}, State3),

            ?assertEqual(800, maps:get(cpu_seconds, Usage)),   % (4*100) + (8*50)
            ?assertEqual(200, maps:get(node_seconds, Usage)),  % (1*100) + (2*50)
            ?assertEqual(2, maps:get(job_count, Usage)),
            ?assertEqual(150, maps:get(elapsed_total, Usage)),

            cleanup_state(State3)
        end},

       {"cluster usage calculation",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add jobs for different accounts
            Job1 = #{job_id => 4201, account => <<"research">>, num_cpus => 4, num_nodes => 1, elapsed => 100},
            Job2 = #{job_id => 4202, account => <<"dev">>, num_cpus => 2, num_nodes => 1, elapsed => 200},

            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),
            {noreply, State3} = flurm_dbd_server:handle_cast({record_job_start, Job2}, State2),

            %% Calculate cluster usage
            {reply, Usage, _} = flurm_dbd_server:handle_call(
                get_cluster_usage, {self(), make_ref()}, State3),

            ?assertEqual(800, maps:get(cpu_seconds, Usage)),   % (4*100) + (2*200)
            ?assertEqual(300, maps:get(node_seconds, Usage)),  % (1*100) + (1*200)
            ?assertEqual(2, maps:get(job_count, Usage)),
            ?assertEqual(300, maps:get(elapsed_total, Usage)),

            cleanup_state(State3)
        end},

       {"TRES usage calculation includes GPU and memory",
        fun() ->
            {ok, State} = flurm_dbd_server:init([]),
            flush_messages(),

            %% Add job with GPU and memory
            Job1 = #{
                job_id => 4301,
                user_name => <<"alice">>,
                account => <<"research">>,
                num_cpus => 4,
                num_nodes => 1,
                req_mem => 1024,
                elapsed => 100,
                tres_alloc => #{gpu => 2}
            },
            {noreply, State2} = flurm_dbd_server:handle_cast({record_job_start, Job1}, State),

            %% Calculate TRES usage
            {reply, Usage, _} = flurm_dbd_server:handle_call(
                {calculate_user_tres_usage, <<"alice">>}, {self(), make_ref()}, State2),

            ?assertEqual(400, maps:get(cpu_seconds, Usage)),      % 4 cpus * 100 sec
            ?assertEqual(102400, maps:get(mem_seconds, Usage)),   % 1024 MB * 100 sec
            ?assertEqual(200, maps:get(gpu_seconds, Usage)),      % 2 gpus * 100 sec
            ?assertEqual(100, maps:get(node_seconds, Usage)),     % 1 node * 100 sec

            cleanup_state(State2)
        end}
      ]}}.

%%====================================================================
%% Helper Functions
%%====================================================================

cleanup_state(State) ->
    %% Delete all ETS tables in state
    try
        ets:delete(element(2, State)),  % job_records
        ets:delete(element(3, State)),  % step_records
        ets:delete(element(4, State)),  % usage_records
        ets:delete(element(5, State)),  % usage_by_user
        ets:delete(element(6, State)),  % usage_by_account
        ets:delete(element(7, State)),  % associations
        ets:delete(element(8, State))   % archive
    catch
        _:_ -> ok
    end,
    ok.

flush_messages() ->
    receive
        _ -> flush_messages()
    after 0 ->
        ok
    end.
