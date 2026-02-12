%%%-------------------------------------------------------------------
%%% @doc Boundary condition tests for flurm_dbd_server
%%%
%%% Tests archive/purge boundary conditions and update_usage via the
%%% gen_server callbacks directly. Uses meck to mock lager.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_boundary_tests).

-include_lib("eunit/include/eunit.hrl").

%% We duplicate the state record definition locally so we can access
%% fields by name (e.g. State#state.job_records) without needing a
%% header export from the server module.
-record(state, {
    job_records     :: ets:tid(),
    step_records    :: ets:tid(),
    usage_records   :: ets:tid(),
    usage_by_user   :: ets:tid(),
    usage_by_account :: ets:tid(),
    associations    :: ets:tid(),
    archive         :: ets:tid(),
    next_assoc_id   :: pos_integer(),
    stats = #{}     :: map()
}).

%% Duplicate the job_record definition so we can insert test records
%% directly into the ETS tables owned by the server state.
-record(job_record, {
    job_id          :: pos_integer(),
    job_name        :: binary(),
    user_name       :: binary(),
    user_id         :: non_neg_integer(),
    group_id        :: non_neg_integer(),
    account         :: binary(),
    partition       :: binary(),
    cluster         :: binary(),
    qos             :: binary(),
    state           :: atom(),
    exit_code       :: integer(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    num_tasks       :: pos_integer(),
    req_mem         :: non_neg_integer(),
    submit_time     :: non_neg_integer(),
    eligible_time   :: non_neg_integer(),
    start_time      :: non_neg_integer(),
    end_time        :: non_neg_integer(),
    elapsed         :: non_neg_integer(),
    tres_alloc      :: map(),
    tres_req        :: map(),
    work_dir        :: binary(),
    std_out         :: binary(),
    std_err         :: binary()
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Mock lager to suppress output during tests
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Build a fresh server state via init, then flush the start_listener
%% message that init sends to self().
new_state() ->
    {ok, State} = flurm_dbd_server:init([]),
    flush_messages(),
    State.

%% Remove all ETS tables held by the server state.
cleanup_state(State) ->
    catch ets:delete(State#state.job_records),
    catch ets:delete(State#state.step_records),
    catch ets:delete(State#state.usage_records),
    catch ets:delete(State#state.usage_by_user),
    catch ets:delete(State#state.usage_by_account),
    catch ets:delete(State#state.associations),
    catch ets:delete(State#state.archive),
    ok.

%% Drain the process mailbox so leftover messages from init do not
%% interfere with subsequent assertions.
flush_messages() ->
    receive _ -> flush_messages()
    after 0 -> ok
    end.

%% Convenience: build a completed job record with an explicit end_time.
make_completed_job(JobId, EndTime) ->
    make_job(JobId, completed, EndTime).

%% Build a job record in the specified state and end_time.
make_job(JobId, JobState, EndTime) ->
    StartTime = case EndTime > 0 of true -> EndTime - 100; false -> 0 end,
    Elapsed = case EndTime > 0 of true -> 100; false -> 0 end,
    #job_record{
        job_id = JobId,
        job_name = <<"test">>,
        user_name = <<"testuser">>,
        user_id = 1000,
        group_id = 1000,
        account = <<"testacct">>,
        partition = <<"normal">>,
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = JobState,
        exit_code = 0,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 1,
        req_mem = 512,
        submit_time = StartTime,
        eligible_time = StartTime,
        start_time = StartTime,
        end_time = EndTime,
        elapsed = Elapsed,
        tres_alloc = #{},
        tres_req = #{},
        work_dir = <<"/tmp">>,
        std_out = <<>>,
        std_err = <<>>
    }.

%%====================================================================
%% Archive Boundary Tests
%%====================================================================

archive_boundary_test_() ->
    {"archive_old_jobs boundary tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       %% 1. archive_old_jobs with no jobs returns {ok, 0}
       {"archive with no jobs returns {ok, 0}",
        fun() ->
            State = new_state(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       %% 2. archive_old_jobs with jobs newer than cutoff returns {ok, 0}
       {"archive with recent jobs returns {ok, 0}",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            %% Insert a completed job that ended 5 seconds ago (well within 30 days)
            RecentJob = make_completed_job(1, Now - 5),
            ets:insert(State#state.job_records, RecentJob),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            %% Job should still be in job_records
            ?assertMatch([_], ets:lookup(NewState#state.job_records, 1)),
            cleanup_state(NewState)
        end},

       %% 3. archive_old_jobs with old completed jobs returns {ok, N} where N > 0
       {"archive moves old completed jobs to archive table",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            %% Insert 3 completed jobs that ended 60 days ago
            OldTime = Now - (60 * 86400),
            ets:insert(State#state.job_records, make_completed_job(10, OldTime)),
            ets:insert(State#state.job_records, make_completed_job(11, OldTime - 100)),
            ets:insert(State#state.job_records, make_completed_job(12, OldTime - 200)),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 3}, Result),
            %% Jobs should be removed from job_records
            ?assertEqual([], ets:lookup(NewState#state.job_records, 10)),
            ?assertEqual([], ets:lookup(NewState#state.job_records, 11)),
            ?assertEqual([], ets:lookup(NewState#state.job_records, 12)),
            %% Jobs should now be in the archive table
            ?assertMatch([_], ets:lookup(NewState#state.archive, 10)),
            ?assertMatch([_], ets:lookup(NewState#state.archive, 11)),
            ?assertMatch([_], ets:lookup(NewState#state.archive, 12)),
            cleanup_state(NewState)
        end},

       %% 4. archive_old_jobs skips running and pending jobs even if old
       {"archive skips running and pending jobs even if old",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            OldTime = Now - (60 * 86400),
            %% Insert a running job with an old end_time (this is an odd case
            %% but the code checks state explicitly)
            RunningJob = make_job(20, running, OldTime),
            PendingJob = make_job(21, pending, OldTime),
            CompletedJob = make_completed_job(22, OldTime),
            ets:insert(State#state.job_records, RunningJob),
            ets:insert(State#state.job_records, PendingJob),
            ets:insert(State#state.job_records, CompletedJob),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            %% Only the completed job should be archived
            ?assertEqual({ok, 1}, Result),
            %% Running and pending should still be in job_records
            ?assertMatch([_], ets:lookup(NewState#state.job_records, 20)),
            ?assertMatch([_], ets:lookup(NewState#state.job_records, 21)),
            %% Completed should be moved to archive
            ?assertEqual([], ets:lookup(NewState#state.job_records, 22)),
            ?assertMatch([_], ets:lookup(NewState#state.archive, 22)),
            cleanup_state(NewState)
        end},

       %% 5. archive boundary: job with end_time exactly at cutoff is NOT archived
       {"archive boundary: job at exactly cutoff is NOT archived (strict less-than)",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            Cutoff = Now - (30 * 86400),
            %% Job ending exactly at the cutoff
            ExactJob = make_completed_job(30, Cutoff),
            %% Job ending 1 second before cutoff (should be archived)
            OlderJob = make_completed_job(31, Cutoff - 1),
            ets:insert(State#state.job_records, ExactJob),
            ets:insert(State#state.job_records, OlderJob),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {archive_old_jobs, 30}, {self(), make_ref()}, State),
            %% Only the job strictly before cutoff should be archived
            ?assertEqual({ok, 1}, Result),
            %% The job at exactly the cutoff should remain in job_records
            ?assertMatch([_], ets:lookup(NewState#state.job_records, 30)),
            %% The older job should be in archive
            ?assertEqual([], ets:lookup(NewState#state.job_records, 31)),
            ?assertMatch([_], ets:lookup(NewState#state.archive, 31)),
            cleanup_state(NewState)
        end}
      ]}}.

%%====================================================================
%% Purge Boundary Tests
%%====================================================================

purge_boundary_test_() ->
    {"purge_old_jobs boundary tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       %% 6. purge_old_jobs with empty archive returns {ok, 0}
       {"purge with empty archive returns {ok, 0}",
        fun() ->
            State = new_state(),
            {reply, Result, _NewState} = flurm_dbd_server:handle_call(
                {purge_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 0}, Result),
            cleanup_state(State)
        end},

       %% 7. purge_old_jobs with archived jobs older than cutoff returns {ok, N}
       {"purge removes old archived jobs",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            OldTime = Now - (90 * 86400),
            %% Manually insert jobs into the archive table
            ets:insert(State#state.archive, make_completed_job(40, OldTime)),
            ets:insert(State#state.archive, make_completed_job(41, OldTime - 500)),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {purge_old_jobs, 30}, {self(), make_ref()}, State),
            ?assertEqual({ok, 2}, Result),
            %% Both jobs should be deleted from archive
            ?assertEqual([], ets:lookup(NewState#state.archive, 40)),
            ?assertEqual([], ets:lookup(NewState#state.archive, 41)),
            cleanup_state(NewState)
        end},

       %% 8. purge boundary: job at exactly cutoff is NOT purged
       {"purge boundary: job at exactly cutoff is NOT purged (strict less-than)",
        fun() ->
            State = new_state(),
            Now = erlang:system_time(second),
            Cutoff = Now - (30 * 86400),
            %% Job ending exactly at the cutoff
            ExactJob = make_completed_job(50, Cutoff),
            %% Job ending 1 second before cutoff (should be purged)
            OlderJob = make_completed_job(51, Cutoff - 1),
            ets:insert(State#state.archive, ExactJob),
            ets:insert(State#state.archive, OlderJob),
            {reply, Result, NewState} = flurm_dbd_server:handle_call(
                {purge_old_jobs, 30}, {self(), make_ref()}, State),
            %% Only the job strictly before cutoff should be purged
            ?assertEqual({ok, 1}, Result),
            %% The job at exactly the cutoff should remain in archive
            ?assertMatch([_], ets:lookup(NewState#state.archive, 50)),
            %% The older job should be gone
            ?assertEqual([], ets:lookup(NewState#state.archive, 51)),
            cleanup_state(NewState)
        end}
      ]}}.

%%====================================================================
%% update_usage Indirectly via record_job_end
%%====================================================================

update_usage_test_() ->
    {"update_usage indirectly via record_job_end",
     {setup, fun setup/0, fun cleanup/1,
      [
       %% 9. Record a job end for an existing job - usage record is created
       {"record_job_end for existing job creates usage record",
        fun() ->
            State = new_state(),
            %% First create a job via record_job_start
            StartInfo = #{
                job_id => 100,
                name => <<"usage_test">>,
                user_name => <<"alice">>,
                account => <<"research">>,
                state => running,
                num_cpus => 4,
                num_nodes => 2,
                req_mem => 1024
            },
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_start, StartInfo}, State),
            %% Now end the job via record_job_end (this triggers update_usage)
            EndTime = erlang:system_time(second),
            EndInfo = #{
                job_id => 100,
                state => completed,
                exit_code => 0,
                end_time => EndTime
            },
            {noreply, State3} = flurm_dbd_server:handle_cast(
                {record_job_end, EndInfo}, State2),
            %% Verify a usage record was created in the usage_records ETS
            UsageRecords = ets:tab2list(State3#state.usage_records),
            ?assert(length(UsageRecords) > 0),
            %% Verify stats updated
            Stats = State3#state.stats,
            ?assertEqual(1, maps:get(jobs_completed, Stats)),
            cleanup_state(State3)
        end},

       %% 10. Record a job end for unknown job - creates new record without crash
       {"record_job_end for unknown job does not crash",
        fun() ->
            State = new_state(),
            EndInfo = #{
                job_id => 999,
                name => <<"orphan">>,
                state => completed,
                exit_code => 0,
                end_time => erlang:system_time(second)
            },
            %% Should not crash; when job is not found, a new record is created
            {noreply, State2} = flurm_dbd_server:handle_cast(
                {record_job_end, EndInfo}, State),
            %% The job should exist now (created from the end info)
            {reply, {ok, _Record}, _} = flurm_dbd_server:handle_call(
                {get_job_record, 999}, {self(), make_ref()}, State2),
            cleanup_state(State2)
        end}
      ]}}.

%%====================================================================
%% Helper Function Tests (exported under -ifdef(TEST))
%%====================================================================

helper_functions_test_() ->
    {"exported helper function tests",
     {setup, fun setup/0, fun cleanup/1,
      [
       %% 11. calculate_tres_usage/1 returns expected values
       %%     Note: calculate_tres_usage takes a job_record, not a map.
       %%     It delegates to flurm_tres:from_job/1.
       {"calculate_tres_usage returns map with cpu_seconds, mem_seconds, etc.",
        fun() ->
            %% We need flurm_tres to be available (it should be, as a
            %% compile-time dependency). Build a minimal job record.
            Job = #job_record{
                job_id = 200,
                job_name = <<>>,
                user_name = <<>>,
                user_id = 0,
                group_id = 0,
                account = <<>>,
                partition = <<>>,
                cluster = <<"flurm">>,
                qos = <<"normal">>,
                state = completed,
                exit_code = 0,
                num_nodes = 2,
                num_cpus = 8,
                num_tasks = 1,
                req_mem = 1024,
                submit_time = 0,
                eligible_time = 0,
                start_time = 1000,
                end_time = 1100,
                elapsed = 100,
                tres_alloc = #{gpu => 3},
                tres_req = #{},
                work_dir = <<>>,
                std_out = <<>>,
                std_err = <<>>
            },
            Result = flurm_dbd_server:calculate_tres_usage(Job),
            ?assert(is_map(Result)),
            %% cpu_seconds = elapsed * num_cpus = 100 * 8 = 800
            ?assertEqual(800, maps:get(cpu_seconds, Result)),
            %% mem_seconds = elapsed * req_mem = 100 * 1024 = 102400
            ?assertEqual(102400, maps:get(mem_seconds, Result)),
            %% node_seconds = elapsed * num_nodes = 100 * 2 = 200
            ?assertEqual(200, maps:get(node_seconds, Result)),
            %% gpu_seconds = elapsed * gpu = 100 * 3 = 300
            ?assertEqual(300, maps:get(gpu_seconds, Result)),
            %% job_count = 1
            ?assertEqual(1, maps:get(job_count, Result)),
            %% job_time = elapsed = 100
            ?assertEqual(100, maps:get(job_time, Result))
        end},

       %% 12. current_period/0 returns a binary like "YYYY-MM"
       {"current_period returns binary in YYYY-MM format",
        fun() ->
            Period = flurm_dbd_server:current_period(),
            ?assert(is_binary(Period)),
            %% Should be 7 bytes: "YYYY-MM"
            ?assertEqual(7, byte_size(Period)),
            %% Should match the pattern NNNN-NN
            <<Y1, Y2, Y3, Y4, $-, M1, M2>> = Period,
            ?assert(Y1 >= $0 andalso Y1 =< $9),
            ?assert(Y2 >= $0 andalso Y2 =< $9),
            ?assert(Y3 >= $0 andalso Y3 =< $9),
            ?assert(Y4 >= $0 andalso Y4 =< $9),
            ?assert(M1 >= $0 andalso M1 =< $1),
            ?assert(M2 >= $0 andalso M2 =< $9),
            %% Verify it matches today's date
            {{Year, Month, _}, _} = calendar:local_time(),
            Expected = list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])),
            ?assertEqual(Expected, Period)
        end},

       %% 13. format_elapsed/1 formats seconds as HH:MM:SS
       {"format_elapsed formats seconds correctly",
        fun() ->
            %% 0 seconds
            ?assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(0))),
            %% 1 second
            ?assertEqual("00:00:01", lists:flatten(flurm_dbd_server:format_elapsed(1))),
            %% 60 seconds = 1 minute
            ?assertEqual("00:01:00", lists:flatten(flurm_dbd_server:format_elapsed(60))),
            %% 3661 seconds = 1 hour, 1 minute, 1 second
            ?assertEqual("01:01:01", lists:flatten(flurm_dbd_server:format_elapsed(3661))),
            %% 86399 seconds = 23:59:59
            ?assertEqual("23:59:59", lists:flatten(flurm_dbd_server:format_elapsed(86399))),
            %% Large value: 99 hours, 59 minutes, 59 seconds
            ?assertEqual("99:59:59", lists:flatten(flurm_dbd_server:format_elapsed(359999))),
            %% Non-integer returns "00:00:00"
            ?assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(undefined)))
        end},

       %% 14. format_exit_code/1 formats exit codes as "Code:0"
       {"format_exit_code formats codes correctly",
        fun() ->
            %% Normal exit
            ?assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(0))),
            %% Non-zero exit
            ?assertEqual("1:0", lists:flatten(flurm_dbd_server:format_exit_code(1))),
            %% Negative exit code (e.g. cancelled)
            ?assertEqual("-1:0", lists:flatten(flurm_dbd_server:format_exit_code(-1))),
            %% Large exit code
            ?assertEqual("127:0", lists:flatten(flurm_dbd_server:format_exit_code(127))),
            %% Non-integer returns "0:0"
            ?assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(undefined)))
        end}
      ]}}.
