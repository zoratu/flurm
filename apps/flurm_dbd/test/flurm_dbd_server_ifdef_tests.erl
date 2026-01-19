%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_dbd_server internal functions via ifdef(TEST) exports
%%%
%%% Tests the pure internal functions exported for testing:
%%% - Record conversion functions
%%% - Filtering functions
%%% - Formatting functions
%%% - Calculation functions
%%% - Utility functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%% Local record definitions matching the server's records for testing
-record(job_record, {
    job_id :: pos_integer(),
    job_name :: binary(),
    user_name :: binary(),
    user_id :: non_neg_integer(),
    group_id :: non_neg_integer(),
    account :: binary(),
    partition :: binary(),
    cluster :: binary(),
    qos :: binary(),
    state :: atom(),
    exit_code :: integer(),
    num_nodes :: pos_integer(),
    num_cpus :: pos_integer(),
    num_tasks :: pos_integer(),
    req_mem :: non_neg_integer(),
    submit_time :: non_neg_integer(),
    eligible_time :: non_neg_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer(),
    elapsed :: non_neg_integer(),
    tres_alloc :: map(),
    tres_req :: map(),
    work_dir :: binary(),
    std_out :: binary(),
    std_err :: binary()
}).

-record(step_record, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    step_name :: binary(),
    state :: atom(),
    exit_code :: integer(),
    num_tasks :: pos_integer(),
    num_nodes :: pos_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer(),
    elapsed :: non_neg_integer(),
    tres_alloc :: map()
}).

-record(assoc_record, {
    id :: pos_integer(),
    cluster :: binary(),
    account :: binary(),
    user :: binary(),
    partition = <<>> :: binary(),
    shares = 1 :: non_neg_integer(),
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer(),
    grp_tres = #{} :: map(),
    max_tres_per_job = #{} :: map(),
    qos = [] :: [binary()],
    default_qos = <<>> :: binary(),
    usage = #{} :: map()
}).

%%====================================================================
%% format_elapsed/1 Tests
%%====================================================================

format_elapsed_test_() ->
    [{"0 seconds formats as 00:00:00",
      ?_assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(0)))},
     {"1 second formats as 00:00:01",
      ?_assertEqual("00:00:01", lists:flatten(flurm_dbd_server:format_elapsed(1)))},
     {"59 seconds formats as 00:00:59",
      ?_assertEqual("00:00:59", lists:flatten(flurm_dbd_server:format_elapsed(59)))},
     {"60 seconds formats as 00:01:00",
      ?_assertEqual("00:01:00", lists:flatten(flurm_dbd_server:format_elapsed(60)))},
     {"61 seconds formats as 00:01:01",
      ?_assertEqual("00:01:01", lists:flatten(flurm_dbd_server:format_elapsed(61)))},
     {"3599 seconds formats as 00:59:59",
      ?_assertEqual("00:59:59", lists:flatten(flurm_dbd_server:format_elapsed(3599)))},
     {"3600 seconds (1 hour) formats as 01:00:00",
      ?_assertEqual("01:00:00", lists:flatten(flurm_dbd_server:format_elapsed(3600)))},
     {"3661 seconds formats as 01:01:01",
      ?_assertEqual("01:01:01", lists:flatten(flurm_dbd_server:format_elapsed(3661)))},
     {"86399 seconds formats as 23:59:59",
      ?_assertEqual("23:59:59", lists:flatten(flurm_dbd_server:format_elapsed(86399)))},
     {"86400 seconds (24 hours) formats as 24:00:00",
      ?_assertEqual("24:00:00", lists:flatten(flurm_dbd_server:format_elapsed(86400)))},
     {"90061 seconds (25h 1m 1s) formats as 25:01:01",
      ?_assertEqual("25:01:01", lists:flatten(flurm_dbd_server:format_elapsed(90061)))},
     {"negative value returns 00:00:00",
      ?_assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(-1)))},
     {"non-integer returns 00:00:00",
      ?_assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(invalid)))}].

%%====================================================================
%% format_exit_code/1 Tests
%%====================================================================

format_exit_code_test_() ->
    [{"exit code 0 formats as 0:0",
      ?_assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(0)))},
     {"exit code 1 formats as 1:0",
      ?_assertEqual("1:0", lists:flatten(flurm_dbd_server:format_exit_code(1)))},
     {"exit code -1 formats as -1:0",
      ?_assertEqual("-1:0", lists:flatten(flurm_dbd_server:format_exit_code(-1)))},
     {"exit code 127 formats as 127:0",
      ?_assertEqual("127:0", lists:flatten(flurm_dbd_server:format_exit_code(127)))},
     {"exit code 255 formats as 255:0",
      ?_assertEqual("255:0", lists:flatten(flurm_dbd_server:format_exit_code(255)))},
     {"non-integer returns 0:0",
      ?_assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(invalid)))}].

%%====================================================================
%% current_period/0 Tests
%%====================================================================

current_period_test_() ->
    [{"current_period returns binary",
      ?_assert(is_binary(flurm_dbd_server:current_period()))},
     {"current_period format is YYYY-MM",
      fun() ->
          Period = flurm_dbd_server:current_period(),
          ?assertMatch(<<_Y1, _Y2, _Y3, _Y4, $-, _M1, _M2>>, Period),
          %% Verify it's a valid year-month
          [YearStr, MonthStr] = binary:split(Period, <<"-">>),
          Year = binary_to_integer(YearStr),
          Month = binary_to_integer(MonthStr),
          ?assert(Year >= 2024 andalso Year =< 2100),
          ?assert(Month >= 1 andalso Month =< 12)
      end},
     {"current_period matches actual date",
      fun() ->
          Period = flurm_dbd_server:current_period(),
          {{Year, Month, _}, _} = calendar:local_time(),
          Expected = list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])),
          ?assertEqual(Expected, Period)
      end}].

%%====================================================================
%% job_record_to_map/1 Tests
%%====================================================================

job_record_to_map_test_() ->
    [{"converts complete job record to map",
      fun() ->
          Record = #job_record{
              job_id = 12345,
              job_name = <<"test_job">>,
              user_name = <<"testuser">>,
              user_id = 1000,
              group_id = 1000,
              account = <<"research">>,
              partition = <<"compute">>,
              cluster = <<"flurm">>,
              qos = <<"normal">>,
              state = completed,
              exit_code = 0,
              num_nodes = 2,
              num_cpus = 16,
              num_tasks = 4,
              req_mem = 8192,
              submit_time = 1704067200,
              eligible_time = 1704067200,
              start_time = 1704067260,
              end_time = 1704070800,
              elapsed = 3540,
              tres_alloc = #{cpu => 16, mem => 8192},
              tres_req = #{cpu => 16, mem => 8192},
              work_dir = <<"/home/testuser">>,
              std_out = <<"/tmp/job.out">>,
              std_err = <<"/tmp/job.err">>
          },
          Map = flurm_dbd_server:job_record_to_map(Record),
          ?assertEqual(12345, maps:get(job_id, Map)),
          ?assertEqual(<<"test_job">>, maps:get(job_name, Map)),
          ?assertEqual(<<"testuser">>, maps:get(user_name, Map)),
          ?assertEqual(1000, maps:get(user_id, Map)),
          ?assertEqual(1000, maps:get(group_id, Map)),
          ?assertEqual(<<"research">>, maps:get(account, Map)),
          ?assertEqual(<<"compute">>, maps:get(partition, Map)),
          ?assertEqual(<<"flurm">>, maps:get(cluster, Map)),
          ?assertEqual(<<"normal">>, maps:get(qos, Map)),
          ?assertEqual(completed, maps:get(state, Map)),
          ?assertEqual(0, maps:get(exit_code, Map)),
          ?assertEqual(2, maps:get(num_nodes, Map)),
          ?assertEqual(16, maps:get(num_cpus, Map)),
          ?assertEqual(4, maps:get(num_tasks, Map)),
          ?assertEqual(8192, maps:get(req_mem, Map)),
          ?assertEqual(3540, maps:get(elapsed, Map)),
          ?assertEqual(<<"/home/testuser">>, maps:get(work_dir, Map))
      end},
     {"converts minimal job record to map",
      fun() ->
          Record = #job_record{
              job_id = 1,
              job_name = <<>>,
              user_name = <<>>,
              user_id = 0,
              group_id = 0,
              account = <<>>,
              partition = <<>>,
              cluster = <<>>,
              qos = <<>>,
              state = pending,
              exit_code = 0,
              num_nodes = 1,
              num_cpus = 1,
              num_tasks = 1,
              req_mem = 0,
              submit_time = 0,
              eligible_time = 0,
              start_time = 0,
              end_time = 0,
              elapsed = 0,
              tres_alloc = #{},
              tres_req = #{},
              work_dir = <<>>,
              std_out = <<>>,
              std_err = <<>>
          },
          Map = flurm_dbd_server:job_record_to_map(Record),
          ?assertEqual(1, maps:get(job_id, Map)),
          ?assertEqual(pending, maps:get(state, Map)),
          ?assertEqual(#{}, maps:get(tres_alloc, Map))
      end}].

%%====================================================================
%% assoc_record_to_map/1 Tests
%%====================================================================

assoc_record_to_map_test_() ->
    [{"converts complete assoc record to map",
      fun() ->
          Record = #assoc_record{
              id = 42,
              cluster = <<"production">>,
              account = <<"engineering">>,
              user = <<"alice">>,
              partition = <<"gpu">>,
              shares = 100,
              max_jobs = 50,
              max_submit = 100,
              max_wall = 4320,
              grp_tres = #{cpu => 1000, gpu => 8},
              max_tres_per_job = #{cpu => 64, gpu => 4},
              qos = [<<"high">>, <<"normal">>],
              default_qos = <<"normal">>,
              usage = #{cpu_seconds => 100000}
          },
          Map = flurm_dbd_server:assoc_record_to_map(Record),
          ?assertEqual(42, maps:get(id, Map)),
          ?assertEqual(<<"production">>, maps:get(cluster, Map)),
          ?assertEqual(<<"engineering">>, maps:get(account, Map)),
          ?assertEqual(<<"alice">>, maps:get(user, Map)),
          ?assertEqual(<<"gpu">>, maps:get(partition, Map)),
          ?assertEqual(100, maps:get(shares, Map)),
          ?assertEqual(50, maps:get(max_jobs, Map)),
          ?assertEqual(100, maps:get(max_submit, Map)),
          ?assertEqual(4320, maps:get(max_wall, Map)),
          ?assertEqual(#{cpu => 1000, gpu => 8}, maps:get(grp_tres, Map)),
          ?assertEqual(#{cpu => 64, gpu => 4}, maps:get(max_tres_per_job, Map)),
          ?assertEqual([<<"high">>, <<"normal">>], maps:get(qos, Map)),
          ?assertEqual(<<"normal">>, maps:get(default_qos, Map)),
          ?assertEqual(#{cpu_seconds => 100000}, maps:get(usage, Map))
      end},
     {"converts minimal assoc record with defaults to map",
      fun() ->
          Record = #assoc_record{
              id = 1,
              cluster = <<"default">>,
              account = <<"root">>,
              user = <<"admin">>
          },
          Map = flurm_dbd_server:assoc_record_to_map(Record),
          ?assertEqual(1, maps:get(id, Map)),
          ?assertEqual(<<>>, maps:get(partition, Map)),
          ?assertEqual(1, maps:get(shares, Map)),
          ?assertEqual(0, maps:get(max_jobs, Map)),
          ?assertEqual(#{}, maps:get(grp_tres, Map)),
          ?assertEqual([], maps:get(qos, Map))
      end}].

%%====================================================================
%% matches_job_filters/2 Tests
%%====================================================================

matches_job_filters_test_() ->
    BaseRecord = #job_record{
        job_id = 100,
        job_name = <<"test">>,
        user_name = <<"alice">>,
        user_id = 1000,
        group_id = 1000,
        account = <<"research">>,
        partition = <<"compute">>,
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = completed,
        exit_code = 0,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 1,
        req_mem = 1024,
        submit_time = 1000,
        eligible_time = 1000,
        start_time = 1100,
        end_time = 1200,
        elapsed = 100,
        tres_alloc = #{},
        tres_req = #{},
        work_dir = <<>>,
        std_out = <<>>,
        std_err = <<>>
    },
    [{"empty filters match any record",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{}))},
     {"matching user filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{user => <<"alice">>}))},
     {"non-matching user filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{user => <<"bob">>}))},
     {"matching account filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{account => <<"research">>}))},
     {"non-matching account filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{account => <<"sales">>}))},
     {"matching partition filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{partition => <<"compute">>}))},
     {"non-matching partition filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{partition => <<"gpu">>}))},
     {"matching state filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{state => completed}))},
     {"non-matching state filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{state => running}))},
     {"matching start_time_after filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{start_time_after => 1000}))},
     {"non-matching start_time_after filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{start_time_after => 1200}))},
     {"matching start_time_before filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{start_time_before => 1200}))},
     {"non-matching start_time_before filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{start_time_before => 1000}))},
     {"matching end_time_after filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{end_time_after => 1100}))},
     {"non-matching end_time_after filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{end_time_after => 1300}))},
     {"matching end_time_before filter",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{end_time_before => 1300}))},
     {"non-matching end_time_before filter",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord, #{end_time_before => 1100}))},
     {"multiple matching filters",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord,
          #{user => <<"alice">>, account => <<"research">>, state => completed}))},
     {"one non-matching filter among many",
      ?_assertNot(flurm_dbd_server:matches_job_filters(BaseRecord,
          #{user => <<"alice">>, account => <<"sales">>, state => completed}))},
     {"unknown filter key is ignored",
      ?_assert(flurm_dbd_server:matches_job_filters(BaseRecord, #{unknown => value}))}].

%%====================================================================
%% filter_job_records/2 Tests
%%====================================================================

filter_job_records_test_() ->
    Records = [
        #job_record{job_id = 1, user_name = <<"alice">>, account = <<"research">>,
                    partition = <<"compute">>, state = completed, exit_code = 0,
                    job_name = <<>>, user_id = 1000, group_id = 1000, cluster = <<>>,
                    qos = <<>>, num_nodes = 1, num_cpus = 1, num_tasks = 1, req_mem = 0,
                    submit_time = 0, eligible_time = 0, start_time = 1000, end_time = 1100,
                    elapsed = 100, tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
                    std_out = <<>>, std_err = <<>>},
        #job_record{job_id = 2, user_name = <<"bob">>, account = <<"research">>,
                    partition = <<"compute">>, state = running, exit_code = 0,
                    job_name = <<>>, user_id = 1001, group_id = 1001, cluster = <<>>,
                    qos = <<>>, num_nodes = 1, num_cpus = 1, num_tasks = 1, req_mem = 0,
                    submit_time = 0, eligible_time = 0, start_time = 1050, end_time = 0,
                    elapsed = 0, tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
                    std_out = <<>>, std_err = <<>>},
        #job_record{job_id = 3, user_name = <<"alice">>, account = <<"sales">>,
                    partition = <<"gpu">>, state = completed, exit_code = 1,
                    job_name = <<>>, user_id = 1000, group_id = 1000, cluster = <<>>,
                    qos = <<>>, num_nodes = 2, num_cpus = 8, num_tasks = 4, req_mem = 0,
                    submit_time = 0, eligible_time = 0, start_time = 900, end_time = 1000,
                    elapsed = 100, tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
                    std_out = <<>>, std_err = <<>>}
    ],
    [{"empty filters returns all records",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{}),
          ?assertEqual(3, length(Result))
      end},
     {"filter by user",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{user => <<"alice">>}),
          ?assertEqual(2, length(Result)),
          JobIds = [R#job_record.job_id || R <- Result],
          ?assert(lists:member(1, JobIds)),
          ?assert(lists:member(3, JobIds))
      end},
     {"filter by account",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{account => <<"research">>}),
          ?assertEqual(2, length(Result)),
          JobIds = [R#job_record.job_id || R <- Result],
          ?assert(lists:member(1, JobIds)),
          ?assert(lists:member(2, JobIds))
      end},
     {"filter by partition",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{partition => <<"gpu">>}),
          ?assertEqual(1, length(Result)),
          ?assertEqual(3, (hd(Result))#job_record.job_id)
      end},
     {"filter by state",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{state => completed}),
          ?assertEqual(2, length(Result))
      end},
     {"filter by multiple criteria",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records,
              #{user => <<"alice">>, account => <<"research">>}),
          ?assertEqual(1, length(Result)),
          ?assertEqual(1, (hd(Result))#job_record.job_id)
      end},
     {"filter returns empty list when no matches",
      fun() ->
          Result = flurm_dbd_server:filter_job_records(Records, #{user => <<"carol">>}),
          ?assertEqual([], Result)
      end},
     {"filter on empty list returns empty list",
      ?_assertEqual([], flurm_dbd_server:filter_job_records([], #{user => <<"alice">>}))}].

%%====================================================================
%% calculate_tres_usage/1 Tests
%%====================================================================

calculate_tres_usage_test_() ->
    [{"calculates usage for job with all resources",
      fun() ->
          Record = #job_record{
              job_id = 1,
              job_name = <<>>,
              user_name = <<>>,
              user_id = 0,
              group_id = 0,
              account = <<>>,
              partition = <<>>,
              cluster = <<>>,
              qos = <<>>,
              state = completed,
              exit_code = 0,
              num_nodes = 4,
              num_cpus = 32,
              num_tasks = 8,
              req_mem = 16384,
              submit_time = 0,
              eligible_time = 0,
              start_time = 0,
              end_time = 3600,
              elapsed = 3600,
              tres_alloc = #{gpu => 2},
              tres_req = #{},
              work_dir = <<>>,
              std_out = <<>>,
              std_err = <<>>
          },
          Usage = flurm_dbd_server:calculate_tres_usage(Record),
          %% cpu_seconds = elapsed * num_cpus = 3600 * 32 = 115200
          ?assertEqual(115200, maps:get(cpu_seconds, Usage)),
          %% mem_seconds = elapsed * req_mem = 3600 * 16384 = 58982400
          ?assertEqual(58982400, maps:get(mem_seconds, Usage)),
          %% node_seconds = elapsed * num_nodes = 3600 * 4 = 14400
          ?assertEqual(14400, maps:get(node_seconds, Usage)),
          %% gpu_seconds = elapsed * gpu = 3600 * 2 = 7200
          ?assertEqual(7200, maps:get(gpu_seconds, Usage))
      end},
     {"calculates usage for job with no GPU",
      fun() ->
          Record = #job_record{
              job_id = 2,
              job_name = <<>>,
              user_name = <<>>,
              user_id = 0,
              group_id = 0,
              account = <<>>,
              partition = <<>>,
              cluster = <<>>,
              qos = <<>>,
              state = completed,
              exit_code = 0,
              num_nodes = 1,
              num_cpus = 4,
              num_tasks = 1,
              req_mem = 1024,
              submit_time = 0,
              eligible_time = 0,
              start_time = 0,
              end_time = 60,
              elapsed = 60,
              tres_alloc = #{},
              tres_req = #{},
              work_dir = <<>>,
              std_out = <<>>,
              std_err = <<>>
          },
          Usage = flurm_dbd_server:calculate_tres_usage(Record),
          ?assertEqual(240, maps:get(cpu_seconds, Usage)),
          ?assertEqual(61440, maps:get(mem_seconds, Usage)),
          ?assertEqual(60, maps:get(node_seconds, Usage)),
          ?assertEqual(0, maps:get(gpu_seconds, Usage))
      end},
     {"calculates zero usage for zero elapsed time",
      fun() ->
          Record = #job_record{
              job_id = 3,
              job_name = <<>>,
              user_name = <<>>,
              user_id = 0,
              group_id = 0,
              account = <<>>,
              partition = <<>>,
              cluster = <<>>,
              qos = <<>>,
              state = pending,
              exit_code = 0,
              num_nodes = 10,
              num_cpus = 100,
              num_tasks = 50,
              req_mem = 65536,
              submit_time = 0,
              eligible_time = 0,
              start_time = 0,
              end_time = 0,
              elapsed = 0,
              tres_alloc = #{gpu => 8},
              tres_req = #{},
              work_dir = <<>>,
              std_out = <<>>,
              std_err = <<>>
          },
          Usage = flurm_dbd_server:calculate_tres_usage(Record),
          ?assertEqual(0, maps:get(cpu_seconds, Usage)),
          ?assertEqual(0, maps:get(mem_seconds, Usage)),
          ?assertEqual(0, maps:get(node_seconds, Usage)),
          ?assertEqual(0, maps:get(gpu_seconds, Usage))
      end}].

%%====================================================================
%% format_sacct_row/1 Tests
%%====================================================================

format_sacct_row_test_() ->
    [{"formats complete job data",
      fun() ->
          Job = #{
              job_id => 12345,
              user_name => <<"testuser">>,
              account => <<"research">>,
              partition => <<"compute">>,
              state => completed,
              elapsed => 3661,
              exit_code => 0
          },
          Row = lists:flatten(flurm_dbd_server:format_sacct_row(Job)),
          ?assert(string:find(Row, "12345") =/= nomatch),
          ?assert(string:find(Row, "testuser") =/= nomatch),
          ?assert(string:find(Row, "research") =/= nomatch),
          ?assert(string:find(Row, "compute") =/= nomatch),
          ?assert(string:find(Row, "completed") =/= nomatch),
          ?assert(string:find(Row, "01:01:01") =/= nomatch),
          ?assert(string:find(Row, "0:0") =/= nomatch)
      end},
     {"formats job with minimal data",
      fun() ->
          Job = #{
              job_id => 1,
              user_name => <<>>,
              account => <<>>,
              partition => <<>>,
              state => unknown,
              elapsed => 0,
              exit_code => 0
          },
          Row = lists:flatten(flurm_dbd_server:format_sacct_row(Job)),
          ?assert(string:find(Row, "1") =/= nomatch),
          ?assert(string:find(Row, "00:00:00") =/= nomatch)
      end},
     {"formats job with failed exit code",
      fun() ->
          Job = #{
              job_id => 999,
              user_name => <<"user">>,
              account => <<"acc">>,
              partition => <<"part">>,
              state => failed,
              elapsed => 100,
              exit_code => 127
          },
          Row = lists:flatten(flurm_dbd_server:format_sacct_row(Job)),
          ?assert(string:find(Row, "failed") =/= nomatch),
          ?assert(string:find(Row, "127:0") =/= nomatch)
      end}].

%%====================================================================
%% job_info_to_record/1 Tests
%%====================================================================

job_info_to_record_test_() ->
    [{"converts complete job info map to record",
      fun() ->
          Info = #{
              job_id => 12345,
              name => <<"my_job">>,
              user_name => <<"alice">>,
              user_id => 1000,
              group_id => 1000,
              account => <<"research">>,
              partition => <<"compute">>,
              cluster => <<"production">>,
              qos => <<"high">>,
              state => running,
              exit_code => 0,
              num_nodes => 4,
              num_cpus => 32,
              num_tasks => 16,
              req_mem => 8192,
              submit_time => 1000,
              eligible_time => 1000,
              start_time => 1100,
              end_time => 0,
              elapsed => 0,
              tres_alloc => #{cpu => 32, gpu => 2},
              tres_req => #{cpu => 32, gpu => 2},
              work_dir => <<"/home/alice">>,
              std_out => <<"/tmp/out">>,
              std_err => <<"/tmp/err">>
          },
          Record = flurm_dbd_server:job_info_to_record(Info),
          ?assertEqual(12345, Record#job_record.job_id),
          ?assertEqual(<<"my_job">>, Record#job_record.job_name),
          ?assertEqual(<<"alice">>, Record#job_record.user_name),
          ?assertEqual(1000, Record#job_record.user_id),
          ?assertEqual(<<"research">>, Record#job_record.account),
          ?assertEqual(<<"compute">>, Record#job_record.partition),
          ?assertEqual(<<"production">>, Record#job_record.cluster),
          ?assertEqual(<<"high">>, Record#job_record.qos),
          ?assertEqual(running, Record#job_record.state),
          ?assertEqual(4, Record#job_record.num_nodes),
          ?assertEqual(32, Record#job_record.num_cpus),
          ?assertEqual(16, Record#job_record.num_tasks),
          ?assertEqual(8192, Record#job_record.req_mem)
      end},
     {"converts minimal job info with defaults",
      fun() ->
          Info = #{job_id => 1},
          Record = flurm_dbd_server:job_info_to_record(Info),
          ?assertEqual(1, Record#job_record.job_id),
          ?assertEqual(<<>>, Record#job_record.job_name),
          ?assertEqual(<<>>, Record#job_record.user_name),
          ?assertEqual(0, Record#job_record.user_id),
          ?assertEqual(<<>>, Record#job_record.account),
          ?assertEqual(<<>>, Record#job_record.partition),
          ?assertEqual(<<"flurm">>, Record#job_record.cluster),
          ?assertEqual(<<"normal">>, Record#job_record.qos),
          ?assertEqual(pending, Record#job_record.state),
          ?assertEqual(1, Record#job_record.num_nodes),
          ?assertEqual(1, Record#job_record.num_cpus),
          ?assertEqual(1, Record#job_record.num_tasks),
          ?assertEqual(0, Record#job_record.req_mem)
      end}].

%%====================================================================
%% step_info_to_record/1 Tests
%%====================================================================

step_info_to_record_test_() ->
    [{"converts complete step info to record",
      fun() ->
          Info = #{
              job_id => 12345,
              step_id => 0,
              name => <<"step0">>,
              state => completed,
              exit_code => 0,
              num_tasks => 4,
              num_nodes => 2,
              start_time => 1100,
              end_time => 1200,
              elapsed => 100,
              tres_alloc => #{cpu => 8, mem => 4096}
          },
          Record = flurm_dbd_server:step_info_to_record(Info),
          ?assertEqual(12345, Record#step_record.job_id),
          ?assertEqual(0, Record#step_record.step_id),
          ?assertEqual(<<"step0">>, Record#step_record.step_name),
          ?assertEqual(completed, Record#step_record.state),
          ?assertEqual(0, Record#step_record.exit_code),
          ?assertEqual(4, Record#step_record.num_tasks),
          ?assertEqual(2, Record#step_record.num_nodes),
          ?assertEqual(100, Record#step_record.elapsed)
      end},
     {"converts minimal step info with defaults",
      fun() ->
          Info = #{job_id => 1, step_id => 0},
          Record = flurm_dbd_server:step_info_to_record(Info),
          ?assertEqual(1, Record#step_record.job_id),
          ?assertEqual(0, Record#step_record.step_id),
          ?assertEqual(<<>>, Record#step_record.step_name),
          ?assertEqual(pending, Record#step_record.state),
          ?assertEqual(0, Record#step_record.exit_code),
          ?assertEqual(1, Record#step_record.num_tasks),
          ?assertEqual(1, Record#step_record.num_nodes)
      end}].

%%====================================================================
%% update_job_record/2 Tests
%%====================================================================

update_job_record_test_() ->
    BaseRecord = #job_record{
        job_id = 100,
        job_name = <<"test">>,
        user_name = <<"user">>,
        user_id = 1000,
        group_id = 1000,
        account = <<"acc">>,
        partition = <<"part">>,
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = running,
        exit_code = 0,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 1,
        req_mem = 1024,
        submit_time = 1000,
        eligible_time = 1000,
        start_time = 1100,
        end_time = 0,
        elapsed = 0,
        tres_alloc = #{cpu => 4},
        tres_req = #{cpu => 4},
        work_dir = <<>>,
        std_out = <<>>,
        std_err = <<>>
    },
    [{"updates state and exit_code",
      fun() ->
          Updates = #{state => completed, exit_code => 0, end_time => 1200},
          Updated = flurm_dbd_server:update_job_record(BaseRecord, Updates),
          ?assertEqual(completed, Updated#job_record.state),
          ?assertEqual(0, Updated#job_record.exit_code),
          ?assertEqual(1200, Updated#job_record.end_time),
          ?assertEqual(100, Updated#job_record.elapsed)
      end},
     {"updates with failed exit code",
      fun() ->
          Updates = #{state => failed, exit_code => 1, end_time => 1500},
          Updated = flurm_dbd_server:update_job_record(BaseRecord, Updates),
          ?assertEqual(failed, Updated#job_record.state),
          ?assertEqual(1, Updated#job_record.exit_code),
          ?assertEqual(400, Updated#job_record.elapsed)
      end},
     {"preserves existing start_time",
      fun() ->
          Updates = #{end_time => 1300},
          Updated = flurm_dbd_server:update_job_record(BaseRecord, Updates),
          ?assertEqual(1100, Updated#job_record.start_time),
          ?assertEqual(200, Updated#job_record.elapsed)
      end},
     {"updates tres_alloc",
      fun() ->
          Updates = #{tres_alloc => #{cpu => 8, gpu => 1}},
          Updated = flurm_dbd_server:update_job_record(BaseRecord, Updates),
          ?assertEqual(#{cpu => 8, gpu => 1}, Updated#job_record.tres_alloc)
      end}].

%%====================================================================
%% update_assoc_record/2 Tests
%%====================================================================

update_assoc_record_test_() ->
    BaseAssoc = #assoc_record{
        id = 1,
        cluster = <<"flurm">>,
        account = <<"research">>,
        user = <<"alice">>,
        partition = <<>>,
        shares = 1,
        max_jobs = 0,
        max_submit = 0,
        max_wall = 0,
        grp_tres = #{},
        max_tres_per_job = #{},
        qos = [],
        default_qos = <<>>,
        usage = #{}
    },
    [{"updates shares",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{shares => 100}),
          ?assertEqual(100, Updated#assoc_record.shares)
      end},
     {"updates max_jobs",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{max_jobs => 50}),
          ?assertEqual(50, Updated#assoc_record.max_jobs)
      end},
     {"updates partition",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{partition => <<"gpu">>}),
          ?assertEqual(<<"gpu">>, Updated#assoc_record.partition)
      end},
     {"updates grp_tres",
      fun() ->
          NewTres = #{cpu => 1000, gpu => 8},
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{grp_tres => NewTres}),
          ?assertEqual(NewTres, Updated#assoc_record.grp_tres)
      end},
     {"updates qos list",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{qos => [<<"high">>, <<"normal">>]}),
          ?assertEqual([<<"high">>, <<"normal">>], Updated#assoc_record.qos)
      end},
     {"updates default_qos",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{default_qos => <<"high">>}),
          ?assertEqual(<<"high">>, Updated#assoc_record.default_qos)
      end},
     {"updates multiple fields",
      fun() ->
          Updates = #{shares => 50, max_jobs => 25, max_submit => 100},
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, Updates),
          ?assertEqual(50, Updated#assoc_record.shares),
          ?assertEqual(25, Updated#assoc_record.max_jobs),
          ?assertEqual(100, Updated#assoc_record.max_submit)
      end},
     {"preserves unchanged fields",
      fun() ->
          Updated = flurm_dbd_server:update_assoc_record(BaseAssoc, #{shares => 100}),
          ?assertEqual(1, Updated#assoc_record.id),
          ?assertEqual(<<"flurm">>, Updated#assoc_record.cluster),
          ?assertEqual(<<"research">>, Updated#assoc_record.account),
          ?assertEqual(<<"alice">>, Updated#assoc_record.user),
          ?assertEqual(0, Updated#assoc_record.max_jobs)
      end}].

%%====================================================================
%% Module Export Verification Tests
%%====================================================================

ifdef_exports_available_test_() ->
    [{"job_info_to_record/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, job_info_to_record, 1))},
     {"update_job_record/2 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, update_job_record, 2))},
     {"step_info_to_record/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, step_info_to_record, 1))},
     {"job_record_to_map/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, job_record_to_map, 1))},
     {"assoc_record_to_map/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, assoc_record_to_map, 1))},
     {"update_assoc_record/2 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, update_assoc_record, 2))},
     {"filter_job_records/2 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, filter_job_records, 2))},
     {"matches_job_filters/2 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, matches_job_filters, 2))},
     {"format_sacct_row/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, format_sacct_row, 1))},
     {"format_elapsed/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, format_elapsed, 1))},
     {"format_exit_code/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, format_exit_code, 1))},
     {"calculate_tres_usage/1 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, calculate_tres_usage, 1))},
     {"current_period/0 is exported in TEST mode",
      ?_assert(erlang:function_exported(flurm_dbd_server, current_period, 0))}].
