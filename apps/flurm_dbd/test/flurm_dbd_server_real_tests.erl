%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_dbd_server module
%%%
%%% Tests the DBD server internal functions via the exported TEST
%%% interface. Tests record conversion, filtering, and formatting
%%% functions directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Helper Functions (exported via -ifdef(TEST))
%%====================================================================

%% Test job_info_to_record function
job_info_to_record_test_() ->
    [
     {"converts minimal job info",
      fun() ->
          JobInfo = #{job_id => 123},
          Record = flurm_dbd_server:job_info_to_record(JobInfo),
          ?assert(is_tuple(Record)),
          ?assertEqual(job_record, element(1, Record)),
          ?assertEqual(123, element(2, Record))  %% job_id field
      end},
     {"converts full job info",
      fun() ->
          Now = erlang:system_time(second),
          JobInfo = #{
              job_id => 456,
              name => <<"test_job">>,
              user_name => <<"testuser">>,
              user_id => 1000,
              group_id => 1000,
              account => <<"research">>,
              partition => <<"compute">>,
              cluster => <<"mycluster">>,
              qos => <<"high">>,
              state => running,
              exit_code => 0,
              num_nodes => 4,
              num_cpus => 32,
              num_tasks => 4,
              req_mem => 65536,
              submit_time => Now,
              eligible_time => Now,
              start_time => Now,
              end_time => 0,
              elapsed => 0,
              work_dir => <<"/home/user/work">>,
              std_out => <<"/tmp/job.out">>,
              std_err => <<"/tmp/job.err">>
          },
          Record = flurm_dbd_server:job_info_to_record(JobInfo),
          ?assertEqual(456, element(2, Record)),  %% job_id
          ?assertEqual(<<"test_job">>, element(3, Record)),  %% job_name
          ?assertEqual(<<"testuser">>, element(4, Record))  %% user_name
      end}
    ].

%% Test update_job_record function
update_job_record_test_() ->
    [
     {"updates exit_code",
      fun() ->
          Existing = create_test_job_record(1),
          Updates = #{exit_code => 42},
          Updated = flurm_dbd_server:update_job_record(Existing, Updates),
          %% Convert to map to check the field
          Map = flurm_dbd_server:job_record_to_map(Updated),
          ?assertEqual(42, maps:get(exit_code, Map))
      end},
     {"updates state",
      fun() ->
          Existing = create_test_job_record(1),
          Updates = #{state => completed},
          Updated = flurm_dbd_server:update_job_record(Existing, Updates),
          %% Convert to map to check the field
          Map = flurm_dbd_server:job_record_to_map(Updated),
          ?assertEqual(completed, maps:get(state, Map))
      end}
    ].

%% Test step_info_to_record function
step_info_to_record_test_() ->
    [
     {"converts step info",
      fun() ->
          StepInfo = #{
              job_id => 123,
              step_id => 0,
              name => <<"step.0">>,
              state => completed,
              exit_code => 0,
              num_tasks => 4,
              num_nodes => 2,
              start_time => 1000,
              end_time => 2000,
              elapsed => 1000
          },
          Record = flurm_dbd_server:step_info_to_record(StepInfo),
          ?assert(is_tuple(Record)),
          ?assertEqual(step_record, element(1, Record)),
          ?assertEqual(123, element(2, Record)),  %% job_id
          ?assertEqual(0, element(3, Record))  %% step_id
      end}
    ].

%% Test job_record_to_map function
job_record_to_map_test_() ->
    [
     {"converts record to map",
      fun() ->
          Record = create_test_job_record(789),
          Map = flurm_dbd_server:job_record_to_map(Record),
          ?assert(is_map(Map)),
          ?assertEqual(789, maps:get(job_id, Map)),
          ?assert(maps:is_key(user_name, Map)),
          ?assert(maps:is_key(account, Map)),
          ?assert(maps:is_key(partition, Map)),
          ?assert(maps:is_key(state, Map))
      end}
    ].

%% Test assoc_record_to_map function
assoc_record_to_map_test_() ->
    [
     {"converts association record to map",
      fun() ->
          Record = create_test_assoc_record(1),
          Map = flurm_dbd_server:assoc_record_to_map(Record),
          ?assert(is_map(Map)),
          ?assertEqual(1, maps:get(id, Map)),
          ?assert(maps:is_key(cluster, Map)),
          ?assert(maps:is_key(account, Map)),
          ?assert(maps:is_key(user, Map))
      end}
    ].

%% Test update_assoc_record function
update_assoc_record_test_() ->
    [
     {"updates shares",
      fun() ->
          Existing = create_test_assoc_record(1),
          Updates = #{shares => 100},
          Updated = flurm_dbd_server:update_assoc_record(Existing, Updates),
          %% Convert to map to check the field
          Map = flurm_dbd_server:assoc_record_to_map(Updated),
          ?assertEqual(100, maps:get(shares, Map))
      end},
     {"updates max_jobs",
      fun() ->
          Existing = create_test_assoc_record(1),
          Updates = #{max_jobs => 50},
          Updated = flurm_dbd_server:update_assoc_record(Existing, Updates),
          %% Convert to map to check the field
          Map = flurm_dbd_server:assoc_record_to_map(Updated),
          ?assertEqual(50, maps:get(max_jobs, Map))
      end}
    ].

%% Test filter_job_records function
filter_job_records_test_() ->
    [
     {"empty filter returns all records",
      fun() ->
          Records = [create_test_job_record(1), create_test_job_record(2)],
          Filtered = flurm_dbd_server:filter_job_records(Records, #{}),
          ?assertEqual(2, length(Filtered))
      end}
    ].

%% Test matches_job_filters function
matches_job_filters_test_() ->
    [
     {"empty filter matches any record",
      fun() ->
          Record = create_test_job_record(1),
          Result = flurm_dbd_server:matches_job_filters(Record, #{}),
          ?assertEqual(true, Result)
      end}
    ].

%% Test format_sacct_row function
format_sacct_row_test_() ->
    [
     {"formats job map as sacct row",
      fun() ->
          Job = #{
              job_id => 123,
              user_name => <<"testuser">>,
              account => <<"research">>,
              partition => <<"compute">>,
              state => completed,
              elapsed => 3661,
              exit_code => 0
          },
          Row = flurm_dbd_server:format_sacct_row(Job),
          ?assert(is_list(Row) orelse is_binary(Row)),
          %% Should contain job id
          RowStr = lists:flatten(Row),
          ?assert(string:find(RowStr, "123") =/= nomatch)
      end}
    ].

%% Test format_elapsed function
format_elapsed_test_() ->
    [
     {"formats 0 seconds",
      ?_assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(0)))},
     {"formats 1 hour",
      ?_assertEqual("01:00:00", lists:flatten(flurm_dbd_server:format_elapsed(3600)))},
     {"formats mixed time",
      ?_assertEqual("02:30:45", lists:flatten(flurm_dbd_server:format_elapsed(9045)))},
     {"handles negative as default",
      ?_assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(-1)))}
    ].

%% Test format_exit_code function
format_exit_code_test_() ->
    [
     {"formats 0 exit code",
      ?_assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(0)))},
     {"formats non-zero exit code",
      ?_assertEqual("1:0", lists:flatten(flurm_dbd_server:format_exit_code(1)))},
     {"handles non-integer as default",
      ?_assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(not_an_int)))}
    ].

%% Test current_period function
current_period_test_() ->
    [
     {"returns YYYY-MM format",
      fun() ->
          Period = flurm_dbd_server:current_period(),
          ?assert(is_binary(Period)),
          ?assertEqual(7, byte_size(Period)),  %% "YYYY-MM"
          %% Check format: should match DDDD-DD pattern
          <<Y1, Y2, Y3, Y4, $-, M1, M2>> = Period,
          ?assert(Y1 >= $0 andalso Y1 =< $9),
          ?assert(Y2 >= $0 andalso Y2 =< $9),
          ?assert(Y3 >= $0 andalso Y3 =< $9),
          ?assert(Y4 >= $0 andalso Y4 =< $9),
          ?assert(M1 >= $0 andalso M1 =< $1),
          ?assert(M2 >= $0 andalso M2 =< $9)
      end}
    ].

%%====================================================================
%% API Function Export Tests
%%====================================================================

api_exports_test_() ->
    [
     {"start_link/0 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({start_link, 0}, Exports))
      end},
     {"record_job_start/1 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({record_job_start, 1}, Exports))
      end},
     {"record_job_end/1 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({record_job_end, 1}, Exports))
      end},
     {"get_job_record/1 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({get_job_record, 1}, Exports))
      end},
     {"list_job_records/0 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({list_job_records, 0}, Exports))
      end},
     {"get_user_usage/1 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({get_user_usage, 1}, Exports))
      end},
     {"get_account_usage/1 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({get_account_usage, 1}, Exports))
      end},
     {"add_association/3 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({add_association, 3}, Exports))
      end},
     {"get_stats/0 is exported",
      fun() ->
          Exports = flurm_dbd_server:module_info(exports),
          ?assert(lists:member({get_stats, 0}, Exports))
      end}
    ].

%%====================================================================
%% Internal Test Helpers
%%====================================================================

%% Create a test job_record tuple
create_test_job_record(JobId) ->
    {job_record,
     JobId,                    %% job_id
     <<"test_job">>,           %% job_name
     <<"testuser">>,           %% user_name
     1000,                     %% user_id
     1000,                     %% group_id
     <<"research">>,           %% account
     <<"compute">>,            %% partition
     <<"flurm">>,              %% cluster
     <<"normal">>,             %% qos
     pending,                  %% state
     0,                        %% exit_code
     1,                        %% num_nodes
     1,                        %% num_cpus
     1,                        %% num_tasks
     1024,                     %% req_mem
     0,                        %% submit_time
     0,                        %% eligible_time
     0,                        %% start_time
     0,                        %% end_time
     0,                        %% elapsed
     #{},                      %% tres_alloc
     #{},                      %% tres_req
     <<"/tmp">>,               %% work_dir
     <<>>,                     %% std_out
     <<>>                      %% std_err
    }.

%% Create a test assoc_record tuple
create_test_assoc_record(AssocId) ->
    {assoc_record,
     AssocId,                  %% id
     <<"flurm">>,              %% cluster
     <<"research">>,           %% account
     <<"testuser">>,           %% user
     <<>>,                     %% partition
     1,                        %% shares
     0,                        %% max_jobs
     0,                        %% max_submit
     0,                        %% max_wall
     #{},                      %% grp_tres
     #{},                      %% max_tres_per_job
     [],                       %% qos
     <<>>,                     %% default_qos
     #{}                       %% usage
    }.
