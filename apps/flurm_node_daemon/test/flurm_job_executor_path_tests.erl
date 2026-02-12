%%%-------------------------------------------------------------------
%%% @doc EUnit tests for flurm_job_executor path helper functions
%%%
%%% Tests the expand_output_path/2 and ensure_working_dir/2 internal
%%% functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_path_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

expand_output_path_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"expand_output_path replaces %j with job ID",
       fun expand_job_id_lowercase_test/0},
      {"expand_output_path replaces %J with job ID",
       fun expand_job_id_uppercase_test/0},
      {"expand_output_path replaces %N with hostname",
       fun expand_hostname_test/0},
      {"expand_output_path replaces %n with 0",
       fun expand_node_number_test/0},
      {"expand_output_path replaces %t with 0",
       fun expand_task_number_test/0},
      {"expand_output_path replaces %% with literal %",
       fun expand_literal_percent_test/0},
      {"expand_output_path handles multiple placeholders in one path",
       fun expand_multiple_placeholders_test/0},
      {"expand_output_path returns path unchanged when no placeholders",
       fun expand_no_placeholders_test/0},
      {"expand_output_path handles default SLURM path format",
       fun expand_default_slurm_path_test/0},
      {"expand_output_path handles all placeholders combined",
       fun expand_all_placeholders_combined_test/0}
     ]}.

ensure_working_dir_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"ensure_working_dir returns existing directory as-is",
       fun ensure_existing_dir_test/0},
      {"ensure_working_dir falls back to /tmp for non-existent parent",
       fun ensure_nonexistent_parent_test/0},
      {"ensure_working_dir handles empty binary path gracefully",
       fun ensure_empty_path_test/0}
     ]}.

%%====================================================================
%% expand_output_path Tests
%%====================================================================

expand_job_id_lowercase_test() ->
    Result = flurm_job_executor:expand_output_path("/output/slurm-%j.out", 42),
    ?assertEqual("/output/slurm-42.out", Result).

expand_job_id_uppercase_test() ->
    Result = flurm_job_executor:expand_output_path("/output/slurm-%J.out", 99),
    ?assertEqual("/output/slurm-99.out", Result).

expand_hostname_test() ->
    {ok, Hostname} = inet:gethostname(),
    Result = flurm_job_executor:expand_output_path("/output/%N.out", 1),
    ?assertEqual("/output/" ++ Hostname ++ ".out", Result).

expand_node_number_test() ->
    Result = flurm_job_executor:expand_output_path("/output/node-%n.out", 1),
    ?assertEqual("/output/node-0.out", Result).

expand_task_number_test() ->
    Result = flurm_job_executor:expand_output_path("/output/task-%t.out", 1),
    ?assertEqual("/output/task-0.out", Result).

expand_literal_percent_test() ->
    Result = flurm_job_executor:expand_output_path("/output/100%%.out", 1),
    ?assertEqual("/output/100%.out", Result).

expand_multiple_placeholders_test() ->
    {ok, Hostname} = inet:gethostname(),
    Result = flurm_job_executor:expand_output_path("/output/%j/%N/slurm-%j.out", 777),
    ?assertEqual("/output/777/" ++ Hostname ++ "/slurm-777.out", Result).

expand_no_placeholders_test() ->
    Result = flurm_job_executor:expand_output_path("/var/log/my_job.out", 500),
    ?assertEqual("/var/log/my_job.out", Result).

expand_default_slurm_path_test() ->
    Result = flurm_job_executor:expand_output_path("/tmp/slurm-%j.out", 12345),
    ?assertEqual("/tmp/slurm-12345.out", Result).

expand_all_placeholders_combined_test() ->
    {ok, Hostname} = inet:gethostname(),
    Result = flurm_job_executor:expand_output_path(
        "/data/%j/%J/%N/%n/%t/%%.log", 256),
    Expected = "/data/256/256/" ++ Hostname ++ "/0/0/%.log",
    ?assertEqual(Expected, Result).

%%====================================================================
%% ensure_working_dir Tests
%%====================================================================

ensure_existing_dir_test() ->
    %% /tmp always exists
    Result = flurm_job_executor:ensure_working_dir(<<"/tmp">>, 1),
    ?assertEqual("/tmp", Result).

ensure_nonexistent_parent_test() ->
    %% A deeply nested path under a non-existent root should fall back to /tmp
    Result = flurm_job_executor:ensure_working_dir(
        <<"/nonexistent_root_abc123/deeply/nested/dir">>, 2),
    ?assertEqual("/tmp", Result).

ensure_empty_path_test() ->
    %% An empty binary path will not be a valid directory, so ensure
    %% it handles gracefully (falls back to /tmp or returns current dir)
    Result = flurm_job_executor:ensure_working_dir(<<>>, 3),
    ?assert(is_list(Result)).
