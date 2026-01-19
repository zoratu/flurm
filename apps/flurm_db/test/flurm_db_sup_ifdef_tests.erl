%%%-------------------------------------------------------------------
%%% @doc Tests for internal functions exported via -ifdef(TEST)
%%% in flurm_db_sup module.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% get_data_dir/0 Tests
%%====================================================================

get_data_dir_test_() ->
    {"get_data_dir/0 returns appropriate data directory",
     {setup,
      fun() ->
          %% Save original env value
          application:get_env(flurm_db, data_dir)
      end,
      fun(OriginalEnv) ->
          %% Restore original env value
          case OriginalEnv of
              {ok, Value} ->
                  application:set_env(flurm_db, data_dir, Value);
              undefined ->
                  application:unset_env(flurm_db, data_dir)
          end
      end,
      [
       {"returns configured data_dir when set",
        fun() ->
            TestDir = "/tmp/flurm_test_data",
            application:set_env(flurm_db, data_dir, TestDir),
            Result = flurm_db_sup:get_data_dir(),
            ?assertEqual(TestDir, Result)
        end},

       {"returns /var/lib/flurm when exists and no config",
        fun() ->
            application:unset_env(flurm_db, data_dir),
            Result = flurm_db_sup:get_data_dir(),
            %% Result should be either /var/lib/flurm or cwd/data
            %% depending on whether /var/lib/flurm exists
            ?assert(is_list(Result)),
            ?assert(length(Result) > 0)
        end},

       {"returns cwd/data fallback when /var/lib/flurm does not exist",
        fun() ->
            application:unset_env(flurm_db, data_dir),
            Result = flurm_db_sup:get_data_dir(),
            %% On most test systems /var/lib/flurm won't exist
            %% so we expect the cwd/data fallback
            case filelib:is_dir("/var/lib/flurm") of
                true ->
                    ?assertEqual("/var/lib/flurm", Result);
                false ->
                    {ok, Cwd} = file:get_cwd(),
                    Expected = filename:join(Cwd, "data"),
                    ?assertEqual(Expected, Result)
            end
        end},

       {"returns string result",
        fun() ->
            application:unset_env(flurm_db, data_dir),
            Result = flurm_db_sup:get_data_dir(),
            ?assert(is_list(Result))
        end},

       {"handles custom path with trailing slash",
        fun() ->
            TestDir = "/tmp/flurm_test/",
            application:set_env(flurm_db, data_dir, TestDir),
            Result = flurm_db_sup:get_data_dir(),
            ?assertEqual(TestDir, Result)
        end},

       {"handles relative path in config",
        fun() ->
            TestDir = "relative/path/data",
            application:set_env(flurm_db, data_dir, TestDir),
            Result = flurm_db_sup:get_data_dir(),
            ?assertEqual(TestDir, Result)
        end}
      ]}}.

%%====================================================================
%% Integration-style tests for get_data_dir behavior
%%====================================================================

get_data_dir_integration_test_() ->
    {"get_data_dir integration tests",
     {setup,
      fun() ->
          application:get_env(flurm_db, data_dir)
      end,
      fun(OriginalEnv) ->
          case OriginalEnv of
              {ok, Value} ->
                  application:set_env(flurm_db, data_dir, Value);
              undefined ->
                  application:unset_env(flurm_db, data_dir)
          end
      end,
      [
       {"returns non-empty path",
        fun() ->
            application:unset_env(flurm_db, data_dir),
            Result = flurm_db_sup:get_data_dir(),
            ?assertNotEqual("", Result),
            ?assertNotEqual([], Result)
        end},

       {"result is valid for filename operations",
        fun() ->
            application:set_env(flurm_db, data_dir, "/tmp/test_flurm"),
            Result = flurm_db_sup:get_data_dir(),
            JoinedPath = filename:join(Result, "test.dets"),
            ?assertEqual("/tmp/test_flurm/test.dets", JoinedPath)
        end}
      ]}}.
