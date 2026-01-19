%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config internal functions exposed via -ifdef(TEST)
%%%-------------------------------------------------------------------
-module(flurm_config_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% key_to_env_name/1 tests
%%====================================================================

key_to_env_name_test_() ->
    [
        {"converts tuple key to uppercase env name",
         ?_assertEqual("FLURM_CONTROLLER_LISTEN_PORT",
                       flurm_config:key_to_env_name({flurm_controller, listen_port}))},
        {"converts atom key to uppercase env name with FLURM prefix",
         ?_assertEqual("FLURM_CLUSTER_NAME",
                       flurm_config:key_to_env_name(cluster_name))},
        {"handles single word atom",
         ?_assertEqual("FLURM_PORT",
                       flurm_config:key_to_env_name(port))},
        {"handles tuple with underscores in both atoms",
         ?_assertEqual("FLURM_DB_RA_CLUSTER_NAME",
                       flurm_config:key_to_env_name({flurm_db_ra, cluster_name}))},
        {"handles empty atom in tuple",
         ?_assertEqual("FLURM_CONFIG_",
                       flurm_config:key_to_env_name({flurm_config, ''}))}
    ].

%%====================================================================
%% key_to_app_key/1 tests
%%====================================================================

key_to_app_key_test_() ->
    [
        {"returns tuple unchanged when already in app/key format",
         ?_assertEqual({flurm_controller, listen_port},
                       flurm_config:key_to_app_key({flurm_controller, listen_port}))},
        {"converts single atom to {flurm_config, Key} tuple",
         ?_assertEqual({flurm_config, cluster_name},
                       flurm_config:key_to_app_key(cluster_name))},
        {"preserves app name in tuple",
         ?_assertEqual({flurm_db, timeout},
                       flurm_config:key_to_app_key({flurm_db, timeout}))},
        {"handles single word atom",
         ?_assertEqual({flurm_config, port},
                       flurm_config:key_to_app_key(port))}
    ].

%%====================================================================
%% parse_env_value/1 tests
%%====================================================================

parse_env_value_test_() ->
    [
        {"parses 'true' string to true atom",
         ?_assertEqual(true, flurm_config:parse_env_value("true"))},
        {"parses 'false' string to false atom",
         ?_assertEqual(false, flurm_config:parse_env_value("false"))},
        {"parses integer string to integer",
         ?_assertEqual(42, flurm_config:parse_env_value("42"))},
        {"parses negative integer string",
         ?_assertEqual(-123, flurm_config:parse_env_value("-123"))},
        {"parses zero",
         ?_assertEqual(0, flurm_config:parse_env_value("0"))},
        {"parses float string to float",
         ?_assertEqual(3.14, flurm_config:parse_env_value("3.14"))},
        {"parses negative float",
         ?_assertEqual(-2.5, flurm_config:parse_env_value("-2.5"))},
        {"parses regular string to binary",
         ?_assertEqual(<<"hello">>, flurm_config:parse_env_value("hello"))},
        {"parses path string to binary",
         ?_assertEqual(<<"/etc/flurm/config">>, flurm_config:parse_env_value("/etc/flurm/config"))},
        {"parses string with spaces to binary",
         ?_assertEqual(<<"hello world">>, flurm_config:parse_env_value("hello world"))},
        {"parses empty string to binary",
         ?_assertEqual(<<>>, flurm_config:parse_env_value(""))},
        {"parses large integer",
         ?_assertEqual(1000000, flurm_config:parse_env_value("1000000"))}
    ].
