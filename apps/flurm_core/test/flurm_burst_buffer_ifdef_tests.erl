%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_burst_buffer internal functions
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test: normalize_type/1
%%====================================================================

normalize_type_test_() ->
    [
     {"normalize atom generic",
      ?_assertEqual(generic, flurm_burst_buffer:normalize_type(generic))},

     {"normalize atom datawarp",
      ?_assertEqual(datawarp, flurm_burst_buffer:normalize_type(datawarp))},

     {"normalize atom lua",
      ?_assertEqual(lua, flurm_burst_buffer:normalize_type(lua))},

     {"normalize binary generic",
      ?_assertEqual(generic, flurm_burst_buffer:normalize_type(<<"generic">>))},

     {"normalize binary datawarp",
      ?_assertEqual(datawarp, flurm_burst_buffer:normalize_type(<<"datawarp">>))},

     {"normalize binary lua",
      ?_assertEqual(lua, flurm_burst_buffer:normalize_type(<<"lua">>))},

     {"normalize string generic",
      ?_assertEqual(generic, flurm_burst_buffer:normalize_type("generic"))},

     {"unknown type defaults to generic",
      ?_assertEqual(generic, flurm_burst_buffer:normalize_type(unknown))},

     {"unknown binary defaults to generic",
      ?_assertEqual(generic, flurm_burst_buffer:normalize_type(<<"unknown">>))}
    ].

%%====================================================================
%% Test: parse_size/1
%%====================================================================

parse_size_test_() ->
    [
     {"parse integer",
      ?_assertEqual(1024, flurm_burst_buffer:parse_size(1024))},

     {"parse bytes",
      ?_assertEqual(500, flurm_burst_buffer:parse_size(<<"500B">>))},

     {"parse kilobytes",
      ?_assertEqual(1024, flurm_burst_buffer:parse_size(<<"1KB">>))},

     {"parse kilobytes shorthand",
      ?_assertEqual(1024, flurm_burst_buffer:parse_size(<<"1K">>))},

     {"parse megabytes",
      ?_assertEqual(1024 * 1024, flurm_burst_buffer:parse_size(<<"1MB">>))},

     {"parse megabytes shorthand",
      ?_assertEqual(1024 * 1024, flurm_burst_buffer:parse_size(<<"1M">>))},

     {"parse gigabytes",
      ?_assertEqual(1024 * 1024 * 1024, flurm_burst_buffer:parse_size(<<"1GB">>))},

     {"parse gigabytes shorthand",
      ?_assertEqual(1024 * 1024 * 1024, flurm_burst_buffer:parse_size(<<"1G">>))},

     {"parse terabytes",
      ?_assertEqual(1024 * 1024 * 1024 * 1024, flurm_burst_buffer:parse_size(<<"1TB">>))},

     {"parse terabytes shorthand",
      ?_assertEqual(1024 * 1024 * 1024 * 1024, flurm_burst_buffer:parse_size(<<"1T">>))},

     {"parse large gigabytes",
      ?_assertEqual(100 * 1024 * 1024 * 1024, flurm_burst_buffer:parse_size(<<"100GB">>))},

     {"parse string format",
      ?_assertEqual(512 * 1024 * 1024, flurm_burst_buffer:parse_size("512MB"))},

     {"parse lowercase",
      ?_assertEqual(1024 * 1024, flurm_burst_buffer:parse_size(<<"1mb">>))},

     {"invalid format returns 0",
      ?_assertEqual(0, flurm_burst_buffer:parse_size(<<"invalid">>))},

     {"empty string returns 0",
      ?_assertEqual(0, flurm_burst_buffer:parse_size(<<>>))}
    ].

%%====================================================================
%% Test: format_size/1
%%====================================================================

format_size_test_() ->
    [
     {"format bytes",
      ?_assertEqual(<<"500B">>, flurm_burst_buffer:format_size(500))},

     {"format kilobytes",
      ?_assertEqual(<<"1KB">>, flurm_burst_buffer:format_size(1024))},

     {"format megabytes",
      ?_assertEqual(<<"1MB">>, flurm_burst_buffer:format_size(1024 * 1024))},

     {"format gigabytes",
      ?_assertEqual(<<"1GB">>, flurm_burst_buffer:format_size(1024 * 1024 * 1024))},

     {"format terabytes",
      ?_assertEqual(<<"1TB">>, flurm_burst_buffer:format_size(1024 * 1024 * 1024 * 1024))},

     {"format 100 gigabytes",
      ?_assertEqual(<<"100GB">>, flurm_burst_buffer:format_size(100 * 1024 * 1024 * 1024))},

     {"format zero",
      ?_assertEqual(<<"0B">>, flurm_burst_buffer:format_size(0))}
    ].

%%====================================================================
%% Test: round_to_granularity/2
%%====================================================================

round_to_granularity_test_() ->
    [
     {"rounds up to granularity",
      ?_assertEqual(1024 * 1024, flurm_burst_buffer:round_to_granularity(
                                   500000, 1024 * 1024))},

     {"exact match no change",
      ?_assertEqual(1024 * 1024, flurm_burst_buffer:round_to_granularity(
                                   1024 * 1024, 1024 * 1024))},

     {"rounds up small amount",
      ?_assertEqual(2 * 1024 * 1024, flurm_burst_buffer:round_to_granularity(
                                       1024 * 1024 + 1, 1024 * 1024))},

     {"zero size returns zero",
      ?_assertEqual(0, flurm_burst_buffer:round_to_granularity(0, 1024))},

     {"granularity of 1 returns same size",
      ?_assertEqual(12345, flurm_burst_buffer:round_to_granularity(12345, 1))}
    ].

%%====================================================================
%% Test: generate_bb_path/2
%%====================================================================

generate_bb_path_test_() ->
    [
     {"generates path with job id and pool",
      ?_assertEqual(<<"/bb/default/123">>,
                    flurm_burst_buffer:generate_bb_path(123, <<"default">>))},

     {"generates path with different pool",
      ?_assertEqual(<<"/bb/fast_nvme/456">>,
                    flurm_burst_buffer:generate_bb_path(456, <<"fast_nvme">>))},

     {"handles large job ids",
      ?_assertEqual(<<"/bb/pool/9999999999">>,
                    flurm_burst_buffer:generate_bb_path(9999999999, <<"pool">>))}
    ].

%%====================================================================
%% Test: map_to_request/1
%%====================================================================

map_to_request_test_() ->
    [
     {"converts map to request with all fields",
      fun() ->
          Map = #{pool => <<"fast">>, size => <<"10GB">>,
                  access => private, type => cache,
                  persistent => true, persistent_name => <<"mydata">>},
          Result = flurm_burst_buffer:map_to_request(Map),
          %% Result is a bb_request record
          ?assertEqual(<<"fast">>, element(2, Result)),  % pool
          ?assertEqual(10 * 1024 * 1024 * 1024, element(3, Result)),  % size
          ?assertEqual(private, element(4, Result)),  % access
          ?assertEqual(cache, element(5, Result)),  % type
          ?assertEqual(true, element(8, Result))  % persistent
      end},

     {"uses defaults for missing fields",
      fun() ->
          Map = #{},
          Result = flurm_burst_buffer:map_to_request(Map),
          ?assertEqual(any, element(2, Result)),  % pool defaults to any
          ?assertEqual(0, element(3, Result)),  % size defaults to 0
          ?assertEqual(striped, element(4, Result)),  % access defaults to striped
          ?assertEqual(scratch, element(5, Result)),  % type defaults to scratch
          ?assertEqual(false, element(8, Result))  % persistent defaults to false
      end},

     {"handles capacity alias for size",
      fun() ->
          Map = #{capacity => <<"5GB">>},
          Result = flurm_burst_buffer:map_to_request(Map),
          ?assertEqual(5 * 1024 * 1024 * 1024, element(3, Result))
      end}
    ].

%%====================================================================
%% Test: parse_directive_opts/1
%%====================================================================

parse_directive_opts_test_() ->
    [
     {"parses key=value pairs",
      fun() ->
          Parts = [<<"name=test">>, <<"capacity=10GB">>],
          Result = flurm_burst_buffer:parse_directive_opts(Parts),
          ?assertEqual(<<"test">>, maps:get(name, Result)),
          ?assertEqual(<<"10GB">>, maps:get(capacity, Result))
      end},

     {"parses flag without value",
      fun() ->
          Parts = [<<"persistent">>, <<"pool=fast">>],
          Result = flurm_burst_buffer:parse_directive_opts(Parts),
          ?assertEqual(true, maps:get(persistent, Result)),
          ?assertEqual(<<"fast">>, maps:get(pool, Result))
      end},

     {"handles empty list",
      ?_assertEqual(#{}, flurm_burst_buffer:parse_directive_opts([]))}
    ].

%%====================================================================
%% Test: format_if_set/2
%%====================================================================

format_if_set_test_() ->
    [
     {"format pool",
      ?_assertEqual(<<"pool=default">>,
                    flurm_burst_buffer:format_if_set(pool, <<"default">>))},

     {"format capacity",
      fun() ->
          Result = flurm_burst_buffer:format_if_set(capacity, 1024 * 1024 * 1024),
          ?assertEqual(<<"capacity=1GB">>, Result)
      end},

     {"format access",
      ?_assertEqual(<<"access=striped">>,
                    flurm_burst_buffer:format_if_set(access, striped))},

     {"format type",
      ?_assertEqual(<<"type=cache">>,
                    flurm_burst_buffer:format_if_set(type, cache))},

     {"returns empty for any",
      ?_assertEqual(<<>>, flurm_burst_buffer:format_if_set(pool, any))},

     {"returns empty for undefined",
      ?_assertEqual(<<>>, flurm_burst_buffer:format_if_set(pool, undefined))},

     {"returns empty for zero",
      ?_assertEqual(<<>>, flurm_burst_buffer:format_if_set(capacity, 0))}
    ].

%%====================================================================
%% Test: parse_bb_part/2
%%====================================================================

parse_bb_part_test_() ->
    [
     {"parse pool",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"pool=fast">>, Request),
          ?assertEqual(<<"fast">>, element(2, Result))
      end},

     {"parse capacity",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"capacity=5GB">>, Request),
          ?assertEqual(5 * 1024 * 1024 * 1024, element(3, Result))
      end},

     {"parse size alias",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"size=2GB">>, Request),
          ?assertEqual(2 * 1024 * 1024 * 1024, element(3, Result))
      end},

     {"parse access striped",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"access=striped">>, Request),
          ?assertEqual(striped, element(4, Result))
      end},

     {"parse access private",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"access=private">>, Request),
          ?assertEqual(private, element(4, Result))
      end},

     {"parse type scratch",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"type=scratch">>, Request),
          ?assertEqual(scratch, element(5, Result))
      end},

     {"parse type cache",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"type=cache">>, Request),
          ?assertEqual(cache, element(5, Result))
      end},

     {"parse persistent flag",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"persistent">>, Request),
          ?assertEqual(true, element(8, Result))
      end},

     {"ignore unknown parts",
      fun() ->
          Request = flurm_burst_buffer:map_to_request(#{}),
          Result = flurm_burst_buffer:parse_bb_part(<<"unknown=value">>, Request),
          ?assertEqual(Request, Result)
      end}
    ].
