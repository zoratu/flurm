%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_power internal functions
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test: validate_transition/2
%%====================================================================

validate_transition_test_() ->
    [
     {"powered_on can transition to off",
      ?_assertEqual(ok, flurm_power:validate_transition(powered_on, off))},

     {"powered_on can transition to suspend",
      ?_assertEqual(ok, flurm_power:validate_transition(powered_on, suspend))},

     {"powered_on transitioning to on is already in state",
      ?_assertEqual(ok, flurm_power:validate_transition(powered_on, powered_on))},

     {"powered_off can transition to on",
      ?_assertEqual(ok, flurm_power:validate_transition(powered_off, on))},

     {"powered_off can transition to resume",
      ?_assertEqual(ok, flurm_power:validate_transition(powered_off, resume))},

     {"suspended can transition to resume",
      ?_assertEqual(ok, flurm_power:validate_transition(suspended, resume))},

     {"suspended can transition to off",
      ?_assertEqual(ok, flurm_power:validate_transition(suspended, off))},

     {"powering_up cannot be interrupted",
      ?_assertMatch({error, {invalid_transition, powering_up, _}},
                    flurm_power:validate_transition(powering_up, off))},

     {"powering_down cannot be interrupted",
      ?_assertMatch({error, {invalid_transition, powering_down, _}},
                    flurm_power:validate_transition(powering_down, on))}
    ].

%%====================================================================
%% Test: get_policy_settings/1
%%====================================================================

get_policy_settings_test_() ->
    [
     {"aggressive policy has 60 second idle timeout",
      fun() ->
          Settings = flurm_power:get_policy_settings(aggressive),
          ?assertEqual(60, maps:get(idle_timeout, Settings))
      end},

     {"aggressive policy has 1 min powered node",
      fun() ->
          Settings = flurm_power:get_policy_settings(aggressive),
          ?assertEqual(1, maps:get(min_powered_nodes, Settings))
      end},

     {"aggressive policy has 3 resume threshold",
      fun() ->
          Settings = flurm_power:get_policy_settings(aggressive),
          ?assertEqual(3, maps:get(resume_threshold, Settings))
      end},

     {"balanced policy has 300 second idle timeout",
      fun() ->
          Settings = flurm_power:get_policy_settings(balanced),
          ?assertEqual(300, maps:get(idle_timeout, Settings))
      end},

     {"balanced policy has 2 min powered nodes",
      fun() ->
          Settings = flurm_power:get_policy_settings(balanced),
          ?assertEqual(2, maps:get(min_powered_nodes, Settings))
      end},

     {"balanced policy has 5 resume threshold",
      fun() ->
          Settings = flurm_power:get_policy_settings(balanced),
          ?assertEqual(5, maps:get(resume_threshold, Settings))
      end},

     {"conservative policy has 900 second idle timeout",
      fun() ->
          Settings = flurm_power:get_policy_settings(conservative),
          ?assertEqual(900, maps:get(idle_timeout, Settings))
      end},

     {"conservative policy has 5 min powered nodes",
      fun() ->
          Settings = flurm_power:get_policy_settings(conservative),
          ?assertEqual(5, maps:get(min_powered_nodes, Settings))
      end},

     {"conservative policy has 10 resume threshold",
      fun() ->
          Settings = flurm_power:get_policy_settings(conservative),
          ?assertEqual(10, maps:get(resume_threshold, Settings))
      end}
    ].

%%====================================================================
%% Test: parse_mac_address/1
%%====================================================================

parse_mac_address_test_() ->
    [
     {"parses colon-separated MAC address",
      ?_assertEqual(<<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
                    flurm_power:parse_mac_address(<<"AA:BB:CC:DD:EE:FF">>))},

     {"parses dash-separated MAC address",
      ?_assertEqual(<<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
                    flurm_power:parse_mac_address(<<"AA-BB-CC-DD-EE-FF">>))},

     {"parses lowercase MAC address",
      ?_assertEqual(<<16#aa, 16#bb, 16#cc, 16#dd, 16#ee, 16#ff>>,
                    flurm_power:parse_mac_address(<<"aa:bb:cc:dd:ee:ff">>))},

     {"parses all zeros MAC",
      ?_assertEqual(<<0, 0, 0, 0, 0, 0>>,
                    flurm_power:parse_mac_address(<<"00:00:00:00:00:00">>))},

     {"parses broadcast MAC",
      ?_assertEqual(<<16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>,
                    flurm_power:parse_mac_address(<<"FF:FF:FF:FF:FF:FF">>))}
    ].

%%====================================================================
%% Test: build_wol_packet/1
%%====================================================================

build_wol_packet_test_() ->
    [
     {"builds 102 byte WoL magic packet",
      fun() ->
          MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
          Packet = flurm_power:build_wol_packet(MAC),
          ?assertEqual(102, byte_size(Packet))
      end},

     {"WoL packet starts with 6 bytes of 0xFF",
      fun() ->
          MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
          Packet = flurm_power:build_wol_packet(MAC),
          <<Header:6/binary, _Rest/binary>> = Packet,
          ?assertEqual(<<255, 255, 255, 255, 255, 255>>, Header)
      end},

     {"WoL packet contains MAC repeated 16 times",
      fun() ->
          MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
          Packet = flurm_power:build_wol_packet(MAC),
          <<_Header:6/binary, Body/binary>> = Packet,
          %% Body should be 96 bytes (16 * 6)
          ?assertEqual(96, byte_size(Body)),
          %% Each 6-byte chunk should be the MAC
          ?assertEqual(MAC, binary:part(Body, 0, 6)),
          ?assertEqual(MAC, binary:part(Body, 6, 6)),
          ?assertEqual(MAC, binary:part(Body, 90, 6))
      end},

     {"WoL packet for all-zeros MAC",
      fun() ->
          MAC = <<0, 0, 0, 0, 0, 0>>,
          Packet = flurm_power:build_wol_packet(MAC),
          ?assertEqual(102, byte_size(Packet)),
          <<Header:6/binary, _/binary>> = Packet,
          ?assertEqual(<<255, 255, 255, 255, 255, 255>>, Header)
      end}
    ].
