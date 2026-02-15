%%%-------------------------------------------------------------------
%%% @doc Cover-effective tests for flurm_power internal functions.
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST):
%%% - validate_transition/2
%%% - get_policy_settings/1
%%% - parse_mac_address/1
%%% - build_wol_packet/1
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% validate_transition/2 tests
%%====================================================================

validate_transition_test_() ->
    {"validate_transition/2 tests",
     [
      %% Valid transitions from powered_on
      {"powered_on -> off is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_on, off))},
      {"powered_on -> suspend is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_on, suspend))},
      {"powered_on -> powered_on (same state) is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_on, powered_on))},

      %% Valid transitions from powered_off
      {"powered_off -> on is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_off, on))},
      {"powered_off -> resume is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_off, resume))},

      %% Valid transitions from suspended
      {"suspended -> resume is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(suspended, resume))},
      {"suspended -> off is valid",
       ?_assertEqual(ok, flurm_power:validate_transition(suspended, off))},

      %% Transitional states cannot be interrupted
      {"powering_up -> off is invalid (cannot interrupt)",
       ?_assertMatch({error, {invalid_transition, powering_up, off}},
                     flurm_power:validate_transition(powering_up, off))},
      {"powering_up -> on is invalid (cannot interrupt)",
       ?_assertMatch({error, {invalid_transition, powering_up, on}},
                     flurm_power:validate_transition(powering_up, on))},
      {"powering_down -> on is invalid (cannot interrupt)",
       ?_assertMatch({error, {invalid_transition, powering_down, on}},
                     flurm_power:validate_transition(powering_down, on))},
      {"powering_down -> off is invalid (cannot interrupt)",
       ?_assertMatch({error, {invalid_transition, powering_down, off}},
                     flurm_power:validate_transition(powering_down, off))},

      %% Invalid transitions
      {"powered_on -> on is invalid (except same state check)",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_on, on))},
      {"powered_off -> off is same state - valid",
       ?_assertEqual(ok, flurm_power:validate_transition(powered_off, powered_off))},
      {"powered_off -> suspend is invalid",
       ?_assertMatch({error, {invalid_transition, powered_off, suspend}},
                     flurm_power:validate_transition(powered_off, suspend))},
      {"suspended -> suspend is same state - valid",
       ?_assertEqual(ok, flurm_power:validate_transition(suspended, suspended))}
     ]}.

%%====================================================================
%% get_policy_settings/1 tests
%%====================================================================

get_policy_settings_test_() ->
    {"get_policy_settings/1 tests",
     [
      %% Aggressive policy
      {"aggressive: idle_timeout is 60 seconds",
       ?_assertEqual(60, maps:get(idle_timeout,
                                  flurm_power:get_policy_settings(aggressive)))},
      {"aggressive: min_powered_nodes is 1",
       ?_assertEqual(1, maps:get(min_powered_nodes,
                                 flurm_power:get_policy_settings(aggressive)))},
      {"aggressive: resume_threshold is 3",
       ?_assertEqual(3, maps:get(resume_threshold,
                                 flurm_power:get_policy_settings(aggressive)))},

      %% Balanced policy
      {"balanced: idle_timeout is 300 seconds",
       ?_assertEqual(300, maps:get(idle_timeout,
                                   flurm_power:get_policy_settings(balanced)))},
      {"balanced: min_powered_nodes is 2",
       ?_assertEqual(2, maps:get(min_powered_nodes,
                                 flurm_power:get_policy_settings(balanced)))},
      {"balanced: resume_threshold is 5",
       ?_assertEqual(5, maps:get(resume_threshold,
                                 flurm_power:get_policy_settings(balanced)))},

      %% Conservative policy
      {"conservative: idle_timeout is 900 seconds",
       ?_assertEqual(900, maps:get(idle_timeout,
                                   flurm_power:get_policy_settings(conservative)))},
      {"conservative: min_powered_nodes is 5",
       ?_assertEqual(5, maps:get(min_powered_nodes,
                                 flurm_power:get_policy_settings(conservative)))},
      {"conservative: resume_threshold is 10",
       ?_assertEqual(10, maps:get(resume_threshold,
                                  flurm_power:get_policy_settings(conservative)))}
     ]}.

%%====================================================================
%% parse_mac_address/1 tests
%%====================================================================

parse_mac_address_test_() ->
    {"parse_mac_address/1 tests",
     [
      %% Colon-separated format
      {"parses colon-separated uppercase MAC",
       ?_assertEqual(<<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
                     flurm_power:parse_mac_address(<<"AA:BB:CC:DD:EE:FF">>))},
      {"parses colon-separated lowercase MAC",
       ?_assertEqual(<<16#aa, 16#bb, 16#cc, 16#dd, 16#ee, 16#ff>>,
                     flurm_power:parse_mac_address(<<"aa:bb:cc:dd:ee:ff">>))},

      %% Hyphen-separated format
      {"parses hyphen-separated uppercase MAC",
       ?_assertEqual(<<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
                     flurm_power:parse_mac_address(<<"AA-BB-CC-DD-EE-FF">>))},
      {"parses hyphen-separated lowercase MAC",
       ?_assertEqual(<<16#aa, 16#bb, 16#cc, 16#dd, 16#ee, 16#ff>>,
                     flurm_power:parse_mac_address(<<"aa-bb-cc-dd-ee-ff">>))},

      %% Edge cases
      {"parses all-zeros MAC",
       ?_assertEqual(<<0, 0, 0, 0, 0, 0>>,
                     flurm_power:parse_mac_address(<<"00:00:00:00:00:00">>))},
      {"parses broadcast MAC (all FF)",
       ?_assertEqual(<<16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>,
                     flurm_power:parse_mac_address(<<"FF:FF:FF:FF:FF:FF">>))},
      {"parses mixed case MAC",
       ?_assertEqual(<<16#Aa, 16#Bb, 16#Cc, 16#Dd, 16#Ee, 16#Ff>>,
                     flurm_power:parse_mac_address(<<"Aa:Bb:Cc:Dd:Ee:Ff">>))}
     ]}.

%%====================================================================
%% build_wol_packet/1 tests
%%====================================================================

build_wol_packet_test_() ->
    {"build_wol_packet/1 tests",
     [
      {"packet is exactly 102 bytes",
       fun() ->
           MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
           Packet = flurm_power:build_wol_packet(MAC),
           ?assertEqual(102, byte_size(Packet))
       end},

      {"first 6 bytes are 0xFF (synchronization stream)",
       fun() ->
           MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
           Packet = flurm_power:build_wol_packet(MAC),
           <<Header:6/binary, _Body/binary>> = Packet,
           ?assertEqual(<<255, 255, 255, 255, 255, 255>>, Header)
       end},

      {"MAC is repeated 16 times after header",
       fun() ->
           MAC = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF>>,
           Packet = flurm_power:build_wol_packet(MAC),
           <<_Header:6/binary, Body/binary>> = Packet,
           %% Body should be 96 bytes (16 repetitions * 6 bytes)
           ?assertEqual(96, byte_size(Body)),
           %% Verify MAC appears 16 times
           ?assertEqual(binary:copy(MAC, 16), Body)
       end},

      {"packet structure for all-zeros MAC",
       fun() ->
           MAC = <<0, 0, 0, 0, 0, 0>>,
           Packet = flurm_power:build_wol_packet(MAC),
           ?assertEqual(102, byte_size(Packet)),
           <<Header:6/binary, Body/binary>> = Packet,
           ?assertEqual(<<255, 255, 255, 255, 255, 255>>, Header),
           ?assertEqual(binary:copy(MAC, 16), Body)
       end},

      {"packet structure for broadcast MAC",
       fun() ->
           MAC = <<16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>,
           Packet = flurm_power:build_wol_packet(MAC),
           ?assertEqual(102, byte_size(Packet)),
           %% Entire packet should be 0xFF
           ?assertEqual(binary:copy(<<255>>, 102), Packet)
       end}
     ]}.
