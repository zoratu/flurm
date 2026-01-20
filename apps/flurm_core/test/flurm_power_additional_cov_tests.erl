%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM Power Management
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").

power_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun basic_api_tests/1,
       fun power_state_tests/1,
       fun policy_tests/1,
       fun energy_tracking_tests/1,
       fun power_cap_tests/1,
       fun legacy_api_tests/1,
       fun message_handling_tests/1
      ]}}.

setup() ->
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

per_test_setup() ->
    {ok, Pid} = flurm_power:start_link(),
    Pid.

per_test_cleanup(Pid) ->
    catch gen_server:stop(Pid),
    catch ets:delete(flurm_power_state),
    catch ets:delete(flurm_energy_metrics),
    catch ets:delete(flurm_job_energy),
    catch ets:delete(flurm_node_releases),
    ok.

basic_api_tests(_Pid) ->
    [
        {"get_status returns map", fun() ->
            Status = flurm_power:get_status(),
            ?assert(is_map(Status)),
            ?assert(maps:is_key(enabled, Status))
        end},

        {"enable/disable", fun() ->
            ?assertEqual(ok, flurm_power:enable()),
            ?assertEqual(ok, flurm_power:disable())
        end}
    ].

power_state_tests(_Pid) ->
    [
        {"get_node_power_state unknown", fun() ->
            ?assertEqual({error, not_found}, flurm_power:get_node_power_state(<<"unknown">>))
        end},

        {"configure_node_power creates entry", fun() ->
            ?assertEqual(ok, flurm_power:configure_node_power(<<"node01">>, #{method => script}))
        end},

        {"get_nodes_by_power_state returns list", fun() ->
            Nodes = flurm_power:get_nodes_by_power_state(powered_on),
            ?assert(is_list(Nodes))
        end}
    ].

policy_tests(_Pid) ->
    [
        {"get_policy returns default", fun() ->
            ?assertEqual(balanced, flurm_power:get_policy())
        end},

        {"set_policy changes policy", fun() ->
            ?assertEqual(ok, flurm_power:set_policy(aggressive)),
            ?assertEqual(aggressive, flurm_power:get_policy())
        end}
    ].

energy_tracking_tests(_Pid) ->
    [
        {"record_power_usage works", fun() ->
            ?assertEqual(ok, flurm_power:record_power_usage(<<"node01">>, 150)),
            _ = sys:get_state(flurm_power)
        end},

        {"get_job_energy for unknown", fun() ->
            ?assertEqual({error, not_found}, flurm_power:get_job_energy(99999))
        end},

        {"record_job_energy accumulates", fun() ->
            ?assertEqual(ok, flurm_power:record_job_energy(12345, [<<"n1">>], 5.0)),
            _ = sys:get_state(flurm_power),
            ?assertEqual(ok, flurm_power:record_job_energy(12345, [<<"n1">>], 3.0)),
            _ = sys:get_state(flurm_power),
            {ok, Energy} = flurm_power:get_job_energy(12345),
            ?assertEqual(8.0, Energy)
        end}
    ].

power_cap_tests(_Pid) ->
    [
        {"get_power_cap initially undefined", fun() ->
            ?assertEqual(undefined, flurm_power:get_power_cap())
        end},

        {"set_power_cap sets value", fun() ->
            ?assertEqual(ok, flurm_power:set_power_cap(10000)),
            ?assertEqual(10000, flurm_power:get_power_cap())
        end},

        {"get_current_power returns integer", fun() ->
            Power = flurm_power:get_current_power(),
            ?assert(is_integer(Power))
        end}
    ].

legacy_api_tests(_Pid) ->
    [
        {"get_power_stats returns map", fun() ->
            Stats = flurm_power:get_power_stats(),
            ?assert(is_map(Stats))
        end},

        {"idle_timeout functions", fun() ->
            ?assertEqual(300, flurm_power:get_idle_timeout()),
            ?assertEqual(ok, flurm_power:set_idle_timeout(600)),
            ?assertEqual(600, flurm_power:get_idle_timeout())
        end},

        {"set_power_budget delegates", fun() ->
            ?assertEqual(ok, flurm_power:set_power_budget(5000)),
            ?assertEqual(5000, flurm_power:get_power_budget())
        end}
    ].

message_handling_tests(Pid) ->
    [
        {"handle_info check_power", fun() ->
            Pid ! check_power,
            _ = sys:get_state(flurm_power),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info sample_energy", fun() ->
            Pid ! sample_energy,
            _ = sys:get_state(flurm_power),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info unknown", fun() ->
            Pid ! unknown,
            _ = sys:get_state(flurm_power),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_call unknown", fun() ->
            Result = gen_server:call(Pid, unknown_request),
            ?assertEqual({error, unknown_request}, Result)
        end}
    ].
