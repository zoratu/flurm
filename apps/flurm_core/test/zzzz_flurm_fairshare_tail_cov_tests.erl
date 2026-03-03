%%%-------------------------------------------------------------------
%%% @doc Deterministic tail coverage tests for flurm_fairshare.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_fairshare_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    decay_timer :: reference() | undefined,
    total_shares :: non_neg_integer(),
    total_usage :: float()
}).

-define(USAGE_TABLE, flurm_fairshare_usage).
-define(SHARES_TABLE, flurm_fairshare_shares).

start_link_error_branch_test() ->
    ensure_server_stopped(),
    cleanup_tables(),
    OldTrap = process_flag(trap_exit, true),
    try
        _ = ets:new(?USAGE_TABLE, [named_table, public, set]),
        ?assertMatch({error, _}, flurm_fairshare:start_link()),
        receive
            {'EXIT', _, _} -> ok
        after 0 ->
            ok
        end
    after
        _ = process_flag(trap_exit, OldTrap),
        ensure_server_stopped(),
        cleanup_tables()
    end.

terminate_undefined_timer_branch_test() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    ?assertEqual(ok, flurm_fairshare:terminate(normal, State)).

code_change_passthrough_branch_test() ->
    State = #state{decay_timer = undefined, total_shares = 7, total_usage = 9.5},
    ?assertEqual({ok, State}, flurm_fairshare:code_change(old, State, [])).

decay_tiny_usage_delete_branch_test() ->
    ensure_server_stopped(),
    cleanup_tables(),
    try
        _ = ets:new(?USAGE_TABLE, [named_table, public, set]),
        _ = ets:new(?SHARES_TABLE, [named_table, public, set]),
        Old = erlang:system_time(second) - (365 * 24 * 60 * 60),
        Key = {<<"tiny">>, <<"acct">>},
        ets:insert(?USAGE_TABLE, {Key, 0.001, Old}),
        State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.001},
        {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),
        ?assertEqual([], ets:lookup(?USAGE_TABLE, Key)),
        ?assertEqual(0.0, NewState#state.total_usage)
    after
        cleanup_tables()
    end.

ensure_server_stopped() ->
    case whereis(flurm_fairshare) of
        undefined ->
            ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            ok
    end.

cleanup_tables() ->
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.
