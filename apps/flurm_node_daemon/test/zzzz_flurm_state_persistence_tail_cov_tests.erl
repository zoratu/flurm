%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_state_persistence edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_state_persistence_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

save_state_non_map_hits_exception_catch_test() ->
    Unique = integer_to_list(erlang:unique_integer([positive, monotonic])),
    StateFile = filename:join("/tmp", "flurm_state_tail_cov_" ++ Unique ++ ".dat"),
    application:set_env(flurm_node_daemon, state_file, StateFile),
    try
        Result = flurm_state_persistence:save_state(not_a_map),
        ?assertMatch({error, {error, {badmap, _}}}, Result)
    after
        application:unset_env(flurm_node_daemon, state_file),
        file:delete(StateFile),
        file:delete(StateFile ++ ".tmp")
    end.

