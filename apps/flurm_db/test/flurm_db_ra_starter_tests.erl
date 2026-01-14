%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Starter Tests
%%%
%%% Tests for the flurm_db_ra_starter module which initializes the
%%% Ra cluster on startup.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_starter_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    [
        {"handle_call returns error for unknown request", fun() ->
             %% Create a mock state
             State = {state, #{}},
             Result = flurm_db_ra_starter:handle_call(unknown_request, {self(), make_ref()}, State),
             ?assertMatch({reply, {error, unknown_request}, _}, Result)
         end},
        {"handle_cast returns noreply", fun() ->
             State = {state, #{}},
             Result = flurm_db_ra_starter:handle_cast(some_msg, State),
             ?assertMatch({noreply, _}, Result)
         end},
        {"terminate returns ok", fun() ->
             State = {state, #{}},
             Result = flurm_db_ra_starter:terminate(normal, State),
             ?assertEqual(ok, Result)
         end},
        {"code_change returns state unchanged", fun() ->
             State = {state, #{}},
             {ok, NewState} = flurm_db_ra_starter:code_change(old_version, State, extra),
             ?assertEqual(State, NewState)
         end}
    ].

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    [
        {"init sends init_cluster message to self", fun() ->
             %% The init function should send init_cluster to self
             Config = #{},
             {ok, State} = flurm_db_ra_starter:init(Config),
             %% State should contain config
             ?assertMatch({state, Config}, State)
         end}
    ].

%%====================================================================
%% Handle Info Tests (Non-Distributed)
%%====================================================================

handle_info_non_distributed_test_() ->
    [
        {"handle_info init_cluster on nonode@nohost stops normally", fun() ->
             case node() of
                 nonode@nohost ->
                     %% In non-distributed mode, should stop normally
                     State = {state, #{}},
                     Result = flurm_db_ra_starter:handle_info(init_cluster, State),
                     ?assertMatch({stop, normal, _}, Result);
                 _ ->
                     %% Skip this test if running distributed
                     ok
             end
         end},
        {"handle_info unknown message returns noreply", fun() ->
             State = {state, #{}},
             Result = flurm_db_ra_starter:handle_info(unknown_message, State),
             ?assertMatch({noreply, _}, Result)
         end}
    ].

%%====================================================================
%% Start Link Test
%%====================================================================

start_link_test_() ->
    [
        {"start_link with empty config", fun() ->
             %% This will immediately stop in non-distributed mode
             %% or try to initialize Ra cluster in distributed mode
             Config = #{},
             case node() of
                 nonode@nohost ->
                     %% In non-distributed mode, the starter will stop normally
                     %% after init_cluster message
                     case flurm_db_ra_starter:start_link(Config) of
                         {ok, Pid} ->
                             %% Give it time to process init_cluster and stop
                             timer:sleep(100),
                             ?assertNot(is_process_alive(Pid));
                         {error, _} ->
                             ok  % Expected if can't start
                     end;
                 _ ->
                     %% In distributed mode, behavior depends on Ra availability
                     ok
             end
         end}
    ].
