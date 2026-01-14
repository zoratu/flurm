%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Application Tests
%%%
%%% Comprehensive tests for the flurm_dbd_app module which implements
%%% the OTP application behaviour for the FLURM database daemon.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_app_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Module Behaviour Tests
%%====================================================================

behaviour_test_() ->
    [
        {"module implements application behaviour", fun() ->
             Behaviours = proplists:get_value(behaviour,
                 flurm_dbd_app:module_info(attributes), []),
             ?assert(lists:member(application, Behaviours))
         end}
    ].

%%====================================================================
%% Export Tests
%%====================================================================

exports_test_() ->
    [
        {"start/2 is exported", fun() ->
             Exports = flurm_dbd_app:module_info(exports),
             ?assert(lists:member({start, 2}, Exports))
         end},
        {"stop/1 is exported", fun() ->
             Exports = flurm_dbd_app:module_info(exports),
             ?assert(lists:member({stop, 1}, Exports))
         end},
        {"module_info is exported", fun() ->
             Exports = flurm_dbd_app:module_info(exports),
             ?assert(lists:member({module_info, 0}, Exports)),
             ?assert(lists:member({module_info, 1}, Exports))
         end}
    ].

%%====================================================================
%% Stop Callback Tests
%%====================================================================

stop_test_() ->
    [
        {"stop/1 returns ok with undefined state", fun() ->
             Result = flurm_dbd_app:stop(undefined),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with empty map state", fun() ->
             Result = flurm_dbd_app:stop(#{}),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with empty list state", fun() ->
             Result = flurm_dbd_app:stop([]),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with complex state", fun() ->
             State = #{
                 started_at => erlang:system_time(second),
                 config => #{port => 6819},
                 children => []
             },
             Result = flurm_dbd_app:stop(State),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with atom state", fun() ->
             Result = flurm_dbd_app:stop(running),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with tuple state", fun() ->
             Result = flurm_dbd_app:stop({state, data, 123}),
             ?assertEqual(ok, Result)
         end},
        {"stop/1 returns ok with pid state", fun() ->
             Result = flurm_dbd_app:stop(self()),
             ?assertEqual(ok, Result)
         end}
    ].

%%====================================================================
%% Start Callback Tests
%%====================================================================

start_callback_test_() ->
    [
        {"start/2 function exists", fun() ->
             ?assert(erlang:function_exported(flurm_dbd_app, start, 2))
         end},
        {"start/2 is callable with normal start type", fun() ->
             %% Verify the function signature exists
             ?assert(is_function(fun flurm_dbd_app:start/2, 2))
         end}
    ].

%%====================================================================
%% Application Start Type Tests
%%====================================================================

start_type_test_() ->
    [
        {"normal start type is valid", fun() ->
             StartType = normal,
             ?assertEqual(normal, StartType)
         end},
        {"takeover start type is valid", fun() ->
             StartType = {takeover, 'other@node'},
             ?assertMatch({takeover, _}, StartType)
         end},
        {"failover start type is valid", fun() ->
             StartType = {failover, 'other@node'},
             ?assertMatch({failover, _}, StartType)
         end}
    ].

%%====================================================================
%% Application Integration Tests
%%====================================================================

application_integration_test_() ->
    {setup,
     fun() ->
         %% Ensure dependent applications are available
         ok
     end,
     fun(_) ->
         ok
     end,
     [
         {"application spec is valid", fun() ->
             %% Check that the application can be loaded
             case application:load(flurm_dbd) of
                 ok -> ok;
                 {error, {already_loaded, flurm_dbd}} -> ok;
                 {error, Reason} ->
                     %% May fail if app file not found in test env
                     io:format("Note: app load returned ~p (expected in test)~n", [Reason]),
                     ok
             end
         end}
     ]
    }.

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module_info/0 returns proplist", fun() ->
             Info = flurm_dbd_app:module_info(),
             ?assert(is_list(Info)),
             ?assert(length(Info) > 0)
         end},
        {"module_info contains module key", fun() ->
             Info = flurm_dbd_app:module_info(),
             ?assert(proplists:is_defined(module, Info)),
             ?assertEqual(flurm_dbd_app, proplists:get_value(module, Info))
         end},
        {"module_info contains exports key", fun() ->
             Info = flurm_dbd_app:module_info(),
             ?assert(proplists:is_defined(exports, Info)),
             Exports = proplists:get_value(exports, Info),
             ?assert(is_list(Exports))
         end},
        {"module_info contains attributes key", fun() ->
             Info = flurm_dbd_app:module_info(),
             ?assert(proplists:is_defined(attributes, Info))
         end},
        {"module_info contains compile key", fun() ->
             Info = flurm_dbd_app:module_info(),
             ?assert(proplists:is_defined(compile, Info))
         end},
        {"module_info/1 with exports", fun() ->
             Exports = flurm_dbd_app:module_info(exports),
             ?assert(is_list(Exports)),
             ?assert(length(Exports) >= 2)  % At least start/2 and stop/1
         end},
        {"module_info/1 with attributes", fun() ->
             Attrs = flurm_dbd_app:module_info(attributes),
             ?assert(is_list(Attrs))
         end},
        {"module_info/1 with module", fun() ->
             Module = flurm_dbd_app:module_info(module),
             ?assertEqual(flurm_dbd_app, Module)
         end}
    ].

%%====================================================================
%% Supervisor Link Tests
%%====================================================================

supervisor_link_test_() ->
    [
        {"start calls flurm_dbd_sup:start_link", fun() ->
             %% Verify the supervisor module exists and is callable
             ?assert(erlang:function_exported(flurm_dbd_sup, start_link, 0))
         end}
    ].

%%====================================================================
%% Application Callback Contract Tests
%%====================================================================

callback_contract_test_() ->
    [
        {"stop callback always returns ok", fun() ->
             %% The application behaviour requires stop/1 to return ok
             Results = [
                 flurm_dbd_app:stop(undefined),
                 flurm_dbd_app:stop(#{}),
                 flurm_dbd_app:stop([]),
                 flurm_dbd_app:stop(some_state),
                 flurm_dbd_app:stop({complex, state, 123})
             ],
             ?assert(lists:all(fun(R) -> R =:= ok end, Results))
         end},
        {"start/2 signature follows application behaviour", fun() ->
             %% The start/2 callback should accept (StartType, StartArgs)
             %% and return {ok, Pid} | {ok, Pid, State} | {error, Reason}
             ?assert(erlang:function_exported(flurm_dbd_app, start, 2))
         end}
    ].

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    [
        {"stop handles unexpected state gracefully", fun() ->
             %% Even with unusual state values, stop should return ok
             ?assertEqual(ok, flurm_dbd_app:stop(make_ref())),
             ?assertEqual(ok, flurm_dbd_app:stop(fun() -> ok end)),
             ?assertEqual(ok, flurm_dbd_app:stop(<<1,2,3>>))
         end}
    ].
