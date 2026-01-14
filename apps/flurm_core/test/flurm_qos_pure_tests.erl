%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_qos module
%%%
%%% These tests directly test all exported functions and gen_server
%%% callbacks WITHOUT using meck or any mocking framework.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_qos_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

-define(QOS_TABLE, flurm_qos).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup function for tests that need the ETS table
setup_ets() ->
    %% Clean up any existing table
    catch ets:delete(?QOS_TABLE),
    %% Create fresh ETS table matching what init/1 creates
    ets:new(?QOS_TABLE, [
        named_table, public, set,
        {keypos, #qos.name}
    ]),
    ok.

cleanup_ets(_) ->
    catch ets:delete(?QOS_TABLE),
    ok.

%% Helper to create a state tuple (mirrors the internal #state{} record)
%% The state record is: -record(state, {default_qos :: binary()}).
make_state(DefaultQOS) ->
    {state, DefaultQOS}.

get_default_qos({state, DefaultQOS}) ->
    DefaultQOS.

%%====================================================================
%% gen_server Callback Tests: init/1
%%====================================================================

init_test_() ->
    {foreach,
     fun() -> catch ets:delete(?QOS_TABLE) end,
     fun(_) -> catch ets:delete(?QOS_TABLE) end,
     [
      {"init/1 creates ETS table and default QOS entries",
       fun() ->
           {ok, State} = flurm_qos:init([]),

           %% Verify ETS table was created
           ?assert(ets:info(?QOS_TABLE) =/= undefined),

           %% Verify table properties
           ?assertEqual(set, ets:info(?QOS_TABLE, type)),
           ?assertEqual(public, ets:info(?QOS_TABLE, protection)),
           ?assertEqual(#qos.name, ets:info(?QOS_TABLE, keypos)),

           %% Verify state has default QOS set (extract from tuple)
           ?assertEqual(<<"normal">>, get_default_qos(State))
       end},

      {"init/1 creates all standard QOS entries",
       fun() ->
           {ok, _State} = flurm_qos:init([]),

           %% Verify all 5 default QOS entries exist
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"normal">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"high">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"low">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"interactive">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"standby">>))
       end},

      {"init/1 sets correct properties for normal QOS",
       fun() ->
           {ok, _State} = flurm_qos:init([]),
           [NormalQOS] = ets:lookup(?QOS_TABLE, <<"normal">>),

           ?assertEqual(<<"normal">>, NormalQOS#qos.name),
           ?assertEqual(0, NormalQOS#qos.priority),
           ?assertEqual(86400, NormalQOS#qos.max_wall_per_job)
       end},

      {"init/1 sets correct properties for high QOS",
       fun() ->
           {ok, _State} = flurm_qos:init([]),
           [HighQOS] = ets:lookup(?QOS_TABLE, <<"high">>),

           ?assertEqual(<<"high">>, HighQOS#qos.name),
           ?assertEqual(1000, HighQOS#qos.priority),
           ?assertEqual([<<"low">>, <<"standby">>], HighQOS#qos.preempt),
           ?assertEqual(requeue, HighQOS#qos.preempt_mode),
           ?assertEqual(172800, HighQOS#qos.max_wall_per_job),
           ?assertEqual(50, HighQOS#qos.max_jobs_pu)
       end},

      {"init/1 sets correct properties for low QOS",
       fun() ->
           {ok, _State} = flurm_qos:init([]),
           [LowQOS] = ets:lookup(?QOS_TABLE, <<"low">>),

           ?assertEqual(<<"low">>, LowQOS#qos.name),
           ?assertEqual(-500, LowQOS#qos.priority),
           ?assertEqual(0.5, LowQOS#qos.usage_factor),
           ?assertEqual(604800, LowQOS#qos.max_wall_per_job)
       end},

      {"init/1 sets correct properties for interactive QOS",
       fun() ->
           {ok, _State} = flurm_qos:init([]),
           [InteractiveQOS] = ets:lookup(?QOS_TABLE, <<"interactive">>),

           ?assertEqual(<<"interactive">>, InteractiveQOS#qos.name),
           ?assertEqual(500, InteractiveQOS#qos.priority),
           ?assertEqual(3600, InteractiveQOS#qos.max_wall_per_job),
           ?assertEqual(5, InteractiveQOS#qos.max_jobs_pu),
           ?assertEqual(30, InteractiveQOS#qos.grace_time)
       end},

      {"init/1 sets correct properties for standby QOS",
       fun() ->
           {ok, _State} = flurm_qos:init([]),
           [StandbyQOS] = ets:lookup(?QOS_TABLE, <<"standby">>),

           ?assertEqual(<<"standby">>, StandbyQOS#qos.name),
           ?assertEqual(-1000, StandbyQOS#qos.priority),
           ?assertEqual(0.0, StandbyQOS#qos.usage_factor),
           ?assertEqual(cancel, StandbyQOS#qos.preempt_mode)
       end}
     ]}.

%%====================================================================
%% gen_server Callback Tests: handle_call/3
%%====================================================================

handle_call_create_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call {create, Name, Options} creates new QOS",
       fun() ->
           State = make_state(<<"normal">>),
           Options = #{
               description => <<"Test QOS">>,
               priority => 100,
               max_wall_per_job => 7200
           },

           {reply, ok, NewState} = flurm_qos:handle_call({create, <<"test">>, Options}, {self(), make_ref()}, State),

           %% Verify state unchanged
           ?assertEqual(State, NewState),

           %% Verify QOS was created
           [QOS] = ets:lookup(?QOS_TABLE, <<"test">>),
           ?assertEqual(<<"test">>, QOS#qos.name),
           ?assertEqual(<<"Test QOS">>, QOS#qos.description),
           ?assertEqual(100, QOS#qos.priority),
           ?assertEqual(7200, QOS#qos.max_wall_per_job)
       end},

      {"handle_call {create, Name, Options} with empty options uses defaults",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, ok, _} = flurm_qos:handle_call({create, <<"default_test">>, #{}}, {self(), make_ref()}, State),

           [QOS] = ets:lookup(?QOS_TABLE, <<"default_test">>),
           ?assertEqual(<<"default_test">>, QOS#qos.name),
           ?assertEqual(<<>>, QOS#qos.description),
           ?assertEqual(0, QOS#qos.priority),
           ?assertEqual([], QOS#qos.flags),
           ?assertEqual(0, QOS#qos.grace_time),
           ?assertEqual(0, QOS#qos.max_jobs_pa),
           ?assertEqual(0, QOS#qos.max_jobs_pu),
           ?assertEqual(#{}, QOS#qos.max_tres_pa),
           ?assertEqual(#{}, QOS#qos.max_tres_pu),
           ?assertEqual([], QOS#qos.preempt),
           ?assertEqual(off, QOS#qos.preempt_mode),
           ?assertEqual(1.0, QOS#qos.usage_factor),
           ?assertEqual(0.0, QOS#qos.usage_threshold)
       end},

      {"handle_call {create, Name, Options} fails for duplicate name",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create first QOS
           {reply, ok, _} = flurm_qos:handle_call({create, <<"dup">>, #{}}, {self(), make_ref()}, State),

           %% Try to create duplicate
           {reply, Result, _} = flurm_qos:handle_call({create, <<"dup">>, #{}}, {self(), make_ref()}, State),
           ?assertEqual({error, already_exists}, Result)
       end},

      {"handle_call {create, Name, Options} with all options",
       fun() ->
           State = make_state(<<"normal">>),
           Options = #{
               description => <<"Full options test">>,
               priority => 500,
               flags => [deny_limit, require_reservation],
               grace_time => 60,
               max_jobs_pa => 100,
               max_jobs_pu => 10,
               max_submit_jobs_pa => 200,
               max_submit_jobs_pu => 20,
               max_tres_pa => #{<<"cpu">> => 1000},
               max_tres_pu => #{<<"cpu">> => 100},
               max_tres_per_job => #{<<"cpu">> => 32},
               max_tres_per_node => #{<<"mem">> => 64000},
               max_tres_per_user => #{<<"gpu">> => 4},
               max_wall_per_job => 86400,
               min_tres_per_job => #{<<"cpu">> => 1},
               preempt => [<<"low">>, <<"standby">>],
               preempt_mode => requeue,
               usage_factor => 2.0,
               usage_threshold => 0.5
           },

           {reply, ok, _} = flurm_qos:handle_call({create, <<"full">>, Options}, {self(), make_ref()}, State),

           [QOS] = ets:lookup(?QOS_TABLE, <<"full">>),
           ?assertEqual(<<"Full options test">>, QOS#qos.description),
           ?assertEqual(500, QOS#qos.priority),
           ?assertEqual([deny_limit, require_reservation], QOS#qos.flags),
           ?assertEqual(60, QOS#qos.grace_time),
           ?assertEqual(100, QOS#qos.max_jobs_pa),
           ?assertEqual(10, QOS#qos.max_jobs_pu),
           ?assertEqual(200, QOS#qos.max_submit_jobs_pa),
           ?assertEqual(20, QOS#qos.max_submit_jobs_pu),
           ?assertEqual(#{<<"cpu">> => 1000}, QOS#qos.max_tres_pa),
           ?assertEqual(#{<<"cpu">> => 100}, QOS#qos.max_tres_pu),
           ?assertEqual(#{<<"cpu">> => 32}, QOS#qos.max_tres_per_job),
           ?assertEqual(#{<<"mem">> => 64000}, QOS#qos.max_tres_per_node),
           ?assertEqual(#{<<"gpu">> => 4}, QOS#qos.max_tres_per_user),
           ?assertEqual(86400, QOS#qos.max_wall_per_job),
           ?assertEqual(#{<<"cpu">> => 1}, QOS#qos.min_tres_per_job),
           ?assertEqual([<<"low">>, <<"standby">>], QOS#qos.preempt),
           ?assertEqual(requeue, QOS#qos.preempt_mode),
           ?assertEqual(2.0, QOS#qos.usage_factor),
           ?assertEqual(0.5, QOS#qos.usage_threshold)
       end}
     ]}.

handle_call_update_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call {update, Name, Updates} updates existing QOS",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create initial QOS
           {reply, ok, _} = flurm_qos:handle_call({create, <<"update_test">>, #{priority => 100}}, {self(), make_ref()}, State),

           %% Update it
           Updates = #{priority => 200, description => <<"Updated">>},
           {reply, ok, _} = flurm_qos:handle_call({update, <<"update_test">>, Updates}, {self(), make_ref()}, State),

           [QOS] = ets:lookup(?QOS_TABLE, <<"update_test">>),
           ?assertEqual(200, QOS#qos.priority),
           ?assertEqual(<<"Updated">>, QOS#qos.description)
       end},

      {"handle_call {update, Name, Updates} fails for non-existent QOS",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, Result, _} = flurm_qos:handle_call({update, <<"nonexistent">>, #{priority => 100}}, {self(), make_ref()}, State),
           ?assertEqual({error, not_found}, Result)
       end},

      {"handle_call {update, Name, Updates} updates all fields",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create initial QOS with defaults
           {reply, ok, _} = flurm_qos:handle_call({create, <<"all_fields">>, #{}}, {self(), make_ref()}, State),

           %% Update all fields
           Updates = #{
               description => <<"New desc">>,
               priority => 999,
               flags => [new_flag],
               grace_time => 120,
               max_jobs_pa => 50,
               max_jobs_pu => 5,
               max_submit_jobs_pa => 100,
               max_submit_jobs_pu => 10,
               max_tres_pa => #{<<"new">> => 1},
               max_tres_pu => #{<<"new">> => 2},
               max_tres_per_job => #{<<"new">> => 3},
               max_tres_per_node => #{<<"new">> => 4},
               max_tres_per_user => #{<<"new">> => 5},
               max_wall_per_job => 3600,
               min_tres_per_job => #{<<"new">> => 1},
               preempt => [<<"some_qos">>],
               preempt_mode => suspend,
               usage_factor => 0.5,
               usage_threshold => 0.75
           },

           {reply, ok, _} = flurm_qos:handle_call({update, <<"all_fields">>, Updates}, {self(), make_ref()}, State),

           [QOS] = ets:lookup(?QOS_TABLE, <<"all_fields">>),
           ?assertEqual(<<"New desc">>, QOS#qos.description),
           ?assertEqual(999, QOS#qos.priority),
           ?assertEqual([new_flag], QOS#qos.flags),
           ?assertEqual(120, QOS#qos.grace_time),
           ?assertEqual(50, QOS#qos.max_jobs_pa),
           ?assertEqual(5, QOS#qos.max_jobs_pu),
           ?assertEqual(100, QOS#qos.max_submit_jobs_pa),
           ?assertEqual(10, QOS#qos.max_submit_jobs_pu),
           ?assertEqual(#{<<"new">> => 1}, QOS#qos.max_tres_pa),
           ?assertEqual(#{<<"new">> => 2}, QOS#qos.max_tres_pu),
           ?assertEqual(#{<<"new">> => 3}, QOS#qos.max_tres_per_job),
           ?assertEqual(#{<<"new">> => 4}, QOS#qos.max_tres_per_node),
           ?assertEqual(#{<<"new">> => 5}, QOS#qos.max_tres_per_user),
           ?assertEqual(3600, QOS#qos.max_wall_per_job),
           ?assertEqual(#{<<"new">> => 1}, QOS#qos.min_tres_per_job),
           ?assertEqual([<<"some_qos">>], QOS#qos.preempt),
           ?assertEqual(suspend, QOS#qos.preempt_mode),
           ?assertEqual(0.5, QOS#qos.usage_factor),
           ?assertEqual(0.75, QOS#qos.usage_threshold)
       end},

      {"handle_call {update, Name, Updates} ignores unknown keys",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, ok, _} = flurm_qos:handle_call({create, <<"ignore_unknown">>, #{priority => 100}}, {self(), make_ref()}, State),

           %% Update with unknown key
           Updates = #{priority => 200, unknown_key => <<"ignored">>},
           {reply, ok, _} = flurm_qos:handle_call({update, <<"ignore_unknown">>, Updates}, {self(), make_ref()}, State),

           [QOS] = ets:lookup(?QOS_TABLE, <<"ignore_unknown">>),
           ?assertEqual(200, QOS#qos.priority)
       end}
     ]}.

handle_call_delete_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call {delete, Name} deletes QOS",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create QOS
           {reply, ok, _} = flurm_qos:handle_call({create, <<"to_delete">>, #{}}, {self(), make_ref()}, State),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"to_delete">>)),

           %% Delete it
           {reply, ok, _} = flurm_qos:handle_call({delete, <<"to_delete">>}, {self(), make_ref()}, State),
           ?assertEqual([], ets:lookup(?QOS_TABLE, <<"to_delete">>))
       end},

      {"handle_call {delete, normal} fails - cannot delete default",
       fun() ->
           State = make_state(<<"normal">>),

           %% Try to delete the "normal" QOS
           {reply, Result, _} = flurm_qos:handle_call({delete, <<"normal">>}, {self(), make_ref()}, State),
           ?assertEqual({error, cannot_delete_default}, Result)
       end},

      {"handle_call {delete, Name} on non-existent QOS succeeds silently",
       fun() ->
           State = make_state(<<"normal">>),

           %% Deleting non-existent should still return ok (ets:delete behavior)
           {reply, ok, _} = flurm_qos:handle_call({delete, <<"nonexistent">>}, {self(), make_ref()}, State)
       end}
     ]}.

handle_call_default_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call get_default returns current default",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, Result, _} = flurm_qos:handle_call(get_default, {self(), make_ref()}, State),
           ?assertEqual(<<"normal">>, Result)
       end},

      {"handle_call {set_default, Name} sets new default",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create a QOS to set as default
           {reply, ok, _} = flurm_qos:handle_call({create, <<"new_default">>, #{}}, {self(), make_ref()}, State),

           %% Set it as default
           {reply, ok, NewState} = flurm_qos:handle_call({set_default, <<"new_default">>}, {self(), make_ref()}, State),
           ?assertEqual(<<"new_default">>, get_default_qos(NewState))
       end},

      {"handle_call {set_default, Name} fails for non-existent QOS",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, Result, _} = flurm_qos:handle_call({set_default, <<"nonexistent">>}, {self(), make_ref()}, State),
           ?assertEqual({error, qos_not_found}, Result)
       end}
     ]}.

handle_call_init_defaults_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call init_defaults creates default QOS entries",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, ok, _} = flurm_qos:handle_call(init_defaults, {self(), make_ref()}, State),

           %% Verify all default QOS entries exist
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"normal">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"high">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"low">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"interactive">>)),
           ?assertMatch([_], ets:lookup(?QOS_TABLE, <<"standby">>))
       end}
     ]}.

handle_call_unknown_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_call unknown request returns error",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, Result, _} = flurm_qos:handle_call(unknown_request, {self(), make_ref()}, State),
           ?assertEqual({error, unknown_request}, Result)
       end},

      {"handle_call with arbitrary term returns error",
       fun() ->
           State = make_state(<<"normal">>),

           {reply, Result, _} = flurm_qos:handle_call({some, weird, tuple}, {self(), make_ref()}, State),
           ?assertEqual({error, unknown_request}, Result)
       end}
     ]}.

%%====================================================================
%% gen_server Callback Tests: handle_cast/2
%%====================================================================

handle_cast_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_cast returns noreply for any message",
       fun() ->
           State = make_state(<<"normal">>),

           {noreply, NewState} = flurm_qos:handle_cast(any_message, State),
           ?assertEqual(State, NewState)
       end},

      {"handle_cast with complex message returns noreply",
       fun() ->
           State = make_state(<<"normal">>),

           {noreply, NewState} = flurm_qos:handle_cast({complex, message, [1,2,3]}, State),
           ?assertEqual(State, NewState)
       end}
     ]}.

%%====================================================================
%% gen_server Callback Tests: handle_info/2
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"handle_info returns noreply for any info",
       fun() ->
           State = make_state(<<"normal">>),

           {noreply, NewState} = flurm_qos:handle_info(any_info, State),
           ?assertEqual(State, NewState)
       end},

      {"handle_info with timeout returns noreply",
       fun() ->
           State = make_state(<<"normal">>),

           {noreply, NewState} = flurm_qos:handle_info(timeout, State),
           ?assertEqual(State, NewState)
       end}
     ]}.

%%====================================================================
%% gen_server Callback Tests: terminate/2
%%====================================================================

terminate_test_() ->
    [
     {"terminate returns ok for normal shutdown",
      fun() ->
          State = make_state(<<"normal">>),
          ?assertEqual(ok, flurm_qos:terminate(normal, State))
      end},

     {"terminate returns ok for shutdown",
      fun() ->
          State = make_state(<<"normal">>),
          ?assertEqual(ok, flurm_qos:terminate(shutdown, State))
      end},

     {"terminate returns ok for error reason",
      fun() ->
          State = make_state(<<"normal">>),
          ?assertEqual(ok, flurm_qos:terminate({error, some_error}, State))
      end}
    ].

%%====================================================================
%% gen_server Callback Tests: code_change/3
%%====================================================================

code_change_test_() ->
    [
     {"code_change returns ok with unchanged state",
      fun() ->
          State = make_state(<<"normal">>),
          ?assertEqual({ok, State}, flurm_qos:code_change("1.0.0", State, []))
      end},

     {"code_change with extra data returns ok",
      fun() ->
          State = make_state(<<"high">>),
          ?assertEqual({ok, State}, flurm_qos:code_change("2.0.0", State, {some, extra, data}))
      end}
    ].

%%====================================================================
%% API Function Tests: get/1
%%====================================================================

get_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"get/1 returns QOS when found",
       fun() ->
           QOS = #qos{name = <<"test_qos">>, priority = 100},
           ets:insert(?QOS_TABLE, QOS),

           {ok, Result} = flurm_qos:get(<<"test_qos">>),
           ?assertEqual(QOS, Result)
       end},

      {"get/1 returns error when not found",
       fun() ->
           Result = flurm_qos:get(<<"nonexistent">>),
           ?assertEqual({error, not_found}, Result)
       end},

      {"get/1 returns full QOS record",
       fun() ->
           QOS = #qos{
               name = <<"full_qos">>,
               description = <<"Test">>,
               priority = 500,
               flags = [flag1],
               grace_time = 30,
               max_jobs_pa = 100,
               max_jobs_pu = 10,
               preempt = [<<"low">>],
               preempt_mode = requeue,
               usage_factor = 1.5
           },
           ets:insert(?QOS_TABLE, QOS),

           {ok, Result} = flurm_qos:get(<<"full_qos">>),
           ?assertEqual(<<"full_qos">>, Result#qos.name),
           ?assertEqual(<<"Test">>, Result#qos.description),
           ?assertEqual(500, Result#qos.priority),
           ?assertEqual([flag1], Result#qos.flags),
           ?assertEqual(30, Result#qos.grace_time),
           ?assertEqual(100, Result#qos.max_jobs_pa),
           ?assertEqual(10, Result#qos.max_jobs_pu),
           ?assertEqual([<<"low">>], Result#qos.preempt),
           ?assertEqual(requeue, Result#qos.preempt_mode),
           ?assertEqual(1.5, Result#qos.usage_factor)
       end}
     ]}.

%%====================================================================
%% API Function Tests: list/0
%%====================================================================

list_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"list/0 returns empty list when no QOS",
       fun() ->
           Result = flurm_qos:list(),
           ?assertEqual([], Result)
       end},

      {"list/0 returns all QOS entries",
       fun() ->
           QOS1 = #qos{name = <<"qos1">>, priority = 100},
           QOS2 = #qos{name = <<"qos2">>, priority = 200},
           QOS3 = #qos{name = <<"qos3">>, priority = 300},

           ets:insert(?QOS_TABLE, QOS1),
           ets:insert(?QOS_TABLE, QOS2),
           ets:insert(?QOS_TABLE, QOS3),

           Result = flurm_qos:list(),
           ?assertEqual(3, length(Result)),

           Names = [Q#qos.name || Q <- Result],
           ?assert(lists:member(<<"qos1">>, Names)),
           ?assert(lists:member(<<"qos2">>, Names)),
           ?assert(lists:member(<<"qos3">>, Names))
       end},

      {"list/0 returns single QOS",
       fun() ->
           QOS = #qos{name = <<"single">>},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:list(),
           ?assertEqual([QOS], Result)
       end}
     ]}.

%%====================================================================
%% API Function Tests: check_limits/2
%%====================================================================

check_limits_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"check_limits/2 returns error for invalid QOS",
       fun() ->
           Result = flurm_qos:check_limits(<<"nonexistent">>, #{}),
           ?assertEqual({error, {invalid_qos, <<"nonexistent">>}}, Result)
       end},

      {"check_limits/2 returns ok when within wall time limit",
       fun() ->
           QOS = #qos{name = <<"test">>, max_wall_per_job = 3600},
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{time_limit => 1800, user => <<"testuser">>},
           Result = flurm_qos:check_limits(<<"test">>, JobSpec),
           ?assertEqual(ok, Result)
       end},

      {"check_limits/2 returns error when exceeds wall time limit",
       fun() ->
           QOS = #qos{name = <<"test">>, max_wall_per_job = 3600},
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{time_limit => 7200, user => <<"testuser">>},
           Result = flurm_qos:check_limits(<<"test">>, JobSpec),
           ?assertEqual({error, {exceeds_wall_limit, 7200, 3600}}, Result)
       end},

      {"check_limits/2 returns ok when max_wall_per_job is 0 (unlimited)",
       fun() ->
           QOS = #qos{name = <<"unlimited">>, max_wall_per_job = 0},
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{time_limit => 999999, user => <<"testuser">>},
           Result = flurm_qos:check_limits(<<"unlimited">>, JobSpec),
           ?assertEqual(ok, Result)
       end},

      {"check_limits/2 returns ok when time_limit equals max",
       fun() ->
           QOS = #qos{name = <<"exact">>, max_wall_per_job = 3600},
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{time_limit => 3600, user => <<"testuser">>},
           Result = flurm_qos:check_limits(<<"exact">>, JobSpec),
           ?assertEqual(ok, Result)
       end},

      {"check_limits/2 handles missing time_limit in job spec",
       fun() ->
           QOS = #qos{name = <<"missing">>, max_wall_per_job = 3600},
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{user => <<"testuser">>},  % No time_limit
           Result = flurm_qos:check_limits(<<"missing">>, JobSpec),
           ?assertEqual(ok, Result)  % Defaults to 0, which is under limit
       end}
     ]}.

%%====================================================================
%% API Function Tests: check_tres_limits/3
%%====================================================================

check_tres_limits_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"check_tres_limits/3 returns error for invalid QOS",
       fun() ->
           Result = flurm_qos:check_tres_limits(<<"nonexistent">>, <<"user">>, #{}),
           ?assertEqual({error, {invalid_qos, <<"nonexistent">>}}, Result)
       end},

      {"check_tres_limits/3 returns ok with no limits set",
       fun() ->
           QOS = #qos{name = <<"nolimit">>, max_tres_pu = #{}, max_tres_per_job = #{}},
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 32, <<"mem">> => 64000},
           Result = flurm_qos:check_tres_limits(<<"nolimit">>, <<"user">>, TRESRequest),
           ?assertEqual(ok, Result)
       end},

      {"check_tres_limits/3 returns ok when within per-job limits",
       fun() ->
           QOS = #qos{
               name = <<"perjob">>,
               max_tres_per_job = #{<<"cpu">> => 64, <<"mem">> => 128000},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 32, <<"mem">> => 64000},
           Result = flurm_qos:check_tres_limits(<<"perjob">>, <<"user">>, TRESRequest),
           ?assertEqual(ok, Result)
       end},

      {"check_tres_limits/3 returns error when exceeds per-job limit",
       fun() ->
           QOS = #qos{
               name = <<"exceed">>,
               max_tres_per_job = #{<<"cpu">> => 16},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 32},
           Result = flurm_qos:check_tres_limits(<<"exceed">>, <<"user">>, TRESRequest),
           ?assertMatch({error, {tres_limit_exceeded, _}}, Result)
       end},

      {"check_tres_limits/3 reports correct violation details",
       fun() ->
           QOS = #qos{
               name = <<"details">>,
               max_tres_per_job = #{<<"cpu">> => 10, <<"gpu">> => 2},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 20, <<"gpu">> => 4},
           {error, {tres_limit_exceeded, Violations}} = flurm_qos:check_tres_limits(<<"details">>, <<"user">>, TRESRequest),

           %% Should have violations for both cpu and gpu
           ?assertEqual(2, length(Violations)),
           CpuViolation = lists:keyfind(<<"cpu">>, 1, Violations),
           GpuViolation = lists:keyfind(<<"gpu">>, 1, Violations),
           ?assertEqual({<<"cpu">>, 20, 10}, CpuViolation),
           ?assertEqual({<<"gpu">>, 4, 2}, GpuViolation)
       end}
     ]}.

%%====================================================================
%% API Function Tests: get_priority_adjustment/1
%%====================================================================

get_priority_adjustment_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"get_priority_adjustment/1 returns priority for existing QOS",
       fun() ->
           QOS = #qos{name = <<"high">>, priority = 1000},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_priority_adjustment(<<"high">>),
           ?assertEqual(1000, Result)
       end},

      {"get_priority_adjustment/1 returns 0 for non-existent QOS",
       fun() ->
           Result = flurm_qos:get_priority_adjustment(<<"nonexistent">>),
           ?assertEqual(0, Result)
       end},

      {"get_priority_adjustment/1 handles negative priority",
       fun() ->
           QOS = #qos{name = <<"low">>, priority = -500},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_priority_adjustment(<<"low">>),
           ?assertEqual(-500, Result)
       end},

      {"get_priority_adjustment/1 handles zero priority",
       fun() ->
           QOS = #qos{name = <<"normal">>, priority = 0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_priority_adjustment(<<"normal">>),
           ?assertEqual(0, Result)
       end}
     ]}.

%%====================================================================
%% API Function Tests: get_preemptable_qos/1
%%====================================================================

get_preemptable_qos_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"get_preemptable_qos/1 returns preempt list for existing QOS",
       fun() ->
           QOS = #qos{name = <<"high">>, preempt = [<<"low">>, <<"standby">>]},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_preemptable_qos(<<"high">>),
           ?assertEqual([<<"low">>, <<"standby">>], Result)
       end},

      {"get_preemptable_qos/1 returns empty list for non-existent QOS",
       fun() ->
           Result = flurm_qos:get_preemptable_qos(<<"nonexistent">>),
           ?assertEqual([], Result)
       end},

      {"get_preemptable_qos/1 returns empty list when QOS has no preempt",
       fun() ->
           QOS = #qos{name = <<"normal">>, preempt = []},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_preemptable_qos(<<"normal">>),
           ?assertEqual([], Result)
       end},

      {"get_preemptable_qos/1 handles single preemptable QOS",
       fun() ->
           QOS = #qos{name = <<"medium">>, preempt = [<<"standby">>]},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_preemptable_qos(<<"medium">>),
           ?assertEqual([<<"standby">>], Result)
       end}
     ]}.

%%====================================================================
%% API Function Tests: can_preempt/2
%%====================================================================

can_preempt_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"can_preempt/2 returns true when target is in preempt list",
       fun() ->
           QOS = #qos{name = <<"high">>, preempt = [<<"low">>, <<"standby">>]},
           ets:insert(?QOS_TABLE, QOS),

           ?assert(flurm_qos:can_preempt(<<"high">>, <<"low">>)),
           ?assert(flurm_qos:can_preempt(<<"high">>, <<"standby">>))
       end},

      {"can_preempt/2 returns false when target is not in preempt list",
       fun() ->
           QOS = #qos{name = <<"high">>, preempt = [<<"low">>]},
           ets:insert(?QOS_TABLE, QOS),

           ?assertNot(flurm_qos:can_preempt(<<"high">>, <<"normal">>)),
           ?assertNot(flurm_qos:can_preempt(<<"high">>, <<"high">>))
       end},

      {"can_preempt/2 returns false for non-existent preemptor",
       fun() ->
           ?assertNot(flurm_qos:can_preempt(<<"nonexistent">>, <<"low">>))
       end},

      {"can_preempt/2 returns false when preempt list is empty",
       fun() ->
           QOS = #qos{name = <<"normal">>, preempt = []},
           ets:insert(?QOS_TABLE, QOS),

           ?assertNot(flurm_qos:can_preempt(<<"normal">>, <<"low">>)),
           ?assertNot(flurm_qos:can_preempt(<<"normal">>, <<"standby">>))
       end}
     ]}.

%%====================================================================
%% API Function Tests: apply_usage_factor/2
%%====================================================================

apply_usage_factor_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"apply_usage_factor/2 multiplies usage by factor",
       fun() ->
           QOS = #qos{name = <<"double">>, usage_factor = 2.0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"double">>, 100.0),
           ?assertEqual(200.0, Result)
       end},

      {"apply_usage_factor/2 returns original usage for non-existent QOS",
       fun() ->
           Result = flurm_qos:apply_usage_factor(<<"nonexistent">>, 100.0),
           ?assertEqual(100.0, Result)
       end},

      {"apply_usage_factor/2 handles factor of 0.0",
       fun() ->
           QOS = #qos{name = <<"standby">>, usage_factor = 0.0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"standby">>, 100.0),
           ?assertEqual(0.0, Result)
       end},

      {"apply_usage_factor/2 handles factor of 0.5",
       fun() ->
           QOS = #qos{name = <<"low">>, usage_factor = 0.5},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"low">>, 100.0),
           ?assertEqual(50.0, Result)
       end},

      {"apply_usage_factor/2 handles factor of 1.0 (default)",
       fun() ->
           QOS = #qos{name = <<"normal">>, usage_factor = 1.0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"normal">>, 100.0),
           ?assertEqual(100.0, Result)
       end},

      {"apply_usage_factor/2 handles negative usage",
       fun() ->
           QOS = #qos{name = <<"test">>, usage_factor = 2.0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"test">>, -50.0),
           ?assertEqual(-100.0, Result)
       end},

      {"apply_usage_factor/2 handles zero usage",
       fun() ->
           QOS = #qos{name = <<"test">>, usage_factor = 2.0},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:apply_usage_factor(<<"test">>, 0.0),
           ?assertEqual(0.0, Result)
       end}
     ]}.

%%====================================================================
%% Edge Case and Integration Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"QOS with binary name edge cases",
       fun() ->
           %% Test with unicode binary
           QOS = #qos{name = <<"unicode_qos">>, priority = 100},
           ets:insert(?QOS_TABLE, QOS),

           {ok, Result} = flurm_qos:get(<<"unicode_qos">>),
           ?assertEqual(<<"unicode_qos">>, Result#qos.name)
       end},

      {"QOS with large priority value",
       fun() ->
           QOS = #qos{name = <<"large_priority">>, priority = 2147483647},  % Max int32
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_priority_adjustment(<<"large_priority">>),
           ?assertEqual(2147483647, Result)
       end},

      {"QOS with large negative priority value",
       fun() ->
           QOS = #qos{name = <<"neg_priority">>, priority = -2147483648},  % Min int32
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_priority_adjustment(<<"neg_priority">>),
           ?assertEqual(-2147483648, Result)
       end},

      {"QOS with many preemptable entries",
       fun() ->
           PreemptList = [list_to_binary("qos_" ++ integer_to_list(N)) || N <- lists:seq(1, 100)],
           QOS = #qos{name = <<"many_preempt">>, preempt = PreemptList},
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:get_preemptable_qos(<<"many_preempt">>),
           ?assertEqual(100, length(Result)),
           ?assert(lists:member(<<"qos_1">>, Result)),
           ?assert(lists:member(<<"qos_100">>, Result))
       end},

      {"check_limits with all default values",
       fun() ->
           %% QOS with all default (zero) limits
           QOS = #qos{
               name = <<"all_default">>,
               max_wall_per_job = 0,
               max_jobs_pu = 0,
               max_submit_jobs_pu = 0
           },
           ets:insert(?QOS_TABLE, QOS),

           JobSpec = #{time_limit => 999999, user => <<"user">>},
           Result = flurm_qos:check_limits(<<"all_default">>, JobSpec),
           ?assertEqual(ok, Result)
       end}
     ]}.

%%====================================================================
%% Multiple Operations Tests
%%====================================================================

multiple_ops_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"create, update, and get QOS",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create
           {reply, ok, _} = flurm_qos:handle_call({create, <<"workflow">>, #{priority => 100}}, {self(), make_ref()}, State),

           %% Verify initial value
           {ok, QOS1} = flurm_qos:get(<<"workflow">>),
           ?assertEqual(100, QOS1#qos.priority),

           %% Update
           {reply, ok, _} = flurm_qos:handle_call({update, <<"workflow">>, #{priority => 200}}, {self(), make_ref()}, State),

           %% Verify updated value
           {ok, QOS2} = flurm_qos:get(<<"workflow">>),
           ?assertEqual(200, QOS2#qos.priority)
       end},

      {"create multiple QOS and list",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create multiple QOS
           {reply, ok, _} = flurm_qos:handle_call({create, <<"batch">>, #{priority => 100}}, {self(), make_ref()}, State),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"debug">>, #{priority => 200}}, {self(), make_ref()}, State),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"urgent">>, #{priority => 300}}, {self(), make_ref()}, State),

           %% List all
           AllQOS = flurm_qos:list(),
           ?assertEqual(3, length(AllQOS)),

           %% Verify each exists
           Names = [Q#qos.name || Q <- AllQOS],
           ?assert(lists:member(<<"batch">>, Names)),
           ?assert(lists:member(<<"debug">>, Names)),
           ?assert(lists:member(<<"urgent">>, Names))
       end},

      {"create, delete, and verify removal",
       fun() ->
           State = make_state(<<"normal">>),

           %% Create
           {reply, ok, _} = flurm_qos:handle_call({create, <<"temp">>, #{}}, {self(), make_ref()}, State),

           %% Verify exists
           ?assertMatch({ok, _}, flurm_qos:get(<<"temp">>)),

           %% Delete
           {reply, ok, _} = flurm_qos:handle_call({delete, <<"temp">>}, {self(), make_ref()}, State),

           %% Verify removed
           ?assertEqual({error, not_found}, flurm_qos:get(<<"temp">>))
       end}
     ]}.

%%====================================================================
%% Preemption Mode Tests
%%====================================================================

preemption_mode_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"QOS with preempt_mode off",
       fun() ->
           State = make_state(<<"normal">>),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"mode_off">>, #{preempt_mode => off}}, {self(), make_ref()}, State),

           {ok, QOS} = flurm_qos:get(<<"mode_off">>),
           ?assertEqual(off, QOS#qos.preempt_mode)
       end},

      {"QOS with preempt_mode cancel",
       fun() ->
           State = make_state(<<"normal">>),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"mode_cancel">>, #{preempt_mode => cancel}}, {self(), make_ref()}, State),

           {ok, QOS} = flurm_qos:get(<<"mode_cancel">>),
           ?assertEqual(cancel, QOS#qos.preempt_mode)
       end},

      {"QOS with preempt_mode requeue",
       fun() ->
           State = make_state(<<"normal">>),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"mode_requeue">>, #{preempt_mode => requeue}}, {self(), make_ref()}, State),

           {ok, QOS} = flurm_qos:get(<<"mode_requeue">>),
           ?assertEqual(requeue, QOS#qos.preempt_mode)
       end},

      {"QOS with preempt_mode suspend",
       fun() ->
           State = make_state(<<"normal">>),
           {reply, ok, _} = flurm_qos:handle_call({create, <<"mode_suspend">>, #{preempt_mode => suspend}}, {self(), make_ref()}, State),

           {ok, QOS} = flurm_qos:get(<<"mode_suspend">>),
           ?assertEqual(suspend, QOS#qos.preempt_mode)
       end}
     ]}.

%%====================================================================
%% TRES Limits Internal Functions Tests
%%====================================================================

tres_limits_edge_cases_test_() ->
    {foreach,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"check_tres_limits with empty request",
       fun() ->
           QOS = #qos{
               name = <<"empty_req">>,
               max_tres_per_job = #{<<"cpu">> => 32},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           Result = flurm_qos:check_tres_limits(<<"empty_req">>, <<"user">>, #{}),
           ?assertEqual(ok, Result)
       end},

      {"check_tres_limits with request type not in limits",
       fun() ->
           QOS = #qos{
               name = <<"diff_types">>,
               max_tres_per_job = #{<<"cpu">> => 32},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           %% Request GPU, but only CPU is limited
           TRESRequest = #{<<"gpu">> => 100},
           Result = flurm_qos:check_tres_limits(<<"diff_types">>, <<"user">>, TRESRequest),
           ?assertEqual(ok, Result)
       end},

      {"check_tres_limits exactly at limit",
       fun() ->
           QOS = #qos{
               name = <<"exact_limit">>,
               max_tres_per_job = #{<<"cpu">> => 32},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 32},
           Result = flurm_qos:check_tres_limits(<<"exact_limit">>, <<"user">>, TRESRequest),
           ?assertEqual(ok, Result)
       end},

      {"check_tres_limits one over limit",
       fun() ->
           QOS = #qos{
               name = <<"one_over">>,
               max_tres_per_job = #{<<"cpu">> => 32},
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 33},
           Result = flurm_qos:check_tres_limits(<<"one_over">>, <<"user">>, TRESRequest),
           ?assertMatch({error, {tres_limit_exceeded, _}}, Result)
       end},

      {"check_tres_limits with limit of 0 means unlimited",
       fun() ->
           QOS = #qos{
               name = <<"zero_limit">>,
               max_tres_per_job = #{<<"cpu">> => 0},  % 0 means no limit
               max_tres_pu = #{}
           },
           ets:insert(?QOS_TABLE, QOS),

           TRESRequest = #{<<"cpu">> => 1000000},
           Result = flurm_qos:check_tres_limits(<<"zero_limit">>, <<"user">>, TRESRequest),
           ?assertEqual(ok, Result)
       end}
     ]}.
