%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_job_deps
%%%
%%% Tests all exported functions without any mocking (no meck).
%%% Tests gen_server callbacks directly (init/1, handle_call/3, handle_cast/2).
%%%-------------------------------------------------------------------
-module(flurm_job_deps_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Test record matching the module's internal record
-record(dependency, {
    id :: {integer(), atom(), integer() | binary()},
    job_id :: integer(),
    dep_type :: atom(),
    target :: integer() | binary(),
    satisfied :: boolean(),
    satisfied_time :: non_neg_integer() | undefined
}).

-record(state, {}).

%% ETS table names matching the module
-define(DEPS_TABLE, flurm_job_deps).
-define(DEPENDENTS_TABLE, flurm_job_dependents).
-define(SINGLETON_TABLE, flurm_singletons).
-define(GRAPH_TABLE, flurm_dep_graph).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Clean up any existing tables
    cleanup_tables(),
    ok.

cleanup() ->
    cleanup_tables(),
    ok.

cleanup_tables() ->
    catch ets:delete(?DEPS_TABLE),
    catch ets:delete(?DEPENDENTS_TABLE),
    catch ets:delete(?SINGLETON_TABLE),
    catch ets:delete(?GRAPH_TABLE),
    ok.

%% Helper to initialize ETS tables (mimics init/1)
init_tables() ->
    ets:new(?DEPS_TABLE, [
        named_table, public, set,
        {keypos, #dependency.id}
    ]),
    ets:new(?DEPENDENTS_TABLE, [
        named_table, public, set
    ]),
    ets:new(?SINGLETON_TABLE, [
        named_table, public, set
    ]),
    ets:new(?GRAPH_TABLE, [
        named_table, public, set
    ]),
    ok.

%%====================================================================
%% Test: init/1
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"init creates required ETS tables",
       fun() ->
           Result = flurm_job_deps:init([]),
           ?assertEqual({ok, #state{}}, Result),

           %% Verify all tables were created
           ?assert(ets:info(?DEPS_TABLE) =/= undefined),
           ?assert(ets:info(?DEPENDENTS_TABLE) =/= undefined),
           ?assert(ets:info(?SINGLETON_TABLE) =/= undefined),
           ?assert(ets:info(?GRAPH_TABLE) =/= undefined),

           %% Verify table types
           ?assertEqual(set, ets:info(?DEPS_TABLE, type)),
           ?assertEqual(set, ets:info(?DEPENDENTS_TABLE, type)),
           ?assertEqual(set, ets:info(?SINGLETON_TABLE, type)),
           ?assertEqual(set, ets:info(?GRAPH_TABLE, type)),

           %% Verify deps table keypos
           ?assertEqual(#dependency.id, ets:info(?DEPS_TABLE, keypos)),

           cleanup_tables()
       end}
     ]}.

%%====================================================================
%% Test: parse_dependency_spec/1
%%====================================================================

parse_dependency_spec_test_() ->
    [
     {"parse empty spec returns empty list",
      fun() ->
          ?assertEqual({ok, []}, flurm_job_deps:parse_dependency_spec(<<>>))
      end},

     {"parse singleton returns default name",
      fun() ->
          ?assertEqual({ok, [{singleton, <<"default">>}]},
                       flurm_job_deps:parse_dependency_spec(<<"singleton">>))
      end},

     {"parse singleton with name",
      fun() ->
          ?assertEqual({ok, [{singleton, <<"myname">>}]},
                       flurm_job_deps:parse_dependency_spec(<<"singleton:myname">>))
      end},

     {"parse after dependency",
      fun() ->
          ?assertEqual({ok, [{after_start, 123}]},
                       flurm_job_deps:parse_dependency_spec(<<"after:123">>))
      end},

     {"parse afterok dependency",
      fun() ->
          ?assertEqual({ok, [{afterok, 456}]},
                       flurm_job_deps:parse_dependency_spec(<<"afterok:456">>))
      end},

     {"parse afternotok dependency",
      fun() ->
          ?assertEqual({ok, [{afternotok, 789}]},
                       flurm_job_deps:parse_dependency_spec(<<"afternotok:789">>))
      end},

     {"parse afterany dependency",
      fun() ->
          ?assertEqual({ok, [{afterany, 100}]},
                       flurm_job_deps:parse_dependency_spec(<<"afterany:100">>))
      end},

     {"parse aftercorr dependency",
      fun() ->
          ?assertEqual({ok, [{aftercorr, 200}]},
                       flurm_job_deps:parse_dependency_spec(<<"aftercorr:200">>))
      end},

     {"parse afterburstbuffer dependency",
      fun() ->
          ?assertEqual({ok, [{afterburstbuffer, 300}]},
                       flurm_job_deps:parse_dependency_spec(<<"afterburstbuffer:300">>))
      end},

     {"parse multiple dependencies",
      fun() ->
          Result = flurm_job_deps:parse_dependency_spec(<<"afterok:123,afterany:456,singleton">>),
          ?assertEqual({ok, [{afterok, 123}, {afterany, 456}, {singleton, <<"default">>}]}, Result)
      end},

     {"parse multiple targets with + syntax",
      fun() ->
          ?assertEqual({ok, [{afterok, [1, 2, 3]}]},
                       flurm_job_deps:parse_dependency_spec(<<"afterok:1+2+3">>))
      end},

     {"parse invalid dependency type returns error",
      fun() ->
          Result = flurm_job_deps:parse_dependency_spec(<<"invalid:123">>),
          ?assertMatch({error, {unknown_dep_type, <<"invalid">>}}, Result)
      end},

     {"parse invalid format returns error",
      fun() ->
          Result = flurm_job_deps:parse_dependency_spec(<<"noformat">>),
          ?assertMatch({error, {invalid_dependency, <<"noformat">>}}, Result)
      end}
    ].

%%====================================================================
%% Test: format_dependency_spec/1
%%====================================================================

format_dependency_spec_test_() ->
    [
     {"format empty list returns empty binary",
      fun() ->
          ?assertEqual(<<>>, flurm_job_deps:format_dependency_spec([]))
      end},

     {"format default singleton",
      fun() ->
          ?assertEqual(<<"singleton">>,
                       flurm_job_deps:format_dependency_spec([{singleton, <<"default">>}]))
      end},

     {"format named singleton",
      fun() ->
          ?assertEqual(<<"singleton:myname">>,
                       flurm_job_deps:format_dependency_spec([{singleton, <<"myname">>}]))
      end},

     {"format after_start dependency",
      fun() ->
          ?assertEqual(<<"after:123">>,
                       flurm_job_deps:format_dependency_spec([{after_start, 123}]))
      end},

     {"format afterok dependency",
      fun() ->
          ?assertEqual(<<"afterok:456">>,
                       flurm_job_deps:format_dependency_spec([{afterok, 456}]))
      end},

     {"format afternotok dependency",
      fun() ->
          ?assertEqual(<<"afternotok:789">>,
                       flurm_job_deps:format_dependency_spec([{afternotok, 789}]))
      end},

     {"format afterany dependency",
      fun() ->
          ?assertEqual(<<"afterany:100">>,
                       flurm_job_deps:format_dependency_spec([{afterany, 100}]))
      end},

     {"format aftercorr dependency",
      fun() ->
          ?assertEqual(<<"aftercorr:200">>,
                       flurm_job_deps:format_dependency_spec([{aftercorr, 200}]))
      end},

     {"format afterburstbuffer dependency",
      fun() ->
          ?assertEqual(<<"afterburstbuffer:300">>,
                       flurm_job_deps:format_dependency_spec([{afterburstbuffer, 300}]))
      end},

     {"format multiple dependencies",
      fun() ->
          Result = flurm_job_deps:format_dependency_spec([
              {afterok, 123},
              {afterany, 456},
              {singleton, <<"default">>}
          ]),
          ?assertEqual(<<"afterok:123,afterany:456,singleton">>, Result)
      end},

     {"format multiple targets with + syntax",
      fun() ->
          ?assertEqual(<<"afterok:1+2+3">>,
                       flurm_job_deps:format_dependency_spec([{afterok, [1, 2, 3]}]))
      end}
    ].

%%====================================================================
%% Test: Round-trip parse/format
%%====================================================================

round_trip_test_() ->
    [
     {"round-trip singleton",
      fun() ->
          Original = <<"singleton">>,
          {ok, Parsed} = flurm_job_deps:parse_dependency_spec(Original),
          Formatted = flurm_job_deps:format_dependency_spec(Parsed),
          ?assertEqual(Original, Formatted)
      end},

     {"round-trip named singleton",
      fun() ->
          Original = <<"singleton:test">>,
          {ok, Parsed} = flurm_job_deps:parse_dependency_spec(Original),
          Formatted = flurm_job_deps:format_dependency_spec(Parsed),
          ?assertEqual(Original, Formatted)
      end},

     {"round-trip afterok",
      fun() ->
          Original = <<"afterok:123">>,
          {ok, Parsed} = flurm_job_deps:parse_dependency_spec(Original),
          Formatted = flurm_job_deps:format_dependency_spec(Parsed),
          ?assertEqual(Original, Formatted)
      end},

     {"round-trip complex spec",
      fun() ->
          Original = <<"afterok:123,afterany:456,singleton">>,
          {ok, Parsed} = flurm_job_deps:parse_dependency_spec(Original),
          Formatted = flurm_job_deps:format_dependency_spec(Parsed),
          ?assertEqual(Original, Formatted)
      end},

     {"round-trip multiple targets",
      fun() ->
          Original = <<"afterok:1+2+3">>,
          {ok, Parsed} = flurm_job_deps:parse_dependency_spec(Original),
          Formatted = flurm_job_deps:format_dependency_spec(Parsed),
          ?assertEqual(Original, Formatted)
      end}
    ].

%%====================================================================
%% Test: handle_call/3 - add_dependency
%%====================================================================

handle_call_add_dependency_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"add singleton dependency when no other singleton exists",
           fun() ->
               Request = {add_dependency, 1, singleton, <<"test_singleton">>},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               %% Verify dependency was added
               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(1, Dep#dependency.job_id),
               ?assertEqual(singleton, Dep#dependency.dep_type),
               ?assertEqual(<<"test_singleton">>, Dep#dependency.target),
               ?assertEqual(true, Dep#dependency.satisfied),

               %% Verify singleton table was updated
               ?assertEqual([{<<"test_singleton">>, 1}], ets:tab2list(?SINGLETON_TABLE))
           end}
      end,

      fun(State) ->
          {"add singleton dependency when another job holds it",
           fun() ->
               %% First job takes the singleton
               ets:insert(?SINGLETON_TABLE, {<<"shared">>, 1}),

               %% Second job tries to get same singleton
               Request = {add_dependency, 2, singleton, <<"shared">>},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               %% Verify dependency was added but not satisfied
               [Dep] = ets:lookup(?DEPS_TABLE, {2, singleton, <<"shared">>}),
               ?assertEqual(false, Dep#dependency.satisfied),

               %% Verify dependent was registered
               ?assertEqual([{1, [2]}], ets:tab2list(?DEPENDENTS_TABLE))
           end}
      end,

      fun(State) ->
          {"add afterok dependency",
           fun() ->
               Request = {add_dependency, 2, afterok, 1},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               %% Verify dependency was added (unsatisfied since target job doesn't exist)
               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(2, Dep#dependency.job_id),
               ?assertEqual(afterok, Dep#dependency.dep_type),
               ?assertEqual(1, Dep#dependency.target),
               ?assertEqual(false, Dep#dependency.satisfied),

               %% Verify graph was updated
               ?assertEqual([{2, [1]}], ets:tab2list(?GRAPH_TABLE)),

               %% Verify dependents list was updated
               ?assertEqual([{1, [2]}], ets:tab2list(?DEPENDENTS_TABLE))
           end}
      end,

      fun(State) ->
          {"add afterany dependency",
           fun() ->
               Request = {add_dependency, 3, afterany, 2},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(afterany, Dep#dependency.dep_type)
           end}
      end,

      fun(State) ->
          {"add afternotok dependency",
           fun() ->
               Request = {add_dependency, 4, afternotok, 3},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(afternotok, Dep#dependency.dep_type)
           end}
      end,

      fun(State) ->
          {"add after_start dependency",
           fun() ->
               Request = {add_dependency, 5, after_start, 4},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(after_start, Dep#dependency.dep_type)
           end}
      end,

      fun(State) ->
          {"add aftercorr dependency",
           fun() ->
               Request = {add_dependency, 6, aftercorr, 5},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(1, length(Deps)),
               [Dep] = Deps,
               ?assertEqual(aftercorr, Dep#dependency.dep_type)
           end}
      end,

      fun(State) ->
          {"add multiple targets as list",
           fun() ->
               Request = {add_dependency, 10, afterok, [1, 2, 3]},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               %% Should have 3 dependencies
               Deps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(3, length(Deps))
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_call/3 - add_dependencies (multiple)
%%====================================================================

handle_call_add_dependencies_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"add multiple dependencies at once",
           fun() ->
               Deps = [{afterok, 1}, {afterany, 2}, {singleton, <<"test">>}],
               Request = {add_dependencies, 10, Deps},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),

               %% Should have 3 dependencies
               AllDeps = ets:tab2list(?DEPS_TABLE),
               ?assertEqual(3, length(AllDeps))
           end}
      end,

      fun(State) ->
          {"add empty dependencies list",
           fun() ->
               Request = {add_dependencies, 10, []},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),
               ?assertEqual([], ets:tab2list(?DEPS_TABLE))
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_call/3 - remove_dependency
%%====================================================================

handle_call_remove_dependency_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"remove existing dependency",
           fun() ->
               %% First add a dependency
               AddReq = {add_dependency, 2, afterok, 1},
               flurm_job_deps:handle_call(AddReq, {self(), make_ref()}, State),

               ?assertEqual(1, length(ets:tab2list(?DEPS_TABLE))),

               %% Now remove it
               RemoveReq = {remove_dependency, 2, afterok, 1},
               Result = flurm_job_deps:handle_call(RemoveReq, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),
               ?assertEqual([], ets:tab2list(?DEPS_TABLE)),
               ?assertEqual([], ets:tab2list(?DEPENDENTS_TABLE)),
               ?assertEqual([], ets:tab2list(?GRAPH_TABLE))
           end}
      end,

      fun(State) ->
          {"remove non-existent dependency is ok",
           fun() ->
               Request = {remove_dependency, 999, afterok, 888},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result)
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_call/3 - remove_all_dependencies
%%====================================================================

handle_call_remove_all_dependencies_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"remove all dependencies for a job",
           fun() ->
               %% Add multiple dependencies for job 10
               flurm_job_deps:handle_call({add_dependency, 10, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 10, afterany, 2}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 10, singleton, <<"test">>}, {self(), make_ref()}, State),

               ?assertEqual(3, length(ets:tab2list(?DEPS_TABLE))),

               %% Remove all
               Request = {remove_all_dependencies, 10},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result),
               ?assertEqual([], ets:tab2list(?DEPS_TABLE))
           end}
      end,

      fun(State) ->
          {"remove all for job with no dependencies",
           fun() ->
               Request = {remove_all_dependencies, 999},
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, ok, #state{}}, Result)
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_call/3 - clear_completed_deps
%%====================================================================

handle_call_clear_completed_deps_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"clear completed deps returns count",
           fun() ->
               %% Add some dependencies (they'll be "orphaned" since no job exists)
               flurm_job_deps:handle_call({add_dependency, 1, afterok, 100}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 2, afterany, 200}, {self(), make_ref()}, State),

               Request = clear_completed_deps,
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               %% Since no jobs exist, all should be cleared
               ?assertMatch({reply, 2, #state{}}, Result),
               ?assertEqual([], ets:tab2list(?DEPS_TABLE))
           end}
      end,

      fun(State) ->
          {"clear with no deps returns 0",
           fun() ->
               Request = clear_completed_deps,
               Result = flurm_job_deps:handle_call(Request, {self(), make_ref()}, State),

               ?assertMatch({reply, 0, #state{}}, Result)
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_call/3 - unknown request
%%====================================================================

handle_call_unknown_test_() ->
    {setup,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     fun(State) ->
         [{"unknown request returns error",
           fun() ->
               Result = flurm_job_deps:handle_call({unknown_request, foo}, {self(), make_ref()}, State),
               ?assertEqual({reply, {error, unknown_request}, State}, Result)
           end}]
     end}.

%%====================================================================
%% Test: handle_cast/2 - job_state_change
%%====================================================================

handle_cast_job_state_change_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"job state change with no dependents",
           fun() ->
               Result = flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),
               ?assertEqual({noreply, State}, Result)
           end}
      end,

      fun(State) ->
          {"job state change updates dependent's dependency status",
           fun() ->
               %% Job 2 depends on job 1
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               %% Verify dependency is unsatisfied
               [Dep] = ets:lookup(?DEPS_TABLE, {2, afterok, 1}),
               ?assertEqual(false, Dep#dependency.satisfied),

               %% Signal job 1 completed
               Result = flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),
               ?assertEqual({noreply, State}, Result),

               %% Verify dependency is now satisfied
               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afterok, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied),
               ?assert(UpdatedDep#dependency.satisfied_time =/= undefined)
           end}
      end,

      fun(State) ->
          {"afternotok satisfied on failure",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afternotok, 1}, {self(), make_ref()}, State),

               [Dep] = ets:lookup(?DEPS_TABLE, {2, afternotok, 1}),
               ?assertEqual(false, Dep#dependency.satisfied),

               flurm_job_deps:handle_cast({job_state_change, 1, failed}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afternotok, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"afternotok satisfied on cancelled",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afternotok, 1}, {self(), make_ref()}, State),

               flurm_job_deps:handle_cast({job_state_change, 1, cancelled}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afternotok, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"afternotok satisfied on timeout",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afternotok, 1}, {self(), make_ref()}, State),

               flurm_job_deps:handle_cast({job_state_change, 1, timeout}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afternotok, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"afterany satisfied on completed",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterany, 1}, {self(), make_ref()}, State),

               flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afterany, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"afterany satisfied on failed",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterany, 1}, {self(), make_ref()}, State),

               flurm_job_deps:handle_cast({job_state_change, 1, failed}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afterany, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"aftercorr satisfied on completed",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, aftercorr, 1}, {self(), make_ref()}, State),

               flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, aftercorr, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"terminal state cleans up dependents table",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               ?assertEqual([{1, [2]}], ets:tab2list(?DEPENDENTS_TABLE)),

               flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),

               %% Dependents entry should be removed after terminal state
               ?assertEqual([], ets:lookup(?DEPENDENTS_TABLE, 1))
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_cast/2 - job_completed
%%====================================================================

handle_cast_job_completed_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"job_completed acts like state_change",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               Result = flurm_job_deps:handle_cast({job_completed, 1, completed}, State),
               ?assertEqual({noreply, State}, Result),

               [UpdatedDep] = ets:lookup(?DEPS_TABLE, {2, afterok, 1}),
               ?assertEqual(true, UpdatedDep#dependency.satisfied)
           end}
      end
     ]}.

%%====================================================================
%% Test: handle_cast/2 - unknown message
%%====================================================================

handle_cast_unknown_test_() ->
    {setup,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     fun(State) ->
         [{"unknown cast message is ignored",
           fun() ->
               Result = flurm_job_deps:handle_cast({unknown_message, foo}, State),
               ?assertEqual({noreply, State}, Result)
           end}]
     end}.

%%====================================================================
%% Test: handle_info/2
%%====================================================================

handle_info_test_() ->
    [{"handle_info ignores unknown messages",
      fun() ->
          State = #state{},
          Result = flurm_job_deps:handle_info(unknown_info, State),
          ?assertEqual({noreply, State}, Result)
      end}].

%%====================================================================
%% Test: terminate/2
%%====================================================================

terminate_test_() ->
    [{"terminate returns ok",
      fun() ->
          ?assertEqual(ok, flurm_job_deps:terminate(normal, #state{}))
      end},
     {"terminate with error reason returns ok",
      fun() ->
          ?assertEqual(ok, flurm_job_deps:terminate({error, test}, #state{}))
      end}].

%%====================================================================
%% Test: code_change/3
%%====================================================================

code_change_test_() ->
    [{"code_change returns state unchanged",
      fun() ->
          State = #state{},
          ?assertEqual({ok, State}, flurm_job_deps:code_change("1.0.0", State, []))
      end}].

%%====================================================================
%% Test: get_dependencies/1
%%====================================================================

get_dependencies_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"get_dependencies returns empty list for job with no deps",
           fun() ->
               ?assertEqual([], flurm_job_deps:get_dependencies(999))
           end}
      end,

      fun(State) ->
          {"get_dependencies returns all deps for job",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 10, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 10, afterany, 2}, {self(), make_ref()}, State),

               Deps = flurm_job_deps:get_dependencies(10),
               ?assertEqual(2, length(Deps)),

               %% Verify all deps are for job 10
               lists:foreach(fun(Dep) ->
                   ?assertEqual(10, Dep#dependency.job_id)
               end, Deps)
           end}
      end,

      fun(State) ->
          {"get_dependencies only returns deps for specified job",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 10, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 20, afterany, 2}, {self(), make_ref()}, State),

               Deps10 = flurm_job_deps:get_dependencies(10),
               ?assertEqual(1, length(Deps10)),

               Deps20 = flurm_job_deps:get_dependencies(20),
               ?assertEqual(1, length(Deps20))
           end}
      end
     ]}.

%%====================================================================
%% Test: get_dependents/1
%%====================================================================

get_dependents_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"get_dependents returns empty list for job with no dependents",
           fun() ->
               ?assertEqual([], flurm_job_deps:get_dependents(999))
           end}
      end,

      fun(State) ->
          {"get_dependents returns list of dependent jobs",
           fun() ->
               %% Jobs 2, 3, 4 all depend on job 1
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterany, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 4, afternotok, 1}, {self(), make_ref()}, State),

               Dependents = flurm_job_deps:get_dependents(1),
               ?assertEqual(3, length(Dependents)),
               ?assert(lists:member(2, Dependents)),
               ?assert(lists:member(3, Dependents)),
               ?assert(lists:member(4, Dependents))
           end}
      end
     ]}.

%%====================================================================
%% Test: check_dependencies/1
%%====================================================================

check_dependencies_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"check_dependencies returns ok for job with no deps",
           fun() ->
               ?assertEqual({ok, []}, flurm_job_deps:check_dependencies(999))
           end}
      end,

      fun(State) ->
          {"check_dependencies returns waiting with unsatisfied deps",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 10, afterok, 1}, {self(), make_ref()}, State),

               Result = flurm_job_deps:check_dependencies(10),
               ?assertMatch({waiting, [_]}, Result),

               {waiting, Unsatisfied} = Result,
               ?assertEqual(1, length(Unsatisfied)),
               [Dep] = Unsatisfied,
               ?assertEqual(false, Dep#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"check_dependencies returns ok when all deps satisfied",
           fun() ->
               %% Add a singleton dep (satisfied immediately when no other holder)
               flurm_job_deps:handle_call({add_dependency, 10, singleton, <<"test">>}, {self(), make_ref()}, State),

               ?assertEqual({ok, []}, flurm_job_deps:check_dependencies(10))
           end}
      end
     ]}.

%%====================================================================
%% Test: are_dependencies_satisfied/1
%%====================================================================

are_dependencies_satisfied_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"returns true for job with no deps",
           fun() ->
               ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(999))
           end}
      end,

      fun(State) ->
          {"returns false when deps not satisfied",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 10, afterok, 1}, {self(), make_ref()}, State),

               ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(10))
           end}
      end,

      fun(State) ->
          {"returns true when all deps satisfied",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 10, singleton, <<"test">>}, {self(), make_ref()}, State),

               ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(10))
           end}
      end
     ]}.

%%====================================================================
%% Test: get_dependency_graph/0
%%====================================================================

get_dependency_graph_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"empty graph when no deps",
           fun() ->
               ?assertEqual(#{}, flurm_job_deps:get_dependency_graph())
           end}
      end,

      fun(State) ->
          {"graph contains all dependencies",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterany, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterok, 2}, {self(), make_ref()}, State),

               Graph = flurm_job_deps:get_dependency_graph(),

               ?assert(maps:is_key(2, Graph)),
               ?assert(maps:is_key(3, Graph)),

               %% Job 2 depends on job 1
               ?assertEqual([{afterok, 1}], maps:get(2, Graph)),

               %% Job 3 depends on jobs 1 and 2
               Job3Deps = maps:get(3, Graph),
               ?assertEqual(2, length(Job3Deps)),
               ?assert(lists:member({afterany, 1}, Job3Deps)),
               ?assert(lists:member({afterok, 2}, Job3Deps))
           end}
      end
     ]}.

%%====================================================================
%% Test: detect_circular_dependency/2
%%====================================================================

detect_circular_dependency_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"no cycle when no existing deps",
           fun() ->
               ?assertEqual(ok, flurm_job_deps:detect_circular_dependency(2, 1))
           end}
      end,

      fun(State) ->
          {"no cycle with linear deps",
           fun() ->
               %% 3 -> 2 -> 1
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterok, 2}, {self(), make_ref()}, State),

               %% Adding 4 -> 3 would not create cycle
               ?assertEqual(ok, flurm_job_deps:detect_circular_dependency(4, 3))
           end}
      end,

      fun(State) ->
          {"detects simple cycle",
           fun() ->
               %% 2 -> 1
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               %% Adding 1 -> 2 would create cycle
               Result = flurm_job_deps:detect_circular_dependency(1, 2),
               ?assertMatch({error, circular_dependency, _Path}, Result)
           end}
      end,

      fun(State) ->
          {"detects longer cycle",
           fun() ->
               %% 2 -> 1, 3 -> 2
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterok, 2}, {self(), make_ref()}, State),

               %% Adding 1 -> 3 would create cycle: 1 -> 3 -> 2 -> 1
               Result = flurm_job_deps:detect_circular_dependency(1, 3),
               ?assertMatch({error, circular_dependency, _Path}, Result)
           end}
      end,

      fun(_State) ->
          {"non-integer target never creates cycle",
           fun() ->
               ?assertEqual(ok, flurm_job_deps:detect_circular_dependency(1, <<"singleton">>))
           end}
      end
     ]}.

%%====================================================================
%% Test: has_circular_dependency/2
%%====================================================================

has_circular_dependency_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(_State) ->
          {"returns false when no cycle",
           fun() ->
               ?assertEqual(false, flurm_job_deps:has_circular_dependency(2, 1))
           end}
      end,

      fun(State) ->
          {"returns true when cycle exists",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               ?assertEqual(true, flurm_job_deps:has_circular_dependency(1, 2))
           end}
      end
     ]}.

%%====================================================================
%% Test: Singleton dependency behavior
%%====================================================================

singleton_dependency_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"first singleton holder gets satisfied dep",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 1, singleton, <<"unique">>}, {self(), make_ref()}, State),

               [Dep] = ets:lookup(?DEPS_TABLE, {1, singleton, <<"unique">>}),
               ?assertEqual(true, Dep#dependency.satisfied),

               %% Singleton table shows job 1 holds it
               ?assertEqual([{<<"unique">>, 1}], ets:tab2list(?SINGLETON_TABLE))
           end}
      end,

      fun(State) ->
          {"second singleton requester gets unsatisfied dep",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 1, singleton, <<"unique">>}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 2, singleton, <<"unique">>}, {self(), make_ref()}, State),

               [Dep1] = ets:lookup(?DEPS_TABLE, {1, singleton, <<"unique">>}),
               ?assertEqual(true, Dep1#dependency.satisfied),

               [Dep2] = ets:lookup(?DEPS_TABLE, {2, singleton, <<"unique">>}),
               ?assertEqual(false, Dep2#dependency.satisfied)
           end}
      end,

      fun(State) ->
          {"singleton released on job completion transfers to next waiter",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 1, singleton, <<"unique">>}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 2, singleton, <<"unique">>}, {self(), make_ref()}, State),

               %% Job 1 completes
               flurm_job_deps:handle_cast({job_state_change, 1, completed}, State),

               %% Job 2 should now have singleton
               [Dep2] = ets:lookup(?DEPS_TABLE, {2, singleton, <<"unique">>}),
               ?assertEqual(true, Dep2#dependency.satisfied),

               %% Singleton table should show job 2
               ?assertEqual([{<<"unique">>, 2}], ets:tab2list(?SINGLETON_TABLE))
           end}
      end
     ]}.

%%====================================================================
%% Test: Circular dependency prevention
%%====================================================================

circular_dep_prevention_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"adding dependency that creates cycle fails",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               Result = flurm_job_deps:handle_call({add_dependency, 1, afterok, 2}, {self(), make_ref()}, State),

               ?assertMatch({reply, {error, {circular_dependency, _}}, _}, Result)
           end}
      end,

      fun(State) ->
          {"adding multiple deps with cycle fails",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               %% Try to add deps that would create cycle
               Deps = [{afterok, 3}, {afterany, 2}],  %% 1 -> 2 would create cycle
               Result = flurm_job_deps:handle_call({add_dependencies, 1, Deps}, {self(), make_ref()}, State),

               ?assertMatch({reply, {error, {circular_dependency, _}}, _}, Result)
           end}
      end
     ]}.

%%====================================================================
%% Test: Edge cases
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun() ->
         setup(),
         init_tables(),
         #state{}
     end,
     fun(_) -> cleanup() end,
     [
      fun(State) ->
          {"duplicate dependency addition is idempotent",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),

               %% Should still have just one dep
               Deps = flurm_job_deps:get_dependencies(2),
               ?assertEqual(1, length(Deps))
           end}
      end,

      fun(State) ->
          {"multiple deps on same target allowed",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 3, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterany, 1}, {self(), make_ref()}, State),

               Deps = flurm_job_deps:get_dependencies(3),
               ?assertEqual(2, length(Deps))
           end}
      end,

      fun(State) ->
          {"different jobs can have deps on same target",
           fun() ->
               flurm_job_deps:handle_call({add_dependency, 2, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 3, afterok, 1}, {self(), make_ref()}, State),
               flurm_job_deps:handle_call({add_dependency, 4, afterok, 1}, {self(), make_ref()}, State),

               Dependents = flurm_job_deps:get_dependents(1),
               ?assertEqual(3, length(Dependents))
           end}
      end
     ]}.
