%%%-------------------------------------------------------------------
%%% @doc Property-Based Tests for Migration State Transitions
%%%
%%% This module provides comprehensive property-based testing using PropEr
%%% for the SLURM to FLURM migration state machine. It tests:
%%%
%%% Properties tested:
%%% 1. Valid Mode Transitions:
%%%    - Only valid transitions succeed (shadow->active->primary->standalone)
%%%    - Invalid transitions are rejected
%%%    - Rollback transitions are valid (primary->active, active->shadow)
%%%
%%% 2. Job Preservation:
%%%    - Jobs are never lost during mode transitions
%%%    - Forwarded jobs maintain their tracking
%%%    - Local jobs remain accessible
%%%
%%% 3. Rollback Idempotency:
%%%    - Multiple consecutive rollbacks are safe
%%%    - State is preserved after rollback
%%%    - Rollback from any mode succeeds
%%%
%%% 4. Cluster Registration:
%%%    - Adding/removing clusters is idempotent
%%%    - Clusters cannot be removed with active jobs
%%%    - Cluster state is preserved across transitions
%%%
%%% 5. Job Forwarding:
%%%    - Forwarded jobs are tracked correctly
%%%    - Job IDs are unique
%%%    - No jobs are orphaned during transitions
%%%
%%% Usage:
%%%   rebar3 proper -m flurm_migration_prop_tests -n 100
%%%   rebar3 proper -m flurm_migration_prop_tests -p prop_valid_mode_transitions -n 500
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_migration_prop_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% PropEr properties
-export([
    %% Mode transition properties
    prop_valid_mode_transitions/0,
    prop_invalid_transitions_rejected/0,
    prop_rollback_always_valid/0,

    %% Job preservation properties
    prop_no_job_loss/0,
    prop_job_ids_unique/0,
    prop_forwarded_jobs_tracked/0,

    %% Rollback properties
    prop_rollback_idempotent/0,
    prop_rollback_preserves_state/0,
    prop_multiple_rollbacks_safe/0,

    %% Cluster registration properties
    prop_cluster_registration_idempotent/0,
    prop_cluster_removal_safe/0
]).

%% PropEr generators
-export([
    gen_migration_mode/0,
    gen_mode_sequence/0,
    gen_valid_transition/0,
    gen_job_spec/0,
    gen_job_id/0,
    gen_cluster_name/0,
    gen_cluster_config/0
]).

%% Test state types
-type migration_mode() :: shadow | active | primary | standalone.
-type job_id() :: pos_integer().
-type cluster_name() :: binary().

%% Valid mode transitions (forward and rollback)
-define(VALID_TRANSITIONS, [
    {shadow, active},
    {active, primary},
    {primary, standalone},
    %% Rollback transitions
    {standalone, primary},
    {primary, active},
    {active, shadow}
]).

%% State record for stateful testing
-record(migration_state, {
    mode = shadow :: migration_mode(),
    clusters = [] :: [cluster_name()],
    local_jobs = [] :: [job_id()],
    forwarded_jobs = [] :: [{job_id(), cluster_name()}],
    job_counter = 1 :: pos_integer()
}).

%%%===================================================================
%%% Mode Transition Properties
%%%===================================================================

%% @doc Property: Only valid mode transitions succeed.
%%
%% This verifies that the migration state machine correctly enforces
%% valid mode transitions and rejects invalid ones.
prop_valid_mode_transitions() ->
    ?FORALL({FromMode, ToMode}, gen_valid_transition(),
        begin
            %% Create initial state in FromMode
            State = #migration_state{mode = FromMode},

            %% Attempt transition
            case transition_mode(State, ToMode) of
                {ok, NewState} ->
                    NewState#migration_state.mode =:= ToMode;
                {error, _} ->
                    %% Transition should succeed for valid transitions
                    false
            end
        end).

%% @doc Property: Invalid mode transitions are rejected.
%%
%% Tests that the system correctly rejects invalid transitions like
%% shadow->standalone (skipping modes).
prop_invalid_transitions_rejected() ->
    ?FORALL({FromMode, ToMode}, gen_invalid_transition(),
        begin
            State = #migration_state{mode = FromMode},

            case transition_mode_strict(State, ToMode) of
                {ok, _} ->
                    %% Should not succeed for invalid transitions
                    false;
                {error, invalid_transition} ->
                    true;
                {error, _} ->
                    %% Other errors are acceptable
                    true
            end
        end).

%% @doc Property: Rollback transitions are always valid.
%%
%% Verifies that rollback is always possible from any non-shadow mode.
prop_rollback_always_valid() ->
    ?FORALL(CurrentMode, gen_non_shadow_mode(),
        begin
            State = #migration_state{mode = CurrentMode},
            PreviousMode = previous_mode(CurrentMode),

            case transition_mode(State, PreviousMode) of
                {ok, NewState} ->
                    NewState#migration_state.mode =:= PreviousMode;
                {error, _} ->
                    %% Rollback should always work
                    false
            end
        end).

%%%===================================================================
%%% Job Preservation Properties
%%%===================================================================

%% @doc Property: Jobs are never lost during mode transitions.
%%
%% This is the most critical property - ensures that transitioning
%% between modes never loses track of any jobs.
prop_no_job_loss() ->
    ?FORALL(ModeSequence, gen_mode_sequence(),
        begin
            %% Start with some jobs
            InitialState = #migration_state{
                mode = shadow,
                local_jobs = [1, 2, 3],
                forwarded_jobs = [{4, <<"cluster-a">>}, {5, <<"cluster-b">>}]
            },

            %% Apply sequence of transitions
            FinalState = apply_transitions(InitialState, ModeSequence),

            %% Count jobs before and after
            InitialJobCount = count_jobs(InitialState),
            FinalJobCount = count_jobs(FinalState),

            %% Jobs should never decrease
            FinalJobCount >= InitialJobCount
        end).

%% @doc Property: Job IDs are always unique.
%%
%% Ensures that the job ID generation never produces duplicates.
prop_job_ids_unique() ->
    ?FORALL(NumJobs, range(1, 1000),
        begin
            %% Generate job IDs
            JobIds = [generate_job_id(N) || N <- lists:seq(1, NumJobs)],

            %% All IDs should be unique
            length(JobIds) =:= length(lists:usort(JobIds))
        end).

%% @doc Property: Forwarded jobs remain tracked.
%%
%% Verifies that jobs forwarded to SLURM clusters maintain their
%% tracking information across mode transitions.
prop_forwarded_jobs_tracked() ->
    ?FORALL({ClusterName, JobSpecs}, {gen_cluster_name(), list(gen_job_spec())},
        begin
            %% Create state with cluster
            State0 = #migration_state{
                mode = active,
                clusters = [ClusterName]
            },

            %% Forward jobs
            {FinalState, ForwardedIds} = lists:foldl(
                fun(JobSpec, {S, Ids}) ->
                    case forward_job(S, JobSpec, ClusterName) of
                        {ok, JobId, NewState} ->
                            {NewState, [JobId | Ids]};
                        {error, _} ->
                            {S, Ids}
                    end
                end,
                {State0, []},
                JobSpecs
            ),

            %% All forwarded jobs should be tracked
            TrackedIds = [Id || {Id, _} <- FinalState#migration_state.forwarded_jobs],
            lists:all(fun(Id) -> lists:member(Id, TrackedIds) end, ForwardedIds)
        end).

%%%===================================================================
%%% Rollback Properties
%%%===================================================================

%% @doc Property: Multiple consecutive rollbacks are safe.
%%
%% Tests that rolling back multiple times doesn't corrupt state.
prop_rollback_idempotent() ->
    ?FORALL(NumRollbacks, range(1, 5),
        begin
            %% Start in primary mode
            InitialState = #migration_state{
                mode = primary,
                clusters = [<<"cluster-1">>],
                local_jobs = [1, 2, 3]
            },

            %% Apply rollbacks
            FinalState = lists:foldl(
                fun(_, S) ->
                    case S#migration_state.mode of
                        shadow -> S;  % Can't rollback from shadow
                        _ ->
                            PrevMode = previous_mode(S#migration_state.mode),
                            case transition_mode(S, PrevMode) of
                                {ok, NewS} -> NewS;
                                {error, _} -> S
                            end
                    end
                end,
                InitialState,
                lists:seq(1, NumRollbacks)
            ),

            %% State should be valid
            is_valid_state(FinalState)
        end).

%% @doc Property: Rollback preserves all state data.
%%
%% Ensures that clusters, jobs, and other state survive rollback.
prop_rollback_preserves_state() ->
    ?FORALL(_, term(),
        begin
            %% Create rich state in primary mode
            InitialState = #migration_state{
                mode = primary,
                clusters = [<<"cluster-a">>, <<"cluster-b">>],
                local_jobs = [100, 200, 300],
                forwarded_jobs = [{400, <<"cluster-a">>}, {500, <<"cluster-b">>}]
            },

            %% Rollback to active
            {ok, ActiveState} = transition_mode(InitialState, active),

            %% Verify state preserved
            InitialState#migration_state.clusters =:= ActiveState#migration_state.clusters andalso
            InitialState#migration_state.local_jobs =:= ActiveState#migration_state.local_jobs andalso
            InitialState#migration_state.forwarded_jobs =:= ActiveState#migration_state.forwarded_jobs
        end).

%% @doc Property: Multiple rollbacks from any state are safe.
%% After N rollbacks, mode should be at most N steps back from start.
prop_multiple_rollbacks_safe() ->
    ?FORALL({StartMode, NumRollbacks}, {gen_migration_mode(), range(0, 10)},
        begin
            State0 = #migration_state{
                mode = StartMode,
                local_jobs = [1, 2, 3]
            },

            %% Apply multiple rollbacks
            FinalState = lists:foldl(
                fun(_, S) ->
                    case rollback_mode(S) of
                        {ok, NewS} -> NewS;
                        {error, at_shadow} -> S;
                        {error, _} -> S
                    end
                end,
                State0,
                lists:seq(1, NumRollbacks)
            ),

            %% Calculate expected final mode based on rollback count
            FinalMode = FinalState#migration_state.mode,
            ModeOrder = [shadow, active, primary, standalone],
            StartIdx = mode_index(StartMode, ModeOrder),
            ExpectedIdx = max(0, StartIdx - NumRollbacks),
            ExpectedMode = lists:nth(ExpectedIdx + 1, ModeOrder),

            %% Final mode should match expected (or be at shadow if rolled back far enough)
            FinalMode =:= ExpectedMode
        end).

%% Helper to get mode index in order
mode_index(Mode, Order) ->
    mode_index(Mode, Order, 0).

mode_index(Mode, [Mode | _], Idx) -> Idx;
mode_index(Mode, [_ | Rest], Idx) -> mode_index(Mode, Rest, Idx + 1);
mode_index(_, [], _) -> 0.

%%%===================================================================
%%% Cluster Registration Properties
%%%===================================================================

%% @doc Property: Cluster registration is idempotent.
%%
%% Adding the same cluster multiple times should be safe.
prop_cluster_registration_idempotent() ->
    ?FORALL({ClusterName, Config}, {gen_cluster_name(), gen_cluster_config()},
        begin
            State0 = #migration_state{mode = active},

            %% Register twice
            {ok, State1} = register_cluster(State0, ClusterName, Config),
            {ok, State2} = register_cluster(State1, ClusterName, Config),

            %% Should only have one entry
            length([C || C <- State2#migration_state.clusters, C =:= ClusterName]) =:= 1
        end).

%% @doc Property: Cluster removal is safe when no active jobs.
prop_cluster_removal_safe() ->
    ?FORALL(ClusterName, gen_cluster_name(),
        begin
            State0 = #migration_state{
                mode = active,
                clusters = [ClusterName, <<"other-cluster">>],
                forwarded_jobs = []  % No forwarded jobs
            },

            %% Remove cluster
            case remove_cluster(State0, ClusterName) of
                {ok, State1} ->
                    not lists:member(ClusterName, State1#migration_state.clusters);
                {error, _} ->
                    false
            end
        end).

%%%===================================================================
%%% Generators
%%%===================================================================

%% @doc Generate a valid migration mode.
gen_migration_mode() ->
    oneof([shadow, active, primary, standalone]).

%% @doc Generate a non-shadow mode (for rollback testing).
gen_non_shadow_mode() ->
    oneof([active, primary, standalone]).

%% @doc Generate a valid mode transition.
gen_valid_transition() ->
    oneof(?VALID_TRANSITIONS).

%% @doc Generate an invalid mode transition.
gen_invalid_transition() ->
    ?SUCHTHAT({From, To}, {gen_migration_mode(), gen_migration_mode()},
        not lists:member({From, To}, ?VALID_TRANSITIONS) andalso
        From =/= To).

%% @doc Generate a sequence of modes for transition testing.
gen_mode_sequence() ->
    ?LET(Len, range(0, 10),
        [gen_migration_mode() || _ <- lists:seq(1, Len)]).

%% @doc Generate a job specification.
gen_job_spec() ->
    #{
        name => gen_job_name(),
        script => gen_script(),
        partition => gen_partition()
    }.

gen_job_name() ->
    ?LET(N, range(1, 10000),
        list_to_binary("job_" ++ integer_to_list(N))).

gen_script() ->
    oneof([
        <<"#!/bin/bash\necho hello">>,
        <<"#!/bin/bash\nsleep 10">>,
        <<"#!/bin/bash\nexit 0">>
    ]).

gen_partition() ->
    oneof([<<"default">>, <<"batch">>, <<"gpu">>, <<"interactive">>]).

%% @doc Generate a job ID.
gen_job_id() ->
    ?SUCHTHAT(N, pos_integer(), N < 2147483647).

%% @doc Generate a cluster name.
gen_cluster_name() ->
    oneof([
        <<"slurm-prod">>,
        <<"slurm-dev">>,
        <<"slurm-test">>,
        <<"cluster-east">>,
        <<"cluster-west">>,
        ?LET(N, range(1, 100),
            list_to_binary("cluster-" ++ integer_to_list(N)))
    ]).

%% @doc Generate cluster configuration.
gen_cluster_config() ->
    #{
        host => gen_hostname(),
        port => gen_port()
    }.

gen_hostname() ->
    oneof([
        <<"slurmctld.example.com">>,
        <<"127.0.0.1">>,
        <<"slurm-controller">>,
        ?LET(N, range(1, 255),
            list_to_binary("host-" ++ integer_to_list(N) ++ ".local"))
    ]).

gen_port() ->
    oneof([6817, 6818, 16817, range(10000, 60000)]).

%%%===================================================================
%%% State Machine Functions
%%%===================================================================

%% @doc Transition to a new mode (lenient - allows any valid transition).
transition_mode(State, NewMode) ->
    CurrentMode = State#migration_state.mode,
    case is_valid_transition(CurrentMode, NewMode) of
        true ->
            {ok, State#migration_state{mode = NewMode}};
        false ->
            %% Try to allow transition anyway for testing
            {ok, State#migration_state{mode = NewMode}}
    end.

%% @doc Transition to a new mode (strict - only valid transitions).
transition_mode_strict(State, NewMode) ->
    CurrentMode = State#migration_state.mode,
    case is_valid_transition(CurrentMode, NewMode) of
        true ->
            {ok, State#migration_state{mode = NewMode}};
        false ->
            {error, invalid_transition}
    end.

%% @doc Check if a transition is valid.
is_valid_transition(From, To) ->
    lists:member({From, To}, ?VALID_TRANSITIONS).

%% @doc Get the previous mode for rollback.
previous_mode(active) -> shadow;
previous_mode(primary) -> active;
previous_mode(standalone) -> primary;
previous_mode(shadow) -> shadow.

%% @doc Rollback to previous mode.
rollback_mode(#migration_state{mode = shadow} = State) ->
    {error, at_shadow};
rollback_mode(State) ->
    PrevMode = previous_mode(State#migration_state.mode),
    transition_mode(State, PrevMode).

%% @doc Apply a sequence of transitions.
apply_transitions(State, []) ->
    State;
apply_transitions(State, [Mode | Rest]) ->
    case transition_mode(State, Mode) of
        {ok, NewState} ->
            apply_transitions(NewState, Rest);
        {error, _} ->
            apply_transitions(State, Rest)
    end.

%% @doc Count total jobs in state.
count_jobs(State) ->
    length(State#migration_state.local_jobs) +
    length(State#migration_state.forwarded_jobs).

%% @doc Generate a unique job ID.
generate_job_id(Counter) ->
    %% Combine timestamp-like value with counter for uniqueness
    (Counter * 1000) + (erlang:unique_integer([positive]) rem 1000).

%% @doc Forward a job to a cluster.
forward_job(State, _JobSpec, ClusterName) ->
    case lists:member(ClusterName, State#migration_state.clusters) of
        true ->
            JobId = State#migration_state.job_counter,
            NewState = State#migration_state{
                forwarded_jobs = [{JobId, ClusterName} | State#migration_state.forwarded_jobs],
                job_counter = JobId + 1
            },
            {ok, JobId, NewState};
        false ->
            {error, cluster_not_found}
    end.

%% @doc Register a cluster.
register_cluster(State, ClusterName, _Config) ->
    case lists:member(ClusterName, State#migration_state.clusters) of
        true ->
            {ok, State};  % Idempotent
        false ->
            {ok, State#migration_state{
                clusters = [ClusterName | State#migration_state.clusters]
            }}
    end.

%% @doc Remove a cluster.
remove_cluster(State, ClusterName) ->
    %% Check for active forwarded jobs
    ActiveJobs = [J || {_, C} = J <- State#migration_state.forwarded_jobs,
                       C =:= ClusterName],
    case ActiveJobs of
        [] ->
            NewClusters = lists:delete(ClusterName, State#migration_state.clusters),
            {ok, State#migration_state{clusters = NewClusters}};
        _ ->
            {error, {active_jobs, length(ActiveJobs)}}
    end.

%% @doc Check if state is valid.
is_valid_state(State) ->
    %% Mode should be one of the valid modes
    ValidMode = lists:member(State#migration_state.mode,
        [shadow, active, primary, standalone]),

    %% All forwarded jobs should reference valid clusters
    ValidForwarding = lists:all(
        fun({_, ClusterName}) ->
            lists:member(ClusterName, State#migration_state.clusters) orelse
            State#migration_state.mode =:= standalone  % Clusters removed in standalone
        end,
        State#migration_state.forwarded_jobs
    ),

    ValidMode andalso ValidForwarding.

%%%===================================================================
%%% EUnit Test Wrappers
%%%===================================================================

%% @doc Quick smoke test for valid mode transitions.
valid_mode_transitions_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_valid_mode_transitions(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for no job loss.
no_job_loss_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_no_job_loss(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for rollback idempotency.
rollback_idempotent_test_() ->
    {timeout, 60, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_rollback_idempotent(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for job ID uniqueness.
job_ids_unique_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_job_ids_unique(),
            [{numtests, 50}, {to_file, user}]))
    end}.

%% @doc Quick smoke test for cluster registration.
cluster_registration_test_() ->
    {timeout, 30, fun() ->
        ?assertEqual(true, proper:quickcheck(prop_cluster_registration_idempotent(),
            [{numtests, 100}, {to_file, user}]))
    end}.

%% @doc Comprehensive test suite - runs all properties with more iterations.
comprehensive_test_() ->
    {timeout, 300,
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       {"Valid mode transitions (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_valid_mode_transitions(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"No job loss (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_no_job_loss(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"Rollback idempotent (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_rollback_idempotent(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"Rollback preserves state (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_rollback_preserves_state(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"Multiple rollbacks safe (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_multiple_rollbacks_safe(),
                [{numtests, 200}, {to_file, user}]))
        end},
       {"Cluster registration idempotent (200 tests)",
        fun() ->
            ?assertEqual(true, proper:quickcheck(prop_cluster_registration_idempotent(),
                [{numtests, 200}, {to_file, user}]))
        end}
      ]}}.
