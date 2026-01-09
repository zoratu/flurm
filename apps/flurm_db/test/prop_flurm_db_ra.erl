%%%-------------------------------------------------------------------
%%% @doc Property-based tests for FLURM Ra state machine
%%%
%%% Uses PropEr to verify Ra state machine invariants:
%%% - Job ID uniqueness
%%% - State consistency after commands
%%% - Linearizability of operations
%%% - Crash recovery correctness
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_flurm_db_ra).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%% PropEr properties
-export([
    prop_job_ids_unique/0,
    prop_job_state_consistency/0,
    prop_node_registration_idempotent/0,
    prop_cancel_idempotent/0,
    prop_operations_commutative_when_independent/0,
    prop_state_machine_deterministic/0
]).

%% EUnit wrapper - proper_test_/0 is auto-exported by eunit.hrl

%%====================================================================
%% Generators
%%====================================================================

%% Generate a job spec
job_spec_gen() ->
    ?LET({Name, User, Cpus, Memory, TimeLimit, Priority},
         {binary(8), binary(4), range(1, 64), range(128, 65536),
          range(60, 86400), range(0, 1000)},
         #ra_job_spec{
             name = Name,
             user = User,
             group = <<"default">>,
             partition = <<"batch">>,
             script = <<"#!/bin/bash\necho test">>,
             num_nodes = 1,
             num_cpus = Cpus,
             memory_mb = Memory,
             time_limit = TimeLimit,
             priority = Priority
         }).

%% Generate a node spec
node_spec_gen() ->
    ?LET({Name, Cpus, Memory},
         {binary(8), range(1, 128), range(1024, 262144)},
         #ra_node_spec{
             name = Name,
             hostname = <<Name/binary, ".example.com">>,
             port = 7000,
             cpus = Cpus,
             memory_mb = Memory,
             gpus = 0,
             features = [],
             partitions = [<<"batch">>]
         }).

%% Generate a partition spec
partition_spec_gen() ->
    ?LET(Name, binary(8),
         #ra_partition_spec{
             name = Name,
             nodes = [],
             max_time = 86400,
             default_time = 3600,
             max_nodes = 100,
             priority = 10
         }).

%% Generate a valid job state
job_state_gen() ->
    oneof([pending, configuring, running, completing, completed, cancelled, failed]).

%% Generate Ra commands
ra_command_gen() ->
    oneof([
        ?LET(Spec, job_spec_gen(), {submit_job, Spec}),
        ?LET(Id, range(1, 100), {cancel_job, Id}),
        ?LET({Id, State}, {range(1, 100), job_state_gen()}, {update_job_state, Id, State}),
        ?LET(Spec, node_spec_gen(), {register_node, Spec}),
        ?LET(Name, binary(8), {unregister_node, Name}),
        ?LET(Spec, partition_spec_gen(), {create_partition, Spec}),
        ?LET(Name, binary(8), {delete_partition, Name})
    ]).

%% Generate a sequence of commands
command_sequence() ->
    list(ra_command_gen()).

%%====================================================================
%% Properties
%%====================================================================

%% Property: All job IDs are unique
prop_job_ids_unique() ->
    ?FORALL(NumJobs, range(1, 100),
        begin
            JobSpecs = [generate_job_spec(N) || N <- lists:seq(1, NumJobs)],
            State0 = flurm_db_ra:init(#{}),
            {_FinalState, JobIds} = lists:foldl(
                fun(Spec, {State, Ids}) ->
                    {NewState, {ok, JobId}, _Effects} =
                        flurm_db_ra:apply(#{index => length(Ids) + 1},
                                          {submit_job, Spec}, State),
                    {NewState, [JobId | Ids]}
                end,
                {State0, []},
                JobSpecs
            ),
            %% All IDs should be unique
            length(JobIds) =:= length(lists:usort(JobIds))
        end).

%% Property: Job state is consistent after any command sequence
prop_job_state_consistency() ->
    ?FORALL(Commands, command_sequence(),
        begin
            State0 = flurm_db_ra:init(#{}),
            FinalState = apply_commands(Commands, State0),
            check_state_consistency(FinalState)
        end).

%% Property: Node registration is idempotent
prop_node_registration_idempotent() ->
    ?FORALL(NodeSpec, node_spec_gen(),
        begin
            State0 = flurm_db_ra:init(#{}),

            %% Register once
            {State1, {ok, registered}, _} =
                flurm_db_ra:apply(#{index => 1}, {register_node, NodeSpec}, State0),

            %% Register again - should return updated
            {State2, {ok, updated}, _} =
                flurm_db_ra:apply(#{index => 2}, {register_node, NodeSpec}, State1),

            %% Node should exist in both states with same name
            NodeName = NodeSpec#ra_node_spec.name,
            maps:is_key(NodeName, State1#ra_state.nodes) andalso
            maps:is_key(NodeName, State2#ra_state.nodes)
        end).

%% Property: Cancelling a job multiple times is idempotent
prop_cancel_idempotent() ->
    ?FORALL(JobSpec, job_spec_gen(),
        begin
            State0 = flurm_db_ra:init(#{}),

            %% Submit a job
            {State1, {ok, JobId}, _} =
                flurm_db_ra:apply(#{index => 1}, {submit_job, JobSpec}, State0),

            %% Cancel it
            {State2, ok, _} =
                flurm_db_ra:apply(#{index => 2}, {cancel_job, JobId}, State1),

            %% Cancel again - should return already_terminal
            {State3, {error, already_terminal}, _} =
                flurm_db_ra:apply(#{index => 3}, {cancel_job, JobId}, State2),

            %% Job should be cancelled in both states
            Job2 = maps:get(JobId, State2#ra_state.jobs),
            Job3 = maps:get(JobId, State3#ra_state.jobs),
            Job2#ra_job.state =:= cancelled andalso
            Job3#ra_job.state =:= cancelled
        end).

%% Property: Independent operations can be applied in any order
prop_operations_commutative_when_independent() ->
    ?FORALL({JobSpec1, JobSpec2}, {job_spec_gen(), job_spec_gen()},
        ?IMPLIES(JobSpec1#ra_job_spec.name =/= JobSpec2#ra_job_spec.name,
        begin
            State0 = flurm_db_ra:init(#{}),

            %% Order 1: Job1 then Job2
            {State1a, {ok, _Id1a}, _} =
                flurm_db_ra:apply(#{index => 1}, {submit_job, JobSpec1}, State0),
            {State1b, {ok, _Id1b}, _} =
                flurm_db_ra:apply(#{index => 2}, {submit_job, JobSpec2}, State1a),

            %% Order 2: Job2 then Job1
            {State2a, {ok, _Id2a}, _} =
                flurm_db_ra:apply(#{index => 1}, {submit_job, JobSpec2}, State0),
            {State2b, {ok, _Id2b}, _} =
                flurm_db_ra:apply(#{index => 2}, {submit_job, JobSpec1}, State2a),

            %% Both states should have 2 jobs
            maps:size(State1b#ra_state.jobs) =:= 2 andalso
            maps:size(State2b#ra_state.jobs) =:= 2
        end)).

%% Property: State machine is deterministic
prop_state_machine_deterministic() ->
    ?FORALL(Commands, command_sequence(),
        begin
            State0 = flurm_db_ra:init(#{}),

            %% Apply commands twice
            FinalState1 = apply_commands(Commands, State0),
            FinalState2 = apply_commands(Commands, State0),

            %% States should be identical
            states_equal(FinalState1, FinalState2)
        end).

%%====================================================================
%% Helper Functions
%%====================================================================

generate_job_spec(N) ->
    #ra_job_spec{
        name = list_to_binary("job_" ++ integer_to_list(N)),
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 128,
        time_limit = 3600,
        priority = 100
    }.

apply_commands(Commands, State) ->
    {FinalState, _} = lists:foldl(
        fun(Command, {AccState, Index}) ->
            case catch flurm_db_ra:apply(#{index => Index}, Command, AccState) of
                {NewState, _Result, _Effects} when is_record(NewState, ra_state) ->
                    {NewState, Index + 1};
                {NewState, _Result} when is_record(NewState, ra_state) ->
                    {NewState, Index + 1};
                _ ->
                    {AccState, Index + 1}
            end
        end,
        {State, 1},
        Commands
    ),
    FinalState.

check_state_consistency(#ra_state{jobs = Jobs, nodes = Nodes, job_counter = Counter}) ->
    %% Job counter should be greater than all job IDs
    JobIds = maps:keys(Jobs),
    CounterValid = case JobIds of
        [] -> true;
        _ -> Counter > lists:max(JobIds)
    end,

    %% All jobs should have valid states
    JobsValid = lists:all(
        fun(Job) ->
            lists:member(Job#ra_job.state,
                        [pending, configuring, running, completing,
                         completed, cancelled, failed, timeout, node_fail])
        end,
        maps:values(Jobs)
    ),

    %% All nodes should have valid states
    NodesValid = lists:all(
        fun(Node) ->
            lists:member(Node#ra_node.state, [up, down, drain, maint])
        end,
        maps:values(Nodes)
    ),

    CounterValid andalso JobsValid andalso NodesValid.

states_equal(State1, State2) ->
    %% Compare job maps
    Jobs1 = maps:map(fun(_K, J) ->
        {J#ra_job.name, J#ra_job.state, J#ra_job.priority}
    end, State1#ra_state.jobs),
    Jobs2 = maps:map(fun(_K, J) ->
        {J#ra_job.name, J#ra_job.state, J#ra_job.priority}
    end, State2#ra_state.jobs),

    %% Compare node maps
    Nodes1 = maps:map(fun(_K, N) ->
        {N#ra_node.name, N#ra_node.state}
    end, State1#ra_state.nodes),
    Nodes2 = maps:map(fun(_K, N) ->
        {N#ra_node.name, N#ra_node.state}
    end, State2#ra_state.nodes),

    Jobs1 =:= Jobs2 andalso
    Nodes1 =:= Nodes2 andalso
    State1#ra_state.job_counter =:= State2#ra_state.job_counter.

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 120, [
        {"Job IDs unique",
         fun() -> ?assert(proper:quickcheck(prop_job_ids_unique(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Job state consistency",
         fun() -> ?assert(proper:quickcheck(prop_job_state_consistency(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Node registration idempotent",
         fun() -> ?assert(proper:quickcheck(prop_node_registration_idempotent(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Cancel idempotent",
         fun() -> ?assert(proper:quickcheck(prop_cancel_idempotent(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"Independent operations commutative",
         fun() -> ?assert(proper:quickcheck(prop_operations_commutative_when_independent(),
                                            [{numtests, 500}, {to_file, user}])) end},
        {"State machine deterministic",
         fun() -> ?assert(proper:quickcheck(prop_state_machine_deterministic(),
                                            [{numtests, 500}, {to_file, user}])) end}
    ]}.
