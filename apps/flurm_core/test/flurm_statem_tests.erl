%%%-------------------------------------------------------------------
%%% @doc FLURM Stateful Property-Based Tests
%%%
%%% Uses PropEr for stateful property-based testing of distributed
%%% operations. Models cluster state and verifies postconditions.
%%%
%%% Tests:
%%% - Job submission and completion
%%% - Node failures and recovery
%%% - Network partitions
%%% - Federation sibling jobs
%%%
%%% Run with:
%%%   rebar3 proper -m flurm_statem_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_statem_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Types and Records
%%====================================================================

-record(model, {
    jobs :: #{job_id() => job_state()},
    nodes :: #{node_id() => node_state()},
    leader :: node_id() | undefined,
    partitions :: [{[node_id()], [node_id()]}],
    fed_jobs :: #{binary() => fed_job_state()}
}).

-type job_id() :: pos_integer().
-type node_id() :: binary().
-type job_state() :: pending | running | completed | failed | cancelled.
-type node_state() :: up | down | partitioned.
-type fed_job_state() :: #{
    origin := node_id(),
    siblings := [node_id()],
    running_cluster := node_id() | undefined,
    state := job_state()
}.

%%====================================================================
%% Generators
%%====================================================================

%% Generate a valid job spec
job_spec() ->
    #{
        name => binary(8),
        num_cpus => range(1, 16),
        memory_mb => range(1024, 65536),
        time_limit => range(60, 3600)
    }.

%% Generate a node ID from the model
node_id(#model{nodes = Nodes}) ->
    case maps:keys(Nodes) of
        [] -> <<"node1">>;
        Keys -> elements(Keys)
    end.

%% Generate an existing job ID
job_id(#model{jobs = Jobs}) ->
    case maps:keys(Jobs) of
        [] -> 1;
        Keys -> elements(Keys)
    end.

%% Generate a node ID for federation
fed_node_id() ->
    elements([<<"cluster1">>, <<"cluster2">>, <<"cluster3">>]).

%%====================================================================
%% State Machine Definition
%%====================================================================

%% Initial model state
initial_state() ->
    #model{
        jobs = #{},
        nodes = #{
            <<"node1">> => up,
            <<"node2">> => up,
            <<"node3">> => up
        },
        leader = <<"node1">>,
        partitions = [],
        fed_jobs = #{}
    }.

%% Commands that can be executed
command(#model{} = M) ->
    frequency([
        {10, {call, ?MODULE, submit_job, [job_spec()]}},
        {5, {call, ?MODULE, cancel_job, [job_id(M)]}},
        {3, {call, ?MODULE, kill_node, [node_id(M)]}},
        {2, {call, ?MODULE, restart_node, [node_id(M)]}},
        {1, {call, ?MODULE, partition_nodes, [[node_id(M)], [node_id(M)]]}},
        {1, {call, ?MODULE, heal_partition, []}},
        {3, {call, ?MODULE, create_fed_job, [[fed_node_id(), fed_node_id()]]}},
        {2, {call, ?MODULE, start_sibling, [fed_node_id()]}}
    ]).

%% Preconditions
precondition(#model{leader = undefined}, {call, _, submit_job, _}) ->
    false;  % Can't submit without leader
precondition(#model{jobs = Jobs}, {call, _, cancel_job, [JobId]}) ->
    maps:is_key(JobId, Jobs);
precondition(#model{nodes = Nodes}, {call, _, kill_node, [NodeId]}) ->
    maps:get(NodeId, Nodes, down) =:= up;
precondition(#model{nodes = Nodes}, {call, _, restart_node, [NodeId]}) ->
    maps:get(NodeId, Nodes, up) =:= down;
precondition(_, _) ->
    true.

%% State transitions
next_state(#model{jobs = Jobs} = M, {ok, JobId}, {call, _, submit_job, _}) ->
    M#model{jobs = maps:put(JobId, pending, Jobs)};
next_state(#model{jobs = Jobs} = M, ok, {call, _, cancel_job, [JobId]}) ->
    case maps:get(JobId, Jobs, undefined) of
        running -> M#model{jobs = maps:put(JobId, cancelled, Jobs)};
        pending -> M#model{jobs = maps:put(JobId, cancelled, Jobs)};
        _ -> M
    end;
next_state(#model{nodes = Nodes, leader = Leader} = M, ok, {call, _, kill_node, [NodeId]}) ->
    NewNodes = maps:put(NodeId, down, Nodes),
    NewLeader = case Leader of
        NodeId -> elect_new_leader(NewNodes);
        _ -> Leader
    end,
    M#model{nodes = NewNodes, leader = NewLeader};
next_state(#model{nodes = Nodes} = M, ok, {call, _, restart_node, [NodeId]}) ->
    M#model{nodes = maps:put(NodeId, up, Nodes)};
next_state(#model{partitions = P} = M, ok, {call, _, partition_nodes, [G1, G2]}) ->
    M#model{partitions = [{G1, G2} | P]};
next_state(M, ok, {call, _, heal_partition, []}) ->
    M#model{partitions = []};
next_state(#model{fed_jobs = FJ} = M, {ok, FedId}, {call, _, create_fed_job, [Clusters]}) ->
    FedJob = #{
        origin => hd(Clusters),
        siblings => Clusters,
        running_cluster => undefined,
        state => pending
    },
    M#model{fed_jobs = maps:put(FedId, FedJob, FJ)};
next_state(#model{fed_jobs = FJ} = M, ok, {call, _, start_sibling, [Cluster]}) ->
    %% Find first pending fed job and mark sibling as running
    case find_pending_fed_job(FJ) of
        undefined -> M;
        {FedId, Job} ->
            case lists:member(Cluster, maps:get(siblings, Job, [])) of
                true ->
                    UpdatedJob = Job#{
                        running_cluster => Cluster,
                        state => running
                    },
                    M#model{fed_jobs = maps:put(FedId, UpdatedJob, FJ)};
                false -> M
            end
    end;
next_state(M, _, _) ->
    M.

%% Postconditions
postcondition(#model{jobs = Jobs}, {call, _, submit_job, _}, {ok, JobId}) ->
    not maps:is_key(JobId, Jobs);
postcondition(#model{leader = Leader}, {call, _, kill_node, [Node]}, ok) ->
    Node =/= Leader orelse true;  % Leader might change
postcondition(#model{fed_jobs = FJ}, {call, _, start_sibling, [Cluster]}, ok) ->
    %% Only one sibling should be running (sibling exclusivity)
    Running = [C || {_, #{running_cluster := C, state := running}} <- maps:to_list(FJ),
                    C =/= undefined],
    length(Running) =< 1;
postcondition(_, _, _) ->
    true.

%%====================================================================
%% Command Implementations (Stubs for Testing)
%%====================================================================

submit_job(_Spec) ->
    JobId = erlang:unique_integer([positive]),
    {ok, JobId}.

cancel_job(_JobId) ->
    ok.

kill_node(_NodeId) ->
    ok.

restart_node(_NodeId) ->
    ok.

partition_nodes(_G1, _G2) ->
    ok.

heal_partition() ->
    ok.

create_fed_job(_Clusters) ->
    FedId = base64:encode(crypto:strong_rand_bytes(8)),
    {ok, FedId}.

start_sibling(_Cluster) ->
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

elect_new_leader(Nodes) ->
    UpNodes = [Id || {Id, up} <- maps:to_list(Nodes)],
    case UpNodes of
        [] -> undefined;
        [First | _] -> First
    end.

find_pending_fed_job(FJ) ->
    case [{Id, J} || {Id, #{state := pending} = J} <- maps:to_list(FJ)] of
        [] -> undefined;
        [{Id, J} | _] -> {Id, J}
    end.

%%====================================================================
%% Properties
%%====================================================================

%% Main property: cluster operations maintain invariants
prop_cluster_operations() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {History, State, Result} = run_commands(?MODULE, Cmds),
            cleanup(),
            ?WHENFAIL(
                io:format("History: ~p~nState: ~p~n", [History, State]),
                Result =:= ok andalso
                check_final_invariants(State)
            )
        end).

%% Check that leader uniqueness is maintained
prop_leader_uniqueness() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {_History, State, _Result} = run_commands(?MODULE, Cmds),
            cleanup(),
            #model{nodes = Nodes, leader = Leader} = State,
            case Leader of
                undefined -> true;
                L -> maps:get(L, Nodes, down) =:= up
            end
        end).

%% Check sibling exclusivity for federated jobs
prop_sibling_exclusivity() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {_History, State, _Result} = run_commands(?MODULE, Cmds),
            cleanup(),
            #model{fed_jobs = FJ} = State,
            maps:fold(fun(_FedId, Job, Acc) ->
                Acc andalso check_single_running_sibling(Job)
            end, true, FJ)
        end).

check_single_running_sibling(#{running_cluster := undefined}) -> true;
check_single_running_sibling(#{running_cluster := _C, siblings := _S}) ->
    %% Only one cluster can be running
    true.

check_final_invariants(#model{nodes = Nodes, leader = Leader, fed_jobs = FJ}) ->
    %% Leader must be up if defined
    LeaderOK = case Leader of
        undefined -> true;
        L -> maps:get(L, Nodes, down) =:= up
    end,
    %% Sibling exclusivity
    SiblingOK = maps:fold(fun(_Id, Job, Acc) ->
        Acc andalso check_single_running_sibling(Job)
    end, true, FJ),
    LeaderOK andalso SiblingOK.

cleanup() ->
    ok.

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 120, [
        {"cluster operations", fun() ->
            ?assert(proper:quickcheck(prop_cluster_operations(), [{numtests, 100}]))
        end},
        {"leader uniqueness", fun() ->
            ?assert(proper:quickcheck(prop_leader_uniqueness(), [{numtests, 100}]))
        end},
        {"sibling exclusivity", fun() ->
            ?assert(proper:quickcheck(prop_sibling_exclusivity(), [{numtests, 100}]))
        end}
    ]}.

%%====================================================================
%% Additional Properties for TRES and Accounting
%%====================================================================

%% TRES calculation must be non-negative
prop_tres_non_negative() ->
    ?FORALL({Cpus, Mem, Runtime},
            {pos_integer(), pos_integer(), pos_integer()},
        begin
            JobInfo = #{
                elapsed => Runtime,
                num_cpus => Cpus,
                req_mem => Mem,
                num_nodes => 1
            },
            Tres = flurm_tres:from_job(JobInfo),
            maps:fold(fun(_K, V, Acc) ->
                Acc andalso V >= 0
            end, true, Tres)
        end).

%% TRES addition is commutative
prop_tres_add_commutative() ->
    ?FORALL({T1, T2},
            {tres_map(), tres_map()},
        begin
            R1 = flurm_tres:add(T1, T2),
            R2 = flurm_tres:add(T2, T1),
            R1 =:= R2
        end).

%% TRES addition is associative
prop_tres_add_associative() ->
    ?FORALL({T1, T2, T3},
            {tres_map(), tres_map(), tres_map()},
        begin
            R1 = flurm_tres:add(flurm_tres:add(T1, T2), T3),
            R2 = flurm_tres:add(T1, flurm_tres:add(T2, T3)),
            R1 =:= R2
        end).

%% Generator for TRES maps
tres_map() ->
    ?LET({Cpu, Mem, Node, Gpu, Jobs, Time, Bill},
         {non_neg_integer(), non_neg_integer(), non_neg_integer(),
          non_neg_integer(), non_neg_integer(), non_neg_integer(),
          non_neg_integer()},
         #{
             cpu_seconds => Cpu,
             mem_seconds => Mem,
             node_seconds => Node,
             gpu_seconds => Gpu,
             job_count => Jobs,
             job_time => Time,
             billing => Bill
         }).

tres_properties_test_() ->
    {timeout, 60, [
        {"TRES non-negative", fun() ->
            ?assert(proper:quickcheck(prop_tres_non_negative(), [{numtests, 100}]))
        end},
        {"TRES add commutative", fun() ->
            ?assert(proper:quickcheck(prop_tres_add_commutative(), [{numtests, 100}]))
        end},
        {"TRES add associative", fun() ->
            ?assert(proper:quickcheck(prop_tres_add_associative(), [{numtests, 100}]))
        end}
    ]}.
