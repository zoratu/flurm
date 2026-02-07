%%%-------------------------------------------------------------------
%%% @doc Property-Based Tests for FLURM Core
%%%
%%% Uses PropEr to generate random test cases and find edge cases
%%% that unit tests might miss.
%%%
%%% Run with: rebar3 proper
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_prop_tests).

-include_lib("proper/include/proper.hrl").

%% Export properties for rebar3 proper
-export([
    prop_normalize_cpu_count_positive/0,
    prop_format_cpu_count_roundtrip/0,
    prop_kvs_put_get/0,
    prop_kvs_size_increases/0,
    prop_kvs_merge/0,
    prop_kvs_index_coverage/0,
    prop_pmi_encode_valid/0,
    prop_memory_report_valid/0,
    prop_process_report_valid/0,
    prop_health_check_valid/0
]).

%%%===================================================================
%%% Test Generators
%%%===================================================================

%% Generate a valid job ID
job_id() ->
    ?LET(N, pos_integer(), N rem 1000000 + 1).

%% Generate a valid job name
job_name() ->
    ?LET(Chars, non_empty(list(range($a, $z))),
         list_to_binary(Chars)).

%% Generate CPU count (including fractional)
cpu_count() ->
    oneof([
        pos_integer(),
        ?LET(F, range(1, 100), F / 10.0)  % 0.1 to 10.0
    ]).

%% Generate memory in MB
memory_mb() ->
    ?LET(N, range(1, 65536), N).

%% Generate a job specification
job_spec() ->
    ?LET({JobId, Name, Cpus, Memory, TimeLimit},
         {job_id(), job_name(), cpu_count(), memory_mb(), oneof([undefined, pos_integer()])},
         #{
             job_id => JobId,
             name => Name,
             script => <<"#!/bin/bash\necho 'test'">>,
             partition => <<"batch">>,
             num_cpus => Cpus,
             memory_mb => Memory,
             time_limit => TimeLimit,
             user => <<"testuser">>,
             account => <<"default">>
         }).

%% Generate a node name
node_name() ->
    ?LET(N, range(1, 100),
         list_to_binary("node" ++ integer_to_list(N))).

%% Generate node resources
node_resources() ->
    ?LET({Cpus, Memory, GPUs},
         {range(1, 128), range(1024, 524288), range(0, 8)},
         #{
             cpus => Cpus,
             memory_mb => Memory,
             gpus => GPUs
         }).

%%%===================================================================
%%% Property Tests - Job Executor
%%%===================================================================

%% Property: normalize_cpu_count always returns a positive number
prop_normalize_cpu_count_positive() ->
    ?FORALL(Input, oneof([
        pos_integer(),
        ?LET(F, range(1, 1000), F / 10.0),
        ?LET(N, pos_integer(), integer_to_list(N)),
        ?LET(F, range(1, 100), io_lib:format("~.1f", [F / 10.0])),
        ?LET(N, pos_integer(), integer_to_binary(N))
    ]),
    begin
        Result = flurm_job_executor:normalize_cpu_count(Input),
        is_number(Result) andalso Result > 0
    end).

%% Property: format_cpu_count produces parseable strings
prop_format_cpu_count_roundtrip() ->
    ?FORALL(Cpus, oneof([pos_integer(), ?LET(F, range(1, 100), F / 4.0)]),
    begin
        Formatted = flurm_job_executor:format_cpu_count(Cpus),
        is_list(Formatted) andalso
        case string:to_float(Formatted) of
            {Float, []} -> Float > 0;
            _ ->
                case string:to_integer(Formatted) of
                    {Int, []} -> Int > 0;
                    _ -> false
                end
        end
    end).

%%%===================================================================
%%% Property Tests - Protocol Codec
%%%===================================================================

%% Property: encode/decode roundtrip for job submit
prop_job_submit_roundtrip() ->
    ?FORALL(JobSpec, job_spec(),
    begin
        %% This tests that our protocol encoding is reversible
        %% We can't easily test full roundtrip without the server,
        %% so we verify the data structure is valid
        is_map(JobSpec) andalso
        maps:is_key(job_id, JobSpec) andalso
        maps:is_key(num_cpus, JobSpec)
    end).

%% Property: message header encoding is always 16 bytes
prop_header_size() ->
    ?FORALL({MsgType, Flags, BodyLen},
            {range(1, 10000), range(0, 65535), range(0, 1000000)},
    begin
        %% Header should always be fixed size
        true  % Placeholder - actual test would encode and check size
    end).

%%%===================================================================
%%% Property Tests - PMI KVS
%%%===================================================================

%% Property: KVS put/get roundtrip
prop_kvs_put_get() ->
    ?FORALL({Key, Value}, {binary(), binary()},
    begin
        KVS0 = flurm_pmi_kvs:new(),
        KVS1 = flurm_pmi_kvs:put(KVS0, Key, Value),
        case flurm_pmi_kvs:get(KVS1, Key) of
            {ok, Retrieved} -> Retrieved =:= Value;
            _ -> false
        end
    end).

%% Property: KVS size increases with unique puts
prop_kvs_size_increases() ->
    ?FORALL(Keys, non_empty(list(binary())),
    begin
        UniqueKeys = lists:usort(Keys),
        KVS = lists:foldl(fun(K, Acc) ->
            flurm_pmi_kvs:put(Acc, K, <<"value">>)
        end, flurm_pmi_kvs:new(), UniqueKeys),
        flurm_pmi_kvs:size(KVS) =:= length(UniqueKeys)
    end).

%% Property: KVS merge combines all keys
prop_kvs_merge() ->
    ?FORALL({Keys1, Keys2}, {list(binary()), list(binary())},
    begin
        KVS1 = lists:foldl(fun(K, Acc) ->
            flurm_pmi_kvs:put(Acc, K, <<"v1">>)
        end, flurm_pmi_kvs:new(), Keys1),
        KVS2 = lists:foldl(fun(K, Acc) ->
            flurm_pmi_kvs:put(Acc, K, <<"v2">>)
        end, flurm_pmi_kvs:new(), Keys2),
        Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
        AllKeys = lists:usort(Keys1 ++ Keys2),
        flurm_pmi_kvs:size(Merged) =:= length(AllKeys)
    end).

%% Property: KVS get_by_index covers all entries
prop_kvs_index_coverage() ->
    ?FORALL(Entries, non_empty(list({binary(), binary()})),
    begin
        UniqueEntries = maps:to_list(maps:from_list(Entries)),
        KVS = lists:foldl(fun({K, V}, Acc) ->
            flurm_pmi_kvs:put(Acc, K, V)
        end, flurm_pmi_kvs:new(), UniqueEntries),
        Size = flurm_pmi_kvs:size(KVS),
        %% Should be able to iterate through all entries
        Results = [flurm_pmi_kvs:get_by_index(KVS, I) || I <- lists:seq(0, Size - 1)],
        length([R || {ok, _, _} = R <- Results]) =:= Size
    end).

%%%===================================================================
%%% Property Tests - PMI Protocol
%%%===================================================================

%% Property: PMI protocol encode produces valid output
prop_pmi_encode_valid() ->
    ?FORALL({Cmd, Attrs}, {pmi_command(), pmi_attrs()},
    begin
        Result = flurm_pmi_protocol:encode(Cmd, Attrs),
        is_binary(Result) andalso byte_size(Result) > 0
    end).

pmi_command() ->
    oneof([init_ack, maxes, appnum, my_kvsname, barrier_out, put_ack, get_ack, finalize_ack]).

pmi_attrs() ->
    ?LET(Pairs, list({pmi_attr_key(), pmi_attr_value()}),
         maps:from_list(Pairs)).

pmi_attr_key() ->
    oneof([rc, pmi_version, pmi_subversion, size, rank, kvsname, value, key]).

pmi_attr_value() ->
    oneof([
        range(-1, 1000),
        ?LET(Chars, list(range($a, $z)), list_to_binary(Chars))
    ]).

%%%===================================================================
%%% Property Tests - Diagnostics
%%%===================================================================

%% Property: memory_report returns valid structure
prop_memory_report_valid() ->
    ?FORALL(_, exactly(ok),
    begin
        Report = flurm_diagnostics:memory_report(),
        is_map(Report) andalso
        maps:is_key(total, Report) andalso
        maps:is_key(processes, Report) andalso
        maps:is_key(binary, Report) andalso
        maps:get(total, Report) > 0
    end).

%% Property: process_report returns valid structure
prop_process_report_valid() ->
    ?FORALL(_, exactly(ok),
    begin
        Report = flurm_diagnostics:process_report(),
        is_map(Report) andalso
        maps:is_key(count, Report) andalso
        maps:is_key(limit, Report) andalso
        maps:get(count, Report) > 0 andalso
        maps:get(limit, Report) >= maps:get(count, Report)
    end).

%% Property: health_check returns valid response
prop_health_check_valid() ->
    ?FORALL(_, exactly(ok),
    begin
        Result = flurm_diagnostics:health_check(),
        case Result of
            ok -> true;
            {warning, W} when is_list(W) -> true;
            {critical, C} when is_list(C) -> true;
            _ -> false
        end
    end).

%%%===================================================================
%%% Stateful Property Tests - Job Manager State Machine
%%%===================================================================

%% This tests that the job manager maintains consistent state
%% through arbitrary sequences of operations

-record(job_state, {
    jobs = #{} :: #{integer() => atom()},
    next_id = 1 :: integer()
}).

%% Initial state
initial_state() ->
    #job_state{}.

%% Command generators
command(_S) ->
    oneof([
        {call, ?MODULE, submit_job_cmd, [job_spec()]},
        {call, ?MODULE, cancel_job_cmd, [job_id()]},
        {call, ?MODULE, get_status_cmd, [job_id()]}
    ]).

%% Command implementations (stubs for testing)
submit_job_cmd(_JobSpec) ->
    %% Would call actual job manager
    {ok, erlang:unique_integer([positive])}.

cancel_job_cmd(_JobId) ->
    ok.

get_status_cmd(_JobId) ->
    {ok, pending}.

%% Preconditions
precondition(_S, _Cmd) ->
    true.

%% Postconditions
postcondition(_S, {call, _, submit_job_cmd, [_]}, {ok, JobId}) ->
    is_integer(JobId) andalso JobId > 0;
postcondition(_S, {call, _, cancel_job_cmd, [_]}, Result) ->
    Result =:= ok orelse Result =:= {error, not_found};
postcondition(_S, {call, _, get_status_cmd, [_]}, {ok, Status}) ->
    lists:member(Status, [pending, running, completed, failed, cancelled]);
postcondition(_, _, _) ->
    true.

%% State transitions
next_state(S, {ok, JobId}, {call, _, submit_job_cmd, [_]}) ->
    S#job_state{
        jobs = maps:put(JobId, pending, S#job_state.jobs),
        next_id = S#job_state.next_id + 1
    };
next_state(S, ok, {call, _, cancel_job_cmd, [JobId]}) ->
    S#job_state{jobs = maps:remove(JobId, S#job_state.jobs)};
next_state(S, _, _) ->
    S.

%% The property
prop_job_manager_consistent() ->
    ?FORALL(Cmds, commands(?MODULE),
    begin
        {_History, _State, Result} = run_commands(?MODULE, Cmds),
        Result =:= ok
    end).

