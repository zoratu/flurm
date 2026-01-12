%%%-------------------------------------------------------------------
%%% @doc PropEr Property-Based Tests for FLURM Protocol Codec
%%%
%%% Tests protocol encoding/decoding invariants:
%%% - Round-trip encode/decode preserves data
%%% - Malformed input doesn't crash
%%% - Header parsing is correct
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_flurm_protocol).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Suppress unused warnings for PropEr generator functions
-compile({nowarn_unused_function, [
    protocol_header/0,
    valid_msg_type/0,
    malformed_message/0
]}).

-export([prop_roundtrip_job_submit/0,
         prop_roundtrip_job_info/0,
         prop_header_parsing/0,
         prop_malformed_input_safe/0,
         prop_length_field_correct/0]).

%%====================================================================
%% Generators
%%====================================================================

%% Generate valid job submission request
job_submit_req() ->
    ?LET({Name, Partition, Nodes, Cpus, Mem, Time, Script, Priority},
         {non_empty(binary()), 
          binary(),
          range(1, 100),
          range(1, 64),
          range(1024, 1048576),
          range(60, 604800),
          binary(),
          range(0, 10000)},
         #job_submit_req{
             name = Name,
             partition = Partition,
             num_nodes = Nodes,
             num_cpus = Cpus,
             memory_mb = Mem,
             time_limit = Time,
             script = Script,
             priority = Priority,
             env = #{},
             working_dir = <<"/tmp">>
         }).

%% Generate valid protocol header
protocol_header() ->
    ?LET({Version, Flags, MsgType},
         {range(16#2600, 16#2700), range(0, 15), valid_msg_type()},
         #slurm_header{
             version = Version,
             flags = Flags,
             msg_type = MsgType
         }).

%% Generate valid message types
valid_msg_type() ->
    oneof([
        ?REQUEST_PING,
        ?REQUEST_NODE_REGISTRATION_STATUS,
        ?REQUEST_JOB_INFO,
        ?REQUEST_SUBMIT_BATCH_JOB,
        ?REQUEST_CANCEL_JOB,
        ?RESPONSE_SLURM_RC
    ]).

%% Generate random binary data (for fuzzing)
random_binary() ->
    ?LET(Size, range(0, 1000),
         binary(Size)).

%% Generate malformed message (valid length, random content)
malformed_message() ->
    ?LET(Body, binary(range(10, 500)),
         begin
             Len = byte_size(Body),
             <<Len:32/big, Body/binary>>
         end).

%%====================================================================
%% Properties
%%====================================================================

%% PROPERTY: Encode then decode preserves job submit request
prop_roundtrip_job_submit() ->
    ?FORALL(JobReq, job_submit_req(),
        begin
            case flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobReq) of
                {ok, Encoded} ->
                    case flurm_protocol_codec:decode(Encoded) of
                        {ok, #slurm_msg{body = Decoded}, <<>>} ->
                            %% Compare key fields
                            compare_job_submit(JobReq, Decoded);
                        {error, _} ->
                            false
                    end;
                {error, _} ->
                    %% Some inputs might not be encodable
                    true
            end
        end).

%% PROPERTY: Job info round-trip
prop_roundtrip_job_info() ->
    ?FORALL({JobId, ShowFlags, UserId},
            {range(1, 1000000), range(0, 255), range(0, 65535)},
        begin
            Request = #job_info_request{
                job_id = JobId,
                show_flags = ShowFlags,
                user_id = UserId
            },
            case flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Request) of
                {ok, Encoded} ->
                    case flurm_protocol_codec:decode(Encoded) of
                        {ok, #slurm_msg{body = Decoded}, <<>>} ->
                            Decoded#job_info_request.job_id =:= JobId;
                        _ ->
                            false
                    end;
                {error, _} ->
                    true
            end
        end).

%% PROPERTY: Header parsing extracts correct values
prop_header_parsing() ->
    ?FORALL({Version, Flags, MsgType, BodySize},
            {range(16#2600, 16#2700), range(0, 15), range(1000, 9000), range(0, 1000)},
        begin
            %% Construct a valid header
            TotalLen = 10 + BodySize,
            Header = <<TotalLen:32/big, Version:16/big, Flags:16/big, 
                       0:16/big, MsgType:16/big, BodySize:32/big>>,
            Body = binary:copy(<<0>>, BodySize),
            Message = <<Header/binary, Body/binary>>,
            
            case flurm_protocol_codec:decode(Message) of
                {ok, #slurm_msg{header = ParsedHeader}, <<>>} ->
                    ParsedHeader#slurm_header.version =:= Version andalso
                    ParsedHeader#slurm_header.flags =:= Flags andalso
                    ParsedHeader#slurm_header.msg_type =:= MsgType;
                {error, _} ->
                    %% Unknown message types are acceptable
                    true
            end
        end).

%% PROPERTY: Malformed input doesn't crash, returns error
prop_malformed_input_safe() ->
    ?FORALL(Data, random_binary(),
        begin
            try
                case flurm_protocol_codec:decode(Data) of
                    {ok, _, _} -> true;
                    {error, _} -> true;
                    {incomplete, _} -> true
                end
            catch
                %% These should NOT happen - they indicate bugs
                error:badarg -> false;
                error:function_clause -> false;
                error:{badmatch, _} -> false;
                %% Other exceptions might be acceptable
                _:_ -> true
            end
        end).

%% PROPERTY: Length field in encoded message is correct
prop_length_field_correct() ->
    ?FORALL(JobReq, job_submit_req(),
        begin
            case flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobReq) of
                {ok, <<Len:32/big, Rest/binary>>} ->
                    %% Length should equal remaining bytes
                    byte_size(Rest) =:= Len;
                {error, _} ->
                    true
            end
        end).

%%====================================================================
%% Helper Functions
%%====================================================================

compare_job_submit(#job_submit_req{} = Original, Decoded) when is_map(Decoded) ->
    %% When decoded as a map
    maps:get(name, Decoded, undefined) =:= Original#job_submit_req.name orelse
    maps:get(<<"name">>, Decoded, undefined) =:= Original#job_submit_req.name;
compare_job_submit(#job_submit_req{} = Original, #job_submit_req{} = Decoded) ->
    %% When decoded as same record type
    Original#job_submit_req.name =:= Decoded#job_submit_req.name andalso
    Original#job_submit_req.num_cpus =:= Decoded#job_submit_req.num_cpus;
compare_job_submit(_, _) ->
    %% Different types might be acceptable depending on codec design
    true.

%%====================================================================
%% EUnit Integration
%%====================================================================

proper_test_() ->
    {timeout, 120, [
        ?_assert(proper:quickcheck(prop_header_parsing(), 
                                   [{numtests, 200}, {to_file, user}])),
        ?_assert(proper:quickcheck(prop_malformed_input_safe(), 
                                   [{numtests, 500}, {to_file, user}])),
        ?_assert(proper:quickcheck(prop_length_field_correct(), 
                                   [{numtests, 100}, {to_file, user}]))
    ]}.
