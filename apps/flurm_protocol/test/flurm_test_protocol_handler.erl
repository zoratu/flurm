%%%-------------------------------------------------------------------
%%% @doc Test protocol handler for integration tests
%%%
%%% A simple Ranch handler that accepts SLURM protocol messages and
%%% sends appropriate responses.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_protocol_handler).

-behaviour(ranch_protocol).

-export([start_link/3, init/3]).

-include("flurm_protocol.hrl").

start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, _Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    Transport:setopts(Socket, [{active, false}, {packet, 0}]),
    loop(Socket, Transport, <<>>).

loop(Socket, Transport, Buffer) ->
    case Transport:recv(Socket, 0, 30000) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer, Socket, Transport) of
                {ok, Rest} ->
                    loop(Socket, Transport, Rest);
                {error, _Reason} ->
                    Transport:close(Socket)
            end;
        {error, closed} ->
            ok;
        {error, _Reason} ->
            Transport:close(Socket)
    end.

process_buffer(Buffer, Socket, Transport) ->
    case flurm_protocol_codec:decode(Buffer) of
        {ok, Msg, Rest} ->
            handle_message(Msg, Socket, Transport),
            process_buffer(Rest, Socket, Transport);
        {error, {incomplete_message, _, _}} ->
            {ok, Buffer};
        {error, {incomplete_length_prefix, _}} ->
            {ok, Buffer};
        {error, Reason} ->
            {error, Reason}
    end.

handle_message(#slurm_msg{header = Header, body = Body}, Socket, Transport) ->
    MsgType = Header#slurm_header.msg_type,
    Response = handle_request(MsgType, Body),
    case Response of
        {reply, RespType, RespBody} ->
            {ok, Encoded} = flurm_protocol_codec:encode(RespType, RespBody),
            Transport:send(Socket, Encoded);
        noreply ->
            ok
    end.

%% Handle different message types
handle_request(?REQUEST_PING, _Body) ->
    {reply, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}};

handle_request(?REQUEST_JOB_INFO, #job_info_request{}) ->
    %% Return empty job list
    Response = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = 0,
        jobs = []
    },
    {reply, ?RESPONSE_JOB_INFO, Response};

handle_request(?REQUEST_SUBMIT_BATCH_JOB, #batch_job_request{name = Name}) ->
    %% Simulate job submission - return a job ID
    JobId = erlang:unique_integer([positive, monotonic]) rem 1000000,
    Response = #batch_job_response{
        job_id = JobId,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Submitted batch job ", (integer_to_binary(JobId))/binary,
                                " (", Name/binary, ")">>
    },
    {reply, ?RESPONSE_SUBMIT_BATCH_JOB, Response};

handle_request(?REQUEST_CANCEL_JOB, #cancel_job_request{}) ->
    %% Acknowledge the cancel
    {reply, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}};

handle_request(?REQUEST_NODE_INFO, _Body) ->
    Response = #node_info_response{
        last_update = erlang:system_time(second),
        node_count = 0,
        nodes = []
    },
    {reply, ?RESPONSE_NODE_INFO, Response};

handle_request(?REQUEST_PARTITION_INFO, _Body) ->
    Response = #partition_info_response{
        last_update = erlang:system_time(second),
        partition_count = 0,
        partitions = []
    },
    {reply, ?RESPONSE_PARTITION_INFO, Response};

handle_request(_MsgType, _Body) ->
    %% Unknown message - return generic success
    {reply, ?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}.
