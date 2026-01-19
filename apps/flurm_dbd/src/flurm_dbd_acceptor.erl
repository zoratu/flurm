%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Connection Acceptor
%%%
%%% Ranch protocol handler for accepting DBD client connections.
%%% Handles connections from slurmctld, sacctmgr, and other tools.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_acceptor).

-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/3]).

-ifdef(TEST).
-export([
    loop/1,
    process_buffer/2,
    handle_message/2,
    process_request/3,
    handle_dbd_request/2,
    send_response/2,
    peername/2
]).
-endif.

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-record(conn_state, {
    socket :: ranch_transport:socket(),
    transport :: module(),
    buffer = <<>> :: binary(),
    authenticated = false :: boolean(),
    client_info = #{} :: map()
}).

%%====================================================================
%% API
%%====================================================================

start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, _Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, raw}, binary]),

    lager:debug("DBD connection accepted from ~p", [peername(Socket, Transport)]),

    State = #conn_state{
        socket = Socket,
        transport = Transport
    },
    loop(State).

%%====================================================================
%% Internal functions
%%====================================================================

loop(#conn_state{socket = Socket, transport = Transport, buffer = Buffer} = State) ->
    receive
        {tcp, Socket, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer, State) of
                {ok, Remaining, NewState} ->
                    ok = Transport:setopts(Socket, [{active, once}]),
                    loop(NewState#conn_state{buffer = Remaining});
                {error, Reason} ->
                    lager:warning("DBD connection error: ~p", [Reason]),
                    Transport:close(Socket)
            end;
        {tcp_closed, Socket} ->
            lager:debug("DBD connection closed"),
            ok;
        {tcp_error, Socket, Reason} ->
            lager:warning("DBD connection error: ~p", [Reason]),
            Transport:close(Socket);
        _Other ->
            ok = Transport:setopts(Socket, [{active, once}]),
            loop(State)
    after 300000 ->
        %% 5 minute timeout
        lager:debug("DBD connection timeout"),
        Transport:close(Socket)
    end.

process_buffer(Buffer, State) when byte_size(Buffer) < 4 ->
    %% Need more data for length prefix
    {ok, Buffer, State};
process_buffer(<<Length:32/big, Rest/binary>> = Buffer, State) when byte_size(Rest) < Length ->
    %% Need more data for complete message
    {ok, Buffer, State};
process_buffer(<<Length:32/big, Rest/binary>>, State) ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    case handle_message(MsgData, State) of
        {ok, NewState} ->
            %% Process any remaining data
            process_buffer(Remaining, NewState);
        {error, _} = Error ->
            Error
    end.

handle_message(MsgData, State) ->
    case flurm_protocol_codec:decode(<<(byte_size(MsgData)):32/big, MsgData/binary>>) of
        {ok, #slurm_msg{header = Header, body = Body}, <<>>} ->
            process_request(Header, Body, State);
        {error, Reason} ->
            lager:warning("Failed to decode DBD message: ~p", [Reason]),
            {error, decode_failed}
    end.

process_request(#slurm_header{msg_type = MsgType} = Header, Body, State) ->
    TypeName = flurm_protocol_codec:message_type_name(MsgType),
    lager:debug("DBD received request: ~p (~p)", [MsgType, TypeName]),

    Response = handle_dbd_request(Header, Body),
    case send_response(Response, State) of
        ok -> {ok, State};
        {error, _} = Error -> Error
    end.

handle_dbd_request(#slurm_header{msg_type = ?REQUEST_PING}, _Body) ->
    %% Respond to ping
    {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}};

handle_dbd_request(#slurm_header{msg_type = ?ACCOUNTING_UPDATE_MSG}, Body) ->
    %% Handle accounting update (job start/end)
    lager:debug("DBD accounting update: ~p", [Body]),
    %% Process and store the update
    case Body of
        #{type := job_start} = JobInfo ->
            flurm_dbd_server:record_job_start(JobInfo);
        #{type := job_end} = JobInfo ->
            flurm_dbd_server:record_job_end(JobInfo);
        #{type := step} = StepInfo ->
            flurm_dbd_server:record_job_step(StepInfo);
        _ ->
            ok
    end,
    {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}};

handle_dbd_request(#slurm_header{msg_type = ?ACCOUNTING_REGISTER_CTLD}, _Body) ->
    %% Controller registration
    lager:info("Controller registered with DBD"),
    {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}};

handle_dbd_request(#slurm_header{msg_type = MsgType}, _Body) ->
    lager:warning("Unsupported DBD message type: ~p", [MsgType]),
    {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}}.

send_response({MsgType, ResponseBody}, #conn_state{socket = Socket, transport = Transport}) ->
    case flurm_protocol_codec:encode(MsgType, ResponseBody) of
        {ok, EncodedMsg} ->
            case Transport:send(Socket, EncodedMsg) of
                ok -> ok;
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

peername(Socket, Transport) ->
    case Transport:peername(Socket) of
        {ok, {IP, Port}} ->
            io_lib:format("~p:~p", [IP, Port]);
        _ ->
            "unknown"
    end.
