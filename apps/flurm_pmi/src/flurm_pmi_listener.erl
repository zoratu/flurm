%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Listener
%%%
%%% Listens for PMI socket connections from MPI processes. Each MPI
%%% rank connects to this socket and exchanges PMI commands.
%%%
%%% The listener creates a Unix domain socket per job step at:
%%%   /tmp/flurm_pmi_<job_id>_<step_id>.sock
%%%
%%% MPI processes find this via PMI_FD environment variable (when
%%% pre-connected) or PMI_SOCK_PATH for post-connect.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_listener).

-behaviour(gen_server).

-export([
    start_link/3,
    stop/2,
    get_socket_path/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(connection_state, {
    rank :: non_neg_integer() | undefined,
    buffer = <<>> :: binary(),
    initialized = false :: boolean()
}).

-type connection_state() :: #connection_state{}.

-record(state, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    socket_path :: string(),
    listen_socket :: port() | undefined,
    connections = #{} :: #{port() => connection_state()}
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a PMI listener for a job step
-spec start_link(pos_integer(), non_neg_integer(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(JobId, StepId, Size) ->
    gen_server:start_link(?MODULE, [JobId, StepId, Size], []).

%% @doc Stop the PMI listener for a job step
-spec stop(pos_integer(), non_neg_integer()) -> ok.
stop(JobId, StepId) ->
    case whereis(listener_name(JobId, StepId)) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end.

%% @doc Get the socket path for a job step
-spec get_socket_path(pos_integer(), non_neg_integer()) -> string().
get_socket_path(JobId, StepId) ->
    io_lib:format("/tmp/flurm_pmi_~p_~p.sock", [JobId, StepId]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([JobId, StepId, Size]) ->
    %% Initialize PMI job state
    ok = flurm_pmi_manager:init_job(JobId, StepId, Size),

    SocketPath = lists:flatten(get_socket_path(JobId, StepId)),

    %% Remove old socket if exists
    file:delete(SocketPath),

    %% Create Unix domain socket
    case gen_tcp:listen(0, [
        {ifaddr, {local, SocketPath}},
        binary,
        {packet, line},
        {active, true},
        {reuseaddr, true}
    ]) of
        {ok, ListenSocket} ->
            %% Set permissions so MPI processes can connect
            os:cmd("chmod 777 " ++ SocketPath),

            %% Register with name for lookup
            register(listener_name(JobId, StepId), self()),

            lager:info("PMI listener started for job ~p step ~p at ~s",
                      [JobId, StepId, SocketPath]),

            %% Start accepting connections
            self() ! accept,

            {ok, #state{
                job_id = JobId,
                step_id = StepId,
                socket_path = SocketPath,
                listen_socket = ListenSocket
            }};
        {error, Reason} ->
            lager:error("Failed to create PMI socket at ~s: ~p", [SocketPath, Reason]),
            {stop, {socket_error, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(accept, #state{listen_socket = ListenSocket} = State) ->
    %% Non-blocking accept
    case gen_tcp:accept(ListenSocket, 0) of
        {ok, Socket} ->
            lager:debug("PMI connection accepted for job ~p", [State#state.job_id]),
            ConnState = #connection_state{},
            NewConns = maps:put(Socket, ConnState, State#state.connections),
            self() ! accept,
            {noreply, State#state{connections = NewConns}};
        {error, timeout} ->
            %% No pending connection, try again later
            erlang:send_after(100, self(), accept),
            {noreply, State};
        {error, Reason} ->
            lager:error("PMI accept error: ~p", [Reason]),
            erlang:send_after(1000, self(), accept),
            {noreply, State}
    end;

handle_info({tcp, Socket, Data}, State) ->
    case maps:find(Socket, State#state.connections) of
        {ok, ConnState} ->
            case handle_pmi_data(Data, ConnState, State) of
                {ok, NewConnState, Response} ->
                    send_response(Socket, Response),
                    NewConns = maps:put(Socket, NewConnState, State#state.connections),
                    {noreply, State#state{connections = NewConns}};
                {close, Response} ->
                    send_response(Socket, Response),
                    gen_tcp:close(Socket),
                    NewConns = maps:remove(Socket, State#state.connections),
                    {noreply, State#state{connections = NewConns}};
                {error, Reason} ->
                    lager:warning("PMI protocol error: ~p", [Reason]),
                    gen_tcp:close(Socket),
                    NewConns = maps:remove(Socket, State#state.connections),
                    {noreply, State#state{connections = NewConns}}
            end;
        error ->
            lager:warning("Data from unknown socket"),
            {noreply, State}
    end;

handle_info({tcp_closed, Socket}, State) ->
    lager:debug("PMI connection closed"),
    NewConns = maps:remove(Socket, State#state.connections),
    {noreply, State#state{connections = NewConns}};

handle_info({tcp_error, Socket, Reason}, State) ->
    lager:warning("PMI socket error: ~p", [Reason]),
    NewConns = maps:remove(Socket, State#state.connections),
    {noreply, State#state{connections = NewConns}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket_path = SocketPath, listen_socket = ListenSocket}) ->
    %% Close listen socket
    case ListenSocket of
        undefined -> ok;
        _ -> gen_tcp:close(ListenSocket)
    end,
    %% Remove socket file
    file:delete(SocketPath),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

listener_name(JobId, StepId) ->
    list_to_atom(io_lib:format("flurm_pmi_listener_~p_~p", [JobId, StepId])).

send_response(Socket, Response) when is_binary(Response) ->
    gen_tcp:send(Socket, Response);
send_response(_Socket, undefined) ->
    ok.

handle_pmi_data(Data, ConnState, State) ->
    %% Append to buffer and process complete lines
    Buffer = <<(ConnState#connection_state.buffer)/binary, Data/binary>>,
    process_buffer(Buffer, ConnState, State).

process_buffer(Buffer, ConnState, State) ->
    case binary:split(Buffer, <<"\n">>) of
        [Line, Rest] ->
            %% Process complete line
            case process_pmi_line(Line, ConnState, State) of
                {ok, NewConnState, Response} ->
                    %% Continue processing remaining buffer
                    case Rest of
                        <<>> ->
                            {ok, NewConnState#connection_state{buffer = <<>>}, Response};
                        _ ->
                            %% More data to process - for now just update buffer
                            {ok, NewConnState#connection_state{buffer = Rest}, Response}
                    end;
                Other ->
                    Other
            end;
        [Incomplete] ->
            %% No complete line yet
            {ok, ConnState#connection_state{buffer = Incomplete}, undefined}
    end.

process_pmi_line(Line, ConnState, State) ->
    JobId = State#state.job_id,
    StepId = State#state.step_id,

    case flurm_pmi_protocol:decode(Line) of
        {ok, {init, Attrs}} ->
            handle_init(Attrs, ConnState, JobId, StepId);
        {ok, {get_maxes, _Attrs}} ->
            handle_get_maxes(ConnState, JobId, StepId);
        {ok, {get_appnum, _Attrs}} ->
            handle_get_appnum(ConnState, JobId, StepId);
        {ok, {get_my_kvsname, _Attrs}} ->
            handle_get_kvsname(ConnState, JobId, StepId);
        {ok, {barrier_in, _Attrs}} ->
            handle_barrier(ConnState, JobId, StepId);
        {ok, {put, Attrs}} ->
            handle_put(Attrs, ConnState, JobId, StepId);
        {ok, {get, Attrs}} ->
            handle_get(Attrs, ConnState, JobId, StepId);
        {ok, {getbyidx, Attrs}} ->
            handle_getbyidx(Attrs, ConnState, JobId, StepId);
        {ok, {finalize, _Attrs}} ->
            handle_finalize(ConnState, JobId, StepId);
        {ok, {Cmd, _Attrs}} ->
            lager:warning("Unhandled PMI command: ~p", [Cmd]),
            Response = flurm_pmi_protocol:encode(unknown, #{rc => -1}),
            {ok, ConnState, Response};
        {error, Reason} ->
            lager:warning("PMI decode error: ~p for line: ~p", [Reason, Line]),
            {error, Reason}
    end.

handle_init(Attrs, ConnState, JobId, StepId) ->
    PmiVersion = maps:get(pmi_version, Attrs, 1),
    PmiSubversion = maps:get(pmi_subversion, Attrs, 1),

    case flurm_pmi_manager:get_job_info(JobId, StepId) of
        {ok, Info} ->
            Size = maps:get(size, Info, 1),
            %% Rank is typically passed in init or inferred from connection order
            Rank = maps:get(rank, Attrs, maps:get(rank_count, Info, 0)),

            %% Register this rank
            Node = list_to_binary(atom_to_list(node())),
            flurm_pmi_manager:register_rank(JobId, StepId, Rank, Node),

            Response = flurm_pmi_protocol:encode(init_ack, #{
                rc => 0,
                pmi_version => PmiVersion,
                pmi_subversion => PmiSubversion,
                size => Size,
                rank => Rank,
                debug => 0,
                appnum => 0
            }),
            NewConnState = ConnState#connection_state{
                rank = Rank,
                initialized = true
            },
            {ok, NewConnState, Response};
        {error, _} ->
            Response = flurm_pmi_protocol:encode(init_ack, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_get_maxes(ConnState, JobId, StepId) ->
    case flurm_pmi_manager:get_job_info(JobId, StepId) of
        {ok, _Info} ->
            Response = flurm_pmi_protocol:encode(maxes, #{
                rc => 0,
                kvsname_max => 256,
                keylen_max => 256,
                vallen_max => 1024
            }),
            {ok, ConnState, Response};
        {error, _} ->
            Response = flurm_pmi_protocol:encode(maxes, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_get_appnum(ConnState, _JobId, _StepId) ->
    Response = flurm_pmi_protocol:encode(appnum, #{
        rc => 0,
        appnum => 0
    }),
    {ok, ConnState, Response}.

handle_get_kvsname(ConnState, JobId, StepId) ->
    case flurm_pmi_manager:get_kvs_name(JobId, StepId) of
        {ok, KvsName} ->
            Response = flurm_pmi_protocol:encode(my_kvsname, #{
                rc => 0,
                kvsname => KvsName
            }),
            {ok, ConnState, Response};
        {error, _} ->
            Response = flurm_pmi_protocol:encode(my_kvsname, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_barrier(ConnState, JobId, StepId) ->
    Rank = ConnState#connection_state.rank,
    case flurm_pmi_manager:barrier_in(JobId, StepId, Rank) of
        ok ->
            %% Barrier complete, also commit KVS
            flurm_pmi_manager:kvs_commit(JobId, StepId),
            Response = flurm_pmi_protocol:encode(barrier_out, #{rc => 0}),
            {ok, ConnState, Response};
        {error, Reason} ->
            lager:warning("Barrier failed: ~p", [Reason]),
            Response = flurm_pmi_protocol:encode(barrier_out, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_put(Attrs, ConnState, JobId, StepId) ->
    Key = maps:get(key, Attrs, <<>>),
    Value = maps:get(value, Attrs, <<>>),

    case flurm_pmi_manager:kvs_put(JobId, StepId, Key, Value) of
        ok ->
            Response = flurm_pmi_protocol:encode(put_ack, #{rc => 0}),
            {ok, ConnState, Response};
        {error, _} ->
            Response = flurm_pmi_protocol:encode(put_ack, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_get(Attrs, ConnState, JobId, StepId) ->
    Key = maps:get(key, Attrs, <<>>),

    case flurm_pmi_manager:kvs_get(JobId, StepId, Key) of
        {ok, Value} ->
            Response = flurm_pmi_protocol:encode(get_ack, #{
                rc => 0,
                value => Value
            }),
            {ok, ConnState, Response};
        {error, not_found} ->
            Response = flurm_pmi_protocol:encode(get_ack, #{rc => -1}),
            {ok, ConnState, Response}
    end.

handle_getbyidx(Attrs, ConnState, JobId, StepId) ->
    Index = maps:get(idx, Attrs, 0),

    case flurm_pmi_manager:kvs_get_by_index(JobId, StepId, Index) of
        {ok, Key, Value} ->
            Response = flurm_pmi_protocol:encode(getbyidx_ack, #{
                rc => 0,
                key => Key,
                value => Value,
                nextidx => Index + 1
            }),
            {ok, ConnState, Response};
        {error, end_of_kvs} ->
            Response = flurm_pmi_protocol:encode(getbyidx_ack, #{
                rc => -1,
                reason => <<"end_of_kvs">>
            }),
            {ok, ConnState, Response}
    end.

handle_finalize(ConnState, _JobId, _StepId) ->
    Rank = ConnState#connection_state.rank,
    lager:debug("PMI finalize from rank ~p", [Rank]),
    Response = flurm_pmi_protocol:encode(finalize_ack, #{rc => 0}),
    {close, Response}.
