%%%-------------------------------------------------------------------
%%% @doc FLURM srun Callback Handler
%%%
%%% Manages callback connections to srun clients. When srun requests
%%% a resource allocation, it provides a callback port where it listens
%%% for job state updates. This module:
%%%
%%% 1. Connects to srun's callback port after allocation
%%% 2. Sends job state messages (JOB_RUNNING, JOB_COMPLETE, etc.)
%%% 3. Maintains the connection for the duration of the job
%%%
%%% Message types sent to srun:
%%% - RESPONSE_JOB_READY (job is ready to run)
%%% - MESSAGE_TASK_EXIT (task completed)
%%% - RESPONSE_SLURM_RC (general return code)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_callback).

-behaviour(gen_server).

-export([start_link/0]).
-export([register_callback/4, notify_job_ready/2, notify_job_complete/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-define(SERVER, ?MODULE).
-define(CONNECT_TIMEOUT, 5000).
-define(SEND_TIMEOUT, 5000).

-record(state, {
    callbacks = #{} :: #{job_id() => callback_info()}
}).

-type job_id() :: non_neg_integer().
-type callback_info() :: #{
    host := binary(),
    port := non_neg_integer(),
    socket := inet:socket() | undefined,
    srun_pid := non_neg_integer()
}.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the callback manager.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a callback for a job allocation.
%% Called BEFORE sending RESPONSE_RESOURCE_ALLOCATION to srun.
%% Synchronous - blocks until connection attempt is complete.
-spec register_callback(job_id(), binary(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
register_callback(JobId, Host, Port, SrunPid) ->
    gen_server:call(?SERVER, {register_callback, JobId, Host, Port, SrunPid}, 10000).

%% @doc Notify srun that the job is ready to run.
%% This triggers srun to proceed with step creation.
-spec notify_job_ready(job_id(), binary()) -> ok | {error, term()}.
notify_job_ready(JobId, NodeList) ->
    gen_server:call(?SERVER, {notify_job_ready, JobId, NodeList}, 10000).

%% @doc Notify srun that the job has completed.
-spec notify_job_complete(job_id(), integer(), binary()) -> ok | {error, term()}.
notify_job_complete(JobId, ExitCode, Output) ->
    gen_server:call(?SERVER, {notify_job_complete, JobId, ExitCode, Output}, 10000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("srun callback manager started"),
    {ok, #state{}}.

handle_call({notify_job_ready, JobId, NodeList}, _From, State) ->
    Result = do_notify_job_ready(JobId, NodeList, State),
    {reply, Result, State};

handle_call({notify_job_complete, JobId, ExitCode, Output}, _From, State) ->
    Result = do_notify_job_complete(JobId, ExitCode, Output, State),
    %% Remove callback after completion
    NewCallbacks = maps:remove(JobId, State#state.callbacks),
    {reply, Result, State#state{callbacks = NewCallbacks}};

handle_call({register_callback, JobId, Host, Port, SrunPid}, _From, State) ->
    lager:info("Registering srun callback for job ~p: ~s:~p (pid=~p)",
               [JobId, Host, Port, SrunPid]),

    %% Connect to srun's callback port (synchronously before sending allocation response)
    case connect_to_srun(Host, Port) of
        {ok, Socket} ->
            lager:info("Connected to srun callback for job ~p", [JobId]),
            CallbackInfo = #{
                host => Host,
                port => Port,
                socket => Socket,
                srun_pid => SrunPid
            },
            NewCallbacks = maps:put(JobId, CallbackInfo, State#state.callbacks),

            %% Send job ready notification immediately (on connected callback socket)
            NodeList = <<"flurm-node1">>,
            ReadyResult = send_job_ready_message(Socket, JobId, NodeList),
            lager:info("Sent job ready for job ~p, result: ~p", [JobId, ReadyResult]),

            {reply, ok, State#state{callbacks = NewCallbacks}};
        {error, Reason} ->
            lager:warning("Failed to connect to srun callback for job ~p: ~p",
                          [JobId, Reason]),
            {reply, {error, Reason}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State) ->
    lager:debug("srun callback connection closed"),
    %% Find and remove the callback with this socket
    NewCallbacks = maps:filter(fun(_JobId, #{socket := S}) ->
        S =/= Socket
    end, State#state.callbacks),
    {noreply, State#state{callbacks = NewCallbacks}};

handle_info({tcp_error, Socket, Reason}, State) ->
    lager:warning("srun callback connection error: ~p", [Reason]),
    NewCallbacks = maps:filter(fun(_JobId, #{socket := S}) ->
        S =/= Socket
    end, State#state.callbacks),
    {noreply, State#state{callbacks = NewCallbacks}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Close all callback connections
    maps:foreach(fun(_JobId, #{socket := Socket}) ->
        catch gen_tcp:close(Socket)
    end, State#state.callbacks),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Connect to srun's callback port.
-spec connect_to_srun(binary(), non_neg_integer()) -> {ok, inet:socket()} | {error, term()}.
connect_to_srun(Host, Port) when Port > 0 ->
    HostStr = binary_to_list(Host),
    Options = [
        binary,
        {packet, raw},
        {active, true},
        {nodelay, true}
    ],
    lager:debug("Connecting to srun at ~s:~p", [HostStr, Port]),
    case gen_tcp:connect(HostStr, Port, Options, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            {ok, Socket};
        {error, Reason} ->
            {error, Reason}
    end;
connect_to_srun(_Host, _Port) ->
    {error, invalid_port}.

%% @doc Send job ready notification to srun.
%% This tells srun the allocation is active and it can proceed.
-spec do_notify_job_ready(job_id(), binary(), #state{}) -> ok | {error, term()}.
do_notify_job_ready(JobId, NodeList, #state{callbacks = Callbacks}) ->
    case maps:get(JobId, Callbacks, undefined) of
        undefined ->
            lager:warning("No callback registered for job ~p", [JobId]),
            {error, no_callback};
        #{socket := Socket} ->
            %% Send SRUN_JOB_COMPLETE or similar message
            %% SLURM uses MESSAGE_LAUNCH_COMPLETE or similar
            %% For now, send a simple RC message indicating success
            send_job_ready_message(Socket, JobId, NodeList)
    end.

%% @doc Send job complete notification to srun.
-spec do_notify_job_complete(job_id(), integer(), binary(), #state{}) -> ok | {error, term()}.
do_notify_job_complete(JobId, ExitCode, _Output, #state{callbacks = Callbacks}) ->
    case maps:get(JobId, Callbacks, undefined) of
        undefined ->
            {error, no_callback};
        #{socket := Socket} ->
            send_job_complete_message(Socket, JobId, ExitCode)
    end.

%% @doc Send job ready message to srun.
%% Format: SRUN_JOB_COMPLETE message type with job info
-spec send_job_ready_message(inet:socket(), job_id(), binary()) -> ok | {error, term()}.
send_job_ready_message(Socket, JobId, NodeList) ->
    %% SLURM's srun expects specific message formats on the callback
    %% The key message is SRUN_NODE_FAIL or success indicator
    %% For job ready, we can use a simple RC message with code 0

    %% Encode RESPONSE_SLURM_RC with return_code = 0
    case flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC,
            #slurm_rc_response{return_code = 0}) of
        {ok, MessageBin} ->
            lager:debug("Sending job ready to srun for job ~p, node=~s, size=~p",
                        [JobId, NodeList, byte_size(MessageBin)]),
            case gen_tcp:send(Socket, MessageBin) of
                ok ->
                    lager:info("Job ready notification sent to srun for job ~p", [JobId]),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to send job ready to srun: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Send job complete message to srun.
-spec send_job_complete_message(inet:socket(), job_id(), integer()) -> ok | {error, term()}.
send_job_complete_message(Socket, JobId, ExitCode) ->
    %% Send RESPONSE_SLURM_RC with the exit code
    case flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC,
            #slurm_rc_response{return_code = ExitCode}) of
        {ok, MessageBin} ->
            lager:debug("Sending job complete to srun for job ~p, exit=~p",
                        [JobId, ExitCode]),
            case gen_tcp:send(Socket, MessageBin) of
                ok ->
                    lager:info("Job complete notification sent to srun for job ~p", [JobId]),
                    gen_tcp:close(Socket),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
