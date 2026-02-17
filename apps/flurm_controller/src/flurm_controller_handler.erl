%%%-------------------------------------------------------------------
%%% @doc FLURM Controller RPC Handler
%%%
%%% Main dispatcher for incoming SLURM protocol messages. Delegates
%%% to domain-specific handler modules:
%%%
%%% - flurm_handler_job: Job submission, cancellation, suspension, updates
%%% - flurm_handler_node: Node info, registration, reconfiguration
%%% - flurm_handler_step: Step creation, info, prolog/epilog completion
%%% - flurm_handler_query: Job/partition/build/config info queries
%%% - flurm_handler_admin: Ping, shutdown, reservations, licenses
%%%
%%% In a clustered setup, write operations (job submission, cancellation)
%%% are forwarded to the leader if this node is not the leader. Read
%%% operations (job info, node info, partition info) can be served
%%% locally from any node.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler).


-export([handle/2]).

%% Exported helper functions used by domain handlers
-export([
    %% Type conversion helpers
    ensure_binary/1,
    error_to_binary/1,
    default_time/1,
    %% Formatting helpers
    format_allocated_nodes/1,
    format_features/1,
    format_partitions/1,
    format_node_list/1,
    format_licenses/1,
    %% Default value helpers
    default_partition/1,
    default_time_limit/1,
    default_priority/1,
    default_work_dir/1
]).

%% Exports for unit testing pure helper functions
-ifdef(TEST).
-export([
    %% Job ID parsing (delegated to flurm_handler_job)
    parse_job_id_str/1,
    safe_binary_to_integer/1,
    build_field_updates/2,
    execute_job_update/5,
    %% State conversions (delegated to query/node handlers)
    job_state_to_slurm/1,
    node_state_to_slurm/1,
    partition_state_to_slurm/1,
    step_state_to_slurm/1,
    %% Reservation helpers (delegated to admin handler)
    reservation_state_to_flags/1,
    determine_reservation_type/2,
    parse_reservation_flags/1,
    generate_reservation_name/0,
    extract_reservation_fields/1
]).
-endif.

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% API - Main Dispatcher
%%====================================================================

%% @doc Main dispatch function for handling SLURM protocol messages.
%% Takes a header and body record, returns appropriate response.
%% Delegates to domain-specific handler modules based on message type.
-spec handle(#slurm_header{}, term()) ->
    {ok, non_neg_integer(), term()} |
    {ok, non_neg_integer(), term(), map()} |
    {error, term()}.

%%--------------------------------------------------------------------
%% Admin Operations (flurm_handler_admin)
%%--------------------------------------------------------------------

%% REQUEST_PING (1008)
handle(#slurm_header{msg_type = ?REQUEST_PING} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_SHUTDOWN (1005)
handle(#slurm_header{msg_type = ?REQUEST_SHUTDOWN} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_RESERVATION_INFO (2012)
handle(#slurm_header{msg_type = ?REQUEST_RESERVATION_INFO} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_CREATE_RESERVATION (2050)
handle(#slurm_header{msg_type = ?REQUEST_CREATE_RESERVATION} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_UPDATE_RESERVATION (2052)
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_RESERVATION} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_DELETE_RESERVATION (2053)
handle(#slurm_header{msg_type = ?REQUEST_DELETE_RESERVATION} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_LICENSE_INFO (1017)
handle(#slurm_header{msg_type = ?REQUEST_LICENSE_INFO} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_TOPO_INFO (2018)
handle(#slurm_header{msg_type = ?REQUEST_TOPO_INFO} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_FRONT_END_INFO (2028)
handle(#slurm_header{msg_type = ?REQUEST_FRONT_END_INFO} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%% REQUEST_BURST_BUFFER_INFO (2020)
handle(#slurm_header{msg_type = ?REQUEST_BURST_BUFFER_INFO} = Header, Body) ->
    flurm_handler_admin:handle(Header, Body);

%%--------------------------------------------------------------------
%% Job Operations (flurm_handler_job)
%%--------------------------------------------------------------------

%% REQUEST_SUBMIT_BATCH_JOB (4003)
handle(#slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_RESOURCE_ALLOCATION (4001)
handle(#slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_JOB_ALLOCATION_INFO (4014)
handle(#slurm_header{msg_type = ?REQUEST_JOB_ALLOCATION_INFO} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_JOB_READY (4019)
handle(#slurm_header{msg_type = ?REQUEST_JOB_READY} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_KILL_TIMELIMIT (5017)
handle(#slurm_header{msg_type = ?REQUEST_KILL_TIMELIMIT} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_KILL_JOB (5032)
handle(#slurm_header{msg_type = ?REQUEST_KILL_JOB} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_CANCEL_JOB (4006)
handle(#slurm_header{msg_type = ?REQUEST_CANCEL_JOB} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_SUSPEND (5014)
handle(#slurm_header{msg_type = ?REQUEST_SUSPEND} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_SIGNAL_JOB (5018)
handle(#slurm_header{msg_type = ?REQUEST_SIGNAL_JOB} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_UPDATE_JOB (4014) - Note: same as JOB_ALLOCATION_INFO but different body
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_JOB} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%% REQUEST_JOB_WILL_RUN (4012)
handle(#slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN} = Header, Body) ->
    flurm_handler_job:handle(Header, Body);

%%--------------------------------------------------------------------
%% Node Operations (flurm_handler_node)
%%--------------------------------------------------------------------

%% REQUEST_NODE_INFO (2007)
handle(#slurm_header{msg_type = ?REQUEST_NODE_INFO} = Header, Body) ->
    flurm_handler_node:handle(Header, Body);

%% REQUEST_NODE_REGISTRATION_STATUS (1001)
handle(#slurm_header{msg_type = ?REQUEST_NODE_REGISTRATION_STATUS} = Header, Body) ->
    flurm_handler_node:handle(Header, Body);

%% REQUEST_RECONFIGURE (1003)
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE} = Header, Body) ->
    flurm_handler_node:handle(Header, Body);

%% REQUEST_RECONFIGURE_WITH_CONFIG (1004)
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE_WITH_CONFIG} = Header, Body) ->
    flurm_handler_node:handle(Header, Body);

%%--------------------------------------------------------------------
%% Step Operations (flurm_handler_step)
%%--------------------------------------------------------------------

%% REQUEST_JOB_STEP_CREATE (5001)
handle(#slurm_header{msg_type = ?REQUEST_JOB_STEP_CREATE} = Header, Body) ->
    flurm_handler_step:handle(Header, Body);

%% REQUEST_JOB_STEP_INFO (5003)
handle(#slurm_header{msg_type = ?REQUEST_JOB_STEP_INFO} = Header, Body) ->
    flurm_handler_step:handle(Header, Body);

%% REQUEST_COMPLETE_PROLOG (5019)
handle(#slurm_header{msg_type = ?REQUEST_COMPLETE_PROLOG} = Header, Body) ->
    flurm_handler_step:handle(Header, Body);

%% MESSAGE_EPILOG_COMPLETE (6012)
handle(#slurm_header{msg_type = ?MESSAGE_EPILOG_COMPLETE} = Header, Body) ->
    flurm_handler_step:handle(Header, Body);

%% MESSAGE_TASK_EXIT (6003)
handle(#slurm_header{msg_type = ?MESSAGE_TASK_EXIT} = Header, Body) ->
    flurm_handler_step:handle(Header, Body);

%%--------------------------------------------------------------------
%% Query Operations (flurm_handler_query)
%%--------------------------------------------------------------------

%% REQUEST_JOB_INFO (2003)
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_JOB_INFO_SINGLE (2005)
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO_SINGLE} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_JOB_USER_INFO (2021)
handle(#slurm_header{msg_type = ?REQUEST_JOB_USER_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_PARTITION_INFO (2009)
handle(#slurm_header{msg_type = ?REQUEST_PARTITION_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_BUILD_INFO (2001)
handle(#slurm_header{msg_type = ?REQUEST_BUILD_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_CONFIG_INFO (2016)
handle(#slurm_header{msg_type = ?REQUEST_CONFIG_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_STATS_INFO (2026)
handle(#slurm_header{msg_type = ?REQUEST_STATS_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_FED_INFO (2049)
handle(#slurm_header{msg_type = ?REQUEST_FED_INFO} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%% REQUEST_UPDATE_FEDERATION (2064)
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_FEDERATION} = Header, Body) ->
    flurm_handler_query:handle(Header, Body);

%%--------------------------------------------------------------------
%% Unknown/Unsupported Message Types
%%--------------------------------------------------------------------

handle(#slurm_header{msg_type = MsgType}, _Body) ->
    TypeName = flurm_protocol_codec:message_type_name(MsgType),
    lager:warning("Unsupported message type: ~p (~p)", [MsgType, TypeName]),
    Response = #slurm_rc_response{return_code = -1},
    {ok, ?RESPONSE_SLURM_RC, Response}.

%%====================================================================
%% Shared Helper Functions
%%====================================================================

%% @doc Ensure value is binary
-spec ensure_binary(term()) -> binary().
ensure_binary(undefined) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
ensure_binary(_) -> <<>>.

%% @doc Convert error reason to binary
-spec error_to_binary(term()) -> binary().
error_to_binary(Reason) when is_binary(Reason) -> Reason;
error_to_binary(Reason) when is_atom(Reason) -> atom_to_binary(Reason, utf8);
error_to_binary(Reason) when is_list(Reason) -> list_to_binary(Reason);
error_to_binary(Reason) ->
    list_to_binary(io_lib:format("~p", [Reason])).

%% @doc Provide default time value
-spec default_time(non_neg_integer() | undefined) -> non_neg_integer().
default_time(undefined) -> 0;
default_time(Time) -> Time.

%% @doc Format allocated nodes list to binary
-spec format_allocated_nodes([binary()]) -> binary().
format_allocated_nodes([]) -> <<>>;
format_allocated_nodes(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

%% @doc Format features list to binary
-spec format_features([binary()]) -> binary().
format_features([]) -> <<>>;
format_features(Features) ->
    iolist_to_binary(lists:join(<<",">>, Features)).

%% @doc Format partitions list to binary
-spec format_partitions([binary()]) -> binary().
format_partitions([]) -> <<>>;
format_partitions(Partitions) ->
    iolist_to_binary(lists:join(<<",">>, Partitions)).

%% @doc Format node list to binary
-spec format_node_list([binary()]) -> binary().
format_node_list([]) -> <<>>;
format_node_list(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

%% @doc Format licenses list to SLURM format (name:count,name:count)
-spec format_licenses([{binary(), non_neg_integer()}]) -> binary().
format_licenses([]) -> <<>>;
format_licenses(<<>>) -> <<>>;
format_licenses(Bin) when is_binary(Bin) -> Bin;
format_licenses(Licenses) when is_list(Licenses) ->
    Formatted = lists:map(fun({Name, Count}) ->
        iolist_to_binary([Name, <<":">>, integer_to_binary(Count)])
    end, Licenses),
    iolist_to_binary(lists:join(<<",">>, Formatted)).

%% @doc Provide default work directory if not specified
-spec default_work_dir(binary()) -> binary().
default_work_dir(<<>>) -> <<"/tmp">>;
default_work_dir(WorkDir) -> WorkDir.

%% @doc Provide default partition if not specified
-spec default_partition(binary()) -> binary().
default_partition(<<>>) -> <<"default">>;
default_partition(Partition) -> Partition.

%% @doc Provide default time limit if not specified (1 hour in seconds)
-spec default_time_limit(non_neg_integer()) -> pos_integer().
default_time_limit(0) -> 3600;
default_time_limit(TimeLimit) -> TimeLimit.

%% @doc Provide default priority if not specified.
%% NO_VAL (0xFFFFFFFE) means not specified -> use default 100.
%% 0 means hold -> pass through as 0.
-spec default_priority(non_neg_integer()) -> non_neg_integer().
default_priority(16#FFFFFFFE) -> 100;
default_priority(Priority) -> Priority.

%%====================================================================
%% Test Wrapper Functions (delegate to domain handlers)
%%====================================================================

-ifdef(TEST).

%% Job helpers - delegate to flurm_handler_job
parse_job_id_str(JobIdStr) ->
    flurm_handler_job:parse_job_id_str(JobIdStr).

safe_binary_to_integer(Bin) ->
    flurm_handler_job:safe_binary_to_integer(Bin).

build_field_updates(TimeLimit, Name) ->
    flurm_handler_job:build_field_updates(TimeLimit, Name).

execute_job_update(JobId, Priority, TimeLimit, Requeue, Name) ->
    flurm_handler_job:execute_job_update(JobId, Priority, TimeLimit, Requeue, Name).

%% State conversions - delegate to appropriate handlers
job_state_to_slurm(State) ->
    flurm_handler_query:job_state_to_slurm(State).

node_state_to_slurm(State) ->
    flurm_handler_node:node_state_to_slurm(State).

partition_state_to_slurm(State) ->
    flurm_handler_query:partition_state_to_slurm(State).

step_state_to_slurm(State) ->
    flurm_handler_step:step_state_to_slurm(State).

%% Reservation helpers - delegate to flurm_handler_admin
reservation_state_to_flags(State) ->
    flurm_handler_admin:reservation_state_to_flags(State).

determine_reservation_type(Type, Flags) ->
    flurm_handler_admin:determine_reservation_type(Type, Flags).

parse_reservation_flags(Flags) ->
    flurm_handler_admin:parse_reservation_flags(Flags).

generate_reservation_name() ->
    flurm_handler_admin:generate_reservation_name().

extract_reservation_fields(Resv) ->
    flurm_handler_admin:extract_reservation_fields(Resv).

-endif.
