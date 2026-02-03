%%%-------------------------------------------------------------------
%%% @doc Federated Job ID Handling
%%%
%%% Implements SLURM-compatible federated job ID format where:
%%% - Bits 0-25:  Local Job ID (max 67,108,863 jobs per cluster)
%%% - Bits 26-31: Cluster Origin ID (max 64 clusters in federation)
%%%
%%% This allows jobs to be uniquely identified across a federation
%%% while preserving the origin cluster information in the ID itself.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_id).

-export([
    make_fed_job_id/2,
    get_cluster_id/1,
    get_local_id/1,
    is_federated_id/1,
    parse_job_id_string/1,
    format_job_id/1
]).

-export_type([
    fed_job_id/0,
    local_job_id/0,
    cluster_id/0
]).

%% Types
-type fed_job_id() :: non_neg_integer().
-type local_job_id() :: 0..67108863.  % 2^26 - 1
-type cluster_id() :: 0..63.          % 2^6 - 1

%% Constants
-define(CLUSTER_BITS, 6).
-define(LOCAL_BITS, 26).
-define(MAX_CLUSTER_ID, 63).        % 2^6 - 1
-define(MAX_LOCAL_ID, 67108863).    % 2^26 - 1
-define(LOCAL_ID_MASK, 16#3FFFFFF). % 26 bits set

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Create a federated job ID from cluster ID and local job ID.
%%
%% The cluster ID is stored in the upper 6 bits, local ID in lower 26 bits.
%% This matches SLURM's federation job ID format.
%%
%% Example:
%%   make_fed_job_id(1, 12345) -> 67121769  (0x04003039)
%%
-spec make_fed_job_id(ClusterId :: cluster_id(), LocalId :: local_job_id()) ->
    fed_job_id().
make_fed_job_id(ClusterId, LocalId) when
    is_integer(ClusterId), ClusterId >= 0, ClusterId =< ?MAX_CLUSTER_ID,
    is_integer(LocalId), LocalId >= 0, LocalId =< ?MAX_LOCAL_ID ->
    (ClusterId bsl ?LOCAL_BITS) bor LocalId;
make_fed_job_id(ClusterId, LocalId) ->
    error({invalid_job_id_components, ClusterId, LocalId}).

%% @doc Extract the cluster ID from a federated job ID.
%%
%% Returns the origin cluster's ID (0-63) from the upper 6 bits.
%%
-spec get_cluster_id(FedJobId :: fed_job_id()) -> cluster_id().
get_cluster_id(FedJobId) when is_integer(FedJobId), FedJobId >= 0 ->
    FedJobId bsr ?LOCAL_BITS.

%% @doc Extract the local job ID from a federated job ID.
%%
%% Returns the cluster-local job ID from the lower 26 bits.
%%
-spec get_local_id(FedJobId :: fed_job_id()) -> local_job_id().
get_local_id(FedJobId) when is_integer(FedJobId), FedJobId >= 0 ->
    FedJobId band ?LOCAL_ID_MASK.

%% @doc Check if a job ID appears to be a federated ID.
%%
%% Returns true if the cluster ID bits are non-zero, indicating
%% the job originated from a federated cluster other than cluster 0.
%%
%% Note: Cluster 0 jobs will return false even if federated.
%%
-spec is_federated_id(FedJobId :: fed_job_id()) -> boolean().
is_federated_id(FedJobId) when is_integer(FedJobId), FedJobId >= 0 ->
    get_cluster_id(FedJobId) > 0.

%% @doc Parse a job ID string, handling array job notation.
%%
%% Accepts formats:
%%   "12345"       -> {ok, 12345}
%%   "12345_0"     -> {ok, 12345} (array job, returns base ID)
%%   "12345_[0-9]" -> {ok, 12345} (array job range)
%%
-spec parse_job_id_string(binary() | string()) ->
    {ok, fed_job_id()} | {error, invalid_job_id}.
parse_job_id_string(JobIdStr) when is_binary(JobIdStr) ->
    parse_job_id_string(binary_to_list(JobIdStr));
parse_job_id_string(JobIdStr) when is_list(JobIdStr) ->
    %% Strip whitespace
    Stripped = string:trim(JobIdStr),
    %% Handle array job notation (e.g., "12345_0" or "12345_[0-9]")
    BaseStr = case string:split(Stripped, "_") of
        [Base | _] -> Base;
        _ -> Stripped
    end,
    try
        {ok, list_to_integer(BaseStr)}
    catch
        error:badarg -> {error, invalid_job_id}
    end.

%% @doc Format a federated job ID for display.
%%
%% If the job is from a non-zero cluster, includes cluster prefix.
%%
%% Example:
%%   format_job_id(12345) -> "12345"
%%   format_job_id(67121769) -> "1:12345" (cluster 1, local 12345)
%%
-spec format_job_id(FedJobId :: fed_job_id()) -> binary().
format_job_id(FedJobId) when is_integer(FedJobId), FedJobId >= 0 ->
    ClusterId = get_cluster_id(FedJobId),
    LocalId = get_local_id(FedJobId),
    case ClusterId of
        0 -> integer_to_binary(LocalId);
        _ -> iolist_to_binary([
            integer_to_binary(ClusterId),
            <<":">>,
            integer_to_binary(LocalId)
        ])
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% None currently needed
