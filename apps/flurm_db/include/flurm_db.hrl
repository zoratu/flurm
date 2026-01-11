%%%-------------------------------------------------------------------
%%% @doc FLURM Database Header Definitions
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FLURM_DB_HRL).
-define(FLURM_DB_HRL, true).

%% Include flurm_core for job_id() type
-include_lib("flurm_core/include/flurm_core.hrl").

%% Type definitions
-type table_name() :: jobs | nodes | partitions | atom().
-type key() :: term().
-type value() :: term().

%% Ra-specific types (job_id() comes from flurm_core.hrl)
-type node_name() :: binary().
-type partition_name() :: binary().

%% Database configuration
-define(DEFAULT_DATA_DIR, "/var/lib/flurm/db").
-define(DEFAULT_CLUSTER_NAME, flurm_cluster).
-define(RA_CLUSTER_NAME, flurm_db_ra).
-define(RA_TIMEOUT, 5000).

%%====================================================================
%% Ra Machine State Records
%%====================================================================

%% Job record for Ra state
-record(ra_job, {
    id              :: job_id(),
    name            :: binary(),
    user            :: binary(),
    group           :: binary(),
    partition       :: partition_name(),
    state           :: pending | configuring | running | completing |
                       completed | cancelled | failed | timeout | node_fail,
    script          :: binary(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    memory_mb       :: pos_integer(),
    time_limit      :: pos_integer(),       %% seconds
    priority        :: non_neg_integer(),
    submit_time     :: non_neg_integer(),   %% unix timestamp
    start_time      :: non_neg_integer() | undefined,
    end_time        :: non_neg_integer() | undefined,
    allocated_nodes :: [node_name()],
    exit_code       :: integer() | undefined
}).

%% Node record for Ra state
-record(ra_node, {
    name            :: node_name(),
    hostname        :: binary(),
    port            :: inet:port_number(),
    cpus            :: pos_integer(),
    cpus_used       :: non_neg_integer(),
    memory_mb       :: pos_integer(),
    memory_used     :: non_neg_integer(),
    gpus            :: non_neg_integer(),
    gpus_used       :: non_neg_integer(),
    state           :: up | down | drain | maint,
    features        :: [atom()],
    partitions      :: [partition_name()],
    running_jobs    :: [job_id()],
    last_heartbeat  :: non_neg_integer()    %% unix timestamp
}).

%% Partition record for Ra state
-record(ra_partition, {
    name            :: partition_name(),
    state           :: up | down | drain,
    nodes           :: [node_name()],
    max_time        :: pos_integer(),       %% seconds
    default_time    :: pos_integer(),       %% seconds
    max_nodes       :: pos_integer(),
    priority        :: integer()
}).

%% Main Ra state machine record
-record(ra_state, {
    jobs            = #{} :: #{job_id() => #ra_job{}},
    nodes           = #{} :: #{node_name() => #ra_node{}},
    partitions      = #{} :: #{partition_name() => #ra_partition{}},
    job_counter     = 1   :: pos_integer(),
    version         = 1   :: pos_integer()
}).

%%====================================================================
%% Job Spec for submission
%%====================================================================

-record(ra_job_spec, {
    name            :: binary(),
    user            :: binary(),
    group           :: binary(),
    partition       :: partition_name(),
    script          :: binary(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    memory_mb       :: pos_integer(),
    time_limit      :: pos_integer(),       %% seconds
    priority        :: non_neg_integer() | undefined
}).

%%====================================================================
%% Node Spec for registration
%%====================================================================

-record(ra_node_spec, {
    name            :: node_name(),
    hostname        :: binary(),
    port            :: inet:port_number(),
    cpus            :: pos_integer(),
    memory_mb       :: pos_integer(),
    gpus            :: non_neg_integer(),
    features        :: [atom()],
    partitions      :: [partition_name()]
}).

%%====================================================================
%% Partition Spec for creation
%%====================================================================

-record(ra_partition_spec, {
    name            :: partition_name(),
    nodes           :: [node_name()],
    max_time        :: pos_integer(),
    default_time    :: pos_integer(),
    max_nodes       :: pos_integer(),
    priority        :: integer()
}).

%%====================================================================
%% Ra Commands (replicated)
%%====================================================================

-type ra_command() ::
    {submit_job, #ra_job_spec{}} |
    {cancel_job, job_id()} |
    {update_job_state, job_id(), atom()} |
    {register_node, #ra_node_spec{}} |
    {update_node_state, node_name(), atom()} |
    {unregister_node, node_name()} |
    {create_partition, #ra_partition_spec{}} |
    {delete_partition, partition_name()}.

%%====================================================================
%% Ra Queries (local read)
%%====================================================================

-type ra_query() ::
    {get_job, job_id()} |
    {get_node, node_name()} |
    {get_partition, partition_name()} |
    list_jobs |
    list_nodes |
    list_partitions.

%%====================================================================
%% Legacy types (for backward compatibility)
%%====================================================================

%% Ra machine state (legacy - use ra_state instead)
-record(flurm_db_state, {
    tables = #{} :: #{table_name() => #{key() => value()}}
}).

%% Ra machine commands (legacy)
-type db_command() ::
    {put, table_name(), key(), value()} |
    {delete, table_name(), key()} |
    {clear, table_name()}.

%% Ra machine queries (legacy)
-type db_query() ::
    {get, table_name(), key()} |
    {list, table_name()} |
    {list_keys, table_name()}.

-endif.
