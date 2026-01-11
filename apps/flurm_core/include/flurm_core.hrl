%%%-------------------------------------------------------------------
%%% @doc FLURM Core Header Definitions
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FLURM_CORE_HRL).
-define(FLURM_CORE_HRL, true).

%%====================================================================
%% Type Definitions
%%====================================================================

-type job_id() :: pos_integer().
-type user_id() :: pos_integer().
-type group_id() :: pos_integer().
-type job_state() :: pending | configuring | running | completing |
                     completed | cancelled | failed | timeout | node_fail.
-type node_state() :: up | down | drain | idle | allocated | mixed.
-type partition_state() :: up | down | drain | inactive.

%%====================================================================
%% Job State Machine Macros
%%====================================================================

%% Default timeouts (in milliseconds)
-define(PENDING_TIMEOUT, 86400000).      % 24 hours
-define(CONFIGURING_TIMEOUT, 300000).    % 5 minutes
-define(COMPLETING_TIMEOUT, 300000).     % 5 minutes

%% State machine version for hot upgrades
-define(JOB_STATE_VERSION, 1).

%% Job priority bounds
-define(MIN_PRIORITY, 0).
-define(MAX_PRIORITY, 10000).
-define(DEFAULT_PRIORITY, 100).

%%====================================================================
%% Job Record
%%====================================================================

-record(job, {
    id :: job_id(),
    name :: binary(),
    user :: binary(),
    partition :: binary(),
    state :: job_state(),
    script :: binary(),
    num_nodes :: pos_integer(),
    num_cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    time_limit :: pos_integer(),       % seconds
    priority :: non_neg_integer(),
    submit_time :: non_neg_integer(),  % unix timestamp
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    allocated_nodes :: [binary()],
    exit_code :: integer() | undefined
}).

%%====================================================================
%% Node Record
%%====================================================================

-record(node, {
    hostname :: binary(),
    cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    state :: node_state(),
    features :: [binary()],
    partitions :: [binary()],
    running_jobs :: [job_id()],
    load_avg :: float(),
    free_memory_mb :: non_neg_integer(),
    last_heartbeat :: non_neg_integer() | undefined,
    %% Resource allocation tracking: #{JobId => {CpusAllocated, MemoryAllocated}}
    allocations = #{} :: #{job_id() => {pos_integer(), pos_integer()}}
}).

%%====================================================================
%% Partition Record
%%====================================================================

-record(partition, {
    name :: binary(),
    state :: partition_state(),
    nodes :: [binary()],
    max_time :: pos_integer(),      % seconds
    default_time :: pos_integer(),  % seconds
    max_nodes :: pos_integer(),
    priority :: non_neg_integer(),
    allow_root :: boolean()
}).

%%====================================================================
%% Scheduling Types
%%====================================================================

-record(resource_request, {
    num_nodes :: pos_integer(),
    cpus_per_node :: pos_integer(),
    memory_per_node :: pos_integer(),
    features :: [binary()],
    partition :: binary()
}).

-record(allocation, {
    job_id :: job_id(),
    nodes :: [binary()],
    cpus_per_node :: pos_integer(),
    memory_per_node :: pos_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer()
}).

%%====================================================================
%% Job State Machine Data Record
%%====================================================================

%% Internal state data for flurm_job gen_statem
-record(job_data, {
    job_id          :: job_id(),
    user_id         :: user_id(),
    group_id        :: group_id(),
    partition       :: binary(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    time_limit      :: pos_integer(),      % seconds
    script          :: binary(),
    allocated_nodes = [] :: [binary()],
    submit_time     :: erlang:timestamp(),
    start_time      :: erlang:timestamp() | undefined,
    end_time        :: erlang:timestamp() | undefined,
    exit_code       :: integer() | undefined,
    priority        :: integer(),
    state_version   :: pos_integer()       % For hot upgrades
}).

%%====================================================================
%% Job Spec Record (for job submission)
%%====================================================================

-record(job_spec, {
    user_id         :: user_id(),
    group_id        :: group_id(),
    partition       :: binary(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    time_limit      :: pos_integer(),      % seconds
    script          :: binary(),
    priority        :: integer() | undefined
}).

%%====================================================================
%% Job Registry Types
%%====================================================================

-record(job_entry, {
    job_id          :: job_id(),
    pid             :: pid(),
    user_id         :: user_id(),
    state           :: job_state(),
    partition       :: binary(),
    submit_time     :: erlang:timestamp()
}).

%%====================================================================
%% Node State Machine Records
%%====================================================================

%% Node gen_server state record
-record(node_state, {
    name          :: binary(),
    hostname      :: binary(),
    port          :: inet:port_number(),
    cpus          :: pos_integer(),
    cpus_used     :: non_neg_integer(),
    memory        :: pos_integer(),      % MB
    memory_used   :: non_neg_integer(),
    gpus          :: non_neg_integer(),
    gpus_used     :: non_neg_integer(),
    state         :: up | down | drain | maint,
    features      :: [atom()],
    partitions    :: [binary()],
    jobs          :: [pos_integer()],    % Job IDs running on this node
    last_heartbeat :: erlang:timestamp(),
    version       :: pos_integer()
}).

%% Node spec for registration
-record(node_spec, {
    name          :: binary(),
    hostname      :: binary(),
    port          :: inet:port_number(),
    cpus          :: pos_integer(),
    memory        :: pos_integer(),      % MB
    gpus          :: non_neg_integer(),
    features      :: [atom()],
    partitions    :: [binary()]
}).

%% Node registry entry
-record(node_entry, {
    name          :: binary(),
    pid           :: pid(),
    hostname      :: binary(),
    state         :: up | down | drain | maint,
    partitions    :: [binary()],
    cpus_total    :: pos_integer(),
    cpus_avail    :: non_neg_integer(),
    memory_total  :: pos_integer(),
    memory_avail  :: non_neg_integer(),
    gpus_total    :: non_neg_integer(),
    gpus_avail    :: non_neg_integer()
}).

%%====================================================================
%% Partition State Machine Records
%%====================================================================

%% Partition gen_server state record
-record(partition_state, {
    name          :: binary(),
    nodes         :: [binary()],       % Node names
    max_time      :: pos_integer(),    % Max job time in seconds
    default_time  :: pos_integer(),
    max_nodes     :: pos_integer(),
    priority      :: integer(),
    state         :: up | down | drain
}).

%% Partition spec for creation
-record(partition_spec, {
    name          :: binary(),
    nodes         :: [binary()],
    max_time      :: pos_integer(),
    default_time  :: pos_integer(),
    max_nodes     :: pos_integer(),
    priority      :: integer()
}).

%%====================================================================
%% Scheduler Records
%%====================================================================

%% Scheduler state record
-record(sched_state, {
    pending_jobs   :: queue:queue(),  % Job IDs waiting
    running_jobs   :: sets:set(),     % Job IDs running
    nodes          :: ets:tid(),      % Cache of node state
    schedule_timer :: reference() | undefined
}).

%% Scheduler statistics
-record(sched_stats, {
    pending_count    :: non_neg_integer(),
    running_count    :: non_neg_integer(),
    completed_count  :: non_neg_integer(),
    failed_count     :: non_neg_integer(),
    nodes_up         :: non_neg_integer(),
    nodes_down       :: non_neg_integer(),
    schedule_cycles  :: non_neg_integer()
}).

%% Node state version for hot upgrades
-define(NODE_STATE_VERSION, 1).

%% Scheduler defaults
-define(SCHEDULE_INTERVAL, 100).  % milliseconds

-endif.
