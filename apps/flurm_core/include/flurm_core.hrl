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
-type job_state() :: pending | held | configuring | running | completing |
                     completed | cancelled | failed | timeout | node_fail | requeued.
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
    exit_code :: integer() | undefined,
    %% Output file paths
    work_dir = <<"/tmp">> :: binary(),
    std_out = <<>> :: binary(),        % Empty means slurm-<jobid>.out
    std_err = <<>> :: binary(),        % Empty means stderr goes to stdout
    %% Accounting and QOS fields
    account = <<>> :: binary(),        % Account for accounting, fairshare, and limits
    qos = <<"normal">> :: binary(),    % Quality of Service for priority and resource limits
    %% Reservation fields
    reservation = <<>> :: binary(),    % Reservation name (empty for no reservation)
    %% License fields
    licenses = [] :: [{binary(), non_neg_integer()}],  % List of {LicenseName, Count} tuples
    %% GRES (Generic Resources) - GPU, FPGA, etc.
    gres = <<>> :: binary(),           % GRES spec string (e.g., "gpu:2", "gpu:a100:4")
    gres_per_node = <<>> :: binary(),  % GRES per node requirement
    gres_per_task = <<>> :: binary(),  % GRES per task requirement
    gpu_type = <<>> :: binary(),       % GPU type constraint (e.g., "a100", "v100")
    gpu_memory_mb = 0 :: non_neg_integer(), % GPU memory requirement in MB
    gpu_exclusive = true :: boolean()  % Whether GPUs should be exclusive to job
}).

%%====================================================================
%% Node Record
%%====================================================================

-record(node, {
    hostname :: binary(),
    cpus :: pos_integer(),
    memory_mb :: pos_integer(),
    state :: node_state(),
    drain_reason :: binary() | undefined,  % Reason for drain state
    features :: [binary()],
    partitions :: [binary()],
    running_jobs :: [job_id()],
    load_avg :: float(),
    free_memory_mb :: non_neg_integer(),
    last_heartbeat :: non_neg_integer() | undefined,
    %% Resource allocation tracking: #{JobId => {CpusAllocated, MemoryAllocated}}
    allocations = #{} :: #{job_id() => {pos_integer(), pos_integer()}},
    %% GRES (Generic Resources) - GPU, FPGA, etc.
    %% gres_config: List of available GRES on this node
    %%   Each entry: #{type => gpu, name => <<"a100">>, count => 4, memory_mb => 40960}
    gres_config = [] :: [map()],
    %% gres_available: Count of available GRES by type/name
    %%   #{<<"gpu">> => 4, <<"gpu:a100">> => 4, <<"fpga">> => 2}
    gres_available = #{} :: #{binary() => non_neg_integer()},
    %% gres_total: Total GRES on node (for reporting)
    gres_total = #{} :: #{binary() => non_neg_integer()},
    %% gres_allocations: GRES allocated to jobs
    %%   #{JobId => [{Type, Count, Indices}]}
    gres_allocations = #{} :: #{job_id() => [{atom(), non_neg_integer(), [non_neg_integer()]}]}
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
    drain_reason  :: binary() | undefined,  % Reason for drain (maintenance, admin, etc.)
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
    gpus_avail    :: non_neg_integer(),
    %% Job allocations: #{JobId => {CpusAllocated, MemoryAllocated}}
    allocations = #{} :: #{pos_integer() => {pos_integer(), pos_integer()}}
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

%%====================================================================
%% Accounting Records
%%====================================================================

%% Account record - represents a SLURM account (organization unit)
-record(account, {
    name :: binary(),               % Account name (primary key)
    description = <<>> :: binary(), % Description
    organization = <<>> :: binary(),% Organization name
    parent = <<>> :: binary(),      % Parent account (for hierarchy)
    coordinators = [] :: [binary()],% List of coordinator user names
    default_qos = <<>> :: binary(), % Default QOS
    fairshare = 1 :: non_neg_integer(), % Fairshare value
    max_jobs = 0 :: non_neg_integer(),   % Max concurrent jobs (0 = unlimited)
    max_submit = 0 :: non_neg_integer(), % Max submit jobs (0 = unlimited)
    max_wall = 0 :: non_neg_integer()    % Max wall time in minutes (0 = unlimited)
}).

%% User record - represents a SLURM user for accounting
-record(acct_user, {
    name :: binary(),               % Username (primary key)
    default_account = <<>> :: binary(), % Default account
    accounts = [] :: [binary()],    % List of associated accounts
    default_qos = <<>> :: binary(), % Default QOS
    admin_level = none :: none | operator | admin, % Admin level
    fairshare = 1 :: non_neg_integer(),
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer()
}).

%% Association record - links users to accounts with specific limits
-record(association, {
    id :: pos_integer(),            % Association ID (primary key)
    cluster :: binary(),            % Cluster name
    account :: binary(),            % Account name
    user :: binary(),               % User name (empty for account-level)
    partition = <<>> :: binary(),   % Partition (empty for all)
    parent_id = 0 :: non_neg_integer(), % Parent association ID
    shares = 1 :: non_neg_integer(),    % Fairshare shares
    grp_tres_mins = #{} :: map(),       % Group TRES limits (CPU minutes, etc)
    grp_tres = #{} :: map(),            % Group TRES (concurrent)
    grp_jobs = 0 :: non_neg_integer(),  % Max concurrent jobs in group
    grp_submit = 0 :: non_neg_integer(),% Max submit in group
    grp_wall = 0 :: non_neg_integer(),  % Max wall in group
    max_tres_mins_per_job = #{} :: map(), % Per-job TRES minutes
    max_tres_per_job = #{} :: map(),    % Per-job TRES
    max_tres_per_node = #{} :: map(),   % Per-node TRES
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall_per_job = 0 :: non_neg_integer(),
    priority = 0 :: non_neg_integer(),
    qos = [] :: [binary()],         % Allowed QOS list
    default_qos = <<>> :: binary()
}).

%% QOS (Quality of Service) record
-record(qos, {
    name :: binary(),               % QOS name (primary key)
    description = <<>> :: binary(),
    priority = 0 :: integer(),      % Priority adjustment
    flags = [] :: [atom()],         % QOS flags
    grace_time = 0 :: non_neg_integer(), % Grace period in seconds
    max_jobs_pa = 0 :: non_neg_integer(), % Max jobs per account
    max_jobs_pu = 0 :: non_neg_integer(), % Max jobs per user
    max_submit_jobs_pa = 0 :: non_neg_integer(),
    max_submit_jobs_pu = 0 :: non_neg_integer(),
    max_tres_pa = #{} :: map(),     % Max TRES per account
    max_tres_pu = #{} :: map(),     % Max TRES per user
    max_tres_per_job = #{} :: map(),
    max_tres_per_node = #{} :: map(),
    max_tres_per_user = #{} :: map(),
    max_wall_per_job = 0 :: non_neg_integer(),
    min_tres_per_job = #{} :: map(),
    preempt = [] :: [binary()],     % QOS names that can be preempted
    preempt_mode = off :: off | cancel | requeue | suspend,
    usage_factor = 1.0 :: float(),  % Fairshare usage factor
    usage_threshold = 0.0 :: float()% Fairshare usage threshold
}).

%% Cluster record (for multi-cluster accounting)
-record(acct_cluster, {
    name :: binary(),               % Cluster name (primary key)
    control_host = <<>> :: binary(),% Control host
    control_port = 6817 :: non_neg_integer(),
    rpc_version = 0 :: non_neg_integer(),
    classification = 0 :: non_neg_integer(),
    tres = #{} :: map(),            % Configured TRES
    flags = [] :: [atom()]
}).

%% TRES (Trackable Resources) record
-record(tres, {
    id :: pos_integer(),            % TRES ID
    type :: binary(),               % Type (cpu, mem, gres/gpu, etc)
    name = <<>> :: binary(),        % Name (for gres types)
    count = 0 :: non_neg_integer()  % Count
}).

-endif.
