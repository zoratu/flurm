%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Header Definitions
%%%
%%% SLURM Binary Protocol Constants and Record Definitions
%%% Based on SLURM protocol version 22.05
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FLURM_PROTOCOL_HRL).
-define(FLURM_PROTOCOL_HRL, true).

%%%===================================================================
%%% Protocol Version Constants
%%%===================================================================

%% SLURM protocol version (major.minor format as single integer)
-define(SLURM_PROTOCOL_VERSION, 22050).  % 22.05.x

%% Protocol header size (length prefix + header)
-define(SLURM_LENGTH_PREFIX_SIZE, 4).  % 4-byte length prefix
-define(SLURM_HEADER_SIZE, 12).        % 12-byte message header (version:16, flags:16, msg_index:16, msg_type:16, body_length:32)

%% Maximum message size (64 MB)
-define(SLURM_MAX_MESSAGE_SIZE, 67108864).

%% Special values
-define(SLURM_NO_VAL, 16#FFFFFFFE).     % -2 as unsigned
-define(SLURM_NO_VAL64, 16#FFFFFFFFFFFFFFFE). % -2 as 64-bit unsigned
-define(SLURM_INFINITE, 16#FFFFFFFD).   % -3 as unsigned
-define(SLURM_INFINITE64, 16#FFFFFFFFFFFFFFFD).

%%%===================================================================
%%% SLURM Message Types
%%%===================================================================

%%% Node Operations (1001-1029)
-define(REQUEST_NODE_REGISTRATION_STATUS, 1001).
-define(MESSAGE_NODE_REGISTRATION_STATUS, 1002).
-define(REQUEST_RECONFIGURE, 1003).
-define(REQUEST_RECONFIGURE_WITH_CONFIG, 1004).
-define(REQUEST_SHUTDOWN, 1005).
-define(REQUEST_SHUTDOWN_IMMEDIATE, 1006).
-define(REQUEST_TAKEOVER, 1007).
-define(REQUEST_PING, 1008).
-define(REQUEST_CONTROL, 1009).
-define(REQUEST_REGISTRATION, 1010).
-define(MESSAGE_COMPOSITE, 1011).
-define(RESPONSE_MESSAGE_COMPOSITE, 1012).
-define(REQUEST_HEALTH_CHECK, 1013).
-define(REQUEST_ACCT_GATHER_UPDATE, 1014).
-define(REQUEST_ACCT_GATHER_ENERGY, 1015).
-define(RESPONSE_ACCT_GATHER_ENERGY, 1016).
-define(REQUEST_LICENSE_INFO, 1017).
-define(RESPONSE_LICENSE_INFO, 1018).
-define(REQUEST_SET_FS_DAMPENING_FACTOR, 1019).
-define(REQUEST_NODE_ALIAS_ADDRS, 1020).
-define(RESPONSE_NODE_ALIAS_ADDRS, 1021).
-define(REQUEST_PERSIST_INIT, 1022).
-define(REQUEST_PERSIST_FINI, 1023).

%%% Information Requests (2001-2058)
-define(REQUEST_BUILD_INFO, 2001).
-define(RESPONSE_BUILD_INFO, 2002).
-define(REQUEST_JOB_INFO, 2003).
-define(RESPONSE_JOB_INFO, 2004).
-define(REQUEST_JOB_INFO_SINGLE, 2005).
-define(REQUEST_SHARE_INFO, 2006).
-define(REQUEST_NODE_INFO, 2007).
-define(RESPONSE_NODE_INFO, 2008).
-define(REQUEST_PARTITION_INFO, 2009).
-define(RESPONSE_PARTITION_INFO, 2010).
-define(RESPONSE_SHARE_INFO, 2011).
-define(REQUEST_RESERVATION_INFO, 2012).
-define(RESPONSE_RESERVATION_INFO, 2013).
-define(REQUEST_JOB_STATE, 2014).
-define(RESPONSE_JOB_STATE, 2015).
-define(REQUEST_CONFIG_INFO, 2016).
-define(RESPONSE_CONFIG_INFO, 2017).
-define(REQUEST_TOPO_INFO, 2018).
-define(RESPONSE_TOPO_INFO, 2019).
-define(REQUEST_BURST_BUFFER_INFO, 2020).
-define(RESPONSE_BURST_BUFFER_INFO, 2021).
-define(REQUEST_ASSOC_MGR_INFO, 2022).
-define(RESPONSE_ASSOC_MGR_INFO, 2023).
-define(REQUEST_FED_INFO, 2024).
-define(RESPONSE_FED_INFO, 2025).
-define(REQUEST_STATS_INFO, 2026).
-define(RESPONSE_STATS_INFO, 2027).
-define(REQUEST_FRONT_END_INFO, 2028).
-define(RESPONSE_FRONT_END_INFO, 2029).
-define(REQUEST_POWERCAP_INFO, 2030).
-define(RESPONSE_POWERCAP_INFO, 2031).

%%% Job Operations (4001-4029)
-define(REQUEST_RESOURCE_ALLOCATION, 4001).
-define(RESPONSE_RESOURCE_ALLOCATION, 4002).
-define(REQUEST_SUBMIT_BATCH_JOB, 4003).
-define(RESPONSE_SUBMIT_BATCH_JOB, 4004).
-define(REQUEST_BATCH_JOB_LAUNCH, 4005).
-define(REQUEST_CANCEL_JOB, 4006).
-define(RESPONSE_CANCEL_JOB_STEP, 4007).
-define(REQUEST_JOB_RESOURCE, 4008).
-define(RESPONSE_JOB_RESOURCE, 4009).
-define(REQUEST_JOB_ATTACH, 4010).
-define(RESPONSE_JOB_ATTACH, 4011).
-define(REQUEST_JOB_WILL_RUN, 4012).
-define(RESPONSE_JOB_WILL_RUN, 4013).
-define(REQUEST_UPDATE_JOB, 4014).
-define(REQUEST_UPDATE_JOB_TIME, 4015).
-define(REQUEST_JOB_READY, 4016).
-define(RESPONSE_JOB_READY, 4017).
-define(REQUEST_JOB_END_TIME, 4018).
-define(REQUEST_JOB_ALLOCATION_INFO, 4019).
-define(RESPONSE_JOB_ALLOCATION_INFO, 4020).
-define(REQUEST_JOB_ALLOCATION_INFO_LITE, 4021).
-define(RESPONSE_JOB_ALLOCATION_INFO_LITE, 4022).
-define(REQUEST_UPDATE_FRONT_END, 4023).
-define(REQUEST_COMPLETE_JOB_ALLOCATION, 4024).
-define(REQUEST_COMPLETE_BATCH_SCRIPT, 4025).
-define(REQUEST_JOB_STEP_PIDS, 4026).
-define(RESPONSE_JOB_STEP_PIDS, 4027).
-define(REQUEST_JOB_SBCAST_CRED, 4028).
-define(RESPONSE_JOB_SBCAST_CRED, 4029).

%%% Step Operations (5001-5041)
-define(REQUEST_JOB_STEP_CREATE, 5001).
-define(RESPONSE_JOB_STEP_CREATE, 5002).
-define(REQUEST_JOB_STEP_INFO, 5003).
-define(RESPONSE_JOB_STEP_INFO, 5004).
-define(REQUEST_STEP_COMPLETE, 5005).
-define(REQUEST_STEP_LAYOUT, 5006).
-define(RESPONSE_STEP_LAYOUT, 5007).
-define(REQUEST_LAUNCH_TASKS, 5008).
-define(RESPONSE_LAUNCH_TASKS, 5009).
-define(REQUEST_SIGNAL_TASKS, 5010).
-define(REQUEST_TERMINATE_TASKS, 5011).
-define(REQUEST_REATTACH_TASKS, 5012).
-define(RESPONSE_REATTACH_TASKS, 5013).
-define(REQUEST_SUSPEND, 5014).
-define(REQUEST_ABORT_JOB, 5015).
-define(REQUEST_KILL_PREEMPTED, 5016).
-define(REQUEST_KILL_TIMELIMIT, 5017).
-define(REQUEST_SIGNAL_JOB, 5018).
-define(REQUEST_COMPLETE_PROLOG, 5019).

%%% Authentication and Credentials (6001-6020)
-define(REQUEST_JOB_CRED, 6001).
-define(RESPONSE_JOB_CRED, 6002).
-define(CRED_SIGNATURE, 6003).
-define(REQUEST_GET_CREDENTIAL, 6004).
-define(RESPONSE_GET_CREDENTIAL, 6005).

%%% Generic Return Codes (8001-8002)
-define(RESPONSE_SLURM_RC, 8001).
-define(RESPONSE_SLURM_RC_MSG, 8002).

%%% Accounting (9001-9050)
-define(ACCOUNTING_UPDATE_MSG, 9001).
-define(ACCOUNTING_FIRST_REG, 9002).
-define(ACCOUNTING_REGISTER_CTLD, 9003).

%%%===================================================================
%%% Job States
%%%===================================================================

-define(JOB_PENDING, 0).
-define(JOB_RUNNING, 1).
-define(JOB_SUSPENDED, 2).
-define(JOB_COMPLETE, 3).
-define(JOB_CANCELLED, 4).
-define(JOB_FAILED, 5).
-define(JOB_TIMEOUT, 6).
-define(JOB_NODE_FAIL, 7).
-define(JOB_PREEMPTED, 8).
-define(JOB_BOOT_FAIL, 9).
-define(JOB_DEADLINE, 10).
-define(JOB_OOM, 11).
-define(JOB_END, 12).

%%%===================================================================
%%% Node States
%%%===================================================================

-define(NODE_STATE_UNKNOWN, 0).
-define(NODE_STATE_DOWN, 1).
-define(NODE_STATE_IDLE, 2).
-define(NODE_STATE_ALLOCATED, 3).
-define(NODE_STATE_ERROR, 4).
-define(NODE_STATE_MIXED, 5).
-define(NODE_STATE_FUTURE, 6).

%%%===================================================================
%%% Message Flags
%%%===================================================================

-define(SLURM_MSG_NO_FLAGS, 0).
-define(SLURM_MSG_REQUEST, 16#0001).
-define(SLURM_MSG_PERSIST, 16#0002).
-define(SLURM_MSG_AUTH_REQUIRED, 16#0004).
-define(SLURM_MSG_BROADCAST, 16#0008).

%%%===================================================================
%%% Record Definitions
%%%===================================================================

%% SLURM message header (12 bytes)
%% Wire format: version(2) + flags(2) + msg_index(2) + msg_type(2) + body_length(4)
%% Note: body_length in header is 32-bit to support large messages (up to 64MB)
-record(slurm_header, {
    version = ?SLURM_PROTOCOL_VERSION :: non_neg_integer(),
    flags = 0 :: non_neg_integer(),
    msg_index = 0 :: non_neg_integer(),
    msg_type = 0 :: non_neg_integer(),
    body_length = 0 :: non_neg_integer()  % 32-bit, not 16-bit
}).

%% Complete SLURM message
-record(slurm_msg, {
    header = #slurm_header{} :: #slurm_header{},
    body = <<>> :: binary() | term()
}).

%% Batch job submission request (REQUEST_SUBMIT_BATCH_JOB - 4003)
-record(batch_job_request, {
    account = <<>> :: binary(),
    acctg_freq = <<>> :: binary(),
    admin_comment = <<>> :: binary(),
    alloc_node = <<>> :: binary(),
    alloc_resp_port = 0 :: non_neg_integer(),
    alloc_sid = 0 :: non_neg_integer(),
    argc = 0 :: non_neg_integer(),
    argv = [] :: [binary()],
    array_bitmap = <<>> :: binary(),
    array_inx = <<>> :: binary(),
    batch_features = <<>> :: binary(),
    begin_time = 0 :: non_neg_integer(),
    burst_buffer = <<>> :: binary(),
    clusters = <<>> :: binary(),
    cluster_features = <<>> :: binary(),
    comment = <<>> :: binary(),
    container = <<>> :: binary(),
    contiguous = 0 :: non_neg_integer(),
    core_spec = 0 :: non_neg_integer(),
    cpu_bind = <<>> :: binary(),
    cpu_bind_type = 0 :: non_neg_integer(),
    cpu_freq_min = 0 :: non_neg_integer(),
    cpu_freq_max = 0 :: non_neg_integer(),
    cpu_freq_gov = 0 :: non_neg_integer(),
    cpus_per_task = 1 :: non_neg_integer(),
    cpus_per_tres = <<>> :: binary(),
    deadline = 0 :: non_neg_integer(),
    delay_boot = 0 :: non_neg_integer(),
    dependency = <<>> :: binary(),
    end_time = 0 :: non_neg_integer(),
    environment = [] :: [binary()],
    exc_nodes = <<>> :: binary(),
    features = <<>> :: binary(),
    group_id = 0 :: non_neg_integer(),
    immediate = 0 :: non_neg_integer(),
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    kill_on_node_fail = 1 :: non_neg_integer(),
    licenses = <<>> :: binary(),
    mail_type = 0 :: non_neg_integer(),
    mail_user = <<>> :: binary(),
    mcs_label = <<>> :: binary(),
    mem_bind = <<>> :: binary(),
    mem_bind_type = 0 :: non_neg_integer(),
    mem_per_tres = <<>> :: binary(),
    min_cpus = 1 :: non_neg_integer(),
    min_mem_per_cpu = 0 :: non_neg_integer(),
    min_mem_per_node = 0 :: non_neg_integer(),
    min_nodes = 1 :: non_neg_integer(),
    max_nodes = 1 :: non_neg_integer(),
    name = <<>> :: binary(),
    network = <<>> :: binary(),
    nice = 0 :: non_neg_integer(),
    ntasks_per_core = 0 :: non_neg_integer(),
    ntasks_per_node = 0 :: non_neg_integer(),
    ntasks_per_socket = 0 :: non_neg_integer(),
    num_tasks = 1 :: non_neg_integer(),
    open_mode = 0 :: non_neg_integer(),
    origin_cluster = <<>> :: binary(),
    overcommit = 0 :: non_neg_integer(),
    partition = <<>> :: binary(),
    plane_size = 0 :: non_neg_integer(),
    power_flags = 0 :: non_neg_integer(),
    prefer = <<>> :: binary(),
    priority = 0 :: non_neg_integer(),
    profile = 0 :: non_neg_integer(),
    qos = <<>> :: binary(),
    reboot = 0 :: non_neg_integer(),
    req_nodes = <<>> :: binary(),
    requeue = 0 :: non_neg_integer(),
    reservation = <<>> :: binary(),
    script = <<>> :: binary(),
    shared = 0 :: non_neg_integer(),
    site_factor = 0 :: non_neg_integer(),
    sockets_per_node = 0 :: non_neg_integer(),
    spank_job_env = [] :: [binary()],
    spank_job_env_size = 0 :: non_neg_integer(),
    std_err = <<>> :: binary(),
    std_in = <<>> :: binary(),
    std_out = <<>> :: binary(),
    submit_time = 0 :: non_neg_integer(),
    task_dist = 0 :: non_neg_integer(),
    threads_per_core = 0 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    time_min = 0 :: non_neg_integer(),
    tres_bind = <<>> :: binary(),
    tres_freq = <<>> :: binary(),
    tres_per_job = <<>> :: binary(),
    tres_per_node = <<>> :: binary(),
    tres_per_socket = <<>> :: binary(),
    tres_per_task = <<>> :: binary(),
    user_id = 0 :: non_neg_integer(),
    wait_all_nodes = 0 :: non_neg_integer(),
    warn_flags = 0 :: non_neg_integer(),
    warn_signal = 0 :: non_neg_integer(),
    warn_time = 0 :: non_neg_integer(),
    wckey = <<>> :: binary(),
    work_dir = <<>> :: binary(),
    x11 = 0 :: non_neg_integer(),
    x11_magic_cookie = <<>> :: binary(),
    x11_target = <<>> :: binary(),
    x11_target_port = 0 :: non_neg_integer()
}).

%% Batch job submission response (RESPONSE_SUBMIT_BATCH_JOB - 4004)
-record(batch_job_response, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer(),
    error_code = 0 :: non_neg_integer(),
    job_submit_user_msg = <<>> :: binary()
}).

%% Job info request (REQUEST_JOB_INFO - 2003)
-record(job_info_request, {
    show_flags = 0 :: non_neg_integer(),
    job_id = 0 :: non_neg_integer(),
    user_id = 0 :: non_neg_integer()
}).

%% Job info in response (part of RESPONSE_JOB_INFO - 2004)
-record(job_info, {
    account = <<>> :: binary(),
    accrue_time = 0 :: non_neg_integer(),
    admin_comment = <<>> :: binary(),
    alloc_node = <<>> :: binary(),
    alloc_sid = 0 :: non_neg_integer(),
    array_job_id = 0 :: non_neg_integer(),
    array_max_tasks = 0 :: non_neg_integer(),
    array_task_id = 0 :: non_neg_integer(),
    array_task_str = <<>> :: binary(),
    assoc_id = 0 :: non_neg_integer(),
    batch_flag = 0 :: non_neg_integer(),
    batch_host = <<>> :: binary(),
    billable_tres = 0.0 :: float(),
    bitflags = 0 :: non_neg_integer(),
    boards_per_node = 0 :: non_neg_integer(),
    burst_buffer = <<>> :: binary(),
    burst_buffer_state = <<>> :: binary(),
    cluster = <<>> :: binary(),
    cluster_features = <<>> :: binary(),
    command = <<>> :: binary(),
    comment = <<>> :: binary(),
    container = <<>> :: binary(),
    contiguous = 0 :: non_neg_integer(),
    core_spec = 0 :: non_neg_integer(),
    cores_per_socket = 0 :: non_neg_integer(),
    cpus_per_task = 1 :: non_neg_integer(),
    cpus_per_tres = <<>> :: binary(),
    deadline = 0 :: non_neg_integer(),
    delay_boot = 0 :: non_neg_integer(),
    dependency = <<>> :: binary(),
    derived_ec = 0 :: non_neg_integer(),
    eligible_time = 0 :: non_neg_integer(),
    end_time = 0 :: non_neg_integer(),
    exc_nodes = <<>> :: binary(),
    exit_code = 0 :: non_neg_integer(),
    features = <<>> :: binary(),
    group_id = 0 :: non_neg_integer(),
    group_name = <<>> :: binary(),
    het_job_id = 0 :: non_neg_integer(),
    het_job_id_set = <<>> :: binary(),
    het_job_offset = 0 :: non_neg_integer(),
    job_id = 0 :: non_neg_integer(),
    job_state = 0 :: non_neg_integer(),
    last_sched_eval = 0 :: non_neg_integer(),
    licenses = <<>> :: binary(),
    mail_type = 0 :: non_neg_integer(),
    mail_user = <<>> :: binary(),
    max_cpus = 0 :: non_neg_integer(),
    max_nodes = 0 :: non_neg_integer(),
    mcs_label = <<>> :: binary(),
    mem_per_tres = <<>> :: binary(),
    min_cpus = 0 :: non_neg_integer(),
    min_mem_per_cpu = 0 :: non_neg_integer(),
    min_mem_per_node = 0 :: non_neg_integer(),
    name = <<>> :: binary(),
    network = <<>> :: binary(),
    nice = 0 :: non_neg_integer(),
    nodes = <<>> :: binary(),
    ntasks_per_core = 0 :: non_neg_integer(),
    ntasks_per_node = 0 :: non_neg_integer(),
    ntasks_per_socket = 0 :: non_neg_integer(),
    ntasks_per_tres = 0 :: non_neg_integer(),
    num_cpus = 0 :: non_neg_integer(),
    num_nodes = 0 :: non_neg_integer(),
    num_tasks = 0 :: non_neg_integer(),
    partition = <<>> :: binary(),
    pn_min_cpus = 0 :: non_neg_integer(),
    pn_min_memory = 0 :: non_neg_integer(),
    pn_min_tmp_disk = 0 :: non_neg_integer(),
    power_flags = 0 :: non_neg_integer(),
    preempt_time = 0 :: non_neg_integer(),
    preemptable_time = 0 :: non_neg_integer(),
    pre_sus_time = 0 :: non_neg_integer(),
    priority = 0 :: non_neg_integer(),
    profile = 0 :: non_neg_integer(),
    qos = <<>> :: binary(),
    reboot = 0 :: non_neg_integer(),
    req_nodes = <<>> :: binary(),
    req_switch = 0 :: non_neg_integer(),
    requeue = 0 :: non_neg_integer(),
    resize_time = 0 :: non_neg_integer(),
    restart_cnt = 0 :: non_neg_integer(),
    resv_name = <<>> :: binary(),
    sched_nodes = <<>> :: binary(),
    shared = 0 :: non_neg_integer(),
    show_flags = 0 :: non_neg_integer(),
    site_factor = 0 :: non_neg_integer(),
    sockets_per_board = 0 :: non_neg_integer(),
    sockets_per_node = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    state_desc = <<>> :: binary(),
    state_reason = 0 :: non_neg_integer(),
    std_err = <<>> :: binary(),
    std_in = <<>> :: binary(),
    std_out = <<>> :: binary(),
    submit_time = 0 :: non_neg_integer(),
    suspend_time = 0 :: non_neg_integer(),
    system_comment = <<>> :: binary(),
    threads_per_core = 0 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    time_min = 0 :: non_neg_integer(),
    tres_alloc_str = <<>> :: binary(),
    tres_bind = <<>> :: binary(),
    tres_freq = <<>> :: binary(),
    tres_per_job = <<>> :: binary(),
    tres_per_node = <<>> :: binary(),
    tres_per_socket = <<>> :: binary(),
    tres_per_task = <<>> :: binary(),
    tres_req_str = <<>> :: binary(),
    user_id = 0 :: non_neg_integer(),
    user_name = <<>> :: binary(),
    wait4switch = 0 :: non_neg_integer(),
    wckey = <<>> :: binary(),
    work_dir = <<>> :: binary()
}).

%% Job info response (RESPONSE_JOB_INFO - 2004)
-record(job_info_response, {
    last_update = 0 :: non_neg_integer(),
    job_count = 0 :: non_neg_integer(),
    jobs = [] :: [#job_info{}]
}).

%% Node registration request (REQUEST_NODE_REGISTRATION_STATUS - 1001)
-record(node_registration_request, {
    status_only = false :: boolean()
}).

%% Node registration response (MESSAGE_NODE_REGISTRATION_STATUS - 1002)
-record(node_registration_response, {
    node_name = <<>> :: binary(),
    status = 0 :: non_neg_integer(),
    tres_fmt_str = <<>> :: binary(),
    cpus = 0 :: non_neg_integer(),
    boards = 0 :: non_neg_integer(),
    sockets = 0 :: non_neg_integer(),
    cores = 0 :: non_neg_integer(),
    threads = 0 :: non_neg_integer(),
    real_memory = 0 :: non_neg_integer(),
    tmp_disk = 0 :: non_neg_integer(),
    up_time = 0 :: non_neg_integer(),
    hash_val = 0 :: non_neg_integer(),
    cpu_spec_list = <<>> :: binary(),
    features = [] :: [binary()],
    gres = <<>> :: binary(),
    gres_drain = <<>> :: binary(),
    gres_used = <<>> :: binary()
}).

%% Ping request (REQUEST_PING - 1008)
-record(ping_request, {
}).

%% Cancel job request (REQUEST_CANCEL_JOB - 4006)
-record(cancel_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    step_id = 0 :: non_neg_integer(),
    signal = 0 :: non_neg_integer(),
    flags = 0 :: non_neg_integer()
}).

%% Generic return code response (RESPONSE_SLURM_RC - 8001)
-record(slurm_rc_response, {
    return_code = 0 :: integer()
}).

%% Node info request (REQUEST_NODE_INFO - 2007)
-record(node_info_request, {
    show_flags = 0 :: non_neg_integer(),
    node_name = <<>> :: binary()
}).

%% Node info record (part of RESPONSE_NODE_INFO - 2008)
-record(node_info, {
    name = <<>> :: binary(),
    node_hostname = <<>> :: binary(),
    node_addr = <<>> :: binary(),
    bcast_address = <<>> :: binary(),
    port = 0 :: non_neg_integer(),
    node_state = 0 :: non_neg_integer(),
    version = <<>> :: binary(),
    arch = <<>> :: binary(),
    os = <<>> :: binary(),
    boards = 0 :: non_neg_integer(),
    sockets = 0 :: non_neg_integer(),
    cores = 0 :: non_neg_integer(),
    threads = 0 :: non_neg_integer(),
    cpus = 0 :: non_neg_integer(),
    cpu_load = 0 :: non_neg_integer(),
    free_mem = 0 :: non_neg_integer(),
    real_memory = 0 :: non_neg_integer(),
    tmp_disk = 0 :: non_neg_integer(),
    weight = 0 :: non_neg_integer(),
    owner = 0 :: non_neg_integer(),
    features = <<>> :: binary(),
    features_act = <<>> :: binary(),
    gres = <<>> :: binary(),
    gres_drain = <<>> :: binary(),
    gres_used = <<>> :: binary(),
    partitions = <<>> :: binary(),
    reason = <<>> :: binary(),
    reason_time = 0 :: non_neg_integer(),
    reason_uid = 0 :: non_neg_integer(),
    boot_time = 0 :: non_neg_integer(),
    last_busy = 0 :: non_neg_integer(),
    slurmd_start_time = 0 :: non_neg_integer(),
    alloc_cpus = 0 :: non_neg_integer(),
    alloc_memory = 0 :: non_neg_integer(),
    tres_fmt_str = <<>> :: binary(),
    mcs_label = <<>> :: binary(),
    cpu_spec_list = <<>> :: binary(),
    core_spec_cnt = 0 :: non_neg_integer(),
    mem_spec_limit = 0 :: non_neg_integer()
}).

%% Node info response (RESPONSE_NODE_INFO - 2008)
-record(node_info_response, {
    last_update = 0 :: non_neg_integer(),
    node_count = 0 :: non_neg_integer(),
    nodes = [] :: [#node_info{}]
}).

%% Partition info request (REQUEST_PARTITION_INFO - 2009)
-record(partition_info_request, {
    show_flags = 0 :: non_neg_integer(),
    partition_name = <<>> :: binary()
}).

%% Partition info record (part of RESPONSE_PARTITION_INFO - 2010)
-record(partition_info, {
    name = <<>> :: binary(),
    allow_accounts = <<>> :: binary(),
    allow_alloc_nodes = <<>> :: binary(),
    allow_groups = <<>> :: binary(),
    allow_qos = <<>> :: binary(),
    alternate = <<>> :: binary(),
    billing_weights_str = <<>> :: binary(),
    def_mem_per_cpu = 0 :: non_neg_integer(),
    def_mem_per_node = 0 :: non_neg_integer(),
    default_time = 0 :: non_neg_integer(),
    deny_accounts = <<>> :: binary(),
    deny_qos = <<>> :: binary(),
    flags = 0 :: non_neg_integer(),
    grace_time = 0 :: non_neg_integer(),
    max_cpus_per_node = 0 :: non_neg_integer(),
    max_mem_per_cpu = 0 :: non_neg_integer(),
    max_mem_per_node = 0 :: non_neg_integer(),
    max_nodes = 0 :: non_neg_integer(),
    max_share = 0 :: non_neg_integer(),
    max_time = 0 :: non_neg_integer(),
    min_nodes = 0 :: non_neg_integer(),
    nodes = <<>> :: binary(),
    over_subscribe = 0 :: non_neg_integer(),
    over_time_limit = 0 :: non_neg_integer(),
    preempt_mode = 0 :: non_neg_integer(),
    priority_job_factor = 0 :: non_neg_integer(),
    priority_tier = 0 :: non_neg_integer(),
    qos_char = <<>> :: binary(),
    state_up = 0 :: non_neg_integer(),
    total_cpus = 0 :: non_neg_integer(),
    total_nodes = 0 :: non_neg_integer(),
    tres_fmt_str = <<>> :: binary()
}).

%% Partition info response (RESPONSE_PARTITION_INFO - 2010)
-record(partition_info_response, {
    last_update = 0 :: non_neg_integer(),
    partition_count = 0 :: non_neg_integer(),
    partitions = [] :: [#partition_info{}]
}).

%%%===================================================================
%%% Legacy Type Definitions (for backwards compatibility)
%%%===================================================================

%% Protocol version
-define(PROTOCOL_VERSION, 1).

%% Message types (legacy)
-type message_type() ::
    job_submit | job_cancel | job_status |
    node_register | node_heartbeat | node_status |
    partition_create | partition_update | partition_delete |
    ack | error | unknown.

%% Message structure (legacy)
-type message() :: #{
    type := message_type(),
    payload := map() | binary()
}.

%% Protocol header (6 bytes) - legacy
-define(HEADER_SIZE, 6).

%% Maximum message size (16 MB) - legacy
-define(MAX_MESSAGE_SIZE, 16777216).

%% Legacy records (for backwards compatibility)
-record(job_submit_req, {
    name :: binary(),
    script :: binary(),
    partition :: binary(),
    num_nodes :: non_neg_integer(),
    num_cpus :: non_neg_integer(),
    memory_mb :: non_neg_integer(),
    time_limit :: non_neg_integer(),  % seconds
    priority :: non_neg_integer(),
    env :: map(),
    working_dir :: binary()
}).

-record(job_status_resp, {
    job_id :: non_neg_integer(),
    state :: pending | running | completed | failed | cancelled,
    node :: binary() | undefined,
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    exit_code :: integer() | undefined
}).

-record(node_register_req, {
    hostname :: binary(),
    cpus :: non_neg_integer(),
    memory_mb :: non_neg_integer(),
    features :: [binary()],
    partitions :: [binary()]
}).

-record(node_heartbeat, {
    hostname :: binary(),
    load_avg :: float(),
    free_memory_mb :: non_neg_integer(),
    running_jobs :: [non_neg_integer()]
}).

-endif.
