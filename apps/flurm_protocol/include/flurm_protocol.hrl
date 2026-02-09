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

%% SLURM protocol version - SLURM 22.05.x uses 0x2600
%% NOTE: This is different from theoretical (major << 8) | minor = 0x1605
%% Real SLURM 22.05.9 srun uses 0x2600, so we must match it
%% Despite being >= 0x1702, SLURM 22.05 does NOT include msg_index in header
-define(SLURM_PROTOCOL_VERSION, 16#2600).  % SLURM 22.05.x actual = 0x2600

%% Protocol header size (length prefix + header)
%% NOTE: Empirical testing shows SLURM 22.05 does NOT include msg_index despite source code
-define(SLURM_LENGTH_PREFIX_SIZE, 4).  % 4-byte length prefix
-define(SLURM_HEADER_SIZE_MIN, 16).    % Minimum header with AF_UNSPEC (16 bytes, no msg_index)
-define(SLURM_HEADER_SIZE, 16).        % 16-byte header WITHOUT msg_index, with AF_UNSPEC
                                        % Fixed fields: 14 bytes (version:16, flags:16,
                                        %  msg_type:16, body_length:32,
                                        %  forward_cnt:16, ret_cnt:16)
                                        % orig_addr: 2 bytes (AF_UNSPEC)
                                        % Total: 14 + 2 = 16 bytes

%% Maximum message size (64 MB)
-define(SLURM_MAX_MESSAGE_SIZE, 67108864).

%% Special values
-define(SLURM_NO_VAL, 16#FFFFFFFE).     % -2 as unsigned
-define(SLURM_NO_VAL64, 16#FFFFFFFFFFFFFFFE). % -2 as 64-bit unsigned
-define(SLURM_INFINITE, 16#FFFFFFFD).   % -3 as unsigned
-define(SLURM_INFINITE64, 16#FFFFFFFFFFFFFFFD).

%% Header flags
-define(SLURM_NO_AUTH_CRED, 16#0001).   % No auth credential present in message

%%%===================================================================
%%% SLURM I/O Protocol Constants
%%%===================================================================

%% I/O message types (for io_hdr_t.type field)
-define(SLURM_IO_STDIN, 0).
-define(SLURM_IO_STDOUT, 1).
-define(SLURM_IO_STDERR, 2).
-define(SLURM_IO_ALLSTDIN, 3).
-define(SLURM_IO_CONNECTION_TEST, 4).

%% I/O protocol version (0xb001 for SLURM I/O protocol)
%% This is different from SLURM_PROTOCOL_VERSION (0x2600)
-define(IO_PROTOCOL_VERSION, 16#b001).

%% I/O header size (10 bytes: type:16 + gtaskid:16 + ltaskid:16 + length:32)
-define(SLURM_IO_HDR_SIZE, 10).

%% I/O key size for authentication (64 bytes in SLURM)
-define(SLURM_IO_KEY_SIZE, 64).

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
%% Newer SLURM versions (24.x) use different message types for scontrol
-define(REQUEST_JOB_USER_INFO, 2021).     % scontrol show job uses this
%% NOTE: 2049 is REQUEST_FED_INFO in SLURM - do not use for scontrol
-define(REQUEST_SHARE_INFO, 2006).
-define(REQUEST_NODE_INFO, 2007).
-define(RESPONSE_NODE_INFO, 2008).
-define(REQUEST_PARTITION_INFO, 2009).
-define(RESPONSE_PARTITION_INFO, 2010).
-define(RESPONSE_SHARE_INFO, 2011).
-define(REQUEST_RESERVATION_INFO, 2012).
-define(RESPONSE_RESERVATION_INFO, 2013).
%% Reservation management (FLURM extensions - moved to 2060+ to avoid federation conflict)
-define(REQUEST_CREATE_RESERVATION, 2060).
-define(RESPONSE_CREATE_RESERVATION, 2061).
-define(REQUEST_UPDATE_RESERVATION, 2062).
-define(REQUEST_DELETE_RESERVATION, 2063).
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
%% Federation info - SLURM-compatible values (changed from 2024/2025 to match SLURM)
-define(REQUEST_FED_INFO, 2049).
-define(RESPONSE_FED_INFO, 2050).
%% Federation job submission (FLURM extensions - 2032-2039 range)
-define(REQUEST_FEDERATION_SUBMIT, 2032).      % Cross-cluster job submission
-define(RESPONSE_FEDERATION_SUBMIT, 2033).     % Cross-cluster job response
-define(REQUEST_FEDERATION_JOB_STATUS, 2034).  % Query job on remote cluster
-define(RESPONSE_FEDERATION_JOB_STATUS, 2035). % Remote job status
-define(REQUEST_FEDERATION_JOB_CANCEL, 2036).  % Cancel job on remote cluster
-define(RESPONSE_FEDERATION_JOB_CANCEL, 2037). % Remote cancel response
-define(REQUEST_STATS_INFO, 2026).
-define(RESPONSE_STATS_INFO, 2027).
-define(REQUEST_FRONT_END_INFO, 2028).
-define(RESPONSE_FRONT_END_INFO, 2029).
-define(REQUEST_POWERCAP_INFO, 2030).
-define(RESPONSE_POWERCAP_INFO, 2031).

%%% Job Update Operations (3001-3005)
-define(REQUEST_UPDATE_JOB, 3001).
-define(REQUEST_UPDATE_JOB_STEP, 3002).
-define(REQUEST_COMPLETE_JOB_ALLOCATION, 3003).
-define(REQUEST_COMPLETE_BATCH_SCRIPT, 3004).

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
%% SLURM 22.05 message type numbers (verified against slurm-22-05-9-1)
-define(REQUEST_JOB_ALLOCATION_INFO, 4014).  %% srun asks for allocation details
-define(RESPONSE_JOB_ALLOCATION_INFO, 4015). %% Controller returns allocation info
%% 4016-4018 are DEFUNCT_RPC in SLURM 22.05
-define(REQUEST_JOB_READY, 4019).            %% srun asks if nodes are ready
-define(RESPONSE_JOB_READY, 4020).           %% Controller says if job is ready
-define(REQUEST_JOB_END_TIME, 4021).
-define(REQUEST_JOB_NOTIFY, 4022).
-define(REQUEST_JOB_SBCAST_CRED, 4023).
-define(RESPONSE_JOB_SBCAST_CRED, 4024).
-define(REQUEST_HET_JOB_ALLOCATION, 4025).
-define(RESPONSE_HET_JOB_ALLOCATION, 4026).
-define(REQUEST_HET_JOB_ALLOC_INFO, 4027).
-define(REQUEST_SUBMIT_BATCH_HET_JOB, 4028).

%%% Step Operations (5001-5041)
-define(REQUEST_JOB_STEP_CREATE, 5001).
-define(RESPONSE_JOB_STEP_CREATE, 5002).
-define(REQUEST_JOB_STEP_INFO, 5003).
-define(RESPONSE_JOB_STEP_INFO, 5004).
-define(REQUEST_STEP_COMPLETE, 5005).
-define(REQUEST_STEP_LAYOUT, 5006).
-define(RESPONSE_STEP_LAYOUT, 5007).
%% SLURM 22.05 message types for slurmd task management (6000 range)
%% These are sent from srun to slurmd for task execution
-define(REQUEST_LAUNCH_TASKS, 6001).
-define(RESPONSE_LAUNCH_TASKS, 6002).
-define(MESSAGE_TASK_EXIT, 6003).
-define(REQUEST_SIGNAL_TASKS, 6004).
-define(REQUEST_TERMINATE_TASKS, 6006).
-define(REQUEST_REATTACH_TASKS, 6007).
-define(RESPONSE_REATTACH_TASKS, 6008).
-define(REQUEST_KILL_TIMELIMIT, 6009).
-define(REQUEST_TERMINATE_JOB, 6011).
-define(MESSAGE_EPILOG_COMPLETE, 6012).
-define(REQUEST_ABORT_JOB, 6013).

%% Additional slurmctld-specific message types (5000 range)
-define(REQUEST_SUSPEND, 5014).
-define(REQUEST_SIGNAL_JOB, 5018).
-define(REQUEST_COMPLETE_PROLOG, 5019).

%% Additional signal/kill message types (SLURM version specific)
%% scancel in SLURM 19.05+ uses 5032 for job signal requests
-define(REQUEST_KILL_JOB, 5032).

%%% Generic Return Codes (8001-8002)
-define(RESPONSE_SLURM_RC, 8001).
-define(RESPONSE_SLURM_RC_MSG, 8002).

%%% srun Callback Messages (7001-7010)
%% These are sent from slurmctld to srun (on callback or main connection)
-define(SRUN_PING, 7001).
-define(SRUN_TIMEOUT, 7002).
-define(SRUN_NODE_FAIL, 7003).
-define(SRUN_JOB_COMPLETE, 7004).
-define(SRUN_REQUEST_SUSPEND, 7005).
-define(SRUN_USER_MSG, 7006).
-define(SRUN_STEP_MISSING, 7007).
-define(SRUN_REQUEST_SWITCH_JOB, 7008).
-define(SRUN_NET_FORWARD, 7009).
-define(SRUN_STEP_SIGNAL, 7010).

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

%% SLURM message header (16 bytes on wire with AF_UNSPEC)
%% Wire format: version(2) + flags(2) + msg_type(2) + body_length(4) + forward_cnt(2) + ret_cnt(2) + orig_addr(2)
%% Note: body_length in header is 32-bit to support large messages (up to 64MB)
%% Note: msg_index is NOT used on wire (internal use only) - empirical testing confirms this
%% If forward_cnt > 0: followed by forward nodelist, timeout, tree_width, [net_cred], tree_depth
%% If ret_cnt > 0: followed by ret_list packed messages
-record(slurm_header, {
    version = ?SLURM_PROTOCOL_VERSION :: non_neg_integer(),
    flags = 0 :: non_neg_integer(),
    msg_index = 0 :: non_neg_integer(),  % Internal use only, NOT on wire
    msg_type = 0 :: non_neg_integer(),
    body_length = 0 :: non_neg_integer(),  % 32-bit, not 16-bit
    forward_cnt = 0 :: non_neg_integer(),  % Number of forward targets (usually 0)
    ret_cnt = 0 :: non_neg_integer()       % Number of return messages (usually 0)
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
    fed_origin_str = <<>> :: binary(),
    fed_siblings_active = 0 :: non_neg_integer(),
    fed_siblings_active_str = <<>> :: binary(),
    fed_siblings_viable = 0 :: non_neg_integer(),
    fed_siblings_viable_str = <<>> :: binary(),
    gres_detail_cnt = 0 :: non_neg_integer(),
    gres_detail_str = [] :: [binary()],
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

%% Kill job request (REQUEST_KILL_JOB - 5032)
%% Used by scancel in SLURM 19.05+
-record(kill_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    step_id = ?SLURM_NO_VAL :: non_neg_integer(),
    signal = 9 :: non_neg_integer(),  % Default SIGKILL
    flags = 0 :: non_neg_integer(),
    sibling = <<>> :: binary()
}).

%% Suspend job request (REQUEST_SUSPEND - 5014)
%% Used by scontrol suspend/resume
-record(suspend_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    suspend = true :: boolean()  % true = suspend, false = resume
}).

%% Signal job request (REQUEST_SIGNAL_JOB - 5018)
%% Used by scancel -s SIGNAL and scontrol signal
-record(signal_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    step_id = ?SLURM_NO_VAL :: non_neg_integer(),  % NO_VAL = all steps
    signal = 15 :: non_neg_integer(),  % Default SIGTERM
    flags = 0 :: non_neg_integer()
}).

%% Prolog complete request (REQUEST_COMPLETE_PROLOG - 5019)
%% Sent by slurmd when prolog script completes
-record(complete_prolog_request, {
    job_id = 0 :: non_neg_integer(),
    prolog_rc = 0 :: integer(),         % Prolog exit code (0 = success)
    node_name = <<>> :: binary()        % Node where prolog ran
}).

%% Epilog complete message (MESSAGE_EPILOG_COMPLETE - 6012)
%% Sent by slurmd when epilog script completes
-record(epilog_complete_msg, {
    job_id = 0 :: non_neg_integer(),
    epilog_rc = 0 :: integer(),         % Epilog exit code (0 = success)
    node_name = <<>> :: binary()        % Node where epilog ran
}).

%% Task exit message (MESSAGE_TASK_EXIT - 6003)
%% Sent by slurmd when a task (within a job step) exits
-record(task_exit_msg, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer(),
    step_het_comp = 16#FFFFFFFE :: non_neg_integer(),  % Heterogeneous component (NO_VAL)
    task_ids = [] :: [non_neg_integer()],              % List of task IDs that exited
    return_code = 0 :: integer(),                       % Exit code
    node_name = <<>> :: binary()                        % Node where task ran
}).

%% Update job request (REQUEST_UPDATE_JOB - 4014)
%% Used by scontrol update job, hold, release
-record(update_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    priority = ?SLURM_NO_VAL :: non_neg_integer(),  % 0 = hold, other = release
    time_limit = ?SLURM_NO_VAL :: non_neg_integer(),
    partition = <<>> :: binary(),
    account = <<>> :: binary(),
    comment = <<>> :: binary(),
    min_nodes = ?SLURM_NO_VAL :: non_neg_integer(),
    max_nodes = ?SLURM_NO_VAL :: non_neg_integer(),
    features = <<>> :: binary(),
    requeue = ?SLURM_NO_VAL :: non_neg_integer()  % 0/1 for requeue flag
}).

%% Job will run request (REQUEST_JOB_WILL_RUN - 4012)
%% Used by sbatch --test-only to check if job could run
-record(job_will_run_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    %% Job description fields for checking resource availability
    partition = <<>> :: binary(),
    min_nodes = 1 :: non_neg_integer(),
    max_nodes = 1 :: non_neg_integer(),
    min_cpus = 1 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    features = <<>> :: binary()
}).

%% Job will run response (RESPONSE_JOB_WILL_RUN - 4013)
-record(job_will_run_response, {
    job_id = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),  % 0 = never, unix time = estimated start
    node_list = <<>> :: binary(),
    proc_cnt = 0 :: non_neg_integer(),
    error_code = 0 :: non_neg_integer()
}).

%% Hold job request - uses REQUEST_UPDATE_JOB with priority=0
%% This is a convenience record for internal use
-record(hold_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary()
}).

%% Release job request - uses REQUEST_UPDATE_JOB to restore priority
%% This is a convenience record for internal use
-record(release_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary()
}).

%% Requeue job request (REQUEST_REQUEUE_JOB uses SIGNAL_JOB message type)
%% scontrol requeue sends a requeue signal
-record(requeue_job_request, {
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    flags = 0 :: non_neg_integer()
}).

%% Resource allocation request (REQUEST_RESOURCE_ALLOCATION - 4001)
%% Used by srun for interactive jobs
-record(resource_allocation_request, {
    account = <<>> :: binary(),
    acctg_freq = <<>> :: binary(),
    admin_comment = <<>> :: binary(),
    alloc_node = <<>> :: binary(),
    alloc_resp_port = 0 :: non_neg_integer(),
    alloc_sid = 0 :: non_neg_integer(),
    argc = 0 :: non_neg_integer(),
    argv = [] :: [binary()],
    begin_time = 0 :: non_neg_integer(),
    cluster_features = <<>> :: binary(),
    clusters = <<>> :: binary(),
    comment = <<>> :: binary(),
    contiguous = 0 :: non_neg_integer(),
    core_spec = 0 :: non_neg_integer(),
    cpu_bind = <<>> :: binary(),
    cpu_bind_type = 0 :: non_neg_integer(),
    cpus_per_task = 1 :: non_neg_integer(),
    dependency = <<>> :: binary(),
    exc_nodes = <<>> :: binary(),
    features = <<>> :: binary(),
    group_id = 0 :: non_neg_integer(),
    immediate = 0 :: non_neg_integer(),
    job_id = 0 :: non_neg_integer(),
    kill_on_node_fail = 1 :: non_neg_integer(),
    licenses = <<>> :: binary(),
    mail_type = 0 :: non_neg_integer(),
    mail_user = <<>> :: binary(),
    mem_bind = <<>> :: binary(),
    mem_bind_type = 0 :: non_neg_integer(),
    min_cpus = 1 :: non_neg_integer(),
    min_mem_per_cpu = 0 :: non_neg_integer(),
    min_mem_per_node = 0 :: non_neg_integer(),
    min_nodes = 1 :: non_neg_integer(),
    max_nodes = 1 :: non_neg_integer(),
    name = <<>> :: binary(),
    network = <<>> :: binary(),
    nice = 0 :: non_neg_integer(),
    num_tasks = 1 :: non_neg_integer(),
    partition = <<>> :: binary(),
    power_flags = 0 :: non_neg_integer(),
    priority = 0 :: non_neg_integer(),
    qos = <<>> :: binary(),
    req_nodes = <<>> :: binary(),
    reservation = <<>> :: binary(),
    shared = 0 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    user_id = 0 :: non_neg_integer(),
    wait_all_nodes = 0 :: non_neg_integer(),
    work_dir = <<>> :: binary(),
    %% Additional callback fields for srun - controller connects back to srun
    other_port = 0 :: non_neg_integer(),       % Secondary port (I/O forwarding)
    srun_pid = 0 :: non_neg_integer(),         % srun process ID
    resp_host = <<>> :: binary()               % Host where srun is running
}).

%% Resource allocation response (RESPONSE_RESOURCE_ALLOCATION - 4002)
-record(resource_allocation_response, {
    job_id = 0 :: non_neg_integer(),
    node_list = <<>> :: binary(),
    cpus_per_node = [] :: [non_neg_integer()],
    num_cpu_groups = 0 :: non_neg_integer(),
    num_nodes = 0 :: non_neg_integer(),
    partition = <<>> :: binary(),
    alias_list = <<>> :: binary(),
    error_code = 0 :: non_neg_integer(),
    job_submit_user_msg = <<>> :: binary(),
    node_cnt = 0 :: non_neg_integer(),
    select_jobinfo = <<>> :: binary(),
    %% Node addresses for srun to connect to slurmd for step creation
    %% Each entry is {IP, Port} where IP is a tuple like {172,19,0,3}
    node_addrs = [] :: [{tuple(), non_neg_integer()}],
    %% Credential fields for srun to authenticate with compute nodes
    cred = <<>> :: binary()
}).

%% Generic return code response (RESPONSE_SLURM_RC - 8001)
-record(slurm_rc_response, {
    return_code = 0 :: integer()
}).

%% srun job complete message (SRUN_JOB_COMPLETE - 7004)
%% Sent to srun to indicate job is ready/complete
-record(srun_job_complete, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer()
}).

%% srun ping message (SRUN_PING - 7001)
-record(srun_ping, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer()
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
    tres_fmt_str = <<>> :: binary(),
    node_inx = [] :: [integer()]  %% Node index ranges: [start1, end1, ..., -1]
}).

%% Partition info response (RESPONSE_PARTITION_INFO - 2010)
-record(partition_info_response, {
    last_update = 0 :: non_neg_integer(),
    partition_count = 0 :: non_neg_integer(),
    partitions = [] :: [#partition_info{}]
}).

%% Job step create request (REQUEST_JOB_STEP_CREATE - 5001)
-record(job_step_create_request, {
    job_id = 0 :: non_neg_integer(),
    step_id = ?SLURM_NO_VAL :: non_neg_integer(),  % NO_VAL for auto-assign
    name = <<>> :: binary(),
    min_nodes = 1 :: non_neg_integer(),
    max_nodes = 1 :: non_neg_integer(),
    num_tasks = 1 :: non_neg_integer(),
    cpus_per_task = 1 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    flags = 0 :: non_neg_integer(),
    immediate = 0 :: non_neg_integer(),
    host = <<>> :: binary(),
    node_list = <<>> :: binary(),
    exc_nodes = <<>> :: binary(),
    features = <<>> :: binary(),
    cpu_freq_min = 0 :: non_neg_integer(),
    cpu_freq_max = 0 :: non_neg_integer(),
    cpu_freq_gov = 0 :: non_neg_integer(),
    relative = 0 :: non_neg_integer(),
    resv_port_cnt = 0 :: non_neg_integer(),
    user_id = 0 :: non_neg_integer(),
    plane_size = 0 :: non_neg_integer(),
    tres_bind = <<>> :: binary(),
    tres_freq = <<>> :: binary(),
    tres_per_step = <<>> :: binary(),
    tres_per_node = <<>> :: binary(),
    tres_per_socket = <<>> :: binary(),
    tres_per_task = <<>> :: binary(),
    mem_per_tres = <<>> :: binary(),
    submit_line = <<>> :: binary()
}).

%% Job step create response (RESPONSE_JOB_STEP_CREATE - 5002)
-record(job_step_create_response, {
    job_step_id = 0 :: non_neg_integer(),  % The assigned step ID
    step_layout = <<>> :: binary(),        % Step layout info
    cred = <<>> :: binary(),               % Job credential
    switch_job = <<>> :: binary(),         % Switch plugin info
    error_code = 0 :: non_neg_integer(),
    error_msg = <<>> :: binary(),
    %% Additional fields for credential generation
    job_id = 0 :: non_neg_integer(),
    user_id = 0 :: non_neg_integer(),
    group_id = 0 :: non_neg_integer(),
    user_name = <<>> :: binary(),
    node_list = <<>> :: binary(),
    num_tasks = 1 :: non_neg_integer(),
    partition = <<>> :: binary()
}).

%% Job step info request (REQUEST_JOB_STEP_INFO - 5003)
-record(job_step_info_request, {
    show_flags = 0 :: non_neg_integer(),
    job_id = 0 :: non_neg_integer(),       % NO_VAL for all jobs
    step_id = ?SLURM_NO_VAL :: non_neg_integer()  % NO_VAL for all steps
}).

%% Job step info record (part of RESPONSE_JOB_STEP_INFO - 5004)
-record(job_step_info, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer(),
    step_name = <<>> :: binary(),
    partition = <<>> :: binary(),
    user_id = 0 :: non_neg_integer(),
    user_name = <<>> :: binary(),
    state = 0 :: non_neg_integer(),
    num_tasks = 0 :: non_neg_integer(),
    num_cpus = 0 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    run_time = 0 :: non_neg_integer(),
    nodes = <<>> :: binary(),
    node_cnt = 0 :: non_neg_integer(),
    tres_alloc_str = <<>> :: binary(),
    exit_code = 0 :: non_neg_integer()
}).

%% Job step info response (RESPONSE_JOB_STEP_INFO - 5004)
-record(job_step_info_response, {
    last_update = 0 :: non_neg_integer(),
    step_count = 0 :: non_neg_integer(),
    steps = [] :: [#job_step_info{}]
}).

%% Launch tasks request (REQUEST_LAUNCH_TASKS - 6001)
%% Sent by srun to slurmd to launch tasks on allocated nodes
-record(launch_tasks_request, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer(),
    step_het_comp = 0 :: non_neg_integer(),
    uid = 0 :: non_neg_integer(),
    gid = 0 :: non_neg_integer(),
    user_name = <<>> :: binary(),
    gids = [] :: [non_neg_integer()],
    ntasks = 0 :: non_neg_integer(),
    nnodes = 0 :: non_neg_integer(),
    argc = 0 :: non_neg_integer(),
    argv = [] :: [binary()],
    envc = 0 :: non_neg_integer(),
    env = [] :: [binary()],
    cwd = <<>> :: binary(),
    cpu_bind_type = 0 :: non_neg_integer(),
    cpu_bind = <<>> :: binary(),
    task_dist = 0 :: non_neg_integer(),
    flags = 0 :: non_neg_integer(),
    tasks_to_launch = [] :: [non_neg_integer()],
    global_task_ids = [] :: [[non_neg_integer()]],
    resp_port = [] :: [non_neg_integer()],
    io_port = [] :: [non_neg_integer()],
    ofname = <<>> :: binary(),
    efname = <<>> :: binary(),
    ifname = <<>> :: binary(),
    complete_nodelist = <<>> :: binary(),
    partition = <<>> :: binary(),
    job_mem_lim = 0 :: non_neg_integer(),
    step_mem_lim = 0 :: non_neg_integer(),
    cred = undefined :: term(),
    io_key = <<>> :: binary()  % Credential signature for I/O authentication
}).

%% Launch tasks response (RESPONSE_LAUNCH_TASKS - 6002)
%% Sent by slurmd to srun after launching tasks
%% Format: step_id(job_id:32 + step_id:32 + step_het_comp:32), return_code(32),
%%         node_name(packstr), count_of_pids(32),
%%         local_pids(32 * count), task_ids/gtids(32 * count)
-record(launch_tasks_response, {
    job_id = 0 :: non_neg_integer(),
    step_id = 0 :: non_neg_integer(),
    step_het_comp = 0 :: non_neg_integer(),
    return_code = 0 :: integer(),
    node_name = <<>> :: binary(),
    count_of_pids = 0 :: non_neg_integer(),
    local_pids = [] :: [non_neg_integer()],
    gtids = [] :: [non_neg_integer()]
}).

%% Reattach tasks response (RESPONSE_REATTACH_TASKS - 5013)
%% Sent by slurmd when srun reattaches to running tasks
-record(reattach_tasks_response, {
    return_code = 0 :: integer(),
    node_name = <<>> :: binary(),
    count_of_pids = 0 :: non_neg_integer(),
    local_pids = [] :: [non_neg_integer()],
    gtids = [] :: [non_neg_integer()],
    executable_names = [] :: [binary()]
}).

%% Reservation info request (REQUEST_RESERVATION_INFO - 2012)
-record(reservation_info_request, {
    show_flags = 0 :: non_neg_integer(),
    reservation_name = <<>> :: binary()
}).

%% Reservation info record (part of RESPONSE_RESERVATION_INFO - 2013)
-record(reservation_info, {
    name = <<>> :: binary(),
    accounts = <<>> :: binary(),
    burst_buffer = <<>> :: binary(),
    core_cnt = 0 :: non_neg_integer(),
    core_spec_cnt = 0 :: non_neg_integer(),
    end_time = 0 :: non_neg_integer(),
    features = <<>> :: binary(),
    flags = 0 :: non_neg_integer(),
    groups = <<>> :: binary(),
    licenses = <<>> :: binary(),
    max_start_delay = 0 :: non_neg_integer(),
    node_cnt = 0 :: non_neg_integer(),
    node_list = <<>> :: binary(),
    partition = <<>> :: binary(),
    purge_comp_time = 0 :: non_neg_integer(),
    resv_watts = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    tres_str = <<>> :: binary(),
    users = <<>> :: binary()
}).

%% Reservation info response (RESPONSE_RESERVATION_INFO - 2013)
-record(reservation_info_response, {
    last_update = 0 :: non_neg_integer(),
    reservation_count = 0 :: non_neg_integer(),
    reservations = [] :: [#reservation_info{}]
}).

%% Create reservation request (REQUEST_CREATE_RESERVATION - 2050)
%% Used by scontrol create reservation
-record(create_reservation_request, {
    name = <<>> :: binary(),
    start_time = 0 :: non_neg_integer(),       % Unix timestamp
    end_time = 0 :: non_neg_integer(),         % Unix timestamp (if 0, use duration)
    duration = 0 :: non_neg_integer(),         % Duration in minutes
    nodes = <<>> :: binary(),                   % Node list (e.g., "node[01-10]")
    node_cnt = 0 :: non_neg_integer(),         % Number of nodes (alternative to node list)
    partition = <<>> :: binary(),
    users = <<>> :: binary(),                   % Comma-separated user list
    accounts = <<>> :: binary(),                % Comma-separated account list
    groups = <<>> :: binary(),                  % Comma-separated group list
    licenses = <<>> :: binary(),                % License spec (e.g., "matlab:5")
    features = <<>> :: binary(),                % Feature requirements
    flags = 0 :: non_neg_integer(),            % Reservation flags (MAINT, FLEX, etc.)
    type = <<>> :: binary()                     % Reservation type: maint, flex, user
}).

%% Create reservation response (RESPONSE_CREATE_RESERVATION - 2051)
-record(create_reservation_response, {
    name = <<>> :: binary(),                    % Reserved name (may be generated)
    error_code = 0 :: non_neg_integer(),
    error_msg = <<>> :: binary()
}).

%% Update reservation request (REQUEST_UPDATE_RESERVATION - 2052)
-record(update_reservation_request, {
    name = <<>> :: binary(),                    % Name of reservation to update
    start_time = 0 :: non_neg_integer(),       % 0 = no change
    end_time = 0 :: non_neg_integer(),
    duration = 0 :: non_neg_integer(),
    nodes = <<>> :: binary(),
    node_cnt = 0 :: non_neg_integer(),
    partition = <<>> :: binary(),
    users = <<>> :: binary(),
    accounts = <<>> :: binary(),
    groups = <<>> :: binary(),
    licenses = <<>> :: binary(),
    features = <<>> :: binary(),
    flags = 0 :: non_neg_integer()
}).

%% Delete reservation request (REQUEST_DELETE_RESERVATION - 2053)
-record(delete_reservation_request, {
    name = <<>> :: binary()                     % Name of reservation to delete
}).

%% License info request (REQUEST_LICENSE_INFO - 1017)
-record(license_info_request, {
    show_flags = 0 :: non_neg_integer()
}).

%% License info record (part of RESPONSE_LICENSE_INFO - 1018)
-record(license_info, {
    name = <<>> :: binary(),
    total = 0 :: non_neg_integer(),
    in_use = 0 :: non_neg_integer(),
    available = 0 :: non_neg_integer(),
    reserved = 0 :: non_neg_integer(),
    remote = 0 :: non_neg_integer()  % 0 = local, 1 = remote
}).

%% License info response (RESPONSE_LICENSE_INFO - 1018)
-record(license_info_response, {
    last_update = 0 :: non_neg_integer(),
    license_count = 0 :: non_neg_integer(),
    licenses = [] :: [#license_info{}]
}).

%% Topology info request (REQUEST_TOPO_INFO - 2018)
-record(topo_info_request, {
    show_flags = 0 :: non_neg_integer()
}).

%% Topology info record (part of RESPONSE_TOPO_INFO - 2019)
-record(topo_info, {
    level = 0 :: non_neg_integer(),
    link_speed = 0 :: non_neg_integer(),
    name = <<>> :: binary(),
    nodes = <<>> :: binary(),
    switches = <<>> :: binary()
}).

%% Topology info response (RESPONSE_TOPO_INFO - 2019)
-record(topo_info_response, {
    topo_count = 0 :: non_neg_integer(),
    topos = [] :: [#topo_info{}]
}).

%% Front-end info request (REQUEST_FRONT_END_INFO - 2028)
-record(front_end_info_request, {
    show_flags = 0 :: non_neg_integer()
}).

%% Front-end info record (part of RESPONSE_FRONT_END_INFO - 2029)
-record(front_end_info, {
    allow_groups = <<>> :: binary(),
    allow_users = <<>> :: binary(),
    boot_time = 0 :: non_neg_integer(),
    deny_groups = <<>> :: binary(),
    deny_users = <<>> :: binary(),
    name = <<>> :: binary(),
    node_state = 0 :: non_neg_integer(),
    reason = <<>> :: binary(),
    reason_time = 0 :: non_neg_integer(),
    reason_uid = 0 :: non_neg_integer(),
    slurmd_start_time = 0 :: non_neg_integer(),
    version = <<>> :: binary()
}).

%% Front-end info response (RESPONSE_FRONT_END_INFO - 2029)
-record(front_end_info_response, {
    last_update = 0 :: non_neg_integer(),
    front_end_count = 0 :: non_neg_integer(),
    front_ends = [] :: [#front_end_info{}]
}).

%% Burst buffer info request (REQUEST_BURST_BUFFER_INFO - 2020)
-record(burst_buffer_info_request, {
    show_flags = 0 :: non_neg_integer()
}).

%% Burst buffer pool info record
-record(burst_buffer_pool, {
    name = <<>> :: binary(),
    total_space = 0 :: non_neg_integer(),
    granularity = 0 :: non_neg_integer(),
    unfree_space = 0 :: non_neg_integer(),
    used_space = 0 :: non_neg_integer()
}).

%% Burst buffer info record (part of RESPONSE_BURST_BUFFER_INFO - 2021)
-record(burst_buffer_info, {
    name = <<>> :: binary(),
    default_pool = <<>> :: binary(),
    allow_users = <<>> :: binary(),
    create_buffer = <<>> :: binary(),
    deny_users = <<>> :: binary(),
    destroy_buffer = <<>> :: binary(),
    flags = 0 :: non_neg_integer(),
    get_sys_state = <<>> :: binary(),
    get_sys_status = <<>> :: binary(),
    granularity = 0 :: non_neg_integer(),
    pool_cnt = 0 :: non_neg_integer(),
    pools = [] :: [#burst_buffer_pool{}],
    other_timeout = 0 :: non_neg_integer(),
    stage_in_timeout = 0 :: non_neg_integer(),
    stage_out_timeout = 0 :: non_neg_integer(),
    start_stage_in = <<>> :: binary(),
    start_stage_out = <<>> :: binary(),
    stop_stage_in = <<>> :: binary(),
    stop_stage_out = <<>> :: binary(),
    total_space = 0 :: non_neg_integer(),
    unfree_space = 0 :: non_neg_integer(),
    used_space = 0 :: non_neg_integer(),
    validate_timeout = 0 :: non_neg_integer()
}).

%% Burst buffer info response (RESPONSE_BURST_BUFFER_INFO - 2021)
-record(burst_buffer_info_response, {
    last_update = 0 :: non_neg_integer(),
    burst_buffer_count = 0 :: non_neg_integer(),
    burst_buffers = [] :: [#burst_buffer_info{}]
}).

%% Build info response (RESPONSE_BUILD_INFO - 2002)
%% Returns SLURM/FLURM version and build configuration
-record(build_info_response, {
    version = <<>> :: binary(),           %% e.g., "22.05.0"
    version_major = 0 :: non_neg_integer(),
    version_minor = 0 :: non_neg_integer(),
    version_micro = 0 :: non_neg_integer(),
    release = <<>> :: binary(),           %% e.g., "flurm-0.1.0"
    build_host = <<>> :: binary(),
    build_user = <<>> :: binary(),
    build_date = <<>> :: binary(),
    cluster_name = <<>> :: binary(),
    control_machine = <<>> :: binary(),
    backup_controller = <<>> :: binary(),
    accounting_storage_type = <<>> :: binary(),
    auth_type = <<>> :: binary(),
    slurm_user_name = <<>> :: binary(),
    slurmd_user_name = <<>> :: binary(),
    slurmctld_host = <<>> :: binary(),
    slurmctld_port = 6817 :: non_neg_integer(),
    slurmd_port = 6818 :: non_neg_integer(),
    spool_dir = <<>> :: binary(),
    state_save_location = <<>> :: binary(),
    plugin_dir = <<>> :: binary(),
    priority_type = <<>> :: binary(),
    select_type = <<>> :: binary(),
    scheduler_type = <<>> :: binary(),
    job_comp_type = <<>> :: binary()
}).

%% Config info response (RESPONSE_CONFIG_INFO - 2017)
%% Returns current configuration as key-value pairs
-record(config_info_response, {
    last_update = 0 :: non_neg_integer(),
    config = #{} :: map()
}).

%% Stats info request (REQUEST_STATS_INFO - 2026)
-record(stats_info_request, {
    command = 0 :: non_neg_integer()  %% 0=query, 1=reset
}).

%% Stats info response (RESPONSE_STATS_INFO - 2027)
%% Returns scheduler and controller statistics (sdiag output)
-record(stats_info_response, {
    parts_packed = 0 :: non_neg_integer(),
    req_time = 0 :: non_neg_integer(),
    req_time_start = 0 :: non_neg_integer(),
    server_thread_count = 0 :: non_neg_integer(),
    agent_queue_size = 0 :: non_neg_integer(),
    agent_count = 0 :: non_neg_integer(),
    agent_thread_count = 0 :: non_neg_integer(),
    dbd_agent_queue_size = 0 :: non_neg_integer(),
    jobs_submitted = 0 :: non_neg_integer(),
    jobs_started = 0 :: non_neg_integer(),
    jobs_completed = 0 :: non_neg_integer(),
    jobs_canceled = 0 :: non_neg_integer(),
    jobs_failed = 0 :: non_neg_integer(),
    jobs_pending = 0 :: non_neg_integer(),
    jobs_running = 0 :: non_neg_integer(),
    schedule_cycle_max = 0 :: non_neg_integer(),
    schedule_cycle_last = 0 :: non_neg_integer(),
    schedule_cycle_sum = 0 :: non_neg_integer(),
    schedule_cycle_counter = 0 :: non_neg_integer(),
    schedule_cycle_depth = 0 :: non_neg_integer(),
    schedule_queue_len = 0 :: non_neg_integer(),
    bf_backfilled_jobs = 0 :: non_neg_integer(),
    bf_last_backfilled_jobs = 0 :: non_neg_integer(),
    bf_cycle_counter = 0 :: non_neg_integer(),
    bf_cycle_sum = 0 :: non_neg_integer(),
    bf_cycle_last = 0 :: non_neg_integer(),
    bf_cycle_max = 0 :: non_neg_integer(),
    bf_depth_sum = 0 :: non_neg_integer(),
    bf_depth_try_sum = 0 :: non_neg_integer(),
    bf_queue_len = 0 :: non_neg_integer(),
    bf_queue_len_sum = 0 :: non_neg_integer(),
    bf_when_last_cycle = 0 :: non_neg_integer(),
    bf_active = false :: boolean(),
    rpc_type_stats = [] :: list(),
    rpc_user_stats = [] :: list()
}).

%% Reconfigure request (REQUEST_RECONFIGURE - 1003)
%% Used by scontrol reconfigure for full configuration reload
-record(reconfigure_request, {
    flags = 0 :: non_neg_integer()  %% Reserved for future use
}).

%% Reconfigure with config request (REQUEST_RECONFIGURE_WITH_CONFIG - 1004)
%% Used for partial reconfiguration with specific settings
-record(reconfigure_with_config_request, {
    config_file = <<>> :: binary(),           %% Optional config file path
    settings = #{} :: map(),                  %% Key-value settings to apply
    force = false :: boolean(),               %% Force apply even if validation warnings
    notify_nodes = true :: boolean()          %% Whether to notify compute nodes
}).

%% Reconfigure response (shared for both 1003 and 1004)
-record(reconfigure_response, {
    return_code = 0 :: integer(),             %% 0 = success, negative = error
    message = <<>> :: binary(),               %% Human-readable status message
    changed_keys = [] :: [atom()],            %% List of keys that were changed
    version = 0 :: non_neg_integer()          %% New config version number
}).

%%%===================================================================
%%% Federation Message Records
%%%===================================================================

%% Federation info request (REQUEST_FED_INFO - 2049, SLURM-compatible)
-record(fed_info_request, {
    show_flags = 0 :: non_neg_integer()
}).

%% Federation cluster info (part of RESPONSE_FED_INFO)
-record(fed_cluster_info, {
    name = <<>> :: binary(),
    host = <<>> :: binary(),
    port = 6817 :: non_neg_integer(),
    state = <<>> :: binary(),           %% "up", "down", "drain"
    weight = 1 :: non_neg_integer(),
    features = [] :: [binary()],
    partitions = [] :: [binary()],
    fed_id = 0 :: 0..63,                %% Cluster's federation ID (for job ID encoding)
    fed_state = 0 :: non_neg_integer()  %% Federation state flags
}).

%% Federation info response (RESPONSE_FED_INFO - 2050, SLURM-compatible)
-record(fed_info_response, {
    federation_name = <<>> :: binary(),
    local_cluster = <<>> :: binary(),
    cluster_count = 0 :: non_neg_integer(),
    clusters = [] :: [#fed_cluster_info{}]
}).

%% Federation job submit request (REQUEST_FEDERATION_SUBMIT - 2032)
%% Used for cross-cluster job submission
-record(federation_submit_request, {
    source_cluster = <<>> :: binary(),     %% Originating cluster name
    target_cluster = <<>> :: binary(),     %% Target cluster name
    job_id = 0 :: non_neg_integer(),       %% Source cluster's tracking ID (0 = new)
    name = <<>> :: binary(),
    script = <<>> :: binary(),
    partition = <<>> :: binary(),
    num_cpus = 1 :: non_neg_integer(),
    num_nodes = 1 :: non_neg_integer(),
    memory_mb = 0 :: non_neg_integer(),
    time_limit = 0 :: non_neg_integer(),
    user_id = 0 :: non_neg_integer(),
    group_id = 0 :: non_neg_integer(),
    priority = 0 :: non_neg_integer(),
    work_dir = <<>> :: binary(),
    std_out = <<>> :: binary(),
    std_err = <<>> :: binary(),
    environment = [] :: [binary()],
    features = <<>> :: binary()
}).

%% Federation job submit response (RESPONSE_FEDERATION_SUBMIT - 2033)
-record(federation_submit_response, {
    source_cluster = <<>> :: binary(),     %% Original source cluster
    job_id = 0 :: non_neg_integer(),       %% Assigned job ID on target cluster
    error_code = 0 :: non_neg_integer(),
    error_msg = <<>> :: binary()
}).

%% Federation job status request (REQUEST_FEDERATION_JOB_STATUS - 2034)
-record(federation_job_status_request, {
    source_cluster = <<>> :: binary(),     %% Requesting cluster
    job_id = 0 :: non_neg_integer(),       %% Job ID on target cluster
    job_id_str = <<>> :: binary()
}).

%% Federation job status response (RESPONSE_FEDERATION_JOB_STATUS - 2035)
-record(federation_job_status_response, {
    job_id = 0 :: non_neg_integer(),
    job_state = 0 :: non_neg_integer(),
    state_reason = 0 :: non_neg_integer(),
    exit_code = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    end_time = 0 :: non_neg_integer(),
    nodes = <<>> :: binary(),
    error_code = 0 :: non_neg_integer()
}).

%% Federation job cancel request (REQUEST_FEDERATION_JOB_CANCEL - 2036)
-record(federation_job_cancel_request, {
    source_cluster = <<>> :: binary(),
    job_id = 0 :: non_neg_integer(),
    job_id_str = <<>> :: binary(),
    signal = 9 :: non_neg_integer()        %% Default SIGKILL
}).

%% Federation job cancel response (RESPONSE_FEDERATION_JOB_CANCEL - 2037)
-record(federation_job_cancel_response, {
    job_id = 0 :: non_neg_integer(),
    error_code = 0 :: non_neg_integer(),
    error_msg = <<>> :: binary()
}).

%% Federation update request (REQUEST_UPDATE_FEDERATION - 2064)
%% Used by scontrol update federation for dynamic cluster management
-define(REQUEST_UPDATE_FEDERATION, 2064).
-define(RESPONSE_UPDATE_FEDERATION, 2065).

-record(update_federation_request, {
    action :: add_cluster | remove_cluster | update_settings,
    cluster_name = <<>> :: binary(),
    host = <<>> :: binary(),
    port = 6817 :: pos_integer(),
    settings = #{} :: map()
}).

-record(update_federation_response, {
    error_code = 0 :: non_neg_integer(),
    error_msg = <<>> :: binary()
}).

%%%===================================================================
%%% Sibling Job Coordination Message Types (Phase 7D)
%%% TLA+ Safety Invariants:
%%%   - SiblingExclusivity: At most one sibling runs at any time
%%%   - OriginAwareness: Origin cluster tracks which sibling is running
%%%   - NoJobLoss: Jobs must have at least one active sibling or be terminal
%%%===================================================================

%% Message type constants for sibling job coordination
-define(MSG_FED_JOB_SUBMIT, 2070).       % Origin -> All: Create sibling
-define(MSG_FED_JOB_STARTED, 2071).      % Running -> Origin: Notify job started
-define(MSG_FED_SIBLING_REVOKE, 2072).   % Origin -> All: Cancel other siblings
-define(MSG_FED_JOB_COMPLETED, 2073).    % Running -> Origin: Job finished
-define(MSG_FED_JOB_FAILED, 2074).       % Running -> Origin: Job failed

%% Sibling job states (mirrors TLA+ SiblingStates)
-define(SIBLING_STATE_NULL, 0).          % No sibling exists
-define(SIBLING_STATE_PENDING, 1).       % Sibling queued, waiting for resources
-define(SIBLING_STATE_RUNNING, 2).       % Sibling is executing
-define(SIBLING_STATE_REVOKED, 3).       % Sibling cancelled because another cluster started
-define(SIBLING_STATE_COMPLETED, 4).     % Sibling completed successfully
-define(SIBLING_STATE_FAILED, 5).        % Sibling failed

%% Sibling job state record - tracks per-sibling state
-record(sibling_job_state, {
    federation_job_id :: binary(),           % Federation-wide job ID
    sibling_cluster :: binary(),             % Cluster where this sibling exists
    origin_cluster :: binary(),              % Cluster that submitted the original job
    local_job_id = 0 :: non_neg_integer(),   % Job ID on the sibling cluster
    state = ?SIBLING_STATE_NULL :: non_neg_integer(),
    submit_time = 0 :: non_neg_integer(),    % Unix timestamp
    start_time = 0 :: non_neg_integer(),     % Unix timestamp when started (or 0)
    end_time = 0 :: non_neg_integer(),       % Unix timestamp when ended (or 0)
    exit_code = 0 :: integer()               % Exit code if completed/failed
}).

%% Federation job tracker - tracks the overall federated job state at origin
-record(fed_job_tracker, {
    federation_job_id :: binary(),           % Federation-wide unique ID
    origin_cluster :: binary(),              % Cluster where job was submitted
    origin_job_id :: non_neg_integer(),      % Original job ID on origin cluster
    running_cluster = undefined :: binary() | undefined, % Which cluster is running (nil = none)
    sibling_states :: #{binary() => #sibling_job_state{}}, % Cluster -> sibling state
    submit_time = 0 :: non_neg_integer(),    % When job was submitted
    job_spec :: map()                        % Original job specification
}).

%% MSG_FED_JOB_SUBMIT (2070) - Origin -> All: Create sibling job
-record(fed_job_submit_msg, {
    federation_job_id :: binary(),           % Federation-wide job ID
    origin_cluster :: binary(),              % Originating cluster
    target_cluster :: binary(),              % Target cluster for this sibling
    job_spec :: map(),                       % Job specification
    submit_time = 0 :: non_neg_integer()
}).

%% MSG_FED_JOB_STARTED (2071) - Running -> Origin: Job started notification
-record(fed_job_started_msg, {
    federation_job_id :: binary(),           % Federation-wide job ID
    running_cluster :: binary(),             % Cluster that started the job
    local_job_id = 0 :: non_neg_integer(),   % Job ID on running cluster
    start_time = 0 :: non_neg_integer()
}).

%% MSG_FED_SIBLING_REVOKE (2072) - Origin -> All: Revoke/cancel siblings
-record(fed_sibling_revoke_msg, {
    federation_job_id :: binary(),           % Federation-wide job ID
    running_cluster :: binary(),             % Cluster that is running (do not revoke)
    revoke_reason = <<"sibling_started">> :: binary()
}).

%% MSG_FED_JOB_COMPLETED (2073) - Running -> Origin: Job completed
-record(fed_job_completed_msg, {
    federation_job_id :: binary(),           % Federation-wide job ID
    running_cluster :: binary(),             % Cluster that completed the job
    local_job_id = 0 :: non_neg_integer(),   % Job ID on running cluster
    end_time = 0 :: non_neg_integer(),
    exit_code = 0 :: integer()
}).

%% MSG_FED_JOB_FAILED (2074) - Running -> Origin: Job failed
-record(fed_job_failed_msg, {
    federation_job_id :: binary(),           % Federation-wide job ID
    running_cluster :: binary(),             % Cluster where job failed
    local_job_id = 0 :: non_neg_integer(),   % Job ID on running cluster
    end_time = 0 :: non_neg_integer(),
    exit_code = 0 :: integer(),
    error_msg = <<>> :: binary()
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
