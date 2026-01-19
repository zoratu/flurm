%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Defaults
%%%
%%% Provides default values for SLURM-compatible configuration options.
%%% These defaults are applied when loading a slurm.conf file that
%%% doesn't specify certain parameters.
%%%
%%% Categories:
%%% - Daemon settings (ports, hosts, paths)
%%% - Scheduler settings (type, intervals)
%%% - Priority settings (weights, decay)
%%% - Preemption settings
%%% - Accounting settings
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_defaults).

-export([
    get_defaults/0,
    apply_defaults/1,
    get_default/1,
    get_default/2
]).

-ifdef(TEST).
-export([
    daemon_defaults/0,
    scheduler_defaults/0,
    priority_defaults/0,
    misc_defaults/0
]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Get all default configuration values
-spec get_defaults() -> #{atom() => term()}.
get_defaults() ->
    maps:merge(
        maps:merge(
            maps:merge(daemon_defaults(), scheduler_defaults()),
            priority_defaults()
        ),
        misc_defaults()
    ).

%% @doc Apply defaults to a configuration map
%% Only sets values that are not already present
-spec apply_defaults(map()) -> map().
apply_defaults(Config) ->
    Defaults = get_defaults(),
    maps:merge(Defaults, Config).

%% @doc Get a single default value
-spec get_default(atom()) -> term() | undefined.
get_default(Key) ->
    get_default(Key, undefined).

%% @doc Get a single default value with fallback
-spec get_default(atom(), term()) -> term().
get_default(Key, Default) ->
    maps:get(Key, get_defaults(), Default).

%%====================================================================
%% Internal functions - Defaults by category
%%====================================================================

%% Daemon-related defaults
daemon_defaults() ->
    #{
        %% Controller settings
        slurmctldport => 6817,
        slurmctldhost => <<"localhost">>,
        slurmctldpidfile => <<"/var/run/flurm/slurmctld.pid">>,
        slurmctldlogfile => <<"/var/log/flurm/slurmctld.log">>,
        statesavelocation => <<"/var/spool/flurm">>,
        slurmctldtimeout => 120,

        %% Slurmd settings
        slurmdport => 6818,
        slurmdpidfile => <<"/var/run/flurm/slurmd.pid">>,
        slurmdlogfile => <<"/var/log/flurm/slurmd.log">>,
        slurmdspooldir => <<"/var/spool/flurm/d">>,
        slurmdtimeout => 300,

        %% Slurmdbd settings
        slurmdbdport => 6819,

        %% User settings
        slurmuser => <<"slurm">>,
        slurmduser => <<"root">>,

        %% Authentication
        authtype => <<"auth/munge">>,
        cryptotype => <<"crypto/munge">>,

        %% Cluster name
        clustername => <<"flurm">>,

        %% Return to service
        returntoservice => 1
    }.

%% Scheduler-related defaults
scheduler_defaults() ->
    #{
        %% Scheduler type
        schedulertype => <<"sched/backfill">>,
        schedulerparameters => #{
            bf_continue => true,
            bf_max_job_test => 500,
            bf_window => 86400  %% 24 hours
        },

        %% Scheduling intervals (in seconds)
        schedulerintervalsec => 30,
        defmempernode => 64,      %% Default MB per node
        maxjobcount => 10000,
        maxarraysize => 1001,

        %% Resource selection
        selecttype => <<"select/cons_tres">>,
        selecttypeparameters => <<"CR_CPU_Memory">>,

        %% Tree width for hierarchical communication
        treewidth => 50,

        %% Job defaults
        defmempercpu => 0,        %% 0 = not set
        maxmempercpu => 0,        %% 0 = unlimited
        minjobage => 300,         %% Minimum job age for sched (seconds)

        %% Kill wait
        killwait => 30,
        killonbadexit => false,

        %% Inactive limit
        inactivelimit => 0,       %% 0 = no limit

        %% Prolog/Epilog timeouts
        prologepiloglimit => 300
    }.

%% Priority-related defaults (multi-factor priority)
priority_defaults() ->
    #{
        %% Priority type
        prioritytype => <<"priority/multifactor">>,

        %% Priority weights (0-65535)
        priorityweightage => 1000,
        priorityweightfairshare => 10000,
        priorityweightjobsize => 1000,
        priorityweightpartition => 1000,
        priorityweightqos => 10000,
        priorityweighttres => 1000,

        %% Priority decay
        prioritydecayhalflife => <<"7-0">>,  %% 7 days
        prioritymaxage => <<"7-0">>,         %% 7 days

        %% Priority flags
        priorityfavorsmall => false,
        prioritycalcperiod => 5,      %% Minutes between priority recalc

        %% Preemption
        preempttype => <<"preempt/none">>,
        preemptmode => <<"OFF">>,
        preemptexempttime => <<"0">>,

        %% Fair share
        fairshareddampeningfactor => 1,

        %% Usage factor
        usagefactor => 1.0
    }.

%% Miscellaneous defaults
misc_defaults() ->
    #{
        %% Accounting
        accountingstoragetype => <<"accounting_storage/none">>,
        accountingstoragehost => undefined,
        accountingstorageport => 6819,
        jobacctgathertype => <<"jobacct_gather/linux">>,
        jobacctgatherfrequency => 30,

        %% Switch/interconnect
        switchtype => <<"switch/none">>,

        %% Task/cgroup
        taskplugin => <<"task/affinity">>,

        %% Proctrack
        proctracktype => <<"proctrack/cgroup">>,

        %% MPI
        mpidefault => <<"none">>,

        %% Topology
        topologyplugin => <<"topology/none">>,

        %% Debug/logging
        debugflags => [],
        slurmctlddebug => <<"info">>,
        slurmdebug => <<"info">>,

        %% Message timeout
        messagetimeout => 10,

        %% TCP buffer sizes
        tcpbuffersize => 0,       %% 0 = system default

        %% Batch scheduling
        batchstarttimeout => 10,
        completewait => 0,
        epilogmsgtimeout => 2000,

        %% Health check
        healthcheckinterval => 0, %% 0 = disabled
        healthcheckprogram => undefined,

        %% Requeue settings
        jobrequeue => true,

        %% Max step count
        maxstepcnt => 40000,

        %% Propagate resource limits
        propagateresourcelimits => <<"ALL">>,

        %% Prolog/Epilog flags
        prologflags => [],

        %% VSizeFactor
        vsizefactor => 0         %% 0 = disabled
    }.
