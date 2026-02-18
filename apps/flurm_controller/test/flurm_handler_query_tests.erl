%%%-------------------------------------------------------------------
%%% @doc FLURM Query Handler Tests
%%%
%%% Comprehensive tests for the flurm_handler_query module.
%%% Tests all handle/2 clauses for query/info operations:
%%% - Job info queries (including array job expansion)
%%% - Partition info queries
%%% - Build info (version) queries
%%% - Config info queries
%%% - Statistics/diagnostics queries
%%% - Federation info and update operations
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_query_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_query_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_JOB_INFO (2003) tests
        {"Job info - all jobs", fun test_job_info_all/0},
        {"Job info - specific job", fun test_job_info_specific/0},
        {"Job info - job not found", fun test_job_info_not_found/0},
        {"Job info - NO_VAL means all jobs", fun test_job_info_no_val/0},
        {"Job info - array job expansion", fun test_job_info_array_expansion/0},
        {"Job info - response structure", fun test_job_info_response_structure/0},
        {"Job info - filtering terminal jobs", fun test_job_info_filter_terminal/0},

        %% REQUEST_JOB_INFO_SINGLE (2005) tests
        {"Job info single - delegates to job info", fun test_job_info_single/0},

        %% REQUEST_JOB_USER_INFO (2021) tests
        {"Job user info - with job id", fun test_job_user_info_with_id/0},
        {"Job user info - all jobs", fun test_job_user_info_all/0},
        {"Job user info - raw binary body", fun test_job_user_info_raw_body/0},

        %% REQUEST_PARTITION_INFO (2009) tests
        {"Partition info - empty", fun test_partition_info_empty/0},
        {"Partition info - multiple partitions", fun test_partition_info_multiple/0},
        {"Partition info - with nodes", fun test_partition_info_with_nodes/0},
        {"Partition info - response structure", fun test_partition_info_response_structure/0},
        {"Partition info - node_inx calculation", fun test_partition_info_node_inx/0},

        %% REQUEST_BUILD_INFO (2001) tests
        {"Build info - version", fun test_build_info_version/0},
        {"Build info - config values", fun test_build_info_config/0},
        {"Build info - response structure", fun test_build_info_response_structure/0},

        %% REQUEST_CONFIG_INFO (2016) tests
        {"Config info - basic", fun test_config_info_basic/0},
        {"Config info - with config server", fun test_config_info_with_server/0},

        %% REQUEST_STATS_INFO (2026) tests
        {"Stats info - basic", fun test_stats_info_basic/0},
        {"Stats info - job counts", fun test_stats_info_job_counts/0},
        {"Stats info - scheduler stats", fun test_stats_info_scheduler_stats/0},
        {"Stats info - response structure", fun test_stats_info_response_structure/0},

        %% REQUEST_FED_INFO (2049) tests
        {"Federation info - not federated", fun test_fed_info_not_federated/0},
        {"Federation info - federated", fun test_fed_info_federated/0},
        {"Federation info - with sibling counts", fun test_fed_info_sibling_counts/0},
        {"Federation info - federation stats", fun test_fed_info_stats/0},
        {"Federation info - noproc error", fun test_fed_info_noproc/0},

        %% REQUEST_UPDATE_FEDERATION (2064) tests
        {"Update federation - add cluster", fun test_update_fed_add_cluster/0},
        {"Update federation - remove cluster", fun test_update_fed_remove_cluster/0},
        {"Update federation - update settings", fun test_update_fed_settings/0},
        {"Update federation - unknown action", fun test_update_fed_unknown_action/0},
        {"Update federation - raw binary fallback", fun test_update_fed_raw_binary/0},
        {"Update federation - add cluster error", fun test_update_fed_add_error/0},
        {"Update federation - remove cluster error", fun test_update_fed_remove_error/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Start meck for mocking
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_federation),
    meck:new([flurm_job_manager, flurm_partition_manager, flurm_node_manager_server,
              flurm_config_server, flurm_scheduler, flurm_federation],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [] end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    meck:expect(flurm_config_server, get_all, fun() -> #{} end),
    meck:expect(flurm_scheduler, get_stats, fun() -> #{} end),
    meck:expect(flurm_federation, get_federation_info, fun() -> {error, not_federated} end),
    meck:expect(flurm_federation, is_federated, fun() -> false end),

    ok.

cleanup(_) ->
    %% Unload all mocks
    catch meck:unload([flurm_job_manager, flurm_partition_manager, flurm_node_manager_server,
                       flurm_config_server, flurm_scheduler, flurm_federation]),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_header(MsgType) ->
    #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = MsgType,
        body_length = 0
    }.

make_job(JobId) ->
    make_job(JobId, #{}).

make_job(JobId, Overrides) ->
    Now = erlang:system_time(second),
    Defaults = #{
        id => JobId,
        name => <<"test_job">>,
        state => pending,
        partition => <<"default">>,
        user => <<"root">>,
        num_nodes => 1,
        num_cpus => 1,
        num_tasks => 1,
        cpus_per_task => 1,
        time_limit => 3600,
        priority => 100,
        submit_time => Now,
        start_time => undefined,
        end_time => undefined,
        allocated_nodes => [],
        script => <<"#!/bin/bash\necho hello">>,
        memory_mb => 1024,
        account => <<"default">>,
        licenses => <<>>,
        qos => <<"normal">>,
        std_out => <<>>,
        std_err => <<>>,
        array_job_id => 0,
        array_task_id => undefined
    },
    Props = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Props),
        name = maps:get(name, Props),
        state = maps:get(state, Props),
        partition = maps:get(partition, Props),
        user = maps:get(user, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        num_tasks = maps:get(num_tasks, Props),
        cpus_per_task = maps:get(cpus_per_task, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        submit_time = maps:get(submit_time, Props),
        start_time = maps:get(start_time, Props),
        end_time = maps:get(end_time, Props),
        allocated_nodes = maps:get(allocated_nodes, Props),
        script = maps:get(script, Props),
        memory_mb = maps:get(memory_mb, Props),
        account = maps:get(account, Props),
        licenses = maps:get(licenses, Props),
        qos = maps:get(qos, Props),
        std_out = maps:get(std_out, Props),
        std_err = maps:get(std_err, Props),
        array_job_id = maps:get(array_job_id, Props),
        array_task_id = maps:get(array_task_id, Props)
    }.

make_partition(Name) ->
    make_partition(Name, #{}).

make_partition(Name, Overrides) ->
    Defaults = #{
        name => Name,
        state => up,
        nodes => [],
        max_time => 86400,
        default_time => 3600,
        max_nodes => 100,
        priority => 1
    },
    Props = maps:merge(Defaults, Overrides),
    #partition{
        name = maps:get(name, Props),
        state = maps:get(state, Props),
        nodes = maps:get(nodes, Props),
        max_time = maps:get(max_time, Props),
        default_time = maps:get(default_time, Props),
        max_nodes = maps:get(max_nodes, Props),
        priority = maps:get(priority, Props)
    }.

make_node(Hostname) ->
    make_node(Hostname, #{}).

make_node(Hostname, Overrides) ->
    Defaults = #{
        hostname => Hostname,
        cpus => 4,
        memory_mb => 8192,
        state => up,
        partitions => [<<"default">>]
    },
    Props = maps:merge(Defaults, Overrides),
    #node{
        hostname = maps:get(hostname, Props),
        cpus = maps:get(cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        state = maps:get(state, Props),
        partitions = maps:get(partitions, Props)
    }.

%%====================================================================
%% Job Info Tests (REQUEST_JOB_INFO - 2003)
%%====================================================================

test_job_info_all() ->
    Jobs = [make_job(1001), make_job(1002), make_job(1003)],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 0},
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(3, Response#job_info_response.job_count).

test_job_info_specific() ->
    Job = make_job(1004),
    meck:expect(flurm_job_manager, get_job, fun(1004) -> {ok, Job} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [Job] end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 1004},
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(1, Response#job_info_response.job_count).

test_job_info_not_found() ->
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 9999},
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(0, Response#job_info_response.job_count).

test_job_info_no_val() ->
    Jobs = [make_job(1005)],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 16#FFFFFFFE},  % NO_VAL
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(1, Response#job_info_response.job_count).

test_job_info_array_expansion() ->
    ParentJob = make_job(1006),
    TaskJobs = [
        make_job(1007, #{array_job_id => 1006, array_task_id => 0}),
        make_job(1008, #{array_job_id => 1006, array_task_id => 1})
    ],
    meck:expect(flurm_job_manager, get_job, fun(1006) -> {ok, ParentJob} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> TaskJobs end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 1006},
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(2, Response#job_info_response.job_count).

test_job_info_response_structure() ->
    Now = erlang:system_time(second),
    Job = make_job(1009, #{
        name => <<"my_job">>,
        state => running,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8,
        time_limit => 7200,
        priority => 500,
        submit_time => Now - 3600,
        start_time => Now - 1800,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        account => <<"research">>
    }),
    meck:expect(flurm_job_manager, get_job, fun(1009) -> {ok, Job} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 1009},
    {ok, _, Response} = flurm_handler_query:handle(Header, Request),

    ?assert(Response#job_info_response.last_update > 0),
    [JobInfo] = Response#job_info_response.jobs,
    ?assertEqual(1009, JobInfo#job_info.job_id),
    ?assertEqual(<<"my_job">>, JobInfo#job_info.name),
    ?assertEqual(?JOB_RUNNING, JobInfo#job_info.job_state),
    ?assertEqual(<<"compute">>, JobInfo#job_info.partition).

test_job_info_filter_terminal() ->
    Now = erlang:system_time(second),
    Jobs = [
        make_job(1010, #{state => running}),
        make_job(1011, #{state => pending}),
        make_job(1012, #{state => completed, end_time => Now - 30}),  % Recent
        make_job(1013, #{state => completed, end_time => Now - 300})  % Old, should be filtered
    ],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_JOB_INFO),
    Request = #job_info_request{job_id = 0},
    {ok, _, Response} = flurm_handler_query:handle(Header, Request),

    %% Should include running, pending, and recent completed
    ?assertEqual(3, Response#job_info_response.job_count).

%%====================================================================
%% Job Info Single Tests (REQUEST_JOB_INFO_SINGLE - 2005)
%%====================================================================

test_job_info_single() ->
    Job = make_job(1014),
    meck:expect(flurm_job_manager, get_job, fun(1014) -> {ok, Job} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),

    Header = make_header(?REQUEST_JOB_INFO_SINGLE),
    Request = #job_info_request{job_id = 1014},
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(1, Response#job_info_response.job_count).

%%====================================================================
%% Job User Info Tests (REQUEST_JOB_USER_INFO - 2021)
%%====================================================================

test_job_user_info_with_id() ->
    Job = make_job(1015),
    meck:expect(flurm_job_manager, get_job, fun(1015) -> {ok, Job} end),
    meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),

    Header = make_header(?REQUEST_JOB_USER_INFO),
    Body = <<1015:32/big, 0:32>>,
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(1, Response#job_info_response.job_count).

test_job_user_info_all() ->
    Jobs = [make_job(1016), make_job(1017)],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_JOB_USER_INFO),
    Body = <<0:32/big>>,
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Body),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
    ?assertEqual(2, Response#job_info_response.job_count).

test_job_user_info_raw_body() ->
    Jobs = [make_job(1018)],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_JOB_USER_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_JOB_INFO, MsgType).

%%====================================================================
%% Partition Info Tests (REQUEST_PARTITION_INFO - 2009)
%%====================================================================

test_partition_info_empty() ->
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [] end),

    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_PARTITION_INFO, MsgType),
    ?assertEqual(0, Response#partition_info_response.partition_count).

test_partition_info_multiple() ->
    Partitions = [
        make_partition(<<"default">>),
        make_partition(<<"compute">>),
        make_partition(<<"gpu">>)
    ],
    meck:expect(flurm_partition_manager, list_partitions, fun() -> Partitions end),

    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_PARTITION_INFO, MsgType),
    ?assertEqual(3, Response#partition_info_response.partition_count).

test_partition_info_with_nodes() ->
    Partition = make_partition(<<"compute">>, #{nodes => [<<"node1">>, <<"node2">>]}),
    Nodes = [
        make_node(<<"node1">>, #{partitions => [<<"compute">>]}),
        make_node(<<"node2">>, #{partitions => [<<"compute">>]})
    ],
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [Partition] end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> Nodes end),

    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    [PartInfo] = Response#partition_info_response.partitions,
    ?assertEqual(2, PartInfo#partition_info.total_nodes),
    ?assertEqual(8, PartInfo#partition_info.total_cpus).  % 4 + 4

test_partition_info_response_structure() ->
    Partition = make_partition(<<"compute">>, #{
        max_time => 172800,
        default_time => 7200,
        max_nodes => 50,
        priority => 10
    }),
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [Partition] end),

    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assert(Response#partition_info_response.last_update > 0),
    [PartInfo] = Response#partition_info_response.partitions,
    ?assertEqual(<<"compute">>, PartInfo#partition_info.name),
    ?assertEqual(3, PartInfo#partition_info.state_up),  % PARTITION_UP
    ?assertEqual(172800, PartInfo#partition_info.max_time),
    ?assertEqual(7200, PartInfo#partition_info.default_time),
    ?assertEqual(10, PartInfo#partition_info.priority_tier).

test_partition_info_node_inx() ->
    Partition = make_partition(<<"compute">>, #{nodes => [<<"node2">>, <<"node4">>]}),
    Nodes = [
        make_node(<<"node1">>, #{partitions => [<<"other">>]}),
        make_node(<<"node2">>, #{partitions => [<<"compute">>]}),
        make_node(<<"node3">>, #{partitions => [<<"other">>]}),
        make_node(<<"node4">>, #{partitions => [<<"compute">>]})
    ],
    meck:expect(flurm_partition_manager, list_partitions, fun() -> [Partition] end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> Nodes end),

    Header = make_header(?REQUEST_PARTITION_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    [PartInfo] = Response#partition_info_response.partitions,
    %% node_inx should be indices 1 and 3 (0-based)
    ?assertEqual([1, 3], PartInfo#partition_info.node_inx).

%%====================================================================
%% Build Info Tests (REQUEST_BUILD_INFO - 2001)
%%====================================================================

test_build_info_version() ->
    Header = make_header(?REQUEST_BUILD_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_BUILD_INFO, MsgType),
    ?assertEqual(<<"22.05.0">>, Response#build_info_response.version),
    ?assertEqual(22, Response#build_info_response.version_major),
    ?assertEqual(5, Response#build_info_response.version_minor),
    ?assertEqual(0, Response#build_info_response.version_micro).

test_build_info_config() ->
    meck:expect(flurm_config_server, get, fun
        (cluster_name, _) -> <<"my_cluster">>;
        (control_machine, _) -> <<"controller.local">>;
        (slurmctld_port, _) -> 6817;
        (slurmd_port, _) -> 6818;
        (state_save_location, _) -> <<"/var/spool/slurm">>;
        (_, Default) -> Default
    end),

    Header = make_header(?REQUEST_BUILD_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(<<"my_cluster">>, Response#build_info_response.cluster_name),
    ?assertEqual(<<"controller.local">>, Response#build_info_response.control_machine),
    ?assertEqual(6817, Response#build_info_response.slurmctld_port),
    ?assertEqual(6818, Response#build_info_response.slurmd_port).

test_build_info_response_structure() ->
    Header = make_header(?REQUEST_BUILD_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertNotEqual(<<>>, Response#build_info_response.release),
    ?assertEqual(<<"erlang">>, Response#build_info_response.build_host),
    ?assertEqual(<<"flurm">>, Response#build_info_response.build_user),
    ?assertEqual(<<"accounting_storage/none">>, Response#build_info_response.accounting_storage_type),
    ?assertEqual(<<"auth/munge">>, Response#build_info_response.auth_type),
    ?assertEqual(<<"slurm">>, Response#build_info_response.slurm_user_name),
    ?assertEqual(<<"root">>, Response#build_info_response.slurmd_user_name).

%%====================================================================
%% Config Info Tests (REQUEST_CONFIG_INFO - 2016)
%%====================================================================

test_config_info_basic() ->
    Header = make_header(?REQUEST_CONFIG_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_CONFIG_INFO, MsgType),
    ?assert(Response#config_info_response.last_update > 0).

test_config_info_with_server() ->
    meck:expect(flurm_config_server, get_all, fun() ->
        #{cluster_name => <<"test">>, slurmctld_port => 6817}
    end),

    Header = make_header(?REQUEST_CONFIG_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    Config = Response#config_info_response.config,
    ?assertEqual(<<"test">>, maps:get(cluster_name, Config)),
    ?assertEqual(6817, maps:get(slurmctld_port, Config)).

%%====================================================================
%% Stats Info Tests (REQUEST_STATS_INFO - 2026)
%%====================================================================

test_stats_info_basic() ->
    Header = make_header(?REQUEST_STATS_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_STATS_INFO, MsgType),
    ?assert(Response#stats_info_response.req_time > 0).

test_stats_info_job_counts() ->
    Jobs = [
        make_job(2001, #{state => pending}),
        make_job(2002, #{state => pending}),
        make_job(2003, #{state => running}),
        make_job(2004, #{state => completed}),
        make_job(2005, #{state => cancelled}),
        make_job(2006, #{state => failed})
    ],
    meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),

    Header = make_header(?REQUEST_STATS_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(2, Response#stats_info_response.jobs_pending),
    ?assertEqual(1, Response#stats_info_response.jobs_running),
    ?assertEqual(1, Response#stats_info_response.jobs_completed),
    ?assertEqual(1, Response#stats_info_response.jobs_canceled),
    ?assertEqual(1, Response#stats_info_response.jobs_failed).

test_stats_info_scheduler_stats() ->
    meck:expect(flurm_scheduler, get_stats, fun() ->
        {ok, #{
            jobs_submitted => 100,
            jobs_started => 80,
            start_time => 1700000000,
            schedule_cycle_max => 50,
            schedule_cycle_last => 10,
            schedule_cycle_sum => 500,
            schedule_cycle_counter => 50,
            schedule_cycle_depth => 100
        }}
    end),

    Header = make_header(?REQUEST_STATS_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(100, Response#stats_info_response.jobs_submitted),
    ?assertEqual(80, Response#stats_info_response.jobs_started),
    ?assertEqual(50, Response#stats_info_response.schedule_cycle_max),
    ?assertEqual(10, Response#stats_info_response.schedule_cycle_last).

test_stats_info_response_structure() ->
    Header = make_header(?REQUEST_STATS_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(1, Response#stats_info_response.parts_packed),
    ?assert(Response#stats_info_response.server_thread_count > 0),
    ?assertEqual(0, Response#stats_info_response.agent_queue_size),
    ?assertEqual(0, Response#stats_info_response.bf_backfilled_jobs),
    ?assertEqual(false, Response#stats_info_response.bf_active).

%%====================================================================
%% Federation Info Tests (REQUEST_FED_INFO - 2049)
%%====================================================================

test_fed_info_not_federated() ->
    meck:expect(flurm_federation, get_federation_info, fun() -> {error, not_federated} end),

    Header = make_header(?REQUEST_FED_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_FED_INFO, MsgType),
    ?assertEqual(<<>>, maps:get(federation_name, Response)),
    ?assertEqual(0, maps:get(cluster_count, Response)),
    ?assertEqual(<<"inactive">>, maps:get(federation_state, Response)).

test_fed_info_federated() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{
            name => <<"my_federation">>,
            local_cluster => <<"cluster1">>,
            clusters => [
                #{name => <<"cluster1">>, host => <<"host1">>, port => 6817, state => <<"up">>},
                #{name => <<"cluster2">>, host => <<"host2">>, port => 6817, state => <<"up">>}
            ]
        }}
    end),
    meck:expect(flurm_federation, get_federation_jobs, fun() -> [] end),
    meck:expect(flurm_federation, get_federation_stats, fun() -> #{} end),

    Header = make_header(?REQUEST_FED_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_FED_INFO, MsgType),
    ?assertEqual(<<"my_federation">>, maps:get(federation_name, Response)),
    ?assertEqual(<<"cluster1">>, maps:get(local_cluster, Response)),
    ?assertEqual(2, maps:get(cluster_count, Response)).

test_fed_info_sibling_counts() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{
            name => <<"fed">>,
            local_cluster => <<"local">>,
            clusters => [#{name => <<"cluster1">>}, #{name => <<"cluster2">>}]
        }}
    end),
    meck:expect(flurm_federation, get_federation_jobs, fun() ->
        [
            #{origin_cluster => <<"cluster1">>},
            #{origin_cluster => <<"cluster1">>},
            #{origin_cluster => <<"cluster2">>}
        ]
    end),
    meck:expect(flurm_federation, get_federation_stats, fun() -> #{} end),

    Header = make_header(?REQUEST_FED_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    Clusters = maps:get(clusters, Response),
    [C1, C2] = Clusters,
    ?assertEqual(2, maps:get(sibling_job_count, C1)),
    ?assertEqual(1, maps:get(sibling_job_count, C2)).

test_fed_info_stats() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"fed">>, local_cluster => <<"local">>, clusters => []}}
    end),
    meck:expect(flurm_federation, get_federation_jobs, fun() -> [] end),
    meck:expect(flurm_federation, get_federation_stats, fun() ->
        #{
            total_sibling_jobs => 10,
            total_pending_jobs => 5,
            total_running_jobs => 5,
            state => <<"active">>
        }
    end),

    Header = make_header(?REQUEST_FED_INFO),
    {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(10, maps:get(total_sibling_jobs, Response)),
    ?assertEqual(5, maps:get(total_pending_jobs, Response)),
    ?assertEqual(5, maps:get(total_running_jobs, Response)),
    ?assertEqual(<<"active">>, maps:get(federation_state, Response)).

test_fed_info_noproc() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        exit({noproc, {gen_server, call, [flurm_federation, get_info]}})
    end),

    Header = make_header(?REQUEST_FED_INFO),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_FED_INFO, MsgType),
    ?assertEqual(<<>>, maps:get(federation_name, Response)),
    ?assertEqual(<<"inactive">>, maps:get(federation_state, Response)).

%%====================================================================
%% Update Federation Tests (REQUEST_UPDATE_FEDERATION - 2064)
%%====================================================================

test_update_fed_add_cluster() ->
    meck:expect(flurm_federation, add_cluster, fun(<<"new_cluster">>, _) -> ok end),

    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = add_cluster,
        cluster_name = <<"new_cluster">>,
        host = <<"host.example.com">>,
        port = 6817,
        settings = #{}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(0, Response#update_federation_response.error_code).

test_update_fed_remove_cluster() ->
    meck:expect(flurm_federation, remove_cluster, fun(<<"old_cluster">>) -> ok end),

    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = remove_cluster,
        cluster_name = <<"old_cluster">>,
        host = <<>>,
        port = 6817,
        settings = #{}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(0, Response#update_federation_response.error_code).

test_update_fed_settings() ->
    meck:expect(flurm_federation, update_settings, fun(#{key := value}) -> ok end),

    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = update_settings,
        cluster_name = <<>>,
        host = <<>>,
        port = 6817,
        settings = #{key => value}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(0, Response#update_federation_response.error_code).

test_update_fed_unknown_action() ->
    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = invalid_action,
        cluster_name = <<>>,
        host = <<>>,
        port = 6817,
        settings = #{}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(1, Response#update_federation_response.error_code),
    ?assertEqual(<<"unknown action">>, Response#update_federation_response.error_msg).

test_update_fed_raw_binary() ->
    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<"invalid">>),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(1, Response#update_federation_response.error_code),
    ?assertEqual(<<"invalid request format">>, Response#update_federation_response.error_msg).

test_update_fed_add_error() ->
    meck:expect(flurm_federation, add_cluster, fun(_, _) -> {error, already_exists} end),

    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = add_cluster,
        cluster_name = <<"existing">>,
        host = <<"host">>,
        port = 6817,
        settings = #{}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(1, Response#update_federation_response.error_code).

test_update_fed_remove_error() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_UPDATE_FEDERATION),
    Request = #update_federation_request{
        action = remove_cluster,
        cluster_name = <<"nonexistent">>,
        host = <<>>,
        port = 6817,
        settings = #{}
    },
    {ok, MsgType, Response} = flurm_handler_query:handle(Header, Request),

    ?assertEqual(?RESPONSE_UPDATE_FEDERATION, MsgType),
    ?assertEqual(1, Response#update_federation_response.error_code).

%%====================================================================
%% Helper Function Tests
%%====================================================================

job_to_job_info_test_() ->
    [
        {"Convert job to job_info", fun() ->
            Job = make_job(3001, #{
                name => <<"test">>,
                state => running,
                num_nodes => 2,
                num_cpus => 8,
                allocated_nodes => [<<"node1">>, <<"node2">>]
            }),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(3001, JobInfo#job_info.job_id),
            ?assertEqual(<<"test">>, JobInfo#job_info.name),
            ?assertEqual(?JOB_RUNNING, JobInfo#job_info.job_state),
            ?assertEqual(2, JobInfo#job_info.num_nodes),
            ?assertEqual(8, JobInfo#job_info.num_cpus)
        end},
        {"Convert job with array fields", fun() ->
            Job = make_job(3002, #{array_job_id => 3000, array_task_id => 5}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(3000, JobInfo#job_info.array_job_id),
            ?assertEqual(5, JobInfo#job_info.array_task_id)
        end},
        {"Convert job with undefined array_task_id", fun() ->
            Job = make_job(3003),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(16#FFFFFFFE, JobInfo#job_info.array_task_id)
        end}
    ].

job_state_to_slurm_test_() ->
    [
        {"pending -> JOB_PENDING", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_handler_query:job_state_to_slurm(pending))
        end},
        {"configuring -> JOB_PENDING", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_handler_query:job_state_to_slurm(configuring))
        end},
        {"running -> JOB_RUNNING", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_handler_query:job_state_to_slurm(running))
        end},
        {"completing -> JOB_RUNNING", fun() ->
            ?assertEqual(?JOB_RUNNING, flurm_handler_query:job_state_to_slurm(completing))
        end},
        {"completed -> JOB_COMPLETE", fun() ->
            ?assertEqual(?JOB_COMPLETE, flurm_handler_query:job_state_to_slurm(completed))
        end},
        {"cancelled -> JOB_CANCELLED", fun() ->
            ?assertEqual(?JOB_CANCELLED, flurm_handler_query:job_state_to_slurm(cancelled))
        end},
        {"failed -> JOB_FAILED", fun() ->
            ?assertEqual(?JOB_FAILED, flurm_handler_query:job_state_to_slurm(failed))
        end},
        {"timeout -> JOB_TIMEOUT", fun() ->
            ?assertEqual(?JOB_TIMEOUT, flurm_handler_query:job_state_to_slurm(timeout))
        end},
        {"node_fail -> JOB_NODE_FAIL", fun() ->
            ?assertEqual(?JOB_NODE_FAIL, flurm_handler_query:job_state_to_slurm(node_fail))
        end},
        {"unknown -> JOB_PENDING", fun() ->
            ?assertEqual(?JOB_PENDING, flurm_handler_query:job_state_to_slurm(some_unknown))
        end}
    ].

partition_state_to_slurm_test_() ->
    [
        {"up -> 3 (PARTITION_UP)", fun() ->
            ?assertEqual(3, flurm_handler_query:partition_state_to_slurm(up))
        end},
        {"down -> 1 (PARTITION_DOWN)", fun() ->
            ?assertEqual(1, flurm_handler_query:partition_state_to_slurm(down))
        end},
        {"drain -> 2 (PARTITION_DRAIN)", fun() ->
            ?assertEqual(2, flurm_handler_query:partition_state_to_slurm(drain))
        end},
        {"inactive -> 0 (PARTITION_INACTIVE)", fun() ->
            ?assertEqual(0, flurm_handler_query:partition_state_to_slurm(inactive))
        end},
        {"unknown -> 3 (PARTITION_UP)", fun() ->
            ?assertEqual(3, flurm_handler_query:partition_state_to_slurm(some_unknown))
        end}
    ].

compute_node_inx_test_() ->
    [
        {"Empty partition nodes", fun() ->
            ?assertEqual([], flurm_handler_query:compute_node_inx([], [<<"a">>, <<"b">>, <<"c">>]))
        end},
        {"All nodes in partition", fun() ->
            AllNodes = [<<"a">>, <<"b">>, <<"c">>],
            ?assertEqual([0, 1, 2], flurm_handler_query:compute_node_inx(AllNodes, AllNodes))
        end},
        {"Some nodes in partition", fun() ->
            AllNodes = [<<"a">>, <<"b">>, <<"c">>, <<"d">>],
            PartNodes = [<<"b">>, <<"d">>],
            ?assertEqual([1, 3], flurm_handler_query:compute_node_inx(PartNodes, AllNodes))
        end}
    ].

format_fed_clusters_test_() ->
    [
        {"Empty clusters list", fun() ->
            ?assertEqual([], flurm_handler_query:format_fed_clusters([]))
        end},
        {"Format map clusters", fun() ->
            Clusters = [#{name => <<"c1">>, host => <<"h1">>, port => 6817, state => <<"up">>}],
            Result = flurm_handler_query:format_fed_clusters(Clusters),
            [C] = Result,
            ?assertEqual(<<"c1">>, maps:get(name, C)),
            ?assertEqual(<<"h1">>, maps:get(host, C))
        end}
    ].

get_sibling_job_counts_per_cluster_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         meck:new(flurm_federation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_federation),
         ok
     end,
     [
        {"No jobs returns empty map", fun() ->
            meck:expect(flurm_federation, get_federation_jobs, fun() -> [] end),
            ?assertEqual(#{}, flurm_handler_query:get_sibling_job_counts_per_cluster())
        end},
        {"Count jobs by cluster", fun() ->
            meck:expect(flurm_federation, get_federation_jobs, fun() ->
                [
                    #{origin_cluster => <<"c1">>},
                    #{origin_cluster => <<"c1">>},
                    #{origin_cluster => <<"c2">>}
                ]
            end),
            Counts = flurm_handler_query:get_sibling_job_counts_per_cluster(),
            ?assertEqual(2, maps:get(<<"c1">>, Counts)),
            ?assertEqual(1, maps:get(<<"c2">>, Counts))
        end}
     ]}.

count_jobs_by_state_test_() ->
    [
        {"Count pending jobs", fun() ->
            Jobs = [
                #{state => pending},
                #{state => running},
                #{state => pending}
            ],
            ?assertEqual(2, flurm_handler_query:count_jobs_by_state(Jobs, pending))
        end},
        {"Count running jobs", fun() ->
            Jobs = [
                #{state => running},
                #{state => running}
            ],
            ?assertEqual(2, flurm_handler_query:count_jobs_by_state(Jobs, running))
        end},
        {"Empty jobs list", fun() ->
            ?assertEqual(0, flurm_handler_query:count_jobs_by_state([], pending))
        end}
    ].

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_partition_manager),
         catch meck:unload(flurm_node_manager_server),
         meck:new([flurm_job_manager, flurm_partition_manager, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_partition_manager, flurm_node_manager_server]),
         ok
     end,
     [
        {"Job with zero time_limit", fun() ->
            Job = make_job(4001, #{time_limit => 0}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(1, JobInfo#job_info.time_limit)  % Minimum 1 minute
        end},
        {"Job with zero num_nodes", fun() ->
            Job = make_job(4002, #{num_nodes => 0}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(1, JobInfo#job_info.num_nodes)  % Minimum 1
        end},
        {"Job with zero num_cpus", fun() ->
            Job = make_job(4003, #{num_cpus => 0}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(1, JobInfo#job_info.num_cpus)  % Minimum 1
        end},
        {"Partition with empty nodes list", fun() ->
            Part = make_partition(<<"empty">>),
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(0, PartInfo#partition_info.total_nodes)
        end}
     ]}.

%%====================================================================
%% Jobs With Array Expansion Tests
%%====================================================================

get_jobs_with_array_expansion_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Get all jobs (id=0) filters old terminal jobs", fun() ->
            Now = erlang:system_time(second),
            Jobs = [
                make_job(5001, #{state => running}),
                make_job(5002, #{state => completed, end_time => Now - 30}),  % Recent
                make_job(5003, #{state => completed, end_time => Now - 120})  % Old
            ],
            meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),
            Result = flurm_handler_query:get_jobs_with_array_expansion(0),
            ?assertEqual(2, length(Result))
        end},
        {"Get specific job returns just that job", fun() ->
            Job = make_job(5004),
            meck:expect(flurm_job_manager, get_job, fun(5004) -> {ok, Job} end),
            meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
            Result = flurm_handler_query:get_jobs_with_array_expansion(5004),
            ?assertEqual([Job], Result)
        end},
        {"Get array parent returns task jobs", fun() ->
            Parent = make_job(5005),
            Tasks = [
                make_job(5006, #{array_job_id => 5005, array_task_id => 0}),
                make_job(5007, #{array_job_id => 5005, array_task_id => 1})
            ],
            meck:expect(flurm_job_manager, get_job, fun(5005) -> {ok, Parent} end),
            meck:expect(flurm_job_manager, list_jobs, fun() -> Tasks end),
            Result = flurm_handler_query:get_jobs_with_array_expansion(5005),
            ?assertEqual(Tasks, Result)
        end}
     ]}.

%%====================================================================
%% Federation Stats Tests
%%====================================================================

get_federation_stats_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         meck:new(flurm_federation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_federation),
         ok
     end,
     [
        {"Get federation stats from server", fun() ->
            meck:expect(flurm_federation, get_federation_stats, fun() ->
                #{total_sibling_jobs => 10, total_pending_jobs => 5, total_running_jobs => 5}
            end),
            Stats = flurm_handler_query:get_federation_stats(),
            ?assertEqual(10, maps:get(total_sibling_jobs, Stats))
        end},
        {"Calculate stats when server returns non-map", fun() ->
            meck:expect(flurm_federation, get_federation_stats, fun() -> undefined end),
            meck:expect(flurm_federation, get_federation_jobs, fun() ->
                [#{state => pending}, #{state => running}]
            end),
            meck:expect(flurm_federation, list_clusters, fun() -> [cluster1] end),
            Stats = flurm_handler_query:get_federation_stats(),
            ?assertEqual(2, maps:get(total_sibling_jobs, Stats)),
            ?assertEqual(1, maps:get(total_pending_jobs, Stats)),
            ?assertEqual(1, maps:get(total_running_jobs, Stats)),
            ?assertEqual(<<"active">>, maps:get(state, Stats))
        end},
        {"Handle error gracefully", fun() ->
            meck:expect(flurm_federation, get_federation_stats, fun() -> error(crash) end),
            Stats = flurm_handler_query:get_federation_stats(),
            ?assertEqual(0, maps:get(total_sibling_jobs, Stats)),
            ?assertEqual(<<"inactive">>, maps:get(state, Stats))
        end}
     ]}.

%%====================================================================
%% Partition Info Extended Tests
%%====================================================================

partition_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_partition_manager),
         catch meck:unload(flurm_node_manager_server),
         meck:new([flurm_partition_manager, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_partition_manager, flurm_node_manager_server]),
         ok
     end,
     [
        {"Partition with drain state", fun() ->
            Part = make_partition(<<"drain_part">>, #{state => drain}),
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(2, PartInfo#partition_info.state_up)  % PARTITION_DRAIN
        end},
        {"Partition with inactive state", fun() ->
            Part = make_partition(<<"inactive_part">>, #{state => inactive}),
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(0, PartInfo#partition_info.state_up)  % PARTITION_INACTIVE
        end},
        {"Partition with down state", fun() ->
            Part = make_partition(<<"down_part">>, #{state => down}),
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(1, PartInfo#partition_info.state_up)  % PARTITION_DOWN
        end},
        {"Partition with unknown state defaults to UP", fun() ->
            Part = make_partition(<<"unknown_part">>, #{state => some_unknown_state}),
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(3, PartInfo#partition_info.state_up)  % PARTITION_UP
        end},
        {"Default partition includes all nodes", fun() ->
            Part = make_partition(<<"default">>, #{nodes => []}),
            Nodes = [
                make_node(<<"node1">>, #{partitions => [<<"other">>]}),
                make_node(<<"node2">>, #{partitions => []})
            ],
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> Nodes end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(2, PartInfo#partition_info.total_nodes)
        end},
        {"Partition with explicit nodes filters membership", fun() ->
            Part = make_partition(<<"explicit">>, #{nodes => [<<"node1">>]}),
            Nodes = [
                make_node(<<"node1">>, #{partitions => [<<"explicit">>]}),
                make_node(<<"node2">>, #{partitions => [<<"explicit">>]})  % Not in partition node list
            ],
            meck:expect(flurm_partition_manager, list_partitions, fun() -> [Part] end),
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> Nodes end),
            Header = make_header(?REQUEST_PARTITION_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            [PartInfo] = Response#partition_info_response.partitions,
            ?assertEqual(1, PartInfo#partition_info.total_nodes)
        end}
     ]}.

%%====================================================================
%% Job Info Extended Tests
%%====================================================================

job_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Job with held state", fun() ->
            Now = erlang:system_time(second),
            Jobs = [make_job(6001, #{state => held, submit_time => Now})],
            meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),
            Header = make_header(?REQUEST_JOB_INFO),
            Request = #job_info_request{job_id = 0},
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#job_info_response.job_count)  % Held jobs included
        end},
        {"Job with configuring state", fun() ->
            Now = erlang:system_time(second),
            Jobs = [make_job(6002, #{state => configuring, submit_time => Now})],
            meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),
            Header = make_header(?REQUEST_JOB_INFO),
            Request = #job_info_request{job_id = 0},
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#job_info_response.job_count)
        end},
        {"Job state conversion completing -> RUNNING", fun() ->
            Job = make_job(6003, #{state => completing}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(?JOB_RUNNING, JobInfo#job_info.job_state)
        end},
        {"Job state conversion timeout", fun() ->
            Job = make_job(6004, #{state => timeout}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(?JOB_TIMEOUT, JobInfo#job_info.job_state)
        end},
        {"Job state conversion node_fail", fun() ->
            Job = make_job(6005, #{state => node_fail}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(?JOB_NODE_FAIL, JobInfo#job_info.job_state)
        end},
        {"Job with std_err path set", fun() ->
            Job = make_job(6006, #{std_err => <<"/var/log/job.err">>}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(<<"/var/log/job.err">>, JobInfo#job_info.std_err)
        end},
        {"Job with std_out path set", fun() ->
            Job = make_job(6007, #{std_out => <<"/var/log/job.out">>}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(<<"/var/log/job.out">>, JobInfo#job_info.std_out)
        end},
        {"Job with batch host populated", fun() ->
            Job = make_job(6008, #{allocated_nodes => [<<"compute-1">>, <<"compute-2">>]}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(<<"compute-1">>, JobInfo#job_info.batch_host)
        end},
        {"Job with non-zero array_job_id", fun() ->
            Job = make_job(6009, #{array_job_id => 5000}),
            JobInfo = flurm_handler_query:job_to_job_info(Job),
            ?assertEqual(5000, JobInfo#job_info.array_job_id)
        end}
     ]}.

%%====================================================================
%% Federation Cluster Formatting Tests
%%====================================================================

format_fed_clusters_extended_test_() ->
    [
        {"Format cluster from record format", fun() ->
            %% Simulate record format: {fed_cluster, Name, Host, Port, Weight, Auth, State, Features, Partitions, LastCheck, Failures}
            Clusters = [{fed_cluster, <<"cluster1">>, <<"host1.local">>, 6817, 1, none, up, [], [], 0, 0}],
            Result = flurm_handler_query:format_fed_clusters(Clusters),
            [C] = Result,
            ?assertEqual(<<"cluster1">>, maps:get(name, C)),
            ?assertEqual(<<"host1.local">>, maps:get(host, C)),
            ?assertEqual(6817, maps:get(port, C)),
            ?assertEqual(<<"up">>, maps:get(state, C))
        end},
        {"Format cluster from unknown format", fun() ->
            Clusters = [some_unknown_value],
            Result = flurm_handler_query:format_fed_clusters(Clusters),
            [C] = Result,
            ?assertEqual(some_unknown_value, maps:get(cluster, C))
        end}
    ].

format_fed_clusters_with_sibling_counts_test_() ->
    [
        {"Add sibling counts to map cluster", fun() ->
            Clusters = [#{name => <<"c1">>, host => <<"h1">>, port => 6817, state => <<"up">>}],
            Counts = #{<<"c1">> => 5},
            Result = flurm_handler_query:format_fed_clusters_with_sibling_counts(Clusters, Counts),
            [C] = Result,
            ?assertEqual(5, maps:get(sibling_job_count, C))
        end},
        {"Add sibling counts to record cluster", fun() ->
            Clusters = [{fed_cluster, <<"c2">>, <<"h2">>, 6817, 1, none, up, [], [], 0, 0}],
            Counts = #{<<"c2">> => 3},
            Result = flurm_handler_query:format_fed_clusters_with_sibling_counts(Clusters, Counts),
            [C] = Result,
            ?assertEqual(3, maps:get(sibling_job_count, C))
        end},
        {"Missing sibling count defaults to 0", fun() ->
            Clusters = [#{name => <<"c3">>}],
            Counts = #{<<"other">> => 10},
            Result = flurm_handler_query:format_fed_clusters_with_sibling_counts(Clusters, Counts),
            [C] = Result,
            ?assertEqual(0, maps:get(sibling_job_count, C))
        end},
        {"Unknown format wrapped in map", fun() ->
            Clusters = [unknown_format],
            Counts = #{},
            Result = flurm_handler_query:format_fed_clusters_with_sibling_counts(Clusters, Counts),
            [C] = Result,
            ?assertEqual(0, maps:get(sibling_job_count, C))
        end}
    ].

%%====================================================================
%% Build Info Extended Tests
%%====================================================================

build_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_config_server),
         meck:new(flurm_config_server, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_config_server),
         ok
     end,
     [
        {"Build info with exception in config server", fun() ->
            meck:expect(flurm_config_server, get, fun(_, _) -> error(crash) end),
            Header = make_header(?REQUEST_BUILD_INFO),
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_BUILD_INFO, MsgType),
            %% Should use default values
            ?assertEqual(<<"flurm">>, Response#build_info_response.cluster_name),
            ?assertEqual(<<"localhost">>, Response#build_info_response.control_machine),
            ?assertEqual(6817, Response#build_info_response.slurmctld_port),
            ?assertEqual(6818, Response#build_info_response.slurmd_port)
        end},
        {"Build info returns binary state_save_location", fun() ->
            meck:expect(flurm_config_server, get, fun
                (state_save_location, _) -> <<"/custom/spool">>;
                (_, Default) -> Default
            end),
            Header = make_header(?REQUEST_BUILD_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(<<"/custom/spool">>, Response#build_info_response.state_save_location)
        end}
     ]}.

%%====================================================================
%% Config Info Extended Tests
%%====================================================================

config_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_config_server),
         meck:new(flurm_config_server, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_config_server),
         ok
     end,
     [
        {"Config info with exception returns empty map", fun() ->
            meck:expect(flurm_config_server, get_all, fun() -> error(crash) end),
            Header = make_header(?REQUEST_CONFIG_INFO),
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_CONFIG_INFO, MsgType),
            ?assertEqual(#{}, Response#config_info_response.config)
        end}
     ]}.

%%====================================================================
%% Stats Info Extended Tests
%%====================================================================

stats_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         catch meck:unload(flurm_scheduler),
         meck:new([flurm_job_manager, flurm_scheduler], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_job_manager, flurm_scheduler]),
         ok
     end,
     [
        {"Stats info with list_jobs exception", fun() ->
            meck:expect(flurm_job_manager, list_jobs, fun() -> error(crash) end),
            meck:expect(flurm_scheduler, get_stats, fun() -> #{} end),
            Header = make_header(?REQUEST_STATS_INFO),
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_STATS_INFO, MsgType),
            ?assertEqual(0, Response#stats_info_response.jobs_pending),
            ?assertEqual(0, Response#stats_info_response.jobs_running)
        end},
        {"Stats info with scheduler exception", fun() ->
            meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
            meck:expect(flurm_scheduler, get_stats, fun() -> error(crash) end),
            Header = make_header(?REQUEST_STATS_INFO),
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_STATS_INFO, MsgType),
            ?assertEqual(0, Response#stats_info_response.schedule_cycle_max)
        end},
        {"Stats info with scheduler returning raw map", fun() ->
            meck:expect(flurm_job_manager, list_jobs, fun() -> [] end),
            meck:expect(flurm_scheduler, get_stats, fun() ->
                #{schedule_cycle_max => 100, schedule_cycle_last => 50}
            end),
            Header = make_header(?REQUEST_STATS_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(100, Response#stats_info_response.schedule_cycle_max),
            ?assertEqual(50, Response#stats_info_response.schedule_cycle_last)
        end},
        {"Stats info with jobs in map format", fun() ->
            meck:expect(flurm_job_manager, list_jobs, fun() ->
                [#{state => pending}, #{state => running}]
            end),
            meck:expect(flurm_scheduler, get_stats, fun() -> #{} end),
            Header = make_header(?REQUEST_STATS_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(1, Response#stats_info_response.jobs_pending),
            ?assertEqual(1, Response#stats_info_response.jobs_running)
        end},
        {"Stats info with jobs without state field", fun() ->
            meck:expect(flurm_job_manager, list_jobs, fun() ->
                [#{name => <<"no_state">>}]
            end),
            meck:expect(flurm_scheduler, get_stats, fun() -> #{} end),
            Header = make_header(?REQUEST_STATS_INFO),
            {ok, _, Response} = flurm_handler_query:handle(Header, <<>>),
            ?assertEqual(0, Response#stats_info_response.jobs_pending)
        end}
     ]}.

%%====================================================================
%% Federation Update Extended Tests
%%====================================================================

update_federation_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         meck:new(flurm_federation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_federation),
         ok
     end,
     [
        {"Update federation add_cluster noproc error", fun() ->
            meck:expect(flurm_federation, add_cluster, fun(_, _) ->
                exit({noproc, {gen_server, call, [flurm_federation, add_cluster]}})
            end),
            Header = make_header(?REQUEST_UPDATE_FEDERATION),
            Request = #update_federation_request{
                action = add_cluster,
                cluster_name = <<"test">>,
                host = <<"host">>,
                port = 6817,
                settings = #{}
            },
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#update_federation_response.error_code),
            ?assertEqual(<<"federation not running">>, Response#update_federation_response.error_msg)
        end},
        {"Update federation remove_cluster noproc error", fun() ->
            meck:expect(flurm_federation, remove_cluster, fun(_) ->
                exit({noproc, {gen_server, call, [flurm_federation, remove_cluster]}})
            end),
            Header = make_header(?REQUEST_UPDATE_FEDERATION),
            Request = #update_federation_request{
                action = remove_cluster,
                cluster_name = <<"test">>,
                host = <<>>,
                port = 6817,
                settings = #{}
            },
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#update_federation_response.error_code),
            ?assertEqual(<<"federation not running">>, Response#update_federation_response.error_msg)
        end},
        {"Update federation update_settings noproc error", fun() ->
            meck:expect(flurm_federation, update_settings, fun(_) ->
                exit({noproc, {gen_server, call, [flurm_federation, update_settings]}})
            end),
            Header = make_header(?REQUEST_UPDATE_FEDERATION),
            Request = #update_federation_request{
                action = update_settings,
                cluster_name = <<>>,
                host = <<>>,
                port = 6817,
                settings = #{key => value}
            },
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#update_federation_response.error_code),
            ?assertEqual(<<"federation not running">>, Response#update_federation_response.error_msg)
        end},
        {"Update federation update_settings error", fun() ->
            meck:expect(flurm_federation, update_settings, fun(_) -> {error, invalid_settings} end),
            Header = make_header(?REQUEST_UPDATE_FEDERATION),
            Request = #update_federation_request{
                action = update_settings,
                cluster_name = <<>>,
                host = <<>>,
                port = 6817,
                settings = #{bad => settings}
            },
            {ok, _, Response} = flurm_handler_query:handle(Header, Request),
            ?assertEqual(1, Response#update_federation_response.error_code)
        end}
     ]}.

%%====================================================================
%% Sibling Job Counts Tests
%%====================================================================

sibling_job_counts_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         meck:new(flurm_federation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_federation),
         ok
     end,
     [
        {"Count jobs with cluster field instead of origin_cluster", fun() ->
            meck:expect(flurm_federation, get_federation_jobs, fun() ->
                [#{cluster => <<"c1">>}, #{cluster => <<"c1">>}]
            end),
            Counts = flurm_handler_query:get_sibling_job_counts_per_cluster(),
            ?assertEqual(2, maps:get(<<"c1">>, Counts))
        end},
        {"Count jobs with neither cluster field", fun() ->
            meck:expect(flurm_federation, get_federation_jobs, fun() ->
                [#{other => value}]
            end),
            Counts = flurm_handler_query:get_sibling_job_counts_per_cluster(),
            ?assertEqual(1, maps:get(<<"unknown">>, Counts))
        end},
        {"Handle non-list return from get_federation_jobs", fun() ->
            meck:expect(flurm_federation, get_federation_jobs, fun() -> {error, not_available} end),
            Counts = flurm_handler_query:get_sibling_job_counts_per_cluster(),
            ?assertEqual(#{}, Counts)
        end}
     ]}.

%%====================================================================
%% Federation Info Extended Tests
%%====================================================================

federation_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_federation),
         meck:new(flurm_federation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_federation),
         ok
     end,
     [
        {"Fed info handles generic error", fun() ->
            meck:expect(flurm_federation, get_federation_info, fun() ->
                {error, internal_error}
            end),
            Header = make_header(?REQUEST_FED_INFO),
            Result = flurm_handler_query:handle(Header, <<>>),
            ?assertMatch({error, _}, Result)
        end}
     ]}.

%%====================================================================
%% Job User Info Extended Tests
%%====================================================================

job_user_info_extended_test_() ->
    {setup,
     fun() ->
         catch meck:unload(flurm_job_manager),
         meck:new(flurm_job_manager, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_job_manager),
         ok
     end,
     [
        {"Job user info with NO_VAL job_id", fun() ->
            Jobs = [make_job(7001)],
            meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),
            Header = make_header(?REQUEST_JOB_USER_INFO),
            Body = <<16#FFFFFFFE:32/big>>,  % NO_VAL
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, Body),
            ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
            ?assertEqual(1, Response#job_info_response.job_count)
        end},
        {"Job user info with short body defaults to all", fun() ->
            Jobs = [make_job(7002), make_job(7003)],
            meck:expect(flurm_job_manager, list_jobs, fun() -> Jobs end),
            Header = make_header(?REQUEST_JOB_USER_INFO),
            Body = <<1, 2>>,  % Too short
            {ok, MsgType, Response} = flurm_handler_query:handle(Header, Body),
            ?assertEqual(?RESPONSE_JOB_INFO, MsgType),
            ?assertEqual(2, Response#job_info_response.job_count)
        end}
     ]}.
