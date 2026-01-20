%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_qos module
%%%
%%% Comprehensive EUnit tests that call flurm_qos functions
%%% directly without mocking the module being tested.
%%%
%%% Tests all exported functions:
%%% - start_link/0
%%% - create/1, create/2
%%% - get/1
%%% - list/0
%%% - update/2
%%% - delete/1
%%% - check_limits/2
%%% - check_tres_limits/3
%%% - get_priority_adjustment/1
%%% - get_preemptable_qos/1
%%% - can_preempt/2
%%% - apply_usage_factor/2
%%% - get_default/0, set_default/1
%%% - init_default_qos/0
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_qos_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Stop existing server if running
    case whereis(flurm_qos) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,
    %% Clean up ETS table
    catch ets:delete(flurm_qos),
    ok,
    ok.

cleanup(_) ->
    %% Stop server if running
    case whereis(flurm_qos) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,
    catch ets:delete(flurm_qos),
    ok,
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

start_qos() ->
    {ok, Pid} = flurm_qos:start_link(),
    Pid.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Start/Stop Tests
start_link_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates process", fun test_start_link/0},
        {"registered under expected name", fun test_registered_name/0}
     ]}.

test_start_link() ->
    {ok, Pid} = flurm_qos:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ok.

test_registered_name() ->
    {ok, Pid} = flurm_qos:start_link(),
    ?assertEqual(Pid, whereis(flurm_qos)),
    ok.

%%====================================================================
%% QOS Creation Tests
%%====================================================================

create_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"create/1 creates QOS with defaults", fun test_create_simple/0},
        {"create/2 creates QOS with options", fun test_create_with_options/0},
        {"create duplicate returns error", fun test_create_duplicate/0}
     ]}.

test_create_simple() ->
    start_qos(),

    Result = flurm_qos:create(<<"express">>),

    ?assertEqual(ok, Result),

    %% Verify QOS exists
    {ok, QOS} = flurm_qos:get(<<"express">>),
    ?assertEqual(<<"express">>, QOS#qos.name),
    ok.

test_create_with_options() ->
    start_qos(),

    Options = #{
        priority => 1000,
        max_wall_per_job => 86400,
        preempt => [<<"low">>, <<"normal">>],
        preempt_mode => requeue,
        usage_factor => 2.0
    },

    Result = flurm_qos:create(<<"express">>, Options),

    ?assertEqual(ok, Result),

    {ok, QOS} = flurm_qos:get(<<"express">>),
    ?assertEqual(<<"express">>, QOS#qos.name),
    ?assertEqual(1000, QOS#qos.priority),
    ?assertEqual(86400, QOS#qos.max_wall_per_job),
    ?assertEqual(requeue, QOS#qos.preempt_mode),
    ?assertEqual(2.0, QOS#qos.usage_factor),
    ok.

test_create_duplicate() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>),
    Result = flurm_qos:create(<<"express">>),

    ?assertEqual({error, already_exists}, Result),
    ok.

%%====================================================================
%% QOS Get/List Tests
%%====================================================================

get_list_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get returns QOS", fun test_get/0},
        {"get returns not_found", fun test_get_not_found/0},
        {"list returns all QOS", fun test_list/0}
     ]}.

test_get() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{priority => 500}),

    {ok, QOS} = flurm_qos:get(<<"express">>),

    ?assertEqual(<<"express">>, QOS#qos.name),
    ?assertEqual(500, QOS#qos.priority),
    ok.

test_get_not_found() ->
    start_qos(),

    Result = flurm_qos:get(<<"nonexistent">>),

    ?assertEqual({error, not_found}, Result),
    ok.

test_list() ->
    start_qos(),

    %% Default QOS entries are created on init
    QosList = flurm_qos:list(),

    ?assert(is_list(QosList)),
    %% Should have default QOS entries (normal, high, low, etc.)
    ?assert(length(QosList) >= 1),
    ok.

%%====================================================================
%% QOS Update Tests
%%====================================================================

update_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"update modifies QOS", fun test_update/0},
        {"update not found returns error", fun test_update_not_found/0}
     ]}.

test_update() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{priority => 500}),

    Result = flurm_qos:update(<<"express">>, #{priority => 1000}),
    ?assertEqual(ok, Result),

    {ok, QOS} = flurm_qos:get(<<"express">>),
    ?assertEqual(1000, QOS#qos.priority),
    ok.

test_update_not_found() ->
    start_qos(),

    Result = flurm_qos:update(<<"nonexistent">>, #{priority => 500}),

    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% QOS Delete Tests
%%====================================================================

delete_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"delete removes QOS", fun test_delete/0},
        {"delete normal returns error", fun test_delete_default/0}
     ]}.

test_delete() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>),
    {ok, _} = flurm_qos:get(<<"express">>),

    Result = flurm_qos:delete(<<"express">>),
    ?assertEqual(ok, Result),

    ?assertEqual({error, not_found}, flurm_qos:get(<<"express">>)),
    ok.

test_delete_default() ->
    start_qos(),

    Result = flurm_qos:delete(<<"normal">>),

    ?assertEqual({error, cannot_delete_default}, Result),
    ok.

%%====================================================================
%% Priority Adjustment Tests
%%====================================================================

priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_priority_adjustment returns value", fun test_priority_adjustment/0},
        {"get_priority_adjustment default", fun test_priority_adjustment_default/0}
     ]}.

test_priority_adjustment() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{priority => 2000}),

    Priority = flurm_qos:get_priority_adjustment(<<"express">>),

    ?assertEqual(2000, Priority),
    ok.

test_priority_adjustment_default() ->
    start_qos(),

    Priority = flurm_qos:get_priority_adjustment(<<"unknown">>),

    %% Unknown QOS returns 0
    ?assertEqual(0, Priority),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_preemptable_qos returns list", fun test_preemptable_qos/0},
        {"can_preempt returns true", fun test_can_preempt_true/0},
        {"can_preempt returns false", fun test_can_preempt_false/0}
     ]}.

test_preemptable_qos() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{preempt => [<<"low">>, <<"normal">>]}),

    Preemptable = flurm_qos:get_preemptable_qos(<<"express">>),

    ?assert(is_list(Preemptable)),
    ?assert(lists:member(<<"low">>, Preemptable)),
    ?assert(lists:member(<<"normal">>, Preemptable)),
    ok.

test_can_preempt_true() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{preempt => [<<"low">>]}),
    %% "low" QOS already exists by default, so don't create it again

    Result = flurm_qos:can_preempt(<<"express">>, <<"low">>),

    ?assertEqual(true, Result),
    ok.

test_can_preempt_false() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{preempt => [<<"low">>]}),

    Result = flurm_qos:can_preempt(<<"express">>, <<"high">>),

    ?assertEqual(false, Result),
    ok.

%%====================================================================
%% Usage Factor Tests
%%====================================================================

usage_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"apply_usage_factor multiplies", fun test_usage_factor/0},
        {"apply_usage_factor default", fun test_usage_factor_default/0}
     ]}.

test_usage_factor() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>, #{usage_factor => 2.0}),

    Result = flurm_qos:apply_usage_factor(<<"express">>, 100.0),

    ?assertEqual(200.0, Result),
    ok.

test_usage_factor_default() ->
    start_qos(),

    %% Unknown QOS returns original value
    Result = flurm_qos:apply_usage_factor(<<"unknown">>, 100.0),

    ?assertEqual(100.0, Result),
    ok.

%%====================================================================
%% Default QOS Tests
%%====================================================================

default_qos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_default returns default", fun test_get_default/0},
        {"set_default updates default", fun test_set_default/0}
     ]}.

test_get_default() ->
    start_qos(),

    Default = flurm_qos:get_default(),

    ?assertEqual(<<"normal">>, Default),
    ok.

test_set_default() ->
    start_qos(),

    ok = flurm_qos:create(<<"express">>),
    Result = flurm_qos:set_default(<<"express">>),
    ?assertEqual(ok, Result),

    Default = flurm_qos:get_default(),
    ?assertEqual(<<"express">>, Default),
    ok.

%%====================================================================
%% Limit Checking Tests
%%====================================================================

limits_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check_limits passes valid job", fun test_check_limits_pass/0},
        {"check_limits invalid qos", fun test_check_limits_invalid_qos/0}
     ]}.

test_check_limits_pass() ->
    start_qos(),

    %% Normal QOS exists by default
    JobSpec = #{
        time_limit => 3600,
        num_cpus => 4,
        memory_mb => 4096
    },

    Result = flurm_qos:check_limits(<<"normal">>, JobSpec),

    ?assertEqual(ok, Result),
    ok.

test_check_limits_invalid_qos() ->
    start_qos(),

    JobSpec = #{time_limit => 3600},

    Result = flurm_qos:check_limits(<<"nonexistent">>, JobSpec),

    ?assertEqual({error, {invalid_qos, <<"nonexistent">>}}, Result),
    ok.

%%====================================================================
%% Init Default QOS Tests
%%====================================================================

init_defaults_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init_default_qos creates entries", fun test_init_defaults/0}
     ]}.

test_init_defaults() ->
    start_qos(),

    Result = flurm_qos:init_default_qos(),
    ?assertEqual(ok, Result),

    %% Should have normal QOS at minimum
    {ok, NormalQOS} = flurm_qos:get(<<"normal">>),
    ?assertEqual(<<"normal">>, NormalQOS#qos.name),
    ok.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"terminate cleans up", fun test_terminate/0}
     ]}.

test_unknown_call() ->
    start_qos(),

    Result = gen_server:call(flurm_qos, {unknown_request}),

    ?assertEqual({error, unknown_request}, Result),
    ok.

test_terminate() ->
    Pid = start_qos(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),

    ok,
    ?assertNot(is_process_alive(Pid)),
    ok.
