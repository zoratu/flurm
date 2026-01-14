%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_license module
%%%
%%% Tests all exported functions directly without any mocking.
%%% Tests gen_server callbacks directly for coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_license_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% ETS table names (must match module)
-define(LICENSE_TABLE, flurm_licenses).
-define(LICENSE_ALLOC_TABLE, flurm_license_allocations).

%% License record definition (must match module)
-record(license, {
    name :: binary(),
    total :: non_neg_integer(),
    in_use :: non_neg_integer(),
    reserved :: non_neg_integer(),
    remote :: boolean(),
    server :: binary() | undefined,
    port :: non_neg_integer() | undefined
}).

%% License allocation record
-record(license_alloc, {
    id :: {pos_integer(), binary()},
    job_id :: pos_integer(),
    license :: binary(),
    count :: non_neg_integer(),
    alloc_time :: non_neg_integer()
}).

%% gen_server state record
-record(state, {}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup helper that creates ETS tables manually (without calling init)
setup_ets_tables() ->
    %% Delete tables if they exist
    catch ets:delete(?LICENSE_TABLE),
    catch ets:delete(?LICENSE_ALLOC_TABLE),
    %% Create tables
    ets:new(?LICENSE_TABLE, [
        named_table, public, set,
        {keypos, #license.name}
    ]),
    ets:new(?LICENSE_ALLOC_TABLE, [
        named_table, public, set,
        {keypos, #license_alloc.id}
    ]),
    ok.

cleanup_ets_tables() ->
    catch ets:delete(?LICENSE_TABLE),
    catch ets:delete(?LICENSE_ALLOC_TABLE),
    ok.

%% Insert a test license directly into ETS
insert_test_license(Name, Total) ->
    insert_test_license(Name, Total, 0, 0, false).

insert_test_license(Name, Total, InUse, Reserved, Remote) ->
    License = #license{
        name = Name,
        total = Total,
        in_use = InUse,
        reserved = Reserved,
        remote = Remote,
        server = undefined,
        port = undefined
    },
    ets:insert(?LICENSE_TABLE, License).

insert_test_license_full(Name, Total, InUse, Reserved, Remote, Server, Port) ->
    License = #license{
        name = Name,
        total = Total,
        in_use = InUse,
        reserved = Reserved,
        remote = Remote,
        server = Server,
        port = Port
    },
    ets:insert(?LICENSE_TABLE, License).

%% Insert a test allocation directly into ETS
insert_test_allocation(JobId, LicenseName, Count) ->
    Alloc = #license_alloc{
        id = {JobId, LicenseName},
        job_id = JobId,
        license = LicenseName,
        count = Count,
        alloc_time = erlang:system_time(second)
    },
    ets:insert(?LICENSE_ALLOC_TABLE, Alloc).

%%====================================================================
%% parse_license_spec Tests (Pure Function - No ETS needed)
%%====================================================================

parse_license_spec_test_() ->
    [
        {"empty binary spec returns empty list",
         ?_assertEqual({ok, []}, flurm_license:parse_license_spec(<<>>))},

        {"empty string spec returns empty list",
         ?_assertEqual({ok, []}, flurm_license:parse_license_spec(""))},

        {"single license with count",
         ?_assertEqual({ok, [{<<"matlab">>, 2}]},
                       flurm_license:parse_license_spec(<<"matlab:2">>))},

        {"single license without count defaults to 1",
         ?_assertEqual({ok, [{<<"matlab">>, 1}]},
                       flurm_license:parse_license_spec(<<"matlab">>))},

        {"multiple licenses",
         ?_assertEqual({ok, [{<<"matlab">>, 2}, {<<"ansys">>, 1}]},
                       flurm_license:parse_license_spec(<<"matlab:2,ansys:1">>))},

        {"string input converted to binary",
         ?_assertEqual({ok, [{<<"matlab">>, 2}]},
                       flurm_license:parse_license_spec("matlab:2"))},

        {"whitespace trimmed in name",
         ?_assertEqual({ok, [{<<"matlab">>, 2}]},
                       flurm_license:parse_license_spec(<<" matlab :2">>))},

        {"whitespace trimmed in count",
         ?_assertEqual({ok, [{<<"matlab">>, 2}]},
                       flurm_license:parse_license_spec(<<"matlab: 2 ">>))},

        {"multiple licenses with whitespace",
         ?_assertEqual({ok, [{<<"matlab">>, 2}, {<<"ansys">>, 3}]},
                       flurm_license:parse_license_spec(<<" matlab : 2 , ansys : 3 ">>))},

        {"single license name only in list",
         ?_assertEqual({ok, [{<<"matlab">>, 1}, {<<"ansys">>, 1}]},
                       flurm_license:parse_license_spec(<<"matlab,ansys">>))},

        {"empty name returns error",
         ?_assertMatch({error, {invalid_license_spec, empty_name}},
                       flurm_license:parse_license_spec(<<":2">>))},

        {"zero count returns error",
         ?_assertMatch({error, {invalid_license_spec, {invalid_count, 0}}},
                       flurm_license:parse_license_spec(<<"matlab:0">>))},

        {"negative count returns error",
         ?_assertMatch({error, {invalid_license_spec, {invalid_count, -1}}},
                       flurm_license:parse_license_spec(<<"matlab:-1">>))},

        {"invalid count string returns error",
         ?_assertMatch({error, {invalid_license_spec, {invalid_count, _}}},
                       flurm_license:parse_license_spec(<<"matlab:abc">>))},

        {"large count accepted",
         ?_assertEqual({ok, [{<<"matlab">>, 1000}]},
                       flurm_license:parse_license_spec(<<"matlab:1000">>))}
    ].

%%====================================================================
%% list/0 Tests
%%====================================================================

list_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"list returns empty when no licenses",
         fun() ->
             ?assertEqual([], flurm_license:list())
         end},

        {"list returns single license",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             Result = flurm_license:list(),
             ?assertEqual(1, length(Result)),
             [License] = Result,
             ?assertEqual(<<"matlab">>, maps:get(name, License)),
             ?assertEqual(10, maps:get(total, License)),
             ?assertEqual(0, maps:get(in_use, License)),
             ?assertEqual(10, maps:get(available, License)),
             ?assertEqual(false, maps:get(remote, License))
         end},

        {"list returns multiple licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             Result = flurm_license:list(),
             ?assertEqual(2, length(Result)),
             Names = lists:sort([maps:get(name, L) || L <- Result]),
             ?assertEqual([<<"ansys">>, <<"matlab">>], Names)
         end},

        {"list shows correct available count with usage",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 3, 2, false),
             [License] = flurm_license:list(),
             ?assertEqual(10, maps:get(total, License)),
             ?assertEqual(3, maps:get(in_use, License)),
             ?assertEqual(2, maps:get(reserved, License)),
             ?assertEqual(5, maps:get(available, License))
         end},

        {"list shows remote license info",
         fun() ->
             insert_test_license_full(<<"flexlm">>, 100, 0, 0, true,
                                      <<"license.server.com">>, 27000),
             [License] = flurm_license:list(),
             ?assertEqual(true, maps:get(remote, License)),
             ?assertEqual(<<"license.server.com">>, maps:get(server, License)),
             ?assertEqual(27000, maps:get(port, License))
         end}
     ]}.

%% Test list when table doesn't exist
list_no_table_test() ->
    catch ets:delete(?LICENSE_TABLE),
    catch ets:delete(?LICENSE_ALLOC_TABLE),
    ?assertEqual([], flurm_license:list()).

%%====================================================================
%% get/1 Tests
%%====================================================================

get_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get returns not_found for missing license",
         fun() ->
             ?assertEqual({error, not_found}, flurm_license:get(<<"missing">>))
         end},

        {"get returns license info",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 2, 1, false),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(<<"matlab">>, maps:get(name, License)),
             ?assertEqual(10, maps:get(total, License)),
             ?assertEqual(2, maps:get(in_use, License)),
             ?assertEqual(1, maps:get(reserved, License)),
             ?assertEqual(7, maps:get(available, License))
         end},

        {"get returns remote license with server info",
         fun() ->
             insert_test_license_full(<<"flexlm">>, 50, 10, 5, true,
                                      <<"lic.example.com">>, 1055),
             {ok, License} = flurm_license:get(<<"flexlm">>),
             ?assertEqual(true, maps:get(remote, License)),
             ?assertEqual(<<"lic.example.com">>, maps:get(server, License)),
             ?assertEqual(1055, maps:get(port, License)),
             ?assertEqual(35, maps:get(available, License))
         end}
     ]}.

%%====================================================================
%% exists/1 Tests
%%====================================================================

exists_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"exists returns false for missing license",
         fun() ->
             ?assertEqual(false, flurm_license:exists(<<"missing">>))
         end},

        {"exists returns true for existing license",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(true, flurm_license:exists(<<"matlab">>))
         end}
     ]}.

%%====================================================================
%% get_available/1 Tests
%%====================================================================

get_available_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get_available returns 0 for missing license",
         fun() ->
             ?assertEqual(0, flurm_license:get_available(<<"missing">>))
         end},

        {"get_available returns total when nothing in use",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>))
         end},

        {"get_available subtracts in_use",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 3, 0, false),
             ?assertEqual(7, flurm_license:get_available(<<"matlab">>))
         end},

        {"get_available subtracts reserved",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 0, 4, false),
             ?assertEqual(6, flurm_license:get_available(<<"matlab">>))
         end},

        {"get_available subtracts both in_use and reserved",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 3, 2, false),
             ?assertEqual(5, flurm_license:get_available(<<"matlab">>))
         end},

        {"get_available returns 0 when overallocated",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 8, 5, false),
             ?assertEqual(0, flurm_license:get_available(<<"matlab">>))
         end}
     ]}.

%%====================================================================
%% check_availability/1 Tests
%%====================================================================

check_availability_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"check_availability returns true for empty list",
         fun() ->
             ?assertEqual(true, flurm_license:check_availability([]))
         end},

        {"check_availability returns false when license missing",
         fun() ->
             ?assertEqual(false, flurm_license:check_availability([{<<"missing">>, 1}]))
         end},

        {"check_availability returns true when sufficient",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 5}]))
         end},

        {"check_availability returns true when exactly enough",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 10}]))
         end},

        {"check_availability returns false when insufficient",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(false, flurm_license:check_availability([{<<"matlab">>, 11}]))
         end},

        {"check_availability handles multiple licenses - all sufficient",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             ?assertEqual(true, flurm_license:check_availability([
                 {<<"matlab">>, 5},
                 {<<"ansys">>, 3}
             ]))
         end},

        {"check_availability handles multiple licenses - one insufficient",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             ?assertEqual(false, flurm_license:check_availability([
                 {<<"matlab">>, 5},
                 {<<"ansys">>, 10}
             ]))
         end},

        {"check_availability accounts for in_use",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 7, 0, false),
             ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 3}])),
             ?assertEqual(false, flurm_license:check_availability([{<<"matlab">>, 4}]))
         end},

        {"check_availability accounts for reserved",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 0, 6, false),
             ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 4}])),
             ?assertEqual(false, flurm_license:check_availability([{<<"matlab">>, 5}]))
         end}
     ]}.

%%====================================================================
%% validate_licenses/1 Tests
%%====================================================================

validate_licenses_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"validate_licenses returns ok for empty list",
         fun() ->
             ?assertEqual(ok, flurm_license:validate_licenses([]))
         end},

        {"validate_licenses returns error for missing license",
         fun() ->
             ?assertEqual({error, {unknown_license, <<"missing">>}},
                         flurm_license:validate_licenses([{<<"missing">>, 1}]))
         end},

        {"validate_licenses returns ok for existing license",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual(ok, flurm_license:validate_licenses([{<<"matlab">>, 5}]))
         end},

        {"validate_licenses checks all licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             ?assertEqual(ok, flurm_license:validate_licenses([
                 {<<"matlab">>, 5},
                 {<<"ansys">>, 3}
             ]))
         end},

        {"validate_licenses returns first unknown license",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             ?assertEqual({error, {unknown_license, <<"missing">>}},
                         flurm_license:validate_licenses([
                             {<<"matlab">>, 5},
                             {<<"missing">>, 1}
                         ]))
         end}
     ]}.

%%====================================================================
%% gen_server init/1 Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun() ->
         %% Clean up any existing tables
         catch ets:delete(?LICENSE_TABLE),
         catch ets:delete(?LICENSE_ALLOC_TABLE),
         %% Clear any configured licenses
         application:unset_env(flurm_core, licenses),
         ok
     end,
     fun(_) ->
         cleanup_ets_tables(),
         application:unset_env(flurm_core, licenses)
     end,
     [
        {"init creates ETS tables",
         fun() ->
             {ok, _State} = flurm_license:init([]),
             ?assertNotEqual(undefined, ets:info(?LICENSE_TABLE)),
             ?assertNotEqual(undefined, ets:info(?LICENSE_ALLOC_TABLE))
         end},

        {"init returns state record",
         fun() ->
             {ok, State} = flurm_license:init([]),
             ?assertEqual(#state{}, State)
         end},

        {"init loads configured licenses",
         fun() ->
             application:set_env(flurm_core, licenses, [
                 {<<"matlab">>, #{total => 10}},
                 {<<"ansys">>, #{total => 5, remote => true}}
             ]),
             {ok, _State} = flurm_license:init([]),
             ?assertEqual(2, ets:info(?LICENSE_TABLE, size)),
             ?assertEqual(true, flurm_license:exists(<<"matlab">>)),
             ?assertEqual(true, flurm_license:exists(<<"ansys">>))
         end},

        {"init handles empty license config",
         fun() ->
             application:set_env(flurm_core, licenses, []),
             {ok, _State} = flurm_license:init([]),
             ?assertEqual(0, ets:info(?LICENSE_TABLE, size))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - add_license
%%====================================================================

handle_call_add_license_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call add_license creates new license",
         fun() ->
             State = #state{},
             Config = #{total => 10},
             {reply, ok, NewState} = flurm_license:handle_call(
                 {add_license, <<"matlab">>, Config}, {self(), make_ref()}, State),
             ?assertEqual(State, NewState),
             ?assertEqual(true, flurm_license:exists(<<"matlab">>)),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>))
         end},

        {"handle_call add_license with all options",
         fun() ->
             State = #state{},
             Config = #{
                 total => 100,
                 remote => true,
                 server => <<"license.server.com">>,
                 port => 27000
             },
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"flexlm">>, Config}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"flexlm">>),
             ?assertEqual(100, maps:get(total, License)),
             ?assertEqual(true, maps:get(remote, License)),
             ?assertEqual(<<"license.server.com">>, maps:get(server, License)),
             ?assertEqual(27000, maps:get(port, License))
         end},

        {"handle_call add_license defaults total to 0",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"test">>, #{}}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"test">>),
             ?assertEqual(0, maps:get(total, License))
         end},

        {"handle_call add_license defaults remote to false",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"test">>, #{total => 5}}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"test">>),
             ?assertEqual(false, maps:get(remote, License))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - remove_license
%%====================================================================

handle_call_remove_license_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call remove_license removes existing license",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {remove_license, <<"matlab">>}, {self(), make_ref()}, State),
             ?assertEqual(false, flurm_license:exists(<<"matlab">>))
         end},

        {"handle_call remove_license fails when license in use",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_allocation(1001, <<"matlab">>, 2),
             State = #state{},
             {reply, Result, _} = flurm_license:handle_call(
                 {remove_license, <<"matlab">>}, {self(), make_ref()}, State),
             ?assertEqual({error, license_in_use}, Result),
             ?assertEqual(true, flurm_license:exists(<<"matlab">>))
         end},

        {"handle_call remove_license succeeds with other license allocations",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             insert_test_allocation(1001, <<"ansys">>, 2),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {remove_license, <<"matlab">>}, {self(), make_ref()}, State),
             ?assertEqual(false, flurm_license:exists(<<"matlab">>)),
             ?assertEqual(true, flurm_license:exists(<<"ansys">>))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - allocate
%%====================================================================

handle_call_allocate_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call allocate succeeds with sufficient licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"matlab">>, 3}]}, {self(), make_ref()}, State),
             ?assertEqual(7, flurm_license:get_available(<<"matlab">>)),
             %% Check allocation record was created
             ?assertMatch([_], ets:lookup(?LICENSE_ALLOC_TABLE, {1001, <<"matlab">>}))
         end},

        {"handle_call allocate fails with insufficient licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, Result, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"matlab">>, 15}]}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>))
         end},

        {"handle_call allocate handles multiple licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             insert_test_license(<<"ansys">>, 5),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"matlab">>, 3}, {<<"ansys">>, 2}]},
                 {self(), make_ref()}, State),
             ?assertEqual(7, flurm_license:get_available(<<"matlab">>)),
             ?assertEqual(3, flurm_license:get_available(<<"ansys">>))
         end},

        {"handle_call allocate empty list succeeds",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1001, []}, {self(), make_ref()}, State)
         end},

        {"handle_call allocate for missing license succeeds (no-op for missing)",
         fun() ->
             %% When license doesn't exist, availability is 0, so it fails
             State = #state{},
             {reply, Result, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"missing">>, 1}]}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - deallocate
%%====================================================================

handle_call_deallocate_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call deallocate returns licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 3, 0, false),
             insert_test_allocation(1001, <<"matlab">>, 3),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, [{<<"matlab">>, 3}]}, {self(), make_ref()}, State),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>)),
             %% Check allocation record was removed
             ?assertEqual([], ets:lookup(?LICENSE_ALLOC_TABLE, {1001, <<"matlab">>}))
         end},

        {"handle_call deallocate handles empty list",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, []}, {self(), make_ref()}, State)
         end},

        {"handle_call deallocate handles missing license gracefully",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, [{<<"missing">>, 3}]}, {self(), make_ref()}, State)
         end},

        {"handle_call deallocate doesn't go below zero",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 1, 0, false),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, [{<<"matlab">>, 5}]}, {self(), make_ref()}, State),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>))
         end},

        {"handle_call deallocate multiple licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 5, 0, false),
             insert_test_license(<<"ansys">>, 5, 3, 0, false),
             insert_test_allocation(1001, <<"matlab">>, 5),
             insert_test_allocation(1001, <<"ansys">>, 3),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, [{<<"matlab">>, 5}, {<<"ansys">>, 3}]},
                 {self(), make_ref()}, State),
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>)),
             ?assertEqual(5, flurm_license:get_available(<<"ansys">>))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - reserve
%%====================================================================

handle_call_reserve_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call reserve succeeds with sufficient licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {reserve, <<"matlab">>, 3}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(3, maps:get(reserved, License)),
             ?assertEqual(7, maps:get(available, License))
         end},

        {"handle_call reserve fails with insufficient licenses",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 8, 0, false),
             State = #state{},
             {reply, Result, _} = flurm_license:handle_call(
                 {reserve, <<"matlab">>, 5}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result)
         end},

        {"handle_call reserve fails for missing license",
         fun() ->
             State = #state{},
             {reply, Result, _} = flurm_license:handle_call(
                 {reserve, <<"missing">>, 3}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, Result)
         end},

        {"handle_call reserve accounts for existing reservations",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 0, 6, false),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {reserve, <<"matlab">>, 2}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(8, maps:get(reserved, License)),
             %% Cannot reserve more
             {reply, Result, _} = flurm_license:handle_call(
                 {reserve, <<"matlab">>, 3}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - unreserve
%%====================================================================

handle_call_unreserve_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call unreserve releases reservations",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 0, 5, false),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {unreserve, <<"matlab">>, 3}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(2, maps:get(reserved, License)),
             ?assertEqual(8, maps:get(available, License))
         end},

        {"handle_call unreserve doesn't go below zero",
         fun() ->
             insert_test_license(<<"matlab">>, 10, 0, 2, false),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {unreserve, <<"matlab">>, 5}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(0, maps:get(reserved, License))
         end},

        {"handle_call unreserve handles missing license gracefully",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {unreserve, <<"missing">>, 3}, {self(), make_ref()}, State)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - unknown request
%%====================================================================

handle_call_unknown_test() ->
    State = #state{},
    {reply, Result, NewState} = flurm_license:handle_call(
        unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% gen_server handle_cast Tests
%%====================================================================

handle_cast_test() ->
    State = #state{},
    {noreply, NewState} = flurm_license:handle_cast(some_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% gen_server handle_info Tests
%%====================================================================

handle_info_test() ->
    State = #state{},
    {noreply, NewState} = flurm_license:handle_info(some_info, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% gen_server terminate Tests
%%====================================================================

terminate_test() ->
    State = #state{},
    ?assertEqual(ok, flurm_license:terminate(normal, State)),
    ?assertEqual(ok, flurm_license:terminate(shutdown, State)),
    ?assertEqual(ok, flurm_license:terminate({error, reason}, State)).

%%====================================================================
%% gen_server code_change Tests
%%====================================================================

code_change_test() ->
    State = #state{},
    ?assertEqual({ok, State}, flurm_license:code_change("1.0", State, [])),
    ?assertEqual({ok, State}, flurm_license:code_change("2.0", State, extra)).

%%====================================================================
%% Integration-style Tests (Using ETS Directly)
%%====================================================================

allocation_workflow_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"full allocation workflow",
         fun() ->
             %% Add license via handle_call
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"matlab">>, #{total => 10}}, {self(), make_ref()}, State),

             %% Verify initial state
             ?assertEqual(10, flurm_license:get_available(<<"matlab">>)),
             ?assertEqual(true, flurm_license:check_availability([{<<"matlab">>, 5}])),

             %% Allocate for job 1001
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"matlab">>, 3}]}, {self(), make_ref()}, State),
             ?assertEqual(7, flurm_license:get_available(<<"matlab">>)),

             %% Allocate for job 1002
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1002, [{<<"matlab">>, 4}]}, {self(), make_ref()}, State),
             ?assertEqual(3, flurm_license:get_available(<<"matlab">>)),

             %% Cannot allocate more than available
             {reply, Result, _} = flurm_license:handle_call(
                 {allocate, 1003, [{<<"matlab">>, 5}]}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result),

             %% Deallocate job 1001
             {reply, ok, _} = flurm_license:handle_call(
                 {deallocate, 1001, [{<<"matlab">>, 3}]}, {self(), make_ref()}, State),
             ?assertEqual(6, flurm_license:get_available(<<"matlab">>)),

             %% Now allocation should succeed
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1003, [{<<"matlab">>, 5}]}, {self(), make_ref()}, State),
             ?assertEqual(1, flurm_license:get_available(<<"matlab">>))
         end},

        {"reservation workflow",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"ansys">>, #{total => 20}}, {self(), make_ref()}, State),

             %% Reserve some licenses
             {reply, ok, _} = flurm_license:handle_call(
                 {reserve, <<"ansys">>, 5}, {self(), make_ref()}, State),
             ?assertEqual(15, flurm_license:get_available(<<"ansys">>)),

             %% Allocate from non-reserved pool
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 2001, [{<<"ansys">>, 10}]}, {self(), make_ref()}, State),
             ?assertEqual(5, flurm_license:get_available(<<"ansys">>)),

             %% Cannot allocate more (5 available, 5 reserved)
             {reply, Result, _} = flurm_license:handle_call(
                 {allocate, 2002, [{<<"ansys">>, 6}]}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result),

             %% Unreserve to make more available
             {reply, ok, _} = flurm_license:handle_call(
                 {unreserve, <<"ansys">>, 3}, {self(), make_ref()}, State),
             ?assertEqual(8, flurm_license:get_available(<<"ansys">>)),

             %% Now allocation should work
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 2002, [{<<"ansys">>, 6}]}, {self(), make_ref()}, State),
             ?assertEqual(2, flurm_license:get_available(<<"ansys">>))
         end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"zero total license",
         fun() ->
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {add_license, <<"test">>, #{total => 0}}, {self(), make_ref()}, State),
             ?assertEqual(0, flurm_license:get_available(<<"test">>)),
             {reply, Result, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"test">>, 1}]}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_licenses}, Result)
         end},

        {"allocate exactly all available",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {allocate, 1001, [{<<"matlab">>, 10}]}, {self(), make_ref()}, State),
             ?assertEqual(0, flurm_license:get_available(<<"matlab">>))
         end},

        {"reserve exactly all available",
         fun() ->
             insert_test_license(<<"matlab">>, 10),
             State = #state{},
             {reply, ok, _} = flurm_license:handle_call(
                 {reserve, <<"matlab">>, 10}, {self(), make_ref()}, State),
             {ok, License} = flurm_license:get(<<"matlab">>),
             ?assertEqual(10, maps:get(reserved, License)),
             ?assertEqual(0, maps:get(available, License))
         end},

        {"multiple jobs same license",
         fun() ->
             insert_test_license(<<"matlab">>, 100),
             State = #state{},
             %% Allocate for multiple jobs
             lists:foreach(fun(JobId) ->
                 {reply, ok, _} = flurm_license:handle_call(
                     {allocate, JobId, [{<<"matlab">>, 10}]}, {self(), make_ref()}, State)
             end, lists:seq(1, 10)),
             ?assertEqual(0, flurm_license:get_available(<<"matlab">>)),

             %% Deallocate all
             lists:foreach(fun(JobId) ->
                 {reply, ok, _} = flurm_license:handle_call(
                     {deallocate, JobId, [{<<"matlab">>, 10}]}, {self(), make_ref()}, State)
             end, lists:seq(1, 10)),
             ?assertEqual(100, flurm_license:get_available(<<"matlab">>))
         end}
     ]}.
