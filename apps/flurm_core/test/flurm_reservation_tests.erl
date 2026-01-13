%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_reservation module
%%%
%%% Tests reservation creation, management, access control, and
%%% scheduler integration.
%%%
%%% Note: The reservation record is internal to the module, so these
%%% tests use the public API functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_reservation_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

-compile(nowarn_unused_function).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the reservation gen_server
    case whereis(flurm_reservation) of
        undefined ->
            {ok, Pid} = flurm_reservation:start_link(),
            #{reservation_pid => Pid};
        Pid ->
            %% Already running
            #{reservation_pid => Pid}
    end.

cleanup(#{reservation_pid := Pid}) ->
    %% Delete all reservations using list/0 which returns internal records
    %% We'll use list_active/0 and list/0 to get names indirectly
    AllRes = flurm_reservation:list(),
    %% Get the names from the reservation tuples (record is {reservation, name, ...})
    lists:foreach(fun(Res) ->
        %% The record is a tuple where element 2 is the name
        Name = element(2, Res),
        catch flurm_reservation:delete(Name)
    end, AllRes),
    %% Stop the server if we started it
    catch gen_server:stop(Pid, shutdown, 5000),
    ok;
cleanup(_) ->
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_reservation_spec() ->
    make_reservation_spec(#{}).

make_reservation_spec(Overrides) ->
    Now = erlang:system_time(second),
    Defaults = #{
        name => <<"test_reservation">>,
        type => user,
        nodes => [<<"node1">>, <<"node2">>],
        start_time => Now,
        end_time => Now + 3600,
        users => [<<"testuser">>],
        accounts => [],
        partition => <<"default">>,
        flags => []
    },
    maps:merge(Defaults, Overrides).

unique_name() ->
    Rand = rand:uniform(1000000),
    iolist_to_binary([<<"test_res_">>, integer_to_binary(Rand)]).

get_reservation_name(Res) ->
    %% Extract name from reservation record (element 2)
    element(2, Res).

%%====================================================================
%% Basic Reservation Tests
%%====================================================================

reservation_basic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"create reservation", fun test_create_reservation/0},
        {"get reservation", fun test_get_reservation/0},
        {"list reservations", fun test_list_reservations/0},
        {"list active reservations", fun test_list_active_reservations/0},
        {"delete reservation", fun test_delete_reservation/0},
        {"get non-existent reservation", fun test_get_nonexistent/0}
     ]}.

test_create_reservation() ->
    Spec = make_reservation_spec(#{name => unique_name()}),
    {ok, Name} = flurm_reservation:create(Spec),
    ?assert(is_binary(Name)).

test_get_reservation() ->
    Name = unique_name(),
    Spec = make_reservation_spec(#{name => Name}),
    {ok, _} = flurm_reservation:create(Spec),
    {ok, Res} = flurm_reservation:get(Name),
    ?assertEqual(Name, get_reservation_name(Res)).

test_list_reservations() ->
    Name1 = unique_name(),
    Name2 = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name1})),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name2})),
    List = flurm_reservation:list(),
    Names = [get_reservation_name(R) || R <- List],
    ?assert(lists:member(Name1, Names)),
    ?assert(lists:member(Name2, Names)).

test_list_active_reservations() ->
    Now = erlang:system_time(second),
    %% Create active reservation
    ActiveName = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => ActiveName,
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    %% Activate it
    flurm_reservation:activate_reservation(ActiveName),

    Active = flurm_reservation:list_active(),
    ?assert(is_list(Active)).

test_delete_reservation() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    ok = flurm_reservation:delete(Name),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_get_nonexistent() ->
    ?assertEqual({error, not_found}, flurm_reservation:get(<<"nonexistent_reservation">>)).

%%====================================================================
%% Reservation State Tests
%%====================================================================

reservation_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"activate reservation", fun test_activate_reservation/0},
        {"deactivate reservation", fun test_deactivate_reservation/0},
        {"activate nonexistent", fun test_activate_nonexistent/0}
     ]}.

test_activate_reservation() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    Result = flurm_reservation:activate_reservation(Name),
    ?assertEqual(ok, Result).

test_deactivate_reservation() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    flurm_reservation:activate_reservation(Name),
    Result = flurm_reservation:deactivate_reservation(Name),
    ?assertEqual(ok, Result).

test_activate_nonexistent() ->
    Result = flurm_reservation:activate_reservation(<<"nonexistent">>),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% User Access Tests
%%====================================================================

user_access_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"can use reservation - authorized user", fun test_can_use_authorized/0},
        {"cannot use reservation - unauthorized user", fun test_cannot_use_unauthorized/0},
        {"can use reservation - open access", fun test_can_use_open_access/0},
        {"get reservations for user", fun test_get_reservations_for_user/0}
     ]}.

test_can_use_authorized() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"user1">>, <<"user2">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    ?assert(flurm_reservation:can_use_reservation(<<"user1">>, Name)).

test_cannot_use_unauthorized() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"user1">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    ?assertNot(flurm_reservation:can_use_reservation(<<"unauthorized_user">>, Name)).

test_can_use_open_access() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [],  % Empty users = open access
        accounts => [],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    ?assert(flurm_reservation:can_use_reservation(<<"anyone">>, Name)).

test_get_reservations_for_user() ->
    Name1 = unique_name(),
    Name2 = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name1,
        users => [<<"testuser">>]
    })),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name2,
        users => [<<"otheruser">>]
    })),
    UserRes = flurm_reservation:get_reservations_for_user(<<"testuser">>),
    UserNames = [get_reservation_name(R) || R <- UserRes],
    ?assert(lists:member(Name1, UserNames)).

%%====================================================================
%% Node Reservation Tests
%%====================================================================

node_reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"is node reserved - during reservation", fun test_is_node_reserved_active/0},
        {"is node reserved - outside window", fun test_is_node_not_reserved/0},
        {"get reservations for node", fun test_get_reservations_for_node/0},
        {"get reserved nodes", fun test_get_reserved_nodes/0}
     ]}.

test_is_node_reserved_active() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => [<<"reserved_node">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    %% Node should be reserved now
    ?assert(flurm_reservation:is_node_reserved(<<"reserved_node">>, Now)).

test_is_node_not_reserved() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => [<<"future_node">>],
        start_time => Now + 7200,  % 2 hours in future
        end_time => Now + 10800
    })),
    %% Node should not be reserved now
    ?assertNot(flurm_reservation:is_node_reserved(<<"future_node">>, Now)).

test_get_reservations_for_node() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => [<<"compute01">>]
    })),
    Reservations = flurm_reservation:get_reservations_for_node(<<"compute01">>),
    Names = [get_reservation_name(R) || R <- Reservations],
    ?assert(lists:member(Name, Names)).

test_get_reserved_nodes() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => Nodes,
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    {ok, ReservedNodes} = flurm_reservation:get_reserved_nodes(Name),
    ?assertEqual(Nodes, ReservedNodes).

%%====================================================================
%% Job Reservation API Tests
%%====================================================================

job_reservation_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"create job reservation", fun test_create_job_reservation/0},
        {"cancel job reservation", fun test_cancel_job_reservation/0},
        {"confirm job reservation", fun test_confirm_job_reservation/0}
     ]}.

test_create_job_reservation() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => unique_name(),
        nodes => [<<"job_node">>],
        start_time => Now,
        end_time => Now + 3600,
        user => <<"jobuser">>
    },
    Result = flurm_reservation:create_reservation(Spec),
    ?assertMatch({ok, _}, Result).

test_cancel_job_reservation() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    Spec = #{
        name => Name,
        nodes => [<<"cancel_node">>],
        start_time => Now,
        end_time => Now + 3600,
        user => <<"canceluser">>
    },
    {ok, _} = flurm_reservation:create_reservation(Spec),
    Result = flurm_reservation:cancel_reservation(Name),
    ?assertEqual(ok, Result),
    ?assertEqual({error, not_found}, flurm_reservation:get(Name)).

test_confirm_job_reservation() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    Spec = #{
        name => Name,
        nodes => [<<"confirm_node">>],
        start_time => Now,
        end_time => Now + 3600,
        user => <<"confirmuser">>
    },
    {ok, _} = flurm_reservation:create_reservation(Spec),
    Result = flurm_reservation:confirm_reservation(Name),
    ?assertEqual(ok, Result).

%%====================================================================
%% Scheduler Integration Tests
%%====================================================================

scheduler_integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check job reservation - no reservation", fun test_check_job_no_reservation/0},
        {"get active reservations", fun test_get_active_reservations/0}
     ]}.

test_check_job_no_reservation() ->
    Job = #job{id = 1, reservation = <<>>, user = <<"user1">>},
    Result = flurm_reservation:check_job_reservation(Job),
    ?assertEqual({ok, no_reservation}, Result).

test_get_active_reservations() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),
    Active = flurm_reservation:get_active_reservations(),
    ?assert(is_list(Active)).

%%====================================================================
%% Update Reservation Tests
%%====================================================================

update_reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"update reservation end time", fun test_update_end_time/0},
        {"update reservation users", fun test_update_users/0},
        {"update nonexistent reservation", fun test_update_nonexistent/0}
     ]}.

test_update_end_time() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    NewEndTime = Now + 7200,
    Result = flurm_reservation:update(Name, #{end_time => NewEndTime}),
    ?assertEqual(ok, Result).

test_update_users() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"user1">>]
    })),
    Result = flurm_reservation:update(Name, #{users => [<<"user1">>, <<"user2">>]}),
    ?assertEqual(ok, Result).

test_update_nonexistent() ->
    Result = flurm_reservation:update(<<"nonexistent">>, #{end_time => 999999}),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Conflict Check Tests
%%====================================================================

conflict_check_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check no conflict", fun test_no_conflict/0}
     ]}.

test_no_conflict() ->
    Now = erlang:system_time(second),
    Spec = make_reservation_spec(#{
        name => unique_name(),
        nodes => [<<"unique_node">>],
        start_time => Now + 10000,  % Far future
        end_time => Now + 13600
    }),
    Result = flurm_reservation:check_reservation_conflict(Spec),
    ?assertEqual(ok, Result).
