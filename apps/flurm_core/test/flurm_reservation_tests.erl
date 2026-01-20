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
    AllRes = flurm_reservation:list(),
    %% Get the names from the reservation tuples (record is {reservation, name, ...})
    lists:foreach(fun(Res) ->
        %% The record is a tuple where element 2 is the name
        Name = element(2, Res),
        catch flurm_reservation:delete(Name)
    end, AllRes),
    %% Stop the server if we started it - use proper wait
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end;
        false ->
            ok
    end,
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
    %% Add 2 second buffer to ensure start_time is always in the future
    StartTime = Now + 2,
    Defaults = #{
        name => <<"test_reservation">>,
        type => user,
        nodes => [<<"node1">>, <<"node2">>],
        start_time => StartTime,
        end_time => StartTime + 3600,
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
        {"get non-existent reservation", fun test_get_nonexistent/0},
        {"create duplicate reservation fails", fun test_create_duplicate/0}
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

test_create_duplicate() ->
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    %% Second create should fail
    Result = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    ?assertEqual({error, already_exists}, Result).

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
        {"activate nonexistent", fun test_activate_nonexistent/0},
        {"deactivate nonexistent", fun test_deactivate_nonexistent/0}
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

test_deactivate_nonexistent() ->
    Result = flurm_reservation:deactivate_reservation(<<"nonexistent">>),
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
        {"get reservations for user", fun test_get_reservations_for_user/0},
        {"cannot use inactive reservation", fun test_cannot_use_inactive/0}
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

test_cannot_use_inactive() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"user1">>],
        start_time => Now + 60,   %% Future start time - stays inactive
        end_time => Now + 3600
    })),
    %% Don't activate - should not be usable
    ?assertNot(flurm_reservation:can_use_reservation(<<"user1">>, Name)).

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
        {"get reserved nodes", fun test_get_reserved_nodes/0},
        {"get reserved nodes inactive reservation", fun test_get_reserved_nodes_inactive/0}
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

test_get_reserved_nodes_inactive() ->
    Name = unique_name(),
    Nodes = [<<"node1">>, <<"node2">>],
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => Nodes
    })),
    %% Don't activate - should return error
    Result = flurm_reservation:get_reserved_nodes(Name),
    ?assertMatch({error, {invalid_state, inactive}}, Result).

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
        {"confirm job reservation", fun test_confirm_job_reservation/0},
        {"confirm inactive reservation fails", fun test_confirm_inactive_fails/0}
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
    %% Activate first
    flurm_reservation:activate_reservation(Name),
    Result = flurm_reservation:confirm_reservation(Name),
    ?assertEqual(ok, Result).

test_confirm_inactive_fails() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    Spec = #{
        name => Name,
        nodes => [<<"inactive_node">>],
        start_time => Now + 60,   %% Future start time - stays inactive
        end_time => Now + 3600,
        user => <<"inactiveuser">>
    },
    {ok, _} = flurm_reservation:create_reservation(Spec),
    %% Don't activate - confirm should fail
    Result = flurm_reservation:confirm_reservation(Name),
    ?assertMatch({error, {invalid_state, inactive}}, Result).

%%====================================================================
%% Scheduler Integration Tests
%%====================================================================

scheduler_integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check job reservation - no reservation", fun test_check_job_no_reservation/0},
        {"check job reservation - empty binary", fun test_check_job_empty_binary/0},
        {"get active reservations", fun test_get_active_reservations/0},
        {"reservation activated callback", fun test_reservation_activated_callback/0},
        {"reservation deactivated callback", fun test_reservation_deactivated_callback/0}
     ]}.

test_check_job_no_reservation() ->
    Job = #job{id = 1, reservation = <<>>, user = <<"user1">>},
    Result = flurm_reservation:check_job_reservation(Job),
    ?assertEqual({ok, no_reservation}, Result).

test_check_job_empty_binary() ->
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

test_reservation_activated_callback() ->
    %% Just test that the callback doesn't crash
    ok = flurm_reservation:reservation_activated(<<"test_res">>).

test_reservation_deactivated_callback() ->
    %% Just test that the callback doesn't crash
    ok = flurm_reservation:reservation_deactivated(<<"test_res">>).

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
        {"update nonexistent reservation", fun test_update_nonexistent/0},
        {"update multiple fields", fun test_update_multiple_fields/0}
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

test_update_multiple_fields() ->
    Name = unique_name(),
    Now = erlang:system_time(second),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{name => Name})),
    Result = flurm_reservation:update(Name, #{
        end_time => Now + 7200,
        users => [<<"user1">>, <<"user2">>],
        nodes => [<<"node1">>, <<"node2">>, <<"node3">>],
        partition => <<"compute">>
    }),
    ?assertEqual(ok, Result).

%%====================================================================
%% Conflict Check Tests
%%====================================================================

conflict_check_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check no conflict", fun test_no_conflict/0},
        {"check time conflict", fun test_time_conflict/0},
        {"no conflict different nodes", fun test_no_conflict_different_nodes/0},
        {"no conflict different times", fun test_no_conflict_different_times/0}
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

test_time_conflict() ->
    Now = erlang:system_time(second),
    Name1 = unique_name(),
    %% Create first reservation
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name1,
        nodes => [<<"conflict_node">>],
        start_time => Now,
        end_time => Now + 3600
    })),

    %% Try to create overlapping reservation on same node
    Spec = #{
        name => unique_name(),
        nodes => [<<"conflict_node">>],
        start_time => Now + 1800,  % Overlaps
        end_time => Now + 5400
    },
    Result = flurm_reservation:check_reservation_conflict(Spec),
    ?assertMatch({error, {conflicts, _}}, Result).

test_no_conflict_different_nodes() ->
    Now = erlang:system_time(second),
    Name1 = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name1,
        nodes => [<<"nodeA">>],
        start_time => Now,
        end_time => Now + 3600
    })),

    %% Same time, different node - no conflict
    Spec = #{
        name => unique_name(),
        nodes => [<<"nodeB">>],
        start_time => Now,
        end_time => Now + 3600
    },
    Result = flurm_reservation:check_reservation_conflict(Spec),
    ?assertEqual(ok, Result).

test_no_conflict_different_times() ->
    Now = erlang:system_time(second),
    Name1 = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name1,
        nodes => [<<"nodeC">>],
        start_time => Now,
        end_time => Now + 3600
    })),

    %% Same node, different time - no conflict
    Spec = #{
        name => unique_name(),
        nodes => [<<"nodeC">>],
        start_time => Now + 7200,  % 2 hours later
        end_time => Now + 10800
    },
    Result = flurm_reservation:check_reservation_conflict(Spec),
    ?assertEqual(ok, Result).

%%====================================================================
%% Filter Nodes Tests
%%====================================================================

filter_nodes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"filter nodes no reservation", fun test_filter_nodes_no_reservation/0},
        {"filter nodes with active reservation", fun test_filter_nodes_with_reservation/0},
        {"get available nodes excluding reserved", fun test_get_available_excluding_reserved/0}
     ]}.

test_filter_nodes_no_reservation() ->
    Job = #job{id = 1, reservation = <<>>, user = <<"user1">>},
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),
    ?assert(is_list(Result)).

test_filter_nodes_with_reservation() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => [<<"reserved_node">>],
        users => [<<"resuser">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Job = #job{id = 1, reservation = Name, user = <<"resuser">>, account = <<>>},
    Nodes = [<<"reserved_node">>, <<"other_node">>],
    Result = flurm_reservation:filter_nodes_for_scheduling(Job, Nodes),
    ?assertEqual([<<"reserved_node">>], Result).

test_get_available_excluding_reserved() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        nodes => [<<"reserved1">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Nodes = [<<"reserved1">>, <<"free1">>, <<"free2">>],
    Result = flurm_reservation:get_available_nodes_excluding_reserved(Nodes),
    ?assert(lists:member(<<"free1">>, Result)),
    ?assert(lists:member(<<"free2">>, Result)).

%%====================================================================
%% Check Reservation Access Tests
%%====================================================================

check_access_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check access authorized user", fun test_check_access_authorized/0},
        {"check access unauthorized user", fun test_check_access_unauthorized/0},
        {"check access expired reservation", fun test_check_access_expired/0},
        {"check access nonexistent reservation", fun test_check_access_nonexistent/0}
     ]}.

test_check_access_authorized() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"authuser">>],
        nodes => [<<"authnode">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Result = flurm_reservation:check_reservation_access(<<"authuser">>, Name),
    ?assertMatch({ok, [<<"authnode">>]}, Result).

test_check_access_unauthorized() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"specific_user">>],
        accounts => [],
        nodes => [<<"node1">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Result = flurm_reservation:check_reservation_access(<<"other_user">>, Name),
    ?assertEqual({error, access_denied}, Result).

test_check_access_expired() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    %% Create with past end time (will need special handling)
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        users => [<<"user1">>],
        start_time => Now - 7200,  % 2 hours ago
        end_time => Now - 3600     % 1 hour ago (expired)
    })),
    %% Force state to active for testing (normally it would be expired)
    flurm_reservation:activate_reservation(Name),

    Result = flurm_reservation:check_reservation_access(<<"user1">>, Name),
    ?assertEqual({error, reservation_expired}, Result).

test_check_access_nonexistent() ->
    Result = flurm_reservation:check_reservation_access(<<"user1">>, <<"nonexistent">>),
    ?assertEqual({error, reservation_not_found}, Result).

%%====================================================================
%% Get Next Available Window Tests
%%====================================================================

next_window_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get next available window no reservations", fun test_next_window_no_reservations/0}
     ]}.

test_next_window_no_reservations() ->
    ResourceRequest = #{nodes => [<<"node1">>]},
    Duration = 3600,
    Result = flurm_reservation:get_next_available_window(ResourceRequest, Duration),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% Validation Tests
%%====================================================================

validation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"create with missing end time fails", fun test_missing_end_time/0},
        {"create with missing node spec fails", fun test_missing_node_spec/0},
        {"create with past start time fails", fun test_past_start_time/0}
     ]}.

test_missing_end_time() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => unique_name(),
        nodes => [<<"node1">>],
        start_time => Now + 60
        %% No end_time or duration
    },
    Result = flurm_reservation:create(Spec),
    ?assertEqual({error, missing_end_time_or_duration}, Result).

test_missing_node_spec() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => unique_name(),
        start_time => Now + 60,
        end_time => Now + 3660
        %% No nodes or node_count
    },
    Result = flurm_reservation:create(Spec),
    ?assertEqual({error, missing_node_specification}, Result).

test_past_start_time() ->
    Now = erlang:system_time(second),
    Spec = #{
        name => unique_name(),
        nodes => [<<"node1">>],
        start_time => Now - 90000,  % 25 hours in past (exceeds 24h grace period)
        end_time => Now + 3600
    },
    Result = flurm_reservation:create(Spec),
    ?assertEqual({error, start_time_in_past}, Result).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast doesn't crash", fun test_unknown_cast/0},
        {"unknown info doesn't crash", fun test_unknown_info/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_reservation, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    gen_server:cast(flurm_reservation, {unknown_message}),
    _ = sys:get_state(flurm_reservation),
    %% Should not crash - verify server is still running
    ?assert(is_pid(whereis(flurm_reservation))).

test_unknown_info() ->
    flurm_reservation ! unknown_message,
    _ = sys:get_state(flurm_reservation),
    %% Should not crash - verify server is still running
    ?assert(is_pid(whereis(flurm_reservation))).

%%====================================================================
%% Reservation Type Tests
%%====================================================================

reservation_type_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"maintenance reservation blocks access", fun test_maintenance_reservation/0},
        {"flex reservation allows access", fun test_flex_reservation/0}
     ]}.

test_maintenance_reservation() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        type => maintenance,
        users => [],
        nodes => [<<"maint_node">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Result = flurm_reservation:check_reservation_access(<<"anyuser">>, Name),
    ?assertEqual({error, access_denied}, Result).

test_flex_reservation() ->
    Now = erlang:system_time(second),
    Name = unique_name(),
    {ok, _} = flurm_reservation:create(make_reservation_spec(#{
        name => Name,
        type => flex,
        users => [],
        accounts => [],
        nodes => [<<"flex_node">>],
        start_time => Now - 60,
        end_time => Now + 3600
    })),
    flurm_reservation:activate_reservation(Name),

    Result = flurm_reservation:check_reservation_access(<<"anyuser">>, Name),
    ?assertMatch({ok, _}, Result).
