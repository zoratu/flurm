%%%-------------------------------------------------------------------
%%% @doc FLURM Admin Handler
%%%
%%% Handles administrative operations including:
%%% - Ping requests
%%% - Shutdown requests
%%% - Reservation management (info, create, update, delete)
%%% - License info
%%% - Topology info
%%% - Front-end info
%%% - Burst buffer info
%%%
%%% Split from flurm_controller_handler.erl for maintainability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_admin).

-export([handle/2]).

%% Exported for use by main handler
-export([
    reservation_to_reservation_info/1,
    reservation_state_to_flags/1,
    determine_reservation_type/2,
    parse_reservation_flags/1,
    generate_reservation_name/0,
    extract_reservation_fields/1,
    create_reservation_request_to_spec/1,
    update_reservation_request_to_updates/1,
    license_to_license_info/1,
    build_burst_buffer_info/2,
    pool_to_bb_pool/1,
    do_graceful_shutdown/0,
    is_cluster_enabled/0
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Import helpers from main handler
-import(flurm_controller_handler, [
    ensure_binary/1,
    error_to_binary/1,
    format_allocated_nodes/1
]).

%%====================================================================
%% API
%%====================================================================

%% REQUEST_PING (1008) -> RESPONSE_SLURM_RC with return_code = 0
handle(#slurm_header{msg_type = ?REQUEST_PING}, _Body) ->
    lager:debug("Handling PING request"),
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% REQUEST_SHUTDOWN (1005) -> RESPONSE_SLURM_RC
%% scontrol shutdown
handle(#slurm_header{msg_type = ?REQUEST_SHUTDOWN}, _Body) ->
    lager:info("Handling shutdown request"),
    %% Initiate graceful shutdown
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    do_graceful_shutdown();
                false ->
                    case flurm_controller_cluster:forward_to_leader(shutdown, []) of
                        {ok, ShutdownResult} -> ShutdownResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_graceful_shutdown()
    end,
    case Result of
        ok ->
            lager:info("Shutdown initiated successfully"),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Shutdown failed: ~p", [Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_RESERVATION_INFO (2012) -> RESPONSE_RESERVATION_INFO
%% scontrol show reservation
handle(#slurm_header{msg_type = ?REQUEST_RESERVATION_INFO}, _Body) ->
    lager:debug("Handling reservation info request"),
    Reservations = try flurm_reservation:list() catch _:_ -> [] end,
    ResvInfoList = [reservation_to_reservation_info(R) || R <- Reservations],
    Response = #reservation_info_response{
        last_update = erlang:system_time(second),
        reservation_count = length(ResvInfoList),
        reservations = ResvInfoList
    },
    {ok, ?RESPONSE_RESERVATION_INFO, Response};

%% REQUEST_CREATE_RESERVATION (2050) -> RESPONSE_CREATE_RESERVATION
%% scontrol create reservation
handle(#slurm_header{msg_type = ?REQUEST_CREATE_RESERVATION},
       #create_reservation_request{} = Request) ->
    lager:info("Handling create reservation request: ~s", [Request#create_reservation_request.name]),
    ResvSpec = create_reservation_request_to_spec(Request),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:create(ResvSpec);
                false ->
                    case flurm_controller_cluster:forward_to_leader(create_reservation, ResvSpec) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:create(ResvSpec)
    end,
    case Result of
        {ok, ResvName} ->
            lager:info("Reservation ~s created successfully", [ResvName]),
            Response = #create_reservation_response{
                name = ResvName,
                error_code = 0,
                error_msg = <<"Reservation created successfully">>
            },
            {ok, ?RESPONSE_CREATE_RESERVATION, Response};
        {error, Reason2} ->
            lager:warning("Create reservation failed: ~p", [Reason2]),
            Response = #create_reservation_response{
                name = <<>>,
                error_code = 1,
                error_msg = error_to_binary(Reason2)
            },
            {ok, ?RESPONSE_CREATE_RESERVATION, Response}
    end;

%% REQUEST_UPDATE_RESERVATION (2052) -> RESPONSE_SLURM_RC
%% scontrol update reservation
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_RESERVATION},
       #update_reservation_request{} = Request) ->
    Name = Request#update_reservation_request.name,
    lager:info("Handling update reservation request: ~s", [Name]),
    Updates = update_reservation_request_to_updates(Request),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:update(Name, Updates);
                false ->
                    case flurm_controller_cluster:forward_to_leader(update_reservation, {Name, Updates}) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:update(Name, Updates)
    end,
    case Result of
        ok ->
            lager:info("Reservation ~s updated successfully", [Name]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Update reservation ~s failed: ~p", [Name, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_DELETE_RESERVATION (2053) -> RESPONSE_SLURM_RC
%% scontrol delete reservation
handle(#slurm_header{msg_type = ?REQUEST_DELETE_RESERVATION},
       #delete_reservation_request{name = Name}) ->
    lager:info("Handling delete reservation request: ~s", [Name]),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:delete(Name);
                false ->
                    case flurm_controller_cluster:forward_to_leader(delete_reservation, Name) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:delete(Name)
    end,
    case Result of
        ok ->
            lager:info("Reservation ~s deleted successfully", [Name]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Delete reservation ~s failed: ~p", [Name, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_LICENSE_INFO (1017) -> RESPONSE_LICENSE_INFO
%% scontrol show license
handle(#slurm_header{msg_type = ?REQUEST_LICENSE_INFO}, _Body) ->
    lager:debug("Handling license info request"),
    Licenses = try flurm_license:list() catch _:_ -> [] end,
    LicInfoList = [license_to_license_info(L) || L <- Licenses],
    Response = #license_info_response{
        last_update = erlang:system_time(second),
        license_count = length(LicInfoList),
        licenses = LicInfoList
    },
    {ok, ?RESPONSE_LICENSE_INFO, Response};

%% REQUEST_TOPO_INFO (2018) -> RESPONSE_TOPO_INFO
%% scontrol show topology
handle(#slurm_header{msg_type = ?REQUEST_TOPO_INFO}, _Body) ->
    lager:debug("Handling topology info request"),
    %% Return empty topology (no complex network topology configured)
    Response = #topo_info_response{
        topo_count = 0,
        topos = []
    },
    {ok, ?RESPONSE_TOPO_INFO, Response};

%% REQUEST_FRONT_END_INFO (2028) -> RESPONSE_FRONT_END_INFO
%% scontrol show frontend
handle(#slurm_header{msg_type = ?REQUEST_FRONT_END_INFO}, _Body) ->
    lager:debug("Handling front-end info request"),
    %% Return empty (no front-end nodes - typically used on Cray systems)
    Response = #front_end_info_response{
        last_update = erlang:system_time(second),
        front_end_count = 0,
        front_ends = []
    },
    {ok, ?RESPONSE_FRONT_END_INFO, Response};

%% REQUEST_BURST_BUFFER_INFO (2020) -> RESPONSE_BURST_BUFFER_INFO
%% scontrol show burstbuffer
handle(#slurm_header{msg_type = ?REQUEST_BURST_BUFFER_INFO}, _Body) ->
    lager:debug("Handling burst buffer info request"),
    Pools = try flurm_burst_buffer:list_pools() catch _:_ -> [] end,
    Stats = try flurm_burst_buffer:get_stats() catch _:_ -> #{} end,
    BBInfoList = build_burst_buffer_info(Pools, Stats),
    Response = #burst_buffer_info_response{
        last_update = erlang:system_time(second),
        burst_buffer_count = length(BBInfoList),
        burst_buffers = BBInfoList
    },
    {ok, ?RESPONSE_BURST_BUFFER_INFO, Response}.

%%====================================================================
%% Internal Functions - Reservation Management
%%====================================================================

%% @doc Convert internal reservation record to SLURM reservation_info record
-spec reservation_to_reservation_info(tuple()) -> #reservation_info{}.
reservation_to_reservation_info(Resv) ->
    %% Handle both record and map formats from flurm_reservation
    {Name, StartTime, EndTime, Nodes, Users, State, _Flags} = extract_reservation_fields(Resv),
    NodeList = format_allocated_nodes(Nodes),
    UserList = iolist_to_binary(lists:join(<<",">>, Users)),
    #reservation_info{
        name = ensure_binary(Name),
        accounts = <<>>,
        burst_buffer = <<>>,
        core_cnt = 0,
        core_spec_cnt = 0,
        end_time = EndTime,
        features = <<>>,
        flags = reservation_state_to_flags(State),
        groups = <<>>,
        licenses = <<>>,
        max_start_delay = 0,
        node_cnt = length(Nodes),
        node_list = NodeList,
        partition = <<>>,
        purge_comp_time = 0,
        resv_watts = 0,
        start_time = StartTime,
        tres_str = <<>>,
        users = UserList
    }.

%% @doc Extract fields from reservation record/tuple
extract_reservation_fields(Resv) when is_tuple(Resv) ->
    %% Assume record format from flurm_reservation
    %% #reservation{name, type, start_time, end_time, duration, nodes, node_count,
    %%              partition, features, users, accounts, flags, tres, state, ...}
    case tuple_size(Resv) of
        N when N >= 11 ->
            Name = element(2, Resv),
            StartTime = element(4, Resv),
            EndTime = element(5, Resv),
            Nodes = element(7, Resv),
            Users = element(11, Resv),
            State = element(14, Resv),
            Flags = element(12, Resv),
            {Name, StartTime, EndTime, Nodes, Users, State, Flags};
        _ ->
            {<<>>, 0, 0, [], [], inactive, []}
    end;
extract_reservation_fields(_) ->
    {<<>>, 0, 0, [], [], inactive, []}.

%% @doc Convert reservation state to SLURM flags
reservation_state_to_flags(active) -> 1;
reservation_state_to_flags(inactive) -> 0;
reservation_state_to_flags(expired) -> 2;
reservation_state_to_flags(_) -> 0.

%% @doc Convert create reservation request to spec for flurm_reservation:create/1
-spec create_reservation_request_to_spec(#create_reservation_request{}) -> map().
create_reservation_request_to_spec(#create_reservation_request{} = Req) ->
    Now = erlang:system_time(second),

    %% Determine start time
    StartTime = case Req#create_reservation_request.start_time of
        0 -> Now;  % Start now
        S -> S
    end,

    %% Determine end time (from end_time or duration)
    EndTime = case Req#create_reservation_request.end_time of
        0 ->
            %% No end time specified, use duration
            Duration = case Req#create_reservation_request.duration of
                0 -> 60;  % Default 1 hour
                D -> D
            end,
            StartTime + (Duration * 60);  % Convert minutes to seconds
        E -> E
    end,

    %% Parse node list
    Nodes = case Req#create_reservation_request.nodes of
        <<>> -> [];
        NodeList ->
            %% Expand hostlist pattern if needed
            try flurm_config_slurm:expand_hostlist(NodeList)
            catch _:_ -> binary:split(NodeList, <<",">>, [global])
            end
    end,

    %% Parse users list
    Users = case Req#create_reservation_request.users of
        <<>> -> [];
        UserList -> binary:split(UserList, <<",">>, [global])
    end,

    %% Parse accounts list
    Accounts = case Req#create_reservation_request.accounts of
        <<>> -> [];
        AccountList -> binary:split(AccountList, <<",">>, [global])
    end,

    %% Determine reservation type from flags or type field
    Type = determine_reservation_type(
        Req#create_reservation_request.type,
        Req#create_reservation_request.flags
    ),

    %% Build flags list from flags integer
    Flags = parse_reservation_flags(Req#create_reservation_request.flags),

    #{
        name => case Req#create_reservation_request.name of
            <<>> -> generate_reservation_name();
            N -> N
        end,
        type => Type,
        start_time => StartTime,
        end_time => EndTime,
        nodes => Nodes,
        node_count => case Req#create_reservation_request.node_cnt of
            0 -> length(Nodes);
            NC -> NC
        end,
        partition => Req#create_reservation_request.partition,
        users => Users,
        accounts => Accounts,
        features => case Req#create_reservation_request.features of
            <<>> -> [];
            F -> binary:split(F, <<",">>, [global])
        end,
        flags => Flags,
        state => case StartTime =< Now of
            true -> active;
            false -> inactive
        end
    }.

%% @doc Convert update reservation request to updates map for flurm_reservation:update/2
-spec update_reservation_request_to_updates(#update_reservation_request{}) -> map().
update_reservation_request_to_updates(#update_reservation_request{} = Req) ->
    Updates0 = #{},

    %% Only include fields that are actually set (non-zero, non-empty)
    Updates1 = case Req#update_reservation_request.start_time of
        0 -> Updates0;
        S -> maps:put(start_time, S, Updates0)
    end,

    Updates2 = case Req#update_reservation_request.end_time of
        0 -> Updates1;
        E -> maps:put(end_time, E, Updates1)
    end,

    Updates3 = case Req#update_reservation_request.duration of
        0 -> Updates2;
        D -> maps:put(duration, D * 60, Updates2)  % Convert minutes to seconds
    end,

    Updates4 = case Req#update_reservation_request.nodes of
        <<>> -> Updates3;
        NodeList ->
            Nodes = try flurm_config_slurm:expand_hostlist(NodeList)
                    catch _:_ -> binary:split(NodeList, <<",">>, [global])
                    end,
            maps:put(nodes, Nodes, Updates3)
    end,

    Updates5 = case Req#update_reservation_request.users of
        <<>> -> Updates4;
        UserList ->
            Users = binary:split(UserList, <<",">>, [global]),
            maps:put(users, Users, Updates4)
    end,

    Updates6 = case Req#update_reservation_request.accounts of
        <<>> -> Updates5;
        AccountList ->
            Accounts = binary:split(AccountList, <<",">>, [global]),
            maps:put(accounts, Accounts, Updates5)
    end,

    Updates7 = case Req#update_reservation_request.partition of
        <<>> -> Updates6;
        P -> maps:put(partition, P, Updates6)
    end,

    Updates8 = case Req#update_reservation_request.flags of
        0 -> Updates7;
        F -> maps:put(flags, parse_reservation_flags(F), Updates7)
    end,

    Updates8.

%% @doc Determine reservation type from type string or flags
-spec determine_reservation_type(binary(), non_neg_integer()) -> atom().
determine_reservation_type(<<"maint">>, _) -> maint;
determine_reservation_type(<<"MAINT">>, _) -> maint;
determine_reservation_type(<<"maintenance">>, _) -> maintenance;
determine_reservation_type(<<"flex">>, _) -> flex;
determine_reservation_type(<<"FLEX">>, _) -> flex;
determine_reservation_type(<<"user">>, _) -> user;
determine_reservation_type(<<"USER">>, _) -> user;
determine_reservation_type(<<>>, Flags) ->
    %% Infer from flags
    %% SLURM reservation flags:
    %% RESERVE_FLAG_MAINT = 0x0001
    %% RESERVE_FLAG_FLEX = 0x8000
    case Flags band 16#0001 of
        0 ->
            case Flags band 16#8000 of
                0 -> user;
                _ -> flex
            end;
        _ -> maint
    end;
determine_reservation_type(_, _) -> user.

%% @doc Parse reservation flags integer to list of atoms
-spec parse_reservation_flags(non_neg_integer()) -> [atom()].
parse_reservation_flags(0) -> [];
parse_reservation_flags(Flags) ->
    %% SLURM reservation flag definitions
    FlagDefs = [
        {16#0001, maint},
        {16#0002, ignore_jobs},
        {16#0004, daily},
        {16#0008, weekly},
        {16#0010, weekday},
        {16#0020, weekend},
        {16#0040, any},
        {16#0080, first_cores},
        {16#0100, time_float},
        {16#0200, purge_comp},
        {16#0400, part_nodes},
        {16#0800, overlap},
        {16#1000, no_hold_jobs_after},
        {16#2000, static_alloc},
        {16#4000, no_hold_jobs},
        {16#8000, flex}
    ],
    lists:foldl(fun({Mask, Flag}, Acc) ->
        case Flags band Mask of
            0 -> Acc;
            _ -> [Flag | Acc]
        end
    end, [], FlagDefs).

%% @doc Generate a unique reservation name
-spec generate_reservation_name() -> binary().
generate_reservation_name() ->
    Timestamp = erlang:system_time(microsecond),
    Random = rand:uniform(9999),
    iolist_to_binary(io_lib:format("resv_~p_~4..0B", [Timestamp, Random])).

%%====================================================================
%% Internal Functions - License Info
%%====================================================================

%% @doc Convert internal license record/map to SLURM license_info record
-spec license_to_license_info(tuple() | map()) -> #license_info{}.
license_to_license_info(Lic) when is_map(Lic) ->
    #license_info{
        name = ensure_binary(maps:get(name, Lic, <<>>)),
        total = maps:get(total, Lic, 0),
        in_use = maps:get(in_use, Lic, 0),
        available = maps:get(available, Lic, maps:get(total, Lic, 0) - maps:get(in_use, Lic, 0)),
        reserved = maps:get(reserved, Lic, 0),
        remote = case maps:get(remote, Lic, false) of true -> 1; _ -> 0 end
    };
license_to_license_info(Lic) when is_tuple(Lic) ->
    %% Handle record format if used
    case tuple_size(Lic) of
        N when N >= 5 ->
            #license_info{
                name = ensure_binary(element(2, Lic)),
                total = element(3, Lic),
                in_use = element(4, Lic),
                available = element(3, Lic) - element(4, Lic),
                reserved = 0,
                remote = 0
            };
        _ ->
            #license_info{}
    end;
license_to_license_info(_) ->
    #license_info{}.

%%====================================================================
%% Internal Functions - Burst Buffer Info
%%====================================================================

%% @doc Build burst buffer info from pools and stats
-spec build_burst_buffer_info([tuple()], map()) -> [#burst_buffer_info{}].
build_burst_buffer_info([], _Stats) ->
    [];
build_burst_buffer_info(Pools, Stats) ->
    %% Group pools into a single burst_buffer_info entry
    %% In SLURM, each plugin (datawarp, generic, etc.) is one entry
    PoolInfos = [pool_to_bb_pool(P) || P <- Pools],
    TotalSpace = maps:get(total_size, Stats, 0),
    UsedSpace = maps:get(used_size, Stats, 0),
    UnfreeSpace = TotalSpace - maps:get(free_size, Stats, TotalSpace),
    [#burst_buffer_info{
        name = <<"generic">>,
        default_pool = <<"default">>,
        allow_users = <<>>,
        create_buffer = <<>>,
        deny_users = <<>>,
        destroy_buffer = <<>>,
        flags = 0,
        get_sys_state = <<>>,
        get_sys_status = <<>>,
        granularity = 1048576,  % 1 MB
        pool_cnt = length(PoolInfos),
        pools = PoolInfos,
        other_timeout = 300,
        stage_in_timeout = 300,
        stage_out_timeout = 300,
        start_stage_in = <<>>,
        start_stage_out = <<>>,
        stop_stage_in = <<>>,
        stop_stage_out = <<>>,
        total_space = TotalSpace,
        unfree_space = UnfreeSpace,
        used_space = UsedSpace,
        validate_timeout = 60
    }].

%% @doc Convert pool record to burst_buffer_pool
-spec pool_to_bb_pool(tuple()) -> #burst_buffer_pool{}.
pool_to_bb_pool(Pool) when is_tuple(Pool) ->
    %% #bb_pool{name, type, total_size, free_size, granularity, nodes, state, properties}
    case tuple_size(Pool) of
        N when N >= 5 ->
            Name = element(2, Pool),
            TotalSize = element(4, Pool),
            FreeSize = element(5, Pool),
            Granularity = element(6, Pool),
            #burst_buffer_pool{
                name = ensure_binary(Name),
                total_space = TotalSize,
                granularity = Granularity,
                unfree_space = TotalSize - FreeSize,
                used_space = TotalSize - FreeSize
            };
        _ ->
            #burst_buffer_pool{name = <<"default">>}
    end;
pool_to_bb_pool(_) ->
    #burst_buffer_pool{name = <<"default">>}.

%%====================================================================
%% Internal Functions - Shutdown
%%====================================================================

%% @doc Perform graceful shutdown of the controller
-spec do_graceful_shutdown() -> ok | {error, term()}.
do_graceful_shutdown() ->
    lager:info("Starting graceful shutdown"),
    try
        %% Step 1: Stop accepting new jobs
        lager:info("Stopping acceptance of new jobs"),

        %% Step 2: Allow running jobs to continue (don't kill them)
        %% They will be orphaned but nodes can clean them up

        %% Step 3: Schedule application stop
        %% Use a brief delay to ensure response is sent first
        spawn(fun() ->
            timer:sleep(500),
            lager:info("Initiating application stop"),
            application:stop(flurm_controller),
            application:stop(flurm_config),
            application:stop(flurm_protocol),
            init:stop()
        end),
        ok
    catch
        _:Reason ->
            lager:error("Error during graceful shutdown: ~p", [Reason]),
            {error, Reason}
    end.

%%====================================================================
%% Internal Functions - Cluster Helpers
%%====================================================================

%% @doc Check if cluster mode is enabled.
%% Cluster mode is enabled when there are multiple nodes configured.
-spec is_cluster_enabled() -> boolean().
is_cluster_enabled() ->
    case application:get_env(flurm_controller, cluster_nodes) of
        {ok, Nodes} when is_list(Nodes), length(Nodes) > 1 ->
            true;
        _ ->
            %% Also check if the cluster process is running
            case whereis(flurm_controller_cluster) of
                undefined -> false;
                _Pid -> true
            end
    end.
