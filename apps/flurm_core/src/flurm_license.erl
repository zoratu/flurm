%%%-------------------------------------------------------------------
%%% @doc FLURM License Management
%%%
%%% Manages software licenses as consumable resources for job scheduling.
%%% Compatible with SLURM's license specification.
%%%
%%% Supports:
%%% - License tracking and allocation
%%% - Local and remote (server-based) licenses
%%% - License reservations
%%% - Usage reporting
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_license).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    add_license/2,
    remove_license/1,
    list/0,
    get/1,
    allocate/2,
    deallocate/2,
    reserve/2,
    unreserve/2,
    get_available/1,
    check_availability/1,
    parse_license_spec/1,
    validate_licenses/1,
    exists/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(LICENSE_TABLE, flurm_licenses).
-define(LICENSE_ALLOC_TABLE, flurm_license_allocations).

-record(license, {
    name :: binary(),
    total :: non_neg_integer(),
    in_use :: non_neg_integer(),
    reserved :: non_neg_integer(),
    remote :: boolean(),
    server :: binary() | undefined,  % For remote licenses
    port :: non_neg_integer() | undefined
}).

-record(license_alloc, {
    id :: {job_id(), binary()},  % {JobId, LicenseName}
    job_id :: job_id(),
    license :: binary(),
    count :: non_neg_integer(),
    alloc_time :: non_neg_integer()
}).

-record(state, {}).

%% Test exports
-ifdef(TEST).
-export([
    %% License management
    load_configured_licenses/0,
    do_add_license/2,
    do_remove_license/1,
    %% Allocation internals
    do_allocate/2,
    allocate_license/3,
    do_deallocate/2,
    deallocate_license/3,
    %% Reservation internals
    do_reserve/2,
    do_unreserve/2,
    %% Parsing
    parse_single_license/1,
    %% Conversion
    license_to_map/1
]).
-endif.

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Add a new license type
-spec add_license(binary(), map()) -> ok | {error, term()}.
add_license(Name, Config) ->
    gen_server:call(?SERVER, {add_license, Name, Config}).

%% @doc Remove a license type
-spec remove_license(binary()) -> ok | {error, term()}.
remove_license(Name) ->
    gen_server:call(?SERVER, {remove_license, Name}).

%% @doc List all licenses
-spec list() -> [map()].
list() ->
    case ets:info(?LICENSE_TABLE, size) of
        undefined -> [];
        _ ->
            Licenses = ets:tab2list(?LICENSE_TABLE),
            [license_to_map(L) || L <- Licenses]
    end.

%% @doc Get a specific license
-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Name) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [License] -> {ok, license_to_map(License)};
        [] -> {error, not_found}
    end.

%% @doc Allocate licenses for a job
-spec allocate(job_id(), [{binary(), non_neg_integer()}]) -> ok | {error, term()}.
allocate(JobId, LicenseRequests) ->
    gen_server:call(?SERVER, {allocate, JobId, LicenseRequests}).

%% @doc Deallocate licenses from a job
-spec deallocate(job_id(), [{binary(), non_neg_integer()}]) -> ok.
deallocate(JobId, LicenseRequests) ->
    gen_server:call(?SERVER, {deallocate, JobId, LicenseRequests}).

%% @doc Reserve licenses (for advance reservations)
-spec reserve(binary(), non_neg_integer()) -> ok | {error, term()}.
reserve(Name, Count) ->
    gen_server:call(?SERVER, {reserve, Name, Count}).

%% @doc Unreserve licenses
-spec unreserve(binary(), non_neg_integer()) -> ok.
unreserve(Name, Count) ->
    gen_server:call(?SERVER, {unreserve, Name, Count}).

%% @doc Get available count for a license
-spec get_available(binary()) -> non_neg_integer().
get_available(Name) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [#license{total = Total, in_use = InUse, reserved = Reserved}] ->
            max(0, Total - InUse - Reserved);
        [] ->
            0
    end.

%% @doc Check if license requirements can be satisfied
-spec check_availability([{binary(), non_neg_integer()}]) -> boolean().
check_availability(Requests) ->
    lists:all(fun({Name, Count}) ->
        get_available(Name) >= Count
    end, Requests).

%% @doc Parse license specification string (format: "name:count,name:count")
%% Compatible with SLURM's --licenses option
-spec parse_license_spec(binary() | string()) -> {ok, [{binary(), non_neg_integer()}]} | {error, term()}.
parse_license_spec(<<>>) ->
    {ok, []};
parse_license_spec("") ->
    {ok, []};
parse_license_spec(Spec) when is_list(Spec) ->
    parse_license_spec(list_to_binary(Spec));
parse_license_spec(Spec) when is_binary(Spec) ->
    %% Split by comma to get individual license specs
    Parts = binary:split(Spec, <<",">>, [global, trim_all]),
    try
        Licenses = lists:map(fun parse_single_license/1, Parts),
        {ok, Licenses}
    catch
        throw:{invalid_license, Reason} ->
            {error, {invalid_license_spec, Reason}}
    end.

%% @doc Parse a single license specification (format: "name:count" or "name")
-spec parse_single_license(binary()) -> {binary(), non_neg_integer()}.
parse_single_license(Part) ->
    case binary:split(Part, <<":">>) of
        [Name] ->
            %% No count specified, default to 1
            TrimmedName = string:trim(Name),
            case TrimmedName of
                <<>> -> throw({invalid_license, empty_name});
                _ -> {TrimmedName, 1}
            end;
        [Name, CountBin] ->
            TrimmedName = string:trim(Name),
            TrimmedCount = string:trim(CountBin),
            case TrimmedName of
                <<>> -> throw({invalid_license, empty_name});
                _ ->
                    case catch binary_to_integer(TrimmedCount) of
                        Count when is_integer(Count), Count > 0 ->
                            {TrimmedName, Count};
                        Count when is_integer(Count), Count =< 0 ->
                            throw({invalid_license, {invalid_count, Count}});
                        _ ->
                            throw({invalid_license, {invalid_count, TrimmedCount}})
                    end
            end;
        _ ->
            throw({invalid_license, {invalid_format, Part}})
    end.

%% @doc Check if a license exists
-spec exists(binary()) -> boolean().
exists(Name) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [_] -> true;
        [] -> false
    end.

%% @doc Validate that all requested licenses exist
%% Returns ok if all licenses exist, or {error, {unknown_license, Name}} for first unknown
-spec validate_licenses([{binary(), non_neg_integer()}]) -> ok | {error, term()}.
validate_licenses([]) ->
    ok;
validate_licenses([{Name, _Count} | Rest]) ->
    case exists(Name) of
        true -> validate_licenses(Rest);
        false -> {error, {unknown_license, Name}}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?LICENSE_TABLE, [
        named_table, public, set,
        {keypos, #license.name}
    ]),
    ets:new(?LICENSE_ALLOC_TABLE, [
        named_table, public, set,
        {keypos, #license_alloc.id}
    ]),

    %% Load configured licenses
    load_configured_licenses(),

    {ok, #state{}}.

handle_call({add_license, Name, Config}, _From, State) ->
    Result = do_add_license(Name, Config),
    {reply, Result, State};

handle_call({remove_license, Name}, _From, State) ->
    Result = do_remove_license(Name),
    {reply, Result, State};

handle_call({allocate, JobId, Requests}, _From, State) ->
    Result = do_allocate(JobId, Requests),
    {reply, Result, State};

handle_call({deallocate, JobId, Requests}, _From, State) ->
    do_deallocate(JobId, Requests),
    %% Trigger scheduling when licenses become available
    %% This allows jobs waiting for licenses to be scheduled
    case Requests of
        [] -> ok;
        _ ->
            %% Use cast to avoid blocking
            spawn(fun() ->
                catch flurm_scheduler:trigger_schedule()
            end)
    end,
    {reply, ok, State};

handle_call({reserve, Name, Count}, _From, State) ->
    Result = do_reserve(Name, Count),
    {reply, Result, State};

handle_call({unreserve, Name, Count}, _From, State) ->
    do_unreserve(Name, Count),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

load_configured_licenses() ->
    %% Load licenses from application config
    case application:get_env(flurm_core, licenses, []) of
        [] ->
            ok;
        Licenses ->
            lists:foreach(fun({Name, Config}) ->
                do_add_license(Name, Config)
            end, Licenses)
    end.

do_add_license(Name, Config) ->
    License = #license{
        name = Name,
        total = maps:get(total, Config, 0),
        in_use = 0,
        reserved = 0,
        remote = maps:get(remote, Config, false),
        server = maps:get(server, Config, undefined),
        port = maps:get(port, Config, undefined)
    },
    ets:insert(?LICENSE_TABLE, License),
    ok.

do_remove_license(Name) ->
    %% Check for active allocations
    case ets:select(?LICENSE_ALLOC_TABLE, [{
        #license_alloc{license = Name, _ = '_'},
        [],
        ['$_']
    }]) of
        [] ->
            ets:delete(?LICENSE_TABLE, Name),
            ok;
        _ ->
            {error, license_in_use}
    end.

do_allocate(JobId, Requests) ->
    %% First check availability
    case check_availability(Requests) of
        true ->
            %% Allocate each license
            lists:foreach(fun({Name, Count}) ->
                allocate_license(JobId, Name, Count)
            end, Requests),
            ok;
        false ->
            {error, insufficient_licenses}
    end.

allocate_license(JobId, Name, Count) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [License] ->
            %% Update license usage
            NewInUse = License#license.in_use + Count,
            ets:insert(?LICENSE_TABLE, License#license{in_use = NewInUse}),

            %% Record allocation
            Alloc = #license_alloc{
                id = {JobId, Name},
                job_id = JobId,
                license = Name,
                count = Count,
                alloc_time = erlang:system_time(second)
            },
            ets:insert(?LICENSE_ALLOC_TABLE, Alloc);
        [] ->
            ok
    end.

do_deallocate(JobId, Requests) ->
    lists:foreach(fun({Name, Count}) ->
        deallocate_license(JobId, Name, Count)
    end, Requests).

deallocate_license(JobId, Name, Count) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [License] ->
            %% Update license usage
            NewInUse = max(0, License#license.in_use - Count),
            ets:insert(?LICENSE_TABLE, License#license{in_use = NewInUse}),

            %% Remove allocation record
            ets:delete(?LICENSE_ALLOC_TABLE, {JobId, Name});
        [] ->
            ok
    end.

do_reserve(Name, Count) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [License] ->
            Available = License#license.total - License#license.in_use - License#license.reserved,
            case Available >= Count of
                true ->
                    NewReserved = License#license.reserved + Count,
                    ets:insert(?LICENSE_TABLE, License#license{reserved = NewReserved}),
                    ok;
                false ->
                    {error, insufficient_licenses}
            end;
        [] ->
            {error, not_found}
    end.

do_unreserve(Name, Count) ->
    case ets:lookup(?LICENSE_TABLE, Name) of
        [License] ->
            NewReserved = max(0, License#license.reserved - Count),
            ets:insert(?LICENSE_TABLE, License#license{reserved = NewReserved});
        [] ->
            ok
    end.

license_to_map(#license{} = L) ->
    #{
        name => L#license.name,
        total => L#license.total,
        in_use => L#license.in_use,
        available => max(0, L#license.total - L#license.in_use - L#license.reserved),
        reserved => L#license.reserved,
        remote => L#license.remote,
        server => L#license.server,
        port => L#license.port
    }.
