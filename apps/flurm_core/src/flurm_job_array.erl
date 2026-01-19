%%%-------------------------------------------------------------------
%%% @doc FLURM Job Array Support
%%%
%%% Implements SLURM-compatible job arrays for submitting multiple
%%% similar jobs with a single submission.
%%%
%%% Features:
%%% - Array job submission (--array=0-100)
%%% - Array task ID environment variable (SLURM_ARRAY_TASK_ID)
%%% - Array job state tracking
%%% - Parallel limits (--array=0-100%10)
%%% - Array step notation (--array=0-100:2)
%%% - Array task dependencies
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    create_array_job/2,
    get_array_job/1,
    get_array_task/2,
    get_array_tasks/1,
    cancel_array_job/1,
    cancel_array_task/2,
    update_task_state/3,
    parse_array_spec/1,
    format_array_spec/1,
    expand_array/1,
    get_pending_tasks/1,
    get_running_tasks/1,
    get_completed_tasks/1,
    get_array_stats/1,
    get_task_env/2,
    can_schedule_task/1,
    schedule_next_task/1,
    task_started/3,
    task_completed/3,
    get_schedulable_count/1,
    get_schedulable_tasks/1
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
-define(ARRAY_JOBS_TABLE, flurm_array_jobs).
-define(ARRAY_TASKS_TABLE, flurm_array_tasks).

-type array_job_id() :: pos_integer().
-type task_id() :: non_neg_integer().
-type task_state() :: pending | running | completed | failed | cancelled.

-record(array_job, {
    id :: array_job_id(),
    name :: binary(),
    user :: binary(),
    partition :: binary(),
    base_job :: #job{},                    % Template job
    task_ids :: [task_id()],               % All task IDs
    task_count :: pos_integer(),
    max_concurrent :: pos_integer() | unlimited,
    state :: pending | running | completed | failed | cancelled,
    submit_time :: non_neg_integer(),
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    stats :: map()
}).

-record(array_task, {
    id :: {array_job_id(), task_id()},
    array_job_id :: array_job_id(),
    task_id :: task_id(),
    job_id :: job_id() | undefined,        % Actual job ID when scheduled
    state :: task_state(),
    exit_code :: integer() | undefined,
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    node :: binary() | undefined
}).

-record(array_spec, {
    %% Either range-based (start_idx, end_idx, step) or explicit list (indices)
    start_idx :: non_neg_integer() | undefined,
    end_idx :: non_neg_integer() | undefined,
    step :: pos_integer(),
    indices :: [non_neg_integer()] | undefined,  % For comma-separated lists
    max_concurrent :: pos_integer() | unlimited
}).

-record(state, {
    next_array_id :: pos_integer()
}).

-export_type([array_job_id/0, task_id/0, task_state/0]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    do_parse_array_spec/1,
    parse_range_spec/2,
    parse_list_spec/2,
    parse_list_element/1,
    generate_task_ids/1,
    apply_task_updates/2
]).
%% Export records for testing
-export_type([]).
-endif.

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Create a new array job
-spec create_array_job(#job{}, binary() | #array_spec{}) ->
    {ok, array_job_id()} | {error, term()}.
create_array_job(BaseJob, ArraySpec) when is_binary(ArraySpec) ->
    case parse_array_spec(ArraySpec) of
        {ok, Spec} -> create_array_job(BaseJob, Spec);
        {error, Reason} -> {error, Reason}
    end;
create_array_job(BaseJob, #array_spec{} = Spec) ->
    gen_server:call(?SERVER, {create_array_job, BaseJob, Spec}).

%% @doc Get array job information
-spec get_array_job(array_job_id()) -> {ok, #array_job{}} | {error, not_found}.
get_array_job(ArrayJobId) ->
    case ets:lookup(?ARRAY_JOBS_TABLE, ArrayJobId) of
        [Job] -> {ok, Job};
        [] -> {error, not_found}
    end.

%% @doc Get a specific task from an array job
-spec get_array_task(array_job_id(), task_id()) ->
    {ok, #array_task{}} | {error, not_found}.
get_array_task(ArrayJobId, TaskId) ->
    case ets:lookup(?ARRAY_TASKS_TABLE, {ArrayJobId, TaskId}) of
        [Task] -> {ok, Task};
        [] -> {error, not_found}
    end.

%% @doc Get all tasks for an array job
-spec get_array_tasks(array_job_id()) -> [#array_task{}].
get_array_tasks(ArrayJobId) ->
    ets:select(?ARRAY_TASKS_TABLE, [{
        #array_task{array_job_id = ArrayJobId, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Cancel entire array job
-spec cancel_array_job(array_job_id()) -> ok | {error, term()}.
cancel_array_job(ArrayJobId) ->
    gen_server:call(?SERVER, {cancel_array_job, ArrayJobId}).

%% @doc Cancel a specific task
-spec cancel_array_task(array_job_id(), task_id()) -> ok | {error, term()}.
cancel_array_task(ArrayJobId, TaskId) ->
    gen_server:call(?SERVER, {cancel_array_task, ArrayJobId, TaskId}).

%% @doc Update task state (called by scheduler/job manager)
-spec update_task_state(array_job_id(), task_id(), map()) -> ok.
update_task_state(ArrayJobId, TaskId, Updates) ->
    gen_server:cast(?SERVER, {update_task_state, ArrayJobId, TaskId, Updates}).

%% @doc Parse array specification string
%% Formats: "0-100", "1,2,5,8", "0-100:2", "0-100%10"
-spec parse_array_spec(binary()) -> {ok, #array_spec{}} | {error, term()}.
parse_array_spec(Spec) when is_binary(Spec) ->
    parse_array_spec(binary_to_list(Spec));
parse_array_spec(Spec) when is_list(Spec) ->
    try
        {ok, do_parse_array_spec(Spec)}
    catch
        throw:{parse_error, Reason} -> {error, Reason}
    end.

%% @doc Format array spec as string
-spec format_array_spec(#array_spec{}) -> binary().
format_array_spec(#array_spec{indices = Indices, max_concurrent = MaxConc})
  when is_list(Indices) ->
    %% Format comma-separated list
    Base = iolist_to_binary(lists:join(<<",">>, [integer_to_binary(I) || I <- Indices])),
    case MaxConc of
        unlimited -> Base;
        N -> iolist_to_binary([Base, <<"%">>, integer_to_binary(N)])
    end;
format_array_spec(#array_spec{start_idx = Start, end_idx = End,
                               step = Step, max_concurrent = MaxConc}) ->
    Base = if
        Start =:= End -> integer_to_binary(Start);
        Step =:= 1 -> iolist_to_binary([integer_to_binary(Start), <<"-">>,
                                        integer_to_binary(End)]);
        true -> iolist_to_binary([integer_to_binary(Start), <<"-">>,
                                  integer_to_binary(End), <<":">>,
                                  integer_to_binary(Step)])
    end,
    case MaxConc of
        unlimited -> Base;
        N -> iolist_to_binary([Base, <<"%">>, integer_to_binary(N)])
    end.

%% @doc Get pending tasks for scheduling
-spec get_pending_tasks(array_job_id()) -> [#array_task{}].
get_pending_tasks(ArrayJobId) ->
    ets:select(?ARRAY_TASKS_TABLE, [{
        #array_task{array_job_id = ArrayJobId, state = pending, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get running tasks
-spec get_running_tasks(array_job_id()) -> [#array_task{}].
get_running_tasks(ArrayJobId) ->
    ets:select(?ARRAY_TASKS_TABLE, [{
        #array_task{array_job_id = ArrayJobId, state = running, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get completed tasks
-spec get_completed_tasks(array_job_id()) -> [#array_task{}].
get_completed_tasks(ArrayJobId) ->
    ets:select(?ARRAY_TASKS_TABLE, [{
        #array_task{array_job_id = ArrayJobId, state = completed, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get array job statistics
-spec get_array_stats(array_job_id()) -> map().
get_array_stats(ArrayJobId) ->
    Tasks = get_array_tasks(ArrayJobId),
    States = [T#array_task.state || T <- Tasks],
    #{
        total => length(Tasks),
        pending => length([S || S <- States, S =:= pending]),
        running => length([S || S <- States, S =:= running]),
        completed => length([S || S <- States, S =:= completed]),
        failed => length([S || S <- States, S =:= failed]),
        cancelled => length([S || S <- States, S =:= cancelled])
    }.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?ARRAY_JOBS_TABLE, [
        named_table, public, set,
        {keypos, #array_job.id}
    ]),
    ets:new(?ARRAY_TASKS_TABLE, [
        named_table, public, set,
        {keypos, #array_task.id}
    ]),
    {ok, #state{next_array_id = 1}}.

handle_call({create_array_job, BaseJob, Spec}, _From, State) ->
    ArrayJobId = State#state.next_array_id,
    case do_create_array_job(ArrayJobId, BaseJob, Spec) of
        ok ->
            {reply, {ok, ArrayJobId}, State#state{next_array_id = ArrayJobId + 1}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({cancel_array_job, ArrayJobId}, _From, State) ->
    Result = do_cancel_array_job(ArrayJobId),
    {reply, Result, State};

handle_call({cancel_array_task, ArrayJobId, TaskId}, _From, State) ->
    Result = do_cancel_array_task(ArrayJobId, TaskId),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({update_task_state, ArrayJobId, TaskId, Updates}, State) ->
    do_update_task_state(ArrayJobId, TaskId, Updates),
    {noreply, State};

handle_cast({check_throttle, ArrayJobId}, State) ->
    %% When a task completes and throttling is in effect, notify the scheduler
    %% that more tasks may be schedulable for this array job
    case get_schedulable_tasks(ArrayJobId) of
        [] ->
            ok;
        SchedulableTasks ->
            %% Notify scheduler that array tasks are ready
            catch flurm_scheduler:trigger_schedule(),
            lager:debug("Array job ~p: ~p tasks ready to schedule",
                       [ArrayJobId, length(SchedulableTasks)])
    end,
    {noreply, State};

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

do_create_array_job(ArrayJobId, BaseJob, #array_spec{} = Spec) ->
    TaskIds = generate_task_ids(Spec),
    TaskCount = length(TaskIds),

    %% Validate
    case TaskCount of
        0 -> {error, empty_array};
        N when N > 10000 -> {error, array_too_large};
        _ ->
            ArrayJob = #array_job{
                id = ArrayJobId,
                name = BaseJob#job.name,
                user = BaseJob#job.user,
                partition = BaseJob#job.partition,
                base_job = BaseJob,
                task_ids = TaskIds,
                task_count = TaskCount,
                max_concurrent = Spec#array_spec.max_concurrent,
                state = pending,
                submit_time = erlang:system_time(second),
                stats = #{}
            },
            ets:insert(?ARRAY_JOBS_TABLE, ArrayJob),

            %% Create task entries
            lists:foreach(fun(TaskId) ->
                Task = #array_task{
                    id = {ArrayJobId, TaskId},
                    array_job_id = ArrayJobId,
                    task_id = TaskId,
                    job_id = undefined,
                    state = pending,
                    exit_code = undefined,
                    start_time = undefined,
                    end_time = undefined,
                    node = undefined
                },
                ets:insert(?ARRAY_TASKS_TABLE, Task)
            end, TaskIds),

            ok
    end.

generate_task_ids(#array_spec{indices = Indices}) when is_list(Indices) ->
    %% Explicit list of indices
    Indices;
generate_task_ids(#array_spec{start_idx = Start, end_idx = End, step = Step}) ->
    %% Range-based specification
    lists:seq(Start, End, Step).

do_cancel_array_job(ArrayJobId) ->
    case ets:lookup(?ARRAY_JOBS_TABLE, ArrayJobId) of
        [ArrayJob] ->
            %% Cancel all pending/running tasks
            Tasks = get_array_tasks(ArrayJobId),
            lists:foreach(fun(Task) ->
                case Task#array_task.state of
                    S when S =:= pending; S =:= running ->
                        cancel_task_internal(Task);
                    _ ->
                        ok
                end
            end, Tasks),

            %% Update array job state
            ets:insert(?ARRAY_JOBS_TABLE, ArrayJob#array_job{
                state = cancelled,
                end_time = erlang:system_time(second)
            }),
            ok;
        [] ->
            {error, not_found}
    end.

do_cancel_array_task(ArrayJobId, TaskId) ->
    case ets:lookup(?ARRAY_TASKS_TABLE, {ArrayJobId, TaskId}) of
        [Task] ->
            case Task#array_task.state of
                S when S =:= pending; S =:= running ->
                    cancel_task_internal(Task),
                    ok;
                _ ->
                    {error, task_not_cancellable}
            end;
        [] ->
            {error, not_found}
    end.

cancel_task_internal(#array_task{} = Task) ->
    %% If task has an actual job, cancel it
    case Task#array_task.job_id of
        undefined -> ok;
        JobId -> catch flurm_job_registry:cancel_job(JobId)
    end,

    %% Update task state
    ets:insert(?ARRAY_TASKS_TABLE, Task#array_task{
        state = cancelled,
        end_time = erlang:system_time(second)
    }),

    %% Check if all tasks are done
    check_array_completion(Task#array_task.array_job_id).

do_update_task_state(ArrayJobId, TaskId, Updates) ->
    case ets:lookup(?ARRAY_TASKS_TABLE, {ArrayJobId, TaskId}) of
        [Task] ->
            NewTask = apply_task_updates(Task, Updates),
            ets:insert(?ARRAY_TASKS_TABLE, NewTask),

            %% Update array job state if needed
            update_array_job_state(ArrayJobId, NewTask#array_task.state),

            %% Check completion
            check_array_completion(ArrayJobId);
        [] ->
            ok
    end.

apply_task_updates(Task, Updates) ->
    Task#array_task{
        job_id = maps:get(job_id, Updates, Task#array_task.job_id),
        state = maps:get(state, Updates, Task#array_task.state),
        exit_code = maps:get(exit_code, Updates, Task#array_task.exit_code),
        start_time = maps:get(start_time, Updates, Task#array_task.start_time),
        end_time = maps:get(end_time, Updates, Task#array_task.end_time),
        node = maps:get(node, Updates, Task#array_task.node)
    }.

update_array_job_state(ArrayJobId, TaskState) ->
    case ets:lookup(?ARRAY_JOBS_TABLE, ArrayJobId) of
        [ArrayJob] when ArrayJob#array_job.state =:= pending,
                        TaskState =:= running ->
            ets:insert(?ARRAY_JOBS_TABLE, ArrayJob#array_job{
                state = running,
                start_time = erlang:system_time(second)
            });
        _ ->
            ok
    end.

check_array_completion(ArrayJobId) ->
    Stats = get_array_stats(ArrayJobId),
    Total = maps:get(total, Stats),
    Completed = maps:get(completed, Stats),
    Failed = maps:get(failed, Stats),
    Cancelled = maps:get(cancelled, Stats),

    Done = Completed + Failed + Cancelled,
    case Done =:= Total of
        true ->
            case ets:lookup(?ARRAY_JOBS_TABLE, ArrayJobId) of
                [ArrayJob] ->
                    FinalState = if
                        Cancelled > 0, Completed =:= 0, Failed =:= 0 -> cancelled;
                        Failed > 0 -> failed;
                        true -> completed
                    end,
                    ets:insert(?ARRAY_JOBS_TABLE, ArrayJob#array_job{
                        state = FinalState,
                        end_time = erlang:system_time(second),
                        stats = Stats
                    });
                [] ->
                    ok
            end;
        false ->
            ok
    end.

%%====================================================================
%% Parsing Functions
%%====================================================================

%% @doc Parse array specification string
%% Formats:
%%   "0-100"       - Range from 0 to 100
%%   "1,2,5,8"     - Explicit list of indices
%%   "0-100:2"     - Range with step
%%   "0-100%10"    - Range with max concurrent limit
%%   "1,3,5,7%2"   - List with max concurrent limit
do_parse_array_spec(Spec) ->
    %% Handle max concurrent suffix (e.g., "0-100%10")
    {BaseSpec, MaxConc} = case string:split(Spec, "%") of
        [Base, ConcStr] ->
            {Base, list_to_integer(string:trim(ConcStr))};
        [Base] ->
            {Base, unlimited}
    end,

    %% Check if this is a comma-separated list
    case string:find(BaseSpec, ",") of
        nomatch ->
            %% Range-based specification
            parse_range_spec(BaseSpec, MaxConc);
        _ ->
            %% Comma-separated list - parse each element which can be a range or value
            parse_list_spec(BaseSpec, MaxConc)
    end.

%% Parse range-based spec like "0-100" or "0-100:2"
parse_range_spec(BaseSpec, MaxConc) ->
    %% Handle step notation (e.g., "0-100:2")
    {RangeSpec, Step} = case string:split(BaseSpec, ":") of
        [Range, StepStr] ->
            {Range, list_to_integer(string:trim(StepStr))};
        [Range] ->
            {Range, 1}
    end,

    %% Parse the range
    case string:split(RangeSpec, "-") of
        [StartStr, EndStr] ->
            #array_spec{
                start_idx = list_to_integer(string:trim(StartStr)),
                end_idx = list_to_integer(string:trim(EndStr)),
                step = Step,
                indices = undefined,
                max_concurrent = MaxConc
            };
        [SingleStr] ->
            N = list_to_integer(string:trim(SingleStr)),
            #array_spec{
                start_idx = N,
                end_idx = N,
                step = 1,
                indices = undefined,
                max_concurrent = MaxConc
            }
    end.

%% Parse comma-separated list like "1,3,5,7" or "1-5,10,15-20"
parse_list_spec(BaseSpec, MaxConc) ->
    Parts = string:split(BaseSpec, ",", all),
    Indices = lists:usort(lists:flatten([parse_list_element(string:trim(P)) || P <- Parts])),
    #array_spec{
        start_idx = undefined,
        end_idx = undefined,
        step = 1,
        indices = Indices,
        max_concurrent = MaxConc
    }.

%% Parse a single element which could be a number or a range
parse_list_element(Elem) ->
    case string:split(Elem, "-") of
        [StartStr, EndStr] ->
            Start = list_to_integer(string:trim(StartStr)),
            End = list_to_integer(string:trim(EndStr)),
            lists:seq(Start, End);
        [SingleStr] ->
            [list_to_integer(string:trim(SingleStr))]
    end.

%%====================================================================
%% Environment Variables for Array Tasks
%%====================================================================

%% @doc Get environment variables for an array task
-spec get_task_env(array_job_id(), task_id()) -> map().
get_task_env(ArrayJobId, TaskId) ->
    case get_array_job(ArrayJobId) of
        {ok, ArrayJob} ->
            #{
                <<"SLURM_ARRAY_JOB_ID">> => integer_to_binary(ArrayJobId),
                <<"SLURM_ARRAY_TASK_ID">> => integer_to_binary(TaskId),
                <<"SLURM_ARRAY_TASK_COUNT">> => integer_to_binary(ArrayJob#array_job.task_count),
                <<"SLURM_ARRAY_TASK_MIN">> => integer_to_binary(lists:min(ArrayJob#array_job.task_ids)),
                <<"SLURM_ARRAY_TASK_MAX">> => integer_to_binary(lists:max(ArrayJob#array_job.task_ids))
            };
        {error, _} ->
            #{}
    end.

%%====================================================================
%% Array Expansion
%%====================================================================

%% @doc Expand an array spec into a list of task index records
%% Returns a list of maps with task information for job submission
-spec expand_array(#array_spec{} | binary()) -> {ok, [map()]} | {error, term()}.
expand_array(Spec) when is_binary(Spec) ->
    case parse_array_spec(Spec) of
        {ok, ParsedSpec} -> expand_array(ParsedSpec);
        {error, Reason} -> {error, Reason}
    end;
expand_array(#array_spec{} = Spec) ->
    TaskIds = generate_task_ids(Spec),
    TaskCount = length(TaskIds),
    MinIdx = lists:min(TaskIds),
    MaxIdx = lists:max(TaskIds),
    MaxConc = Spec#array_spec.max_concurrent,

    Tasks = [#{
        task_id => TaskId,
        task_count => TaskCount,
        task_min => MinIdx,
        task_max => MaxIdx,
        max_concurrent => MaxConc
    } || TaskId <- TaskIds],

    {ok, Tasks}.

%%====================================================================
%% Throttling and Concurrent Task Management
%%====================================================================

%% @doc Check if a new task can be scheduled for an array job (respects throttling)
-spec can_schedule_task(array_job_id()) -> boolean().
can_schedule_task(ArrayJobId) ->
    case get_array_job(ArrayJobId) of
        {ok, #array_job{max_concurrent = unlimited}} ->
            true;
        {ok, #array_job{max_concurrent = MaxConc}} ->
            RunningTasks = get_running_tasks(ArrayJobId),
            length(RunningTasks) < MaxConc;
        {error, _} ->
            false
    end.

%% @doc Get the next pending task that can be scheduled (respecting throttling)
%% Returns the task or {error, throttled} if at limit, or {error, no_pending} if none left
-spec schedule_next_task(array_job_id()) ->
    {ok, #array_task{}} | {error, throttled | no_pending | not_found}.
schedule_next_task(ArrayJobId) ->
    case can_schedule_task(ArrayJobId) of
        false ->
            {error, throttled};
        true ->
            case get_pending_tasks(ArrayJobId) of
                [] ->
                    {error, no_pending};
                [_Task | _] ->
                    %% Return the first pending task (sorted by task_id)
                    SortedTasks = lists:sort(
                        fun(A, B) -> A#array_task.task_id =< B#array_task.task_id end,
                        get_pending_tasks(ArrayJobId)
                    ),
                    {ok, hd(SortedTasks)}
            end
    end.

%% @doc Mark a task as started (called when task begins execution)
-spec task_started(array_job_id(), task_id(), job_id()) -> ok.
task_started(ArrayJobId, TaskId, JobId) ->
    update_task_state(ArrayJobId, TaskId, #{
        state => running,
        job_id => JobId,
        start_time => erlang:system_time(second)
    }).

%% @doc Mark a task as completed (called when task finishes)
-spec task_completed(array_job_id(), task_id(), integer()) -> ok.
task_completed(ArrayJobId, TaskId, ExitCode) ->
    State = case ExitCode of
        0 -> completed;
        _ -> failed
    end,
    update_task_state(ArrayJobId, TaskId, #{
        state => State,
        exit_code => ExitCode,
        end_time => erlang:system_time(second)
    }),
    %% Trigger scheduling of next task (if throttled)
    gen_server:cast(?SERVER, {check_throttle, ArrayJobId}).

%% @doc Get count of tasks that can still be scheduled (for batch scheduling)
-spec get_schedulable_count(array_job_id()) -> non_neg_integer().
get_schedulable_count(ArrayJobId) ->
    case get_array_job(ArrayJobId) of
        {ok, #array_job{max_concurrent = unlimited}} ->
            length(get_pending_tasks(ArrayJobId));
        {ok, #array_job{max_concurrent = MaxConc}} ->
            RunningCount = length(get_running_tasks(ArrayJobId)),
            PendingCount = length(get_pending_tasks(ArrayJobId)),
            Available = MaxConc - RunningCount,
            min(Available, PendingCount);
        {error, _} ->
            0
    end.

%% @doc Get the next N tasks that can be scheduled
-spec get_schedulable_tasks(array_job_id()) -> [#array_task{}].
get_schedulable_tasks(ArrayJobId) ->
    Count = get_schedulable_count(ArrayJobId),
    case Count > 0 of
        true ->
            PendingTasks = get_pending_tasks(ArrayJobId),
            SortedTasks = lists:sort(
                fun(A, B) -> A#array_task.task_id =< B#array_task.task_id end,
                PendingTasks
            ),
            lists:sublist(SortedTasks, Count);
        false ->
            []
    end.
