# FLURM Development Guide

This document provides guidelines for contributing to FLURM, including environment setup, code style, and procedures for adding new features.

## Table of Contents

1. [Development Environment](#development-environment)
   - [Git Hooks](#git-hooks)
2. [Project Structure](#project-structure)
3. [Code Style Guide](#code-style-guide)
4. [Adding New Message Types](#adding-new-message-types)
5. [Adding Scheduler Plugins](#adding-scheduler-plugins)
6. [Hot Code Upgrades](#hot-code-upgrades)
7. [Debugging](#debugging)
8. [Pull Request Process](#pull-request-process)

## Development Environment

### Required Tools

| Tool | Version | Purpose |
|------|---------|---------|
| Erlang/OTP | 26.0+ | Runtime |
| rebar3 | 3.22+ | Build tool |
| Git | 2.30+ | Version control |
| Make | 4.0+ | Build automation |
| Docker | 24.0+ | Container testing |

### Setup

```bash
# Clone repository
git clone https://github.com/your-org/flurm.git
cd flurm

# Install dependencies
rebar3 get-deps

# Compile
rebar3 compile

# Run tests
rebar3 eunit
rebar3 ct

# Start development shell
rebar3 shell
```

### Git Hooks

FLURM includes versioned hooks and consistency scripts so checks are repeatable on any machine.

#### Install Hooks

```bash
make hooks-install
```

This configures `core.hooksPath=.githooks`.

#### Hook Levels

- `pre-commit` (`.githooks/pre-commit`): fast compile + critical unit tests
- `pre-push` (`.githooks/pre-push`): medium checks (`flurm_dbd` app tests with coverage + coverage threshold gate)

Optional Docker interop on push:

```bash
FLURM_PREPUSH_DOCKER=1 git push
```

#### Consistency Commands

Use these locally or in external CI systems (for example Jenkins/Buildkite/self-hosted):

```bash
# Quick (pre-commit class)
make check-quick

# Medium (pre-push class)
make check-prepush

# Coverage threshold gate only
make check-coverage

# Full deterministic suite
make check-consistency

# Full suite + Docker interop checks
FLURM_CHECK_DOCKER=1 make check-consistency
```

Set minimum coverage threshold for the DBD modules:

```bash
FLURM_COVER_MIN=92 make check-prepush
```

### IDE Configuration

#### VS Code

```json
// .vscode/settings.json
{
  "erlang.languageServerPath": "erlang_ls",
  "files.associations": {
    "*.erl": "erlang",
    "*.hrl": "erlang",
    "*.app.src": "erlang",
    "rebar.config": "erlang"
  },
  "editor.formatOnSave": true
}
```

#### Emacs

```elisp
;; .dir-locals.el
((erlang-mode
  (flycheck-erlang-include-path . ("../include" "../../include"))
  (flycheck-erlang-library-path . ("../_build/default/lib"))))
```

### Development Configuration

```erlang
%% config/dev.config
[
  {flurm, [
    {cluster_name, "dev"},
    {slurmctld_port, 16817},  % Avoid conflict with production
    {log_level, debug},
    {scheduler_plugin, flurm_sched_fifo},  % Simple scheduler for dev
    {checkpoint_interval, 5000}  % Frequent checkpoints for testing
  ]},

  {kernel, [
    {logger_level, debug}
  ]}
].
```

## Project Structure

```
flurm/
├── apps/
│   ├── flurm/                    # Main application
│   │   ├── src/
│   │   │   ├── flurm_app.erl           # Application callback
│   │   │   ├── flurm_sup.erl           # Root supervisor
│   │   │   ├── flurm_job_manager.erl   # Job state management
│   │   │   ├── flurm_node_manager.erl  # Node management
│   │   │   ├── flurm_partition_manager.erl
│   │   │   ├── flurm_queue_manager.erl
│   │   │   └── flurm_state_manager.erl
│   │   ├── include/
│   │   │   ├── flurm.hrl               # Common definitions
│   │   │   └── flurm_records.hrl       # Record definitions
│   │   └── priv/
│   │       └── schema/                  # Mnesia schemas
│   │
│   ├── flurm_protocol/           # Protocol handling
│   │   ├── src/
│   │   │   ├── flurm_protocol.erl      # Encode/decode
│   │   │   ├── flurm_protocol_sup.erl
│   │   │   ├── flurm_acceptor.erl      # TCP acceptor
│   │   │   └── flurm_connection.erl    # Connection handler
│   │   └── include/
│   │       └── flurm_protocol.hrl      # Protocol constants
│   │
│   ├── flurm_scheduler/          # Scheduling engine
│   │   ├── src/
│   │   │   ├── flurm_scheduler_engine.erl
│   │   │   ├── flurm_sched_fifo.erl    # FIFO plugin
│   │   │   ├── flurm_sched_backfill.erl
│   │   │   └── flurm_sched_fairshare.erl
│   │   └── include/
│   │       └── flurm_scheduler.hrl
│   │
│   ├── flurm_consensus/          # Raft implementation
│   │   ├── src/
│   │   │   ├── flurm_consensus.erl     # Raft state machine
│   │   │   ├── flurm_raft_log.erl      # Log management
│   │   │   └── flurm_raft_rpc.erl      # RPC handling
│   │   └── include/
│   │       └── flurm_raft.hrl
│   │
│   └── flurm_metrics/            # Observability
│       └── src/
│           ├── flurm_prometheus.erl
│           └── flurm_tracer.erl
│
├── config/
│   ├── sys.config                # Production config
│   ├── dev.config                # Development config
│   └── test.config               # Test config
│
├── specs/                        # TLA+ specifications
│   ├── Consensus.tla
│   ├── Scheduler.tla
│   └── *.cfg
│
├── test/
│   ├── unit/                     # EUnit tests
│   ├── property/                 # PropEr tests
│   ├── integration/              # Common Test suites
│   └── simulation/               # Deterministic simulation
│
├── docs/                         # Documentation
│
├── scripts/                      # Utility scripts
│   ├── setup_dev.sh
│   ├── run_tests.sh
│   └── release.sh
│
├── rebar.config                  # Build configuration
├── rebar.lock                    # Dependency lock
├── Makefile                      # Build automation
└── README.md
```

## Code Style Guide

### General Principles

1. **Clarity over cleverness**: Write code that is easy to understand
2. **Explicit over implicit**: Avoid magic; make behavior obvious
3. **Small functions**: Each function should do one thing
4. **Meaningful names**: Variables and functions should be self-documenting

### Formatting

```erlang
%% Module header
-module(flurm_example).
-behaviour(gen_server).

%% Module description
%% @doc This module handles example operations.
%%
%% == Overview ==
%% Brief description of the module's purpose.

%% Exports grouped by category
-export([start_link/0, start_link/1]).           % Lifecycle
-export([submit/1, cancel/1, status/1]).         % API
-export([init/1, handle_call/3, handle_cast/2]). % Callbacks

%% Type exports
-export_type([job_id/0, job_spec/0]).

%% Include files
-include("flurm.hrl").
-include_lib("kernel/include/logger.hrl").

%% Macros (SCREAMING_SNAKE_CASE)
-define(DEFAULT_TIMEOUT, 5000).
-define(MAX_RETRIES, 3).

%% Types
-type job_id() :: pos_integer().
-type job_spec() :: #{
    name := binary(),
    script := binary(),
    partition => binary(),
    num_tasks => pos_integer()
}.

%% Records
-record(state, {
    jobs = #{} :: #{job_id() => job()},
    next_id = 1 :: pos_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the server with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start the server with options.
-spec start_link(proplists:proplist()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @doc Submit a new job.
-spec submit(job_spec()) -> {ok, job_id()} | {error, term()}.
submit(JobSpec) ->
    gen_server:call(?MODULE, {submit, JobSpec}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    State = init_state(Opts),
    {ok, State}.

handle_call({submit, JobSpec}, _From, State) ->
    case validate_job(JobSpec) of
        ok ->
            {JobId, NewState} = do_submit(JobSpec, State),
            {reply, {ok, JobId}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
init_state(Opts) ->
    #state{
        jobs = #{},
        next_id = proplists:get_value(start_id, Opts, 1)
    }.

%% @private
validate_job(#{name := Name, script := Script}) when
    is_binary(Name), is_binary(Script) ->
    ok;
validate_job(_) ->
    {error, invalid_job_spec}.
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Modules | snake_case | `flurm_job_manager` |
| Functions | snake_case | `submit_job` |
| Variables | CamelCase | `JobSpec`, `NewState` |
| Records | snake_case | `#job_info{}` |
| Macros | SCREAMING_SNAKE | `?MAX_TIMEOUT` |
| Types | snake_case | `-type job_id()` |
| Atoms | snake_case | `pending`, `job_submitted` |

### Error Handling

```erlang
%% Use tagged tuples for results
-spec get_job(job_id()) -> {ok, job()} | {error, not_found | term()}.
get_job(JobId) ->
    case ets:lookup(jobs, JobId) of
        [{JobId, Job}] -> {ok, Job};
        [] -> {error, not_found}
    end.

%% Pattern match errors explicitly
handle_result(Result) ->
    case Result of
        {ok, Value} ->
            process_value(Value);
        {error, not_found} ->
            ?LOG_WARNING("Item not found"),
            default_value();
        {error, Reason} ->
            ?LOG_ERROR("Unexpected error: ~p", [Reason]),
            erlang:error({unexpected_error, Reason})
    end.

%% Use try/catch sparingly, prefer pattern matching
safe_decode(Data) ->
    try
        {ok, do_decode(Data)}
    catch
        error:badarg -> {error, invalid_data};
        _:Reason -> {error, {decode_failed, Reason}}
    end.
```

### Logging

```erlang
%% Use kernel logger macros
-include_lib("kernel/include/logger.hrl").

%% Log levels:
%% - emergency, alert, critical: System is unusable
%% - error: Operation failed
%% - warning: Unexpected but handled
%% - notice: Normal but significant
%% - info: Informational
%% - debug: Detailed debugging

submit_job(JobSpec) ->
    ?LOG_INFO("Submitting job: ~p", [JobSpec]),
    case validate(JobSpec) of
        ok ->
            ?LOG_DEBUG("Job validation passed"),
            do_submit(JobSpec);
        {error, Reason} ->
            ?LOG_WARNING("Job validation failed: ~p", [Reason]),
            {error, Reason}
    end.
```

## Adding New Message Types

### Step 1: Define Constants

```erlang
%% In include/flurm_protocol.hrl

%% Add message type constant
-define(REQUEST_NEW_FEATURE, 2100).
-define(RESPONSE_NEW_FEATURE, 5100).
```

### Step 2: Define Records

```erlang
%% In include/flurm_protocol.hrl

-record(new_feature_request, {
    param1 :: binary(),
    param2 :: integer(),
    param3 :: [binary()]
}).

-record(new_feature_response, {
    result :: ok | error,
    data :: term()
}).
```

### Step 3: Implement Decoder

```erlang
%% In src/flurm_protocol.erl

decode_body(?REQUEST_NEW_FEATURE, Body) ->
    decode_new_feature_request(Body);
decode_body(?RESPONSE_NEW_FEATURE, Body) ->
    decode_new_feature_response(Body).

decode_new_feature_request(Data) ->
    {Param1, R1} = decode_string(Data),
    <<Param2:32/big, R2/binary>> = R1,
    {Param3, _Rest} = decode_string_list(R2),
    {ok, #new_feature_request{
        param1 = Param1,
        param2 = Param2,
        param3 = Param3
    }}.
```

### Step 4: Implement Encoder

```erlang
%% In src/flurm_protocol.erl

encode_body(#new_feature_request{} = Req) ->
    [
        encode_string(Req#new_feature_request.param1),
        <<(Req#new_feature_request.param2):32/big>>,
        encode_string_list(Req#new_feature_request.param3)
    ];

encode_body(#new_feature_response{result = Result, data = Data}) ->
    ResultCode = case Result of ok -> 0; error -> 1 end,
    [<<ResultCode:32/big>>, encode_term(Data)].
```

### Step 5: Add Handler

```erlang
%% In src/flurm_request_handler.erl

handle_request(?REQUEST_NEW_FEATURE, #new_feature_request{} = Req, State) ->
    case flurm_new_feature:process(Req) of
        {ok, Result} ->
            Response = #new_feature_response{result = ok, data = Result},
            {reply, ?RESPONSE_NEW_FEATURE, Response, State};
        {error, Reason} ->
            Response = #new_feature_response{result = error, data = Reason},
            {reply, ?RESPONSE_NEW_FEATURE, Response, State}
    end.
```

### Step 6: Add Tests

```erlang
%% In test/flurm_protocol_tests.erl

new_feature_roundtrip_test() ->
    Req = #new_feature_request{
        param1 = <<"test">>,
        param2 = 42,
        param3 = [<<"a">>, <<"b">>, <<"c">>]
    },
    Encoded = flurm_protocol:encode_message(?REQUEST_NEW_FEATURE, Req),
    {ok, Decoded} = flurm_protocol:decode_message(Encoded),
    ?assertEqual(Req, Decoded).
```

## Adding Scheduler Plugins

### Plugin Behaviour

```erlang
%% In include/flurm_scheduler.hrl

-callback init(Config :: map()) ->
    {ok, State :: term()} | {error, Reason :: term()}.

-callback schedule(Jobs :: [job()], Nodes :: [node_info()], State :: term()) ->
    {Allocations :: [allocation()], NewState :: term()}.

-callback priority(Job :: job(), State :: term()) ->
    Priority :: integer().

-callback terminate(Reason :: term(), State :: term()) ->
    ok.
```

### Example Plugin

```erlang
-module(flurm_sched_custom).
-behaviour(flurm_scheduler_plugin).

-export([init/1, schedule/3, priority/2, terminate/2]).

-record(state, {
    weight_age :: float(),
    weight_size :: float()
}).

init(Config) ->
    State = #state{
        weight_age = maps:get(weight_age, Config, 0.5),
        weight_size = maps:get(weight_size, Config, 0.5)
    },
    {ok, State}.

schedule(Jobs, Nodes, State) ->
    %% Sort jobs by priority
    SortedJobs = lists:sort(
        fun(A, B) -> priority(A, State) >= priority(B, State) end,
        Jobs
    ),
    %% Allocate in order
    {Allocations, _} = lists:foldl(
        fun(Job, {Allocs, AvailNodes}) ->
            case find_nodes(Job, AvailNodes) of
                {ok, SelectedNodes} ->
                    Alloc = #allocation{
                        job_id = Job#job.id,
                        nodes = SelectedNodes
                    },
                    RemainingNodes = AvailNodes -- SelectedNodes,
                    {[Alloc | Allocs], RemainingNodes};
                not_found ->
                    {Allocs, AvailNodes}
            end
        end,
        {[], Nodes},
        SortedJobs
    ),
    {lists:reverse(Allocations), State}.

priority(#job{submit_time = SubmitTime, num_tasks = NumTasks},
         #state{weight_age = WAge, weight_size = WSize}) ->
    Age = erlang:system_time(second) - SubmitTime,
    %% Higher age = higher priority, smaller jobs = higher priority
    round(WAge * Age - WSize * NumTasks).

terminate(_Reason, _State) ->
    ok.
```

### Register Plugin

```erlang
%% In config/sys.config
{flurm, [
    {scheduler_plugins, [
        {flurm_sched_fifo, #{}},
        {flurm_sched_backfill, #{lookahead => 3600}},
        {flurm_sched_custom, #{weight_age => 0.7, weight_size => 0.3}}
    ]},
    {scheduler_plugin, flurm_sched_custom}
]}
```

## Hot Code Upgrades

### Creating Appup Files

```erlang
%% In src/flurm.appup.src
{VSN,
  %% Upgrade from
  [
    {"1.0.0", [
        {load_module, flurm_job_manager},
        {update, flurm_scheduler_engine, {advanced, []}}
    ]}
  ],
  %% Downgrade to
  [
    {"1.0.0", [
        {load_module, flurm_job_manager},
        {update, flurm_scheduler_engine, {advanced, []}}
    ]}
  ]
}.
```

### State Migration

```erlang
%% In flurm_scheduler_engine.erl

%% Old state record (v1.0.0)
-record(state_v1, {
    jobs :: [job()],
    nodes :: [node()]
}).

%% New state record (v1.1.0)
-record(state, {
    jobs :: [job()],
    nodes :: [node()],
    cache :: #{job_id() => priority()}  % New field
}).

%% Code change callback
code_change({down, "1.0.0"}, #state{jobs = Jobs, nodes = Nodes}, _Extra) ->
    %% Downgrade: remove new field
    {ok, #state_v1{jobs = Jobs, nodes = Nodes}};
code_change("1.0.0", #state_v1{jobs = Jobs, nodes = Nodes}, _Extra) ->
    %% Upgrade: add new field with default
    {ok, #state{jobs = Jobs, nodes = Nodes, cache = #{}}}.
```

### Performing Upgrade

```bash
# Generate appup
rebar3 appup generate

# Build release with relup
rebar3 release
rebar3 relup

# Deploy upgrade
/opt/flurm/bin/flurm upgrade "1.1.0"

# Verify
/opt/flurm/bin/flurm versions
```

## Debugging

### Interactive Debugging

```erlang
%% Attach to running system
$ ./bin/flurm remote_console

%% Inspect process state
> sys:get_state(flurm_job_manager).

%% Trace function calls
> dbg:tracer().
> dbg:p(all, c).
> dbg:tpl(flurm_protocol, decode_message, '_', []).

%% Stop tracing
> dbg:stop_clear().

%% Inspect ETS tables
> ets:tab2list(flurm_jobs).

%% Check process info
> process_info(whereis(flurm_scheduler_engine)).

%% Memory analysis
> recon:proc_count(memory, 10).
> recon:bin_leak(10).
```

### Debugging in Tests

```erlang
%% Add debug output
-ifdef(TEST).
-define(DBG(Fmt, Args), io:format(user, "[DBG] " ++ Fmt ++ "~n", Args)).
-else.
-define(DBG(Fmt, Args), ok).
-endif.

my_function(X) ->
    ?DBG("my_function called with ~p", [X]),
    %% ...
```

### Core Dumps

```bash
# Enable core dumps
ulimit -c unlimited
export ERL_CRASH_DUMP=/var/log/flurm/erl_crash.dump
export ERL_CRASH_DUMP_SECONDS=30

# Analyze crash dump
erl -s crashdump_viewer
```

## Pull Request Process

### Before Submitting

1. **Run all tests**: `rebar3 eunit && rebar3 ct && rebar3 proper`
2. **Check types**: `rebar3 dialyzer`
3. **Lint code**: `rebar3 lint`
4. **Update documentation**: If adding new features
5. **Add tests**: For new functionality

### PR Template

```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Property tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass locally

## Checklist
- [ ] Code follows style guide
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No new warnings
- [ ] Dialyzer passes
```

### Review Criteria

- Code correctness and safety
- Test coverage
- Performance implications
- Documentation clarity
- Backward compatibility

---

See also:
- [Architecture](architecture.md) for system design
- [Testing Guide](testing.md) for testing requirements
- [Agent Guide](AGENT_GUIDE.md) for AI-assisted development
