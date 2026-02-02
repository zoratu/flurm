%% FLURM Common Test Specification
%% Run with: rebar3 ct

%% Include all SUITE files from each application's integration_test directory
{suites, "../apps/flurm_controller/integration_test", all}.
{suites, "../apps/flurm_core/integration_test", all}.
{suites, "../apps/flurm_node_daemon/integration_test", all}.
{suites, "../apps/flurm_protocol/integration_test", all}.

%% Test configuration
{config, "../config/test.sys.config"}.

%% Logging
{logdir, "../_build/test/logs"}.

%% Verbosity
{verbosity, [{default, 50}]}.

%% Create HTML report
{ct_hooks, []}.
