%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Header Definitions
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FLURM_CONFIG_HRL).
-define(FLURM_CONFIG_HRL, true).

%% ETS table for runtime configuration
-define(CONFIG_TABLE, flurm_config_table).

%% Type definitions
-type config_key() :: atom() | {atom(), atom()}.
-type config_value() :: term().

%% Default configuration values
-define(DEFAULT_CONTROLLER_PORT, 6817).
-define(DEFAULT_CONTROLLER_HOST, "localhost").
-define(DEFAULT_HEARTBEAT_INTERVAL, 10000).
-define(DEFAULT_SCHEDULING_INTERVAL, 1000).
-define(DEFAULT_NODE_TIMEOUT, 60000).

%% Configuration sections
-record(controller_config, {
    listen_port = ?DEFAULT_CONTROLLER_PORT :: pos_integer(),
    listen_address = "0.0.0.0" :: string(),
    max_connections = 1000 :: pos_integer(),
    scheduling_interval = ?DEFAULT_SCHEDULING_INTERVAL :: pos_integer()
}).

-record(node_daemon_config, {
    controller_host = ?DEFAULT_CONTROLLER_HOST :: string(),
    controller_port = ?DEFAULT_CONTROLLER_PORT :: pos_integer(),
    heartbeat_interval = ?DEFAULT_HEARTBEAT_INTERVAL :: pos_integer()
}).

-record(cluster_config, {
    name = <<"flurm">> :: binary(),
    node_timeout = ?DEFAULT_NODE_TIMEOUT :: pos_integer(),
    data_dir = "/var/lib/flurm" :: string()
}).

-endif.
