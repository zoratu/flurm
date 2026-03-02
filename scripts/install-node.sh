#!/bin/bash
#
# FLURM Node Daemon Installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/youruser/flurm/main/scripts/install-node.sh | bash -s -- [options]
#
# Options:
#   --controllers "host1,host2"   Comma-separated list of controller hostnames (required)
#   --cookie "cookie_value"       Erlang cookie for cluster auth (required)
#   --port 6819                   Controller port (default: 6819)
#   --install-dir /opt/flurm      Installation directory (default: /opt/flurm)
#   --skip-erlang                 Skip Erlang installation (if already installed)
#   --skip-build                  Skip building from source (use prebuilt release)
#
set -e

# Defaults
CONTROLLERS=""
COOKIE=""
PORT="6819"
INSTALL_DIR="/opt/flurm"
SKIP_ERLANG=false
SKIP_BUILD=false
FLURM_REPO="https://github.com/zoratu/flurm.git"
FLURM_BRANCH="main"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --controllers)
            CONTROLLERS="$2"
            shift 2
            ;;
        --cookie)
            COOKIE="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --skip-erlang)
            SKIP_ERLANG=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --repo)
            FLURM_REPO="$2"
            shift 2
            ;;
        --branch)
            FLURM_BRANCH="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$CONTROLLERS" ]; then
    echo "Error: --controllers is required"
    echo "Example: --controllers 'c3po,r2'"
    exit 1
fi

if [ -z "$COOKIE" ]; then
    echo "Error: --cookie is required"
    exit 1
fi

echo "=== FLURM Node Daemon Installer ==="
echo "Controllers: $CONTROLLERS"
echo "Install dir: $INSTALL_DIR"
echo "Port: $PORT"

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS=$(uname -s)
fi

echo "Detected OS: $OS"

# Install Erlang
install_erlang() {
    echo "=== Installing Erlang ==="
    case $OS in
        ubuntu|debian)
            apt-get update
            apt-get install -y erlang-base erlang-dev erlang-parsetools erlang-tools \
                erlang-eunit erlang-crypto erlang-ssl erlang-inets erlang-mnesia \
                erlang-runtime-tools erlang-xmerl git make
            ;;
        fedora|centos|rhel)
            dnf install -y erlang git make
            ;;
        *)
            echo "Unsupported OS for automatic Erlang installation: $OS"
            echo "Please install Erlang manually and rerun with --skip-erlang"
            exit 1
            ;;
    esac
}

# Install rebar3
install_rebar3() {
    echo "=== Installing rebar3 ==="
    if ! command -v rebar3 &> /dev/null; then
        curl -fsSL https://s3.amazonaws.com/rebar3/rebar3 -o /usr/local/bin/rebar3
        chmod +x /usr/local/bin/rebar3
    fi
}

# Build FLURM from source
build_flurm() {
    echo "=== Building FLURM from source ==="

    # Clone repo
    TEMP_DIR=$(mktemp -d)
    git clone --depth 1 --branch "$FLURM_BRANCH" "$FLURM_REPO" "$TEMP_DIR/flurm"
    cd "$TEMP_DIR/flurm"

    # Build release
    rebar3 as prod release

    # Install
    mkdir -p "$INSTALL_DIR"
    cp -R _build/prod/rel/flurmctld/* "$INSTALL_DIR/"

    # Cleanup
    rm -rf "$TEMP_DIR"
}

# Generate configuration
generate_config() {
    echo "=== Generating configuration ==="

    mkdir -p "$INSTALL_DIR/config"
    mkdir -p "$INSTALL_DIR/data"
    mkdir -p "$INSTALL_DIR/log"

    # Convert comma-separated controllers to Erlang list format
    CONTROLLER_LIST=$(echo "$CONTROLLERS" | sed 's/,/", "/g')

    # Generate sys.config
    cat > "$INSTALL_DIR/config/sys.config" << EOF
[
    {flurm_node_daemon, [
        {controller_hosts, ["$CONTROLLER_LIST"]},
        {controller_port, $PORT},
        {heartbeat_interval, 30000},
        {state_file, "$INSTALL_DIR/data/node_state.dat"}
    ]},

    {lager, [
        {handlers, [
            {lager_console_backend, [{level, info}]},
            {lager_file_backend, [
                {file, "$INSTALL_DIR/log/node.log"},
                {level, info}
            ]}
        ]},
        {error_logger_redirect, true}
    ]},

    {sasl, [{sasl_error_logger, false}]},
    {kernel, [{logger_level, info}]}
].
EOF

    # Generate vm.args
    NODE_NAME="flurm_node_$(hostname -s)"
    cat > "$INSTALL_DIR/config/vm.args" << EOF
-sname $NODE_NAME
-setcookie $COOKIE
+K true
+A 64
EOF
}

# Create systemd service
create_systemd_service() {
    echo "=== Creating systemd service ==="

    cat > /etc/systemd/system/flurm-node.service << EOF
[Unit]
Description=FLURM Node Daemon
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
Environment=HOME=$INSTALL_DIR
ExecStart=$INSTALL_DIR/bin/flurmctld foreground
ExecStop=$INSTALL_DIR/bin/flurmctld stop
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable flurm-node
}

# Start the node daemon
start_node() {
    echo "=== Starting FLURM node daemon ==="

    cd "$INSTALL_DIR"

    # Try systemd first, fall back to direct start
    if command -v systemctl &> /dev/null; then
        systemctl start flurm-node
        echo "Started via systemd. Check status with: systemctl status flurm-node"
    else
        # Start directly with erl
        erl -pa lib/*/ebin \
            -config config/sys.config \
            -args_file config/vm.args \
            -eval 'application:ensure_all_started(flurm_node_daemon).' \
            -noshell -detached
        echo "Started directly. Check with: pgrep -f beam"
    fi
}

# Main installation flow
main() {
    # Check if running as root
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root (sudo)"
        exit 1
    fi

    # Install Erlang if needed
    if [ "$SKIP_ERLANG" = false ]; then
        if ! command -v erl &> /dev/null; then
            install_erlang
        else
            echo "Erlang already installed: $(erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell)"
        fi
    fi

    # Build FLURM
    if [ "$SKIP_BUILD" = false ]; then
        install_rebar3
        build_flurm
    fi

    # Generate config
    generate_config

    # Create systemd service (if available)
    if command -v systemctl &> /dev/null; then
        create_systemd_service
    fi

    # Start the node
    start_node

    echo ""
    echo "=== FLURM Node Installation Complete ==="
    echo "Install directory: $INSTALL_DIR"
    echo "Node name: flurm_node_$(hostname -s)"
    echo "Connecting to controllers: $CONTROLLERS"
    echo ""
    echo "Logs: $INSTALL_DIR/log/node.log"
}

main
