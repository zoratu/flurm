# FLURM Security Documentation

This document provides comprehensive security guidance for deploying and operating FLURM in production environments.

## Table of Contents

1. [Security Overview](#security-overview)
2. [Authentication](#authentication)
3. [Authorization](#authorization)
4. [Network Security](#network-security)
5. [Data Protection](#data-protection)
6. [Hardening Checklist](#hardening-checklist)
7. [Security Differences from SLURM](#security-differences-from-slurm)

---

## Security Overview

### FLURM Security Model

FLURM implements a multi-layered security model designed to provide:

- **Authentication**: Verifying the identity of users and services
- **Authorization**: Controlling what authenticated entities can do
- **Integrity**: Ensuring data and commands are not tampered with
- **Confidentiality**: Protecting sensitive information in transit and at rest
- **Auditability**: Logging security-relevant events for review

### Defense in Depth Approach

FLURM employs multiple security layers:

```
+--------------------------------------------------+
|  Layer 1: Network Perimeter (Firewall/VPN)       |
+--------------------------------------------------+
|  Layer 2: Transport Security (TLS)               |
+--------------------------------------------------+
|  Layer 3: Authentication (MUNGE/Erlang Cookie)   |
+--------------------------------------------------+
|  Layer 4: Authorization (Accounts/QOS/Partitions)|
+--------------------------------------------------+
|  Layer 5: Resource Limits (TRES/Quotas)          |
+--------------------------------------------------+
|  Layer 6: Audit Logging                          |
+--------------------------------------------------+
```

**Key Security Components:**

| Component | Purpose | Implementation |
|-----------|---------|----------------|
| MUNGE Authentication | SLURM client identity verification | `flurm_munge.erl`, `flurm_protocol_auth.erl` |
| Erlang Cookie | Inter-node authentication | Native Erlang distribution |
| Account Manager | User/account authorization | `flurm_account_manager.erl` |
| QOS Manager | Resource access control | `flurm_qos.erl` |
| Rate Limiter | DoS protection | `flurm_rate_limiter.erl` |

---

## Authentication

### MUNGE Authentication (SLURM Compatible)

FLURM supports MUNGE authentication for compatibility with existing SLURM clients. MUNGE provides cryptographic authentication that verifies:

- User identity (UID/GID)
- Client hostname
- Request timestamp (prevents replay attacks)

**Implementation Details:**

The `flurm_munge` module (`apps/flurm_protocol/src/flurm_munge.erl`) provides MUNGE integration:

```erlang
%% Check if MUNGE is available on the system
flurm_munge:is_available() -> boolean()

%% Generate a MUNGE credential with payload
flurm_munge:encode(Payload) -> {ok, Credential} | {error, Reason}
```

**Configuration:**

MUNGE authentication is configured in `flurm.conf` or through application environment:

```erlang
%% Default authentication type
{authtype, <<"auth/munge">>}
{cryptotype, <<"crypto/munge">>}
```

**Requirements:**

- MUNGE daemon (`munged`) must be running on all nodes
- Shared MUNGE key (`/etc/munge/munge.key`) must be identical across all nodes
- MUNGE key should be 256+ bits of random data
- Key file permissions: `400` (owner read-only)
- Key file ownership: `munge:munge`

### How FLURM Validates Credentials

The `flurm_protocol_auth` module handles credential validation:

1. **Wire Format**: Authentication data follows the message body in SLURM protocol:
   ```
   <<Length:32/big, Header:10/binary, Body/binary, ExtraData:39/binary>>
   ```

2. **Extra Data Structure** (39 bytes):
   - Response type indicator (`0x0064`)
   - Hostname (with length prefix)
   - Timestamp (4 bytes, big-endian)

3. **Validation Process**:
   - Extract credentials from wire format via `decode_extra/1`
   - Verify MUNGE credential using system `unmunge` command
   - Check timestamp is within acceptable window (prevents replay)
   - Extract authenticated UID/GID for authorization

**Code Reference:**
```erlang
%% From flurm_controller_acceptor.erl
%% Try decode with auth first (for real SLURM clients)
DecodeResult = case flurm_protocol_codec:decode_with_extra(MessageBin) of
    {ok, Msg, ExtraInfo, Rest} ->
        {ok, Msg, ExtraInfo, Rest};
    {error, _AuthErr} ->
        %% Fallback for test clients without auth
        case flurm_protocol_codec:decode(MessageBin) of
            {ok, Msg, Rest} -> {ok, Msg, #{}, Rest};
            PlainErr -> PlainErr
        end
end
```

### Erlang Cookie for Inter-Node Communication

FLURM uses Erlang's native distributed communication, which requires a shared cookie for node authentication.

**How Erlang Cookies Work:**

- Each Erlang node has a secret cookie
- Nodes can only connect if they share the same cookie
- Cookies are used for EPMD (Erlang Port Mapper Daemon) authentication

**Configuration:**

Set the cookie via command line or file:

```bash
# Command line
erl -setcookie my_secret_cookie -name flurm@node1

# File-based (~/.erlang.cookie)
echo "my_very_secret_cookie" > ~/.erlang.cookie
chmod 400 ~/.erlang.cookie
```

**Production Requirements:**

- Cookie must be at least 32 characters of random data
- Use the same cookie on all FLURM cluster nodes
- Cookie file permissions: `400` (owner read-only)
- Never commit cookies to version control

**Generate a Secure Cookie:**
```bash
openssl rand -base64 32 > ~/.erlang.cookie
chmod 400 ~/.erlang.cookie
```

### API Key Authentication Option

For REST API access (metrics endpoint), FLURM can be configured with API key authentication:

**Configuration:**
```erlang
{flurm_controller, [
    {api_key, <<"your-secure-api-key-here">>},
    {require_api_key, true}
]}
```

**Best Practices:**
- Generate keys using cryptographically secure random number generator
- Rotate keys periodically
- Use environment variables instead of config files for keys
- Log API key usage for audit

---

## Authorization

### User/Account-Based Access Control

FLURM implements hierarchical accounting similar to SLURM, managed by `flurm_account_manager.erl`.

**Entity Hierarchy:**

```
Cluster
  |
  +-- Accounts (organizational units)
        |
        +-- Users
        |
        +-- Child Accounts
```

**Account Structure:**
```erlang
#account{
    name :: binary(),           % Account name (e.g., <<"physics">>)
    description :: binary(),    % Human-readable description
    organization :: binary(),   % Organization name
    parent :: binary(),         % Parent account (empty for root)
    coordinators :: [binary()], % Account coordinators (admin users)
    default_qos :: binary(),    % Default QOS for this account
    fairshare :: integer(),     % Fair-share allocation (1-100)
    max_jobs :: integer(),      % Max concurrent running jobs (0 = unlimited)
    max_submit :: integer(),    % Max submitted jobs (0 = unlimited)
    max_wall :: integer()       % Max wall time in minutes (0 = unlimited)
}
```

**User Structure:**
```erlang
#acct_user{
    name :: binary(),           % Username
    default_account :: binary(), % Default account for job submission
    accounts :: [binary()],     % List of associated accounts
    default_qos :: binary(),    % Default QOS
    admin_level :: none | operator | admin,  % Administrative privileges
    fairshare :: integer(),     % User's fair-share within account
    max_jobs :: integer(),      % Per-user job limits
    max_submit :: integer(),
    max_wall :: integer()
}
```

**Association-Based Limits:**

Associations tie users to accounts with specific limits:

```erlang
#association{
    cluster :: binary(),
    account :: binary(),
    user :: binary(),
    partition :: binary(),      % Optional partition restriction
    shares :: integer(),        % Fair-share shares
    grp_tres :: map(),          % Group TRES limits
    max_jobs :: integer(),
    max_submit :: integer(),
    max_wall_per_job :: integer(),
    qos :: [binary()],          % Allowed QOS list
    default_qos :: binary()
}
```

### Partition Access Restrictions

Partitions can restrict which users and accounts can submit jobs:

**Partition Configuration:**
```erlang
#partition_spec{
    name :: binary(),
    nodes :: [binary()],
    max_time :: integer(),      % Maximum job duration (seconds)
    default_time :: integer(),  % Default if not specified
    max_nodes :: integer(),     % Maximum nodes per job
    priority :: integer(),      % Partition priority factor
    state :: up | down | drain  % Partition availability
}
```

**Access Control via Associations:**

Users must have an association with `partition` field matching the target partition, or an association with empty partition (allowing all partitions).

### Admin vs User Privileges

**Admin Levels:**

| Level | Capabilities |
|-------|--------------|
| `none` | Regular user - can only manage own jobs |
| `operator` | Can view all jobs, manage partitions |
| `admin` | Full cluster administration |

**Privilege Checks:**

The account manager validates admin level for privileged operations:

```erlang
%% Check if user has admin privileges
case get_user(Username) of
    {ok, #acct_user{admin_level = admin}} ->
        %% Allow privileged operation
        perform_admin_action();
    {ok, #acct_user{admin_level = operator}} ->
        %% Allow operator actions
        perform_operator_action();
    _ ->
        {error, permission_denied}
end
```

### QOS-Based Permissions

Quality of Service (QOS) provides fine-grained access control implemented in `flurm_qos.erl`.

**Default QOS Levels:**

| QOS | Priority | Description | Limits |
|-----|----------|-------------|--------|
| `normal` | 0 | Default for regular jobs | 24h max wall |
| `high` | +1000 | High priority, can preempt | 48h max wall, 50 jobs/user |
| `low` | -500 | Lower priority, preemptable | 7d max wall |
| `interactive` | +500 | Short interactive sessions | 1h max wall, 5 jobs/user |
| `standby` | -1000 | Uses idle resources only | No fair-share impact |

**QOS Features:**

```erlang
#qos{
    name :: binary(),
    priority :: integer(),           % Priority adjustment
    flags :: [atom()],               % QOS flags
    grace_time :: integer(),         % Grace period before preemption (seconds)
    max_jobs_pa :: integer(),        % Max jobs per account
    max_jobs_pu :: integer(),        % Max jobs per user
    max_submit_jobs_pu :: integer(), % Max submitted jobs per user
    max_tres_pa :: map(),            % Max TRES per account
    max_tres_pu :: map(),            % Max TRES per user
    max_tres_per_job :: map(),       % Max TRES per job
    max_wall_per_job :: integer(),   % Max wall time (seconds)
    preempt :: [binary()],           % QOS that can be preempted
    preempt_mode :: off | cancel | requeue,
    usage_factor :: float()          % Fair-share usage multiplier
}
```

**Preemption Rules:**

Jobs in `high` QOS can preempt jobs in `low` and `standby` QOS:

```erlang
%% From flurm_qos.erl
do_create(<<"high">>, #{
    preempt => [<<"low">>, <<"standby">>],
    preempt_mode => requeue,
    ...
})
```

---

## Network Security

### Port Requirements

FLURM uses the following network ports:

| Port | Protocol | Service | Direction |
|------|----------|---------|-----------|
| 6817 | TCP | Controller (SLURM clients) | Inbound |
| 6818 | TCP | Node daemon (slurmd compatible) | Inbound |
| 6819 | TCP | Database daemon (slurmdbd) / Node connections | Inbound |
| 4369 | TCP | EPMD (Erlang Port Mapper) | Inter-node |
| 9100-9200 | TCP | Erlang distribution ports | Inter-node |
| 9090 | TCP | Prometheus metrics HTTP | Inbound (optional) |

**Configuration:**

Ports are configured in `config/sys.config`:

```erlang
{flurm_controller, [
    {listen_port, 6817},           % SLURM client connections
    {node_listen_port, 6819},      % Node daemon connections
    {listen_address, "0.0.0.0"}    % Bind address
]}
```

### TLS/SSL Configuration

FLURM supports TLS encryption for all network communications.

#### Controller-to-Controller (Ra Consensus)

Ra consensus traffic between controller nodes uses Erlang distribution, which can be configured for TLS:

**Enable TLS for Erlang Distribution:**

1. Generate certificates:
```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
    -out ca.crt -subj "/CN=FLURM CA"

# Generate node certificate
openssl genrsa -out node.key 2048
openssl req -new -key node.key -out node.csr \
    -subj "/CN=flurm-controller"
openssl x509 -req -in node.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out node.crt -days 365
```

2. Configure Erlang distribution TLS in `vm.args`:
```
-proto_dist inet_tls
-ssl_dist_optfile /etc/flurm/ssl_dist.conf
```

3. Create `ssl_dist.conf`:
```erlang
[{server, [
    {certfile, "/etc/flurm/certs/node.crt"},
    {keyfile, "/etc/flurm/certs/node.key"},
    {cacertfile, "/etc/flurm/certs/ca.crt"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true},
    {secure_renegotiate, true}
]},
{client, [
    {certfile, "/etc/flurm/certs/node.crt"},
    {keyfile, "/etc/flurm/certs/node.key"},
    {cacertfile, "/etc/flurm/certs/ca.crt"},
    {verify, verify_peer},
    {secure_renegotiate, true}
]}].
```

#### Controller-to-Node Daemon

Node daemon connections use Ranch for TCP handling. TLS can be enabled:

```erlang
%% In flurm_controller_app.erl, modify Ranch configuration:
{ok, _} = ranch:start_listener(
    node_listener,
    ranch_ssl,  % Use SSL transport
    #{
        socket_opts => [
            {port, NodePort},
            {certfile, "/etc/flurm/certs/server.crt"},
            {keyfile, "/etc/flurm/certs/server.key"},
            {cacertfile, "/etc/flurm/certs/ca.crt"},
            {verify, verify_peer}
        ]
    },
    flurm_node_acceptor,
    #{}
)
```

#### Client-to-Controller

SLURM client connections can use TLS when configured:

```erlang
%% Configuration for TLS client connections
{flurm_controller, [
    {ssl_enabled, true},
    {ssl_certfile, "/etc/flurm/certs/server.crt"},
    {ssl_keyfile, "/etc/flurm/certs/server.key"},
    {ssl_cacertfile, "/etc/flurm/certs/ca.crt"}
]}
```

### Firewall Recommendations

**Minimal Firewall Rules (iptables example):**

```bash
# Allow SLURM clients to controller
iptables -A INPUT -p tcp --dport 6817 -j ACCEPT

# Allow node daemons to controller
iptables -A INPUT -p tcp --dport 6819 -j ACCEPT

# Allow Erlang distribution (restrict to cluster nodes)
iptables -A INPUT -p tcp --dport 4369 -s <cluster-subnet> -j ACCEPT
iptables -A INPUT -p tcp --dport 9100:9200 -s <cluster-subnet> -j ACCEPT

# Allow Prometheus scraping (if enabled)
iptables -A INPUT -p tcp --dport 9090 -s <monitoring-server> -j ACCEPT

# Drop all other incoming
iptables -A INPUT -j DROP
```

**nftables equivalent:**

```
table inet filter {
    chain input {
        type filter hook input priority 0;

        # Allow SLURM clients
        tcp dport 6817 accept

        # Allow node daemons
        tcp dport 6819 accept

        # Allow Erlang cluster (subnet restriction)
        ip saddr 10.0.0.0/24 tcp dport 4369 accept
        ip saddr 10.0.0.0/24 tcp dport 9100-9200 accept

        # Default drop
        drop
    }
}
```

**Network Segmentation Recommendations:**

1. Place controllers on management VLAN
2. Place compute nodes on separate compute VLAN
3. Use jump hosts for administrative access
4. Consider VPN for remote client access

---

## Data Protection

### State File Encryption

FLURM stores state data in multiple locations:

| Data | Location | Format |
|------|----------|--------|
| Ra consensus state | `/var/lib/flurm/ra/` | Binary (Ra snapshots) |
| Job database | `/var/lib/flurm/db/` | DETS files |
| Node state | `/var/lib/flurm/node_state.dat` | Erlang term format |

**Encryption at Rest:**

FLURM does not currently implement built-in encryption for state files. Use filesystem-level encryption:

```bash
# LUKS encryption for /var/lib/flurm
cryptsetup luksFormat /dev/sdb1
cryptsetup luksOpen /dev/sdb1 flurm_data
mkfs.ext4 /dev/mapper/flurm_data
mount /dev/mapper/flurm_data /var/lib/flurm
```

**File Permissions:**

```bash
# Set restrictive permissions on state directories
chown -R flurm:flurm /var/lib/flurm
chmod 700 /var/lib/flurm
chmod 600 /var/lib/flurm/ra/*
chmod 600 /var/lib/flurm/db/*
```

### Sensitive Data Handling

**Job Scripts:**

Job scripts may contain sensitive information. FLURM stores scripts in the job record:

```erlang
#job{
    script = JobScript,  % May contain credentials
    environment = Env    % May contain secrets
}
```

**Best Practices:**

1. Avoid embedding credentials in job scripts
2. Use environment variables with restricted visibility
3. Implement job script sanitization before logging
4. Consider purging completed job scripts after retention period

**Environment Variable Protection:**

FLURM passes environment variables to jobs. Sensitive variables should be:

- Marked with special prefix (e.g., `FLURM_SECRET_`)
- Excluded from logging
- Cleared after job completion

### Audit Logging

FLURM uses Lager for logging, configured in `config/sys.config`:

```erlang
{lager, [
    {handlers, [
        {lager_console_backend, [{level, info}]},
        {lager_file_backend, [
            {file, "/var/log/flurm/error.log"},
            {level, error}
        ]},
        {lager_file_backend, [
            {file, "/var/log/flurm/console.log"},
            {level, info}
        ]}
    ]},
    {crash_log, "/var/log/flurm/crash.log"}
]}
```

**Security-Relevant Events Logged:**

- Job submissions with user context
- Authentication failures
- Authorization denials
- Node registration/deregistration
- Cluster membership changes
- Administrative operations

**Log Security:**

```bash
# Secure log files
chown root:flurm /var/log/flurm
chmod 750 /var/log/flurm
chmod 640 /var/log/flurm/*.log

# Enable log rotation
cat > /etc/logrotate.d/flurm << EOF
/var/log/flurm/*.log {
    daily
    rotate 90
    compress
    delaycompress
    notifempty
    create 640 root flurm
    sharedscripts
    postrotate
        systemctl reload flurm-controller
    endscript
}
EOF
```

---

## Hardening Checklist

### Production Deployment Security Checklist

**Pre-Deployment:**

- [ ] Generate unique, strong Erlang cookie (32+ characters)
- [ ] Generate unique MUNGE key (256+ bits)
- [ ] Create dedicated service account (`flurm`)
- [ ] Set up TLS certificates for all communication channels
- [ ] Configure firewall rules before deployment
- [ ] Prepare encrypted filesystem for state data

**Controller Configuration:**

- [ ] Bind to specific interface, not `0.0.0.0` if possible
- [ ] Enable TLS for SLURM client connections
- [ ] Enable TLS for Erlang distribution
- [ ] Configure rate limiting
- [ ] Set appropriate log levels (info or above in production)
- [ ] Disable debug endpoints

**Node Configuration:**

- [ ] Configure node daemon to connect via TLS
- [ ] Set up state persistence encryption
- [ ] Configure resource limits (cgroups)
- [ ] Enable prolog/epilog for job isolation

**Accounts and Authorization:**

- [ ] Create account hierarchy
- [ ] Define QOS levels with appropriate limits
- [ ] Assign users to accounts with minimal necessary privileges
- [ ] Configure partition access restrictions
- [ ] Set TRES limits to prevent resource exhaustion

**Monitoring:**

- [ ] Enable audit logging
- [ ] Configure log shipping to SIEM
- [ ] Set up alerting for authentication failures
- [ ] Monitor for unusual job patterns

### File Permissions

| Path | Owner | Permissions | Purpose |
|------|-------|-------------|---------|
| `/etc/flurm/` | `root:flurm` | `750` | Configuration directory |
| `/etc/flurm/flurm.conf` | `root:flurm` | `640` | Main configuration |
| `/etc/flurm/certs/` | `root:flurm` | `750` | TLS certificates |
| `/etc/flurm/certs/*.key` | `root:flurm` | `600` | Private keys |
| `/var/lib/flurm/` | `flurm:flurm` | `700` | State data |
| `/var/lib/flurm/ra/` | `flurm:flurm` | `700` | Ra consensus data |
| `/var/log/flurm/` | `root:flurm` | `750` | Log directory |
| `~flurm/.erlang.cookie` | `flurm:flurm` | `400` | Erlang cookie |
| `/etc/munge/munge.key` | `munge:munge` | `400` | MUNGE key |

### Service Account Setup

```bash
# Create flurm user and group
groupadd -r flurm
useradd -r -g flurm -d /var/lib/flurm -s /sbin/nologin \
    -c "FLURM Workload Manager" flurm

# Create directories
mkdir -p /etc/flurm/certs
mkdir -p /var/lib/flurm/{ra,db}
mkdir -p /var/log/flurm
mkdir -p /var/run/flurm

# Set ownership
chown root:flurm /etc/flurm
chown -R flurm:flurm /var/lib/flurm
chown root:flurm /var/log/flurm
chown flurm:flurm /var/run/flurm

# Set permissions
chmod 750 /etc/flurm /etc/flurm/certs
chmod 700 /var/lib/flurm /var/lib/flurm/ra /var/lib/flurm/db
chmod 750 /var/log/flurm
chmod 755 /var/run/flurm

# Generate Erlang cookie
su - flurm -s /bin/bash -c 'openssl rand -base64 32 > ~/.erlang.cookie && chmod 400 ~/.erlang.cookie'
```

### SELinux/AppArmor Considerations

**SELinux Policy (RHEL/CentOS):**

Create a custom SELinux module for FLURM:

```
# flurm.te
module flurm 1.0;

require {
    type unconfined_t;
    type node_t;
    type unreserved_port_t;
    class tcp_socket { bind connect listen accept };
}

# Allow FLURM to bind to ports
allow unconfined_t unreserved_port_t:tcp_socket { bind listen accept };

# Allow Erlang distribution
allow unconfined_t node_t:tcp_socket { connect };
```

Compile and install:
```bash
checkmodule -M -m -o flurm.mod flurm.te
semodule_package -o flurm.pp -m flurm.mod
semodule -i flurm.pp
```

**AppArmor Profile (Ubuntu/Debian):**

Create `/etc/apparmor.d/flurm`:

```
#include <tunables/global>

/usr/lib/flurm/erts-*/bin/beam.smp {
    #include <abstractions/base>
    #include <abstractions/nameservice>

    # Configuration
    /etc/flurm/** r,

    # State data
    /var/lib/flurm/** rwk,

    # Logs
    /var/log/flurm/** w,

    # Runtime
    /var/run/flurm/** rwk,

    # Network
    network inet tcp,
    network inet udp,

    # Erlang runtime
    /usr/lib/flurm/** rx,
    /usr/lib/erlang/** rx,

    # MUNGE socket
    /var/run/munge/munge.socket.2 rw,
}
```

Enable the profile:
```bash
apparmor_parser -r /etc/apparmor.d/flurm
```

---

## Security Differences from SLURM

### Erlang Distribution Security Considerations

FLURM runs on the Erlang/OTP platform, which introduces unique security characteristics:

**Advantages:**

- Process isolation via lightweight Erlang processes
- Built-in supervision trees for fault tolerance
- Native support for distributed computing
- Hot code loading for security patches

**Considerations:**

- Erlang distribution allows code execution on connected nodes
- Cookie-based authentication is simpler than PKI
- EPMD broadcasts node information (consider epmdless mode)

**Mitigation - EPMD-less Operation:**

For high-security environments, run without EPMD:

```erlang
%% vm.args
-start_epmd false
-erl_epmd_port 4369
-kernel inet_dist_listen_min 9100
-kernel inet_dist_listen_max 9200
```

Configure static node discovery instead of EPMD.

### Ra Consensus Security

FLURM uses Ra (Raft) for distributed consensus, which has security implications:

**Consensus Security Features:**

- Leader election prevents split-brain scenarios
- Log replication ensures data consistency
- Automatic failover maintains availability

**Security Considerations:**

- Ra communication travels over Erlang distribution (enable TLS)
- Ra snapshots contain cluster state (protect on disk)
- Compromised node could disrupt consensus (use network isolation)

**Ra Configuration for Security:**

```erlang
{ra, [
    {data_dir, "/var/lib/flurm/ra"},
    %% Stricter timeouts prevent delayed attack vectors
    {wal_max_size_bytes, 64000000},
    {wal_max_batch_size, 32768}
]}
```

### FLURM-Specific Security Features

**1. Rate Limiting:**

FLURM includes built-in rate limiting (`flurm_rate_limiter.erl`) not present in SLURM:

```erlang
%% Configure rate limits
flurm_rate_limiter:set_limit(submit_job, 100, 60)  % 100 jobs per minute
flurm_rate_limiter:set_limit(api_request, 1000, 60) % 1000 API calls per minute
```

**2. Hierarchical Limit Enforcement:**

FLURM enforces limits in priority order:
1. QOS limits (highest priority)
2. Association limits (user+account+partition)
3. Account limits
4. User limits

This differs from SLURM's flatter limit checking.

**3. Graceful Degradation:**

FLURM can operate in degraded mode when consensus is unavailable:

```erlang
%% From flurm_db_persist.erl
is_ra_available() ->
    case node() of
        nonode@nohost -> false;  % Not distributed
        _ ->
            %% Check Ra cluster health
            case catch ra:members(ServerId) of
                {ok, _, _} -> true;
                _ -> false
            end
    end.
```

**4. Native Metrics Security:**

The Prometheus metrics endpoint (`flurm_metrics_http.erl`) exposes operational data. Secure it:

```erlang
{flurm_controller, [
    {metrics_enabled, true},
    {metrics_port, 9090},
    {metrics_bind, "127.0.0.1"}  % Bind to localhost only
]}
```

---

## Incident Response

### Security Incident Procedures

**1. Authentication Failure Investigation:**

```bash
# Check for failed authentication attempts
grep -i "auth" /var/log/flurm/console.log | grep -i "fail\|error\|deny"

# Check MUNGE logs
journalctl -u munge | grep -i "error\|fail"
```

**2. Unauthorized Job Detection:**

```bash
# List jobs by user
squeue -u suspicious_user

# Check job history
sacct -u suspicious_user --starttime=<date>
```

**3. Node Compromise Response:**

1. Drain the affected node immediately
2. Cancel running jobs on the node
3. Disconnect from cluster (remove from `cluster_nodes`)
4. Investigate and remediate
5. Rotate Erlang cookie if necessary
6. Rejoin after verification

**4. Key Rotation:**

MUNGE key rotation:
```bash
# On all nodes
systemctl stop munge
dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key
systemctl start munge
```

Erlang cookie rotation:
```bash
# On all nodes (coordinate to minimize downtime)
systemctl stop flurm-controller
openssl rand -base64 32 > /var/lib/flurm/.erlang.cookie
chmod 400 /var/lib/flurm/.erlang.cookie
chown flurm:flurm /var/lib/flurm/.erlang.cookie
systemctl start flurm-controller
```

---

## References

- [SLURM Security Documentation](https://slurm.schedmd.com/security.html)
- [MUNGE Authentication](https://dun.github.io/munge/)
- [Erlang Distribution Security](https://www.erlang.org/doc/reference_manual/distributed.html)
- [Ra Consensus Algorithm](https://github.com/rabbitmq/ra)
- [CIS Benchmarks](https://www.cisecurity.org/cis-benchmarks/)
