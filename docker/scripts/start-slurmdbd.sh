#!/bin/bash
# Start SLURM database daemon (slurmdbd) for migration e2e tests.
set -euo pipefail

echo "=== Starting SLURM DBD ==="
echo "MySQL host: ${MYSQL_HOST:-mysql}"
echo "Database:   ${MYSQL_DATABASE:-slurm_acct_db}"

# Ensure runtime directories exist.
mkdir -p /var/run/munge /var/log/munge /var/log/slurm /var/spool/slurmdbd

# Create MUNGE key when volume is empty.
if [ ! -f /etc/munge/munge.key ]; then
    echo "Creating MUNGE key..."
    dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key 2>/dev/null
fi

# Fix ownership/permissions required by munged/slurmdbd.
chown -R munge:munge /etc/munge /var/run/munge /var/log/munge
chmod 700 /etc/munge
chmod 400 /etc/munge/munge.key
chown -R slurm:slurm /var/log/slurm /var/spool/slurmdbd

echo "Starting MUNGE daemon..."
runuser -u munge -- /usr/sbin/munged
sleep 2

if ! munge -n </dev/null >/dev/null 2>&1; then
    echo "ERROR: MUNGE authentication check failed"
    exit 1
fi

# Generate slurmdbd.conf from environment for test environments.
cat >/etc/slurm/slurmdbd.conf <<EOF
AuthType=auth/munge
AuthInfo=/var/run/munge/munge.socket.2
DbdHost=slurm-dbd
DbdAddr=0.0.0.0
DbdPort=6819
SlurmUser=slurm
DebugLevel=info
LogFile=/var/log/slurm/slurmdbd.log
PidFile=/var/run/slurmdbd.pid
StorageType=accounting_storage/mysql
StorageHost=${MYSQL_HOST:-mysql}
StoragePort=3306
StorageUser=${MYSQL_USER:-slurm}
StoragePass=${MYSQL_PASSWORD:-slurm_password}
StorageLoc=${MYSQL_DATABASE:-slurm_acct_db}
EOF

chmod 600 /etc/slurm/slurmdbd.conf
chown slurm:slurm /etc/slurm/slurmdbd.conf

# Wait for MySQL to accept connections before starting slurmdbd.
echo "Waiting for MySQL at ${MYSQL_HOST:-mysql}:3306..."
for i in $(seq 1 60); do
    if nc -z "${MYSQL_HOST:-mysql}" 3306 >/dev/null 2>&1; then
        echo "MySQL is reachable"
        break
    fi
    sleep 2
done

if ! nc -z "${MYSQL_HOST:-mysql}" 3306 >/dev/null 2>&1; then
    echo "ERROR: MySQL is not reachable"
    exit 1
fi

echo "Starting slurmdbd..."
exec /usr/sbin/slurmdbd -D -vvv
