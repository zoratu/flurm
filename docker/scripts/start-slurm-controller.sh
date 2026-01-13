#!/bin/bash
# Start SLURM Controller (slurmctld)
set -e

echo "=== Starting SLURM Controller ==="
echo "Cluster: ${SLURM_CLUSTER_NAME:-migration-test}"

# Create and set permissions on MUNGE key if it doesn't exist
if [ ! -f /etc/munge/munge.key ]; then
    echo "Creating MUNGE key..."
    dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key 2>/dev/null
fi

# Fix MUNGE permissions
chown -R munge:munge /etc/munge /var/run/munge /var/log/munge
chmod 700 /etc/munge
chmod 400 /etc/munge/munge.key

# Start MUNGE daemon
echo "Starting MUNGE daemon..."
runuser -u munge -- /usr/sbin/munged
sleep 2

# Verify MUNGE is working
if munge -n </dev/null >/dev/null 2>&1; then
    echo "MUNGE authentication is working"
else
    echo "WARNING: MUNGE test failed, authentication may not work"
fi

# Create state directories
mkdir -p /var/spool/slurm/ctld /var/log/slurm /var/run/slurm
chown -R slurm:slurm /var/spool/slurm /var/log/slurm /var/run/slurm

# Start slurmctld
echo "Starting slurmctld..."
exec /usr/sbin/slurmctld -D -vvv
