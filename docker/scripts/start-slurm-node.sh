#!/bin/bash
# Start SLURM Compute Node (slurmd)
set -e

echo "=== Starting SLURM Compute Node ==="
echo "Controller: ${SLURM_CONTROLLER:-slurm-controller}"
echo "Hostname: $(hostname)"

# Wait for MUNGE key to be available (shared volume from controller)
echo "Waiting for MUNGE key..."
for i in $(seq 1 30); do
    if [ -f /etc/munge/munge.key ]; then
        echo "MUNGE key found"
        break
    fi
    echo "Waiting for MUNGE key... ($i/30)"
    sleep 2
done

if [ ! -f /etc/munge/munge.key ]; then
    echo "ERROR: MUNGE key not found after 60 seconds"
    exit 1
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
    echo "WARNING: MUNGE test failed"
fi

# Create spool directories
mkdir -p /var/spool/slurm/d /var/log/slurm /var/run/slurm
chown -R slurm:slurm /var/spool/slurm /var/log/slurm /var/run/slurm

# Wait for controller to be ready
echo "Waiting for slurmctld to be ready..."
for i in $(seq 1 30); do
    if scontrol ping 2>/dev/null | grep -q "UP"; then
        echo "Controller is ready"
        break
    fi
    echo "Waiting for controller... ($i/30)"
    sleep 2
done

# Start slurmd
echo "Starting slurmd..."
exec /usr/sbin/slurmd -D -vvv
