#!/bin/bash
# Switch traffic from SLURM to FLURM
# This simulates the traffic switch by updating client configuration

SLURM_HOST=${SLURM_CONTROLLER:-slurm-controller}
FLURM_HOST=${FLURM_CONTROLLER:-flurm-controller}

echo "=== Switching Traffic from SLURM to FLURM ==="

echo ""
echo "Before switch:"
echo "  SLURM controller: $SLURM_HOST:6817"
echo "  FLURM controller: $FLURM_HOST:6820"

# In a real migration, you would:
# 1. Update DNS or load balancer to point to FLURM
# 2. Update client slurm.conf to point to FLURM
# 3. Reconfigure any scripts that reference SLURM directly

# For this test, we update the SLURM client config to point to FLURM
echo ""
echo "Updating client configuration..."

# Backup original config
cp /etc/slurm/slurm.conf /etc/slurm/slurm.conf.slurm

# Create new config pointing to FLURM
cat > /etc/slurm/slurm.conf.flurm << EOF
# SLURM Client Configuration - Pointing to FLURM
ClusterName=migration-test
SlurmctldHost=$FLURM_HOST
SlurmctldPort=6820

AuthType=auth/munge
MpiDefault=none
ProctrackType=proctrack/linuxproc
ReturnToService=2
SlurmctldPidFile=/var/run/slurm/slurmctld.pid
SlurmdPidFile=/var/run/slurm/slurmd.pid
SlurmdSpoolDir=/var/spool/slurm/d
SlurmUser=root
StateSaveLocation=/var/spool/slurm/ctld
SwitchType=switch/none
TaskPlugin=task/none
SchedulerType=sched/backfill
SelectType=select/cons_tres
SelectTypeParameters=CR_Core
SlurmdDebug=info
SlurmctldDebug=info
NodeName=compute-node-1 CPUs=4 RealMemory=4000 State=UNKNOWN
PartitionName=debug Nodes=compute-node-1 Default=YES MaxTime=00:30:00 State=UP
PartitionName=batch Nodes=compute-node-1 MaxTime=24:00:00 State=UP
PartitionName=gpu Nodes=compute-node-1 MaxTime=48:00:00 State=UP
EOF

# Switch to FLURM config
cp /etc/slurm/slurm.conf.flurm /etc/slurm/slurm.conf

echo ""
echo "After switch:"
echo "  Clients now configured to use FLURM at $FLURM_HOST:6820"

echo ""
echo "Traffic switch complete."
echo "Run 'squeue' and 'sinfo' to verify FLURM is responding."
echo ""
echo "To revert: cp /etc/slurm/slurm.conf.slurm /etc/slurm/slurm.conf"
