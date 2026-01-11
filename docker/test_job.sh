#!/bin/bash
#SBATCH --job-name=flurm_test
#SBATCH --output=test_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:05:00

echo "Hello from FLURM test job!"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
sleep 5
echo "Job complete!"
