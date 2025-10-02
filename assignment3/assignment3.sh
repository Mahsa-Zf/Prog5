#!/bin/bash
#SBATCH --job-name=sieve_of_eratosthenes
#SBATCH --partition=workstations
#SBATCH --ntasks-per-node=1
#SBATCH --nodes=8
#SBATCH --time=00:02:00


runtime=$({ /usr/bin/time -f "%e" srun python3 assignment3.py 50 1>/dev/null; } 2>&1)
echo "Timing completed: nnodes $SLURM_NNODES $runtime seconds" >> timing_network_summary_workstations.txt

