#!/bin/bash

#SBATCH --job-name=trapezoid_integration_partitioned
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --partition=workstations
#SBATCH --nodes=1
#SBATCH --ntasks=33
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00


hostname
date

echo "Starting integration jobs..."

# Set integration limits
a=0.0
b=3.141592653589793  # pi

# Run multiple n values in a loop
for n in 10 100 500 1000 2000
do
    # Run python program and append output to results.csv
    srun python3 assignment2.py $a $b $n >> results.csv

    echo "Completed integration with n=$n"
done


echo "Integration jobs completed."
