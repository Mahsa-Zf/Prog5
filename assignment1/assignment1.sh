#!/bin/bash

#SBATCH --job-name=trapezoid_integration
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
#SBATCH --partition=workstations
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00   # 10 minutes max
hostname
date
echo "Starting integration jobs..."

# Write header to results.csv
echo "n,error" > results.csv

# Set integration limits
a=0.0
b=3.141592653589793  # pi

# Run multiple n values in a loop
for n in 10 100 500 1000 2000
do
    # Run python program and append output to results.csv
    python3 assignment1.py $a $b $n >> results.csv
done

echo "Integration jobs completed."
