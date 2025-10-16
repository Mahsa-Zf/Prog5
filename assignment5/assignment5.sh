#!/bin/bash
#SBATCH --job-name=spark-job
#SBATCH --partition=assemblix
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=00:10:00
#SBATCH --output=spark-job-%j.out
#SBATCH --error=spark-job-%j.err

# This SLURM bash script which does nothing but run the script. 
# You may hardcode the "gbff" file as input to the python script.

addrs="/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff"
/usr/bin/time srun python3 assignment5.py $addrs