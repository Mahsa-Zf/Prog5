#!/bin/bash
#SBATCH --job-name=spark-job
#SBATCH --partition=assemblix
#SBATCH --ntasks=1          # Changed from 16 to 1
#SBATCH --cpus-per-task=1  #16 if local, Gives 16 CPUs to the single task should equal local[*] in spark
#SBATCH --mem=16G  # 16G (moderate memory) if on cluster, else, 256G it's the total memory of executer and drive (if using local)
#SBATCH --time=00:10:00
#SBATCH --output=spark-job-%j.out
#SBATCH --error=spark-job-%j.err

# This SLURM bash script which does nothing but run the script. 

# addrs="/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff"
addrs="/homes/mzamanifard/Documents/prog5/archaea.2.genomic.gbff"
/usr/bin/time srun python3 assignment5.py $addrs