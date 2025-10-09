#!/bin/bash
#SBATCH --job-name=sql_archaea
#SBATCH --partition=assemblix
#SBATCH --ntasks=8
#SBATCH --time=00:45:00

# This SLURM bash script which does nothing but run the script. 
# You may hardcode the "gbff" file as input to the python script.


addrs="/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"
/usr/bin/time srun python3 assignment4.py $addrs