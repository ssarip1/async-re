#!/bin/bash
# the queue to be used.
#PBS -q workq 

# specify your project allocation
#PBS -A loni_jhabig09

# number of nodes and number of processors on each node to be used. 
# Set ppn to 8 for QB, and 4 for all other x86s.
#PBS -l nodes=1:ppn=8 

# requested Wall-clock time. HOURS::MINUTES::SECONDS
#PBS -l walltime=12:00:00 

# name of the standard out file to be "output-file".
#PBS -o myoutput2 

# standard error output merge to the standard output file.
#PBS -j oe 

# name of the job (that will appear on executing the qstat command).
#PBS -N namd2

cd /work/athota1/new_bigjob/bigjob #change to the working directory
export NPROCS=`wc -l $PBS_NODEFILE |gawk '//{print $1}'`

python 1bj_1m.py > new

#mpirun -machinefile $PBS_NODEFILE -np $NPROCS /work/athota1/new_bigjob/bigjob/namd2 NPT.conf
