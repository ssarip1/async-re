#!/bin/bash

. ~/.bashrc

#echo "load modules"
#module unload mvapich2
#module unload mvapich
#module swap pgi intel
#module load mvapich
#module load python/2.5.2
#env

#soft add +mpichvmi-intel
#soft add +mpichvmi-intel-ofed1.2
#echo "run advert launcher"
#echo "MPI Path" `which mpirun`
#env
#export SAGA_VERBOSE=100
#grid-proxy-info
scp $PBS_O_HOST:/tmp/x509up_u`id -u` /tmp/
python `dirname $0`/advert_launcher.py $*
