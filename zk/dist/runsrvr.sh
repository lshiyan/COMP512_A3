#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]; then
    ZOOBINDIR=/Users/evanjiang/Desktop/comp_512/p3/apache-zookeeper-3.8.5-bin/bin
fi

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=tr-open-09.cs.mcgill.ca:21826,tr-open-10.cs.mcgill.ca:21826,tr-open-11.cs.mcgill.ca:21826
# export ZKSERVER=open-gpu-26.cs.mcgill.ca:21826,open-gpu-26.cs.mcgill.ca:21826,open-gpu-26.cs.mcgill.ca:21826
#export ZKSERVER=localhost:21811,localhost:21812,localhost:21813

java -cp $CLASSPATH:../task:.: DistProcess 
