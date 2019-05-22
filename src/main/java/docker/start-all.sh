#!/usr/bin/env bash

#start cluster hdfs
./hdfs/start-hdfs.sh

#NIFI
./apache-nifi/nifi-run.sh
NIFI_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' nifi)
python activate_processor_nifi.py 1 $NIFI_HOST

#HBASE
./hbase/start-hbase.sh
./hbase/init-db.sh

#MONGO
./mongo/mongo-server-start.sh


#SPARK
./spark/spark-run.sh $1




#sleep 10
#python activate_processor_nifi.py 2 $NIFI_HOST

