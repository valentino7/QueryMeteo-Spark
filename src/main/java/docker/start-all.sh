#!/usr/bin/env bash
#start cluster hdfs
./hdfs/start-hdfs.sh

#NIFI
./apache-nifi/nifi-run.sh

#HBASE
./hbase/start-hbase.sh

HOST_HBASE=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' hbase)
PORT=16301
echo "$HOST_HBASE"
until $(curl --output /dev/null --silent --head --fail http://$HOST_HBASE:$PORT); do
    printf '.'
    sleep 5s # Or 10s or 1m or whatever time
done
docker cp ./hbase/hbase-site.xml hbase:/hbase/conf/hbase-site.xml
./hbase/init-db.sh

#MONGO
./mongo/mongo-server-start.sh

#SPARK
HOST_HDFS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' master)
cd ./spark
docker-compose up -d
docker exec spark_master /bin/bash -c '$SPARK_HOME/bin/spark-submit --class MainQuery2 --master "local" target/handson-spark-1.0.jar'

