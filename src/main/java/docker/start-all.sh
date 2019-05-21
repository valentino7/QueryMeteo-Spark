#!/usr/bin/env bash
#start cluster hdfs
./hdfs/start-hdfs.sh

#NIFI
./apache-nifi/nifi-run.sh
NIFI_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' nifi)
python activate_processor_nifi.py 1 $NIFI_HOST

#HBASE
./hbase/start-hbase.sh

HOST_HBASE=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' hbase)
PORT_HBASE=16301
echo "$HOST_HBASE"
until $(curl --output /dev/null --silent --head --fail http://$HOST_HBASE:$PORT_HBASE); do
    printf '.'
    sleep 5s # Or 10s or 1m or whatever time
done
docker cp ./hbase/hbase-site.xml hbase:/hbase/conf/hbase-site.xml
./hbase/init-db.sh

#MONGO
./mongo/mongo-server-start.sh

#SPARK
HOST_HDFS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' master)
HDFS_PORT=54310
cd ./spark
docker-compose up -d

SPARK_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark_master)
SPARK_PORT=8080
until $(curl --output /dev/null --silent --head --fail http://$SPARK_HOST:$SPARK_PORT); do
    printf '.'
    sleep 5s # Or 10s or 1m or whatever time
done

cd ..

docker cp ./spark-1.0.jar spark_master:/usr/spark-2.4.2
docker cp ./spark-1.0.jar spark_worker:/usr/spark-2.4.2


SPARK_HOME/bin/spark-submit --class MainSpark 172.18.0.5:54310 spark-1.0.jar
docker exec spark_master /bin/bash -c '$SPARK_HOME/bin/spark-submit --class MainSpark spark-1.0.jar'+$HOST_HDFS:$HDFS_PORT

#sleep 10
#python activate_processor_nifi.py 2 $NIFI_HOST


