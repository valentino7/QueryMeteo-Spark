#!/usr/bin/env bash
 cd spark/

HOST_HDFS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' master)
HDFS_PORT=54310
docker-compose up -d

SPARK_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark_master)
SPARK_PORT=8080
until $(curl --output /dev/null --silent --head --fail http://$SPARK_HOST:$SPARK_PORT); do
    printf '.'
    sleep 5s # Or 10s or 1m or whatever time
done

cd ..

docker cp ../../../../target/spark-1.0-jar-with-dependencies.jar spark_master:/usr/spark-2.4.2/spark-1.0.jar
docker cp ../../../../target/spark-1.0-jar-with-dependencies.jar spark_worker:/usr/spark-2.4.2/spark-1.0.jar
docker cp ../../../../target/spark-1.0-jar-with-dependencies.jar spark_worker_1:/usr/spark-2.4.2/spark-1.0.jar


docker exec spark_master /bin/bash -c "bin/spark-submit --class MainSpark spark-1.0.jar $HOST_HDFS:$HDFS_PORT cluster"


python wait_spark.py $SPARK_HOST:$SPARK_PORT

