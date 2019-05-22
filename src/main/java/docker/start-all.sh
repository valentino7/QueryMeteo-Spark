#!/usr/bin/env bash

echo "Insert --deploy parameter if you want to submit topology in spark cluster"

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
if [ "$1" == "--deploy" ]; then
    ./spark/spark-run.sh
fi

python activate_processor_nifi.py 2 $NIFI_HOST

