#!/usr/bin/env bash
./apache-nifi/nifi-stop.sh
./hbase/hbase-stop.sh
./mongo/mongo-server-stop.sh
./spark/spark-stop.sh
./hdfs/stop-dockers.sh