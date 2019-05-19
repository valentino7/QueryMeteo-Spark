#!/bin/bash
docker network create --driver bridge net
docker run -t -i -p 9864:9864 -d --network=net --name=slave1 valentino94/hadoop
docker run -t -i -p 9863:9864 -d --network=net --name=slave2 valentino94/hadoop
docker run -t -i -p 9862:9864 -d --network=net --name=slave3 valentino94/hadoop
docker run -t -i -p 9870:54310 -d --network=net --name=master valentino94/hadoop

docker exec master /bin/bash -c "hdfs namenode -format;/usr/local/hadoop/sbin/start-dfs.sh;hdfs dfs -chmod -R 777 /"
