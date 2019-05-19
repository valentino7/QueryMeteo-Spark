#!/usr/bin/env bash

HOSTNAME=nifi
HOST=localhost
PORT=9999
HDFS_DEFAULT_FS=hdfs://master:54310
JAR=1.1.32/nifi-deploy-config-1.1.32.jar

docker run -t -i -p $PORT:$PORT -e NIFI_WEB_HTTP_PORT="$PORT" -e HDFS_DEFAULTS_FS="$HDFS_DEFAULT_FS" --network=net -d --hostname=$HOSTNAME --name=nifi apache/nifi:latest

docker cp ./config/core-site.xml nifi:/opt/nifi/core-site.xml
docker cp ./config/hdfs-site.xml nifi:/opt/nifi/hdfs-site.xml
docker cp ./hbase/core-site.xml nifi:/opt/nifi/core-site.xml
docker cp ./hbase/hbase-site.xml nifi:/opt/nifi/hbase-site.xml
docker cp ./data nifi:/opt/nifi/
docker cp ./TemplateFinal.xml nifi:/TemplateFinal.xml
docker cp ./nifi-deploy-config-1.1.32.jar nifi:/nifi-deploy-config-1.1.32.jar




#printf 'waiting for NiFi...\n'
NIFI_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' nifi)
until $(curl --output /dev/null --silent --head --fail http://$NIFI_HOST:$PORT); do
    printf '.'
    sleep 5
done

docker exec -it nifi java -jar /nifi-deploy-config-1.1.32.jar -nifi http://172.18.0.6:9999/nifi-api -conf /TemplateFinal.xml -m deployTemplate


