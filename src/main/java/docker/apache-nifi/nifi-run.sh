#!/usr/bin/env bash

HOSTNAME=nifi
HOST=localhost
PORT=9999
HDFS_DEFAULT_FS=hdfs://master:54310
JAR=1.1.32/nifi-deploy-config-1.1.32.jar

docker run -t -i -p $PORT:$PORT -e NIFI_WEB_HTTP_PORT="$PORT" -e HDFS_DEFAULTS_FS="$HDFS_DEFAULT_FS" --network=net -d --hostname=$HOSTNAME --name=nifi apache/nifi:latest

docker cp ./apache-nifi/config/core-site.xml nifi:/opt/nifi/core-site.xml
docker cp ./apache-nifi/config/hdfs-site.xml nifi:/opt/nifi/hdfs-site.xml
docker cp ./apache-nifi/hbase nifi:/opt/nifi/hbase/
docker cp ./apache-nifi/data nifi:/opt/nifi/
docker cp ./apache-nifi/TemplateV4.xml nifi:/TemplateV4.xml
docker cp ./apache-nifi/templateCopyToHDFS.xml nifi:/templateCopyToHDFS.xml
docker cp ./apache-nifi/nifi-deploy-config-1.1.32.jar nifi:/nifi-deploy-config-1.1.32.jar




#printf 'waiting for NiFi...\n'
NIFI_HOST=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' nifi)
echo "$NIFI_HOST"
until $(curl --output /dev/null --silent --head --fail http://$NIFI_HOST:$PORT); do
    printf '.'
    sleep 5
done

docker exec -it nifi java -jar /nifi-deploy-config-1.1.32.jar -nifi http://$NIFI_HOST:$PORT/nifi-api -conf /templateCopyToHDFS.xml -m deployTemplate
docker exec -it nifi java -jar /nifi-deploy-config-1.1.32.jar -nifi http://$NIFI_HOST:$PORT/nifi-api -conf /TemplateV4.xml -m deployTemplate


