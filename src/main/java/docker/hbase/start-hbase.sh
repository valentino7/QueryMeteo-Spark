#!/usr/bin/env bash
#contattarlo all'indirizzo http://ip:16301/master-status#userTables
#docker run --network=net valentino94/hbase

docker run -ti --name=hbase -h hbase --network=net -d -p 2181:2181  -p 8085:8085 \
    -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 harisekhon/hbase:1.4

 
 
HOST_HBASE=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' hbase)
PORT_HBASE=16010
echo "$HOST_HBASE"
until $(curl --output /dev/null --silent --head --fail http://$HOST_HBASE:$PORT_HBASE); do
    printf '.'
    sleep 5s # Or 10s or 1m or whatever time
done
docker cp ./hbase/hbase-site.xml hbase:/hbase/conf/hbase-site.xml
