#start cluster hdfs
./hdfs/start-hdfs.sh

#./apache-nifi/nifi-run.sh

./hbase/start-hbase.sh
./hbase/init-db.sh


./mongo/mongo-server-start.sh

HOST= docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' master

#running spark and avvio jar
