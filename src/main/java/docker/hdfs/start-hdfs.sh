 docker exec master /bin/bash -c "hdfs namenode -format;/usr/local/hadoop/sbin/start-dfs.sh;hdfs dfs -chmod -R 777 /"
