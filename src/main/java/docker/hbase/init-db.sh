docker exec -i  hbase hbase shell << EOF
    create 'Query1', 'core','SQL'
    create 'Query2', 'corePression','coreTemperature','coreHumidity','SQLtemperature','SQLpressure','SQLhumidity'
    create 'Query3', 'core','SQL'
EOF
 
