docker exec -i  gallant_mendel hbase shell << EOF
  create 'test', 'cf'
  list
  put 'test', 'row1', 'cf:a', 'value1'
  scan 'test'
EOF
 
