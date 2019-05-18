docker exec -i  gallant_mendel hbase shell << EOF
  create 'results', 'query1'
  list
  put 'results', 'row1', 'query1:a', 'value1'
  scan 'results'
EOF
 
