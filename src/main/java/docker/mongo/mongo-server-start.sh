#!/bin/bash
docker run --rm -t -i -p 27017:27017 --network=net --name mongo_server mongo /usr/bin/mongod --smallfiles --bind_ip_all
