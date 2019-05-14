#!/usr/bin/env bash

docker run --network=net --name nifi \                                                       
  -p 8443:8443 \
  -d -e NIFI_WEB_HTTP_PORT='8443' \
  apache/nifi:latest

