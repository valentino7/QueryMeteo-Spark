#!/bin/bash
docker run -it --name mongo-client --network net mongo:xenial /bin/bash -c 'mongo mongo-server:27017'
