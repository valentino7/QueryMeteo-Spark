#!/bin/bash
#mongo = immagine docker
#docker pull mongo




docker cp ./script-mongo.js mongo_server:/script-mongo.js
docker exec -it mongo_server bash -c 'mongo < script-mongo.js'



#docker run -it --name mongo-client --network net mongo /bin/bash -c 'mongo mongo_server:27017'



