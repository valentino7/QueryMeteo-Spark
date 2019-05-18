#!/bin/bash

docker volume create portainer_data
docker run --name portainer --env ADMIN_USERNAME=admin --env ADMIN_PASS=admin111 -d -p 9000:9000 -v /var/run/docker.sock:/var/run/docker.sock portainer/portainer
