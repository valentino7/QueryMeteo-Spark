#!/bin/bash
docker kill spark_master spark_worker spark_worker_1
docker rm spark_master spark_worker spark_worker_1
