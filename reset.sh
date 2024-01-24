#!/bin/bash
sudo docker stop kvs-replica1
sudo docker stop kvs-replica2
sudo docker stop kvs-replica3
sudo docker rm kvs-replica1
sudo docker rm kvs-replica2
sudo docker rm kvs-replica3
sudo docker build -t src/index.py .