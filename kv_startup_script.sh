#!/bin/bash

# [START startup_script]
apt-get update

sleep 5

source map_red_env/bin/activate
cd /home/vikond/project/DistributedMapReduce/
sudo python3 KVServer.py

# [END startup_script]