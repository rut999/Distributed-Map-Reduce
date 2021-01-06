#!/bin/bash


# [START startup_script]
apt-get update

sleep 10

source map_red_env/bin/activate
cd /home/vikond/project/DistributedMapReduce/
sudo python3 master.py

# [END startup_script]