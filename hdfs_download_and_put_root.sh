#!/bin/bash



echo -ne '\n' | sudo apt-get install software-properties-common
echo -ne '\n' | sudo add-apt-repository ppa:deadsnakes/ppa
echo -ne '\n' | sudo apt-get update
echo -ne '\n' | sudo apt-get install python3.8
echo -ne '\n' | python3 -m pip install -r requirements.txt


python3 ./data_acquisition.py