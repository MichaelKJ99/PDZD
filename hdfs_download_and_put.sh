#!/bin/bash

CRIME_DATA = 'crime_data.json'
WEATHER_DATA = 'weather_data.csv'
COLISSION_DATA = 'colission_data.xml'

sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.8


python path/to/the/.py

docker exec -it master bash 
hdfs dfs -mkdir -p

for DATA in CRIME_DATA WEATHER_DATA COLISSION_DATA
do
	hdfs dfs -mkdir -p /${DATA%%.*}/
	hdfs dfs -put /tmp/${DATA} /${DATA%%.*}/
	hdfs fsck /${DATA%%.*}/${DATA} >> /tmp/reports/fsck_report.txt
	hdfs dfsadmin -report >> /tmp/reports/dfsadmin_report.txt
	echo "Finished file $DATA"
done

hdfs dfs -ls >> /ls/reports/ls_report.txt
echo "Finished all files"
