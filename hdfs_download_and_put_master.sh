#!/bin/bash

CRIME_DATA="crimes_data.json"
CITY_ATTRIBUTES_DATA="city_attributes.csv"
PRESSURE_DATA="pressure.csv"
TEMPERATURE_DATA="temperature.csv"
HUMIDITY_DATA="humidity.csv"
WEATHER_DESCRIPTION_DATA="weather_description.csv"
WIND_DIRECTION_DATA="wind_direction.csv"
WIND_SPEED_DATA="wind_speed.csv"
COLLISION_DATA="collisions_data.xml"

mkdir -p /tmp/reports
rm /tmp/reports/ls_report.txt
touch /tmp/reports/ls_report.txt

hdfs dfs -mkdir -p /all_data/

for DATA in CRIME_DATA CITY_ATTRIBUTES_DATA PRESSURE_DATA TEMPERATURE_DATA HUMIDITY_DATA WEATHER_DESCRIPTION_DATA WIND_DIRECTION_DATA WIND_SPEED_DATA COLLISION_DATA
do
        hdfs dfs -rm -R /all_data/${!DATA}
        rm /tmp/reports/fsck_report_${!DATA%%.*}.txt
        rm /tmp/reports/dfsadmin_report_${!DATA%%.*}.txt
        touch /tmp/reports/fsck_report_${!DATA%%.*}.txt
        touch /tmp/reports/dfsadmin_report_${!DATA%%.*}.txt
        hdfs dfs -put /data/hadoop/data/${!DATA} /all_data
        hdfs dfs -setrep -w 3 /all_data/${!DATA}
        hdfs fsck /all_data/${!DATA} >> /tmp/reports/fsck_report_${!DATA%%.*}.txt
        hdfs dfsadmin -report >> /tmp/reports/dfsadmin_report_${!DATA%%.*}.txt
        echo "Finished file ${!DATA}"
done

hdfs dfs -ls /all_data >> /tmp/reports/ls_report.txt
echo "Finished all files"