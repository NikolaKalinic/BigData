#!/bin/bash

function display_loading_animation() {
    local duration=$1  # Number of seconds for the loading animation
    local interval=$2   # Interval between characters

    for ((i=0; i<duration; i++)); do
        printf "#"
        sleep "$interval"
    done

    echo ""
}

echo ">>>> Waiting for docker-compose..." 
docker-compose up -d > logs/compose_output.log 2>&1
compose_exit_code=$?
status="SUCCESSFULLY"
if [ $compose_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Docker-compose complited $status !"
display_loading_animation 40 0.4
echo ">>>> Waiting to transfering file to hdfs..."
docker exec namenode hdfs dfs -copyFromLocal NoviSad.csv / > logs/transfer_output.log 2>&1
docker exec namenode hdfs dfs -copyFromLocal Belgrade.csv / >> logs/transfer_output.log 2>&1
docker exec namenode hdfs dfs -copyFromLocal Nis.csv / >> logs/transfer_output.log 2>&1
docker exec namenode hdfs dfs -copyFromLocal Subotica.csv / >> logs/transfer_output.log 2>&1
docker exec namenode hdfs dfs -copyFromLocal Kragujevac.csv / >> logs/transfer_output.log 2>&1
transfer_exit_code=$?
status="SUCCESSFULLY"
if [ $transfer_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Transfering file to hdfs complited $status !"
display_loading_animation 40 0.2
echo ">>>> Waiting preprocesing with spark..."
docker exec spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./preprocessing.py > logs/spark_output.log 2>&1
spark_exit_code=$?
status="SUCCESSFULLY"
if [ $spark_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Preprocesing complited $status !"
display_loading_animation 40 0.03
echo ">>>> Waiting winter with spark..."
docker exec spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./winter.py > logs/spark_output.log 2>&1
winter_exit_code=$?
status="SUCCESSFULLY"
if [ $winter_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Winter complited $status !"
display_loading_animation 40 0.03
echo ">>>> Waiting wind with spark..."
docker exec spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./wind.py > logs/spark_output.log 2>&1
wind_exit_code=$?
status="SUCCESSFULLY"
if [ $wind_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Wind complited $status !"
display_loading_animation 40 0.03
echo ">>>> Waiting season with spark..."
docker exec spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./season.py > logs/spark_output.log 2>&1
season_exit_code=$?
status="SUCCESSFULLY"
if [ $season_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Season complited $status !"
display_loading_animation 40 0.03



#hive://hive@hive-server:10000/default


