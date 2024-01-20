#!/bin/bash

echo ">>>> Waiting for docker-compose..." 
docker-compose up -d > logs/compose_output.log 2>&1
compose_exit_code=$?
status="SUCCESSFULLY"
if [ $compose_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Docker-compose complited $status !"
echo ">>>> ###################################"
echo ">>>> Waiting to transfering file to hdfs..."
sleep 10 
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
echo ">>>> ###################################"
echo ">>>> Waiting preprocesing with spark..."
sleep 5
docker exec spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./preprocessing.py > logs/spark_output.log 2>&1
spark_exit_code=$?
status="SUCCESSFULLY"
if [ $spark_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Preprocesing complited $status !"
echo ">>>> ###################################"

#hive://hive@hive-server:10000/default


