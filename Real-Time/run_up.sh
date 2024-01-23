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
display_loading_animation 40 0.2
echo ">>>> Waiting to starting spark stream..."
docker exec spark-master-rt ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./consumer.py > logs/spark_output.log 2>&1 &
stream_exit_code=$?
status="SUCCESSFULLY"
if [ $stream_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Starting spark stream complited $status !"
