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
echo ">>>> Waiting to starting spark stream..."
sleep 5
docker exec spark-master-rt ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./consumer.py > logs/spark_output.log 2>&1 &
stream_exit_code=$?
status="SUCCESSFULLY"
if [ $stream_exit_code -ne 0 ]; then
	status="UNSUCCESSFULLY"
fi
echo ">>>> Starting spark stream complited $status !"
echo ">>>> ###################################"
# docker exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic raw

# docker exec -i producer bash -c "echo '* * * * * /usr/bin/python3 /produce_event.py' | crontab - && service cron start"

# docker exec -t consumer bash -c "python3 consumer.py"

#docker exec spark-master-rt ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./consumer.py
