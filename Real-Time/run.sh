#!/bin/bash

docker exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic raw

docker exec -i producer bash -c "echo '* * * * * /usr/bin/python3 /produce_event.py' | crontab - && service cron start"

# docker exec -t consumer bash -c "python3 consumer.py"

#docker exec spark-master-rt ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./consumer.py
