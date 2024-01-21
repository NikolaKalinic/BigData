from confluent_kafka import Consumer, KafkaError
import json
import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

TOPIC = 'raw'
CONSUMER_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python_consumer_group',
    'auto.offset.reset': 'earliest',
}

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setAppName("real-processing").setMaster("spark://spark-master-rt:7078")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

def consume_messages():
    c = Consumer(CONSUMER_CONFIG)
    c.subscribe([TOPIC])

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    # print(f"Error: {msg.error()}")
                    break

            # print(f"Received message: {msg.value().decode('utf-8')}")
            df = spark.read.json(spark.sparkContext.parallelize([msg.value().decode('utf-8')]))
            table_name = "rawRT"

            # Check if the table exists
            if table_name in spark.sql("SHOW TABLES").select("rawRT").rdd.flatMap(lambda x: x).collect():
                df.write.mode("append").saveAsTable(table_name)
            else:
                df.write.saveAsTable(table_name)

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == '__main__':
    consume_messages()
