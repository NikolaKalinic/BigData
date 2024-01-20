import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg
from pyspark.sql import functions as F


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

df = spark.table("data")

df = df.withColumn("winter", F.when((F.month(F.col("time")) >= 12) | (F.month(F.col("time")) <= 2), 1).otherwise(0))
df = df.withColumn("year", F.year(F.col("time")))

# Grupisanje po gradu, zimi i godini, računanje prosečnih temperatura i prividnih prosečnih temperatura
result_df = df.groupBy("city", "year", "winter").agg(
    F.avg("temperature_2m_C").alias("avg_temperature"),
    F.avg("apparent_temperature_C").alias("avg_apparent_temperature")
)

result_df.write.mode("append").saveAsTable("winter")

spark.stop()