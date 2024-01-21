import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import  FloatType


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


@udf(StringType())
def degrees_to_cardinal(direction_degrees):
    if 337.5 <= direction_degrees <= 360 or 0 <= direction_degrees < 22.5:
        return "N"
    elif 22.5 <= direction_degrees < 67.5:
        return "NE"
    elif 67.5 <= direction_degrees < 112.5:
        return "E"
    elif 112.5 <= direction_degrees < 157.5:
        return "SE"
    elif 157.5 <= direction_degrees < 202.5:
        return "S"
    elif 202.5 <= direction_degrees < 247.5:
        return "SW"
    elif 247.5 <= direction_degrees < 292.5:
        return "W"
    elif 292.5 <= direction_degrees < 337.5:
        return "NW"
    else:
        return "Unknown"

df = df.withColumn("day", date_format(col("time"), "yyyy-MM-dd"))

df = df.withColumn("cardinal_direction", degrees_to_cardinal(df["wind_direction_10m"]))
# Računanje prosečnih brzina vetra za svaki dan
df = df.groupBy("city", "day").agg(
    {"wind_gusts_10m_kmh": "avg", "wind_speed_100m_kmh": "avg", "wind_speed_10m_kmh": "avg"}
).withColumnRenamed("avg(wind_gusts_10m_kmh)", "avg_wind_gusts_10m_kmh").withColumnRenamed("avg(wind_speed_100m_kmh)", "avg_wind_speed_100m_kmh").withColumnRenamed("avg(wind_speed_10m_kmh)", "avg_wind_speed_10m_kmh")




# Prikaz rezultata

df.write.mode("overwrite").saveAsTable("wind")
df.show()
spark.stop()