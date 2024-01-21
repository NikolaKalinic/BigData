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
from pyspark.sql.window import Window


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

df = df.withColumn("year", F.year("time"))
df = df.withColumn("day", F.date_format("time", "yyyy-MM-dd"))

# Define a window specification for daily aggregation
daily_window_spec = Window.partitionBy("city", "year", "day")

# Add a column with the daily total precipitation
df = df.withColumn("daily_total_precipitation", F.sum("precipitation_mm").over(daily_window_spec))

# Calculate the total number of cloudy days per year
result = df.groupBy("city", "year").agg(
    F.ceil(F.sum(F.when((F.col("cloud_cover_percent") > 50), 1).otherwise(0)) / 24).alias("cloudy_days"),
    F.round(F.sum("daily_total_precipitation"), 2).alias("total_precipitation_mm")
)

result.write.mode("overwrite").saveAsTable("cloudy_season")
result.show()
spark.stop()