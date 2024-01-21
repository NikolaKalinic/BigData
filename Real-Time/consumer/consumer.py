from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
quiet_logs(spark)

comments = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "raw") \
  .load()


schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("tz_id", StringType(), True),
        StructField("localtime_epoch", IntegerType(), True),
        StructField("localtime", StringType(), True),
    ]), True),
    StructField("current", StructType([
        StructField("last_updated_epoch", IntegerType(), True),
        StructField("last_updated", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("condition", StructType([
            StructField("text", StringType(), True),
            StructField("icon", StringType(), True),
            StructField("code", IntegerType(), True),
        ]), True),
        StructField("wind_mph", DoubleType(), True),
        StructField("wind_kph", DoubleType(), True),
        StructField("wind_degree", IntegerType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("pressure_in", DoubleType(), True),
        StructField("precip_mm", DoubleType(), True),
        StructField("precip_in", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("cloud", IntegerType(), True),
        StructField("feelslike_c", DoubleType(), True),
        StructField("feelslike_f", DoubleType(), True),
        StructField("vis_km", DoubleType(), True),
        StructField("vis_miles", DoubleType(), True),
        StructField("uv", DoubleType(), True),
        StructField("gust_mph", DoubleType(), True),
        StructField("gust_kph", DoubleType(), True),
    ]), True),
])


comments_df = comments.selectExpr("CAST(value AS STRING)")
parsed_df = comments_df.select(from_json(col("value"), schema).alias("json_value")).select("json_value.*")

# Extract fields from the nested structs
parsed_df = parsed_df.withColumn("location_name", col("location.name"))
parsed_df = parsed_df.withColumn("region", col("location.region"))
parsed_df = parsed_df.withColumn("country", col("location.country"))
parsed_df = parsed_df.withColumn("lat", col("location.lat"))
parsed_df = parsed_df.withColumn("lon", col("location.lon"))
parsed_df = parsed_df.withColumn("tz_id", col("location.tz_id"))
parsed_df = parsed_df.withColumn("localtime_epoch", col("location.localtime_epoch"))
parsed_df = parsed_df.withColumn("localtime", col("location.localtime"))

parsed_df = parsed_df.withColumn("last_updated_epoch", col("current.last_updated_epoch"))
parsed_df = parsed_df.withColumn("last_updated", col("current.last_updated"))
parsed_df = parsed_df.withColumn("temp_c", col("current.temp_c"))
parsed_df = parsed_df.withColumn("temp_f", col("current.temp_f"))
parsed_df = parsed_df.withColumn("is_day", col("current.is_day"))
parsed_df = parsed_df.withColumn("condition_text", col("current.condition.text"))
parsed_df = parsed_df.withColumn("condition_icon", col("current.condition.icon"))
parsed_df = parsed_df.withColumn("condition_code", col("current.condition.code"))
parsed_df = parsed_df.withColumn("wind_mph", col("current.wind_mph"))
parsed_df = parsed_df.withColumn("wind_kph", col("current.wind_kph"))
parsed_df = parsed_df.withColumn("wind_degree", col("current.wind_degree"))
parsed_df = parsed_df.withColumn("wind_dir", col("current.wind_dir"))
parsed_df = parsed_df.withColumn("pressure_mb", col("current.pressure_mb"))
parsed_df = parsed_df.withColumn("pressure_in", col("current.pressure_in"))
parsed_df = parsed_df.withColumn("precip_mm", col("current.precip_mm"))
parsed_df = parsed_df.withColumn("precip_in", col("current.precip_in"))
parsed_df = parsed_df.withColumn("humidity", col("current.humidity"))
parsed_df = parsed_df.withColumn("cloud", col("current.cloud"))
parsed_df = parsed_df.withColumn("feelslike_c", col("current.feelslike_c"))
parsed_df = parsed_df.withColumn("feelslike_f", col("current.feelslike_f"))
parsed_df = parsed_df.withColumn("vis_km", col("current.vis_km"))
parsed_df = parsed_df.withColumn("vis_miles", col("current.vis_miles"))
parsed_df = parsed_df.withColumn("uv", col("current.uv"))
parsed_df = parsed_df.withColumn("gust_mph", col("current.gust_mph"))
parsed_df = parsed_df.withColumn("gust_kph", col("current.gust_kph"))

# Drop the original "location" and "current" struct columns if desired
parsed_df = parsed_df.drop("location", "current")

# Write the parsed DataFrame to Hive
query = parsed_df.writeStream.outputMode("update").trigger(processingTime='1 minute').foreachBatch(lambda batch_df, batch_id: batch_df.write.saveAsTable("Raw", mode="append")).start()


# query = comments \
#     .selectExpr("CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("json_value")) \
#     .select("json_value.*") \
#     .writeStream \
#     .outputMode("update") \
#     .trigger(processingTime='1 minute') \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write.saveAsTable("Raw", mode="append")) \
#     .start()

query.awaitTermination()