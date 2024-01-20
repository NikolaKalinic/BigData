import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg

# @udf(StringType())
# def degrees_to_cardinal(direction_degrees):
#     if 337.5 <= direction_degrees <= 360 or 0 <= direction_degrees < 22.5:
#         return "N"
#     elif 22.5 <= direction_degrees < 67.5:
#         return "NE"
#     elif 67.5 <= direction_degrees < 112.5:
#         return "E"
#     elif 112.5 <= direction_degrees < 157.5:
#         return "SE"
#     elif 157.5 <= direction_degrees < 202.5:
#         return "S"
#     elif 202.5 <= direction_degrees < 247.5:
#         return "SW"
#     elif 247.5 <= direction_degrees < 292.5:
#         return "W"
#     elif 292.5 <= direction_degrees < 337.5:
#         return "NW"
#     else:
#         return "Unknown"

def preprecesing(df,c):
    #renamed columns
    df = df.withColumnRenamed("temperature_2m (°C)", "temperature_2m_C")
    df = df.withColumnRenamed("relative_humidity_2m (%)", "relative_humidity_2m_percent")
    df = df.withColumnRenamed("dew_point_2m (°C)", "dew_point_2m_C")
    df = df.withColumnRenamed("apparent_temperature (°C)","apparent_temperature_C")
    df = df.withColumnRenamed("cloud_cover (%)","cloud_cover_percent")
    df = df.withColumnRenamed("cloud_cover_low (%)","cloud_cover_low_percent")
    df = df.withColumnRenamed("cloud_cover_mid (%)","cloud_cover_mid_percent")
    df = df.withColumnRenamed("cloud_cover_high (%)","cloud_cover_high_percent")
    df = df.withColumnRenamed("wind_direction_10m (°)","wind_direction_10m")
    df = df.withColumnRenamed("wind_direction_100m (°)","wind_direction_100m")
    df = df.withColumnRenamed("soil_temperature_0_to_7cm (°C)","soil_temperature_0_to_7cm_C")
    df = df.withColumnRenamed("soil_temperature_7_to_28cm (°C)","soil_temperature_7_to_28cm_C")
    df = df.withColumnRenamed("soil_temperature_28_to_100cm (°C)","soil_temperature_28_to_100cm_C")
    df = df.withColumnRenamed("soil_temperature_100_to_255cm (°C)","soil_temperature_100_to_255cm_C")
    df = df.withColumnRenamed("soil_moisture_0_to_7cm (m³/m³)","soil_moisture_0_to_7cm")
    df = df.withColumnRenamed("soil_moisture_7_to_28cm (m³/m³)","soil_moisture_7_to_28cm")
    df = df.withColumnRenamed("soil_moisture_28_to_100cm (m³/m³)","soil_moisture_28_to_100cm")
    df = df.withColumnRenamed("soil_moisture_100_to_255cm (m³/m³)","soil_moisture_100_to_255cm")
    df = df.withColumnRenamed("shortwave_radiation (W/m²)","shortwave_radiation")
    df = df.withColumnRenamed("direct_radiation (W/m²)","direct_radiation")
    df = df.withColumnRenamed("diffuse_radiation (W/m²)","diffuse_radiation")
    df = df.withColumnRenamed("direct_normal_irradiance (W/m²)","direct_normal_irradiance")
    df = df.withColumnRenamed("terrestrial_radiation (W/m²)","terrestrial_radiation")
    df = df.withColumnRenamed("shortwave_radiation_instant (W/m²)","shortwave_radiation_instant")
    df = df.withColumnRenamed("direct_radiation_instant (W/m²)","direct_radiation_instant")
    df = df.withColumnRenamed("diffuse_radiation_instant (W/m²)","diffuse_radiation_instant")
    df = df.withColumnRenamed("direct_normal_irradiance_instant (W/m²)","direct_normal_irradiance_instant")
    df = df.withColumnRenamed("terrestrial_radiation_instant (W/m²)","terrestrial_radiation_instant")
    df = df.withColumnRenamed("precipitation (mm)","precipitation_mm")
    df = df.withColumnRenamed("rain (mm)","rain_mm")
    df = df.withColumnRenamed("snowfall (cm)","snowfall_cm")
    df = df.withColumnRenamed("snow_depth (m)","snow_depth_m")
    df = df.withColumnRenamed("weather_code (wmo code)","weather_code")
    df = df.withColumnRenamed("pressure_msl (hPa)","pressure_msl_hPa")
    df = df.withColumnRenamed("surface_pressure (hPa)","surface_pressure_hPa")
    df = df.withColumnRenamed("et0_fao_evapotranspiration (mm)","et0_fao_evapotranspiration")
    df = df.withColumnRenamed("vapour_pressure_deficit (kPa)","vapour_pressure_deficit_kPa")
    df = df.withColumnRenamed("wind_speed_10m (km/h)","wind_speed_10m_kmh")
    df = df.withColumnRenamed("wind_speed_100m (km/h)","wind_speed_100m_kmh")
    df = df.withColumnRenamed("wind_gusts_10m (km/h)","wind_gusts_10m_kmh")
    df = df.withColumnRenamed("is_day ()","is_day")
    df = df.withColumnRenamed("sunshine_duration (s)","sunshine_duration_s")
    #add columns
    df = df.withColumn("city",lit(c))
    #drop columns
    df = df.drop("et0_fao_evapotranspiration")
    return df

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

df = spark.read.csv("hdfs://namenode:9000/NoviSad.csv",inferSchema=True,header=True)
df = preprecesing(df,"NS")
df.write.mode("append").saveAsTable("data")
df = spark.read.csv("hdfs://namenode:9000/Belgrade.csv",inferSchema=True,header=True)
df = preprecesing(df,"BG")
df.write.mode("append").saveAsTable("data")
df = spark.read.csv("hdfs://namenode:9000/Nis.csv",inferSchema=True,header=True)
df = preprecesing(df,"NI")
df.write.mode("append").saveAsTable("data")
df = spark.read.csv("hdfs://namenode:9000/Subotica.csv",inferSchema=True,header=True)
df = preprecesing(df,"SU")
df.write.mode("append").saveAsTable("data")
df = spark.read.csv("hdfs://namenode:9000/Kragujevac.csv",inferSchema=True,header=True)
df = preprecesing(df,"KG")
df.write.mode("append").saveAsTable("data")


#testing area
# result_df = df.groupBy("wind_direction_cardinal").agg(avg("wind_speed_10m_kmh").alias("average_wind_speed"))

# df = df.withColumn("wind_direction_cardinal", degrees_to_cardinal(df["wind_direction_10m"].cast("double")))
# rdd = df.rdd
# mapped_rdd = rdd.map(lambda row: (row["wind_direction_cardinal"], (row["wind_speed_10m_kmh"], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))
# result_df = spark.createDataFrame(mapped_rdd, ["wind_direction_cardinal", "average_wind_speed"])
# result_df.write.mode("overwrite").saveAsTable("AvgWindSpeedForDirection")