from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, round as pyspark_round
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  

bucket = os.getenv("BUCKET")

bucket = "mi-bucket-meteo-2025"
df = spark.read.parquet(f"s3://{bucket}/trusted/clima_etl/")

# Crear columnas separadas para cada ciudad y cada variable relevante
analisis = df \
    .withColumn("temp_med", when(col("ciudad") == "Medellin", col("temperature_2m"))) \
    .withColumn("temp_med_pd1", when(col("ciudad") == "Medellin", col("temperature_2m_previous_day1"))) \
    .withColumn("temp_med_pd2", when(col("ciudad") == "Medellin", col("temperature_2m_previous_day2"))) \
    .withColumn("temp_med_pd3", when(col("ciudad") == "Medellin", col("temperature_2m_previous_day3"))) \
    .withColumn("temp_med_pd4", when(col("ciudad") == "Medellin", col("temperature_2m_previous_day4"))) \
    .withColumn("temp_med_pd5", when(col("ciudad") == "Medellin", col("temperature_2m_previous_day5"))) \
    .withColumn("viento_med", when(col("ciudad") == "Medellin", col("wind_speed_10m"))) \
    .withColumn("viento_med_pd1", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day1"))) \
    .withColumn("viento_med_pd2", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day2"))) \
    .withColumn("viento_med_pd3", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day3"))) \
    .withColumn("viento_med_pd4", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day4"))) \
    .withColumn("viento_med_pd5", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day5"))) \
    .withColumn("viento_med_pd6", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day6"))) \
    .withColumn("viento_med_pd7", when(col("ciudad") == "Medellin", col("wind_speed_10m_previous_day7"))) \
    .withColumn("rain_med", when(col("ciudad") == "Medellin", col("rain"))) \
    .withColumn("rain_med_pd1", when(col("ciudad") == "Medellin", col("rain_previous_day1"))) \
    .withColumn("rain_med_pd2", when(col("ciudad") == "Medellin", col("rain_previous_day2"))) \
    .withColumn("rain_med_pd3", when(col("ciudad") == "Medellin", col("rain_previous_day3"))) \
    .withColumn("rain_med_pd4", when(col("ciudad") == "Medellin", col("rain_previous_day4"))) \
    .withColumn("rain_med_pd5", when(col("ciudad") == "Medellin", col("rain_previous_day5"))) \
    .withColumn("rain_med_pd6", when(col("ciudad") == "Medellin", col("rain_previous_day6"))) \
    .withColumn("rain_med_pd7", when(col("ciudad") == "Medellin", col("rain_previous_day7"))) \
    .withColumn("temp_lon", when(col("ciudad") == "Londres", col("temperature_2m"))) \
    .withColumn("temp_lon_pd1", when(col("ciudad") == "Londres", col("temperature_2m_previous_day1"))) \
    .withColumn("temp_lon_pd2", when(col("ciudad") == "Londres", col("temperature_2m_previous_day2"))) \
    .withColumn("temp_lon_pd3", when(col("ciudad") == "Londres", col("temperature_2m_previous_day3"))) \
    .withColumn("temp_lon_pd4", when(col("ciudad") == "Londres", col("temperature_2m_previous_day4"))) \
    .withColumn("temp_lon_pd5", when(col("ciudad") == "Londres", col("temperature_2m_previous_day5"))) \
    .withColumn("viento_lon", when(col("ciudad") == "Londres", col("wind_speed_10m"))) \
    .withColumn("viento_lon_pd1", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day1"))) \
    .withColumn("viento_lon_pd2", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day2"))) \
    .withColumn("viento_lon_pd3", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day3"))) \
    .withColumn("viento_lon_pd4", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day4"))) \
    .withColumn("viento_lon_pd5", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day5"))) \
    .withColumn("viento_lon_pd6", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day6"))) \
    .withColumn("viento_lon_pd7", when(col("ciudad") == "Londres", col("wind_speed_10m_previous_day7"))) \
    .withColumn("rain_lon", when(col("ciudad") == "Londres", col("rain"))) \
    .withColumn("rain_lon_pd1", when(col("ciudad") == "Londres", col("rain_previous_day1"))) \
    .withColumn("rain_lon_pd2", when(col("ciudad") == "Londres", col("rain_previous_day2"))) \
    .withColumn("rain_lon_pd3", when(col("ciudad") == "Londres", col("rain_previous_day3"))) \
    .withColumn("rain_lon_pd4", when(col("ciudad") == "Londres", col("rain_previous_day4"))) \
    .withColumn("rain_lon_pd5", when(col("ciudad") == "Londres", col("rain_previous_day5"))) \
    .withColumn("rain_lon_pd6", when(col("ciudad") == "Londres", col("rain_previous_day6"))) \
    .withColumn("rain_lon_pd7", when(col("ciudad") == "Londres", col("rain_previous_day7")))

# Agrupar por fecha y calcular promedios diarios por ciudad y variable relevante
agg = analisis.groupBy("fecha").agg(
    pyspark_round(avg("temp_med"), 2).alias("avg_temp_med"),
    pyspark_round(avg("temp_med_pd1"), 2).alias("avg_temp_med_pd1"),
    pyspark_round(avg("temp_med_pd2"), 2).alias("avg_temp_med_pd2"),
    pyspark_round(avg("temp_med_pd3"), 2).alias("avg_temp_med_pd3"),
    pyspark_round(avg("temp_med_pd4"), 2).alias("avg_temp_med_pd4"),
    pyspark_round(avg("temp_med_pd5"), 2).alias("avg_temp_med_pd5"),
    pyspark_round(avg("viento_med"), 2).alias("avg_viento_med"),
    pyspark_round(avg("viento_med_pd1"), 2).alias("avg_viento_med_pd1"),
    pyspark_round(avg("viento_med_pd2"), 2).alias("avg_viento_med_pd2"),
    pyspark_round(avg("viento_med_pd3"), 2).alias("avg_viento_med_pd3"),
    pyspark_round(avg("viento_med_pd4"), 2).alias("avg_viento_med_pd4"),
    pyspark_round(avg("viento_med_pd5"), 2).alias("avg_viento_med_pd5"),
    pyspark_round(avg("viento_med_pd6"), 2).alias("avg_viento_med_pd6"),
    pyspark_round(avg("viento_med_pd7"), 2).alias("avg_viento_med_pd7"),
    pyspark_round(avg("rain_med"), 2).alias("avg_rain_med"),
    pyspark_round(avg("rain_med_pd1"), 2).alias("avg_rain_med_pd1"),
    pyspark_round(avg("rain_med_pd2"), 2).alias("avg_rain_med_pd2"),
    pyspark_round(avg("rain_med_pd3"), 2).alias("avg_rain_med_pd3"),
    pyspark_round(avg("rain_med_pd4"), 2).alias("avg_rain_med_pd4"),
    pyspark_round(avg("rain_med_pd5"), 2).alias("avg_rain_med_pd5"),
    pyspark_round(avg("rain_med_pd6"), 2).alias("avg_rain_med_pd6"),
    pyspark_round(avg("rain_med_pd7"), 2).alias("avg_rain_med_pd7"),
    pyspark_round(avg("temp_lon"), 2).alias("avg_temp_lon"),
    pyspark_round(avg("temp_lon_pd1"), 2).alias("avg_temp_lon_pd1"),
    pyspark_round(avg("temp_lon_pd2"), 2).alias("avg_temp_lon_pd2"),
    pyspark_round(avg("temp_lon_pd3"), 2).alias("avg_temp_lon_pd3"),
    pyspark_round(avg("temp_lon_pd4"), 2).alias("avg_temp_lon_pd4"),
    pyspark_round(avg("temp_lon_pd5"), 2).alias("avg_temp_lon_pd5"),
    pyspark_round(avg("viento_lon"), 2).alias("avg_viento_lon"),
    pyspark_round(avg("viento_lon_pd1"), 2).alias("avg_viento_lon_pd1"),
    pyspark_round(avg("viento_lon_pd2"), 2).alias("avg_viento_lon_pd2"),
    pyspark_round(avg("viento_lon_pd3"), 2).alias("avg_viento_lon_pd3"),
    pyspark_round(avg("viento_lon_pd4"), 2).alias("avg_viento_lon_pd4"),
    pyspark_round(avg("viento_lon_pd5"), 2).alias("avg_viento_lon_pd5"),
    pyspark_round(avg("viento_lon_pd6"), 2).alias("avg_viento_lon_pd6"),
    pyspark_round(avg("viento_lon_pd7"), 2).alias("avg_viento_lon_pd7"),
    pyspark_round(avg("rain_lon"), 2).alias("avg_rain_lon"),
    pyspark_round(avg("rain_lon_pd1"), 2).alias("avg_rain_lon_pd1"),
    pyspark_round(avg("rain_lon_pd2"), 2).alias("avg_rain_lon_pd2"),
    pyspark_round(avg("rain_lon_pd3"), 2).alias("avg_rain_lon_pd3"),
    pyspark_round(avg("rain_lon_pd4"), 2).alias("avg_rain_lon_pd4"),
    pyspark_round(avg("rain_lon_pd5"), 2).alias("avg_rain_lon_pd5"),
    pyspark_round(avg("rain_lon_pd6"), 2).alias("avg_rain_lon_pd6"),
    pyspark_round(avg("rain_lon_pd7"), 2).alias("avg_rain_lon_pd7"),
    pyspark_round(avg(col("temp_med") - col("temp_lon")), 2).alias("diferencia_temp")
).orderBy("fecha")

# Guardar en zona refined
agg.write.mode("overwrite").parquet(f"s3://{bucket}/refined/analisis/")
