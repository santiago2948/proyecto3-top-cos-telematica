from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  


# Crear SparkSession
spark = SparkSession.builder.appName("ETL Clima").getOrCreate()

# -------- CONFIGURACIÓN --------
bucket = os.getenv("BUCKET")
zona_raw_medellin = f"s3://{bucket}/raw/medellin/"
zona_raw_londres = f"s3://{bucket}/raw/londres/"
zona_trusted = f"s3://{bucket}/trusted/clima_etl/"

# -------- LECTURA DE DATOS --------
df_med = spark.read.option("header", True).csv(zona_raw_medellin)
df_lon = spark.read.option("header", True).csv(zona_raw_londres)

# -------- LIMPIEZA Y ENRIQUECIMIENTO --------
# Convertir fecha
df_med = df_med.withColumn("fecha", to_date("date"))
df_lon = df_lon.withColumn("fecha", to_date("date"))

# Agregar columna de ciudad
df_med = df_med.withColumn("ciudad", lit("Medellin"))
df_lon = df_lon.withColumn("ciudad", lit("Londres"))

# Seleccionar todas las columnas relevantes
todas_las_columnas = [
    "fecha", "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
    "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
    "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7", "ciudad"
]
df_med = df_med.select(*todas_las_columnas)
df_lon = df_lon.select(*todas_las_columnas)

# Eliminar filas con valores nulos en cualquiera de las columnas principales
df_med = df_med.dropna()
df_lon = df_lon.dropna()

# -------- UNIÓN --------
df_final = df_med.unionByName(df_lon)

# -------- GUARDAR EN ZONA TRUSTED --------
df_final.write.mode("overwrite").parquet(zona_trusted)

print("✅ ETL completado. Datos guardados en zona trusted.")
