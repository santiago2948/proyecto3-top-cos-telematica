import pandas as pd
from sqlalchemy import create_engine
import boto3
import io
from datetime import timedelta, date
import os

# Calcular la fecha del día anterior
fecha = date.today() - timedelta(days=1)
fecha_str = fecha.isoformat()

# Configuración de la base de datos
engine = create_engine('mysql+pymysql://admin:admin123@localhost:3306/weather_data')

# Leer los datos de weather_london del día anterior
query = f"SELECT * FROM weather_london WHERE DATE(date) = '{fecha_str}'"
df = pd.read_sql(query, con=engine)

# Asegurar que hay datos y crear carpeta temporal
if df.empty:
    print(f"No hay datos para Londres en la fecha {fecha_str}")
    exit(1)

os.makedirs("londres", exist_ok=True)
file_path = f"londres/weather{fecha_str}.csv"
df.to_csv(file_path, index=False)

# Subir a S3
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

s3 = boto3.client('s3')
bucket_name = "mi-bucket-meteo-2025"
s3_key = f"raw/londres/weather_{fecha_str}.csv"
s3.upload_fileobj(csv_buffer, bucket_name, s3_key)

