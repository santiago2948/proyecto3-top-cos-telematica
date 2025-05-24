import pandas as pd
from sqlalchemy import create_engine
import boto3
import io

# Configuración de la base de datos
engine = create_engine('mysql+pymysql://admin:admin123@localhost:3306/weather_data')

# Leer los datos de la tabla
query = 'SELECT * FROM weather_london'
df = pd.read_sql(query, con=engine)

# Cliente S3
s3 = boto3.client('s3')

# Nombre del bucket y carpeta en S3
bucket_name = 'mi-bucket-meteo-2025'
prefix = 'raw/londres/'

# Exportar por fecha y subir a S3
for fecha, group in df.groupby(df['date'].dt.date):
    nombre_csv = f'weather_{fecha}.csv'

    # Convertir DataFrame a CSV en memoria y luego codificar a bytes
    csv_buffer = io.BytesIO()
    csv_string = group.to_csv(index=False)
    csv_buffer.write(csv_string.encode('utf-8'))
    csv_buffer.seek(0)

    s3_path = prefix + nombre_csv
    s3.upload_fileobj(csv_buffer, bucket_name, s3_path)
    print(f'Subido: s3://{bucket_name}/{s3_path}')

print('✅ Todos los CSV de Londres fueron subidos a S3 con éxito.')
