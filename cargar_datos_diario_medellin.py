import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import timedelta, date
import boto3
import io
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  


# Calcular la fecha del día anterior
fecha = date.today() - timedelta(days=1)
fecha_str = fecha.isoformat()

# Configuración de la API
url = "https://previous-runs-api.open-meteo.com/v1/forecast"
params = {
    "latitude": 6.25,
    "longitude": -75.56,
    "start_date": fecha_str,
    "end_date": fecha_str,
    "hourly": [
        "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
        "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
        "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
    ],
    "timezone": "America/Bogota"
}

# API con cache y retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Llamada a la API
responses = openmeteo.weather_api(url, params=params)
response = responses[0]
hourly = response.Hourly()

# Crear el DataFrame
hourly_data = {"date": pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
)}
variables = [
    "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
    "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
    "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
]
for i, name in enumerate(variables):
    hourly_data[name] = hourly.Variables(i).ValuesAsNumpy()

df = pd.DataFrame(hourly_data)

# Ajustar el rango de fechas para obtener solo las 24 horas del día anterior en UTC
start_dt = pd.to_datetime(fecha_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pd.Timestamp.utcnow().tz)
end_dt = start_dt + pd.Timedelta(hours=24)
mask = (df['date'] >= start_dt) & (df['date'] < end_dt)
df = df[mask]

# Si faltan horas, rellenar filas vacías para completar las 24 horas
if len(df) < 24:
    all_hours = pd.date_range(start=start_dt, end=end_dt - pd.Timedelta(hours=1), freq='h')
    df = df.set_index('date').reindex(all_hours).reset_index().rename(columns={'index': 'date'})

# Subir a S3
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

s3 = boto3.client('s3')
bucket_name = os.getenv("BUCKET")
s3_key = f"raw/medellin/weather_{fecha_str}.csv"

s3.upload_fileobj(csv_buffer, bucket_name, s3_key)

params = {
    "latitude": 51.5074,
    "longitude": -0.1278,
    "start_date": fecha_str,
    "end_date": fecha_str,
    "hourly": [
        "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
        "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
        "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
    ],
    "timezone": "America/Bogota"
}

# --- Consulta y guarda datos de Londres para el mismo día en MySQL ---

# Llama a la API de Londres con los params ya definidos
url_london = "https://previous-runs-api.open-meteo.com/v1/forecast"
openmeteo_london = openmeteo_requests.Client(session=retry_session)
responses_london = openmeteo_london.weather_api(url_london, params=params)
response_london = responses_london[0]
hourly_london = response_london.Hourly()

# Construcción del DataFrame igual que en load_london_to_sql.py
hourly_data_london = {"date": pd.date_range(
    start=pd.to_datetime(hourly_london.Time(), unit="s", utc=True),
    end=pd.to_datetime(hourly_london.TimeEnd(), unit="s", utc=True),
    freq=pd.Timedelta(seconds=hourly_london.Interval()),
    inclusive="left"
)}

var_names_london = [
    "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
    "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
    "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
]

for i, name in enumerate(var_names_london):
    hourly_data_london[name] = hourly_london.Variables(i).ValuesAsNumpy()

london_df = pd.DataFrame(data=hourly_data_london)

# Filtrar solo las filas cuyo 'date' corresponda exactamente a fecha_str (día en UTC)
london_df = london_df[london_df['date'].dt.date == pd.to_datetime(fecha_str).date()]

# Configura la conexión a tu base de datos MySQL
engine = create_engine('mysql+pymysql://admin:admin123@localhost:3306/weather_data')

# Guarda el DataFrame en la base de datos (tabla: weather_london)
london_df.to_sql('weather_london', con=engine, if_exists='append', index=False)

