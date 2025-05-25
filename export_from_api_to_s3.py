import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import timedelta, date
import boto3
import io
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  

# Configuración S3
bucket_name = os.getenv("BUCKET")
prefix = "raw/medellin/"

s3 = boto3.client('s3')

# Configurar cliente Open-Meteo
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Coordenadas y fechas
url = "https://previous-runs-api.open-meteo.com/v1/forecast"
end_date = date.today() - timedelta(days=1)
start_date = end_date - timedelta(days=6)

params = {
    "latitude": 6.29885286034838,
    "longitude": -75.57204956929291,
    "start_date": str(start_date),
    "end_date": str(end_date),
    "hourly": [
        "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
        "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
        "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
    ]
}
responses = openmeteo.weather_api(url, params=params)

response = responses[0]
hourly = response.Hourly()

# Construir DataFrame
hourly_data = {"date": pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
)}

var_names = [
    "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
    "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
    "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
]

for i, name in enumerate(var_names):
    hourly_data[name] = hourly.Variables(i).ValuesAsNumpy()

hourly_df = pd.DataFrame(data=hourly_data)

# Agrupar por día y subir a S3
hourly_df['date'] = pd.to_datetime(hourly_df['date'])
hourly_df['day'] = hourly_df['date'].dt.date

for day, group in hourly_df.groupby('day'):
    group.drop(columns=['day'], inplace=True)

    csv_buffer = io.BytesIO()
    csv_string = group.to_csv(index=False)
    csv_buffer.write(csv_string.encode('utf-8'))
    csv_buffer.seek(0)

    filename = f"weather_{day}.csv"
    s3_path = prefix + filename

    s3.upload_fileobj(csv_buffer, bucket_name, s3_path)
    print(f"✅ Subido: s3://{bucket_name}/{s3_path}")

print("🎉 Todos los CSV de Medellín han sido subidos a S3.")
