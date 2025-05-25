import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  

# Configuración S3

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")

# Configura la latitud y longitud para Londres
latitude = 51.5074
longitude = -0.1278

# Configura la conexión a tu base de datos MySQL (ajusta usuario, contraseña, host, puerto y base de datos)
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/weather_data')

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://previous-runs-api.open-meteo.com/v1/forecast"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "hourly": [
        "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2", "temperature_2m_previous_day3", "temperature_2m_previous_day4", "temperature_2m_previous_day5",
        "wind_speed_10m", "wind_speed_10m_previous_day1", "wind_speed_10m_previous_day2", "wind_speed_10m_previous_day3", "wind_speed_10m_previous_day4", "wind_speed_10m_previous_day5", "wind_speed_10m_previous_day6", "wind_speed_10m_previous_day7",
        "rain", "rain_previous_day1", "rain_previous_day2", "rain_previous_day3", "rain_previous_day4", "rain_previous_day5", "rain_previous_day6", "rain_previous_day7"
    ]
}

# Calcula el rango de fechas: últimos 7 días hasta hoy
end_date = pd.Timestamp.utcnow().floor('D') - pd.Timedelta(days=1)
start_date = end_date - pd.Timedelta(days=6)

# Formatea las fechas para la API (YYYY-MM-DD)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Agrega los parámetros de fecha a la consulta de la API
params["start_date"] = start_date_str
params["end_date"] = end_date_str

responses = openmeteo.weather_api(url, params=params)

response = responses[0]
hourly = response.Hourly()

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

# Ya no es necesario filtrar el DataFrame por fecha, porque la API solo trae los datos requeridos.
# Guarda el DataFrame en la base de datos (tabla: weather_london)
hourly_df.to_sql('weather_london', con=engine, if_exists='replace', index=False)
print(f"Datos meteorológicos de Londres ({start_date_str} a {end_date_str}) cargados en la base de datos.")
