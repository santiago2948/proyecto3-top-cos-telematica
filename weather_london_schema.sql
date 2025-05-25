-- Esquema para crear la base de datos y la tabla en MySQL para almacenar los datos meteorológicos de Londres

CREATE DATABASE IF NOT EXISTS weather_data;
USE weather_data;

CREATE TABLE IF NOT EXISTS weather_london (
    date DATETIME,
    temperature_2m FLOAT,
    temperature_2m_previous_day1 FLOAT,
    temperature_2m_previous_day2 FLOAT,
    temperature_2m_previous_day3 FLOAT,
    temperature_2m_previous_day4 FLOAT,
    temperature_2m_previous_day5 FLOAT,
    wind_speed_10m FLOAT,
    wind_speed_10m_previous_day1 FLOAT,
    wind_speed_10m_previous_day2 FLOAT,
    wind_speed_10m_previous_day3 FLOAT,
    wind_speed_10m_previous_day4 FLOAT,
    wind_speed_10m_previous_day5 FLOAT,
    wind_speed_10m_previous_day6 FLOAT,
    wind_speed_10m_previous_day7 FLOAT,
    rain FLOAT,
    rain_previous_day1 FLOAT,
    rain_previous_day2 FLOAT,
    rain_previous_day3 FLOAT,
    rain_previous_day4 FLOAT,
    rain_previous_day5 FLOAT,
    rain_previous_day6 FLOAT,
    rain_previous_day7 FLOAT
);
