import streamlit as st
import pandas as pd
import boto3
import io
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')  


# Configura tu bucket y clave
BUCKET = os.getenv("BUCKET")
PREFIX = "refined/analisis/"

# Cargar último archivo Parquet desde S3
def cargar_datos():
    s3 = boto3.client("s3")
    # Obtener el último archivo Parquet del directorio
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    archivos = sorted([obj['Key'] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")])
    
    if not archivos:
        st.error("❌ No se encontraron archivos en S3.")
        return None

    key = archivos[-1]  # Último archivo
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    return df

# Construir interfaz Streamlit
st.set_page_config(page_title="Análisis Clima", layout="centered")
st.title("🌤️ Dashboard Clima Medellín vs Londres")

df = cargar_datos()

if df is not None:
    st.subheader("📊 Tabla de datos")
    st.dataframe(df)

    st.subheader("🌡️ Promedio de temperatura por fecha")
    st.line_chart(df[["avg_temp_med", "avg_temp_lon"]])

    st.subheader("🌧️ Promedio de lluvia por fecha")
    st.line_chart(df[["avg_rain_med", "avg_rain_lon"]])

    st.subheader("💨 Promedio de viento por fecha")
    if "avg_viento_med" in df.columns and "avg_viento_lon" in df.columns:
        st.line_chart(df[["avg_viento_med", "avg_viento_lon"]])
    else:
        st.info("No hay datos de viento promedio para mostrar.")

    st.subheader("🌡️ Diferencia promedio de temperatura (Medellín - Londres)")
    if "diferencia_temp" in df.columns:
        st.line_chart(df["diferencia_temp"])
    else:
        st.info("No hay datos de diferencia de temperatura para mostrar.")

    # Mostrar históricos si existen
    st.subheader("📈 Históricos de temperatura Medellín")
    cols_temp_med = [c for c in df.columns if c.startswith("avg_temp_med")]  # incluye históricos
    if len(cols_temp_med) > 1:
        st.line_chart(df[cols_temp_med])

    st.subheader("📈 Históricos de temperatura Londres")
    cols_temp_lon = [c for c in df.columns if c.startswith("avg_temp_lon")]  # incluye históricos
    if len(cols_temp_lon) > 1:
        st.line_chart(df[cols_temp_lon])

    st.subheader("📈 Históricos de lluvia Medellín")
    cols_rain_med = [c for c in df.columns if c.startswith("avg_rain_med")]  # incluye históricos
    if len(cols_rain_med) > 1:
        st.line_chart(df[cols_rain_med])

    st.subheader("📈 Históricos de lluvia Londres")
    cols_rain_lon = [c for c in df.columns if c.startswith("avg_rain_lon")]  # incluye históricos
    if len(cols_rain_lon) > 1:
        st.line_chart(df[cols_rain_lon])

    st.subheader("📈 Históricos de viento Medellín")
    cols_viento_med = [c for c in df.columns if c.startswith("avg_viento_med")]  # incluye históricos
    if len(cols_viento_med) > 1:
        st.line_chart(df[cols_viento_med])

    st.subheader("📈 Históricos de viento Londres")
    cols_viento_lon = [c for c in df.columns if c.startswith("avg_viento_lon")]  # incluye históricos
    if len(cols_viento_lon) > 1:
        st.line_chart(df[cols_viento_lon])
else:
    st.warning("No se pudo cargar el archivo desde S3.")
