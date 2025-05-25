# ST0263-7290 - Tópicos Especiales en Telemática, 2025-1  
## Trabajo 3 – Automatización del proceso de Captura, Ingesta, Procesamiento y Salida de datos accionables (Arquitectura Batch para Big Data)

**Universidad EAFIT**  
**Fecha de entrega:** 02 de junio de 2025

### Estudiantes:
- Daniel Correa Botero, dcorreab2@eafit.edu.co
- Miguel Ángel Cano Salinas, macanos1@eafit.edu.co
- Santiago Acevedo Urrego, sacevedou1@eafit.edu.co

**Profesor:** Edwin Montoya, emontoya@eafit.edu.co

---

## 1. Descripción general

Este proyecto implementa una arquitectura batch automatizada para la captura, ingesta, procesamiento y visualización de datos meteorológicos de dos ciudades (Medellín y Londres), siguiendo el ciclo de vida de un proceso analítico real de ingeniería de datos. Se utilizan fuentes de datos abiertas (API Open-Meteo) y una base de datos relacional (MySQL), almacenamiento en S3, procesamiento con Spark en EMR y visualización con Streamlit.

---

## 2. Arquitectura y componentes

- **Fuentes de datos:**  
  - API Open-Meteo (datos meteorológicos históricos por hora)
  - Base de datos MySQL (almacena históricos de Londres)

- **Ingesta y almacenamiento:**  
  - Scripts automáticos en Python para descargar datos y cargarlos a S3 (zona raw)
  - Exportación de datos desde MySQL a S3

- **Procesamiento:**  
  - ETL y análisis automatizados en Spark sobre EMR (zonas trusted y refined)
  - Automatización de steps y clúster EMR vía scripts y cron

- **Visualización:**  
  - Aplicación web con Streamlit para mostrar y analizar los resultados refinados

- **Orquestación:**  
  - Automatización total mediante scripts `.sh` y tareas programadas con `cron`

---

## 3. Estructura del repositorio

```
├── cargar_datos_diario_medellin.py
├── carga_datos_london.py
├── carga_datos_london.sh
├── carga_datos_medellin.sh
├── docker-compose.yml
├── emr-steps/
│   ├── analisis_clima.py
│   └── etl_clima.py
├── export_from_api_to_s3.py
├── export_sql_to_s3.py
├── lanzar_emr.sh
├── load_london_to_sql.py
├── londres/
│   └── weatherYYYY-MM-DD.csv
├── log_emr.txt
├── requirements.txt
├── view/
│   ├── app.py
│   └── requirements.txt
├── weather_london_schema.sql
└── .env.example
```

---

## 4. Requisitos y preparación del entorno

- Ubuntu 22.04 (EC2)
- Docker y Docker Compose
- Python 3.10+ y venv
- AWS CLI
- Acceso a AWS S3 y EMR (rol IAM: LabInstanceProfile)

### Instalación de dependencias

```bash
sudo apt update
sudo apt install docker.io docker-compose python3 python3-venv python3-pip unzip -y
sudo systemctl enable docker
sudo systemctl start docker
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

### Clonar el repositorio

```bash
git clone https://github.com/santiago2948/proyecto3-top-cos-telematica.git
cd proyecto3-top-cos-telematica
```

### Configurar base de datos MySQL

```bash
sudo docker-compose up -d
sudo docker exec -i mysql_weather mysql -uadmin -padmin123 -D weather_data < weather_london_schema.sql
```

### Configurar variables de entorno

1. Copia `.env.example` a `.env` y completa los valores:
    ```
    BUCKET=<nombre_bucket_s3>
    BD_USER=<usuario_mysql>
    BD_PASSWORD=<password_mysql>
    BD_HOST=<host_mysql>
    ```

2. Crea el bucket S3 y sube la carpeta `emr-steps` al bucket.

---

## 5. Carga inicial y procesos automáticos

### Carga inicial de datos (últimos 7 días)

```bash
python3 -m venv my_env
source my_env/bin/activate
pip install -r requirements.txt
python export_from_api_to_s3.py
python load_london_to_sql.py
python export_sql_to_s3.py
```

### Permisos de scripts

```bash
chmod +x lanzar_emr.sh
chmod +x carga_datos_london.sh
chmod +x carga_datos_medellin.sh
```

### Programar tareas automáticas con cron

```bash
crontab -e
```
Agrega al final:
```
0 0 * * * /home/ubuntu/workarea/cargar_datos_diario_medellin.py >> /home/ubuntu/workarea/log_medellin.txt 2>&1
5 0 * * * /home/ubuntu/workarea/carga_datos_london.sh >> /home/ubuntu/workarea/log_london.txt 2>&1
0 */4 * * * /home/ubuntu/workarea/lanzar_emr.sh >> /home/ubuntu/workarea/log_emr.txt 2>&1
```

---

## 6. Procesamiento y análisis en EMR

- El script `lanzar_emr.sh` crea el clúster EMR y ejecuta los steps de ETL y análisis automáticamente.
- Los scripts Spark (`emr-steps/etl_clima.py` y `emr-steps/analisis_clima.py`) procesan los datos y los almacenan en S3 en las zonas `trusted` y `refined`.

---

## 7. Visualización de resultados

La aplicación web en Streamlit permite visualizar los datos refinados y estadísticas.

Se puede visualizar en: http://34.205.142.167:8501

### Ejecución:

```bash
cd view
nohup streamlit run app.py --server.port=8501 > output.log 2>&1 &
```
Accede en: `http://<IP_publica>:8501`

---

---

## 9. Referencias

- [Open-Meteo API](https://open-meteo.com/)
- [AWS EMR](https://docs.aws.amazon.com/emr/)
- [AWS S3](https://docs.aws.amazon.com/s3/)
- [Streamlit](https://streamlit.io/)
- [Pandas](https://pandas.pydata.org/)
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Universidad EAFIT](https://www.eafit.edu.co/)

---


