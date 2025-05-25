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



