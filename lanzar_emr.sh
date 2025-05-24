#!/bin/bash

# CONFIGURA ESTOS VALORES
BUCKET="mi-bucket-meteo-2025"
SCRIPT_ETL="s3://$BUCKET/emr-steps/etl_clima.py"
SCRIPT_ANALISIS="s3://$BUCKET/emr-steps/analisis_clima.py"
LOGS_S3="s3://$BUCKET/emr-logs/"

# Crear el clúster con dos Steps: ETL y análisis
aws emr create-cluster \
  --name "ClimaETLCluster" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --service-role EMR_DefaultRole \
  --log-uri "$LOGS_S3" \
  --steps "[
    {
      \"Type\": \"Spark\",
      \"Name\": \"ETL Clima\",
      \"ActionOnFailure\": \"CONTINUE\",
      \"Args\": [\"spark-submit\", \"--deploy-mode\", \"cluster\", \"$SCRIPT_ETL\"]
    },
    {
      \"Type\": \"Spark\",
      \"Name\": \"Análisis Clima\",
      \"ActionOnFailure\": \"CONTINUE\",
      \"Args\": [\"spark-submit\", \"--deploy-mode\", \"cluster\", \"$SCRIPT_ANALISIS\"]
    }
  ]" \
  --auto-terminate
