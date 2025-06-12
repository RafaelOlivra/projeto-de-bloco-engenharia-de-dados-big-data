from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, abs, to_timestamp, hour, expr, when, split, upper, unix_timestamp, current_timestamp
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

# MinIO/S3 configs
minio_endpoint = "http://minio:9000"
minio_user = os.environ['MINIO_ROOT_USER']
minio_password = os.environ['MINIO_ROOT_PASSWORD']
raw_bucket = "raw"
refined_bucket = "refined"

TIME_THRESHOLD = 7200  # Últimas 2 horas

# SparkSession
spark = (
    SparkSession
    .builder
    .master("spark://spark-master:7077")
    .appName("Air Quality Anomaly Detection")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_user)
    .config("spark.hadoop.fs.s3a.secret.key", minio_password)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar")
    .getOrCreate()
)

############## Análise de Qualidade do Ar ##############

# Calcular janelas de tempo para partições relevantes
now = datetime.utcnow()
start_time = now - timedelta(seconds=TIME_THRESHOLD)

partition_paths = [
    f"s3a://{raw_bucket}/airquality/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
]

# Incluir dia anterior se necessário
if start_time.date() != now.date():
    partition_paths.append(
        f"s3a://{raw_bucket}/airquality/year={start_time.year}/month={start_time.month:02d}/day={start_time.day:02d}/"
    )

# Leitura otimizada
df_airquality = spark.read.parquet(*partition_paths)
df_airquality = df_airquality.withColumn("timestamp", to_timestamp(col("timestamp")))

# Filtro: apenas dados nas últimas 2 horas
cutoff_unix = unix_timestamp(current_timestamp()) - TIME_THRESHOLD
df_airquality = df_airquality.filter(unix_timestamp(col("timestamp")) >= cutoff_unix)

# Parâmetros de qualidade do ar relevantes
aqi_params = ["pm25", "pm10", "no2", "o3", "co", "so2"]
df_airquality = df_airquality.filter(col("parameter").isin(aqi_params))

# Adiciona coluna com hora do dia
df_airquality = df_airquality.withColumn("hour", hour("timestamp"))

# Remove valores extremos (5º e 95º percentil)
quantiles = df_airquality.approxQuantile("value", [0.05, 0.95], 0.05)
if not quantiles or len(quantiles) < 2:
    print("⚠️ Não foi possível calcular quantis para o filtro de outliers.")
else:
    df_airquality = df_airquality.filter(
        (col("value") >= quantiles[0]) & (col("value") <= quantiles[1])
    )

# Estatísticas por cidade, parâmetro e hora
stats_hourly = df_airquality.groupBy("city", "parameter", "hour").agg(
    mean("value").alias("avg_value"),
    stddev("value").alias("std_value")
)

# Junta estatísticas ao dataframe
df_airquality = df_airquality.join(stats_hourly, on=["city", "parameter", "hour"])

# Z-score e severidade
df_airquality = df_airquality.withColumn(
    "aqi_z_score", abs((col("value") - col("avg_value")) / col("std_value"))
)
df_airquality = df_airquality.withColumn(
    "anomaly_level",
    when(col("aqi_z_score") > 3, "extrema")
    .when(col("aqi_z_score") > 2.5, "severa")
    .when(col("aqi_z_score") > 2, "moderada")
)

# Estatísticas IQR
iqr_stats = df_airquality.groupBy("city", "parameter").agg(
    expr("percentile_approx(value, 0.25)").alias("Q1"),
    expr("percentile_approx(value, 0.75)").alias("Q3")
)
df_airquality = df_airquality.join(iqr_stats, on=["city", "parameter"])
df_airquality = df_airquality.withColumn("IQR", col("Q3") - col("Q1"))

# Anomalias via IQR
df_airquality = df_airquality.withColumn(
    "is_iqr_anomaly",
    (col("value") < (col("Q1") - 1.5 * col("IQR"))) |
    (col("value") > (col("Q3") + 1.5 * col("IQR")))
)

# Filtrar anomalias (Z-score ou IQR)
df_airquality_anomalies = df_airquality.filter(
    (col("aqi_z_score") > 2) | (col("is_iqr_anomaly") == True)
)

# Mostrar exemplos
df_airquality_anomalies.select(
    "timestamp", "city", "parameter", "value",
    "avg_value", "std_value", "aqi_z_score", "anomaly_level", "is_iqr_anomaly"
).show(10, truncate=False)

############## Refinamento e Coordenadas ##############

# Lê os arquivos de cidades com lat/long
df_cities = pd.read_json("/shared/cities.json", orient="index").reset_index()
df_cities.columns = ["city", "lat", "long"]
cities_dict = df_cities.to_dict(orient="index")

# Converte para DataFrame do Spark
df_cities = spark.createDataFrame(list(cities_dict.values()))

# Junta coordenadas
df_airquality_anomalies = df_airquality_anomalies.join(df_cities, on="city", how="left").drop("hour")

# Substitui valores inválidos
df_airquality_anomalies = df_airquality_anomalies.replace([np.nan, float('inf'), float('-inf')], None)
df_airquality_anomalies = df_airquality_anomalies.fillna({'std_value': 0, 'aqi_z_score': 0})

# Ajusta nomes: UF e cidade separadas
df_airquality_anomalies = df_airquality_anomalies.withColumn("uf", upper(split("city", "-")[0])) \
                                                 .withColumn("city", split("city", "-")[1]) \
                                                 .dropDuplicates()

# Salva resultados refinados
df_airquality_anomalies.write.mode("overwrite").parquet(f"s3a://{refined_bucket}/airquality_recent_anomalies_with_coords")

print("✅ Análise de qualidade do ar recente concluída e salva no MinIO!")
spark.stop()
