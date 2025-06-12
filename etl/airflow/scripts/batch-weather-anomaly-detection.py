from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, abs, to_timestamp, hour, expr, when, split, upper
import pandas as pd
import numpy as np
import os

# MinIO/S3 configs
minio_endpoint = os.environ['MINIO_ENDPOINT']
minio_user = os.environ['MINIO_ROOT_USER']
minio_password = os.environ['MINIO_ROOT_PASSWORD']
raw_bucket = "raw"
refined_bucket = "refined"

spark = (
    SparkSession
    .builder
    .master("spark://spark-master:7077")
    .appName("Weather Anomaly Detection")
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

############## Análise do Clima ##############

# Analiza os dados de clima (JSON)
df_weather = spark.read.json(f"s3a://{raw_bucket}/weather/*/*.json")
df_weather.printSchema()

# Converte o campo de tempo para timestamp se necessário
df_weather = df_weather.withColumn("timestamp", to_timestamp(col("time")))

# Adiciona coluna com hora do dia
df_weather = df_weather.withColumn("hour", hour("timestamp"))

# Remove outliers extremos (temperatura fora do 5º-95º percentil)
quantiles = df_weather.approxQuantile("temperature", [0.05, 0.95], 0.05)
df_weather = df_weather.filter(
    (col("temperature") >= quantiles[0]) & (col("temperature") <= quantiles[1])
)

# Estatísticas por cidade e hora
stats_weather = df_weather.groupBy("city", "hour").agg(
    mean("temperature").alias("avg_temp"),
    stddev("temperature").alias("std_temp")
)

# Junta as estatísticas no dataframe
df_weather = df_weather.join(stats_weather, on=["city", "hour"])

# Calcula z-score
df_weather = df_weather.withColumn(
    "temp_z_score", abs((col("temperature") - col("avg_temp")) / col("std_temp"))
)

# Classificação da severidade
df_weather = df_weather.withColumn(
    "anomaly_level",
    when(col("temp_z_score") > 3, "extrema")
    .when(col("temp_z_score") > 2.5, "severa")
    .when(col("temp_z_score") > 2, "moderada")
)

# Estatísticas de IQR (por cidade)
iqr_stats = df_weather.groupBy("city").agg(
    expr("percentile_approx(temperature, 0.25)").alias("Q1"),
    expr("percentile_approx(temperature, 0.75)").alias("Q3")
)
df_weather = df_weather.join(iqr_stats, on="city")
df_weather = df_weather.withColumn("IQR", col("Q3") - col("Q1"))

# Detecta anomalias por IQR
df_weather = df_weather.withColumn(
    "is_iqr_anomaly",
    (col("temperature") < (col("Q1") - 1.5 * col("IQR"))) |
    (col("temperature") > (col("Q3") + 1.5 * col("IQR")))
)

# Filtra apenas registros anômalos por z-score ou IQR
df_weather_anomalies = df_weather.filter(
    (col("temp_z_score") > 2) | (col("is_iqr_anomaly") == True)
)

# Exibe os resultados
df_weather_anomalies.select(
    "timestamp", "city", "temperature", "avg_temp", "std_temp",
    "temp_z_score", "anomaly_level", "is_iqr_anomaly"
).show(10, truncate=False)

# Salva os resultados no S3
df_weather_anomalies.write.mode("overwrite").parquet(
    f"s3a://{raw_bucket}/anomalies/weather"
)

############## Refinamento e Coordenadas ##############

# Lê os arquivos de cidades com lat/long
df_cities = pd.read_json("/shared/cities.json", orient="index").reset_index()
df_cities.columns = ["city", "lat", "long"]
cities_dict = df_cities.to_dict(orient="index")

# Converte para DataFrame do Spark
df_cities = spark.createDataFrame(list(cities_dict.values()))

# Anomalias do clima
df_weather_anomalies = df_weather_anomalies.join(df_cities, on="city", how="left")

# Quebra o UF-cidade em duas colunas separadas
df_weather_anomalies = df_weather_anomalies.withColumn("uf", upper(split("city", "-")[0])) \
                                           .withColumn("city", split("city", "-")[1])\
                                           .dropDuplicates()
                                           
df_weather_anomalies.write.mode("overwrite").parquet(f"s3a://{refined_bucket}/weather_anomalies_with_coords")

############################

print("✅ Análise de clima concluída e salva no MinIO!")
spark.stop()
