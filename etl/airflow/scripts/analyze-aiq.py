from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, abs, to_timestamp, hour, expr, when
import os

# MinIO/S3 configs
minio_endpoint = "http://minio:9000"
minio_user = os.environ['MINIO_ROOT_USER']
minio_password = os.environ['MINIO_ROOT_PASSWORD']
bucket = "raw"

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

############################

# Analisa os dados de qualidade do ar (Parquet)
df_airquality = spark.read.parquet(f"s3a://{bucket}/airquality/*/*/*/*.parquet")
df_airquality.printSchema()

# Lê os dados Parquet da qualidade do ar
df_airquality = spark.read.parquet(f"s3a://{bucket}/airquality/*/*/*/*.parquet")

# Ajusta timestamp se for string
df_airquality = df_airquality.withColumn("timestamp", to_timestamp(col("timestamp")))

# Filtro para apenas parâmetros relevantes de AQI
aqi_params = ["pm25", "pm10", "no2", "o3", "co", "so2"]
df_airquality = df_airquality.filter(col("parameter").isin(aqi_params))

# Adiciona coluna com hora do dia
df_airquality = df_airquality.withColumn("hour", hour("timestamp"))

# Remove valores extremos antes de calcular estatísticas (5º e 95º percentil)
quantiles = df_airquality.approxQuantile("value", [0.05, 0.95], 0.05)
df_airquality = df_airquality.filter(
    (col("value") >= quantiles[0]) & (col("value") <= quantiles[1])
)

# Calcula estatísticas por cidade, parâmetro e hora
stats_hourly = df_airquality.groupBy("city", "parameter", "hour").agg(
    mean("value").alias("avg_value"),
    stddev("value").alias("std_value")
)

# Junta as estatísticas no DataFrame original
df_airquality = df_airquality.join(stats_hourly, on=["city", "parameter", "hour"])

# Calcula Z-score e marca anomalias com severidade
df_airquality = df_airquality.withColumn(
    "aqi_z_score", abs((col("value") - col("avg_value")) / col("std_value"))
)

df_airquality = df_airquality.withColumn(
    "anomaly_level",
    when(col("aqi_z_score") > 3, "extrema")
    .when(col("aqi_z_score") > 2.5, "severa")
    .when(col("aqi_z_score") > 2, "moderada")
)

# Calcula IQR para reforçar detecção robusta de outliers
iqr_stats = df_airquality.groupBy("city", "parameter").agg(
    expr("percentile_approx(value, 0.25)").alias("Q1"),
    expr("percentile_approx(value, 0.75)").alias("Q3")
)
df_airquality = df_airquality.join(iqr_stats, on=["city", "parameter"])
df_airquality = df_airquality.withColumn("IQR", col("Q3") - col("Q1"))

# Marca anomalias com base no IQR (regra de 1.5*IQR)
df_airquality = df_airquality.withColumn(
    "is_iqr_anomaly",
    (col("value") < (col("Q1") - 1.5 * col("IQR"))) |
    (col("value") > (col("Q3") + 1.5 * col("IQR")))
)

# Filtra anomalias detectadas por Z-score OU por IQR
df_airquality_anomalies = df_airquality.filter(
    (col("aqi_z_score") > 2) | (col("is_iqr_anomaly") == True)
)

# Mostra exemplos
df_airquality_anomalies.select(
    "timestamp", "city", "parameter", "value",
    "avg_value", "std_value", "aqi_z_score", "anomaly_level", "is_iqr_anomaly"
).show(10, truncate=False)

# Salva os resultados
df_airquality_anomalies.write.mode("overwrite").parquet(f"s3a://{bucket}/anomalies/airquality")

############################
print("✅ Análise de qualidade do ar concluída e salva no MinIO!")
spark.stop()
