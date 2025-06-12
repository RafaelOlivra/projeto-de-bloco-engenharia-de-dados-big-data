from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, upper, col, to_timestamp, split
import pandas as pd
import numpy as np
import os

# MinIO/S3 configs
minio_endpoint = "http://minio:9000"
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

############################

# Lê os arquivos de cidades com lat/long
df_cities = pd.read_json("/shared/cities.json", orient="index").reset_index()
df_cities.columns = ["city", "lat", "long"]
cities_dict = df_cities.to_dict(orient="index")

# Converte para DataFrame do Spark
df_cities = spark.createDataFrame(list(cities_dict.values()))

############## Processa dados do Clima ##############

# Le os dados de clima (JSON)
df_weather = spark.read.json(f"s3a://{raw_bucket}/weather/*/*.json")
df_weather.printSchema()

# Une os dados de clima com as coordenadas das cidades
df_weather = df_weather.join(df_cities, on="city", how="left")

# Quebra o UF-cidade em duas colunas separadas e renomeia a coluna de tempo
df_weather = df_weather.withColumn("uf", upper(expr("split(city, '-')[0]"))) \
                       .withColumn("city", expr("split(city, '-')[1]")) \
                       .withColumn("time", to_timestamp(col("time"))) \
                       .withColumnRenamed("time", "timestamp") \
                       .dropDuplicates()
                       
df_weather.write.mode("overwrite").parquet(f"s3a://{refined_bucket}/weather_with_coords")

############## Processa dados de Qualidade do Ar ##############

# Lê os dados de qualidade do ar (Parquet)
df_airquality = spark.read.parquet(f"s3a://{raw_bucket}/airquality/*/*/*/*.parquet")
df_airquality.printSchema()

# Une os dados de qualidade do ar com as coordenadas das cidades
df_airquality = df_airquality.join(df_cities, on="city", how="left")

# Quebra o UF-cidade em duas colunas separadas
df_airquality = df_airquality.withColumn("uf", upper(split("city", "-")[0])) \
                             .withColumn("city", split("city", "-")[1]) \
                             .dropDuplicates()
                             
df_airquality.write.mode("overwrite").parquet(f"s3a://{refined_bucket}/airquality_with_coords")

############################

print("✅ Dados processados salvos com sucesso!")
spark.stop()
