import time
import pandas as pd
import io
from kafka import KafkaConsumer
import json
import os
import boto3

# Kafka config
KAFKA_TOPIC = 'airquality'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']

# MinIO config
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY = os.environ['MINIO_ROOT_PASSWORD']
BUCKET_NAME = 'raw'

# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# MinIO client
s3 = boto3.client('s3',
    endpoint_url=f'http://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

# Verifica se o bucket existe, caso contrário cria
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
except:
    s3.create_bucket(Bucket=BUCKET_NAME)

# Buffer config
BUFFER_SIZE = 100
BATCH_INTERVAL = 30  # segundos
buffer = []
last_flush = time.time()

def save_parquet_to_minio(data_list):
    if not data_list:
        return
    df = pd.DataFrame(data_list)

    # Se o campo timestamp do dado existe, converta para datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    else:
        df['timestamp'] = pd.NaT

    # Para registros sem timestamp, usar timestamp atual
    df['timestamp'].fillna(pd.Timestamp.now(), inplace=True)

    # Pega timestamp mínimo para usar no path
    min_ts = df['timestamp'].min()

    # Formata path com partições por ano, mês, dia
    prefix = f"airquality/year={min_ts.year}/month={min_ts.month:02d}/day={min_ts.day:02d}"
    filename = f"airquality_{min_ts.strftime('%Y%m%d-%H%M%S')}.parquet"
    key = f"{prefix}/{filename}"

    # Salvar parquet em buffer com timestamps em microssegundos
    buffer_io = io.BytesIO()
    df.to_parquet(
        buffer_io,
        index=False,
        engine='pyarrow',
        coerce_timestamps='us'
    )
    buffer_io.seek(0)

    # Upload para MinIO
    print(f"📥 Salvando {len(data_list)} registros no MinIO como {key}")
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer_io.read())


for message in consumer:
    buffer.append(message.value)
    now = time.time()
    if len(buffer) >= BUFFER_SIZE or now - last_flush > BATCH_INTERVAL:
        save_parquet_to_minio(buffer)
        buffer.clear()
        last_flush = now
